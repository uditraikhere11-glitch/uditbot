"""
Process Singleton Lock and Listener Lease Manager.
Implements STEP 23:
- Guarantees exactly one active Telegram listener and executor per VM instance.
- Prevents split-brain duplicate orders from accidental multi-process launches.
- Records PID, hostname, and acquisition timestamp in data/listener.lock.
- Retains lock file inode on clean release to prevent race conditions.
"""

import fcntl
import logging
import os
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_LOCK_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "listener.lock",
)


class SingletonListenerLock:
    def __init__(self, lock_path: Optional[str] = None):
        self.lock_path = lock_path or os.environ.get("LISTENER_LOCK_FILE") or DEFAULT_LOCK_FILE
        self._fd: Optional[int] = None

    def acquire(self) -> Tuple[bool, str]:
        """
        Attempts non-blocking exclusive flock on lock file.
        Returns: (acquired: bool, message: str)
        """
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.lock_path)), exist_ok=True)
            self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Write lease info
            os.ftruncate(self._fd, 0)
            os.lseek(self._fd, 0, os.SEEK_SET)
            lease_info = f"pid={os.getpid()};acquired_at={time.time()};instance=AuditBot\n"
            os.write(self._fd, lease_info.encode("utf-8"))

            logger.info(f"🔒 [LEASE] Singleton listener lock acquired by PID {os.getpid()}.")
            return True, f"Lock acquired (PID {os.getpid()})"
        except (BlockingIOError, OSError) as e:
            # Read existing holder info if available
            holder_info = "Unknown PID"
            try:
                if self._fd is not None:
                    os.lseek(self._fd, 0, os.SEEK_SET)
                    raw = os.read(self._fd, 128).decode("utf-8", errors="ignore")
                    if raw:
                        holder_info = raw.strip()
            except Exception:
                pass
            logger.critical(f"🚨 [DUPLICATE RUNNER BLOCKED] Another listener instance holds lease: {holder_info}")
            return False, f"Duplicate runner blocked: {holder_info}"

    def release(self):
        """Releases lock on clean shutdown without unlinking path to preserve inode."""
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except Exception:
                pass
            self._fd = None
            logger.info("🔓 [LEASE] Singleton listener lock released (file descriptor closed).")
