"""Synchronize inspection of prior settings with already-running older writers."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path


from src.engine.config_patch import ConfigurationConflict


class ChangeBlocked(ConfigurationConflict):
    """Previous settings state cannot be handled safely."""


@contextmanager
def prior_settings_lock(folder: Path):
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    if folder.is_symlink() or folder.stat().st_mode & 0o022:
        raise ChangeBlocked("Tracker transaction directory is insecure")
    fd = os.open(folder / ".transaction.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
