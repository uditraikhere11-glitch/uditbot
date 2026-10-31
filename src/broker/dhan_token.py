"""
Shared Dhan Access Token Coordinator (adopted from `darvas_script/client_agent/dhan_token.py`).

Ensures that multiple processes on the same VM (e.g. main trading bot daemon, CLI `bot check-dhan`,
or scheduled daily refresh) share a single Dhan access token safely via an `fcntl.flock` lockfile
and PID/thread-unique `0600` atomic file replacement (`V12-20`, `CV13-06`), preventing
self-invalidation and concurrent temp-file collisions.
"""

from contextlib import contextmanager
import fcntl
import logging
import os
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)

TOKEN_FILE = os.environ.get("DHAN_TOKEN_FILE") or os.path.join(os.path.expanduser("~"), ".dhan_token")


def is_token_error(status_code: int, body: object) -> bool:
    """
    CV13-06: Official Dhan v2 authentication error classifier.
    Matches HTTP 401/403, DH-901 (Invalid Authentication), Data API auth codes 807/808/809,
    or explicit 'invalid token' / 'access token is invalid' / 'token is expired' messages.
    Does NOT misclassify DH-906 Order_Error (e.g. circuit limit or RMS rejection) as a token error.
    """
    text = str(body or "").lower()
    if "order_error" in text and "invalid token" not in text and "access token" not in text:
        return False
    return (
        status_code in (401, 403)
        or "dh-901" in text
        or ("dh-906" in text and ("invalid token" in text or "access token" in text or "auth" in text))
        or "invalid token" in text
        or "unauthorized" in text
        or "access token is invalid" in text
        or "token is expired" in text
        or '"807"' in text or ": 807" in text or 'errorcode": "807' in text or "807" in text.split()
        or '"808"' in text or ": 808" in text or 'errorcode": "808' in text
        or '"809"' in text or ": 809" in text or 'errorcode": "809' in text or "809" in text.split()
    )


def read_token(path: Optional[str] = None) -> str:
    target = os.path.expanduser(path or TOKEN_FILE)
    try:
        with open(target, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""
    except Exception as e:
        logger.error(f"Error reading Dhan token file {target}: {e}")
        return ""


def write_token_atomic(token: str, path: Optional[str] = None) -> None:
    target = os.path.expanduser(path or TOKEN_FILE)
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    tmp = f"{target}.{os.getpid()}.{threading.get_ident()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(token)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, target)
        os.chmod(target, 0o600)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


@contextmanager
def locked_token_file(path: Optional[str] = None):
    target = os.path.expanduser(path or TOKEN_FILE)
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    fd = os.open(f"{target}.lock", os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def fresh_token(
    rejected: str,
    login: Callable[[], Optional[str]],
    path: Optional[str] = None,
) -> Optional[str]:
    """
    Returns a usable token after `rejected` failed (pass '' when there is no token yet).
    Adopts a newer token from the shared file if another process already logged in;
    otherwise calls `login()` and writes the new token atomically with 0600 permissions.
    """
    target = os.path.expanduser(path or TOKEN_FILE)
    with locked_token_file(target):
        shared = read_token(target)
        if shared and shared != rejected:
            logger.info("Adopted fresh Dhan token from shared token file (no new TOTP login needed).")
            return shared
        token = login()
        if token:
            write_token_atomic(token, target)
        return token


def force_login(
    login: Callable[[], Optional[str]],
    path: Optional[str] = None,
) -> Optional[str]:
    """Scheduled daily 08:00 refresh: always performs TOTP login under lock and shares the new token."""
    target = os.path.expanduser(path or TOKEN_FILE)
    with locked_token_file(target):
        token = login()
        if token:
            write_token_atomic(token, target)
        return token
