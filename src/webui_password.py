"""Password-only WebUI authentication with an explicit insecure HTTP opt-in.

Default requires loopback or trusted encrypted ingress. The optional public
plaintext HTTP mode transmits passwords and session tokens without encryption;
only the operator may enable this acknowledged-risk compatibility mode.
"""
from __future__ import annotations

import getpass
import hashlib
import hmac
import json
import os
import secrets
import stat
import threading
import time
from pathlib import Path

from fastapi import Request

_ITERATIONS = 600_000
_SESSION_SECONDS = 8 * 3600
_MAX_FAILURES = 5
_LOCK_SECONDS = 300
_COOKIE = "uditbot_webui_session"
_PUBLIC_HTTP_ENV = "UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP"
_PUBLIC_HTTP_CONFIRM = "I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK"
_lock = threading.RLock()
_sessions: dict[str, tuple[float, str]] = {}
_failures: dict[str, tuple[int, float]] = {}


def auth_dir() -> Path:
    raw = Path(os.environ.get("UDITBOT_WEBUI_AUTH_DIR", "data")).expanduser()
    if raw.is_symlink():
        raise RuntimeError("WebUI auth directory must not be a symlink")
    return raw.resolve()


def password_file() -> Path:
    return auth_dir() / "webui_password.json"


def _secure_dir() -> None:
    directory = auth_dir()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeError("WebUI auth directory must be a private directory")
    if directory.stat().st_mode & 0o077:
        raise RuntimeError("WebUI auth directory permissions must be 0700")


def _read_verifier() -> dict | None:
    path = password_file()
    try:
        _secure_dir()
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("version") != 1:
            return None
        if data.get("iterations") != _ITERATIONS:
            return None
        if len(bytes.fromhex(data["salt"])) != 16 or len(bytes.fromhex(data["digest"])) != 32:
            return None
        return data
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return None


def provision(password: str) -> None:
    """Explicit local provisioning; never create a default credential."""
    if not isinstance(password, str) or len(password) < 14 or len(password) > 256:
        raise ValueError("Choose a password between 14 and 256 characters")
    if password.lower() == password or password.upper() == password or not any(c.isdigit() for c in password):
        raise ValueError("Use mixed case and at least one number")
    _secure_dir()
    salt = secrets.token_bytes(16)
    verifier = {
        "version": 1,
        "iterations": _ITERATIONS,
        "salt": salt.hex(),
        "digest": hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _ITERATIONS).hex(),
    }
    target = password_file()
    if target.is_symlink():
        raise RuntimeError("Refusing to replace an authentication symlink")
    temporary = target.with_name(target.name + "." + secrets.token_hex(6) + ".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(verifier, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    # Invalidate all sessions on password rotation, even in the running process.
    with _lock:
        _sessions.clear()


def password_configured() -> bool:
    return _read_verifier() is not None


def authenticate(password: str, address: str) -> bool:
    now = time.monotonic()
    with _lock:
        attempts, blocked_until = _failures.get(address, (0, 0.0))
        if blocked_until > now:
            return False
    record = _read_verifier()
    if record is None:
        return False
    derived = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(record["salt"]), _ITERATIONS,
    )
    accepted = hmac.compare_digest(derived, bytes.fromhex(record["digest"]))
    with _lock:
        if accepted:
            _failures.pop(address, None)
        else:
            attempts += 1
            _failures[address] = (attempts, now + _LOCK_SECONDS if attempts >= _MAX_FAILURES else 0.0)
    return accepted


def blocked(address: str) -> bool:
    with _lock:
        return _failures.get(address, (0, 0.0))[1] > time.monotonic()


def start_session() -> str:
    token = secrets.token_urlsafe(40)
    record = _read_verifier()
    if record is None:
        raise RuntimeError("No WebUI password provisioned")
    with _lock:
        _sessions[hashlib.sha256(token.encode()).hexdigest()] = (
            time.monotonic() + _SESSION_SECONDS, record["digest"]
        )
    return token


def valid_session(token: str) -> bool:
    if not token:
        return False
    now = time.monotonic()
    key = hashlib.sha256(token.encode()).hexdigest()
    with _lock:
        expiry, digest = _sessions.get(key, (0.0, ""))
        record = _read_verifier()
        if expiry <= now or record is None or not hmac.compare_digest(digest, record["digest"]):
            _sessions.pop(key, None)
            return False
    return True


def end_session(token: str) -> None:
    with _lock:
        _sessions.pop(hashlib.sha256(token.encode()).hexdigest(), None)


def cookie_name() -> str:
    return _COOKIE


def public_http_explicitly_enabled() -> bool:
    """Never enable public plaintext HTTP by accident or from a browser input."""
    return os.environ.get(_PUBLIC_HTTP_ENV, "") == _PUBLIC_HTTP_CONFIRM


def cookie_secure(request: Request) -> bool:
    """A Secure cookie will not work on explicit plaintext HTTP (known risk)."""
    if public_http_explicitly_enabled():
        return request.url.scheme == "https"
    return request.url.hostname not in ("127.0.0.1", "localhost", "::1")


def permitted_transport(request: Request) -> bool:
    """Require loopback by default; opt-in permits remote peers without trusting headers."""
    host = request.client.host if request.client else ""
    if host in ("127.0.0.1", "::1"):
        return True
    if host == "testclient" and os.environ.get("AUDITBOT_TEST_ISOLATION") == "1":
        return True
    return public_http_explicitly_enabled()


def cli() -> None:
    import sys
    if len(sys.argv) != 2 or sys.argv[1] != "provision":
        raise SystemExit("Usage: python -m src.webui_password provision")
    pw = getpass.getpass("Choose UDITBOT WebUI password (14+ characters): ")
    if not hmac.compare_digest(pw, getpass.getpass("Confirm password: ")):
        raise SystemExit("Passwords do not match; no changes made")
    provision(pw)
    print("WebUI password verifier provisioned locally; the password was not stored")


if __name__ == "__main__":
    cli()
