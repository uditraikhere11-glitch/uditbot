"""Versioned HMAC fingerprint of the *same* effective credentials on both sides.

Do not store raw/PIN/TOTP/Telegram credentials, and do not use an unsalted
low-entropy password hash in the runtime status file. This key is local to the
bot's private DATA_DIR and must be preserved across upgrades and backups.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
from typing import Any, Mapping


class CredentialEvidenceError(RuntimeError):
    pass


def _get_key(data_dir: str) -> bytes:
    d = Path(data_dir).resolve()
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    p = d / ".tracker_credential_hmac_key"
    if p.is_symlink():
        raise CredentialEvidenceError("Credential fingerprint key must not be a symlink")
    try:
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        pass
    else:
        try:
            os.fchmod(fd, 0o600)
            key = secrets.token_bytes(32)
            if os.write(fd, key) != 32:
                raise CredentialEvidenceError("Credential fingerprint key not durably written")
            os.fsync(fd)
        finally:
            os.close(fd)
        directory_fd = os.open(d, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    try:
        st = p.lstat()
        if not p.is_file() or p.is_symlink() or st.st_mode & 0o077:
            raise CredentialEvidenceError("Credential fingerprint key has unsafe permissions")
        fd = os.open(p, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            key = os.read(fd, 33)
        finally:
            os.close(fd)
        if len(key) != 32:
            raise CredentialEvidenceError("Credential fingerprint key is invalid")
        return key
    except OSError as exc:
        raise CredentialEvidenceError("Credential fingerprint key inaccessible") from exc


def fingerprint(config: Any, data_dir: str) -> str:
    """Only compare fingerprints derived from this exact canonical function."""
    if is_dataclass(config):
        source: Mapping[str, Any] = asdict(config)
    elif isinstance(config, Mapping):
        source = config
    else:
        raise CredentialEvidenceError("Unsupported credential source")
    # Reuse the repository's canonical secret aliasing. Source is never printed
    # or included in runtime status; HMAC protects low-entropy account PINs.
    from src.engine.config_manager import _extract_normalized_secrets
    normalized = _extract_normalized_secrets(dict(source))
    encoded = json.dumps(
        normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hmac.new(_get_key(data_dir), b"uditbot-credential-v2\0" + encoded, hashlib.sha256).hexdigest()
