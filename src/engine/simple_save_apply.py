"""Simple synchronous Save & Apply; does not create config-tracker transactions.

This does not change the trading engine, Dhan broker protections, emergency STOP,
or the owner-controlled Start/Stop service actions.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Callable, Mapping

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, patch_env, revision,
    validate_effective_config, parse_env_strict,
)


@dataclass(frozen=True)
class SaveApplyResult:
    status: str
    message: str
    saved: bool
    restart_performed: bool
    revision: str
    changed_keys: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": True, "status": self.status, "message": self.message,
            "saved": self.saved, "restart_performed": self.restart_performed,
            "revision": self.revision, "changed_keys": list(self.changed_keys),
        }


@contextmanager
def _short_lock(directory: Path):
    """Short-lived flock, not a persisted tracker transaction or lease."""
    lock_path = directory / ".simple_save_apply.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _atomic_env_write(path: Path, content: str) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".env.apply.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if os.path.exists(temporary):
            os.unlink(temporary)


def trading_service_state() -> str:
    result = subprocess.run(
        ["systemctl", "show", "trading_bot.service", "-p", "ActiveState", "--value"],
        capture_output=True, text=True, timeout=6, check=False,
    )
    if result.returncode != 0:
        raise ConfigurationConflict("Cannot inspect trading service state")
    state = result.stdout.strip()
    if state not in ("active", "inactive"):
        raise ConfigurationConflict("Trading service is transitioning or unavailable; retry after checking status")
    return state


def restart_trading_service() -> None:
    result = subprocess.run(
        ["sudo", "-n", "/usr/bin/systemctl", "try-restart", "trading_bot.service"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if result.returncode != 0:
        raise ConfigurationConflict(
            "Settings saved but trading service restart failed. Inspect systemctl before retrying."
        )


def apply_settings(
    values: Mapping[str, Any], *, env_path: str | Path,
    expected_revision: str, confirm_live: bool = False,
    state_reader: Callable[[], str] = trading_service_state,
    restart: Callable[[], None] = restart_trading_service,
) -> SaveApplyResult:
    """Apply validated field changes; restart the bot once if already active.

    No V2 restart journal, older pending-configuration ledger, or tracker popup.
    A bot stopped by its owner remains stopped; no automatic LIVE start.
    """
    path = Path(env_path).absolute()
    if path.is_symlink() or not path.is_file():
        raise ConfigurationConflict("Configuration file missing or unsafe")
    if not isinstance(expected_revision, str) or len(expected_revision) != 64:
        raise ConfigurationConflict("Refresh Setup: missing configuration revision")
    with _short_lock(path.parent):
        if path.is_symlink():
            raise ConfigurationConflict("Configuration symlinked during save")
        with path.open("r", encoding="utf-8", newline="") as fh:
            existing = fh.read()
        if revision(existing) != expected_revision:
            raise ConfigurationConflict("Settings changed since page loaded. Refresh before saving.")
        proposed, changed = patch_env(existing, values, expected_revision=expected_revision)
        if not changed:
            return SaveApplyResult("UNCHANGED", "Settings already up to date; no restart needed.",
                                   False, False, expected_revision, ())
        validate_effective_config(proposed)
        old_values, _ = parse_env_strict(existing)
        new_values, _ = parse_env_strict(proposed)
        if old_values.get("DHAN_CLIENT_ID") and old_values.get("DHAN_CLIENT_ID") != new_values.get("DHAN_CLIENT_ID"):
            raise ConfigurationRejected("Dhan account migration is not a settings restart")
        old_mode = old_values.get("TRADING_MODE", "TEST")
        new_mode = new_values.get("TRADING_MODE", old_mode)
        if old_mode != "LIVE" and new_mode == "LIVE" and not confirm_live:
            raise ConfigurationRejected("Confirm enabling LIVE before saving.")
        state = state_reader()
        if state not in ("active", "inactive"):
            raise ConfigurationConflict("Trading service state uncertain; no settings changed")
        _atomic_env_write(path, proposed)
        new_revision = revision(proposed)
        keys = tuple(sorted(changed))
        if state == "inactive":
            return SaveApplyResult(
                "SAVED_STOPPED",
                "Settings saved. Trading remains stopped; use Control Center Start Bot when ready.",
                True, False, new_revision, keys,
            )
        # Never resurrect a bot that stopped between saving and restart.
        # try-restart is also no-op if Owner STOP wins this race.
        if state_reader() != "active":
            return SaveApplyResult(
                "SAVED_STOPPED",
                "Settings saved; bot stopped during apply and was not restarted.",
                True, False, new_revision, keys,
            )
        try:
            restart()
        except (OSError, subprocess.SubprocessError, ConfigurationConflict) as exc:
            raise ConfigurationConflict(
                "Settings saved, but restart could not be verified. Check service status before retrying."
            ) from exc
        if state_reader() != "active":
            raise ConfigurationConflict(
                "Settings saved; bot did not return active. Inspect service before proceeding."
            )
        return SaveApplyResult(
            "RESTARTED",
            "Settings saved and trading service restarted. Check reconciliation before LIVE entries.",
            True, True, new_revision, keys,
        )
