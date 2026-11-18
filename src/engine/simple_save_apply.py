"""Simple synchronous Save & Apply; does not create config-tracker transactions.

This does not change the trading engine, Dhan broker protections, emergency STOP,
or the owner-controlled Start/Stop service actions.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import os
import secrets
import time
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Callable, Mapping

from src.config import AppConfig
from src.engine.config_manager import (
    ConfigChangeTracker, compute_config_version, inspect_systemd_service_properties,
    is_isolated_test_environment,
)
from src.engine.tracker_v2_safety import (
    evaluate_active_broker_operation_gate, read_private_json,
)
from src.engine.broker_handoff import (
    acquire_apply_barrier, release_apply_barrier, BrokerHandoffBlocked,
)

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
    lock_path = directory / "config_operation.lock"
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


def _owner_stop_requested(env_file: Path, cfg: Mapping[str, str]) -> bool:
    """Read the independent Owner STOP record; do not replay any tracker flow."""
    directory = Path(cfg.get("DATA_DIR") or "data").expanduser()
    if not directory.is_absolute():
        directory = env_file.parent / directory
    control = directory / "service_control_state.json"
    try:
        if control.is_symlink():
            raise ConfigurationConflict("Owner STOP record is a symlink")
        if not control.exists():
            return False
        if control.stat().st_size > 65536:
            raise ConfigurationConflict("Owner STOP record is unexpectedly large")
        data = json.loads(control.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Owner STOP record must be an object")
        if data.get("_corrupted"):
            raise ValueError("Owner STOP record marked corrupted")
        return (
            data.get("deferred_stop_pending") is True
            or str(data.get("desired_service_state") or "").upper() == "STOPPED"
        )
    except (OSError, ValueError) as exc:
        raise ConfigurationConflict(
            "Owner STOP state unreadable; cannot restart trading"
        ) from exc


def _positive_running_restart_proof(path: Path, old_values: Mapping[str, str]) -> dict[str, Any]:
    """Require matching systemd/ACK/account/empty broker handoff before restart."""
    cfg = AppConfig.from_env(env=dict(old_values), env_file="")
    data = Path(cfg.data_dir).expanduser()
    if not data.is_absolute():
        data = path.parent / data
    data = data.resolve()
    tracker = ConfigChangeTracker(data_dir=str(data))
    props = inspect_systemd_service_properties("trading_bot.service")
    main_pid = int(props.get("MainPID") or 0)
    if props.get("ActiveState") != "active" or main_pid <= 0:
        raise ConfigurationConflict("Trading service state changed; restart refused")
    ok, ack, _ = tracker.is_active_version_confirmed(
        expected_hashes={compute_config_version(cfg)},
        expected_mode=str(getattr(cfg.trading_mode, "value", cfg.trading_mode)).upper(),
        require_systemd_active=True,
    )
    if not ok or not isinstance(ack, dict) or ack.get("pid") != main_pid:
        raise ConfigurationConflict("Current runtime PID and saved version not verified")
    gate = evaluate_active_broker_operation_gate(data, require_handoff=True)
    ledger = read_private_json(data / "active_broker_mutations.json", missing_ok=True)
    if (not gate.allowed or gate.code != "NO_ACTIVE_BROKER_OPERATION"
            or not isinstance(ledger, dict)
            or ledger.get("account_id") != cfg.dhan_client_id
            or gate.evidence.get("producer_pid") != main_pid
            or gate.evidence.get("producer_starttime") != ack.get("process_starttime")):
        raise ConfigurationConflict("Broker mutation handoff is active, unresolved or unverifiable")
    # Do not erase an Emergency Stop, an active order, or a prior barrier.
    return {"data": data, "account": cfg.dhan_client_id,
            "pid": main_pid, "starttime": ack["process_starttime"],
            "old_hash": compute_config_version(cfg)}


def _verify_new_runtime(handoff: dict[str, Any], new_values: Mapping[str, str]) -> None:
    """Read-only ACK polling; never send another restart on timeout."""
    cfg = AppConfig.from_env(env=dict(new_values), env_file="")
    tracker = ConfigChangeTracker(data_dir=str(handoff["data"]))
    expected = compute_config_version(cfg)
    mode = str(getattr(cfg.trading_mode, "value", cfg.trading_mode)).upper()
    deadline = time.monotonic() + 15
    while True:
        ok, ack, _ = tracker.is_active_version_confirmed(
            expected_hashes={expected}, expected_mode=mode,
            require_systemd_active=True,
        )
        if (ok and isinstance(ack, dict) and
                ack.get("pid") != handoff["pid"]):
            # The new producer must be registered with the same account before
            # the shared broker mutation barrier can be released.
            if not release_apply_barrier(
                handoff["data"], handoff["account"], handoff["barrier_id"],
                ack["pid"], ack["process_starttime"],
            ):
                raise ConfigurationConflict("New runtime ACK verified but broker fence remains held")
            return
        if time.monotonic() >= deadline:
            raise ConfigurationConflict(
                "Settings saved and one restart issued; new PID/version not verified. "
                "Do not repeat restart or clear broker fence."
            )
        time.sleep(0.25)


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
    from src.config import parse_env_dict
    bootstrap = parse_env_dict(str(path))
    data_root = Path(bootstrap.get("DATA_DIR") or "data").expanduser()
    if not data_root.is_absolute():
        data_root = path.parent / data_root
    data_root.mkdir(parents=True, exist_ok=True)
    with _short_lock(data_root):
        if path.is_symlink():
            raise ConfigurationConflict("Configuration symlinked during save")
        with path.open("r", encoding="utf-8", newline="") as fh:
            existing = fh.read()
        actual_revision = revision(existing)
        # An exact no-op retry cannot submit a second restart, even when the
        # browser resends the revision from the first successful save.
        if actual_revision != expected_revision:
            _, retry_changes = patch_env(existing, values)
            if not retry_changes:
                return SaveApplyResult(
                    "UNCHANGED", "Setting already saved; no second restart.",
                    False, False, actual_revision, (),
                )
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
        if state == "active" and _owner_stop_requested(path, old_values):
            raise ConfigurationConflict("Owner STOP is pending; refusing a configuration restart")
        # Production: cannot restart a LIVE watcher just because systemd
        # reports active; prove old PID/hash and no in-flight/UNKNOWN broker
        # mutation. Isolated tests inject the systemd state/restart.
        real_runtime = not is_isolated_test_environment()
        handoff = None
        if state == "active":
            if new_mode == "LIVE" and not confirm_live:
                raise ConfigurationRejected("Explicit LIVE restart confirmation required")
            if real_runtime:
                handoff = _positive_running_restart_proof(path, old_values)
                barrier_id = "SAVE-" + secrets.token_hex(8)
                try:
                    acquired = acquire_apply_barrier(
                        handoff["data"], handoff["account"], barrier_id,
                        handoff["pid"], handoff["starttime"],
                    )
                except (BrokerHandoffBlocked, OSError) as exc:
                    raise ConfigurationConflict("Broker restart fence could not be acquired") from exc
                if not acquired:
                    raise ConfigurationConflict("An active or unresolved broker mutation blocks restart")
                handoff["barrier_id"] = barrier_id
        # After the handoff barrier is acquired, no new Dhan mutations can
        # begin while we write the file and issue the single restart.
        _atomic_env_write(path, proposed)
        new_revision = revision(proposed)
        keys = tuple(sorted(changed))
        if state == "inactive":
            return SaveApplyResult(
                "SAVED_STOPPED",
                "Settings saved. Trading remains stopped; use Control Center Start Bot when ready.",
                True, False, new_revision, keys,
            )
        if _owner_stop_requested(path, old_values) or state_reader() != "active":
            # Keep an acquired broker fence if a STOP raced the update:
            # it may not be released without verified new producer evidence.
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
        if real_runtime and handoff is not None:
            _verify_new_runtime(handoff, new_values)
        elif state_reader() != "active":
            raise ConfigurationConflict(
                "Settings saved; bot did not return active. Inspect service before proceeding."
            )
        return SaveApplyResult(
            "RESTARTED",
            "Settings saved and trading service restarted. Check reconciliation before LIVE entries.",
            True, True, new_revision, keys,
        )
