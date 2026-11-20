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
    is_isolated_test_environment, write_json_atomic,
)
from src.engine.runtime_evidence import (
    evaluate_active_broker_operation_gate, read_private_json,
)
from src.engine.prior_settings import prior_settings_lock
from src.engine.broker_handoff import (
    acquire_apply_barrier, release_apply_barrier, release_aborted_apply_barrier, BrokerHandoffBlocked,
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
    """Prove old service, credentials and broker fence; diagnose without mutating.

    A missing/stale producer record is NOT an empty mutation ledger. Never
    register a producer from the WebUI or bypass an ambiguous broker outcome.
    """
    cfg = AppConfig.from_env(env=dict(old_values), env_file="")
    data = Path(cfg.data_dir).expanduser()
    if not data.is_absolute():
        data = path.parent / data
    data = data.resolve()
    tracker = ConfigChangeTracker(data_dir=str(data))
    props = inspect_systemd_service_properties("trading_bot.service")
    main_pid = int(props.get("MainPID") or 0)
    if props.get("ActiveState") != "active" or main_pid <= 0:
        raise ConfigurationConflict("RUNNING_SERVICE_UNVERIFIED: Trading service changed; no restart issued")
    ok, ack, _ = tracker.is_active_version_confirmed(
        expected_hashes={compute_config_version(cfg)},
        expected_mode=str(getattr(cfg.trading_mode, "value", cfg.trading_mode)).upper(),
        require_systemd_active=True,
    )
    if not ok or not isinstance(ack, dict) or ack.get("pid") != main_pid:
        raise ConfigurationConflict(
            "RUNTIME_ACK_UNVERIFIED: Running PID, mode, configuration hash or starttime "
            "cannot be confirmed; no restart issued"
        )

    # The running process must have acknowledged the SAME credentials, not
    # merely an equal non-secret configuration hash. Never create/rekey
    # credential evidence from a WebUI preflight.
    from src.engine.tracker_credentials import CredentialEvidenceError, fingerprint_existing
    try:
        expected_fp = fingerprint_existing(cfg, str(data))
    except (CredentialEvidenceError, OSError, ValueError):
        raise ConfigurationConflict(
            "CREDENTIAL_EVIDENCE_UNAVAILABLE: Runtime credential key cannot be read; "
            "controlled service verification required"
        ) from None
    actual_fp = ack.get("credential_fingerprint")
    if not isinstance(actual_fp, str) or not actual_fp:
        raise ConfigurationConflict(
            "RUNTIME_CREDENTIAL_ACK_MISSING: Running process did not acknowledge its "
            "credentials; deploy a verified producer before Save & Apply"
        )
    import hmac
    if not hmac.compare_digest(actual_fp, expected_fp):
        raise ConfigurationConflict(
            "RUNTIME_CREDENTIAL_MISMATCH: Running credentials differ from saved "
            "configuration; do not restart until account/session evidence is reconciled"
        )

    # Never conflate an empty mutation list with a valid, currently registered
    # producer. Keep every rejection visible as a non-secret reason code.
    gate = evaluate_active_broker_operation_gate(data, require_handoff=True)
    if not gate.allowed or gate.code != "NO_ACTIVE_BROKER_OPERATION":
        code = str(gate.code or "SAFETY_RECORD_UNVERIFIABLE")
        raise ConfigurationConflict(
            f"BROKER_HANDOFF_{code}: Broker mutation safety evidence blocks this "
            "restart; review producer, active orders, uncertainty and service state"
        )
    try:
        ledger = read_private_json(data / "active_broker_mutations.json", missing_ok=False)
        barrier = read_private_json(data / "config_restart_v2" / "apply_barrier.json", missing_ok=True)
    except (OSError, ValueError):
        raise ConfigurationConflict(
            "BROKER_HANDOFF_RECORD_UNREADABLE: Broker evidence cannot be verified; "
            "no restart issued"
        ) from None
    if ledger.get("account_id") != cfg.dhan_client_id:
        raise ConfigurationConflict(
            "BROKER_HANDOFF_ACCOUNT_MISMATCH: Registered broker account differs "
            "from saved configuration; no restart issued"
        )
    if gate.evidence.get("producer_pid") != main_pid:
        raise ConfigurationConflict(
            "BROKER_HANDOFF_PRODUCER_PID_MISMATCH: Registered broker producer is "
            "not the running trading service; controlled cutover required"
        )
    if gate.evidence.get("producer_starttime") != ack.get("process_starttime"):
        raise ConfigurationConflict(
            "BROKER_HANDOFF_STARTTIME_MISMATCH: Broker producer identity is stale; "
            "no restart issued"
        )
    if barrier is not None and barrier.get("active") is not False:
        raise ConfigurationConflict(
            "BROKER_RESTART_BARRIER_UNRESOLVED: Prior restart fence is active or "
            "unverifiable; verify the issued restart, never clear it blindly"
        )
    # acquire_apply_barrier() subsequently repeats admission under the SAME
    # broker interprocess lock used by ORDER POST/PUT/DELETE.
    return {"data": data, "account": cfg.dhan_client_id,
            "pid": main_pid, "starttime": ack["process_starttime"],
            "old_hash": compute_config_version(cfg)}


def _verify_new_runtime(handoff: dict[str, Any], new_values: Mapping[str, str]) -> None:
    """Read-only ACK polling; never send another restart on timeout."""
    cfg = AppConfig.from_env(env=dict(new_values), env_file="")
    tracker = ConfigChangeTracker(data_dir=str(handoff["data"]))
    expected = compute_config_version(cfg)
    mode = str(getattr(cfg.trading_mode, "value", cfg.trading_mode)).upper()
    from src.engine.tracker_credentials import CredentialEvidenceError, fingerprint_existing
    try:
        expected_credentials = fingerprint_existing(cfg, str(handoff["data"]))
    except (CredentialEvidenceError, OSError, ValueError):
        raise ConfigurationConflict(
            "RUNTIME_CREDENTIAL_EVIDENCE_UNAVAILABLE: Settings saved and a restart may "
            "have been issued; broker fence retained for verified recovery"
        ) from None
    deadline = time.monotonic() + 15
    while True:
        ok, ack, _ = tracker.is_active_version_confirmed(
            expected_hashes={expected}, expected_mode=mode,
            require_systemd_active=True,
        )
        if (ok and isinstance(ack, dict) and
                ack.get("pid") != handoff["pid"] and expected_credentials
                and ack.get("credential_fingerprint") == expected_credentials):
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


def _retire_previous_transaction(data: Path, path: Path, values: Mapping[str, str], current: str) -> None:
    folder = data / "config_restart_v2"
    if folder.is_symlink():
        raise ConfigurationConflict("Unsafe previous settings directory.")
    journal = folder / "current_transaction.json"
    if not journal.exists() and not journal.is_symlink():
        fence = read_private_json(folder / "apply_barrier.json", missing_ok=True)
        if fence is not None and fence.get("active") is not False:
            raise ConfigurationConflict("Broker restart fence is unresolved.")
        return
    # An old WebUI process may still hold the V2 transaction lock during rollout.
    with prior_settings_lock(folder):
        tx = read_private_json(journal, missing_ok=True)
        barrier = read_private_json(folder / "apply_barrier.json", missing_ok=True)
        if tx is None:
            if barrier is not None and barrier.get("active") is not False:
                raise ConfigurationConflict("Broker restart fence is unresolved.")
            return
        phase = tx.get("phase")
        prepared = (phase == "PREPARED" and tx.get("restart_issued") is False
                    and tx.get("previous_file_revision") == current)
        completed = (phase in ("VERIFIED", "SAVED_FOR_NEXT_START", "NO_CHANGE")
                     and current in (tx.get("requested_file_revision"), tx.get("direct_save_to_revision")))
        if not (prepared or completed):
            raise ConfigurationConflict("Previous settings operation needs verification before saving.")
        if barrier is not None and barrier.get("active") is not False:
            account = values.get("DHAN_CLIENT_ID", "")
            cid = tx.get("change_id")
            pid, start = tx.get("previous_pid"), tx.get("previous_starttime")
            if (barrier.get("active") is not True or not prepared or not account or not cid or barrier.get("change_id") != cid
                    or barrier.get("account_id") != account or type(pid) is not int or pid <= 0
                    or type(start) is not int or start <= 0):
                raise ConfigurationConflict("Previous broker fence identity is unverified.")
            def stopped():
                props = inspect_systemd_service_properties("trading_bot.service")
                return props.get("ActiveState") == "inactive" and str(props.get("MainPID")) == "0"
            if stopped():
                released = release_aborted_apply_barrier(data, account, cid, pid, start, service_stopped=stopped)
            else:
                proof = _positive_running_restart_proof(path, values)
                released = ((pid, start) == (proof["pid"], proof["starttime"])
                            and release_apply_barrier(data, account, cid, pid, start))
            if not released:
                raise ConfigurationConflict("Previous broker fence retained. Broker or service evidence is unresolved.")
        # Atomic archive retains the exact historical record, including both revisions.
        journal.rename(folder / ("retired-" + secrets.token_hex(12) + ".json"))


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
    admission_check: Callable[[], None] | None = None,
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
    # A production handler always uses the standard systemd adapter. Loading
    # unittest.mock in an unrelated process must NOT turn off broker checks.
    injected_test_adapters = (
        is_isolated_test_environment()
        and state_reader is not trading_service_state
        and restart is not restart_trading_service
    )
    real_runtime = not injected_test_adapters
    with _short_lock(data_root):
        if path.is_symlink():
            raise ConfigurationConflict("Configuration symlinked during save")
        if real_runtime:
            # Historical configuration records are immutable audit evidence.
            # A pending, corrupted or ambiguous old operation blocks a new
            # write; never replay, rewrite or silently clear those records.
            tracker = ConfigChangeTracker(data_dir=str(data_root))
            if (tracker.has_unresolved_pending_operation()
                    or os.path.exists(tracker.deferred_env_file)
                    or os.path.exists(tracker.deferred_meta_file)):
                raise ConfigurationConflict("Historical deferred settings evidence needs separate review.")
        if admission_check is not None:
            admission_check()
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
        if real_runtime:
            from src.engine.tracker_v2_safety import preview_legacy_tracker_migration
            migration = preview_legacy_tracker_migration(
                data_root, allow_owner_stopped=(state == "inactive"),
            )
            if not migration.allowed:
                raise ConfigurationConflict(
                    "Prior configuration/Owner STOP safety evidence unresolved: " + migration.code
                )
        # Validate completed-V2 journal compatibility before any broker fence
        # or file write; do not claim a successor until the file is committed.
        _retire_previous_transaction(data_root, path, old_values, actual_revision)
        # Production: cannot restart a LIVE watcher just because systemd
        # reports active; prove old PID/hash and no in-flight/UNKNOWN broker
        # mutation. Isolated tests inject the systemd state/restart.
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
        # Check STOP again before the .env commit, while the broker fence is held.
        # A STOP that wins before commit must never be reported as success.
        if state == "active" and (
            _owner_stop_requested(path, old_values) or state_reader() != "active"
        ):
            # No .env write and no restart has happened. Release ONLY if
            # systemd positively confirms the service is stopped and the
            # broker ledger still contains no active/unknown mutations.
            recovered = False
            if handoff is not None and revision(path.read_text(encoding="utf-8")) == actual_revision:
                def _stopped_now() -> bool:
                    props = inspect_systemd_service_properties("trading_bot.service")
                    return (props.get("ActiveState") == "inactive"
                            and int(props.get("MainPID") or 0) == 0)
                try:
                    recovered = release_aborted_apply_barrier(
                        handoff["data"], handoff["account"], handoff["barrier_id"],
                        handoff["pid"], handoff["starttime"],
                        service_stopped=_stopped_now,
                    )
                except (BrokerHandoffBlocked, OSError, ValueError, ConfigurationConflict):
                    recovered = False
            raise ConfigurationConflict(
                "Owner STOP/service transition won before save; configuration unchanged. "
                + ("Pre-commit broker fence safely released; service remains stopped."
                   if recovered else
                   "Broker fence retained; inspect broker and service evidence before recovery.")
            )
        # After the handoff barrier is acquired, no new Dhan mutations can
        # begin while we write the file and issue the single restart.
        _atomic_env_write(path, proposed)
        # Preserve the existing last-known-good Telegram response destination
        # handoff on settings changes. No Telegram API call or session mutation.
        if "DHAN_RESPONSE_GROUP_NAME" in changed:
            from src.notifier.response_activation import stage_destination_change
            try:
                stage_destination_change(
                    str(data_root),
                    old_values.get("DHAN_RESPONSE_GROUP_NAME", ""),
                    new_values.get("DHAN_RESPONSE_GROUP_NAME", ""),
                )
            except OSError as exc:
                raise ConfigurationConflict(
                    "Settings saved, but Telegram response destination handoff failed; restart was not issued"
                ) from exc
        new_revision = revision(proposed)
        keys = tuple(sorted(changed))
        if state == "inactive":
            return SaveApplyResult(
                "SAVED_STOPPED",
                "Settings saved. Trading remains stopped; use Control Center Start Bot when ready.",
                True, False, new_revision, keys,
            )
        if _owner_stop_requested(path, old_values) or state_reader() != "active":
            raise ConfigurationConflict(
                "Settings saved, but Owner STOP/service transition won before restart. "
                "No restart issued; broker fence retained for verified recovery."
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
        message = "Settings updated. Trading bot restarted and the new configuration was verified."
        if real_runtime:
            from src.journal.db import JournalDB
            try:
                journal = Path(new_values.get("JOURNAL_DB_PATH") or data_root / "trade_journal.db")
                db = JournalDB(str(path.parent / journal), account_id=new_values.get("DHAN_CLIENT_ID", ""))
                # Formatting is opt-in and must never suppress a verified save notice.
                # Only names of edited fields are exposed; never send credential values.
                try:
                    from src.notifier.message_designer import render as render_designed_message
                    styled = render_designed_message(
                        "settings_update",
                        {
                            "instance": new_values.get("INSTANCE_NAME") or "AuditBot",
                            "changed_keys": ", ".join(keys),
                            "mode": "LIVE" if str(new_values.get("IS_LIVE_MODE", "")).lower() in ("true", "1", "yes") else "TEST",
                        },
                        [message],
                        str(data_root),
                    )
                except Exception:
                    styled = None  # Revert to the proven, mandatory original card.
                db.enqueue_outbox("SETTINGS_RESTART_VERIFIED", {"text": styled if styled is not None else message})
            except Exception:
                message += " The Telegram confirmation could not be queued."
        return SaveApplyResult("RESTARTED", message, True, True, new_revision, keys)