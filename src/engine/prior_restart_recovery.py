"""One-shot recovery of historical V2 restart evidence, not a settings tracker.

This module NEVER stages settings, invokes systemctl restart, calls Dhan, clears
Emergency STOP or replays a legacy transaction. The durable journal and broker
barrier retain their existing filenames and locks for mixed-version cutovers.
It can prove late ACK when the historical V2 feature flag is absent.
"""
from __future__ import annotations

import os
import json
import tempfile
import re
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.engine.config_patch import ConfigurationConflict
from src.engine.config_manager import (
    ConfigChangeRecord, ConfigApplyState, ConfigChangeTracker, OperationState,
    compute_config_version, inspect_systemd_service_properties,
    parse_env_content_string,
)
from src.engine.prior_settings import prior_settings_lock, ChangeBlocked
from src.engine.tracker_credentials import fingerprint
from src.engine.runtime_evidence import proc_starttime, read_private_json
from src.engine.tracker_v2_safety import verify_runtime_ack, preview_legacy_tracker_migration
from src.engine.broker_handoff import release_apply_barrier, BrokerHandoffBlocked

_ID = re.compile(r"V2-[0-9a-f]{16}\Z")
_ALLOWED = frozenset(("RESTART_ISSUED", "VERIFYING", "ACK_UNCERTAIN", "VERIFIED"))


def _blocked(message: str) -> None:
    raise ConfigurationConflict("Prior restart recheck blocked: " + message)


def _write_verified_journal(path: Path, content: dict) -> None:
    """Atomic, private, fsynced historical status update; never touch .env."""
    if path.is_symlink() or not path.is_file() or path.parent.is_symlink():
        _blocked("unsafe historical journal path")
    fd, temp = tempfile.mkstemp(prefix=".prior-restart.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf8") as f:
            fd = -1
            json.dump(content, f, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        parent_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        if read_private_json(path, missing_ok=False) != content:
            _blocked("historical journal readback mismatch")
    finally:
        if fd >= 0:
            os.close(fd)
        if os.path.exists(temp):
            os.unlink(temp)


def _strict_env(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        _blocked("configuration file is not a regular file")
    try:
        return path.read_bytes()
    except OSError:
        _blocked("configuration file unreadable")


def _barrier_inactive_same_change(barrier: dict, tx: dict, account: str) -> bool:
    """Only the exact released barrier from the SAME transaction is resumable."""
    return (barrier.get("active") is False
            and barrier.get("change_id") == tx.get("change_id")
            and barrier.get("account_id") == account
            and type(barrier.get("released_at")) in (int, float)
            and barrier["released_at"] > 0)


def recover_prior_restart(
    tracker: ConfigChangeTracker,
    env_path: str,
    *,
    requested_change_id: str,
    check_service_fn: Any = None,
    ack_probe: Any = None,
    starttime_reader: Any = proc_starttime,
    release_fence: Any = None,
) -> ConfigChangeRecord:
    """Verify an *already issued* restart exactly once, including crash gaps.

    Under the old writer's original transaction flock, both the transaction
    and apply barrier are re-read. Success requires the prior journal's own
    committed expected hash, mode, credentials, account and PID identity.

    The barrier may already be released after a crash; only an inactive barrier
    with exact matching change/account/release evidence allows idempotent
    completion. Unknown/missing barriers, wrong account or a changed .env block.
    No restart callback exists in this API by design.
    """
    if not isinstance(requested_change_id, str) or not _ID.fullmatch(requested_change_id):
        _blocked("invalid historical restart ID")
    data = Path(tracker.data_dir)
    folder = data / "config_restart_v2"
    journal = folder / "current_transaction.json"
    barrier_path = folder / "apply_barrier.json"
    env = Path(env_path).absolute()
    with tracker.operation_lock(operation_type="PRIOR_RESTART_READBACK", operation_id=requested_change_id):
        with prior_settings_lock(folder):
            try:
                tx = read_private_json(journal, missing_ok=False)
                barrier = read_private_json(barrier_path, missing_ok=False)
            except (OSError, ValueError) as exc:
                _blocked("journal or barrier unreadable: " + type(exc).__name__)
            if tx.get("change_id") != requested_change_id:
                _blocked("unrelated restart journal")
            if tx.get("phase") not in _ALLOWED or tx.get("restart_issued") is not True:
                _blocked("no already-issued restart to verify")
            if barrier.get("change_id") != requested_change_id:
                _blocked("unrelated broker barrier")
            if tx.get("phase") == "VERIFIED" and barrier.get("active") is not False:
                _blocked("verified journal contradicts unreleased broker fence")
            raw = _strict_env(env)
            if sha256(raw).hexdigest() != tx.get("requested_file_revision"):
                _blocked("saved configuration changed since restart")
            try:
                cfg = AppConfig.from_env(env=parse_env_content_string(raw.decode("utf8")), env_file="")
                account = cfg.dhan_client_id
                expected_hash = compute_config_version(cfg)
                expected_credential = fingerprint(cfg, tracker.data_dir)
            except (ValueError, UnicodeError, OSError, TypeError):
                _blocked("current configuration/credential evidence unavailable")
            if (not account or barrier.get("account_id") != account
                    or tx.get("expected_runtime_hash") != expected_hash
                    or tx.get("credential_fingerprint") != expected_credential
                    or not isinstance(expected_credential, str)
                    or not re.fullmatch(r"[0-9a-f]{64}\Z", expected_credential)
                    or tx.get("target_mode") != cfg.trading_mode.value):
                _blocked("stored restart identity differs from current account, mode or credentials")
            old_pid = tx.get("previous_pid")
            old_start = tx.get("previous_starttime")
            if type(old_pid) is not int or old_pid <= 0 or type(old_start) is not int or old_start <= 0:
                _blocked("prior process PID/starttime missing")
            prior = tracker.get_change_record(requested_change_id)
            if not isinstance(prior, ConfigChangeRecord) or prior.change_id != requested_change_id:
                _blocked("historical UI change evidence missing")
            safe = preview_legacy_tracker_migration(data)
            if not safe.allowed:
                _blocked("owner STOP or other legacy safety evidence unresolved: " + safe.code)
            if barrier.get("active") is not True and not _barrier_inactive_same_change(barrier, tx, account):
                _blocked("broker barrier absent, altered or unproven")
            try:
                if ack_probe is None:
                    ack = tracker.get_active_running_version(require_live_pid=False)
                    service = inspect_systemd_service_properties("trading_bot.service", check_fn=check_service_fn)
                else:
                    ack, service = ack_probe()
                proof = verify_runtime_ack(
                    ack=ack, systemd=service,
                    expected_hash=tx["expected_runtime_hash"],
                    expected_mode=tx["target_mode"],
                    expected_credential_fingerprint=tx["credential_fingerprint"],
                    min_acknowledged_at=float(tx.get("restart_initiated_at") or 0),
                    starttime_reader=starttime_reader,
                    before_pid=old_pid, before_starttime=old_start,
                )
            except (OSError, ValueError, TypeError):
                _blocked("unable to obtain authoritative new process evidence")
            if not proof.allowed:
                # Keep the original journal and barrier intact when uncertain;
                # never overwrite VERIFIED history with a transient read error.
                if tx.get("phase") == "VERIFIED":
                    _blocked("previous VERIFIED record no longer has live process proof: " + proof.code)
                _blocked("late ACK cannot be verified: " + proof.code)
            # The operation_lock prevents a concurrent Owner STOP from winning
            # inside the same coordinator; explicitly recheck durable evidence.
            if not preview_legacy_tracker_migration(data).allowed:
                _blocked("owner STOP changed during readback")
            if sha256(_strict_env(env)).hexdigest() != tx["requested_file_revision"]:
                _blocked("external settings writer changed the saved file")
            # Runtime ACK alone cannot establish a trusted broker producer;
            # validate ledger identity under the broker fence protocol.
            try:
                ledger = read_private_json(data / "active_broker_mutations.json", missing_ok=False)
            except (OSError, ValueError):
                _blocked("broker mutation ledger is missing or unreadable")
            if (ledger.get("schema_version") != 2
                    or ledger.get("handoff_verified") is not True
                    or ledger.get("account_id") != account
                    or ledger.get("producer_pid") != ack.get("pid")
                    or ledger.get("producer_starttime") != ack.get("process_starttime")
                    or ledger.get("active") != [] or ledger.get("unresolved") != []):
                _blocked("broker mutation ledger differs from new runtime or is not empty")
            if barrier.get("active") is True:
                try:
                    released = (
                        release_fence(requested_change_id, ack["pid"], ack["process_starttime"])
                        if release_fence is not None else
                        release_apply_barrier(data, account, requested_change_id,
                                              ack["pid"], ack["process_starttime"])
                    )
                except (BrokerHandoffBlocked, OSError, ValueError):
                    released = False
                if not released:
                    _blocked("broker barrier release denied; never retry the restart")
            # A crash can occur after the broker fence release but before the
            # journal becomes VERIFIED. Readback makes that window idempotent.
            try:
                after = read_private_json(barrier_path, missing_ok=False)
            except (OSError, ValueError):
                _blocked("broker barrier release cannot be read back")
            if not _barrier_inactive_same_change(after, tx, account):
                _blocked("released barrier does not match the historical transaction")
            if tx.get("phase") != "VERIFIED":
                tx["phase"] = "VERIFIED"
                tx["reason"] = "Historical restart independently verified; no second restart"
                _write_verified_journal(journal, tx)
            prior.state = ConfigApplyState.APPLIED_VERIFIED
            prior.operation_state = OperationState.APPLIED_VERIFIED.value
            prior.runtime_verified = True
            prior.restart_performed = True
            prior.blocked_reason = ""
            prior.reason = "Historical restart independently verified; no second restart"
            prior.progress_percent = 100
            prior.current_step = "VERIFIED"
            prior.effective_version_hash = expected_hash
            prior.running_mode = cfg.trading_mode.value
            prior.verified_at = time.time()
            tracker.save_change_record(prior)
            check = tracker.get_change_record(requested_change_id)
            if check is None or check.state != ConfigApplyState.APPLIED_VERIFIED:
                _blocked("historical UI acknowledgement did not persist")
            tracker.dispatch_telegram_update(prior)
            return prior
