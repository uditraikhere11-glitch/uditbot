"""Opt-in adapter joining the authenticated Setup route to tracker V2.

Disabled by default. Reuses existing Form/CSRF/password processing and the
field registry; does not change trading BRAIN or a broker payload. Both the
legacy service lock and V2 transaction lock are held during the transaction.
Actual Dhan GET verification and the shared mutation fence are mandatory
before the one permitted restart.
"""
from __future__ import annotations

import os
import re
import time
from typing import Any, Mapping, Optional

from src.config import AppConfig
from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, patch_env, parse_env_strict,
    redact, revision, validate_effective_config,
)
from src.engine.config_manager import (
    ConfigChangeRecord, ConfigApplyState, ConfigChangeTracker, OperationState,
    ServiceOperatingState, compute_config_version,
    inspect_systemd_service_properties, parse_env_content_string,
    is_isolated_test_environment,
)
from src.engine.broker_handoff import (
    BrokerHandoffBlocked, acquire_apply_barrier, release_apply_barrier,
)
from src.engine.tracker_credentials import fingerprint
from src.engine.tracker_v2_candidate import CandidateCoordinator, ChangeBlocked
from src.engine.tracker_v2_safety import (
    SafetyDecision, evaluate_active_broker_operation_gate, proc_starttime,
    preview_legacy_tracker_migration,
)

_FLAG = "UDITBOT_TRACKER_V2_ENABLE"
_RESTART_STATE = {
    "NO_CHANGE": ConfigApplyState.SAVED_FOR_NEXT_START,
    "SAVED_FOR_NEXT_START": ConfigApplyState.SAVED_FOR_NEXT_START,
    "VERIFIED": ConfigApplyState.APPLIED_VERIFIED,
    "ACK_UNCERTAIN": ConfigApplyState.APPLY_FAILED,
    "ACTION_REQUIRED": ConfigApplyState.APPLY_FAILED,
}


def enabled() -> bool:
    return os.environ.get(_FLAG) == "1"


def _read_env(path: str) -> str:
    if os.path.islink(path) or not os.path.isfile(path):
        raise ConfigurationConflict("Tracked .env file is missing, unsafe or symlinked")
    with open(path, "r", encoding="utf8", newline="") as f:
        return f.read()


def _app_config(text: str) -> AppConfig:
    return AppConfig.from_env(env=parse_env_content_string(text), env_file="")


def submit_v2_webui_patch(
    tracker: ConfigChangeTracker, form_data: Mapping[str, Any], env_path: str,
    *, expected_revision: Optional[str], request_id: Optional[str],
    check_service_fn: Any = None, systemctl_restart_fn: Any = None,
    broker: Any = None,
) -> ConfigChangeRecord:
    """A single exact CAS save; RUNNING restarts only under positive evidence."""
    if not enabled():
        raise ConfigurationConflict("Tracker V2 is disabled")
    if not isinstance(expected_revision, str) or re.fullmatch("[0-9a-f]{64}", expected_revision) is None:
        raise ConfigurationConflict("Invalid revision; reload Setup")
    if not isinstance(request_id, str) or re.fullmatch("[A-Za-z0-9_.:-]{8,128}", request_id) is None:
        raise ConfigurationConflict("Invalid request id; reload Setup")
    env_path = os.path.abspath(env_path)
    with tracker.operation_lock(operation_type="TRACKER_V2_SAVE", operation_id=request_id):
        original = _read_env(env_path)
        incoming, changed = patch_env(original, dict(form_data), expected_revision=expected_revision)
        validate_effective_config(incoming)
        old_parsed, _ = parse_env_strict(original)
        new_parsed, _ = parse_env_strict(incoming)
        old_mode = str(old_parsed.get("TRADING_MODE") or "TEST").upper()
        new_mode = str(new_parsed.get("TRADING_MODE") or old_mode).upper()
        if old_mode not in ("TEST", "LIVE") or new_mode not in ("TEST", "LIVE"):
            raise ConfigurationRejected("Invalid trading mode")
        if old_mode != "LIVE" and new_mode == "LIVE" and form_data.get("confirm_live") not in (
            True, "true", "yes", "1", "on"
        ):
            raise ConfigurationRejected("Enabling LIVE requires explicit owner confirmation")
        old_cfg = _app_config(original)
        new_cfg = _app_config(incoming)
        if new_cfg.dhan_client_id != old_cfg.dhan_client_id:
            # An account switch must undergo a separate broker/journal migration.
            raise ConfigurationConflict("Broker account changes require separate account migration")
        ctrl = tracker.get_service_control_record()
        if ctrl.get("_corrupted") or ctrl.get("deferred_stop_pending") is True:
            raise ConfigurationConflict("Owner control state unresolved; no configuration restart")
        owner_stop_requested = str(ctrl.get("desired_service_state") or "").upper() == "STOPPED"

        op_state, _ = tracker.resolve_service_operating_state(check_fn=check_service_fn)
        if op_state not in (
            ServiceOperatingState.RUNNING, ServiceOperatingState.STOPPED_OTHER,
            ServiceOperatingState.STOPPED_BY_OWNER, ServiceOperatingState.FAILED,
        ):
            raise ConfigurationConflict("Service state cannot be verified")
        running = op_state == ServiceOperatingState.RUNNING
        if owner_stop_requested and op_state != ServiceOperatingState.STOPPED_BY_OWNER:
            raise ConfigurationConflict("Owner Stop must not be superseded by configuration updates")
        old_hash = compute_config_version(old_cfg)
        new_hash = compute_config_version(new_cfg)
        old_fp = fingerprint(old_cfg, tracker.data_dir)
        new_fp = fingerprint(new_cfg, tracker.data_dir)
        prior_ack = tracker.get_active_running_version(require_live_pid=False) or {}
        previous_pid = prior_ack.get("pid")
        previous_starttime = prior_ack.get("process_starttime")
        pre_runtime_verified = False
        if running:
            ok, info, reason = tracker.is_active_version_confirmed(
                expected_hashes={old_hash}, expected_mode=old_mode,
                check_service_fn=check_service_fn, require_systemd_active=True,
            )
            pre_runtime_verified = (
                ok and isinstance(info, dict) and
                info.get("credential_fingerprint") == old_fp and
                type(previous_pid) is int and previous_pid > 0 and
                type(previous_starttime) is int and previous_starttime > 0
            )
            if not pre_runtime_verified:
                raise ConfigurationConflict("Running PID/hash/mode/credentials not positively verified: " + reason)
        v2 = CandidateCoordinator(
            data_dir=tracker.data_dir, env_path=env_path,
            feature_enabled=True, require_external_fence=True,
            allow_stopped_owner_save=op_state == ServiceOperatingState.STOPPED_BY_OWNER,
        )
        if not preview_legacy_tracker_migration(
            tracker.data_dir, allow_owner_stopped=op_state == ServiceOperatingState.STOPPED_BY_OWNER,
        ).allowed:
            raise ConfigurationConflict("Legacy configuration or owner STOP requires reconciliation")
        if running and systemctl_restart_fn is None and is_isolated_test_environment():
            raise ConfigurationConflict("Injected systemd restart required in isolated tests")
        last_ack: dict[str, Any] = {}
        def operation_gate() -> SafetyDecision:
            ok, reason, code, count = tracker.evaluate_trading_safety_gate_detailed(
                os.path.join(tracker.data_dir, "trade_journal.db"),
                os.path.join(tracker.data_dir, "emergency_stop.json"),
                is_service_running=True,
                is_live_mode=old_mode == "LIVE" or new_mode == "LIVE",
                target_is_live=new_mode == "LIVE",
                broker=broker, env_path=env_path,
            )
            if not ok:
                return SafetyDecision(False, code or "BROKER_GATE_BLOCKED", reason, {"count": count})
            return evaluate_active_broker_operation_gate(tracker.data_dir, require_handoff=True)
        def fence(cid: str) -> bool:
            try:
                return acquire_apply_barrier(
                    tracker.data_dir, old_cfg.dhan_client_id, cid,
                    previous_pid, previous_starttime,
                )
            except BrokerHandoffBlocked:
                return False
        def restart() -> bool:
            if systemctl_restart_fn is not None:
                return bool(systemctl_restart_fn())
            return bool(tracker._invoke_systemctl("restart", "trading_bot.service"))
        def ack_probe():
            ack = tracker.get_active_running_version(require_live_pid=False)
            props = inspect_systemd_service_properties(
                "trading_bot.service", check_fn=check_service_fn,
            )
            if isinstance(ack, dict):
                last_ack.clear()
                last_ack.update(ack)
            return ack, props
        def release(cid: str) -> bool:
            pid = last_ack.get("pid")
            stamp = last_ack.get("process_starttime")
            if type(pid) is not int or type(stamp) is not int:
                return False
            try:
                return release_apply_barrier(
                    tracker.data_dir, old_cfg.dhan_client_id, cid, pid, stamp,
                )
            except BrokerHandoffBlocked:
                return False
        try:
            result = v2.submit(
                request_id=request_id, expected_file_revision=revision(original),
                new_env=incoming.encode("utf8"), previous_mode=old_mode,
                target_mode=new_mode, service_running=running,
                confirm_live=old_mode == "LIVE" or new_mode != "LIVE" or
                form_data.get("confirm_live") in (True, "true", "yes", "1", "on"),
                operation_gate=operation_gate if running else None,
                pre_runtime_verified=pre_runtime_verified,
                entry_fence=fence if running else None,
                restart_callback=restart if running else None,
                ack_probe=ack_probe if running else None,
                expected_runtime_hash=new_hash if running else "",
                credential_fingerprint=new_fp if running else "",
                starttime_reader=proc_starttime if running else None,
                before_pid=previous_pid if running else None,
                before_starttime=previous_starttime if running else None,
                release_fence=release if running else None,
            )
        except (ChangeBlocked, BrokerHandoffBlocked, ValueError, OSError) as exc:
            raise ConfigurationConflict("Tracker V2 safely blocked save/restart: " + str(exc)) from None
        rec = ConfigChangeRecord(
            change_id=result.change_id, state=_RESTART_STATE[result.phase],
            reason=result.message, requested_at=time.time(),
            previous_mode=old_mode, requested_mode=new_mode,
            saved_mode=new_mode if result.phase != "NO_CHANGE" else old_mode,
            running_mode=new_mode if result.runtime_verified else (
                old_mode if running else "STOPPED"
            ),
            service_state=op_state.value,
            current_step="VERIFIED" if result.runtime_verified else "PERSISTED",
            progress_percent=100 if result.phase in ("VERIFIED", "NO_CHANGE", "SAVED_FOR_NEXT_START") else 90,
            operation_state=(
                OperationState.APPLIED_VERIFIED.value if result.runtime_verified
                else OperationState.SAVED_FOR_NEXT_START.value if not running
                else OperationState.FAILED.value if result.restart_issued
                else OperationState.WAITING_FOR_ORDER.value
            ),
            previous_version_hash=old_hash, proposed_version_hash=new_hash,
            effective_version_hash=new_hash if result.runtime_verified else "",
            runtime_verified=result.runtime_verified,
            restart_required=running and bool(changed),
            restart_performed=result.restart_issued,
            application_method="TRACKER_V2_SHARED_FENCE",
            changed_fields=sorted(changed),
            changes_summary=[
                f"{k}: {redact(k, vals[0])} -> {redact(k, vals[1])}"
                for k, vals in sorted(changed.items())
            ],
            blocked_reason=result.phase if result.phase in ("ACK_UNCERTAIN", "ACTION_REQUIRED") else "",
            client_request_id=request_id,
            previous_file_revision=revision(original),
            requested_file_revision=revision(incoming),
        )
        # Do not report success unless the V2 durable record says VERIFIED.
        tracker.save_change_record(rec)
        stored = tracker.get_change_record(rec.change_id)
        if stored is None or stored.state != rec.state:
            raise ConfigurationConflict("Tracker UI status record cannot be verified")
        tracker.dispatch_telegram_update(rec)
        return rec
