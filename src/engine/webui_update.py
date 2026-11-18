"""Non-trading field-scoped WebUI transaction coordinator.

Only safety-class operations enter the legacy broker-aware transition
coordinator. Ordinary changes are saved atomically and explicitly marked
pending until a future, owner-approved process start. There is no simulated
hot reload, no broker call, and no implicit service activation.
"""
from __future__ import annotations

from hashlib import sha256
import json
import os
import re
import secrets
import time
from typing import Any, Dict, Mapping, Optional

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, ENV_REGISTRY,
    classify, parse_env_strict, patch_env, redact, request_id_valid,
    revision, validate_effective_config,
)
from src.engine.config_manager import (
    ConfigApplyState, ConfigChangeRecord, ConfigChangeTracker,
    OperationState, ServiceOperatingState, compute_config_version, normalize_configuration_dict,
    write_env_atomic, write_json_atomic,
)


_BUSY = {
    ConfigApplyState.REQUEST_RECEIVED,
    ConfigApplyState.VALIDATING,
    ConfigApplyState.SAVED,
    ConfigApplyState.SAFETY_CHECK_PENDING,
    ConfigApplyState.WAITING_FOR_SAFE_APPLY,
    ConfigApplyState.APPLYING,
}


def _read_env(env_path: str) -> str:
    if os.path.islink(env_path):
        raise ConfigurationConflict("Configuration path must not be a symbolic link")
    if not os.path.exists(env_path):
        return ""
    if not os.path.isfile(env_path):
        raise ConfigurationConflict("Configuration path must be a regular file")
    with open(env_path, "r", encoding="utf-8", newline="") as fh:
        return fh.read()


def _payload_hash(form_data: Mapping[str, Any]) -> str:
    return sha256(json.dumps(dict(form_data), sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def _persist_record(tracker: ConfigChangeTracker, rec: ConfigChangeRecord) -> None:
    """The legacy tracker logs disk-write failures instead of raising them.

    The field coordinator cannot accept that silently: a saved environment
    with an unrecorded status would leave the owner unable to recover safely.
    """
    tracker.save_change_record(rec)
    stored = tracker.get_change_record(rec.change_id)
    if (stored is None or stored.state != rec.state
            or stored.operation_state != rec.operation_state
            or stored.requested_file_revision != rec.requested_file_revision):
        raise ConfigurationConflict(
            "Configuration status could not be durably verified; review transaction recovery before another update"
        )


def _request_path(tracker: ConfigChangeTracker, request_id: str) -> str:
    return os.path.join(tracker.data_dir, "config_request_ids", sha256(request_id.encode()).hexdigest() + ".json")


def _remember_request(tracker: ConfigChangeTracker, request_id: str, payload_hash: str, change_id: str) -> None:
    path = _request_path(tracker, request_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_json_atomic(path, {"request_id": request_id, "payload_sha256": payload_hash, "change_id": change_id})


def _existing_request(tracker: ConfigChangeTracker, request_id: str, payload_hash: str) -> Optional[ConfigChangeRecord]:
    path = _request_path(tracker, request_id)
    if not os.path.exists(path):
        # Legacy safety enqueue publishes its durable pending envelope before
        # its request index. If it crashed between these two atomic writes,
        # the envelope still identifies the accepted request exactly.
        if not os.path.exists(tracker.pending_op_file):
            return None
        with open(tracker.pending_op_file, "r", encoding="utf-8") as fh:
            op = json.load(fh)
        if op.get("webui_request_id") != request_id:
            return None
        old = {
            "payload_sha256": op.get("webui_payload_sha256"),
            "change_id": op.get("change_id"),
        }
        # Recover the missing request index before returning. Never replay.
        if old["payload_sha256"] != payload_hash:
            raise ConfigurationConflict("Request ID reused for a different configuration change")
        _remember_request(tracker, request_id, payload_hash, old["change_id"])
    else:
        with open(path, "r", encoding="utf-8") as fh:
            old = json.load(fh)
    if old.get("payload_sha256") != payload_hash:
        raise ConfigurationConflict("Request ID reused for a different configuration change")
    rec = tracker.get_change_record(old.get("change_id"))
    if rec is not None:
        if (rec.application_method == "SAVE_FOR_CONTROLLED_START"
                and rec.state in _BUSY):
            # Previous request may have crashed after accepting the receipt,
            # before finishing its atomic file update. Recover in place
            # instead of replaying it or returning an indefinite 15% state.
            recover_field_transactions(tracker)
            rec = tracker.get_change_record(old.get("change_id"))
        if rec is not None:
            return rec
    # An older request has already been accepted. Never replay it over newer changes.
    raise ConfigurationConflict("This configuration request has already been handled; refresh the page")


def _safe_state(tracker: ConfigChangeTracker) -> ServiceOperatingState:
    op_state, _ = tracker.resolve_service_operating_state()
    if op_state in (ServiceOperatingState.UNKNOWN, ServiceOperatingState.STARTING, ServiceOperatingState.STOPPING):
        raise ConfigurationConflict("Bot service state is changing or cannot be verified; retry when stable")
    return op_state



def recover_legacy_notification_deferred(tracker: ConfigChangeTracker, env_path: str) -> bool:
    """Reclassify the old 75% notification-only record without broker calls.

    Refuse recovery unless its complete staged configuration is semantically
    identical to the saved file except for DHAN_RESPONSE_GROUP_NAME. In
    particular, do not edit YESBANK, reset any transition gate or clear a
    genuine mode-transition obligation.
    """
    rec = tracker.get_change_record()
    if not rec or rec.state != ConfigApplyState.WAITING_FOR_SAFE_APPLY:
        return False
    meta = tracker.get_deferred_metadata() or {}
    if meta.get("change_id") not in (None, "", rec.change_id):
        return False
    if not os.path.isfile(tracker.deferred_env_file):
        return False
    old_raw = _read_env(env_path)
    with open(tracker.deferred_env_file, "r", encoding="utf-8", newline="") as fh:
        deferred = fh.read()
    try:
        old, _ = parse_env_strict(old_raw)
        staged, _ = parse_env_strict(deferred)
        # The legacy form regenerated a complete .env, often adding default
        # values or reformatting comments. Reconcile only when *effective*
        # non-secret values differ in the notification destination alone and
        # raw secret values are identical. Apply against original raw text so
        # legacy formatting is not imported into the saved .env.
        old_norm = normalize_configuration_dict(old)
        staged_norm = normalize_configuration_dict(staged)
        different = {k for k in set(old_norm) | set(staged_norm)
                     if old_norm.get(k) != staged_norm.get(k)}
        if different not in ({"dhan_response_group_name"}, set()):
            return False
        if not different:
            # If the coordinator crashed after replacing .env but before
            # storing its completion state, current/staged values match.
            # Accept only an operation whose original intent was explicitly
            # notification-only; never reinterpret a real mode transition.
            if not rec.changes_summary or not all(
                "dhan_response_group_name" in str(item).lower()
                for item in rec.changes_summary
            ):
                return False
        protected = {k for k in set(old) | set(staged)
                     if any(s in k.lower() for s in ("pin", "token", "secret", "hash", "session", "password", "client_id"))}
        if any(old.get(k, "") != staged.get(k, "") for k in protected):
            return False
        if old.get("TRADING_MODE", "TEST") != staged.get("TRADING_MODE", "TEST"):
            return False
        new_raw, changes = patch_env(old_raw, {
            "dhan_response_group_name": staged.get("DHAN_RESPONSE_GROUP_NAME", "")
        })
        if set(changes) != ({"DHAN_RESPONSE_GROUP_NAME"} if different else set()):
            return False
        validate_effective_config(new_raw)
    except (ConfigurationConflict, ConfigurationRejected, ValueError, OSError):
        return False
    backup = os.path.join(tracker.data_dir, "config_field_backups", rec.change_id + ".env")
    try:
        os.makedirs(os.path.dirname(backup), exist_ok=True)
        write_env_atomic(backup, old_raw)
        if revision(_read_env(env_path)) != revision(old_raw):
            return False
        if "DHAN_RESPONSE_GROUP_NAME" in changes:
            from src.notifier.response_activation import stage_destination_change
            stage_destination_change(
                tracker.data_dir, old.get("DHAN_RESPONSE_GROUP_NAME", ""),
                staged.get("DHAN_RESPONSE_GROUP_NAME", ""),
            )
        write_env_atomic(env_path, new_raw)
        if revision(_read_env(env_path)) != revision(new_raw):
            raise OSError("Legacy notification recovery write could not be verified")
        rec.state = ConfigApplyState.SAVED_FOR_NEXT_START
        rec.operation_state = OperationState.SAVED_FOR_NEXT_START.value
        rec.saved_at = time.time()
        rec.progress_percent = 100
        rec.current_step = "PERSISTED"
        rec.runtime_verified = False
        rec.restart_required = False
        rec.restart_performed = False
        rec.auto_resume = False
        rec.blocked_reason = ""
        rec.blocking_order_count = 0
        rec.application_method = "LEGACY_NOTIFICATION_RECOVERY_NEXT_START"
        rec.changed_fields = ["DHAN_RESPONSE_GROUP_NAME"]
        rec.reason = (
            "Legacy notification-only update recovered and saved for next controlled start; "
            "running destination is not yet verified. No broker or order records changed."
        )
        _persist_record(tracker, rec)
        # Do not clear an unrelated LIVE entry gate. Only remove the old
        # notification stage, whose content is now reflected in saved .env.
        tracker.clear_deferred_artifacts(clear_gate=False)
        # The old global coordinator could have created a transition gate
        # even for this notification-only update. Release it ONLY if its
        # durable owner is this exact change and there is no real pending
        # LIVE transition or Owner Stop. Never release a foreign/unknown gate.
        gate = tracker.inspect_transition_gate_status(read_only=True)
        if (gate.get("active") is True and gate.get("ok") is True
                and gate.get("change_id") == rec.change_id):
            still_required, _, _ = tracker._has_pending_transition_or_stop_in_state()
            if not still_required:
                release = tracker.set_transition_gate(
                    active=False, change_id=rec.change_id,
                    target_mode=rec.requested_mode,
                    reason="Notification-only legacy update recovered; no trading transition pending",
                )
                if not release:
                    rec.blocked_reason = "TRANSITION_GATE_RELEASE_UNVERIFIED"
                    rec.reason += " Existing entry gate was retained; operator review required."
                    _persist_record(tracker, rec)
        return True
    except Exception:
        # Never claim success without preserving existing active configuration.
        try:
            if revision(_read_env(env_path)) == revision(new_raw):
                write_env_atomic(env_path, old_raw)
        except Exception:
            pass
        return False


def submit_webui_patch(
    tracker: ConfigChangeTracker,
    form_data: Mapping[str, Any],
    env_path: str = ".env",
    *,
    expected_revision: Optional[str] = None,
    request_id: Optional[str] = None,
    spawn_worker: bool = True,
    check_service_fn: Any = None,
    systemctl_restart_fn: Any = None,
    broker: Any = None,
) -> ConfigChangeRecord:
    """Accept exactly one version-checked WebUI patch under the existing lock.

    This is a *save* endpoint for ordinary settings; runtime verification is
    deliberately false while a process is still using the prior configuration.
    Safety-controlled changes use the existing broker-aware coordinator.
    """
    env_path = os.path.abspath(env_path)
    # PR-V2-ACTIVATE: a mixed legacy WebUI / V2 trading worker must never
    # save configuration through the wrong coordinator or bypass its fence.
    from src.engine.tracker_v2_dual_service import (
        assert_consistent_save_route, DualServiceMismatch,
    )
    try:
        assert_consistent_save_route(
            tracker.data_dir,
            web_v2=os.environ.get("UDITBOT_TRACKER_V2_ENABLE") == "1",
        )
    except DualServiceMismatch as exc:
        raise ConfigurationConflict("V2 routing safety gate: " + str(exc)) from None
    if os.environ.get("UDITBOT_TRACKER_V2_ENABLE") == "1":
        from src.engine.tracker_v2_integration import submit_v2_webui_patch
        return submit_v2_webui_patch(
            tracker, form_data, env_path, expected_revision=expected_revision,
            request_id=request_id, check_service_fn=check_service_fn,
            systemctl_restart_fn=systemctl_restart_fn, broker=broker,
        )
    request_id = str(request_id or secrets.token_hex(16))
    if not request_id_valid(request_id):
        raise ConfigurationRejected("Invalid idempotency request ID")
    # Require a concrete browser-observed revision for every write, including
    # idempotent submissions. Without it the optimistic CAS can be bypassed
    # by omitting the field from JSON requests.
    if not isinstance(expected_revision, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_revision):
        raise ConfigurationConflict("Missing or invalid configuration revision; refresh Setup before saving")
    form_data = dict(form_data)
    payload_hash = _payload_hash(form_data)
    with tracker.operation_lock(operation_type="FIELD_SCOPED_WEBUI", operation_id=request_id):
        prior_result = _existing_request(tracker, request_id, payload_hash)
        if prior_result is not None:
            if spawn_worker and os.path.exists(tracker.pending_op_file):
                # An idempotent retry is also a coordinator wake-up after a
                # previously accepted request lost its worker to a crash.
                try:
                    tracker.start_coordinator_daemon(
                        env_path=env_path, stop_when_idle=True,
                    )
                except Exception:
                    pass
            return prior_result
        # Reconcile an interrupted save before accepting any NEW request.
        # Unknown disk state is never interpreted as a clean prior version,
        # even when the browser supplied the latest file hash.
        recovery = recover_field_transactions(tracker)
        unresolved = [cid for cid, outcome in recovery.items()
                      if outcome not in ("RECOVERED_SAVED", "PREVIOUS_RETAINED")]
        if unresolved:
            raise ConfigurationConflict(
                "An earlier configuration save is unresolved. Open Configuration Change "
                "Tracker and resolve the file conflict before changing settings."
            )
        # Do not supersede a safety-critical pending transition in the legacy
        # single-record coordinator. Independent, non-restart changes would be
        # possible in a multi-record tracker, but not by overwriting its gate.
        tracker.reconcile_stale_control_records(env_path, check_service_fn=check_service_fn)
        prior = tracker.get_change_record()
        if prior is not None and prior.state == ConfigApplyState.WAITING_FOR_SAFE_APPLY:
            if recover_legacy_notification_deferred(tracker, env_path):
                prior = tracker.get_change_record()
        if prior is not None and prior.state in _BUSY:
            raise ConfigurationConflict(
                "Another configuration change is pending. Review or cancel it before resubmitting"
            )
        if os.path.exists(tracker.pending_op_file):
            raise ConfigurationConflict("Existing durable configuration operation needs coordinator recovery")
        control_rec = tracker.get_service_control_record()
        if isinstance(control_rec, dict) and control_rec.get("deferred_stop_pending"):
            raise ConfigurationConflict("Owner Stop is pending; configuration updates must not supersede it")
        raw = _read_env(env_path)
        new_content, changed = patch_env(raw, form_data, expected_revision=expected_revision)
        validate_effective_config(new_content)
        policy = classify(changed)
        service_state = _safe_state(tracker)
        old_env, _ = parse_env_strict(raw)
        new_env, _ = parse_env_strict(new_content)
        old_mode = old_env.get("TRADING_MODE", "TEST").upper()
        new_mode = new_env.get("TRADING_MODE", old_mode).upper()
        is_running = service_state == ServiceOperatingState.RUNNING
        change_id = tracker.generate_change_id()
        old_ver = compute_config_version(old_env)
        new_ver = compute_config_version(new_env)
        changes_summary = [f"{k}: {redact(k, v[0])} -> {redact(k, v[1])}" for k, v in sorted(changed.items())]

        rec = ConfigChangeRecord(
            change_id=change_id, state=ConfigApplyState.REQUEST_RECEIVED,
            changes_summary=changes_summary,
            previous_version_hash=old_ver,
            proposed_version_hash=new_ver,
            previous_mode=old_mode, requested_mode=new_mode,
            saved_mode=old_mode,
            running_mode=old_mode if is_running else "STOPPED",
            service_state=service_state.value,
            service_status="LIVE" if is_running and old_mode == "LIVE" else ("TEST" if is_running else "STOPPED"),
            current_step="RECEIVED", progress_percent=15,
            operation_state=OperationState.REQUESTED.value,
            restart_required=policy == "SAFETY_CONTROLLED" and is_running,
            restart_performed=False, runtime_verified=False,
        )
        # Additive metadata; old API fields retain their meaning.
        rec.changed_fields = sorted(changed)
        rec.application_method = "LEGACY_SAFETY_COORDINATOR" if policy == "SAFETY_CONTROLLED" and is_running else "SAVE_FOR_CONTROLLED_START"
        rec.broker_checks_required = policy == "SAFETY_CONTROLLED" and is_running
        rec.client_request_id = request_id
        rec.previous_file_revision = revision(raw)
        rec.requested_file_revision = revision(new_content)
        rec.requested_at = time.time()

        if not changed:
            rec.state = ConfigApplyState.SAVED_FOR_NEXT_START
            rec.operation_state = OperationState.SAVED_FOR_NEXT_START.value
            rec.progress_percent = 100
            rec.current_step = "PERSISTED"
            rec.restart_required = False
            rec.reason = "No configuration values changed. No service was restarted or broker called."
            _persist_record(tracker, rec)
            _remember_request(tracker, request_id, payload_hash, change_id)
            return rec

        # The owner must explicitly confirm a new LIVE mode, in addition to the
        # preexisting broker and emergency-stop guards in the transition engine.
        if old_mode != "LIVE" and new_mode == "LIVE" and form_data.get("confirm_live") not in (True, "true", "1", "yes", "on"):
            raise ConfigurationRejected("Enabling LIVE requires explicit owner confirmation")

        # Record an old destination candidate for a future Telegram session
        # restart BEFORE the .env is changed, without resolving or sending to
        # Telegram from the WebUI process. Stale metadata is ignored unless the
        # running requested group matches the pending target.
        if "DHAN_RESPONSE_GROUP_NAME" in changed:
            from src.notifier.response_activation import stage_destination_change
            try:
                stage_destination_change(
                    tracker.data_dir,
                    old_env.get("DHAN_RESPONSE_GROUP_NAME", ""),
                    new_env.get("DHAN_RESPONSE_GROUP_NAME", ""),
                )
            except OSError as exc:
                raise ConfigurationConflict(
                    "Could not stage safe Telegram destination fallback; configuration unchanged"
                ) from None

        # A running safety-controlled change still requires authoritative broker
        # reconciliation and the existing restart identity verification.
        if policy == "SAFETY_CONTROLLED" and is_running:
            rec = tracker.enqueue_durable_configuration_update(
                form_params=new_env, env_content=new_content, env_path=env_path,
                check_service_fn=check_service_fn,
                systemctl_restart_fn=systemctl_restart_fn,
                broker=broker, spawn_worker=False,
                webui_request={
                    "request_id": request_id,
                    "payload_sha256": payload_hash,
                    "index_path": _request_path(tracker, request_id),
                },
            )
            rec.changed_fields = sorted(changed)
            rec.application_method = "LEGACY_SAFETY_COORDINATOR"
            rec.broker_checks_required = True
            rec.client_request_id = request_id
            rec.previous_file_revision = revision(raw)
            rec.requested_file_revision = revision(new_content)
            rec.changes_summary = changes_summary
            _persist_record(tracker, rec)
            _remember_request(tracker, request_id, payload_hash, rec.change_id)
        else:
            # Commit record before the file update so a crash never presents an
            # untracked configuration change as a successful operation.
            txn_dir = os.path.join(tracker.data_dir, "config_field_transactions")
            backup_dir = os.path.join(tracker.data_dir, "config_field_backups")
            os.makedirs(txn_dir, exist_ok=True)
            os.makedirs(backup_dir, exist_ok=True)
            backup = os.path.join(backup_dir, change_id + ".env")
            txn_path = os.path.join(txn_dir, change_id + ".json")
            write_env_atomic(backup, raw)
            write_json_atomic(txn_path, {
                "change_id": change_id,
                "previous_sha256": revision(raw),
                "target_sha256": revision(new_content),
                "status": "PREPARED", "backup_path": backup,
                "env_path": env_path, "policy": policy,
            })
            _persist_record(tracker, rec)
            _remember_request(tracker, request_id, payload_hash, change_id)
            # This compare-and-swap may race with non-cooperating tools
            # that do not take the WebUI operation lock. A failed CAS is NOT
            # permission to restore the earlier snapshot over their edits.
            write_started = False
            try:
                if revision(_read_env(env_path)) != revision(raw):
                    raise ConfigurationConflict("Configuration changed before atomic replacement")
                write_started = True
                write_env_atomic(env_path, new_content)
                if revision(_read_env(env_path)) != revision(new_content):
                    raise OSError("Persisted configuration failed verification")
            except Exception:
                try:
                    current_revision = revision(_read_env(env_path))
                except Exception:
                    current_revision = None
                if write_started and current_revision == revision(new_content):
                    # Only restore an exact file created by THIS write. Never
                    # overwrite unknown third-party changes (including a
                    # pre-write revision mismatch).
                    try:
                        write_env_atomic(env_path, raw)
                        current_revision = revision(_read_env(env_path))
                    except Exception:
                        current_revision = None
                if current_revision == revision(raw):
                    rec.reason = "Configuration write failed; previous file retained"
                elif current_revision is None:
                    rec.reason = "Configuration file cannot be verified after failed write; manual review required"
                else:
                    rec.reason = (
                        "Configuration changed outside this WebUI transaction; "
                        "external file preserved, manual review required"
                    )
                rec.state = ConfigApplyState.APPLY_FAILED
                rec.operation_state = OperationState.FAILED.value
                rec.runtime_verified = False
                _persist_record(tracker, rec)
                raise ConfigurationConflict(rec.reason) from None
            rec.state = ConfigApplyState.SAVED_FOR_NEXT_START
            rec.operation_state = OperationState.SAVED_FOR_NEXT_START.value
            rec.saved_at = time.time()
            rec.saved_mode = new_mode
            rec.progress_percent = 100
            rec.current_step = "PERSISTED"
            rec.restart_required = False
            rec.runtime_verified = False
            rec.reason = (
                "Saved for the next controlled trading-service start. Existing running settings remain active; "
                "no trading restart or broker safety check was necessary."
                if is_running else
                "Saved for next start; the trading service remains stopped. No broker calls were made."
            )
            _persist_record(tracker, rec)
            try:
                write_json_atomic(txn_path, {
                    "change_id": change_id,
                    "previous_sha256": revision(raw),
                    "target_sha256": revision(new_content),
                    "status": "SAVED_FOR_NEXT_START", "backup_path": backup,
                    "env_path": env_path, "policy": policy,
                })
            except OSError:
                # Both the exact .env bytes and durable change record were
                # verified already. The PREPARED journal remains recoverable;
                # do not falsely turn a completed safe save into a new update.
                pass
    if policy == "SAFETY_CONTROLLED" and is_running and spawn_worker:
        try:
            # Only broker-sensitive settings need a broker adapter. Instantiate
            # it outside the submission lock; no order submission occurs here.
            ro_broker = broker if broker is not None else tracker.get_readonly_broker(env_path=env_path)
            tracker.start_coordinator_daemon(env_path=env_path, broker=ro_broker, stop_when_idle=True)
        except Exception:
            # The queued operation is durable. Do not lie about application;
            # startup recovery can resume it without duplicating a restart.
            pass
    return rec


def recover_field_transactions(tracker: ConfigChangeTracker) -> Dict[str, str]:
    """Recover interrupted save-only transactions without starting any service.

    A mismatching file is never overwritten; it requires operator review.
    """
    txn_dir = os.path.join(tracker.data_dir, "config_field_transactions")
    outcome = {}
    if not os.path.isdir(txn_dir):
        return outcome
    with tracker.operation_lock(operation_type="RECOVER_FIELD_TRANSACTIONS"):
        for name in sorted(os.listdir(txn_dir)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(txn_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                if meta.get("status") != "PREPARED":
                    continue
                rec = tracker.get_change_record(meta["change_id"])
                if rec is None:
                    # Crash may have occurred before the status file survived.
                    # Reconstruct a safe state from durable hashes only. Never
                    # infer runtime activation or permission to enter LIVE.
                    rec = ConfigChangeRecord(
                        change_id=meta["change_id"], state=ConfigApplyState.APPLY_FAILED,
                        runtime_verified=False, restart_performed=False,
                        application_method="SAVE_FOR_CONTROLLED_START",
                    )
                current = revision(_read_env(meta["env_path"]))
                if current == meta["target_sha256"]:
                    if rec is not None:
                        rec.state = ConfigApplyState.SAVED_FOR_NEXT_START
                        rec.operation_state = OperationState.SAVED_FOR_NEXT_START.value
                        rec.current_step = "PERSISTED"
                        rec.progress_percent = 100
                        rec.restart_required = False
                        rec.runtime_verified = False
                        rec.reason = "Recovered saved configuration after interrupted WebUI transaction; awaiting controlled startup"
                        _persist_record(tracker, rec)
                    meta["status"] = "SAVED_FOR_NEXT_START"
                    outcome[meta["change_id"]] = "RECOVERED_SAVED"
                elif current == meta["previous_sha256"]:
                    if rec is not None:
                        rec.state = ConfigApplyState.APPLY_FAILED
                        rec.operation_state = OperationState.FAILED.value
                        rec.runtime_verified = False
                        rec.reason = "Interrupted before file replacement; previous configuration retained"
                        _persist_record(tracker, rec)
                    meta["status"] = "FAILED_PREVIOUS_RETAINED"
                    outcome[meta["change_id"]] = "PREVIOUS_RETAINED"
                else:
                    # Fail closed: neither snapshot is authoritative.
                    if rec is not None:
                        rec.state = ConfigApplyState.APPLY_FAILED
                        rec.operation_state = OperationState.FAILED.value
                        rec.reason = "Configuration conflict after interrupted save; owner review required"
                        rec.runtime_verified = False
                        _persist_record(tracker, rec)
                    meta["status"] = "UNRESOLVED_CONFLICT"
                    outcome[meta["change_id"]] = "UNRESOLVED_CONFLICT"
                write_json_atomic(path, meta)
            except (OSError, ValueError, KeyError, TypeError):
                outcome[name] = "UNREADABLE_TRANSACTION"
    return outcome
