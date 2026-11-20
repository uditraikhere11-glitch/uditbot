"""Authenticated Control Center status and independent Equity/F&O AMO settings.

F&O AMO routes only change configuration or cancel LOCAL staged signals. They
never submit a naked option order; dispatch uses protected Super Orders at OPEN.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config import AppConfig, parse_env_dict
from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, patch_env, revision, validate_effective_config
from src.engine.runtime_evidence import read_private_json
from src.engine.simple_save_apply import apply_settings
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeTracker,
    compute_config_version,
    parse_env_content_string,
)

_PENDING_STATES = {
    ConfigApplyState.REQUEST_RECEIVED,
    ConfigApplyState.VALIDATING,
    ConfigApplyState.SAVED,
    ConfigApplyState.SAFETY_CHECK_PENDING,
    ConfigApplyState.WAITING_FOR_SAFE_APPLY,
    ConfigApplyState.APPLYING,
}
_ALLOWED_IDLE = {"RUNNING", "STOPPED_BY_OWNER", "STOPPED_OTHER", "FAILED"}
_CONFIG_KEY = "EQUITY_AMO_ENABLED"
_KEY_LINE = re.compile(r"^[ \t]*EQUITY_AMO_ENABLED[ \t]*=", re.IGNORECASE)
_ALLOWED_AMO_KEYS = {"EQUITY_AMO_ENABLED", "FNO_AMO_ENABLED"}


def _json(payload: Dict[str, Any], code: int = 200) -> JSONResponse:
    return JSONResponse(status_code=code, content=payload, headers={"Cache-Control": "no-store"})


def _bool(raw: Any) -> bool:
    return isinstance(raw, str) and raw.strip().lower() in {"1", "true", "yes", "on"}


def _config_and_paths(env_path: str = ".env") -> Tuple[AppConfig, str, str]:
    absolute_env = os.path.abspath(env_path)
    current = parse_env_dict(absolute_env) if os.path.isfile(absolute_env) else {}
    cfg = AppConfig.from_env(env=current, env_file="") if current else AppConfig.from_env()
    return cfg, absolute_env, os.path.abspath(getattr(cfg, "data_dir", "data"))


def _pending(tracker: ConfigChangeTracker) -> bool:
    """Under the global operation lock, refuse overlap with any coordinator work."""
    if tracker.has_unresolved_pending_operation() or os.path.exists(tracker.deferred_env_file):
        return True
    rec = tracker.get_change_record()
    return bool(rec and rec.state in _PENDING_STATES)


def _owner_stop_pending(tracker: ConfigChangeTracker) -> bool:
    rec = tracker.get_service_control_record()
    return bool(isinstance(rec, dict) and rec.get("deferred_stop_pending"))


def _single_key_change(content: str, enabled: bool, key: str = _CONFIG_KEY) -> str:
    """Preserve bytes outside one existing .env assignment (newline-style inclusive).

    Refuse duplicate/case-insensitive keys, CR-only line separators and corrupted
    values, instead of producing an ambiguous config. Never serialize secrets.
    """
    if key not in _ALLOWED_AMO_KEYS:
        raise ValueError("Unsupported AMO configuration key")
    key_line = re.compile(r"^[ \t]*" + re.escape(key) + r"[ \t]*=", re.IGNORECASE)
    if "\x00" in content or ("\r" in content and "\r\n" not in content):
        raise ValueError("Configuration file has unsupported line encoding")
    value = "true" if enabled else "false"
    lines = content.splitlines(keepends=True)
    idxs = [i for i, s in enumerate(lines) if key_line.match(s)]
    if len(idxs) > 1:
        raise ValueError(f"Duplicate {key} keys must be repaired in Setup")
    if idxs:
        i = idxs[0]
        original = lines[i]
        nl = "\r\n" if original.endswith("\r\n") else ("\n" if original.endswith("\n") else "")
        # Preserve existing inline comments and whitespace where safe.
        lhs, rhs = original.rstrip("\r\n").split("=", 1)
        comment = ""
        if "#" in rhs:
            suffix = rhs[rhs.index("#") :].strip()
            if suffix:
                comment = " " + suffix
        lines[i] = lhs + "=" + value + comment + nl
    else:
        nl = "\r\n" if "\r\n" in content else "\n"
        if lines and not lines[-1].endswith(("\n", "\r")):
            lines[-1] += nl
        lines.append(f"{key}={value}{nl}")
    return "".join(lines)


def _revision_for_control(env_path: str) -> str:
    """Return a revision fingerprint, never raw configuration or secrets."""
    try:
        if os.path.islink(env_path):
            return "UNAVAILABLE"
        with open(env_path, "r", encoding="utf-8", newline="") as fh:
            return revision(fh.read())
    except (OSError, UnicodeError):
        return "UNAVAILABLE"


def build_control_status(env_path: str = ".env") -> Dict[str, Any]:
    """A read-only, field-allowlisted status; never expose raw systemd/config."""
    cfg, absolute_env, data_dir = _config_and_paths(env_path)
    tracker = ConfigChangeTracker(data_dir=data_dir)
    raw = tracker.control_service(action="status", env_path=absolute_env)
    state = str(raw.get("service_state") or "UNKNOWN")
    verified = raw.get("runtime_verified") is True and state == "RUNNING"
    saved = bool(cfg.equity_amo_enabled)
    fno_saved = bool(cfg.fno_amo_enabled)
    newest = tracker.get_change_record()
    v2_blocked = False
    v2_recheck = False
    try:
        tx = read_private_json(os.path.join(data_dir, "config_restart_v2", "current_transaction.json"), missing_ok=True)
        fence = read_private_json(os.path.join(data_dir, "config_restart_v2", "apply_barrier.json"), missing_ok=True)
        if fence is not None and fence.get("active") is not False:
            v2_blocked = True
        if tx is not None and tx.get("phase") not in ("VERIFIED", "SAVED_FOR_NEXT_START", "NO_CHANGE"):
            v2_blocked = True
            v2_recheck = bool(
                tx.get("phase") in ("RESTART_ISSUED", "VERIFYING", "ACK_UNCERTAIN")
                and fence is not None and fence.get("change_id") == tx.get("change_id")
                and isinstance(fence.get("active"), bool)
                and newest is not None and newest.change_id == tx.get("change_id")
            )
        if (tx is not None and tx.get("phase") == "VERIFIED"
                and newest is not None and newest.change_id == tx.get("change_id")
                and newest.runtime_verified is not True):
            v2_blocked = True
            v2_recheck = bool(fence is not None and fence.get("change_id") == tx.get("change_id")
                              and fence.get("active") is False)
    except (ValueError, OSError):
        v2_blocked = True
    verified = verified and not v2_blocked
    pending = _pending(tracker)
    stopping = _owner_stop_pending(tracker)
    gate = tracker.inspect_transition_gate_status(read_only=True)
    # No concurrent new request when a transition/gate cannot be verified.
    can_edit = (
        os.path.isfile(absolute_env)
        and state in _ALLOWED_IDLE
        and not pending
        and not v2_blocked
        and not stopping
        and gate.get("ok") is True
        and gate.get("active") is False
        and (state != "RUNNING" or verified)
    )
    active = saved if verified else None
    msg = (
        "Configuration change in progress; request disabled"
        if pending else "Owner Stop pending; AMO change disabled"
        if stopping else "Runtime active setting verified" if verified else "Saved setting only — active bot setting unverified"
    )
    return {
        "service_state": state,
        "config_file_revision": _revision_for_control(absolute_env),
        "service_detail": "Runtime inspected through the existing systemd service coordinator",
        "saved_mode": str(raw.get("saved_mode") or "UNKNOWN"),
        "running_mode": str(raw.get("running_mode") or "UNKNOWN"),
        "runtime_verified": verified,
        "warning": "Restart verification pending; inspect the saved transaction and broker fence" if v2_blocked else "Control requests blocked while configuration/safety gate is unresolved" if pending or stopping else "",
        "tracker_v2_apply_blocked": v2_blocked,
        "tracker_v2_late_ack_pending": v2_recheck,
        "equity_amo_saved": saved,
        "equity_amo_active": active,
        "equity_amo_can_edit": can_edit,
        "equity_amo_message": msg,
        "fno_amo_supported": True,
        "fno_amo_route": "DHAN_REGULAR_ORDERS_FNO_AMO_ENTRY_ONLY",
        "fno_amo_saved": fno_saved,
        "fno_amo_active": fno_saved if verified else None,
        "fno_amo_can_edit": can_edit,
        "fno_amo_message": (
            msg + " · F&O sends broker /orders AMO after hours; no automatic SL/target"
        ),
        "latest_change_state": newest.state.value if newest else None,
        "latest_change_id": newest.change_id if newest else None,
        "transition_gate_active": gate.get("active", True),
    }


def build_control_health(env_path: str = ".env") -> Dict[str, Any]:
    """Read-only local checks, never claim network or watcher health without proof."""
    cfg, absolute_env, data_dir = _config_and_paths(env_path)
    tracker = ConfigChangeTracker(data_dir=data_dir)
    checks = []

    def check(name: str, status: str, detail: str) -> None:
        checks.append({"name": name, "status": status, "detail": detail})

    status = tracker.control_service(action="status", env_path=absolute_env)
    state = status.get("service_state", "UNKNOWN")
    check("Trading service", "PASS" if state == "RUNNING" else ("WARN" if state in ("STOPPED_BY_OWNER", "STOPPED_OTHER") else "UNKNOWN"), "systemd state: " + str(state))
    verified = status.get("runtime_verified") is True
    check("Active mode/version", "PASS" if verified else "UNKNOWN", "Verified" if verified else "Running process version not verified")
    # Positive service/process verification alone cannot override a saved
    # owner STOP. This diagnostic is read-only and never edits control files.
    from src.engine.tracker_v2_health import owner_intent_health
    intent = owner_intent_health(str(state), status.get("owner_intent"))
    check("Owner service intent", intent.status, intent.message)
    stop_file = os.path.join(data_dir, "emergency_stop.json")
    try:
        with open(stop_file, "r", encoding="utf-8") as fh:
            stopped_data = json.load(fh)
        if not isinstance(stopped_data, dict):
            raise ValueError("Invalid state structure")
        if stopped_data.get("is_stopped"):
            check("Emergency STOP", "WARN", "Emergency STOP engaged — new entries blocked")
        else:
            check("Emergency STOP", "PASS", "Durable STOP flag clear (broker binding not verified by this check)")
        bound = str(stopped_data.get("account_id") or "")
        if bound and str(cfg.dhan_client_id) and bound != str(cfg.dhan_client_id):
            check("Broker account binding", "FAIL", "Recorded account differs from configured account")
        else:
            check("Broker account binding", "UNKNOWN", "Read-only Dhan profile confirmation not performed")
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        check("Emergency STOP", "UNKNOWN", "Durable STOP state missing/unreadable")
        check("Broker account binding", "UNKNOWN", "Account binding could not be verified")

    token_path = os.path.expanduser(str(cfg.dhan_token_file or "~/.dhan_token"))
    try:
        token_stat = os.stat(token_path)
        if not stat.S_ISREG(token_stat.st_mode) or token_stat.st_size <= 0:
            check("Dhan token file", "WARN", "Token file missing or empty")
        elif not os.access(token_path, os.R_OK):
            check("Dhan token file", "WARN", "Token file is not readable by WebUI process")
        elif token_stat.st_mode & 0o077:
            check("Dhan token file", "WARN", "Token-file permissions are broader than 0600")
        else:
            check("Dhan token file", "PASS", "Token file present and private; validity unverified")
    except OSError:
        check("Dhan token file", "UNKNOWN", "Token file cannot be inspected")
    check("Dhan broker authentication", "UNKNOWN", "Read-only authenticated profile call not performed")
    check("Telegram ingestion", "UNKNOWN", "Configured channels: " + str(len(cfg.monitored_channel_ids)) + "; listener heartbeat not independently verified")
    check("Yahoo quote freshness", "UNKNOWN", "No live market quote requested by read-only health check")
    gate = tracker.inspect_transition_gate_status(read_only=True)
    check("Transition safety gate", "WARN" if gate.get("active", True) else "PASS", "Active or unverified" if gate.get("active", True) else "Not active")
    check("Configuration coordinator", "WARN" if _pending(tracker) else "PASS", "Change pending" if _pending(tracker) else "No pending update detected")
    check("Order protection watcher", "UNKNOWN", "Watcher heartbeat and broker protection not independently verified")
    summary = "Read-only health checks completed: " + ", ".join(f"{x}: {sum(c['status']==x for c in checks)}" for x in ("PASS", "WARN", "FAIL", "UNKNOWN"))
    return {"summary": summary, "checks": checks}


def build_amo_orders(data_dir: str = "data") -> Dict[str, Any]:
    """Journal-only, read-only AMO observation. Never equate ACKED with filled."""
    db_path = os.path.abspath(os.path.join(data_dir, "trade_journal.db"))
    if not os.path.isfile(db_path):
        return {"summary": "Journal unavailable — broker orders not verified", "orders": [], "verified": False}
    uri = "file:" + quote(db_path, safe="/") + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=0.6)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            cur = conn.execute("""
              SELECT i.symbol, i.state, o.status AS order_status,
                     o.filled_quantity AS filled_qty, o.quantity AS order_qty
              FROM order_intents i
              LEFT JOIN orders o ON o.correlation_id=i.correlation_id
              WHERE i.is_amo=1
              ORDER BY i.created_at DESC LIMIT 30
            """)
            rows = cur.fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return {"summary": "AMO journal could not be read safely", "orders": [], "verified": False}

    orders = []
    for r in rows:
        state = str(r["order_status"] or r["state"] or "UNKNOWN").upper()
        # Use only journal evidence; no inferred broker fill status.
        qty = int(r["filled_qty"] or 0)
        if qty > 0 or state in ("TRADED", "FILLED"):
            shown = "FILL REPORTED — broker unverified"
        elif state in ("REJECTED", "CANCELLED", "EXPIRED", "FAILED"):
            shown = state + " (journal)"
        elif state in ("PENDING", "TRANSIT", "ACKED", "SUBMITTING", "INTENT_CREATED"):
            shown = "QUEUED/PENDING (journal)"
        else:
            shown = state + " (journal unverified)"
        orders.append({
            "symbol": str(r["symbol"] or "UNKNOWN")[:90],
            "status": shown,
            "protection": "Not independently verified",
        })
    return {
        "summary": f"{len(orders)} recent AMO journal records · broker state not verified",
        "orders": orders,
        "verified": False,
    }


def register_control_center_routes(app: FastAPI, csrf_token: str) -> None:
    """Register additive endpoints; existing Setup form and service API stay intact."""

    def resume_deferred_owner_stop_on_webui_startup() -> None:
        """After WebUI restart, resume *only* an outstanding owner STOP.

        Do not create a settings coordinator, replay historical config, or
        start trading. An owner who requested STOP remains authoritative.
        """
        if not os.path.isfile(".env") or os.path.islink(".env"):
            return
        _, env_path, data_dir = _config_and_paths()
        tracker = ConfigChangeTracker(data_dir=data_dir)
        control = tracker.get_service_control_record()
        if (isinstance(control, dict) and not control.get("_corrupted")
                and control.get("deferred_stop_pending") is True
                and str(control.get("desired_service_state") or "").upper() == "STOPPED"):
            tracker.start_coordinator_daemon(
                env_path=env_path, stop_when_idle=True,
            )

    # FastAPI versions used on Python 3.14 no longer expose the app-level
    # add_event_handler method. Register the same idempotent STOP-only hook
    # through the router when necessary; never start settings replay here.
    add_handler = getattr(app, "add_event_handler", None)
    if callable(add_handler):
        add_handler("startup", resume_deferred_owner_stop_on_webui_startup)
    else:
        startup_hooks = getattr(getattr(app, "router", None), "on_startup", None)
        if not isinstance(startup_hooks, list):
            raise RuntimeError("WebUI cannot safely register Owner STOP recovery")
        startup_hooks.append(resume_deferred_owner_stop_on_webui_startup)

    @app.get("/control/status")
    def get_control_status():
        try:
            return _json(build_control_status())
        except Exception:
            return _json({"service_state": "UNKNOWN", "runtime_verified": False,
                          "equity_amo_can_edit": False, "equity_amo_active": None,
                           "fno_amo_can_edit": False, "fno_amo_active": None,
                          "warning": "Unable to verify service configuration"}, 503)

    @app.get("/control/health")
    def get_control_health():
        try:
            return _json(build_control_health())
        except Exception:
            return _json({"summary": "Health checks are not available", "checks": []}, 503)

    @app.get("/control/amo/orders")
    def get_control_amo_orders():
        try:
            cfg, _, data_dir = _config_and_paths()
            response = build_amo_orders(data_dir)
            try:
                from src.engine.fno_amo_queue import FnoAmoQueue
                queue = FnoAmoQueue(data_dir, account_id=cfg.dhan_client_id)
                response["fno_local_queue"] = queue.snapshot()
                count = sum(x["state"] == "QUEUED" for x in response["fno_local_queue"])
                response["summary"] += f" · LEGACY local F&O queue: {count} NOT auto-dispatched"
            except Exception:
                response["fno_local_queue"] = []
                response["summary"] += " · F&O local queue unreadable (verify on VM)"
            return _json(response)
        except Exception:
            return _json({"summary": "AMO journal unavailable", "orders": [], "verified": False}, 503)

    async def _save_amo_toggle(request: Request, form_key: str):
        """Save field directly under the shared service lock; never skip broker handoff."""
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON content type required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed JSON"}, 400)
        allowed = {"enabled", "csrf_token", "expected_revision", "request_id", "confirm_live"}
        if not isinstance(body, dict) or "enabled" not in body or "csrf_token" not in body or set(body) - allowed:
            return _json({"error": "Only enabled, csrf_token, expected_revision and request_id are permitted"}, 400)
        if body.get("csrf_token") != csrf_token or not isinstance(body.get("csrf_token"), str):
            return _json({"error": "Invalid CSRF token"}, 403)
        if type(body["enabled"]) is not bool:
            return _json({"error": "enabled must be a JSON boolean"}, 422)
        if not isinstance(body.get("expected_revision"), str) or not re.fullmatch(r"[0-9a-f]{64}", body["expected_revision"]):
            return _json({"error": "Missing or invalid configuration revision; refresh the Control Center"}, 409)
        try:
            cfg, env_path, data_dir = _config_and_paths()
            if not os.path.isfile(env_path) or os.path.islink(env_path):
                return _json({"error": "Configuration file must be a regular file"}, 409)
            # Historical tracker records are read-only audit evidence.
            tracker = ConfigChangeTracker(data_dir=data_dir)
            def _admission_check():
                # Called from apply_settings while holding the SAME global
                # config_operation.lock as validation, CAS, save and restart.
                # Never rewrite a legacy configuration record on an AMO save.
                if _pending(tracker):
                    raise ConfigurationConflict("Existing configuration operation is still active")
                if _owner_stop_pending(tracker):
                    raise ConfigurationConflict("Owner STOP pending; AMO update deferred")
                operating, _ = tracker.resolve_service_operating_state()
                if operating.value not in _ALLOWED_IDLE:
                    raise ConfigurationConflict("Service operating state uncertain; AMO update blocked")
                if operating.value == "RUNNING":
                    ok, _, _ = tracker.is_active_version_confirmed(
                        expected_hashes={compute_config_version(cfg)},
                        expected_mode=cfg.trading_mode.value,
                        require_systemd_active=True,
                    )
                    if not ok:
                        raise ConfigurationConflict("Running PID/version mismatch; AMO restart blocked")
            result = apply_settings(
                {form_key: body["enabled"]}, env_path=env_path,
                expected_revision=body["expected_revision"],
                confirm_live=body.get("confirm_live") is True,
                admission_check=_admission_check,
            )
            return _json({
                "ok": True,
                "state": (
                    "APPLIED_VERIFIED" if result.restart_performed
                    else "SAVED_FOR_NEXT_START" if result.saved
                    else "UNCHANGED"
                ),
                "runtime_verified": result.restart_performed,
                "restart_performed": result.restart_performed,
                "message": result.message, "revision": result.revision,
                "change_id": str(body.get("request_id") or "DIRECT-APPLY"),
            })
        except ConfigurationConflict as exc:
            return _json({"error": str(exc)}, 409)
        except ConfigurationRejected as exc:
            if "LIVE restart confirmation" in str(exc):
                return _json({"error": str(exc), "requires_live_confirmation": True}, 409)
            return _json({"error": str(exc)}, 422)
        except ValueError as exc:
            return _json({"error": str(exc)}, 422)
        except (OSError, TimeoutError):
            return _json({"error": "Configuration storage or service state unavailable"}, 409)
        except Exception:
            return _json({"error": "AMO configuration request could not be verified"}, 503)

    @app.post("/control/amo/equity")
    async def post_equity_amo(request: Request):
        return await _save_amo_toggle(request, "equity_amo_enabled")

    @app.post("/control/amo/fno/cancel")
    async def post_cancel_staged_fno_amo(request: Request):
        """Cancel only a LOCAL unclaimed F&O signal; never send broker cancellation."""
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON content type required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed JSON"}, 400)
        if (not isinstance(body, dict) or set(body) != {"item_id", "csrf_token"}
                or not isinstance(body.get("csrf_token"), str)
                or body["csrf_token"] != csrf_token):
            return _json({"error": "Invalid request/CSRF token"}, 403)
        if not isinstance(body.get("item_id"), str) or not re.fullmatch(r"-?\d+:-?\d+", body["item_id"]):
            return _json({"error": "Invalid staged signal identifier"}, 422)
        try:
            from src.engine.fno_amo_queue import FnoAmoQueue
            cfg, _, data_dir = _config_and_paths()
            q = FnoAmoQueue(data_dir, account_id=cfg.dhan_client_id)
            cancelled = q.cancel_queued(body["item_id"])
            return _json({
                "ok": cancelled,
                "state": "CANCELLED" if cancelled else "NOT_QUEUED",
                "message": "Legacy local F&O entry cancelled (no Dhan order was submitted)" if cancelled
                           else "Already dispatched, terminal, or not found. Check Dhan before any broker action.",
            }, 200 if cancelled else 409)
        except Exception:
            return _json({"error": "Unable to verify local F&O AMO cancellation"}, 503)

    @app.post("/control/amo/fno")
    async def post_fno_amo(request: Request):
        # Configuration only; never cancels existing Dhan or legacy local orders.
        return await _save_amo_toggle(request, "fno_amo_enabled")

    @app.post("/control/tracker-v2/recheck")
    async def recheck_tracker_v2(request: Request):
        """Password-authenticated outer Control Center + CSRF; no restart."""
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"ok": False, "error": "JSON request required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"ok": False, "error": "Malformed JSON"}, 400)
        if (not isinstance(body, dict) or set(body) != {"csrf_token", "change_id"}
                or body.get("csrf_token") != csrf_token):
            return _json({"ok": False, "error": "Invalid request/CSRF token"}, 403)
        try:
            cfg, env_path, data_dir = _config_and_paths()
            tracker = ConfigChangeTracker(data_dir=data_dir)
            from src.engine.prior_restart_recovery import recover_prior_restart
            result = recover_prior_restart(
                tracker, env_path, requested_change_id=body.get("change_id"),
            )
            return _json({
                "ok": result.runtime_verified,
                "change_id": result.change_id,
                "state": result.state.value,
                "runtime_verified": result.runtime_verified,
                "message": result.reason,
            }, 200 if result.runtime_verified else 409)
        except ConfigurationConflict as exc:
            return _json({"ok": False, "error": str(exc)}, 409)
        except (OSError, ValueError):
            return _json({"ok": False, "error": "Safety evidence unreadable"}, 409)
        except Exception:
            return _json({"ok": False, "error": "Late-ACK verification unavailable"}, 503)
