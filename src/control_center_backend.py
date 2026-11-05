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
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeTracker,
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
    if os.path.exists(tracker.pending_op_file) or os.path.exists(tracker.deferred_env_file):
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
    pending = _pending(tracker)
    stopping = _owner_stop_pending(tracker)
    gate = tracker.inspect_transition_gate_status(read_only=True)
    # No concurrent new request when a transition/gate cannot be verified.
    can_edit = (
        os.path.isfile(absolute_env)
        and state in _ALLOWED_IDLE
        and not pending
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
        "service_detail": "Runtime inspected through the existing systemd service coordinator",
        "saved_mode": str(raw.get("saved_mode") or "UNKNOWN"),
        "running_mode": str(raw.get("running_mode") or "UNKNOWN"),
        "runtime_verified": verified,
        "warning": "Control requests blocked while configuration/safety gate is unresolved" if pending or stopping else "",
        "equity_amo_saved": saved,
        "equity_amo_active": active,
        "equity_amo_can_edit": can_edit,
        "equity_amo_message": msg,
        "fno_amo_supported": True,
        "fno_amo_route": "LOCAL_STAGED_PROTECTED_SUPER_AT_OPEN",
        "fno_amo_saved": fno_saved,
        "fno_amo_active": fno_saved if verified else None,
        "fno_amo_can_edit": can_edit,
        "fno_amo_message": (
            msg + " · F&O AMO stages locally; no broker order until market OPEN"
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
                response["summary"] += f" · F&O local queue: {count} staged"
            except Exception:
                response["fno_local_queue"] = []
                response["summary"] += " · F&O local queue unreadable (verify on VM)"
            return _json(response)
        except Exception:
            return _json({"summary": "AMO journal unavailable", "orders": [], "verified": False}, 503)

    @app.post("/control/amo/equity")
    async def post_equity_amo(request: Request):
        # Exactly the same CSRF secret as POST /setup/service-control.
        # Deploy behind the same trusted operator authentication boundary as Setup.
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON content type required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed JSON"}, 400)
        if not isinstance(body, dict) or set(body) != {"enabled", "csrf_token"}:
            return _json({"error": "Exactly enabled and csrf_token are required"}, 400)
        if not isinstance(body.get("csrf_token"), str) or body["csrf_token"] != csrf_token:
            return _json({"error": "Invalid CSRF token"}, 403)
        if type(body["enabled"]) is not bool:
            return _json({"error": "enabled must be a JSON boolean"}, 422)
        requested = body["enabled"]

        cfg, env_path, data_dir = _config_and_paths()
        tracker = ConfigChangeTracker(data_dir=data_dir)
        if not os.path.isfile(env_path) or os.path.islink(env_path):
            return _json({"error": "Configuration file is not a regular file"}, 409)
        try:
            # Acquire the SAME cross-process lock as Setup and coordinator before
            # reading the existing file or inspecting pending operations.
            with tracker.operation_lock(operation_type="CONTROL_EQUITY_AMO", timeout_sec=3.0):
                if _pending(tracker) or _owner_stop_pending(tracker):
                    return _json({"error": "Another config transition or Owner Stop is pending"}, 409)
                gate = tracker.inspect_transition_gate_status(read_only=True)
                if gate.get("active", True) or gate.get("ok") is not True:
                    return _json({"error": "Safety transition gate not verified inactive"}, 409)
                status = tracker.control_service(action="status", env_path=env_path)
                state = str(status.get("service_state") or "UNKNOWN")
                if state not in _ALLOWED_IDLE:
                    return _json({"error": "Cannot verify safe bot service state"}, 409)
                if state == "RUNNING" and status.get("runtime_verified") is not True:
                    return _json({"error": "Running configuration version cannot be verified"}, 409)
                with open(env_path, "r", encoding="utf-8", newline="") as fh:
                    previous = fh.read()
                old_dict = parse_env_content_string(previous)
                old_value = old_dict.get(_CONFIG_KEY, "false")
                if str(old_value).strip().lower() not in ("false", "true", "0", "1", "yes", "no", "on", "off"):
                    return _json({"error": "Existing Equity AMO setting is invalid"}, 409)
                current_enabled = str(old_value).strip().lower() in ("true", "1", "yes", "on")
                if current_enabled == requested:
                    return _json({"ok": True, "state": "UNCHANGED", "message": "Equity AMO setting already saved"})
                new_content = _single_key_change(previous, requested)
                new_params = parse_env_content_string(new_content)
                # Do not silently rewrite other keys or request service restart on noop.
                if new_content == previous:
                    return _json({"ok": True, "state": "UNCHANGED", "message": "Equity AMO setting already saved"})
                # Persist via established durable coordinator. No direct .env write.
                read_only_broker = tracker.get_readonly_broker(env_path=env_path)
                rec = tracker.enqueue_durable_configuration_update(
                    form_params=new_params,
                    env_content=new_content,
                    env_path=env_path,
                    broker=read_only_broker,
                    spawn_worker=False,
                )
                change_id = rec.change_id
                state_name = rec.state.value if hasattr(rec.state, "value") else str(rec.state)
            # Coordinator wakes AFTER releasing outer cross-process lock.
            try:
                tracker.start_coordinator_daemon(env_path=env_path, broker=read_only_broker, stop_when_idle=False)
                message = "Equity AMO update queued for verified safe application"
            except Exception:
                message = "Equity AMO queued durably; coordinator wake not confirmed. Verify configuration status before retrying."
            return _json({"ok": True, "change_id": change_id, "state": state_name,
                          "message": message}, 202)
        except (OSError, ValueError, TimeoutError):
            return _json({"error": "Unable to safely queue Equity AMO change"}, 409)
        except Exception:
            return _json({"error": "Equity AMO change rejected; configuration unchanged or status unverified"}, 500)

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
                "message": "Local F&O AMO cancelled before market-open dispatch" if cancelled
                           else "Already dispatched, terminal, or not found. Check Dhan before any broker action.",
            }, 200 if cancelled else 409)
        except Exception:
            return _json({"error": "Unable to verify local F&O AMO cancellation"}, 503)

    @app.post("/control/amo/fno")
    async def post_fno_amo(request: Request):
        # Exactly the same CSRF secret as POST /setup/service-control.
        # Deploy behind the same trusted operator authentication boundary as Setup.
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON content type required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed JSON"}, 400)
        if not isinstance(body, dict) or set(body) != {"enabled", "csrf_token"}:
            return _json({"error": "Exactly enabled and csrf_token are required"}, 400)
        if not isinstance(body.get("csrf_token"), str) or body["csrf_token"] != csrf_token:
            return _json({"error": "Invalid CSRF token"}, 403)
        if type(body["enabled"]) is not bool:
            return _json({"error": "enabled must be a JSON boolean"}, 422)
        requested = body["enabled"]

        cfg, env_path, data_dir = _config_and_paths()
        tracker = ConfigChangeTracker(data_dir=data_dir)
        if not os.path.isfile(env_path) or os.path.islink(env_path):
            return _json({"error": "Configuration file is not a regular file"}, 409)
        try:
            # Acquire the SAME cross-process lock as Setup and coordinator before
            # reading the existing file or inspecting pending operations.
            with tracker.operation_lock(operation_type="CONTROL_FNO_AMO", timeout_sec=3.0):
                if _pending(tracker) or _owner_stop_pending(tracker):
                    return _json({"error": "Another config transition or Owner Stop is pending"}, 409)
                gate = tracker.inspect_transition_gate_status(read_only=True)
                if gate.get("active", True) or gate.get("ok") is not True:
                    return _json({"error": "Safety transition gate not verified inactive"}, 409)
                status = tracker.control_service(action="status", env_path=env_path)
                state = str(status.get("service_state") or "UNKNOWN")
                if state not in _ALLOWED_IDLE:
                    return _json({"error": "Cannot verify safe bot service state"}, 409)
                if state == "RUNNING" and status.get("runtime_verified") is not True:
                    return _json({"error": "Running configuration version cannot be verified"}, 409)
                with open(env_path, "r", encoding="utf-8", newline="") as fh:
                    previous = fh.read()
                old_dict = parse_env_content_string(previous)
                old_value = old_dict.get(_CONFIG_KEY, "false")
                if str(old_value).strip().lower() not in ("false", "true", "0", "1", "yes", "no", "on", "off"):
                    return _json({"error": "Existing F&O AMO setting is invalid"}, 409)
                current_enabled = str(old_value).strip().lower() in ("true", "1", "yes", "on")
                if current_enabled == requested:
                    return _json({"ok": True, "state": "UNCHANGED", "message": "F&O AMO setting already saved"})
                new_content = _single_key_change(previous, requested, key="FNO_AMO_ENABLED")
                new_params = parse_env_content_string(new_content)
                # Do not silently rewrite other keys or request service restart on noop.
                if new_content == previous:
                    return _json({"ok": True, "state": "UNCHANGED", "message": "F&O AMO setting already saved"})
                # Persist via established durable coordinator. No direct .env write.
                read_only_broker = tracker.get_readonly_broker(env_path=env_path)
                rec = tracker.enqueue_durable_configuration_update(
                    form_params=new_params,
                    env_content=new_content,
                    env_path=env_path,
                    broker=read_only_broker,
                    spawn_worker=False,
                )
                change_id = rec.change_id
                state_name = rec.state.value if hasattr(rec.state, "value") else str(rec.state)
            # Coordinator wakes AFTER releasing outer cross-process lock.
            try:
                tracker.start_coordinator_daemon(env_path=env_path, broker=read_only_broker, stop_when_idle=False)
                message = "F&O AMO update queued for verified safe application"
            except Exception:
                message = "F&O AMO queued durably; coordinator wake not confirmed. Verify configuration status before retrying."
            return _json({"ok": True, "change_id": change_id, "state": state_name,
                          "message": message}, 202)
        except (OSError, ValueError, TimeoutError):
            return _json({"error": "Unable to safely queue F&O AMO change"}, 409)
        except Exception:
            return _json({"error": "F&O AMO change rejected; configuration unchanged or status unverified"}, 500)