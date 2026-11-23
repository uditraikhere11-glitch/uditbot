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
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config import AppConfig, parse_env_dict
from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, patch_env, revision, validate_effective_config
from src.engine.runtime_evidence import read_private_json
from src.engine.operational_snapshot import read_verified_snapshot
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


def _owner_stop_cancellation_available(control: dict, gate: dict) -> bool:
    """Permit an explicit retry after owner-intent save but gate-clear failure."""
    if not (isinstance(control, dict) and isinstance(gate, dict)):
        return False
    if not (gate.get('ok') is True and gate.get('active') is True
            and gate.get('change_id') == 'OWNER-STOP-PENDING'
            and gate.get('target_mode') == 'STOPPED'):
        return False
    if (control.get('desired_service_state') == 'STOPPED'
            and control.get('deferred_stop_pending') is True):
        return True
    return (control.get('desired_service_state') == 'RUNNING'
            and control.get('deferred_stop_pending') is False
            and control.get('last_action') == 'OWNER_CANCEL_STOP')

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


# Broker profile GET is explicit, bounded and cached. Never perform a TOTP login.
_BROKER_LOCK = threading.Lock()
_BROKER_CACHE: Dict[str, Any] = {"key": None, "at": 0.0, "value": None}
_BROKER_CACHE_TTL = 90.0


def _cached_broker_status(cfg: AppConfig, *, refresh: bool = False) -> Dict[str, Any]:
    """Read-only Dhan /v2/profile evidence. No POST, token minting or auto refresh."""
    token_path = os.path.expanduser(str(cfg.dhan_token_file or "~/.dhan_token"))
    try:
        token_stat = os.stat(token_path)
        token_key = (str(cfg.dhan_client_id), token_path, token_stat.st_ino,
                     token_stat.st_size, token_stat.st_mtime_ns)
        if os.path.islink(token_path) or not stat.S_ISREG(token_stat.st_mode) or token_stat.st_size < 1 or (token_stat.st_mode & 0o077):
            raise OSError("Token file not private")
    except OSError:
        return {"status": "UNVERIFIED", "detail": "Private token unavailable", "verified_at": None,
                "token_expiration_ist": None}
    with _BROKER_LOCK:
        now = time.time()
        cached = (_BROKER_CACHE.get("key") == token_key and
                  now - _BROKER_CACHE.get("at", 0) < _BROKER_CACHE_TTL)
        if cached and _BROKER_CACHE.get("value") is not None:
            value = dict(_BROKER_CACHE["value"])
            expiry = value.get("token_expiration_ts")
            if type(expiry) in (int, float) and now >= expiry:
                value["status"] = "EXPIRED"
                value["detail"] = "Token expired"
            return value
        if not refresh:
            return {"status": "UNVERIFIED", "detail": "No recent broker profile verification",
                    "verified_at": None, "token_expiration_ist": None}
        result = {"status": "UNVERIFIED", "detail": "Broker profile could not be verified",
                  "verified_at": None, "token_expiration_ist": None}
        client = None
        try:
            from src.broker.dhan_client import DhanClient
            client = DhanClient(cfg, auto_generate_token=False)
            evidence = client.verify_profile_and_token_expiry(allow_refresh=False)
            status = str(evidence.get("identity_status") or "UNVERIFIED").upper()
            if status not in {"VERIFIED", "EXPIRED", "MISMATCH", "UNVERIFIED"}:
                status = "UNVERIFIED"
            result["status"] = status
            if status == "VERIFIED" and evidence.get("identity_verified") is True:
                result["verified_at"] = now
                result["detail"] = "Dhan profile account identity verified"
            elif status in {"MISMATCH", "EXPIRED"}:
                result["detail"] = "Broker identity mismatch" if status == "MISMATCH" else "Token expired"
            expiry = evidence.get("token_expiration_ts")
            if status in ("VERIFIED", "EXPIRED") and type(expiry) in (int, float) and 0 < expiry < 4102444800:
                result["token_expiration_ist"] = datetime.fromtimestamp(
                    expiry, ZoneInfo("Asia/Kolkata")).strftime("%d %b %H:%M IST")
                result["token_expiration_ts"] = expiry
        except Exception:
            # Do not expose broker exceptions, token, account ID or raw payload.
            pass
        finally:
            if client is not None:
                try:
                    client.session.close()
                except Exception:
                    pass
        _BROKER_CACHE.update({"key": token_key, "at": now, "value": result})
        return dict(result)


def _operational_evidence(tracker: ConfigChangeTracker, raw: Dict[str, Any],
                          cfg: AppConfig, data_dir: str) -> Dict[str, Any]:
    """Correlate the 10-second producer heartbeat with systemd/PID/starttime."""
    unknown = {"reconciliation": "UNVERIFIED", "protection_watcher": "UNVERIFIED",
               "new_entries_gate": "UNVERIFIED", "telegram_listener": "UNVERIFIED", "runtime_evidence_at": None}
    if raw.get("runtime_verified") is not True or raw.get("service_state") != "RUNNING":
        return unknown
    ack = tracker.get_active_running_version(require_live_pid=True) or {}
    pid, start = ack.get("pid"), ack.get("process_starttime")
    data = read_verified_snapshot(data_dir, expected_pid=pid, expected_starttime=start,
                                  expected_account_id=str(cfg.dhan_client_id or ""))
    if data is None:
        return unknown
    recon = data["reconciliation"]
    watcher = "ACTIVE" if data["watcher_running"] is True else "INACTIVE"
    # An open gate is credible only after a healthy recon AND clear STOP intent.
    gate = "OPEN" if data["entries_open"] and recon == "HEALTHY" else "BLOCKED"
    telegram = ("CONNECTED" if data.get("telegram_connected") is True else
                "DISCONNECTED" if data.get("telegram_connected") is False else "UNVERIFIED")
    return {"reconciliation": recon, "protection_watcher": watcher,
            "new_entries_gate": gate, "telegram_listener": telegram,
            "runtime_evidence_at": data["created_at"]}


def build_control_reports(env_path: str = ".env") -> Dict[str, Any]:
    """Allowlisted read-only journal metrics; no log text, secrets or mutations."""
    cfg, _env_path, _dir = _config_and_paths(env_path)
    path = os.path.abspath(cfg.journal_db_path)
    if not os.path.isfile(path) or os.path.islink(path):
        return {"available": False, "summary": {}, "events": [], "recent_orders": []}
    uri = "file:" + quote(path, safe="/") + "?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True, timeout=1) as con:
            con.execute("PRAGMA query_only=ON")
            # The labels intentionally do not expose original event descriptions.
            event_types = {
                "DAILY_HEALTH_CHECK", "RECONCILIATION_GATE_HALTED",
                "RECONCILIATION_GATE_RECOVERED", "RECONCILIATION_GATE_BLOCKED"
            }
            rows = con.execute(
                "SELECT event_type, created_at FROM system_events "
                "WHERE event_type IN (?,?,?,?) ORDER BY id DESC LIMIT 20",
                tuple(sorted(event_types)),
            ).fetchall()
            events = [{"event": typ, "at": ts} for typ, ts in rows
                      if typ in event_types and isinstance(ts, (int, float))]
            today_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            totals = {
                "orders_today": con.execute("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (today_ist,)).fetchone()[0],
                "signals_today": con.execute("SELECT COUNT(*) FROM signals WHERE received_at >= ?", (today_ist,)).fetchone()[0],
                "trades_today": con.execute("SELECT COUNT(*) FROM fills WHERE traded_at >= ?", (today_ist,)).fetchone()[0],
                "orders": con.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
                "signals": con.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                "unresolved_intents": con.execute("SELECT COUNT(*) FROM order_intents WHERE state='UNKNOWN'").fetchone()[0],
                "unprotected_records": con.execute("SELECT COUNT(*) FROM protection_orders "
                                                 "WHERE status IN ('UNPROTECTED','UNVERIFIED')").fetchone()[0],
            }
            orders = con.execute(
                "SELECT symbol, status, trading_mode, created_at FROM orders ORDER BY id DESC LIMIT 12"
            ).fetchall()
            latest = []
            for sym, state, mode, ts in orders:
                clean_symbol = str(sym or "")[:32]
                if not re.fullmatch(r"[A-Za-z0-9._ -]{1,32}", clean_symbol):
                    clean_symbol = "Instrument"
                clean_state = str(state or "UNKNOWN").upper()
                if not re.fullmatch(r"[A-Z_]{1,32}", clean_state):
                    clean_state = "UNKNOWN"
                latest.append({"symbol": clean_symbol, "status": clean_state,
                               "mode": mode if mode in ("LIVE", "TEST") else "UNKNOWN",
                               "at": ts if isinstance(ts, (int, float)) else None})
            return {"available": True, "summary": totals, "events": events, "recent_orders": latest}
    except (sqlite3.Error, OSError, ValueError):
        return {"available": False, "summary": {}, "events": [], "recent_orders": []}


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
    from src.engine.tracker_v2_health import owner_intent_health
    owner = owner_intent_health(state, raw.get("owner_intent"))
    observed = _operational_evidence(tracker, raw, cfg, data_dir)
    if (owner.status != "PASS" or gate.get("ok") is not True
            or gate.get("active") is not False) and observed["new_entries_gate"] == "OPEN":
        # A healthy reconciliation heartbeat alone cannot override a pending
        # owner transition, including an interrupted Cancel Pending STOP.
        observed["new_entries_gate"] = "REVIEW_REQUIRED"
    return {
        **observed,
        "owner_intent_status": owner.status,
        "owner_stop_pending": bool(stopping),
        "owner_stop_cancel_available": _owner_stop_cancellation_available(raw.get("owner_intent") or {}, gate),
        "mode_can_edit": bool(can_edit and owner.status == "PASS"),
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
    broker = _cached_broker_status(cfg)
    broker_state = broker.get("status")
    check("Dhan broker authentication", "PASS" if broker_state == "VERIFIED" else
          "FAIL" if broker_state in ("MISMATCH", "EXPIRED") else "UNKNOWN",
          broker.get("detail", "Authenticated broker identity unavailable"))
    expiry = broker.get("token_expiration_ts")
    check("Dhan token expiration", "PASS" if broker_state == "VERIFIED" and
          type(expiry) in (int, float) and expiry > time.time() else
          "FAIL" if type(expiry) in (int, float) and expiry <= time.time() else "UNKNOWN",
          broker.get("token_expiration_ist") or "No broker/JWT-verified expiry evidence")
    runtime_listener = _operational_evidence(tracker, status, cfg, data_dir)
    tg_state = runtime_listener["telegram_listener"]
    check("Telegram connection", "PASS" if tg_state == "CONNECTED" else
          "WARN" if tg_state == "DISCONNECTED" else "UNKNOWN",
          "Telegram client connected; receiving monitored channel messages and outbound delivery are not tested" if tg_state == "CONNECTED" else
          "Telegram client disconnected" if tg_state == "DISCONNECTED" else
          "Configured channels: " + str(len(cfg.monitored_channel_ids)) + "; live client connection not verified")
    check("Yahoo quote freshness", "UNKNOWN", "No live market quote requested by read-only health check")
    gate = tracker.inspect_transition_gate_status(read_only=True)
    check("Transition safety gate", "WARN" if gate.get("active", True) else "PASS", "Active or unverified" if gate.get("active", True) else "Not active")
    check("Configuration coordinator", "WARN" if _pending(tracker) else "PASS", "Change pending" if _pending(tracker) else "No pending update detected")
    runtime = _operational_evidence(tracker, status, cfg, data_dir)
    recon = runtime["reconciliation"]
    check("Reconciliation", "PASS" if recon == "HEALTHY" else
          "WARN" if recon == "BLOCKED" else "UNKNOWN",
          "Process-matched runtime evidence" if recon != "UNVERIFIED" else "Fresh process snapshot unavailable")
    watcher = runtime["protection_watcher"]
    check("Order protection watcher", "PASS" if watcher == "ACTIVE" else
          "WARN" if watcher == "INACTIVE" else "UNKNOWN",
          "Watcher loop active (does not prove every open leg)" if watcher == "ACTIVE"
          else "Watcher loop inactive" if watcher == "INACTIVE" else "Watcher heartbeat not independently verified")
    gate_state = runtime["new_entries_gate"]
    check("New entries gate", "PASS" if gate_state == "OPEN" and intent.status == "PASS" else
          "WARN" if gate_state == "BLOCKED" or intent.status == "WARN" else "UNKNOWN",
          "Open based on fresh healthy process evidence" if gate_state == "OPEN" and intent.status == "PASS"
          else "Blocked or owner intent requires review" if gate_state == "BLOCKED" or intent.status == "WARN"
          else "Live gate status unverified")
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

    @app.post("/control/cancel-pending-stop")
    async def cancel_pending_stop(request: Request):
        """Explicit CSRF-protected owner decision; never issues systemctl."""
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON request required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed request"}, 400)
        if (not isinstance(body, dict) or set(body) !=
                {"csrf_token", "confirm_keep_running"}):
            return _json({"error": "Invalid owner-control request"}, 400)
        if (not isinstance(body.get("csrf_token"), str)
                or body["csrf_token"] != csrf_token):
            return _json({"error": "Invalid CSRF token"}, 403)
        if body.get("confirm_keep_running") is not True:
            return _json({"error": "Explicit keep-running confirmation required"}, 422)
        try:
            _cfg, env_path, data_dir = _config_and_paths()
            tracker = ConfigChangeTracker(data_dir=data_dir)
            result = tracker.cancel_deferred_owner_stop(env_path=env_path)
            return _json(result, 200 if result.get("ok") else 409)
        except (OSError, ValueError, TimeoutError):
            return _json({"error": "Owner control cannot be verified"}, 409)
        except Exception:
            return _json({"error": "Owner cancellation unavailable"}, 503)

    @app.get("/control/health")
    def get_control_health():
        try:
            return _json(build_control_health())
        except Exception:
            return _json({"summary": "Health checks are not available", "checks": []}, 503)

    @app.get("/control/broker-status")
    def get_control_broker_status():
        try:
            cfg, _env, _data = _config_and_paths()
            return _json(_cached_broker_status(cfg, refresh=True))
        except Exception:
            return _json({"status": "UNVERIFIED", "detail": "Read-only broker check unavailable"}, 503)

    @app.get("/control/reports")
    def get_control_reports():
        try:
            return _json(build_control_reports())
        except Exception:
            return _json({"available": False, "summary": {}, "events": []}, 503)

    @app.post("/control/mode")
    async def post_control_mode(request: Request):
        """Single-setting Save & Apply transaction shared with Setup; never edit .env directly."""
        if "application/json" not in request.headers.get("content-type", "").lower():
            return _json({"error": "JSON content type required"}, 415)
        try:
            body = await request.json()
        except Exception:
            return _json({"error": "Malformed JSON"}, 400)
        if (not isinstance(body, dict) or set(body) !=
                {"mode", "csrf_token", "expected_revision", "confirm_live"}):
            return _json({"error": "Invalid mode request"}, 400)
        if body["csrf_token"] != csrf_token or not isinstance(body["csrf_token"], str):
            return _json({"error": "Invalid CSRF token"}, 403)
        if body["mode"] not in ("LIVE", "TEST"):
            return _json({"error": "Only LIVE and TEST supported"}, 422)
        if type(body["confirm_live"]) is not bool:
            return _json({"error": "Boolean confirmation required"}, 422)
        if not isinstance(body["expected_revision"], str) or not re.fullmatch(r"[0-9a-f]{64}", body["expected_revision"]):
            return _json({"error": "Current configuration revision required"}, 409)
        try:
            status = build_control_status()
            if status.get("mode_can_edit") is not True:
                return _json({"error": "Safety/Owner STOP gate prevents mode change"}, 409)
            result = apply_settings(
                {"is_live_mode": body["mode"] == "LIVE"}, env_path=os.path.abspath(".env"),
                expected_revision=body["expected_revision"],
                confirm_live=body["confirm_live"],
            )
            return _json(result.as_dict())
        except ConfigurationConflict as exc:
            return _json({"error": str(exc)}, 409)
        except ConfigurationRejected as exc:
            return _json({"error": str(exc)}, 422)
        except (ValueError, OSError, TimeoutError):
            return _json({"error": "Mode configuration could not be verified"}, 409)
        except Exception:
            return _json({"error": "Mode change unavailable"}, 503)

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
