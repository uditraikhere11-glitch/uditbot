"""Read-only runtime evidence shared from the trading process to the WebUI.

No network calls and no trading decisions. A snapshot is never authoritative unless
it matches the current systemd process and current account/configuration identity.
"""
from __future__ import annotations

import hashlib
import os
import time
from typing import Any

from src.engine.config_manager import write_json_atomic
from src.engine.tracker_v2_safety import inspect_apply_barrier
from src.engine.runtime_evidence import proc_starttime, read_private_json

FILE_NAME = "runtime_operational_snapshot.json"
MAX_AGE_SECONDS = 35.0
MAX_SUPERVISION_AGE_SECONDS = 300.0


def account_fingerprint(client_id: str) -> str:
    return hashlib.sha256(str(client_id or "").encode("utf-8")).hexdigest()


def collect_operational_snapshot(
    pipeline: Any, *, telegram_connected: bool | None = None
) -> dict[str, Any]:
    """Build the common WebUI/Telegram process-matched readiness truth."""
    gate = pipeline.reconciliation_gate
    watcher = pipeline.watcher
    from contextlib import nullcontext
    with getattr(gate, "_entry_state_lock", nullcontext()):
        state = str(getattr(getattr(gate, "state", None), "value", "UNKNOWN")).upper()
        entry_open = gate.is_accepting_entries is True and state == "HEALTHY"
        reconciled_at = float(getattr(gate, "last_reconciled_at", 0.0) or 0.0)
    if entry_open:
        barrier_path = os.path.join(pipeline.cfg.data_dir, "config_restart_v2", "apply_barrier.json")
        entry_open = inspect_apply_barrier(barrier_path).allowed
    pid = os.getpid()
    starttime = proc_starttime(pid)
    if starttime is None:
        raise RuntimeError("Cannot attest runtime process identity")
    watcher_running = getattr(watcher, "_is_running", None) is True
    now = time.time()
    success_at = getattr(watcher, "_supervision_success_at", None)
    proof_fresh = (type(success_at) in (int, float)
                   and 0 <= now - success_at <= MAX_SUPERVISION_AGE_SECONDS)
    watcher_verified = bool(watcher_running and proof_fresh
                            and getattr(watcher, "_supervision_read_complete", None) is True)
    snapshot = {
        "schema": 2,
        "created_at": now,
        "pid": pid,
        "process_starttime": starttime,
        "account_fingerprint": account_fingerprint(pipeline.cfg.dhan_client_id),
        "reconciliation": state if state in ("HEALTHY", "BLOCKED", "RECONCILING", "INITIALIZING") else "UNKNOWN",
        "watcher_running": watcher_running,
        "watcher_supervision_verified": watcher_verified,
        "watcher_supervision_at": float(success_at) if watcher_verified else None,
        "telegram_connected": telegram_connected if type(telegram_connected) is bool else None,
        "entries_open": entry_open,
        "last_reconciled_at": reconciled_at,
    }
    return snapshot


def publish_operational_snapshot(
    pipeline: Any, *, telegram_connected: bool | None = None
) -> dict[str, Any]:
    """Write the same status facts used by Telegram, without broker calls."""
    snapshot = collect_operational_snapshot(pipeline, telegram_connected=telegram_connected)
    write_json_atomic(os.path.join(pipeline.cfg.data_dir, FILE_NAME), snapshot)
    return snapshot


def read_verified_snapshot(
    data_dir: str, *, expected_pid: int, expected_starttime: int,
    expected_account_id: str, now_ts: float | None = None,
) -> dict[str, Any] | None:
    """Reject stale, replaced, wrong-account, wrong-PID and malformed records."""
    try:
        item = read_private_json(os.path.join(data_dir, FILE_NAME), missing_ok=True)
        if item is None or item.get("schema") not in (1, 2):
            return None
        if type(expected_pid) is not int or expected_pid <= 0:
            return None
        if type(expected_starttime) is not int or expected_starttime <= 0:
            return None
        if type(item.get("pid")) is not int or item["pid"] != expected_pid:
            return None
        if type(item.get("process_starttime")) is not int or item["process_starttime"] != expected_starttime:
            return None
        if proc_starttime(expected_pid) != expected_starttime:
            return None
        if item.get("account_fingerprint") != account_fingerprint(expected_account_id):
            return None
        now = time.time() if now_ts is None else float(now_ts)
        stamp = item.get("created_at")
        if type(stamp) not in (int, float) or not -3 <= now - stamp <= MAX_AGE_SECONDS:
            return None
        if item.get("reconciliation") not in ("HEALTHY", "BLOCKED", "RECONCILING", "INITIALIZING", "UNKNOWN"):
            return None
        if type(item.get("watcher_running")) is not bool or type(item.get("entries_open")) is not bool:
            return None
        if item.get("telegram_connected") is not None and type(item.get("telegram_connected")) is not bool:
            return None
        # A gate can never be displayed OPEN without the healthy reconciliation state.
        if item["entries_open"] and item["reconciliation"] != "HEALTHY":
            return None
        verified = False
        verified_at = None
        if item["schema"] == 2:
            if type(item.get("watcher_supervision_verified")) is not bool:
                return None
            verified = item["watcher_supervision_verified"]
            verified_at = item.get("watcher_supervision_at")
            if verified and (not item["watcher_running"]
                             or type(verified_at) not in (int, float)
                             or not 0 <= now - verified_at <= MAX_SUPERVISION_AGE_SECONDS):
                return None
        result = {key: item[key] for key in
                  ("created_at", "reconciliation", "watcher_running", "telegram_connected", "entries_open", "last_reconciled_at")}
        result["watcher_supervision_verified"] = verified
        result["watcher_supervision_at"] = verified_at if verified else None
        return result
    except (OSError, ValueError, TypeError, KeyError):
        return None
