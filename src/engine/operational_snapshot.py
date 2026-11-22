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
from src.engine.runtime_evidence import proc_starttime, read_private_json

FILE_NAME = "runtime_operational_snapshot.json"
MAX_AGE_SECONDS = 35.0


def account_fingerprint(client_id: str) -> str:
    return hashlib.sha256(str(client_id or "").encode("utf-8")).hexdigest()


def publish_operational_snapshot(
    pipeline: Any, *, telegram_connected: bool | None = None
) -> dict[str, Any]:
    """Publish only facts held by the currently running trading process.

    This is observability only. A failed write does not touch the entry gate,
    broker, watcher lifecycle, or emergency STOP state.
    """
    gate = pipeline.reconciliation_gate
    watcher = pipeline.watcher
    state = str(getattr(getattr(gate, "state", None), "value", "UNKNOWN")).upper()
    pid = os.getpid()
    starttime = proc_starttime(pid)
    if starttime is None:
        raise RuntimeError("Cannot attest runtime process identity")
    watcher_running = getattr(watcher, "_is_running", None) is True
    entry_open = gate.is_accepting_entries is True
    snapshot = {
        "schema": 1,
        "created_at": time.time(),
        "pid": pid,
        "process_starttime": starttime,
        "account_fingerprint": account_fingerprint(pipeline.cfg.dhan_client_id),
        "reconciliation": state if state in ("HEALTHY", "BLOCKED", "INITIALIZING") else "UNKNOWN",
        "watcher_running": watcher_running,
        "telegram_connected": telegram_connected if type(telegram_connected) is bool else None,
        "entries_open": entry_open,
        "last_reconciled_at": float(getattr(gate, "last_reconciled_at", 0.0) or 0.0),
    }
    write_json_atomic(os.path.join(pipeline.cfg.data_dir, FILE_NAME), snapshot)
    return snapshot


def read_verified_snapshot(
    data_dir: str, *, expected_pid: int, expected_starttime: int,
    expected_account_id: str, now_ts: float | None = None,
) -> dict[str, Any] | None:
    """Reject stale, replaced, wrong-account, wrong-PID and malformed records."""
    try:
        item = read_private_json(os.path.join(data_dir, FILE_NAME), missing_ok=True)
        if item is None or item.get("schema") != 1:
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
        if item.get("reconciliation") not in ("HEALTHY", "BLOCKED", "INITIALIZING", "UNKNOWN"):
            return None
        if type(item.get("watcher_running")) is not bool or type(item.get("entries_open")) is not bool:
            return None
        if item.get("telegram_connected") is not None and type(item.get("telegram_connected")) is not bool:
            return None
        # A gate can never be displayed OPEN without the healthy reconciliation state.
        if item["entries_open"] and item["reconciliation"] != "HEALTHY":
            return None
        return {key: item[key] for key in
                ("created_at", "reconciliation", "watcher_running", "telegram_connected", "entries_open", "last_reconciled_at")}
    except (OSError, ValueError, TypeError, KeyError):
        return None
