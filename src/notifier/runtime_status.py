"""Deduplicated process-truth updates for the Telegram log group.

Consumes the exact same read-only operational snapshot as the WebUI. Never
performs Dhan calls or modifies entry admission. Startup is a point-in-time
message; later *stable* material changes receive their own timestamped card.
"""
from __future__ import annotations

from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo


class RuntimeStatusTransitions:
    """Emit once per stable changed state; ignore one-frame watcher poll jitter."""

    def __init__(self) -> None:
        self._last_emitted = None
        self._pending_state = None
        self._candidate = None
        self._seen = 0

    @staticmethod
    def state(snapshot: dict) -> tuple:
        return (
            snapshot.get("reconciliation"),
            snapshot.get("entries_open") is True,
            snapshot.get("watcher_running") is True,
            snapshot.get("watcher_supervision_verified") is True,
        )

    def arm(self, snapshot: dict) -> None:
        self._last_emitted = self.state(snapshot)
        self._pending_state = None
        self._candidate = None
        self._seen = 0

    def observe(self, snapshot: dict, *, instance: str, mode: str) -> str | None:
        if self._last_emitted is None:
            self.arm(snapshot)
            return None
        state = self.state(snapshot)
        if state == self._last_emitted:
            self._candidate = None
            self._seen = 0
            return None
        if state != self._candidate:
            self._candidate = state
            self._seen = 1
            return None
        self._seen += 1
        if self._seen < 2:
            return None
        # Do not mark delivered until the durable outbox INSERT succeeds.
        self._pending_state = state
        return format_runtime_status(snapshot, instance=instance, mode=mode)

    def confirm_queued(self, snapshot: dict) -> None:
        """Advance the dedupe cursor ONLY after the SQLite outbox accepted the card."""
        if self._pending_state == self.state(snapshot):
            self._last_emitted = self._pending_state
            self._pending_state = None
            self._candidate = None
            self._seen = 0


def summarize_runtime_status(
    snapshot: dict | None,
    *,
    unified_snapshot: dict | None = None,
) -> dict:
    """Phase 4: Single shared read-only status projection for both Telegram and WebUI.

    Never performs broker I/O and never authorizes trading mutations.
    """
    snap = snapshot if isinstance(snapshot, dict) else {}
    uni = unified_snapshot if isinstance(unified_snapshot, dict) else (
        snap.get("unified_broker_snapshot") if isinstance(snap.get("unified_broker_snapshot"), dict) else {}
    )
    state = str(snap.get("reconciliation") or "UNKNOWN").upper()
    gate_open = snap.get("entries_open") is True and state == "HEALTHY"
    watcher_running = snap.get("watcher_running") is True
    supervised = watcher_running and snap.get("watcher_supervision_verified") is True
    if gate_open and supervised:
        badge = "HEALTHY"
        icon = "🟢"
    elif not gate_open:
        badge = "BLOCKED"
        icon = "🔴"
    else:
        badge = "DEGRADED"
        icon = "🟡"
    uni_status = str(uni.get("status") or ("FRESH" if uni.get("available") else "UNAVAILABLE")).upper()
    return {
        "reconciliation_state": state,
        "entries_open": gate_open,
        "watcher_running": watcher_running,
        "supervision_verified": supervised,
        "status_badge": badge,
        "icon": icon,
        "unified_snapshot_available": bool(uni.get("available") is True),
        "unified_snapshot_status": uni_status,
        "session_phase": str(uni.get("session_phase") or "UNKNOWN"),
        "display_only": True,
        "authoritative_for_mutation": False,
    }


def format_runtime_status(snapshot: dict, *, instance: str, mode: str) -> str:
    summary = summarize_runtime_status(snapshot)
    observed_at = float(snapshot.get("created_at") or 0)
    timestamp = datetime.fromtimestamp(observed_at, ZoneInfo("Asia/Kolkata")).strftime("%d %b %H:%M:%S IST")
    lines = [
        f"{summary['icon']} <b>{escape(str(instance))} — Runtime Status Updated</b>",
        f"Observed: {escape(timestamp)}",
        f"Mode: {escape(str(mode))}",
        f"Reconciliation: {escape(summary['reconciliation_state'])}",
        f"Watcher Loop: {'ACTIVE' if summary['watcher_running'] else 'INACTIVE'}",
        f"Broker Supervision Read: {'VERIFIED' if summary['supervision_verified'] else 'UNVERIFIED'}",
        f"New Entries Gate: {'OPEN' if summary['entries_open'] else 'BLOCKED'}",
    ]
    if summary["unified_snapshot_available"]:
        lines.append(
            f"Unified Snapshot: {escape(summary['unified_snapshot_status'])} ({escape(summary['session_phase'])})"
        )
    lines.append("This is a point-in-time runtime update; the WebUI shows the latest process-matched state.")
    return "\n".join(lines)
