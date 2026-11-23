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


def format_runtime_status(snapshot: dict, *, instance: str, mode: str) -> str:
    state = str(snapshot.get("reconciliation") or "UNKNOWN").upper()
    gate_open = snapshot.get("entries_open") is True and state == "HEALTHY"
    watcher_running = snapshot.get("watcher_running") is True
    supervised = watcher_running and snapshot.get("watcher_supervision_verified") is True
    observed_at = float(snapshot.get("created_at") or 0)
    timestamp = datetime.fromtimestamp(observed_at, ZoneInfo("Asia/Kolkata")).strftime("%d %b %H:%M:%S IST")
    icon = "🟢" if gate_open and supervised else ("🔴" if not gate_open else "🟡")
    return "\n".join((
        f"{icon} <b>{escape(str(instance))} — Runtime Status Updated</b>",
        f"Observed: {escape(timestamp)}",
        f"Mode: {escape(str(mode))}",
        f"Reconciliation: {escape(state)}",
        f"Watcher Loop: {'ACTIVE' if watcher_running else 'INACTIVE'}",
        f"Broker Supervision Read: {'VERIFIED' if supervised else 'UNVERIFIED'}",
        f"New Entries Gate: {'OPEN' if gate_open else 'BLOCKED'}",
        "This is a point-in-time runtime update; the WebUI shows the latest process-matched state.",
    ))
