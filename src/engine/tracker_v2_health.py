"""Pure read-only state consistency checks for UditBot Control Center.

No broker I/O or state mutation. A green running service is insufficient when a
saved owner STOP exists. An unavailable control record is never inferred PASS.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Any


@dataclass(frozen=True)
class ControlIntentHealth:
    status: str
    message: str


def owner_intent_health(service_state: str, control: Mapping[str, Any] | None) -> ControlIntentHealth:
    if not isinstance(control, Mapping) or not control or control.get("_corrupted"):
        return ControlIntentHealth("UNKNOWN", "Owner control intent missing or unreadable")
    desired = str(control.get("desired_service_state") or "").upper()
    observed = str(control.get("observed_state") or "").upper()
    deferred = control.get("deferred_stop_pending")
    state = str(service_state or "").upper()
    if deferred is True:
        return ControlIntentHealth("WARN", "Owner STOP remains pending; restart/config changes blocked")
    if type(deferred) is not bool:
        return ControlIntentHealth("UNKNOWN", "Deferred STOP state not positively recorded")
    if desired not in ("RUNNING", "STOPPED"):
        return ControlIntentHealth("UNKNOWN", "Owner desired service state missing or unrecognized")
    if state == "RUNNING":
        if desired != "RUNNING" or observed != "RUNNING":
            return ControlIntentHealth("WARN", "Running service conflicts with saved Owner STOP/observed state")
        return ControlIntentHealth("PASS", "Owner RUNNING intent agrees with observed running service")
    if state == "STOPPED_BY_OWNER":
        if desired == "STOPPED" and observed in ("STOPPED_BY_OWNER", "STOPPED"):
            return ControlIntentHealth("PASS", "Owner STOP confirmed by service observation")
        return ControlIntentHealth("WARN", "Stopped service conflicts with saved owner control state")
    if state in ("STOPPED_OTHER", "FAILED"):
        return ControlIntentHealth("WARN", "Service is not running; owner intent requires review")
    return ControlIntentHealth("UNKNOWN", "Current service state not positively reconciled")
