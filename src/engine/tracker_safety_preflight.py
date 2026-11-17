"""Read-only, fail-closed UditBot configuration/restart evidence preflight.

This module is deliberately NOT wired to the running coordinator. It never
restarts services, writes files, contacts brokers, or interprets a file's
presence as proof that LIVE configuration is applied. The caller must supply
fresh systemd evidence; the preflight itself does not call systemctl.

Future integration must be protected by an off-by-default feature flag and
must independently prove broker-order submission safety before restarting.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping, Optional

_HASH = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class Evidence:
    safe_to_propose: bool
    runtime_verified: bool
    state: str
    blockers: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "safe_to_propose": self.safe_to_propose,
            "runtime_verified": self.runtime_verified,
            "state": self.state,
            "blockers": list(self.blockers),
        }


def _json_object(path: Path, *, required: bool) -> Optional[dict[str, Any]]:
    """Read without following symlinks; fail closed on races and bad types."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    except FileNotFoundError:
        if required:
            raise ValueError("Required evidence missing") from None
        return None
    except OSError as exc:
        raise ValueError("Evidence unreadable or symlinked") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Evidence is not a regular file")
        if info.st_size > 1048576:
            raise ValueError("Evidence file exceeds 1 MiB")
        with os.fdopen(fd, "rb", closefd=False) as source:
            raw = source.read(1048577)
        if len(raw) > 1048576:
            raise ValueError("Evidence file exceeds 1 MiB")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("Evidence is not a JSON object")
        return value
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Evidence malformed") from exc
    finally:
        os.close(fd)


def _inspect(path: Path, label: str, blockers: list[str], *, required: bool = False) -> Optional[dict[str, Any]]:
    try:
        return _json_object(path, required=required)
    except ValueError as exc:
        blockers.append(f"{label}: {exc}")
        return None


def _active_gate(value: dict[str, Any], blockers: list[str]) -> None:
    # A positive gate is *not* a stale gate to be deleted; only the owning
    # coordinator can close it after a durable verified state transition.
    if value.get("active") is True or value.get("is_active") is True:
        blockers.append("MODE_TRANSITION_GATE_ACTIVE: transition requires authoritative release")
    elif not any(key in value for key in ("active", "is_active")):
        blockers.append("MODE_TRANSITION_GATE_UNKNOWN: missing active status")
    elif any(value.get(k) not in (None, False) for k in ("active", "is_active")):
        blockers.append("MODE_TRANSITION_GATE_INVALID: non-boolean active status")


def verify_ack(
    ack: Optional[Mapping[str, Any]],
    *,
    main_pid: int,
    pid_starttime: int,
    effective_hash: str,
    effective_mode: str,
    credential_fingerprint: str,
) -> bool:
    """No-op and restart ACK verification must check *all* runtime identity data.

    No fallback to service-active, file timestamp, or saved .env values.
    This helper assumes `pid_starttime` is an authoritative process identity
    from the same PID inspected by the caller, not a wall-clock timestamp.
    """
    if not isinstance(ack, Mapping):
        return False
    if not isinstance(main_pid, int) or isinstance(main_pid, bool) or main_pid <= 0:
        return False
    if not isinstance(pid_starttime, int) or isinstance(pid_starttime, bool) or pid_starttime <= 0:
        return False
    if not isinstance(effective_hash, str) or not _HASH.fullmatch(effective_hash):
        return False
    if effective_mode not in ("LIVE", "TEST") or not credential_fingerprint:
        return False
    try:
        return (
            type(ack.get("pid")) is int and ack["pid"] == main_pid
            and type(ack.get("pid_starttime")) is int
            and ack["pid_starttime"] == pid_starttime
            and ack.get("version_hash") == effective_hash
            and ack.get("trading_mode") == effective_mode
            and ack.get("credential_fingerprint") == credential_fingerprint
        )
    except (KeyError, TypeError):
        return False


def inspect_snapshot(
    data_dir: str | Path,
    *,
    service_state: str,
    main_pid: int = 0,
    pid_starttime: int = 0,
    effective_hash: str = "",
    effective_mode: str = "",
    credential_fingerprint: str = "",
) -> Evidence:
    """Conservative preflight using only read-only local evidence.

    `safe_to_propose` never authorizes a restart or LIVE entry; it merely says
    no *locally observable* blocker was found. A broker and journal admission
    gate is still required for a running trading process.
    """
    d = Path(data_dir)
    blockers: list[str] = []
    if service_state not in ("active", "inactive"):
        blockers.append("SERVICE_UNKNOWN: running state not authoritative")

    control = _inspect(d / "service_control_state.json", "OWNER_CONTROL", blockers)
    if control is not None:
        if control.get("deferred_stop_pending") is True:
            blockers.append("OWNER_STOP_PENDING: do not supersede Owner Stop")
        if str(control.get("desired_service_state") or "").upper() == "STOPPED":
            blockers.append("OWNER_STOP_REQUESTED: do not start/restart")
        if "deferred_stop_pending" in control and type(control["deferred_stop_pending"]) is not bool:
            blockers.append("OWNER_CONTROL_INVALID: deferred_stop_pending must be boolean")

    pending = _inspect(d / "pending_config_operation.json", "PENDING_OPERATION", blockers)
    if pending is not None:
        blockers.append("PENDING_OPERATION_EXISTS: preserve and reconcile, do not delete")

    gate = _inspect(d / "mode_transition_gate.json", "MODE_TRANSITION_GATE", blockers)
    if gate is not None:
        _active_gate(gate, blockers)

    # Missing emergency-stop evidence while *running* is ambiguous when
    # migrating a pre-handoff process: do not infer there is no mutation.
    emergency = _inspect(
        d / "emergency_stop.json", "EMERGENCY_STOP", blockers,
        required=service_state == "active",
    )
    if emergency is not None:
        if type(emergency.get("is_stopped")) is not bool:
            blockers.append("EMERGENCY_STOP_INVALID: is_stopped must be boolean")
        elif emergency["is_stopped"]:
            blockers.append("EMERGENCY_STOP_ACTIVE: never clear automatically")
        inflight = emergency.get("in_flight_orders", [])
        if not isinstance(inflight, list):
            blockers.append("IN_FLIGHT_INVALID: order list malformed")
        elif inflight:
            blockers.append("IN_FLIGHT_ORDERS: broker action unresolved")

    # A separate mutation ledger is not automatically trustworthy when missing;
    # a running pre-handoff process must still undergo the real broker gate.
    mutations = _inspect(d / "active_broker_mutations.json", "BROKER_MUTATIONS", blockers)
    if mutations is not None:
        if not isinstance(mutations.get("mutations", []), list):
            blockers.append("BROKER_MUTATIONS_INVALID: expected list")
        elif mutations.get("mutations"):
            blockers.append("BROKER_MUTATIONS_PRESENT: broker action unresolved")

    ack = _inspect(d / "active_config_version.json", "RUNTIME_ACK", blockers)
    verified = (service_state == "active" and not blockers and verify_ack(
        ack, main_pid=main_pid, pid_starttime=pid_starttime,
        effective_hash=effective_hash, effective_mode=effective_mode,
        credential_fingerprint=credential_fingerprint,
    ))
    if service_state == "active" and not verified and not blockers:
        blockers.append("RUNTIME_UNVERIFIED: missing or stale PID/starttime/hash/mode/fingerprint")
    if service_state == "inactive":
        # No stopped service can be *runtime verified*. Safe-to-propose a save
        # must never be confused with permission to start it.
        return Evidence(not blockers, False, "STOPPED", tuple(blockers))
    return Evidence(not blockers and verified, verified,
                    "VERIFIED" if verified else "ACTION_REQUIRED", tuple(blockers))
