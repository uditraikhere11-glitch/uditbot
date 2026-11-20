"""Read broker handoff evidence and Linux process identity for configuration restarts."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional
import json
import os
import stat


@dataclass(frozen=True)
class SafetyDecision:
    allowed: bool
    code: str
    reason: str
    evidence: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _result(ok: bool, code: str, reason: str, **evidence: Any) -> SafetyDecision:
    return SafetyDecision(ok, code, reason, evidence)


def read_private_json(path: os.PathLike[str] | str, *, missing_ok: bool = False) -> Optional[dict[str, Any]]:
    """Read bounded, non-symlinked JSON, never interpreting corruption as empty.

    File permissions are checked when present. Avoid following a symlink race by
    comparing lstat/fstat identities after opening with O_NOFOLLOW on Linux.
    """
    p = Path(path)
    try:
        info = p.lstat()
    except FileNotFoundError:
        if missing_ok:
            return None
        raise ValueError(f"Required safety record missing: {p.name}") from None
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Safety record is not a regular file: {p.name}")
    if info.st_size == 0 or info.st_size > 256 * 1024:
        raise ValueError(f"Safety record empty or too large: {p.name}")
    fd = os.open(str(p), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        actual = os.fstat(fd)
        if (actual.st_dev, actual.st_ino) != (info.st_dev, info.st_ino):
            raise ValueError(f"Safety record replaced while opening: {p.name}")
        if actual.st_mode & 0o022:
            raise ValueError(f"Safety record is writable by group/other: {p.name}")
        with os.fdopen(fd, 'rb') as fh:
            fd = -1
            raw = fh.read(256 * 1024 + 1)
        data = json.loads(raw.decode('utf-8'))
        if not isinstance(data, dict):
            raise ValueError(f"Safety record must contain a JSON object: {p.name}")
        return data
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Safety record malformed: {p.name}: {exc}") from None
    finally:
        if fd >= 0:
            os.close(fd)


def proc_starttime(pid: int, *, proc_root: os.PathLike[str] | str = '/proc') -> Optional[int]:
    """Linux /proc/<pid>/stat field 22; robust to spaces in the comm field."""
    if type(pid) is not int or pid <= 0:
        return None
    try:
        raw = (Path(proc_root) / str(pid) / 'stat').read_text(encoding='ascii')
        tail = raw.rsplit(')', 1)[1].strip().split()
        # tail[0] is stat field 3; field 22 corresponds to index 19.
        return int(tail[19])
    except (OSError, ValueError, IndexError):
        return None


def evaluate_active_broker_operation_gate(
    data_dir: os.PathLike[str] | str,
    *, require_handoff: bool = True,
) -> SafetyDecision:
    """Require evidence that no broker request is in flight or unresolved."""
    data = Path(data_dir)
    try:
        emergency = read_private_json(data / 'emergency_stop.json', missing_ok=True)
        if emergency is not None:
            if emergency.get('is_stopped') is True:
                return _result(False, 'EMERGENCY_STOP_ACTIVE', 'Emergency Stop is active; no restart permitted')
            if 'is_stopped' in emergency and type(emergency['is_stopped']) is not bool:
                return _result(False, 'EMERGENCY_SCHEMA_UNKNOWN', 'Emergency Stop flag is invalid')
            in_flight = emergency.get('in_flight_orders', [])
            if not isinstance(in_flight, list):
                return _result(False, 'EMERGENCY_SCHEMA_UNKNOWN', 'Emergency in-flight collection is invalid')
            if in_flight:
                return _result(False, 'EMERGENCY_IN_FLIGHT', 'A legacy broker mutation is in flight', count=len(in_flight))
        mutation = read_private_json(data / 'active_broker_mutations.json', missing_ok=True)
        if mutation is None:
            return (_result(False, 'NO_RUNTIME_HANDOFF',
                            'Running trading process has not proved V2 broker-operation registration')
                    if require_handoff else _result(True, 'NO_MUTATION_RECORD', 'No V2 mutation record'))
        # The proposal requires a positive, versioned handoff, not a guessed
        # empty array in an old producer version.
        if mutation.get('schema_version') != 2 or mutation.get('handoff_verified') is not True:
            return _result(False, 'HANDOFF_UNVERIFIED', 'Broker mutation producer handshake is not verified')
        active = mutation.get('active')
        if not isinstance(active, list):
            return _result(False, 'MUTATION_SCHEMA_UNKNOWN', 'Mutation list is invalid')
        unresolved = mutation.get('unresolved', [])
        if not isinstance(unresolved, list):
            return _result(False, 'UNRESOLVED_SCHEMA_UNKNOWN', 'Unresolved mutation list invalid')
        if unresolved:
            return _result(False, 'UNRESOLVED_BROKER_MUTATION', 'Unknown mutation outcome requires broker reconciliation', count=len(unresolved))
        if active:
            return _result(False, 'BROKER_MUTATION_ACTIVE', 'Broker mutation is active', count=len(active))
        # File presence alone is insufficient: production must also verify the
        # producer PID/starttime against the systemd MainPID before restart.
        if type(mutation.get('producer_pid')) is not int or type(mutation.get('producer_starttime')) is not int:
            return _result(False, 'HANDOFF_IDENTITY_UNKNOWN', 'Broker producer identity absent')
        return _result(True, 'NO_ACTIVE_BROKER_OPERATION', 'Broker handoff verified, no mutation active',
                       producer_pid=mutation['producer_pid'],
                       producer_starttime=mutation['producer_starttime'])
    except (OSError, ValueError) as exc:
        return _result(False, 'SAFETY_RECORD_UNREADABLE', f'{exc}; do not restart')
