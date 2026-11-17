"""Read-only safety primitives for proposed UditBot configuration tracker V2.

Deliberately does not contact Dhan, Telegram or systemd, place broker orders,
modify protection, clear transition gates, or restart any service. Production
activation is not provided in this module. All ambiguous evidence blocks.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
import json
import os
import re
import stat
import time


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


def inspect_apply_barrier(path: os.PathLike[str] | str,
                          *, starttime_reader: Callable[[int], Optional[int]] = proc_starttime,
                          now: Optional[float] = None) -> SafetyDecision:
    """Never auto-release an active apply barrier by age.

    A dead owner needs explicit operator recovery as well: TTL alone is not
    proof that the prior restart/atomic save finished safely.
    """
    try:
        info = read_private_json(path, missing_ok=True)
    except (OSError, ValueError) as exc:
        return _result(False, 'BARRIER_UNREADABLE', str(exc))
    if info is None:
        return _result(True, 'NO_BARRIER', 'No V2 barrier exists')
    if info.get('active') is False:
        return _result(True, 'BARRIER_INACTIVE', 'Recorded barrier explicitly inactive')
    if info.get('active') is not True:
        return _result(False, 'BARRIER_INVALID', 'Barrier active flag is malformed')
    pid = info.get('owner_pid')
    start = info.get('owner_starttime')
    if type(pid) is not int or pid <= 0 or type(start) is not int or start <= 0:
        return _result(False, 'BARRIER_IDENTITY_AMBIGUOUS', 'Owner identity missing or malformed')
    live_start = starttime_reader(pid)
    if live_start == start:
        return _result(False, 'BARRIER_OWNER_LIVE', 'Apply owner is still alive; barrier cannot expire', owner_pid=pid)
    if live_start is None:
        return _result(False, 'BARRIER_OWNER_MISSING', 'Owner process not found; manual recovery required', owner_pid=pid)
    return _result(False, 'BARRIER_PID_REUSED', 'PID identity changed; operator review required', owner_pid=pid)


def evaluate_active_broker_operation_gate(
    data_dir: os.PathLike[str] | str,
    *, require_handoff: bool = True,
) -> SafetyDecision:
    """Conservative read-only broker-mutation gate for the staged V2 design.

    The legacy emergency_stop in_flight_orders format is honored. The newer
    mutation-handshake format has not yet been verified against the trading
    process, so its absence/ambiguous shape fails closed in production mode.
    An ordinary UI status reader need not call this function at all.
    """
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


def verify_runtime_ack(
    *,
    ack: Optional[Mapping[str, Any]],
    systemd: Mapping[str, Any],
    expected_hash: str,
    expected_mode: str,
    expected_credential_fingerprint: str,
    min_acknowledged_at: float = 0.0,
    starttime_reader: Callable[[int], Optional[int]] = proc_starttime,
    before_pid: Optional[int] = None,
    before_starttime: Optional[int] = None,
) -> SafetyDecision:
    """Strict positive runtime verification; never infer health from .env alone."""
    if not isinstance(ack, Mapping):
        return _result(False, 'NO_ACK', 'Runtime acknowledgement is missing')
    if str(systemd.get('ActiveState', '')).lower() != 'active':
        return _result(False, 'SERVICE_NOT_ACTIVE', 'systemd is not active')
    if str(systemd.get('Result', 'success')).lower() not in ('', 'success'):
        return _result(False, 'SERVICE_FAILED', 'systemd result is not successful')
    try:
        pid, ack_pid = int(systemd.get('MainPID') or 0), int(ack.get('pid') or 0)
        start = int(ack.get('process_starttime') or 0)
        ts = float(ack.get('acknowledged_at') or 0)
    except (ValueError, TypeError):
        return _result(False, 'ACK_IDENTITY_INVALID', 'Runtime identity or timestamp malformed')
    if pid <= 0 or pid != ack_pid or start <= 0:
        return _result(False, 'ACK_PID_MISMATCH', 'Runtime PID does not match systemd MainPID')
    if starttime_reader(pid) != start:
        return _result(False, 'ACK_STARTTIME_MISMATCH', 'Runtime process starttime is stale or unknown')
    if before_pid == pid and before_starttime == start:
        return _result(False, 'RESTART_NOT_OBSERVED', 'Running process identity has not changed')
    if str(ack.get('version_hash') or '') != expected_hash:
        return _result(False, 'ACK_HASH_MISMATCH', 'Running configuration hash differs')
    if str(ack.get('trading_mode') or '').upper() != expected_mode.upper():
        return _result(False, 'ACK_MODE_MISMATCH', 'Running trading mode differs')
    if not expected_credential_fingerprint or not re.fullmatch('[0-9a-f]{64}', expected_credential_fingerprint):
        return _result(False, 'CREDENTIAL_FINGERPRINT_UNAVAILABLE', 'Expected credential fingerprint missing')
    if str(ack.get('credential_fingerprint') or '') != expected_credential_fingerprint:
        return _result(False, 'CREDENTIAL_FINGERPRINT_MISMATCH', 'Credential fingerprint is absent or differs')
    if ts < min_acknowledged_at or ts <= 0:
        return _result(False, 'ACK_STALE', 'Runtime acknowledgment predates restart')
    return _result(True, 'RUNTIME_VERIFIED', 'PID/starttime/hash/mode/credentials match running process', pid=pid)


def preview_legacy_tracker_migration(data_dir: os.PathLike[str] | str, *, allow_owner_stopped: bool = False) -> SafetyDecision:
    """Pure read-only migration preview: never resets Owner Stop or safety gate."""
    p = Path(data_dir)
    try:
        control = read_private_json(p / 'service_control_state.json', missing_ok=True) or {}
        gate = read_private_json(p / 'mode_transition_gate.json', missing_ok=True)
        pending = read_private_json(p / 'pending_config_operation.json', missing_ok=True)
        # Gemini B07 used this alternate durable pending-operation name.
        # It must not be silently discarded or overwritten during migration
        # into PR87's V2 coordinator, even if the old tracker is no longer
        # importable on the updated GitHub branch.
        pending_replacement = read_private_json(
            p / 'pending_tracker_apply.json', missing_ok=True,
        )
        state = read_private_json(p / 'config_change_state.json', missing_ok=True)
        if control.get('_corrupted'):
            return _result(False, 'CONTROL_STATE_CORRUPT', 'Owner control status corrupt')
        desired = str(control.get('desired_service_state') or '').upper()
        observed = str(control.get('observed_state') or '').upper()
        # Desired STOPPED is an owner instruction even if observed state is
        # stale or missing. Never infer permission to restart from it.
        owner_stop = bool(control.get('deferred_stop_pending') is True or desired == 'STOPPED')
        if owner_stop:
            # A verified STOPPED_BY_OWNER service may save next-start settings
            # without revoking the owner's STOP intent or starting the unit.
            # Never permit this exception for deferred STOP or a running unit.
            safely_stopped = (
                allow_owner_stopped and desired == 'STOPPED'
                and control.get('deferred_stop_pending') is not True
                and observed in ('STOPPED_BY_OWNER', 'STOPPED')
            )
            if not safely_stopped:
                return _result(False, 'OWNER_STOP_PRIORITY', 'Owner Stop must be honored before tracker migration',
                               owner_stop_pending=True, pending_operation_present=pending is not None)
        if gate is not None and gate.get('active') is not False:
            return _result(False, 'TRANSITION_GATE_REQUIRES_REVIEW', 'An active/ambiguous gate must not be cleared')
        if pending is not None:
            return _result(False, 'PENDING_LEGACY_OPERATION', 'Preserve durable pending legacy operation',
                           pending_operation_present=True)
        if pending_replacement is not None:
            return _result(
                False, 'PENDING_REPLACEMENT_OPERATION',
                'Preserve prior Gemini tracker pending apply and reconcile before V2 save/restart',
                pending_operation_present=True,
            )
        if state and str(state.get('state') or '').upper() in (
            'REQUEST_RECEIVED', 'VALIDATING', 'SAVED', 'SAFETY_CHECK_PENDING',
            'WAITING_FOR_SAFE_APPLY', 'APPLYING',
        ):
            return _result(False, 'LEGACY_CHANGE_IN_PROGRESS', 'Legacy change has not reached a safe terminal state')
        return _result(True, 'MIGRATION_PREVIEW_ONLY', 'No obvious conflicting legacy state; no files modified',
                       owner_stop_pending=False, pending_operation_present=False)
    except (OSError, ValueError) as exc:
        return _result(False, 'LEGACY_STATE_UNREADABLE', f'{exc}; no migration allowed')


def inspect_transaction_recovery(data_dir: os.PathLike[str] | str, env_path: os.PathLike[str] | str) -> SafetyDecision:
    """Classify a V2 transaction without modifying .env or restarting.

    The journal *never* assumes the process reverted because a backup exists.
    Recovery is deliberately read-only until a reviewed replay protocol exists.
    """
    p = Path(data_dir) / 'config_restart_v2' / 'current_transaction.json'
    try:
        tx = read_private_json(p, missing_ok=True)
        if tx is None:
            return _result(True, 'NO_V2_TRANSACTION', 'No current V2 transaction')
        expected = str(tx.get('requested_file_revision') or '')
        previous = str(tx.get('previous_file_revision') or '')
        if not re.fullmatch('[0-9a-f]{64}', expected) or not re.fullmatch('[0-9a-f]{64}', previous):
            return _result(False, 'TX_CORRUPTED', 'Transaction hashes missing or malformed')
        ep = Path(env_path)
        if ep.is_symlink():
            return _result(False, 'ENV_SYMLINK', 'Configuration file is symlinked')
        actual = sha256(ep.read_bytes()).hexdigest()
        if actual not in (expected, previous):
            return _result(False, 'EXTERNAL_ENV_WRITER', 'External .env update detected; preserve file', file_sha256=actual)
        status = str(tx.get('phase') or '')
        if status in ('RESTART_ISSUED', 'VERIFYING', 'ACK_UNCERTAIN'):
            return _result(False, 'RESTART_OUTCOME_UNCERTAIN',
                           'Never restore old .env or restart twice; inspect live runtime and late ACK',
                           file_sha256=actual, phase=status)
        if status == 'SAVED_FOR_NEXT_START' and actual == expected:
            return _result(True, 'SAVED_NOT_RUNNING', 'Saved for a future controlled start; no runtime proof')
        if status == 'VERIFIED' and actual == expected:
            return _result(True, 'VERIFIED_RECORD', 'Recorded verification; recheck runtime before claiming active')
        if status in ('PREPARED', 'ENV_SAVED', 'WAITING_FOR_ORDER'):
            return _result(False, 'TRANSACTION_RECOVERY_REQUIRED',
                           'Durable previous transaction must be recovered without replaying restart',
                           file_sha256=actual, phase=status)
        return _result(False, 'TRANSACTION_STATE_UNKNOWN', 'Unknown transaction phase; manual review required', phase=status)
    except (OSError, ValueError) as exc:
        return _result(False, 'TX_UNREADABLE', f'{exc}; no automatic restart permitted')
