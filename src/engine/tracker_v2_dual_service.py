"""Fail-closed V2/legacy save routing between WebUI and trading process.

Running a V2 trading process behind a legacy WebUI can bypass the broker
mutation fence. This read-only guard prohibits split-brain configuration saves.
It neither repairs stale records nor changes feature flags or order state.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Callable


class DualServiceMismatch(RuntimeError):
    pass


def trading_service_pid() -> int:
    try:
        p = subprocess.run(
            ['systemctl', 'show', 'trading_bot.service', '-p', 'MainPID', '--value', '--no-pager'],
            capture_output=True, text=True, timeout=4,
        )
        if p.returncode:
            raise ValueError('Cannot inspect trading service')
        return int(p.stdout.strip())
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise DualServiceMismatch('Cannot verify trading process for configuration update') from exc


def trading_service_state() -> str:
    try:
        p = subprocess.run(
            ['systemctl', 'show', 'trading_bot.service', '-p', 'ActiveState', '--value', '--no-pager'],
            capture_output=True, text=True, timeout=4,
        )
        if p.returncode:
            raise ValueError('Cannot inspect trading service state')
        return p.stdout.strip()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise DualServiceMismatch('Cannot verify trading service state') from exc


def flag_for_pid(pid: int, proc_root: Path = Path('/proc')) -> bool:
    if pid <= 0:
        return False
    try:
        values = (proc_root / str(pid) / 'environ').read_bytes().split(b'\0')
        matches = [v.partition(b'=')[2] for v in values if v.startswith(b'UDITBOT_TRACKER_V2_ENABLE=')]
        if len(matches) > 1:
            raise DualServiceMismatch('Ambiguous tracker flag in running trading process')
        return matches == [b'1']
    except OSError as exc:
        raise DualServiceMismatch('Trading process V2 flag is unreadable') from exc


def assert_consistent_save_route(
    data_dir: str | Path,
    *,
    web_v2: bool,
    pid_reader: Callable[[], int] = trading_service_pid,
    process_flag: Callable[[int], bool] = flag_for_pid,
    state_reader: Callable[[], str] = trading_service_state,
) -> None:
    """Raise before any .env write when routing could be split-brain.

    An absent V2 record on a fully legacy deployment preserves legacy saves.
    A V2 record on a legacy WebUI blocks (including after a bot crash), so
    pending/unknown outcomes cannot silently fall back to the old coordinator.
    """
    from src.engine.tracker_v2_safety import read_private_json

    data = Path(data_dir)
    ledger_path = data / 'active_broker_mutations.json'
    tx = data / 'config_restart_v2' / 'current_transaction.json'
    barrier = data / 'config_restart_v2' / 'apply_barrier.json'
    try:
        ledger = read_private_json(ledger_path, missing_ok=True)
        # Preserve unknown transaction/barrier evidence even on legacy route.
        if not web_v2:
            if ledger is not None or tx.exists() or tx.is_symlink() or barrier.exists() or barrier.is_symlink():
                raise DualServiceMismatch('V2 state exists while WebUI uses legacy configuration route')
            # A missing/deleted broker ledger is not proof that an active
            # trading process is legacy. Check the process flag independently
            # before permitting the old coordinator to write .env.
            pid = pid_reader()
            state = state_reader()
            if type(pid) is not int or pid < 0 or state not in ('active', 'inactive'):
                raise DualServiceMismatch('Legacy trading process identity/state unverified')
            if state == 'active' and pid == 0:
                raise DualServiceMismatch('Active trading service has no verified PID')
            if state == 'inactive' and pid != 0:
                raise DualServiceMismatch('Stopped trading service has an unexpected PID')
            if pid > 0 and process_flag(pid):
                raise DualServiceMismatch('Trading process V2 flag active while WebUI uses legacy route')
            return
        pid = pid_reader()
        if type(pid) is not int or pid < 0:
            raise DualServiceMismatch('Trading process identity unavailable')
        if pid == 0:
            # A systemd restart can briefly report MainPID=0 while service
            # state is activating. Never interpret that as an Owner STOP.
            if state_reader() != 'inactive':
                raise DualServiceMismatch('Trading service is not positively inactive')
            owner = read_private_json(data/'service_control_state.json', missing_ok=True)
            if (not isinstance(owner, dict) or
                    owner.get('desired_service_state') != 'STOPPED' or
                    owner.get('observed_state') != 'STOPPED_BY_OWNER' or
                    owner.get('deferred_stop_pending') is not False):
                raise DualServiceMismatch('Owner-stopped service state not verified')
            if ledger is not None:
                if (ledger.get('schema_version') != 2 or
                        ledger.get('active') != [] or
                        ledger.get('unresolved') != []):
                    raise DualServiceMismatch('Broker handoff has unresolved or malformed evidence')
            return
        if not process_flag(pid):
            raise DualServiceMismatch('Trading process does not run with V2 enabled')
        if not isinstance(ledger, dict):
            raise DualServiceMismatch('Trading process V2 broker handoff missing')
        if (ledger.get('schema_version') != 2 or
                ledger.get('handoff_verified') is not True or
                ledger.get('producer_pid') != pid):
            raise DualServiceMismatch('Trading process V2 handoff identity not verified')
        # The real V2 coordinator then checks starttime, account, active
        # mutations, broker reconciliation and credential fingerprint.
    except DualServiceMismatch:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DualServiceMismatch('V2 routing safety record unreadable') from exc
