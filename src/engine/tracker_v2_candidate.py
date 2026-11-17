"""Staged, dependency-injected candidate for UditBot single-save restart V2.

THIS IS NOT AN ACTIVATED WEBUI INTEGRATION. No production entrypoint imports
or enables this candidate. The class never contacts Dhan/Telegram/systemd; a
reviewed caller must supply signed-off readiness, runtime probes and service
callbacks. All ambiguous states remain blocked. The default is review-only.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
import fcntl
import json
import os
import re
import secrets
import stat
import tempfile
import time

from src.engine.tracker_v2_safety import (
    SafetyDecision, evaluate_active_broker_operation_gate, inspect_apply_barrier,
    inspect_transaction_recovery, preview_legacy_tracker_migration,
    read_private_json, verify_runtime_ack,
)


class ChangeBlocked(RuntimeError):
    """An update cannot proceed without operator recovery or verified safety."""


@dataclass(frozen=True)
class SubmitResult:
    phase: str
    change_id: str
    runtime_verified: bool
    restart_issued: bool
    message: str


def _sha(b: bytes) -> str:
    return sha256(b).hexdigest()


def _atomic_private(path: Path, b: bytes) -> None:
    """Preserve same-directory atomicity and never expose credential backups."""
    if path.is_symlink():
        raise ChangeBlocked(f'{path.name} is symlinked')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.stat().st_mode & 0o022:
        raise ChangeBlocked(f'{path.parent.name} is group/world writable')
    fd, tmp = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb') as out:
            fd = -1
            out.write(b)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        dfd = os.open(str(path.parent), os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        if fd >= 0:
            os.close(fd)
        if os.path.exists(tmp):
            os.unlink(tmp)


def _json_write(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_private(path, (json.dumps(dict(value), sort_keys=True, indent=2) + '\n').encode('utf-8'))


def _read_env(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ChangeBlocked('.env must be a regular non-symlinked file')
    return path.read_bytes()


class CandidateCoordinator:
    """Durable single-operation state machine for offline review.

    Production requires a reviewed integration that proves the running trading
    process observes the same entry barrier before allowing new order dispatch.
    """
    def __init__(self, data_dir: str | Path, env_path: str | Path, *, feature_enabled: bool = False,
                 require_external_fence: bool = False,
                 allow_stopped_owner_save: bool = False):
        self.feature_enabled = bool(feature_enabled)
        self.require_external_fence = bool(require_external_fence)
        self.allow_stopped_owner_save = bool(allow_stopped_owner_save)
        self.data_dir = Path(data_dir).resolve()
        self.env_path = Path(env_path).absolute()
        self.txdir = self.data_dir / 'config_restart_v2'
        self.journal = self.txdir / 'current_transaction.json'
        self.lock = self.txdir / '.transaction.lock'
        self.barrier = self.txdir / 'apply_barrier.json'

    @contextmanager
    def _exclusive(self):
        self.txdir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.txdir.stat().st_mode & 0o022:
            raise ChangeBlocked('Tracker transaction directory is insecure')
        fd = os.open(self.lock, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def read_status(self) -> SafetyDecision:
        """No side effects; unverified status is never green by inference."""
        recovery = inspect_transaction_recovery(self.data_dir, self.env_path)
        if not recovery.allowed:
            return recovery
        try:
            return preview_legacy_tracker_migration(self.data_dir)
        except Exception:
            # Fail closed if an unexpected error occurs reading safety evidence.
            return SafetyDecision(False, 'STATUS_UNAVAILABLE', 'Safety state could not be inspected', {})

    def _check_legacy(self, *, service_running: bool = True) -> None:
        d = preview_legacy_tracker_migration(
            self.data_dir,
            allow_owner_stopped=self.allow_stopped_owner_save and not service_running,
        )
        if not d.allowed:
            raise ChangeBlocked(f'{d.code}: {d.reason}')
        if self.barrier.exists():
            b = inspect_apply_barrier(self.barrier)
            if not b.allowed:
                raise ChangeBlocked(f'{b.code}: {b.reason}')
        # Migration preview deliberately never edits legacy state.

    def submit(
        self,
        *, request_id: str,
        expected_file_revision: str,
        new_env: bytes,
        previous_mode: str,
        target_mode: str,
        service_running: bool,
        confirm_live: bool = False,
        # Positive pre-flight handoff proof is supplied by reviewed caller.
        operation_gate: Optional[Callable[[], SafetyDecision]] = None,
        pre_runtime_verified: bool = False,
        entry_fence: Optional[Callable[[str], bool]] = None,
        restart_callback: Optional[Callable[[], bool]] = None,
        ack_probe: Optional[Callable[[], tuple[Optional[Mapping[str, Any]], Mapping[str, Any]]]] = None,
        expected_runtime_hash: str = '',
        credential_fingerprint: str = '',
        starttime_reader: Optional[Callable[[int], Optional[int]]] = None,
        before_pid: Optional[int] = None,
        before_starttime: Optional[int] = None,
        release_fence: Optional[Callable[[str], bool]] = None,
    ) -> SubmitResult:
        """Single transaction; restart callback runs AT MOST once per request.

        A timeout never triggers implicit rollback or another restart. The
        package intentionally has no production service callbacks or feature
        activation switch. The caller must independently validate new_env via
        the existing WebUI registry and preserve its explicit CSRF/auth checks.
        """
        if not self.feature_enabled:
            raise ChangeBlocked('Tracker V2 is disabled by default; no production activation')
        if not isinstance(new_env, bytes):
            raise ChangeBlocked('new_env must be canonical bytes validated by existing registry')
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{8,128}', str(request_id)):
            raise ChangeBlocked('Invalid idempotency request ID')
        if not re.fullmatch(r'[0-9a-f]{64}', str(expected_file_revision)):
            raise ChangeBlocked('Invalid .env compare-and-swap revision')
        if previous_mode not in ('TEST', 'LIVE') or target_mode not in ('TEST', 'LIVE'):
            raise ChangeBlocked('Unrecognized trading mode')
        if previous_mode != 'LIVE' and target_mode == 'LIVE' and not confirm_live:
            raise ChangeBlocked('Owner confirmation required to enable LIVE')
        if service_running and not pre_runtime_verified:
            raise ChangeBlocked('Previous running PID/hash/mode/credential version not verified')

        new_revision = _sha(new_env)
        with self._exclusive():
            # Read existing journal first. Never use a "last failed" marker as
            # permission to restart an operation with an uncertain outcome.
            existing = read_private_json(self.journal, missing_ok=True)
            if existing is not None:
                if existing.get('request_id') == request_id:
                    if existing.get('requested_file_revision') != new_revision:
                        raise ChangeBlocked('Idempotency key reused for different configuration')
                    return SubmitResult(str(existing.get('phase') or 'ACTION_REQUIRED'),
                                        str(existing.get('change_id') or ''),
                                        bool(existing.get('phase') == 'VERIFIED'),
                                        bool(existing.get('restart_issued')), 'Idempotent replay: no second restart')
                if existing.get('phase') not in ('NO_CHANGE', 'SAVED_FOR_NEXT_START', 'VERIFIED'):
                    raise ChangeBlocked('Previous transaction unresolved; no new restart allowed')
            self._check_legacy(service_running=service_running)
            current = _read_env(self.env_path)
            if _sha(current) != expected_file_revision:
                raise ChangeBlocked('External .env modification detected (CAS mismatch)')
            if current == new_env:
                # No verified ACK is asserted, even when systemd says active.
                return SubmitResult('NO_CHANGE', request_id, False, False,
                                    'No values changed; no restart; runtime verification not inferred')

            if service_running:
                if operation_gate is None:
                    raise ChangeBlocked('No active broker operation probe supplied')
                gate = operation_gate()
                if not isinstance(gate, SafetyDecision) or not gate.allowed:
                    raise ChangeBlocked('Active broker gate failed or produced invalid evidence')
                if (gate.code != 'NO_ACTIVE_BROKER_OPERATION'
                        or gate.evidence.get('producer_pid') != before_pid
                        or gate.evidence.get('producer_starttime') != before_starttime):
                    raise ChangeBlocked('Broker handoff identity does not match running trading process')
                if entry_fence is None or restart_callback is None:
                    raise ChangeBlocked('Production entry barrier or restart action has no reviewed implementation')
                if ack_probe is None or starttime_reader is None or release_fence is None:
                    raise ChangeBlocked('Runtime verification or gate release is not configured')
                if not re.fullmatch(r'[0-9a-f]{64}', expected_runtime_hash) or not re.fullmatch(r'[0-9a-f]{64}', credential_fingerprint):
                    raise ChangeBlocked('Missing canonical config or credential fingerprint')
                if (type(before_pid) is not int or type(before_starttime) is not int
                        or before_pid <= 0 or before_starttime <= 0):
                    raise ChangeBlocked('Missing old runtime PID identity')

            cid = 'V2-' + secrets.token_hex(8)
            # Initial journal is written before .env, and contains no secret.
            base = {
                'change_id': cid, 'request_id': request_id, 'phase': 'PREPARED',
                'previous_file_revision': expected_file_revision,
                'requested_file_revision': new_revision,
                'previous_mode': previous_mode, 'target_mode': target_mode,
                'restart_issued': False, 'created_at': time.time(),
                'previous_pid': before_pid if service_running else None,
                'previous_starttime': before_starttime if service_running else None,
                'expected_runtime_hash': expected_runtime_hash if service_running else None,
                'credential_fingerprint': credential_fingerprint if service_running else None,
            }
            backup = self.txdir / (cid + '.env.bak')
            _atomic_private(backup, current)
            _json_write(self.journal, base)
            if service_running:
                # **Before** .env replacement and before systemctl. A failed
                # fence leaves existing configuration intact and blocks replay.
                if not entry_fence(cid):
                    base.update(phase='ACTION_REQUIRED', reason='Entry fence could not be verified')
                    _json_write(self.journal, base)
                    raise ChangeBlocked('Entry fence unavailable; no .env change or restart')
                owner_identity = starttime_reader(os.getpid())
                if type(owner_identity) is not int or owner_identity <= 0:
                    base.update(phase='ACTION_REQUIRED', reason='Entry fence owner identity unverified')
                    _json_write(self.journal, base)
                    raise ChangeBlocked('Owner identity unavailable; entry fence retained; no restart')
                if self.require_external_fence:
                    installed = read_private_json(self.barrier, missing_ok=False)
                    if (installed.get('active') is not True or
                            installed.get('change_id') != cid or
                            installed.get('owner_pid') != os.getpid() or
                            installed.get('owner_starttime') != owner_identity):
                        base.update(phase='ACTION_REQUIRED', reason='Shared broker apply fence is unverified')
                        _json_write(self.journal, base)
                        raise ChangeBlocked('Shared broker apply fence not durably verified')
                else:
                    # Existing review tests use a fake callback; production
                    # must explicitly choose the shared interlock above.
                    _json_write(self.barrier, {
                        'active': True, 'owner_pid': os.getpid(),
                        'owner_starttime': owner_identity,
                        'change_id': cid, 'created_at': time.time(),
                    })
            # Revalidate optimistic CAS right before the atomic replacement;
            # noncooperating writers remain an external operational risk.
            if _sha(_read_env(self.env_path)) != expected_file_revision:
                base.update(phase='ACTION_REQUIRED', reason='External writer before replacement')
                _json_write(self.journal, base)
                raise ChangeBlocked('External .env writer detected before replacement')
            _atomic_private(self.env_path, new_env)
            base['phase'] = 'ENV_SAVED'
            _json_write(self.journal, base)
            if not service_running:
                base['phase'] = 'SAVED_FOR_NEXT_START'
                _json_write(self.journal, base)
                return SubmitResult('SAVED_FOR_NEXT_START', cid, False, False,
                                    'Stopped service remains stopped; settings saved for controlled start')
            # Durable intent must be fsynced BEFORE restart is invoked. If
            # callback never returns, a recovery must not issue it again.
            # Owner Stop may arrive after admission or the atomic save.
            # It takes priority over issuing a new restart. Preserve fence and
            # new file for review rather than resurrecting the trading service.
            stop_check = preview_legacy_tracker_migration(self.data_dir)
            if not stop_check.allowed:
                base.update(phase='ACTION_REQUIRED', reason='Pre-restart safety change: ' + stop_check.code)
                _json_write(self.journal, base)
                return SubmitResult('ACTION_REQUIRED', cid, False, False,
                                    'Owner or legacy safety state changed before restart; fence retained')
            base.update(phase='RESTART_ISSUED', restart_issued=True, restart_initiated_at=time.time())
            _json_write(self.journal, base)
            try:
                issued = restart_callback()
            except Exception:
                issued = False
            if not issued:
                base.update(phase='ACK_UNCERTAIN', reason='Cannot prove restart outcome')
                _json_write(self.journal, base)
                return SubmitResult('ACK_UNCERTAIN', cid, False, True,
                                    'Restart outcome uncertain; no rollback, no second restart; entry fence retained')
            base['phase'] = 'VERIFYING'
            _json_write(self.journal, base)
            try:
                ack, systemd = ack_probe()
                v = verify_runtime_ack(
                    ack=ack, systemd=systemd, expected_hash=expected_runtime_hash,
                    expected_mode=target_mode,
                    expected_credential_fingerprint=credential_fingerprint,
                    min_acknowledged_at=base['restart_initiated_at'],
                    starttime_reader=starttime_reader,
                    before_pid=before_pid, before_starttime=before_starttime,
                )
            except Exception:
                v = SafetyDecision(False, 'ACK_PROBE_UNAVAILABLE', 'Verification probe failed', {})
            if not v.allowed:
                base.update(phase='ACK_UNCERTAIN', reason=v.code)
                _json_write(self.journal, base)
                return SubmitResult('ACK_UNCERTAIN', cid, False, True,
                                    'Restart issued; running configuration unverified; entry fence retained')
            # Gemini B03/B05: a correct process ACK alone is not enough.
            # Another process may have changed .env after the atomic save.
            # Recheck the exact durable file revision before opening broker
            # admission; never silently mark that different disk state green.
            try:
                current_revision_ok = _sha(_read_env(self.env_path)) == new_revision
            except (OSError, ValueError, UnicodeError):
                current_revision_ok = False
            if not current_revision_ok:
                base.update(phase='ACTION_REQUIRED', reason='Saved .env changed after restart ACK')
                _json_write(self.journal, base)
                return SubmitResult('ACTION_REQUIRED', cid, False, True,
                                    'Runtime ACK valid, but saved configuration revision changed; entry fence retained')
            # Owner STOP and legacy safety changes can arrive while systemd
            # restarts the worker or while an acknowledgement is being read.
            # In particular, desired=STOPPED with observed=STOPPED_BY_OWNER
            # is *still* a STOP intent. Previously this rare interleaving
            # could release the broker fence after an apparently good ACK.
            # Never interpret a corrupt state file as permission to resume.
            post_restart_safety = preview_legacy_tracker_migration(self.data_dir)
            if not post_restart_safety.allowed:
                base.update(phase='ACTION_REQUIRED',
                            reason='Post-restart safety gate: ' + post_restart_safety.code)
                _json_write(self.journal, base)
                return SubmitResult('ACTION_REQUIRED', cid, False, True,
                                    'Owner Stop or legacy safety state changed after runtime ACK; entry fence retained')
            if not release_fence(cid):
                base.update(phase='ACTION_REQUIRED', reason='Entry barrier release unverified')
                _json_write(self.journal, base)
                return SubmitResult('ACTION_REQUIRED', cid, False, True,
                                    'Runtime ACK valid but barrier release unverified')
            if self.require_external_fence:
                released = read_private_json(self.barrier, missing_ok=False)
                if released.get('active') is not False or released.get('change_id') != cid:
                    base.update(phase='ACTION_REQUIRED', reason='Shared barrier release readback failed')
                    _json_write(self.journal, base)
                    return SubmitResult('ACTION_REQUIRED', cid, False, True,
                                        'Barrier release not durably verified')
            else:
                _json_write(self.barrier, {'active': False, 'change_id': cid, 'released_at': time.time()})
            base.update(phase='VERIFIED', reason='Runtime ACK, systemd identity and entry fence verified')
            _json_write(self.journal, base)
            return SubmitResult('VERIFIED', cid, True, True, 'New running configuration verified')

    def recover_late_ack(
        self, *,
        ack_probe: Callable[[], tuple[Optional[Mapping[str, Any]], Mapping[str, Any]]],
        starttime_reader: Callable[[int], Optional[int]],
        release_fence: Callable[[str], bool],
    ) -> SubmitResult:
        """Review-only recovery from an already issued restart; NEVER issue one.

        Must only be invoked with a real runtime identity/ACK probe. The
        existing durable restart-intent record is the source of truth; caller
        cannot substitute new expected values to obtain a green result.
        """
        if not self.feature_enabled:
            raise ChangeBlocked('Tracker V2 is disabled by default')
        with self._exclusive():
            tx = read_private_json(self.journal, missing_ok=False)
            if tx.get('phase') not in ('RESTART_ISSUED', 'VERIFYING', 'ACK_UNCERTAIN'):
                raise ChangeBlocked('No uncertain restart is eligible for late ACK recovery')
            if tx.get('restart_issued') is not True:
                raise ChangeBlocked('No durable restart initiation evidence')
            if _sha(_read_env(self.env_path)) != tx.get('requested_file_revision'):
                raise ChangeBlocked('Current environment differs from requested revision; manual recovery')
            barrier = read_private_json(self.barrier, missing_ok=False)
            if barrier.get('active') is not True or barrier.get('change_id') != tx.get('change_id'):
                raise ChangeBlocked('Matching active entry barrier missing; refuse verification')
            prior_pid = tx.get('previous_pid')
            prior_start = tx.get('previous_starttime')
            if type(prior_pid) is not int or type(prior_start) is not int:
                raise ChangeBlocked('Original process identity missing')
            if not preview_legacy_tracker_migration(self.data_dir).allowed:
                raise ChangeBlocked('Legacy operation or Owner Stop prevents barrier release')
            try:
                ack, systemd = ack_probe()
                evidence = verify_runtime_ack(
                    ack=ack, systemd=systemd,
                    expected_hash=tx.get('expected_runtime_hash', ''),
                    expected_mode=tx.get('target_mode', ''),
                    expected_credential_fingerprint=tx.get('credential_fingerprint', ''),
                    min_acknowledged_at=float(tx.get('restart_initiated_at') or 0),
                    starttime_reader=starttime_reader,
                    before_pid=prior_pid, before_starttime=prior_start,
                )
            except Exception:
                evidence = SafetyDecision(False, 'ACK_PROBE_UNAVAILABLE', 'Late ACK probe unavailable', {})
            if not evidence.allowed:
                tx.update(phase='ACK_UNCERTAIN', reason=evidence.code)
                _json_write(self.journal, tx)
                return SubmitResult('ACK_UNCERTAIN', tx['change_id'], False, True,
                                    'Late ACK not verified; entry barrier retained')
            # A simultaneous Owner Stop is possible; recheck before release.
            if not preview_legacy_tracker_migration(self.data_dir).allowed:
                tx.update(phase='ACTION_REQUIRED', reason='Owner Stop or legacy safety state changed')
                _json_write(self.journal, tx)
                return SubmitResult('ACTION_REQUIRED', tx['change_id'], False, True,
                                    'Owner Stop has priority; barrier retained')
            # Recheck persisted .env immediately before a late-ACK gate
            # release as well; the initial recovery preflight can race an
            # external writer. The restart is never retried automatically.
            try:
                current_revision_ok = (
                    _sha(_read_env(self.env_path)) == tx['requested_file_revision']
                )
            except (OSError, ValueError, UnicodeError):
                current_revision_ok = False
            if not current_revision_ok:
                tx.update(phase='ACTION_REQUIRED', reason='Saved .env changed before late-ACK barrier release')
                _json_write(self.journal, tx)
                return SubmitResult('ACTION_REQUIRED', tx['change_id'], False, True,
                                    'Late ACK cannot release barrier; disk revision changed')
            if not release_fence(tx['change_id']):
                tx.update(phase='ACTION_REQUIRED', reason='Entry barrier release unverified')
                _json_write(self.journal, tx)
                return SubmitResult('ACTION_REQUIRED', tx['change_id'], False, True,
                                    'Late ACK valid, but barrier release unverified')
            if self.require_external_fence:
                released = read_private_json(self.barrier, missing_ok=False)
                if released.get('active') is not False or released.get('change_id') != tx['change_id']:
                    tx.update(phase='ACTION_REQUIRED', reason='Shared barrier release readback failed')
                    _json_write(self.journal, tx)
                    return SubmitResult('ACTION_REQUIRED', tx['change_id'], False, True,
                                        'Barrier release not durably verified')
            else:
                _json_write(self.barrier, {'active':False, 'change_id':tx['change_id'], 'released_at':time.time()})
            tx.update(phase='VERIFIED', reason='Late ACK matched actual process; no second restart')
            _json_write(self.journal, tx)
            return SubmitResult('VERIFIED', tx['change_id'], True, True,
                                'Late ACK verified; no additional restart')
