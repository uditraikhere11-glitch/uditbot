"""Offline acceptance coverage for the disabled-by-default V2 candidate.

No real Dhan, Telegram, systemctl, network or trading functions are imported.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from src.engine.tracker_v2_candidate import CandidateCoordinator, ChangeBlocked
from src.engine.tracker_v2_safety import (
    SafetyDecision, read_private_json, proc_starttime, verify_runtime_ack,
    evaluate_active_broker_operation_gate, inspect_apply_barrier,
    inspect_transaction_recovery, preview_legacy_tracker_migration,
)

HASH = 'a'*64
FP = 'b'*64
PID = 40001
START = 701
OLD = b'TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\n'
NEW = b'TRADING_MODE=TEST\nEQUITY_BUDGET_INR=1000\n'

def digest(b):
    return hashlib.sha256(b).hexdigest()

class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root/'data'
        self.data.mkdir(mode=0o700)
        self.env = self.root/'.env'
        self.env.write_bytes(OLD)
        self.co = CandidateCoordinator(self.data, self.env, feature_enabled=True)
        self.calls = []

    def put(self, name, obj):
        path = self.data/name
        # Force private fixture directories regardless of operator umask
        # (e.g. 0002 on a Debian VM). Never relax production's 0700 check.
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_text(json.dumps(obj), encoding='utf-8')
        path.chmod(0o600)
        return path

    def nochange(self, **kw):
        args = dict(request_id='request-0001', expected_file_revision=digest(OLD),
                    new_env=OLD, previous_mode='TEST', target_mode='TEST',
                    service_running=False)
        args.update(kw)
        return self.co.submit(**args)

    def changed_stopped(self, **kw):
        args = dict(request_id='request-0001', expected_file_revision=digest(OLD),
                    new_env=NEW, previous_mode='TEST', target_mode='TEST',
                    service_running=False)
        args.update(kw)
        return self.co.submit(**args)

    def running(self, **kw):
        # Positive handoff is supplied only by a *test fake*. Production must
        # implement actual process/handoff proof before this module is enabled.
        gate = SafetyDecision(True, 'NO_ACTIVE_BROKER_OPERATION', 'fake proof',
                              {'producer_pid':PID,'producer_starttime':START})
        args = dict(request_id='request-0001', expected_file_revision=digest(OLD),
                    new_env=NEW, previous_mode='TEST', target_mode='TEST',
                    service_running=True, pre_runtime_verified=True,
                    before_pid=PID, before_starttime=START,
                    operation_gate=lambda:gate,
                    entry_fence=lambda c:self.calls.append('fence') or True,
                    restart_callback=lambda:self.calls.append('restart') or True,
                    ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                                       'version_hash':HASH, 'trading_mode':'TEST',
                                       'credential_fingerprint':FP, 'acknowledged_at':time.time()+10},
                                      {'ActiveState':'active','MainPID':PID+1,'Result':'success'}),
                    expected_runtime_hash=HASH,credential_fingerprint=FP,
                    starttime_reader=lambda pid:START if pid==PID else START+1,
                    release_fence=lambda c:self.calls.append('release') or True)
        args.update(kw)
        return self.co.submit(**args)

    def test_flag_off_by_default(self):
        c = CandidateCoordinator(self.data, self.env)
        with self.assertRaisesRegex(ChangeBlocked,'disabled'):
            c.submit(request_id='request-0001',expected_file_revision=digest(OLD),
                     new_env=NEW,previous_mode='TEST',target_mode='TEST',service_running=False)
        self.assertEqual(self.env.read_bytes(),OLD)

    def test_noop_never_verifies_or_restarts(self):
        r=self.nochange()
        self.assertEqual(r.phase,'NO_CHANGE')
        self.assertFalse(r.runtime_verified)
        self.assertFalse(r.restart_issued)
        self.assertFalse(self.co.journal.exists())

    def test_noop_running_cannot_bypass_missing_runtime(self):
        with self.assertRaisesRegex(ChangeBlocked,'not verified'):
            self.nochange(service_running=True)

    def test_stopped_save_remains_stopped_and_private(self):
        r=self.changed_stopped()
        self.assertEqual(r.phase,'SAVED_FOR_NEXT_START')
        self.assertFalse(r.runtime_verified)
        self.assertFalse(r.restart_issued)
        self.assertEqual(self.env.read_bytes(),NEW)
        self.assertEqual(os.stat(self.env).st_mode & 0o777,0o600)
        self.assertEqual(os.stat(self.co.txdir/(r.change_id+'.env.bak')).st_mode & 0o777,0o600)
        self.assertEqual(inspect_transaction_recovery(self.data,self.env).code,'SAVED_NOT_RUNNING')

    def test_duplicate_request_never_restarts_twice(self):
        first=self.running()
        self.assertEqual(first.phase,'VERIFIED')
        n=self.calls.count('restart')
        replay=self.running()
        self.assertTrue(replay.runtime_verified)
        self.assertEqual(self.calls.count('restart'),n)

    def test_new_request_after_completed_tx_allowed(self):
        self.changed_stopped()
        result=self.co.submit(request_id='request-0002',expected_file_revision=digest(NEW),
                              new_env=NEW+b'EXTRA=1\n',previous_mode='TEST',target_mode='TEST',
                              service_running=False)
        self.assertEqual(result.phase,'SAVED_FOR_NEXT_START')

    def test_request_id_reuse_different_target_rejected(self):
        self.changed_stopped()
        with self.assertRaisesRegex(ChangeBlocked,'reused'):
            self.changed_stopped(new_env=NEW+b'EXTRA=2\n')

    def test_wrong_revision_rejected_without_write(self):
        with self.assertRaisesRegex(ChangeBlocked,'CAS mismatch'):
            self.changed_stopped(expected_file_revision='0'*64)
        self.assertEqual(self.env.read_bytes(),OLD)

    def test_live_switch_without_confirmation_rejected(self):
        with self.assertRaisesRegex(ChangeBlocked,'confirmation'):
            self.changed_stopped(new_env=b'TRADING_MODE=LIVE\n',target_mode='LIVE')

    def test_live_switch_with_confirmation_still_stopped(self):
        r=self.changed_stopped(new_env=b'TRADING_MODE=LIVE\n',target_mode='LIVE',confirm_live=True)
        self.assertEqual(r.phase,'SAVED_FOR_NEXT_START')
        self.assertFalse(r.runtime_verified)

    def test_pending_legacy_operation_preserved(self):
        p=self.put('pending_config_operation.json',{'state':'IN_PROGRESS','change_id':'abc'})
        old=p.read_bytes()
        with self.assertRaisesRegex(ChangeBlocked,'PENDING_LEGACY_OPERATION'):
            self.changed_stopped()
        self.assertEqual(p.read_bytes(),old)
        self.assertEqual(self.env.read_bytes(),OLD)

    def test_queued_pending_legacy_operation_preserved(self):
        self.put('pending_config_operation.json',{'state':'QUEUED'})
        self.assertFalse(preview_legacy_tracker_migration(self.data).allowed)
        with self.assertRaises(ChangeBlocked):self.changed_stopped()

    def test_invalid_pending_file_fails_closed(self):
        p=self.data/'pending_config_operation.json'
        p.write_text('{broken')
        with self.assertRaises(ChangeBlocked): self.changed_stopped()

    def test_owner_stop_deferred_stays(self):
        p=self.put('service_control_state.json',{'deferred_stop_pending':True,
                    'desired_service_state':'STOPPED','observed_state':'RUNNING'})
        before=p.read_bytes()
        with self.assertRaises(ChangeBlocked):self.changed_stopped()
        self.assertEqual(p.read_bytes(),before)

    def test_owner_stop_desired_stopped_observed_running(self):
        self.put('service_control_state.json',{'deferred_stop_pending':False,
                    'desired_service_state':'STOPPED','observed_state':'RUNNING'})
        self.assertEqual(preview_legacy_tracker_migration(self.data).code,'OWNER_STOP_PRIORITY')

    def test_gemini_b07_previous_pending_tracker_apply_blocks_and_preserves(self):
        path = self.put('pending_tracker_apply.json', {
            'change_id': 'LEGACY-B07', 'state': 'WAITING_FOR_SAFE_APPLY',
            'target_version': HASH,
        })
        before = path.read_bytes()
        self.assertEqual(
            preview_legacy_tracker_migration(self.data).code,
            'PENDING_REPLACEMENT_OPERATION',
        )
        with self.assertRaises(ChangeBlocked):
            self.changed_stopped()
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.env.read_bytes(), OLD)

    def test_gemini_b07_corrupt_pending_tracker_apply_fails_closed(self):
        path = self.data/'pending_tracker_apply.json'
        path.write_text('{invalid', encoding='utf-8')
        path.chmod(0o600)
        decision = preview_legacy_tracker_migration(self.data)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.code, 'LEGACY_STATE_UNREADABLE')
        self.assertEqual(path.read_text(), '{invalid')
        self.assertEqual(self.env.read_bytes(), OLD)

    def test_gemini_b03_disk_revision_changes_after_ack_no_fence_release(self):
        # Simulated external writer between correct runtime ACK and fence
        # release: fail closed without inventing applied verification.
        def changed_disk_then_ack():
            self.env.write_bytes(b'TRADING_MODE=TEST\nEQUITY_BUDGET_INR=9999\n')
            return (
                {'pid': PID+1, 'process_starttime': START+1,
                 'version_hash': HASH, 'trading_mode': 'TEST',
                 'credential_fingerprint': FP, 'acknowledged_at': time.time()+10},
                {'ActiveState': 'active', 'MainPID': PID+1, 'Result': 'success'},
            )
        result = self.running(ack_probe=changed_disk_then_ack)
        self.assertEqual(result.phase, 'ACTION_REQUIRED')
        self.assertFalse(result.runtime_verified)
        self.assertTrue(result.restart_issued)
        self.assertEqual(self.calls.count('restart'), 1)
        self.assertEqual(self.calls.count('release'), 0)
        journal = read_private_json(self.co.journal, missing_ok=False)
        self.assertEqual(journal['phase'], 'ACTION_REQUIRED')
        with self.assertRaises(ChangeBlocked):
            self.co.recover_late_ack(
                ack_probe=changed_disk_then_ack,
                starttime_reader=lambda pid: START+1,
                release_fence=lambda cid: self.calls.append('release') or True,
            )
        self.assertEqual(self.calls.count('release'), 0)

    def test_transition_gate_active_never_deleted(self):
        p=self.put('mode_transition_gate.json',{'active':True,'created_at':1})
        with self.assertRaises(ChangeBlocked):self.changed_stopped()
        self.assertTrue(p.exists())

    def test_migration_preview_is_idempotent_and_readonly(self):
        p=self.put('service_control_state.json',{'deferred_stop_pending':False})
        before=p.read_bytes()
        a=preview_legacy_tracker_migration(self.data)
        b=preview_legacy_tracker_migration(self.data)
        self.assertEqual(a,b)
        self.assertEqual(p.read_bytes(),before)

    def test_emergency_stop_active_blocks(self):
        self.put('emergency_stop.json',{'is_stopped':True,'in_flight_orders':[]})
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'EMERGENCY_STOP_ACTIVE')

    def test_inflight_orders_blocks(self):
        self.put('emergency_stop.json',{'is_stopped':False,'in_flight_orders':[{'id':'x'}]})
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'EMERGENCY_IN_FLIGHT')

    def test_malformed_emergency_stop_fails_closed(self):
        (self.data/'emergency_stop.json').write_text('{invalid')
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'SAFETY_RECORD_UNREADABLE')

    def test_symlink_emergency_stop_fails_closed(self):
        p=self.data/'emergency_stop.json';p.symlink_to(self.env)
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'SAFETY_RECORD_UNREADABLE')

    def test_pre_handoff_live_process_is_blocked(self):
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'NO_RUNTIME_HANDOFF')

    def test_active_broker_mutation_blocks(self):
        self.put('active_broker_mutations.json',{'schema_version':2,
                'handoff_verified':True,'active':[{'kind':'ORDER'}],
                'producer_pid':PID,'producer_starttime':START})
        self.assertEqual(evaluate_active_broker_operation_gate(self.data).code,'BROKER_MUTATION_ACTIVE')

    def test_mutation_producer_identity_must_match(self):
        gate=SafetyDecision(True,'NO_ACTIVE_BROKER_OPERATION','fake proof',
                            {'producer_pid':123,'producer_starttime':START})
        with self.assertRaisesRegex(ChangeBlocked,'handoff identity'):
            self.running(operation_gate=lambda:gate)
        self.assertEqual(self.env.read_bytes(),OLD)

    def test_restart_timeout_never_rolls_back(self):
        r=self.running(ack_probe=lambda:(None,{'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(r.phase,'ACK_UNCERTAIN')
        self.assertEqual(self.calls.count('restart'),1)
        self.assertEqual(self.calls.count('release'),0)
        self.assertEqual(self.env.read_bytes(),NEW)
        self.assertEqual(inspect_transaction_recovery(self.data,self.env).code,'RESTART_OUTCOME_UNCERTAIN')
        self.assertEqual(self.running().phase,'ACK_UNCERTAIN')
        self.assertEqual(self.calls.count('restart'),1)

    def test_restart_callback_exception_is_uncertain(self):
        def fail():
            self.calls.append('restart');raise RuntimeError('systemctl unknown')
        r=self.running(restart_callback=fail)
        self.assertEqual(r.phase,'ACK_UNCERTAIN')
        self.assertEqual(self.env.read_bytes(),NEW)
        self.assertEqual(self.calls.count('restart'),1)

    def test_wrong_runtime_pid_keeps_fence(self):
        r=self.running(ack_probe=lambda:({'pid':9,'process_starttime':START+1,
                    'version_hash':HASH,'trading_mode':'TEST','credential_fingerprint':FP,
                    'acknowledged_at':time.time()+10},
                    {'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(r.phase,'ACK_UNCERTAIN')
        self.assertEqual(self.calls.count('release'),0)

    def test_wrong_runtime_hash_keeps_fence(self):
        r=self.running(ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                    'version_hash':'f'*64,'trading_mode':'TEST','credential_fingerprint':FP,
                    'acknowledged_at':time.time()+10},
                    {'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(r.phase,'ACK_UNCERTAIN')

    def test_wrong_runtime_mode_keeps_fence(self):
        r=self.running(ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                    'version_hash':HASH,'trading_mode':'LIVE','credential_fingerprint':FP,
                    'acknowledged_at':time.time()+10},
                    {'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(r.phase,'ACK_UNCERTAIN')

    def test_wrong_runtime_credential_fingerprint(self):
        r=self.running(ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                    'version_hash':HASH,'trading_mode':'TEST','credential_fingerprint':'c'*64,
                    'acknowledged_at':time.time()+10},
                    {'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(r.phase,'ACK_UNCERTAIN')

    def test_runtime_ack_rejects_same_process_identity(self):
        ack={'pid':PID,'process_starttime':START,'version_hash':HASH,'trading_mode':'TEST',
             'credential_fingerprint':FP,'acknowledged_at':time.time()+10}
        d=verify_runtime_ack(ack=ack,systemd={'ActiveState':'active','MainPID':PID},
                expected_hash=HASH,expected_mode='TEST',expected_credential_fingerprint=FP,
                min_acknowledged_at=time.time(),starttime_reader=lambda pid:START,
                before_pid=PID,before_starttime=START)
        self.assertEqual(d.code,'RESTART_NOT_OBSERVED')

    def test_barrier_live_owner_not_expired(self):
        self.put('config_restart_v2/apply_barrier.json',{'active':True,
                    'owner_pid':PID,'owner_starttime':START,'created_at':1})
        barrier=self.co.barrier
        self.assertEqual(inspect_apply_barrier(barrier,starttime_reader=lambda p:START,now=time.time()+999999).code,
                         'BARRIER_OWNER_LIVE')

    def test_barrier_dead_owner_manual_recovery(self):
        self.put('config_restart_v2/apply_barrier.json',{'active':True,
                    'owner_pid':PID,'owner_starttime':START,'created_at':1})
        self.assertEqual(inspect_apply_barrier(self.co.barrier,starttime_reader=lambda p:None).code,
                         'BARRIER_OWNER_MISSING')

    def test_barrier_real_separate_process_lifecycle(self):
        child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)'])
        try:
            st=proc_starttime(child.pid)
            if st is None:self.skipTest('/proc starttime not available')
            self.put('config_restart_v2/apply_barrier.json',{'active':True,
                        'owner_pid':child.pid,'owner_starttime':st,'created_at':1})
            self.assertEqual(inspect_apply_barrier(self.co.barrier).code,'BARRIER_OWNER_LIVE')
        finally:
            child.terminate();child.wait(timeout=5)
        self.assertEqual(inspect_apply_barrier(self.co.barrier).code,'BARRIER_OWNER_MISSING')

    def test_corrupt_transaction_journal_blocks(self):
        # Reproduce a malformed JSON journal, NOT an accidentally insecure
        # temporary directory/file caused by the host VM's umask=0002.
        self.co.txdir.mkdir(mode=0o700)
        self.co.journal.write_text('{broken', encoding='utf-8')
        self.co.journal.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'malformed'):
            self.changed_stopped()
        self.assertEqual(self.env.read_bytes(), OLD)

    def test_insecure_transaction_directory_stays_fail_closed(self):
        self.co.txdir.mkdir(mode=0o700)
        self.co.txdir.chmod(0o770)
        try:
            with self.assertRaisesRegex(ChangeBlocked, 'insecure'):
                self.changed_stopped()
            self.assertEqual(self.env.read_bytes(), OLD)
            self.assertFalse(self.co.journal.exists())
        finally:
            self.co.txdir.chmod(0o700)

    def test_external_writer_conflict_is_not_overwritten(self):
        self.changed_stopped()
        self.env.write_bytes(b'TRADING_MODE=TEST\nMANUAL_WRITER=YES\n')
        self.assertEqual(inspect_transaction_recovery(self.data,self.env).code,'EXTERNAL_ENV_WRITER')
        self.assertEqual(self.env.read_bytes(),b'TRADING_MODE=TEST\nMANUAL_WRITER=YES\n')

    def test_owner_stop_appearing_before_restart_prevents_service_call(self):
        def fence(cid):
            self.calls.append('fence')
            self.put('service_control_state.json',{'desired_service_state':'STOPPED',
                                                   'observed_state':'RUNNING',
                                                   'deferred_stop_pending':True})
            return True
        result=self.running(entry_fence=fence)
        self.assertEqual(result.phase,'ACTION_REQUIRED')
        self.assertNotIn('restart',self.calls)
        self.assertEqual(self.env.read_bytes(),NEW)
        self.assertEqual(read_private_json(self.co.barrier)['active'],True)

    def test_owner_stop_priority_after_restart_ack(self):
        def mark_stop():
            self.calls.append('restart')
            self.put('service_control_state.json',{'deferred_stop_pending':True,
                        'desired_service_state':'STOPPED','observed_state':'RUNNING'})
            return True
        r=self.running(restart_callback=mark_stop)
        self.assertEqual(r.phase,'ACTION_REQUIRED')
        self.assertNotIn('release',self.calls)

    def test_owner_stop_after_valid_ack_blocks_for_all_observed_states(self):
        for observed in ('RUNNING', 'STOPPING', 'UNKNOWN', 'STOPPED', 'STOPPED_BY_OWNER', ''):
            with self.subTest(observed=observed), tempfile.TemporaryDirectory() as td:
                data = Path(td) / 'data'
                env = Path(td) / '.env'
                data.mkdir()
                env.write_bytes(OLD)
                co = CandidateCoordinator(data, env, feature_enabled=True)
                calls = []
                def probe():
                    p = data / 'service_control_state.json'
                    p.write_text(json.dumps({
                        'desired_service_state': 'STOPPED', 'observed_state': observed,
                        'deferred_stop_pending': False
                    }), encoding='utf-8')
                    p.chmod(0o600)
                    return ({'pid': PID+1, 'process_starttime': START+1,
                             'version_hash': HASH, 'trading_mode': 'TEST',
                             'credential_fingerprint': FP,
                             'acknowledged_at': time.time()+10},
                            {'ActiveState': 'active', 'MainPID': PID+1, 'Result': 'success'})
                ok = SafetyDecision(True, 'NO_ACTIVE_BROKER_OPERATION', 'test',
                                    {'producer_pid': PID, 'producer_starttime': START})
                result = co.submit(
                    request_id='stopafter-12345', expected_file_revision=digest(OLD),
                    new_env=NEW, previous_mode='TEST', target_mode='TEST',
                    service_running=True, pre_runtime_verified=True,
                    before_pid=PID, before_starttime=START,
                    operation_gate=lambda: ok,
                    entry_fence=lambda c: calls.append('fence') or True,
                    restart_callback=lambda: calls.append('restart') or True,
                    ack_probe=probe, expected_runtime_hash=HASH,
                    credential_fingerprint=FP,
                    starttime_reader=lambda pid: START if pid == PID else START+1,
                    release_fence=lambda c: calls.append('release') or True,
                )
                self.assertEqual(result.phase, 'ACTION_REQUIRED')
                self.assertEqual(calls.count('restart'), 1)
                self.assertEqual(calls.count('release'), 0)
                self.assertTrue(read_private_json(co.barrier)['active'])

    def test_corrupt_owner_stop_record_after_ack_never_releases_fence(self):
        def corrupt_status():
            p = self.data/'service_control_state.json'
            p.write_text('{invalid', encoding='utf-8')
            p.chmod(0o600)
            return ({'pid': PID+1, 'process_starttime': START+1,
                     'version_hash': HASH, 'trading_mode': 'TEST',
                     'credential_fingerprint': FP, 'acknowledged_at': time.time()+10},
                    {'ActiveState': 'active', 'MainPID': PID+1, 'Result': 'success'})
        r = self.running(ack_probe=corrupt_status)
        self.assertEqual(r.phase, 'ACTION_REQUIRED')
        self.assertEqual(self.calls.count('release'), 0)
        self.assertTrue(read_private_json(self.co.barrier)['active'])

    def test_no_prod_imports_or_network_calls(self):
        import src.engine.tracker_v2_candidate as mod
        self.assertNotIn('dhan_client',mod.__dict__)
        self.assertNotIn('requests',mod.__dict__)
        self.assertNotIn('subprocess',mod.__dict__)


    def test_late_ack_recovery_does_not_restart(self):
        result = self.running(ack_probe=lambda:(None,{'ActiveState':'active','MainPID':PID+1}))
        self.assertEqual(result.phase,'ACK_UNCERTAIN')
        self.assertEqual(self.calls.count('restart'),1)
        recovered=self.co.recover_late_ack(
            ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                                'version_hash':HASH,'trading_mode':'TEST',
                                'credential_fingerprint':FP,'acknowledged_at':time.time()+10},
                               {'MainPID':PID+1,'ActiveState':'active','Result':'success'}),
            starttime_reader=lambda p:START+1,
            release_fence=lambda cid:self.calls.append('release') or True)
        self.assertEqual(recovered.phase,'VERIFIED')
        self.assertEqual(self.calls.count('restart'),1)
        self.assertEqual(self.calls.count('release'),1)

    def test_late_ack_wrong_identity_remains_uncertain(self):
        self.running(ack_probe=lambda:(None,{'ActiveState':'active','MainPID':PID+1}))
        recovered=self.co.recover_late_ack(
            ack_probe=lambda:({'pid':PID+1,'process_starttime':START+1,
                                'version_hash':HASH,'trading_mode':'TEST',
                                'credential_fingerprint':FP,'acknowledged_at':time.time()+10},
                               {'MainPID':PID+1,'ActiveState':'active'}),
            starttime_reader=lambda p:0,
            release_fence=lambda cid:self.calls.append('release') or True)
        self.assertEqual(recovered.phase,'ACK_UNCERTAIN')
        self.assertNotIn('release',self.calls)

    def test_late_ack_owner_stop_blocks_release(self):
        self.running(ack_probe=lambda:(None,{'ActiveState':'active','MainPID':PID+1}))
        self.put('service_control_state.json',{'deferred_stop_pending':True})
        with self.assertRaisesRegex(ChangeBlocked,'Owner Stop'):
            self.co.recover_late_ack(ack_probe=lambda:(None,{}),starttime_reader=lambda p:0,
                                     release_fence=lambda cid:True)
        self.assertEqual(self.calls.count('restart'),1)

    def test_late_ack_external_env_edit_blocks(self):
        self.running(ack_probe=lambda:(None,{'ActiveState':'active','MainPID':PID+1}))
        self.env.write_bytes(b'MANUAL=EDIT\n')
        with self.assertRaisesRegex(ChangeBlocked,'differs'):
            self.co.recover_late_ack(ack_probe=lambda:(None,{}),starttime_reader=lambda p:0,
                                     release_fence=lambda cid:True)

if __name__=='__main__':
    unittest.main()
