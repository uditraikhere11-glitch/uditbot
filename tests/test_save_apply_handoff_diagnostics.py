"""Focused offline, fail-closed tests for broker mutation handoff in Save & Apply.

Uses no Dhan, Telegram, systemd, network or real bot data.  A blocked
preflight must never repair a ledger, clear a barrier or issue a restart.
"""
from __future__ import annotations

from pathlib import Path
import os
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.engine.config_patch import ConfigurationConflict
from src.engine.runtime_evidence import SafetyDecision
from src.engine import simple_save_apply as apply_module
from src.engine.tracker_credentials import (
    CredentialEvidenceError, fingerprint, fingerprint_existing,
)


class HandoffDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data = Path(tmp.name)
        self.env = self.data / '.env'
        self.env.write_text('TRADING_MODE=LIVE\nDHAN_CLIENT_ID=FAKE\n')
        self.cfg = SimpleNamespace(
            data_dir=str(self.data),
            trading_mode=SimpleNamespace(value='LIVE'),
            dhan_client_id='FAKE',
        )
        self.pid = 555
        self.stamp = 333
        self.ack = {'pid': self.pid, 'process_starttime': self.stamp,
                    'credential_fingerprint': 'a' * 64}
        self.gate = SafetyDecision(True, 'NO_ACTIVE_BROKER_OPERATION', 'verified',
                                   {'producer_pid': self.pid, 'producer_starttime': self.stamp})
        self.ledger = {'schema_version': 2, 'account_id': 'FAKE',
                       'producer_pid': self.pid, 'producer_starttime': self.stamp,
                       'handoff_verified': True, 'active': [], 'unresolved': []}
        self.barrier = {'active': False}
        self.tracker = Mock()
        self.tracker.is_active_version_confirmed.return_value = (True, self.ack, 'Verified')
        self.patches = [
            patch.object(apply_module.AppConfig, 'from_env', return_value=self.cfg),
            patch.object(apply_module, 'ConfigChangeTracker', return_value=self.tracker),
            patch.object(apply_module, 'inspect_systemd_service_properties', return_value={
                'ActiveState': 'active', 'MainPID': self.pid}),
            patch.object(apply_module, 'compute_config_version', return_value='b'*64),
            patch('src.engine.tracker_credentials.fingerprint_existing', return_value='a'*64),
            patch.object(apply_module, 'evaluate_active_broker_operation_gate',
                         side_effect=lambda *a, **k: self.gate),
            patch.object(apply_module, 'read_private_json', side_effect=self.read_record),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def read_record(self, p, **kwargs):
        name = Path(p).name
        if name == 'active_broker_mutations.json':
            return self.ledger
        if name == 'apply_barrier.json':
            return self.barrier
        raise AssertionError('Unexpected read')

    def prove(self):
        return apply_module._positive_running_restart_proof(self.env, {'DHAN_CLIENT_ID':'FAKE'})

    def reject(self, code):
        with self.assertRaises(ConfigurationConflict) as result:
            self.prove()
        self.assertIn(code, str(result.exception))

    def test_verified_idle_handoff_allows_existing_proof(self):
        result = self.prove()
        self.assertEqual((result['pid'], result['starttime']), (self.pid, self.stamp))
        self.assertEqual(result['account'], 'FAKE')

    def test_missing_producer_is_not_an_empty_handoff(self):
        self.gate = SafetyDecision(False, 'NO_RUNTIME_HANDOFF', 'missing', {})
        self.reject('BROKER_HANDOFF_NO_RUNTIME_HANDOFF')

    def test_active_order_blocks(self):
        self.gate = SafetyDecision(False, 'BROKER_MUTATION_ACTIVE', 'busy', {'count': 1})
        self.reject('BROKER_HANDOFF_BROKER_MUTATION_ACTIVE')

    def test_unknown_order_blocks(self):
        self.gate = SafetyDecision(False, 'UNRESOLVED_BROKER_MUTATION', 'unknown', {'count': 1})
        self.reject('BROKER_HANDOFF_UNRESOLVED_BROKER_MUTATION')

    def test_account_mismatch_blocks(self):
        self.ledger['account_id'] = 'WRONG'
        self.reject('BROKER_HANDOFF_ACCOUNT_MISMATCH')

    def test_producer_pid_mismatch_blocks(self):
        self.gate = SafetyDecision(True, 'NO_ACTIVE_BROKER_OPERATION', 'verified',
                                   {'producer_pid': 999, 'producer_starttime': self.stamp})
        self.reject('BROKER_HANDOFF_PRODUCER_PID_MISMATCH')

    def test_producer_starttime_mismatch_blocks(self):
        self.gate = SafetyDecision(True, 'NO_ACTIVE_BROKER_OPERATION', 'verified',
                                   {'producer_pid': self.pid, 'producer_starttime': 444})
        self.reject('BROKER_HANDOFF_STARTTIME_MISMATCH')

    def test_active_prior_barrier_blocks(self):
        self.barrier = {'active': True, 'change_id': 'SAVE-UNKNOWN'}
        self.reject('BROKER_RESTART_BARRIER_UNRESOLVED')

    def test_missing_credential_ack_blocks(self):
        self.ack.pop('credential_fingerprint')
        self.reject('RUNTIME_CREDENTIAL_ACK_MISSING')

    def test_different_credential_ack_blocks(self):
        self.ack['credential_fingerprint'] = 'f' * 64
        self.reject('RUNTIME_CREDENTIAL_MISMATCH')

    def test_unreadable_ledger_blocks(self):
        self.ledger = None
        # A missing ledger cannot be considered a valid handoff.
        self.gate = SafetyDecision(False, 'NO_RUNTIME_HANDOFF', 'missing', {})
        self.reject('BROKER_HANDOFF_NO_RUNTIME_HANDOFF')

    def test_broken_credential_key_blocks_without_create(self):
        with patch('src.engine.tracker_credentials.fingerprint_existing', side_effect=CredentialEvidenceError('missing')):
            self.reject('CREDENTIAL_EVIDENCE_UNAVAILABLE')

    def test_corrupt_barrier_refuses_save(self):
        self.barrier = {'active': 'invalid'}
        self.reject('BROKER_RESTART_BARRIER_UNRESOLVED')


class CredentialReadOnlyTests(unittest.TestCase):
    def test_unknown_key_does_not_get_created_by_preflight(self):
        with tempfile.TemporaryDirectory() as tmp:
            key = Path(tmp) / '.tracker_credential_hmac_key'
            with self.assertRaises(CredentialEvidenceError):
                fingerprint_existing({'DHAN_PIN': '1234'}, tmp)
            self.assertFalse(key.exists())

    def test_worker_created_key_can_be_rechecked_without_rekeying(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = {'DHAN_PIN': '1234'}
            produced = fingerprint(src, tmp)
            key = Path(tmp) / '.tracker_credential_hmac_key'
            before = key.read_bytes()
            self.assertEqual(fingerprint_existing(src, tmp), produced)
            self.assertNotEqual(fingerprint_existing({'DHAN_PIN': '8888'}, tmp), produced)
            self.assertEqual(key.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()