"""PR84 remaining acceptance matrix: synthetic, no network/systemd/broker activity.

These tests deliberately assert NEGATIVE effects for a financial trading bot:
no unexpected restart, broker GET, queue mutation or runtime verification.
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, REGISTRY, ENV_REGISTRY,
    parse_env_strict, patch_env, revision, validate_effective_config,
)
from src.engine.config_manager import (
    ConfigApplyState, ConfigChangeRecord, ConfigChangeTracker, OperationState,
    ServiceOperatingState, write_env_atomic, write_json_atomic,
)
from src.engine.webui_update import submit_webui_patch, recover_field_transactions

SYNTHETIC = (
    '# Synthetic data: no Dhan or Telegram credentials\n'
    'TRADING_MODE=LIVE\n'
    'EQUITY_ACTIVE=true\n'
    'FNO_ACTIVE=true\n'
    'DHAN_RESPONSE_GROUP_NAME=-100551111111\n'
    'DHAN_CLIENT_ID=FAKE_ID\n'
    'DHAN_PIN=FAKE_PIN\n'
    'DHAN_TOTP_SECRET=FAKE_TOTP\n'
    'TG_API_HASH=FAKE_HASH\n'
    'EQUITY_BUDGET_INR=500\n'
    'CUSTOM_PRESERVE=as-is\n'
)


class WebUIFullMatrix(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.env = self.root / '.env'
        self.env.write_text(SYNTHETIC, encoding='utf-8')
        self.tracker = ConfigChangeTracker(data_dir=str(self.root / 'data'))

    def request(self, values, *, service=ServiceOperatingState.RUNNING,
                request_id='REGRESSION-UNIQUE-0001', spawn_worker=False):
        with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                          return_value=(service, 'synthetic')), \
             patch.object(ConfigChangeTracker, 'get_readonly_broker',
                          side_effect=AssertionError('Unexpected Dhan client')), \
             patch.object(ConfigChangeTracker, 'start_coordinator_daemon',
                          side_effect=AssertionError('Unexpected coordinator launch')):
            return submit_webui_patch(
                self.tracker, values, str(self.env),
                request_id=request_id,
                expected_revision=revision(self.env.read_text(encoding='utf-8')),
                spawn_worker=spawn_worker,
            )

    def test_ac03_invalid_input_for_every_registered_field_is_rejected_without_disk_mutation(self):
        invalid = {
            'mode': 'BUY_LIVE_NOW', 'bool': 'perhaps', 'enum': 'INVALID_ENUM',
            'integer': '1.5', 'number': 'NaN', 'groups': 'not,a,group',
            'response': 'spoof\nMORE=evil', 'secret': 'spoof\nMORE=evil',
            'text': 'spoof\nMORE=evil',
        }
        self.assertEqual(len(REGISTRY), 51)
        for field, spec in REGISTRY.items():
            with self.subTest(field=field):
                before = self.env.read_bytes()
                with self.assertRaises(ConfigurationRejected):
                    patch_env(before.decode(), {field: invalid[spec.value_type]})
                self.assertEqual(self.env.read_bytes(), before)

    def test_ac03_duplicate_alias_and_noncanonical_keys_rejected(self):
        with self.assertRaises(ConfigurationRejected):
            patch_env(SYNTHETIC, {'max_daily_trades': 4, 'daily_trade_limit': 7})
        modified = SYNTHETIC.replace('EQUITY_BUDGET_INR', 'equity_budget_inr')
        with self.assertRaises(ConfigurationConflict):
            parse_env_strict(modified)
        with self.assertRaises(ConfigurationConflict):
            parse_env_strict(SYNTHETIC + 'DHAN_PIN=OVERRIDE\n')
        modified, changes = patch_env(SYNTHETIC, {'daily_trade_limit': 3})
        self.assertEqual(set(changes), {'MAX_DAILY_TRADES'})
        self.assertIn('MAX_DAILY_TRADES=3\n', modified)

    def test_ac04_nontrading_matrix_all_lifecycle_states_no_restart(self):
        states = [
            ServiceOperatingState.RUNNING, ServiceOperatingState.STOPPED_BY_OWNER,
            ServiceOperatingState.STOPPED_OTHER, ServiceOperatingState.FAILED,
            ServiceOperatingState.STARTING, ServiceOperatingState.STOPPING,
            ServiceOperatingState.UNKNOWN,
        ]
        for i, state in enumerate(states):
            with self.subTest(state=state):
                # Separate tracker on each attempt; no cross-test state leakage.
                root = self.root / ('case' + str(i))
                root.mkdir()
                fn = root / '.env'
                fn.write_text(SYNTHETIC)
                tr = ConfigChangeTracker(data_dir=str(root / 'data'))
                before = fn.read_bytes()
                with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                                  return_value=(state, 'synthetic')), \
                     patch.object(ConfigChangeTracker, 'get_readonly_broker',
                                  side_effect=AssertionError('Dhan must not be called')), \
                     patch.object(ConfigChangeTracker, 'start_coordinator_daemon',
                                  side_effect=AssertionError('Coordinator must not start')):
                    if state in {ServiceOperatingState.UNKNOWN, ServiceOperatingState.STARTING,
                                 ServiceOperatingState.STOPPING}:
                        with self.assertRaises(ConfigurationConflict):
                            submit_webui_patch(tr, {'dhan_response_group_name': '-5514889030'}, str(fn),
                                               expected_revision=revision(before.decode()),
                                               request_id='LIFECYCLE-' + str(i) + '-00000', spawn_worker=True)
                        self.assertEqual(fn.read_bytes(), before)
                    else:
                        rec = submit_webui_patch(tr, {'dhan_response_group_name': '-5514889030'}, str(fn),
                                                 expected_revision=revision(before.decode()),
                                                 request_id='LIFECYCLE-' + str(i) + '-00000', spawn_worker=True)
                        self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
                        self.assertFalse(rec.runtime_verified)
                        self.assertFalse(rec.restart_performed)

    def test_ac04_ac06_stopped_live_request_requires_confirmation_but_never_broker(self):
        before = SYNTHETIC.replace('TRADING_MODE=LIVE', 'TRADING_MODE=TEST')
        self.env.write_text(before)
        with self.assertRaises(ConfigurationRejected):
            self.request({'is_live_mode': True}, service=ServiceOperatingState.STOPPED_BY_OWNER)
        self.assertEqual(self.env.read_text(), before)
        record = self.request({'is_live_mode': True, 'confirm_live': 'true'},
                              service=ServiceOperatingState.STOPPED_BY_OWNER)
        self.assertEqual(record.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertFalse(record.broker_checks_required)
        self.assertFalse(record.restart_performed)
        self.assertFalse(record.runtime_verified)
        self.assertIn('TRADING_MODE=LIVE', self.env.read_text())

    def test_ac05_ac06_running_safety_transition_queues_not_bypasses_broker_gate(self):
        def fake_enqueue(**kwargs):
            self.assertIn('TRADING_MODE=TEST', kwargs['env_content'])
            change = ConfigChangeRecord(change_id='CFG-SYNTHETIC-GATE-01',
                                        state=ConfigApplyState.REQUEST_RECEIVED,
                                        operation_state=OperationState.REQUESTED.value)
            self.tracker.save_change_record(change)
            return change
        with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                          return_value=(ServiceOperatingState.RUNNING, 'synthetic')), \
             patch.object(ConfigChangeTracker, 'enqueue_durable_configuration_update',
                          side_effect=fake_enqueue) as enqueue, \
             patch.object(ConfigChangeTracker, 'get_readonly_broker',
                          side_effect=AssertionError('Actual broker call occurs in protected coordinator')):
            record = submit_webui_patch(self.tracker, {'is_live_mode': False}, str(self.env),
                                        request_id='SAFE-GATE-REQUEST-0001',
                                        expected_revision=revision(self.env.read_text()),
                                        spawn_worker=False)
        enqueue.assert_called_once()
        self.assertEqual(record.application_method, 'LEGACY_SAFETY_COORDINATOR')
        self.assertTrue(record.broker_checks_required)
        self.assertEqual(record.state, ConfigApplyState.REQUEST_RECEIVED)
        self.assertEqual(self.env.read_text(), SYNTHETIC)

    def test_ac05_notification_per_broker_scenario_is_order_neutral(self):
        broker_states = (
            'no_orders', 'pending_buy', 'pending_sell', 'partial_fill',
            'protected_fill', 'unprotected_fill', 'manual_risk_yesbank',
            'unknown_cancel', 'unknown_oco', 'broker_503', 'http_429',
            'account_mismatch',
        )
        for index, scenario in enumerate(broker_states):
            with self.subTest(scenario=scenario):
                path = self.root / (scenario + '.env')
                path.write_text(SYNTHETIC)
                tr = ConfigChangeTracker(data_dir=str(self.root / scenario))
                fake_broker = MagicMock()
                fake_broker.get_order_book.side_effect = AssertionError('No broker read')
                fake_broker.get_positions.side_effect = AssertionError('No positions read')
                with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                                  return_value=(ServiceOperatingState.RUNNING, scenario)):
                    rec = submit_webui_patch(tr, {'dhan_response_group_name': '-5514889030'},
                                             str(path), request_id='BROKER-' + str(index) + '-SIMULATED',
                                             expected_revision=revision(SYNTHETIC), broker=fake_broker,
                                             spawn_worker=False)
                self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
                fake_broker.get_order_book.assert_not_called()
                fake_broker.get_positions.assert_not_called()

    def test_ac07_ac18_multiple_ordinary_categories_have_zero_restart_and_truthful_status(self):
        for i, (field, val) in enumerate((
            ('dhan_response_group_name', '-5514889030'),
            ('equity_budget_inr', 750),
            ('operator_name', 'OFFLINE_OPERATOR'),
        )):
            root = self.root / ('class' + str(i))
            root.mkdir()
            fn = root / '.env'
            fn.write_text(SYNTHETIC)
            tr = ConfigChangeTracker(data_dir=str(root / 'data'))
            with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                              return_value=(ServiceOperatingState.RUNNING, 'synthetic')):
                rec = submit_webui_patch(tr, {field: val}, str(fn),
                                         expected_revision=revision(SYNTHETIC),
                                         request_id='CLASSES-' + str(i) + '-SIM',
                                         spawn_worker=False)
            self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
            self.assertFalse(rec.runtime_verified)
            self.assertFalse(rec.restart_performed)
            self.assertFalse(rec.broker_checks_required)

    def test_ac08_unresolved_crash_blocks_later_update(self):
        record = self.request({'dhan_response_group_name': '-5514889030'})
        transaction = Path(self.tracker.data_dir) / 'config_field_transactions' / (record.change_id + '.json')
        status = json.loads(transaction.read_text())
        status['status'] = 'PREPARED'
        status['target_sha256'] = '0'*64
        status['previous_sha256'] = 'f'*64
        write_json_atomic(str(transaction), status)
        original = self.env.read_bytes()
        with self.assertRaises(ConfigurationConflict):
            self.request({'operator_name': 'NEXT_CHANGE'}, request_id='CRASH-CONFLICT-0002')
        self.assertEqual(self.env.read_bytes(), original)

    def test_ac10_same_id_changed_payload_rejected(self):
        rec = self.request({'dhan_response_group_name': '-5514889030'})
        with self.assertRaises(ConfigurationConflict):
            self.request({'operator_name': 'MALICIOUS_CHANGE'})
        self.assertEqual(rec.change_id, self.tracker.get_change_record().change_id)

    def test_ac14_secret_preservation_and_private_transaction_modes(self):
        rec = self.request({'dhan_response_group_name': '-5514889030'})
        summary = json.dumps(rec.to_dict(), sort_keys=True)
        for secret in ('FAKE_PIN', 'FAKE_HASH', 'FAKE_TOTP'):
            self.assertNotIn(secret, summary)
            self.assertIn(secret, self.env.read_text())
        for p in (Path(self.tracker.data_dir) / 'config_field_transactions').glob('*.json'):
            self.assertEqual(p.stat().st_mode & 0o077, 0)
            self.assertNotIn('FAKE_PIN', p.read_text())
        for p in (Path(self.tracker.data_dir) / 'config_field_backups').glob('*.env'):
            self.assertEqual(p.stat().st_mode & 0o077, 0)
        from src.setup_app import HTML_TEMPLATE
        self.assertIn('changesList.replaceChildren()', HTML_TEMPLATE)
        self.assertIn('row.textContent', HTML_TEMPLATE)

    def test_ac17_legacy_sqlite_not_touched_by_notification_save(self):
        db = Path(self.tracker.data_dir) / 'trade_journal.db'
        con = __import__('sqlite3').connect(db)
        con.execute('CREATE TABLE historical_fills (order_id TEXT PRIMARY KEY, state TEXT)')
        con.executemany('INSERT INTO historical_fills VALUES (?, ?)', [
            ('221261102657708', 'TRADED'), ('23126110439808', 'TRADED')])
        con.commit(); con.close()
        before = db.read_bytes()
        self.request({'dhan_response_group_name': '-5514889030'})
        self.assertEqual(db.read_bytes(), before)


class ControlCenterFnoAmoTransactionAudit(unittest.TestCase):
    def test_fno_disable_failed_or_accepted_enqueues_cannot_mutate_old_local_queue(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from src.control_center_backend import register_control_center_routes
        from src.engine.fno_amo_queue import FnoAmoQueue
        with tempfile.TemporaryDirectory() as root:
            env = os.path.join(root, '.env')
            Path(env).write_text('TRADING_MODE=TEST\nFNO_AMO_ENABLED=true\nDHAN_PIN=FAKE_PIN\n')
            app = FastAPI()
            register_control_center_routes(app, 'SYNTHETIC_CSRF')
            t = MagicMock()
            t.pending_op_file = os.path.join(root, 'pending.json')
            t.deferred_env_file = os.path.join(root, 'deferred.env')
            t.get_change_record.return_value = None
            t.get_service_control_record.return_value = {}
            t.inspect_transition_gate_status.return_value = {'ok': True, 'active': False}
            t.control_service.return_value = {'service_state': 'STOPPED_BY_OWNER'}
            t.get_readonly_broker.return_value = None
            t.enqueue_durable_configuration_update.return_value = ConfigChangeRecord(
                change_id='CFG-FNO-0001', state=ConfigApplyState.REQUEST_RECEIVED)
            cfg = SimpleNamespace(dhan_client_id='FAKE_ID', fno_amo_enabled=True,
                                  equity_amo_enabled=False, data_dir=root)
            with patch('src.control_center_backend._config_and_paths',return_value=(cfg,env,root)), \
                 patch('src.control_center_backend.ConfigChangeTracker',return_value=t), \
                 patch.object(FnoAmoQueue,'cancel_all_queued',
                              side_effect=AssertionError('Queue cancellation must require explicit user action')) as cancelled:
                client = TestClient(app)
                payload={'enabled':False, 'csrf_token':'SYNTHETIC_CSRF'}
                t.enqueue_durable_configuration_update.side_effect = RuntimeError('Injected enqueue failure')
                failed = client.post('/control/amo/fno', json=payload)
                self.assertEqual(failed.status_code, 500)
                cancelled.assert_not_called()
                t.enqueue_durable_configuration_update.side_effect = None
                accepted = client.post('/control/amo/fno', json=payload)
                self.assertEqual(accepted.status_code, 202, accepted.text)
                cancelled.assert_not_called()
                self.assertIn('FNO_AMO_ENABLED=true', Path(env).read_text())
                # Actual Control Center clients transmit the revision they
                # received from GET /control/status. Stale submissions must be
                # rejected before any mutation, for both AMO segments.
                t.enqueue_durable_configuration_update.reset_mock()
                for endpoint in ('/control/amo/equity', '/control/amo/fno'):
                    stale = client.post(endpoint, json={
                        'enabled': True, 'csrf_token': 'SYNTHETIC_CSRF',
                        'expected_revision': '0' * 64,
                    })
                    self.assertEqual(stale.status_code, 409)
                t.enqueue_durable_configuration_update.assert_not_called()


if __name__ == '__main__':
    unittest.main()
