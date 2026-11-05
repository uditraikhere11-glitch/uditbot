"""Step 2 offline safety tests; all systemd/network operations are mocked."""
from __future__ import annotations
import contextlib
import os
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.control_center_backend import (
    _single_key_change, build_amo_orders, build_control_status,
    register_control_center_routes,
)


class ChangeTextTests(unittest.TestCase):
    def test_patches_single_setting_preserves_secret_bytes(self):
        old = 'TRADING_MODE=LIVE\nDHAN_PIN=PRIVATE\nEQUITY_AMO_ENABLED=false # comment\nFOO=bar\n'
        new = _single_key_change(old, True)
        self.assertEqual(new, old.replace('EQUITY_AMO_ENABLED=false', 'EQUITY_AMO_ENABLED=true'))
        self.assertIn('DHAN_PIN=PRIVATE', new)

    def test_missing_key_added_without_rewriting_other_keys(self):
        old = 'TRADING_MODE=LIVE\nTG_API_HASH=a=b#c'
        new = _single_key_change(old, True)
        self.assertEqual(new, old + '\nEQUITY_AMO_ENABLED=true\n')

    def test_duplicate_key_fails_closed(self):
        with self.assertRaises(ValueError):
            _single_key_change('EQUITY_AMO_ENABLED=true\nequity_amo_enabled=false\n', False)

    def test_crlf_preserved(self):
        self.assertEqual(_single_key_change('A=1\r\nEQUITY_AMO_ENABLED=false\r\n', True),
                         'A=1\r\nEQUITY_AMO_ENABLED=true\r\n')

    def test_no_change_preserves_exact_content(self):
        text = 'EQUITY_AMO_ENABLED=true\nDATA_DIR=data\n'
        self.assertEqual(_single_key_change(text, True), text)


class AmoJournalTests(unittest.TestCase):
    def test_missing_db_is_unverified_not_empty_broker(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = build_amo_orders(tmp)
            self.assertFalse(result['verified'])
            self.assertIn('unavailable', result['summary'])

    def test_ack_is_not_traded_and_no_order_ids_exposed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'trade_journal.db')
            con = sqlite3.connect(db)
            try:
                con.executescript('''
                CREATE TABLE order_intents(correlation_id TEXT, symbol TEXT, state TEXT,
                  is_amo INTEGER, created_at REAL);
                CREATE TABLE orders(correlation_id TEXT, status TEXT, filled_quantity INTEGER,
                  quantity INTEGER, broker_order_id TEXT);
                INSERT INTO order_intents VALUES('private-corr','TRIDENT-EQ','ACKED',1,123);
                INSERT INTO orders VALUES('private-corr','TRANSIT',0,1,'SECRET-ORDER-ID');
                INSERT INTO order_intents VALUES('normal-corr','ABC-EQ','ACKED',0,124);
                ''')
                con.commit()
            finally:
                con.close()
            result = build_amo_orders(tmp)
            self.assertEqual(len(result['orders']), 1)
            self.assertIn('PENDING', result['orders'][0]['status'])
            self.assertFalse(result['verified'])
            self.assertNotIn('SECRET-ORDER-ID', str(result))
            self.assertNotIn('TRADED', str(result))

    def test_journal_fill_is_reported_not_automatically_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'trade_journal.db')
            con = sqlite3.connect(db)
            try:
                con.executescript('''
                CREATE TABLE order_intents(correlation_id TEXT, symbol TEXT, state TEXT,
                  is_amo INTEGER, created_at REAL);
                CREATE TABLE orders(correlation_id TEXT, status TEXT, filled_quantity INTEGER,quantity INTEGER);
                INSERT INTO order_intents VALUES('c','ABC-EQ','ACKED',1,123);
                INSERT INTO orders VALUES('c','TRADED',2,2);
                ''')
                con.commit()
            finally:
                con.close()
            result = build_amo_orders(tmp)
            self.assertIn('broker unverified', result['orders'][0]['status'])


class ControlApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = os.path.join(self.tmp.name, '.env')
        Path(self.env).write_text('TRADING_MODE=TEST\nDHAN_PIN=unmodified\nEQUITY_AMO_ENABLED=false\n', encoding='utf-8')
        self.cfg = types.SimpleNamespace(equity_amo_enabled=False,data_dir=self.tmp.name)
        self.tracker = MagicMock()
        self.tracker.pending_op_file = os.path.join(self.tmp.name,'pending.json')
        self.tracker.deferred_env_file = os.path.join(self.tmp.name,'deferred.env')
        self.tracker.get_change_record.return_value = None
        self.tracker.get_service_control_record.return_value = {}
        self.tracker.inspect_transition_gate_status.return_value = {'active':False,'ok':True}
        self.tracker.control_service.return_value = {'service_state':'STOPPED_BY_OWNER','saved_mode':'TEST',
                                                     'running_mode':'STOPPED','runtime_verified':False}
        self.tracker.operation_lock.return_value = contextlib.nullcontext()
        rec=types.SimpleNamespace(change_id='CFG-offline',state=types.SimpleNamespace(value='REQUEST_RECEIVED'))
        self.tracker.enqueue_durable_configuration_update.return_value=rec
        self.app=FastAPI()
        register_control_center_routes(self.app,'test-csrf')
        self.client=TestClient(self.app)
        self.p1=patch('src.control_center_backend._config_and_paths',return_value=(self.cfg,self.env,self.tmp.name))
        self.p2=patch('src.control_center_backend.ConfigChangeTracker',return_value=self.tracker)
        self.p1.start();self.p2.start()
        self.addCleanup(self.p1.stop);self.addCleanup(self.p2.stop)

    def test_forged_csrf_no_mutations(self):
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'forged'})
        self.assertEqual(r.status_code,403)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_non_boolean_rejected(self):
        for bad in ['true',1,None,{},[]]:
            r=self.client.post('/control/amo/equity',json={'enabled':bad,'csrf_token':'test-csrf'})
            self.assertEqual(r.status_code,422)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_malformed_payload_rejected(self):
        for bad in ({'enabled':True},{'enabled':True,'csrf_token':'test-csrf','extra':'x'}):
            r=self.client.post('/control/amo/equity',json=bad)
            self.assertEqual(r.status_code,400)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_equity_amo_is_queued_via_existing_coordinator(self):
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,202, r.text)
        self.assertEqual(r.json()['change_id'],'CFG-offline')
        kwargs=self.tracker.enqueue_durable_configuration_update.call_args.kwargs
        self.assertIn('DHAN_PIN=unmodified',kwargs['env_content'])
        self.assertIn('EQUITY_AMO_ENABLED=true',kwargs['env_content'])
        self.assertEqual(kwargs['form_params']['TRADING_MODE'],'TEST')
        self.assertFalse(kwargs['spawn_worker'])
        self.assertEqual(Path(self.env).read_text(), 'TRADING_MODE=TEST\nDHAN_PIN=unmodified\nEQUITY_AMO_ENABLED=false\n')

    def test_preexisting_pending_change_prevents_overwrite(self):
        Path(self.tracker.pending_op_file).write_text('pending')
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,409)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_owner_stop_pending_prevents_toggle(self):
        self.tracker.get_service_control_record.return_value={'deferred_stop_pending':True}
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,409)

    def test_unknown_service_state_disables_changes(self):
        self.tracker.control_service.return_value={'service_state':'UNKNOWN','saved_mode':'LIVE'}
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,409)
        s=self.client.get('/control/status').json()
        self.assertFalse(s['equity_amo_can_edit'])
        self.assertIsNone(s['equity_amo_active'])

    def test_running_version_mismatch_blocks_toggle(self):
        self.tracker.control_service.return_value={'service_state':'RUNNING','saved_mode':'LIVE',
                                                   'running_mode':'LIVE','runtime_verified':False}
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,409)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_status_is_readonly_and_no_secrets(self):
        r=self.client.get('/control/status')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.headers['cache-control'],'no-store')
        self.assertFalse(r.json()['fno_amo_supported'])
        self.assertNotIn('unmodified',r.text)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_same_setting_does_not_create_restart(self):
        r=self.client.post('/control/amo/equity',json={'enabled':False,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.json()['state'],'UNCHANGED')
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_fno_amo_post_does_not_exist(self):
        r=self.client.post('/control/amo/fno',json={'enabled':True,'csrf_token':'test-csrf'})
        self.assertEqual(r.status_code,404)
        self.tracker.enqueue_durable_configuration_update.assert_not_called()

    def test_health_endpoint_is_read_only_and_bounded(self):
        with patch('src.control_center_backend.build_control_health',return_value={'summary':'test','checks':[]}):
            r=self.client.get('/control/health')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.headers['cache-control'],'no-store')
        self.tracker.enqueue_durable_configuration_update.assert_not_called()


if __name__=='__main__':
    unittest.main()