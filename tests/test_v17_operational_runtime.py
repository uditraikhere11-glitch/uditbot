"""Offline fail-closed contract tests for the V17 operational correction.

No Dhan/Telegram traffic, no service restart and no trading/DB mutation.
"""
import os
import json
import sqlite3
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.control_center_backend import (
    _cached_broker_status, _operational_evidence, _BROKER_CACHE,
    build_control_reports, register_control_center_routes,
)
from src.engine.operational_snapshot import (
    publish_operational_snapshot, read_verified_snapshot,
)
from src.engine.runtime_evidence import proc_starttime


class RuntimeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = types.SimpleNamespace(data_dir=self.tmp.name, dhan_client_id='test-client-42')
        gate = types.SimpleNamespace(
            state=types.SimpleNamespace(value='HEALTHY'),
            is_accepting_entries=True, last_reconciled_at=time.time() - 4,
        )
        self.pipeline = types.SimpleNamespace(
            cfg=self.cfg, reconciliation_gate=gate,
            watcher=types.SimpleNamespace(_is_running=True),
        )

    def test_verified_runtime_and_account_fence(self):
        actual = publish_operational_snapshot(self.pipeline)
        path = Path(self.tmp.name) / 'runtime_operational_snapshot.json'
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        common = dict(expected_pid=os.getpid(), expected_starttime=proc_starttime(os.getpid()),
                      expected_account_id='test-client-42')
        self.assertEqual(read_verified_snapshot(self.tmp.name, **common)['reconciliation'], 'HEALTHY')
        self.assertIsNone(read_verified_snapshot(self.tmp.name, **common)['telegram_connected'])
        publish_operational_snapshot(self.pipeline, telegram_connected=True)
        self.assertIs(read_verified_snapshot(self.tmp.name, **common)['telegram_connected'], True)
        self.assertIsNone(read_verified_snapshot(self.tmp.name,
                      **{**common, 'expected_account_id': 'wrong-account'}))
        self.assertIsNone(read_verified_snapshot(self.tmp.name,
                      **{**common, 'expected_pid': 123456789}))
        self.assertIsNone(read_verified_snapshot(self.tmp.name, now_ts=actual['created_at'] + 37, **common))
        self.assertIsNone(read_verified_snapshot(self.tmp.name,
                      **{**common, 'expected_starttime': common['expected_starttime'] + 1}))
        payload = json.loads(path.read_text())
        payload['entries_open'] = True
        payload['reconciliation'] = 'BLOCKED'
        path.write_text(json.dumps(payload))
        self.assertIsNone(read_verified_snapshot(self.tmp.name, **common))

    def test_producer_blocked_gate_never_claims_open(self):
        self.pipeline.reconciliation_gate.state.value = 'BLOCKED'
        self.pipeline.reconciliation_gate.is_accepting_entries = False
        publish_operational_snapshot(self.pipeline)
        snap = read_verified_snapshot(self.tmp.name, expected_pid=os.getpid(),
                  expected_starttime=proc_starttime(os.getpid()), expected_account_id='test-client-42')
        self.assertEqual(snap['reconciliation'], 'BLOCKED')
        self.assertFalse(snap['entries_open'])

    def test_web_ui_requires_verified_running_pid(self):
        self.assertEqual(_operational_evidence(object(), {'runtime_verified': False}, self.cfg,
                                              self.tmp.name)['new_entries_gate'], 'UNVERIFIED')
        self.assertEqual(_operational_evidence(object(), {'runtime_verified': True,
                   'service_state': 'STOPPED_OTHER'}, self.cfg, self.tmp.name)['reconciliation'], 'UNVERIFIED')


class ReadOnlyReportingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.db=Path(self.tmp.name)/'journal.db'
        with sqlite3.connect(self.db) as con:
            con.executescript('''
                CREATE TABLE system_events (id INTEGER PRIMARY KEY, event_type TEXT, description TEXT, payload TEXT, created_at REAL);
                CREATE TABLE orders (id INTEGER PRIMARY KEY, symbol TEXT, status TEXT, trading_mode TEXT, created_at REAL);
                CREATE TABLE signals (id INTEGER PRIMARY KEY, received_at REAL);
                CREATE TABLE order_intents (id INTEGER PRIMARY KEY, state TEXT);
                CREATE TABLE protection_orders (id INTEGER PRIMARY KEY, status TEXT);
                CREATE TABLE fills (id INTEGER PRIMARY KEY, traded_at REAL);
            ''')
            con.execute("INSERT INTO system_events VALUES (1, 'DAILY_HEALTH_CHECK', 'SECRET PIN or token', '', ?)", (time.time(),))
            con.execute("INSERT INTO system_events VALUES (2, 'OWNER_PRIVATE_NOTE', 'PRIVATE', '', ?)", (time.time(),))
            con.execute("INSERT INTO orders VALUES (1, 'SBIN', 'TRADED', 'LIVE', ?)", (time.time(),))
            con.execute("INSERT INTO signals VALUES (1,?)", (time.time(),))
            con.execute("INSERT INTO fills VALUES (1,?)", (time.time(),))
            con.execute("INSERT INTO order_intents VALUES (1,'UNKNOWN')")
            con.execute("INSERT INTO protection_orders VALUES (1,'UNPROTECTED')")

    def test_reports_are_allowlisted_and_never_return_description(self):
        cfg=types.SimpleNamespace(journal_db_path=str(self.db))
        with patch('src.control_center_backend._config_and_paths', return_value=(cfg, '', self.tmp.name)):
            r=build_control_reports()
        self.assertTrue(r['available'])
        self.assertEqual(r['summary']['orders_today'], 1)
        self.assertEqual(r['summary']['trades_today'], 1)
        self.assertEqual(r['recent_orders'][0]['symbol'], 'SBIN')
        self.assertEqual(r['summary']['unresolved_intents'], 1)
        self.assertEqual(r['summary']['unprotected_records'], 1)
        self.assertEqual(len(r['events']), 1)
        self.assertNotIn('SECRET', json.dumps(r))
        self.assertNotIn('PRIVATE', json.dumps(r))


class BrokerSafetyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.token=Path(self.tmp.name)/'token'
        self.token.write_text('fake-token');os.chmod(self.token,0o600)
        self.cfg=types.SimpleNamespace(dhan_token_file=str(self.token),dhan_client_id='fake-id',data_dir=self.tmp.name)
        _BROKER_CACHE.update({'key':None,'at':0.0,'value':None})

    def test_profile_only_get_no_auto_totp_and_cache(self):
        class FakeDhan:
            calls=[]
            def __init__(self,cfg,auto_generate_token=True):
                self.calls.append(('construct',auto_generate_token))
                self.session=types.SimpleNamespace(close=lambda:None)
            def verify_profile_and_token_expiry(self,allow_refresh=True):
                self.calls.append(('verify',allow_refresh))
                return {'identity_status':'VERIFIED','identity_verified':True,
                        'token_expiration_ts':time.time()+3600}
        with patch('src.broker.dhan_client.DhanClient',FakeDhan):
            self.assertEqual(_cached_broker_status(self.cfg)['status'],'UNVERIFIED')
            checked=_cached_broker_status(self.cfg,refresh=True)
            self.assertEqual(checked['status'],'VERIFIED')
            self.assertIn('IST',checked['token_expiration_ist'])
            self.assertEqual(_cached_broker_status(self.cfg,refresh=True)['status'],'VERIFIED')
        self.assertEqual(FakeDhan.calls,[('construct',False),('verify',False)])

    def test_broad_token_permissions_block_broker_call(self):
        os.chmod(self.token,0o644)
        with patch('src.broker.dhan_client.DhanClient') as broker:
            self.assertEqual(_cached_broker_status(self.cfg,refresh=True)['status'],'UNVERIFIED')
            broker.assert_not_called()


class ModeAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.app=FastAPI()
        register_control_center_routes(self.app,'known-csrf')
        self.client=TestClient(self.app)
        self.payload={'mode':'LIVE','csrf_token':'known-csrf','expected_revision':'a'*64,'confirm_live':True}

    def test_bad_csrf_and_shape_rejected_without_saving(self):
        with patch('src.control_center_backend.apply_settings') as apply:
            self.assertEqual(self.client.post('/control/mode',json={**self.payload,'csrf_token':'wrong'}).status_code,403)
            self.assertEqual(self.client.post('/control/mode',json={**self.payload,'mode':'DEBUG'}).status_code,422)
            self.assertEqual(self.client.post('/control/mode',json={**self.payload,'unexpected':'yes'}).status_code,400)
            apply.assert_not_called()

    def test_owner_stop_prevents_mode_change(self):
        with patch('src.control_center_backend.build_control_status',return_value={'mode_can_edit':False}),\
             patch('src.control_center_backend.apply_settings') as apply:
            self.assertEqual(self.client.post('/control/mode',json=self.payload).status_code,409)
            apply.assert_not_called()

    def test_mode_change_uses_existing_coordinator_only(self):
        result=types.SimpleNamespace(as_dict=lambda:{'ok':True,'status':'SAVED_FOR_NEXT_START',
                                                      'restart_performed':False,'message':'Saved'})
        with patch('src.control_center_backend.build_control_status',return_value={'mode_can_edit':True}),\
             patch('src.control_center_backend.apply_settings',return_value=result) as apply:
            r=self.client.post('/control/mode',json={**self.payload,'mode':'TEST','confirm_live':False})
        self.assertEqual(r.status_code,200)
        self.assertFalse(r.json()['restart_performed'])
        self.assertEqual(apply.call_args.args[0],{'is_live_mode':False})
        self.assertFalse(apply.call_args.kwargs['confirm_live'])


if __name__=='__main__': unittest.main()
