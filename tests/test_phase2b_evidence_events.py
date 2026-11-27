"""Real durable mutation/journal/control/auth events invalidate shared snapshots."""
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from src.broker.broker_snapshot import EvidenceRequest, LIVE_BOOKS, NS, Purpose, ReadDeferredResult
from src.broker.dhan_client import DhanClient
from src.broker.evidence_events import note_event
from src.broker.snapshot_coordinator import CrossProcessReadCoordinator
from src.config import AppConfig
from src.engine.broker_handoff import broker_mutation, register_producer
from src.engine.config_manager import write_json_atomic
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, TradingMode
from tests.test_phase2b_snapshot_coordinator import ACCOUNT, collect, observation


class EventTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name)
        self.c=CrossProcessReadCoordinator(self.root,ACCOUNT,observe=observation,collect=collect)
        self.req=lambda: EvidenceRequest(ACCOUNT,Purpose.WATCHER,LIVE_BOOKS,deadline_mono_ns=time.monotonic_ns()+5*NS)
        self.assertTrue(self.c.acquire(self.req()).complete)

    def state(self):
        with closing(sqlite3.connect(self.c.path)) as conn:
            return conn.execute('SELECT token_epoch,mutation_epoch,owner_epoch,journal_epoch FROM account_state').fetchone(),conn.execute("SELECT count(*) FROM snapshots WHERE state='COMPLETE'").fetchone()[0]

    def test_handoff_mutation_start_end_invalidate_before_send_and_unknown_retained(self):
        register_producer(self.root,ACCOUNT)
        self.assertTrue(self.c.acquire(self.req()).complete)
        before=self.state()[0]
        with broker_mutation(self.root,ACCOUNT,'POST'):
            during,n=self.state()
            self.assertEqual(n,0)
            self.assertGreater(during[1],before[1])
            ledger=json.loads((self.root/'active_broker_mutations.json').read_text())
            self.assertEqual(len(ledger['active']),1)
        after,n=self.state(); self.assertEqual(n,0);self.assertGreater(after[1],during[1])
        ledger=json.loads((self.root/'active_broker_mutations.json').read_text())
        self.assertEqual(len(ledger['unresolved']),1)
        self.assertFalse(ledger['active'])

    def test_control_atomic_write_and_journal_commit_invalidate(self):
        before=self.state()[0]
        write_json_atomic(str(self.root/'service_control_state.json'),{'desired_service_state':'STOPPED'})
        after,n=self.state();self.assertEqual(n,0);self.assertGreater(after[2],before[2])
        db=JournalDB(str(self.root/'fixture.sqlite3'),account_id=ACCOUNT)
        self.assertTrue(self.c.acquire(self.req()).complete)
        before=self.state()[0]
        db.record_system_event('SYNTHETIC_TEST','test evidence')
        after,n=self.state();self.assertEqual(n,0);self.assertGreater(after[3],before[3])

    def test_actual_broker_auth_rejection_invalidates_without_minting(self):
        cfg=AppConfig(trading_mode=TradingMode.LIVE,data_dir=str(self.root),dhan_client_id=ACCOUNT,
                      dhan_token_file=str(self.root/'missing'),dhan_access_token='synthetic')
        client=DhanClient(cfg,auto_generate_token=False,allow_read_auth_recovery=False)
        self.addCleanup(client.session.close)
        before=self.state()[0]
        with patch.object(client.session,'get',return_value=Mock(status_code=401,text='{"errorCode":"DH-901"}',headers={})),patch.object(client,'generate_token_with_totp') as mint:
            result=client.get_holdings()
        self.assertIsInstance(result,BrokerReadFailure);mint.assert_not_called()
        after,n=self.state();self.assertEqual(n,0);self.assertGreater(after[0],before[0])

    def test_failed_invalidation_fault_is_persistent_and_blocks_cache(self):
        # Simulate a metadata write failure without modifying source evidence.
        with patch('src.broker.snapshot_coordinator._connect_existing',side_effect=sqlite3.OperationalError('synthetic busy')):
            self.assertFalse(note_event(self.root,ACCOUNT,'TOKEN_AUTH_REJECTED'))
        self.assertTrue((self.c.directory/'authority_fault').is_file())
        self.assertIsInstance(self.c.acquire(self.req()),ReadDeferredResult)
        self.assertIsNone(self.c.peek_display(self.c.account_key))
        with self.assertRaises(Exception):
            CrossProcessReadCoordinator(self.root,ACCOUNT,observe=observation,collect=collect)

    def test_uninitialized_writer_does_not_create_sidecar(self):
        sub=self.root/'uninitialized';sub.mkdir()
        self.assertTrue(note_event(sub,ACCOUNT,'TOKEN_PUBLISHED_OR_ADOPTED'))
        self.assertFalse((sub/'broker_evidence').exists())
