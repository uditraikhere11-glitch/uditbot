"""Pinned raw broker reads; all transports synthetic, no account or VM access."""
from dataclasses import replace
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import Mock
from contextlib import closing

from requests import Response
from src.broker.broker_snapshot import LIVE_BOOKS, NS, BookStatus, EvidenceRequest, Purpose, SessionContext, SessionPhase
from src.broker.dhan_snapshot_source import DhanSnapshotSource, JOURNAL_TABLES, valid_collection
from src.broker.snapshot_coordinator import EvidenceUnavailable, ReadDeferredResult

ACCOUNT='9000000001'


def response(code, payload, **headers):
    r=Response(); r.status_code=code; r._content=json.dumps(payload).encode(); r.headers.update(headers)
    return r


class SourceTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup); self.root=Path(tmp.name)
        token=self.root/'token'; token.write_text('synthetic-test-token');token.chmod(0o600)
        (self.root/'active_broker_mutations.json').write_text(json.dumps({'schema_version':2,'account_id':ACCOUNT,
            'handoff_verified':True,'active':[],'unresolved':[]}))
        journal=self.root/'fixture.sqlite3'
        with closing(sqlite3.connect(journal)) as conn,conn:
            for table in JOURNAL_TABLES: conn.execute('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY)')
        self.client=Mock();self.client.cfg=SimpleNamespace(dhan_client_id=ACCOUNT,data_dir=str(self.root),journal_db_path=str(journal),dhan_token_file=str(token))
        self.client.extract_token_expiration.return_value=(time.time()+600,'')
        self.client._decode_jwt_claims.return_value={}
        self.client._get_active_read_governor.return_value.account_id=ACCOUNT
        self.client.validate_historical_trade_record.return_value=(True,{},'')
        self.client._governed_get.side_effect=self.good_response
        self.source=DhanSnapshotSource(self.client,lambda: SessionContext(SessionPhase.SETTLED,date(2026,11,26),time.time_ns(),'a'*64,True))
        self.coordinator=self.source.coordinator()

    def good_response(self,url,**kwargs):
        self.assertEqual(kwargs['headers']['access-token'],'synthetic-test-token')
        if url.endswith('/profile'): return response(200,{'dhanClientId':ACCOUNT})
        return response(200,[])

    def acquire(self,**kwargs):
        return self.coordinator.acquire(EvidenceRequest(ACCOUNT,Purpose.WATCHER,LIVE_BOOKS,deadline_mono_ns=time.monotonic_ns()+5*NS,**kwargs))

    def test_verified_empty_and_coherent_callback(self):
        s=self.acquire();self.assertTrue(s.complete,s);self.assertTrue(s.profile_verified)
        self.assertEqual(s.holdings.status,BookStatus.OK_EMPTY)
        self.assertEqual(self.client._governed_get.call_count,7)
        self.assertEqual(self.client._record_validated_get.call_count,7)
        self.client._get_active_read_governor.return_value.record_coherent_snapshot.assert_called_once()
        self.assertEqual(self.acquire().snapshot_id,s.snapshot_id)
        self.assertEqual(self.client._governed_get.call_count,7)
        self.client.generate_token_with_totp.assert_not_called()
        self.client._get_with_auth_recovery.assert_not_called()
        self.assertNotIn(b'synthetic-test-token',self.coordinator.path.read_bytes())

    def test_malformed_book_and_local_deferral_not_empty(self):
        def get(url,**kw):
            if url.endswith('/holdings'): return response(200,[{}])
            if url.endswith('/positions'): return response(429,{},**{'X-AuditBot-Local-Deferral':'1','Retry-After':'60'})
            return self.good_response(url,**kw)
        self.client._governed_get.side_effect=get
        s=self.acquire();self.assertFalse(s.complete,s);self.assertEqual(s.holdings.status,BookStatus.MALFORMED)
        self.assertEqual(s.positions.status,BookStatus.COOLDOWN);self.assertIsNone(s.positions.http_status)
        self.assertFalse(s.positions.transport_sent);self.assertIsNone(s.holdings.payload)
        self.client._get_active_read_governor.return_value.record_coherent_snapshot.assert_not_called()

    def test_auth_mismatch_stops_remaining_reads(self):
        self.client._governed_get.side_effect=lambda *a,**kw:response(200,{'dhanClientId':'wrong'})
        s=self.acquire();self.assertFalse(s.complete,s);self.assertFalse(s.profile_verified)
        self.assertEqual(s.profile.status,BookStatus.AUTH_FAILURE)
        self.assertEqual(self.client._governed_get.call_count,1)

    def test_journal_and_token_rotation_invalidate(self):
        a=self.acquire();self.assertTrue(a.complete,a)
        with closing(sqlite3.connect(self.client.cfg.journal_db_path)) as c,c:
            c.execute('INSERT INTO orders VALUES (1)')
        b=self.acquire();self.assertTrue(b.complete,b);self.assertGreater(b.generation.journal_epoch,a.generation.journal_epoch)
        p=Path(self.client.cfg.dhan_token_file); p.unlink();p.symlink_to(self.root/'absent')
        r=self.acquire();self.assertIsInstance(r,ReadDeferredResult)

    def test_active_mutation_or_missing_ledger_blocks_complete_capture(self):
        a=self.acquire();self.assertTrue(a.complete)
        ledger=self.root/'active_broker_mutations.json'
        original=json.loads(ledger.read_text())
        ledger.write_text(json.dumps(dict(original,active=[{'id':'synthetic-inflight'}])))
        b=self.acquire();self.assertFalse(b.complete);self.assertIn('MUTATION_IN_FLIGHT_OR_UNVERIFIED',b.invalid_reasons)
        ledger.unlink()
        c=self.acquire();self.assertFalse(c.complete)
        ledger.write_text(json.dumps(dict(original,unresolved=[{'id':'synthetic-unknown'}])))
        d=self.acquire();self.assertTrue(d.complete)  # evidence only; ledger must still resolve the unknown
        self.assertEqual(json.loads(ledger.read_text())['unresolved'],[{'id':'synthetic-unknown'}])

    def test_known_auth_failure_invalidates_previous_complete_snapshot(self):
        first=self.acquire();self.assertTrue(first.complete)
        self.client._governed_get.side_effect=lambda *a,**kw:response(401,{'errorCode':'DH-901'})
        failed=self.acquire(independent_after_snapshot_id=first.snapshot_id)
        self.assertFalse(failed.complete)
        self.assertIsNone(self.coordinator.peek_display(self.coordinator.account_key))
        with closing(sqlite3.connect(self.coordinator.path)) as c:
            self.assertEqual(c.execute("SELECT count(*) FROM snapshots WHERE state='COMPLETE'").fetchone()[0],0)
        self.client._governed_get.side_effect=self.good_response
        fresh=self.acquire();self.assertTrue(fresh.complete)
        self.assertGreater(fresh.generation.token_epoch,first.generation.token_epoch)

    def test_newer_failed_read_invalidates_old_authority(self):
        a = self.acquire(); self.assertTrue(a.complete)
        def get(url, **kw):
            if url.endswith('/holdings'): return response(503, {'errorCode': 'DH-500'})
            return self.good_response(url, **kw)
        self.client._governed_get.side_effect = get
        b = self.acquire(independent_after_snapshot_id=a.snapshot_id)
        self.assertFalse(b.complete)
        self.assertEqual(b.holdings.status, BookStatus.HTTP_FAILURE)
        req = EvidenceRequest(ACCOUNT, Purpose.WATCHER, LIVE_BOOKS,
                              deadline_mono_ns=time.monotonic_ns()+5*NS)
        self.assertTrue(self.coordinator.revalidate(a, req))
        self.assertIsNone(self.coordinator.peek_display(self.coordinator.account_key))

    def test_profile_expiration_prevents_cached_authority(self):
        self.client.extract_token_expiration.return_value=(time.time()+0.4,'')
        a=self.acquire();self.assertTrue(a.complete,a)
        time.sleep(.45)
        self.assertIsNone(self.coordinator.peek_display(self.coordinator.account_key))
        b=self.acquire();self.assertFalse(b.complete)
        self.assertNotEqual(a.snapshot_id,b.snapshot_id)

    def test_historical_terminal_page_required(self):
        a=self.acquire(history_ranges=((date(2026,11,20),date(2026,11,21)),))
        self.assertTrue(a.complete,a);self.assertTrue(a.history_coverage[0].terminal_empty_page_verified)
        self.assertTrue(a.history_coverage[0].archive_only)

    def test_edis_exact_isin_only_and_not_reused_for_another_isin(self):
        first, second = 'INE528G01035', 'INE002A01018'
        def get(url, **kw):
            if '/edis/inquire/' in url:
                return response(200, {'isin': url.rsplit('/', 1)[1], 'clientId': ACCOUNT,
                                      'status': 'SUCCESS', 'totalQty': '2', 'authorizedQty': 0})
            return self.good_response(url, **kw)
        self.client._governed_get.side_effect = get
        def req(isin):
            return EvidenceRequest(ACCOUNT, Purpose.WATCHER, LIVE_BOOKS | {'edis'},
                                   deadline_mono_ns=time.monotonic_ns()+5*NS, edis_isin=isin)
        missing = self.coordinator.acquire(req(None))
        self.assertFalse(missing.complete)
        self.assertIn('EDIS_ISIN_REQUIRED', missing.invalid_reasons)
        a = self.coordinator.acquire(req(first))
        self.assertTrue(a.complete, a)
        self.assertEqual(a.edis.payload['totalQty'], '2')
        self.assertEqual(a.edis.payload['authorizedQty'], 0)
        self.assertEqual(self.coordinator.acquire(req(first)).snapshot_id, a.snapshot_id)
        b = self.coordinator.acquire(req(second))
        self.assertTrue(b.complete, b)
        self.assertNotEqual(a.snapshot_id, b.snapshot_id)
        self.assertIn('EDIS_IDENTITY_UNVERIFIED', self.coordinator.revalidate(a, req(second)))
        with self.assertRaises(ValueError): req('../profile')

    def test_edis_bad_identity_and_quantity_block(self):
        isin = 'INE528G01035'
        for override in ({'clientId': 'wrong'}, {'isin': 'INE002A01018'},
                         {'status': 'FAILURE'}, {'totalQty': True}, {'totalQty': -1}):
            with self.subTest(override=override):
                def get(url, **kw):
                    if '/edis/inquire/' in url:
                        return response(200, dict({'isin': isin, 'clientId': ACCOUNT,
                                                  'status': 'SUCCESS', 'totalQty': 2}, **override))
                    return self.good_response(url, **kw)
                self.client._governed_get.side_effect = get
                req = EvidenceRequest(ACCOUNT, Purpose.WATCHER, LIVE_BOOKS | {'edis'},
                                      deadline_mono_ns=time.monotonic_ns()+5*NS, edis_isin=isin)
                s = self.coordinator.acquire(req)
                self.assertFalse(s.complete, s)
                self.assertEqual(s.edis.status, BookStatus.MALFORMED)
                self.assertIsNone(s.edis.payload)

    def test_bad_rows_never_clean(self):
        self.assertFalse(valid_collection('holdings',[{'securityId':'1','dpQty':True}],ACCOUNT))
        self.assertFalse(valid_collection('positions',[{'securityId':'1','netQty':1}],ACCOUNT))
        self.assertTrue(valid_collection('positions',[{'securityId':'1','netQty':-1,'exchangeSegment':'NSE_EQ','productType':'CNC'}],ACCOUNT))


if __name__=='__main__': unittest.main()
