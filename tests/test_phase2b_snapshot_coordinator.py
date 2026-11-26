"""Cross-process coordination with disposable SQLite sidecars and synthetic books."""
from dataclasses import replace
from datetime import date
import multiprocessing as mp
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing

from src.broker.broker_snapshot import (
    LIVE_BOOKS, NS, BookEvidence, BookStatus, BrokerSnapshot, EvidenceRequest,
    Purpose, ReadDeferredResult, SessionContext, SessionPhase, payload_digest,
)
from src.broker.snapshot_coordinator import CrossProcessReadCoordinator, EnvironmentObservation, EvidenceUnavailable

ACCOUNT='9000000001'


def observation():
    return EnvironmentObservation((1,2,3,4),'a'*64,'b'*64,'c'*64,
        SessionContext(SessionPhase.SETTLED,date(2026,11,26),time.time_ns(),'d'*64,True),mutation_clear=True)


def collect(req,generation,sid,obs):
    utc=time.time_ns();mono=time.monotonic_ns()
    def book(name):
        payload={'dhanClientId':ACCOUNT} if name=='profile' else []
        return BookEvidence(name,'/v2/'+name,{},BookStatus.OK_RECORDS if payload else BookStatus.OK_EMPTY,
                           200,None,None,True,utc,utc,mono,mono,ACCOUNT,generation.token_epoch,payload,payload_digest(payload),True)
    return BrokerSnapshot(snapshot_id=sid,generation=generation,account_id=ACCOUNT,profile_account_id=ACCOUNT,
        profile_verified=True,session=obs.session,capture_started_utc_ns=utc,capture_finished_utc_ns=utc,
        capture_started_mono_ns=mono,capture_finished_mono_ns=mono,**{key:book(key) for key in LIVE_BOOKS},
        historical_trades=(),order_status={},trades_for_order={},profile=book('profile'),funds=None,edis=None,
        history_coverage=(),complete=True,invalid_reasons=(),producer_pid=os.getpid(),producer_starttime=1,
        journal_digest=obs.journal_digest,profile_valid_until_mono_ns=mono+600*NS)


def worker(root,ready,start,out):
    def counted(*args):
        with open(Path(root)/'collect-count','a') as f: f.write('flight\n')
        time.sleep(.15)
        return collect(*args)
    try:
        c=CrossProcessReadCoordinator(root,ACCOUNT,observe=observation,collect=counted)
        ready.put(True)
        if not start.wait(5): raise RuntimeError('start timeout')
        result=c.acquire(EvidenceRequest(ACCOUNT,Purpose.WATCHER,LIVE_BOOKS,deadline_mono_ns=time.monotonic_ns()+5*NS))
        out.put((type(result).__name__,getattr(result,'snapshot_id',None),getattr(result,'reason_code',None)))
    except BaseException as exc:
        out.put(('error',None,repr(exc)))


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=self.tmp.name
        self.obs=observation()
        self.count=0

    def request(self,**kwargs):
        args=dict(account_id=ACCOUNT,purpose=Purpose.WATCHER,required_books=LIVE_BOOKS,
                  deadline_mono_ns=time.monotonic_ns()+5*NS)
        args.update(kwargs);return EvidenceRequest(**args)

    def coordinator(self,collector=None):
        def counted(*args):
            self.count+=1
            return collect(*args)
        return CrossProcessReadCoordinator(self.root,ACCOUNT,observe=lambda:self.obs,collect=collector or counted)

    def test_reuse_and_display_are_not_authorization(self):
        c=self.coordinator()
        a=c.acquire(self.request());self.assertIsInstance(a,BrokerSnapshot,a)
        b=c.acquire(self.request());self.assertEqual(a.snapshot_id,b.snapshot_id);self.assertEqual(self.count,1)
        display=c.acquire(self.request(purpose=Purpose.UI));self.assertIsInstance(display,BrokerSnapshot)
        self.assertFalse(display.complete);self.assertEqual(display.invalid_reasons,('DISPLAY_ONLY',))
        self.assertEqual(self.count,1)
        self.assertEqual(c.path.stat().st_mode&0o777,0o600)
        self.assertEqual(c.directory.stat().st_mode&0o777,0o700)

    def test_token_owner_mutation_and_journal_invalidation(self):
        c=self.coordinator();s=c.acquire(self.request());self.assertIsInstance(s,BrokerSnapshot,s)
        for field,value,epoch in (('token_identity',(1,3,4,5),'token_epoch'),('owner_signature','e'*64,'owner_epoch'),
                                  ('mutation_signature','f'*64,'mutation_epoch'),('journal_digest','9'*64,'journal_epoch')):
            before=s
            self.obs=replace(self.obs,**{field:value})
            s=c.acquire(self.request());self.assertIsInstance(s,BrokerSnapshot,s)
            self.assertNotEqual(before.snapshot_id,s.snapshot_id)
            self.assertGreater(getattr(s.generation,epoch),getattr(before.generation,epoch))
        before=s
        c.invalidate(c.account_key,'MUTATION_BEFORE_SEND','synthetic-event')
        s=c.acquire(self.request());self.assertIsInstance(s,BrokerSnapshot,s)
        self.assertGreater(s.generation.mutation_epoch,before.generation.mutation_epoch)

    def test_generation_change_during_collection_never_publishes_complete(self):
        c=self.coordinator()
        def mutate(*args):
            # This short metadata write must succeed even with the collector lock held.
            c.invalidate(c.account_key,'MUTATION_BEFORE_SEND','simulated')
            return collect(*args)
        c._collect=mutate
        r=c.acquire(self.request());self.assertIsInstance(r,ReadDeferredResult,r)
        with closing(sqlite3.connect(c.path)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM snapshots WHERE state='COMPLETE'").fetchone()[0],0)

    def test_independent_and_post_intent_fence(self):
        c=self.coordinator();a=c.acquire(self.request());self.assertIsInstance(a,BrokerSnapshot,a)
        b=c.acquire(self.request(independent_after_snapshot_id=a.snapshot_id))
        self.assertIsInstance(b,BrokerSnapshot,b);self.assertNotEqual(a.snapshot_id,b.snapshot_id)
        # Cleanup must wait at least two seconds after previous receipt.
        r=c.acquire(self.request(purpose=Purpose.CLEANUP,independent_after_snapshot_id=b.snapshot_id))
        self.assertIsInstance(r,ReadDeferredResult,r)
        self.assertEqual(r.reason_code,'INDEPENDENT_CAPTURE_TOO_EARLY')
        self.assertIsInstance(c.acquire(self.request(purpose=Purpose.SHUTDOWN)),ReadDeferredResult)
        fence=time.monotonic_ns()
        s=c.acquire(self.request(purpose=Purpose.SHUTDOWN,min_capture_mono_ns=fence))
        self.assertIsInstance(s,BrokerSnapshot,s);self.assertGreaterEqual(s.capture_started_mono_ns,fence)

    def test_corrupt_persisted_evidence_fails_closed(self):
        c=self.coordinator();s=c.acquire(self.request());self.assertIsInstance(s,BrokerSnapshot,s)
        with closing(sqlite3.connect(c.path)) as conn,conn:
            conn.execute("UPDATE snapshot_books SET payload_sha256='fake' WHERE book_key='holdings'")
        r=c.acquire(self.request());self.assertIsInstance(r,ReadDeferredResult,r)
        self.assertIsNone(c.peek_display(c.account_key))
        self.assertEqual(self.count,1)

    def test_final_revalidation_refuses_mutation_and_stale_publication(self):
        c = self.coordinator()
        req = self.request()
        s = c.acquire(req)
        self.assertEqual(c.revalidate(s, req), ())
        self.assertTrue(c.revalidate(replace(s, producer_starttime=2), req))
        self.assertTrue(c.revalidate(s, self.request(purpose=Purpose.SHUTDOWN)))
        self.obs = replace(self.obs, mutation_clear=False)
        self.assertTrue(c.revalidate(s, req))
        self.obs = replace(self.obs, mutation_clear=True)
        c.invalidate(c.account_key, 'OWNER_STOP', 'synthetic')
        self.assertTrue(c.revalidate(s, req))

    def test_invalidation_during_recovery_callback_cannot_return_complete(self):
        c = self.coordinator()
        c._on_snapshot = lambda snapshot, generation: c.invalidate(
            c.account_key, 'MUTATION_BEFORE_SEND', 'synthetic')
        self.assertIsInstance(c.acquire(self.request()), ReadDeferredResult)

    def test_private_paths_and_symlink_rejection(self):
        target=Path(self.root)/'target';target.mkdir()
        (Path(self.root)/'broker_evidence').symlink_to(target,target_is_directory=True)
        with self.assertRaises(EvidenceUnavailable): self.coordinator()
        self.assertFalse(list(target.iterdir()))

    def test_actual_process_single_flight(self):
        ctx=mp.get_context('fork')
        ready=ctx.Queue();out=ctx.Queue();start=ctx.Event()
        procs=[ctx.Process(target=worker,args=(self.root,ready,start,out)) for _ in range(2)]
        try:
            for p in procs: p.start()
            for _ in procs: self.assertTrue(ready.get(timeout=5))
            start.set()
            results=[out.get(timeout=8) for _ in procs]
            self.assertEqual([r[0] for r in results],['BrokerSnapshot','BrokerSnapshot'],results)
            self.assertEqual(results[0][1],results[1][1])
            self.assertEqual((Path(self.root)/'collect-count').read_text().splitlines(),['flight'])
        finally:
            for p in procs:
                p.join(timeout=2)
                if p.is_alive():p.terminate();p.join(timeout=2)
            for q in (ready,out): q.close();q.join_thread()
