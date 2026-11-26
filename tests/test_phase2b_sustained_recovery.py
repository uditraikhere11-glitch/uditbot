"""Locked F03 regression with real shared SQLite and distinct receipts."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock

from src.broker.read_governor import ReadGovernor, ReadDeferred
from src.broker.broker_snapshot import NS, Generation, account_key
from tests.test_phase2b_broker_snapshot import snap, book, ACCOUNT, UTC, START


class SustainedRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.utc = UTC / NS
        self.mono = START / NS
        self.boot = 'fake-boot'
        self.g = self.new()

    def new(self, boot=None):
        return ReadGovernor(self.tmp.name, ACCOUNT, clock=lambda:self.utc,
                            monotonic_clock=lambda:self.mono, boot_id=boot or self.boot)

    def advance(self, sec):
        self.utc += sec
        self.mono += sec

    def state(self):
        with closing(sqlite3.connect(self.g.path)) as conn:
            return conn.execute('SELECT state,strikes,successful_get_count,coherent_snapshot_count FROM category_recovery WHERE category=?',('NON_TRADING',)).fetchone()

    def test_single_probe_across_instances_and_failed_probe_lease(self):
        self.g.throttled(None)
        self.advance(60)
        def probe(_):
            try: return self.new().admit(transport_timeout=5)
            except ReadDeferred: return None
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(probe,range(4)))
        self.assertEqual(sum(x is not None for x in results),1)
        self.assertEqual(self.state(),('HALF_OPEN',1,0,0))
        self.advance(4.99)
        with self.assertRaises(ReadDeferred): self.new().admit()
        self.advance(.02)
        receipt=self.new().admit()
        self.assertTrue(self.new().record_success(receipt=receipt))
        self.assertEqual(self.state(),('RECOVERY',1,1,0))
        self.assertFalse(self.new().record_success(receipt=receipt))
        with self.assertRaises(ReadDeferred): self.new().admit()
        self.advance(.5)
        receipt=self.new().admit()
        self.assertTrue(self.new().record_success(receipt=receipt))
        self.assertEqual(self.state(),('RECOVERY',1,2,0))

    def make_snapshot(self, sid):
        g=Generation(account_key(ACCOUNT),self.boot,1,2,3,4,5)
        now_mono=int(self.mono*NS);now_utc=int(self.utc*NS)
        base=snap()
        # Construct a new coherent capture, never change a previously counted ID.
        def move(b):
            return replace(b,request_started_mono_ns=now_mono,received_mono_ns=now_mono,
                           request_started_utc_ns=now_utc,received_utc_ns=now_utc)
        s=replace(base,snapshot_id=sid,generation=g,
                  capture_started_mono_ns=now_mono,capture_finished_mono_ns=now_mono,
                  capture_started_utc_ns=now_utc,capture_finished_utc_ns=now_utc,
                  profile_valid_until_mono_ns=now_mono+600*NS,
                  profile=move(base.profile), holdings=move(base.holdings),positions=move(base.positions),
                  trade_book=move(base.trade_book),order_book=move(base.order_book),
                  forever_orders=move(base.forever_orders),super_orders=move(base.super_orders))
        return s,g

    def test_quiet_time_distinct_gets_and_distinct_snapshots_all_required(self):
        self.g.throttled(None)
        self.advance(60)
        for _ in range(10):
            receipt=self.g.admit()
            self.assertTrue(self.g.record_success(receipt=receipt))
            self.advance(.5)
        s,g=self.make_snapshot('first')
        self.assertTrue(self.g.record_coherent_snapshot(s,g))
        self.assertFalse(self.g.record_coherent_snapshot(s,g))
        s,g=self.make_snapshot('second')
        self.assertTrue(self.g.record_coherent_snapshot(s,g))
        self.assertEqual(self.state(),('RECOVERY',1,10,2))
        self.advance(535)
        receipt=self.g.admit()
        self.assertTrue(self.g.record_success(receipt=receipt))
        self.assertEqual(self.state(),('NORMAL',0,0,0))

    def test_actual_429_escalates_after_one_success(self):
        for strike in range(1,8):
            delay=self.g.throttled(None)
            self.assertEqual(delay,60*2**(strike-1))
            self.advance(delay)
            receipt=self.g.admit()
            self.assertTrue(self.g.record_success(receipt=receipt))
            self.advance(.5)
        self.assertEqual(self.g.throttled(None),3840)
        self.assertEqual(self.g.throttled('7200'),7200)
        self.assertEqual(self.state(),('COOLDOWN',7,0,0))

    def test_forward_wall_jump_and_reboot_do_not_release_cooldown(self):
        self.g.throttled(None)
        self.utc += 3600
        with self.assertRaises(ReadDeferred): self.new().admit()
        self.advance(61)
        with self.assertRaises(ReadDeferred): self.new(boot='new-boot').admit()
        self.assertEqual(self.state(),('COOLDOWN',1,0,0))

    def test_malformed_http_200_never_opens_probe(self):
        from tests.test_r02_read_governor import ReadControlTests, response
        from src.models import BrokerReadFailure
        fixture=ReadControlTests();fixture.setUp()
        try:
            c=fixture.client()
            fixture.g.throttled(None)
            fixture.now+=60
            c.session.get.return_value=response(200,'{"bad":"shape"}')
            self.assertIsInstance(c.get_holdings(),BrokerReadFailure)
            c.session.get.return_value=response(200,'[]')
            self.assertIsInstance(c.get_holdings(),BrokerReadFailure)
            self.assertEqual(c.session.get.call_count,1)
            fixture.now+=6
            self.assertEqual(c.get_holdings(),[])
        finally:
            fixture.doCleanups()
