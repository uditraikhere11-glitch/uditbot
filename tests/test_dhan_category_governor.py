"""DhanHQ v2 API category accounting: isolated, no live broker network."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock

from src.broker.read_governor import ReadDeferred, ReadGovernor


class CategoryGovernorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100000.0
        self.g = ReadGovernor(self.tmp.name, 'ACCOUNT_TEST', clock=lambda: self.now)

    def test_exact_documented_category_limits(self):
        self.assertEqual(self.g.CATEGORY_LIMITS['ORDER'],
                         ((1, 10), (60, 250), (3600, 1000), (86400, 7000)))
        self.assertEqual(self.g.CATEGORY_LIMITS['DATA'], ((1, 5), (86400, 100000)))
        self.assertEqual(self.g.CATEGORY_LIMITS['QUOTE'], ((1, 1),))
        self.assertEqual(self.g.CATEGORY_LIMITS['NON_TRADING'], ((1, 20),))
        self.assertIsNone(self.g.LIMITS)

    def test_endpoint_classification_read_vs_mutation(self):
        checks = [
            ('GET','https://api.dhan.co/v2/orders','NON_TRADING'),
            ('GET','https://api.dhan.co/v2/orders/external/123','NON_TRADING'),
            ('GET','https://api.dhan.co/v2/trades','NON_TRADING'),
            ('GET','https://api.dhan.co/v2/fundlimit','NON_TRADING'),
            ('GET','https://api.dhan.co/v2/holdings','NON_TRADING'),
            ('GET','https://api.dhan.co/v2/profile','NON_TRADING'),
            ('POST','https://api.dhan.co/v2/orders','ORDER'),
            ('DELETE','https://api.dhan.co/v2/forever/orders/id','ORDER'),
            ('PUT','https://api.dhan.co/v2/super/orders/id','ORDER'),
            ('POST','https://api.dhan.co/v2/marketfeed/ltp','QUOTE'),
            ('POST','https://api.dhan.co/v2/charts/historical','DATA'),
        ]
        for method, url, category in checks:
            with self.subTest(method=method,url=url):
                self.assertEqual(ReadGovernor.classify(method,url),category)
        with self.assertRaises(ValueError):
            ReadGovernor.classify('DELETE','https://api.dhan.co/v2/unknown')

    def test_twenty_reads_per_second_but_not_21(self):
        for _ in range(20):
            self.g.admit(category='NON_TRADING')
        with self.assertRaises(ReadDeferred):
            self.g.admit(category='NON_TRADING')
        self.now += 1.01
        self.g.admit(category='NON_TRADING')

    def test_four_categories_independent(self):
        for _ in range(20):self.g.admit(category='NON_TRADING')
        for _ in range(10):self.g.admit(category='ORDER')
        for _ in range(5):self.g.admit(category='DATA')
        self.g.admit(category='QUOTE')
        for cat in self.g.CATEGORY_LIMITS:
            with self.subTest(cat=cat):
                with self.assertRaises(ReadDeferred):self.g.admit(category=cat)
        self.now += 1.1
        for cat in self.g.CATEGORY_LIMITS:self.g.admit(category=cat)

    def test_daily_order_cap_is_not_a_get_cap(self):
        self.g.admit(category='ORDER')
        with sqlite3.connect(self.g.path) as db:
            db.executemany('INSERT INTO reads(at,category) VALUES (?,?)',
                           [(self.now-4000,'ORDER')]*6998)
        self.g.admit(category='ORDER')
        with self.assertRaises(ReadDeferred):self.g.admit(category='ORDER')
        for _ in range(20):self.g.admit(category='NON_TRADING')
        self.assertEqual(self.g.seconds_until_capacity(required=1,
                          category='NON_TRADING',priority='urgent'),1.01)

    def test_old_5225_gets_migrate_without_fake_24h_block(self):
        self.g.directory.mkdir(exist_ok=True)
        with sqlite3.connect(self.g.path) as db:
            db.execute('CREATE TABLE reads (at REAL NOT NULL)')
            db.execute('CREATE TABLE cooldown (id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
            db.executemany('INSERT INTO reads(at) VALUES (?)',[(self.now-100,)]*5225)
        self.assertEqual(self.g.seconds_until_capacity(category='NON_TRADING'),0)
        self.g.admit(category='NON_TRADING')
        with sqlite3.connect(self.g.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reads').fetchone()[0],5226)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reads WHERE category='NON_TRADING'").fetchone()[0],5226)

    def test_old_shared_broker_cooldown_is_preserved(self):
        self.g.directory.mkdir(exist_ok=True)
        with sqlite3.connect(self.g.path) as db:
            db.execute('CREATE TABLE reads (at REAL NOT NULL)')
            db.execute('CREATE TABLE cooldown (id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
            db.execute('INSERT INTO cooldown VALUES (1,?,2)',(self.now+120,))
        with self.assertRaises(ReadDeferred):self.g.admit(category='ORDER')
        with self.assertRaises(ReadDeferred):self.g.admit(category='NON_TRADING')
        self.now += 121
        self.g.admit(category='NON_TRADING')

    def test_genuine_quote_429_does_not_exhaust_nontrading(self):
        self.g.throttled(120,category='QUOTE')
        with self.assertRaises(ReadDeferred):self.g.admit(category='QUOTE')
        self.g.admit(category='NON_TRADING')
        self.g.record_success(category='NON_TRADING')
        self.now += 121
        self.g.admit(category='QUOTE')

    def test_read_only_preview_does_not_insert_or_migrate(self):
        self.g.directory.mkdir(exist_ok=True)
        with sqlite3.connect(self.g.path) as db:
            db.execute('CREATE TABLE reads (at REAL NOT NULL)')
            db.execute('CREATE TABLE cooldown (id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
            db.execute('INSERT INTO reads(at) VALUES (?)',(self.now,))
        self.g.seconds_until_capacity(required=8,category='NON_TRADING')
        with sqlite3.connect(self.g.path) as db:
            self.assertEqual(db.execute('PRAGMA table_info(reads)').fetchall()[-1][1],'at')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reads').fetchone()[0],1)

    def test_batch_preview_never_permanently_denies_over_20_plan(self):
        self.assertEqual(self.g.seconds_until_capacity(required=40,category='NON_TRADING'),0)
        for _ in range(16):self.g.admit(category='NON_TRADING')
        self.assertGreater(self.g.seconds_until_capacity(required=40,category='NON_TRADING'),0)

    def test_cross_process_contends_without_exceeding_per_second(self):
        def work(n):
            g=ReadGovernor(self.tmp.name,'ACCOUNT_TEST',clock=lambda:self.now)
            try:
                g.admit(category='NON_TRADING',priority='urgent')
                return True
            except ReadDeferred:
                return False
        with ThreadPoolExecutor(max_workers=5) as pool:
            result=list(pool.map(work,range(25)))
        self.assertEqual(sum(result),20)

    def test_corrupt_read_store_is_never_a_zero_used_window(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as db:db.execute('DROP TABLE reads')
        with self.assertRaises(sqlite3.Error):self.g.seconds_until_capacity()


if __name__ == '__main__':
    unittest.main()

class ModificationCapTests(unittest.TestCase):
    def test_25_modifications_per_order_durable_and_account_scoped(self):
        with tempfile.TemporaryDirectory() as d:
            now=[100000.0]
            g=ReadGovernor(d,'ACCOUNT_A',clock=lambda:now[0])
            oid=ReadGovernor.modification_order_id('PUT','https://api.dhan.co/v2/orders/ORD-123')
            self.assertEqual(oid,'ORD-123')
            for _ in range(25):
                g.admit(category='ORDER',priority='urgent',modification_order_id=oid)
                now[0] += 1.1
            with self.assertRaisesRegex(ReadDeferred,'25 modifications'):
                g.admit(category='ORDER',priority='urgent',modification_order_id=oid)
            ReadGovernor(d,'ACCOUNT_B',clock=lambda:now[0]).admit(
                category='ORDER',modification_order_id=oid)
