"""Dhan rate-governor fast path must preserve cross-process durable semantics.

No Dhan API calls. Tests use a synthetic isolated account and temporary SQLite.
"""
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from src.broker.read_governor import ReadGovernor, ReadDeferred


class TestGovFastConnection(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.clock=[400000.]
        self.g=ReadGovernor(self.tmp.name,"SYNTHETIC_ACCOUNT",
                            clock=lambda: self.clock[0])

    def tearDown(self):
        self.tmp.cleanup()

    def test_original_schema_barrier_only_on_first_use(self):
        self.g.admit(category="NON_TRADING")
        self.assertIsNotNone(self.g._verified_file_key)
        # Even if the schema flock becomes unavailable, subsequent broker
        # reads can use the already-verified ledger safely.
        with patch.object(self.g,"_schema_initialization_lock",
                          side_effect=AssertionError("unexpected schema lock")):
            self.clock[0]+=1.1
            self.g.admit(category="NON_TRADING")
        with sqlite3.connect(self.g.path) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) FROM reads").fetchone()[0],2)

    def test_second_admission_obeys_one_second_ceiling(self):
        for _ in range(20):
            self.g.admit(category="NON_TRADING")
        with self.assertRaises(ReadDeferred):
            self.g.admit(category="NON_TRADING")
        self.clock[0]+=1.01
        self.g.admit(category="NON_TRADING")

    def test_replaced_ledger_never_resets_quota(self):
        self.g.admit()
        original=self.g.path
        renamed=Path(self.tmp.name)/"backup.sqlite3"
        os.replace(original,renamed)
        with sqlite3.connect(original) as c:
            c.execute("CREATE TABLE irrelevant(id INTEGER)")
        with self.assertRaises(sqlite3.DatabaseError):
            self.g.admit()
        self.assertFalse(self.g._verified_file_key is None)

    def test_deleted_ledger_does_not_recreate_empty_quota_db(self):
        self.g.admit()
        os.unlink(self.g.path)
        with self.assertRaises(sqlite3.DatabaseError):
            self.g.admit()
        self.assertFalse(self.g.path.exists())

    def test_deleted_category_cooldown_table_fails_closed(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as c:
            c.execute("DROP TABLE category_cooldown")
        with self.assertRaises(sqlite3.Error):
            self.g.admit()

    def test_dropped_modification_history_blocks_even_reads(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as c:
            c.execute("DROP TABLE order_modifications")
        with self.assertRaises(sqlite3.DatabaseError):
            self.g.admit(category="NON_TRADING")

    def test_unauthorized_schema_change_blocks_fast_path(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as c:
            c.execute("CREATE TABLE unrelated_change (id INTEGER)")
        with self.assertRaises(sqlite3.DatabaseError):
            self.g.admit()

    def test_truncated_ledger_fails_closed(self):
        self.g.admit()
        with open(self.g.path,"wb") as f:
            f.truncate(0)
        with self.assertRaises(sqlite3.DatabaseError):
            self.g.admit()

    def test_persisted_cooldown_still_honored_by_fast_path(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as c:
            c.execute("INSERT OR REPLACE INTO category_cooldown "
                      "(category,until,strikes) VALUES('NON_TRADING',?,1)",
                      (self.clock[0]+10,))
        with self.assertRaises(ReadDeferred):
            self.g.admit()
        self.clock[0]+=10.1
        self.g.admit()

    def test_second_governor_instance_preserves_shared_admissions(self):
        self.g.admit()
        other=ReadGovernor(self.tmp.name,"SYNTHETIC_ACCOUNT",
                           clock=lambda: self.clock[0])
        for _ in range(19):
            other.admit()
        with self.assertRaises(ReadDeferred):
            self.g.admit()
