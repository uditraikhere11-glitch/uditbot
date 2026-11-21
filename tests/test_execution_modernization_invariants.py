"""Read-only journal-invariant checks on synthetic SQLite data only."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from tools.execution_invariant_audit import audit_journal, SchemaUnavailable


class TestJournalInvariantAudit(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=Path(self.tmp.name)/"db.sqlite"
        with sqlite3.connect(self.db) as c:
            c.executescript("""
            CREATE TABLE positions_lifecycle (
                parent_order_id TEXT, buy_filled_qty INTEGER,sell_filled_qty INTEGER,
                confirmed_open_qty INTEGER,position_state TEXT);
            CREATE TABLE fills (
                exchange_trade_id TEXT,traded_quantity INTEGER,is_verified_trade INTEGER);
            CREATE TABLE exit_allocations (exchange_trade_id TEXT,allocated_qty INTEGER);
            CREATE TABLE order_intents (correlation_id TEXT,state TEXT);
            CREATE TABLE protection_attempts (state TEXT);
            """)

    def tearDown(self):
        self.tmp.cleanup()

    def test_correct_journal_is_not_falsely_flagged(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO positions_lifecycle VALUES ('BUY',3,1,2,'ACTIVE')")
            c.execute("INSERT INTO fills VALUES ('E1',1,1)")
            c.execute("INSERT INTO exit_allocations VALUES ('E1',1)")
        before=self.db.read_bytes()
        r=audit_journal(self.db)
        self.assertEqual(r["hard_inconsistency_count"],0)
        self.assertEqual(r["position_rows"],1)
        self.assertFalse(r["production_approved"])
        self.assertEqual(before,self.db.read_bytes())

    def test_arithmetic_and_allocation_errors_detected(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO positions_lifecycle VALUES ('B1',5,2,5,'OPEN')")
            c.execute("INSERT INTO positions_lifecycle VALUES ('B2',2,3,-1,'OPEN')")
            c.execute("INSERT INTO fills VALUES ('E1',1,1)")
            c.execute("INSERT INTO exit_allocations VALUES ('E1',2)")
        r=audit_journal(self.db)
        self.assertEqual(r["position_arithmetic_mismatch"],1)
        self.assertEqual(r["position_quantity_invalid"],1)
        self.assertEqual(r["exit_overallocated"],1)
        self.assertEqual(r["hard_inconsistency_count"],3)

    def test_unresolved_state_is_not_counted_as_hard_inconsistency(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO order_intents VALUES ('C1','UNKNOWN')")
            c.execute("INSERT INTO protection_attempts VALUES ('UNKNOWN')")
            c.execute("INSERT INTO positions_lifecycle VALUES ('P1',1,0,1,'UNPROTECTED_MANUAL_RISK')")
        r=audit_journal(self.db)
        self.assertEqual(r["hard_inconsistency_count"],0)
        self.assertEqual(r["unresolved_intent_count"],1)
        self.assertEqual(r["unresolved_protection_attempt_count"],1)
        self.assertEqual(r["acknowledged_manual_risk_lot_count"],1)

    def test_missing_schema_fails_closed(self):
        with sqlite3.connect(self.db) as c:
            c.execute("DROP TABLE fills")
        with self.assertRaises(SchemaUnavailable):
            audit_journal(self.db)
