"""Operator snapshot benchmark never mutates or reports order identities."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.journal.db import JournalDB
from tools.measure_risk_query_snapshot import measure_snapshot


class TestOperatorSnapshotBenchmark(unittest.TestCase):
    def test_readonly_fixture_and_equivalence(self):
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/"approved-snapshot.sqlite3"
            JournalDB(str(source),account_id="SYNTHETIC")
            with sqlite3.connect(source) as c:
                c.execute(
                    "INSERT INTO orders(correlation_id,broker_order_id,trading_mode,"
                    "security_id,price,quantity,status,created_at) "
                    "VALUES('TEST-CORR','TEST-BROKER','LIVE','TEST-SEC',1.0,1,"
                    "'PENDING',1800000005)"
                )
                c.execute(
                    "INSERT INTO fills(broker_order_id,exchange_trade_id,"
                    "traded_quantity) VALUES('TEST-BROKER','TEST-EXEC',1)"
                )
                c.executemany(
                    "INSERT INTO fills(broker_order_id,exchange_trade_id,"
                    "traded_quantity) VALUES(?,?,?)",
                    [(f"OLD-{i}",f"TRADE-{i}",1) for i in range(1200)],
                )
            before=sqlite3.connect(source).execute(
                "SELECT COUNT(*) FROM fills").fetchone()[0]
            report=measure_snapshot(source,start_of_day=1800000000,rounds=3)
            after=sqlite3.connect(source).execute(
                "SELECT COUNT(*) FROM fills").fetchone()[0]
            self.assertEqual(before,after)
            self.assertTrue(report["indexed_candidate_available"])
            self.assertTrue(report["risk_fill_sums_equivalent"])
            self.assertEqual(report["current_day_distinct_broker_orders"],1)
            self.assertFalse(report["production_approved"])
            self.assertNotIn("TEST-BROKER",str(report))
            self.assertNotIn("TEST-EXEC",str(report))

    def test_missing_file_fails(self):
        with self.assertRaises(FileNotFoundError):
            measure_snapshot("/absent/snapshot.sqlite3")
