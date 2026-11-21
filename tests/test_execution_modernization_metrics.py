"""Offline read-only latency comparison test with a synthetic SQLite journal."""
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from tools.execution_modernization_metrics import collect, compare, describe


class TestExecutionMeasurement(unittest.TestCase):
    def test_percentiles_and_small_samples(self):
        self.assertEqual(describe([])["p95"],None)
        d=describe([10,20,30,40,50])
        self.assertEqual(d["p50"],30)
        self.assertEqual(d["p95"],48)

    def test_read_only_collection(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/"journal.db"
            with sqlite3.connect(p) as conn:
                conn.execute("CREATE TABLE signal_latency_telemetry ("
                             "ist_received_time REAL, telegram_to_ack_ms REAL, "
                             "ack_to_fill_ms REAL)")
                conn.executemany(
                    "INSERT INTO signal_latency_telemetry VALUES (?,?,?)",
                    [(1763000000+i, float(10+i), None) for i in range(40)],
                )
            before=p.read_bytes()
            r=collect(p,limit=40)
            self.assertEqual(r["sample_rows"],40)
            self.assertEqual(r["metrics_ms"]["telegram_to_ack_ms"]["n"],40)
            self.assertEqual(r["metrics_ms"]["ack_to_fill_ms"]["n"],0)
            self.assertEqual(before,p.read_bytes())

    def test_missing_journal_is_explicit_error(self):
        with self.assertRaises(FileNotFoundError):
            collect("/definitely/nonexistent/sqlite/dataset.db")

    def test_comparison_never_certifies_production(self):
        b={"sample_rows":40,"metrics_ms":{"telegram_to_ack_ms":describe([100]*40)}}
        c={"sample_rows":40,"metrics_ms":{"telegram_to_ack_ms":describe([80]*40)}}
        o=compare(b,c)
        self.assertFalse(o["production_approved"])
        self.assertTrue(o["metrics"]["telegram_to_ack_ms"]["minimum_sample_gate"])
        self.assertEqual(o["metrics"]["telegram_to_ack_ms"]["p95"]["change_pct"],-20)

    def test_small_comparison_sample_fails_minimum_gate(self):
        b={"sample_rows":2,"metrics_ms":{"test":describe([10,11])}}
        c={"sample_rows":2,"metrics_ms":{"test":describe([9,10])}}
        self.assertFalse(compare(b,c)["metrics"]["test"]["minimum_sample_gate"])
