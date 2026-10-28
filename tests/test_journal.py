"""
Unit tests for SQLite Audit Journal and Persistence.
"""

import os
import tempfile
import unittest
from src.journal.db import JournalDB


class TestJournalDB(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_journal.db")
        self.journal = JournalDB(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_record_signal_and_retrieve(self):
        sig_id = self.journal.record_signal(
            channel_id=-10012345,
            message_id=987,
            signal_type="EQUITY",
            symbol="SBIN",
            action="BUY_ENTRY",
            raw_text="Buy #SBIN @ 805",
            status="PARSED",
            reason="Regex matched",
        )
        self.assertGreater(sig_id, 0)

        recent = self.journal.get_recent_signals(limit=5)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["symbol"], "SBIN")
        self.assertEqual(recent[0]["action"], "BUY_ENTRY")

    def test_record_validation(self):
        val_id = self.journal.record_validation(
            signal_id=1,
            symbol="TATAMOTORS",
            limit_price=950.0,
            units=25,
            total_quantity=25,
            total_cost=23750.0,
            is_passed=True,
            rejection_reason="Passed all risk bounds",
        )
        self.assertGreater(val_id, 0)

    def test_record_order_and_zero_fake_fills(self):
        order_id = self.journal.record_order(
            correlation_id="TEST-CORR-001",
            trading_mode="TEST",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="INFY",
            security_id="1594",
            order_type="LIMIT",
            price=1500.0,
            quantity=15,
            status="TEST_VALIDATED",
        )
        self.assertGreater(order_id, 0)

        # In TEST mode, orders table records 'TEST_VALIDATED', and fills table remains 0 (NO FAKE FILLS)
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM fills")
            self.assertEqual(cur.fetchone()[0], 0, "Fills table must be 0 (no simulated/fake fills in TEST mode)")

    def test_record_system_event(self):
        evt_id = self.journal.record_system_event(
            event_type="EMERGENCY_STOP",
            description="Operator halt command received",
        )
        self.assertGreater(evt_id, 0)


if __name__ == "__main__":
    unittest.main()
