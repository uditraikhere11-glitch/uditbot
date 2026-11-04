"""
Unit tests for STEP 23 (Singleton Process Lease) and STEP 24 (Read-Only SSH Dashboard).
"""

import io
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.config import AppConfig
from src.engine.lock import SingletonListenerLock
from src.engine.emergency_stop import EmergencyStopManager
from src.journal.db import JournalDB
from src.cli import cmd_dashboard, cmd_check_dhan


class TestStep23And24(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.lock_file = os.path.join(self.temp_dir.name, "listener.lock")
        self.db_path = os.path.join(self.temp_dir.name, "journal.db")
        self.stop_file = os.path.join(self.temp_dir.name, "stop.json")

        self.journal = JournalDB(self.db_path)
        self.stop_mgr = EmergencyStopManager(self.stop_file, fail_closed_if_missing=True)
        self.stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_23_b_singleton_lock_blocks_second_instance(self):
        """23-B: Second listener instance fails to acquire lock (prevents duplicate orders)."""
        lock1 = SingletonListenerLock(self.lock_file)
        ok1, msg1 = lock1.acquire()
        self.assertTrue(ok1)
        self.assertTrue(os.path.exists(self.lock_file))

        # Second instance attempts acquisition while lock1 is held
        lock2 = SingletonListenerLock(self.lock_file)
        ok2, msg2 = lock2.acquire()
        self.assertFalse(ok2)
        self.assertIn("blocked", msg2.lower())

        # Clean release allows subsequent acquisition
        lock1.release()
        ok3, msg3 = lock2.acquire()
        self.assertTrue(ok3)
        lock2.release()

    def test_24_a_dashboard_read_only_metrics(self):
        """24-A & 24-C: Metrics aggregated accurately from journal without broker mutation."""
        # Insert test fixtures
        self.journal.record_signal(
            channel_id=-1001, message_id=1, signal_type="EQUITY",
            symbol="SBIN", action="BUY_ENTRY", raw_text="buy sbin",
            status="PARSED", reason="ok"
        )
        self.journal.record_signal(
            channel_id=-1001, message_id=2, signal_type="OPTION",
            symbol="INFY", action="BUY_ENTRY", raw_text="buy infy",
            status="PARSED", reason="ok"
        )
        self.journal.record_order(
            correlation_id="EQ-SBIN-1", trading_mode="LIVE",
            product_type="CNC", exchange_segment="NSE_EQ",
            symbol="SBIN", security_id="3045", order_type="LIMIT",
            price=800.0, quantity=10, status="TRADED"
        )
        self.journal.record_order(
            correlation_id="OPT-INFY-1", trading_mode="TEST",
            product_type="MARGIN", exchange_segment="NSE_FNO",
            symbol="INFY", security_id="54232", order_type="LIMIT",
            price=25.0, quantity=300, status="TEST_VALIDATED"
        )

        metrics = self.journal.get_dashboard_summary()
        self.assertEqual(metrics["signals_all"], 2)
        self.assertEqual(metrics["orders_live"], 1)
        self.assertEqual(metrics["orders_test"], 1)
        self.assertEqual(metrics["orders_eq"], 1)
        self.assertEqual(metrics["orders_fno"], 1)

    def test_24_d_cmd_dashboard_output(self):
        """24-D: Dashboard displays formatted status without errors or network calls."""
        cfg = AppConfig.from_env()
        output_capture = io.StringIO()
        with patch("sys.stdout", output_capture):
            cmd_dashboard(cfg, self.journal, self.stop_mgr)

        out = output_capture.getvalue()
        self.assertIn("AUDIT BOT READ-ONLY SSH DASHBOARD", out)
        self.assertIn("Operating Mode", out)
        self.assertIn("Emergency STOP", out)
        self.assertIn("Signals Received", out)

    def test_cmd_check_dhan_output(self):
        """Verifies check-dhan diagnostic displays token, marketfeed, and execution readiness."""
        cfg = AppConfig.from_env()
        output_capture = io.StringIO()
        with patch("sys.stdout", output_capture):
            cmd_check_dhan(cfg, self.stop_mgr)

        out = output_capture.getvalue()
        self.assertIn("DHAN API CONNECTION & EXECUTION DIAGNOSTIC", out)
        self.assertIn("Testing Dhan API Authentication", out)
        self.assertIn("Testing Market Data Feed", out)
        self.assertIn("Order Execution Readiness Matrix", out)


if __name__ == "__main__":
    unittest.main()
