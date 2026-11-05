"""
Unit tests for STEP 23 (Singleton Process Lease) and STEP 24 (Read-Only SSH Dashboard).
"""

import io
import os
import socket
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import tests
from tests import (
    OfflineNetworkIsolationError,
    allow_blocked_network_probe,
    get_recorded_network_attempts,
    install_offline_network_guard,
)
from src.broker.yahoo_observer import YahooObservation, YahooObserver
from src.config import AppConfig
from src.engine.lock import SingletonListenerLock
from src.engine.emergency_stop import EmergencyStopManager
from src.journal.db import JournalDB
from src.cli import cmd_dashboard, cmd_check_dhan

install_offline_network_guard()


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
        """Verifies check-dhan diagnostic displays token, marketfeed, Yahoo observation, and execution readiness without network calls."""
        cfg = AppConfig.from_env()
        output_capture = io.StringIO()

        mock_dhan_instance = MagicMock()
        mock_dhan_instance.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.mock_dhan_jwt_token"
        mock_dhan_instance.last_funds_is_fallback = False
        mock_dhan_instance.get_fund_limits.return_value = (True, 150000.00, "OK")
        mock_dhan_instance.get_ltp.return_value = (True, 2950.50, "OK")

        mock_yahoo_obs = YahooObservation(
            ticker="RELIANCE.NS",
            symbol="RELIANCE",
            exchange_segment="NSE_EQ",
            reported_price=2951.25,
            quote_timestamp=1762270000.0,
            measured_age_sec=1.20,
            exchange="NSI",
            currency="INR",
            market_state="REGULAR",
            delayed_by_sec=0,
            delay_status="REALTIME",
            is_success=True,
            status_code=200,
            executable=False,
        )
        mock_yahoo_instance = MagicMock()
        mock_yahoo_instance.observe.return_value = mock_yahoo_obs

        with patch("src.broker.dhan_client.DhanClient", return_value=mock_dhan_instance) as mock_dhan_cls, \
             patch("src.broker.yahoo_observer.YahooObserver", return_value=mock_yahoo_instance) as mock_yahoo_cls, \
             patch("sys.stdout", output_capture):
            cmd_check_dhan(cfg, self.stop_mgr)

        out = output_capture.getvalue()
        self.assertIn("DHAN API CONNECTION & EXECUTION DIAGNOSTIC", out)
        self.assertIn("Access Token        : PRESENT", out)
        self.assertIn("Testing Dhan API Authentication", out)
        self.assertIn("[OK] AUTHENTICATION SUCCESSFUL (BROKER CONFIRMED)", out)
        self.assertIn("Available Ledger Balance: INR 150,000.00", out)
        self.assertIn("Testing Market Data Feed", out)
        self.assertIn("[OK] MARKETFEED CONNECTED", out)
        self.assertIn("Live Quote for RELIANCE (Sec ID 2885): INR 2,950.50", out)
        self.assertIn("Yahoo Finance Observational Diagnostic (Darvas Observer)", out)
        self.assertIn("YAHOO_FINANCE_DARVAS_OBSERVER (executable=False)", out)
        self.assertIn("Reported Price      : INR 2,951.25", out)
        self.assertIn("Market State        : REGULAR (Exchange: NSI)", out)
        self.assertIn("Execution Status    : OBSERVATION ONLY (Not authorized for order execution)", out)
        self.assertIn("Order Execution Readiness Matrix", out)
        self.assertIn("EMERGENCY STOP        : INACTIVE (Trading gate open)", out)
        self.assertIn("STATUS: TEST MODE ACTIVE", out)
        self.assertNotIn("[FAIL]", out)
        self.assertNotIn("[SKIPPED]", out)

        mock_dhan_cls.assert_called_once_with(cfg, stop_manager=self.stop_mgr)
        mock_dhan_instance.get_fund_limits.assert_called_once_with()
        mock_dhan_instance.get_ltp.assert_called_once_with("NSE_EQ", "2885", symbol="RELIANCE")
        mock_yahoo_cls.assert_called_once_with()
        mock_yahoo_instance.observe.assert_called_once_with("RELIANCE", exchange_segment="NSE_EQ")
        self.assertEqual(get_recorded_network_attempts(), [])

    def test_24_e_offline_network_blocking_safeguard_rejects_unmocked_external_connections(self):
        """Verifies the offline network-blocking safeguard rejects unmocked Yahoo/Dhan DNS and TCP connections."""
        with allow_blocked_network_probe():
            with self.assertRaises(OfflineNetworkIsolationError):
                socket.getaddrinfo("query1.finance.yahoo.com", 443)
            with self.assertRaises(OfflineNetworkIsolationError):
                socket.create_connection(("api.dhan.co", 443), timeout=0.1)

            observer = YahooObserver()
            obs = observer.observe("RELIANCE", exchange_segment="NSE_EQ", timeout=0.1)
            self.assertFalse(obs.is_success)
            self.assertIn("Unmocked external network call blocked", obs.error or "")


if __name__ == "__main__":
    unittest.main()
