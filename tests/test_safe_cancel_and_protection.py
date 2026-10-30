"""
Unit tests for STEP 10 (safe cancellation and controlled retry)
and STEP 11 (verified broker protection matrix and truthful status).
Includes Correction 03 regression scenarios (C03-01 through C03-14).
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import requests

from src.config import AppConfig
from src.broker.dhan_client import DhanClient
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import (
    ProtectionWatcher,
    OrderLifecycleState,
)
from src.journal.db import JournalDB
from src.models import OrderRequest, ProductType, TradingMode


class TestSafeCancelAndProtection(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.pending_file = os.path.join(self.temp_dir.name, "pending_protection.json")
        self.db_path = os.path.join(self.temp_dir.name, "journal.db")
        self.journal = JournalDB(self.db_path)
        self.mock_broker = MagicMock()

        self.watcher = ProtectionWatcher(
            broker=self.mock_broker,
            sl_pct=3.0,
            target_pct=3.0,
            sl_limit_buffer_pct=1.0,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=self.journal,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_10_a_safe_cancellation_lifecycle(self):
        """10-A: Cancel request transitions to CANCEL_PENDING; broker DELETE response is not terminal."""
        oid = self.watcher.track(
            order_id="ORD-CANCEL-1",
            symbol="INFY",
            security_id="1594",
            segment="NSE_EQ",
            qty=100,
            product_type="CNC",
        )
        self.mock_broker.cancel_order.return_value = (True, "Order cancellation requested")

        ok, msg = self.watcher.request_cancel(oid)
        self.assertTrue(ok)
        rec = self.watcher.pending[oid]
        self.assertEqual(rec["state"], OrderLifecycleState.CANCEL_PENDING.value)
        # Order must still be tracked!
        self.assertIn(oid, self.watcher.pending)

    def test_10_c_partial_fill_before_cancel(self):
        """10-C: Confirms partial fills protected before terminal cancellation."""
        oid = self.watcher.track(
            order_id="ORD-PART-1",
            symbol="SBIN",
            security_id="3045",
            segment="NSE_EQ",
            qty=100,
            product_type="CNC",
        )
        # 40 shares filled, then order cancelled by exchange
        self.mock_broker.order_status.return_value = {
            "orderStatus": "CANCELLED",
            "filledQty": 40,
            "averageTradedPrice": 800.0,
        }
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-SBIN-40", "Success")

        self.watcher.check_once()

        # 40 shares protected
        self.mock_broker.place_forever_oco.assert_called_once()
        # After protecting the 40 shares, order is now safely terminal and removed
        self.assertNotIn(oid, self.watcher.pending)

        # Check SQLite journal recorded the protection order
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT quantity, forever_order_id FROM protection_orders WHERE symbol = 'SBIN'")
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["quantity"], 40)
            self.assertEqual(row["forever_order_id"], "OCO-SBIN-40")

    def test_10_d_one_controlled_retry_for_remaining_qty(self):
        """10-D: Single retry approved for remaining quantity only after terminal cancel."""
        oid = self.watcher.track(
            order_id="ORD-RETRY-1",
            symbol="TATAMOTORS",
            security_id="3456",
            segment="NSE_EQ",
            qty=100,
            product_type="CNC",
            dry_price=950.0,
            original_market_price=950.0,
        )

        # Simulate 30 shares filled, then cancelled
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value
        self.watcher.pending[oid]["protected_qty"] = 30

        # Evaluate retry: market moved to 955.0 (+0.53% < 1.00% ceiling)
        # Remaining shares = 100 - 30 = 70
        can_retry, retry_qty, reason = self.watcher.evaluate_retry(oid, current_market_price=955.0)
        self.assertTrue(can_retry)
        self.assertEqual(retry_qty, 70)

        # Second retry attempt is hard blocked (max 1 retry)
        can_retry2, retry_qty2, reason2 = self.watcher.evaluate_retry(oid, current_market_price=955.0)
        self.assertFalse(can_retry2)
        self.assertIn("Retry limit reached", reason2)

    def test_10_e_price_movement_ceiling_blocks_retry(self):
        """Section B: Price move >= 1.00% is rejected; strictly < 1.00% is eligible for single retry."""
        oid = self.watcher.track(
            order_id="ORD-PRICE-MOVE",
            symbol="RELIANCE",
            security_id="2885",
            segment="NSE_EQ",
            qty=50,
            original_market_price=2500.0,
        )
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value

        # 1. Price moved to 2530 (+1.20% >= 1.00% ceiling) -> REJECTS retry
        can_retry, retry_qty, reason = self.watcher.evaluate_retry(oid, current_market_price=2530.0)
        self.assertFalse(can_retry)
        self.assertIn("1.00%", reason)

        # 2. Price moved to 2510 (+0.40% < 1.00% ceiling) -> approves single retry
        can_retry2, retry_qty2, reason2 = self.watcher.evaluate_retry(oid, current_market_price=2510.0)
        self.assertTrue(can_retry2)
        self.assertEqual(retry_qty2, 50)

    def test_11_e_unprotected_exposure_failsafe(self):
        """11-E: Broker protection rejection triggers fail-safe retry backoff and alert."""
        oid = self.watcher.track(
            order_id="ORD-REJECT-PROT",
            symbol="TCS",
            security_id="11536",
            segment="NSE_EQ",
            qty=20,
        )
        self.mock_broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 20,
            "averageTradedPrice": 3200.0,
        }
        # Broker rejects Forever OCO placement
        self.mock_broker.place_forever_oco.return_value = (False, None, "Dhan Error: RMS Limit Exceeded")

        changed = self.watcher.check_once()
        self.assertTrue(changed)
        rec = self.watcher.pending[oid]
        # Filled shares remain unprotected
        self.assertEqual(rec["protected_qty"], 0)
        # Retry backoff scheduled
        self.assertGreater(rec["retry_at"], 1000.0)

    # =========================================================================
    # CORRECTION 03 FOCUSED REGRESSION SCENARIOS (C03-01 through C03-14)
    # =========================================================================

    @patch("requests.Session.delete")
    def test_c03_01_http_202_acknowledged_state_cancel_pending(self, mock_delete):
        """C03-01: Dhan returns HTTP 202 -> Request acknowledged; state transitions to CANCEL_PENDING."""
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_resp.text = ""
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE

        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        ok, msg = client.cancel_order("ORD-101")
        self.assertTrue(ok)
        self.assertIn("HTTP 202", msg)
        self.assertIn("request accepted", msg)
        mock_delete.assert_called_once()

        # In watcher
        watcher = ProtectionWatcher(broker=client, sl_pct=3.0, target_pct=3.0, path=os.path.join(self.temp_dir.name, "p1.json"))
        oid = watcher.track("ORD-101", "INFY", "1594", "NSE_EQ", 50)
        req_ok, req_msg = watcher.request_cancel(oid)
        self.assertTrue(req_ok)
        self.assertEqual(watcher.pending[oid]["state"], OrderLifecycleState.CANCEL_PENDING.value)

    @patch("requests.Session.delete")
    def test_c03_02_http_200_valid_response_acknowledged(self, mock_delete):
        """C03-02: Dhan returns HTTP 200 with compatible response -> Acknowledged."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "success"}'
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        ok, msg = client.cancel_order("ORD-102")
        self.assertTrue(ok)
        self.assertIn("HTTP 200", msg)

    @patch("requests.Session.delete")
    def test_c03_03_http_400_rejected_not_cancelled(self, mock_delete):
        """C03-03: Dhan returns HTTP 400 -> Rejected, order state is NOT terminal CANCELLED."""
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = '{"error": "Order already traded"}'
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        watcher = ProtectionWatcher(broker=client, sl_pct=3.0, target_pct=3.0, path=os.path.join(self.temp_dir.name, "p2.json"))
        oid = watcher.track("ORD-103", "INFY", "1594", "NSE_EQ", 50)
        ok, msg = watcher.request_cancel(oid)
        self.assertFalse(ok)
        self.assertIn("400", msg)
        self.assertEqual(watcher.pending[oid]["state"], OrderLifecycleState.PENDING.value)
        self.assertFalse(watcher.pending[oid].get("cancel_uncertain", False))

    @patch("requests.Session.delete")
    def test_c03_04_http_401_authentication_failure(self, mock_delete):
        """C03-04: Dhan returns HTTP 401 -> Authentication failure; no claim of cancellation."""
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = '{"error": "invalid token"}'
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        ok, msg = client.cancel_order("ORD-104")
        self.assertFalse(ok)
        self.assertIn("Dhan auth error (401)", msg)

    @patch("requests.Session.delete")
    def test_c03_05_http_429_rate_limit_error(self, mock_delete):
        """C03-05: Dhan returns HTTP 429 -> Rate-limit error; original order remains unresolved in previous state."""
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = '{"error": "Too Many Requests"}'
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        watcher = ProtectionWatcher(broker=client, sl_pct=3.0, target_pct=3.0, path=os.path.join(self.temp_dir.name, "p3.json"))
        oid = watcher.track("ORD-105", "INFY", "1594", "NSE_EQ", 50)
        watcher.pending[oid]["state"] = OrderLifecycleState.PART_FILLED.value
        watcher.pending[oid]["filled_qty"] = 20

        ok, msg = watcher.request_cancel(oid)
        self.assertFalse(ok)
        self.assertIn("429", msg)
        self.assertEqual(watcher.pending[oid]["state"], OrderLifecycleState.PART_FILLED.value)

    @patch("requests.Session.delete")
    def test_c03_06_http_500_broker_error(self, mock_delete):
        """C03-06: Dhan returns HTTP 500 -> Broker error; no assumption of terminal state."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = '{"error": "Internal Server Error"}'
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        ok, msg = client.cancel_order("ORD-106")
        self.assertFalse(ok)
        self.assertIn("500", msg)

    @patch("requests.Session.delete")
    def test_c03_07_network_timeout_no_automatic_second_cancel_or_retry(self, mock_delete):
        """C03-07: Network timeout produces unknown outcome; no automatic second cancellation or retry."""
        mock_delete.side_effect = requests.exceptions.Timeout("Connection timed out waiting for Dhan DELETE")

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        watcher = ProtectionWatcher(broker=client, sl_pct=3.0, target_pct=3.0, path=os.path.join(self.temp_dir.name, "p4.json"))
        oid = watcher.track("ORD-107", "INFY", "1594", "NSE_EQ", 50)
        ok, msg = watcher.request_cancel(oid)

        self.assertFalse(ok)
        self.assertIn("HTTP exception cancelling order", msg)
        self.assertEqual(mock_delete.call_count, 1)  # No automatic second attempt
        self.assertEqual(watcher.pending[oid]["state"], OrderLifecycleState.CANCEL_REQUESTED.value)
        self.assertTrue(watcher.pending[oid].get("cancel_uncertain", False))

        # Retry must be rejected because order is not broker-confirmed cancelled
        can_retry, retry_qty, r_msg = watcher.evaluate_retry(oid, current_market_price=1600.0)
        self.assertFalse(can_retry)
        self.assertEqual(retry_qty, 0)

    def test_c03_08_202_followed_by_pending_remains_cancel_pending(self):
        """C03-08: Order in CANCEL_PENDING polled while broker reports PENDING remains CANCEL_PENDING."""
        oid = self.watcher.track("ORD-108", "INFY", "1594", "NSE_EQ", 100)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        self.mock_broker.order_status.return_value = {
            "orderStatus": "PENDING",
            "filledQty": 0,
            "averageTradedPrice": 0.0,
        }
        self.watcher.check_once()
        self.assertEqual(self.watcher.pending[oid]["state"], OrderLifecycleState.CANCEL_PENDING.value)
        self.assertIn(oid, self.watcher.pending)

    def test_c03_09_202_followed_by_cancelled_broker_confirmed(self):
        """C03-09: Order in CANCEL_PENDING polled when broker reports CANCELLED finalizes terminal cancellation."""
        oid = self.watcher.track("ORD-109", "INFY", "1594", "NSE_EQ", 100)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        self.mock_broker.order_status.return_value = {
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "averageTradedPrice": 0.0,
        }
        self.watcher.check_once()
        # Order closed cleanly as terminal with 0 fills
        self.assertNotIn(oid, self.watcher.pending)

    def test_c03_10_202_followed_by_traded_preserves_executed_qty_no_retry(self):
        """C03-10: Order in CANCEL_PENDING that fills before cancellation completes protects filled qty and permits no retry."""
        oid = self.watcher.track("ORD-110", "TCS", "11536", "NSE_EQ", 50)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        self.mock_broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 50,
            "averageTradedPrice": 3500.0,
        }
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-TRADED-1", "Success")

        self.watcher.check_once()
        self.mock_broker.place_forever_oco.assert_called_once()
        # 50 shares protected
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT quantity, status FROM protection_orders WHERE symbol = 'TCS'")
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["quantity"], 50)
            self.assertEqual(row["status"], "ACTIVE")

    def test_c03_11_202_followed_by_part_traded_preserves_partial_fill(self):
        """C03-11: 202 followed by PART_TRADED preserves partial fill and continues reconciliation."""
        oid = self.watcher.track("ORD-111", "SBIN", "3045", "NSE_EQ", 100)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        self.mock_broker.order_status.return_value = {
            "orderStatus": "PART_TRADED",
            "filledQty": 40,
            "averageTradedPrice": 800.0,
        }
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-PART-1", "Success")

        self.watcher.check_once()
        self.mock_broker.place_forever_oco.assert_called_once()
        self.assertEqual(self.watcher.pending[oid]["protected_qty"], 40)
        self.assertEqual(self.watcher.pending[oid]["state"], OrderLifecycleState.PART_FILLED.value)
        self.assertIn(oid, self.watcher.pending)

    @patch("requests.Session.delete")
    def test_c03_12_test_mode_no_real_dhan_delete(self, mock_delete):
        """C03-12: TEST mode produces no real Dhan DELETE request."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.TEST
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        ok, msg = client.cancel_order("ORD-TEST-1")
        self.assertTrue(ok)
        self.assertIn("TEST mode", msg)
        mock_delete.assert_not_called()

    @patch("requests.Session.delete")
    def test_c03_13_invalid_order_id_no_network_delete(self, mock_delete):
        """C03-13: Invalid or missing order IDs produce no network DELETE call."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        for invalid_id in ("", "   ", None, "ORD/123", "ORD;DROP TABLE", "ORD!@#"):
            ok, msg = client.cancel_order(invalid_id)
            self.assertFalse(ok)
            self.assertIn("rejected", msg)
        mock_delete.assert_not_called()

    @patch("requests.Session.delete")
    def test_c03_14_emergency_stop_active_authorized_cancel_permitted(self, mock_delete):
        """C03-14: Emergency STOP active still permits authorized risk-reducing cancellations while blocking new entries."""
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_delete.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        # Trigger emergency stop
        stop_mgr.trigger_stop(operator="Udit", reason="Market anomaly")
        self.assertTrue(stop_mgr.is_stopped())

        # 1. Cancellation permitted
        ok, msg = client.cancel_order("ORD-HALT-1")
        self.assertTrue(ok)
        self.assertIn("HTTP 202", msg)
        mock_delete.assert_called_once()

        # 2. New risk-increasing order submission BLOCKED
        req = OrderRequest(
            correlation_id="NEW-BUY-1",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=800.0,
            quantity=10,
            trading_mode=TradingMode.LIVE,
        )
        place_ok, order_id, place_msg = client.place_order(req)
        self.assertFalse(place_ok)
        self.assertIn("EMERGENCY STOP active", place_msg)


    # =========================================================================
    # CORRECTION 03 FOLLOW-UP TESTS (Uncertainty, Terminal Preservation, Late Fills, Single-Retry)
    # =========================================================================

    def test_c03_followup_cancellation_uncertainty_handling(self):
        """Follow-up 1: Cancellation transport timeout marks cancel_uncertain and preserves state without retry."""
        oid = self.watcher.track("ORD-UNCERTAIN-1", "WIPRO", "3787", "NSE_EQ", 100)
        self.mock_broker.cancel_order.return_value = (False, "HTTP exception cancelling order: Connection timed out waiting for Dhan DELETE")

        ok, msg = self.watcher.request_cancel(oid)
        self.assertFalse(ok)
        rec = self.watcher.pending[oid]
        self.assertTrue(rec.get("cancel_uncertain"))
        self.assertEqual(rec["state"], OrderLifecycleState.CANCEL_REQUESTED.value)

        # Retry must be rejected due to cancellation uncertainty
        can_retry, retry_qty, r_reason = self.watcher.evaluate_retry(oid, current_market_price=500.0)
        self.assertFalse(can_retry)
        self.assertIn("uncertain", r_reason)

        # Subsequent status poll confirms broker cancelled the order -> resolves uncertainty
        self.mock_broker.order_status.return_value = {"orderStatus": "CANCELLED", "filledQty": 0}
        self.watcher.check_once()
        self.assertNotIn(oid, self.watcher.pending)
        self.assertIn(oid, self.watcher.terminal_orders)
        self.assertEqual(self.watcher.terminal_orders[oid]["state"], OrderLifecycleState.CANCELLED.value)

    def test_c03_followup_preservation_of_terminal_parent_orders(self):
        """Follow-up 2: Terminal parent orders are preserved in terminal_orders for retry evaluation."""
        oid = self.watcher.track("ORD-TERM-PRES-1", "INFY", "1594", "NSE_EQ", 50, original_market_price=1500.0)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        self.mock_broker.order_status.return_value = {"orderStatus": "CANCELLED", "filledQty": 0}
        self.watcher.check_once()

        # Popped from active pending, but preserved in terminal_orders
        self.assertNotIn(oid, self.watcher.pending)
        self.assertIn(oid, self.watcher.terminal_orders)

        # evaluate_retry successfully accesses the terminal parent order
        can_retry, retry_qty, r_reason = self.watcher.evaluate_retry(oid, current_market_price=1510.0)
        self.assertTrue(can_retry)
        self.assertEqual(retry_qty, 50)
        self.assertIn("Retry approved", r_reason)

    def test_c03_followup_late_fills_reconciled_and_protected(self):
        """Follow-up 3: Late fills arriving after terminal cancellation are protected chunk-by-chunk."""
        oid = self.watcher.track("ORD-LATE-FILL-1", "HDFCBANK", "1333", "NSE_EQ", 100, original_market_price=1600.0)
        self.watcher.pending[oid]["state"] = OrderLifecycleState.CANCEL_PENDING.value

        # First poll: Cancelled with 0 fills
        self.mock_broker.order_status.return_value = {"orderStatus": "CANCELLED", "filledQty": 0}
        self.watcher.check_once()
        self.assertIn(oid, self.watcher.terminal_orders)

        # Late fill arrives from exchange (25 shares filled before cancel processed)
        late_status = {"orderStatus": "CANCELLED", "filledQty": 25, "averageTradedPrice": 1600.0}
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-LATE-25", "Success")

        handled = self.watcher.reconcile_terminal_order(oid, st=late_status)
        self.assertTrue(handled)
        self.mock_broker.place_forever_oco.assert_called_once()
        self.assertEqual(self.watcher.terminal_orders[oid]["protected_qty Columbia" if False else "protected_qty"], 25)

        # Retry only for remaining 75 shares (not the 25 late-filled shares)
        can_retry, retry_qty, r_reason = self.watcher.evaluate_retry(oid, current_market_price=1610.0)
        self.assertTrue(can_retry)
        self.assertEqual(retry_qty, 75)

    def test_c03_followup_actual_single_retry_integration(self):
        """Follow-up 4: execute_retry places single retry order, tracks it, and enforces 1-retry cap."""
        cfg = AppConfig.from_env()
        cfg.trading_mode = TradingMode.TEST
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "retry_stop.json"), fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        watcher = ProtectionWatcher(
            broker=client,
            sl_pct=3.0,
            target_pct=3.0,
            path=os.path.join(self.temp_dir.name, "retry_pending.json"),
            dry_run=True,
            clock=lambda: 1000.0,
            journal_db=self.journal,
        )

        oid = watcher.track("ORD-EXEC-RETRY", "SBIN", "3045", "NSE_EQ", 100, original_market_price=800.0)
        watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value

        # Mock broker place_order
        client.place_order = MagicMock(return_value=(True, "RETRY-ORD-101", "Success"))

        # Market moved to 805.0 (+0.625% < 1.00% ceiling)
        ok, new_oid, msg = watcher.execute_retry(oid, current_market_price=805.0, revised_limit_price=805.0)
        self.assertTrue(ok)
        self.assertEqual(new_oid, "RETRY-ORD-101")
        client.place_order.assert_called_once()

        # New order is actively tracked
        self.assertIn("RETRY-ORD-101", watcher.pending)
        self.assertEqual(watcher.pending["RETRY-ORD-101"]["qty"], 100)

        # Parent order links retry order
        parent_rec = watcher.pending.get(oid) or watcher.terminal_orders.get(oid)
        self.assertEqual(parent_rec["retry_order_id"], "RETRY-ORD-101")
        self.assertEqual(parent_rec["retry_count"], 1)

        # Second retry attempt is rejected (max 1 retry)
        ok2, new_oid2, msg2 = watcher.execute_retry(oid, current_market_price=806.0)
        self.assertFalse(ok2)
        self.assertIn("Retry limit reached", msg2)


if __name__ == "__main__":
    unittest.main()
