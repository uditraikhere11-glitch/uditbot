"""Offline regressions for opt-in NSE CNC After Market Orders (AMO)."""

import tempfile
import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.models import OrderRequest, OrderType, ProductType, TradingMode
from src.main import _is_eligible_amo_closed_session
from src.broker.dhan_client import DhanClient
from src.engine.protection_watcher import ProtectionWatcher


def _order(is_amo=True, segment="NSE_EQ"):
    return OrderRequest(
        correlation_id="AMO-TEST-01",
        symbol="TRIDENT-EQ",
        security_id="12345",
        exchange_segment=segment,
        product_type=ProductType.CNC,
        order_type=OrderType.LIMIT,
        price=30.5,
        quantity=1,
        trading_mode=TradingMode.LIVE,
        after_market_order=is_amo,
        amo_time="OPEN",
    )


class TestAMOSelection(unittest.TestCase):
    def test_explicit_opt_in_and_defaults(self):
        self.assertFalse(AppConfig().equity_amo_enabled)
        cfg = AppConfig.from_env(
            env={"TRADING_MODE": "LIVE", "EQUITY_AMO_ENABLED": "true",
                 "EQUITY_AMO_MAX_REFERENCE_AGE_SEC": "72000"}, env_file=""
        )
        self.assertTrue(cfg.equity_amo_enabled)
        self.assertEqual(cfg.equity_amo_max_reference_age_sec, 72000.0)

    def test_eligible_sessions_not_calendar_failures(self):
        for reason in (
            "Market Pre-Open / Closed (00:41:34 IST < 09:15:00)",
            "Market Closed Post-Hours (16:00:00 IST > 15:30:00)",
            "Market Closed (Saturday)",
            "Market Closed (Sunday)",
            "Market Closed (Exchange Holiday: 2026-11-06)",
        ):
            self.assertTrue(_is_eligible_amo_closed_session(reason))
        for reason in (
            "Market Calendar Unverified (Missing file)",
            "Entry Cutoff Exceeded (15:26:00 IST > 15:25)",
            "Market Open (10:00:00 IST)",
        ):
            self.assertFalse(_is_eligible_amo_closed_session(reason))

    def test_order_model_has_explicit_route(self):
        self.assertTrue(_order().after_market_order)
        self.assertEqual(_order().amo_time, "OPEN")
        self.assertFalse(_order(False).after_market_order)


class TestBrokerAMOPayload(unittest.TestCase):
    def setUp(self):
        self.client = DhanClient.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(
            dhan_client_id="TEST_ACCOUNT", equity_amo_enabled=True,
            trading_mode=TradingMode.LIVE, data_dir="/tmp/nonprod-amo-test",
        )
        self.client.stop_manager = MagicMock()
        self.client.stop_manager.try_begin_mutation.return_value = (True, {})
        self.client.reconciliation_gate = SimpleNamespace(is_accepting_entries=True)
        self.client._headers = lambda: {"access-token": "mock"}
        response = MagicMock(status_code=201, text='{"orderId":"TEST-123"}')
        response.json.return_value = {"orderId": "TEST-123", "orderStatus": "PENDING"}
        self.client.session = MagicMock()
        self.client.session.post.return_value = response

    @patch("src.engine.config_manager.ConfigChangeTracker")
    @patch("src.engine.market.market_status", return_value=(False, "Market Pre-Open / Closed (00:41:34 IST < 09:15:00)"))
    def test_amo_payload_explicit(self, mock_status, mock_tracker):
        mock_tracker.return_value.is_live_entry_blocked_by_transition.return_value = (False, "OK")
        ok, oid, _ = self.client.place_order(_order(True))
        self.assertTrue(ok)
        self.assertEqual(oid, "TEST-123")
        payload = self.client.session.post.call_args.kwargs["json"]
        self.assertTrue(payload["afterMarketOrder"])
        self.assertEqual(payload["amoTime"], "OPEN")
        self.assertEqual(payload["orderType"], "LIMIT")

    @patch("src.engine.config_manager.ConfigChangeTracker")
    def test_normal_order_never_sets_amo(self, mock_tracker):
        mock_tracker.return_value.is_live_entry_blocked_by_transition.return_value = (False, "OK")
        ok, _, _ = self.client.place_order(_order(False))
        self.assertTrue(ok)
        payload = self.client.session.post.call_args.kwargs["json"]
        self.assertFalse(payload["afterMarketOrder"])
        self.assertNotIn("amoTime", payload)

    @patch("src.engine.market.market_status", return_value=(False, "Market Pre-Open / Closed (00:41:34 IST < 09:15:00)"))
    def test_bse_amo_is_blocked_before_post(self, _):
        ok, _, msg = self.client.place_order(_order(True, segment="BSE_EQ"))
        self.assertFalse(ok)
        self.assertIn("Only NSE_EQ CNC", msg)
        self.client.session.post.assert_not_called()


class TestWatcherOvernightAMO(unittest.TestCase):
    def test_amo_is_not_cancelled_overnight_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "pending.json")
            broker = MagicMock()
            broker.reconciliation_gate = None
            broker.order_status.return_value = {"orderStatus": "PENDING", "filledQty": 0}
            clock = [1000.0]
            w = ProtectionWatcher(
                broker=broker, path=path, dry_run=False,
                clock=lambda: clock[0], order_timeout_sec=60,
            )
            w.track(order_id="AMO-PENDING-1", symbol="TRIDENT", security_id="12345",
                    segment="NSE_EQ", qty=1, is_amo=True)
            clock[0] += 36000  # Overnight queue is much older than a standard 60s timeout.
            with patch("src.engine.protection_watcher.is_market_open", return_value=False):
                w.check_once()
            self.assertFalse(w.pending["AMO-PENDING-1"]["entry_timeout_elapsed"])
            self.assertEqual(w.pending["AMO-PENDING-1"]["amo_activated_at"], 0.0)
            broker.cancel_order.assert_not_called()
            w2 = ProtectionWatcher(broker=broker, path=path, dry_run=False,
                                   clock=lambda: clock[0], order_timeout_sec=60)
            self.assertTrue(w2.pending["AMO-PENDING-1"]["is_amo"])
            with patch("src.engine.protection_watcher.is_market_open", return_value=True):
                w2.check_once()
            self.assertGreater(w2.pending["AMO-PENDING-1"]["amo_activated_at"], 0.0)
            self.assertFalse(w2.pending["AMO-PENDING-1"]["entry_timeout_elapsed"])


if __name__ == "__main__":
    unittest.main()
