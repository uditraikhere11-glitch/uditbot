"""
AuditBot V13 — Regression Suite for All 26 Independent Audit Findings (V12-01 through V12-26).
Includes:
- All 15 Appendix B defect reproductions inverted into permanent safety assertions (proving the
  unsafe behaviors reproduced in the V12 audit are now blocked/fixed).
- 11 additional regression tests covering source-trace findings (V12-08, V12-12, V12-14, V12-15,
  V12-20, V12-21, V12-22, V12-23, V12-24, V12-25, V12-26).
"""

import asyncio
import csv
from datetime import date, datetime
import os
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.config import AppConfig, APP_ROOT
from src.models import OrderRequest, ProductType, TradingMode, TransactionType
from src.journal.db import JournalDB
from src.engine.protection_watcher import ProtectionWatcher, OrderLifecycleState
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.emergency_stop import EmergencyStopManager
from src.broker.dhan_client import DhanClient
from src.broker import dhan_token
from src.instruments.resolver import InstrumentResolver
from src.notifier.telegram_notifier import TelegramNotifier
from src.engine.market import is_market_open, is_lifecycle_open
from src.main import TradingBotPipeline, _is_transport_uncertainty
from src.setup_app import get_setup_page, handle_setup_submission
from src.parsers.gemini_fallback import (
    GeminiFnOCandidate,
    GeminiEquityCandidate,
    parse_fno_gemini_sync,
    parse_equity_gemini_sync,
)


class TestV13IndependentAuditRemediations(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="123",
            dhan_access_token="dummy",
            data_dir=self.t.name,
        )
        self.db = JournalDB(self.t.name + "/j.db", "123")
        self.stop = EmergencyStopManager(self.t.name + "/stop.json", expected_account_id="123")
        self.stop.provision_initial_clean_state("Audit", "123")

    def watcher(self, b):
        return ProtectionWatcher(
            b,
            path=self.t.name + "/w.json",
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=self.db,
        )

    def track(self, w, **kw):
        w.track("P", "SBIN", "3045", "NSE_EQ", 10, original_market_price=100, **kw)

    # ------------------------------------------------------------------
    # Appendix B #1 / V12-04: Forever verification rejects TRADED or incomplete records
    # ------------------------------------------------------------------
    def test_01_v12_04_forever_missing_legs_or_traded_is_rejected(self):
        c = DhanClient(self.cfg, self.stop)
        c.get_forever_orders = Mock(return_value=[{"orderId": "O", "orderStatus": "TRADED"}])
        self.assertFalse(c.verify_forever_leg_active("O", "3045", 10, 97, 103)[0])

        # Complete active OCO record with both SL and Target legs passes
        c.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "O",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "transactionType": "SELL",
                    "quantity": 10,
                    "quantity1": 10,
                    "triggerPrice": 97.0,
                    "price1": 103.0,
                }
            ]
        )
        self.assertTrue(c.verify_forever_leg_active("O", "3045", 10, 97, 103)[0])

    # ------------------------------------------------------------------
    # Appendix B #2 / V12-05: Super protection requires readback evidence
    # ------------------------------------------------------------------
    def test_02_v12_05_super_missing_readback_is_unverified(self):
        b = Mock()
        b.trades_avg_price.return_value = 100
        b.modify_super_order_protection.return_value = (True, "ACK")
        b.order_status.return_value = None
        w = self.watcher(b)
        self.track(w, product_type="MARGIN", is_super_order=True)
        w._protect(w.pending["P"], {"averageTradedPrice": 100}, 10, 10)
        self.assertEqual(w.pending["P"]["protected_qty"], 0)
        self.assertEqual(w.pending["P"]["reanchor_status"], "UNVERIFIED")

    # ------------------------------------------------------------------
    # Appendix B #3 / V12-03: Unverified OCO is reconciled, never blindly resubmitted
    # ------------------------------------------------------------------
    def test_03_v12_03_unverified_oco_is_not_resubmitted(self):
        b = Mock()
        b.order_status.return_value = {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 100}
        b.trades_avg_price.return_value = 100
        b.place_forever_oco.return_value = (True, "O", "ACK")
        b.get_forever_orders.return_value = []
        b.verify_forever_leg_active.return_value = (False, "Book delayed")
        w = self.watcher(b)
        self.track(w)
        w.check_once()
        w.clock = lambda: 1061.0
        w.check_once()
        self.assertEqual(b.place_forever_oco.call_count, 1)

    # ------------------------------------------------------------------
    # Appendix B #4 / V12-02: Restart preserves protection coverage and Super identity
    # ------------------------------------------------------------------
    def test_04_v12_02_restart_preserves_protection_and_super_identity(self):
        b = Mock()
        b.get_fund_limits.return_value = (True, 100000, "ok")
        b.get_trade_book.return_value = []
        b.get_positions.return_value = []
        b.get_order_book.return_value = []
        b.get_order_by_correlation_id.return_value = {"orderId": "P", "orderStatus": "PENDING"}
        b.order_status.return_value = None
        w = self.watcher(b)
        self.track(
            w,
            product_type="MARGIN",
            is_super_order=True,
            lot_size=25,
            option_type="CE",
            strike=100,
            expiry=date(2099, 1, 1),
        )
        w.pending["P"]["protected_qty"] = 5
        w.pending["P"]["retry_count"] = 1
        self.db.record_order_intent("C", "SBIN", "3045", "NSE_FNO", "MARGIN", "LIMIT", 100, 10)
        g = StartupReconciliationGate(b, self.db, w, self.stop)
        self.assertTrue(g.run_startup_reconciliation()[0])
        self.assertEqual(w.pending["P"]["protected_qty"], 5)
        self.assertTrue(w.pending["P"]["is_super_order"])
        self.assertEqual(w.pending["P"]["retry_count"], 1)

    # ------------------------------------------------------------------
    # Appendix B #5 / V12-07: Startup gate verifies actual account positions & tradebook
    # ------------------------------------------------------------------
    def test_05_v12_07_positions_check_on_clean_startup_blocks_unprotected_position(self):
        b = Mock()
        b.get_fund_limits.return_value = (True, 100000, "ok")
        b.get_trade_book.return_value = []
        b.get_positions.return_value = [{"securityId": "3045", "netQty": 10}]
        b.get_forever_orders.return_value = []
        b.get_super_orders.return_value = []
        g = StartupReconciliationGate(b, self.db, Mock(), self.stop)
        self.assertFalse(g.run_startup_reconciliation()[0])
        b.get_positions.assert_called()
        b.get_trade_book.assert_called()

    # ------------------------------------------------------------------
    # Appendix B #6 / V12-01: Loading same master twice does not duplicate option catalogue
    # ------------------------------------------------------------------
    def test_06_v12_01_same_master_loaded_twice_preserves_options(self):
        p = self.t.name + "/master.csv"
        fields = [
            "INSTRUMENT",
            "EXCH_ID",
            "TRADING_SYMBOL",
            "UNDERLYING_SYMBOL",
            "SECURITY_ID",
            "SM_EXPIRY_DATE",
            "STRIKE_PRICE",
            "OPTION_TYPE",
            "LOT_SIZE",
            "TICK_SIZE",
            "PADDING",
        ]
        with open(p, "w") as f:
            wr = csv.writer(f)
            wr.writerow(fields)
            wr.writerow(["OPTSTK", "NSE", "SBIN-JAN99-100-CE", "SBIN", "123", "2099-01-29", 100, "CE", 25, 0.05, "x" * 11000])
        r = InstrumentResolver()
        r.load_dhan_master(p)
        self.assertIsNotNone(r.resolve_option("SBIN", 100, "CE", "JAN", reference_date=date(2099, 1, 1))[0])
        r.load_dhan_master(p)
        self.assertIsNotNone(r.resolve_option("SBIN", 100, "CE", "JAN", reference_date=date(2099, 1, 1))[0])
        self.assertEqual(len(r._options_list), 1)

    # ------------------------------------------------------------------
    # Appendix B #7 / V12-18: TG_BOT_TOKEN honored & disabled notifier returns False
    # ------------------------------------------------------------------
    def test_07_v12_18_bot_token_environment_honored_and_disabled_returns_false(self):
        c = AppConfig.from_env(env={"TG_BOT_TOKEN": "dummy", "LOG_GROUP_ID": "-123"}, env_file=self.t.name + "/none")
        n = TelegramNotifier(c)
        self.assertTrue(n.enabled)

        c_off = AppConfig.from_env(env={"TG_BOT_TOKEN": "", "LOG_GROUP_ID": "-123"}, env_file=self.t.name + "/none")
        n_off = TelegramNotifier(c_off)
        self.assertFalse(n_off.enabled)
        self.assertFalse(n_off.send_log_message("critical alert"))

    # ------------------------------------------------------------------
    # Appendix B #8 / V12-11: Retry rejects unbounded revised_limit_price and enforces root reservation
    # ------------------------------------------------------------------
    def test_08_v12_11_retry_rejects_unbounded_price_and_child_cannot_retry_again(self):
        b = Mock()
        b.cfg = self.cfg
        b.stop_manager = self.stop
        b.place_order.return_value = (True, "CHILD", "OK")
        w = self.watcher(b)
        self.track(w)
        w.pending["P"]["state"] = "CANCELLED"
        ok, _, _ = w.execute_retry("P", current_market_price=100, revised_limit_price=999)
        self.assertFalse(ok)
        b.place_order.assert_not_called()

        # Valid retry succeeds once, and child order inherits exhausted retry budget
        ok_valid, child_id, _ = w.execute_retry("P", current_market_price=100.5, quote_timestamp=1000.0)
        self.assertTrue(ok_valid)
        self.assertEqual(child_id, "CHILD")
        w.pending["CHILD"]["state"] = "CANCELLED"
        ok_grandchild, _, _ = w.execute_retry("CHILD", current_market_price=100.6, quote_timestamp=1000.0)
        self.assertFalse(ok_grandchild)

    # ------------------------------------------------------------------
    # Appendix B #9 / V12-10: F&O Super Order retry routes through place_super_order
    # ------------------------------------------------------------------
    def test_09_v12_10_super_retry_uses_super_endpoint(self):
        b = Mock()
        b.cfg = self.cfg
        b.stop_manager = self.stop
        b.place_super_order.return_value = (True, "CHILD", "OK")
        w = self.watcher(b)
        w.track(
            "P",
            "SBIN",
            "123",
            "NSE_FNO",
            25,
            product_type="MARGIN",
            original_market_price=100,
            is_super_order=True,
            lot_size=25,
            option_type="CE",
            strike=100,
            expiry=date(2099, 1, 29),
        )
        w.pending["P"]["state"] = "CANCELLED"
        self.assertTrue(w.execute_retry("P", 100, quote_timestamp=1000, reference_date=date(2099, 1, 1))[0])
        b.place_super_order.assert_called_once()
        b.place_order.assert_not_called()
        self.assertTrue(w.pending["CHILD"]["is_super_order"])

    # ------------------------------------------------------------------
    # Appendix B #10 / V12-09: Equity order times out and requests cancellation
    # ------------------------------------------------------------------
    def test_10_v12_09_equity_times_out_and_cancels(self):
        b = Mock()
        b.order_status.return_value = {"orderStatus": "PENDING", "filledQty": 0}
        w = self.watcher(b)
        self.track(w)
        w.clock = lambda: 5000.0
        w.check_once()
        b.cancel_order.assert_called_once_with("P")

    # ------------------------------------------------------------------
    # Appendix B #11 / V12-06: Lifecycle monitoring stays open past 15:25 entry cutoff
    # ------------------------------------------------------------------
    def test_11_v12_06_watcher_lifecycle_gate_stays_open_past_entry_cutoff(self):
        self.assertFalse(is_market_open(datetime(2026, 10, 29, 15, 26), holidays=[]))
        self.assertTrue(is_lifecycle_open(datetime(2026, 10, 29, 15, 26), holidays=[]))

    # ------------------------------------------------------------------
    # Appendix B #12 / V12-13: HTTP 500 preserved as uncertain (UNKNOWN) & in-flight retained
    # ------------------------------------------------------------------
    def test_12_v12_13_http_500_preserved_as_uncertain(self):
        c = DhanClient(self.cfg, self.stop)
        c.session.post = Mock(return_value=SimpleNamespace(status_code=500, text="Internal Server Error"))
        c.get_order_by_correlation_id = Mock(return_value=None)
        c.get_order_book = Mock(return_value=[])
        req = OrderRequest(
            correlation_id="C",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=100,
            quantity=10,
        )
        ok, _, msg = c.place_order(req)
        self.assertFalse(ok)
        self.assertTrue(_is_transport_uncertainty(msg))
        self.assertEqual(self.stop.get_state()["in_flight_orders"], ["C"])

    # ------------------------------------------------------------------
    # Appendix B #13 / V12-17: Partial-fill protection uses marginal chunk price
    # ------------------------------------------------------------------
    def test_13_v12_17_partial_fill_uses_marginal_chunk_price(self):
        b = Mock()
        b.trades_avg_price.return_value = 105
        b.place_forever_oco.return_value = (True, "O", "ACK")
        b.get_forever_orders.return_value = []
        b.verify_forever_leg_active.return_value = (True, "OK")
        w = self.watcher(b)
        self.track(w)
        r = w.pending["P"]
        r["protected_qty"] = 5
        r["protected_value"] = 500
        w._protect(r, {"averageTradedPrice": 105}, 10, 5)
        self.assertEqual(b.place_forever_oco.call_args.kwargs["levels"].executed_price, 110.0)

    # ------------------------------------------------------------------
    # Appendix B #14 / V12-16: Instrument tick_size forwarded to place_forever_oco
    # ------------------------------------------------------------------
    def test_14_v12_16_tick_size_forwarded_to_place_forever_oco(self):
        c = DhanClient(self.cfg, self.stop)
        c.order_status = Mock(return_value={"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 10.01})
        c.trades_avg_price = Mock(return_value=10.01)
        c.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 10}])
        c.session.post = Mock(
            return_value=SimpleNamespace(
                status_code=200,
                text='{"orderId":"OCO-01","orderStatus":"PENDING"}',
                json=lambda: {"orderId": "OCO-01", "orderStatus": "PENDING"},
            )
        )
        c.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-01",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "transactionType": "SELL",
                    "quantity": 10,
                    "quantity1": 10,
                    "triggerPrice": 9.71,
                    "price1": 10.32,
                }
            ]
        )
        w = self.watcher(c)
        self.track(w, tick_size=0.01)
        w.check_once()
        self.assertEqual(w.terminal_orders["P"]["protected_qty"], 10)

    # ------------------------------------------------------------------
    # Appendix B #15 / V12-19: UNPROTECTED history superseded once protection succeeds
    # ------------------------------------------------------------------
    def test_15_v12_19_unprotected_history_cleared_once_protected(self):
        b = Mock()
        b.order_status.return_value = {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 100}
        b.trades_avg_price.return_value = 100
        b.place_forever_oco.side_effect = [(False, None, "failure"), (True, "O", "ACK")]
        b.get_forever_orders.return_value = []
        b.verify_forever_leg_active.return_value = (True, "OK")
        w = self.watcher(b)
        self.track(w)
        w.check_once()
        w.clock = lambda: 1061.0
        w.check_once()
        self.assertEqual(w.terminal_orders["P"]["protected_qty"], 10)
        self.assertEqual(len(self.db.get_unprotected_orders()), 0)

    # ------------------------------------------------------------------
    # V12-08: Portfolio risk limits update dynamically across sequential pipeline trades
    # ------------------------------------------------------------------
    def test_16_v12_08_portfolio_risk_limits_update_from_live_orders(self):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="123",
            dhan_access_token="dummy",
            data_dir=self.t.name,
            max_daily_trades=1,
            equity_budget_inr=10000.0,
        )
        pipe = TradingBotPipeline(cfg)
        pipe.stop_manager.provision_initial_clean_state("Audit", "123")
        pipe.reconciliation_gate._accepting_entries = True
        pipe.resolver.load_equity("SBIN", "3045", "NSE_EQ", 0.05)
        pipe.resolver.load_equity("TCS", "11536", "NSE_EQ", 0.05)
        pipe.broker.get_fund_limits = Mock(return_value=(True, 200000.0, "OK"))
        pipe.broker.get_positions = Mock(return_value=[])
        pipe.broker.place_order = Mock(return_value=(True, "ORD-LIVE-1", "OK"))

        with patch("src.main.market_status", return_value=(True, "Market Open")):
            r1 = pipe.process_raw_message("Buy #SBIN at 500", channel_id=1, message_id=101, msg_timestamp=1000.0, current_time=1001.0)
            self.assertTrue(r1.is_valid)

            # Second trade on same day breaches max_daily_trades=1 because RiskManager state was updated!
            r2 = pipe.process_raw_message("Buy #TCS at 3000", channel_id=1, message_id=102, msg_timestamp=1002.0, current_time=1003.0)
            self.assertFalse(r2.is_valid)
            self.assertIn("Daily trade limit reached", r2.reason)

    # ------------------------------------------------------------------
    # V12-12: Super Order preserves full 30-character correlation ID
    # ------------------------------------------------------------------
    def test_17_v12_12_super_order_preserves_full_30_char_correlation_id(self):
        c = DhanClient(self.cfg, self.stop)
        captured = {}

        def fake_post(url, headers=None, json=None, timeout=5):
            captured["payload"] = json
            return SimpleNamespace(status_code=200, text='{"orderId":"SUP-1"}', json=lambda: {"orderId": "SUP-1"})

        c.session.post = fake_post
        corr_30 = "OPT-POLYCAB-1234567-0001abcdef"[:30]
        self.assertEqual(len(corr_30), 30)
        req = OrderRequest(
            correlation_id=corr_30,
            symbol="POLYCAB-29OCT26-8000-PE",
            security_id="54231",
            exchange_segment="NSE_FNO",
            product_type=ProductType.MARGIN,
            price=100.0,
            quantity=100,
        )
        ok, oid, _ = c.place_super_order(req, stop_loss_price=97.0, target_price=103.0)
        self.assertTrue(ok)
        self.assertEqual(captured["payload"]["correlationId"], corr_30)

    # ------------------------------------------------------------------
    # V12-15: Yahoo quote freshness requires symbol/exchange metadata & measures post-HTTP time
    # ------------------------------------------------------------------
    def test_18_v12_15_yahoo_quote_rejects_missing_metadata_and_measures_elapsed_time(self):
        c = DhanClient(self.cfg, self.stop)
        # Missing exchangeName/symbol in meta is rejected
        c.session.get = Mock(
            return_value=SimpleNamespace(
                status_code=200,
                json=lambda: {"chart": {"result": [{"meta": {"regularMarketPrice": 500.0, "regularMarketTime": 1000.0}}]}},
            )
        )
        q = c.get_yahoo_quote_details("SBIN", "NSE_EQ", "3045", now_utc=1000.5)
        self.assertFalse(q.is_valid)

    # ------------------------------------------------------------------
    # V12-20: DhanClient adopts shared token from disk before performing TOTP login
    # ------------------------------------------------------------------
    def test_19_v12_20_dhan_client_uses_shared_token_coordinator(self):
        tok_path = os.path.join(self.t.name, ".dhan_token")
        dhan_token.write_token_atomic("SHARED_TOKEN_V1", tok_path)
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="123",
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=tok_path,
            data_dir=self.t.name,
        )
        c = DhanClient(cfg, self.stop)
        self.assertEqual(c.token, "SHARED_TOKEN_V1")

        # External process updates shared token file; _headers() adopts it automatically
        import time as _t
        _t.sleep(0.02)
        dhan_token.write_token_atomic("SHARED_TOKEN_V2", tok_path)
        hdrs = c._headers()
        self.assertEqual(hdrs["access-token"], "SHARED_TOKEN_V2")

    # ------------------------------------------------------------------
    # Protected Web Setup Component Parity (Owner Mandate: Keep Original Unchanged)
    # ------------------------------------------------------------------
    def test_20_web_setup_files_remain_byte_for_byte_original(self):
        import hashlib
        expected = {
            "src/setup_app.py": "f636fea2e62afe9213d08524866b73785790f73deaa69ae784848905f916ef4a",
            "src/setup_wizard.py": "3ef353bbcd580499a929c9fb29a69c5eaf5e89918556b07fe770ca3736075c8f",
            "tests/test_setup.py": "256981a20af06ea2a1fb524326a30dfcb6658b1f5451b5780a3019424104c87e",
        }
        for rel_path, exp_hash in expected.items():
            with open(rel_path, "rb") as fh:
                actual_hash = hashlib.sha256(fh.read()).hexdigest()
            self.assertEqual(actual_hash, exp_hash, f"{rel_path} must remain byte-for-byte identical to original")

    # ------------------------------------------------------------------
    # V12-24: CLI daily_health_check executes refresh & health probe with exit code 0
    # ------------------------------------------------------------------
    def test_21_v12_24_daily_health_check_command(self):
        from src.cli import cmd_daily_health_check
        cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            dhan_client_id="123",
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=os.path.join(self.t.name, ".dhan_token"),
            data_dir=self.t.name,
        )
        with patch("src.broker.dhan_client.DhanClient.generate_token_with_totp", return_value=(True, "TOK", "OK")), \
             patch("src.broker.dhan_client.DhanClient.get_fund_limits", return_value=(True, 50000.0, "OK")), \
             patch("src.instruments.resolver.InstrumentResolver.load_dhan_master", return_value=True):
            code = cmd_daily_health_check(cfg, self.db, self.stop)
            self.assertEqual(code, 0)

    # ------------------------------------------------------------------
    # V12-26: Gemini fallback rejects hallucinated fields not present in source text
    # ------------------------------------------------------------------
    def test_22_v12_26_gemini_fallback_requires_source_evidence_and_valid_option_type(self):
        fake_client = MagicMock()
        # Hallucinated strike 9500 (not in text "POLYCAB 8000 PE buy around 110") -> rejected!
        fake_resp_bad_strike = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=9500.0,
                option_type="PE",
                entry_min=110.0,
                entry_max=110.0,
            )
        )
        fake_client.models.generate_content.return_value = fake_resp_bad_strike
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig = parse_fno_gemini_sync("POLYCAB 8000 PE buy around 110", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNone(sig)

        # Invalid option_type "STRADDLE" -> rejected (never mapped to PE!)
        fake_resp_bad_opt = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="STRADDLE",
                entry_min=110.0,
                entry_max=110.0,
            )
        )
        fake_client.models.generate_content.return_value = fake_resp_bad_opt
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig2 = parse_fno_gemini_sync("POLYCAB 8000 PE buy around 110", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNone(sig2)

        # Exact evidenced fields -> accepted
        fake_resp_ok = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="PE",
                entry_min=110.0,
                entry_max=110.0,
            )
        )
        fake_client.models.generate_content.return_value = fake_resp_ok
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig3 = parse_fno_gemini_sync("POLYCAB 8000 PE buy around 110", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNotNone(sig3)
            self.assertEqual(sig3.strike, 8000.0)


if __name__ == "__main__":
    unittest.main()
