"""
CV13-06 + CV13-07: Consolidated Automated Regression & P0/P1 Verification Test Suite.

Covers all required test matrices from Section 14 and all P0/P1 review findings:
A. Required CMP Tests & P0 Yahoo Source Timestamp:
   - DARVAS_HTTP_SUCCESS vs STRICT_TIMESTAMP, Buy #IDEA / Buy #YESBANK without price.
   - P0: Never assign local current time as quote_timestamp when Yahoo omits regularMarketTime
     (preserve quote_timestamp=None, age_sec=None, quote_age_status="QUOTE_AGE_UNKNOWN").
   - P0/P1: Distinguish accepted pricing from verified quote freshness (freshness_verified,
     quote_freshness_status="ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S" / "ACCEPTED_UNVERIFIED_TIMESTAMP"
     vs "FRESHNESS_VERIFIED").
   - P1: Never label TRANSIT, PENDING, or broker-acknowledged orders as EXECUTED or FILLED.
B. Required Latency & P1 Durable Telemetry Tests:
   - L1-L12 monotonic stage measurements and strict separation of HTTP latency (ms) from quote age (s).
   - P1: Correlate Telegram ingestion, Yahoo retrieval, Dhan acknowledgement, actual fill, and
     protection verification using durable order identifiers (broker_order_id, correlation_id)
     persisted in SQLite across bot restarts.
C. Required WebUI, TOTP & P0/P1 Operational Tests:
   - P0: dhan-token-refresh.service executes /home/udit_raikhere11/uditbot/bot daily_health_check
     (never run.py) and cannot start or restart trading_bot.service.
   - P1: TOTP recovery verifies broker authentication on /fundlimit before claiming recovery
     (distinguishes RECOVERY_VERIFIED from RECOVERY_UNVERIFIED when old token was invalidated).
   - P1: Strict test isolation proof (cannot execute real systemctl, read/write production ~/.dhan_token,
     refresh live tokens, or submit live broker orders).
"""

import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from src.broker.dhan_client import DhanClient
from src.broker.dhan_token import is_token_error, read_token, write_token_atomic
from src.cli import cmd_daily_health_check, cmd_refresh_token
from src.config import AppConfig, parse_env_dict
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeTracker,
    compute_config_version,
    execute_configuration_update,
    is_isolated_test_environment,
)
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR, LatencyMonitor, SignalLatencyTrace
from src.journal.db import JournalDB
from src.main import TradingBotPipeline
from src.models import OrderRequest, OrderType, ProductType, SignalAction, SignalType, TradingMode, TransactionType
from src.notifier.formatter import format_trade_log
from src.setup_app import build_env_and_params_dict, get_setup_page


def _mock_yahoo_chart_resp(
    symbol: str = "IDEA.NS",
    price: object = 8.50,
    quote_ts: object = 1730200000.0,
    exchange: object = "NSI",
    currency: object = "INR",
    market_state: object = "REGULAR",
    delayed_by: object = 0,
    status_code: int = 200,
):
    meta = {}
    if symbol is not None:
        meta["symbol"] = symbol
    if price is not None:
        meta["regularMarketPrice"] = price
    if quote_ts is not None:
        meta["regularMarketTime"] = quote_ts
    if exchange is not None:
        meta["exchangeName"] = exchange
    if currency is not None:
        meta["currency"] = currency
    if market_state is not None:
        meta["marketState"] = market_state
    if delayed_by is not None:
        meta["exchangeDataDelayedBy"] = delayed_by

    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"chart": {"result": [{"meta": meta}]}}
    return resp


class TestCV1306And07Consolidated(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.tok_path = os.path.join(self.data_dir, ".dhan_token")
        write_token_atomic("VALID_SHARED_JWT_TOKEN_304_CHARS", self.tok_path)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def _make_pipeline(
        self,
        mode: TradingMode = TradingMode.TEST,
        cmp_policy: str = "DARVAS_HTTP_SUCCESS",
        markup_pct: float = 0.5,
        budget_inr: float = 25000.0,
    ) -> TradingBotPipeline:
        cfg = AppConfig(
            trading_mode=mode,
            equity_active=True,
            fno_active=True,
            equity_cmp_execution_policy=cmp_policy,
            equity_price_markup_pct=markup_pct,
            equity_budget_inr=budget_inr,
            monitored_channel_ids=[-1001],
            test_group_id=-1002,
            log_group_id=-1003,
            dhan_client_id="1113731536",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
        )
        pipe = TradingBotPipeline(cfg)
        pipe.stop_manager.provision_initial_clean_state("Udit", "1113731536")
        pipe.reconciliation_gate._accepting_entries = True
        pipe.resolver.load_equity("IDEA", security_id="14366", exchange_segment="NSE_EQ", tick_size=0.05)
        pipe.resolver.load_equity("YESBANK", security_id="11915", exchange_segment="NSE_EQ", tick_size=0.05)
        pipe.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ", tick_size=0.05)
        pipe.broker.get_positions = MagicMock(return_value=[])
        return pipe

    # =========================================================================
    # A. REQUIRED CMP TESTS + P0 YAHOO SOURCE TIMESTAMP + P1 NOTIFICATION LABELS
    # =========================================================================

    def test_01_buy_idea_and_yesbank_without_price_in_darvas_mode(self):
        """
        Tests 'Buy #IDEA' and 'Buy #YESBANK' without stated price under DARVAS_HTTP_SUCCESS:
        - P0: Missing regularMarketTime preserves quote_timestamp=None and age_sec=None (never local clock).
        - P0/P1: Distinguishes accepted pricing (is_valid=True) from verified quote freshness (freshness_verified=False).
        - P1: Never labels TRANSIT/PENDING broker-acknowledged orders as EXECUTED or FILLED.
        """
        pipe = self._make_pipeline(mode=TradingMode.LIVE, cmp_policy="DARVAS_HTTP_SUCCESS", markup_pct=0.5, budget_inr=25000.0)
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
        pipe.broker.last_order_broker_status = "TRANSIT"
        pipe.broker.place_order = MagicMock(return_value=(True, "DHAN-ORD-IDEA-1", "Order acknowledged by Dhan (status=TRANSIT)"))

        # 1. Quote is 6.4s old (> 2.0s), missing marketState and exchangeDataDelayedBy -> accepted in DARVAS_HTTP_SUCCESS
        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch.object(pipe.broker.session, "get") as mock_yf:
            mock_yf.return_value = _mock_yahoo_chart_resp(
                symbol="IDEA.NS",
                price=8.50,
                quote_ts=1730200000.0,
                market_state="",
                delayed_by=None,
            )
            res_idea = pipe.process_raw_message(
                "Buy #IDEA",
                channel_id=-1001,
                message_id=101,
                msg_timestamp=1730200005.0,
                current_time=1730200006.4,
            )

        self.assertTrue(res_idea.is_valid)
        self.assertEqual(res_idea.symbol, "IDEA")
        self.assertEqual(res_idea.limit_price, 8.55)
        self.assertEqual(res_idea.cmp_policy, "DARVAS_HTTP_SUCCESS")
        # Pricing is accepted, but strict 2s freshness is NOT verified (6.40s > 2.00s)
        self.assertFalse(res_idea.freshness_verified)
        self.assertIn("ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S", res_idea.quote_freshness_status)
        self.assertIn("CMP Policy: DARVAS_HTTP_SUCCESS", res_idea.log_card)
        self.assertIn("Reported Quote Age: 6.40s", res_idea.log_card)
        self.assertIn("ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S", res_idea.log_card)
        self.assertIn("Darvas policy accepted quote exceeding 2.00s", res_idea.log_card)
        # P1: Never label TRANSIT or PENDING as EXECUTED or FILLED
        self.assertIn("Order Submission: ORDER_ACKNOWLEDGED (TRANSIT)", res_idea.log_card)
        self.assertIn("Execution Status: TRANSIT (Broker Acknowledged — Not Yet Filled)", res_idea.log_card)
        self.assertNotIn("EXECUTED", res_idea.log_card)
        self.assertNotIn("Execution Status: FILLED", res_idea.log_card)
        pipe.broker.place_order.assert_called_once()
        placed_req = pipe.broker.place_order.call_args[0][0]
        self.assertEqual(placed_req.order_type, OrderType.LIMIT)
        self.assertEqual(placed_req.price, 8.55)

        # 2. Buy #YESBANK without price with missing regularMarketTime -> quote_timestamp is None (never local time)
        pipe.broker.place_order.reset_mock()
        pipe.broker.last_order_broker_status = "PENDING"
        pipe.broker.place_order.return_value = (True, "DHAN-ORD-YB-2", "Order acknowledged by Dhan (status=PENDING)")
        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch.object(pipe.broker.session, "get") as mock_yf2:
            mock_yf2.return_value = _mock_yahoo_chart_resp(
                symbol="YESBANK.NS",
                price=15.20,
                quote_ts=None,
                market_state=None,
                delayed_by=None,
            )
            res_yb = pipe.process_raw_message(
                "Buy #YESBANK",
                channel_id=-1001,
                message_id=102,
                msg_timestamp=1730200010.0,
                current_time=1730200011.0,
            )

        self.assertTrue(res_yb.is_valid)
        self.assertEqual(res_yb.symbol, "YESBANK")
        self.assertAlmostEqual(res_yb.limit_price, 15.30, places=2)
        # P0: Verify missing regularMarketTime is preserved as None, never assigned local current_time
        last_q = pipe.broker.last_quote_result
        self.assertIsNone(last_q.quote_timestamp)
        self.assertIsNone(last_q.age_sec)
        self.assertEqual(last_q.quote_age_status, "QUOTE_AGE_UNKNOWN")
        self.assertFalse(last_q.freshness_verified)
        self.assertEqual(last_q.quote_freshness_status, "ACCEPTED_UNVERIFIED_TIMESTAMP")
        self.assertIn("Reported Quote Age: QUOTE_AGE_UNKNOWN", res_yb.log_card)
        self.assertIn("Quote Freshness: ACCEPTED_UNVERIFIED_TIMESTAMP", res_yb.log_card)
        self.assertIn("Execution Status: PENDING (Broker Acknowledged — Not Yet Filled)", res_yb.log_card)
        self.assertNotIn("EXECUTED", res_yb.log_card)

    def test_02_quote_age_above_2s_rejected_in_strict_timestamp_mode(self):
        """Tests that quote age > 2.0s is still rejected when EQUITY_CMP_EXECUTION_POLICY=STRICT_TIMESTAMP."""
        pipe = self._make_pipeline(mode=TradingMode.LIVE, cmp_policy="STRICT_TIMESTAMP")
        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch.object(pipe.broker.session, "get") as mock_yf:
            mock_yf.return_value = _mock_yahoo_chart_resp(
                symbol="IDEA.NS",
                price=8.50,
                quote_ts=1730200000.0,
                market_state="REGULAR",
                delayed_by=0,
            )
            res = pipe.process_raw_message(
                "Buy #IDEA",
                channel_id=-1001,
                message_id=103,
                msg_timestamp=1730200005.0,
                current_time=1730200006.4,
            )
        self.assertFalse(res.is_valid)
        self.assertIn("Stale source quote rejected", res.reason)

    def test_03_stated_price_signal_preserves_approved_markup(self):
        """Tests that equity signals with stated price use configured markup without calling Yahoo."""
        pipe = self._make_pipeline(mode=TradingMode.TEST, cmp_policy="DARVAS_HTTP_SUCCESS", markup_pct=0.5)
        with patch.object(pipe.broker.session, "get") as mock_yf:
            res = pipe.process_raw_message(
                "Buy #SBIN @ 805",
                channel_id=-1001,
                message_id=104,
                msg_timestamp=1000.0,
                current_time=1001.0,
            )
            self.assertFalse(any("yahoo.com" in str(c) for c in mock_yf.call_args_list))
        self.assertTrue(res.is_valid)
        self.assertEqual(res.limit_price, 809.0)
        self.assertEqual(res.reference_label, "STATED_PRICE")

    def test_04_invalid_symbol_venue_currency_price_http_and_malformed_rejected(self):
        """Tests that invalid symbol, wrong venue, wrong currency, zero price, HTTP 500, timeout, and bad JSON are rejected even in DARVAS_HTTP_SUCCESS mode."""
        pipe = self._make_pipeline(mode=TradingMode.LIVE, cmp_policy="DARVAS_HTTP_SUCCESS")

        # 1. Unresolved symbol
        r_unresolved = pipe.process_raw_message("Buy #GROWW", channel_id=-1001, message_id=201, msg_timestamp=1000.0, current_time=1001.0)
        self.assertFalse(r_unresolved.is_valid)
        self.assertIn("Instrument unverified", r_unresolved.reason)

        # 2. Wrong venue (NYSE)
        with patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=8.5, exchange="NYSE")):
            q_venue = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_venue.is_valid)
            self.assertIn("Venue mismatch", q_venue.failure_reason)

        # 3. Unsupported currency (USD)
        with patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=8.5, currency="USD")):
            q_curr = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_curr.is_valid)
            self.assertIn("currency", q_curr.failure_reason)

        # 4. Zero / negative price
        with patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=0.0)):
            q_zero = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_zero.is_valid)

        # 5. HTTP 500 error
        with patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(status_code=500)):
            q_500 = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_500.is_valid)
            self.assertIn("HTTP 500", q_500.failure_reason)

        # 6. Timeout
        with patch.object(pipe.broker.session, "get", side_effect=TimeoutError("Yahoo request timed out")):
            q_to = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_to.is_valid)
            self.assertIn("timed out", q_to.failure_reason)

        # 7. Malformed JSON (empty result)
        bad_resp = MagicMock(status_code=200)
        bad_resp.json.return_value = {"chart": {"result": []}}
        with patch.object(pipe.broker.session, "get", return_value=bad_resp):
            q_bad = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
            self.assertFalse(q_bad.is_valid)

        # 8. F&O quote isolation
        q_fno = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_FNO", cmp_policy="DARVAS_HTTP_SUCCESS")
        self.assertFalse(q_fno.is_valid)
        self.assertIn("cannot substitute underlying equity quote", q_fno.failure_reason)

    def test_05_test_mode_historical_reference_and_no_1000_fallback(self):
        """Tests Section 8: TEST mode outside market hours labels HISTORICAL_REFERENCE and blocks when Yahoo fails (no 1000.0 fallback)."""
        pipe = self._make_pipeline(mode=TradingMode.TEST, cmp_policy="STRICT_TIMESTAMP")

        # 1. Valid Yahoo response outside market hours -> HISTORICAL_REFERENCE
        with patch("src.main.is_market_open", return_value=False), \
             patch.object(pipe.broker.session, "get") as mock_yf:
            mock_yf.return_value = _mock_yahoo_chart_resp(
                symbol="IDEA.NS",
                price=8.50,
                quote_ts=1730190000.0,
                market_state="CLOSED",
            )
            res = pipe.process_raw_message("Buy #IDEA", channel_id=-1001, message_id=301, msg_timestamp=1730200000.0, current_time=1730200001.0)
        self.assertTrue(res.is_valid)
        self.assertEqual(res.reference_label, "HISTORICAL_REFERENCE")
        self.assertEqual(res.limit_price, 8.55)
        self.assertIn("Reference Type: HISTORICAL_REFERENCE", res.log_card)

        # 2. Yahoo HTTP failure in TEST mode -> blocked (never falls back to 1000.0)
        pipe.dedup._seen.clear()
        with patch.object(pipe.broker.session, "get", side_effect=ConnectionError("Offline")):
            res_fail = pipe.process_raw_message("Buy #IDEA", channel_id=-1001, message_id=302, msg_timestamp=1730200010.0, current_time=1730200011.0)
        self.assertFalse(res_fail.is_valid)
        self.assertIn("Could not establish CMP reference", res_fail.reason)

    def test_06_safety_gates_still_enforced_in_darvas_mode(self):
        """Tests Section 3: Closed market, Emergency STOP, and Insufficient funds still block orders in DARVAS_HTTP_SUCCESS mode."""
        pipe = self._make_pipeline(mode=TradingMode.LIVE, cmp_policy="DARVAS_HTTP_SUCCESS")
        pipe.broker.place_order = MagicMock(return_value=(True, "ORD-1", "OK"))

        # 1. Closed market in LIVE mode -> blocked
        with patch("src.main.market_status", return_value=(False, "Market Closed Post-Hours")), \
             patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=8.50)):
            pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
            res_closed = pipe.process_raw_message("Buy #IDEA", channel_id=-1001, message_id=401, msg_timestamp=1000.0, current_time=1001.0)
        self.assertFalse(res_closed.is_valid)
        self.assertIn("LIVE Order Blocked: Market Closed", res_closed.reason)
        pipe.broker.place_order.assert_not_called()

        # 2. Insufficient funds -> blocked
        pipe.dedup._seen.clear()
        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=8.50)):
            pipe.broker.get_fund_limits = MagicMock(return_value=(True, 50.0, "OK"))
            res_funds = pipe.process_raw_message("Buy #IDEA", channel_id=-1001, message_id=402, msg_timestamp=1000.0, current_time=1001.0)
        self.assertFalse(res_funds.is_valid)
        self.assertIn("Available funds", res_funds.reason)
        pipe.broker.place_order.assert_not_called()

        # 3. Emergency STOP -> blocked
        pipe.dedup._seen.clear()
        pipe.stop_manager.trigger_stop("Udit", "Manual emergency stop")
        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch.object(pipe.broker.session, "get", return_value=_mock_yahoo_chart_resp(symbol="IDEA.NS", price=8.50)):
            pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
            res_stop = pipe.process_raw_message("Buy #IDEA", channel_id=-1001, message_id=403, msg_timestamp=1000.0, current_time=1001.0)
        self.assertFalse(res_stop.is_valid)
        self.assertIn("EMERGENCY STOP", res_stop.reason)
        pipe.broker.place_order.assert_not_called()

    # =========================================================================
    # B. REQUIRED LATENCY & DURABLE TELEMETRY CORRELATION TESTS
    # =========================================================================

    def test_07_end_to_end_l1_to_l12_latency_and_durable_restart_correlation(self):
        """
        Tests monotonic stage latency measurements (L1-L12), strict separation of HTTP latency (ms)
        from quote age (s), and P1 durable correlation of L1–L12 across bot restarts via SQLite.
        """
        db1 = JournalDB(self.db_path, account_id="1113731536")
        mon1 = LatencyMonitor(db=db1)

        tr = SignalLatencyTrace(
            trace_id="TRC-TEST-01",
            symbol="IDEA",
            channel_id=-1001,
            message_id=501,
            ist_received_time=1730200010.0,
            telegram_msg_timestamp=1730200009.5,
            l1_received_ns=1_000_000_000,
        )
        tr.mark_l2_parsed(now_ns=1_002_000_000)       # +2.0 ms
        tr.mark_l3_resolved(now_ns=1_003_500_000)     # +1.5 ms
        tr.record_yahoo_stages(
            http_ms=215.4,
            parse_ms=1.2,
            auth_ms=0.4,
            quote_age_sec=6.4,
            quote_age_status="6.40s",
            cmp_policy="DARVAS_HTTP_SUCCESS",
            yahoo_cmp=8.50,
            quote_timestamp=1730200003.6,
            freshness_verified=False,
            quote_freshness_status="ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S (6.40s > 2.00s)",
            now_ns=1_220_500_000,                     # +217.0 ms
        )
        tr.mark_l7_sized(now_ns=1_221_500_000)        # +1.0 ms
        tr.mark_l8_preflight(now_ns=1_225_000_000)    # +3.5 ms
        tr.mark_l9_dhan_start(now_ns=1_226_000_000)
        tr.mark_l10_dhan_ack(dhan_http_ms=140.0, now_ns=1_366_000_000, wall_time=1730200010.366)
        tr.mark_completed(now_ns=1_366_500_000)

        mon1.record_yahoo_request(True, 215.4, 6.4, symbol="IDEA")
        mon1.record_rejection("Stale source quote rejected: age 4.50s exceeds 2.00s maximum")
        mon1.record_trace(tr, order_id="ORD-LAT-1", correlation_id="CORR-LAT-1")

        bd = tr.to_breakdown_dict()
        self.assertEqual(bd["l2_parse_ms"], 2.0)
        self.assertEqual(bd["l3_resolve_ms"], 1.5)
        self.assertEqual(bd["yahoo_http_latency_ms"], 215.4)
        self.assertEqual(bd["reported_quote_age_sec"], 6.4)
        self.assertEqual(bd["telegram_to_cmp_ms"], 220.5)
        self.assertEqual(bd["cmp_to_submission_ms"], 5.5)
        self.assertEqual(bd["dhan_request_to_ack_ms"], 140.0)
        self.assertEqual(bd["telegram_to_ack_ms"], 366.0)
        self.assertEqual(bd["external_telegram_transit_sec"], 0.5)

        # Simulate a bot restart by creating a new JournalDB and new LatencyMonitor instance
        db2 = JournalDB(self.db_path, account_id="1113731536")
        mon2 = LatencyMonitor(db=db2)

        # Verify pre-restart metrics survived the restart
        s_restarted = mon2.get_summary()
        self.assertEqual(s_restarted["yahoo_success_count"], 1)
        self.assertEqual(s_restarted["avg_yahoo_http_ms"], 215.4)
        self.assertEqual(s_restarted["quote_age_distribution"]["5s_to_15s"], 1)
        self.assertEqual(s_restarted["rejection_reasons"].get("Stale source quote rejected"), 1)

        # Now confirm fill (L11) and protection verification (L12) AFTER restart using durable broker_order_id
        fill_ms = mon2.record_fill_confirmed("ORD-LAT-1", wall_time=1730200010.866)        # +500 ms
        prot_ms = mon2.record_protection_verified("CORR-LAT-1", wall_time=1730200011.046)  # +180 ms
        self.assertAlmostEqual(fill_ms, 500.0, places=1)
        self.assertAlmostEqual(prot_ms, 180.0, places=1)

        # Verify correlated trace in SQLite has all L1-L12 fields linked to ORD-LAT-1 and CORR-LAT-1
        persisted_tr = mon2.get_correlated_trace("ORD-LAT-1")
        self.assertIsNotNone(persisted_tr)
        self.assertEqual(persisted_tr["correlation_id"], "CORR-LAT-1")
        self.assertEqual(persisted_tr["broker_order_id"], "ORD-LAT-1")
        self.assertAlmostEqual(persisted_tr["ack_to_fill_ms"], 500.0, places=1)
        self.assertAlmostEqual(persisted_tr["fill_to_protection_ms"], 180.0, places=1)

    # =========================================================================
    # C. REQUIRED WEBUI, TOTP & P0/P1 OPERATIONAL TESTS
    # =========================================================================

    def test_08_authoritative_shared_token_and_official_error_classifier(self):
        """Tests ~/.dhan_token precedence, account identity check on refresh, and DH-906 Order_Error non-misclassification."""
        # 1. DH-906 Order_Error (circuit limit) must NOT be classified as a token error
        ckt_err = '{"errorType":"Order_Error","errorCode":"DH-906","errorMessage":"Rate Not Within Ckt Limit 14.75 To 15.65"}'
        self.assertFalse(is_token_error(400, ckt_err))

        # 2. DH-901 and 401/403 and DH-906 Invalid Token ARE classified as token errors
        self.assertTrue(is_token_error(401, '{"errorCode":"DH-901","errorMessage":"Invalid Authentication"}'))
        self.assertTrue(is_token_error(400, '{"errorCode":"DH-906","errorMessage":"Invalid Token"}'))

        # 3. Shared token file takes precedence and has 0600 permissions
        mode = stat.S_IMODE(os.stat(self.tok_path).st_mode)
        self.assertEqual(mode, 0o600)
        cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            dhan_client_id="1113731536",
            dhan_access_token="STALE_ENV_TOKEN",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
        )
        client = DhanClient(cfg)
        self.assertEqual(client.token, "VALID_SHARED_JWT_TOKEN_304_CHARS")

        # 4. TOTP response with mismatched dhanClientId is rejected before publishing
        cfg.dhan_pin = "123456"
        cfg.dhan_totp_secret = "JBSWY3DPEHPK3PXP"
        bad_auth_resp = MagicMock(status_code=200)
        bad_auth_resp.json.return_value = {
            "dhanClientId": "9999999999",
            "accessToken": "WRONG_ACCOUNT_TOKEN",
        }
        with patch.object(client.session, "post", return_value=bad_auth_resp):
            ok, tok, msg = client.generate_token_with_totp(force=True)
            self.assertFalse(ok)
            self.assertIn("account mismatch", msg.lower())
        self.assertEqual(read_token(self.tok_path), "VALID_SHARED_JWT_TOKEN_304_CHARS")

    def test_09_webui_secret_non_disclosure_unknown_keys_and_rollback(self):
        """Tests that HTML excludes secrets, .env never stores DHAN_ACCESS_TOKEN, unknown keys are preserved, and failed apply rolls back .env."""
        env_file = os.path.join(self.tmp_dir.name, ".env")
        with open(env_file, "w", encoding="utf-8") as f:
            f.write(
                "OPERATOR_NAME=Udit\n"
                "TRADING_MODE=TEST\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "DHAN_PIN=998877\n"
                "DHAN_TOTP_SECRET=SUPER_SECRET_TOTP\n"
                "TG_API_HASH=SECRET_TG_HASH\n"
                "TG_BOT_TOKEN=SECRET_BOT_TOKEN\n"
                "EQUITY_BUDGET_INR=25000.0\n"
                "CUSTOM_USER_KEY=PRESERVE_ME_123\n"
            )

        old_cwd = os.getcwd()
        os.chdir(self.tmp_dir.name)
        try:
            page_resp = get_setup_page()
            html = page_resp.body.decode("utf-8")
            self.assertNotIn("998877", html)
            self.assertNotIn("SUPER_SECRET_TOTP", html)
            self.assertNotIn("SECRET_TG_HASH", html)
            self.assertNotIn("SECRET_BOT_TOKEN", html)
            self.assertIn("EQUITY_CMP_EXECUTION_POLICY", html.upper() if "equity_cmp_execution_policy" in html else html)

            env_content, params = build_env_and_params_dict(
                operator_name="Udit",
                dhan_client_id="1113731536",
                dhan_pin="",
                dhan_totp_secret="",
                tg_api_hash="",
                tg_bot_token="",
                dhan_access_token="NEW_SHARED_TOKEN_XYZ",
                equity_budget_inr=40000.0,
                equity_cmp_execution_policy="DARVAS_HTTP_SUCCESS",
                env_path=env_file,
            )
            self.assertIn("DHAN_PIN=998877", env_content)
            self.assertIn("DHAN_TOTP_SECRET=SUPER_SECRET_TOTP", env_content)
            self.assertIn("TG_API_HASH=SECRET_TG_HASH", env_content)
            self.assertIn("TG_BOT_TOKEN=SECRET_BOT_TOKEN", env_content)
            self.assertIn("CUSTOM_USER_KEY=PRESERVE_ME_123", env_content)
            self.assertIn("EQUITY_CMP_EXECUTION_POLICY=DARVAS_HTTP_SUCCESS", env_content)
            self.assertNotIn("NEW_SHARED_TOKEN_XYZ", env_content)
            self.assertNotIn("DHAN_ACCESS_TOKEN=", env_content)

            rec = execute_configuration_update(
                form_params=params,
                env_content=env_content,
                data_dir=self.data_dir,
                env_path=env_file,
                systemctl_restart_fn=lambda: False,
                check_service_fn=lambda: True,
                rollback_on_apply_failure=True,
            )
            self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
            self.assertTrue(rec.rolled_back)
            restored_env = parse_env_dict(env_file)
            self.assertEqual(float(restored_env["EQUITY_BUDGET_INR"]), 25000.0)
        finally:
            os.chdir(old_cwd)

    def test_10_p0_systemd_timer_uses_verified_bot_cli_and_cannot_touch_trading_bot_service(self):
        """
        P0 Verification:
        - deploy/dhan-token-refresh.service uses /home/udit_raikhere11/uditbot/bot daily_health_check
          and does NOT invoke run.py.
        - Neither dhan-token-refresh.service, dhan-token-refresh.timer, ./bot daily_health_check,
          nor cmd_daily_health_check can start or restart trading_bot.service.
        """
        repo_root = Path(__file__).resolve().parent.parent
        svc_text = (repo_root / "deploy" / "dhan-token-refresh.service").read_text(encoding="utf-8")
        tmr_text = (repo_root / "deploy" / "dhan-token-refresh.timer").read_text(encoding="utf-8")
        bot_text = (repo_root / "bot").read_text(encoding="utf-8")

        self.assertIn("ExecStart=/home/udit_raikhere11/uditbot/bot daily_health_check", svc_text)
        self.assertNotIn("run.py", svc_text)
        self.assertNotIn("trading_bot.service", svc_text)
        self.assertNotIn("systemctl", svc_text)
        self.assertNotIn("trading_bot.service", tmr_text)
        self.assertIn("OnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata", tmr_text)

        # Verify ./bot routes daily_health_check strictly to python -m src.cli daily_health_check
        self.assertIn('"$PYTHON" -m src.cli daily_health_check', bot_text)

        # Verify executing cmd_daily_health_check never invokes systemctl
        cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            dhan_client_id="1113731536",
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
        )
        db = JournalDB(self.db_path, account_id="1113731536")
        stop_mgr = EmergencyStopManager(os.path.join(self.data_dir, "emergency_stop.json"), expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state("Udit", "1113731536")

        totp_resp = MagicMock(status_code=200)
        totp_resp.json.return_value = {"dhanClientId": "1113731536", "accessToken": "FRESH_TOTP_TOKEN_0800"}
        funds_resp = MagicMock(status_code=200)
        funds_resp.json.return_value = {"availabelBalance": 50000.0}

        with patch("subprocess.run", side_effect=AssertionError("systemctl execution prohibited in daily_health_check")), \
             patch("requests.Session.post", return_value=totp_resp), \
             patch("requests.Session.get", return_value=funds_resp), \
             patch("src.instruments.resolver.InstrumentResolver.load_dhan_master", return_value=True):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_daily_health_check(cfg, db, stop_mgr)
            self.assertEqual(rc, 0)

    def test_11_p1_totp_recovery_requires_verified_broker_auth(self):
        """
        P1 Verification:
        - Do not assume restoring an earlier token makes it valid after a new token has been generated.
        - Require verified broker authentication (/fundlimit HTTP 200) before claiming recovery.
        """
        cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            dhan_client_id="1113731536",
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
        )
        stop_mgr = EmergencyStopManager(os.path.join(self.data_dir, "emergency_stop.json"), expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state("Udit", "1113731536")

        # Case 1: New token fails /fundlimit (401), AND previous token was invalidated by Dhan on TOTP generation (also 401)
        totp_ok = MagicMock(status_code=200)
        totp_ok.json.return_value = {"dhanClientId": "1113731536", "accessToken": "NEW_BROKEN_TOKEN"}
        funds_401 = MagicMock(status_code=401, text='{"errorCode":"DH-901","errorMessage":"Token Invalidated"}')

        with patch("requests.Session.post", return_value=totp_ok), \
             patch("requests.Session.get", return_value=funds_401):
            buf1 = io.StringIO()
            with redirect_stdout(buf1):
                res1 = cmd_refresh_token(cfg, stop_mgr)
            out1 = buf1.getvalue()
            self.assertFalse(res1)
            self.assertIn("RECOVERY_UNVERIFIED", out1)
            self.assertNotIn("RECOVERY_VERIFIED", out1)

        # Case 2: New token fails /fundlimit (500), but previous token passes /fundlimit (200) -> RECOVERY_VERIFIED
        write_token_atomic("PREV_STILL_VALID_TOKEN", self.tok_path)
        funds_500 = MagicMock(status_code=500, text="Gateway error")
        funds_200 = MagicMock(status_code=200)
        funds_200.json.return_value = {"availabelBalance": 75000.0}

        with patch("requests.Session.post", return_value=totp_ok), \
             patch("requests.Session.get", side_effect=[funds_500, funds_200]):
            buf2 = io.StringIO()
            with redirect_stdout(buf2):
                res2 = cmd_refresh_token(cfg, stop_mgr)
            out2 = buf2.getvalue()
            self.assertFalse(res2)
            self.assertIn("RECOVERY_VERIFIED", out2)

    def test_12_p1_strict_regression_test_isolation_proof(self):
        """
        P1 Verification:
        Proves that automated tests cannot execute real systemd operations, write production
        credentials/tokens, refresh live tokens, or submit live broker orders.
        """
        # 1. Systemd isolation proof
        self.assertTrue(is_isolated_test_environment())

        # 2. Default ~/.dhan_token isolation proof inside unittest
        default_cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536", data_dir=self.data_dir)
        default_client = DhanClient(default_cfg)
        self.assertTrue(default_client._is_isolated_test_default_token_path(os.path.expanduser("~/.dhan_token")))
        self.assertEqual(default_client.token, "")

        # 3. Hard TEST-mode broker order mutation block proof (even if session.post were unmocked)
        req = OrderRequest(
            correlation_id="TEST-ISO-1",
            symbol="IDEA-EQ",
            security_id="14366",
            exchange_segment="NSE_EQ",
            transaction_type=TransactionType.BUY,
            product_type=ProductType.CNC,
            order_type=OrderType.LIMIT,
            price=8.55,
            quantity=10,
            trading_mode=TradingMode.TEST,
        )
        default_client.stop_manager.provision_initial_clean_state("Udit", "1113731536")
        with patch.object(default_client.session, "post", side_effect=AssertionError("Live HTTP POST prohibited in TEST mode")):
            ok_ord, oid, msg_ord = default_client.place_order(req)
            self.assertFalse(ok_ord)
            self.assertIsNone(oid)
            self.assertIn("TEST mode", msg_ord)


    def test_13_defect1_webui_and_runtime_config_hash_parity(self):
        """
        Defect 1 Regression Test:
        Verifies that WebUI uppercase form_params, parsed .env dictionaries, and runtime
        AppConfig instances produce the exact same normalized SHA-256 configuration hash,
        so a running process acknowledging its AppConfig version transitions the WebUI change
        record to APPLIED_VERIFIED instead of APPLY_FAILED.
        """
        env_file = os.path.join(self.tmp_dir.name, ".env")
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            operator_name="Udit",
            instance_name="Audit bot",
            tg_api_id="123456",
            tg_api_hash="SECRET_HASH",
            monitored_channel_ids="-100111, -100222",
            log_group_id="-5514889030",
            test_group_id="-5100801652",
            ignore_backlog="true",
            dhan_client_id="1113731536",
            dhan_pin="123456",
            dhan_totp_secret="SECRET_TOTP",
            equity_budget_inr=35000.0,
            equity_product_type="CNC",
            equity_price_markup_pct=0.5,
            equity_cmp_execution_policy="DARVAS_HTTP_SUCCESS",
            fno_capital_per_trade=25000.0,
            fno_max_lots=3,
            sl_pct=3.0,
            target_pct=4.0,
            env_path=env_file,
        )
        with open(env_file, "w", encoding="utf-8") as f:
            f.write(env_content)

        # Load runtime AppConfig from the written .env file (isolated from os.environ)
        env_dict = parse_env_dict(env_file)
        runtime_cfg = AppConfig.from_env(env=env_dict, env_file=env_file)

        hash_from_webui_params = compute_config_version(form_params)
        hash_from_env_dict = compute_config_version(env_dict)
        hash_from_runtime_cfg = compute_config_version(runtime_cfg)

        self.assertEqual(hash_from_webui_params, hash_from_runtime_cfg)
        self.assertEqual(hash_from_env_dict, hash_from_runtime_cfg)

        # Verify end-to-end execute_configuration_update reaches APPLIED_VERIFIED when
        # the restarted runtime process records compute_config_version(runtime_cfg)
        tracker = ConfigChangeTracker(data_dir=self.data_dir)

        def restart_simulating_runtime_bot():
            reloaded_cfg = AppConfig.from_env(env=parse_env_dict(env_file), env_file=env_file)
            tracker.record_active_running_version(
                compute_config_version(reloaded_cfg),
                trading_mode=reloaded_cfg.trading_mode.value,
            )
            return True

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=env_file,
            systemctl_restart_fn=restart_simulating_runtime_bot,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.effective_version_hash, hash_from_runtime_cfg)

    def test_14_defect2_webui_save_never_overwrites_shared_dhan_token(self):
        """
        Defect 2 Regression Test:
        Verifies that a WebUI Save carrying dhan_access_token never writes to or overwrites
        the authoritative shared Dhan token file (even before or during validation).
        """
        env_file = os.path.join(self.tmp_dir.name, ".env")
        local_tok_file = os.path.join(self.tmp_dir.name, ".dhan_token")
        write_token_atomic("AUTHORITATIVE_TOTP_TOKEN_DO_NOT_OVERWRITE", local_tok_file)

        with open(env_file, "w", encoding="utf-8") as f:
            f.write(f"DHAN_CLIENT_ID=1113731536\nDHAN_TOKEN_FILE={local_tok_file}\n")

        env_content, form_params = build_env_and_params_dict(
            dhan_client_id="1113731536",
            dhan_access_token="STALE_WEBUI_POSTED_TOKEN_SHOULD_BE_IGNORED",
            equity_budget_inr=-999.0,  # Invalid value
            env_path=env_file,
        )
        # Shared token file must remain untouched
        self.assertEqual(read_token(local_tok_file), "AUTHORITATIVE_TOTP_TOKEN_DO_NOT_OVERWRITE")
        self.assertEqual(read_token(self.tok_path), "VALID_SHARED_JWT_TOKEN_304_CHARS")
        self.assertNotIn("STALE_WEBUI_POSTED_TOKEN_SHOULD_BE_IGNORED", env_content)

    def test_15_defect3_csrf_enforced_on_all_settings_post_routes(self):
        """
        Defect 3 Regression Test:
        Verifies that both POST /setup/submit-async and POST /setup reject requests with
        a missing CSRF token, an empty CSRF token, or an invalid CSRF token (HTTP 403),
        and accept requests only when the valid SETUP_CSRF_TOKEN is supplied.
        """
        from fastapi.testclient import TestClient
        from src.setup_app import SETUP_CSRF_TOKEN, app

        client = TestClient(app)
        old_cwd = os.getcwd()
        os.chdir(self.tmp_dir.name)
        try:
            # 1. Async endpoint: missing csrf_token -> 403
            r_async_missing = client.post("/setup/submit-async", json={"equity_budget_inr": 30000.0})
            self.assertEqual(r_async_missing.status_code, 403)
            self.assertEqual(r_async_missing.json()["state"], "REJECTED")

            # 2. Async endpoint: empty csrf_token -> 403
            r_async_empty = client.post("/setup/submit-async", data={"equity_budget_inr": "30000", "csrf_token": ""})
            self.assertEqual(r_async_empty.status_code, 403)

            # 3. Async endpoint: wrong csrf_token -> 403
            r_async_wrong = client.post("/setup/submit-async", json={"equity_budget_inr": 30000.0, "csrf_token": " forged_token "})
            self.assertEqual(r_async_wrong.status_code, 403)

            # 4. Legacy POST /setup endpoint: missing csrf_token -> 403
            r_sync_missing = client.post("/setup", data={"dhan_client_id": "1113731536", "equity_budget_inr": "30000"})
            self.assertEqual(r_sync_missing.status_code, 403)

            # 5. Legacy POST /setup endpoint: wrong csrf_token -> 403
            r_sync_wrong = client.post("/setup", data={"dhan_client_id": "1113731536", "csrf_token": "wrong"})
            self.assertEqual(r_sync_wrong.status_code, 403)

            # 6. Both endpoints succeed (200) when valid SETUP_CSRF_TOKEN is provided
            r_async_ok = client.post(
                "/setup/submit-async",
                data={"dhan_client_id": "1113731536", "equity_budget_inr": "30000", "csrf_token": SETUP_CSRF_TOKEN},
            )
            self.assertEqual(r_async_ok.status_code, 200)

            r_sync_ok = client.post(
                "/setup",
                data={"dhan_client_id": "1113731536", "equity_budget_inr": "30000", "csrf_token": SETUP_CSRF_TOKEN},
            )
            self.assertEqual(r_sync_ok.status_code, 200)
        finally:
            os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
