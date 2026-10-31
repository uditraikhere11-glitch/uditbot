"""
AuditBot V14 — Final Market-Safety & Trading-Safety Regression Test Suite (`test_v14_market_safety_regressions.py`).

Verifies the 4 mandatory V14 market-safety corrections + the 3 independent ZIP review findings:
1. NSE 2026 holiday calendar updated per official NSE circulars (`NSE/CMTR/71775` and `NSE/CMTR/72260`,
   including `2026-01-15`), and missing, unreadable, corrupt, empty, or stale-year calendar data fails
   closed and cannot authorize new LIVE entries.
2. Yahoo Finance quote validator never fabricates missing `exchangeName` (`venue`) or `currency` metadata;
   preserves `"UNKNOWN"` honestly and enforces required instrument, venue (`NSI`/`NSE`), and currency (`INR`)
   authorization under both `DARVAS_HTTP_SUCCESS` and `STRICT_TIMESTAMP`.
3. When `DARVAS_HTTP_SUCCESS` specifies Yahoo Finance as the executable CMP source, a Yahoo HTTP failure
   (HTTP 500/404, timeout, connection error, or empty JSON) never silently substitutes an unverified Dhan
   `/marketfeed/ltp` price.
4. When broker positions cannot be retrieved or verified (HTTP error, `BrokerReadFailure`, exception,
   malformed non-list/non-dict payload, or open position with `netQty != 0` missing valid `costPrice`/`buyAvg`),
   `RiskManager` rejects new LIVE orders rather than approving them with potentially incomplete exposure data.
5. Independent Review Finding 1: `2026-01-15, 11:00 IST` (NSE Circular `NSE/CMTR/72260`) is classified as
   `Market Closed` (never `Market Open`), blocking LIVE order entries.
6. Independent Review Finding 2: A malformed Dhan HTTP 200 `/positions` response containing an unrelated
   `{"chart": ...}` object or any other dict returns `BrokerReadFailure(status="MALFORMED")`, never an
   empty `BrokerReadList([])`.
7. Independent Review Finding 3: A fresh LIVE-mode `RiskManager` initializes `_broker_positions_verified = False`
   and rejects `validate_order_risk()` before a successful broker-position synchronization, and
   `ProtectionWatcher.execute_retry()` refreshes and verifies broker positions before approving a LIVE retry.
"""

from datetime import date, datetime
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.broker.dhan_client import DhanClient
from src.broker.dhan_token import write_token_atomic
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.market import IST, is_lifecycle_open, is_market_open, load_holidays, load_holidays_verified, market_status
from src.engine.protection_watcher import OrderLifecycleState, ProtectionWatcher
from src.engine.risk_manager import RiskManager
from src.journal.db import JournalDB
from src.main import TradingBotPipeline
from src.models import BrokerReadFailure, BrokerReadList, BrokerReadStatus, SignalAction, TradingMode


class TestV14MarketSafetyRegressions(unittest.TestCase):
    def setUp(self):
        self._orig_env = dict(os.environ)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.tok_path = os.path.join(self.data_dir, ".dhan_token")
        write_token_atomic("VALID_SHARED_JWT_TOKEN_V14", self.tok_path)

    def tearDown(self):
        self.tmp_dir.cleanup()
        os.environ.clear()
        os.environ.update(self._orig_env)

    def _make_live_pipeline(self, cmp_policy: str = "DARVAS_HTTP_SUCCESS") -> TradingBotPipeline:
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            equity_active=True,
            fno_active=True,
            equity_cmp_execution_policy=cmp_policy,
            equity_price_markup_pct=0.5,
            equity_budget_inr=25000.0,
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
        pipe.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ", tick_size=0.05)
        return pipe

    # =========================================================================
    # 1. NSE 2026 HOLIDAY CALENDAR & FAIL-CLOSED CALENDAR VERIFICATION
    # =========================================================================

    def test_01_nse_2026_holiday_calendar_accuracy_and_fail_closed_live_entry_gate(self):
        """
        Verifies:
        - All 16 official 2026 NSE weekday holidays (`NSE/CMTR/71775` + `NSE/CMTR/72260`, including
          `2026-01-15`, `2026-03-26`, `2026-05-28`, `2026-06-26`, `2026-09-14`, `2026-11-10`) are present
          in `data/holidays.json` and block market open.
        - Non-holiday 2026 weekdays previously mislisted (`2026-02-16`, `2026-03-20`, `2026-05-27`, `2026-11-09`)
          are open trading days.
        - Missing, unreadable, corrupt, empty, or stale-year holiday calendar data fails closed and
          cannot authorize new LIVE entries.
        """
        ok_cal, holidays_2026_all, msg_cal = load_holidays_verified(required_year=2026)
        self.assertTrue(ok_cal, msg_cal)

        expected_2026_nse_holidays = {
            date(2026, 1, 15),   # Municipal Corporation Election in Maharashtra (NSE/CMTR/72260)
            date(2026, 1, 26),   # Republic Day
            date(2026, 3, 3),    # Holi
            date(2026, 3, 26),   # Shri Ram Navami
            date(2026, 3, 31),   # Shri Mahavir Jayanti
            date(2026, 4, 3),    # Good Friday
            date(2026, 4, 14),   # Dr. Baba Saheb Ambedkar Jayanti
            date(2026, 5, 1),    # Maharashtra Day
            date(2026, 5, 28),   # Bakri Id
            date(2026, 6, 26),   # Muharram
            date(2026, 9, 14),   # Ganesh Chaturthi
            date(2026, 10, 2),   # Mahatma Gandhi Jayanti
            date(2026, 10, 20),  # Dussehra
            date(2026, 11, 10),  # Diwali-Balipratipada
            date(2026, 11, 24),  # Prakash Gurpurb Sri Guru Nanak Dev
            date(2026, 12, 25),  # Christmas
        }
        actual_2026 = {d for d in holidays_2026_all if d.year == 2026}
        self.assertEqual(actual_2026, expected_2026_nse_holidays)

        for d in expected_2026_nse_holidays:
            dt_hol = datetime(d.year, d.month, d.day, 11, 0, 0, tzinfo=IST)
            is_open, reason = market_status(dt_hol)
            self.assertFalse(is_open, f"Expected holiday closure on {d}")
            self.assertIn("Exchange Holiday", reason)

        # Previously mislisted 2026 weekdays must be open during trading hours
        for open_d in (date(2026, 2, 16), date(2026, 3, 20), date(2026, 5, 27), date(2026, 11, 9)):
            dt_open = datetime(open_d.year, open_d.month, open_d.day, 11, 0, 0, tzinfo=IST)
            is_open, reason = market_status(dt_open)
            self.assertTrue(is_open, f"Expected {open_d} to be open trading weekday, got: {reason}")

        # Missing holidays file -> fails closed
        missing_path = os.path.join(self.tmp_dir.name, "nonexistent_holidays.json")
        dt_regular = datetime(2026, 10, 28, 11, 0, 0, tzinfo=IST)
        is_open_missing, reason_missing = market_status(dt_regular, holidays_path=missing_path)
        self.assertFalse(is_open_missing)
        self.assertIn("Unverified", reason_missing)

        # Corrupt / unreadable JSON -> fails closed
        corrupt_path = os.path.join(self.tmp_dir.name, "corrupt_holidays.json")
        with open(corrupt_path, "w", encoding="utf-8") as f:
            f.write("{corrupt json")
        is_open_corrupt, reason_corrupt = market_status(dt_regular, holidays_path=corrupt_path)
        self.assertFalse(is_open_corrupt)
        self.assertIn("Unverified", reason_corrupt)

        # Empty holidays list -> fails closed
        empty_path = os.path.join(self.tmp_dir.name, "empty_holidays.json")
        with open(empty_path, "w", encoding="utf-8") as f:
            json.dump({"holidays": []}, f)
        is_open_empty, reason_empty = market_status(dt_regular, holidays_path=empty_path)
        self.assertFalse(is_open_empty)
        self.assertIn("Unverified", reason_empty)

        # Calendar missing 2026 coverage -> fails closed
        stale_year_path = os.path.join(self.tmp_dir.name, "only_2025.json")
        with open(stale_year_path, "w", encoding="utf-8") as f:
            json.dump({"holidays": ["2025-01-26", "2025-12-25"]}, f)
        is_open_stale, reason_stale = market_status(dt_regular, holidays_path=stale_year_path)
        self.assertFalse(is_open_stale)
        self.assertIn("2026", reason_stale)

        # End-to-end LIVE pipeline order blocked when HOLIDAYS_FILE is missing/unreadable
        pipe = self._make_live_pipeline()
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
        pipe.broker.get_positions = MagicMock(return_value=BrokerReadList([], status=BrokerReadStatus.OK_EMPTY.value))
        pipe.broker.place_order = MagicMock(return_value=(True, "SHOULD-NOT-PLACE", "OK"))

        os.environ["HOLIDAYS_FILE"] = missing_path
        ts_wed_11am = dt_regular.timestamp()
        res_live = pipe.process_raw_message(
            "Buy #SBIN @ 800",
            channel_id=-1001,
            message_id=701,
            msg_timestamp=ts_wed_11am - 1.0,
            current_time=ts_wed_11am,
        )
        self.assertFalse(res_live.is_valid)
        self.assertIn("LIVE Order Blocked", res_live.reason)
        self.assertIn("Unverified", res_live.reason)
        pipe.broker.place_order.assert_not_called()

    # =========================================================================
    # 2. HONEST YAHOO METADATA & VENUE / CURRENCY AUTHORIZATION
    # =========================================================================

    def test_02_missing_yahoo_exchange_or_currency_preserved_as_unknown_and_rejected(self):
        """
        Verifies:
        - Missing `exchangeName` or `currency` is never fabricated as `'NSI'`, `'NSE'`, `'NSE_EQ'`, or `'INR'`.
        - Preserves `venue='UNKNOWN'` and `currency='UNKNOWN'` honestly.
        - Rejects quotes missing `exchangeName` or `currency` under both `DARVAS_HTTP_SUCCESS` and `STRICT_TIMESTAMP`.
        """
        pipe = self._make_live_pipeline(cmp_policy="DARVAS_HTTP_SUCCESS")

        # 1. Missing exchangeName (and fullExchangeName) -> venue == "UNKNOWN", rejected
        resp_no_exch = MagicMock(status_code=200)
        resp_no_exch.json.return_value = {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "IDEA.NS",
                        "regularMarketPrice": 8.50,
                        "regularMarketTime": 1730200000.0,
                        "currency": "INR",
                    }
                }]
            }
        }
        with patch.object(pipe.broker.session, "get", return_value=resp_no_exch):
            q_no_exch = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
        self.assertFalse(q_no_exch.is_valid)
        self.assertEqual(q_no_exch.venue, "UNKNOWN")
        self.assertEqual(q_no_exch.currency, "INR")
        self.assertIn("exchangeName", q_no_exch.failure_reason)

        # 2. Missing currency -> currency == "UNKNOWN", rejected
        resp_no_curr = MagicMock(status_code=200)
        resp_no_curr.json.return_value = {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "IDEA.NS",
                        "regularMarketPrice": 8.50,
                        "regularMarketTime": 1730200000.0,
                        "exchangeName": "NSI",
                    }
                }]
            }
        }
        with patch.object(pipe.broker.session, "get", return_value=resp_no_curr):
            q_no_curr = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
        self.assertFalse(q_no_curr.is_valid)
        self.assertEqual(q_no_curr.venue, "NSI")
        self.assertEqual(q_no_curr.currency, "UNKNOWN")
        self.assertIn("currency", q_no_curr.failure_reason.lower())

        # 3. Both exchangeName and currency missing -> both preserved as "UNKNOWN", rejected
        resp_both_missing = MagicMock(status_code=200)
        resp_both_missing.json.return_value = {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "IDEA.NS",
                        "regularMarketPrice": 8.50,
                    }
                }]
            }
        }
        with patch.object(pipe.broker.session, "get", return_value=resp_both_missing):
            q_both = pipe.broker.get_yahoo_quote_details("IDEA", "NSE_EQ", cmp_policy="DARVAS_HTTP_SUCCESS")
        self.assertFalse(q_both.is_valid)
        self.assertEqual(q_both.venue, "UNKNOWN")
        self.assertEqual(q_both.currency, "UNKNOWN")

    # =========================================================================
    # 3. NO SILENT DHAN MARKETFEED SUBSTITUTION ON YAHOO FAILURE
    # =========================================================================

    def test_03_yahoo_http_failure_never_substitutes_unverified_dhan_marketfeed_price(self):
        """
        Verifies:
        - When `DARVAS_HTTP_SUCCESS` specifies Yahoo as the executable CMP source, a Yahoo HTTP failure
          (HTTP 500, timeout, connection error, or empty payload) never falls back to or substitutes
          an unverified Dhan `/marketfeed/ltp` price.
        """
        pipe = self._make_live_pipeline(cmp_policy="DARVAS_HTTP_SUCCESS")
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
        pipe.broker.get_positions = MagicMock(return_value=BrokerReadList([], status=BrokerReadStatus.OK_EMPTY.value))

        # Mock Dhan /marketfeed/ltp returning a 200 OK with last_price=8.50 so we prove it is NEVER used
        dhan_ltp_resp = MagicMock(status_code=200)
        dhan_ltp_resp.json.return_value = {
            "data": {"NSE_EQ": {"14366": {"last_price": 8.50}}},
            "status": "success",
        }
        yahoo_500_resp = MagicMock(status_code=500, text="Internal Server Error")

        with patch.object(pipe.broker.session, "get", return_value=yahoo_500_resp) as mock_get, \
             patch.object(pipe.broker.session, "post", return_value=dhan_ltp_resp) as mock_post:
            ok_ltp, price_ltp, msg_ltp = pipe.broker.get_ltp(
                "NSE_EQ",
                "14366",
                symbol="IDEA",
                cmp_policy="DARVAS_HTTP_SUCCESS",
            )
            self.assertFalse(ok_ltp)
            self.assertEqual(price_ltp, 0.0)
            self.assertIn("HTTP 500", msg_ltp)
            mock_get.assert_called_once()
            # Dhan /marketfeed/ltp POST must NOT be called
            mock_post.assert_not_called()

            # Also verify full pipeline blocks the order on Yahoo HTTP failure without calling Dhan /marketfeed/ltp or /orders
            res = pipe.process_raw_message(
                "Buy #IDEA",
                channel_id=-1001,
                message_id=702,
                msg_timestamp=1730200000.0,
                current_time=1730200001.0,
            )
            self.assertFalse(res.is_valid)
            self.assertEqual(res.action, SignalAction.BLOCKED)
            mock_post.assert_not_called()

    # =========================================================================
    # 4. FAIL-CLOSED RISK MANAGER WHEN BROKER POSITIONS CANNOT BE RETRIEVED OR VERIFIED
    # =========================================================================

    def test_04_unverified_or_failed_broker_positions_block_live_order_approval(self):
        """
        Verifies:
        - When `broker.get_positions()` returns `BrokerReadFailure`, `None`, a non-list, raises an exception,
          or returns an open position (`netQty != 0`) with missing/invalid `costPrice`/`buyAvg`,
          `RiskManager` refuses to approve a new LIVE order.
        """
        pipe = self._make_live_pipeline(cmp_policy="DARVAS_HTTP_SUCCESS")
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
        pipe.broker.place_order = MagicMock(return_value=(True, "SHOULD-NOT-PLACE", "OK"))

        failure_cases = [
            BrokerReadFailure(status=BrokerReadStatus.FAILED.value, error="Dhan positions HTTP 500", status_code=500),
            BrokerReadFailure(status=BrokerReadStatus.UNAVAILABLE.value, error="Connection timed out", status_code=0),
            None,
            {"error": "malformed dict instead of list"},
            ["not-a-dict"],
            [{"securityId": "3045", "netQty": "invalid_qty", "costPrice": 800.0}],
            [{"securityId": "3045", "netQty": 25}],  # Open position missing costPrice/buyAvg -> incomplete exposure!
            [{"securityId": "3045", "netQty": 25, "costPrice": 0.0}],  # Zero costPrice on open position -> incomplete exposure!
        ]

        for idx, bad_positions in enumerate(failure_cases, start=1):
            pipe.dedup._seen.clear()
            pipe.broker.get_positions = MagicMock(return_value=bad_positions)
            with patch("src.main.market_status", return_value=(True, "Market Open")):
                res = pipe.process_raw_message(
                    "Buy #SBIN @ 800",
                    channel_id=-1001,
                    message_id=800 + idx,
                    msg_timestamp=1730200000.0,
                    current_time=1730200001.0,
                )
            self.assertFalse(res.is_valid, f"Case {idx} ({bad_positions!r}) should have blocked LIVE order")
            self.assertIn("Broker positions could not be", res.reason)
            pipe.broker.place_order.assert_not_called()

        # Exception raised by get_positions() -> blocks LIVE order
        pipe.dedup._seen.clear()
        pipe.broker.get_positions = MagicMock(side_effect=RuntimeError("Socket hang up reading /positions"))
        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res_exc = pipe.process_raw_message(
                "Buy #SBIN @ 800",
                channel_id=-1001,
                message_id=850,
                msg_timestamp=1730200000.0,
                current_time=1730200001.0,
            )
        self.assertFalse(res_exc.is_valid)
        self.assertIn("Broker positions retrieval failed", res_exc.reason)
        pipe.broker.place_order.assert_not_called()

        # Valid verified open position within exposure limits -> approved
        pipe.dedup._seen.clear()
        pipe.broker.get_positions = MagicMock(
            return_value=BrokerReadList(
                [{"securityId": "3045", "netQty": 10, "costPrice": 800.0, "realizedProfit": 0.0}],
                status=BrokerReadStatus.OK_RECORDS.value,
            )
        )
        pipe.broker.place_order = MagicMock(return_value=(True, "ORD-VERIFIED-1", "OK"))
        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res_ok = pipe.process_raw_message(
                "Buy #SBIN @ 800",
                channel_id=-1001,
                message_id=851,
                msg_timestamp=1730200000.0,
                current_time=1730200001.0,
            )
        self.assertTrue(res_ok.is_valid)
        pipe.broker.place_order.assert_called_once()

    # =========================================================================
    # 5. INDEPENDENT REVIEW FINDING 1: JANUARY 15, 2026 (NSE/CMTR/72260) CLOSED
    # =========================================================================

    def test_05_january_15_2026_nse_holiday_amendment_is_closed(self):
        """
        Reproduces and verifies Finding 1 from the Independent V14 ZIP Review:
        Date: 2026-01-15, 11:00 IST (NSE circular NSE/CMTR/72260) must be classified as Market Closed.
        """
        dt_jan15 = datetime(2026, 1, 15, 11, 0, 0, tzinfo=IST)
        is_open, reason = market_status(dt_jan15)
        self.assertFalse(is_open)
        self.assertFalse(is_market_open(dt_jan15))
        self.assertIn("Market Closed", reason)
        self.assertIn("2026-01-15", reason)

    # =========================================================================
    # 6. INDEPENDENT REVIEW FINDING 2: MALFORMED /POSITIONS CHART OBJECT REJECTED
    # =========================================================================

    def test_06_malformed_dhan_positions_chart_object_returns_broker_read_failure(self):
        """
        Reproduces and verifies Finding 2 from the Independent V14 ZIP Review:
        A mocked Dhan HTTP 200 `/positions` response containing `{"chart": ...}` (or any dict instead
        of a list of position dicts) must return `BrokerReadFailure(status="MALFORMED")`, never
        `BrokerReadList([])`.
        """
        pipe = self._make_live_pipeline()
        malformed_chart_resp = MagicMock(status_code=200)
        malformed_chart_resp.json.return_value = {"chart": {"result": [{"meta": {"regularMarketPrice": 100.0}}]}}

        with patch.object(pipe.broker.session, "get", return_value=malformed_chart_resp):
            pos_res = pipe.broker.get_positions()
            self.assertIsInstance(pos_res, BrokerReadFailure)
            self.assertFalse(pos_res.ok)
            self.assertEqual(pos_res.status, BrokerReadStatus.MALFORMED.value)
            self.assertNotIsInstance(pos_res, list)

            # Verify RiskManager rejects LIVE order when get_positions() returns this malformed response
            ok_sync, sync_err = pipe.risk_manager.sync_from_journal_and_broker(journal_db=pipe.db, broker=pipe.broker)
        self.assertFalse(ok_sync)
        ok_risk, risk_reason = pipe.risk_manager.validate_order_risk(10000.0, 100000.0)
        self.assertFalse(ok_risk)
        self.assertIn("Malformed positions payload", risk_reason)

    # =========================================================================
    # 7. INDEPENDENT REVIEW FINDING 3: FRESH LIVE RISK MANAGER & WATCHER RETRY GATE
    # =========================================================================

    def test_07_fresh_live_risk_manager_requires_sync_and_watcher_retry_verifies_positions(self):
        """
        Reproduces and verifies Finding 3 from the Independent V14 ZIP Review:
        1. A fresh LIVE-mode RiskManager initializes `_broker_positions_verified = False` and rejects
           `validate_order_risk()` before a successful broker-position synchronization.
        2. `ProtectionWatcher.execute_retry()` synchronizes and verifies broker positions via
           `RiskManager` before approving a LIVE partial-fill/timeout retry.
        """
        stop_file = os.path.join(self.data_dir, "stop_f3.json")
        stop_mgr = EmergencyStopManager(state_file=stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="Udit", account_id="1113731536")

        live_cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
        )
        fresh_rm = RiskManager(live_cfg, stop_manager=stop_mgr)

        # 1. Fresh LIVE RiskManager before sync -> must be False and reject validate_order_risk()
        self.assertFalse(fresh_rm._broker_positions_verified)
        passed_unsynced, reason_unsynced = fresh_rm.validate_order_risk(10000.0, 100000.0)
        self.assertFalse(passed_unsynced)
        self.assertIn("not been synchronized or verified", reason_unsynced)

        # 2. ProtectionWatcher.execute_retry() in LIVE mode refreshes broker positions before approving retry
        db = JournalDB(self.db_path, account_id="1113731536")
        client = DhanClient(live_cfg, stop_manager=stop_mgr)
        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.place_order = Mock(return_value=(True, "RETRY-CHILD-1", "OK"))

        watcher = ProtectionWatcher(
            broker=client,
            sl_pct=3.0,
            target_pct=3.0,
            sl_limit_buffer_pct=1.0,
            path=os.path.join(self.data_dir, "pending_f3.json"),
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=db,
            risk_manager=fresh_rm,
        )
        watcher.track("ORD-PARENT-F3", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-PARENT-F3"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["ORD-PARENT-F3"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-PARENT-F3"]["retry_eligible"] = True

        # 2a. When broker.get_positions() fails during retry -> retry is rejected by risk manager
        client.get_positions = Mock(
            return_value=BrokerReadFailure(status=BrokerReadStatus.FAILED.value, error="HTTP 500 on /positions", status_code=500)
        )
        ok_retry_fail, child_fail, msg_fail = watcher.execute_retry(
            "ORD-PARENT-F3",
            current_market_price=100.4,
            quote_timestamp=999.8,
            max_quote_age_sec=2.0,
        )
        self.assertFalse(ok_retry_fail)
        self.assertIsNone(child_fail)
        self.assertIn("Retry rejected by risk manager", msg_fail)
        client.place_order.assert_not_called()

        # 2b. When broker.get_positions() succeeds with verified positions -> retry succeeds
        watcher.pending["ORD-PARENT-F3"]["retry_count"] = 0
        watcher.pending["ORD-PARENT-F3"]["retry_reserved"] = False
        watcher.pending["ORD-PARENT-F3"]["retry_eligible"] = True
        client.get_positions = Mock(
            return_value=BrokerReadList(
                [{"securityId": "3045", "netQty": 5, "costPrice": 100.0, "realizedProfit": 0.0}],
                status=BrokerReadStatus.OK_RECORDS.value,
            )
        )
        ok_retry_pass, child_pass, msg_pass = watcher.execute_retry(
            "ORD-PARENT-F3",
            current_market_price=100.4,
            quote_timestamp=999.8,
            max_quote_age_sec=2.0,
        )
        self.assertTrue(ok_retry_pass, msg_pass)
        self.assertEqual(child_pass, "RETRY-CHILD-1")
        client.get_positions.assert_called()
        client.place_order.assert_called_once()


if __name__ == "__main__":
    unittest.main()
