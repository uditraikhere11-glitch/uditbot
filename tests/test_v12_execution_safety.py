"""
Mandatory Offline Regression Suite for AUDITBOT V12 — Consolidated Execution-Safety Correction.
Tests all 18 mandatory scenarios (Section I.1 through I.18) independently with fixed timestamps
and mocked HTTP transports (zero external network access):

1. Yahoo quote age 1.9 seconds — eligible if all other conditions pass.
2. Age 2.0 seconds — eligible at the specified inclusive maximum.
3. Age 2.1 seconds — rejected.
4. Missing / invalid / future source timestamp — rejected.
5. BSE delayed quote (`.BO`) — rejected for LIVE.
6. Correct price but wrong exchange / security identity — rejected.
7. Fresh HTTP response with stale market timestamp — rejected.
8. NSE F&O instrument using only underlying Yahoo price — rejected.
9. Retry movement 0.99% (eligible), 1.00% (rejected), 1.01% (rejected).
10. Configured markup 0.5%, 1.0%, 1.5% (capped at 1.00%; XX% + 1% prohibited).
11. Journal failure — zero broker POSTs.
12. Two concurrent retries — maximum one child.
13. F&O retry preserves exact contract and lot size (never appends `-EQ` or defaults to `CNC`).
14. Super Order token error with ambiguous outcome — zero resubmission.
15. Missing fill price — no invented protective price (`original_market_price` / `100.0` fallback removed).
16. OCO ACK without active legs — not PROTECTED (`UNPROTECTED` + gate halted).
17. Failed Super Order reanchoring — no false confirmation (`UNPROTECTED` + gate halted).
18. Original web setup workflows remain operational and byte-for-byte intact.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.models import OrderRequest, ProductType, TradingMode
from src.broker.dhan_client import DhanClient
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher, OrderLifecycleState
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.journal.db import JournalDB
from src.setup_app import get_setup_page, handle_setup_submission


def _make_yf_response(
    price: float = 810.0,
    quote_ts: object = 1730200000.0,
    symbol: str = "SBIN.NS",
    exchange: str = "NSI",
    currency: str = "INR",
    market_state: str = "REGULAR",
    delayed_by: object = 0,
    security_id: str = "",
):
    meta = {
        "regularMarketPrice": price,
        "symbol": symbol,
        "exchangeName": exchange,
        "currency": currency,
        "marketState": market_state,
    }
    if quote_ts is not None:
        meta["regularMarketTime"] = quote_ts
    if delayed_by is not None:
        meta["exchangeDataDelayedBy"] = delayed_by
    if security_id:
        meta["securityId"] = security_id

    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"chart": {"result": [{"meta": meta}]}}
    return resp


class TestV12ExecutionSafetyRegressions(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.temp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.stop_file = os.path.join(self.data_dir, "emergency_stop.json")
        self.db_path = os.path.join(self.data_dir, "journal.db")
        self.pending_file = os.path.join(self.data_dir, "pending_protection.json")

        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=os.path.join(self.data_dir, "token.txt"),
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            max_quote_age_sec=2.0,
        )
        self.stop_mgr = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        self.stop_mgr.provision_initial_clean_state("Udit", "1113731536")
        self.db = JournalDB(self.db_path, account_id="1113731536")

    def tearDown(self):
        self.temp_dir.cleanup()

    # ------------------------------------------------------------------
    # Scenario 1: Yahoo quote age 1.9 seconds — eligible
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_01_yahoo_quote_age_1_9s_eligible(self, mock_get):
        mock_get.return_value = _make_yf_response(price=812.50, quote_ts=1730200000.0, symbol="SBIN.NS")
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        q = client.get_yahoo_quote_details("SBIN", "NSE_EQ", security_id="3045", now_utc=1730200001.9)
        self.assertTrue(q.is_valid)
        self.assertEqual(q.price, 812.50)
        self.assertAlmostEqual(q.age_sec, 1.9, places=4)
        self.assertFalse(q.is_delayed)

    # ------------------------------------------------------------------
    # Scenario 2: Yahoo quote age 2.0 seconds — eligible (inclusive max)
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_02_yahoo_quote_age_2_0s_inclusive_eligible(self, mock_get):
        mock_get.return_value = _make_yf_response(price=812.50, quote_ts=1730200000.0, symbol="SBIN.NS")
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        q = client.get_yahoo_quote_details("SBIN", "NSE_EQ", security_id="3045", now_utc=1730200002.0)
        self.assertTrue(q.is_valid)
        self.assertEqual(q.price, 812.50)
        self.assertAlmostEqual(q.age_sec, 2.0, places=4)

    # ------------------------------------------------------------------
    # Scenario 3: Yahoo quote age 2.1 seconds — rejected
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_03_yahoo_quote_age_2_1s_rejected(self, mock_get):
        mock_get.return_value = _make_yf_response(price=812.50, quote_ts=1730200000.0, symbol="SBIN.NS")
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        q = client.get_yahoo_quote_details("SBIN", "NSE_EQ", security_id="3045", now_utc=1730200002.1)
        self.assertFalse(q.is_valid)
        self.assertIn("Stale source quote rejected", q.failure_reason)

        # Also prove a previously cached quote expires after 2.0s and cannot pass a later check
        q_fresh = client.get_yahoo_quote_details("SBIN", "NSE_EQ", security_id="3045", now_utc=1730200001.0)
        self.assertTrue(q_fresh.is_valid)
        q_expired_cache = client.get_yahoo_quote_details(
            "SBIN", "NSE_EQ", security_id="3045", now_utc=1730200002.5, allow_cached=True
        )
        self.assertFalse(q_expired_cache.is_valid)

    # ------------------------------------------------------------------
    # Scenario 4: Missing / invalid / future source timestamp — rejected
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_04_missing_invalid_or_future_timestamp_rejected(self, mock_get):
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        # 4a. Missing regularMarketTime
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=None)
        q_missing = client.get_yahoo_quote_details("SBIN", "NSE_EQ", now_utc=1730200000.0)
        self.assertFalse(q_missing.is_valid)
        self.assertIn("Missing or invalid source quote timestamp", q_missing.failure_reason)

        # 4b. Invalid string/zero regularMarketTime
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=0)
        q_zero = client.get_yahoo_quote_details("SBIN", "NSE_EQ", now_utc=1730200000.0)
        self.assertFalse(q_zero.is_valid)

        # 4c. Future timestamp (quote_ts > now_utc -> negative age)
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=1730200005.0)
        q_future = client.get_yahoo_quote_details("SBIN", "NSE_EQ", now_utc=1730200000.0)
        self.assertFalse(q_future.is_valid)
        self.assertIn("Future source quote timestamp rejected", q_future.failure_reason)

    # ------------------------------------------------------------------
    # Scenario 5: BSE delayed quote (`.BO`) — rejected for LIVE
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_05_bse_delayed_quote_rejected_for_live(self, mock_get):
        mock_get.return_value = _make_yf_response(
            price=810.0, quote_ts=1730200000.0, symbol="SBIN.BO", exchange="BSE", delayed_by=900
        )
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        q = client.get_yahoo_quote_details("SBIN", "BSE_EQ", now_utc=1730200000.5)
        self.assertFalse(q.is_valid)
        self.assertTrue(q.is_delayed)
        self.assertIn("Delayed quote rejected", q.failure_reason)

    # ------------------------------------------------------------------
    # Scenario 6: Correct price but wrong exchange / security identity — rejected
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_06_wrong_exchange_or_security_identity_rejected(self, mock_get):
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        # 6a. Wrong symbol in payload (requested SBIN.NS, got RELIANCE.NS)
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=1730200000.0, symbol="RELIANCE.NS")
        q_sym = client.get_yahoo_quote_details("SBIN", "NSE_EQ", now_utc=1730200001.0)
        self.assertFalse(q_sym.is_valid)
        self.assertIn("Instrument identity mismatch", q_sym.failure_reason)

        # 6b. Wrong exchange venue in payload (requested NSE_EQ, got NYSE)
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=1730200000.0, symbol="SBIN.NS", exchange="NYSE")
        q_exch = client.get_yahoo_quote_details("SBIN", "NSE_EQ", now_utc=1730200001.0)
        self.assertFalse(q_exch.is_valid)
        self.assertIn("Venue mismatch", q_exch.failure_reason)

        # 6c. Wrong securityId in payload
        mock_get.return_value = _make_yf_response(
            price=810.0, quote_ts=1730200000.0, symbol="SBIN.NS", security_id="9999"
        )
        q_sec = client.get_yahoo_quote_details("SBIN", "NSE_EQ", security_id="3045", now_utc=1730200001.0)
        self.assertFalse(q_sec.is_valid)
        self.assertIn("Security ID mismatch", q_sec.failure_reason)

    # ------------------------------------------------------------------
    # Scenario 7: Fresh HTTP response with stale market timestamp — rejected
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_07_fresh_http_response_with_stale_market_timestamp_rejected(self, mock_get):
        # HTTP call completes in <1ms at t=1730200000.0, but Yahoo market quote is from 60 seconds ago
        mock_get.return_value = _make_yf_response(price=810.0, quote_ts=1730199940.0, symbol="SBIN.NS")
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        ok, ltp, msg = client.fetch_yahoo_ltp("SBIN", "NSE_EQ", now_utc=1730200000.0)
        self.assertFalse(ok)
        self.assertEqual(ltp, 0.0)
        self.assertIn("Stale source quote rejected", msg)

    # ------------------------------------------------------------------
    # Scenario 8: NSE F&O instrument using only underlying Yahoo price — rejected
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_08_nse_fno_using_underlying_yahoo_price_rejected(self, mock_get):
        mock_get.return_value = _make_yf_response(price=7000.0, quote_ts=1730200000.0, symbol="POLYCAB.NS")
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        q = client.get_yahoo_quote_details("POLYCAB", "NSE_FNO", security_id="54231", now_utc=1730200001.0)
        self.assertFalse(q.is_valid)
        self.assertIn("cannot substitute underlying equity quote for F&O", q.failure_reason)
        mock_get.assert_not_called()

    # ------------------------------------------------------------------
    # Scenario 9: Retry movement 0.99% (eligible), 1.00% (rejected), 1.01% (rejected)
    # ------------------------------------------------------------------
    def test_09_retry_movement_099_100_101_boundaries(self):
        mock_broker = MagicMock()
        mock_broker.cfg = self.cfg
        mock_broker.stop_manager = self.stop_mgr
        watcher = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=self.db
        )

        # Parent 1: test 1.01% (rejected) and 1.00% (rejected), then 0.99% (approved)
        oid = watcher.track("ORD-M1", "SBIN", "3045", "NSE_EQ", qty=20, original_market_price=1000.0)
        watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value

        ok_101, _, r_101 = watcher.evaluate_retry(oid, current_market_price=1010.10)  # 1.01%
        self.assertFalse(ok_101)
        self.assertIn("1.00%", r_101)

        ok_100, _, r_100 = watcher.evaluate_retry(oid, current_market_price=1010.00)  # 1.00%
        self.assertFalse(ok_100)
        self.assertIn("1.00%", r_100)

        ok_099, qty_099, r_099 = watcher.evaluate_retry(oid, current_market_price=1009.90)  # 0.99%
        self.assertTrue(ok_099)
        self.assertEqual(qty_099, 20)
        self.assertIn("Retry approved", r_099)

    # ------------------------------------------------------------------
    # Scenario 10: Configured markup 0.5%, 1.0%, 1.5% (capped at 1.00%; XX% + 1% prohibited)
    # ------------------------------------------------------------------
    def test_10_configured_markup_05_10_15_capped_at_1_percent(self):
        mock_broker = MagicMock()
        mock_broker.cfg = self.cfg
        mock_broker.stop_manager = self.stop_mgr
        mock_broker.place_order.return_value = (True, "CHILD-1", "OK")

        watcher = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=self.db
        )

        # 10a. Markup 0.5% on CMP 1000.0 -> 1005.00
        o1 = watcher.track("ORD-MK-05", "SBIN", "3045", "NSE_EQ", qty=10, original_market_price=1000.0)
        watcher.pending[o1]["state"] = OrderLifecycleState.CANCELLED.value
        ok1, _, _ = watcher.execute_retry(o1, current_market_price=1000.0, markup_pct=0.5)
        self.assertTrue(ok1)
        self.assertEqual(mock_broker.place_order.call_args[0][0].price, 1005.00)

        # 10b. Markup 1.0% on CMP 1000.0 -> 1010.00
        mock_broker.place_order.return_value = (True, "CHILD-2", "OK")
        o2 = watcher.track("ORD-MK-10", "INFY", "1594", "NSE_EQ", qty=10, original_market_price=1000.0)
        watcher.pending[o2]["state"] = OrderLifecycleState.CANCELLED.value
        ok2, _, _ = watcher.execute_retry(o2, current_market_price=1000.0, markup_pct=1.0)
        self.assertTrue(ok2)
        self.assertEqual(mock_broker.place_order.call_args[0][0].price, 1010.00)

        # 10c. Markup 1.5% on CMP 1000.0 -> capped at 1.00% -> 1010.00 (NEVER 1015.00 or 1025.00!)
        mock_broker.place_order.return_value = (True, "CHILD-3", "OK")
        o3 = watcher.track("ORD-MK-15", "TCS", "11536", "NSE_EQ", qty=10, original_market_price=1000.0)
        watcher.pending[o3]["state"] = OrderLifecycleState.CANCELLED.value
        ok3, _, _ = watcher.execute_retry(o3, current_market_price=1000.0, markup_pct=1.5)
        self.assertTrue(ok3)
        self.assertEqual(mock_broker.place_order.call_args[0][0].price, 1010.00)

    # ------------------------------------------------------------------
    # Scenario 11: Journal failure — zero broker POSTs
    # ------------------------------------------------------------------
    def test_11_journal_failure_produces_zero_broker_posts(self):
        mock_broker = MagicMock()
        mock_broker.cfg = self.cfg
        mock_broker.stop_manager = self.stop_mgr

        # 11a. Missing journal_db (None) -> blocked with 0 POSTs
        watcher_no_db = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=None
        )
        oid1 = watcher_no_db.track("ORD-NODB", "SBIN", "3045", "NSE_EQ", qty=10, original_market_price=800.0)
        watcher_no_db.pending[oid1]["state"] = OrderLifecycleState.CANCELLED.value
        ok1, _, msg1 = watcher_no_db.execute_retry(oid1, current_market_price=802.0)
        self.assertFalse(ok1)
        self.assertIn("active durable SQLite journal is required", msg1)
        mock_broker.place_order.assert_not_called()

        # 11b. Journal DB write raises OperationalError -> blocked with 0 POSTs
        failing_db = MagicMock()
        failing_db.record_order_intent.side_effect = RuntimeError("SQLite database is locked / read-only")
        watcher_fail_db = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=failing_db
        )
        oid2 = watcher_fail_db.track("ORD-FAILDB", "SBIN", "3045", "NSE_EQ", qty=10, original_market_price=800.0)
        watcher_fail_db.pending[oid2]["state"] = OrderLifecycleState.CANCELLED.value
        ok2, _, msg2 = watcher_fail_db.execute_retry(oid2, current_market_price=802.0)
        self.assertFalse(ok2)
        self.assertIn("journal write failed", msg2)
        mock_broker.place_order.assert_not_called()

    # ------------------------------------------------------------------
    # Scenario 12: Two concurrent retries — maximum one child
    # ------------------------------------------------------------------
    def test_12_two_concurrent_retries_maximum_one_child(self):
        mock_broker = MagicMock()
        mock_broker.cfg = self.cfg
        mock_broker.stop_manager = self.stop_mgr
        mock_broker.place_order.return_value = (True, "SINGLE-CHILD-ORD", "OK")

        watcher = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=self.db
        )
        oid = watcher.track("ORD-CONC-RETRY", "SBIN", "3045", "NSE_EQ", qty=50, original_market_price=800.0)
        watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value

        def call_retry(_):
            return watcher.execute_retry(oid, current_market_price=804.0, markup_pct=0.5)

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(call_retry, range(8)))

        successes = [o for o in outcomes if o[0] is True]
        failures = [o for o in outcomes if o[0] is False]
        self.assertEqual(len(successes), 1, "At most ONE child retry may succeed across concurrent threads")
        self.assertEqual(len(failures), 7)
        self.assertEqual(mock_broker.place_order.call_count, 1)

    # ------------------------------------------------------------------
    # Scenario 13: F&O retry preserves exact contract and lot size
    # ------------------------------------------------------------------
    def test_13_fno_retry_preserves_exact_contract_and_lot_size(self):
        mock_broker = MagicMock()
        mock_broker.cfg = self.cfg
        mock_broker.stop_manager = self.stop_mgr
        mock_broker.place_super_order.return_value = (True, "OPT-RETRY-CHILD-1", "OK")

        watcher = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=self.db
        )
        oid = watcher.track(
            order_id="OPT-PARENT-1",
            symbol="POLYCAB",
            trading_symbol="POLYCAB-26NOV26-8000-PE",
            security_id="54231",
            segment="NSE_FNO",
            qty=200,
            product_type="MARGIN",
            original_market_price=100.0,
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
            option_type="PE",
            strike=8000.0,
            expiry=date(2026, 11, 26),
        )
        # 1 lot (100 units) filled before cancel; 1 lot (100 units) remaining
        watcher.pending[oid]["filled_qty"] = 100
        watcher.pending[oid]["protected_qty"] = 100
        watcher.pending[oid]["state"] = OrderLifecycleState.CANCELLED.value

        ok, child_id, msg = watcher.execute_retry(
            oid, current_market_price=100.50, markup_pct=0.5, reference_date=date(2026, 10, 29)
        )
        self.assertTrue(ok, f"F&O retry failed: {msg}")
        self.assertEqual(child_id, "OPT-RETRY-CHILD-1")
        req = mock_broker.place_super_order.call_args[0][0]
        self.assertEqual(req.symbol, "POLYCAB-26NOV26-8000-PE")
        self.assertFalse(req.symbol.endswith("-EQ"), "F&O retry must NEVER append -EQ")
        self.assertEqual(req.exchange_segment, "NSE_FNO")
        self.assertEqual(req.product_type, ProductType.MARGIN)
        self.assertEqual(req.security_id, "54231")
        self.assertEqual(req.quantity, 100)

        # Also verify unknown product_type is rejected rather than defaulting to CNC
        oid_bad = watcher.track(
            order_id="ORD-BAD-PROD",
            symbol="SBIN",
            security_id="3045",
            segment="NSE_EQ",
            qty=10,
            product_type="UNKNOWN_PROD",
            original_market_price=800.0,
        )
        watcher.pending[oid_bad]["state"] = OrderLifecycleState.CANCELLED.value
        ok_bad, _, msg_bad = watcher.execute_retry(oid_bad, current_market_price=802.0)
        self.assertFalse(ok_bad)
        self.assertIn("defaulting to CNC is prohibited", msg_bad)

    # ------------------------------------------------------------------
    # Scenario 14: Super Order token error with ambiguous outcome — zero resubmission
    # ------------------------------------------------------------------
    @patch("requests.Session.get")
    @patch("requests.Session.post")
    def test_14_super_order_token_error_ambiguous_outcome_zero_resubmission(self, mock_post, mock_get):
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)

        # First POST to /super/orders returns 401 token error; second POST is to /app/generateAccessToken (auth only)
        super_401 = MagicMock()
        super_401.status_code = 401
        super_401.text = '{"errorCode": "DH-906", "errorMessage": "Invalid Token"}'

        totp_200 = MagicMock()
        totp_200.status_code = 200
        totp_200.json.return_value = {"accessToken": "NEW_TOKEN_AFTER_401"}

        mock_post.side_effect = [super_401, totp_200]

        # Read-only correlation lookup returns empty (ambiguous whether first order landed before 401)
        empty_get = MagicMock()
        empty_get.status_code = 404
        empty_get.json.return_value = {}
        mock_get.return_value = empty_get

        req = OrderRequest(
            correlation_id="SUPER-AMBIG-1",
            symbol="POLYCAB-26NOV26-8000-PE",
            security_id="54231",
            exchange_segment="NSE_FNO",
            product_type=ProductType.MARGIN,
            price=100.0,
            quantity=100,
            trading_mode=TradingMode.LIVE,
        )
        ok, oid, msg = client.place_super_order(req, stop_loss_price=97.0, target_price=103.0)
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertIn("automatic resubmission prohibited", msg)

        # Verify /super/orders was called AT MOST ONCE (never recursively resubmitted!)
        super_post_calls = [c for c in mock_post.call_args_list if "/super/orders" in str(c[0][0])]
        self.assertEqual(len(super_post_calls), 1, "Super Order POST must NEVER be resubmitted after auth error")

    # ------------------------------------------------------------------
    # Scenario 15: Missing fill price — no invented protective price
    # ------------------------------------------------------------------
    def test_15_missing_fill_price_no_invented_protective_price(self):
        mock_broker = MagicMock()
        gate = StartupReconciliationGate(mock_broker, self.db, MagicMock(), self.stop_mgr)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        mock_broker.reconciliation_gate = gate

        watcher = ProtectionWatcher(
            broker=mock_broker, path=self.pending_file, dry_run=False, clock=lambda: 1000.0, journal_db=self.db
        )
        # Track order WITH original_market_price=850.0, but broker reports TRADED with averageTradedPrice=0.0 / None
        oid = watcher.track(
            "ORD-NO-FILL-PRICE", "SBIN", "3045", "NSE_EQ", qty=20, original_market_price=850.0
        )
        mock_broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 20,
            "averageTradedPrice": 0.0,
        }
        mock_broker.trades_avg_price.return_value = None

        watcher.check_once()

        # MUST NOT call place_forever_oco with 850.0 or 100.0!
        mock_broker.place_forever_oco.assert_not_called()
        self.assertEqual(watcher.pending[oid]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries, "Missing fill price must block new entries")
        unprot = self.db.get_unprotected_orders()
        self.assertEqual(len(unprot), 1)
        self.assertEqual(unprot[0]["status"], "UNPROTECTED")

    # ------------------------------------------------------------------
    # Scenario 16: OCO ACK without active legs — not PROTECTED
    # ------------------------------------------------------------------
    def test_16_oco_ack_without_active_legs_not_protected(self):
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)
        gate = StartupReconciliationGate(client, self.db, MagicMock(), self.stop_mgr)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        watcher = ProtectionWatcher(
            broker=client,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=self.db,
            require_broker_leg_confirmation=True,
        )
        oid = watcher.track("ORD-OCO-EMPTY", "SBIN", "3045", "NSE_EQ", qty=15)

        client.order_status = MagicMock(
            return_value={"orderStatus": "TRADED", "filledQty": 15, "averageTradedPrice": 800.0}
        )
        client.trades_avg_price = MagicMock(return_value=800.0)
        # Dhan returns order ID on POST, but Forever order book is empty (no active legs)
        client.get_positions = MagicMock(return_value=[{"securityId": "3045", "netQty": 15}])
        client.place_forever_oco = MagicMock(return_value=(True, "OCO-GHOST-1", "Placed"))
        client.get_forever_orders = MagicMock(return_value=[])

        watcher.check_once()

        self.assertEqual(watcher.pending[oid]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries)
        unprot = self.db.get_unprotected_orders()
        self.assertEqual(len(unprot), 1)
        self.assertEqual(unprot[0]["status"], "UNPROTECTED")

    # ------------------------------------------------------------------
    # Scenario 17: Failed Super Order reanchoring — no false confirmation
    # ------------------------------------------------------------------
    def test_17_failed_super_order_reanchoring_no_false_confirmation(self):
        client = DhanClient(self.cfg, stop_manager=self.stop_mgr)
        gate = StartupReconciliationGate(client, self.db, MagicMock(), self.stop_mgr)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        watcher = ProtectionWatcher(
            broker=client,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=self.db,
        )
        oid = watcher.track(
            "SUPER-REANCHOR-FAIL",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=100,
            product_type="MARGIN",
            is_super_order=True,
        )
        client.order_status = MagicMock(
            return_value={"orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 95.0}
        )
        client.trades_avg_price = MagicMock(return_value=95.0)
        client.modify_super_order_protection = MagicMock(
            return_value=(False, "Failed to modify STOP_LOSS_LEG (400): RMS rejected")
        )

        watcher.check_once()

        self.assertEqual(watcher.pending[oid]["protected_qty"], 0)
        self.assertEqual(watcher.pending[oid].get("reanchor_status"), "FAILED")
        self.assertFalse(gate.is_accepting_entries)
        unprot = self.db.get_unprotected_orders()
        self.assertEqual(len(unprot), 1)
        self.assertEqual(unprot[0]["status"], "UNPROTECTED")

    # ------------------------------------------------------------------
    # Scenario 18: Original web setup workflows remain operational
    # ------------------------------------------------------------------
    def test_18_original_web_setup_workflows_remain_operational(self):
        import hashlib
        import inspect
        from src.cli import cmd_web

        # Verify cmd_web preserves localhost default bind
        sig = inspect.signature(cmd_web)
        self.assertEqual(sig.parameters["host"].default, "127.0.0.1")

        # Verify byte-for-byte SHA-256 parity of protected setup files
        for rel_path, expected in [
            ("src/setup_app.py", "ac2e108f051db971034d5c1c64bc1a8a1083f01304761ce84f67c47fd5f00a93"),
            ("src/setup_wizard.py", "72223eb1f1671fd2dd6f083d435cbc922ff818e7d45e96e40c05916aa7b29d14"),
            ("tests/test_setup.py", "256981a20af06ea2a1fb524326a30dfcb6658b1f5451b5780a3019424104c87e"),
        ]:
            with open(rel_path, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
            self.assertEqual(digest, expected)

        resp = get_setup_page()
        html = resp.body.decode("utf-8")
        self.assertIn("Audit bot Setup", html)
        self.assertIn("Exchange Credentials", html)
        self.assertIn("Execution Mode", html)

        old_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        try:
            saved_resp = handle_setup_submission(
                is_live_mode=False,
                equity_active=True,
                fno_active=True,
                operator_name="Udit",
                instance_name="Audit bot",
                tg_api_id="123456",
                tg_api_hash="mock_hash",
                monitored_channel_ids="-100111,-100222",
                log_group_id="-5514889030",
                test_group_id="-5100801652",
                ignore_backlog="true",
                dhan_client_id="1113731536",
                dhan_pin="123456",
                dhan_totp_secret="SECRET",
                dhan_access_token="TOKEN",
                equity_budget_inr=25000.0,
                equity_product_type="CNC",
                equity_price_markup_pct=0.5,
                fno_capital_per_trade=20000.0,
                fno_max_lots=3,
                max_order_amount_inr=50000.0,
                min_trade_funds_inr=10000.0,
                max_daily_loss_inr=15000.0,
                max_daily_trades=20,
                max_total_exposure_inr=150000.0,
                max_open_positions=5,
                sl_pct=3.0,
                target_pct=3.0,
                sl_limit_buffer_pct=1.0,
                max_message_age_sec=60.0,
                dedup_window_sec=20.0,
                max_quote_age_sec=2.0,
            )
            saved_html = saved_resp.body.decode("utf-8")
            self.assertIn("Audit bot Configuration Saved & Applied!", saved_html)
            env_path = os.path.join(self.temp_dir.name, ".env")
            self.assertTrue(os.path.exists(env_path))
            with open(env_path, "r", encoding="utf-8") as ef:
                env_text = ef.read()
            self.assertIn("DHAN_CLIENT_ID=1113731536", env_text)
            self.assertIn("TRADING_MODE=TEST", env_text)
        finally:
            os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
