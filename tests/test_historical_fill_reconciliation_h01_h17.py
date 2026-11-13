"""
Mandatory Offline Acceptance Suite (H01 through H17) for Future-Proof Dhan Historical Fill Reconciliation.

Covers all 17 scenarios specified in `INDEPENDENT_CODE_AUDIT.md` and `DEVELOPMENT_BRIEF.md`:
- H01: ACKED 1-share BUY on Monday, exact Dhan fill exists, restart Friday; today's books empty.
- H02: Two independent 1-share trades, same symbol, different order IDs and dates (Nov 2 & Nov 4 YESBANK).
- H03: Holdings=2 but historical fills missing/mismatched -> remains UNVERIFIED / BLOCKED.
- H04: Historical result missing account/security/orderId/product/side/exchangeTradeId -> rejected.
- H05: Multi-page responses and duplicated trade IDs -> deduplicated; conflicting duplicates rejected.
- H06: HTTP 401/403/429/500, timeout, corrupt JSON, page cap reached -> no false TRADED; fail closed.
- H07: ACKED 5-share order with only 2 verified fills -> PART_FILLED; remainder unresolved; blocks gate.
- H08: Full verified fill but no OCO, settled DP holdings remain -> TRADED entry but UNPROTECTED holding; blocks gate.
- H09: Simulated SQLite crash between fill and order status update -> atomic rollback / idempotent repair.
- H10: Re-run recovery 5 times -> stable counts, zero duplicate fills, order writes, or notifications.
- H11: Restart with `watcher_orders.record_json` stale `PENDING/filled=0` -> consistent watcher migration.
- H12: Broker account A response while configured for account B -> rejected; never updates B's journal.
- H13: Emergency STOP active / protection unresolved -> historical read-only recovery allowed; gate stays closed.
- H14: Startup auth verified but reconciliation blocked -> Telegram cards report auth verified & entries BLOCKED.
- H15: History outside API-supported retention (>31d) or missing broker_order_id -> escalates for manual review.
- H16: LIVE test order explicitly owner acknowledged -> narrow audited UNPROTECTED_MANUAL_RISK; no global bypass.
- H17: `bypass_reconciliation.py` disabled and zero automatic order mutations during historical reconciliation.
"""

import datetime
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch
import zoneinfo

import requests

from src.broker.dhan_client import BASE_URL, DhanClient, MAX_HISTORICAL_TRADE_WINDOW_DAYS
from src.broker.dhan_token import write_token_atomic
from src.cli import (
    cmd_daily_health_check,
    format_morning_health_report,
    format_startup_online_card,
)
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import (
    OrderLifecycleState,
    PositionLifecycleState,
    ProtectionWatcher,
)
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, BrokerReadList, TradingMode

FAKE_ACCT = "9000000001"
OTHER_ACCT = "9999888877"
IST = zoneinfo.ZoneInfo("Asia/Kolkata")


def _http_resp(status: int, payload=None, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text or (json.dumps(payload) if payload is not None and not isinstance(payload, Exception) else "")
    if isinstance(payload, Exception):
        r.json.side_effect = payload
    else:
        r.json.return_value = payload
    return r


def _active_forever_oco_rows(
    forever_id: str,
    security_id: str = "11915",
    qty: int = 1,
    sl_price: float = 15.05,
    target_price: float = 16.00,
    account_id: str = FAKE_ACCT,
):
    return [
        {
            "orderId": forever_id,
            "dhanClientId": account_id,
            "securityId": security_id,
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "SELL",
            "orderStatus": "PENDING",
            "legName": "STOP_LOSS_LEG",
            "quantity": qty,
            "remainingQuantity": qty,
            "triggerPrice": sl_price,
            "price": sl_price,
        },
        {
            "orderId": forever_id,
            "dhanClientId": account_id,
            "securityId": security_id,
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "SELL",
            "orderStatus": "PENDING",
            "legName": "TARGET_LEG",
            "quantity": qty,
            "remainingQuantity": qty,
            "price": target_price,
            "triggerPrice": target_price,
        },
    ]


class TestHistoricalFillReconciliationH01ToH17(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = os.path.join(self.tmp.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.tok_path = os.path.join(self.tmp.name, ".dhan_token_test")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.watcher_path = os.path.join(self.data_dir, "pending_protection.json")

        write_token_atomic("TEST_ACTIVE_TOKEN", self.tok_path)
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id=FAKE_ACCT,
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            tg_bot_token="123456:FAKE_BOT_TOKEN",
            log_group_id=-1009000000001,
            instance_name="AuditBot Test",
        )
        self.db = JournalDB(self.db_path, account_id=FAKE_ACCT)
        self.stop_mgr = EmergencyStopManager(self.stop_path, expected_account_id=FAKE_ACCT)
        self.stop_mgr.provision_initial_clean_state("OperatorTest", FAKE_ACCT)
        self.client = DhanClient(self.cfg, stop_manager=self.stop_mgr, auto_generate_token=False)
        self.watcher = ProtectionWatcher(
            self.client,
            path=self.watcher_path,
            dry_run=False,
            journal_db=self.db,
        )
        self.gate = StartupReconciliationGate(
            broker=self.client,
            journal_db=self.db,
            watcher=self.watcher,
            stop_manager=self.stop_mgr,
        )
        self.client.reconciliation_gate = self.gate

    def _seed_acked_intent(
        self,
        *,
        correlation_id: str,
        broker_order_id: str,
        symbol: str = "YESBANK-EQ",
        security_id: str = "11915",
        segment: str = "NSE_EQ",
        product_type: str = "CNC",
        price: float = 15.60,
        quantity: int = 1,
        created_ts: float,
    ) -> None:
        self.db.record_order_intent(
            correlation_id=correlation_id,
            symbol=symbol,
            security_id=security_id,
            exchange_segment=segment,
            product_type=product_type,
            order_type="LIMIT",
            price=price,
            quantity=quantity,
            account_id=FAKE_ACCT,
            original_market_price=price,
        )
        self.db.update_order_intent(correlation_id, state="ACKED", broker_order_id=broker_order_id)
        self.db.record_order(
            correlation_id=correlation_id,
            trading_mode="LIVE",
            product_type=product_type,
            exchange_segment=segment,
            symbol=symbol,
            security_id=security_id,
            order_type="LIMIT",
            price=price,
            quantity=quantity,
            status="PENDING",
            broker_order_id=broker_order_id,
        )
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE order_intents SET created_at = ?, updated_at = ? WHERE correlation_id = ?",
                (created_ts, created_ts, correlation_id),
            )
            conn.execute(
                "UPDATE orders SET created_at = ?, updated_at = ? WHERE correlation_id = ?",
                (created_ts, created_ts, correlation_id),
            )
        self.watcher.track(
            order_id=broker_order_id,
            correlation_id=correlation_id,
            symbol=symbol,
            security_id=security_id,
            segment=segment,
            qty=quantity,
            product_type=product_type,
            original_market_price=price,
        )

    # ------------------------------------------------------------------
    # H01: ACKED 1-share BUY on Monday, exact Dhan fill exists, restart Friday; today's books empty
    # ------------------------------------------------------------------
    def test_h01_monday_acked_1_share_buy_reconciled_on_friday_when_today_books_empty(self):
        mon_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=mon_ts,
        )

        hist_trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "900001102657701",
            "exchangeTradeId": "ETID-20261102-001",
            "tradingSymbol": "YESBANK-EQ",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if url.endswith("/forever/orders"):
                return _http_resp(200, _active_forever_oco_rows("FO-YES-1", "11915", 1))
            if url.endswith("/super/orders") or url.endswith("/trades") or url.endswith("/positions") or url.endswith("/orders"):
                return _http_resp(200, [])
            if "/orders/external/" in url or "/orders/900001102657701" in url:
                return _http_resp(404, {})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, [hist_trade])
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/holdings"):
                return _http_resp(200, [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 1, "dpQty": 1}])
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get), \
             patch.object(self.client.session, "post", side_effect=AssertionError("Zero POSTs permitted")):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertTrue(ok, msg)
            self.assertEqual(self.gate.state, ReconciliationState.HEALTHY)
            self.assertTrue(self.gate.is_accepting_entries)

        self.assertEqual(len(self.db.get_non_terminal_intents()), 0)
        acct = self.db.get_position_accounting("900001102657701")
        self.assertEqual(acct["buy_filled_qty"], 1)
        self.assertAlmostEqual(acct["buy_vwap"], 15.52, places=2)
        self.assertIn("900001102657701", self.watcher.terminal_orders)
        self.assertEqual(self.watcher.terminal_orders["900001102657701"]["state"], "TRADED")
        self.assertEqual(self.watcher.terminal_orders["900001102657701"]["filled_qty"], 1)
        self.assertEqual(self.watcher.terminal_orders["900001102657701"]["position_state"], "PROTECTED")

    # ------------------------------------------------------------------
    # H02: Two independent 1-share trades, same symbol, different order IDs and dates
    # ------------------------------------------------------------------
    def test_h02_two_independent_1_share_trades_same_symbol_different_order_ids_and_dates(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        nov4_ts = datetime.datetime(2026, 11, 4, 11, 25, 21, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-1721041-0028851f5",
            broker_order_id="900001104398802",
            price=15.59,
            quantity=1,
            created_ts=nov4_ts,
        )

        hist_trades = [
            {
                "dhanClientId": FAKE_ACCT,
                "orderId": "900001102657701",
                "exchangeTradeId": "ETID-NOV2-11915",
                "tradingSymbol": "YESBANK-EQ",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "tradedQuantity": 1,
                "tradedPrice": 15.52,
            },
            {
                "dhanClientId": FAKE_ACCT,
                "orderId": "900001104398802",
                "exchangeTradeId": "ETID-NOV4-11915",
                "tradingSymbol": "YESBANK-EQ",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "tradedQuantity": 1,
                "tradedPrice": 15.51,
            },
        ]

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if url.endswith("/forever/orders"):
                return _http_resp(200, _active_forever_oco_rows("FO-YES-2", "11915", 2))
            if url.endswith("/super/orders") or url.endswith("/trades") or url.endswith("/positions") or url.endswith("/orders"):
                return _http_resp(200, [])
            if "/orders/external/" in url or "/orders/900001102657701" in url or "/orders/900001104398802" in url:
                return _http_resp(404, {})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, hist_trades)
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/holdings"):
                return _http_resp(200, [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 2, "dpQty": 2, "avgCostPrice": 15.515}])
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertTrue(ok, msg)

        a1 = self.db.get_position_accounting("900001102657701")
        a2 = self.db.get_position_accounting("900001104398802")
        self.assertEqual(a1["buy_filled_qty"], 1)
        self.assertEqual(a2["buy_filled_qty"], 1)
        self.assertAlmostEqual(a1["buy_vwap"], 15.52, places=3)
        self.assertAlmostEqual(a2["buy_vwap"], 15.51, places=3)
        combined_qty = a1["buy_filled_qty"] + a2["buy_filled_qty"]
        combined_cost = a1["buy_vwap"] * a1["buy_filled_qty"] + a2["buy_vwap"] * a2["buy_filled_qty"]
        self.assertEqual(combined_qty, 2)
        self.assertAlmostEqual(combined_cost, 31.03, places=2)
        self.assertAlmostEqual(combined_cost / combined_qty, 15.515, places=3)

    # ------------------------------------------------------------------
    # H03: Holdings=2 but historical fills missing/mismatched -> remains UNVERIFIED
    # ------------------------------------------------------------------
    def test_h03_holdings_2_but_historical_fills_missing_or_mismatched_remains_unverified(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if url.endswith("/holdings"):
                return _http_resp(200, [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 2, "dpQty": 2}])
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertEqual(self.gate.state, ReconciliationState.BLOCKED)
            self.assertIn("absent from current order book and historical trade book", msg)
            self.assertEqual(len(self.db.get_non_terminal_intents()), 1)
            self.assertEqual(self.db.get_non_terminal_intents()[0]["state"], "ACKED")

    # ------------------------------------------------------------------
    # H04: Historical result missing account/security/orderId/product/side/exchangeTradeId -> rejected
    # ------------------------------------------------------------------
    def test_h04_historical_result_missing_identity_or_synthetic_trade_id_rejected(self):
        base_trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "900001102657701",
            "exchangeTradeId": "REAL-ETID-100",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }
        invalid_variants = [
            {**base_trade, "dhanClientId": ""},
            {**base_trade, "orderId": ""},
            {**base_trade, "securityId": ""},
            {**base_trade, "securityId": "99999"},
            {**base_trade, "exchangeSegment": "NSE_FNO"},
            {**base_trade, "productType": "INTRADAY"},
            {**base_trade, "transactionType": "SELL"},
            {**base_trade, "exchangeTradeId": ""},
            {**base_trade, "exchangeTradeId": "900001102657701-TRD-1"},  # synthetic rejected
            {**base_trade, "exchangeTradeId": "900001102657701-FILL-1"},
        ]
        for bad in invalid_variants:
            ok_v, norm, reason = DhanClient.validate_historical_trade_record(
                bad,
                expected_account_id=FAKE_ACCT,
                expected_order_id="900001102657701",
                expected_security_id="11915",
                expected_exchange_segment="NSE_EQ",
                expected_product_type="CNC",
                expected_transaction_type="BUY",
            )
            self.assertFalse(ok_v, f"Expected rejection for {bad}, got {norm} ({reason})")

    # ------------------------------------------------------------------
    # H05: Multi-page responses and duplicated trade IDs -> deduplicate; do not double count
    # ------------------------------------------------------------------
    def test_h05_multi_page_responses_and_duplicated_trade_ids_deduplicated_and_conflicts_rejected(self):
        t1 = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "ORD-P1",
            "exchangeTradeId": "ETID-DUP-1",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.50,
        }
        t2 = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "ORD-P1",
            "exchangeTradeId": "ETID-DUP-2",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 2,
            "tradedPrice": 15.60,
        }
        # Page 0 has t1; Page 1 repeats t1 and adds t2; Page 2 is empty []
        with patch.object(
            self.client.session,
            "get",
            side_effect=[
                _http_resp(200, [t1]),
                _http_resp(200, [t1, t2]),
                _http_resp(200, []),
            ],
        ):
            res = self.client.get_historical_trades("2026-11-02", "2026-11-05")
            self.assertIsInstance(res, BrokerReadList)
            self.assertEqual(len(res), 2)
            self.assertEqual(sum(r["tradedQuantity"] for r in res), 3)

        # Conflicting duplicate exchangeTradeId across pages fails closed
        t1_conflict = {**t1, "tradedQuantity": 5}
        with patch.object(
            self.client.session,
            "get",
            side_effect=[
                _http_resp(200, [t1]),
                _http_resp(200, [t1_conflict]),
                _http_resp(200, []),
            ],
        ):
            bad_res = self.client.get_historical_trades("2026-11-02", "2026-11-05")
            self.assertIsInstance(bad_res, BrokerReadFailure)
            self.assertIn("Conflicting duplicate exchangeTradeId", bad_res.error)

    # ------------------------------------------------------------------
    # H06: HTTP 401/403/429/500, timeout, corrupt JSON, page cap reached -> no false TRADED
    # ------------------------------------------------------------------
    def test_h06_http_401_403_429_500_timeout_corrupt_json_and_page_cap_fail_closed(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )

        for status_code, body in ((401, "DH-901"), (403, "Forbidden"), (429, "Rate limit"), (500, "Server error")):
            with patch.object(self.client, "_get_with_auth_recovery", return_value=_http_resp(status_code, text=body)):
                r = self.client.get_historical_trades("2026-11-02", "2026-11-05")
                self.assertIsInstance(r, BrokerReadFailure)

        # Timeout
        with patch.object(self.client, "_get_with_auth_recovery", side_effect=requests.exceptions.ReadTimeout("timeout")):
            r_to = self.client.get_historical_trades("2026-11-02", "2026-11-05")
            self.assertIsInstance(r_to, BrokerReadFailure)

        # Corrupt JSON
        with patch.object(self.client, "_get_with_auth_recovery", return_value=_http_resp(200, ValueError("Corrupt JSON"))):
            r_json = self.client.get_historical_trades("2026-11-02", "2026-11-05")
            self.assertIsInstance(r_json, BrokerReadFailure)

        # Page cap reached without empty terminal page
        valid_page = [{
            "dhanClientId": FAKE_ACCT,
            "orderId": "900001102657701",
            "exchangeTradeId": "ETID-CAP-1",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }]
        with patch.object(self.client, "_get_with_auth_recovery", return_value=_http_resp(200, valid_page)):
            r_cap = self.client.get_historical_trades("2026-11-02", "2026-11-05", max_pages=2)
            self.assertIsInstance(r_cap, BrokerReadFailure)
            self.assertIn("max_pages=2", r_cap.error)

    # ------------------------------------------------------------------
    # H07: ACKED 5-share order with only 2 verified fills -> preserve partial remainder
    # ------------------------------------------------------------------
    def test_h07_acked_5_share_order_with_only_2_verified_fills_preserves_partial_remainder(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-PARTIAL-5",
            broker_order_id="ORD-PARTIAL-5",
            price=15.60,
            quantity=5,
            created_ts=nov2_ts,
        )
        part_trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "ORD-PARTIAL-5",
            "exchangeTradeId": "ETID-PART-2OF5",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 2,
            "tradedPrice": 15.50,
        }

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, [part_trade])
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/forever/orders"):
                return _http_resp(200, _active_forever_oco_rows("FO-PART-2", "11915", 2))
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertIn("partially filled (2/5)", msg)

        intents = self.db.get_non_terminal_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0]["state"], "PART_FILLED")
        self.assertIn("ORD-PARTIAL-5", self.watcher.pending)
        self.assertNotIn("ORD-PARTIAL-5", self.watcher.terminal_orders)
        self.assertEqual(self.watcher.pending["ORD-PARTIAL-5"]["filled_qty"], 2)
        self.assertEqual(self.watcher.pending["ORD-PARTIAL-5"]["state"], "PART_FILLED")

    # ------------------------------------------------------------------
    # H08: Full verified fill but no OCO, settled DP holdings remain -> UNPROTECTED block
    # ------------------------------------------------------------------
    def test_h08_full_verified_fill_without_oco_and_settled_dp_holdings_remains_unprotected_blocked(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        hist_trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "900001102657701",
            "exchangeTradeId": "ETID-NOV2-NO-OCO",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, [hist_trade])
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/holdings"):
                return _http_resp(200, [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 1, "dpQty": 1}])
            if url.endswith("/forever/orders"):
                return _http_resp(200, [])  # No Forever OCO at broker!
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get), \
             patch.object(self.client.session, "post", side_effect=AssertionError("Must not auto-place OCO")):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertEqual(self.gate.state, ReconciliationState.BLOCKED)
            self.assertIn("Unprotected bot-owned settled holding detected", msg)

        # Entry intent is TRADED, but position is UNPROTECTED (never POSITION_CLOSED or PROTECTED)
        self.assertEqual(len(self.db.get_non_terminal_intents()), 0)
        w_rec = self.watcher.terminal_orders["900001102657701"]
        self.assertEqual(w_rec["state"], "TRADED")
        self.assertEqual(w_rec["filled_qty"], 1)
        self.assertEqual(w_rec["protected_qty"], 0)
        self.assertEqual(w_rec["position_state"], PositionLifecycleState.UNPROTECTED.value)
        pl = self.db.get_position_lifecycle("900001102657701")
        self.assertEqual(pl["position_state"], PositionLifecycleState.UNPROTECTED.value)

    # ------------------------------------------------------------------
    # H09: Simulated SQLite crash between fill and order status update -> rollback / repair
    # ------------------------------------------------------------------
    def test_h09_simulated_sqlite_crash_between_fill_and_order_update_rolls_back_and_repairs(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-CRASH-1",
            broker_order_id="ORD-CRASH-1",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        # Simulate an interrupted state where verified fill row was written before crash left intent ACKED
        self.db.record_fill(
            broker_order_id="ORD-CRASH-1",
            exchange_trade_id="ETID-CRASH-1",
            symbol="YESBANK-EQ",
            traded_price=15.52,
            traded_quantity=1,
            traded_at=nov2_ts + 5,
            transaction_type="BUY",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            parent_order_id="ORD-CRASH-1",
            is_verified_trade=True,
        )
        self.assertEqual(len(self.db.get_non_terminal_intents()), 1)

        repaired = self.db.repair_interrupted_fill_states()
        self.assertEqual(repaired, 1)
        self.assertEqual(len(self.db.get_non_terminal_intents()), 0)
        with self.db._get_conn() as conn:
            o_row = conn.execute("SELECT status, filled_quantity FROM orders WHERE correlation_id = 'EQ-YESBANK-CRASH-1'").fetchone()
            self.assertEqual(o_row["status"], "TRADED")
            self.assertEqual(o_row["filled_quantity"], 1)

    # ------------------------------------------------------------------
    # H10: Re-run recovery 5 times -> stable counts, no duplicate fills or notifications
    # ------------------------------------------------------------------
    def test_h10_rerun_historical_recovery_5_times_idempotent_stable_counts(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-IDEMP-1",
            broker_order_id="ORD-IDEMP-1",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "ORD-IDEMP-1",
            "exchangeTradeId": "ETID-IDEMP-1",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }
        for _ in range(5):
            res = self.db.apply_verified_historical_fills(
                correlation_id="EQ-YESBANK-IDEMP-1",
                broker_order_id="ORD-IDEMP-1",
                expected_account_id=FAKE_ACCT,
                symbol="YESBANK-EQ",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                requested_quantity=1,
                verified_trades=[trade],
            )
            self.assertEqual(res["filled_quantity"], 1)
            self.assertEqual(res["resolved_state"], "TRADED")

        with self.db._get_conn() as conn:
            fill_cnt = conn.execute("SELECT COUNT(*) FROM fills WHERE broker_order_id = 'ORD-IDEMP-1'").fetchone()[0]
            ev_cnt = conn.execute("SELECT COUNT(*) FROM system_events WHERE event_type = 'HISTORICAL_FILL_RECONCILED'").fetchone()[0]
        self.assertEqual(fill_cnt, 1)
        self.assertEqual(ev_cnt, 1)

    # ------------------------------------------------------------------
    # H11: Restart with watcher_orders.record_json stale PENDING/filled=0 -> migrated consistently
    # ------------------------------------------------------------------
    def test_h11_restart_with_watcher_orders_record_json_stale_pending_migrated_consistently(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-STALE-W",
            broker_order_id="ORD-STALE-W",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        # Simulate one-field SQL shortcut: is_terminal=1 while record_json still says PENDING, filled_qty=0,
        # AND verified fill exists in fills
        self.db.record_fill(
            broker_order_id="ORD-STALE-W",
            exchange_trade_id="ETID-STALE-W",
            symbol="YESBANK-EQ",
            traded_price=15.52,
            traded_quantity=1,
            traded_at=nov2_ts + 5,
            transaction_type="BUY",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            parent_order_id="ORD-STALE-W",
            is_verified_trade=True,
        )
        with self.db._get_conn() as conn:
            conn.execute("UPDATE watcher_orders SET is_terminal = 1 WHERE order_id = 'ORD-STALE-W'")

        restarted_watcher = ProtectionWatcher(
            self.client,
            path=self.watcher_path,
            dry_run=False,
            journal_db=self.db,
        )
        self.assertIn("ORD-STALE-W", restarted_watcher.terminal_orders)
        migrated_rec = restarted_watcher.terminal_orders["ORD-STALE-W"]
        self.assertEqual(migrated_rec["state"], OrderLifecycleState.TRADED.value)
        self.assertEqual(migrated_rec["filled_qty"], 1)
        self.assertEqual(migrated_rec["position_state"], PositionLifecycleState.UNPROTECTED.value)

    # ------------------------------------------------------------------
    # H12: Broker account A response while configured for account B -> rejected
    # ------------------------------------------------------------------
    def test_h12_broker_account_a_response_while_configured_for_account_b_rejected(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-ACCT-B",
            broker_order_id="ORD-ACCT-B",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        wrong_acct_trade = {
            "dhanClientId": OTHER_ACCT,
            "orderId": "ORD-ACCT-B",
            "exchangeTradeId": "ETID-WRONG-ACCT",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }
        with patch.object(
            self.client.session,
            "get",
            return_value=_http_resp(200, [wrong_acct_trade]),
        ):
            page_res = self.client.get_historical_trades_page("2026-11-02", "2026-11-05", 0)
            self.assertIsInstance(page_res, BrokerReadFailure)
            self.assertIn("dhanClientId mismatch", page_res.error)

        with self.assertRaises(ValueError):
            self.db.apply_verified_historical_fills(
                correlation_id="EQ-YESBANK-ACCT-B",
                broker_order_id="ORD-ACCT-B",
                expected_account_id=OTHER_ACCT,
                symbol="YESBANK-EQ",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                requested_quantity=1,
                verified_trades=[wrong_acct_trade],
            )
        self.assertEqual(self.db.get_non_terminal_intents()[0]["state"], "ACKED")

    # ------------------------------------------------------------------
    # H13: Emergency STOP active / protection unresolved -> read-only recovery allowed, gate stays closed
    # ------------------------------------------------------------------
    def test_h13_emergency_stop_active_or_unresolved_protection_never_opens_entry_gate(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-YESBANK-STOP-1",
            broker_order_id="ORD-STOP-1",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        self.stop_mgr.trigger_stop("OperatorTest", "Testing H13 STOP gate", account_id=FAKE_ACCT)

        hist_trade = {
            "dhanClientId": FAKE_ACCT,
            "orderId": "ORD-STOP-1",
            "exchangeTradeId": "ETID-STOP-1",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "transactionType": "BUY",
            "tradedQuantity": 1,
            "tradedPrice": 15.52,
        }

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, [hist_trade])
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/forever/orders"):
                return _http_resp(200, _active_forever_oco_rows("FO-STOP-1", "11915", 1))
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertIn("EMERGENCY STOP is active", msg)
            self.assertFalse(self.gate.is_accepting_entries)
            # Historical fill recovery still succeeded read-only
            self.assertEqual(len(self.db.get_non_terminal_intents()), 0)

    # ------------------------------------------------------------------
    # H14: Startup auth verified but reconciliation blocked -> Telegram says auth verified & entries BLOCKED
    # ------------------------------------------------------------------
    def test_h14_startup_auth_verified_but_reconciliation_blocked_reports_entries_blocked(self):
        startup_card = format_startup_online_card(
            self.cfg,
            allowed_chats_count=2,
            reconciliation_ok=False,
            reconciliation_msg="2 unverified order(s) require broker reconciliation",
            unresolved_intents_count=2,
            unprotected_positions_count=1,
            is_stopped=False,
        )
        self.assertIn("LISTENER RUNNING", startup_card)
        self.assertIn("RECONCILIATION PENDING (2)", startup_card)
        self.assertIn("PROTECTION UNVERIFIED", startup_card)
        self.assertIn("NEW ENTRIES BLOCKED", startup_card)
        self.assertNotIn("Actively listening for trade signals", startup_card)

        sun_1700 = datetime.datetime(2026, 11, 15, 17, 0, 5, tzinfo=IST)
        mon_1700 = datetime.datetime(2026, 11, 16, 17, 0, 5, tzinfo=IST)
        health_card = format_morning_health_report(
            self.cfg,
            now_ts=sun_1700.timestamp(),
            token_verified=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=10000.0,
            master_loaded=True,
            is_stopped=False,
            account_identity_status="VERIFIED",
            token_expiration_str=mon_1700.strftime("%Y-%m-%d %H:%M:%S IST"),
            token_expiration_ts=mon_1700.timestamp(),
            token_expired=False,
            holiday_calendar_verified=True,
            reconciliation_ok=False,
            unresolved_intents_count=2,
            unprotected_positions_count=0,
        )
        self.assertIn("Dhan authentication: ✅ VERIFIED", health_card)
        self.assertIn("NEW ENTRIES BLOCKED", health_card)
        self.assertIn("RECONCILIATION PENDING (2)", health_card)
        self.assertIn("Overall result: ❌ ACTION REQUIRED", health_card)

    # ------------------------------------------------------------------
    # H15: History outside API-supported retention or no broker order ID -> escalate for manual review
    # ------------------------------------------------------------------
    def test_h15_history_outside_retention_window_or_missing_broker_order_id_escalates_manual_review(self):
        old_ts = time.time() - (MAX_HISTORICAL_TRADE_WINDOW_DAYS + 5) * 86400.0
        self._seed_acked_intent(
            correlation_id="EQ-OLD-RETENTION",
            broker_order_id="ORD-TOO-OLD",
            price=15.60,
            quantity=1,
            created_ts=old_ts,
        )
        self.db.record_order_intent(
            correlation_id="EQ-NO-BROKER-ID",
            symbol="YESBANK-EQ",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
            account_id=FAKE_ACCT,
        )
        self.db.update_order_intent("EQ-NO-BROKER-ID", state="UNKNOWN", broker_order_id=None)

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            return _http_resp(200, [])

        with patch.object(self.client.session, "get", side_effect=fake_get):
            ok, msg = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertIn("exceeds 31-day historical trade retention window; manual review required", msg)
            self.assertIn("missing broker_order_id; cannot verify via historical trades (manual review required)", msg)
            # Never assumed CANCELLED or EXPIRED
            states = {r["correlation_id"]: r["state"] for r in self.db.get_non_terminal_intents()}
            self.assertEqual(states["EQ-OLD-RETENTION"], "ACKED")
            self.assertEqual(states["EQ-NO-BROKER-ID"], "UNKNOWN")

    # ------------------------------------------------------------------
    # H16: LIVE test order explicitly owner acknowledged -> narrow audited manual-risk state
    # ------------------------------------------------------------------
    def test_h16_live_test_order_explicitly_owner_acknowledged_manual_risk_narrow_and_audited(self):
        nov2_ts = datetime.datetime(2026, 11, 2, 14, 1, 18, tzinfo=IST).timestamp()
        nov4_ts = datetime.datetime(2026, 11, 4, 11, 25, 21, tzinfo=IST).timestamp()
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            broker_order_id="900001102657701",
            price=15.60,
            quantity=1,
            created_ts=nov2_ts,
        )
        self._seed_acked_intent(
            correlation_id="EQ-TESTBANK-1721041-0028851f5",
            broker_order_id="900001104398802",
            price=15.59,
            quantity=1,
            created_ts=nov4_ts,
        )
        hist_trades = [
            {
                "dhanClientId": FAKE_ACCT,
                "orderId": "900001102657701",
                "exchangeTradeId": "ETID-NOV2-1",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "tradedQuantity": 1,
                "tradedPrice": 15.52,
            },
            {
                "dhanClientId": FAKE_ACCT,
                "orderId": "900001104398802",
                "exchangeTradeId": "ETID-NOV4-1",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "tradedQuantity": 1,
                "tradedPrice": 15.51,
            },
        ]

        def fake_get(url, headers=None, timeout=5):
            if url.endswith("/fundlimit"):
                return _http_resp(200, {"availabelBalance": 5000.0})
            if "/trades/2026-11-02/" in url and url.endswith("/0"):
                return _http_resp(200, hist_trades)
            if "/trades/2026-11-02/" in url and url.endswith("/1"):
                return _http_resp(200, [])
            if url.endswith("/holdings"):
                return _http_resp(200, [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 2, "dpQty": 2}])
            if url.endswith("/forever/orders"):
                return _http_resp(200, [])
            return _http_resp(200, [])

        # Acknowledge ONLY the first order (900001102657701); the second order (900001104398802) is NOT acknowledged yet
        ack1 = self.db.acknowledge_manual_risk_holding(
            parent_order_id="900001102657701",
            correlation_id="EQ-TESTBANK-8278766-00017076f",
            account_id=FAKE_ACCT,
            symbol="YESBANK-EQ",
            security_id="11915",
            quantity=1,
            acknowledged_by="OwnerTest",
            reason="Controlled Nov 2 LIVE 1-share test order",
        )
        self.assertEqual(ack1["status"], "UNPROTECTED_MANUAL_RISK")

        with patch.object(self.client.session, "get", side_effect=fake_get):
            # Gate still blocks because the second order (900001104398802) is NOT acknowledged! (Proves no global bypass)
            ok1, msg1 = self.gate.run_startup_reconciliation()
            self.assertFalse(ok1)
            self.assertIn("900001104398802", msg1)

            # Now owner explicitly acknowledges the second test order (900001104398802)
            self.db.acknowledge_manual_risk_holding(
                parent_order_id="900001104398802",
                correlation_id="EQ-TESTBANK-1721041-0028851f5",
                account_id=FAKE_ACCT,
                symbol="YESBANK-EQ",
                security_id="11915",
                quantity=1,
                acknowledged_by="OwnerTest",
                reason="Controlled Nov 4 LIVE 1-share test order",
            )
            ok2, msg2 = self.gate.recheck_and_recover_if_verified()
            self.assertTrue(ok2, msg2)
            self.assertTrue(self.gate.is_accepting_entries)

        # Both orders retain UNPROTECTED_MANUAL_RISK (never PROTECTED or POSITION_CLOSED)
        for oid in ("900001102657701", "900001104398802"):
            pl = self.db.get_position_lifecycle(oid)
            self.assertEqual(pl["position_state"], "UNPROTECTED_MANUAL_RISK")
            self.assertEqual(pl["protected_qty"], 0)
            self.assertEqual(pl["confirmed_open_qty"], 1)

    # ------------------------------------------------------------------
    # H17: bypass_reconciliation.py disabled and no bulk-close SQL path exists
    # ------------------------------------------------------------------
    def test_h17_bypass_reconciliation_script_disabled_and_no_bulk_close_path_exists(self):
        repo_root = Path(__file__).resolve().parent.parent
        bypass_path = repo_root / "bypass_reconciliation.py"
        if bypass_path.exists():
            content = bypass_path.read_text(encoding="utf-8")
            self.assertNotIn("UPDATE ", content.upper())
            self.assertNotIn("SET STATE", content.upper())
            self.assertNotIn("SET STATUS", content.upper())
            with self.assertRaises(RuntimeError):
                exec(compile(content, str(bypass_path), "exec"), {})


    # Additional independent-audit regressions outside Gemini's H01–H17 set.
    def test_h18_lifecycle_database_read_exception_must_block(self):
        with patch.object(self.db, "get_open_positions_lifecycle", side_effect=sqlite3.OperationalError("database disk I/O error")):
            ok, reason = self.gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        self.assertIn("query failed", reason)

    def test_h19_two_bot_lots_cannot_be_verified_against_one_share_holding(self):
        for oid in ("SYNTH-BUY-1", "SYNTH-BUY-2"):
            self.db.upsert_position_lifecycle(
                parent_order_id=oid,
                symbol="YESBANK-EQ",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                protection_route="FOREVER_OCO",
                position_state="UNPROTECTED",
                entry_qty=1,
                buy_filled_qty=1,
                buy_vwap=15.5,
                confirmed_open_qty=1,
            )
        self.client.get_holdings = MagicMock(return_value=[{"securityId": "11915", "totalQty": 1}])
        self.client.get_forever_orders = MagicMock(return_value=[])
        ok, reason = self.gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        self.assertIn("sumOfBotOpenQty=2", reason)

    def test_h20_watcher_runtime_does_not_call_slow_historical_history_in_poll(self):
        self._seed_acked_intent(
            correlation_id="EQ-TEST-RUNTIME-1",
            broker_order_id="SYNTH-OLD-ORDER-1",
            created_ts=datetime.datetime(2026, 11, 2, 10, 0, tzinfo=IST).timestamp(),
        )
        self.client.get_order_book = MagicMock(return_value=[])
        self.client.get_order_by_correlation_id = MagicMock(return_value=None)
        self.client.order_status = MagicMock(return_value=None)
        with patch.object(self.gate, "_schedule_historical_background_recheck") as schedule, \
             patch.object(self.gate, "_reconcile_unresolved_intents_via_historical_trades", side_effect=AssertionError("slow Dhan history must not run on watcher thread")):
            result = self.gate.reconcile_runtime_unknown_intents()
        self.assertEqual(result, 0)
        schedule.assert_called_once()

    def test_h21_crash_after_sqlite_commit_before_watcher_save_recovers_fail_closed(self):
        oid = "SYNTH-COMMIT-ORDER-1"
        cid = "EQ-TEST-COMMIT-1"
        self._seed_acked_intent(
            correlation_id=cid,
            broker_order_id=oid,
            created_ts=datetime.datetime(2026, 11, 2, 10, 0, tzinfo=IST).timestamp(),
        )
        self.db.apply_verified_historical_fills(
            correlation_id=cid,
            broker_order_id=oid,
            expected_account_id=FAKE_ACCT,
            symbol="YESBANK-EQ",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            requested_quantity=1,
            verified_trades=[{
                "dhanClientId": FAKE_ACCT, "orderId": oid,
                "exchangeTradeId": "SYNTH-EXCHANGE-TRADE-1", "securityId": "11915",
                "exchangeSegment": "NSE_EQ", "productType": "CNC",
                "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.52,
            }],
        )
        # Simulate process dying before historical watcher migration was saved.
        self.assertEqual(self.watcher.pending[oid]["filled_qty"], 0)
        self.client.get_fund_limits = MagicMock(return_value=(True, 10000.0, "OK"))
        self.client.get_trade_book = MagicMock(return_value=[])
        self.client.get_positions = MagicMock(return_value=[])
        self.client.get_order_book = MagicMock(return_value=[])
        self.client.get_holdings = MagicMock(return_value=[{"securityId": "11915", "totalQty": 1}])
        self.client.get_forever_orders = MagicMock(return_value=[])
        self.client.get_super_orders = MagicMock(return_value=[])
        ok, reason = self.gate.run_startup_reconciliation()
        self.assertFalse(ok, "settled BUY with zero OCO must remain blocked")
        self.assertIn("Unprotected bot-owned settled holding", reason)
        self.assertEqual(self.watcher.terminal_orders[oid]["filled_qty"], 1)
        self.assertEqual(self.db.get_position_lifecycle(oid)["position_state"], "UNPROTECTED")

    def test_h22_existing_trade_id_price_disagreement_rejects_and_preserves_journal(self):
        oid = "SYNTH-DUP-ORDER-1"
        cid = "EQ-TEST-DUP-1"
        self._seed_acked_intent(
            correlation_id=cid,
            broker_order_id=oid,
            created_ts=datetime.datetime(2026, 11, 2, 10, 0, tzinfo=IST).timestamp(),
        )
        kwargs = dict(
            correlation_id=cid, broker_order_id=oid, expected_account_id=FAKE_ACCT,
            symbol="YESBANK-EQ", security_id="11915", exchange_segment="NSE_EQ",
            product_type="CNC", requested_quantity=1,
        )
        row = {
            "dhanClientId": FAKE_ACCT, "orderId": oid, "exchangeTradeId": "SYNTH-ETID-DUP-1",
            "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.52,
        }
        self.db.apply_verified_historical_fills(**kwargs, verified_trades=[row])
        with self.assertRaisesRegex(ValueError, "Conflicting stored execution"):
            self.db.apply_verified_historical_fills(**kwargs, verified_trades=[{**row, "tradedPrice": 17.00}])
        self.assertAlmostEqual(self.db.get_position_accounting(oid)["buy_vwap"], 15.52, places=3)

if __name__ == "__main__":
    unittest.main()
