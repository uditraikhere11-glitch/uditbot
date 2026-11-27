"""Regression tests: a genuinely closed equity fill must not strand WebUI at 75%."""
import json
import os
import tempfile
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from src.engine.broker_session import IST
from src.engine.config_manager import ConfigChangeTracker
from src.journal.db import JournalDB
from src.models import BrokerReadFailure
from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker


class ClosedFillConfigGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="auditbot_closed_fill_")
        self.data_dir = self.tmp.name
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.env_path = os.path.join(self.data_dir, ".env")
        with open(self.env_path, "w", encoding="utf-8") as fh:
            fh.write("TRADING_MODE=LIVE\nDHAN_CLIENT_ID=1113731536\n")
        with open(self.stop_path, "w", encoding="utf-8") as fh:
            json.dump({"is_stopped": False, "in_flight_orders": []}, fh)
        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self._now = float(int(time.time()) - 30)
        self._ex_time = datetime.fromtimestamp(self._now, IST).strftime("%Y-%m-%d %H:%M:%S")
        self.db.record_order_intent("CLOSED-CORR", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 25.0, 1)
        self.db.update_order_intent("CLOSED-CORR", state="TRADED", broker_order_id="ORD-BUY")
        self.db.record_order(
            correlation_id="CLOSED-CORR", broker_order_id="ORD-BUY",
            trading_mode="LIVE", product_type="CNC", exchange_segment="NSE_EQ",
            symbol="YESBANK", security_id="11915", order_type="LIMIT",
            price=25.0, quantity=1, status="TRADED",
        )
        watcher = {
            "order_id": "ORD-BUY", "correlation_id": "CLOSED-CORR",
            "symbol": "YESBANK", "security_id": "11915",
            "segment": "NSE_EQ", "product_type": "CNC",
            "state": "TRADED", "qty": 1, "filled_qty": 1,
            "exit_filled_qty": 1, "position_state": "POSITION_CLOSED",
            "protected_qty": 0, "confirmed_protection_ids": [],
        }
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE orders SET filled_quantity=1 WHERE broker_order_id='ORD-BUY'"
            )
            conn.execute(
                "INSERT INTO positions_lifecycle ("
                "parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, "
                "protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, "
                "confirmed_open_qty, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("ORD-BUY", "1113731536", "YESBANK", "11915", "NSE_EQ", "CNC",
                 "MANUAL", "POSITION_CLOSED", 1, 1, 1, 0, self._now),
            )
            conn.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, "
                "traded_quantity, traded_at, transaction_type, security_id, exchange_segment, "
                "product_type, parent_order_id, is_verified_trade) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                ("ORD-BUY", "ET11915B", "YESBANK", 25.0, 1, self._now, "BUY", "11915", "NSE_EQ", "CNC", "ORD-BUY"),
            )
            conn.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, "
                "traded_quantity, traded_at, transaction_type, security_id, exchange_segment, "
                "product_type, parent_order_id, is_verified_trade) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                ("ORD-SELL", "ET11915S", "YESBANK", 26.0, 1, self._now, "SELL", "11915", "NSE_EQ", "CNC", "ORD-BUY"),
            )
            conn.execute(
                "INSERT INTO watcher_orders(order_id, is_terminal, record_json, updated_at) VALUES (?, ?, ?, ?)",
                ("ORD-BUY", 1, json.dumps(watcher), self._now),
            )
            conn.execute(
                "INSERT INTO protection_orders(parent_order_id, forever_order_id, symbol, quantity, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("ORD-BUY", "FAILED-ORD-BUY", "YESBANK", 1, "CLOSED_NO_EXPOSURE_VERIFIED", self._now),
            )

    def tearDown(self):
        self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        self.tmp.cleanup()

    def broker(self):
        broker = MagicMock()
        broker.get_order_book.return_value = [
            {
                "dhanClientId": "1113731536", "orderId": "ORD-BUY", "correlationId": "CLOSED-CORR",
                "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "BUY",
                "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0,
            },
            {
                "dhanClientId": "1113731536", "orderId": "ORD-SELL", "securityId": "11915",
                "exchangeSegment": "NSE_EQ", "productType": "CNC",
                "transactionType": "SELL", "orderStatus": "TRADED",
                "quantity": 1, "filledQty": 1, "remainingQuantity": 0,
            },
        ]
        broker.get_trade_book.return_value = [
            {
                "dhanClientId": "1113731536", "orderId": "ORD-BUY", "exchangeTradeId": "ET11915B",
                "exchangeOrderId": "EXO-BUY-1", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 25.0,
                "exchangeTime": self._ex_time,
            },
            {
                "dhanClientId": "1113731536", "orderId": "ORD-SELL", "exchangeTradeId": "ET11915S",
                "exchangeOrderId": "EXO-SELL-1", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 1, "tradedPrice": 26.0,
                "exchangeTime": self._ex_time,
            },
        ]
        broker.get_positions.return_value = [
            {
                "dhanClientId": "1113731536", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "netQty": 0, "dayBuyQty": 1, "daySellQty": 1,
                "carryForwardBuyQty": 0, "carryForwardSellQty": 0,
            }
        ]
        broker.get_holdings.return_value = [{"dhanClientId": "1113731536", "securityId": "11915", "isin": "INE528G01035", "totalQty": 0, "dpQty": 0, "t1Qty": 0, "availableQty": 0}]
        broker.get_forever_orders.return_value = []
        broker.get_super_orders.return_value = []
        attach_unified_live_evidence_to_stub_broker(broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")
        return broker

    def check(self, broker):
        return self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path, self.stop_path, is_service_running=True,
            is_live_mode=True, target_is_live=True,
            broker=broker, env_path=self.env_path,
        )

    def test_closed_buy_and_confirmed_exit_unblocks_without_fake_protection(self):
        broker = self.broker()
        safe, reason, code, count = self.check(broker)
        self.assertTrue(safe, reason)
        self.assertEqual(code, "")
        self.assertEqual(count, 0)
        broker.get_positions.assert_called()
        broker.get_holdings.assert_called()
        broker.get_trade_book.assert_called()
        with self.db._get_conn() as conn:
            watcher = conn.execute(
                "SELECT is_terminal, record_json FROM watcher_orders WHERE order_id='ORD-BUY'"
            ).fetchone()
            protection = conn.execute(
                "SELECT status FROM protection_orders WHERE parent_order_id='ORD-BUY'"
            ).fetchone()
        self.assertEqual(watcher["is_terminal"], 1)
        self.assertEqual(json.loads(watcher["record_json"])["protected_qty"], 0)
        self.assertEqual(protection["status"], "CLOSED_NO_EXPOSURE_VERIFIED")

        # Reconciliation must be idempotent, not dependent on the original UI tick.
        self.assertTrue(self.check(broker)[0])

    def test_next_day_old_order_remains_closed_only_with_fresh_broker_checks(self):
        # Prior-session closed buy/sell backed by historical archive
        prev_ts = self._now - 86400
        prev_ex = datetime.fromtimestamp(prev_ts, IST).strftime("%Y-%m-%d %H:%M:%S")
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id='ORD-BUY'", (prev_ts,))
            conn.execute("UPDATE orders SET created_at=? WHERE broker_order_id='ORD-BUY'", (prev_ts,))
            conn.execute("UPDATE fills SET traded_at=? WHERE broker_order_id IN ('ORD-BUY','ORD-SELL')", (prev_ts,))
        broker = self.broker()
        broker.get_order_book.return_value = []
        broker.get_trade_book.return_value = []
        broker.get_positions.return_value = []
        broker.get_historical_trades.return_value = [
            {
                "dhanClientId": "1113731536", "orderId": "ORD-BUY", "exchangeTradeId": "ET11915B",
                "exchangeOrderId": "EXO-BUY-1", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 25.0,
                "exchangeTime": prev_ex,
            },
            {
                "dhanClientId": "1113731536", "orderId": "ORD-SELL", "exchangeTradeId": "ET11915S",
                "exchangeOrderId": "EXO-SELL-1", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 1, "tradedPrice": 26.0,
                "exchangeTime": prev_ex,
            },
        ]
        broker.get_order_status.side_effect = lambda oid: {
            "dhanClientId": "1113731536", "orderId": oid,
            "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "transactionType": "BUY" if oid == "ORD-BUY" else "SELL",
            "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0,
        }
        safe, reason, _, _ = self.check(broker)
        self.assertTrue(safe, reason)

        # A new holding next session invalidates the flat proof (fresh broker read after invalidation).
        broker.get_holdings.return_value = [
            {"dhanClientId": "1113731536", "securityId": "11915", "isin": "INE528G01035",
             "totalQty": 1, "dpQty": 1, "t1Qty": 0, "availableQty": 1}
        ]
        # Force fresh broker acquisition by clearing cached snapshot
        self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        if os.path.exists(os.path.join(self.data_dir, "snapshots.sqlite3")):
            os.remove(os.path.join(self.data_dir, "snapshots.sqlite3"))
        safe2, _, code2, _ = self.check(broker)
        self.assertFalse(safe2)
        self.assertIn(code2, ("UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_next_day_pending_order_invalidates_historical_proof(self):
        broker = self.broker()
        self.assertTrue(self.check(broker)[0])
        broker.get_order_book.return_value = [{
            "dhanClientId": "1113731536", "orderId": "NEXT-DAY-PENDING", "securityId": "11915",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY",
            "orderStatus": "PENDING", "quantity": 1, "filledQty": 0, "remainingQuantity": 1,
        }]
        broker.get_trade_book.return_value = []
        self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        if os.path.exists(os.path.join(self.data_dir, "snapshots.sqlite3")):
            os.remove(os.path.join(self.data_dir, "snapshots.sqlite3"))
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertIn(code, ("LIVE_ORDER_IN_PROGRESS", "UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_delivery_or_t1_holding_still_blocks(self):
        broker = self.broker()
        broker.get_holdings.return_value = [{
            "dhanClientId": "1113731536", "securityId": "11915", "isin": "INE528G01035",
            "totalQty": 1, "dpQty": 0, "t1Qty": 1, "availableQty": 0,
        }]
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertIn(code, ("UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_unclosed_position_still_blocks(self):
        broker = self.broker()
        broker.get_positions.return_value = [{
            "dhanClientId": "1113731536", "securityId": "11915",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 1,
            "dayBuyQty": 1, "daySellQty": 0, "carryForwardBuyQty": 0, "carryForwardSellQty": 0,
        }]
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertIn(code, ("UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_no_exit_execution_does_not_use_lagging_zero_positions(self):
        broker = self.broker()
        broker.get_trade_book.return_value = broker.get_trade_book.return_value[:1]
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertIn(code, ("UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_active_forever_order_keeps_gate_closed(self):
        broker = self.broker()
        broker.get_forever_orders.return_value = [
            {
                "dhanClientId": "1113731536",
                "orderId": "ACTIVE-OCO", "parentOrderId": "ORD-BUY",
                "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                "transactionType": "SELL", "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG", "quantity": 1,
            }
        ]
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertIn(code, ("UNPROTECTED_POSITION", "BROKER_RECONCILIATION_FAILED"))

    def test_holdings_api_failure_never_implies_flat(self):
        broker = self.broker()
        broker.get_holdings.return_value = BrokerReadFailure(
            status="UNAVAILABLE", error="holdings 503", status_code=503
        )
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertEqual(code, "BROKER_RECONCILIATION_FAILED")

    def test_pending_exit_order_keeps_gate_closed(self):
        broker = self.broker()
        broker.get_order_book.return_value[1]["orderStatus"] = "PENDING"
        broker.get_order_book.return_value[1]["filledQty"] = 0
        broker.get_order_book.return_value[1]["remainingQuantity"] = 1
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertEqual(code, "LIVE_ORDER_IN_PROGRESS")

    def test_missing_holdings_capability_never_implies_flat(self):
        broker = self.broker()
        broker.get_holdings.return_value = None
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertEqual(code, "BROKER_RECONCILIATION_FAILED")


if __name__ == "__main__":
    unittest.main()
