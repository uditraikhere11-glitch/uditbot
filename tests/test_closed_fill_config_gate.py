"""Regression tests: a genuinely closed equity fill must not strand WebUI at 75%."""
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

from src.engine.config_manager import ConfigChangeTracker
from src.journal.db import JournalDB
from src.models import BrokerReadFailure


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
        self.db.record_order_intent("CLOSED-CORR", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 25.0, 1)
        self.db.update_order_intent("CLOSED-CORR", state="TRADED", broker_order_id="ORD-BUY")
        self.db.record_order(
            "CLOSED-CORR", "ORD-BUY", "LIVE", "CNC", "NSE_EQ",
            "YESBANK", "11915", "LIMIT", 25.0, 1, "TRADED",
        )
        watcher = {
            "order_id": "ORD-BUY", "correlation_id": "CLOSED-CORR",
            "symbol": "YESBANK", "security_id": "11915",
            "segment": "NSE_EQ", "product_type": "CNC",
            "state": "TRADED", "qty": 1, "filled_qty": 1,
            "protected_qty": 0, "confirmed_protection_ids": [],
        }
        with self.db._get_conn() as conn:
            conn.execute(
                "UPDATE orders SET filled_quantity=1 WHERE broker_order_id='ORD-BUY'"
            )
            conn.execute(
                "INSERT INTO watcher_orders(order_id, is_terminal, record_json, updated_at) VALUES (?, ?, ?, ?)",
                ("ORD-BUY", 0, json.dumps(watcher), 1.0),
            )
            conn.execute(
                "INSERT INTO protection_orders(parent_order_id, forever_order_id, symbol, quantity, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("ORD-BUY", "FAILED-ORD-BUY", "YESBANK", 1, "UNPROTECTED", 1.0),
            )

    def tearDown(self):
        self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        self.tmp.cleanup()

    def broker(self):
        broker = MagicMock()
        broker.get_order_book.return_value = [
            {
                "orderId": "ORD-BUY", "correlationId": "CLOSED-CORR",
                "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "BUY",
                "orderStatus": "TRADED", "filledQty": 1,
            },
            {
                "orderId": "ORD-SELL", "securityId": "11915",
                "exchangeSegment": "NSE_EQ", "productType": "CNC",
                "transactionType": "SELL", "orderStatus": "TRADED", "filledQty": 1,
            },
        ]
        broker.get_trade_book.return_value = [
            {
                "orderId": "ORD-BUY", "securityId": "11915",
                "exchangeSegment": "NSE_EQ", "transactionType": "BUY", "tradedQuantity": 1,
            },
            {
                "orderId": "ORD-SELL", "securityId": "11915",
                "exchangeSegment": "NSE_EQ", "transactionType": "SELL", "tradedQuantity": 1,
            },
        ]
        broker.get_positions.return_value = []
        broker.get_holdings.return_value = []
        broker.get_forever_orders.return_value = []
        broker.get_super_orders.return_value = []
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
        self.assertIn("broker_flat_verified_at", json.loads(watcher["record_json"]))
        self.assertEqual(protection["status"], "CLOSED_NO_EXPOSURE_VERIFIED")

        # Reconciliation must be idempotent, not dependent on the original UI tick.
        self.assertTrue(self.check(broker)[0])

    def test_next_day_old_order_remains_closed_only_with_fresh_broker_checks(self):
        broker = self.broker()
        self.assertTrue(self.check(broker)[0])

        # Next session Dhan does not return yesterday's order/trade book.
        broker.get_order_book.return_value = []
        broker.get_trade_book.return_value = []
        self.assertTrue(self.check(broker)[0])

        # A new holding next session invalidates the historical proof.
        broker.get_holdings.return_value = [
            {"securityId": "11915", "totalQty": 1, "dpQty": 1}
        ]
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_next_day_pending_order_invalidates_historical_proof(self):
        broker = self.broker()
        self.assertTrue(self.check(broker)[0])
        broker.get_order_book.return_value = [{
            "orderId": "NEXT-DAY-PENDING", "securityId": "11915",
            "exchangeSegment": "NSE_EQ", "transactionType": "BUY",
            "orderStatus": "PENDING", "filledQty": 0,
        }]
        broker.get_trade_book.return_value = []
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_delivery_or_t1_holding_still_blocks(self):
        broker = self.broker()
        broker.get_holdings.return_value = [{
            "securityId": "11915", "totalQty": 1, "dpQty": 0, "t1Qty": 1,
            "availableQty": 0,
        }]
        safe, _, code, _ = self.check(broker)
        self.assertFalse(safe)
        self.assertEqual(code, "UNPROTECTED_POSITION")

    def test_unclosed_position_still_blocks(self):
        broker = self.broker()
        broker.get_positions.return_value = [{"securityId": "11915", "netQty": 1}]
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_no_exit_execution_does_not_use_lagging_zero_positions(self):
        broker = self.broker()
        broker.get_trade_book.return_value = broker.get_trade_book.return_value[:1]
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_active_forever_order_keeps_gate_closed(self):
        broker = self.broker()
        broker.get_forever_orders.return_value = [
            {
                "orderId": "ACTIVE-OCO", "parentOrderId": "ORD-BUY",
                "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "orderStatus": "CONFIRM", "legName": "STOP_LOSS_LEG",
                "quantity": 1,
            }
        ]
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_holdings_api_failure_never_implies_flat(self):
        broker = self.broker()
        broker.get_holdings.return_value = BrokerReadFailure(
            status="UNAVAILABLE", error="holdings 503", status_code=503
        )
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_pending_exit_order_keeps_gate_closed(self):
        broker = self.broker()
        broker.get_order_book.return_value[1]["orderStatus"] = "PENDING"
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")

    def test_missing_holdings_capability_never_implies_flat(self):
        broker = self.broker()
        broker.get_holdings.return_value = None
        self.assertEqual(self.check(broker)[2], "UNPROTECTED_POSITION")


if __name__ == "__main__":
    unittest.main()
