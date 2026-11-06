"""Offline tests: isolated orphan OCO cleanup never cancels unproven exposure."""
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from src.engine.orphan_cleanup import clean_orphaned_forever_orders
from src.models import BrokerReadFailure, BrokerReadList, TradingMode


ACCOUNT = "12345"
SEC_ID = "3813"
OCO_ID = "F-42"
PARENT = "ENTRY-1"


def oco_rows():
    common = {
        "orderId": OCO_ID,
        "dhanClientId": ACCOUNT,
        "securityId": SEC_ID,
        "exchangeSegment": "NSE_EQ",
        "productType": "CNC",
        "transactionType": "SELL",
        "tradingSymbol": "ARIHANTCAP",
        "orderStatus": "CONFIRM",
        "quantity": 1,
    }
    return [
        dict(common, legName="STOP_LOSS_LEG", triggerPrice=95.0, price=94.0),
        dict(common, legName="TARGET_LEG", triggerPrice=105.0, price=105.0),
    ]


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "journal.db")
        with sqlite3.connect(self.db_path) as c:
            c.executescript("""
                CREATE TABLE protection_attempts (
                    parent_order_id TEXT, broker_order_id TEXT,
                    security_id TEXT, exchange_segment TEXT, product_type TEXT,
                    symbol TEXT, trading_symbol TEXT, account_id TEXT,
                    quantity INTEGER, state TEXT, route TEXT
                );
                CREATE TABLE protection_orders (
                    parent_order_id TEXT, forever_order_id TEXT, status TEXT
                );
                CREATE TABLE order_intents (
                    security_id TEXT, exchange_segment TEXT, product_type TEXT,
                    broker_order_id TEXT, state TEXT
                );
            """)
            c.execute(
                "INSERT INTO protection_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (PARENT, OCO_ID, SEC_ID, "NSE_EQ", "CNC", "ARIHANTCAP",
                 "ARIHANTCAP", ACCOUNT, 1, "CONFIRMED", "FOREVER_OCO"),
            )
            c.execute("INSERT INTO protection_orders VALUES (?, ?, ?)",
                      (PARENT, OCO_ID, "ACTIVE_CONFIRMED"))
            c.execute("INSERT INTO order_intents VALUES (?, ?, ?, ?, ?)",
                      (SEC_ID, "NSE_EQ", "CNC", PARENT, "TRADED"))

        self.broker = SimpleNamespace(
            cfg=SimpleNamespace(trading_mode=TradingMode.LIVE, dhan_client_id=ACCOUNT),
            get_positions=Mock(return_value=BrokerReadList([])),
            get_forever_orders=Mock(return_value=BrokerReadList(oco_rows())),
            get_order_book=Mock(return_value=BrokerReadList([])),
            get_holdings=Mock(return_value=BrokerReadList([])),
            session=SimpleNamespace(delete=Mock(return_value=SimpleNamespace(status_code=202))),
            _headers=Mock(return_value={"access-token": "FAKE"}),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def run_scan(self, execute=False, sleep=lambda _: None):
        return clean_orphaned_forever_orders(
            self.broker, self.db_path, execute=execute, sleep=sleep
        )

    def test_preview_is_readonly(self):
        out = self.run_scan()
        self.assertEqual(out["eligible"], [OCO_ID])
        self.assertEqual(out["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()

    def test_execute_cancels_only_using_forever_endpoint_after_second_snapshot(self):
        out = self.run_scan(execute=True)
        self.assertEqual(out["cancel_requested"], [OCO_ID], out)
        self.assertEqual(self.broker.get_positions.call_count, 2)
        self.assertEqual(self.broker.get_holdings.call_count, 2)
        self.broker.session.delete.assert_called_once()
        self.assertEqual(
            self.broker.session.delete.call_args.args[0],
            "https://api.dhan.co/v2/forever/orders/F-42",
        )

    def test_positive_position_protects_active_oco(self):
        self.broker.get_positions.return_value = BrokerReadList([{
            "securityId": SEC_ID, "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 1,
        }])
        out = self.run_scan(execute=True)
        self.assertEqual(out["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()

    def test_settled_holdings_protect_oco_even_when_today_positions_zero(self):
        self.broker.get_holdings.return_value = BrokerReadList([{
            "securityId": SEC_ID, "tradingSymbol": "ARIHANTCAP",
            "totalQty": 1, "dpQty": 1, "t1Qty": 0,
            "availableQty": 1, "collateralQty": 0,
        }])
        out = self.run_scan(execute=True)
        self.assertEqual(out["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()

    def test_t1_holdings_protect_oco(self):
        self.broker.get_holdings.return_value = BrokerReadList([{
            "securityId": SEC_ID, "tradingSymbol": "ARIHANTCAP",
            "totalQty": 0, "dpQty": 0, "t1Qty": 1,
            "availableQty": 0, "collateralQty": 0,
        }])
        self.assertEqual(self.run_scan(execute=True)["cancel_requested"], [])

    def test_pending_buy_blocks_cleanup(self):
        self.broker.get_order_book.return_value = BrokerReadList([{
            "securityId": SEC_ID, "tradingSymbol": "ARIHANTCAP",
            "transactionType": "BUY", "orderStatus": "PENDING",
        }])
        out = self.run_scan(execute=True)
        self.assertEqual(out["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()

    def test_unresolved_local_intent_blocks_cleanup(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("INSERT INTO order_intents VALUES (?, ?, ?, ?, ?)",
                      (SEC_ID, "NSE_EQ", "CNC", "ENTRY-NEW", "UNKNOWN"))
        self.assertEqual(self.run_scan(execute=True)["cancel_requested"], [])

    def test_failed_positions_read_is_not_treated_as_empty(self):
        self.broker.get_positions.return_value = BrokerReadFailure(
            "FAILED", "HTTP 503"
        )
        out = self.run_scan(execute=True)
        self.assertTrue(out["error"])
        self.broker.session.delete.assert_not_called()

    def test_failed_holdings_read_is_not_treated_as_empty(self):
        self.broker.get_holdings.return_value = BrokerReadFailure(
            "FAILED", "HTTP 503"
        )
        out = self.run_scan(execute=True)
        self.assertTrue(out["error"])
        self.broker.session.delete.assert_not_called()

    def test_malformed_holdings_are_not_zero(self):
        self.broker.get_holdings.return_value = BrokerReadList([{
            "securityId": SEC_ID, "tradingSymbol": "ARIHANTCAP",
            "totalQty": None, "dpQty": 0, "t1Qty": 0,
            "availableQty": 0, "collateralQty": 0,
        }])
        out = self.run_scan(execute=True)
        self.assertTrue(out["error"])
        self.broker.session.delete.assert_not_called()

    def test_wrong_account_oco_is_not_cancelled(self):
        orders = oco_rows()
        for row in orders:
            row["dhanClientId"] = "FOREIGN"
        self.broker.get_forever_orders.return_value = BrokerReadList(orders)
        self.assertEqual(self.run_scan(execute=True)["cancel_requested"], [])

    def test_non_bot_oco_is_not_cancelled(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("DELETE FROM protection_attempts")
        self.assertEqual(self.run_scan(execute=True)["eligible"], [])
        self.broker.session.delete.assert_not_called()

    def test_super_order_record_is_not_cancelled(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("UPDATE protection_attempts SET route='SUPER_ORDER'")
        self.assertEqual(self.run_scan(execute=True)["eligible"], [])

    def test_second_snapshot_reveals_position_and_blocks(self):
        calls = [BrokerReadList([]), BrokerReadList([{
            "securityId": SEC_ID, "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 1,
        }])]
        self.broker.get_positions.side_effect = calls
        self.assertEqual(self.run_scan(execute=True)["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()

    def test_cancel_http_error_is_not_reported_as_success(self):
        self.broker.session.delete.return_value = SimpleNamespace(status_code=429)
        out = self.run_scan(execute=True)
        self.assertEqual(out["cancel_requested"], [])
        self.assertEqual(len(out["failed"]), 1)

    def test_test_mode_never_sends_delete(self):
        self.broker.cfg.trading_mode = TradingMode.TEST
        out = self.run_scan(execute=True)
        self.assertTrue(out["error"])
        self.broker.session.delete.assert_not_called()

    def test_broker_terminal_oco_is_not_deleted(self):
        rows = oco_rows()
        for row in rows:
            row["orderStatus"] = "CANCELLED"
        self.broker.get_forever_orders.return_value = BrokerReadList(rows)
        self.assertEqual(self.run_scan(execute=True)["cancel_requested"], [])
        self.broker.session.delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
