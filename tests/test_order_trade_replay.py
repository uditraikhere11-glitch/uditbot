import unittest

from src.broker.dhan_client import DhanClient


class OrderTradeReplayTests(unittest.TestCase):
    def test_identical_fill_is_applied_once(self):
        first = dict(orderId="parent", tradedQuantity=2, tradedPrice=100, exchangeTradeId="A")
        second = dict(first, tradedPrice=130, exchangeTradeId="B")
        for rows in ([first, second, first], [second, first, second]):
            ok, fills, reason = DhanClient.normalize_order_trades_payload(rows, "parent")
            self.assertTrue(ok, reason)
            self.assertEqual(sum(row["tradedQuantity"] for row in fills), 4)
            self.assertEqual(len(fills), 2)

    def test_conflicting_replay_rejects_whole_response(self):
        first = dict(orderId="parent", tradedQuantity=2, tradedPrice=100, exchangeTradeId="A")
        for field in ("tradedQuantity", "tradedPrice"):
            ok, fills, _ = DhanClient.normalize_order_trades_payload([first, dict(first, **{field: 3})], "parent")
            self.assertFalse(ok)
            self.assertEqual(fills, [])

    def test_entry_journal_requires_explicit_matching_execution(self):
        from unittest.mock import patch
        from tests.test_step14_step15 import TestStep14And15

        for change, verified in [
            ({}, True), ({"exchangeTradeId": None}, False),
            ({"transactionType": None}, False), ({"transactionType": "SELL"}, False),
            ({"securityId": "wrong"}, False),
        ]:
            with self.subTest(change=change):
                fixture = TestStep14And15()
                fixture.setUp()
                try:
                    broker = fixture.mock_broker
                    broker.place_forever_oco.return_value = True, "group", "accepted"
                    broker.order_status.return_value = dict(orderStatus="PART_TRADED", filledQty=1, averageTradedPrice=100)
                    row = dict(orderId="parent", transactionType="BUY", securityId="99",
                               tradedQuantity=1, tradedPrice=100, exchangeTradeId="fill")
                    row.update(change)
                    ok, fills, reason = DhanClient.normalize_order_trades_payload(row, "parent")
                    self.assertTrue(ok, reason)
                    broker.get_trades_for_order.return_value = fills
                    fixture.watcher.track(order_id="parent", symbol="SYM", security_id="99",
                                          segment="NSE_EQ", qty=2, product_type="CNC")
                    with patch.object(fixture.journal, "record_fill", wraps=fixture.journal.record_fill) as record:
                        fixture.watcher.check_once()
                    self.assertEqual([call.kwargs["is_verified_trade"] for call in record.call_args_list], [verified])
                finally:
                    fixture.tearDown()
