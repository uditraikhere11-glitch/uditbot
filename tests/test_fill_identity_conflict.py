import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from src.journal.db import JournalDB


class FillIdentityConflictTests(unittest.TestCase):
    def test_conflicting_orders_preserve_losing_placeholder_across_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "journal.db")
            db = JournalDB(path)
            def record(order, trade, verified):
                return db.record_fill(broker_order_id=order, exchange_trade_id=trade,
                                      symbol="SYM", traded_price=100, traded_quantity=1,
                                      is_verified_trade=verified)

            for order in ("A", "B"):
                self.assertTrue(record(order, order + "-FILL-1", False))

            def claim(order):
                try:
                    return record(order, "E", True)
                except ValueError as exc:
                    self.assertIn("conflicts", str(exc))
                    return False

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(claim, ("A", "B")))
            self.assertCountEqual(results, [True, False])
            winner, loser = ("A", "B") if results[0] else ("B", "A")
            self.assertFalse(record(winner, "E", True))
            with JournalDB(path)._get_conn() as conn:
                rows = conn.execute("SELECT broker_order_id, exchange_trade_id FROM fills").fetchall()
            self.assertCountEqual([tuple(row) for row in rows], [(winner, "E"), (loser, loser + "-FILL-1")])

    def test_watcher_keeps_conflict_pending_across_empty_poll(self):
        from tests.test_step14_step15 import TestStep14And15

        fixture = TestStep14And15()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.journal.record_fill(broker_order_id="A", exchange_trade_id="E", symbol="SYM",
                                    traded_price=100, traded_quantity=1, is_verified_trade=True)
        fixture.watcher.track(order_id="B", symbol="SYM", security_id="99",
                              segment="NSE_EQ", qty=1, product_type="CNC")
        broker = fixture.mock_broker
        broker.reconciliation_gate = fixture.gate
        broker.order_status.return_value = dict(orderStatus="TRADED", filledQty=1, averageTradedPrice=100)
        broker.get_trades_for_order.return_value = [
            dict(orderId="B", transactionType="BUY", securityId="99",
                 tradedQuantity=1, tradedPrice=100, exchangeTradeId="E")]
        fixture.watcher.check_once()
        self.assertTrue(fixture.watcher.pending["B"]["persistence_uncertain"])
        self.assertEqual(fixture.gate.state.value, "BLOCKED")
        broker.get_trades_for_order.return_value = []
        fixture.watcher.check_once()
        self.assertTrue(fixture.watcher.pending["B"]["entry_fill_evidence_required"])
        self.assertEqual(fixture.watcher.pending["B"]["filled_qty"], 0)
        broker.place_forever_oco.assert_not_called()
