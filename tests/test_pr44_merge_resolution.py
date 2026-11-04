"""PR44 merge-resolution regressions: read-only broker cancellation and watcher gates."""
import json
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.broker.dhan_client import DhanClient
from src.engine.config_manager import ConfigChangeTracker
from src.models import TradingMode


class TestOrderStatusReadFallback(unittest.TestCase):
    def _client(self, status_code, rows, single=None):
        client = DhanClient.__new__(DhanClient)
        client.cfg = SimpleNamespace(trading_mode=TradingMode.LIVE)
        response = SimpleNamespace(
            status_code=status_code,
            text="response",
            json=lambda: single,
        )
        client.session = SimpleNamespace(get=lambda *args, **kwargs: response)
        client._headers = lambda: {}
        client.get_order_book = lambda: rows
        return client

    def test_cancelled_order_can_be_verified_by_read_only_book(self):
        target = {"orderId": "OID-44", "orderStatus": "CANCELLED", "filledQty": 0}
        client = self._client(404, [target])
        self.assertEqual(client.order_status("OID-44"), target)

    def test_order_book_never_substitutes_another_order(self):
        client = self._client(404, [
            {"orderId": "OTHER", "orderStatus": "CANCELLED", "filledQty": 0}
        ])
        self.assertIsNone(client.order_status("OID-44"))

    def test_ambiguous_order_book_rows_fail_closed(self):
        target = {"orderId": "OID-44", "orderStatus": "CANCELLED", "filledQty": 0}
        client = self._client(404, [target, dict(target)])
        self.assertIsNone(client.order_status("OID-44"))

    def test_valid_single_order_read_keeps_primary_path(self):
        target = {"orderId": "OID-44", "orderStatus": "PENDING", "filledQty": 0}
        client = self._client(200, [], target)
        self.assertEqual(client.order_status("OID-44"), target)


class TestVerifiedWatcherTransition(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = tmp.name
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.db_path = os.path.join(self.data_dir, "journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": False, "in_flight_orders": []}, f)
        with sqlite3.connect(self.db_path) as db:
            db.executescript("""
            CREATE TABLE order_intents(correlation_id TEXT, state TEXT);
            CREATE TABLE orders(correlation_id TEXT, status TEXT);
            CREATE TABLE protection_attempts(attempt_id TEXT, state TEXT);
            CREATE TABLE protection_orders(parent_order_id TEXT, status TEXT);
            CREATE TABLE watcher_orders(order_id TEXT, record_json TEXT, is_terminal INTEGER);
            """)

    def _gate(self, status, filled, record=None, book_valid=True):
        state = {
            "filled_qty": 0,
            "protected_qty": 0,
            "state": "CANCELLED",
            "retry_reserved": False,
            "retry_eligible": False,
        }
        if record:
            state.update(record)
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO watcher_orders(order_id,record_json,is_terminal) VALUES (?,?,0)",
                ("OID-44", json.dumps(state)),
            )
        broker = MagicMock()
        broker.get_order_book.return_value = (
            [{"orderId": "OID-44", "orderStatus": status, "filledQty": filled}]
            if book_valid else None
        )
        with patch.object(
            self.tracker, "reconcile_stale_journal_with_broker", return_value=(True, None)
        ):
            return self.tracker.evaluate_trading_safety_gate_detailed(
                db_path=self.db_path,
                stop_state_path=self.stop_path,
                is_service_running=True,
                is_live_mode=True,
                target_is_live=False,
                broker=broker,
            )

    def test_verified_zero_fill_cancelled_watcher_can_clear_mode_gate(self):
        ok, reason, code, count = self._gate("CANCELLED", 0)
        self.assertTrue(ok, (reason, code, count))

    def test_active_broker_order_blocks_mode_transition(self):
        ok, _, code, _ = self._gate("PENDING", 0)
        self.assertFalse(ok)
        self.assertEqual(code, "LIVE_ORDER_IN_PROGRESS")

    def test_filled_broker_order_cannot_be_disregarded(self):
        ok, _, code, _ = self._gate("CANCELLED", 1)
        self.assertFalse(ok)
        self.assertEqual(code, "LIVE_ORDER_IN_PROGRESS")

    def test_missing_broker_book_fails_closed(self):
        ok, _, code, _ = self._gate("CANCELLED", 0, book_valid=False)
        self.assertFalse(ok)
        self.assertEqual(code, "BROKER_RECONCILIATION_FAILED")

    def test_retry_reserved_watcher_still_blocks(self):
        ok, _, code, _ = self._gate("CANCELLED", 0, record={"retry_reserved": True})
        self.assertFalse(ok)
        self.assertEqual(code, "LIVE_ORDER_IN_PROGRESS")


if __name__ == "__main__":
    unittest.main()
