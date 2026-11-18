"""Simulated transport and restart regressions. No credentials or external I/O."""

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.broker.dhan_client import DhanClient
from src.engine.reconciliation import StartupReconciliationGate
from src.journal.db import JournalDB
from src.notifier.outbox_worker import OutboxWorker
from src.notifier.telegram_notifier import TelegramNotifier


def telegram_response(status, **body):
    return Mock(status_code=status, json=Mock(return_value=body))


class TelegramTests(unittest.TestCase):
    def setUp(self):
        self.sender = TelegramNotifier(
            SimpleNamespace(tg_bot_token="dummy", log_group_id="test")
        )

    @patch("src.notifier.telegram_notifier.requests.post")
    def test_flood_wait_survives_outbox_restart(self, post):
        post.side_effect = [
            telegram_response(429, parameters={"retry_after": 90}),
            telegram_response(200, ok=True),
        ]
        with TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "journal.db")
            db = JournalDB(path)
            db.enqueue_outbox("LOG", {"text": "entry"})
            worker = OutboxWorker(
                db, lambda msg: self.sender.send_log_message(msg, outbox=True)
            )
            self.assertEqual(worker.process_pending_events(now=100), 0)
            replacement = TelegramNotifier(self.sender.cfg)
            worker = OutboxWorker(JournalDB(path), replacement.send_log_message)
            self.assertEqual(worker.process_pending_events(now=189), 0)
            self.assertEqual(worker.process_pending_events(now=190), 1)
            self.assertEqual(worker.process_pending_events(now=200), 0)
            self.assertEqual(post.call_count, 2)

    @patch("src.notifier.telegram_notifier.requests.post")
    def test_fallback_only_for_entity_errors(self, post):
        for status in [401, 403, 500, 200]:
            post.return_value = telegram_response(status, ok=False)
            self.assertFalse(self.sender.send_log_message("entry"))
        self.assertEqual(post.call_count, 4)
        post.side_effect = [
            telegram_response(400, description="Bad Request: can't parse entities"),
            telegram_response(200, ok=True),
        ]
        self.assertTrue(self.sender.send_log_message("<broken>"))
        self.assertNotIn("parse_mode", post.call_args.kwargs["json"])

    @patch("src.notifier.telegram_notifier.requests.post")
    def test_exception_details_stay_out_of_logs(self, post):
        post.side_effect = OSError("sensitive-placeholder")
        with self.assertLogs("telegram_notifier") as captured:
            self.assertFalse(self.sender.send_log_message("entry"))
        self.assertNotIn(
            "sensitive-placeholder",
            str(captured.output) + self.sender.last_delivery_status,
        )


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.broker = Mock()
        self.gate = StartupReconciliationGate(self.broker, None, None, None)
        self.row = {"orderId": "123", "correlationId": "abc", "orderStatus": "PENDING"}

    def test_book_match_needs_no_point_requests(self):
        self.assertEqual(
            self.gate._lookup_intent_at_broker("abc", "123", [self.row]), self.row
        )
        self.assertEqual(self.broker.method_calls, [])

    def test_wrong_correlation_and_absent_broker_id(self):
        self.broker.get_order_by_correlation_id.return_value = dict(
            self.row, correlationId="wrong"
        )
        self.assertIsNone(self.gate._lookup_intent_at_broker("abc", None, []))
        self.broker.order_status.assert_not_called()

    def test_terminal_and_partial_fill_evidence(self):
        for status, filled, expected in [
            ("CANCELLED", None, False),
            ("CANCELLED", 2, True),
            ("PART_TRADED", 2, True),
            ("TRADED", 8, False),
            ("NEW_UNKNOWN", 0, False),
        ]:
            row = dict(self.row, orderStatus=status, filledQty=filled)
            ok, _ = self.gate._validate_broker_order_matches_intent(
                row, {"quantity": 5}, "abc", "123"
            )
            self.assertEqual(ok, expected)

    def test_missing_ack_id_cannot_become_an_order(self):
        for value in (None, False, [], {}, "null", "None", 0, ""):
            self.assertEqual(DhanClient._ack_order_id({"orderId": value}), "")
        self.assertEqual(DhanClient._ack_order_id({"orderId": 123}), "123")

    def test_duplicate_correlation_after_timeout_stays_unknown(self):
        broker = DhanClient.__new__(DhanClient)
        broker.get_order_by_correlation_id = Mock(return_value=None)
        broker.get_order_book = Mock(
            return_value=[self.row, dict(self.row, orderId="234")]
        )
        ok, order_id, _ = broker._reconcile_uncertain_submission("abc", "")
        self.assertFalse(ok)
        self.assertIsNone(order_id)


class AdmissionRecoveryTests(unittest.TestCase):
    def test_reconciliation_paces_local_burst_and_sends_one_get(self):
        from src.broker.read_governor import RECONCILIATION_PACING, ReadGovernor

        with TemporaryDirectory() as tmp:
            clock = [100.0]
            g = ReadGovernor(tmp, "test", clock=lambda: clock[0])
            broker = DhanClient.__new__(DhanClient)
            broker._read_governor = g
            broker.cfg = SimpleNamespace(dhan_client_id="test")
            broker.session = Mock()
            broker.session.get.return_value = telegram_response(200)
            for _ in range(20):
                g.admit(priority="urgent")
            token = RECONCILIATION_PACING.set(True)
            try:
                with patch("src.broker.dhan_client.time.sleep",
                           side_effect=lambda seconds: clock.__setitem__(0, clock[0]+seconds)) as wait:
                    response = broker._governed_get("https://api.dhan.co/v2/fundlimit", headers={}, timeout=1)
                    self.assertEqual(response.status_code, 200)
                    wait.assert_called_once_with(1)
                    broker.session.get.assert_called_once()
                    g.throttled(120)
                    response = broker._governed_get("https://api.dhan.co/v2/fundlimit", headers={}, timeout=1)
                    self.assertEqual(response.status_code, 429)
                    broker.session.get.assert_called_once()
                    wait.assert_called_once()
            finally:
                RECONCILIATION_PACING.reset(token)

    def test_missing_modification_ledger_blocks_next_mutation(self):
        import sqlite3
        from src.broker.read_governor import ReadGovernor

        with TemporaryDirectory() as tmp:
            g = ReadGovernor(tmp, "test")
            g.admit(category="ORDER", modification_order_id="123")
            with sqlite3.connect(g.path) as db:
                db.execute("DROP TABLE order_modifications")
            with self.assertRaises(sqlite3.DatabaseError):
                g.admit(category="ORDER", modification_order_id="123")
