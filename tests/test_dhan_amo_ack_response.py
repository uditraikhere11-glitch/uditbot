"""AMO acknowledgement routing stays separate from executed/final Dhan status.

All tests are offline: no real Dhan or Telegram endpoints are called.
"""
import json
import os
import tempfile
import unittest

from src.journal.db import JournalDB
from src.notifier.execution_updates import format_amo_acknowledged
from src.notifier.outbox_worker import OutboxWorker


class TestDhanAMOAcknowledgement(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.db = JournalDB(os.path.join(folder.name, "journal.db"))

    def test_accepted_amo_is_not_labeled_filled(self):
        text = format_amo_acknowledged(
            {"symbol": "IDEA", "limit_price": 12.34}, 5, "TRANSIT"
        )
        self.assertIn("AMO Accepted by Dhan", text)
        self.assertIn("NOT EXECUTED", text)
        self.assertIn("IDEA", text)
        self.assertNotIn("Order Executed", text)
        self.assertNotIn("Order ID", text)

    def test_fno_amo_matches_equity_card_with_broker_id(self):
        text = format_amo_acknowledged(
            {
                "symbol": "HAL28NOV4750CE", "limit_price": 120.45,
                "order_id": "DHAN-456", "entry_only_amo": True,
            }, 75, "TRANSIT", allow_unverified=True,
        )
        self.assertIn("🌙 <b>Dhan AMO Acknowledgement</b>", text)
        self.assertIn("• Symbol: HAL28NOV4750CE", text)
        self.assertIn("• Quantity: 75", text)
        self.assertIn("• Limit Price: ₹120.45", text)
        self.assertIn("• Dhan Order ID: DHAN-456", text)
        self.assertIn("• Dhan Status: TRANSIT", text)
        self.assertIn("✅ <b>AMO Accepted by Dhan</b>", text)
        self.assertIn("NOT EXECUTED", text)
        self.assertIn("No automatic SL/target", text)
        self.assertNotIn("Order Executed", text)

    def test_fno_amo_unverified_status_never_claims_queued(self):
        text = format_amo_acknowledged(
            {"symbol": "HAL28NOV4750CE", "limit_price": 120.45,
             "order_id": "DHAN-456", "entry_only_amo": True},
            75, "TRADED", allow_unverified=True,
        )
        self.assertIn("• Dhan Status: TRADED", text)
        self.assertIn("Broker status needs verification", text)
        self.assertNotIn("AMO Accepted by Dhan", text)
        self.assertNotIn("Queued for market OPEN", text)
        self.assertNotIn("NOT EXECUTED", text)

    def test_fno_amo_html_escapes_symbol_and_order_id(self):
        text = format_amo_acknowledged(
            {"symbol": "<BAD&CO>", "limit_price": 12.0,
             "order_id": "<id&123>", "entry_only_amo": True},
            1, "PENDING", allow_unverified=True,
        )
        self.assertIn("&lt;BAD&amp;CO&gt;", text)
        self.assertIn("&lt;id&amp;123&gt;", text)
        self.assertNotIn("<id&123>", text)

    def test_ambiguous_broker_status_does_not_format_ack(self):
        for status in ("UNKNOWN", "REJECTED", "TRADED", "", "CANCELLED"):
            with self.subTest(status=status), self.assertRaises(ValueError):
                format_amo_acknowledged({"symbol": "IDEA"}, 5, status)

    def test_amo_queued_once_without_duplicate_log(self):
        self.assertTrue(self.db.enqueue_dhan_response_ack_once(
            "BROKER-123", "AMO received — NOT EXECUTED",
        ))
        self.assertFalse(self.db.enqueue_dhan_response_ack_once(
            "BROKER-123", "AMO received — NOT EXECUTED",
        ))
        rows = self.db.fetch_pending_outbox()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "DHAN_RESPONSE_ACK")
        self.assertEqual(json.loads(rows[0]["payload"])["text"], "AMO received — NOT EXECUTED")

    def test_ack_goes_to_response_group_not_existing_log_group(self):
        self.db.enqueue_dhan_response_ack_once("BROKER-222", "Queued for OPEN")
        existing_log = []
        responses = []
        worker = OutboxWorker(
            self.db,
            dispatch_fn=lambda text: existing_log.append(text) or True,
            dhan_response_dispatch_fn=lambda text: responses.append(text) or True,
        )
        self.assertEqual(worker.process_pending_events(), 1)
        self.assertEqual(existing_log, [])
        self.assertEqual(responses, ["Queued for OPEN"])
        self.assertEqual(self.db.fetch_pending_outbox(), [])

    def test_missing_destination_is_never_marked_delivered(self):
        self.db.enqueue_dhan_response_ack_once("BROKER-333", "Queued for OPEN")
        worker = OutboxWorker(self.db, dispatch_fn=lambda text: True)
        self.assertEqual(worker.process_pending_events(), 0)
        with self.db._get_conn() as conn:
            item = conn.execute(
                "SELECT status, retry_count FROM outbox WHERE event_type = 'DHAN_RESPONSE_ACK'"
            ).fetchone()
        self.assertEqual(item["status"], "PENDING")
        self.assertEqual(item["retry_count"], 1)

    def test_final_status_remains_independent(self):
        self.assertTrue(self.db.enqueue_dhan_response_ack_once("BROKER-444", "Queued"))
        self.assertTrue(self.db.enqueue_order_notification_once(
            "BROKER-444", "TRADED", "Confirmed filled", mirror_final=True,
        ))
        items = self.db.fetch_pending_outbox()
        self.assertEqual(
            [item["event_type"] for item in items],
            ["DHAN_RESPONSE_ACK", "ORDER_STATUS_UPDATE", "DHAN_FINAL_RESPONSE"],
        )


if __name__ == "__main__":
    unittest.main()
