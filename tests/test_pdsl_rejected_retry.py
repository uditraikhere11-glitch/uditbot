import unittest
from unittest.mock import Mock
from src.engine.protection_watcher import ProtectionWatcher
from src.notifier.execution_updates import format_order_outcome


class PdslRejectionTests(unittest.TestCase):
    def test_repeat_and_changed_quantity(self):
        watcher = Mock()
        rec = {"symbol": "PDSL", "order_id": "123"}
        for now, qty, alerts, delay in [(0, 1, 1, 60), (60, 1, 1, 120),
                                       (900, 1, 2, 240), (901, 2, 3, 60)]:
            watcher.clock.return_value = now
            ProtectionWatcher._record_unprotected_and_halt(
                watcher, rec, qty, 390, 389, 415, "UNPROTECTED",
                "Forever OCO rejected (400): DH-906")
            self.assertEqual(watcher.journal_db.enqueue_outbox.call_count, alerts)
            self.assertEqual(rec["retry_at"], now + delay)
        self.assertEqual(watcher.broker.reconciliation_gate.halt_new_entries.call_count, 4)

    def test_rejection_card(self):
        text = format_order_outcome({"oco_rejected": True}, 1, 1, "TRADED",
                                    1000, 402.5, False)
        self.assertIn("Forever OCO rejected", text)
        self.assertNotIn("Pending Broker Verification", text)

    def test_restart_blocks_rejected_parent_before_new_reservation(self):
        from tempfile import TemporaryDirectory
        from src.journal.db import JournalDB, RejectedProtectionNeedsReview
        with TemporaryDirectory() as tmp:
            path = tmp + "/journal.db"
            db = JournalDB(path)
            args = dict(parent_order_id="123", root_order_id="123", account_id="A",
                        symbol="PDSL", security_id="1", exchange_segment="NSE_EQ",
                        product_type="CNC", route="FOREVER_OCO", uncovered_from_qty=0,
                        quantity=1, executed_price=402.5, sl_trigger=390, sl_limit=389,
                        target_price=415, tick_size=0.05)
            db.create_protection_attempt(attempt_id="a", correlation_id="a", **args)
            db.update_protection_attempt("a", state="REJECTED",
                                         error_msg="Forever OCO rejected (400): DH-906")
            with self.assertRaises(RejectedProtectionNeedsReview):
                JournalDB(path).create_protection_attempt(attempt_id="b", correlation_id="b", **args)
            self.assertEqual(len(db.get_protection_attempts("123")), 1)
