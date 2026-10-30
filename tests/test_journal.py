"""
Unit tests for SQLite Audit Journal, State Persistence, and STEP 08 requirements.
Tests atomic claims, message revisions, order intent states, and outbox.
"""

import os
import tempfile
import unittest
from src.journal.db import JournalDB


class TestJournalDB(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_journal.db")
        self.journal = JournalDB(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_record_signal_and_retrieve(self):
        sig_id = self.journal.record_signal(
            channel_id=-10012345,
            message_id=987,
            signal_type="EQUITY",
            symbol="SBIN",
            action="BUY_ENTRY",
            raw_text="Buy #SBIN @ 805",
            status="PARSED",
            reason="Regex matched",
        )
        self.assertGreater(sig_id, 0)

        recent = self.journal.get_recent_signals(limit=5)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["symbol"], "SBIN")
        self.assertEqual(recent[0]["action"], "BUY_ENTRY")

    def test_08_b_claim_signal_unique_and_duplicate(self):
        """08-B: Signal claims are uniquely indexed per (channel_id, message_id)."""
        is_new, reason, claim_id = self.journal.claim_signal(
            channel_id=-100999, message_id=101, raw_text="Buy #TMPV 280CE", symbol="TMPV"
        )
        self.assertTrue(is_new)
        self.assertEqual(reason, "CLAIM_ACCEPTED")

        # Duplicate message
        is_new2, reason2, claim_id2 = self.journal.claim_signal(
            channel_id=-100999, message_id=101, raw_text="Buy #TMPV 280CE", symbol="TMPV"
        )
        self.assertFalse(is_new2)
        self.assertEqual(reason2, "DUPLICATE_MESSAGE_CLAIM")
        self.assertEqual(claim_id, claim_id2)

    def test_08_b_edited_message_creates_revision_not_claim(self):
        """DEC-06 / 08-B: Edited message records revision event; does not create new entry claim."""
        is_new, reason, claim_id = self.journal.claim_signal(
            channel_id=-100999, message_id=202, raw_text="Buy #INFY @ 1500", symbol="INFY"
        )
        self.assertTrue(is_new)

        # Message edited on Telegram
        is_new_edit, reason_edit, claim_id_edit = self.journal.claim_signal(
            channel_id=-100999, message_id=202, raw_text="Buy #INFY @ 1510 (Updated Target)", symbol="INFY"
        )
        self.assertFalse(is_new_edit)
        self.assertEqual(reason_edit, "MESSAGE_EDITED_EVENT")
        self.assertEqual(claim_id, claim_id_edit)

        # Verify revision history in database
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT * FROM signal_revisions WHERE claim_id = ?", (claim_id,))
            revs = cur.fetchall()
            self.assertEqual(len(revs), 1)
            self.assertEqual(revs[0]["revision_num"], 1)
            self.assertIn("1500", revs[0]["old_text"])
            self.assertIn("1510", revs[0]["new_text"])

    def test_08_c_order_intent_lifecycle(self):
        """08-C: Pre-submit intent recorded before network POST and updated reliably."""
        corr_id = "OPT-TMPV-1730000000"
        intent_id = self.journal.record_order_intent(
            correlation_id=corr_id,
            symbol="TMPV",
            security_id="9988",
            exchange_segment="NSE_FNO",
            product_type="MARGIN",
            order_type="LIMIT",
            price=9.20,
            quantity=500,
        )
        self.assertGreater(intent_id, 0)

        # Verify initially in INTENT_CREATED state
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT state FROM order_intents WHERE correlation_id = ?", (corr_id,))
            self.assertEqual(cur.fetchone()[0], "INTENT_CREATED")

        # Update to ACKED with broker order ID
        self.journal.update_order_intent(corr_id, state="ACKED", broker_order_id="DHAN-12345")
        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT state, broker_order_id FROM order_intents WHERE correlation_id = ?", (corr_id,))
            row = cur.fetchone()
            self.assertEqual(row["state"], "ACKED")
            self.assertEqual(row["broker_order_id"], "DHAN-12345")

    def test_08_e_reconcile_unknown_intents(self):
        """08-E: Recovers UNKNOWN intents to avoid blind re-execution."""
        self.journal.record_order_intent(
            correlation_id="OPT-RELIANCE-999",
            symbol="RELIANCE",
            security_id="2885",
            exchange_segment="NSE_FNO",
            product_type="MARGIN",
            order_type="LIMIT",
            price=25.0,
            quantity=250,
        )
        self.journal.update_order_intent("OPT-RELIANCE-999", state="UNKNOWN", error_msg="HTTP Timeout")

        unknowns = self.journal.get_unknown_intents()
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0]["symbol"], "RELIANCE")
        self.assertEqual(unknowns[0]["state"], "UNKNOWN")

    def test_record_validation(self):
        # Create parent signal first to satisfy foreign key constraint
        sig_id = self.journal.record_signal(
            channel_id=-100111,
            message_id=303,
            signal_type="EQUITY",
            symbol="TATAMOTORS",
            action="BUY_ENTRY",
            raw_text="Buy #TATAMOTORS @ 950",
            status="PARSED",
            reason="Approved",
        )
        val_id = self.journal.record_validation(
            signal_id=sig_id,
            symbol="TATAMOTORS",
            limit_price=950.0,
            units=25,
            total_quantity=25,
            total_cost=23750.0,
            is_passed=True,
            rejection_reason="Passed all risk bounds",
        )
        self.assertGreater(val_id, 0)

    def test_record_order_and_zero_fake_fills(self):
        order_id = self.journal.record_order(
            correlation_id="TEST-CORR-001",
            trading_mode="TEST",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="INFY",
            security_id="1594",
            order_type="LIMIT",
            price=1500.0,
            quantity=15,
            status="TEST_VALIDATED",
        )
        self.assertGreater(order_id, 0)

        with self.journal._get_conn() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM fills")
            self.assertEqual(cur.fetchone()[0], 0, "Fills table must be 0 (no simulated/fake fills in TEST mode)")

    def test_outbox_queue(self):
        out_id = self.journal.enqueue_outbox("TRADE_ACCEPTED", {"symbol": "SBIN", "qty": 10})
        self.assertGreater(out_id, 0)

        pending = self.journal.fetch_pending_outbox()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["event_type"], "TRADE_ACCEPTED")

        self.journal.mark_outbox_processed(out_id)
        pending_after = self.journal.fetch_pending_outbox()
        self.assertEqual(len(pending_after), 0)


if __name__ == "__main__":
    unittest.main()
