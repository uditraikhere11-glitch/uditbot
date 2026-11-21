"""Read-only Phase 2 comparison on actual UditBot JournalDB schema.

No live broker queries, journal mutation, credentials or sensitive stdout.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest

from src.journal.db import JournalDB
from tools.phase2_recovery_compare import read_only_comparison
from tests.test_execution_modernization_recovery_evidence import (
    INTENT, ORDER, TRADE,
)


class TestReadOnlyPhase2Comparison(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        folder=Path(self.tmp.name)
        self.path=folder/"journal.sqlite3"
        self.books=folder/"books.json"
        self.db=JournalDB(str(self.path),account_id="ACC")
        self.db.record_order_intent(
            correlation_id="CORR-1",symbol="TEST",security_id="SEC",
            exchange_segment="NSE_EQ",product_type="CNC",order_type="LIMIT",
            price=100.0,quantity=5,account_id="ACC",
        )
        self.db.update_order_intent(
            "CORR-1",state="PENDING",broker_order_id="OID-1",
        )
        self.set_books({
            "account_id":"ACC","books_complete":True,
            "order_book":[ORDER],"trade_book":[],
        })

    def tearDown(self):
        self.tmp.cleanup()

    def set_books(self,payload):
        self.books.write_text(json.dumps(payload),encoding="utf-8")
        os.chmod(self.books,0o600)

    def test_unfilled_pending_comparison_cannot_auto_resubmit(self):
        before=self.path.read_bytes()
        result=read_only_comparison(str(self.path),str(self.books),account_id="ACC")
        self.assertEqual(result["baseline_nonterminal_journal_intents"],1)
        self.assertEqual(result["strict_evidence_classifications"],{"OPEN_UNFILLED":1})
        self.assertFalse(result["safe_for_automatic_replay"])
        self.assertFalse(result["safe_to_open_entry_gate"])
        self.assertFalse(result["all_positions_protected"])
        self.assertEqual(before,self.path.read_bytes())
        for forbidden in ("OID-1","CORR-1","SEC","ACC"):
            self.assertNotIn(forbidden,repr(result))

    def test_partial_fill_still_requires_protection_review(self):
        self.set_books({
            "account_id":"ACC","books_complete":True,
            "order_book":[{**ORDER,"orderStatus":"PART_TRADED","filledQty":2}],
            "trade_book":[TRADE],
        })
        r=read_only_comparison(str(self.path),str(self.books),account_id="ACC")
        self.assertEqual(r["strict_evidence_classifications"],{
            "PARTIAL_FILL_AWAITING_PROTECTION":1,
        })
        self.assertEqual(r["partial_or_full_fills_needing_protection_review"],1)

    def test_missing_books_are_not_empty_safe_portfolio(self):
        self.set_books({
            "account_id":"ACC","books_complete":False,
            "order_book":[],"trade_book":[],
        })
        r=read_only_comparison(str(self.path),str(self.books),account_id="ACC")
        self.assertEqual(r["strict_evidence_reason_codes"],{
            "INCOMPLETE_BROKER_BOOKS":1,
        })
        self.assertEqual(r["intents_needing_broker_review"],1)

    def test_account_mismatch_rejected(self):
        self.set_books({
            "account_id":"OTHER","books_complete":True,
            "order_book":[ORDER],"trade_book":[],
        })
        with self.assertRaises(ValueError):
            read_only_comparison(str(self.path),str(self.books),account_id="ACC")

    def test_file_permissions_restrict_broker_book_evidence(self):
        if os.name!="posix":
            self.skipTest("POSIX file permissions")
        os.chmod(self.books,0o644)
        with self.assertRaises(PermissionError):
            read_only_comparison(str(self.path),str(self.books),account_id="ACC")

    def test_query_bound_fails_instead_of_truncating_unknowns(self):
        with self.assertRaises(ValueError):
            read_only_comparison(
                str(self.path),str(self.books),account_id="ACC",max_intents=0,
            )
