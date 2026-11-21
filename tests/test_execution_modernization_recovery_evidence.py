"""Phase 2 strict broker/journal identity and recovery classifier.

Every case is synthetic, network-isolated, and MUST keep order-resubmission,
new-entry gate and protection authorization disabled. Original UditBot
StartupReconciliationGate remains the sole live trading authority.
"""
import copy
import unittest

from src.engine.recovery_evidence import (
    assess_order_recovery, summarize_recovery_assessments,
)

INTENT={
    "correlation_id":"CORR-1","broker_order_id":"OID-1",
    "account_id":"ACC","security_id":"SEC",
    "exchange_segment":"NSE_EQ","product_type":"CNC","quantity":5,
}
ORDER={
    "correlationId":"CORR-1","orderId":"OID-1","dhanClientId":"ACC",
    "securityId":"SEC","exchangeSegment":"NSE_EQ","productType":"CNC",
    "transactionType":"BUY","quantity":5,"filledQty":0,
    "orderStatus":"PENDING",
}
TRADE={
    "orderId":"OID-1","dhanClientId":"ACC","securityId":"SEC",
    "exchangeSegment":"NSE_EQ","productType":"CNC",
    "transactionType":"BUY","exchangeTradeId":"X1","tradedQuantity":2,
    "tradedPrice":100.5,"exchangeTime":"2026-11-21 10:30:00",
}


def run(*, intent=None,order=None,trades=None,books_complete=True,
        orders=None,trade_book=None):
    if orders is None:
        orders=[ORDER if order is None else order]
    if trade_book is None:
        trade_book=[] if trades is None else trades
    return assess_order_recovery(
        INTENT if intent is None else intent,account_id="ACC",
        order_book=orders,trade_book=trade_book,
        books_complete=books_complete,
    )


class TestRecoveryEvidence(unittest.TestCase):
    def assert_non_authoritative(self,assessment):
        self.assertFalse(assessment.may_resubmit)
        self.assertFalse(assessment.may_open_gate)
        self.assertFalse(assessment.may_claim_protection)

    def test_pending_unfilled_is_not_safe_to_retry(self):
        r=run()
        self.assertEqual(r.classification,"OPEN_UNFILLED")
        self.assertTrue(r.requires_broker_review)
        self.assert_non_authoritative(r)

    def test_no_order_is_not_proof_of_missing_submission(self):
        r=run(orders=[])
        self.assertEqual(r.reason_code,"ORDER_NOT_IN_CURRENT_BOOK")
        self.assertTrue(r.requires_broker_review)
        self.assert_non_authoritative(r)

    def test_unavailable_or_malformed_books_fail_closed(self):
        for books_complete,orders,trade_book in (
            (False,[],[]),(True,None,[]),(True,[42],[]),
            (True,[ORDER],["invalid"]),
        ):
            with self.subTest(books_complete=books_complete,orders=orders):
                r=assess_order_recovery(INTENT,account_id="ACC",
                    order_book=orders,trade_book=trade_book,
                    books_complete=books_complete)
                self.assertEqual(r.classification,"UNVERIFIED")
                self.assert_non_authoritative(r)

    def test_partial_fill_requires_real_exchange_trade(self):
        order={**ORDER,"orderStatus":"PART_TRADED","filledQty":2}
        r=run(order=order,trades=[TRADE])
        self.assertEqual(r.classification,"PARTIAL_FILL_AWAITING_PROTECTION")
        self.assertEqual(r.tradebook_executed_qty,2)
        self.assertTrue(r.requires_protection_verification)
        self.assert_non_authoritative(r)

    def test_full_fill_still_not_protection(self):
        t2={**TRADE,"exchangeTradeId":"X2","tradedQuantity":3}
        r=run(order={**ORDER,"orderStatus":"TRADED","filledQty":5},
              trades=[TRADE,t2])
        self.assertEqual(r.classification,"FILLED_AWAITING_PROTECTION")
        self.assertEqual(r.tradebook_executed_qty,5)
        self.assert_non_authoritative(r)

    def test_identical_exchange_trades_from_multiple_reads_count_once(self):
        r=run(order={**ORDER,"orderStatus":"PART_TRADED","filledQty":2},
              trades=[TRADE,TRADE])
        self.assertEqual(r.tradebook_executed_qty,2)
        self.assertEqual(r.classification,"PARTIAL_FILL_AWAITING_PROTECTION")

    def test_conflicting_duplicate_execution_id_rejected(self):
        r=run(order={**ORDER,"orderStatus":"PART_TRADED","filledQty":2},
              trades=[TRADE,{**TRADE,"tradedQuantity":3}])
        self.assertEqual(r.reason_code,"CONTRADICTORY_EXCHANGE_TRADE_ID")
        self.assert_non_authoritative(r)

    def test_exchange_trade_id_cannot_belong_to_two_broker_orders(self):
        other={**TRADE,"orderId":"OTHER-OID"}
        order={**ORDER,"orderStatus":"PART_TRADED","filledQty":2}
        r=run(order=order,trades=[TRADE,other])
        self.assertEqual(r.reason_code,"EXCHANGE_TRADE_ID_REUSED_ACROSS_ORDERS")
        self.assert_non_authoritative(r)

    def test_lost_ack_book_traded_but_missing_trades_is_unknown(self):
        r=run(order={**ORDER,"orderStatus":"TRADED","filledQty":5},trades=[])
        self.assertEqual(r.reason_code,"BROKER_TRADEBOOK_FILL_DISAGREEMENT")
        self.assert_non_authoritative(r)

    def test_cancelled_partial_fill_still_needs_protection(self):
        r=run(order={**ORDER,"orderStatus":"CANCELLED","filledQty":2},
              trades=[TRADE])
        self.assertEqual(r.classification,"PARTIAL_FILL_AWAITING_PROTECTION")
        self.assertTrue(r.requires_protection_verification)
        self.assert_non_authoritative(r)

    def test_terminal_no_fill_not_permission_to_reenter(self):
        for status in ("CANCELLED","EXPIRED","REJECTED"):
            with self.subTest(status=status):
                order={**ORDER,"orderStatus":status}
                if status=="REJECTED":
                    order.pop("filledQty")
                r=run(order=order)
                self.assertFalse(r.requires_broker_review)
                self.assert_non_authoritative(r)

    def test_duplicate_order_id_and_correlations_are_ambiguous(self):
        r=run(orders=[ORDER,ORDER])
        self.assertEqual(r.reason_code,"AMBIGUOUS_ORDERBOOK_OWNERSHIP")
        self.assert_non_authoritative(r)

    def test_missing_or_conflicting_identity_fields_block(self):
        variants=(
            {"dhanClientId":"OTHER"},{"dhanClientId":None},
            {"securityId":"OTHER"},{"exchangeSegment":None},
            {"productType":"MARGIN"},{"transactionType":"SELL"},
            {"transactionType":None},{"correlationId":None},
            {"correlationId":"OTHER"},{"quantity":6},
            {"orderId":"OTHER"},
        )
        for updates in variants:
            with self.subTest(updates=updates):
                r=run(order={**ORDER,**updates})
                self.assertEqual(r.classification,"UNVERIFIED")
                self.assert_non_authoritative(r)

    def test_overfilled_status_is_not_accepted(self):
        for value in (6,-1,True,3.4,"5.0",None):
            with self.subTest(value=value):
                r=run(order={**ORDER,"filledQty":value})
                self.assertEqual(r.classification,"UNVERIFIED")
                self.assert_non_authoritative(r)

    def test_filled_qty_and_trade_identities_must_match(self):
        variants=(
            {"dhanClientId":"OTHER"},{"dhanClientId":None},
            {"securityId":"OTHER"},{"exchangeSegment":"NSE_FNO"},
            {"productType":"MTF"},{"transactionType":"SELL"},
            {"exchangeTradeId":None},{"exchangeTime":None},
            {"tradedPrice":0},{"tradedQuantity":0},
        )
        order={**ORDER,"orderStatus":"PART_TRADED","filledQty":2}
        for updates in variants:
            with self.subTest(updates=updates):
                r=run(order=order,trades=[{**TRADE,**updates}])
                self.assertEqual(r.classification,"UNVERIFIED")
                self.assert_non_authoritative(r)

    def test_full_book_trades_sum_cannot_overrun_order_size(self):
        order={**ORDER,"orderStatus":"TRADED","filledQty":5}
        r=run(order=order,trades=[
            {**TRADE,"tradedQuantity":4},
            {**TRADE,"exchangeTradeId":"X2","tradedQuantity":3},
        ])
        self.assertEqual(r.reason_code,"TRADEBOOK_OVERFILLED")

    def test_cross_account_intent_rejected(self):
        r=run(intent={**INTENT,"account_id":"OTHER"})
        self.assertEqual(r.reason_code,"JOURNAL_IDENTITY_UNVERIFIED")

    def test_missing_broker_oid_can_resolve_only_exact_correlation(self):
        r=run(intent={**INTENT,"broker_order_id":""})
        self.assertEqual(r.classification,"OPEN_UNFILLED")
        bad=run(intent={**INTENT,"broker_order_id":"OTHER"})
        self.assertEqual(bad.reason_code,"BROKER_ORDER_CORRELATION_CONFLICT")

    def test_status_partial_must_be_proportionate(self):
        for status,filled in (("TRADED",2),("PART_TRADED",0),("PART_TRADED",5),
                              ("REJECTED",2),("GARBAGE",0)):
            with self.subTest(status=status,filled=filled):
                r=run(order={**ORDER,"orderStatus":status,"filledQty":filled})
                self.assertEqual(r.classification,"UNVERIFIED")
                self.assert_non_authoritative(r)

    def test_invalid_journal_intents_rejected(self):
        for change in ({"quantity":0},{"quantity":True},
                       {"security_id":""},{"correlation_id":""}):
            with self.subTest(change=change):
                r=run(intent={**INTENT,**change})
                self.assertEqual(r.classification,"UNVERIFIED")

    def test_report_is_anonymized_and_always_non_authoritative(self):
        report=summarize_recovery_assessments(
            [INTENT,{**INTENT,"correlation_id":"OTHER"}],
            account_id="ACC",order_book=[ORDER],trade_book=[],books_complete=True)
        self.assertEqual(report["intent_count"],2)
        self.assertEqual(report["unresolved_broker_review_count"],2)
        self.assertFalse(report["may_open_new_entries"])
        self.assertFalse(report["may_resubmit_unknown_orders"])
        self.assertFalse(report["may_claim_verified_protection"])
        for sensitive in ("OID-1","CORR-1","SEC","ACC"):
            self.assertNotIn(sensitive,repr(report))
