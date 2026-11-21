"""Phase 2 real protection-evidence predicate under adversarial OCO states.

No Dhan calls, no cancellations and no journal writes. The actual UditBot
unified-exit safety predicate must reject unverified OCO/Super execution.
"""
import unittest
from src.engine.unified_exit_reconciliation import (
    ReconciliationEvidenceError, _verify_protective_orders_safe,
)

KEY=("SEC","NSE_EQ","CNC")
ACCOUNT="ACC"


def leg(name,status,**kw):
    return {
        "orderId":"OCO1","orderFlag":"OCO","orderType":"LIMIT",
        "securityId":"SEC","exchangeSegment":"NSE_EQ","productType":"CNC",
        "dhanClientId":"ACC","legName":name,"orderStatus":status,
        "quantity":2,"legOrderId":"LEG-T" if name=="TARGET_LEG" else "LEG-S",
        **kw,
    }


def check(forever, evidence=None, super_orders=None):
    return _verify_protective_orders_safe(
        forever=forever, super_orders=[] if super_orders is None else super_orders,
        key=KEY, account=ACCOUNT,
        broker_evidence=evidence if evidence is not None else {
            "EXEC1":{"side":"SELL","oid":"LEG-T","qty":2}},
    )


class TestProtectionRecovery(unittest.TestCase):
    def test_traded_target_closed_stop_with_matching_sell_is_safe(self):
        self.assertEqual(check([
            leg("TARGET_LEG","TRADED"),leg("STOP_LOSS_LEG","CANCELLED"),
        ]),set())

    def test_traded_protection_without_exact_executed_order_is_not_safe(self):
        for oid,qty in (("OTHER",2),("LEG-T",1),("LEG-S",2)):
            with self.subTest(oid=oid,qty=qty), self.assertRaises(ReconciliationEvidenceError):
                check([leg("TARGET_LEG","TRADED"),leg("STOP_LOSS_LEG","CANCELLED")],
                      {"EXEC1":{"side":"SELL","oid":oid,"qty":qty}})

    def test_partial_protection_or_triggered_not_verified(self):
        for status in ("TRIGGERED","PENDING","PART_TRADED","OPEN"):
            with self.subTest(status=status),self.assertRaises(ReconciliationEvidenceError):
                check([leg("TARGET_LEG",status),leg("STOP_LOSS_LEG","CANCELLED")])

    def test_sibling_not_closed_rejected(self):
        for st in ("PENDING","OPEN","TRIGGERED","PART_TRADED"):
            with self.subTest(status=st),self.assertRaises(ReconciliationEvidenceError):
                check([leg("TARGET_LEG","TRADED"),leg("STOP_LOSS_LEG",st)])

    def test_two_traded_oco_legs_are_not_verified(self):
        with self.assertRaises(ReconciliationEvidenceError):
            check([leg("TARGET_LEG","TRADED"),leg("STOP_LOSS_LEG","TRADED")])

    def test_unattributed_or_wrong_account_is_blocked(self):
        for wrong in (
            {**leg("TARGET_LEG","TRADED"),"exchangeSegment":None},
            {**leg("TARGET_LEG","TRADED"),"dhanClientId":"OTHER"},
        ):
            with self.subTest(wrong=wrong),self.assertRaises(ReconciliationEvidenceError):
                check([wrong,leg("STOP_LOSS_LEG","CANCELLED")])

    def test_oco_and_single_flags_conflict_is_blocked(self):
        bad={**leg("TARGET_LEG","TRADED"),"orderFlag":"SINGLE"}
        with self.assertRaises(ReconciliationEvidenceError):
            check([bad,leg("STOP_LOSS_LEG","CANCELLED")])

    def test_missing_exit_sibling_never_proves_oco(self):
        with self.assertRaises(ReconciliationEvidenceError):
            check([leg("TARGET_LEG","TRADED")])
