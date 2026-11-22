"""Fail-closed old settled BUY read-policy tests. No network access."""
import unittest
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo
from src.engine.settled_owner_read_policy import may_omit_prior_day_buy_trade_read

NOW = datetime(2026, 11, 22, 18, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
OLD = datetime(2026, 11, 18, 10, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
TODAY = "2026-11-22"

def holding(oid):
    return dict(order_id=oid, correlation_id=f"CORR-{oid}", security_id="11915",
                segment="NSE_EQ", product_type="CNC", position_state="RECONCILIATION_REQUIRED",
                filled_qty=1, exit_filled_qty=0, manual_risk_acknowledged=True,
                unreadable_broker_response=True, owner_history_day=TODAY)

class SettledOwnerReadPolicy(unittest.TestCase):
    def setUp(self):
        self.a, self.b = holding("BUY-A"), holding("BUY-B")
        self.db = SimpleNamespace(
            is_manual_risk_acknowledged=lambda *a, **kw: True,
            owner_lot_basis=lambda *a: (OLD, [dict(traded_quantity=1)]),
        )
        self.data = dict(record=self.a, all_open_records=[self.a,self.b],
                         leg_order_ids={"BUY-A"}, journal_db=self.db,
                         gate_books={"verified":{"BUY-A":("ACCOUNT","11915",1)}},
                         current_trades=[], current_positions=[],
                         current_holdings=[dict(securityId="11915",totalQty=2,dpQty=2,t1Qty=0)],
                         account_id="ACCOUNT", now=NOW)
    def permitted(self,**changes):
        return may_omit_prior_day_buy_trade_read(**dict(self.data,**changes))

    def test_two_original_buy_parents_only(self):
        self.assertTrue(self.permitted())
        self.assertTrue(self.permitted(record=self.b,leg_order_ids={"BUY-B"},
                     gate_books={"verified":{"BUY-B":("ACCOUNT","11915",1)}}))
    def test_recovery_requires_fresh_historical_proof(self):
        # Cached date is explicitly insufficient while reconciliation is blocked.
        self.assertFalse(self.permitted(gate_books=None))
        self.assertFalse(self.permitted(gate_books={"verified":{}}))
        self.assertTrue(self.permitted())
        # Healthy settled positions can use the date cache for bounded routine
        # surveillance, but stale or unreadable states may not.
        stable = dict(self.a, position_state="UNPROTECTED_MANUAL_RISK",
                      reconciliation_required=False, unreadable_broker_response=False,
                      manual_exit_detected=False)
        self.assertTrue(self.permitted(gate_books=None, record=stable))
        self.assertFalse(self.permitted(gate_books=None,record=dict(stable,owner_history_day="2026-11-21")))
        self.assertFalse(self.permitted(gate_books=None,record=dict(stable,unreadable_broker_response=True)))
        self.assertFalse(self.permitted(gate_books=None,record=dict(stable,reconciliation_required=True)))
    def test_live_exit_or_pending_mutation_never_qualifies(self):
        self.assertFalse(self.permitted(leg_order_ids={"BUY-A","SELL-EXIT"}))
        for field in ("sl_order_id","tp_order_id","pending_oco","cancel_uncertain",
                      "uncommitted_broker_sell_trades","persistence_uncertain",
                      "external_unlinked_sell_observed","protected_qty"):
            with self.subTest(field=field):
                self.assertFalse(self.permitted(record=dict(self.a,**{field:1})))
    def test_sell_buy_replacement_and_cross_exchange_never_qualify(self):
        for side in ("SELL","BUY"):
            self.assertFalse(self.permitted(current_trades=[dict(securityId="11915",transactionType=side)]))
        self.assertFalse(self.permitted(current_trades=[dict(transactionType="SELL")]))
        self.assertFalse(self.permitted(current_trades=[dict(securityId="BSE",isin="INE528G01035",
                      exchangeSegment="BSE_EQ",transactionType="SELL")],
                      current_holdings=[dict(securityId="11915",totalQty=2,isin="INE528G01035")]))
        self.assertFalse(self.permitted(current_trades=[dict(securityId="BSE",exchangeSegment="BSE_EQ",transactionType="SELL")]))
    def test_failed_books_and_mismatched_quantity_never_qualify(self):
        self.assertFalse(self.permitted(current_trades=None))
        self.assertFalse(self.permitted(current_positions=None))
        self.assertFalse(self.permitted(current_holdings=None))
        self.assertFalse(self.permitted(current_holdings=[]))
        for qty in (1,3):
            self.assertFalse(self.permitted(current_holdings=[dict(securityId="11915",totalQty=qty)]))
        self.assertFalse(self.permitted(current_holdings=[dict(securityId="11915",totalQty=2,dhanClientId="OTHER")]))
        self.assertFalse(self.permitted(current_positions=[dict(securityId="11915",exchangeSegment="NSE_EQ",productType="CNC",netQty=-1)]))
    def test_stale_buy_or_invalid_verified_fills_never_qualify(self):
        self.db.owner_lot_basis=lambda *a:(NOW,[dict(traded_quantity=1)])
        self.assertFalse(self.permitted())
        self.db.owner_lot_basis=lambda *a:(OLD,[dict(traded_quantity=2)])
        self.assertFalse(self.permitted())
        self.db.owner_lot_basis=lambda *a:(_ for _ in ()).throw(ValueError("SELL conflict"))
        self.assertFalse(self.permitted())
    def test_missing_ack_or_historical_proof_never_qualifies(self):
        self.db.is_manual_risk_acknowledged=lambda *a,**kw:False
        self.assertFalse(self.permitted())
        self.db.is_manual_risk_acknowledged=lambda *a,**kw:True
        self.assertFalse(self.permitted(gate_books={"verified":{}},record=dict(self.a,owner_history_day="2026-11-21")))
    def test_other_contract_is_not_exempt(self):
        self.assertFalse(self.permitted(record=dict(self.a,position_state="ENTRY")))
        self.assertFalse(self.permitted(record=dict(self.a,product_type="INTRADAY")))
        self.assertFalse(self.permitted(record=dict(self.a,exit_filled_qty=1)))
        self.assertFalse(self.permitted(account_id=""))

if __name__ == "__main__":
    unittest.main()
