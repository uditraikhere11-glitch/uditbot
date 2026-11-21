"""Evidence-based CNC/MTF signed day position tests on the existing gate helper.

These fixtures exercise the REAL _signed_cnc_delivery_sell_reconciles()
helper—not the new diagnostic. No Dhan calls, order writes, or OCO mutations.
"""
from datetime import datetime
import unittest

from src.engine.market import IST
from src.engine.unified_exit_reconciliation import (
    _signed_cnc_delivery_sell_reconciles as reconcile_signed,
)


ACCOUNT="ACC"
KEY=("SEC","NSE_EQ","CNC")
DAY={"dhanClientId":ACCOUNT,"securityId":"SEC","exchangeSegment":"NSE_EQ",
     "productType":"CNC","netQty":-2}
HOLD={"dhanClientId":ACCOUNT,"securityId":"SEC","totalQty":3,"dpQty":3,
      "t1Qty":0,"availableQty":3}
TODAY=datetime.now(IST).timestamp()


def run(*,position=None,holdings=None,positions=None,projected=1,trades=None,
        account=ACCOUNT,key=KEY):
    if positions is None:
        positions=[DAY if position is None else position]
    if holdings is None:
        holdings=[HOLD]
    if trades is None:
        trades={"E1":{"qty":2,"side":"SELL","from_today":True,"when":TODAY}}
    return reconcile_signed(positions=positions,holdings=holdings,key=key,
        account=account,projected=projected,broker_evidence=trades)


class TestSignedDeliveryCnc(unittest.TestCase):
    def test_exact_signed_sale_and_remaining_holding(self):
        self.assertTrue(run())

    def test_fully_flat_after_verified_delivery_sale(self):
        self.assertTrue(run(holdings=[{**HOLD,"totalQty":2,"dpQty":2}],
                            projected=0))

    def test_missing_holdings_row_is_not_verified_zero(self):
        self.assertFalse(run(holdings=[],projected=0))

    def test_missing_holdings_read_never_establishes_exposure(self):
        self.assertFalse(run(holdings=[{**HOLD,"securityId":"OTHER"}],
                             projected=0))

    def test_conflicting_day_position_is_not_verified(self):
        self.assertFalse(run(position={**DAY,"netQty":-1}))

    def test_duplicate_contract_positions_are_ambiguous(self):
        self.assertFalse(run(positions=[DAY,DAY]))

    def test_malformed_day_net_quantity_blocks(self):
        for val in (-2.5,"-2.5",None,True,"bad"):
            with self.subTest(val=val):
                self.assertFalse(run(position={**DAY,"netQty":val}))

    def test_incomplete_sell_execution_fails(self):
        self.assertFalse(run(trades={}))

    def test_wrong_trade_direction_fails(self):
        self.assertFalse(run(trades={"E1":{"qty":2,"side":"BUY",
                     "from_today":True,"when":TODAY}}))

    def test_sell_exceeds_settled_holdings(self):
        self.assertFalse(run(holdings=[{**HOLD,"totalQty":1,"dpQty":1}],
                             projected=0))

    def test_projected_quantity_mismatch(self):
        self.assertFalse(run(projected=2))

    def test_wrong_account_holding_blocks(self):
        self.assertFalse(run(holdings=[{**HOLD,"dhanClientId":"OTHER"}]))

    def test_competing_same_day_buy_netting(self):
        trades={"sell":{"qty":3,"side":"SELL","from_today":True,"when":TODAY},
                "buy":{"qty":1,"side":"BUY","from_today":True,"when":TODAY}}
        self.assertTrue(run(trades=trades))

    def test_wrong_day_evidence_blocks(self):
        trades={"E1":{"qty":2,"side":"SELL","from_today":True,
                      "when":TODAY-86400}}
        self.assertFalse(run(trades=trades))
