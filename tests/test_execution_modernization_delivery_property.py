"""Exhaustive signed CNC/MTF delivery-day evidence grid (offline).

A negative day position NEVER proves flat just because the day's net is
negative. The verified helper must agree with *explicit* settled holdings,
the precise number of exchange trades, the account and lifecycle projection.
No Dhan request or real trading journal is touched by this matrix.
"""
from datetime import datetime
import unittest

from src.engine.market import IST
from src.engine.unified_exit_reconciliation import (
    _signed_cnc_delivery_sell_reconciles,
)


ACCOUNT="SYNTHETIC"
KEY=("SEC","NSE_EQ","CNC")
NOW=datetime.now(IST).timestamp()


def check(sold,hold,projected,*,holding_present=True,
          account=ACCOUNT,product="CNC"):
    row={"securityId":"SEC","exchangeSegment":"NSE_EQ",
         "productType":product,"netQty":-sold,
         "dhanClientId":ACCOUNT}
    h={"dhanClientId":ACCOUNT,"securityId":"SEC",
       "totalQty":hold,"dpQty":hold,"t1Qty":0}
    evidence={"EX-SYN":{"qty":sold,"side":"SELL",
                        "from_today":True,"when":NOW}}
    return _signed_cnc_delivery_sell_reconciles(
        positions=[row],holdings=[h] if holding_present else [],
        key=("SEC","NSE_EQ",product),account=account,
        projected=projected,broker_evidence=evidence,
    )


class TestSignedCNCMTFPropertyGrid(unittest.TestCase):
    def test_over_5000_concrete_signed_day_book_combinations(self):
        tested=0
        for sold in range(1,13):
            for settled in range(0,21):
                for projected in range(0,21):
                    expected=settled>=sold and settled-sold==projected
                    self.assertEqual(
                        check(sold,settled,projected),expected,
                        (sold,settled,projected)
                    )
                    tested+=1
        self.assertEqual(tested,5292)

    def test_absent_holdings_row_never_proves_flat(self):
        for sold in range(1,13):
            self.assertFalse(check(sold,sold,0,holding_present=False))

    def test_oversold_prior_holding_never_proves_zero(self):
        for sold in range(1,13):
            for prior in range(sold):
                self.assertFalse(check(sold,prior,0))

    def test_wrong_account_never_proves_flat(self):
        for sold in range(1,13):
            self.assertFalse(check(sold,sold,0,account="DIFFERENT_ACCOUNT"))

    def test_mtf_requires_explicit_mtf_identity(self):
        for sold in range(1,13):
            self.assertTrue(check(sold,sold+2,2,product="MTF"))
            self.assertFalse(check(sold,sold+2,0,product="MTF"))

    def test_no_non_delivery_instrument_is_treated_as_delivery_sale(self):
        for sold in range(1,13):
            self.assertFalse(check(sold,sold,0,product="MARGIN"))
