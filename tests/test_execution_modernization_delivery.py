"""Synthetic, offline negative delivery-sale triage tests: fail-closed by design."""
import unittest
from src.engine.delivery_sell_triage import triage_negative_delivery_position as triage

P={"dhanClientId":"ACC","securityId":"S1","exchangeSegment":"NSE_EQ","productType":"CNC","netQty":-2}
H={"dhanClientId":"ACC","securityId":"S1","totalQty":3,"dpQty":3,"t1Qty":0}
T={"dhanClientId":"ACC","securityId":"S1","exchangeSegment":"NSE_EQ","productType":"CNC",
   "exchangeTradeId":"T1","orderId":"O1","transactionType":"SELL","tradedQuantity":2,
   "exchangeTime":"2026-11-20 09:30:00"}


def go(position=P,trades=None,holdings=None,books_complete=True):
    return triage(position,account_id="ACC",market_date="2026-11-20",
                  trades=[T] if trades is None else trades,
                  holdings=[H] if holdings is None else holdings,
                  books_complete=books_complete)

class TestNegativeDeliveryTriage(unittest.TestCase):
    def test_matched_sell_still_never_authorizes_lifecycle(self):
        x=go()
        self.assertEqual(x.classification,"CORROBORATED_DELIVERY_SELL_REQUIRES_ALLOCATION")
        self.assertFalse(x.allow_entry_gate_open)
        self.assertFalse(x.allow_position_close)
        self.assertFalse(x.allow_protection_cancel)

    def test_incomplete_books_fail_closed(self):
        self.assertEqual(go(books_complete=False).classification,"UNVERIFIED")

    def test_missing_sell_is_not_verified(self):
        self.assertEqual(go(trades=[]).classification,"UNVERIFIED")

    def test_trade_net_mismatch(self):
        x=go(trades=[{**T,"tradedQuantity":1}])
        self.assertEqual(x.classification,"NET_MISMATCH")

    def test_conflicting_duplicate_trade_ids_fail_closed(self):
        x=go(trades=[T,{**T,"tradedQuantity":3}])
        self.assertEqual(x.classification,"UNVERIFIED")

    def test_repeated_identical_trade_counted_once(self):
        self.assertEqual(go(trades=[T,T]).unique_executions,1)

    def test_wrong_account_fails_closed(self):
        self.assertEqual(go(position={**P,"dhanClientId":"ANOTHER"}).classification,"UNVERIFIED")

    def test_unknown_products_do_not_qualify(self):
        self.assertEqual(go(position={**P,"productType":"INTRADAY"}).classification,"UNVERIFIED")

    def test_wrong_session_fails_closed(self):
        self.assertEqual(go(trades=[{**T,"exchangeTime":"2026-11-19 09:30:00"}]).classification,"UNVERIFIED")

    def test_malformed_quantity_and_holdings_fail_closed(self):
        self.assertEqual(go(position={**P,"netQty":-2.5}).classification,"UNVERIFIED")
        self.assertEqual(go(holdings=[{**H,"totalQty":-2}]).classification,"UNVERIFIED")

    def test_positive_net_qty_no_triage(self):
        self.assertEqual(go(position={**P,"netQty":2}).classification,"NOT_NEGATIVE")
