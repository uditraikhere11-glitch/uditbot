"""Dhan order stream is strictly observational; no network and no trade mutation."""
import unittest
from src.engine.dhan_order_update_shadow import normalize_dhan_order_update, DhanUpdateRejected
from src.engine.shadow_execution_events import OrderProjection


RAW={"Type":"order_alert","Data":{
    "ClientId":"ACC","OrderNo":"OID","SecurityId":"SEC",
    "Exchange":"NSE","Segment":"E","Product":"C","ProductName":"CNC",
    "Status":"PENDING","Quantity":5,"TradedQty":2,"RemainingQuantity":3,
    "LastUpdatedTime":"2026-11-21 10:00:00",
}}


def update(**kw):
    data=dict(RAW["Data"]);data.update(kw)
    return {"Type":"order_alert","Data":data}


class TestDhanWebsocketNormalization(unittest.TestCase):
    def test_cumulative_status_not_a_trade(self):
        x=normalize_dhan_order_update(RAW,expected_account_id="ACC")
        self.assertEqual(x.kind,"STATUS")
        self.assertIsNone(x.exchange_trade_id)
        self.assertIsNone(x.traded_qty)
        self.assertEqual(x.cumulative_filled_qty,2)
        self.assertEqual(x.exchange_segment,"NSE_EQ")

    def test_duplicate_order_snapshot_is_idempotent(self):
        p=OrderProjection("ACC","OID","SEC","NSE_EQ","CNC",5)
        x=normalize_dhan_order_update(RAW,expected_account_id="ACC")
        self.assertEqual(p.apply(x),"APPLIED")
        self.assertEqual(p.apply(x),"DUPLICATE")
        self.assertEqual(p.cumulative_filled_qty,2)

    def test_changed_cumulative_is_new_snapshot(self):
        a=normalize_dhan_order_update(RAW,expected_account_id="ACC")
        b=normalize_dhan_order_update(update(TradedQty=3,RemainingQuantity=2),
                                      expected_account_id="ACC")
        self.assertNotEqual(a.event_id,b.event_id)

    def test_reject_wrong_account(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(RAW,expected_account_id="OTHER")

    def test_reject_unknown_type(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update({"Type":"other","Data":RAW["Data"]},expected_account_id="ACC")

    def test_reject_missing_identity(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(OrderNo=""),expected_account_id="ACC")

    def test_reject_unsupported_segment(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(Exchange="INVALID"),expected_account_id="ACC")

    def test_reject_unsupported_product(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(Product="X"),expected_account_id="ACC")

    def test_reject_product_disagreement(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(ProductName="MTF"),expected_account_id="ACC")

    def test_reject_oversized_cumulative(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(TradedQty=6),expected_account_id="ACC")

    def test_reject_nonintegral_and_boolean_quantities(self):
        for v in (True,2.5,-1,"not a number"):
            with self.subTest(v=v),self.assertRaises(DhanUpdateRejected):
                normalize_dhan_order_update(update(TradedQty=v),expected_account_id="ACC")

    def test_reject_contradictory_remaining(self):
        with self.assertRaises(DhanUpdateRejected):
            normalize_dhan_order_update(update(RemainingQuantity=4),expected_account_id="ACC")

    def test_status_case_normalization(self):
        x=normalize_dhan_order_update(update(Status="Cancelled",RemainingQuantity=0),
                                      expected_account_id="ACC")
        self.assertEqual(x.status,"CANCELLED")
