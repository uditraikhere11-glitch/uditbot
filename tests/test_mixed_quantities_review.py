import unittest

from src.engine.mixed_cnc_evidence import MixedCNCUnverified
from tests.test_pr132_mixed_delivery_and_status import BrokerFixture, FakeDB


class MixedQuantityEvidenceTests(unittest.TestCase):
    def test_overlapping_or_inconsistent_position_components(self):
        for components in ({"carryForwardBuyQty": 2}, {"carryForwardSellQty": 1},
                           {"dayBuyQty": 27, "daySellQty": 0}, {"dayBuyQty": 28}):
            with self.subTest(components=components):
                fixture = BrokerFixture(FakeDB())
                fixture.positions[0].update(components)
                with self.assertRaises(MixedCNCUnverified):
                    fixture.proof()

    def test_cross_exchange_sibling_requires_verified_zero(self):
        for qty in (None, False, 2, "0"):
            with self.subTest(qty=qty):
                fixture = BrokerFixture(FakeDB())
                sibling = dict(fixture.positions[0], exchangeSegment="BSE_EQ", netQty=qty)
                if qty is None:
                    del sibling["netQty"]
                fixture.positions.append(sibling)
                if qty == "0":
                    self.assertEqual(fixture.proof().total_qty, 30)
                else:
                    with self.assertRaises(MixedCNCUnverified):
                        fixture.proof()

    def test_fractional_broker_fill_cannot_confirm_integer_lot(self):
        fixture = BrokerFixture(FakeDB())
        fixture.trades[0]["tradedQuantity"] = 16.9
        with self.assertRaises(MixedCNCUnverified):
            fixture.proof()

    def test_disjoint_matching_components_preserve_verified_split(self):
        fixture = BrokerFixture(FakeDB())
        fixture.positions[0].update(carryForwardBuyQty="0", dayBuyQty="28", daySellQty=0)
        self.assertEqual(fixture.proof().total_qty, 30)
