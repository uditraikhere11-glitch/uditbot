import unittest

from src.engine.holding_quantity import observed_open_quantity


class ShortExposureTest(unittest.TestCase):
    def test_short_position_cannot_confirm_projected_flat_balance(self):
        for product in ("INTRADAY", "MARGIN"):
            with self.subTest(product=product):
                contract = dict(securityId="123", exchangeSegment="NSE_EQ",
                                productType=product, dhanClientId="account")
                for net, expected in ((0, 0), (1, 1), (-1, None)):
                    observed, source = observed_open_quantity(
                        [dict(contract, netQty=net)], None, security_id="123",
                        segment="NSE_EQ", product=product, account_id="account",
                    )
                    self.assertEqual(observed, expected)
                    self.assertEqual(source, "NEGATIVE_NET_POSITION_REVIEW"
                                     if net < 0 else "VERIFIED_POSITIONS")
