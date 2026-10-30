"""
Unit tests for STEP 20: Additive regex aliases and regression-proven stock signals.
Verifies new alias patterns without regressing legacy parser safety or index rejection.
"""

import unittest
from src.parsers.equity_parser import parse_equity_regex
from src.parsers.fno_parser import parse_fno_regex
from src.models import SignalAction


class TestStep20RegexAliases(unittest.TestCase):
    def test_fno_alias_with_buy_keyword(self):
        """FNO alias matching '#TMPV 280CE buy 8.5-9'."""
        sig, reason, action = parse_fno_regex("#TMPV 280CE buy 8.5-9")
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "TMPV")
        self.assertEqual(sig.strike, 280.0)
        self.assertEqual(sig.option_type, "CE")
        self.assertEqual(sig.entry_min, 8.5)
        self.assertEqual(sig.entry_max, 9.0)

    def test_fno_alias_with_at_keyword(self):
        """FNO alias matching '#INFY 1500CE at 25.5'."""
        sig, reason, action = parse_fno_regex("#INFY 1500CE at 25.5 Target 30")
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "INFY")
        self.assertEqual(sig.strike, 1500.0)
        self.assertEqual(sig.entry_min, 25.5)
        self.assertEqual(sig.entry_max, 25.5)
        self.assertEqual(sig.targets, [30.0])

    def test_equity_alias_with_around_and_above(self):
        """Equity alias matching 'Buy #SBIN around 805' and 'Buy #SBIN above 805'."""
        sig1, msg1 = parse_equity_regex("Buy #SBIN around 805")
        self.assertIsNotNone(sig1)
        self.assertEqual(sig1.symbol, "SBIN")
        self.assertEqual(sig1.stated_price, 805.0)

        sig2, msg2 = parse_equity_regex("Buy #TATAMOTORS above 950")
        self.assertIsNotNone(sig2)
        self.assertEqual(sig2.symbol, "TATAMOTORS")
        self.assertEqual(sig2.stated_price, 950.0)

    def test_index_blocked_in_aliases(self):
        """Index rejection must hold across all aliases."""
        sig, reason, action = parse_fno_regex("#NIFTY 25000CE buy 120")
        self.assertIsNone(sig)
        self.assertEqual(action, SignalAction.BLOCKED)
        self.assertIn("Index option rejected", reason)


if __name__ == "__main__":
    unittest.main()
