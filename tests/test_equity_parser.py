"""
Unit tests for Cash Equity signal parser and gatekeepers.
"""

import unittest
from src.parsers.equity_parser import parse_equity_regex, filter_equity_message
from src.models import SignalAction


class TestEquityParser(unittest.TestCase):
    def test_valid_hashtag_buy_with_price(self):
        msg = "Buy #SBIN @ 805-810 for short term target"  # Note: contains target
        # Let's test a clean buy signal
        clean_msg = "Buy #SBIN @ 808"
        sig, reason = parse_equity_regex(clean_msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "SBIN")
        self.assertEqual(sig.stated_price, 808.0)
        self.assertEqual(sig.action, SignalAction.BUY_ENTRY)

    def test_valid_action_phrase_without_price_cmp(self):
        msg = "Bought in RELIANCE at CMP"
        sig, reason = parse_equity_regex(msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "RELIANCE")
        self.assertEqual(sig.stated_price, 0.0)

    def test_index_blocker(self):
        indices = ["Buy #NIFTY @ 24500", "Bought BANKNIFTY @ 51000", "#SENSEX looking strong buy", "FINNIFTY @ 23000"]
        for msg in indices:
            sig, reason = parse_equity_regex(msg, current_time=100.0, msg_timestamp=95.0)
            self.assertIsNone(sig, f"Should have blocked index message: {msg}")
            self.assertIn("Index trade detected", reason)

    def test_exit_signal_classification(self):
        exits = [
            "Exit #TATAMOTORS now",
            "Book profit in #INFY",
            "Square off #SBIN",
            "SL hit for #RELIANCE",
        ]
        for msg in exits:
            passed, reason, action = filter_equity_message(msg, current_time=100.0, msg_timestamp=95.0)
            self.assertFalse(passed)
            self.assertEqual(action, SignalAction.EXIT_SIGNAL)

    def test_message_age_gatekeeper(self):
        # Age = 100 - 30 = 70s (> 60s max allowed)
        sig, reason = parse_equity_regex("Buy #SBIN @ 808", current_time=100.0, msg_timestamp=30.0, max_age_sec=60.0)
        self.assertIsNone(sig)
        self.assertIn("Message too old", reason)

        # Fresh message (age = 10s <= 60s)
        sig, reason = parse_equity_regex("Buy #SBIN @ 808", current_time=100.0, msg_timestamp=90.0, max_age_sec=60.0)
        self.assertIsNotNone(sig)

    def test_message_too_long(self):
        long_msg = "Buy #SBIN @ 800 " + "analysis "*40
        sig, reason = parse_equity_regex(long_msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("Message too long", reason)

    def test_multiple_stocks_blocked(self):
        multi_msg = "Buy #SBIN and #RELIANCE together"
        sig, reason = parse_equity_regex(multi_msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("Multiple stocks detected", reason)

    def test_option_routing(self):
        opt_msg = "Buy #SBIN 850 CE @ 15"
        sig, reason = parse_equity_regex(opt_msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("routed to F&O", reason)


if __name__ == "__main__":
    unittest.main()
