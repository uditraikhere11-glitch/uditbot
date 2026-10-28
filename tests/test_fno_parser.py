"""
Unit tests for Stock Option (F&O) signal parser and gatekeepers.
"""

import unittest
from src.parsers.fno_parser import parse_fno_regex
from src.models import SignalAction


class TestFnOParser(unittest.TestCase):
    def test_valid_option_range_and_targets(self):
        msg = "#POLYCAB 8000 PE OCT @90-105\nSL-30\nTarget-500,1000"
        sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "POLYCAB")
        self.assertEqual(sig.strike, 8000.0)
        self.assertEqual(sig.option_type, "PE")
        self.assertEqual(sig.expiry_month, "OCT")
        self.assertEqual(sig.entry_min, 90.0)
        self.assertEqual(sig.entry_max, 105.0)
        self.assertEqual(sig.stop_loss, 30.0)
        self.assertEqual(sig.targets, [500.0, 1000.0])
        self.assertEqual(action, SignalAction.BUY_ENTRY)

    def test_single_entry_price(self):
        msg = "Buy TATACHEM 1100 CE @ 25"
        sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.symbol, "TATACHEM")
        self.assertEqual(sig.strike, 1100.0)
        self.assertEqual(sig.option_type, "CE")
        self.assertEqual(sig.entry_min, 25.0)
        self.assertEqual(sig.entry_max, 25.0)

    def test_call_put_mapping(self):
        msg = "INFY 1500 CALL OCT @ 32.50"
        sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.option_type, "CE")

        msg_put = "INFY 1450 PUT OCT @ 20"
        sig2, reason2, action2 = parse_fno_regex(msg_put, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig2)
        self.assertEqual(sig2.option_type, "PE")

    def test_index_option_rejection(self):
        index_signals = [
            "Buy #NIFTY 24500 CE @ 120",
            "#BANKNIFTY 51000 PE OCT @ 300",
            "FINNIFTY 23000 CE @ 50",
            "#SENSEX 80000 PE @ 200",
        ]
        for msg in index_signals:
            sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
            self.assertIsNone(sig, f"Index option should be blocked: {msg}")
            self.assertIn("Index option rejected", reason)
            self.assertEqual(action, SignalAction.BLOCKED)

    def test_exit_wording_classification(self):
        exits = [
            "Book profit in POLYCAB 8000 PE",
            "Exit TATACHEM 1100 CE immediately",
            "Square off INFY 1500 CE",
            "SL hit in RELIANCE 2500 CE",
        ]
        for msg in exits:
            sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
            self.assertIsNone(sig)
            self.assertEqual(action, SignalAction.EXIT_SIGNAL)

    def test_missing_price_rejection(self):
        msg = "Buy SBIN 850 CE OCT"
        sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("No option trade pattern matched", reason)

    def test_message_age_rejection(self):
        msg = "#POLYCAB 8000 PE OCT @90-105"
        sig, reason, action = parse_fno_regex(msg, current_time=100.0, msg_timestamp=30.0, max_age_sec=60.0)
        self.assertIsNone(sig)
        self.assertIn("Message too old", reason)


if __name__ == "__main__":
    unittest.main()
