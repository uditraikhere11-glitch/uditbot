"""F&O-only historical Telegram replay and conflict-safety regression tests.

Never calls the broker, trading pipeline, journal, network, or Gemini.
Equity parsing code is deliberately not modified by this feature.
"""
import unittest

from src.models import SignalAction
from src.parsers.brain import parse_signal_brain
from src.parsers.brain_validation import DecisionReasonCode


# Real historical formats, including provider SL/target/holding commentary.
# BRAIN does not use these provider levels to decide if a buy entry exists.
STOCK_CASES = [
    ("TMPV", "#TMPV 280CE @8.5-9\n\nSl-5\n\nTarget-16,20\n\n#Positional",
     "TMPV", 280, "CE", None, 8.5, 9),
    ("KOTAKBANK", "#Positional ,Ram DHS - PREMIUM:\nKotakbank SEP 400CE@7-7.5\n\nSL-3.8\n\nTarget-16,20\n\n3 weeks Holding\n\nslow moving so allocate accordingly",
     "KOTAKBANK", 400, "CE", "SEP", 7, 7.5),
    ("RELIANCE", "#Reliance 1320CE SEP @31-33\n\nSL-20\n\nTarget-54,60+",
     "RELIANCE", 1320, "CE", "SEP", 31, 33),
    ("COCHINSHIPYARD", "#Cochinshipyard 1600CE SEP @19-21\n\nSL-5\n\nTarget-44,80+\n\nKeep in mind its Hero/Zero worst case SL-5",
     "COCHINSHIPYARD", 1600, "CE", "SEP", 19, 21),
    ("JIOFIN", "#JIOFIN 240CE@4.2-4.7\n\nSL-2\n\ntarget-10\n\n3 weeks Holding",
     "JIOFIN", 240, "CE", None, 4.2, 4.7),
    ("CDSL", "CDSL 1400CE OCT @33-35\n\nSL_10\n\nTarget-100+\n\n3 weeks holding\n\nGo with limited qty if any chance we may need to avg",
     "CDSL", 1400, "CE", "OCT", 33, 35),
    ("LT", "#LT 3900PE @80\n\nSL-55\n\nTarget-140",
     "LT", 3900, "PE", None, 80, 80),
    ("POLYCAB", "#POLYCAB 8000 PE OCT @90-105\n\nSL-30\n\nTarget-500,1000\n\nBigger Targets you may need to hold till month end so plan accordingly with appropriate qty\n\npresent low liquidity in options so add slowly",
     "POLYCAB", 8000, "PE", "OCT", 90, 105),
    ("HINDZINC", "#HINDZINC 590CE @12-13\n\nTargets & SL later\n\nLimited qty\n\n#Positional",
     "HINDZINC", 590, "CE", None, 12, 13),
    ("HDFCLIFE", "#HDFCLIFE 550CE @10.5-12\n\nSL-6.8\n\nTARGET-24,30\n\n#Positional",
     "HDFCLIFE", 550, "CE", None, 10.5, 12),
    ("HAL", "#HAL 4750CE @88-91\n\nSL-48\n\nTarget-145,240\n\n#Positional",
     "HAL", 4750, "CE", None, 88, 91),
]

INDEX_CASES = [
    ("NIFTY", "Nifty 24200CE SEP @360-370\n\nSL-290\n\nTarget-595\n\n#Positional with limited qty"),
    ("SENSEX", "Sensex 74500CE@ 450-460\n\nSL-360\n\nTarget-820,1100\n\na bit risky so add limited qty\n\nPositional"),
]


class TestFnoTradeLineSafety(unittest.TestCase):
    @staticmethod
    def parse(message):
        return parse_signal_brain(
            text=message,
            channel_id=0,
            message_id=0,
            msg_timestamp=995.0,
            current_time=1000.0,
            journal_db=None,
            enable_ai_fallback=False,
        )

    def test_all_11_historical_stock_options_accepted(self):
        for name, msg, sym, strike, side, month, pmin, pmax in STOCK_CASES:
            with self.subTest(name=name):
                result = self.parse(msg)
                self.assertEqual(result.action, SignalAction.BUY_ENTRY, result.reason)
                self.assertEqual(result.source, "REGEX")
                self.assertEqual(result.reason_code, DecisionReasonCode.OK.value)
                sig = result.fno_signal
                self.assertIsNotNone(sig)
                self.assertEqual((sig.symbol, sig.strike, sig.option_type),
                                 (sym, strike, side))
                self.assertEqual(sig.expiry_month, month)
                self.assertAlmostEqual(sig.entry_min, pmin)
                self.assertAlmostEqual(sig.entry_max, pmax)

    def test_both_index_options_rejected_with_correct_reason(self):
        for name, message in INDEX_CASES:
            with self.subTest(name=name):
                result = self.parse(message)
                self.assertEqual(result.action, SignalAction.BLOCKED)
                self.assertEqual(result.reason_code, DecisionReasonCode.INDEX_REJECTED.value)

    def test_metadata_before_and_after_trade_line_is_commentary(self):
        examples = [
            "#Positional\n#HAL 4750CE @88-91\nLimited qty",
            "#HAL 4750CE @88-91\nKeep in mind this is low liquidity, add slowly",
            "#HAL 4750CE @88-91\nTargets & SL later\n#Positional",
            "Ram DHS - PREMIUM:\n#HAL 4750CE @88-91\n3 weeks holding",
        ]
        for message in examples:
            with self.subTest(message=message):
                self.assertEqual(self.parse(message).action, SignalAction.BUY_ENTRY)

    def test_true_multiple_contracts_still_blocked(self):
        examples = [
            "#HAL 4750CE @88-91\n#SBIN 800PE @20",
            "#HAL4750CE @88-91 and #SBIN800PE @20",
            "#HAL 4750CE @88-91 and #HAL 4800CE @30",
            "#HAL 4750CE/PE @88-91",
            "#HAL 4750/4800 CE @88-91",
            "BUY SBIN? RELIANCE 800 PE @100",
            "#SBIN RELIANCE 800 PE @100",
            "#HAL 4750CE @88-91\nBUY #SBIN @800",
        ]
        for message in examples:
            with self.subTest(message=message):
                result = self.parse(message)
                self.assertEqual(result.action, SignalAction.BLOCKED, result.reason)
                self.assertEqual(result.reason_code, DecisionReasonCode.AMBIGUOUS_CONTRACT.value)

    def test_cancellations_and_negation_still_blocked(self):
        examples = [
            "#HAL 4750CE @88-91\nCALL CANCELLED",
            "#HAL 4750CE @88-91\nOrder canceled",
            "#HAL 4750CE @88-91\nDO NOT BUY - CALL CANCELLED",
            "#HAL 4750CE @88-91\nIgnore this signal",
            "#HAL 4750CE @88-91\nSkip this entry",
        ]
        for message in examples:
            with self.subTest(message=message):
                result = self.parse(message)
                self.assertEqual(result.action, SignalAction.BLOCKED, result.reason)

    def test_no_entry_or_invalid_entry_cannot_buy(self):
        examples = [
            "#HAL 4750CE\nSL-48\nTarget-145,240",
            "#HAL 4750CE @-88",
            "#HAL 4750CE @88-abc",
            "#HAL 4750CE @0",
        ]
        for message in examples:
            with self.subTest(message=message):
                self.assertNotEqual(self.parse(message).action, SignalAction.BUY_ENTRY)


if __name__ == "__main__":
    unittest.main()
