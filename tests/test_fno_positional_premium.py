"""Fail-closed tests for immediate post-contract unanchored F&O premiums.

The test set includes the observed RAM DHS format plus permitted human spacing
mistakes and dangerous ambiguities. No trading or network calls.
"""
import unittest

from src.models import SignalAction
from src.parsers.brain import parse_signal_brain
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    extract_fno_entry_bounds,
)
from src.parsers.fno_parser import parse_fno_regex_detailed


class TestUnanchoredFnoPremiums(unittest.TestCase):
    def _fno(self, message):
        return parse_fno_regex_detailed(
            message, msg_timestamp=995.0, current_time=1000.0
        )

    def test_actual_ram_dhs_message_is_parsed_with_original_range(self):
        message = "#HAL 5200CE 90-100\n\nSL-55\n\nTarget-200,290+"
        sig, reason, action, code, status, evidence = self._fno(message)
        self.assertIsNotNone(sig, reason)
        self.assertEqual(action, SignalAction.BUY_ENTRY)
        self.assertEqual(sig.symbol, "HAL")
        self.assertEqual(sig.strike, 5200)
        self.assertEqual(sig.option_type, "CE")
        self.assertEqual((sig.entry_min, sig.entry_max), (90, 100))
        self.assertEqual(sig.stop_loss, 55)
        self.assertEqual(sig.targets, [200, 290])
        self.assertEqual(code, DecisionReasonCode.OK.value)
        self.assertEqual(status, PriceStatus.VALID)
        self.assertEqual((evidence["entry_min"], evidence["entry_max"]), (90, 100))
        self.assertEqual(
            message[evidence["price_span_offsets"][0]:evidence["price_span_offsets"][1]].strip(),
            "90-100",
        )

    def test_safe_spacing_and_separator_variants(self):
        examples = [
            "#HAL 5200CE 90 - 100",
            "#HAL 5200CE 90  -  100",
            "#HAL 5200CE  90- 100 ",
            "#HAL 5200CE:90-100",
            "#HAL 5200CE = 90-100",
            "#HAL 5200CE 90~100",
            "#HAL 5200CE 90 to 100",
            "#HAL 5200CE 90to100",
            "#HAL 5200CE 100-90",
            "#HAL 5200CE 90\u2013100",
            "#HAL 5200CE\n90-100",
            "#HAL 5200CE 90.5 - 100.50",
        ]
        for msg in examples:
            with self.subTest(msg=msg):
                sig, why, action, *_ = self._fno(msg)
                self.assertIsNotNone(sig, why)
                self.assertEqual(action, SignalAction.BUY_ENTRY)
                if "90.5" in msg:
                    self.assertEqual((sig.entry_min, sig.entry_max), (90.5, 100.5))
                else:
                    self.assertEqual((sig.entry_min, sig.entry_max), (90, 100))

    def test_single_price_is_only_used_as_a_single_price(self):
        for msg in ("#HAL 5200CE 90", "#HAL 5200CE : 90.50"):
            with self.subTest(msg=msg):
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNotNone(sig, reason)
                self.assertEqual(action, SignalAction.BUY_ENTRY)
                self.assertEqual(sig.entry_min, sig.entry_max)

    def test_explicit_price_anchor_remains_unchanged(self):
        for msg in ("#HAL 5200CE @90-100", "BUY HAL 5200CE at 90-100"):
            with self.subTest(msg=msg):
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNotNone(sig, reason)
                self.assertEqual((sig.entry_min, sig.entry_max), (90, 100))

    def test_ambiguous_or_invalid_premium_never_becomes_a_trade(self):
        invalid = [
            "-90", "+90", "- 90", "0", "90-0", "90-abc", "90-", "90 100",
            "90,100", "90/100", "90&100", "90|100", "90_100",
            "90 and 100", "90 - 100 - 110", "90-1000", "90-100 + 105",
            "90.5.6", "90-NaN", "90  to  ", ": -90",
        ]
        for tail in invalid:
            with self.subTest(tail=tail):
                msg = "#HAL 5200CE " + tail
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNone(sig, (tail, reason))
                self.assertNotEqual(action, SignalAction.BUY_ENTRY)

    def test_sl_and_targets_never_become_premium(self):
        messages = (
            "#HAL 5200CE\nSL-55\nTarget-200,290+",
            "#HAL 5200CE SL @55 Target @200",
            "#HAL 5200CE Target-200,290+",
        )
        for msg in messages:
            with self.subTest(msg=msg):
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNone(sig)
                self.assertNotEqual(action, SignalAction.BUY_ENTRY)

    def test_mixed_contracts_and_negations_remain_blocked(self):
        examples = (
            "#HAL 5200CE 90-100 and #TCS 4000PE 40-50",
            "Do not buy #HAL 5200CE 90-100",
            "Watching #HAL 5200CE 90-100",
            "Yesterday #HAL 5200CE 90-100",
            "#NIFTY 24500CE 90-100",
        )
        for msg in examples:
            with self.subTest(msg=msg):
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNone(sig)
                self.assertNotEqual(action, SignalAction.BUY_ENTRY)

    def test_fno_extractor_keeps_min_and_max_and_offsets(self):
        message = "#HAL 5200CE 90 - 100\nSL-55\nTarget-200,290+"
        hdr = message.index("CE") + len("CE")
        result = extract_fno_entry_bounds(message, after_pos=hdr)
        self.assertEqual(result.status, PriceStatus.VALID)
        self.assertEqual((result.entry_min, result.entry_max), (90, 100))
        self.assertEqual(result.reason, "Validated unanchored post-contract premium")
        self.assertEqual(message[slice(*result.span_offsets)].strip(), "90 - 100")

    def test_existing_post_contract_expiry_is_not_guessed_as_price(self):
        for msg in (
            "#HAL 5200CE OCT SL-55 Target-200",
            "#HAL 5200CE 28 OCT 90-100",
        ):
            with self.subTest(msg=msg):
                sig, reason, action, *_ = self._fno(msg)
                self.assertIsNone(sig)
                self.assertNotEqual(action, SignalAction.BUY_ENTRY)

    def test_brain_route_is_deterministic_for_valid_variant(self):
        msg = "#HAL 5200CE 90 - 100\nSL-55\nTarget-200,290+"
        decision = parse_signal_brain(
            msg, current_time=1000.0, msg_timestamp=995.0,
            enable_ai_fallback=False,
        )
        self.assertEqual(decision.route, "OPTION")
        self.assertEqual(decision.action, SignalAction.BUY_ENTRY, decision.reason)
        self.assertEqual(decision.source, "REGEX")
        self.assertEqual(
            (decision.fno_signal.entry_min, decision.fno_signal.entry_max),
            (90, 100),
        )

    def test_brain_blocks_malformed_variant_before_ai(self):
        msg = "#HAL 5200CE -90\nSL-55\nTarget-200,290+"
        decision = parse_signal_brain(
            msg, current_time=1000.0, msg_timestamp=995.0,
            enable_ai_fallback=False,
        )
        self.assertNotEqual(decision.action, SignalAction.BUY_ENTRY)
        self.assertEqual(decision.source, "FILTER")


if __name__ == "__main__":
    unittest.main()
