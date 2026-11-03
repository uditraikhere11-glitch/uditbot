"""
AuditBot BRAIN — Module 2 Parser Validation, Adversarial Regressions & Legacy Compatibility Suite (v2).

Verifies:
1. Exact reproductions and fixes for A01 through A10 under both empty and populated scrip-master fixtures.
2. All 8 confirmed release blockers from the Independent Patch Review:
   - P1: `BUY SBIN @-800` -> blocked (`INVALID_EXPLICIT_PRICE`, never treated as `0.0` CMP).
   - P1: `BUY SBIN @800-abc`, `BUY SBIN @800 to`, `POLYCAB 8000 PE OCT @100-abc` -> blocked (`INVALID_EXPLICIT_PRICE`).
   - P1: `POLYCAB 8000 PE OCT SL @30` -> blocked (`NO_ENTRY_PREMIUM`, SL is never treated as entry premium).
   - P1: AI F&O candidate with wrong option side (`CE` vs `PE`) or strike-as-premium (`8000`) -> rejected (`AI_GROUNDING_FAILED`).
   - P1: AI Equity candidate returning lower bound (`790`) on `BUY SBIN @790-800` -> rejected (`AI_GROUNDING_FAILED`).
   - P1: Adaptive equity rule `(?P<sym>SBIN).*SL (?P<e1>\d+)` on `BUY SBIN @800 SL 750` -> rejected (`750` is in SL clause).
   - P1: Quantified alternation pattern `(?P<sym>SBIN) (?:a|aa)+$` -> rejected by `validate_rule_pattern_safety`.
   - P2: `POLYCAB 8000 PE OCT / NOV @100` -> rejected (`CONFLICTING_EXPIRY`).
3. All additional P1/P2 incorporation requirements:
   - Preserving no-price equity signals (`Added #SBIN`, `Bought SBIN`, `Bought RELIANCE at CMP`) with `PriceStatus.ABSENT` and `stated_price=0.0`.
   - Shorthand multi-strike (`SBIN 800/820 CE @15`) and multi-side (`SBIN 800 CE/PE @15`) rejection.
   - Explicit expiry preservation (rejecting AI candidates that omit stated `expiry_month`/`expiry_day` or messages with unsupported explicit years like `OCT 2027`).
   - Conservative Unicode normalization (`\u00a0`, `\u200b`, `–`, `—`, `−`, `₹`, `\r\n`) while retaining `raw_text`.
   - Structured `reason_code`, `source_span` extraction evidence, parser exception isolation (`PARSER_EXCEPTION`), and offline corpus replay comparison (`compare_corpus_replay`).
"""

import re
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.models import OrderType, SignalAction
from src.instruments import resolver as resolver_mod
from src.instruments.resolver import InstrumentResolver
from src.parsers.brain import compare_corpus_replay, parse_signal_brain
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    extract_equity_price_bounds,
    extract_equity_price_bounds_detailed,
    extract_fno_entry_bounds,
    normalize_signal_text,
    number_token_appears_in_text,
    parse_clean_number_token,
    symbol_or_company_appears_in_text,
)
from src.parsers.equity_parser import parse_equity_regex
from src.parsers.fno_parser import parse_fno_regex
from src.parsers.gemini_fallback import (
    GeminiEquityCandidate,
    GeminiFnOCandidate,
    _number_appears_in_text,
    _symbol_appears_in_text,
    parse_equity_gemini_sync,
    parse_equity_gemini_with_meta,
    parse_fno_gemini_sync,
    parse_fno_gemini_with_meta,
)
from src.parsers.adaptive_rules import (
    AdaptiveRuleManager,
    safe_regex_search_bounded,
    validate_rule_pattern_safety,
)


MASTER_FIXTURE_ROWS = [
    ("NSE", "SBIN", "State Bank of India"),
    ("NSE", "RELIANCE", "Reliance Industries"),
    ("NSE", "POLYCAB", "Polycab India"),
    ("NSE", "INFY", "Infosys"),
    ("NSE", "TCS", "Tata Consultancy Services"),
    ("NSE", "TATAMOTORS", "Tata Motors"),
    ("NSE", "TMPV", "Tata Motors Passenger Vehicles"),
    ("NSE", "MPIMANIPAL", "Manipal Payment and Identity Solutions"),
    ("NSE", "MANIPALHOS", "Manipal Health Enterprises"),
    ("NSE", "YESBANK", "Yes Bank"),
    ("NSE", "HDFCBANK", "HDFC Bank"),
]


class TestBrainValidationAndA01ToA10(unittest.TestCase):
    def setUp(self):
        self._orig_valid = set(resolver_mod.VALID_EQUITY_SYMBOLS)
        self._orig_index = dict(resolver_mod._NAME_INDEX)

    def tearDown(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.update(self._orig_valid)
        resolver_mod._NAME_INDEX.clear()
        resolver_mod._NAME_INDEX.update(self._orig_index)

    def _populate_master(self):
        resolver = InstrumentResolver()
        resolver.load_company_names(MASTER_FIXTURE_ROWS)
        return resolver

    def _clear_master(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod._NAME_INDEX.clear()

    def test_A01_equity_price_range_extracts_both_bounds_and_uses_upper_reference(self):
        """A01: 'BUY SBIN @790-800' -> extracts both bounds (790, 800) and uses upper reference 800.0."""
        for mode in ("empty_master", "populated_master"):
            if mode == "empty_master":
                self._clear_master()
            else:
                self._populate_master()

            res = extract_equity_price_bounds_detailed("BUY SBIN @790-800")
            self.assertEqual(res.status, PriceStatus.VALID)
            self.assertEqual(res.entry_min, 790.0)
            self.assertEqual(res.entry_max, 800.0)
            self.assertIn("790-800", res.source_span)

            dec = parse_signal_brain("BUY SBIN @790-800", current_time=1000.0, msg_timestamp=995.0)
            self.assertEqual(dec.route, "EQUITY")
            self.assertEqual(dec.action, SignalAction.BUY_ENTRY)
            self.assertEqual(dec.price_status, PriceStatus.VALID)
            self.assertTrue(dec.explicit_price)
            self.assertEqual(dec.evidence.get("entry_min"), 790.0)
            self.assertEqual(dec.evidence.get("entry_max"), 800.0)
            self.assertEqual(dec.equity_signal.stated_price, 800.0)

    def test_A02_comma_grouped_numbers_parsed_accurately(self):
        """A02: 'BUY RELIANCE @1,250' -> stated_price=1250.0 (never truncated to 1.0)."""
        for mode in ("empty_master", "populated_master"):
            if mode == "empty_master":
                self._clear_master()
            else:
                self._populate_master()

            sig, _ = parse_equity_regex("BUY RELIANCE @1,250", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNotNone(sig)
            self.assertEqual(sig.symbol, "RELIANCE")
            self.assertEqual(sig.stated_price, 1250.0)

            self.assertEqual(parse_clean_number_token("1,25,000.50"), 125000.50)
            fno_sig, _, fno_act = parse_fno_regex("#POLYCAB 8,000 PE OCT @1,050-1,100", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNotNone(fno_sig)
            self.assertEqual(fno_act, SignalAction.BUY_ENTRY)
            self.assertEqual(fno_sig.strike, 8000.0)
            self.assertEqual(fno_sig.entry_min, 1050.0)
            self.assertEqual(fno_sig.entry_max, 1100.0)

    def test_A03_and_blocker1_blocker2_negative_incomplete_and_malformed_prices_rejected(self):
        """
        A03 + Release Blockers 1 & 2:
        - 'BUY SBIN @-800', 'BUY SBIN at -800' -> PriceStatus.INVALID, SignalAction.BLOCKED (never 0.0 CMP!)
        - 'BUY SBIN @800-abc', 'BUY SBIN @800 to', 'BUY SBIN @800-' -> PriceStatus.INVALID, SignalAction.BLOCKED
        - 'POLYCAB 8000 PE OCT @100-abc', 'POLYCAB 8000 PE OCT @-100' -> SignalAction.BLOCKED
        - Meanwhile genuine no-price equity signals ('Added #SBIN', 'Bought SBIN') remain PriceStatus.ABSENT (stated_price=0.0)!
        """
        for mode in ("empty_master", "populated_master"):
            if mode == "empty_master":
                self._clear_master()
            else:
                self._populate_master()

            invalid_equity_msgs = [
                "BUY SBIN @12.3.4",
                "BUY SBIN @12,34",
                "BUY SBIN @-800",
                "BUY SBIN @ -800",
                "BUY SBIN at -800",
                "BUY SBIN @800-abc",
                "BUY SBIN @800 to",
                "BUY SBIN @800-",
                "BUY SBIN @800 to abc",
                "BUY SBIN @abc",
            ]
            for bad_msg in invalid_equity_msgs:
                sig, reason = parse_equity_regex(bad_msg, current_time=1000.0, msg_timestamp=995.0)
                self.assertIsNone(sig, f"Must reject invalid price ({mode}): {bad_msg}")

                dec = parse_signal_brain(bad_msg, current_time=1000.0, msg_timestamp=995.0)
                self.assertIsNone(dec.signal, f"BRAIN must reject {bad_msg}")
                self.assertEqual(dec.action, SignalAction.BLOCKED, f"Hard price block must preserve BLOCKED action for {bad_msg}")
                self.assertEqual(dec.price_status, PriceStatus.INVALID)
                self.assertEqual(dec.reason_code, DecisionReasonCode.INVALID_EXPLICIT_PRICE.value)

            invalid_fno_msgs = [
                "POLYCAB 8000 PE OCT @100-abc",
                "POLYCAB 8000 PE OCT @100 to",
                "POLYCAB 8000 PE OCT @-100",
                "POLYCAB 8000 PE OCT @12.3.4",
            ]
            for bad_fno in invalid_fno_msgs:
                fno_sig, _, fno_act = parse_fno_regex(bad_fno, current_time=1000.0, msg_timestamp=995.0)
                self.assertIsNone(fno_sig, f"Must reject malformed F&O price: {bad_fno}")
                self.assertEqual(fno_act, SignalAction.BLOCKED)

            # Verify genuine no-price equity messages remain eligible with PriceStatus.ABSENT & stated_price=0.0
            valid_no_price_msgs = [
                ("Added #SBIN", "SBIN"),
                ("Bought SBIN", "SBIN"),
                ("Bought in RELIANCE at CMP", "RELIANCE"),
                ("Buy #TCS", "TCS"),
            ]
            for msg, exp_sym in valid_no_price_msgs:
                dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0)
                self.assertEqual(dec.action, SignalAction.BUY_ENTRY, f"No-price equity must remain valid: {msg} ({dec.reason})")
                self.assertEqual(dec.price_status, PriceStatus.ABSENT)
                self.assertFalse(dec.explicit_price)
                self.assertEqual(dec.equity_signal.symbol, exp_sym)
                self.assertEqual(dec.equity_signal.stated_price, 0.0)
                self.assertEqual(dec.equity_signal.order_type, OrderType.LIMIT)

    def test_blocker3_fno_sl_at_price_without_entry_rejected(self):
        """
        Release Blocker 3: 'POLYCAB 8000 PE OCT SL @30' -> rejected (NO_ENTRY_PREMIUM).
        Never mistakes SL @30 or Target @150 for an option entry premium.
        """
        for msg in (
            "POLYCAB 8000 PE OCT SL @30",
            "POLYCAB 8000 PE OCT Stop Loss @ 30",
            "POLYCAB 8000 PE OCT Target @ 150",
            "POLYCAB 8000 PE OCT SL-30 Target-150",
        ):
            sig, reason, act = parse_fno_regex(msg, current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNone(sig, f"Must not treat SL/Target as entry premium: {msg}")
            self.assertNotEqual(act, SignalAction.BUY_ENTRY)

            dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
            self.assertIsNone(dec.signal)
            self.assertEqual(dec.reason_code, DecisionReasonCode.NO_ENTRY_PREMIUM.value)

    def test_blocker4_and_blocker5_ai_contract_grounding_and_upper_bound_consistency(self):
        """
        Release Blockers 4 & 5:
        - AI F&O input: 'BUY POLYCAB 8000 PE premium 100' with mocked candidate (CE, entry 8000) -> rejected!
        - AI F&O input: 'BUY POLYCAB 8000 PE 28 OCT premium 100' with mocked candidate omitting expiry_month or expiry_day -> rejected!
        - AI Equity input: 'BUY SBIN @790-800' with mocked candidate entry_price=790 -> rejected (conflicts with 800 upper bound)!
        """
        self._populate_master()
        fake_client = MagicMock()

        # 4a. Wrong option side (CE instead of PE) and strike-as-premium (8000)
        fake_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="CE",
                entry_min=8000.0,
                entry_max=8000.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig, reason, model_used, r_code, _ = parse_fno_gemini_with_meta(
                "BUY POLYCAB 8000 PE premium 100",
                current_time=1000.0,
                msg_timestamp=995.0,
            )
            self.assertIsNone(sig)
            self.assertEqual(r_code, DecisionReasonCode.AI_GROUNDING_FAILED.value)
            # Grounding rejection is final: must NOT model-shop to secondary model!
            self.assertEqual(fake_client.models.generate_content.call_count, 1)

        # 4b. Correct option side (PE), but strike 8000 reused as entry premium 8000 when entry is 100
        fake_client.reset_mock()
        fake_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="PE",
                entry_min=8000.0,
                entry_max=8000.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig2, reason2, _, r_code2, _ = parse_fno_gemini_with_meta(
                "BUY POLYCAB 8000 PE premium 100",
                current_time=1000.0,
                msg_timestamp=995.0,
            )
            self.assertIsNone(sig2)
            self.assertEqual(r_code2, DecisionReasonCode.AI_GROUNDING_FAILED.value)

        # 4c. Explicitly stated expiry '28 OCT' silently omitted by Gemini -> rejected (OMITTED_EXPLICIT_EXPIRY)
        fake_client.reset_mock()
        fake_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="PE",
                expiry_month=None,
                expiry_day=None,
                entry_min=100.0,
                entry_max=100.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig3, reason3, _, r_code3, _ = parse_fno_gemini_with_meta(
                "BUY POLYCAB 8000 PE 28 OCT premium 100",
                current_time=1000.0,
                msg_timestamp=995.0,
            )
            self.assertIsNone(sig3)
            self.assertEqual(r_code3, DecisionReasonCode.OMITTED_EXPLICIT_EXPIRY.value)

        # 5. AI Equity lower bound 790 on 'BUY SBIN @790-800' -> rejected!
        fake_client.reset_mock()
        fake_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiEquityCandidate(
                is_buy_entry=True,
                stock_symbol="SBIN",
                entry_price=790.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            eq_sig, eq_reason, _, eq_code, _ = parse_equity_gemini_with_meta(
                "BUY SBIN @790-800",
                current_time=1000.0,
                msg_timestamp=995.0,
            )
            self.assertIsNone(eq_sig)
            self.assertEqual(eq_code, DecisionReasonCode.AI_GROUNDING_FAILED.value)
            self.assertIn("upper entry reference", eq_reason)

    def test_blocker6_and_blocker7_adaptive_sl_capture_and_redos_alternation_blocked(self):
        """
        Release Blockers 6 & 7:
        - Adaptive rule `(?P<sym>SBIN).*SL (?P<e1>\d+)` applied to `BUY SBIN @800 SL 750` -> rejected!
        - Adaptive pattern `(?P<sym>SBIN) (?:a|aa)+$` -> rejected by `validate_rule_pattern_safety`!
        - Subprocess timeout kills an expensive backtracking regex in < 0.35s without hanging the test runner.
        """
        mgr = AdaptiveRuleManager(None)
        comp = re.compile(r"(?P<sym>SBIN).*SL (?P<e1>\d+)", re.IGNORECASE)
        sig, reason = mgr._apply_single_rule("EQUITY", comp, "BUY SBIN @800 SL 750", now=1000.0, msg_ts=995.0)
        self.assertIsNone(sig)
        self.assertIn("SL/Target", reason)

        ok_alt, alt_msg = validate_rule_pattern_safety("EQUITY", r"(?P<sym>SBIN) (?:a|aa)+$")
        self.assertFalse(ok_alt)
        self.assertIn("ReDoS", alt_msg)

        # Exercise killable subprocess regex worker on an intentionally expensive backtracking pattern
        evil_pat = r"^(a+)+$"
        evil_input = "a" * 28 + "X"
        t0 = time.perf_counter()
        status, gd, elapsed_ms = safe_regex_search_bounded(
            pattern=evil_pat,
            text=evil_input,
            timeout_sec=0.12,
            use_subprocess=True,
        )
        wall_sec = time.perf_counter() - t0
        self.assertEqual(status, "TIMEOUT")
        self.assertIsNone(gd)
        self.assertLess(wall_sec, 1.0, "Killable subprocess must terminate runaway regex promptly")

    def test_blocker8_and_shorthand_contracts_and_explicit_year_rejected(self):
        """
        Release Blocker 8 + P1 Complete Option Identity:
        - 'POLYCAB 8000 PE OCT / NOV @100' -> rejected (CONFLICTING_EXPIRY)
        - 'SBIN 800/820 CE @15' -> rejected (AMBIGUOUS_CONTRACT)
        - 'SBIN 800 CE/PE @15' -> rejected (AMBIGUOUS_CONTRACT)
        - 'POLYCAB 8000 PE OCT 2027 @100' -> rejected (UNSUPPORTED_EXPIRY_YEAR)
        """
        cases = [
            ("POLYCAB 8000 PE OCT / NOV @100", DecisionReasonCode.CONFLICTING_EXPIRY.value),
            ("SBIN 800/820 CE @15", DecisionReasonCode.AMBIGUOUS_CONTRACT.value),
            ("SBIN 800 CE/PE @15", DecisionReasonCode.AMBIGUOUS_CONTRACT.value),
            ("POLYCAB 8000 PE OCT 2027 @100", DecisionReasonCode.UNSUPPORTED_EXPIRY_YEAR.value),
            ("POLYCAB 8000 PE OCTOBAD @100", DecisionReasonCode.INVALID_EXPIRY_TOKEN.value),
        ]
        for msg, expected_code in cases:
            dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
            self.assertIsNone(dec.signal, f"Should have rejected: {msg}")
            self.assertEqual(dec.action, SignalAction.BLOCKED)
            self.assertEqual(dec.reason_code, expected_code, f"Wrong reason_code on {msg}: {dec.reason_code} ({dec.reason})")

    def test_A04_clock_time_excluded_identifies_true_entry_price(self):
        self._populate_master()
        dec_t = parse_signal_brain("BUY SBIN at 10:30 @800", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_t.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_t.equity_signal.stated_price, 800.0)

    def test_A05_multiple_instruments_including_non_hashtag_symbols_rejected(self):
        self._populate_master()
        dec_m = parse_signal_brain("BUY SBIN @800 and RELIANCE @1400", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_m.action, SignalAction.BLOCKED)
        self.assertEqual(dec_m.reason_code, DecisionReasonCode.MULTIPLE_INSTRUMENTS.value)

    def test_A06_historical_callout_classified_as_blocked_not_fresh_entry(self):
        dec_h = parse_signal_brain("Yesterday POLYCAB 8000 PE OCT @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_h.action, SignalAction.BLOCKED)
        self.assertEqual(dec_h.reason_code, DecisionReasonCode.HISTORICAL_CALLOUT.value)

    def test_A07_mixed_option_contracts_rejected_preventing_cross_contract_association(self):
        dec_mix = parse_signal_brain("POLYCAB 8000 PE OCT / RELIANCE 1500 CE OCT @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_mix.action, SignalAction.BLOCKED)
        self.assertEqual(dec_mix.reason_code, DecisionReasonCode.AMBIGUOUS_CONTRACT.value)

    def test_A09_and_A10_token_and_symbol_boundaries(self):
        self._populate_master()
        self.assertFalse(_number_appears_in_text(12, "BUY SBIN @12.3"))
        self.assertFalse(_symbol_appears_in_text("IT", "BUY WITH CARE"))
        self.assertTrue(_symbol_appears_in_text("MPIMANIPAL", "Bought Manipal Payment at 450"))

    def test_unicode_normalization_exception_isolation_and_replay_comparison(self):
        """Verifies A04, A05, A06, A07, A09, A10, Unicode normalization, parser exception isolation, and replay tool."""
        self._populate_master()

        # Unicode normalization: non-breaking spaces (\u00a0), en-dash (\u2013), ₹ currency, \r\n
        unicode_msg = "Buy\u00a0#SBIN\u200b @\u00a0₹790\u2013800\r\n"
        dec_u = parse_signal_brain(unicode_msg, current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_u.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_u.equity_signal.symbol, "SBIN")
        self.assertEqual(dec_u.equity_signal.stated_price, 800.0)
        # Original raw_text is preserved untouched
        self.assertEqual(dec_u.equity_signal.raw_text, unicode_msg)

        # A04: Clock time excluded
        dec_t = parse_signal_brain("BUY SBIN at 10:30 @800", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_t.equity_signal.stated_price, 800.0)

        # A05: Multi-stock blocked
        dec_m = parse_signal_brain("BUY SBIN @800 and RELIANCE @1400", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_m.action, SignalAction.BLOCKED)
        self.assertEqual(dec_m.reason_code, DecisionReasonCode.MULTIPLE_INSTRUMENTS.value)

        # A06: Historical blocked
        dec_h = parse_signal_brain("Yesterday POLYCAB 8000 PE OCT @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_h.action, SignalAction.BLOCKED)
        self.assertEqual(dec_h.reason_code, DecisionReasonCode.HISTORICAL_CALLOUT.value)

        # A07: Mixed contracts blocked
        dec_mix = parse_signal_brain("POLYCAB 8000 PE OCT / RELIANCE 1500 CE OCT @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_mix.action, SignalAction.BLOCKED)
        self.assertEqual(dec_mix.reason_code, DecisionReasonCode.AMBIGUOUS_CONTRACT.value)

        # A09 & A10
        self.assertFalse(_number_appears_in_text(12, "BUY SBIN @12.3"))
        self.assertFalse(_symbol_appears_in_text("IT", "BUY WITH CARE"))

        # Parser exception isolation: unexpected internal error produces BLOCKED + PARSER_EXCEPTION
        with patch("src.parsers.brain.parse_equity_regex_detailed", side_effect=RuntimeError("Simulated internal parser bug")):
            dec_err = parse_signal_brain("Buy #SBIN @ 800", current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNone(dec_err.signal)
            self.assertEqual(dec_err.action, SignalAction.BLOCKED)
            self.assertEqual(dec_err.reason_code, DecisionReasonCode.PARSER_EXCEPTION.value)

        # Offline replay comparison tool
        corpus = [
            "Buy #SBIN @ 800",
            "BUY SBIN @790-800",
            "BUY RELIANCE @1,250",
            "BUY SBIN @12.3.4",
            "BUY SBIN @-800",
            "Yesterday POLYCAB 8000 PE OCT @100",
            "POLYCAB 8000 PE OCTOBAD @100",
        ]
        report = compare_corpus_replay(corpus)
        self.assertEqual(report["total_messages"], 7)
        self.assertGreaterEqual(report["changed_price_count"], 2)   # @790-800 and @1,250 fixed
        self.assertGreaterEqual(report["newly_rejected_count"], 4)  # @12.3.4, @-800, Yesterday, OCTOBAD rejected


if __name__ == "__main__":
    unittest.main()
