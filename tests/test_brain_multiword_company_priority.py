"""BRAIN regression: use verified two-word company identities before shorter tickers."""

import unittest

from src.instruments import resolver as resolver_mod
from src.instruments.resolver import InstrumentResolver, build_name_index, resolve_company_name
from src.parsers.equity_parser import get_unique_stocks_in_message, parse_equity_regex
from src.models import SignalAction


class TestMultiwordCompanySymbolPriority(unittest.TestCase):
    def setUp(self):
        self.valid_before = set(resolver_mod.VALID_EQUITY_SYMBOLS)
        self.index_before = dict(resolver_mod._NAME_INDEX)
        resolver = InstrumentResolver()
        resolver.load_company_names([
            ("NSE", "ARIHANT", "Arihant Limited"),
            ("NSE", "ARIHANTCAP", "Arihant Capital Markets Limited"),
            ("NSE", "SBIN", "State Bank of India"),
            ("NSE", "YESBANK", "Yes Bank"),
            ("NSE", "RELIANCE", "Reliance Industries Limited"),
        ])

    def tearDown(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.update(self.valid_before)
        resolver_mod._NAME_INDEX.clear()
        resolver_mod._NAME_INDEX.update(self.index_before)

    def test_abbreviated_second_word_overrides_shorter_valid_ticker(self):
        for text in ("Bought ARIHANT CAP", "Bought\nARIHANT CAP", "Bought #ARIHANT CAP", "Bought ARIHANT CAP at CMP"):
            with self.subTest(text=text):
                sig, reason = parse_equity_regex(text, current_time=100.0, msg_timestamp=95.0)
                self.assertIsNotNone(sig, reason)
                self.assertEqual(sig.symbol, "ARIHANTCAP")
                self.assertEqual(sig.action, SignalAction.BUY_ENTRY)
                self.assertEqual(get_unique_stocks_in_message(text), {"ARIHANTCAP"})

    def test_exact_single_word_ticker_still_works(self):
        sig, reason = parse_equity_regex("Bought ARIHANT", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig, reason)
        self.assertEqual(sig.symbol, "ARIHANT")

    def test_unmatched_second_word_does_not_fall_back_to_first(self):
        sig, reason = parse_equity_regex("Bought ARIHANT COSMO", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("Unresolved or ambiguous", reason)

    def test_no_verified_compound_cannot_trade_shorter_ticker(self):
        resolver_mod._NAME_INDEX.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.discard("ARIHANTCAP")
        sig, reason = parse_equity_regex("Bought ARIHANT CAP", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)
        self.assertIn("Unresolved or ambiguous", reason)

    def test_known_multiword_alias_without_index_still_works(self):
        resolver_mod._NAME_INDEX.clear()
        sig, reason = parse_equity_regex("Bought YES BANK", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig, reason)
        self.assertEqual(sig.symbol, "YESBANK")

    def test_exact_joined_symbol_without_company_name_index(self):
        resolver_mod._NAME_INDEX.clear()
        for text in (
            "Bought ARIHANT CAP",
            "Bought\nARIHANT CAP",
            "Bought #ARIHANT CAP",
            "#ARIHANT CAP buy",
        ):
            with self.subTest(text=text):
                sig, reason = parse_equity_regex(text, current_time=100.0, msg_timestamp=95.0)
                self.assertIsNotNone(sig, reason)
                self.assertEqual(sig.symbol, "ARIHANTCAP")
                self.assertEqual(get_unique_stocks_in_message(text), {"ARIHANTCAP"})

    def test_final_validator_rejects_shorter_symbol_across_parser_paths(self):
        from src.parsers.brain_validation import validate_equity_candidate, PriceStatus
        for text in ("Bought ARIHANT CAP", "Bought #ARIHANT CAP"):
            with self.subTest(text=text):
                sig, status, evidence, code, reason = validate_equity_candidate(
                    symbol="ARIHANT", raw_text=text, msg_timestamp=95.0
                )
                self.assertIsNone(sig)
                self.assertEqual(status, PriceStatus.INVALID)
                self.assertIn("ARIHANTCAP", reason)

    def test_brain_facade_uses_exact_joined_symbol(self):
        from src.parsers.brain import parse_signal_brain
        resolver_mod._NAME_INDEX.clear()
        result = parse_signal_brain(
            "Bought ARIHANT CAP", current_time=100.0,
            msg_timestamp=95.0, enable_ai_fallback=False
        )
        self.assertEqual(result.action, SignalAction.BUY_ENTRY, result.reason)
        self.assertEqual(result.equity_signal.symbol, "ARIHANTCAP")

    def test_colliding_abbreviations_fail_closed(self):
        index = build_name_index([
            ("NSE", "ARIHANTCAP", "Arihant Capital Markets Limited"),
            ("NSE", "ARIHANTSTONE", "Arihant Capstone Holdings Limited"),
        ])
        self.assertIsNone(resolve_company_name(("ARIHANT", "CAP"), index))

    def test_existing_multi_word_company_resolution(self):
        sig, reason = parse_equity_regex("Bought State Bank of India @ 800", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNotNone(sig, reason)
        self.assertEqual(sig.symbol, "SBIN")

    def test_conjunctive_two_equities_still_blocked(self):
        sig, _ = parse_equity_regex("BUY SBIN and RELIANCE", current_time=100.0, msg_timestamp=95.0)
        self.assertIsNone(sig)


if __name__ == "__main__":
    unittest.main()
