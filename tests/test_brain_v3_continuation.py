"""
Focused, broker-free AuditBot BRAIN v3.1 Corrective & Safety-Hardened Regression Suite.

Covers all defects and requirements from the BRAIN v3.1 Master Development Prompt:
- BRAIN-V3-01: Gemini no-price equity sentinel (`entry_price = 0` accepted as `PriceStatus.ABSENT`
  only when genuinely no price exists in text; explicit `@0`, `@-800`, `@NaN`, `@Infinity`,
  `at NaN`, `at Infinity`, `@800-abc` rejected across all routes).
- BRAIN-V3-02: Atomic F&O contract grounding (`BUY SBIN? RELIANCE 800 PE @100` rejected;
  pre-strike expiry `BUY SBIN NOV 800 CE @20` supported; conflicting pre/post expiry rejected;
  `SL: @30` and `Target: @150` masked; verified character offset evidence).
- BRAIN-V3-03: Strict F&O AI numeric validation (`NaN`, `±Infinity`, booleans, negative/zero
  premiums, malformed strings, and inverted `entry_min > entry_max` bounds rejected before text comparison).
- BRAIN-V3-04: Adaptive regex corpus limits, zero silent truncation, mandatory safety negatives
  always evaluated first, expected structured output comparisons, and skip-blocked approval.
- BRAIN-V3-05: Explicit worker outcome classification (`MATCH`, `NO_MATCH`, `TIMEOUT`,
  `WORKER_ERROR`, `INVALID_PATTERN`, `EVALUATION_ERROR`); worker crash on negative cases blocks approval.
- BRAIN-V3-06: Enforceable aggregate adaptive regex execution budget & thread-safe bounded
  exec-isolated worker pool (`subprocess.Popen` `python3 -I -S -u`, zero unsafe `fork`).
- BRAIN-V3-07: Hard wall-clock deadline preemption on hanging Gemini client init / transport,
  primary (`gemini-2.5-flash`) to secondary (`gemini-3.1-flash-lite`) fallback within shared budget,
  and post-BRAIN pre-executor freshness gate blocking late responses.
- Part 7, Part 8 & Part 11: Immutable rule lifecycle across process restarts, fail-closed unknown
  DB rule states, emergency STOP, source authorization, duplicate claim boundary, TEST mode zero
  broker mutations, and real V14 frozen baseline replay (`unexpected_regression_count == 0`).
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.instruments import resolver as resolver_mod
from src.instruments.resolver import InstrumentResolver
from src.journal.db import JournalDB
from src.main import TradingBotPipeline
from src.models import OrderType, SignalAction, SignalType, TradingMode
from src.parsers.adaptive_rules import (
    AdaptiveRuleManager,
    MANDATORY_SAFETY_NEGATIVES,
    RegexEvalOutcome,
    RuleLifecycleState,
    safe_regex_search_bounded,
    validate_rule_pattern_safety,
)
from src.parsers.brain import compare_corpus_replay, parse_signal_brain
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    validate_equity_candidate,
    validate_fno_candidate,
)
from src.parsers.gemini_fallback import (
    GeminiEquityCandidate,
    GeminiFnOCandidate,
    parse_equity_gemini_with_meta,
    parse_fno_gemini_with_meta,
)


MASTER_FIXTURE_ROWS = [
    ("NSE", "SBIN", "State Bank of India"),
    ("NSE", "RELIANCE", "Reliance Industries"),
    ("NSE", "POLYCAB", "Polycab India"),
    ("NSE", "INFY", "Infosys"),
    ("NSE", "TCS", "Tata Consultancy Services"),
    ("NSE", "TATAMOTORS", "Tata Motors"),
    ("NSE", "MPIMANIPAL", "Manipal Payment and Identity Solutions"),
    ("NSE", "MANIPALHOS", "Manipal Health Enterprises"),
]


class TestBrainV3Continuation(unittest.TestCase):
    def setUp(self):
        self._orig_valid = set(resolver_mod.VALID_EQUITY_SYMBOLS)
        self._orig_index = dict(resolver_mod._NAME_INDEX)
        self.resolver = InstrumentResolver()
        self.resolver.load_company_names(MASTER_FIXTURE_ROWS)

    def tearDown(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.update(self._orig_valid)
        resolver_mod._NAME_INDEX.clear()
        resolver_mod._NAME_INDEX.update(self._orig_index)

    # ------------------------------------------------------------------
    # BRAIN-V3-01: Gemini no-price equity zero sentinel vs invalid price
    # ------------------------------------------------------------------
    def test_v3_01_equity_no_price_gemini_zero_sentinel_and_explicit_invalid_prices(self):
        # 1. Genuine no-price equity message ('Bought SBIN', 'BUY RELIANCE AT CMP') + Gemini entry_price=0.0 -> ABSENT + LIMIT
        fake_client = MagicMock()
        fake_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiEquityCandidate(is_buy_entry=True, stock_symbol="SBIN", entry_price=0.0)
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            sig, reason, model_used, r_code, ev = parse_equity_gemini_with_meta(
                "Bought SBIN", current_time=1000.0, msg_timestamp=995.0
            )
            self.assertIsNotNone(sig, f"Expected valid no-price EquitySignal, got None ({reason})")
            self.assertEqual(sig.symbol, "SBIN")
            self.assertEqual(sig.stated_price, 0.0)
            self.assertEqual(sig.order_type, OrderType.LIMIT)
            self.assertEqual(r_code, DecisionReasonCode.OK.value)
            self.assertEqual(ev.get("price_status"), PriceStatus.ABSENT.value)
            self.assertFalse(ev.get("explicit_price"))

        # 2. When explicit price exists in text ('Buy SBIN @800'), candidate_price=0 or malformed values are rejected
        for malformed in [0, 0.0, -1.0, -800.0, float("nan"), float("inf"), float("-inf"), "bad", True, False]:
            sig_bad, pstat_bad, _, _, _ = validate_equity_candidate(
                symbol="SBIN", raw_text="Buy SBIN @800", candidate_price=malformed
            )
            self.assertIsNone(sig_bad, f"Should reject {malformed!r} when explicit price @800 exists")
            self.assertEqual(pstat_bad, PriceStatus.INVALID)

        # 3. When no price exists in text ('Bought SBIN'), non-zero/negative/NaN/Inf/bool candidates are rejected
        for bad_no_price in [800.0, -1.0, float("nan"), float("inf"), float("-inf"), "bad", True, False]:
            sig_np, pstat_np, _, _, _ = validate_equity_candidate(
                symbol="SBIN", raw_text="Bought SBIN", candidate_price=bad_no_price
            )
            self.assertIsNone(sig_np, f"Should reject ungrounded/invalid candidate {bad_no_price!r} on 'Bought SBIN'")
            self.assertEqual(pstat_np, PriceStatus.INVALID)

        # 4. Explicit invalid prices in text must be rejected across REGEX, ADAPTIVE_REGEX, and GEMINI
        explicit_invalid_msgs = [
            "BUY SBIN @0",
            "BUY SBIN @-800",
            "BUY SBIN @NaN",
            "BUY SBIN @Infinity",
            "BUY SBIN @-Infinity",
            "BUY SBIN at NaN",
            "BUY SBIN at Infinity",
            "BUY SBIN entry 0",
            "BUY SBIN @800-abc",
            "BUY SBIN @790-abc",
        ]
        for msg in explicit_invalid_msgs:
            dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=True)
            self.assertIsNone(dec.signal, f"Must reject explicit invalid price: {msg}")
            self.assertEqual(dec.action, SignalAction.BLOCKED, f"Expected BLOCKED for {msg}")
            self.assertEqual(dec.price_status, PriceStatus.INVALID)
            self.assertEqual(dec.reason_code, DecisionReasonCode.INVALID_EXPLICIT_PRICE.value)

    # ------------------------------------------------------------------
    # BRAIN-V3-02: Atomic F&O contract grounding & source-span offsets
    # ------------------------------------------------------------------
    def test_v3_02_atomic_fno_contract_grounding_and_offsets(self):
        # 1. Adversarial cross-phrase stitching: 'BUY SBIN? RELIANCE 800 PE @100'
        sig_adv, pstat_adv, _, code_adv, _ = validate_fno_candidate(
            symbol="SBIN",
            strike=800.0,
            option_type="PE",
            entry_min=100.0,
            entry_max=100.0,
            raw_text="BUY SBIN? RELIANCE 800 PE @100",
        )
        self.assertIsNone(sig_adv, "Must never combine SBIN from one phrase with 800 PE from RELIANCE!")
        self.assertEqual(pstat_adv, PriceStatus.INVALID)

        dec_adv = parse_signal_brain("BUY SBIN? RELIANCE 800 PE @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertIsNone(dec_adv.signal)
        self.assertEqual(dec_adv.action, SignalAction.BLOCKED)
        self.assertEqual(dec_adv.reason_code, DecisionReasonCode.AMBIGUOUS_CONTRACT.value)

        # 2. Pre-strike expiry ('BUY SBIN NOV 800 CE @20', 'BUY SBIN 28 NOV 800CE @20')
        dec_pre = parse_signal_brain("BUY SBIN NOV 800 CE @20", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_pre.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_pre.fno_signal.symbol, "SBIN")
        self.assertEqual(dec_pre.fno_signal.strike, 800.0)
        self.assertEqual(dec_pre.fno_signal.option_type, "CE")
        self.assertEqual(dec_pre.fno_signal.expiry_month, "NOV")
        self.assertEqual(dec_pre.fno_signal.entry_max, 20.0)

        dec_pre_day = parse_signal_brain("BUY SBIN 28 NOV 800CE @20", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_pre_day.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_pre_day.fno_signal.symbol, "SBIN")
        self.assertEqual(dec_pre_day.fno_signal.expiry_day, 28)
        self.assertEqual(dec_pre_day.fno_signal.expiry_month, "NOV")

        # 3. Conflicting pre-strike and post-strike expiry ('BUY SBIN NOV 800 CE DEC @20') -> rejected
        dec_conf_exp = parse_signal_brain("BUY SBIN NOV 800 CE DEC @20", current_time=1000.0, msg_timestamp=995.0)
        self.assertIsNone(dec_conf_exp.signal)
        self.assertEqual(dec_conf_exp.action, SignalAction.BLOCKED)
        self.assertEqual(dec_conf_exp.reason_code, DecisionReasonCode.CONFLICTING_EXPIRY.value)

        # 4. Special-attention required examples from prompt:
        # 4a. 'BUY POLYCAB 8000 PE OCT @100' -> valid with verified character offsets
        dec_poly = parse_signal_brain("BUY POLYCAB 8000 PE OCT @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_poly.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_poly.fno_signal.symbol, "POLYCAB")
        self.assertEqual(dec_poly.fno_signal.strike, 8000.0)
        self.assertEqual(dec_poly.fno_signal.option_type, "PE")
        self.assertEqual(dec_poly.fno_signal.expiry_month, "OCT")
        self.assertEqual(dec_poly.fno_signal.entry_max, 100.0)
        self.assertIsNotNone(dec_poly.evidence.get("symbol_span_offsets"))
        self.assertIsNotNone(dec_poly.evidence.get("strike_span_offsets"))
        self.assertIsNotNone(dec_poly.evidence.get("option_side_offsets"))
        self.assertIsNotNone(dec_poly.evidence.get("contract_span_offsets"))
        self.assertIsNotNone(dec_poly.evidence.get("price_span_offsets"))

        # 4b. 'BUY POLYCAB 8000 PE OCT SL @30' and 'BUY POLYCAB 8000 PE OCT SL: @30' -> rejected (NO_ENTRY_PREMIUM)
        for sl_only in ("BUY POLYCAB 8000 PE OCT SL @30", "BUY POLYCAB 8000 PE OCT SL: @30", "BUY POLYCAB 8000 PE OCT TARGET: @150"):
            dec_sl = parse_signal_brain(sl_only, current_time=1000.0, msg_timestamp=995.0)
            self.assertIsNone(dec_sl.signal, f"Must reject SL/Target without entry: {sl_only}")
            self.assertEqual(dec_sl.action, SignalAction.BLOCKED)
            self.assertEqual(dec_sl.reason_code, DecisionReasonCode.NO_ENTRY_PREMIUM.value)

        # 4c. 'BUY RELIANCE 1500 CE @50 TARGET 75' -> entry=50.0, targets=[75.0]
        dec_rel = parse_signal_brain("BUY RELIANCE 1500 CE @50 TARGET 75", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_rel.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_rel.fno_signal.symbol, "RELIANCE")
        self.assertEqual(dec_rel.fno_signal.strike, 1500.0)
        self.assertEqual(dec_rel.fno_signal.option_type, "CE")
        self.assertEqual(dec_rel.fno_signal.entry_min, 50.0)
        self.assertEqual(dec_rel.fno_signal.entry_max, 50.0)
        self.assertEqual(dec_rel.fno_signal.targets, [75.0])

        # 4d. 'BUY SBIN 800 PE @100' -> valid, and rejects wrong strike, wrong side, or hallucinated expiry
        dec_sbin = parse_signal_brain("BUY SBIN 800 PE @100", current_time=1000.0, msg_timestamp=995.0)
        self.assertEqual(dec_sbin.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_sbin.fno_signal.symbol, "SBIN")
        self.assertEqual(dec_sbin.fno_signal.strike, 800.0)
        self.assertEqual(dec_sbin.fno_signal.option_type, "PE")
        self.assertIsNone(dec_sbin.fno_signal.expiry_month)
        self.assertEqual(dec_sbin.fno_signal.entry_max, 100.0)

        # Wrong strike (820 vs 800), wrong side (CE vs PE), hallucinated expiry ('OCT'), target-as-entry (75 vs 50)
        self.assertIsNone(validate_fno_candidate("SBIN", 820.0, "PE", 100.0, 100.0, "BUY SBIN 800 PE @100")[0])
        self.assertIsNone(validate_fno_candidate("SBIN", 800.0, "CE", 100.0, 100.0, "BUY SBIN 800 PE @100")[0])
        self.assertIsNone(validate_fno_candidate("SBIN", 800.0, "PE", 100.0, 100.0, "BUY SBIN 800 PE @100", expiry_month="OCT")[0])
        self.assertIsNone(validate_fno_candidate("RELIANCE", 1500.0, "CE", 75.0, 75.0, "BUY RELIANCE 1500 CE @50 TARGET 75")[0])

    # ------------------------------------------------------------------
    # BRAIN-V3-03: Invalid F&O AI numbers rejected before text fallback
    # ------------------------------------------------------------------
    def test_v3_03_invalid_fno_ai_numbers_rejected(self):
        raw = "BUY POLYCAB 8000 PE OCT @95-105"
        invalid_pairs = [
            (float("nan"), float("nan")),
            (95.0, float("nan")),
            (float("nan"), 105.0),
            (float("inf"), 105.0),
            (95.0, float("inf")),
            (float("-inf"), 105.0),
            (-95.0, 105.0),
            (0.0, 105.0),
            (95.0, 0.0),
            (True, 105.0),
            (95.0, False),
            ("95-abc", 105.0),
            (105.0, 95.0),  # Conflicting/inverted lower > upper bounds!
        ]
        for emin, emax in invalid_pairs:
            sig, pstat, _, r_code, reason = validate_fno_candidate(
                symbol="POLYCAB",
                strike=8000.0,
                option_type="PE",
                entry_min=emin,
                entry_max=emax,
                raw_text=raw,
                expiry_month="OCT",
            )
            self.assertIsNone(sig, f"Must reject invalid F&O entry bounds ({emin!r}, {emax!r}): {reason}")
            self.assertEqual(pstat, PriceStatus.INVALID)
            self.assertEqual(r_code, DecisionReasonCode.AI_GROUNDING_FAILED.value)

        # Invalid strike, stop_loss, targets, or expiry_day must also be rejected
        for bad_strike in [float("nan"), float("inf"), -8000.0, 0.0, True, "bad"]:
            sig_s, *_ = validate_fno_candidate("POLYCAB", bad_strike, "PE", 95.0, 105.0, raw, expiry_month="OCT")
            self.assertIsNone(sig_s, f"Must reject bad strike {bad_strike!r}")

        for bad_sl in [float("nan"), float("inf"), -10.0, 0.0, True, "bad"]:
            sig_sl, *_ = validate_fno_candidate("POLYCAB", 8000.0, "PE", 95.0, 105.0, raw, expiry_month="OCT", stop_loss=bad_sl)
            self.assertIsNone(sig_sl, f"Must reject bad stop_loss {bad_sl!r}")

        for bad_tgts in [[float("nan")], [float("inf")], [-150.0], [0.0], [True], "not-a-list"]:
            sig_t, *_ = validate_fno_candidate("POLYCAB", 8000.0, "PE", 95.0, 105.0, raw, expiry_month="OCT", targets=bad_tgts)
            self.assertIsNone(sig_t, f"Must reject bad targets {bad_tgts!r}")

        for bad_day in [True, False, 0, 32, -5, 15.5, float("nan")]:
            sig_d, *_ = validate_fno_candidate("POLYCAB", 8000.0, "PE", 95.0, 105.0, raw, expiry_month="OCT", expiry_day=bad_day)
            self.assertIsNone(sig_d, f"Must reject bad expiry_day {bad_day!r}")

    # ------------------------------------------------------------------
    # BRAIN-V3-04 & BRAIN-V3-05: Corpus limits, no silent truncation,
    # mandatory safety negatives, and worker failure classification
    # ------------------------------------------------------------------
    def test_v3_04_and_v3_05_corpus_truncation_blocked_and_worker_crash_classified(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = JournalDB(str(Path(tmp) / "journal.sqlite"), account_id="offline-test")
            mgr = AdaptiveRuleManager(db)
            pat = r"LONG\s+TICKER\s*::\s*(?P<sym>[A-Z]+)\s+ENTRY\s*::\s*@?(?P<e1>\d+)"

            # 1. Proposing > 100 positives is rejected upfront
            pos_101 = [
                {"text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800", "expected": {"symbol": "SBIN", "stated_price": 800.0}}
                for _ in range(100)
            ] + [
                {"text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800", "expected": {"symbol": "RELIANCE", "stated_price": 9999.0}}
            ]
            ok_over, msg_over, _ = mgr.propose_rule("OVER_POS", "EQUITY", pat, sample_positives=pos_101)
            self.assertFalse(ok_over)
            self.assertIn("exceeds max limit", msg_over)

            # 2. Passing 101 positives to evaluate_rule never silently truncates: sets total_skipped=1, passed=False, blocks approval
            ok_p, _, _ = mgr.propose_rule("EVAL_OVER", "EQUITY", pat, sample_positives=pos_101[:10])
            self.assertTrue(ok_p)
            eval_ok, _, eval_sum = mgr.evaluate_rule("EVAL_OVER", positive_corpus=pos_101)
            self.assertFalse(eval_ok)
            self.assertEqual(eval_sum["positive_submitted"], 101)
            self.assertEqual(eval_sum["positive_evaluated"], 100)
            self.assertEqual(eval_sum["positive_skipped"], 1)
            self.assertEqual(eval_sum["total_skipped"], 1)
            self.assertFalse(mgr.approve_rule("EVAL_OVER", reviewer="Udit")[0])

            # 3. Bare string positive sample without expected structured dict is rejected
            eval_bare_ok, _, eval_bare_sum = mgr.evaluate_rule("EVAL_OVER", positive_corpus=["Buy intent LONG TICKER :: SBIN ENTRY :: @800"])
            self.assertFalse(eval_bare_ok)
            self.assertGreater(eval_bare_sum["outcome_counts"][RegexEvalOutcome.EVALUATION_ERROR.value], 0)

            # 4. BRAIN-V3-05: Worker crash ('WORKER_ERROR' or 'ERROR') on mandatory negatives fails evaluation & blocks approval
            def _crashing_worker(pattern, text, timeout_sec=0.25, use_subprocess=True):
                if "LONG TICKER" in text:
                    return RegexEvalOutcome.MATCH.value, {"sym": "SBIN", "e1": "800"}, 0.5
                return RegexEvalOutcome.WORKER_ERROR.value, None, 0.5

            with patch("src.parsers.adaptive_rules.safe_regex_search_bounded", side_effect=_crashing_worker):
                crash_ok, _, crash_sum = mgr.evaluate_rule(
                    "EVAL_OVER",
                    positive_corpus=[{"text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800", "expected": {"symbol": "SBIN", "stated_price": 800.0}}],
                )
            self.assertFalse(crash_ok)
            self.assertEqual(crash_sum["infrastructure_error_count"], len(MANDATORY_SAFETY_NEGATIVES))
            self.assertEqual(crash_sum["outcome_counts"][RegexEvalOutcome.WORKER_ERROR.value], len(MANDATORY_SAFETY_NEGATIVES))
            self.assertFalse(mgr.approve_rule("EVAL_OVER", reviewer="Udit")[0])

    # ------------------------------------------------------------------
    # BRAIN-V3-06: Aggregate regex execution budget & multithreaded pool
    # ------------------------------------------------------------------
    def test_v3_06_runtime_match_from_threads_and_aggregate_budget_enforcement(self):
        pattern = r"(?P<sym>SBIN) @(?P<e1>\d+)"
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda _: safe_regex_search_bounded(pattern, "SBIN @790", timeout_sec=2.0),
                    range(24),
                )
            )
        self.assertTrue(all(status == "MATCH" and fields["e1"] == "790" for status, fields, _ in results), results)

        # Catastrophic backtracking pattern terminates within per-rule timeout
        status, groups, _ = safe_regex_search_bounded(r"^(a+)+$", "a" * 29 + "X", 0.12)
        self.assertEqual(status, RegexEvalOutcome.TIMEOUT.value)
        self.assertIsNone(groups)

        # Aggregate timeout across multiple active rules stops evaluation when aggregate budget is reached
        with tempfile.TemporaryDirectory() as tmp:
            db = JournalDB(str(Path(tmp) / "agg.sqlite"), account_id="offline-test")
            mgr = AdaptiveRuleManager(db)
            for idx in range(5):
                rid = f"SLOW_RULE_{idx}"
                mgr.propose_rule(
                    rid,
                    "EQUITY",
                    rf"CUSTOM_{idx}\s+(?P<sym>[A-Z]+)\s+@(?P<e1>\d+)",
                    sample_positives=[{"text": f"Buy CUSTOM_{idx} SBIN @800", "expected": {"symbol": "SBIN", "stated_price": 800.0}}],
                )
                mgr.evaluate_rule(rid)
                mgr.approve_rule(rid, reviewer="Udit")
                mgr.activate_rule(rid, actor="Udit")

            call_counter = {"count": 0}

            def _slow_worker(pattern, text, timeout_sec=0.25, use_subprocess=True):
                call_counter["count"] += 1
                time.sleep(0.08)
                return RegexEvalOutcome.NO_MATCH.value, None, 80.0

            t0 = time.perf_counter()
            with patch("src.parsers.adaptive_rules.safe_regex_search_bounded", side_effect=_slow_worker):
                sig, rule_id, reason, ev = mgr.match_active_rules(
                    route="EQUITY",
                    text="Buy something custom @800",
                    current_time=1000.0,
                    msg_timestamp=995.0,
                    per_rule_timeout_sec=0.15,
                    aggregate_timeout_sec=0.18,
                )
            elapsed = time.perf_counter() - t0
            self.assertIsNone(sig)
            self.assertLess(call_counter["count"], 5, "Aggregate budget must stop before running all 5 slow rules")
            self.assertEqual(ev.get("reason_code"), DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value)
            self.assertLess(elapsed, 0.35)

    # ------------------------------------------------------------------
    # BRAIN-V3-07: Gemini hard deadline, fallback models, and freshness gate
    # ------------------------------------------------------------------
    def test_v3_07_gemini_hard_deadline_fallback_models_and_pre_executor_freshness(self):
        # 1. Hanging transport inside generate_content is preempted by _call_with_hard_deadline and cancelled via client.close()
        import threading as _th
        hang_evt = _th.Event()
        hang_client = MagicMock()
        hang_client.close.side_effect = hang_evt.set
        hang_client.models.generate_content.side_effect = lambda **kw: (
            hang_evt.wait(0.6),
            SimpleNamespace(parsed=GeminiEquityCandidate(is_buy_entry=True, stock_symbol="SBIN", entry_price=800.0)),
        )[1]
        t0 = time.perf_counter()
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=hang_client):
            sig, reason, model_used, r_code, _ = parse_equity_gemini_with_meta(
                "Accumulate SBIN entry 800",
                current_time=1000.0,
                msg_timestamp=995.0,
                timeout_budget_sec=0.10,
            )
        elapsed = time.perf_counter() - t0
        self.assertIsNone(sig)
        self.assertEqual(r_code, DecisionReasonCode.AI_TIMEOUT.value)
        self.assertLess(elapsed, 0.25, f"Hard deadline failed to preempt blocking call ({elapsed:.3f}s)")

        # 2. Primary model ('gemini-2.5-flash') transport error falls back to secondary ('gemini-3.1-flash-lite')
        fallback_client = MagicMock()
        models_called = []

        def _primary_fail_secondary_ok(model, contents, config):
            models_called.append(model)
            if model == "gemini-2.5-flash":
                raise ConnectionError("Simulated 503 / connection reset on primary model")
            return SimpleNamespace(
                parsed=GeminiEquityCandidate(is_buy_entry=True, stock_symbol="SBIN", entry_price=0.0)
            )

        fallback_client.models.generate_content.side_effect = _primary_fail_secondary_ok
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fallback_client):
            sig_fb, _, model_fb, code_fb, _ = parse_equity_gemini_with_meta(
                "Accumulate State Bank of India now",
                current_time=1000.0,
                msg_timestamp=995.0,
            )
        self.assertIsNotNone(sig_fb)
        self.assertEqual(sig_fb.symbol, "SBIN")
        self.assertEqual(sig_fb.stated_price, 0.0)
        self.assertEqual(model_fb, "gemini-3.1-flash-lite")
        self.assertEqual(models_called, ["gemini-2.5-flash", "gemini-3.1-flash-lite"])
        self.assertEqual(code_fb, DecisionReasonCode.OK.value)

        # 3. Late Gemini response that causes message age to exceed max_age_sec (60s) is blocked before executor handoff!
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AppConfig(
                trading_mode=TradingMode.TEST,
                equity_active=True,
                fno_active=True,
                dhan_client_id="1113731536",
                monitored_channel_ids=[-1001],
                data_dir=tmp,
                journal_db_path=str(Path(tmp) / "j.db"),
                max_message_age_sec=60,
            )
            pipeline = TradingBotPipeline(cfg)
            pipeline.stop_manager.provision_initial_clean_state("Udit", "1113731536")
            pipeline.reconciliation_gate._accepting_entries = True
            pipeline.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ", company_name="State Bank of India")
            pipeline._handle_equity_signal = MagicMock()

            slow_evt = _th.Event()
            slow_valid_client = MagicMock()
            slow_valid_client.close.side_effect = slow_evt.set

            def _slow_valid_resp(model, contents, config):
                slow_evt.wait(0.15)
                return SimpleNamespace(
                    parsed=GeminiEquityCandidate(is_buy_entry=True, stock_symbol="SBIN", entry_price=800.0)
                )

            slow_valid_client.models.generate_content.side_effect = _slow_valid_resp
            with patch("src.parsers.gemini_fallback._get_genai_client", return_value=slow_valid_client):
                # Message is already 59.92s old when received; 0.15s AI latency pushes it to 60.07s > 60.0s!
                res_late = pipeline.process_raw_message(
                    text="Accumulate SBIN with entry 800",
                    channel_id=-1001,
                    message_id=901,
                    msg_timestamp=1000.0,
                    current_time=1059.92,
                )
            self.assertFalse(res_late.is_valid)
            self.assertEqual(res_late.action, SignalAction.BLOCKED)
            self.assertEqual(pipeline._handle_equity_signal.call_count, 0, "Stale message after AI must NEVER reach executor!")

    # ------------------------------------------------------------------
    # Part 7 & Part 8: Immutable lifecycle, restart persistence, unknown state fail-closed
    # ------------------------------------------------------------------
    def test_restore_immutable_restart_persistence_and_unknown_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_file = str(Path(tmp) / "journal.sqlite")
            db = JournalDB(db_file, account_id="offline-test")
            db.create_or_update_parser_rule("SAMPLE", "EQUITY", r"(?P<sym>SBIN)", proposed_by="dev")
            # Direct activation from PROPOSED must be blocked by DB invariant
            self.assertFalse(db.update_parser_rule_state("SAMPLE", "ACTIVE", "operator"))

            db.update_parser_rule_state("SAMPLE", "EVALUATED", "dev", eval_summary={"passed": True})
            db.update_parser_rule_state("SAMPLE", "APPROVED", "reviewer")
            db.update_parser_rule_state("SAMPLE", "ACTIVE", "operator")
            db.create_or_update_parser_rule("SAMPLE", "EQUITY", r"(?P<sym>RELIANCE)", proposed_by="dev")

            mgr = AdaptiveRuleManager(db)
            self.assertFalse(mgr.restore_rule_version("SAMPLE", 1, "operator", activate=True)[0])
            self.assertFalse(db.restore_parser_rule_version("SAMPLE", 1, "operator", activate=True)[0])
            okay, _, rec = mgr.restore_rule_version("SAMPLE", 1, "operator", activate=False)
            self.assertTrue(okay)
            self.assertEqual(rec["state"], "APPROVED")
            self.assertEqual(db.get_parser_rule("SAMPLE", version=1)["approved_by"], "reviewer")

            with self.assertRaises(sqlite3.DatabaseError):
                with db._get_conn() as conn:
                    conn.execute("UPDATE parser_rule_versions SET pattern = 'tamper' WHERE rule_id = 'SAMPLE'")
            with self.assertRaises(sqlite3.DatabaseError):
                with db._get_conn() as conn:
                    conn.execute("DELETE FROM parser_rule_reviews WHERE rule_id = 'SAMPLE'")

            # Simulate bot restart: reopen JournalDB and verify restored rule did NOT silently promote to ACTIVE
            db_restarted = JournalDB(db_file, account_id="offline-test")
            self.assertEqual(db_restarted.get_parser_rule("SAMPLE")["state"], "APPROVED")
            self.assertEqual(len(db_restarted.list_parser_rules(route="EQUITY", state="ACTIVE")), 0)

            mgr_restarted = AdaptiveRuleManager(db_restarted)
            self.assertTrue(mgr_restarted.activate_rule("SAMPLE", "operator")[0])
            self.assertEqual(db_restarted.get_parser_rule("SAMPLE")["state"], "ACTIVE")

            # Corrupt state in parser_rule_candidates to an unknown value -> fails closed as REJECTED
            with db_restarted._get_conn() as conn:
                conn.execute("UPDATE parser_rule_candidates SET state = 'CORRUPT_UNKNOWN_STATE' WHERE rule_id = 'SAMPLE'")
            corrupted_rec = db_restarted.get_parser_rule("SAMPLE")
            self.assertEqual(corrupted_rec["state"], "REJECTED")
            self.assertEqual(len(db_restarted.list_parser_rules(route="EQUITY", state="ACTIVE")), 0)

    # ------------------------------------------------------------------
    # Part 11 Item #7: Real V14 Frozen Baseline Replay Comparison
    # ------------------------------------------------------------------
    def test_real_v14_frozen_baseline_replay_zero_unexpected_regressions(self):
        corpus = [
            "Buy #SBIN @ 800",
            "Bought SBIN",
            "BUY RELIANCE AT CMP",
            "BUY SBIN @790-800",
            "BUY RELIANCE @1,250",
            "BUY SBIN @12.3.4",
            "BUY SBIN @-800",
            "BUY SBIN @0",
            "BUY SBIN @800-abc",
            "SBIN TARGET HIT",
            "Exit #SBIN now",
            "Buy #NIFTY @ 24500",
            "#POLYCAB 8000 PE OCT @100",
            "BUY RELIANCE 1500 CE @50 TARGET 75",
            "BUY SBIN 800 PE @100",
            "BUY POLYCAB 8000 PE OCT SL @30",
            "BUY POLYCAB 8000 PE OCT SL: @30",
            "Yesterday POLYCAB 8000 PE OCT @100",
            "POLYCAB 8000 PE OCTOBAD @100",
            "BUY SBIN NOV 800 CE @20",
            "BUY SBIN? RELIANCE 800 PE @100",
        ]
        report = compare_corpus_replay(corpus)
        self.assertEqual(report["total_messages"], len(corpus))
        self.assertEqual(
            report["unexpected_regression_count"],
            0,
            f"Unexpected regressions detected: {report['unexpected_regressions']}",
        )
        self.assertGreaterEqual(report["expected_safety_correction_count"], 8)
        self.assertGreaterEqual(report["approved_behavior_preservation_count"], 8)



    # ------------------------------------------------------------------
    # v3.2 Audit Regression P0-01: Negated & informational F&O messages blocked
    # ------------------------------------------------------------------
    def test_v3_2_p0_01_negated_and_informational_fno_messages_blocked(self):
        negated_fno_messages = [
            "DO NOT BUY POLYCAB 8000 PE OCT @100",
            "DONT BUY POLYCAB 8000 PE OCT @100",
            "DON'T BUY POLYCAB 8000 PE OCT @100",
            "AVOID POLYCAB 8000 PE OCT @100",
            "WATCH POLYCAB 8000 PE OCT @100",
            "ONLY FOR LEARNING POLYCAB 8000 PE OCT @100",
            "NOT A BUY POLYCAB 8000 PE OCT @100",
            "WAIT FOR POLYCAB 8000 PE OCT @100",
            "PAPER TRADE ONLY POLYCAB 8000 PE OCT @100",
            "NO BUY IN POLYCAB 8000 PE OCT @100",
        ]
        fake_ai = MagicMock()
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_ai):
            for msg in negated_fno_messages:
                dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=True)
                self.assertEqual(dec.route, "OPTION", f"Expected OPTION route for {msg!r}")
                self.assertEqual(dec.action, SignalAction.BLOCKED, f"Expected BLOCKED for {msg!r}, got {dec.action}")
                self.assertEqual(dec.reason_code, DecisionReasonCode.ANALYSIS_COMMENTARY.value, f"Wrong reason_code for {msg!r}")
                self.assertIsNone(dec.signal, f"Must not emit executable signal for {msg!r}")
            self.assertEqual(fake_ai.models.generate_content.call_count, 0, "Negated F&O messages must never reach Gemini fallback")

        # Direct validator also blocks negated F&O and Equity messages
        sig_v, pstat_v, _, code_v, _ = validate_fno_candidate(
            "POLYCAB", 8000.0, "PE", 100.0, 100.0, "DO NOT BUY POLYCAB 8000 PE OCT @100", expiry_month="OCT"
        )
        self.assertIsNone(sig_v)
        self.assertEqual(code_v, DecisionReasonCode.ANALYSIS_COMMENTARY.value)

        # Control: approved terse option call still succeeds
        dec_ok = parse_signal_brain("POLYCAB 8000 PE OCT @100", current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
        self.assertEqual(dec_ok.route, "OPTION")
        self.assertEqual(dec_ok.action, SignalAction.BUY_ENTRY)
        self.assertEqual(dec_ok.fno_signal.symbol, "POLYCAB")
        self.assertEqual(dec_ok.fno_signal.entry_max, 100.0)

    # ------------------------------------------------------------------
    # v3.2 Audit Regression P0-02: Space-separated negative/signed prices rejected
    # ------------------------------------------------------------------
    def test_v3_2_p0_02_space_separated_negative_and_signed_prices_blocked(self):
        malformed_signed_msgs = [
            "BUY SBIN at - 800",
            "BUY SBIN CMP - 800",
            "BUY SBIN at -800",
            "BUY SBIN @ - 800",
            "BUY SBIN entry - 800",
            "BUY SBIN around - 800",
            "BUY SBIN above - 800",
            "BUY SBIN price - 800",
            "BUY SBIN at + 800",
            "BUY SBIN at + - 800",
            "POLYCAB 8000 PE OCT at - 100",
            "POLYCAB 8000 PE OCT CMP - 100",
        ]
        for msg in malformed_signed_msgs:
            dec = parse_signal_brain(msg, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
            self.assertIsNone(dec.signal, f"Must reject malformed signed price in {msg!r}")
            self.assertEqual(dec.action, SignalAction.BLOCKED, f"Expected BLOCKED for {msg!r}")
            self.assertEqual(dec.price_status, PriceStatus.INVALID, f"Expected PriceStatus.INVALID for {msg!r}")
            self.assertEqual(dec.reason_code, DecisionReasonCode.INVALID_EXPLICIT_PRICE.value)

        # Controls: legitimate no-price messages remain PriceStatus.ABSENT (stated_price=0.0)
        for ok_cmp in ("Bought SBIN", "BUY RELIANCE AT CMP", "BUY #SBIN CMP", "BUY #SBIN @ CMP"):
            dec_cmp = parse_signal_brain(ok_cmp, current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
            self.assertEqual(dec_cmp.action, SignalAction.BUY_ENTRY, f"Expected valid no-price BUY for {ok_cmp!r}")
            self.assertEqual(dec_cmp.price_status, PriceStatus.ABSENT)
            self.assertEqual(dec_cmp.equity_signal.stated_price, 0.0)

    # ------------------------------------------------------------------
    # v3.2 Audit Regression P1-03: Boolean AI numbers rejected through response pathway
    # ------------------------------------------------------------------
    def test_v3_2_p1_03_pydantic_boolean_ai_numbers_rejected_in_response_pathway(self):
        # 1. Mocked GeminiFnOCandidate with boolean entry bounds on 'BUY SBIN 800 PE premium 1'
        cand_fno = GeminiFnOCandidate(
            is_option_buy_entry=True,
            symbol="SBIN",
            strike=800,
            option_type="PE",
            entry_min=True,
            entry_max=True,
        )
        self.assertIs(cand_fno.entry_min, True, "Pydantic must not coerce entry_min=True to 1.0")
        self.assertIs(cand_fno.entry_max, True, "Pydantic must not coerce entry_max=True to 1.0")
        self.assertFalse(cand_fno.is_strictly_valid()[0])

        fake_fno_client = MagicMock()
        fake_fno_client.models.generate_content.return_value = SimpleNamespace(parsed=cand_fno)
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_fno_client):
            sig_fno, reason_fno, _, code_fno, _ = parse_fno_gemini_with_meta(
                "BUY SBIN 800 PE premium 1", current_time=1000.0, msg_timestamp=995.0
            )
        self.assertIsNone(sig_fno, f"Must reject boolean entry bounds in F&O AI response ({reason_fno})")
        self.assertEqual(code_fno, DecisionReasonCode.AI_GROUNDING_FAILED.value)

        # 2. Mocked GeminiEquityCandidate with boolean entry_price on 'Accumulate SBIN entry 1'
        cand_eq = GeminiEquityCandidate(
            is_buy_entry=True,
            stock_symbol="SBIN",
            entry_price=True,
        )
        self.assertIs(cand_eq.entry_price, True, "Pydantic must not coerce entry_price=True to 1.0")
        self.assertFalse(cand_eq.is_strictly_valid()[0])

        fake_eq_client = MagicMock()
        fake_eq_client.models.generate_content.return_value = SimpleNamespace(parsed=cand_eq)
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_eq_client):
            sig_eq, reason_eq, _, code_eq, _ = parse_equity_gemini_with_meta(
                "Accumulate SBIN entry 1", current_time=1000.0, msg_timestamp=995.0
            )
        self.assertIsNone(sig_eq, f"Must reject boolean entry_price in Equity AI response ({reason_eq})")
        self.assertEqual(code_eq, DecisionReasonCode.AI_GROUNDING_FAILED.value)

        # 3. Raw JSON string with boolean numeric fields in resp.text is also rejected
        fake_json_client = MagicMock()
        fake_json_client.models.generate_content.return_value = SimpleNamespace(
            text='{"is_buy_entry": true, "stock_symbol": "SBIN", "entry_price": true}',
            parsed=GeminiEquityCandidate.model_validate_json('{"is_buy_entry": true, "stock_symbol": "SBIN", "entry_price": true}'),
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_json_client):
            sig_j, _, _, code_j, _ = parse_equity_gemini_with_meta(
                "Accumulate SBIN entry 1", current_time=1000.0, msg_timestamp=995.0
            )
        self.assertIsNone(sig_j)
        self.assertEqual(code_j, DecisionReasonCode.AI_GROUNDING_FAILED.value)

    # ------------------------------------------------------------------
    # v3.2 Audit Regression P1-04: Adaptive rule TIMEOUT/ERROR fails closed before Gemini
    # ------------------------------------------------------------------
    def test_v3_2_p1_04_adaptive_timeout_and_worker_error_never_bypassed_by_gemini(self):
        fake_ai = MagicMock()
        fake_ai.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiEquityCandidate(is_buy_entry=True, stock_symbol="SBIN", entry_price=800.0)
        )
        # 1. Equity route: adaptive TIMEOUT blocks immediately before Gemini
        with patch.object(
            AdaptiveRuleManager,
            "match_active_rules",
            return_value=(
                None,
                "EQ_ADAP@v1",
                "Adaptive rule timed out",
                {"worker_outcome": RegexEvalOutcome.TIMEOUT.value, "reason_code": DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value},
            ),
        ), patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_ai):
            dec_eq = parse_signal_brain(
                "Accumulate SBIN with entry 800",
                current_time=1000.0,
                msg_timestamp=995.0,
                journal_db=object(),
                enable_ai_fallback=True,
            )
        self.assertEqual(dec_eq.action, SignalAction.BLOCKED)
        self.assertEqual(dec_eq.source, "ADAPTIVE_REGEX")
        self.assertEqual(dec_eq.reason_code, DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value)
        self.assertEqual(fake_ai.models.generate_content.call_count, 0)

        # 2. Option route: adaptive TIMEOUT blocks immediately before Gemini
        with patch.object(
            AdaptiveRuleManager,
            "match_active_rules",
            return_value=(
                None,
                "OPT_ADAP@v1",
                "Adaptive option rule timed out",
                {"worker_outcome": RegexEvalOutcome.TIMEOUT.value, "reason_code": DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value},
            ),
        ), patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_ai):
            dec_opt = parse_signal_brain(
                "Fresh option call POLYCAB 8000 PUT OCT premium 95 to 100",
                current_time=1000.0,
                msg_timestamp=995.0,
                journal_db=object(),
                enable_ai_fallback=True,
            )
        self.assertEqual(dec_opt.action, SignalAction.BLOCKED)
        self.assertEqual(dec_opt.source, "ADAPTIVE_REGEX")
        self.assertEqual(dec_opt.reason_code, DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value)
        self.assertEqual(fake_ai.models.generate_content.call_count, 0)

        # 3. Inside match_active_rules: first rule worker failure stops immediately and cannot be masked by rule 2 success
        with tempfile.TemporaryDirectory() as tmp:
            db = JournalDB(str(Path(tmp) / "p104.sqlite"), account_id="offline-test")
            mgr = AdaptiveRuleManager(db)
            for idx in (1, 2):
                rid = f"R_{idx}"
                mgr.propose_rule(
                    rid,
                    "EQUITY",
                    rf"LONG\s+TICKER\s*::\s*(?P<sym>[A-Z]+)\s+ENTRY\s*::\s*@?(?P<e1>\d+)",
                    sample_positives=[{"text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800", "expected": {"symbol": "SBIN", "stated_price": 800.0}}],
                )
                mgr.evaluate_rule(rid)
                mgr.approve_rule(rid, reviewer="Udit")
                mgr.activate_rule(rid, actor="Udit")

            calls = {"n": 0}
            def _first_timeout_second_match(pattern, text, timeout_sec=0.25, use_subprocess=True):
                calls["n"] += 1
                if calls["n"] == 1:
                    return RegexEvalOutcome.TIMEOUT.value, None, 50.0
                return RegexEvalOutcome.MATCH.value, {"sym": "SBIN", "e1": "800"}, 1.0

            with patch("src.parsers.adaptive_rules.safe_regex_search_bounded", side_effect=_first_timeout_second_match):
                sig_m, r_tag, _, ev_m = mgr.match_active_rules(
                    route="EQUITY",
                    text="Buy intent LONG TICKER :: SBIN ENTRY :: @800",
                    current_time=1000.0,
                    msg_timestamp=995.0,
                )
            self.assertIsNone(sig_m)
            self.assertEqual(calls["n"], 1, "Must fail closed on first rule TIMEOUT without evaluating second rule")
            self.assertEqual(ev_m.get("worker_outcome"), RegexEvalOutcome.TIMEOUT.value)

    # ------------------------------------------------------------------
    # v3.2 Audit Regression P1-05: Bounded in-flight Gemini workers & client cancellation
    # ------------------------------------------------------------------
    def test_v3_2_p1_05_bounded_inflight_gemini_workers_never_accumulate(self):
        import threading as _th
        from src.parsers.gemini_fallback import _call_with_hard_deadline, get_active_gemini_worker_count

        release_evt = _th.Event()
        def _stuck_callable():
            release_evt.wait(5.0)
            return "completed"

        outcomes = []
        for _ in range(5):
            ok, res, err = _call_with_hard_deadline(_stuck_callable, 0.025)
            outcomes.append((ok, err))

        self.assertTrue(all(not ok and isinstance(err, TimeoutError) for ok, err in outcomes))
        self.assertEqual(
            get_active_gemini_worker_count(),
            1,
            "Repeated timeouts must never spawn more than MAX_INFLIGHT_GEMINI_CALLS (1) worker thread!",
        )
        release_evt.set()
        time.sleep(0.04)
        self.assertEqual(get_active_gemini_worker_count(), 0)

    # ------------------------------------------------------------------
    # v3.2 Audit Regression P1-06 & P2-07: Explicit replay approval & PARSER_VERSION
    # ------------------------------------------------------------------
    def test_v3_2_p1_06_and_p2_07_strict_replay_exceptions_and_parser_version(self):
        from src.parsers.brain_validation import PARSER_VERSION
        self.assertEqual(PARSER_VERSION, "BRAIN-V14.3.2")
        dec = parse_signal_brain("Buy #SBIN @ 800", current_time=1000.0, msg_timestamp=995.0, enable_ai_fallback=False)
        self.assertEqual(dec.parser_version, "BRAIN-V14.3.2")

        # Unapproved change (when approved_exceptions={}) is flagged as unverified & unexpected_regression
        rep_unapproved = compare_corpus_replay(["BUY SBIN at - 800"], approved_exceptions={})
        self.assertEqual(rep_unapproved["expected_safety_correction_count"], 0)
        self.assertEqual(rep_unapproved["unverified_count"], 1)
        self.assertEqual(rep_unapproved["unexpected_regression_count"], 1)

        # Mismatched approved spec (e.g. expecting BUY_ENTRY when patched is BLOCKED) is flagged as unexpected_regression
        rep_mismatch = compare_corpus_replay(
            ["BUY SBIN at - 800"],
            approved_exceptions={
                "BUY SBIN at - 800": {
                    "diff_type": "changed_price",
                    "expected_route": "EQUITY",
                    "expected_action": "BUY_ENTRY",
                    "expected_price": 0.0,
                }
            },
        )
        self.assertEqual(rep_mismatch["expected_safety_correction_count"], 0)
        self.assertEqual(rep_mismatch["unexpected_regression_count"], 1)


if __name__ == "__main__":
    unittest.main()
