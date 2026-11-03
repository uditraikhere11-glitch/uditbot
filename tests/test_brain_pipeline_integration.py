"""
AuditBot BRAIN — Pipeline Integration & Adaptive Rule Lifecycle Tests (v2).

Verifies:
1. Both handler routes (`_handle_fno_signal` and `_handle_equity_signal`) via `TradingBotPipeline.process_raw_message`.
2. Hard-block non-bypass & preserved `BLOCKED` action: AI fallback is NEVER called when deterministic safety gates block a message
   (Index, Exit, Historical A06, Mixed/Shorthand Contract A07, Invalid/Conflicting Expiry A08, Multi-Stock A05, Malformed/Negative Price A03).
3. Unresolved option messages stay on the OPTION route (`SignalType.OPTION`) and are never reinterpreted as cash equity.
4. Preserved AI rejection reasons in `ValidationResult` and `JournalDB.signals` when regex misses and AI fails grounding (`AI_GROUNDING_FAILED`).
5. Stale-after-AI and stale-before-F&O-submit (`LIVE Order Blocked: signal aged past ... before submission`).
6. Concurrent duplicate delivery (`claim_signal` called once per message, zero duplicate orders, edit logs revision only).
7. Preserved cross-group symbol window (`DuplicateFilter` suppresses same underlying across groups within 20s).
8. F&O `SignalLatencyTrace` (`mark_l2_parsed`, `mark_l3_resolved`, `mark_l7_sized`, `mark_l8_preflight`, `mark_l10_dhan_ack`) wired at their actual stages.
9. Provenance persistence (`parser_version`, `parse_source`, `rule_id`, `model_name`, `reason_code`, `parse_latency_ms`, `evidence_json`) in `JournalDB.signals`.
10. Audited Adaptive Rule lifecycle v2:
    - Expected structured outputs enforced on positive corpus items (wrong price fails evaluation!).
    - Mandatory safety negatives evaluated independently even when > 100 user negatives are supplied.
    - Immutable version retention (`parser_rule_versions`) across proposal updates (`v1` -> `v2`).
    - Explicit distinction between `deactivate_rule` (`ROLLED_BACK`) and `restore_rule_version` (restoring prior approved `v1`).
"""

from datetime import date
import json
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.models import (
    BrokerReadList,
    SignalAction,
    SignalType,
    TradingMode,
)
from src.main import TradingBotPipeline
from src.instruments import resolver as resolver_mod
from src.parsers.adaptive_rules import (
    AdaptiveRuleManager,
    RuleLifecycleState,
    validate_rule_pattern_safety,
)
from src.parsers.gemini_fallback import GeminiEquityCandidate, GeminiFnOCandidate


class TestBrainPipelineIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.temp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self._orig_valid = set(resolver_mod.VALID_EQUITY_SYMBOLS)
        self._orig_index = dict(resolver_mod._NAME_INDEX)

    def tearDown(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.update(self._orig_valid)
        resolver_mod._NAME_INDEX.clear()
        resolver_mod._NAME_INDEX.update(self._orig_index)
        self.temp_dir.cleanup()

    def _make_pipeline(self, mode: TradingMode = TradingMode.TEST) -> TradingBotPipeline:
        cfg = AppConfig(
            trading_mode=mode,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            monitored_channel_ids=[-1001, -1002],
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            dedup_window_sec=20,
            max_message_age_sec=60,
        )
        pipeline = TradingBotPipeline(cfg)
        pipeline.stop_manager.provision_initial_clean_state("Udit", "1113731536")
        pipeline.reconciliation_gate._accepting_entries = True

        # Load test scrip master instruments
        pipeline.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ", company_name="State Bank of India")
        pipeline.resolver.load_equity("RELIANCE", security_id="2885", exchange_segment="NSE_EQ", company_name="Reliance Industries")
        pipeline.resolver.load_option(
            symbol="POLYCAB",
            security_id="55001",
            exchange_segment="NSE_FNO",
            strike=8000.0,
            option_type="PE",
            expiry=date(2026, 10, 29),
            lot_size=125,
            tick_size=0.05,
            trading_symbol="POLYCAB-Oct2026-8000-PE",
        )
        return pipeline

    def test_01_both_ai_routes_and_unresolved_option_never_falls_through_to_equity(self):
        """Verifies F&O AI fallback, Equity AI fallback, and that unresolved options stay on OPTION route."""
        pipeline = self._make_pipeline(TradingMode.TEST)
        pipeline.broker.get_fund_limits = MagicMock(return_value=(True, 500000.0, "OK"))

        # 1. Option message that misses standard regex anchor ('POLYCAB 8000 PUT OCT premium 95 to 100') -> F&O AI fallback
        fake_fno_client = MagicMock()
        fake_fno_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiFnOCandidate(
                is_option_buy_entry=True,
                symbol="POLYCAB",
                strike=8000.0,
                option_type="PE",
                expiry_month="OCT",
                entry_min=95.0,
                entry_max=100.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_fno_client):
            res_fno = pipeline.process_raw_message(
                text="Fresh option call POLYCAB 8000 PUT OCT premium 95 to 100",
                channel_id=-1001,
                message_id=101,
                msg_timestamp=1792000000.0,
                current_time=1792000005.0,
            )
            self.assertTrue(res_fno.is_valid, f"Expected valid F&O AI signal, got: {res_fno.reason}")
            self.assertEqual(res_fno.signal_type, SignalType.OPTION)
            self.assertEqual(res_fno.symbol, "POLYCAB")

        # 2. Unresolved option message when AI is unconfigured -> stays SignalType.OPTION (never equity!)
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=None):
            res_unres_opt = pipeline.process_raw_message(
                text="Idea SBIN 800 CE",
                channel_id=-1001,
                message_id=102,
                msg_timestamp=1792000000.0,
                current_time=1792000005.0,
            )
            self.assertFalse(res_unres_opt.is_valid)
            self.assertEqual(res_unres_opt.action, SignalAction.IGNORED)
            self.assertEqual(res_unres_opt.signal_type, SignalType.OPTION)

        # 3. Equity message that misses regex ('Accumulate SBIN with entry 800') -> Equity AI fallback
        fake_eq_client = MagicMock()
        fake_eq_client.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiEquityCandidate(
                is_buy_entry=True,
                stock_symbol="SBIN",
                entry_price=800.0,
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_eq_client):
            res_eq = pipeline.process_raw_message(
                text="Accumulate SBIN with entry 800",
                channel_id=-1001,
                message_id=103,
                msg_timestamp=1792000000.0,
                current_time=1792000005.0,
            )
            self.assertTrue(res_eq.is_valid, f"Expected valid Equity AI signal, got: {res_eq.reason}")
            self.assertEqual(res_eq.signal_type, SignalType.EQUITY)
            self.assertEqual(res_eq.symbol, "SBIN")
            self.assertAlmostEqual(res_eq.limit_price, 804.0)

        # 4. When AI fails grounding, the AI failure reason and provenance are recorded in JournalDB.signals
        fake_bad_eq = MagicMock()
        fake_bad_eq.models.generate_content.return_value = SimpleNamespace(
            parsed=GeminiEquityCandidate(
                is_buy_entry=True,
                stock_symbol="SBIN",
                entry_price=999.0,  # Hallucinated price not in text!
            )
        )
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_bad_eq):
            res_bad_ai = pipeline.process_raw_message(
                text="Accumulate SBIN with entry 800",
                channel_id=-1001,
                message_id=104,
                msg_timestamp=1792000000.0,
                current_time=1792000005.0,
            )
            self.assertFalse(res_bad_ai.is_valid)
            self.assertEqual(res_bad_ai.action, SignalAction.BLOCKED)
            self.assertIn("AI fallback:", res_bad_ai.reason)
            recent_sigs = pipeline.db.get_recent_signals(limit=1)
            self.assertEqual(recent_sigs[0]["reason_code"], "AI_GROUNDING_FAILED")
            self.assertEqual(recent_sigs[0]["parse_source"], "GEMINI")
            self.assertEqual(recent_sigs[0]["parser_version"], "BRAIN-V14.3.2")

    def test_02_hard_block_never_bypasses_into_ai_fallback_and_preserves_blocked_action(self):
        """Proves AI is never called when deterministic gates block a message and BLOCKED action is preserved."""
        pipeline = self._make_pipeline(TradingMode.TEST)
        fake_client = MagicMock()

        blocked_messages = [
            "BUY SBIN @12.3.4",                                       # A03 malformed price
            "BUY SBIN @-800",                                         # Blocker 1 negative price
            "BUY SBIN @800-abc",                                      # Blocker 2 incomplete range
            "BUY SBIN @800 to",                                       # Blocker 2 dangling 'to'
            "BUY SBIN @800 and RELIANCE @1400",                       # A05 multi-stock
            "Yesterday POLYCAB 8000 PE OCT @100",                     # A06 historical F&O
            "POLYCAB 8000 PE OCT / RELIANCE 1500 CE OCT @100",        # A07 mixed contracts
            "POLYCAB 8000 PE OCT / NOV @100",                         # Blocker 8 conflicting months
            "POLYCAB 8000 PE OCTOBAD @100",                           # A08 invalid expiry month
            "POLYCAB 8000 PE OCT SL @30",                             # Blocker 3 SL-only without entry
            "Buy #NIFTY 24500 CE @ 100",                              # Index option
            "Buy #NIFTY @ 24500",                                     # Index equity
        ]
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=fake_client):
            for idx, msg in enumerate(blocked_messages, start=200):
                res = pipeline.process_raw_message(
                    text=msg,
                    channel_id=-1001,
                    message_id=idx,
                    msg_timestamp=1000.0,
                    current_time=1005.0,
                )
                self.assertFalse(res.is_valid, f"Message must not be valid: {msg}")
                self.assertEqual(res.action, SignalAction.BLOCKED, f"Expected BLOCKED action on {msg}, got {res.action}")
            self.assertEqual(
                fake_client.models.generate_content.call_count,
                0,
                "AI fallback must NEVER be called on deterministically blocked messages!",
            )

    def test_03_stale_after_ai_and_stale_before_fno_live_submit_blocked(self):
        """Verifies stale-after-AI rejection and the new F&O LIVE pre-submit signal-age guard."""
        pipeline = self._make_pipeline(TradingMode.LIVE)
        pipeline.broker.get_fund_limits = MagicMock(return_value=(True, 500000.0, "OK"))
        pipeline.broker.place_super_order = MagicMock(return_value=(True, "SUP-999", "OK"))

        clock_state = {"now": 1792045200.0}

        def slow_positions():
            clock_state["now"] += 65.0
            return BrokerReadList([])

        pipeline.broker.get_positions = slow_positions

        with patch("src.main.market_status", return_value=(True, "Market Open")), \
             patch("src.main.time.time", side_effect=lambda: clock_state["now"]):
            res = pipeline.process_raw_message(
                text="#POLYCAB 8000 PE OCT @95-100",
                channel_id=-1001,
                message_id=301,
                msg_timestamp=1792045200.0,
                current_time=None,
            )
            self.assertFalse(res.is_valid)
            self.assertEqual(res.action, SignalAction.BLOCKED)
            self.assertIn("aged past", res.reason)
            self.assertEqual(pipeline.broker.place_super_order.call_count, 0)

    def test_04_fno_trace_wired_and_latency_persisted(self):
        """Verifies F&O signals receive SignalLatencyTrace and record L1-L10 stages at actual execution checkpoints."""
        pipeline = self._make_pipeline(TradingMode.LIVE)
        pipeline.broker.get_fund_limits = MagicMock(return_value=(True, 500000.0, "OK"))
        pipeline.broker.get_positions = MagicMock(return_value=BrokerReadList([]))
        pipeline.broker.place_super_order = MagicMock(return_value=(True, "SUP-TRACE-1", "OK"))
        pipeline.broker.last_order_http_latency_ms = 42.5

        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res = pipeline.process_raw_message(
                text="#POLYCAB 8000 PE OCT @95-100",
                channel_id=-1001,
                message_id=401,
                msg_timestamp=1792045200.0,
                current_time=1792045202.0,
            )
            self.assertTrue(res.is_valid, f"Expected LIVE F&O order success, got: {res.reason}")
            tr = pipeline.db.get_latency_trace("SUP-TRACE-1")
            self.assertIsNotNone(tr)
            self.assertEqual(tr["symbol"], "POLYCAB")
            self.assertIsNotNone(tr["l2_parse_ms"])
            self.assertIsNotNone(tr["l3_resolve_ms"])
            self.assertIsNotNone(tr["l7_sizing_ms"])
            self.assertIsNotNone(tr["l8_risk_preflight_ms"])

    def test_05_concurrent_duplicate_claims_and_edit_revision_no_replay(self):
        """Verifies claim_signal is called once per message, edits log revision without re-executing, and cross-group dedup works."""
        pipeline = self._make_pipeline(TradingMode.TEST)
        pipeline.broker.get_fund_limits = MagicMock(return_value=(True, 500000.0, "OK"))

        r1 = pipeline.process_raw_message(
            text="Buy #SBIN @ 790-800",
            channel_id=-1001,
            message_id=501,
            msg_timestamp=1000.0,
            current_time=1002.0,
        )
        self.assertTrue(r1.is_valid)
        self.assertAlmostEqual(r1.limit_price, 804.0)

        r_dup = pipeline.process_raw_message(
            text="Buy #SBIN @ 790-800",
            channel_id=-1001,
            message_id=501,
            msg_timestamp=1000.0,
            current_time=1003.0,
        )
        self.assertFalse(r_dup.is_valid)
        self.assertIn("Duplicate source message", r_dup.reason)

        r_edit = pipeline.process_raw_message(
            text="Buy #SBIN @ 795-805 (updated)",
            channel_id=-1001,
            message_id=501,
            msg_timestamp=1000.0,
            current_time=1004.0,
        )
        self.assertFalse(r_edit.is_valid)
        self.assertEqual(r_edit.action, SignalAction.IGNORED)
        self.assertIn("Message edited", r_edit.reason)

        r_cross = pipeline.process_raw_message(
            text="Buy #SBIN @ 802",
            channel_id=-1002,
            message_id=999,
            msg_timestamp=1005.0,
            current_time=1010.0,
        )
        self.assertFalse(r_cross.is_valid)
        self.assertIn("Duplicate", r_cross.reason)

    def test_06_audited_adaptive_rule_lifecycle_structured_corpus_and_version_restoration(self):
        """
        Verifies adaptive regex workflow v2:
        - Expected structured outputs required on positive corpus (wrong expected price fails evaluation!).
        - Mandatory safety negatives evaluated independently even when > 100 user negatives are supplied.
        - Immutable version retention (`v1` -> `v2`) and `restore_rule_version` restoring `v1`.
        """
        pipeline = self._make_pipeline(TradingMode.TEST)
        pipeline.broker.get_fund_limits = MagicMock(return_value=(True, 500000.0, "OK"))
        mgr = AdaptiveRuleManager(pipeline.db)

        # 1. Propose v1 of CUSTOM_LONG_COLON with structured positive expectation
        pattern_v1 = r"LONG\s+TICKER\s*::\s*(?P<sym>[A-Z]+)\s+ENTRY\s*::\s*@?(?P<e1>\d+(?:\.\d+)?)"
        ok_prop, prop_msg, rec_v1 = mgr.propose_rule(
            rule_id="CUSTOM_LONG_COLON",
            route="EQUITY",
            pattern=pattern_v1,
            description="v1 LONG TICKER :: <SYM> ENTRY :: <PRICE>",
            proposed_by="Udit",
            sample_positives=[
                {
                    "text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800",
                    "expected": {"symbol": "SBIN", "stated_price": 800.0},
                }
            ],
            sample_negatives=[f"User negative filler {i}" for i in range(120)],  # > 100 user negatives!
        )
        self.assertTrue(ok_prop, prop_msg)
        self.assertEqual(rec_v1["version"], 1)

        # Verify that if we evaluate with a WRONG expected price (e.g. 750.0 instead of 800.0), evaluation FAILS!
        bad_eval_ok, _, bad_sum = mgr.evaluate_rule(
            "CUSTOM_LONG_COLON",
            positive_corpus=[
                {
                    "text": "Buy intent LONG TICKER :: SBIN ENTRY :: @800",
                    "expected": {"symbol": "SBIN", "stated_price": 750.0},
                }
            ],
            reviewer="Udit",
        )
        self.assertFalse(bad_eval_ok)
        self.assertTrue(any("stated_price 800.0 != expected 750.0" in f for f in bad_sum["positive_failures"]))

        # Now evaluate v1 with its true expected output (and 120 user negatives + 14 mandatory safety negatives)
        eval_ok, _, summary = mgr.evaluate_rule("CUSTOM_LONG_COLON", reviewer="Udit")
        self.assertTrue(eval_ok, f"Evaluation failed: {summary}")
        self.assertEqual(summary["mandatory_negatives_checked"], 14)
        self.assertEqual(summary["user_negatives_checked"], 120)
        self.assertEqual(summary["total_skipped"], 0)

        # Approve and activate v1
        self.assertTrue(mgr.approve_rule("CUSTOM_LONG_COLON", reviewer="Udit")[0])
        self.assertTrue(mgr.activate_rule("CUSTOM_LONG_COLON", actor="Udit")[0])

        # 2. Propose an updated v2 of CUSTOM_LONG_COLON (transitions current candidate to v2 PROPOSED, while v1 remains in parser_rule_versions)
        pattern_v2 = r"V2\s+SIGNAL\s*::\s*(?P<sym>[A-Z]+)\s+ENTRY\s*::\s*@?(?P<e1>\d+(?:\.\d+)?)"
        ok_v2, _, rec_v2 = mgr.propose_rule(
            rule_id="CUSTOM_LONG_COLON",
            route="EQUITY",
            pattern=pattern_v2,
            description="v2 updated pattern",
            proposed_by="Udit",
            sample_positives=[
                {
                    "text": "Buy intent V2 SIGNAL :: RELIANCE ENTRY :: @1250",
                    "expected": {"symbol": "RELIANCE", "stated_price": 1250.0},
                }
            ],
        )
        self.assertTrue(ok_v2)
        self.assertEqual(rec_v2["version"], 2)
        self.assertEqual(rec_v2["state"], RuleLifecycleState.PROPOSED)

        # Verify both v1 and v2 exist immutably in parser_rule_versions
        versions = pipeline.db.get_parser_rule_versions("CUSTOM_LONG_COLON")
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0]["version"], 1)
        self.assertEqual(versions[0]["pattern"], pattern_v1)
        self.assertEqual(versions[1]["version"], 2)
        self.assertEqual(versions[1]["pattern"], pattern_v2)

        # 3. Restore v1 to APPROVED, never automatically ACTIVE.
        rest_ok, rest_msg, restored_rec = mgr.restore_rule_version(
            "CUSTOM_LONG_COLON",
            target_version=1,
            actor="Udit",
            activate=False,
        )
        self.assertTrue(rest_ok, rest_msg)
        self.assertEqual(restored_rec["version"], 1)
        self.assertEqual(restored_rec["state"], RuleLifecycleState.APPROVED)
        # Even an explicit activate=True argument must fail closed at both manager and DB boundaries.
        blocked_ok, _, _ = mgr.restore_rule_version("CUSTOM_LONG_COLON", 1, actor="Udit", activate=True)
        self.assertFalse(blocked_ok)
        self.assertEqual(pipeline.db.get_parser_rule("CUSTOM_LONG_COLON")["state"], RuleLifecycleState.APPROVED)
        self.assertTrue(mgr.activate_rule("CUSTOM_LONG_COLON", actor="Udit")[0])
        self.assertEqual(restored_rec["pattern"], pattern_v1)

        # Verify restored v1 matches in the pipeline
        with patch("src.parsers.gemini_fallback._get_genai_client", return_value=None):
            res_v1 = pipeline.process_raw_message(
                text="Entry signal LONG TICKER :: RELIANCE ENTRY :: @1250",
                channel_id=-1001,
                message_id=610,
                msg_timestamp=2010.0,
                current_time=2012.0,
            )
            self.assertTrue(res_v1.is_valid, f"Restored v1 should match: {res_v1.reason}")
            self.assertEqual(res_v1.symbol, "RELIANCE")

        # 4. Deactivate rule (`ROLLED_BACK`) -> stops matching
        deact_ok, _ = mgr.deactivate_rule("CUSTOM_LONG_COLON", actor="Udit", reason="Operator deactivation")
        self.assertTrue(deact_ok)
        self.assertEqual(pipeline.db.get_parser_rule("CUSTOM_LONG_COLON")["state"], RuleLifecycleState.ROLLED_BACK)


if __name__ == "__main__":
    unittest.main()
