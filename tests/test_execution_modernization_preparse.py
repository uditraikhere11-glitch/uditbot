"""Offline opt-in deterministic preparse parity and serialized safety gates."""
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.engine.deterministic_preparse import (
    DeterministicPreparse, prepare_deterministic_buy,
)
from src.main import TradingBotPipeline
from src.models import SignalAction, TradingMode, ValidationResult


class TestDeterministicPreparse(unittest.TestCase):
    def setUp(self):
        self.now=time.time()
        self.prep=prepare_deterministic_buy(
            text="Buy #SBIN @805",channel_id=123,message_id=7,
            msg_timestamp=self.now,max_age_sec=120.0,
        )

    def make_pipeline(self):
        p=object.__new__(TradingBotPipeline)
        p._pipeline_lock=threading.Lock()
        p.cfg=SimpleNamespace(
            monitored_channel_ids=[123],
            trading_mode=TradingMode.TEST,
            test_group_id=None,
            enforce_source_allowlist=True,
            log_group_id=None,
            max_message_age_sec=120.0,
        )
        p.db=SimpleNamespace(
            claim_signal=Mock(return_value=(True,"CLAIMED",1)),
            record_signal=Mock(),
        )
        p._handle_equity_signal=Mock(return_value=ValidationResult(
            is_valid=True,action=SignalAction.BUY_ENTRY,reason="safe stub"
        ))
        return p

    def test_pure_regular_equity_entry_is_preparable(self):
        self.assertIsNotNone(self.prep)
        self.assertEqual(self.prep.decision.source,"REGEX")
        self.assertEqual(self.prep.decision.action,SignalAction.BUY_ENTRY)

    def test_valid_candidate_skips_parser_but_not_durable_claim(self):
        p=self.make_pipeline()
        with patch("src.main.parse_signal_brain",side_effect=AssertionError("reparse")):
            r=p.process_raw_message(
                "Buy #SBIN @805",channel_id=123,message_id=7,
                msg_timestamp=self.now,preparsed_brain=self.prep)
        self.assertTrue(r.is_valid)
        p.db.claim_signal.assert_called_once()
        p._handle_equity_signal.assert_called_once()

    def test_tampered_raw_text_triggers_original_full_brain(self):
        p=self.make_pipeline()
        with patch("src.main.parse_signal_brain",return_value=self.prep.decision) as m:
            p.process_raw_message(
                "Buy #SBIN @806",channel_id=123,message_id=7,
                msg_timestamp=self.now,preparsed_brain=self.prep)
        m.assert_called_once()

    def test_wrong_message_or_channel_cannot_reuse(self):
        for cid,mid in ((124,7),(123,8)):
            self.assertFalse(self.prep.usable_for(
                text="Buy #SBIN @805",channel_id=cid,message_id=mid,
                msg_timestamp=self.now,max_age_sec=120.0))

    def test_policy_drift_disables_preparation(self):
        self.assertFalse(self.prep.usable_for(
            text="Buy #SBIN @805",channel_id=123,message_id=7,
            msg_timestamp=self.now,max_age_sec=30.0))

    def test_injected_clock_never_reuses_external_preparse(self):
        self.assertFalse(self.prep.usable_for(
            text="Buy #SBIN @805",channel_id=123,message_id=7,
            msg_timestamp=self.now,max_age_sec=120.0,
            injected_clock=self.now))

    def test_duplicate_claim_still_blocks_even_prepared_entry(self):
        p=self.make_pipeline()
        p.db.claim_signal.return_value=(False,"DUPLICATE_MESSAGE_CLAIM",1)
        with patch("src.main.parse_signal_brain",side_effect=AssertionError("should block before parse")):
            r=p.process_raw_message(
                "Buy #SBIN @805",channel_id=123,message_id=7,
                msg_timestamp=self.now,preparsed_brain=self.prep)
        self.assertFalse(r.is_valid)
        p._handle_equity_signal.assert_not_called()

    def test_age_filter_still_blocks_prepared_entry(self):
        p=self.make_pipeline()
        with patch("src.main.parse_signal_brain",side_effect=AssertionError("should block before parse")):
            r=p.process_raw_message(
                "Buy #SBIN @805",channel_id=123,message_id=7,
                msg_timestamp=self.now-200,preparsed_brain=self.prep)
        self.assertFalse(r.is_valid)
        p._handle_equity_signal.assert_not_called()

    def test_unmatched_signal_never_bypasses_adaptive_ai(self):
        r=prepare_deterministic_buy(
            text="some arbitrary message",channel_id=123,message_id=7,
            msg_timestamp=self.now,max_age_sec=120.0)
        self.assertIsNone(r)

    def test_no_network_ai_or_db_preparse(self):
        with patch("src.engine.deterministic_preparse.parse_signal_brain",wraps=prepare_deterministic_buy.__globals__["parse_signal_brain"]) as f:
            prepare_deterministic_buy(text="Buy #SBIN @805",channel_id=123,
                message_id=7,msg_timestamp=self.now,max_age_sec=120.0)
        self.assertFalse(f.call_args.kwargs["enable_ai_fallback"])
        self.assertIsNone(f.call_args.kwargs["journal_db"])

    def test_no_timestamp_or_invalid_age_returns_none(self):
        for ts,age in ((0,120),(-1,120),(self.now,0),(self.now,-1)):
            self.assertIsNone(prepare_deterministic_buy(
                text="Buy #SBIN @805",channel_id=123,message_id=7,
                msg_timestamp=ts,max_age_sec=age))
