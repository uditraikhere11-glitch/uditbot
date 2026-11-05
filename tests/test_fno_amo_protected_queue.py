"""Offline F&O AMO queue and protected market-open dispatch tests.

No live broker/Telegram requests. Explicitly guard against naked F&O AMO orders.
"""
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.engine.fno_amo_queue import FnoAmoQueue, DEFAULT_MAX_AGE_SEC
from src.engine.market import IST
from src.main import TradingBotPipeline
from src.models import FnOSignal, SignalAction, SignalType, TradingMode, ValidationResult


def signal(channel=-100, msg=15, timestamp=1793869200.0):
    return FnOSignal(symbol="HAL", strike=4750, option_type="CE",
                     entry_min=88, entry_max=91, stop_loss=48, targets=[145, 240],
                     raw_text="#HAL 4750CE @88-91\nSL-48\nTarget-145,240",
                     channel_id=channel, message_id=msg, message_timestamp=timestamp)


class TestFnoAmoQueue(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.q=FnoAmoQueue(self.tmp.name,"1008885")
        self.t=1793869200.0

    def push(self, msg=15, age=DEFAULT_MAX_AGE_SEC):
        return self.q.enqueue(signal(msg=msg), "Trusted source", "S123",
                              "2026-11-28", self.t, age)

    def test_staging_not_broker_submission_and_no_duplicate(self):
        accepted, identifier=self.push()
        self.assertTrue(accepted)
        self.assertEqual(identifier,"-100:15")
        self.assertFalse(self.push()[0])
        rec=self.q.snapshot()[0]
        self.assertEqual(rec["state"],"QUEUED")
        self.assertEqual(rec["symbol"],"HAL")
        self.assertNotIn("raw_text",rec)
        self.assertEqual(os.stat(self.q.path).st_mode & 0o777,0o600)

    def test_claim_is_atomic_and_never_repeated(self):
        self.push()
        first=self.q.claim_one(self.t+12)
        self.assertEqual(first["item_id"],"-100:15")
        self.assertIsNone(self.q.claim_one(self.t+13))
        self.q.finish(first["item_id"],"UNCERTAIN","Dhan timeout, reconcile manually")
        self.assertIsNone(self.q.claim_one(self.t+14))
        self.assertEqual(self.q.snapshot()[0]["state"],"UNCERTAIN")

    def test_unfinished_dispatch_survives_restart_without_resubmission(self):
        self.push()
        first=self.q.claim_one(self.t+10)
        self.assertIsNotNone(first)
        reloaded=FnoAmoQueue(self.tmp.name,"1008885")
        self.assertIsNone(reloaded.claim_one(self.t+11))
        self.assertEqual(reloaded.snapshot()[0]["state"],"DISPATCHING")

    def test_expired_automatically_blocked(self):
        self.push(age=80)
        self.assertIsNone(self.q.claim_one(self.t+81))
        self.assertEqual(self.q.snapshot()[0]["state"],"EXPIRED")

    def test_local_cancel_prevents_dispatch(self):
        self.push()
        self.assertTrue(self.q.cancel_queued("-100:15"))
        self.assertFalse(self.q.cancel_queued("-100:15"))
        self.assertIsNone(self.q.claim_one(self.t+10))
        self.assertEqual(self.q.snapshot()[0]["state"],"CANCELLED")

    def test_contract_competition_and_account_scoping(self):
        self.push()
        self.assertFalse(self.push(msg=16)[0])
        other=FnoAmoQueue(self.tmp.name,"another-account")
        self.assertEqual(other.snapshot(),[])

    def test_unverified_and_unbounded_queue_rejected(self):
        self.assertFalse(self.q.enqueue(signal(channel=0), "X", "S1", "2026-11-28", self.t)[0])
        self.assertFalse(self.q.enqueue(signal(), "X", "", "2026-11-28", self.t)[0])
        self.assertFalse(self.q.enqueue(signal(), "X", "S1", "2026-11-28", self.t, 86400)[0])

    def test_fno_amo_settings_default_off(self):
        cfg=AppConfig.from_env(env={},env_file="")
        self.assertFalse(cfg.fno_amo_enabled)
        self.assertEqual(cfg.fno_amo_max_age_sec,64800)
        self.assertTrue(AppConfig.from_env(env={"FNO_AMO_ENABLED":"true"},env_file="").fno_amo_enabled)


class TestFnoAmoExecutor(unittest.TestCase):
    def make_pipeline(self, now):
        cfg=AppConfig.from_env(env={
            "TRADING_MODE":"LIVE","FNO_ACTIVE":"true","FNO_AMO_ENABLED":"true",
            "DHAN_CLIENT_ID":"1008885","MONITORED_CHANNEL_IDS":"-100",
            "FNO_CAPITAL_PER_TRADE":"19500",
        },env_file="")
        cfg.data_dir=self.tmp.name
        pipe=object.__new__(TradingBotPipeline)
        pipe.cfg=cfg
        pipe._pipeline_lock=threading.Lock()
        pipe._amo_queue=FnoAmoQueue(self.tmp.name,account_id=cfg.dhan_client_id)
        pipe.db=MagicMock()
        pipe.dedup=MagicMock()
        pipe.dedup.is_duplicate.return_value=(False,"")
        pipe.resolver=MagicMock()
        pipe.resolver.resolve_option.return_value=(SimpleNamespace(
            symbol="HAL", security_id="S123", exchange_segment="NSE_FNO",
            expiry=date(2026,11,28), tick_size=0.05, lot_size=75,
            option_type="CE", strike=4750, trading_symbol="HAL28NOV4750CE"
        ),"resolved")
        pipe.broker=MagicMock()
        pipe.watcher=MagicMock()
        pipe.stop_manager=MagicMock()
        pipe.stop_manager.is_stopped.return_value=False
        pipe.reconciliation_gate=MagicMock()
        pipe.reconciliation_gate.is_accepting_entries=True
        pipe.dispatch_notification=MagicMock()
        return pipe

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.t=datetime(2026,11,5,17,0,tzinfo=IST).timestamp()

    def test_closed_market_locally_queues_without_placing_order(self):
        pipe=self.make_pipeline(self.t)
        with patch("src.main.market_status",return_value=(False,"Market Closed Post-Hours")):
            result=pipe._handle_fno_signal(signal(timestamp=self.t), "trusted", self.t)
        self.assertTrue(result.is_valid,result.reason)
        self.assertIn("LOCALLY",result.reason)
        self.assertEqual(pipe._amo_queue.snapshot()[0]["state"],"QUEUED")
        pipe.broker.get_fund_limits.assert_not_called()
        pipe.broker.place_order.assert_not_called()
        pipe.broker.place_super_order.assert_not_called()

    def test_fno_amo_disabled_keeps_original_market_close_behavior(self):
        pipe=self.make_pipeline(self.t)
        pipe.cfg.fno_amo_enabled=False
        pipe.broker.get_fund_limits.return_value=(True,200000,"OK")
        pipe.risk_manager=MagicMock()
        pipe.risk_manager.validate_order_risk.return_value=(True,"OK")
        with patch("src.main.market_status",return_value=(False,"Market Closed Post-Hours")):
            result=pipe._handle_fno_signal(signal(timestamp=self.t), "trusted",self.t)
        self.assertFalse(result.is_valid)
        pipe.broker.place_order.assert_not_called()
        pipe.broker.place_super_order.assert_not_called()

    def test_market_open_claim_dispatches_one_via_existing_protected_path(self):
        pipe=self.make_pipeline(self.t)
        pipe._amo_queue.enqueue(signal(timestamp=self.t), "trusted", "S123",
                                "2026-11-28", self.t)
        mock_result=ValidationResult(is_valid=True,action=SignalAction.BUY_ENTRY,
                                      signal_type=SignalType.OPTION,
                                      reason="LIVE Order placed on Dhan: PROTECTED-123")
        with patch("src.main.market_status",return_value=(True,"OPEN")), \
             patch("src.main.time.time",return_value=self.t+10), \
             patch.object(pipe,"_handle_fno_signal",return_value=mock_result) as protected:
            self.assertTrue(pipe.dispatch_due_fno_amo())
            self.assertFalse(pipe.dispatch_due_fno_amo())
        self.assertEqual(protected.call_count,1)
        opts=protected.call_args.kwargs
        self.assertTrue(opts["amo_dispatch"])
        self.assertEqual(opts["expected_amo_contract_id"],"S123")
        self.assertEqual(pipe._amo_queue.snapshot()[0]["state"],"SUBMITTED")

    def test_ambiguous_dhan_reply_never_auto_retries(self):
        pipe=self.make_pipeline(self.t)
        pipe._amo_queue.enqueue(signal(timestamp=self.t), "trusted", "S123",
                                "2026-11-28", self.t)
        failure=ValidationResult(is_valid=False,action=SignalAction.BLOCKED,
                                 reason="Dhan connection timed out")
        with patch("src.main.market_status",return_value=(True,"OPEN")), \
             patch("src.main.time.time",return_value=self.t+10), \
             patch.object(pipe,"_handle_fno_signal",return_value=failure) as executor:
            self.assertTrue(pipe.dispatch_due_fno_amo())
            self.assertFalse(pipe.dispatch_due_fno_amo())
        self.assertEqual(executor.call_count,1)
        self.assertEqual(pipe._amo_queue.snapshot()[0]["state"],"UNCERTAIN")

    def test_disabled_or_closed_does_not_claim(self):
        pipe=self.make_pipeline(self.t)
        pipe._amo_queue.enqueue(signal(timestamp=self.t), "trusted", "S123",
                                "2026-11-28", self.t)
        with patch("src.main.market_status",return_value=(False,"Market Closed Post-Hours")):
            self.assertFalse(pipe.dispatch_due_fno_amo())
        self.assertEqual(pipe._amo_queue.snapshot()[0]["state"],"QUEUED")
        pipe.cfg.fno_amo_enabled=False
        self.assertFalse(pipe.dispatch_due_fno_amo())
        # Switching AMO OFF must invalidate staged work, not leave a latent entry.
        self.assertEqual(pipe._amo_queue.snapshot()[0]["state"],"CANCELLED")


if __name__=="__main__":
    unittest.main()
