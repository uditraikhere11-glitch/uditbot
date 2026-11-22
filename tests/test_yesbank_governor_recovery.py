"""Governor cooldown must defer background recovery without dropping fail-closed gate."""
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
from src.broker.read_governor import ReadGovernor
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState

class GovernorRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now=1000.0
        self.gov=ReadGovernor(self.tmp.name,"SYNTH",clock=lambda:self.now)
        self.broker=NS(_read_governor=self.gov)
        self.db=NS(account_id="SYNTH",
            get_open_positions_lifecycle=lambda:[dict(parent_order_id="BUY-OLD",
                position_state="RECONCILIATION_REQUIRED",product_type="CNC",
                security_id="11915",confirmed_open_qty=1)],
            is_manual_risk_acknowledged=lambda *args,**kw:True,
            owner_lot_review_reason=lambda *args:None)
        self.gate=StartupReconciliationGate(self.broker,self.db,None,
            NS(is_stopped=lambda:False))
        self.gate.state=ReconciliationState.BLOCKED
        self.gate.recheck_and_recover_if_verified=Mock(return_value=(True,"verified"))
    def test_429_cooldown_then_serialized_recheck_without_restart(self):
        self.gov.throttled(15)
        with patch("src.engine.reconciliation.time.monotonic",return_value=200.0):
            self.gate._schedule_historical_background_recheck()
        self.assertEqual(self.gate.state,ReconciliationState.BLOCKED)
        self.assertFalse(self.gate._accepting_entries)
        self.gate.recheck_and_recover_if_verified.assert_not_called()
        self.assertFalse(self.gate._historical_background_lock.locked())
        self.now+=20
        with patch("src.engine.reconciliation.time.monotonic",return_value=400.0):
            with patch("src.engine.reconciliation.threading.Thread") as thread:
                thread.return_value.start.side_effect=lambda:thread.call_args.kwargs["target"]()
                self.gate._schedule_historical_background_recheck()
        self.gate.recheck_and_recover_if_verified.assert_called_once()
        self.assertFalse(self.gate._historical_background_lock.locked())
        self.assertGreaterEqual(self.gate._historical_background_retry_after,520.0)
    def test_invalid_governor_preview_cannot_schedule_recovery(self):
        self.gov.throttled(15)
        with patch.object(self.gov,"seconds_until_capacity",side_effect=ValueError("bad preview")):
            with patch("src.engine.reconciliation.time.monotonic",return_value=200.0):
                with patch("src.engine.reconciliation.threading.Thread") as thread:
                    self.gate._schedule_historical_background_recheck()
        thread.assert_not_called()
        self.gate.recheck_and_recover_if_verified.assert_not_called()
        self.assertFalse(self.gate._historical_background_lock.locked())
        self.assertFalse(self.gate._accepting_entries)

if __name__=="__main__":
    unittest.main()
