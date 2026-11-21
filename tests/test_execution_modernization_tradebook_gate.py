"""Actual StartupReconciliationGate integration: malformed late broker trade
must not write the earlier valid fill to the canonical journal.

This is a narrow, hermetic test of the REAL recovery method. Broker and
journal boundaries are mocked; no live Dhan/Telegram/API/SQLite mutations.
"""
import unittest
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from src.engine.reconciliation import (
    ReconciliationState, StartupReconciliationGate,
)

GOOD = {
    "dhanClientId":"ACC","orderId":"O1","exchangeTradeId":"E1",
    "transactionType":"BUY","tradedQuantity":2,"tradedPrice":100.0,
    "securityId":"SEC","exchangeSegment":"NSE_EQ","productType":"CNC",
    "exchangeTime":"2026-11-21 09:31:00",
}


class TestRecoveryBatchPreflight(unittest.TestCase):
    def _gate(self, book):
        gate=object.__new__(StartupReconciliationGate)
        gate.db=SimpleNamespace(
            account_id="ACC",record_fill=Mock(),
        )
        gate.broker=SimpleNamespace(
            cfg=SimpleNamespace(dhan_client_id="ACC",trading_mode="TEST"),
            get_fund_limits=Mock(return_value=(True,10000,"OK")),
            get_trade_book=Mock(),
        )
        gate.stop_manager=SimpleNamespace(is_stopped=Mock(return_value=False))
        gate.watcher=SimpleNamespace()
        gate._restore_committed_historical_watcher_records=Mock()
        gate._read_required_broker_collection=Mock(
            side_effect=lambda name, label: (True,book,"OK")
        )
        gate.last_error=""
        gate._entry_state_lock=threading.RLock()
        gate._halt_generation=0
        gate._accepting_entries=False
        return gate

    def test_late_conflicting_execution_blocks_before_any_fill_write(self):
        conflicting={**GOOD,"orderId":"OTHER"}
        gate=self._gate([GOOD,conflicting])
        result, reason=gate._run_startup_reconciliation_inner(0)
        self.assertFalse(result)
        self.assertIn("tradebook preflight blocked",reason.lower())
        self.assertEqual(gate.state,ReconciliationState.BLOCKED)
        gate.db.record_fill.assert_not_called()
        self.assertEqual(gate._read_required_broker_collection.call_count,1)

    def test_late_malformed_trade_blocks_before_any_fill_write(self):
        gate=self._gate([GOOD,{**GOOD,"exchangeTradeId":"E2","tradedPrice":"nan"}])
        result, reason=gate._run_startup_reconciliation_inner(0)
        self.assertFalse(result)
        self.assertIn("preflight",reason.lower())
        gate.db.record_fill.assert_not_called()

    def test_other_account_trade_blocked_before_first_fill(self):
        gate=self._gate([GOOD,{**GOOD,"exchangeTradeId":"E2",
                               "dhanClientId":"WRONG"}])
        result, reason=gate._run_startup_reconciliation_inner(0)
        self.assertFalse(result)
        self.assertIn("preflight",reason.lower())
        gate.db.record_fill.assert_not_called()
