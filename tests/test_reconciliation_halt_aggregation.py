"""Append-only halt logging should not produce one journal row per poll."""
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState

class HaltAggregation(unittest.TestCase):
    def setUp(self):
        self.db=SimpleNamespace(record_system_event=Mock())
        self.gate=StartupReconciliationGate(
            broker=SimpleNamespace(),journal_db=self.db,watcher=None,
            stop_manager=SimpleNamespace(is_stopped=lambda:False))
    def test_many_identical_halts_keep_gate_closed(self):
        with patch("src.engine.reconciliation.time.monotonic",return_value=101.0):
            for _ in range(1140):
                self.gate.halt_new_entries("Open holding BUY-A: exit evidence remains unresolved")
        self.assertEqual(self.gate._halt_generation,1140)
        self.assertEqual(self.gate.state,ReconciliationState.BLOCKED)
        self.assertFalse(self.gate._accepting_entries)
        self.assertEqual(self.db.record_system_event.call_count,1)
    def test_changed_reason_and_periodic_summary_append_only(self):
        with patch("src.engine.reconciliation.time.monotonic",return_value=10.0):
            self.gate.halt_new_entries("Reason A")
            for _ in range(7):self.gate.halt_new_entries("Reason A")
            self.gate.halt_new_entries("Reason B")
        with patch("src.engine.reconciliation.time.monotonic",return_value=311.0):
            self.gate.halt_new_entries("Reason A")
        names=[c.args[0] for c in self.db.record_system_event.call_args_list]
        self.assertEqual(names.count("RECONCILIATION_GATE_HALTED"),3)
        self.assertEqual(names.count("RECONCILIATION_GATE_HALTED_REPEAT_SUMMARY"),1)
    def test_reentry_into_blocked_is_new_event(self):
        with patch("src.engine.reconciliation.time.monotonic",return_value=10.0):
            self.gate.halt_new_entries("Reason A")
            self.gate._accepting_entries=True
            self.gate.state=ReconciliationState.HEALTHY
            self.gate.halt_new_entries("Reason A")
        self.assertEqual(self.db.record_system_event.call_count,2)

if __name__=="__main__":
    unittest.main()
