"""Real SQLite WAL audit aggregates are durable; protected events stay append-only."""
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.journal.db import JournalDB

class HaltAggregation(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = str(Path(tmp.name) / "real_journal.db")
        self.db = JournalDB(self.path, account_id="SYNTH-HALT-ACCOUNT")
        self.gate = self._gate()

    def _gate(self):
        return StartupReconciliationGate(
            broker=SimpleNamespace(),journal_db=self.db,watcher=None,
            stop_manager=SimpleNamespace(is_stopped=lambda:False))

    def rows(self):
        with self.db._get_conn() as conn:
            halts=[dict(x) for x in conn.execute(
                "SELECT * FROM reconciliation_halt_aggregates ORDER BY id")]
            events=[dict(x) for x in conn.execute(
                "SELECT * FROM system_events WHERE event_type='RECONCILIATION_GATE_HALTED' ORDER BY id")]
        return halts, events

    def test_many_identical_halts_keep_gate_closed(self):
        with patch("src.journal.db.time.time",return_value=1000.0):
            for _ in range(1140):
                self.gate.halt_new_entries("Open holding BUY-A: exit evidence remains unresolved")
        self.assertEqual(self.gate._halt_generation,1140)
        self.assertEqual(self.gate.state,ReconciliationState.BLOCKED)
        self.assertFalse(self.gate._accepting_entries)
        groups,events=self.rows()
        self.assertEqual(len(groups),1)
        self.assertEqual(len(events),1)
        self.assertEqual(groups[0]["occurrence_count"],1140)
        self.assertEqual(groups[0]["first_seen_at"],1000.0)
        self.assertEqual(groups[0]["last_seen_at"],1000.0)
        # Reinstantiate SQLite handle AND gate to verify durable aggregation.
        self.db=JournalDB(self.path,account_id="SYNTH-HALT-ACCOUNT")
        self.gate=self._gate()
        with patch("src.journal.db.time.time",return_value=1002.0):
            self.gate.halt_new_entries("Open holding BUY-A: exit evidence remains unresolved")
        groups,events=self.rows()
        self.assertEqual(sum(g["occurrence_count"] for g in groups),1141)
        self.assertEqual(len(events),2)  # first halt of fresh process/state

    def test_changed_reason_and_periodic_summary_append_only(self):
        with patch("src.journal.db.time.time",return_value=1000.0):
            self.gate.halt_new_entries("Reason A")
            for _ in range(7): self.gate.halt_new_entries("Reason A")
            self.gate.halt_new_entries("Reason B")
        with patch("src.journal.db.time.time",return_value=1302.0):
            self.gate.halt_new_entries("Reason A")
        groups,events=self.rows()
        self.assertEqual([g["occurrence_count"] for g in groups],[8,1,1])
        self.assertEqual([e["description"] for e in events],["Reason A","Reason B","Reason A"])
        with self.db._get_conn() as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE system_events SET description='rewritten' WHERE id=?",(events[0]["id"],))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM system_events WHERE id=?",(events[0]["id"],))

    def test_gate_closed_even_if_telemetry_fails(self):
        with patch.object(self.db,"record_reconciliation_halt_occurrence",side_effect=sqlite3.OperationalError("database locked")):
            with patch.object(self.db,"record_system_event",side_effect=sqlite3.OperationalError("database locked")):
                self.gate.halt_new_entries("Synthetic telemetry failure")
        self.assertEqual(self.gate.state,ReconciliationState.BLOCKED)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_reopen_then_rehalt_is_new_immutable_event(self):
        with patch("src.journal.db.time.time",return_value=1000.0):
            self.gate.halt_new_entries("Reason A")
            self.gate._accepting_entries=True
            self.gate.state=ReconciliationState.HEALTHY
            self.gate.halt_new_entries("Reason A")
        groups,events=self.rows()
        self.assertEqual(len(groups),2)
        self.assertEqual(len(events),2)

if __name__=="__main__": unittest.main()
