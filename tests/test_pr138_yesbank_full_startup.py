"""Full, offline run_startup_reconciliation integration for YESBANK 1+1+28.

Uses real SQLite JournalDB, real ProtectionWatcher.check_once and actual
StartupReconciliationGate public entrypoint; does not mock the gate.
All external Dhan operations use synthetic Mock responses and mutation guards.
"""
import json
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock

from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from tests.test_ia001_yesbank_actual_watcher import ThreeParentRealWatcher as _Fixture


class FullStartupThreeParentYESBANK(unittest.TestCase):
    setUp = _Fixture.setUp
    _assert_no_mutation = _Fixture._assert_no_mutation

    def gate(self, *, stopped=False, db=None, watcher=None):
        self.broker.cfg = NS(
            dhan_client_id=self.ACCOUNT, trading_mode="LIVE",
            data_dir=str(self.root),
        )
        self.broker.get_fund_limits = Mock(return_value=(True, 20000.0, "OK"))
        gate = StartupReconciliationGate(
            self.broker, db or self.db, watcher or self.watcher,
            NS(is_stopped=lambda: bool(stopped)),
        )
        self.broker.reconciliation_gate = gate
        return gate

    def assert_lots(self, db=None):
        db = db or self.db
        rows = [db.get_position_lifecycle(p) for p in
                (self.FIRST, self.SECOND, self.THIRD)]
        self.assertEqual(
            [(r["confirmed_open_qty"], r["protected_qty"]) for r in rows],
            [(1, 0), (1, 0), (28, 28)],
        )
        for parent in (self.FIRST, self.SECOND):
            self.assertTrue(db.is_manual_risk_acknowledged(
                parent, parent, expected_account_id=self.ACCOUNT,
                expected_security_id=self.SECURITY, required_qty=1,
            ))
            self.assertEqual(db.get_protection_order_ids_for_parent(parent), [])
        self.assertEqual(db.get_protection_order_ids_for_parent(self.THIRD),
                         [self.GROUP])

    def test_full_startup_two_pass_recovery_and_restart_persistence(self):
        gate = self.gate()
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(
            [self.db.get_position_lifecycle(p)["position_state"]
             for p in (self.FIRST, self.SECOND)],
            ["RECONCILIATION_REQUIRED", "RECONCILIATION_REQUIRED"],
        )
        ok, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok, reason)
        self.assertEqual(gate.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate.is_accepting_entries, gate.last_error)
        self.assertGreaterEqual(self.broker.get_fund_limits.call_count, 2,
                                "Recovered journal state requires another full pass")
        self.assertGreaterEqual(self.broker.get_authenticated_profile_id.call_count, 2)
        self.assertGreaterEqual(self.broker.get_holdings.call_count, 2)
        self.assertGreaterEqual(self.broker.get_forever_orders.call_count, 2)
        for parent in (self.FIRST, self.SECOND):
            self.assertEqual(self.db.get_position_lifecycle(parent)["position_state"],
                             "UNPROTECTED_MANUAL_RISK")
        self.assert_lots()
        self._assert_no_mutation()

        restarted_db = JournalDB(str(self.root / "isolated-test-journal.db"),
                                 account_id=self.ACCOUNT)
        restarted_watcher = ProtectionWatcher(
            self.broker, journal_db=restarted_db,
            path=str(self.root / "pending_protection.json"),
            dry_run=True, clock=lambda: self.now,
        )
        gate2 = self.gate(db=restarted_db, watcher=restarted_watcher)
        ok2, reason2 = gate2.run_startup_reconciliation()
        self.assertTrue(ok2, reason2)
        self.assertTrue(gate2.is_accepting_entries)
        self.assert_lots(restarted_db)
        self._assert_no_mutation()

    def test_full_startup_rejects_missing_ack(self):
        with self.db._get_conn() as con:
            con.execute("DELETE FROM manual_risk_acknowledgements WHERE parent_order_id=?",
                        (self.SECOND,))
        gate = self.gate()
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(self.db.get_position_lifecycle(self.SECOND)["position_state"],
                         "RECONCILIATION_REQUIRED")
        self._assert_no_mutation()

    def test_full_startup_rejects_short_demat(self):
        self.broker.get_holdings.return_value = [dict(
            securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
            isin="SYNTHETIC-YESBANK-ISIN", totalQty=29, dpQty=29, t1Qty=0,
        )]
        gate = self.gate()
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertFalse(gate.is_accepting_entries)
        self._assert_no_mutation()

    def test_full_startup_rejects_wrong_forever_quantity(self):
        self.forever[0].update(quantity=27, quantity1=27)
        gate = self.gate()
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertFalse(gate.is_accepting_entries)
        self._assert_no_mutation()

    def test_full_startup_rejects_unavailable_holdings(self):
        self.broker.get_holdings.return_value = None
        gate = self.gate()
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertFalse(gate.is_accepting_entries)
        self._assert_no_mutation()

    def test_full_startup_honors_emergency_stop(self):
        gate = self.gate(stopped=True)
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertIn("STOP", reason.upper())
        self.assertFalse(gate.is_accepting_entries)
        self._assert_no_mutation()

    def test_owner_stop_fences_previously_healthy_startup(self):
        gate = self.gate()
        ok, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok, reason)
        self.assertTrue(gate.is_accepting_entries)
        (self.root / "service_control_state.json").write_text(json.dumps({
            "desired_service_state": "STOPPED", "observed_state": "RUNNING",
            "deferred_stop_pending": True,
        }))
        self.assertFalse(gate.is_accepting_entries)
        self.assert_lots()
        self._assert_no_mutation()


if __name__ == "__main__":
    unittest.main()
