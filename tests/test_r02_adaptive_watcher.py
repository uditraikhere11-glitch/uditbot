"""R02 offline tests. Never call a real Dhan endpoint or submit an order."""
import asyncio
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadDeferred, ReadGovernor
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import ReconciliationState
from src.engine.watch_poll_policy import (
    watcher_cadence_seconds,
    supervision_read_requirement,
    POLL_HEARTBEAT_SECONDS,
)


def record(oid="SYNTH-1", state="UNPROTECTED_MANUAL_RISK"):
    return {
        "order_id": oid, "symbol": "YESBANK",
        "state": "TRADED", "filled_qty": 1, "qty": 1,
        "position_state": state, "created_at": 1.0,
    }


class R02CadencePolicyTests(unittest.TestCase):
    def test_manual_risk_is_still_open_but_uses_lower_get_cadence(self):
        hold = record()
        self.assertEqual(
            watcher_cadence_seconds(pending={}, open_records=[hold], now=100000, market_open=True),
            60.0,
        )
        self.assertEqual(hold["position_state"], "UNPROTECTED_MANUAL_RISK")

    def test_other_protected_or_unprotected_open_positions_still_supervised(self):
        for state in ("PROTECTED", "UNPROTECTED", "POSITION_OPEN", "UNKNOWN"):
            with self.subTest(state=state):
                self.assertEqual(
                    watcher_cadence_seconds(
                        pending={}, open_records=[record(state=state)], now=100000, market_open=True,
                    ), 45.0
                )

    def test_new_order_fast_burst_then_stale_order_backoff(self):
        fresh = {"created_at": 99990, "state": "ACKED"}
        self.assertEqual(
            watcher_cadence_seconds(
                pending={"SYNTH-2": fresh}, open_records=[], now=100000, market_open=True,
            ), POLL_HEARTBEAT_SECONDS
        )
        self.assertEqual(
            watcher_cadence_seconds(
                pending={"SYNTH-2": fresh}, open_records=[], now=100050, market_open=True,
            ), 75.0
        )

    def test_manual_risk_does_not_override_a_pending_order(self):
        self.assertEqual(
            watcher_cadence_seconds(
                pending={"NEW": {"created_at": 99999}},
                open_records=[record()],
                now=100000,
            ), 2.0
        )

    def test_closed_and_dry_records_not_treated_as_live_positions(self):
        self.assertEqual(
            watcher_cadence_seconds(
                pending={}, open_records=[
                    record(state="POSITION_CLOSED"), record("DRY-123")
                ], now=100000, market_open=True,
            ), 90.0
        )

    def test_after_hours_holdings_continue_to_be_monitored(self):
        for state in ("UNPROTECTED_MANUAL_RISK", "PROTECTED", "UNPROTECTED"):
            with self.subTest(state=state):
                self.assertEqual(
                    watcher_cadence_seconds(
                        pending={}, open_records=[record(state=state)],
                        now=100000, market_open=False,
                    ), 300.0
                )

    def test_both_hourly_and_24_hour_steady_state_budget(self):
        import math
        for state in ("PROTECTED", "UNPROTECTED_MANUAL_RISK"):
            market = watcher_cadence_seconds(
                pending={}, open_records=[record(state=state)],
                now=100000, market_open=True,
            )
            off = watcher_cadence_seconds(
                pending={}, open_records=[record(state=state)],
                now=100000, market_open=False,
            )
            # Five shared broker book snapshots per supervision pass.
            hourly = math.ceil(3600 / market) * 5
            daily = math.ceil(6.34 * 3600 / market) * 5 + math.ceil(17.66 * 3600 / off) * 5
            self.assertLess(hourly, 640)
            self.assertLess(daily, 4400)

    def test_stale_pending_order_slows_outside_lifecycle_hours(self):
        pending = {"SYNTH-2": {"created_at": 1}}
        self.assertEqual(
            watcher_cadence_seconds(
                pending=pending, open_records=[], now=100000, market_open=False,
            ), 300.0
        )

    def test_uncertain_cancel_or_protection_never_waits_five_minutes(self):
        for fields in (
            {"cancel_uncertain": True},
            {"cancel_requested": True},
            {"entry_timeout_elapsed": True},
            {"persistence_uncertain": True},
            {"reconciliation_required": True},
            {"retry_reserved": True},
            {"pending_oco": {"state": "UNKNOWN"}},
            {"pending_oco": {"state": "SUBMITTING"}},
        ):
            with self.subTest(fields=fields):
                self.assertEqual(
                    watcher_cadence_seconds(
                        pending={"UNRESOLVED": {"created_at": 1, **fields}},
                        open_records=[], now=100000, market_open=False,
                    ), 10.0
                )

    def test_read_requirement_is_bounded(self):
        self.assertEqual(supervision_read_requirement(0), 8)
        self.assertEqual(supervision_read_requirement(2), 12)
        self.assertEqual(supervision_read_requirement(1000), 40)


class R02GovernorPreviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100000.0
        self.g = ReadGovernor(self.tmp.name, "SYNTH-ACCOUNT", clock=lambda: self.now)
        self.g.LIMITS = ((3600, 10),)

    def count(self):
        with sqlite3.connect(self.g.path) as db:
            return db.execute("SELECT COUNT(*) FROM reads").fetchone()[0]

    def test_preview_never_reserves_or_deletes_admissions(self):
        for _ in range(5):
            self.g.admit()
        self.assertEqual(self.g.seconds_until_capacity(required=2, priority="routine"), 0)
        self.assertEqual(self.count(), 5)

    def test_routine_reserve_preserves_urgent_capacity(self):
        for _ in range(7):
            self.g.admit()
        self.assertGreater(self.g.seconds_until_capacity(required=2, priority="routine"), 0)
        self.assertEqual(self.g.seconds_until_capacity(required=2, priority="urgent"), 0)
        self.assertEqual(self.count(), 7)
        self.now += 3601
        self.assertEqual(self.g.seconds_until_capacity(required=2, priority="routine"), 0)

    def test_preview_observes_durable_cooldown_without_undoing_it(self):
        self.g.throttled(170)
        self.assertGreaterEqual(
            self.g.seconds_until_capacity(required=2, priority="urgent"), 170
        )
        with self.assertRaises(ReadDeferred):
            self.g.admit()
        self.now += 171
        self.assertEqual(self.g.seconds_until_capacity(required=2, priority="urgent"), 0)

    def test_corrupt_store_is_not_treated_as_full_capacity(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as db:
            db.execute("DROP TABLE reads")
        with self.assertRaises(sqlite3.Error):
            self.g.seconds_until_capacity(required=1)

    def test_separate_account_has_separate_capacity(self):
        for _ in range(10):
            self.g.admit()
        other = ReadGovernor(self.tmp.name, "OTHER", clock=lambda: self.now)
        self.assertEqual(other.seconds_until_capacity(required=5), 0)


class R02WatcherRunTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = time.time()
        self.g = ReadGovernor(self.tmp.name, "SYNTH-ACCOUNT")
        self.broker = NS(_read_governor=self.g)
        self.w = ProtectionWatcher(
            broker=self.broker, journal_db=None,
            path=str(Path(self.tmp.name) / "watcher.json"),
        )
        self.w.should_poll = lambda *_: True
        self.w.check_once = Mock(return_value=False)
        self.w.terminal_orders = {"SYNTH-1": record()}
        self.g.LIMITS = ((3600, 800),)

    async def drive(self, ticks, callback=None):
        completed = 0

        class StopLoop(Exception):
            pass

        async def sleep(_seconds):
            nonlocal completed
            completed += 1
            if callback:
                callback(completed)
            if completed >= ticks:
                raise StopLoop()

        with patch("src.engine.protection_watcher.asyncio.sleep", new=sleep):
            with self.assertRaises(StopLoop):
                await self.w.run()
        self.assertFalse(self.w._is_running)

    async def test_manual_risk_remains_monitored_without_five_gets_every_two_seconds(self):
        self.assertTrue(self.w.has_open_monitored_positions())
        await self.drive(7)
        self.assertEqual(self.w.check_once.call_count, 1)
        self.assertEqual(self.w.terminal_orders["SYNTH-1"]["position_state"],
                         "UNPROTECTED_MANUAL_RISK")

    async def test_new_pending_order_wakes_watcher_before_stale_interval(self):
        def add_order(n):
            if n == 2:
                self.w.pending["SYNTH-2"] = {
                    **record("SYNTH-2", state="ENTRY"),
                    "created_at": time.time(),
                }

        await self.drive(4, callback=add_order)
        self.assertEqual(self.w.check_once.call_count, 2)

    async def test_budget_exhaustion_fails_closed_and_does_not_poll(self):
        self.g.LIMITS = ((3600, 20),)
        for _ in range(15):
            self.g.admit()
        gate = NS(
            is_accepting_entries=True,
            state=ReconciliationState.HEALTHY,
            halt_new_entries=Mock(),
        )
        self.broker.reconciliation_gate = gate
        await self.drive(5)
        self.w.check_once.assert_not_called()
        gate.halt_new_entries.assert_called_once()

    async def test_governor_corruption_halts_without_sending_broker_gets(self):
        self.g.admit()
        with sqlite3.connect(self.g.path) as db:
            db.execute("DROP TABLE reads")
        gate = NS(
            is_accepting_entries=True,
            state=ReconciliationState.HEALTHY,
            halt_new_entries=Mock(),
        )
        self.broker.reconciliation_gate = gate
        await self.drive(3)
        self.w.check_once.assert_not_called()
        gate.halt_new_entries.assert_called_once()

    async def test_no_governor_support_preserves_test_watcher_liveness(self):
        self.broker._read_governor = None
        await self.drive(4)
        self.assertEqual(self.w.check_once.call_count, 1)


class R02GateHaltDedupTests(unittest.TestCase):
    def test_already_blocked_enum_does_not_record_redundant_halts(self):
        c = DhanClient.__new__(DhanClient)
        gate = NS(
            is_accepting_entries=False,
            state=ReconciliationState.BLOCKED,
            halt_new_entries=Mock(),
        )
        c.reconciliation_gate = gate
        for _ in range(10):
            c._halt_for_read_deferral()
        gate.halt_new_entries.assert_not_called()

    def test_healthy_gate_is_immediately_halted(self):
        c = DhanClient.__new__(DhanClient)
        gate = NS(
            is_accepting_entries=True,
            state=ReconciliationState.HEALTHY,
            halt_new_entries=Mock(),
        )
        c.reconciliation_gate = gate
        c._halt_for_read_deferral()
        gate.halt_new_entries.assert_called_once()


if __name__ == "__main__":
    unittest.main()
