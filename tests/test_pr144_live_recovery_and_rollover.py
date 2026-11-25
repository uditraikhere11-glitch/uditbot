"""PR144 production-shape replay. No live broker, account, service, or journal."""
import asyncio
from datetime import datetime
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

from src.broker.read_governor import ReadGovernor
from src.engine.market import IST
from src.engine.owner_lot_evidence import GATE_BOOKS
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.models import BrokerReadFailure, BrokerReadStatus
from tests import test_t0_signed_cnc_sibling as fixture


class LiveRecoveryAndRollover(unittest.TestCase):
    def setUp(self):
        self.wall = datetime(2026, 11, 25, 9, 16, 13, tzinfo=IST).timestamp()
        self.addCleanup(patch.stopall)
        patch("time.time", side_effect=lambda: self.wall).start()
        test = self

        class ReplayDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(test.wall, tz)

        patch("src.engine.unified_exit_reconciliation.datetime", ReplayDateTime).start()
        patch("src.engine.closed_parent_safety.datetime", ReplayDateTime).start()
        # Deterministic verified calendar, not the machine's mutable calendar.
        patch("src.engine.market.load_holidays_verified",
              return_value=(True, set(), "offline calendar")).start()
        self.f = fixture.SignedCNCT0ClosedSibling(methodName="runTest")
        self.f.TARGET_CHILD = "22126112566908"
        self.f.SELL_TRADE = "600252830"
        self.f.GROUP = "23132611231775"
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.f._archived_rollup_schema()
        self.f.broker.get_trade_book.return_value[0]["exchangeTime"] = "2026-11-25 09:16:04"
        self.f.sell["exchangeTime"] = "2026-11-25 09:16:04"
        self.f.broker.get_trades_for_order.side_effect = lambda oid: (
            BrokerReadFailure(status=BrokerReadStatus.FAILED.value,
                              error="offline obsolete original BUY", status_code=404)
            if oid in (self.f.FIRST, self.f.SECOND) else []
        )
        self.move(25, 23, 57, 22)
        self.gate = StartupReconciliationGate(self.f.broker, self.f.db, self.f.watcher,
                                               NS(is_stopped=lambda: False))
        self.f.broker.reconciliation_gate = self.gate
        # Explicit gate passes are deterministic. No detached retry worker is
        # allowed to outlive the isolated fixture or mask a failed assertion.
        self.gate._historical_background_retry_after = float("inf")
        self.gate.halt_new_entries = Mock(wraps=self.gate.halt_new_entries)
        self.assertIsNone(GATE_BOOKS.get())
        with self.f.db._get_conn() as conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")

    def move(self, day, hour, minute, second=0):
        self.wall = datetime(2026, 11, day, hour, minute, second, tzinfo=IST).timestamp()
        self.f.now = self.wall

    def healthy(self, evidence=None):
        self.assertEqual(self.gate.state, ReconciliationState.HEALTHY, self.gate.last_error)
        self.assertTrue(self.gate.is_accepting_entries, self.gate.last_error)
        self.f.assert_retained()
        for oid in (self.f.FIRST, self.f.SECOND):
            rec = self.f.watcher.terminal_orders[oid]
            self.assertEqual(rec["position_state"], "UNPROTECTED_MANUAL_RISK")
            self.assertFalse(rec.get("external_unlinked_sell_observed"))
            self.assertFalse(rec.get("reconciliation_required"))
            self.assertFalse(rec.get("unreadable_broker_response"))
            self.assertFalse(rec.get("owner_evidence_error"))
            if evidence:
                self.assertEqual(rec["broker_quantity_evidence"], evidence)
        self.assertIsNone(GATE_BOOKS.get())

    def startup(self, evidence="VERIFIED_SIGNED_CNC_DELIVERY"):
        ok, detail = self.gate.run_startup_reconciliation()
        self.assertTrue(ok, detail)
        self.healthy(evidence)

    def poll(self, evidence="VERIFIED_SIGNED_CNC_DELIVERY"):
        self.gate.halt_new_entries.reset_mock()
        self.f.watcher.check_once()
        self.gate.halt_new_entries.assert_not_called()
        self.healthy(evidence)

    def overnight(self):
        self.move(26, 0, 8, 10)

    def settle(self):
        self.move(26, 8, 45)
        self.f.history.append(dict(self.f.sell))
        child = dict(self.f.broker.get_order_book.return_value[0])
        self.f.broker.get_order_book.return_value = []
        self.f.broker.order_status = Mock(side_effect=lambda oid: dict(child)
                                           if oid == self.f.TARGET_CHILD else None)
        self.f.broker.get_positions.return_value = []
        self.f.broker.get_trade_book.return_value = []
        self.f.broker.get_holdings.return_value[0].update(totalQty=2, dpQty=2, t1Qty=0, availableQty=2)

    def test_a_b_c_d_complete_replay(self):
        self.startup()
        self.move(25, 23, 57, 24)
        self.f.broker.get_historical_trades.reset_mock()
        self.poll()
        self.f.broker.get_historical_trades.assert_not_called()
        self.overnight()
        self.poll()
        for oid in (self.f.FIRST, self.f.SECOND):
            self.assertEqual(self.f.watcher.terminal_orders[oid]["owner_history_day"], "2026-11-25")
        self.startup()
        self.poll()
        self.settle()
        self.startup("VERIFIED_SETTLED_HOLDINGS")
        self.poll("VERIFIED_SETTLED_HOLDINGS")
        self.assertTrue(self.f._closed_parent_stop_proof())

    def blocked(self, *, startup=False):
        if startup:
            ok, _ = self.gate.run_startup_reconciliation()
            self.assertFalse(ok)
        else:
            self.f.watcher.check_once()
        self.assertEqual(self.gate.state, ReconciliationState.BLOCKED)
        self.assertFalse(self.gate.is_accepting_entries)
        # An invalid broker response may be rejected before the lifecycle is
        # written; never interpret a remaining healthy label as gate admission.
        self.assertFalse(self.f.broker.place_forever_oco.called)
        self.assertFalse(self.f.broker.cancel_forever_order.called)
        self.assertFalse(self.f.broker.place_super_order.called)
        self.assertFalse(self.f.broker.cancel_order.called)

    def unknown_sell(self):
        self.f.broker.get_trade_book.return_value.append(
            dict(self.f.sell, orderId="OFFLINE-UNKNOWN", exchangeTradeId="OFFLINE-UNKNOWN-EID",
                 tradedQuantity=1))

    def test_unknown_sell_blocks_startup(self):
        self.unknown_sell()
        self.blocked(startup=True)

    def test_unknown_sell_blocks_routine(self):
        self.startup()
        self.unknown_sell()
        self.blocked()

    def test_unknown_sell_blocks_overnight(self):
        self.startup()
        self.overnight()
        self.unknown_sell()
        self.blocked()

    def mismatch(self, field, value):
        self.startup()
        self.f.broker.get_trade_book.return_value[0][field] = value
        self.blocked()

    def test_price_mismatch(self):
        self.mismatch("tradedPrice", 17.94)

    def test_quantity_mismatch(self):
        self.mismatch("tradedQuantity", 27)

    def test_account_mismatch(self):
        self.mismatch("dhanClientId", "WRONG-OFFLINE-ACCOUNT")

    def test_segment_mismatch(self):
        self.mismatch("exchangeSegment", "BSE_EQ")

    def test_blank_exchange_trade_id(self):
        self.mismatch("exchangeTradeId", "")

    def test_missing_exchange_time(self):
        self.mismatch("exchangeTime", None)

    def test_missing_owner_ack(self):
        self.startup()
        with self.f.db._get_conn() as conn:
            conn.execute("DELETE FROM manual_risk_acknowledgements WHERE parent_order_id=?",
                         (self.f.FIRST,))
        self.blocked()

    def test_mixed_date_overnight(self):
        self.startup()
        self.overnight()
        self.f.broker.get_trade_book.return_value.append(
            dict(self.f.sell, exchangeTime="2026-11-26 00:01:00"))
        self.blocked()

    def test_0830_stale_snapshot_blocks(self):
        self.startup()
        self.move(26, 8, 30)
        self.blocked()

    def governor_cycle(self, *, cooldown=False, final_short_cooldown=False, refill=False):
        self.startup()
        governor = ReadGovernor(self.f.root / "offline-governor", self.f.ACCOUNT,
                                clock=lambda: self.wall)
        self.f.broker._read_governor = governor
        for _ in range(20):
            governor.admit()
        if cooldown or final_short_cooldown:
            governor.throttled()
        self.wall += 59.7 if final_short_cooldown else 0.71
        self.f.now = self.wall
        self.f.watcher._startup_supervision_hint = None
        calls = []
        self.gate.halt_new_entries.reset_mock()

        async def sleep(delay):
            calls.append(delay)
            if len(calls) == 1 and 0 < delay <= 1.55:
                self.wall += delay
                self.f.now = self.wall
                if refill:
                    for _ in range(20):
                        governor.admit()
                return
            raise asyncio.CancelledError()

        with patch("src.engine.protection_watcher.is_lifecycle_open", return_value=True), \
             patch("src.engine.protection_watcher.asyncio.sleep", side_effect=sleep), \
             patch.object(self.f.watcher, "check_once", wraps=self.f.watcher.check_once) as poll:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(self.f.watcher.run(lambda: True))
            if cooldown or final_short_cooldown:
                poll.assert_not_called()
                self.gate.halt_new_entries.assert_called()
                self.assertEqual(len(calls), 1)
                self.assertFalse(self.gate.is_accepting_entries)
                self.assertFalse(self.f.watcher._supervision_read_complete)
            elif refill:
                poll.assert_not_called()
                self.gate.halt_new_entries.assert_called()
                self.assertEqual(len(calls), 2)
                self.assertFalse(self.gate.is_accepting_entries)
            else:
                poll.assert_called_once()
                self.gate.halt_new_entries.assert_not_called()
                self.assertEqual(len(calls), 2)
                self.assertAlmostEqual(calls[0], .35, delta=.02)
                self.healthy()

    def test_e_local_pacing_retries_once(self):
        self.governor_cycle()

    def test_e_real_429_cooldown_blocks(self):
        self.governor_cycle(cooldown=True)

    def test_e_last_subsecond_of_real_cooldown_blocks(self):
        self.governor_cycle(final_short_cooldown=True)

    def test_e_second_pacing_deferral_blocks(self):
        self.governor_cycle(refill=True)
