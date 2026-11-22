"""Exact November 22 dual-parent YESBANK offline acceptance.

This module never makes Dhan/Telegram network requests or touches production
SQLite. Real JournalDB WAL, ProtectionWatcher, ReadGovernor and gate recovery
worker are exercised with account-bound, synthetic broker responses.
"""
import asyncio
from datetime import datetime
from pathlib import Path
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from src.broker.read_governor import ReadGovernor
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, BrokerReadStatus
from tests import test_historical_zero_trade_id_r01 as r01

ORDERS = ("221261102657708", "23126110439808")
NOW = datetime(2026, 11, 22, 18, 19, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
READ_NAMES = (
    "get_fund_limits", "get_trade_book", "get_positions", "get_authenticated_profile_id",
    "get_holdings", "get_order_book", "get_forever_orders", "get_historical_trades",
    "get_super_orders", "get_trades_for_order",
)

class SyntheticNov22Fixture(unittest.TestCase):
    def setUp(self):
        fx = self.fx = r01.R01HistoricalZeroTradeIdTests()
        fx.setUp()
        self.addCleanup(fx.doCleanups)
        fx.first_oid, fx.second_oid = ORDERS
        fx.first["orderId"] = ORDERS[0]
        fx.second["orderId"] = ORDERS[1]
        fx.broker.get_historical_trades.return_value = r01.as_read_list([
            fx._normalized(fx.first), fx._normalized(fx.second),
        ])
        for name in ("get_trade_book", "get_positions", "get_order_book",
                     "get_super_orders", "get_trades_for_order"):
            getattr(fx.broker,name).return_value = r01.as_read_list([])
        fx.broker.get_fund_limits.return_value = (True, 1000.0, "OK")
        fx._seed_two()
        for oid, correlation in zip(ORDERS, ("EQ-ALPHA-01", "EQ-BETA-02")):
            fx.db.acknowledge_manual_risk_holding(
                parent_order_id=oid, correlation_id=correlation,
                account_id=r01.ACCOUNT, symbol="YESBANK", security_id="11915", quantity=1,
                acknowledged_by="Synthetic Owner", reason="Synthetic offline audit risk acknowledgement",
            )
        _, pending, error = fx.gate._reconcile_unresolved_intents_via_historical_trades(
            fx.db.get_non_terminal_intents(), now_ts=NOW,
        )
        self.assertEqual((pending,error),([],None))
        self.now = NOW
        self.broker = fx.broker
        self.db_path = str(Path(fx.tmp.name) / "journal.db")
        self.watcher_path = str(Path(fx.tmp.name) / "watcher.json")
        with fx.db._get_conn() as conn:
            self.initial_ack_rows = [tuple(row) for row in conn.execute(
                "SELECT parent_order_id, correlation_id, account_id, quantity, status, acknowledged_at "
                "FROM manual_risk_acknowledgements ORDER BY parent_order_id"
            )]
        self.gov = ReadGovernor(str(Path(fx.tmp.name) / "governor"), r01.ACCOUNT,
                                clock=lambda:self.now)
        self.broker._read_governor = self.gov
        self.broker._get_active_read_governor = lambda:self.gov
        self.broker.cfg = SimpleNamespace(dhan_client_id=r01.ACCOUNT)
        self.restart()

    def restart(self):
        self.db = JournalDB(self.db_path, account_id=r01.ACCOUNT)
        self.watcher = ProtectionWatcher(
            self.broker, journal_db=self.db, path=self.watcher_path, clock=lambda:self.now)
        self.gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher,
            SimpleNamespace(is_stopped=lambda:False),
        )
        self.broker.reconciliation_gate = self.gate

    def gate_once(self):
        with patch("src.engine.reconciliation.time.time",return_value=self.now), \
             patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False):
            return self.gate.run_startup_reconciliation()

    def initial_green(self):
        ok, reason = self.gate_once()
        self.assertTrue(ok, reason)
        for oid in ORDERS:
            row = self.db.get_position_lifecycle(oid)
            self.assertEqual(row["position_state"],"UNPROTECTED_MANUAL_RISK")
            self.assertEqual(row["confirmed_open_qty"],1)
        self.assertTrue(self.gate.is_accepting_entries)

    def fail_broker(self):
        failed = BrokerReadFailure(BrokerReadStatus.UNAVAILABLE.value,
                                   "synthetic HTTP 429 DH-904",429)
        self.broker.get_positions.return_value = failed
        self.broker.get_holdings.return_value = failed
        self.broker.get_trades_for_order.return_value = failed
        with patch("src.engine.protection_watcher.time.time",return_value=self.now):
            self.watcher.check_once()
        self.gate.halt_new_entries("Synthetic DH-904 broker evidence unavailable")
        for oid in ORDERS:
            self.assertEqual(self.db.get_position_lifecycle(oid)["position_state"],"RECONCILIATION_REQUIRED")
        self.assertFalse(self.gate.is_accepting_entries)

    def healthy_broker(self):
        self.broker.get_positions.return_value = r01.as_read_list([])
        self.broker.get_holdings.return_value = r01.as_read_list([
            dict(securityId="11915",isin="INE528G01035",dpQty=2,totalQty=2)
        ])
        self.broker.get_authenticated_profile_id.return_value=r01.ACCOUNT
        self.broker.get_trades_for_order.return_value=BrokerReadFailure(
            BrokerReadStatus.UNAVAILABLE.value,"Synthetic old daily order GET expired",429)

    def assert_ack_and_no_sell(self):
        with self.db._get_conn() as conn:
            rows=conn.execute("SELECT parent_order_id, status, quantity FROM manual_risk_acknowledgements ORDER BY parent_order_id").fetchall()
            self.assertEqual([(r[0],r[1],r[2]) for r in rows],
                             [(oid,"UNPROTECTED_MANUAL_RISK",1) for oid in ORDERS])
            current_acks = [tuple(row) for row in conn.execute(
                "SELECT parent_order_id, correlation_id, account_id, quantity, status, acknowledged_at "
                "FROM manual_risk_acknowledgements ORDER BY parent_order_id")]
            self.assertEqual(current_acks, self.initial_ack_rows)
            self.assertEqual(conn.execute("SELECT count(*) FROM fills WHERE transaction_type='SELL'").fetchone()[0],0)
        self.broker.place_forever_oco.assert_not_called()

    def recovery_worker(self):
        """Run production worker body inline; do not mock recovery or SQLite."""
        class RunInline:
            def __init__(self,*,target,**kw):self.target=target
            def start(self):self.target()
        with patch("src.engine.reconciliation.time.time",return_value=self.now), \
             patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False), \
             patch("src.engine.reconciliation.threading.Thread",RunInline):
            self.gate._schedule_historical_background_recheck()


class November22ReadAndRecovery(SyntheticNov22Fixture):
    def test_probe_b1_b2_recovery_uses_real_worker_sqlite_gate_and_old_daily_get_stays_denied(self):
        self.initial_green()
        self.fail_broker()
        self.assert_ack_and_no_sell()
        self.gov.throttled(60)
        self.recovery_worker()
        self.assertFalse(self.gate.is_accepting_entries)
        self.healthy_broker()
        self.now += 61  # read governor minimum cooldown is 60 seconds
        self.gate._historical_background_retry_after = 0
        self.broker.get_historical_trades.reset_mock()
        self.broker.get_trades_for_order.reset_mock()
        self.recovery_worker()
        self.assertTrue(self.gate.is_accepting_entries,self.gate.last_error)
        self.assertEqual(self.gate.state,ReconciliationState.HEALTHY)
        self.assertGreaterEqual(self.broker.get_historical_trades.call_count,2)  # recovery and independent readback
        self.broker.get_trades_for_order.assert_not_called()
        for oid in ORDERS:
            rec=self.db.get_position_lifecycle(oid)
            self.assertEqual((rec["position_state"],rec["confirmed_open_qty"],rec["sell_filled_qty"]),
                             ("UNPROTECTED_MANUAL_RISK",1,0))
        self.assertTrue(self.watcher._verify_recovery_saved(ORDERS))
        self.assert_ack_and_no_sell()
        # Recovery must survive a process reload and still avoid the obsolete
        # daily-trade endpoint for both settled owner-acknowledged parents.
        self.restart()
        self.broker.get_trades_for_order.reset_mock()
        ok, reason = self.gate_once()
        self.assertTrue(ok, reason)
        self.broker.get_trades_for_order.assert_not_called()
        self.assert_ack_and_no_sell()

    def test_historical_sell_and_buy_even_if_qty_two_fails_closed(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        existing=list(self.broker.get_historical_trades.return_value)
        bad=dict(existing[0],orderId="SELL-EXTERNAL",exchangeTradeId="SELL-001",
                 transactionType="SELL",tradedPrice=21)
        replacement=dict(existing[0],orderId="BUY-EXTERNAL",exchangeTradeId="BUY-001",
                         transactionType="BUY",tradedPrice=20)
        self.broker.get_historical_trades.return_value=r01.as_read_list([*existing,bad,replacement])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assert_ack_and_no_sell()

    def test_genuine_sell_alone_blocks_even_when_settled_quantity_unchanged(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        existing = list(self.broker.get_historical_trades.return_value)
        sell = dict(existing[0], orderId="SELL-ONLY", exchangeTradeId="SELL-EXEC-1",
                    transactionType="SELL", tradedPrice=20)
        self.broker.get_historical_trades.return_value = r01.as_read_list([*existing, sell])
        ok, _ = self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assert_ack_and_no_sell()

    def test_cached_day_without_fresh_gate_evidence_never_repairs_stale_owner(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        for oid in ORDERS:
            rec = self.watcher.terminal_orders[oid]
            self.assertEqual(rec.get("owner_history_day"), "2026-11-22")
        self.broker.get_trades_for_order.reset_mock()
        # A routine watcher is not a serialized historical gate pass.
        with patch("src.engine.protection_watcher.time.time",return_value=self.now):
            self.watcher.check_once()
        for oid in ORDERS:
            self.assertEqual(self.db.get_position_lifecycle(oid)["position_state"],
                             "RECONCILIATION_REQUIRED")
        self.broker.get_trades_for_order.assert_called()
        self.assertFalse(self.gate.is_accepting_entries)

    def test_active_exit_leg_prevents_old_buy_read_exemption_and_recovery(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        self.watcher.terminal_orders[ORDERS[0]]["sl_order_id"]="SYNTH-LIVE-EXIT"
        self.broker.get_trades_for_order.reset_mock()
        ok, _=self.gate_once()
        self.assertFalse(ok)
        self.broker.get_trades_for_order.assert_called()
        self.assertFalse(self.gate.is_accepting_entries)
        self.assert_ack_and_no_sell()

    def test_sqlite_write_failure_cannot_open_gate(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        with patch.object(self.db,"upsert_position_lifecycle",
                          side_effect=sqlite3.OperationalError("Synthetic WAL write refused")):
            ok, _ = self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assert_ack_and_no_sell()

    def test_incomplete_history_profile_mismatch_and_shortfall_fail_closed(self):
        for scenario in ("missing_history","account_mismatch","quantity_shortfall"):
            with self.subTest(scenario=scenario):
                self.initial_green()
                self.fail_broker()
                self.healthy_broker()
                saved_h=list(self.broker.get_historical_trades.return_value)
                saved_acct=self.broker.get_authenticated_profile_id.return_value
                saved_hold=self.broker.get_holdings.return_value
                if scenario=="missing_history":
                    self.broker.get_historical_trades.return_value=BrokerReadFailure(
                        BrokerReadStatus.UNAVAILABLE.value,"Synthetic page incomplete",429)
                elif scenario=="account_mismatch":
                    self.broker.get_authenticated_profile_id.return_value="WRONG_ACCOUNT"
                else:
                    self.broker.get_holdings.return_value=r01.as_read_list([
                        dict(securityId="11915",isin="INE528G01035",dpQty=1,totalQty=1)
                    ])
                ok,_=self.gate_once()
                self.assertFalse(ok)
                self.assertFalse(self.gate.is_accepting_entries)
                self.broker.get_historical_trades.return_value=r01.as_read_list(saved_h)
                self.broker.get_authenticated_profile_id.return_value=saved_acct
                self.broker.get_holdings.return_value=saved_hold

    def test_sqlite_readback_failure_cannot_open_gate(self):
        self.initial_green()
        self.fail_broker()
        self.healthy_broker()
        with patch.object(self.watcher,"_verify_recovery_saved",return_value=False):
            ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

class November22RestartCalls(SyntheticNov22Fixture,unittest.IsolatedAsyncioTestCase):
    async def test_probe_a_startup_to_watcher_get_budget_and_live_order_wake(self):
        for name in READ_NAMES:
            getattr(self.broker,name).reset_mock()
        self.initial_green()
        names=READ_NAMES
        startup={name:getattr(self.broker,name).call_count for name in names}
        self.assertEqual(startup["get_trades_for_order"],0)
        baseline={"get_fund_limits":1,"get_trade_book":1,"get_positions":1,
                  "get_authenticated_profile_id":1,"get_holdings":1,"get_order_book":1,
                  "get_forever_orders":1,"get_historical_trades":1,"get_super_orders":1,
                  "get_trades_for_order":2}
        # The independent Nov 22 baseline measured 11 startup + 8 immediate.
        self.assertEqual(sum(baseline.values()),11)
        self.assertEqual(sum(startup.values()),9, startup)
        self.watcher.should_poll=lambda *_:True
        total_start=sum(getattr(self.broker,name).call_count for name in names)
        ticks=0
        class StopLoop(Exception):pass
        async def fast_sleep(_):
            nonlocal ticks
            ticks+=1
            if ticks>=3:raise StopLoop()
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False), \
             patch("src.engine.protection_watcher.asyncio.sleep",new=fast_sleep):
            with self.assertRaises(StopLoop):await self.watcher.run()
        total_after=sum(getattr(self.broker,name).call_count for name in names)
        self.assertEqual(total_after,total_start)
        self.assertEqual(total_after,9)  # 9 startup + 0 immediate = 9 vs prior 19
        # An unrelated pending order forces immediate supervision; no network.
        self.watcher.pending["SYNTH-UNRELATED"] = {
            "order_id":"SYNTH-UNRELATED", "correlation_id":"NEW-OTHER",
            "symbol":"OTHER", "security_id":"42", "segment":"NSE_EQ",
            "product_type":"CNC", "state":"PENDING", "qty":1,
            "filled_qty":0, "created_at":self.now,
        }
        ticks=0
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False), \
             patch("src.engine.protection_watcher.asyncio.sleep",new=fast_sleep):
            with self.assertRaises(StopLoop):await self.watcher.run()
        total_after_wake=sum(getattr(self.broker,name).call_count for name in names)
        self.assertGreater(total_after_wake,total_after)

if __name__=="__main__":unittest.main()
