"""Offline integration: actual fill journal, owner acknowledgements, watcher files and entry gate."""
import json
import sqlite3
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate
from src.journal.db import JournalDB
from src.models import BrokerReadFailure
from tests import test_historical_zero_trade_id_r01 as r01


class OwnerLotRestart(unittest.TestCase):
    def setUp(self):
        fx = self.fx = r01.R01HistoricalZeroTradeIdTests()
        fx.setUp()
        self.addCleanup(fx.doCleanups)
        self.clock = patch("src.engine.reconciliation.time.time", return_value=r01.NOW)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.path = Path(fx.tmp.name) / "watcher.json"
        self.broker = fx.broker
        self.broker.get_fund_limits.return_value = (True, 5000., "OK")
        for method in ("get_trade_book", "get_positions", "get_order_book", "get_super_orders", "get_trades_for_order"):
            getattr(self.broker, method).return_value = []
        fx._seed_two()
        for oid, cid in ((fx.first_oid, "EQ-ALPHA-01"), (fx.second_oid, "EQ-BETA-02")):
            fx.db.acknowledge_manual_risk_holding(parent_order_id=oid, correlation_id=cid,
                account_id=r01.ACCOUNT, symbol="YESBANK", security_id="11915", quantity=1,
                acknowledged_by="synthetic owner", reason="synthetic controlled lot")
        _, unresolved, error = fx.gate._reconcile_unresolved_intents_via_historical_trades(
            fx.db.get_non_terminal_intents(), now_ts=r01.NOW)
        self.assertEqual((unresolved, error), ([], None))
        self.restart()

    def restart(self):
        self.db = JournalDB(str(self.path.parent / "journal.db"), account_id=r01.ACCOUNT)
        self.watcher = ProtectionWatcher(self.broker, path=str(self.path), journal_db=self.db, clock=lambda: r01.NOW)
        self.gate = StartupReconciliationGate(self.broker, self.db, self.watcher, SimpleNamespace(is_stopped=lambda: False))
        self.broker.reconciliation_gate = self.gate

    def run_gate(self, expected):
        ok, reason = self.gate.run_startup_reconciliation()
        self.assertEqual(ok, expected, reason)
        self.assertEqual(self.gate.is_accepting_entries, expected)
        self.broker.place_forever_oco.assert_not_called()

    def test_legacy_warning_omission_and_restarts(self):
        self.run_gate(True)
        ids = (self.fx.first_oid, self.fx.second_oid)
        for oid in ids:
            rec = self.watcher.terminal_orders[oid]
            rec.update(manual_exit_detected=True, manual_exit_evidence_verified=True,
                       manual_exit_evidence_reason="VERIFIED_AGGREGATE_QUANTITY_REDUCTION",
                       position_state="RECONCILIATION_REQUIRED", reconciliation_required=True)
            self.watcher._sync_position_lifecycle(rec)
        self.watcher._save_strict()
        saved = self.broker.get_holdings.return_value
        self.broker.get_holdings.return_value = []
        self.restart()
        self.run_gate(False)
        self.broker.get_holdings.return_value = saved
        self.broker.get_historical_trades.reset_mock()
        self.run_gate(True)
        self.assertEqual(self.broker.get_historical_trades.call_count, 2)  # repair + independent readback pass
        for _ in range(2):
            self.restart()
            self.run_gate(True)
            self.assertTrue(self.watcher._verify_recovery_saved(ids))
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM fills WHERE transaction_type='SELL'").fetchone()[0], 0)
            archived = [json.loads(r[0]) for r in conn.execute(
                "SELECT payload FROM system_events WHERE event_type='OWNER_LOT_RECOVERY'")]
            self.assertEqual({r["order_id"] for r in archived if r["previous_reason"] ==
                              "VERIFIED_AGGREGATE_QUANTITY_REDUCTION"}, set(ids))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM system_events WHERE event_type='OWNER_LOT_RECOVERY'")

    def test_actual_sell_and_repurchase_survive_later_omission_from_history(self):
        self.run_gate(True)
        original = self.broker.get_historical_trades.return_value
        sell = dict(self.fx.second, orderId="MANUAL1", exchangeTradeId="EXEC1",
                    transactionType="SELL", exchangeSegment="BSE_EQ", securityId="98765")
        buy = dict(sell, orderId="MANUAL2", exchangeTradeId="EXEC2", transactionType="BUY")
        self.broker.get_historical_trades.return_value = [*original, sell, buy]
        self.run_gate(False)
        self.broker.get_historical_trades.return_value = original
        self.restart()
        self.run_gate(False)
        self.assertEqual(self.db.owner_lot_review_reason(r01.ACCOUNT, self.fx.first_oid), "OWNER_LOT_EXECUTION_CONFLICT")

    def test_unavailable_history_and_demat_shortfall(self):
        self.run_gate(True)
        original = self.broker.get_historical_trades.return_value
        self.broker.get_historical_trades.return_value = BrokerReadFailure("FAILED", "synthetic", 429)
        self.run_gate(False)
        self.broker.get_historical_trades.return_value = original
        self.broker.get_holdings.return_value = [dict(self.broker.get_holdings.return_value[0], totalQty=1, dpQty=1)]
        self.run_gate(False)
        for oid in (self.fx.first_oid, self.fx.second_oid):
            row = self.db.get_position_lifecycle(oid)
            self.assertEqual((row["sell_filled_qty"], row["confirmed_open_qty"]), (0, 1))
            self.assertFalse(row["manual_exit_detected"])

    def test_audit_write_failure_does_not_open_gate(self):
        self.run_gate(True)
        with patch.object(self.db, "record_system_event", side_effect=sqlite3.OperationalError("locked")):
            self.run_gate(False)
        self.run_gate(True)

    def test_corroborated_unexplained_demat_shortfall_survives_restored_total(self):
        self.run_gate(True)
        original = self.broker.get_holdings.return_value
        self.broker.get_edis_owned_quantity.assert_not_called()
        self.broker.get_holdings.return_value = [dict(original[0], totalQty=1, dpQty=1)]
        self.broker.get_edis_owned_quantity.return_value = 1
        self.run_gate(False)
        self.broker.get_holdings.return_value = original
        self.restart()
        self.run_gate(False)
        for oid in (self.fx.first_oid, self.fx.second_oid):
            self.assertEqual(self.db.owner_lot_review_reason(r01.ACCOUNT, oid), "OWNER_LOT_DEMAT_REVIEW")
            self.assertFalse(self.db.get_position_lifecycle(oid)["manual_exit_detected"])

    def test_governed_edis_total_and_forever_get_routes(self):
        from src.broker.dhan_client import DhanClient, BASE_URL
        from unittest.mock import Mock
        client = object.__new__(DhanClient)
        client.cfg = SimpleNamespace(trading_mode="LIVE", dhan_client_id=r01.ACCOUNT)
        response = Mock(status_code=200)
        client._get_with_auth_recovery = Mock(return_value=response)
        isin = "INE528G01035"
        payload = dict(isin=isin, clientId=r01.ACCOUNT, status="SUCCESS", totalQty="2", aprvdQty=0)
        response.json.return_value = payload
        self.assertEqual(client.get_edis_owned_quantity(isin), 2)
        client._get_with_auth_recovery.assert_called_once_with(f"{BASE_URL}/edis/inquire/{isin}")
        for invalid in (dict(clientId="OTHER"), dict(isin="OTHER"), dict(totalQty=True), dict(status="FAILED")):
            response.json.return_value = dict(payload, **invalid)
            self.assertIsNone(client.get_edis_owned_quantity(isin))
        response.json.return_value = []
        self.assertEqual(client.get_forever_orders(), [])
        client._get_with_auth_recovery.assert_called_with(f"{BASE_URL}/forever/orders")

    def test_wrong_live_profile_blocks_before_recording_trades(self):
        self.broker.cfg.trading_mode = "LIVE"
        self.broker.get_authenticated_profile_id.return_value = "OTHER"
        self.broker.get_trade_book.reset_mock()
        self.run_gate(False)
        self.broker.get_trade_book.assert_not_called()

    def test_new_buy_does_not_inherit_old_acknowledgements(self):
        self.run_gate(True)
        original = self.broker.get_historical_trades.return_value
        unrelated = dict(self.fx.second, orderId="OTHERBUY", exchangeTradeId="EXECNEW", tradedQuantity=1)
        self.broker.get_historical_trades.return_value = [*original, unrelated]
        self.run_gate(False)
        self.assertFalse(self.db.is_manual_risk_acknowledged("OTHERBUY"))

    def test_t1_settlement_and_unexplained_day_sale(self):
        self.broker.get_holdings.return_value = [dict(self.broker.get_holdings.return_value[0],
                                                     dpQty=1, t1Qty=1, totalQty=2)]
        self.run_gate(True)
        self.broker.get_positions.return_value = [dict(securityId="11915", exchangeSegment="NSE_EQ",
                                                       productType="CNC", netQty=0, sellQty=1)]
        self.run_gate(False)

    def test_verified_sell_in_journal_blocks_even_if_history_omits_it(self):
        self.run_gate(True)
        self.db.record_fill("EXTERNAL", "CONFIRMED1", "YESBANK", 20., 1, traded_at=r01.NOW,
                            transaction_type="SELL", security_id="11915", exchange_segment="NSE_EQ",
                            product_type="CNC", is_verified_trade=True)
        self.restart()
        self.run_gate(False)

    def test_no_blocking_history_on_regular_poll_and_one_shared_book_per_full_check(self):
        self.run_gate(True)
        for method in ("get_holdings", "get_positions", "get_trade_book", "get_authenticated_profile_id",
                       "get_forever_orders", "get_historical_trades"):
            getattr(self.broker, method).reset_mock()
        self.run_gate(True)
        for method in ("get_holdings", "get_positions", "get_trade_book", "get_authenticated_profile_id",
                       "get_forever_orders", "get_historical_trades"):
            self.assertEqual(getattr(self.broker, method).call_count, 1, method)
        self.broker.get_historical_trades.reset_mock()
        self.watcher.check_once()
        self.broker.get_historical_trades.assert_not_called()
