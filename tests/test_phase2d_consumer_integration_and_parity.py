"""Phase 2D Production Consumer Integration & 3 Approved Shadow-Parity Safety Improvement Tests."""
from __future__ import annotations

import json
from contextlib import closing
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.broker.broker_snapshot import (
    BookEvidence,
    BrokerSnapshot,
    Generation,
    SessionContext,
    SessionPhase,
)
from src.config import AppConfig, TradingMode
from src.control_center_backend import _display_unified_snapshot
from src.engine.broker_ledger_reconciler import BrokerLedgerReconciler, Purpose
from src.engine.config_manager import ConfigChangeTracker
from src.engine.journal_view import read_journal_view
from src.engine.orphan_cleanup import clean_orphaned_forever_orders, verify_and_resolve_orphan_legs
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.engine.risk_manager import RiskManager
from src.engine.unified_account_runtime import (
    UnifiedAccountRuntime,
    build_policy_context,
    peek_display_snapshot_summary,
)
from src.journal.db import JournalDB
from tests.test_phase2d_c01_c05_and_settlement import (
    ACCT,
    _book,
    _make_snapshot,
    _seed_yesbank_db,
)


class TestPhase2DAllSixProductionConsumers(unittest.TestCase):
    def test_consumer_1_webui_display_only_snapshot_never_refreshes_broker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            summary = _display_unified_snapshot(td, cfg)
            self.assertTrue(summary.get("display_only"))
            self.assertFalse(summary.get("available"))
            # Direct helper also strictly display-only
            summary2 = peek_display_snapshot_summary(td, ACCT)
            self.assertTrue(summary2.get("display_only"))

    def test_consumer_2_risk_manager_accepts_fresh_snapshot_and_rejects_stale(self) -> None:
        cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT)
        rm = RiskManager(cfg)
        now_u = time.time_ns()
        fresh_snap = _make_snapshot(
            snapshot_id="RM-FRESH",
            positions=[{"dhanClientId": ACCT, "securityId": "3045", "tradingSymbol": "SBIN-EQ", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 5, "buyAvg": 800.0}],
            utc_ns=now_u,
        )
        ok, msg = rm.sync_from_journal_and_broker(broker_snapshot=fresh_snap)
        self.assertTrue(ok, msg)
        self.assertEqual(getattr(rm, "_last_snapshot_id", None), "RM-FRESH")

        # Stale snapshot (>10s old) -> MUST FAIL CLOSED
        stale_snap = _make_snapshot(
            snapshot_id="RM-STALE",
            positions=[],
            utc_ns=now_u - 25_000_000_000,
        )
        ok_stale, msg_stale = rm.sync_from_journal_and_broker(broker_snapshot=stale_snap)
        self.assertFalse(ok_stale)
        self.assertIn("stale", msg_stale.lower())

    def test_consumer_3_startup_reconciliation_gate_generation_fence_and_ttl(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            broker = MagicMock()
            broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock(); watcher._is_running = True
            gate = StartupReconciliationGate(broker, db, watcher, stop_mgr)

            # Healthy flat verdict opens gate
            jv_gate = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            flat_snap = _make_snapshot(snapshot_id="GATE-FLAT", holdings=[], positions=[], journal_digest=jv_gate.digest_sha256)
            ok, msg = gate.run_unified_startup_verification(data_dir=td, broker_snapshot=flat_snap, ttl_sec=60.0)
            self.assertTrue(ok, msg)
            self.assertEqual(gate.state, ReconciliationState.HEALTHY)
            self.assertTrue(gate._accepting_entries)

            # Generation change during authorization -> stale verdict MUST BE REJECTED
            start_gen = gate._halt_generation
            gate.halt_new_entries("Simulated concurrent halt")
            ok_stale, msg_stale = gate.publish_unified_verdict(
                gate._unified_verdict,
                expected_halt_generation=start_gen,
            )
            self.assertFalse(ok_stale)
            self.assertIn("Stale unified verdict rejected", msg_stale)
            self.assertFalse(gate._accepting_entries)

            # Expired TTL -> is_accepting_entries MUST fail closed
            jv_gate2 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            flat_snap2 = _make_snapshot(snapshot_id="GATE-FLAT-2", holdings=[], positions=[], journal_digest=jv_gate2.digest_sha256)
            ok_reopen, _ = gate.run_unified_startup_verification(data_dir=td, broker_snapshot=flat_snap2, ttl_sec=0.001)
            self.assertTrue(ok_reopen)
            time.sleep(0.01)
            self.assertFalse(gate.is_accepting_entries)

    def test_consumer_4_protection_watcher_write_side_durable_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            broker = MagicMock()
            broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            pw = ProtectionWatcher(broker, db, MagicMock())

            rec = {
                "order_id": "PW-1",
                "symbol": "SBIN-EQ",
                "security_id": "3045",
                "segment": "NSE_EQ",
                "product_type": "CNC",
                "position_state": "PROTECTED_OPEN",
                "filled_qty": 5,
                "persistence_uncertain": True,
                "orphaned_leg_cancel_details": {"ORPH-1": {"order_id": "ORPH-1", "status": "UNCERTAIN"}},
                "cancel_order_id": "CANC-1",
            }
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                    ("PW-1", 0, json.dumps({"order_id": "PW-1", "position_state": "PROTECTED_OPEN", "filled_qty": 5}), time.time()),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("PW-1", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 5, 5, 0, 5, time.time()),
                )
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("PW-1", "ET-PW1", "SBIN-EQ", 800.0, 5, time.time(), "BUY", "3045", "NSE_EQ", "CNC", "PW-1"),
                )
            ok_p, _ = pw.clear_persistence_risk_with_readback(
                rec, expected_order_id="PW-1", expected_position_state="PROTECTED_OPEN", expected_filled_qty=5
            )
            self.assertTrue(ok_p)
            self.assertFalse(rec.get("persistence_uncertain", False))

            now_m = time.monotonic_ns()
            leg_row = {
                "dhanClientId": ACCT,
                "orderId": "ORPH-1", "parentOrderId": "PW-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CANCELLED",
                "filledQty": 0,
                "remainingQuantity": 0,
            }
            s1 = _make_snapshot(snapshot_id="PW-S1", order_book=[leg_row], mono_ns=now_m)
            s2 = _make_snapshot(snapshot_id="PW-S2", order_book=[leg_row], mono_ns=now_m + 2_000_000_000)
            out_orph = pw.resolve_orphan_legs_durably(rec, s1, s2, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
            self.assertTrue(out_orph.resolved, out_orph)

    def test_consumer_5_orphan_cleanup_two_independent_snapshots(self) -> None:
        now_m = time.monotonic_ns()
        rec = {
            "order_id": "P-ORPH",
            "symbol": "SBIN-EQ",
            "security_id": "3045",
            "segment": "NSE_EQ",
            "product_type": "CNC",
            "orphaned_leg_cancel_details": {"L-88": {"order_id": "L-88", "status": "UNCERTAIN"}},
        }
        leg = {
            "dhanClientId": ACCT,
            "orderId": "L-88", "parentOrderId": "P-ORPH",
            "securityId": "3045",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "remainingQuantity": 0,
        }
        s1 = _make_snapshot(snapshot_id="OC-1", order_book=[leg], mono_ns=now_m)
        s2_dup = _make_snapshot(snapshot_id="OC-1", order_book=[leg], mono_ns=now_m)
        s2_indep = _make_snapshot(snapshot_id="OC-2", order_book=[leg], mono_ns=now_m + 2_000_000_000)
        self.assertFalse(verify_and_resolve_orphan_legs(rec, s1, s2_dup, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000).resolved)
        self.assertTrue(verify_and_resolve_orphan_legs(rec, s1, s2_indep, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000).resolved)

    def test_consumer_6_config_manager_unified_shutdown_gate_and_approved_parity_improvements(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            JournalDB(db_path)
            tracker = ConfigChangeTracker(data_dir=td)

            # Approved Safety Improvement #1: Empty journal + unmatched broker exposure -> BLOCKED
            unmatched_snap = _make_snapshot(
                snapshot_id="PARITY-1",
                positions=[{"dhanClientId": ACCT, "securityId": "3045", "tradingSymbol": "SBIN-EQ", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}],
            )
            res_unmatched = tracker.evaluate_unified_shutdown_gate(
                action="stop", db_path=db_path, pre_fetched_snapshot=unmatched_snap
            )
            self.assertFalse(res_unmatched["allowed"], "Approved safety improvement #1: unmatched broker exposure must BLOCK")

            # Approved Safety Improvement #2: Unverified historical closure without durable execution records -> BLOCKED
            db_path_2 = str(Path(td) / "unverified_closed.db")
            db2 = JournalDB(db_path_2)
            with db2._get_conn() as con:
                con.execute(
                    "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("HIST-1", "HIST-1", "LIVE", "CNC", "NSE_EQ", "SBIN-EQ", "3045", "LIMIT", 800.0, 5, 5, "TRADED", time.time() - 86400, time.time(), "FOREVER_OCO"),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("HIST-1", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "POSITION_CLOSED", 5, 5, 0, 0, time.time()),
                )
            jv_flat = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            flat_snap = _make_snapshot(snapshot_id="PARITY-2", holdings=[], positions=[], journal_digest=jv_flat.digest_sha256)
            res_unver = tracker.evaluate_unified_shutdown_gate(
                action="stop", db_path=db_path_2, pre_fetched_snapshot=flat_snap
            )
            self.assertFalse(res_unver["allowed"], "Approved safety improvement #2: unverified historical closure must BLOCK")

            # Verified flat account -> ALLOWED
            res_flat = tracker.evaluate_unified_shutdown_gate(
                action="stop", db_path=db_path, pre_fetched_snapshot=flat_snap
            )
            self.assertTrue(res_flat["allowed"], res_flat)

    def test_approved_parity_improvement_3_yesbank_closed_sibling_and_retained_lots_owner_stop(self) -> None:
        # Use fe946810's canonical YESBANK closed-sibling (28 shares) + 2 retained acknowledged 1-share lots fixture
        from tests.test_phase2c_ledger_shadow import LedgerShadowTests, MONO
        helper = LedgerShadowTests()
        helper.setUp()
        try:
            view, snap, _group, _child = helper._closed_rollup(prior_signed=False)
            with closing(sqlite3.connect(helper.path)) as con:
                acks_before = con.execute(
                    "SELECT parent_order_id, quantity, status FROM manual_risk_acknowledgements ORDER BY parent_order_id"
                ).fetchall()
            tracker = ConfigChangeTracker(data_dir=str(Path(helper.path).parent))
            res = tracker.evaluate_unified_shutdown_gate(
                action="stop", db_path=helper.path, pre_fetched_snapshot=snap, now=MONO
            )
            self.assertTrue(res["allowed"], f"Approved safety improvement #3 failed: {res}")
            with closing(sqlite3.connect(helper.path)) as con:
                acks_after = con.execute(
                    "SELECT parent_order_id, quantity, status FROM manual_risk_acknowledgements ORDER BY parent_order_id"
                ).fetchall()
            self.assertEqual(acks_before, acks_after)
            self.assertEqual(len(acks_after), 2)
        finally:
            helper.doCleanups()
            helper.tearDown()


if __name__ == "__main__":
    unittest.main()
