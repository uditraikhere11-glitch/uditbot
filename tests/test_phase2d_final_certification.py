"""Phase 2D Final System-Wide Certification Test Suite.

Covers:
  1. End-to-End Real HTTP Positive Acquisition Matrix (DhanClient -> ReadGovernor ->
     DhanSnapshotSource -> CrossProcessReadCoordinator -> BrokerLedgerReconciler).
  2. C15-C22 Adversarial & Regression Checks (every required field & failure mode).
  3. Concurrency, Deadlock & Crash-Recovery Verification.
  4. Non-tautological Mutation-Detection Invariant Verification.
"""
from __future__ import annotations

import os
import json
import os
import sqlite3
import time
import threading
import time
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.broker.broker_snapshot import (
    BookStatus,
    Purpose,
    SessionContext,
    SessionPhase,
    account_key,
)
from src.engine.config_manager import ConfigChangeTracker
from src.engine.journal_view import read_journal_view
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.engine.unified_account_runtime import (
    UnifiedAccountRuntime,
    build_verified_policy_context,
    load_verified_exchange_calendar,
    reconcile_and_repair_terminal_orders,
)
from tests._phase2d_live_harness import DEFAULT_TEST_ACCOUNT, LivePhase2DHarness
from tests.test_phase2d_c01_c05_and_settlement import _make_snapshot


class TestPhase2DFinalCertification(unittest.TestCase):
    # ------------------------------------------------------------------
    # 1. END-TO-END REAL HTTP POSITIVE ACQUISITION MATRIX
    # ------------------------------------------------------------------
    def test_e2e_real_http_flat_live_account_startup_transport_and_shutdown(self) -> None:
        with LivePhase2DHarness() as h:
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True

            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)
            ok, msg = gate.run_unified_startup_verification(data_dir=str(h.root))
            self.assertTrue(ok, msg)
            self.assertEqual(gate.state, ReconciliationState.HEALTHY)
            self.assertTrue(gate.is_accepting_entries)

            # Assert real HTTP calls actually occurred through DhanClient.session.get
            counts = h.transport.endpoint_counts()
            for ep in ("/profile", "/holdings", "/positions", "/orders", "/trades", "/forever/orders", "/super/orders"):
                self.assertGreaterEqual(counts.get(ep, 0), 1, f"Expected real HTTP call to {ep}, got {counts}")

            # Authority sidecar snapshots.sqlite3 MUST exist and pass transport interlock
            sc = h.root / "broker_evidence" / account_key(h.account_id) / "snapshots.sqlite3"
            self.assertTrue(sc.is_file(), "Authority sidecar snapshots.sqlite3 must be persisted")
            ok_rv, msg_rv = gate.revalidate_for_transport_submission()
            self.assertTrue(ok_rv, msg_rv)

            # End-to-end ConfigChangeTracker shutdown gate via real HTTP acquisition
            tracker = ConfigChangeTracker(data_dir=str(h.root))
            res = tracker.evaluate_unified_shutdown_gate(action="stop", db_path=h.db_path, broker=h.client)
            self.assertTrue(res["allowed"], res)

    def test_e2e_real_http_protected_cnc_holding_and_retained_yesbank_lots(self) -> None:
        """Real HTTP acquisition with protected CNC position (Forever OCO) through real DhanClient + Coordinator."""
        from datetime import datetime
        from src.engine.broker_session import IST

        now_ts = time.time() - 60.0
        ex_time = datetime.fromtimestamp(now_ts, IST).isoformat()
        positions = [
            {
                "dhanClientId": DEFAULT_TEST_ACCOUNT,
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "tradingSymbol": "SBIN-EQ",
                "securityId": "3045",
                "netQty": 5,
                "dayBuyQty": 5,
                "daySellQty": 0,
                "carryForwardBuyQty": 0,
                "carryForwardSellQty": 0,
                "buyAvg": 800.0,
            },
        ]
        trades = [
            {
                "dhanClientId": DEFAULT_TEST_ACCOUNT,
                "orderId": "SBIN-ORD-1",
                "exchangeTradeId": "EID-SBIN-1",
                "exchangeOrderId": "EXO-SBIN-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "tradedQuantity": 5,
                "tradedPrice": 800.0,
                "exchangeTime": ex_time,
            },
        ]
        forever_orders = [
            {
                "dhanClientId": DEFAULT_TEST_ACCOUNT,
                "orderId": "FOCO-SBIN-1",
                "orderStatus": "PENDING",
                "transactionType": "SELL",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "securityId": "3045",
                "quantity": 5,
                "quantity1": 5,
                "triggerPrice": 760.0,
                "price1": 860.0,
            }
        ]
        with LivePhase2DHarness(positions=positions, trades=trades, forever_orders=forever_orders) as h:
            h.db.record_order_intent(
                correlation_id="SBIN-ORD-1", account_id=h.account_id, symbol="SBIN-EQ", security_id="3045",
                exchange_segment="NSE_EQ", product_type="CNC", order_type="LIMIT", price=800.0,
                quantity=5, protection_route="FOREVER_OCO",
            )
            h.db.update_order_intent("SBIN-ORD-1", "TRADED", broker_order_id="SBIN-ORD-1")
            h.db.record_fill(
                "SBIN-ORD-1", "EID-SBIN-1", "SBIN-EQ", 800.0, 5, traded_at=now_ts,
                transaction_type="BUY", security_id="3045", exchange_segment="NSE_EQ",
                product_type="CNC", parent_order_id="SBIN-ORD-1", is_verified_trade=True,
            )
            h.db.upsert_position_lifecycle(
                parent_order_id="SBIN-ORD-1", account_id=h.account_id, symbol="SBIN-EQ", security_id="3045",
                exchange_segment="NSE_EQ", product_type="CNC", protection_route="FOREVER_OCO",
                position_state="PROTECTED", entry_qty=5, buy_filled_qty=5,
                buy_vwap=800.0, protected_qty=5, confirmed_open_qty=5,
            )
            with h.db._get_conn() as con:
                con.execute(
                    "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    ("SBIN-ORD-1", "FOCO-SBIN-1", "SBIN-EQ", 760.0, 755.0, 860.0, 5, "ACTIVE_CONFIRMED", now_ts, "FOREVER_OCO"),
                )

            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True

            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)
            ok, msg = gate.run_unified_startup_verification(data_dir=str(h.root))
            self.assertTrue(ok, msg)
            self.assertTrue(gate.revalidate_for_transport_submission()[0])

            tracker = ConfigChangeTracker(data_dir=str(h.root))
            res = tracker.evaluate_unified_shutdown_gate(action="stop", db_path=h.db_path, broker=h.client)
            self.assertTrue(res["allowed"], res)

    def test_e2e_real_http_unmatched_broker_exposure_blocks(self) -> None:
        """Approved Safety Difference #1: Unmatched broker holding blocks both startup & shutdown."""
        holdings = [
            {
                "dhanClientId": DEFAULT_TEST_ACCOUNT,
                "exchange": "NSE",
                "tradingSymbol": "INFY-EQ",
                "securityId": "1594",
                "totalQty": 10,
                "dpQty": 10,
                "t1Qty": 0,
                "availableQty": 10,
                "collateralQty": 0,
                "avgCostPrice": 1500.0,
            }
        ]
        with LivePhase2DHarness(holdings=holdings) as h:
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True

            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)
            ok, msg = gate.run_unified_startup_verification(data_dir=str(h.root))
            self.assertFalse(ok, "Unmatched broker exposure must block startup")

            tracker = ConfigChangeTracker(data_dir=str(h.root))
            res = tracker.evaluate_unified_shutdown_gate(action="stop", db_path=h.db_path, broker=h.client)
            self.assertFalse(res["allowed"], "Unmatched broker exposure must block shutdown")

    # ------------------------------------------------------------------
    # 2. C15 - C22 ADVERSARIAL & REGRESSION SUITE
    # ------------------------------------------------------------------
    def test_c15_verified_calendar_digest_is_str_property(self) -> None:
        cal = load_verified_exchange_calendar()
        self.assertIsInstance(cal.digest, str)
        self.assertEqual(len(cal.digest), 64)
        self.assertGreaterEqual(len(cal.holidays), 30)

    def test_c16_pre_labelled_settled_without_6point_evidence_rejected(self) -> None:
        with LivePhase2DHarness(account_id='1105678901') as h:
            jv = read_journal_view(h.db_path, account_id=h.account_id, journal_epoch=1, read_utc_ns=time.time_ns())
            bad_settled = _make_snapshot(
                snapshot_id="C16-BAD-SETTLED",
                account_id=h.account_id,
                holdings=[],
                positions=[],
                journal_digest=jv.digest_sha256,
                session_phase=SessionPhase.SETTLED,
            )
            rt = UnifiedAccountRuntime(data_dir=str(h.root), db_path=h.db_path, account_id=h.account_id)
            pol = build_verified_policy_context(
                mode="LIVE",
                purpose=Purpose.STARTUP,
                snapshot=bad_settled,
                journal_view=jv,
                risk_funds_verified=True,
            )
            res = rt.evaluate_snapshot(bad_settled, pol)
            self.assertFalse(res.allowed)
            self.assertEqual(res.reason_code, "HISTORY_COVERAGE_INCOMPLETE"); self.assertIn("C16", res.detail)

    def test_c17_terminal_order_repair_rejects_every_missing_field_and_failed_trade_book(self) -> None:
        base_row = {
            "dhanClientId": DEFAULT_TEST_ACCOUNT,
            "orderId": "ORD-C17",
            "correlationId": "CORR-C17",
            "securityId": "3045",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "remainingQuantity": 0,
        }

        # Every required field missing individually MUST reject!
        for missing_key in (
            "dhanClientId",
            "orderId",
            "correlationId",
            "securityId",
            "exchangeSegment",
            "productType",
            "filledQty",
            "remainingQuantity",
        ):
            with LivePhase2DHarness() as h:
                now_ts = time.time()
                with h.db._get_conn() as con:
                    con.execute(
                        "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        ("CORR-C17", "ORD-C17", "LIVE", "CNC", "NSE_EQ", "SBIN-EQ", "3045", "LIMIT", 800.0, 5, 0, "PENDING", now_ts - 60, now_ts - 60, "FOREVER_OCO"),
                    )
                bad = dict(base_row)
                bad.pop(missing_key)
                broker = MagicMock()
                broker.cfg = h.cfg
                broker.get_order_status.return_value = bad; broker.get_order_book.return_value = [bad]
                broker.get_trade_book.return_value = []; broker.get_forever_orders.return_value = []; broker.get_super_orders.return_value = []
                ok_rep, err_rep, repaired_map = reconcile_and_repair_terminal_orders(
                    db_path=h.db_path, broker=broker, account_id=h.account_id, data_dir=str(h.root)
                )
                self.assertFalse(ok_rep, f"C17 failed: missing '{missing_key}' must block: {(ok_rep, err_rep, repaired_map)}")
                self.assertEqual(repaired_map, {}, f"C17 failed: missing '{missing_key}' must NOT repair: {repaired_map}")

        # Failed get_trade_book() MUST fail closed (never coerce to [])!
        with LivePhase2DHarness() as h:
            now_ts = time.time()
            with h.db._get_conn() as con:
                con.execute(
                    "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("CORR-C17", "ORD-C17", "LIVE", "CNC", "NSE_EQ", "SBIN-EQ", "3045", "LIMIT", 800.0, 5, 0, "PENDING", now_ts - 60, now_ts - 60, "FOREVER_OCO"),
                )
            broker = MagicMock()
            broker.cfg = h.cfg
            broker.get_order_status.return_value = dict(base_row); broker.get_order_book.return_value = [dict(base_row)]
            broker.get_trade_book.side_effect = RuntimeError("trade book unavailable")
            ok_rep, err_rep, repaired_map = reconcile_and_repair_terminal_orders(
                db_path=h.db_path, broker=broker, account_id=h.account_id, data_dir=str(h.root)
            )
            self.assertFalse(ok_rep, f"C17 failed: trade_book error must fail closed: {(ok_rep, err_rep, repaired_map)}")
            self.assertEqual(repaired_map, {})

        # Complete valid row + valid trade_book MUST repair cleanly!
        with LivePhase2DHarness() as h:
            now_ts = time.time()
            with h.db._get_conn() as con:
                con.execute(
                    "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("CORR-C17", "ORD-C17", "LIVE", "CNC", "NSE_EQ", "SBIN-EQ", "3045", "LIMIT", 800.0, 5, 0, "PENDING", now_ts - 60, now_ts - 60, "FOREVER_OCO"),
                )
            broker = MagicMock()
            broker.cfg = h.cfg
            broker.get_order_status.return_value = dict(base_row); broker.get_order_book.return_value = [dict(base_row)]
            broker.get_trade_book.return_value = []; broker.get_forever_orders.return_value = []; broker.get_super_orders.return_value = []
            ok_rep, err_rep, repaired_map = reconcile_and_repair_terminal_orders(
                db_path=h.db_path, broker=broker, account_id=h.account_id, data_dir=str(h.root)
            )
            self.assertTrue(ok_rep, f"C17 valid terminal order must repair: {(ok_rep, err_rep, repaired_map)}")
            self.assertEqual(len(repaired_map), 1, f"Expected 1 repaired order: {repaired_map}")

    def test_c18_transport_interlock_blocks_when_sidecar_missing_or_expired(self) -> None:
        with LivePhase2DHarness() as h:
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True

            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)
            ok, msg = gate.run_unified_startup_verification(data_dir=str(h.root))
            self.assertTrue(ok, msg)
            self.assertTrue(gate.revalidate_for_transport_submission()[0])

            sc = h.root / "broker_evidence" / account_key(h.account_id) / "snapshots.sqlite3"
            # 1. Expire persisted snapshot row -> MUST block!
            with closing(sqlite3.connect(str(sc))) as c, c:
                c.execute("UPDATE snapshots SET expires_mono_ns = 1")
            ok_exp, msg_exp = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_exp)
            self.assertIn("UNIFIED_SNAPSHOT_MISSING_OR_EXPIRED", msg_exp)

            # 2. Delete account_state row -> MUST block!
            with closing(sqlite3.connect(str(sc))) as c, c:
                c.execute("DELETE FROM account_state")
            ok_as, msg_as = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_as)
            self.assertIn("UNIFIED_ACCOUNT_STATE_MISSING", msg_as)

            # 3. Delete snapshots.sqlite3 file completely -> MUST block!
            sc.unlink()
            ok_miss, msg_miss = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_miss)
            self.assertIn("UNIFIED_SIDECAR_MISSING", msg_miss)

    def test_c19_no_production_test_double_bypass_in_config_manager(self) -> None:
        src_text = Path("src/engine/config_manager.py").read_text(encoding="utf-8")
        self.assertNotIn("if not _is_real_dhan", src_text, "C19: production test-double bypass must be deleted")

    def test_c20_stop_intent_and_entry_fence_persisted_before_acquisition(self) -> None:
        with LivePhase2DHarness() as h:
            tracker = ConfigChangeTracker(data_dir=str(h.root))
            events = []
            orig_gate = tracker.set_transition_gate
            orig_intent = tracker.record_service_control_intent

            def _wrapped_gate(*a, **kw):
                events.append(("set_transition_gate", a[0] if a else kw.get("active")))
                return orig_gate(*a, **kw)

            def _wrapped_intent(*a, **kw):
                events.append(("record_service_control_intent", kw.get("deferred_stop_pending")))
                return orig_intent(*a, **kw)

            def _wrapped_safety(*a, **kw):
                events.append(("check_service_control_safety", True))
                return False, "SIMULATED_SAFETY_BLOCK", "SIMULATED_SAFETY_BLOCK", 1

            from src.engine.config_manager import ServiceOperatingState
            with patch.object(tracker, "resolve_service_operating_state", return_value=(ServiceOperatingState.RUNNING, "active")), \
                 patch.object(tracker, "set_transition_gate", side_effect=_wrapped_gate), \
                 patch.object(tracker, "record_service_control_intent", side_effect=_wrapped_intent), \
                 patch.object(tracker, "evaluate_trading_safety_gate_detailed", side_effect=_wrapped_safety):
                res = tracker.control_service("stop", reason="owner stop test", broker=h.client, db_path=h.db_path)

            self.assertFalse(res.get("ok"))
            names = [e[0] for e in events]
            self.assertIn("set_transition_gate", names)
            self.assertIn("record_service_control_intent", names)
            self.assertIn("check_service_control_safety", names)
            self.assertLess(
                names.index("set_transition_gate"),
                names.index("check_service_control_safety"),
                f"C20: entry fence MUST be activated before safety acquisition: {events}",
            )
            self.assertLess(
                names.index("record_service_control_intent"),
                names.index("check_service_control_safety"),
                f"C20: STOP intent MUST be persisted before safety acquisition: {events}",
            )

    # ------------------------------------------------------------------
    # 3. CONCURRENCY, DEADLOCK & CRASH-RECOVERY MATRIX
    # ------------------------------------------------------------------
    def test_concurrent_coordinator_acquisition_and_gate_halt_no_deadlock(self) -> None:
        with LivePhase2DHarness() as h:
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True
            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)

            errors = []
            done = threading.Event()

            def _acquire_worker():
                try:
                    for _ in range(3):
                        gate.run_unified_startup_verification(data_dir=str(h.root))
                except Exception as exc:
                    errors.append(exc)

            def _halt_worker():
                try:
                    for i in range(3):
                        gate.halt_new_entries(f"concurrent-halt-{i}")
                        time.sleep(0.01)
                except Exception as exc:
                    errors.append(exc)

            t1 = threading.Thread(target=_acquire_worker, daemon=True)
            t2 = threading.Thread(target=_halt_worker, daemon=True)
            t1.start()
            t2.start()
            t1.join(timeout=8.0)
            t2.join(timeout=8.0)
            self.assertFalse(t1.is_alive(), "Deadlock in concurrent startup verification worker")
            self.assertFalse(t2.is_alive(), "Deadlock in concurrent halt worker")
            self.assertEqual(errors, [], f"Unexpected concurrency errors: {errors}")

    def test_crash_recovery_persisted_stop_intent_survives_restart(self) -> None:
        with LivePhase2DHarness() as h:
            t1 = ConfigChangeTracker(data_dir=str(h.root))
            t1.set_transition_gate(True, reason="Owner STOP pending")
            t1.record_service_control_intent(
                desired_service_state="STOPPED",
                last_action="stop",
                reason="crash recovery test",
                requested_mode="LIVE",
                deferred_stop_pending=True,
            )
            # Simulate process restart: new ConfigChangeTracker reading same data_dir
            t2 = ConfigChangeTracker(data_dir=str(h.root))
            blocked, _why = t2.is_live_entry_blocked_by_transition()
            self.assertTrue(blocked, "Entry fence must survive restart")
            intent = t2.get_service_control_record()
            self.assertTrue(intent.get("deferred_stop_pending"), f"STOP intent must survive restart: {intent}")

    def test_corrupted_sidecar_fails_closed_never_fabricates_healthy(self) -> None:
        with LivePhase2DHarness() as h:
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True
            gate = StartupReconciliationGate(h.client, h.db, watcher, stop_mgr)
            ok, msg = gate.run_unified_startup_verification(data_dir=str(h.root))
            self.assertTrue(ok, msg)

            # Corrupt snapshots.sqlite3 with garbage bytes -> transport interlock MUST fail closed
            sc = h.root / "broker_evidence" / account_key(h.account_id) / "snapshots.sqlite3"
            sc.write_bytes(b"NOT_A_VALID_SQLITE_DATABASE_HEADER")
            try:
                ok_rv, msg_rv = gate.revalidate_for_transport_submission()
                self.assertFalse(ok_rv, "Corrupted sidecar must never return True")
            except sqlite3.DatabaseError:
                pass  # Raising DatabaseError is also fail-closed (never returns True)



import sqlite3
import tempfile
from unittest.mock import MagicMock, patch
from src.engine.emergency_stop import EmergencyStopManager
from src.journal.db import JournalDB

class TestPhase2DDefectsC30ToC42Certification(unittest.TestCase):
    """Independent positive + negative production-path certification for C30-C42."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="p2d_c30_c42_")
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = self.tmp.name
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.env_path = os.path.join(self.data_dir, ".env")
        self.token_path = os.path.join(self.data_dir, ".dhan_token")
        with open(self.env_path, "w", encoding="utf-8") as fh:
            fh.write("TRADING_MODE=LIVE\nDHAN_CLIENT_ID=1113731536\n")
        with open(self.stop_path, "w", encoding="utf-8") as fh:
            json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, fh)
        from src.engine.broker_handoff import proc_starttime
        with open(os.path.join(self.data_dir, "active_broker_mutations.json"), "w", encoding="utf-8") as fh:
            json.dump({
                "schema_version": 2, "account_id": "1113731536", "handoff_verified": True,
                "producer_pid": os.getpid(), "producer_starttime": proc_starttime(os.getpid()),
                "active": [], "unresolved": [],
            }, fh)
        from tests._phase2d_live_harness import make_synthetic_jwt
        with open(self.token_path, "w", encoding="utf-8") as _tf:
            _tf.write(make_synthetic_jwt("1113731536"))
        os.chmod(self.token_path, 0o600)
        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.stop_mgr = EmergencyStopManager(state_file=self.stop_path, expected_account_id="1113731536")

    def _make_live_client(self, *, orders=None, trades=None, positions=None, holdings=None, forever=None, super_orders=None, history=None, order_by_id=None):
        from src.config import AppConfig
        from src.broker.dhan_client import DhanClient
        from src.models import TradingMode
        from tests._phase2d_live_harness import SimulatedDhanTransport, make_http_response
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            dhan_token_file=self.token_path,
            dhan_access_token=Path(self.token_path).read_text(encoding="utf-8").strip(),
        )
        client = DhanClient(cfg, stop_manager=self.stop_mgr)
        _hist = list(history or [])
        _oby = dict(order_by_id or {})
        def _override(url, kw):
            path = url.split("?", 1)[0].rstrip("/")
            if "/trades/" in path:
                parts = path.split("/trades/", 1)[1].split("/")
                if len(parts) == 3:
                    return make_http_response(200, _hist if parts[2] == "0" else [])
            if "/orders/" in path and not path.endswith("/forever/orders") and not path.endswith("/super/orders"):
                oid = path.rsplit("/orders/", 1)[1]
                if oid in _oby:
                    return make_http_response(200, _oby[oid])
            return None
        transport = SimulatedDhanTransport(
            account_id="1113731536",
            orders=orders or [],
            trades=trades or [],
            positions=positions or [],
            holdings=holdings or [],
            forever_orders=forever or [],
            super_orders=super_orders or [],
            override_handler=_override,
        )
        client.session.get = transport.get
        return client, transport

    # ------------------------------------------------------------------
    # C30: Coherent wall-clock & monotonic-clock handling
    # ------------------------------------------------------------------
    def test_c30_coherent_wall_clock_follows_patched_time_and_unit_conversion(self):
        from src.engine.unified_account_runtime import UnifiedAccountRuntime
        fixed_sec = 1795689000.25  # 2026-11-26 16:00:00.25 IST
        with patch("time.time", return_value=fixed_sec):
            rt = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
            obs_ns = rt.utc_clock()
            self.assertEqual(obs_ns, int(fixed_sec * 1_000_000_000))
            self.assertGreater(rt.mono_clock(), 0)

    # ------------------------------------------------------------------
    # C31: Bounded single follow-up acquisition for additional_evidence
    # ------------------------------------------------------------------
    def test_c31_bounded_additional_evidence_followup_no_infinite_loop(self):
        from datetime import datetime, timedelta
        from src.engine.broker_session import IST
        from src.engine.unified_account_runtime import UnifiedAccountRuntime, build_verified_policy_context
        from src.broker.broker_snapshot import Purpose
        prev_dt = datetime.now(IST) - timedelta(days=2)
        prev_ts = float(int(prev_dt.timestamp()))
        prev_ex = datetime.fromtimestamp(prev_ts, IST).strftime("%Y-%m-%d %H:%M:%S")
        self.db.record_order_intent("C31-CORR", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("C31-CORR", state="TRADED", broker_order_id="C31-BUY")
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id='C31-BUY'", (prev_ts,))
            conn.execute(
                "INSERT INTO positions_lifecycle (parent_order_id,account_id,symbol,security_id,exchange_segment,product_type,protection_route,position_state,entry_qty,buy_filled_qty,sell_filled_qty,confirmed_open_qty,updated_at) "
                "VALUES ('C31-BUY','1113731536','YESBANK','11915','NSE_EQ','CNC','MANUAL','POSITION_CLOSED',1,1,1,0,?)",
                (prev_ts,),
            )
            conn.execute(
                "INSERT INTO fills (broker_order_id,exchange_trade_id,symbol,traded_price,traded_quantity,traded_at,transaction_type,security_id,exchange_segment,product_type,parent_order_id,is_verified_trade) "
                "VALUES ('C31-BUY','ET31B','YESBANK',20.0,1,?,'BUY','11915','NSE_EQ','CNC','C31-BUY',1)",
                (prev_ts,),
            )
            conn.execute(
                "INSERT INTO fills (broker_order_id,exchange_trade_id,symbol,traded_price,traded_quantity,traded_at,transaction_type,security_id,exchange_segment,product_type,parent_order_id,is_verified_trade) "
                "VALUES ('C31-SELL','ET31S','YESBANK',21.0,1,?,'SELL','11915','NSE_EQ','CNC','C31-BUY',1)",
                (prev_ts,),
            )
        hist = [
            {"dhanClientId": "1113731536", "orderId": "C31-BUY", "exchangeTradeId": "ET31B", "exchangeOrderId": "EXO31B", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 20.0, "exchangeTime": prev_ex},
            {"dhanClientId": "1113731536", "orderId": "C31-SELL", "exchangeTradeId": "ET31S", "exchangeOrderId": "EXO31S", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 1, "tradedPrice": 21.0, "exchangeTime": prev_ex},
        ]
        order_by_id = {
            "C31-SELL": {"dhanClientId": "1113731536", "orderId": "C31-SELL", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0}
        }
        client, transport = self._make_live_client(history=hist, order_by_id=order_by_id)
        rt = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
        res = rt.acquire_and_evaluate(
            client,
            policy=build_verified_policy_context(

                mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),

            policy_builder=lambda snap, jv: build_verified_policy_context(

                snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),
        )
        self.assertIsNotNone(res.verdict)
        self.assertTrue(res.verdict.entry_eligible, res.verdict.reason_codes)
        # Bounded: at most 2 /orders calls (initial + single follow-up)
        self.assertLessEqual(transport.endpoint_counts().get("/orders", 0), 2)

    # ------------------------------------------------------------------
    # C32: PRIOR_SIGNED requires verified calendar & non-contradictory books
    # ------------------------------------------------------------------
    def test_c32_prior_signed_overnight_weekend_holiday_and_contradictory_books(self):
        from datetime import date, datetime
        from src.engine.broker_session import IST, SessionPhase
        from src.engine.unified_account_runtime import UnifiedAccountRuntime, build_verified_policy_context
        from src.broker.broker_snapshot import Purpose
        # 1. Overnight 02:15 IST on a trading day (Thu 2026-11-26) -> PRIOR_SIGNED on Wed 2026-11-25
        overnight_sec = datetime(2026, 11, 26, 2, 15, 0, tzinfo=IST).timestamp()
        client, _ = self._make_live_client()
        with patch("time.time", return_value=overnight_sec):
            rt = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
            res = rt.acquire_and_evaluate(
                client,
                policy=build_verified_policy_context(

                    mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),

                policy_builder=lambda snap, jv: build_verified_policy_context(

                    snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),
            )
            self.assertIsNotNone(res.snapshot)
            self.assertEqual(res.snapshot.session.phase, SessionPhase.PRIOR_SIGNED)
            self.assertEqual(res.snapshot.session.trading_session_date, date(2026, 11, 25))
            self.assertTrue(res.snapshot.session.calendar_verified)
            self.assertTrue(res.verdict.entry_eligible)

        # 2. Weekend (Sunday 2026-11-29 14:00 IST) -> PRIOR_SIGNED on Friday 2026-11-27
        weekend_sec = datetime(2026, 11, 29, 14, 0, 0, tzinfo=IST).timestamp()
        with patch("time.time", return_value=weekend_sec):
            rt2 = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
            res2 = rt2.acquire_and_evaluate(
                client,
                policy=build_verified_policy_context(

                    mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),

                policy_builder=lambda snap, jv: build_verified_policy_context(

                    snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),
            )
            self.assertEqual(res2.snapshot.session.phase, SessionPhase.PRIOR_SIGNED)
            self.assertEqual(res2.snapshot.session.trading_session_date, date(2026, 11, 27))

        # 3. Contradictory books during 02:15 IST overnight window MUST STILL BLOCK!
        bad_client, _ = self._make_live_client(
            holdings=[{"dhanClientId": "1113731536", "securityId": "11915", "isin": "INE528G01035", "totalQty": 5, "dpQty": 5, "t1Qty": 0, "availableQty": 5}]
        )
        with patch("time.time", return_value=overnight_sec):
            rt3 = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
            res3 = rt3.acquire_and_evaluate(
                bad_client,
                policy=build_verified_policy_context(

                    mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),

                policy_builder=lambda snap, jv: build_verified_policy_context(

                    snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

                ),
            )
            self.assertIsNotNone(res3.verdict)
            self.assertFalse(res3.verdict.entry_eligible)

    # ------------------------------------------------------------------
    # C33 + C42: STOP fence accepts own +1 owner_epoch, rejects concurrent command
    # ------------------------------------------------------------------
    def test_c33_c42_stop_fence_accepts_own_stop_and_rejects_concurrent_owner_command(self):
        from src.engine.config_manager import ConfigChangeTracker
        client, _ = self._make_live_client()
        tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.addCleanup(lambda: tracker.stop_coordinator_daemon(join_timeout=1.0))

        # Positive: clean flat LIVE STOP executes cleanly (including first-ever sidecar creation)
        svc_state = {"s": "active"}
        sys_calls = []
        def _sys_fn(act, svc):
            sys_calls.append((act, svc))
            if act == "stop":
                svc_state["s"] = "inactive"
            return True
        with patch.object(tracker, "get_active_running_version", return_value={"trading_mode": "LIVE", "pid": os.getpid()}):
            res = tracker.control_service(
                action="stop", db_path=self.db_path, stop_state_path=self.stop_path,
                env_path=self.env_path, broker=client, reason="Legitimate owner stop",
                systemctl_fn=_sys_fn, check_service_fn=lambda: svc_state["s"],
            )
            self.assertTrue(res["ok"], res)
            self.assertEqual(sys_calls, [("stop", "trading_bot.service")])

        # Negative: concurrent unrelated owner START command during safety check MUST block STOP!
        svc_state["s"] = "active"
        orig_eval = tracker.evaluate_trading_safety_gate_detailed
        def inject_concurrent_start(*a, **kw):
            r = orig_eval(*a, **kw)
            tracker.record_service_control_intent(
                desired_service_state="ACTIVE", last_action="OWNER_START",
                reason="Concurrent unrelated START", requested_mode="LIVE",
            )
            return r
        with patch.object(tracker, "get_active_running_version", return_value={"trading_mode": "LIVE", "pid": os.getpid()}), \
             patch.object(tracker, "evaluate_trading_safety_gate_detailed", side_effect=inject_concurrent_start):
            res2 = tracker.control_service(
                action="stop", db_path=self.db_path, stop_state_path=self.stop_path,
                env_path=self.env_path, broker=client, reason="Stale stop",
                systemctl_fn=lambda act, svc: (_ for _ in ()).throw(AssertionError("Must not stop after concurrent START")),
                check_service_fn=lambda: svc_state["s"],
            )
            self.assertFalse(res2["ok"])
            self.assertEqual(res2.get("blocked_reason"), "STALE_AUTHORIZATION_GENERATION_CHANGED")

    # ------------------------------------------------------------------
    # C34 + C37: RiskManager immutable MappingProxyType snapshot calculations
    # ------------------------------------------------------------------
    def test_c34_c37_risk_manager_immutable_mapping_proxy_snapshot_positive_and_negative(self):
        from types import MappingProxyType
        from src.config import AppConfig
        from src.engine.risk_manager import RiskManager
        from src.engine.unified_account_runtime import UnifiedAccountRuntime, build_verified_policy_context
        from src.broker.broker_snapshot import Purpose
        from src.models import TradingMode

        # Acquire a REAL immutable BrokerSnapshot with MappingProxyType position rows
        client, _ = self._make_live_client(
            positions=[{
                "dhanClientId": "1113731536", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "netQty": 10, "buyAvg": 25.0, "realizedProfit": -150.0, "unrealizedProfit": 20.0,
            }]
        )
        rt = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
        res = rt.acquire_and_evaluate(
            client,
            policy=build_verified_policy_context(

                mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),

            policy_builder=lambda snap, jv: build_verified_policy_context(

                snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),
        )
        self.assertIsNotNone(res.snapshot)
        self.assertIsInstance(res.snapshot.positions.payload[0], MappingProxyType)

        cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir, journal_db_path=self.db_path)
        rm = RiskManager(cfg, self.db)
        ok_sync, msg_sync = rm.sync_from_journal_and_broker(broker_snapshot=res.snapshot)
        self.assertTrue(ok_sync, msg_sync)
        self.assertTrue(rm._broker_positions_verified)
        self.assertAlmostEqual(rm._current_exposure, 250.0, places=2)
        self.assertAlmostEqual(rm._daily_realized_pnl, -150.0, places=2)

        # Negative 1: non-BrokerSnapshot object fails closed
        rm2 = RiskManager(cfg, self.db)
        ok_bad, _ = rm2.sync_from_journal_and_broker(broker_snapshot=object())
        self.assertFalse(ok_bad)
        self.assertFalse(rm2._broker_positions_verified)

        # Negative 2: missing price on open position fails closed
        from src.broker.broker_snapshot import account_key
        _sc_c34 = os.path.join(self.data_dir, "broker_evidence", account_key("1113731536"), "snapshots.sqlite3")
        if os.path.exists(_sc_c34):
            os.remove(_sc_c34)
        client_bad, _ = self._make_live_client(
            positions=[{
                "dhanClientId": "1113731536", "securityId": "11915", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "netQty": 5, "buyAvg": 0.0, "costPrice": 0.0, "realizedProfit": 0.0,
            }]
        )
        rt2 = UnifiedAccountRuntime(data_dir=self.data_dir, db_path=self.db_path, account_id="1113731536")
        res_bad = rt2.acquire_and_evaluate(
            client_bad,
            policy=build_verified_policy_context(

                mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),

            policy_builder=lambda snap, jv: build_verified_policy_context(

                snapshot=snap, journal_view=jv, mode="LIVE", purpose=Purpose.STARTUP, data_dir=self.data_dir, stop_manager=self.stop_mgr, watcher=__import__("src.engine.protection_watcher", fromlist=["ProtectionWatcher"]).ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w_cert.json"), dry_run=False, journal_db=self.db), risk_funds_verified=True

            ),
        )
        rm3 = RiskManager(cfg, self.db)
        ok_np, _ = rm3.sync_from_journal_and_broker(broker_snapshot=res_bad.snapshot)
        self.assertFalse(ok_np)
        self.assertFalse(rm3._broker_positions_verified)

    # ------------------------------------------------------------------
    # C35 + C36 + C41: Comprehensive TOCTOU Audit on real DhanClient.place_order()
    # ------------------------------------------------------------------
    def test_c35_c36_c41_toctou_transport_interlock_positive_and_all_adversarial_races(self):
        from src.engine.protection_watcher import ProtectionWatcher
        from src.engine.reconciliation import StartupReconciliationGate
        from src.models import OrderRequest, OrderType, ProductType, TransactionType
        from src.broker.broker_snapshot import account_key
        from src.broker.snapshot_coordinator import invalidate_owner_scope

        def setup_healthy_gate():
            client, _ = self._make_live_client()
            watcher = ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w.json"), dry_run=False, journal_db=self.db)
            gate = StartupReconciliationGate(broker=client, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
            gate.enable_mandatory_unified_verdict()
            client.reconciliation_gate = gate
            ok, msg = gate.run_startup_reconciliation()
            self.assertTrue(ok, msg)
            return client, gate

        def make_req(cid):
            return OrderRequest(
                correlation_id=cid, symbol="YESBANK", security_id="11915",
                exchange_segment="NSE_EQ", product_type=ProductType.CNC,
                transaction_type=TransactionType.BUY, order_type=OrderType.LIMIT,
                quantity=1, price=20.0,
            )

        # Positive: single newly written PENDING_SUBMIT intent for corr_id passes both pre-check & post-registration interlock
        client, gate = setup_healthy_gate()
        cid_ok = "C41-OK-01"
        self.db.record_order_intent(cid_ok, "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent(cid_ok, state="PENDING_SUBMIT")
        fake_post = MagicMock()
        fake_post.return_value = MagicMock(status_code=200, text=json.dumps({"orderId": "ORD-C41-OK", "orderStatus": "PENDING"}))
        fake_post.return_value.json.return_value = {"orderId": "ORD-C41-OK", "orderStatus": "PENDING"}
        with patch.object(client.session, "post", fake_post):
            ok_p, oid_p, msg_p = client.place_order(make_req(cid_ok))
            self.assertTrue(ok_p, msg_p)
            self.assertEqual(oid_p, "ORD-C41-OK")
            fake_post.assert_called_once()
        # Clean up intent so subsequent fresh gates see flat DB
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM order_intents WHERE correlation_id=?", (cid_ok,))

        # Negative TOCTOU races injected INSIDE try_begin_mutation (after pre-check, BEFORE HTTP POST!):
        ak = account_key("1113731536")
        sc_path = os.path.join(self.data_dir, "broker_evidence", ak, "snapshots.sqlite3")

        def run_toctou_race(race_name, race_fn):
            c, g = setup_healthy_gate()
            cid = f"C41-RACE-{race_name}"[:30]
            self.db.record_order_intent(cid, "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
            self.db.update_order_intent(cid, state="PENDING_SUBMIT")
            orig_begin = self.stop_mgr.try_begin_mutation
            def racing_begin(correlation_id):
                res = orig_begin(correlation_id)
                race_fn(cid)
                return res
            post_spy = MagicMock(side_effect=AssertionError(f"HTTP POST MUST NOT FIRE during race {race_name}"))
            with patch.object(self.stop_mgr, "try_begin_mutation", side_effect=racing_begin),                  patch.object(c.session, "post", post_spy):
                ok_r, oid_r, msg_r = c.place_order(make_req(cid))
            post_spy.assert_not_called()
            self.assertFalse(ok_r, f"Race {race_name} MUST fail closed, got ok=True")
            self.assertIsNone(oid_r)
            # Reset stop_mgr and DB for next race
            with open(self.stop_path, "w", encoding="utf-8") as _sf:
                json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, _sf)
            with self.db._get_conn() as conn:
                conn.execute("DELETE FROM order_intents")
                conn.execute("DELETE FROM fills")

        # 1. Concurrent Owner STOP (owner_epoch advance)
        def adv_owner(_cid):
            with closing(sqlite3.connect(sc_path)) as sc, sc:
                sc.execute("UPDATE account_state SET owner_epoch=owner_epoch+1 WHERE account_key=?", (ak,))
        run_toctou_race("OWNER_STOP", adv_owner)
        # 2. Concurrent Emergency STOP
        run_toctou_race("EMERG_STOP", lambda _cid: self.stop_mgr.trigger_stop("RaceAudit", "Concurrent emergency stop"))
        # 3. Concurrent Token Rotation
        def rot_tok(_cid):
            with closing(sqlite3.connect(sc_path)) as sc, sc:
                sc.execute("UPDATE account_state SET token_epoch=token_epoch+1 WHERE account_key=?", (ak,))
        run_toctou_race("TOKEN_ROT", rot_tok)
        # 4. Concurrent Journal Modification (unrelated second order_intent)
        run_toctou_race("UNREL_INTENT", lambda _cid: self.db.record_order_intent("ROGUE-OTHER", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1))
        # 5. Concurrent Parent Fill
        run_toctou_race("PARENT_FILL", lambda _cid: self.db.record_fill("ROGUE-BUY", "ET-ROGUE-1", "YESBANK", 20.0, 1, security_id="11915", transaction_type="BUY", exchange_segment="NSE_EQ", product_type="CNC"))
        # 6. Concurrent Extra Mutation Registration (+2 mutation_epoch instead of +1)
        def extra_mut(_cid):
            with closing(sqlite3.connect(sc_path)) as sc, sc:
                sc.execute("UPDATE account_state SET mutation_epoch=mutation_epoch+2 WHERE account_key=?", (ak,))
        run_toctou_race("EXTRA_MUT", extra_mut)
        # 7. Generation Invalidation (boot_id drift in snapshots.sqlite3)
        def drift_boot(_cid):
            with closing(sqlite3.connect(sc_path)) as sc, sc:
                sc.execute("UPDATE account_state SET boot_id='drifted-boot-id' WHERE account_key=?", (ak,))
        run_toctou_race("BOOT_DRIFT", drift_boot)
        # 8. Snapshot Expiration (expires_mono_ns elapsed)
        def expire_snap(_cid):
            with closing(sqlite3.connect(sc_path)) as sc, sc:
                sc.execute("UPDATE snapshots SET expires_mono_ns=1 WHERE account_key=?", (ak,))
        run_toctou_race("SNAP_EXPIRE", expire_snap)

    # ------------------------------------------------------------------
    # C38 + C39 + C40: Production fixes discovered during regression triage
    # ------------------------------------------------------------------
    def test_c38_c39_c40_retry_eligible_multilot_and_historical_timestamp(self):
        from src.engine.protection_watcher import ProtectionWatcher
        from src.engine.watcher_risk_classification import legacy_risks
        from src.broker.dhan_client import DhanClient

        # C38: active order preserves retry_eligible=None; verified terminal TRADED/POSITION_CLOSED clears it to False
        client, _ = self._make_live_client()
        w = ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w38.json"), dry_run=False, journal_db=self.db)
        w.track(order_id="ORD-C38", correlation_id="CORR-C38", symbol="YESBANK", security_id="11915", segment="NSE_EQ", product_type="CNC", qty=1)
        self.assertIsNone(w.pending["ORD-C38"]["retry_eligible"])
        w.reconcile_historical_order_fills(
            order_id="ORD-C38", correlation_id="CORR-C38", symbol="YESBANK", security_id="11915",
            segment="NSE_EQ", product_type="CNC", requested_qty=1, filled_qty=1, avg_fill_price=20.0,
            verified_protected_qty=1, confirmed_protection_ids=["FO-C38"],
        )
        self.assertIs(w.terminal_orders["ORD-C38"]["retry_eligible"], False)
        self.assertNotIn("LEGACY_UNCLASSIFIED_RISK:retry_eligible", legacy_risks(w.terminal_orders["ORD-C38"]))

        # C40: validate_historical_trade_record parses exchangeTime into traded_at
        ok_v, norm, err = DhanClient.validate_historical_trade_record(
            {
                "dhanClientId": "1113731536", "orderId": "ORD-C40", "exchangeTradeId": "ET40",
                "exchangeOrderId": "EXO40", "exchangeTime": "2026-11-02 14:01:18",
                "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.52,
            },
            expected_account_id="1113731536",
        )
        self.assertTrue(ok_v, err)
        self.assertIsNotNone(norm.get("traded_at"))
        self.assertLess(norm["traded_at"], time.time() - 86400)


class TestPhase2DDefectsC44ToC50Certification(unittest.TestCase):
    """Comprehensive positive + negative production-path tests for C44-C50."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="p2d_c44_c50_")
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = self.tmp.name
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.env_path = os.path.join(self.data_dir, ".env")
        self.token_path = os.path.join(self.data_dir, ".dhan_token")
        with open(self.env_path, "w", encoding="utf-8") as fh:
            fh.write("TRADING_MODE=LIVE\nDHAN_CLIENT_ID=1113731536\n")
        with open(self.stop_path, "w", encoding="utf-8") as fh:
            json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, fh)
        from src.engine.broker_handoff import proc_starttime
        with open(os.path.join(self.data_dir, "active_broker_mutations.json"), "w", encoding="utf-8") as fh:
            json.dump({
                "schema_version": 2, "account_id": "1113731536", "handoff_verified": True,
                "producer_pid": os.getpid(), "producer_starttime": proc_starttime(os.getpid()),
                "active": [], "unresolved": [],
            }, fh)
        from tests._phase2d_live_harness import make_synthetic_jwt
        with open(self.token_path, "w", encoding="utf-8") as fh:
            fh.write(make_synthetic_jwt("1113731536"))
        os.chmod(self.token_path, 0o600)
        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.stop_mgr = EmergencyStopManager(state_file=self.stop_path, expected_account_id="1113731536")

    def _make_live_stack(self, *, orders=None):
        from src.config import AppConfig
        from src.broker.dhan_client import DhanClient
        from src.engine.protection_watcher import ProtectionWatcher
        from src.engine.reconciliation import StartupReconciliationGate
        from src.models import TradingMode
        from tests._phase2d_live_harness import SimulatedDhanTransport
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE, dhan_client_id="1113731536",
            data_dir=self.data_dir, journal_db_path=self.db_path,
            dhan_token_file=self.token_path,
            dhan_access_token=Path(self.token_path).read_text(encoding="utf-8").strip(),
        )
        client = DhanClient(cfg, stop_manager=self.stop_mgr)
        transport = SimulatedDhanTransport(account_id="1113731536", orders=orders or [])
        client.session.get = transport.get
        watcher = ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w.json"), dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=client, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        gate.enable_mandatory_unified_verdict()
        client.reconciliation_gate = gate
        ok, msg = gate.run_startup_reconciliation()
        self.assertTrue(ok, msg)
        return client, gate, watcher, transport

    def _req(self, cid, *, sec="11915", qty=1, price=20.0):
        from src.models import OrderRequest, OrderType, ProductType, TransactionType
        return OrderRequest(
            correlation_id=cid, symbol="YESBANK", security_id=sec,
            exchange_segment="NSE_EQ", product_type=ProductType.CNC,
            transaction_type=TransactionType.BUY, order_type=OrderType.LIMIT,
            quantity=qty, price=price,
        )

    # ------------------------------------------------------------------
    # C44: Final protected transport-admission boundary races + uncertain send
    # ------------------------------------------------------------------
    def test_c44_final_transport_boundary_blocks_all_pre_send_races_and_preserves_unknown_post_send(self):
        client, gate, _, _ = self._make_live_stack()
        self.db.record_order_intent("C44-HALT", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("C44-HALT", state="PENDING_SUBMIT")
        orig_http = client._broker_mutating_http
        def halt_at_http(*a, **kw):
            gate.halt_new_entries("C44 race at _broker_mutating_http")
            return orig_http(*a, **kw)
        post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed on C44 race"))
        with patch.object(client, "_broker_mutating_http", side_effect=halt_at_http),              patch.object(client.session, "post", post_spy):
            ok, oid, msg = client.place_order(self._req("C44-HALT"))
        self.assertFalse(ok)
        self.assertIsNone(oid)
        post_spy.assert_not_called()

        # Super Order path: race right before _broker_mutating_http also produces 0 HTTP POSTs
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM order_intents")
        self.assertTrue(gate.run_startup_reconciliation()[0])
        self.db.record_order_intent("C44-SUP", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1, is_super_order=True)
        self.db.update_order_intent("C44-SUP", state="PENDING_SUBMIT")
        with patch.object(client, "_broker_mutating_http", side_effect=halt_at_http),              patch.object(client.session, "post", post_spy):
            ok_s, oid_s, _ = client.place_super_order(self._req("C44-SUP"), stop_loss_price=19.0, target_price=22.0)
        self.assertFalse(ok_s)
        self.assertIsNone(oid_s)
        post_spy.assert_not_called()

        # Post-send uncertain timeout: MUST NOT falsely record NOT_SUBMITTED; preserves durable unresolved mutation
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM order_intents")
        self.assertTrue(gate.run_startup_reconciliation()[0])
        self.db.record_order_intent("C44-TIMEOUT", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("C44-TIMEOUT", state="PENDING_SUBMIT")
        import requests
        with patch.object(client.session, "post", side_effect=requests.Timeout("read timed out after send")):
            ok_t, oid_t, msg_t = client.place_order(self._req("C44-TIMEOUT"))
        self.assertFalse(ok_t)
        self.assertNotIn("NOT_SUBMITTED", msg_t)
        ledger = json.loads(Path(os.path.join(self.data_dir, "active_broker_mutations.json")).read_text(encoding="utf-8"))
        self.assertTrue(len(ledger.get("unresolved") or []) >= 1, "Post-send timeout must record durable unresolved obligation")

    # ------------------------------------------------------------------
    # C45: Exact outgoing payload binding to durable order_intent
    # ------------------------------------------------------------------
    def test_c45_exact_payload_binding_rejects_mismatches_and_allows_exact_match(self):
        client, gate, _, _ = self._make_live_stack()
        self.db.record_order_intent("AUDIT-MISMATCH", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("AUDIT-MISMATCH", state="PENDING_SUBMIT")
        post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed on C45 mismatch"))

        # Mismatched security, quantity, price
        for bad_req in (
            self._req("AUDIT-MISMATCH", sec="3045", qty=1, price=20.0),
            self._req("AUDIT-MISMATCH", sec="11915", qty=7, price=20.0),
            self._req("AUDIT-MISMATCH", sec="11915", qty=1, price=25.0),
        ):
            with patch.object(client.session, "post", post_spy):
                ok, oid, msg = client.place_order(bad_req)
            self.assertFalse(ok)
            self.assertIn("UNIFIED_INTENT_PAYLOAD_MISMATCH", msg)
            post_spy.assert_not_called()

        # Exact matching payload succeeds
        post_ok = MagicMock(return_value=MagicMock(status_code=200, text='{"orderId":"ORD-C45-OK"}', json=lambda: {"orderId": "ORD-C45-OK"}))
        with patch.object(client.session, "post", post_ok):
            ok_p, oid_p, msg_p = client.place_order(self._req("AUDIT-MISMATCH", sec="11915", qty=1, price=20.0))
        self.assertTrue(ok_p, msg_p)
        self.assertEqual(oid_p, "ORD-C45-OK")
        post_ok.assert_called_once()

    # ------------------------------------------------------------------
    # C46: Strict orphan parent & group linkage
    # ------------------------------------------------------------------
    def test_c46_orphan_linkage_rejects_omitted_or_conflicting_and_accepts_verified_linkage(self):
        from datetime import date
        from src.broker.broker_snapshot import BookEvidence, BookStatus, BrokerSnapshot, Generation, SessionContext, SessionPhase, account_key, freeze_json, payload_digest
        from src.engine.durable_risk_resolution import resolve_durable_orphan_legs

        def mk_snap(sid, mns, ob_rows, fo_rows=()):
            ak = account_key("1113731536")
            gen = Generation(ak, "boot-1", 1, 1, 1, 1, 1)
            sess = SessionContext(SessionPhase.CURRENT_SIGNED, date(2026, 11, 26), 1795700000000000000, "a"*64, True)
            def bk(k, ep, payload):
                pl = list(payload) if isinstance(payload, (list, tuple)) else payload
                return BookEvidence(k, ep, freeze_json({}), BookStatus.OK_RECORDS if pl else BookStatus.OK_EMPTY,
                                    200, None, None, True, 1795700000000000000, 1795700000000000100, mns, mns+100,
                                    "1113731536", 1, freeze_json(pl), payload_digest(pl), True)
            return BrokerSnapshot(sid, gen, "1113731536", "1113731536", True, sess,
                                  1795700000000000000, 1795700000000000100, mns, mns+100,
                                  holdings=bk("holdings", "/v2/holdings", []),
                                  positions=bk("positions", "/v2/positions", []),
                                  order_book=bk("order_book", "/v2/orders", ob_rows),
                                  trade_book=bk("trade_book", "/v2/trades", []),
                                  forever_orders=bk("forever_orders", "/v2/forever/orders", fo_rows),
                                  super_orders=bk("super_orders", "/v2/super/orders", []),
                                  historical_trades=(), order_status=freeze_json({}), trades_for_order=freeze_json({}),
                                  profile=bk("profile", "/v2/profile", {"dhanClientId": "1113731536"}),
                                  funds=None, edis=None, history_coverage=(), complete=True, invalid_reasons=(),
                                  producer_pid=os.getpid(), producer_starttime=1, journal_digest="b"*64,
                                  profile_valid_until_mono_ns=mns+60_000_000_000)

        rec = {"order_id": "PARENT-1", "security_id": "3045", "segment": "NSE_EQ", "product_type": "CNC",
               "orphaned_leg_cancel_details": {"LEG-99": {"group_id": "GROUP-1"}}}
        base_row = {"dhanClientId": "1113731536", "orderId": "LEG-99", "securityId": "3045",
                    "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL",
                    "orderStatus": "CANCELLED", "quantity": 5, "filledQty": 0, "remainingQuantity": 0}

        # 1. Missing both parentOrderId and algoId -> REJECTED
        out_miss = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [base_row]), mk_snap("S2", 13_000_000_000, [base_row]), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertFalse(out_miss.resolved)
        self.assertEqual(out_miss.reason_code, "ORPHAN_PARENT_LINKAGE_UNVERIFIED")

        # 2. Wrong parent -> REJECTED
        wrong_p = dict(base_row, parentOrderId="OTHER-PARENT", algoId="GROUP-1")
        out_wp = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [wrong_p]), mk_snap("S2", 13_000_000_000, [wrong_p]), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertFalse(out_wp.resolved)
        self.assertEqual(out_wp.reason_code, "ORDER_PARENT_BINDING_MISMATCH")

        # 3. Residual executable sibling in forever_orders -> REJECTED
        fo_live = [{"dhanClientId": "1113731536", "orderId": "GROUP-1", "parentOrderId": "PARENT-1", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "orderStatus": "CONFIRM", "triggeredOrderId": "LEG-99"}]
        out_live = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [base_row], fo_live), mk_snap("S2", 13_000_000_000, [base_row], fo_live), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertFalse(out_live.resolved)
        self.assertIn("ORPHAN_RESIDUAL_EXECUTABLE_SIBLING", out_live.reason_code)

        # 4. Valid explicit parentOrderId + algoId -> RESOLVED
        good_row = dict(base_row, parentOrderId="PARENT-1", algoId="GROUP-1")
        out_ok = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [good_row]), mk_snap("S2", 13_000_000_000, [good_row]), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertTrue(out_ok.resolved, out_ok)

    # ------------------------------------------------------------------
    # C47: Production watcher acquires paired independent snapshots
    # ------------------------------------------------------------------
    def test_c47_watcher_acquires_two_independent_snapshots_and_resolves_cancellation(self):
        dead_row = {"dhanClientId": "1113731536", "orderId": "ORD-C47", "securityId": "11915",
                    "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY",
                    "orderStatus": "CANCELLED", "quantity": 1, "filledQty": 0, "remainingQuantity": 0}
        client, gate, watcher, _ = self._make_live_stack(orders=[dead_row])
        rec = {"order_id": "ORD-C47", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC",
               "cancel_uncertain": True, "cancel_requested": True}
        # Tick 1 acquires snap1 and returns None (cannot resolve from 1 capture)
        p1 = watcher.acquire_paired_resolution_snapshots(rec, expected_account_id="1113731536", min_separation_ns=50_000_000)
        self.assertIsNone(p1)
        time.sleep(0.06)
        # Tick 2 after separation acquires independent snap2 and returns paired distinct snapshots
        p2 = watcher.acquire_paired_resolution_snapshots(rec, expected_account_id="1113731536", min_separation_ns=50_000_000)
        self.assertIsNotNone(p2)
        self.assertNotEqual(p2[0].snapshot_id, p2[1].snapshot_id)

    # ------------------------------------------------------------------
    # C48: Persistence-risk readback rejects partial commits (watcher_orders only)
    # ------------------------------------------------------------------
    def test_c48_persistence_readback_rejects_uncommitted_lifecycle_or_fills(self):
        from src.engine.durable_issue_writer import verify_operation_readback_and_clear_persistence_risk
        rec = {"order_id": "ORD-C48", "persistence_uncertain": True, "owner_readback_failed": True}
        with self.db._get_conn() as conn:
            conn.execute("INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                         ("ORD-C48", 0, json.dumps({"order_id": "ORD-C48", "position_state": "PROTECTED", "filled_qty": 3}), time.time()))
        # watcher_orders matches, but positions_lifecycle & fills NOT committed -> MUST FAIL CLOSED!
        ok1, r1 = verify_operation_readback_and_clear_persistence_risk(
            self.db_path, rec, expected_order_id="ORD-C48", expected_position_state="PROTECTED", expected_filled_qty=3
        )
        self.assertFalse(ok1)
        self.assertEqual(r1, "TARGET_LIFECYCLE_NOT_COMMITTED")
        self.assertTrue(rec.get("persistence_uncertain"))

        # Commit lifecycle + fills -> MUST PASS and clear flags!
        with self.db._get_conn() as conn:
            conn.execute(
                "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("ORD-C48", "1113731536", "YESBANK", "11915", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED", 3, 3, 0, 3, time.time()),
            )
            conn.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                ("ORD-C48", "ET-C48", "YESBANK", 20.0, 3, time.time(), "BUY", "11915", "NSE_EQ", "CNC", "ORD-C48"),
            )
        ok2, r2 = verify_operation_readback_and_clear_persistence_risk(
            self.db_path, rec, expected_order_id="ORD-C48", expected_position_state="PROTECTED",
            expected_filled_qty=3, expected_account_id="1113731536", expected_fill_ids=["ET-C48"],
        )
        self.assertTrue(ok2, r2)
        self.assertFalse(rec.get("persistence_uncertain", False))

    # ------------------------------------------------------------------
    # C49: control_service("stop") calls evaluate_unified_shutdown_gate()
    # ------------------------------------------------------------------
    def test_c49_control_service_stop_invokes_evaluate_unified_shutdown_gate(self):
        from src.engine.config_manager import ConfigChangeTracker
        client, _, _, _ = self._make_live_stack()
        tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.addCleanup(lambda: tracker.stop_coordinator_daemon(join_timeout=1.0))
        called = {"n": 0}
        orig_gate = tracker.evaluate_unified_shutdown_gate
        def spy_gate(*a, **kw):
            called["n"] += 1
            return orig_gate(*a, **kw)
        svc = {"s": "active"}
        def _sys(act, _svc):
            if act == "stop":
                svc["s"] = "inactive"
            return True
        with patch.object(tracker, "evaluate_unified_shutdown_gate", side_effect=spy_gate),              patch.object(tracker, "get_active_running_version", return_value={"trading_mode": "LIVE", "pid": os.getpid()}):
            res = tracker.control_service(
                action="stop", db_path=self.db_path, stop_state_path=self.stop_path,
                env_path=self.env_path, broker=client, reason="C49 spy test",
                systemctl_fn=_sys, check_service_fn=lambda: svc["s"],
            )
        self.assertTrue(res["ok"], res)
        self.assertEqual(called["n"], 1)


class TestPhase2DArchitectureClosureC51ToC57(unittest.TestCase):
    """Comprehensive positive + negative production-path tests for C51-C57 architecture closures."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="p2d_c51_c57_")
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = self.tmp.name
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.token_path = os.path.join(self.data_dir, ".dhan_token")
        with open(self.stop_path, "w", encoding="utf-8") as fh:
            json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, fh)
        from src.engine.broker_handoff import proc_starttime
        with open(os.path.join(self.data_dir, "active_broker_mutations.json"), "w", encoding="utf-8") as fh:
            json.dump({
                "schema_version": 2, "account_id": "1113731536", "handoff_verified": True,
                "producer_pid": os.getpid(), "producer_starttime": proc_starttime(os.getpid()),
                "active": [], "unresolved": [],
            }, fh)
        from tests._phase2d_live_harness import make_synthetic_jwt
        with open(self.token_path, "w", encoding="utf-8") as fh:
            fh.write(make_synthetic_jwt("1113731536"))
        os.chmod(self.token_path, 0o600)
        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.stop_mgr = EmergencyStopManager(state_file=self.stop_path, expected_account_id="1113731536")

    def _make_stack(self, *, orders=None):
        from src.config import AppConfig
        from src.broker.dhan_client import DhanClient
        from src.engine.protection_watcher import ProtectionWatcher
        from src.engine.reconciliation import StartupReconciliationGate
        from src.models import TradingMode
        from tests._phase2d_live_harness import SimulatedDhanTransport
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE, dhan_client_id="1113731536",
            data_dir=self.data_dir, journal_db_path=self.db_path,
            dhan_token_file=self.token_path,
            dhan_access_token=Path(self.token_path).read_text(encoding="utf-8").strip(),
            equity_sl_pct=1.0, equity_target_pct=2.0,
        )
        client = DhanClient(cfg, stop_manager=self.stop_mgr)
        transport = SimulatedDhanTransport(account_id="1113731536", orders=orders or [])
        client.session.get = transport.get
        watcher = ProtectionWatcher(broker=client, path=os.path.join(self.data_dir, "w.json"), dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=client, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        gate.enable_mandatory_unified_verdict()
        client.reconciliation_gate = gate
        ok, msg = gate.run_startup_reconciliation()
        self.assertTrue(ok, msg)
        return client, gate, watcher

    def _req(self, cid, *, sec="11915", qty=1, price=20.0):
        from src.models import OrderRequest, OrderType, ProductType, TransactionType
        return OrderRequest(
            correlation_id=cid, symbol="YESBANK", security_id=sec,
            exchange_segment="NSE_EQ", product_type=ProductType.CNC,
            transaction_type=TransactionType.BUY, order_type=OrderType.LIMIT,
            quantity=qty, price=price,
        )

    # ------------------------------------------------------------------
    # C51: Atomic transport-admission linearization point catches halt INSIDE get_in_flight()
    # ------------------------------------------------------------------
    def test_c51_atomic_linearization_blocks_halt_injected_during_get_in_flight(self):
        client, gate, _ = self._make_stack()
        self.db.record_order_intent("C51-HALT-IN-FLIGHT", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("C51-HALT-IN-FLIGHT", state="PENDING_SUBMIT")

        orig_get_if = self.stop_mgr.get_in_flight
        def halt_inside_get_in_flight():
            res = orig_get_if()
            gate.halt_new_entries("C51 concurrent halt during get_in_flight")
            return res

        post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed when halted during admission"))
        with patch.object(self.stop_mgr, "get_in_flight", side_effect=halt_inside_get_in_flight),              patch.object(client.session, "post", post_spy):
            ok, oid, msg = client.place_order(self._req("C51-HALT-IN-FLIGHT"))
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertFalse(gate.is_accepting_entries)
        post_spy.assert_not_called()

    # ------------------------------------------------------------------
    # C52 + C57: Super Order protection bounds + exact retry child-correlation binding
    # ------------------------------------------------------------------
    def test_c52_c57_super_order_protection_bounds_and_retry_child_correlation_binding(self):
        client, gate, _ = self._make_stack()

        # C52 Negative: absurd Super Order protection levels (SL=0.05, target=200, trailingJump=999 on entry=20)
        self.db.record_order_intent("C52-SUPER-BAD", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1, is_super_order=True)
        self.db.update_order_intent("C52-SUPER-BAD", state="PENDING_SUBMIT")
        post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed on out-of-bounds Super Order"))
        with patch.object(client.session, "post", post_spy):
            ok_bad, oid_bad, msg_bad = client.place_super_order(
                self._req("C52-SUPER-BAD", price=20.0),
                stop_loss_price=0.05, target_price=200.0, trailing_jump=999.0,
            )
        self.assertFalse(ok_bad)
        self.assertIn("UNIFIED_SUPER_PROTECTION_OUT_OF_BOUNDS", msg_bad)
        post_spy.assert_not_called()

        # C52 Positive: valid in-bounds Super Order (entry=20.0, SL=19.80 (1%), target=20.40 (2%), trailing=0.10) succeeds
        post_ok = MagicMock(return_value=MagicMock(status_code=200, text='{"orderId":"SUP-C52-OK"}', json=lambda: {"orderId": "SUP-C52-OK"}))
        with patch.object(client.session, "post", post_ok):
            ok_good, oid_good, msg_good = client.place_super_order(
                self._req("C52-SUPER-BAD", price=20.0),
                stop_loss_price=19.80, target_price=20.40, trailing_jump=0.10,
            )
        self.assertTrue(ok_good, msg_good)
        self.assertEqual(oid_good, "SUP-C52-OK")
        post_ok.assert_called_once()

        # C57 Negative: retry reservation for ANOTHER child correlation MUST NOT authorize RTRY-MY-CHILD
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM order_intents")
            conn.execute("DELETE FROM retry_reservations")
        self.assertTrue(gate.run_startup_reconciliation()[0])
        with self.db._get_conn() as conn:
            conn.execute("INSERT INTO retry_reservations (root_order_id, parent_order_id, child_correlation_id, reserved_at) VALUES ('ROOT-1', 'PARENT-1', 'RTRY-OTHER-CHILD', ?)", (time.time(),))
        self.db.record_order_intent("RTRY-MY-CHILD", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("RTRY-MY-CHILD", state="PENDING_SUBMIT")
        req_r = self._req("RTRY-MY-CHILD")
        with patch.object(client.session, "post", post_spy):
            ok_rbad, _, msg_rbad = client.place_order(req_r)
        self.assertFalse(ok_rbad)
        post_spy.assert_not_called()

        # C57 Positive: exact matching child_correlation_id in retry_reservations succeeds
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM order_intents")
            conn.execute("DELETE FROM retry_reservations")
        self.assertTrue(gate.run_startup_reconciliation()[0])
        with self.db._get_conn() as conn:
            conn.execute("INSERT INTO retry_reservations (root_order_id, parent_order_id, child_correlation_id, reserved_at) VALUES ('ROOT-1', 'PARENT-1', 'RTRY-MY-CHILD', ?)", (time.time(),))
        self.db.record_order_intent("RTRY-MY-CHILD", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent("RTRY-MY-CHILD", state="PENDING_SUBMIT")
        post_ok2 = MagicMock(return_value=MagicMock(status_code=200, text='{"orderId":"ORD-RTRY-OK"}', json=lambda: {"orderId": "ORD-RTRY-OK"}))
        with patch.object(client.session, "post", post_ok2):
            ok_rgood, oid_rgood, msg_rgood = client.place_order(req_r)
        self.assertTrue(ok_rgood, msg_rgood)
        self.assertEqual(oid_rgood, "ORD-RTRY-OK")
        post_ok2.assert_called_once()

    # ------------------------------------------------------------------
    # C54: Unconditional sibling-leg safety even when parentOrderId + algoId are explicit
    # ------------------------------------------------------------------
    def test_c54_explicit_linkage_with_live_forever_sibling_must_fail_closed(self):
        from datetime import date
        from src.broker.broker_snapshot import BookEvidence, BookStatus, BrokerSnapshot, Generation, SessionContext, SessionPhase, account_key, freeze_json, payload_digest
        from src.engine.durable_risk_resolution import resolve_durable_orphan_legs

        def mk_snap(sid, mns, ob_rows, fo_rows):
            ak = account_key("1113731536")
            gen = Generation(ak, "boot-1", 1, 1, 1, 1, 1)
            sess = SessionContext(SessionPhase.CURRENT_SIGNED, date(2026, 11, 26), 1795700000000000000, "a"*64, True)
            def bk(k, ep, payload):
                pl = list(payload) if isinstance(payload, (list, tuple)) else payload
                return BookEvidence(k, ep, freeze_json({}), BookStatus.OK_RECORDS if pl else BookStatus.OK_EMPTY,
                                    200, None, None, True, 1795700000000000000, 1795700000000000100, mns, mns+100,
                                    "1113731536", 1, freeze_json(pl), payload_digest(pl), True)
            return BrokerSnapshot(sid, gen, "1113731536", "1113731536", True, sess,
                                  1795700000000000000, 1795700000000000100, mns, mns+100,
                                  holdings=bk("holdings", "/v2/holdings", []),
                                  positions=bk("positions", "/v2/positions", []),
                                  order_book=bk("order_book", "/v2/orders", ob_rows),
                                  trade_book=bk("trade_book", "/v2/trades", []),
                                  forever_orders=bk("forever_orders", "/v2/forever/orders", fo_rows),
                                  super_orders=bk("super_orders", "/v2/super/orders", []),
                                  historical_trades=(), order_status=freeze_json({}), trades_for_order=freeze_json({}),
                                  profile=bk("profile", "/v2/profile", {"dhanClientId": "1113731536"}),
                                  funds=None, edis=None, history_coverage=(), complete=True, invalid_reasons=(),
                                  producer_pid=os.getpid(), producer_starttime=1, journal_digest="b"*64,
                                  profile_valid_until_mono_ns=mns+60_000_000_000)

        rec = {"order_id": "PARENT-1", "security_id": "3045", "segment": "NSE_EQ", "product_type": "CNC",
               "orphaned_leg_cancel_details": {"LEG-99": {"group_id": "GROUP-1"}}}
        explicit_leg = {"dhanClientId": "1113731536", "orderId": "LEG-99", "parentOrderId": "PARENT-1", "algoId": "GROUP-1",
                        "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL",
                        "orderStatus": "CANCELLED", "quantity": 5, "filledQty": 0, "remainingQuantity": 0}
        live_fo = [{"dhanClientId": "1113731536", "orderId": "GROUP-1", "parentOrderId": "PARENT-1",
                    "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "orderStatus": "CONFIRM"}]
        out_live = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [explicit_leg], live_fo), mk_snap("S2", 13_000_000_000, [explicit_leg], live_fo), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertFalse(out_live.resolved)
        self.assertEqual(out_live.reason_code, "ORPHAN_RESIDUAL_EXECUTABLE_SIBLING:CONFIRM")

        # Dead sibling in forever_orders + explicit linkage -> PASS
        dead_fo = [dict(live_fo[0], orderStatus="CANCELLED")]
        out_dead = resolve_durable_orphan_legs(rec, mk_snap("S1", 10_000_000_000, [explicit_leg], dead_fo), mk_snap("S2", 13_000_000_000, [explicit_leg], dead_fo), expected_account_id="1113731536", now_mono_ns=13_500_000_000)
        self.assertTrue(out_dead.resolved, out_dead)

    # ------------------------------------------------------------------
    # C53 + C56: Parent-scoped fill readback rejects another parent's execution ID
    # ------------------------------------------------------------------
    def test_c53_c56_persistence_readback_rejects_cross_parent_fill_id(self):
        client, _, watcher = self._make_stack()
        now = time.time()
        with self.db._get_conn() as conn:
            # Parent P1 (security 11915, fill OLD-P1, qty 3)
            conn.execute("INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                         ("P1", 0, json.dumps({"order_id": "P1", "position_state": "PROTECTED", "filled_qty": 3}), now))
            conn.execute("INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES ('P1','1113731536','YESBANK','11915','NSE_EQ','CNC','FOREVER_OCO','PROTECTED',3,3,0,3,?)", (now,))
            conn.execute("INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES ('P1','OLD-P1','YESBANK',20.0,3,?,'BUY','11915','NSE_EQ','CNC','P1',1)", (now,))
            # Parent P2 (security 3045, fill REQUIRED-BUT-WRONG-PARENT, qty 3)
            conn.execute("INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES ('P2','REQUIRED-BUT-WRONG-PARENT','SBIN',800.0,3,?,'BUY','3045','NSE_EQ','CNC','P2',1)", (now,))

        rec_p1 = {"order_id": "P1", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC",
                  "persistence_uncertain": True, "owner_readback_failed": True}
        # Negative: clearing P1 using P2's execution ID MUST FAIL CLOSED!
        ok_bad, r_bad = watcher.clear_persistence_risk_with_readback(
            rec_p1, expected_order_id="P1", expected_position_state="PROTECTED", expected_filled_qty=3,
            expected_fill_ids=["REQUIRED-BUT-WRONG-PARENT"],
        )
        self.assertFalse(ok_bad)
        self.assertIn("TARGET_FILL_IDS_MISSING_FOR_PARENT", r_bad)
        self.assertTrue(rec_p1.get("persistence_uncertain"))

        # Positive: clearing P1 using P1's actual committed fill OLD-P1 SUCCEEDS!
        ok_good, r_good = watcher.clear_persistence_risk_with_readback(
            rec_p1, expected_order_id="P1", expected_position_state="PROTECTED", expected_filled_qty=3,
            expected_fill_ids=["OLD-P1"],
        )
        self.assertTrue(ok_good, r_good)
        self.assertFalse(rec_p1.get("persistence_uncertain", False))

    # ------------------------------------------------------------------
    # C55: Stale first_snap (>10s) is replaced instead of trapping obligation
    # ------------------------------------------------------------------
    def test_c55_stale_first_snapshot_replaced_and_independent_capture_succeeds(self):
        dead_row = {"dhanClientId": "1113731536", "orderId": "ORD-C55", "securityId": "11915",
                    "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY",
                    "orderStatus": "CANCELLED", "quantity": 1, "filledQty": 0, "remainingQuantity": 0}
        client, _, watcher = self._make_stack(orders=[dead_row])
        rec = {"order_id": "ORD-C55", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC",
               "cancel_uncertain": True}
        # Seed a stale (>15s old) first snapshot
        stale_snap = type("Stale", (), {"snapshot_id": "OLD-STALE", "capture_finished_mono_ns": time.monotonic_ns() - 15_000_000_000, "generation": None, "journal_digest": ""})()
        _k55 = ("ORD-C55", watcher._resolution_operation_scope(rec))
        watcher._resolution_first_snapshots = {
            _k55: {
                **watcher._resolution_binding_metadata(rec, expected_account_id="1113731536", operation_scope=_k55[1]),
                "snapshot": stale_snap,
                "generation": None,
                "capture_id": "OLD-STALE",
                "capture_mono_ns": stale_snap.capture_finished_mono_ns,
                "journal_digest": "",
            }
        }
        # Calling acquire_paired_resolution_snapshots discards OLD-STALE and acquires fresh snap1
        r1 = watcher.acquire_paired_resolution_snapshots(rec, expected_account_id="1113731536", min_separation_ns=50_000_000)
        self.assertIsNone(r1)
        self.assertNotEqual(watcher._resolution_first_snapshots[_k55]["snapshot"].snapshot_id, "OLD-STALE")


    def test_c52_exact_numeric_truncation_rejected(self):
        """C52: Strict integer quantity validation rejects float truncation (1.9 != 1) and bool."""
        _, gate, _ = self._make_stack()
        cid = "C52-FLOAT-QTY"
        self.db.record_order_intent(cid, "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent(cid, state="PENDING_SUBMIT")
        ok_v, reason_v = gate.revalidate_for_transport_submission(
            pending_correlation_id=cid,
            expected_order_payload={
                "symbol": "YESBANK", "security_id": "11915", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "transaction_type": "BUY",
                "quantity": 1.9, "price": 20.0,
            },
        )
        self.assertFalse(ok_v)
        self.assertIn("UNIFIED_INTENT_PAYLOAD_MISMATCH", reason_v)

    def test_c57_preexisting_other_child_retry_reservation_cannot_authorize_retry(self):
        """C57: Pre-existing retry_reservation for child A cannot authorize retry child B even with zero digest drift."""
        from dataclasses import replace as _dc_replace
        from src.engine.journal_view import read_journal_view
        _, gate, _ = self._make_stack()
        with self.db._get_conn() as con:
            con.execute(
                "INSERT INTO retry_reservations (root_order_id, parent_order_id, child_correlation_id, reserved_at) VALUES (?,?,?,?)",
                ("ROOT-1", "PARENT-1", "RTRY-OTHER-CHILD", time.time()),
            )
        _jv_now = read_journal_view(
            str(self.db.db_path),
            account_id="1100000001",
            journal_epoch=getattr(getattr(gate._unified_verdict, "generation", None), "journal_epoch", 1),
            read_utc_ns=int(time.time() * 1_000_000_000),
        )
        gate._unified_verdict = _dc_replace(gate._unified_verdict, journal_digest=_jv_now.digest_sha256)
        gate._unified_verdict_journal_view = _jv_now
        cid = "RTRY-MY-CHILD"
        self.db.record_order_intent(cid, "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 20.0, 1)
        self.db.update_order_intent(cid, state="PENDING_SUBMIT")
        ok_v, reason_v = gate.revalidate_for_transport_submission(
            pending_correlation_id=cid,
            expected_order_payload={
                "symbol": "YESBANK", "security_id": "11915", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "transaction_type": "BUY",
                "quantity": 1, "price": 20.0,
            },
        )
        self.assertFalse(ok_v)
        self.assertIn("UNIFIED_RETRY_RESERVATION_CHILD_MISMATCH", reason_v)

    def test_c56_system_events_issue_id_rejects_substring_collision(self):
        """C56/C53: Original-operation readback rejects substring issue_id match ('ISS-1' vs 'ISS-10')."""
        from src.engine.durable_issue_writer import verify_operation_readback_and_clear_persistence_risk
        with tempfile.TemporaryDirectory() as td:
            db = JournalDB(f"{td}/trade_journal.db")
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                    ("ORD-SUB", 0, json.dumps({"order_id": "ORD-SUB", "position_state": "PROTECTED_OPEN", "filled_qty": 1}), time.time()),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ORD-SUB", "1100000001", "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 1, 1, 0, 1, time.time()),
                )
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("ORD-SUB", "ET-SUB-1", "SBIN-EQ", 800.0, 1, time.time(), "BUY", "3045", "NSE_EQ", "CNC", "ORD-SUB"),
                )
                con.execute(
                    "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?,?,?,?)",
                    ("PROTECTION_RISK", "ISS-10", json.dumps({"issue_id": "ISS-10", "order_id": "ORD-SUB"}), time.time()),
                )
            rec = {"order_id": "ORD-SUB", "persistence_uncertain": True}
            ok, reason = verify_operation_readback_and_clear_persistence_risk(
                db_path=str(db.db_path),
                record=rec,
                expected_order_id="ORD-SUB",
                expected_position_state="PROTECTED_OPEN",
                expected_filled_qty=1,
                expected_event_type="PROTECTION_RISK",
                expected_issue_id="ISS-1",
            )
            self.assertFalse(ok)
            self.assertIn("TARGET_ISSUE_LINKAGE_NOT_COMMITTED", reason)

    def test_c55_continuing_watcher_loop_resolves_cancel_uncertain_without_qty_transition(self):
        """C55: supervise_open_positions invokes continuing recovery when cancel_uncertain=True and broker_quantity_unverified=False."""
        from src.config import AppConfig, TradingMode
        from src.engine.protection_watcher import ProtectionWatcher
        with tempfile.TemporaryDirectory() as td:
            db = JournalDB(f"{td}/trade_journal.db")
            broker = MagicMock()
            broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1100000001", data_dir=td)
            broker.get_holdings.return_value = [{"securityId": "3045", "tradingSymbol": "SBIN-EQ", "totalQty": 1, "availableQty": 1}]
            broker.get_positions.return_value = []
            broker.get_forever_orders.return_value = [{"orderId": "FO-1", "securityId": "3045", "orderStatus": "CONFIRM", "quantity": 1, "transactionType": "SELL", "orderType": "OCO"}]
            broker.get_order_book.return_value = []
            broker.get_trade_book.return_value = []
            pw = ProtectionWatcher(broker, journal_db=db, path=f"{td}/pending.json")
            pw.terminal_orders["ORD-C55-LOOP"] = {
                "order_id": "ORD-C55-LOOP",
                "symbol": "SBIN-EQ",
                "security_id": "3045",
                "segment": "NSE_EQ",
                "product_type": "CNC",
                "quantity": 1,
                "filled_qty": 1,
                "confirmed_open_qty": 1,
                "position_state": "PROTECTED_OPEN",
                "protection_route": "FOREVER_OCO",
                "forever_order_id": "FO-1",
                "cancel_uncertain": True,
                "broker_quantity_unverified": False,
                "broker_quantity_evidence": "VERIFIED_HOLDING_MATCH",
                "created_at": time.time() - 60.0,
            }
            called = []
            def _fake_acquire(rec, **kw):
                called.append(rec.get("order_id"))
                return None
            pw.acquire_paired_resolution_snapshots = _fake_acquire
            pw.supervise_open_positions(current_time=time.time())
            self.assertIn("ORD-C55-LOOP", called)

if __name__ == "__main__":
    unittest.main()
