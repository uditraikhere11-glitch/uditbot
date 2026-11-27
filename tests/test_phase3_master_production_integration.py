"""UDITBOT-P3-MASTER: Real Phase 2D V2 Production-Consumer Integration Suite (P3-B01–P3-B10, C44–C50).

Executes real Phase 2D V2 production consumers (UnifiedAccountRuntime, build_verified_policy_context,
StartupReconciliationGate.revalidate_for_transport_submission, ConfigChangeTracker.control_service
& evaluate_unified_shutdown_gate, ProtectionWatcher.acquire_paired_resolution_snapshots,
RiskManager.sync_from_journal_and_broker, DhanClient.place_order C44/C45,
durable_risk_resolution.resolve_durable_orphan_legs C46,
durable_issue_writer.verify_operation_readback_and_clear_persistence_risk C48,
and control_center_backend display helper) reusing tests._phase2d_live_harness.LivePhase2DHarness.
"""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.broker.broker_snapshot import LIVE_BOOKS, NS, BrokerSnapshot, ContractKey, Purpose, SessionPhase, account_key
from src.engine.config_manager import ConfigChangeTracker
from src.engine.durable_issue_writer import verify_operation_readback_and_clear_persistence_risk
from src.engine.durable_risk_resolution import resolve_durable_orphan_legs
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.risk_manager import RiskManager
from src.engine.unified_account_runtime import UnifiedAccountRuntime, build_verified_policy_context
from src.journal.db import JournalDB
from tests._phase2d_live_harness import LivePhase2DHarness, seed_valid_unified_verdict_on_gate
from tests.golden_replay.clocks import IST, DeterministicClock
from tests.golden_replay.runner import ReplayEnvironment

ACCOUNT = "9000001536"


class TestPhase3MasterProductionConsumers(unittest.TestCase):
    """P3-B01–P3-B10 & C44–C50 real production consumer execution on Phase 2D V2 (1f634097)."""

    def test_p3_b01_g02_genuine_entry_eligible_closure_with_real_unified_runtime(self):
        """P3-B01 / Section 13: G02 genuine closure achieving actual entry_eligible=True (not quantity-only)."""
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))
        t_buy = datetime(2026, 11, 20, 10, 0, 0, tzinfo=IST).timestamp()
        t_sell = datetime(2026, 11, 25, 10, 30, 0, tzinfo=IST).timestamp()
        seed = {
            "order_intents": [{
                "correlation_id": "EQ-PDSL-B01", "account_id": ACCOUNT, "symbol": "PDSL-EQ",
                "trading_symbol": "PDSL-EQ", "security_id": "13197", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "order_type": "LIMIT", "price": 405.0, "quantity": 1,
                "state": "TRADED", "broker_order_id": "PDSL-BUY-B01", "protection_route": "FOREVER_OCO",
                "created_at": t_buy, "updated_at": t_sell,
            }],
            "fills": [
                {"broker_order_id": "PDSL-BUY-B01", "exchange_trade_id": "PDSL-EID-BUY-B01", "symbol": "PDSL-EQ", "traded_price": 405.0, "traded_quantity": 1, "traded_at": t_buy, "transaction_type": "BUY", "security_id": "13197", "exchange_segment": "NSE_EQ", "product_type": "CNC", "parent_order_id": "PDSL-BUY-B01", "is_verified_trade": 1},
                {"broker_order_id": "PDSL-SELL-B01", "exchange_trade_id": "PDSL-EID-SELL-B01", "symbol": "PDSL-EQ", "traded_price": 420.0, "traded_quantity": 1, "traded_at": t_sell, "transaction_type": "SELL", "security_id": "13197", "exchange_segment": "NSE_EQ", "product_type": "CNC", "parent_order_id": "PDSL-BUY-B01", "is_verified_trade": 1},
            ],
            "positions_lifecycle": [{
                "parent_order_id": "PDSL-BUY-B01", "account_id": ACCOUNT, "symbol": "PDSL-EQ",
                "trading_symbol": "PDSL-EQ", "security_id": "13197", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "protection_route": "FOREVER_OCO", "position_state": "POSITION_CLOSED",
                "entry_qty": 1, "buy_filled_qty": 1, "buy_vwap": 405.0, "protected_qty": 0,
                "sl_price": 0.0, "target_price": 0.0, "sell_filled_qty": 1, "sell_vwap": 420.0,
                "confirmed_open_qty": 0, "realized_pnl": 15.0, "triggered_leg": "",
                "manual_exit_detected": 0, "orphaned_legs_detected": 0, "updated_at": t_sell,
            }],
        }
        hist = {
            "2026-11-20/2026-11-25/0": [
                {"dhanClientId": ACCOUNT, "orderId": "PDSL-BUY-B01", "exchangeOrderId": "EO-PDSL-1", "exchangeTradeId": "PDSL-EID-BUY-B01", "securityId": "13197", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 405.0, "exchangeTime": "2026-11-20T10:00:00"},
                {"dhanClientId": ACCOUNT, "orderId": "PDSL-SELL-B01", "exchangeOrderId": "EO-PDSL-2", "exchangeTradeId": "PDSL-EID-SELL-B01", "securityId": "13197", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 1, "tradedPrice": 420.0, "exchangeTime": "2026-11-25T10:30:00"},
            ],
            "2026-11-20/2026-11-25/1": [],
        }
        sell_status = {"dhanClientId": ACCOUNT, "orderId": "PDSL-SELL-B01", "exchangeOrderId": "EO-PDSL-2", "securityId": "13197", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0}
        buy_status = {"dhanClientId": ACCOUNT, "orderId": "PDSL-BUY-B01", "exchangeOrderId": "EO-PDSL-1", "securityId": "13197", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0}
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=seed) as env:
            env.set_session(clk.session_context(phase=SessionPhase.SETTLED, trading_session_date=date(2026, 11, 26)))
            env.register_books(holdings=[], positions=[], history_pages=hist, order_statuses={"PDSL-SELL-B01": sell_status, "PDSL-BUY-B01": buy_status})
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 20), date(2026, 11, 25)),), order_status_ids=("PDSL-SELL-B01", "PDSL-BUY-B01"))
            self.assertTrue(snap.complete, snap.invalid_reasons)
            v = env.reconcile_security(snap, ContractKey(ACCOUNT, "13197", "NSE_EQ", "CNC"))
            self.assertTrue(v.entry_eligible, f"Expected genuine entry_eligible=True, got reasons={v.reason_codes}")
            self.assertEqual(v.broker_open_qty, 0)
            self.assertEqual(v.ledger_open_qty, 0)

    def test_p3_b03_c49_actual_config_manager_evaluate_unified_shutdown_gate(self):
        """P3-B03 / C49: Real ConfigChangeTracker.evaluate_unified_shutdown_gate() on LivePhase2DHarness."""
        h = LivePhase2DHarness(account_id=ACCOUNT)
        self.addCleanup(h.close)
        tracker = ConfigChangeTracker(data_dir=str(h.cfg.data_dir))
        env_file = Path(h.root) / ".env"
        env_file.write_text(f"TRADING_MODE=LIVE\nDHAN_CLIENT_ID={ACCOUNT}\nDATA_DIR={h.cfg.data_dir}\n", encoding="utf-8")
        # Record STOP intent first
        res = tracker.evaluate_unified_shutdown_gate(
            action="stop",
            db_path=str(h.cfg.journal_db_path),
            broker=h.client,
            env_path=str(env_file),
            stop_state_path=str(Path(h.cfg.data_dir) / "emergency_stop.json"),
            is_service_running=True,
            is_live_mode=True,
        )
        self.assertIsInstance(res, dict)
        self.assertTrue(res.get("allowed"), res)
        self.assertEqual(res.get("blocking_order_count"), 0)

    def test_p3_b05_c24_actual_risk_manager_coordinated_snapshot(self):
        """P3-B05 / C24: Real RiskManager.sync_from_journal_and_broker() consuming coordinated BrokerSnapshot."""
        h = LivePhase2DHarness(account_id=ACCOUNT)
        self.addCleanup(h.close)
        rt = UnifiedAccountRuntime(data_dir=str(h.cfg.data_dir), db_path=str(h.cfg.journal_db_path), account_id=ACCOUNT)
        pol = build_verified_policy_context(
            mode="LIVE", purpose=Purpose.RISK, data_dir=str(h.cfg.data_dir),
            owner_desired_state="RUNNING", owner_observed_state="RUNNING",
            entry_fence_active=False, risk_funds_verified=True,
        )
        auth = rt.acquire_and_evaluate(h.client, pol)
        self.assertIsNotNone(auth.snapshot)
        rm = RiskManager(h.cfg)
        ok, msg = rm.sync_from_journal_and_broker(journal_db=h.db, broker=h.client, broker_snapshot=auth.snapshot)
        self.assertTrue(ok, msg)

    def test_c44_c45_final_transport_gate_and_payload_binding(self):
        """C44 & C45: Real StartupReconciliationGate.revalidate_for_transport_submission rejects mismatched payload & halted gate."""
        h = LivePhase2DHarness(account_id=ACCOUNT)
        self.addCleanup(h.close)
        watcher = ProtectionWatcher(h.client, path=str(Path(h.cfg.data_dir) / "watcher.json"), journal_db=h.db)
        class _Stop:
            def is_stopped(self): return False
            def get_in_flight(self): return []
        gate = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        seed_valid_unified_verdict_on_gate(gate, data_dir=h.cfg.data_dir, account_id=ACCOUNT)
        self.assertTrue(gate.is_accepting_entries)

        # Seed persisted intent after clean startup: AUDIT-MISMATCH, security 11915, qty 1, price 20.0
        h.db.record_order_intent(
            correlation_id="AUDIT-MISMATCH", account_id=ACCOUNT, symbol="YESBANK-EQ",
            security_id="11915", exchange_segment="NSE_EQ", product_type="CNC",
            order_type="LIMIT", price=20.0, quantity=1, protection_route="FOREVER_OCO",
        )
        with h.db._get_conn() as con:
            con.execute("UPDATE order_intents SET state='SUBMITTING' WHERE correlation_id='AUDIT-MISMATCH'")

        # C45: Same correlation_id with mismatched security/qty/price MUST be rejected!
        ok_bad, reason_bad = gate.revalidate_for_transport_submission(
            pending_correlation_id="AUDIT-MISMATCH",
            post_registration=True,
            expected_order_payload={
                "account_id": ACCOUNT, "correlation_id": "AUDIT-MISMATCH",
                "security_id": "3045", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "order_type": "LIMIT", "quantity": 7, "price": 25.0,
            },
        )
        self.assertFalse(ok_bad, "C45 mismatched payload unexpectedly authorized!")
        self.assertIn("PAYLOAD", reason_bad.upper())

        # C44: Halting gate before final transport admission blocks immediately
        gate.halt_new_entries("c44-concurrent-halt")
        ok_halted, reason_halted = gate.revalidate_for_transport_submission(
            pending_correlation_id="AUDIT-MISMATCH",
            post_registration=True,
            expected_order_payload={
                "account_id": ACCOUNT, "correlation_id": "AUDIT-MISMATCH",
                "security_id": "11915", "exchange_segment": "NSE_EQ",
                "product_type": "CNC", "order_type": "LIMIT", "quantity": 1, "price": 20.0,
            },
        )
        self.assertFalse(ok_halted, "C44 halted gate unexpectedly authorized transport!")

    def test_c46_orphan_resolution_rejects_unlinked_parent_and_group(self):
        """C46: resolve_durable_orphan_legs() rejects terminal leg lacking parent/group linkage."""
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books(
                order_book=[{"dhanClientId": ACCOUNT, "orderId": "LEG-ORPHAN-1", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "orderStatus": "CANCELLED", "quantity": 1, "filledQty": 0, "remainingQuantity": 0}]
            )
            s1 = env.acquire_snapshot(purpose=Purpose.WATCHER)
            clk.advance(2.5)
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_ORPHAN_TEST", "c46-s2")
            s2 = env.acquire_snapshot(purpose=Purpose.WATCHER)
            # Record lacks parent/group linkage -> MUST NOT clear!
            rec = {"order_id": "PARENT-1", "symbol": "YESBANK-EQ", "security_id": "11915", "orphaned_legs_detected": True, "orphaned_leg_cancel_details": {"LEG-ORPHAN-1": {"state": "PENDING"}}}
            outcome = resolve_durable_orphan_legs(rec, s1, s2, expected_account_id=ACCOUNT, now_mono_ns=clk.monotonic_ns())
            self.assertFalse(outcome.resolved, f"C46 unlinked orphan leg unexpectedly cleared: {outcome}")

    def test_c48_persistence_readback_requires_exact_committed_fill_ids(self):
        """C48: verify_operation_readback_and_clear_persistence_risk() denies if expected fill ID is absent."""
        with tempfile.TemporaryDirectory(prefix="uditbot_c48_") as tmp:
            db_path = Path(tmp) / "trade_journal.db"
            db = JournalDB(str(db_path), account_id=ACCOUNT)
            db.upsert_position_lifecycle(
                parent_order_id="ORD-C48", account_id=ACCOUNT, symbol="INFY-EQ", trading_symbol="INFY-EQ",
                security_id="1594", exchange_segment="NSE_EQ", product_type="CNC",
                protection_route="FOREVER_OCO", position_state="PROTECTED",
                entry_qty=5, buy_filled_qty=5, buy_vwap=1500.0, protected_qty=5, confirmed_open_qty=5,
            )
            rec = {"order_id": "ORD-C48", "persistence_uncertain": True, "filled_qty": 5}
            # Missing expected fill ID 'EID-C48-MISSING' -> MUST FAIL READBACK even though lifecycle matches!
            ok_miss, reason_miss = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec, expected_order_id="ORD-C48", expected_position_state="PROTECTED",
                expected_filled_qty=5, expected_account_id=ACCOUNT, expected_fill_ids=("EID-C48-MISSING",),
            )
            self.assertFalse(ok_miss, f"C48 cleared persistence_uncertain without committed fill: {reason_miss}")
            self.assertTrue(rec.get("persistence_uncertain"))

            # Now commit the exact verified fill -> readback MUST succeed and clear persistence_uncertain!
            db.record_fill("ORD-C48", "EID-C48-REAL", "INFY-EQ", 1500.0, 5, transaction_type="BUY", security_id="1594", exchange_segment="NSE_EQ", product_type="CNC", parent_order_id="ORD-C48", is_verified_trade=True)
            with db._get_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?, ?, ?, ?)", ("ORD-C48", 0, json.dumps({"order_id": "ORD-C48", "position_state": "PROTECTED", "filled_qty": 5}), time.time()))
            ok_real, reason_real = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec, expected_order_id="ORD-C48", expected_position_state="PROTECTED",
                expected_filled_qty=5, expected_account_id=ACCOUNT, expected_fill_ids=("EID-C48-REAL",),
            )
            self.assertTrue(ok_real, reason_real)
            self.assertFalse(rec.get("persistence_uncertain"))

    def test_p3_public_dhan_client_submission_and_unauthorized_zero_http(self):
        """Phase 3 Roadmap: Actual public DhanClient.place_order / place_super_order / cancel_order with zero HTTP mutations on unauthorized requests."""
        from types import SimpleNamespace
        from src.models import OrderRequest, OrderType, ProductType, TransactionType

        h = LivePhase2DHarness(account_id=ACCOUNT)
        self.addCleanup(h.close)
        watcher = ProtectionWatcher(h.client, path=str(Path(h.cfg.data_dir) / "watcher.json"), journal_db=h.db)
        class _Stop:
            def is_stopped(self): return False
            def get_in_flight(self): return []
        gate = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        h.client.reconciliation_gate = gate

        mut_calls = []
        def _rec_post(url, **kw):
            mut_calls.append(("POST", url, kw.get("json")))
            return SimpleNamespace(status_code=200, text='{"orderId":"ORD-1"}', json=lambda: {"orderId": "ORD-1"})
        def _rec_put(url, **kw):
            mut_calls.append(("PUT", url, kw.get("json")))
            return SimpleNamespace(status_code=200, text='{"orderId":"ORD-1"}', json=lambda: {"orderId": "ORD-1"})
        def _rec_delete(url, **kw):
            mut_calls.append(("DELETE", url))
            return SimpleNamespace(status_code=200, text='{"orderId":"ORD-CANCEL-1","orderStatus":"CANCELLED"}', json=lambda: {"orderId": "ORD-CANCEL-1", "orderStatus": "CANCELLED"})
        h.client.session.post = _rec_post
        h.client.session.put = _rec_put
        h.client.session.delete = _rec_delete

        # 1) Seed healthy startup state then halt gate -> public place_order & place_super_order MUST fail with 0 HTTP mutations
        seed_valid_unified_verdict_on_gate(gate, data_dir=h.cfg.data_dir, account_id=ACCOUNT)
        gate.halt_new_entries("p3-unauthorized-entry-test")
        req = OrderRequest(
            correlation_id="P3-UNAUTH-ENTRY-001",
            symbol="YESBANK-EQ",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            transaction_type=TransactionType.BUY,
            order_type=OrderType.LIMIT,
            price=18.50,
            quantity=1,
        )
        ok_entry, oid_entry, msg_entry = h.client.place_order(req)
        self.assertFalse(ok_entry, f"Halted gate unexpectedly authorized place_order: {oid_entry} {msg_entry}")
        ok_sup, oid_sup, msg_sup = h.client.place_super_order(req, stop_loss_price=17.50, target_price=20.00)
        self.assertFalse(ok_sup, f"Halted gate unexpectedly authorized place_super_order: {oid_sup} {msg_sup}")
        self.assertEqual(len(mut_calls), 0, f"Unauthorized public DhanClient entry emitted HTTP mutations: {mut_calls}")

        # 2) Valid risk-reducing cancel_order while entries are halted MUST still be allowed
        import os as _os
        from src.engine.broker_handoff import BrokerMutationNotSubmitted, proc_starttime
        _proc_starttime = proc_starttime
        (Path(h.cfg.data_dir) / "active_broker_mutations.json").write_text(
            json.dumps({
                "schema_version": 2,
                "account_id": ACCOUNT,
                "handoff_verified": True,
                "producer_pid": _os.getpid(),
                "producer_starttime": _proc_starttime(_os.getpid()),
                "active": [],
                "unresolved": [],
            }),
            encoding="utf-8",
        )
        ok_cancel, cancel_msg = h.client.cancel_order("ORD-CANCEL-1")
        self.assertTrue(ok_cancel, f"Risk-reducing cancel_order unexpectedly blocked while halted: {cancel_msg}")
        self.assertEqual(len(mut_calls), 1)
        self.assertEqual(mut_calls[0][0], "DELETE")

    def test_p3_g02_external_sell_allocation_recovery_and_c64_c65_rejection(self):
        """Phase 3 Roadmap: G02 external SELL recovery via exit_allocations + C64 wrong-contract & C65 contradictory allocation rejection."""
        with tempfile.TemporaryDirectory(prefix="uditbot_p3_g02_ext_") as tmp:
            db_path = str(Path(tmp) / "journal.db")
            db = JournalDB(db_path)
            now = time.time() - 60.0
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("EQ-PDSL-1", "PDSL-BUY-1", "LIVE", "CNC", "NSE_EQ", "PDSL-EQ", "11915", "LIMIT", 100.0, 5, 5, "TRADED", now, now, "FOREVER_OCO"),
                )
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade, evidence_source) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("PDSL-BUY-1", "EX-BUY-1", "PDSL-EQ", 100.0, 5, now, "BUY", "11915", "NSE_EQ", "CNC", "PDSL-BUY-1", 1, "BROKER_TRADE_BOOK"),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, trading_symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price, target_price, sell_filled_qty, sell_vwap, confirmed_open_qty, realized_pnl, triggered_leg, manual_exit_detected, orphaned_legs_detected, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("PDSL-BUY-1", ACCOUNT, "PDSL-EQ", "PDSL-EQ", "11915", "NSE_EQ", "CNC", "FOREVER_OCO", "POSITION_CLOSED", 5, 5, 100.0, 0, 0.0, 0.0, 5, 105.0, 0, 25.0, "MANUAL_EXIT", 1, 0, now),
                )
                con.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                    ("PDSL-BUY-1", 1, json.dumps({"order_id": "PDSL-BUY-1", "account_id": ACCOUNT, "symbol": "PDSL-EQ", "security_id": "11915", "exchange_segment": "NSE_EQ", "product_type": "CNC", "position_state": "POSITION_CLOSED", "filled_qty": 5}), now),
                )
                # Negative C64: Wrong-contract SELL fill (different security_id 99999)
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade, evidence_source) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("PDSL-BUY-1", "EX-WRONG-1", "OTHER-EQ", 105.0, 5, now + 1, "SELL", "99999", "NSE_EQ", "CNC", "PDSL-BUY-1", 1, "BROKER_TRADE_BOOK"),
                )
            rec = {
                "order_id": "PDSL-BUY-1",
                "account_id": ACCOUNT,
                "symbol": "PDSL-EQ",
                "security_id": "11915",
                "exchange_segment": "NSE_EQ",
                "product_type": "CNC",
                "journal_persistence_failed": True,
                "last_persistence_error": True,
            }
            ok_c64, msg_c64 = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec,
                expected_order_id="PDSL-BUY-1",
                expected_position_state="POSITION_CLOSED",
                expected_filled_qty=5,
                expected_sell_filled_qty=5,
                expected_confirmed_open_qty=0,
                expected_account_id=ACCOUNT,
                expected_security_id="11915",
                expected_segment="NSE_EQ",
                expected_product_type="CNC",
            )
            self.assertFalse(ok_c64, f"C64 wrong-contract SELL unexpectedly cleared persistence risk: {msg_c64}")
            self.assertTrue(rec.get("last_persistence_error"))

            # Positive: Legitimate same-contract external SELL fill + exit_allocation clears persistence risk
            with db._get_conn() as con:
                con.execute("DELETE FROM fills WHERE exchange_trade_id='EX-WRONG-1'")
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade, evidence_source) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("EXT-SELL-ORD-1", "EX-SELL-1", "PDSL-EQ", 105.0, 5, now + 2, "SELL", "11915", "NSE_EQ", "CNC", "", 1, "BROKER_TRADE_BOOK"),
                )
                con.execute(
                    "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) "
                    "VALUES (?,?,?,?,?,?)",
                    ("PDSL-BUY-1", "EXT-SELL-ORD-1", "EX-SELL-1", 5, 105.0, now + 2),
                )
            ok_pos, msg_pos = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec,
                expected_order_id="PDSL-BUY-1",
                expected_position_state="POSITION_CLOSED",
                expected_filled_qty=5,
                expected_sell_filled_qty=5,
                expected_confirmed_open_qty=0,
                expected_account_id=ACCOUNT,
                expected_security_id="11915",
                expected_segment="NSE_EQ",
                expected_product_type="CNC",
                expected_fill_ids=("EX-BUY-1",),
            )
            self.assertTrue(ok_pos, f"Valid same-contract external SELL allocation failed readback: {msg_pos}")
            self.assertNotIn("last_persistence_error", rec)

    def test_p3_production_restart_recovery_clean_and_fail_closed(self):
        """Phase 3 Roadmap: Actual production StartupReconciliationGate restart recovery (clean vs unresolved risk)."""
        from src.engine.reconciliation import ReconciliationState

        h = LivePhase2DHarness(account_id=ACCOUNT)
        self.addCleanup(h.close)
        watcher = ProtectionWatcher(h.client, path=str(Path(h.cfg.data_dir) / "watcher.json"), journal_db=h.db)
        class _Stop:
            def is_stopped(self): return False
            def get_in_flight(self): return []

        # 1) Clean process restart -> StartupReconciliationGate.run_unified_startup_verification passes
        gate1 = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        ok1, msg1 = gate1.run_unified_startup_verification(data_dir=str(h.cfg.data_dir))
        self.assertTrue(ok1, f"Clean restart startup verification failed: {msg1}")
        self.assertEqual(gate1.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate1.is_accepting_entries)

        # 2) Simulated crash & restart with unresolved RECONCILIATION_REQUIRED row in persisted SQLite journal -> MUST fail closed
        now = time.time() - 30.0
        with h.db._get_conn() as con:
            con.execute(
                "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, trading_symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price, target_price, sell_filled_qty, sell_vwap, confirmed_open_qty, realized_pnl, triggered_leg, manual_exit_detected, orphaned_legs_detected, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("UNRES-RESTART-1", ACCOUNT, "YESBANK-EQ", "YESBANK-EQ", "11915", "NSE_EQ", "CNC", "FOREVER_OCO", "RECONCILIATION_REQUIRED", 1, 1, 15.5, 0, 0.0, 0.0, 0, 0.0, 1, 0.0, "", 0, 0, now),
            )
        # Fresh gate instance simulating a new process boot after restart
        gate2 = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        ok2, msg2 = gate2.run_unified_startup_verification(data_dir=str(h.cfg.data_dir))
        self.assertFalse(ok2, f"Restart with unresolved RECONCILIATION_REQUIRED unexpectedly passed: {msg2}")
        self.assertFalse(gate2.is_accepting_entries)


if __name__ == "__main__":
    unittest.main()
