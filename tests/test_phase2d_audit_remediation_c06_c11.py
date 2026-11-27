"""Phase 2D Independent Audit Remediation Tests: C06-C11 & All 12 Mandatory Adversarial Checks."""
from __future__ import annotations

from contextlib import closing
import datetime
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.broker.broker_snapshot import (
    ContractKey,
    EvidenceRef,
    SessionPhase,
    account_key,
    payload_digest,
)
from src.broker.dhan_client import DhanClient
from src.broker.evidence_events import note_owner_event
from src.config import AppConfig, TradingMode
from src.engine.broker_ledger_reconciler import Purpose
from src.engine.config_manager import ConfigChangeTracker
from src.engine.durable_issue_writer import (
    commit_durable_issue_open,
    commit_durable_issue_resolution,
)
from src.engine.durable_ledger_issues import issue_identity, read_durable_issues
from src.engine.durable_risk_resolution import (
    _strict_zero_fill_terminal_dead,
    resolve_durable_orphan_legs,
    resolve_ordinary_zero_fill_cancellation,
)
from src.engine.journal_view import read_journal_view
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.risk_manager import RiskManager
from src.engine.unified_account_runtime import (
    UnifiedAccountRuntime,
    build_policy_context,
    build_verified_policy_context,
    peek_display_snapshot_summary,
)
from src.journal.db import JournalDB
from tests.test_phase2d_c01_c05_and_settlement import (
    ACCT,
    _book,
    _make_snapshot,
    _seed_yesbank_db,
)


class TestC06ToC11AuditRemediation(unittest.TestCase):
    def test_c06_live_broker_unavailable_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            env_path = str(Path(td) / "empty.env")
            Path(env_path).write_text("TRADING_MODE=LIVE\n", encoding="utf-8")
            db = JournalDB(db_path)
            # Real SQLite journal with a filled unacknowledged order + LIVE broker=None MUST fail closed!
            db.record_order(correlation_id="C-FILLED-1", broker_order_id="ORD-FILLED-1", trading_mode="LIVE", product_type="CNC", exchange_segment="NSE_EQ", symbol="SBIN", security_id="3045", order_type="LIMIT", price=800.0, quantity=5, status="TRADED")
            with db._get_conn() as _c:
                _c.execute("UPDATE orders SET filled_quantity=5 WHERE correlation_id='C-FILLED-1'")
            tracker = ConfigChangeTracker(data_dir=td)
            with patch.object(tracker, "get_readonly_broker", return_value=None):
                ok, err = tracker.reconcile_stale_journal_with_broker(
                    db_path=db_path,
                    broker=None,
                    env_path=env_path,
                    is_live_mode=True,
                    is_service_running=True,
                )
            self.assertFalse(ok)
            self.assertIn("BROKER_RECONCILIATION_FAILED", str(err))

            # With pending LIVE intent and broker=None -> MUST also fail closed!
            db.record_order_intent("C-C06", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
            db.update_order_intent("C-C06", state="PENDING", broker_order_id="ORD-C06")
            with patch.object(tracker, "get_readonly_broker", return_value=None):
                ok2, err2 = tracker.reconcile_stale_journal_with_broker(
                    db_path=db_path,
                    broker=None,
                    env_path=env_path,
                    is_live_mode=True,
                    is_service_running=True,
                )
            self.assertFalse(ok2)
            self.assertIn("LIVE_ORDER_IN_PROGRESS", str(err2))

    def test_c07_owner_intent_change_invalidates_stop_and_observation_failure_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_path = str(Path(td) / ".env")
            Path(env_path).write_text(f"TRADING_MODE=LIVE\nDHAN_CLIENT_ID={ACCT}\n", encoding="utf-8")
            db_path = str(Path(td) / "trade_journal.db")
            JournalDB(db_path)
            stop_path = str(Path(td) / "emergency_stop.json")
            tracker = ConfigChangeTracker(data_dir=td)

            # 1. Generation observation failure MUST block STOP (never swallow exception!)
            with patch.object(tracker, "_observe_owner_generation_fence", side_effect=OSError("sidecar unreadable")):
                res_fail = tracker.control_service(
                    action="stop",
                    env_path=env_path,
                    db_path=db_path,
                    stop_state_path=stop_path,
                    systemctl_fn=lambda a, s: True,
                    check_service_fn=lambda: "active",
                )
            self.assertFalse(res_fail.get("ok"))
            self.assertEqual(res_fail.get("blocked_reason"), "OWNER_GENERATION_FENCE_UNAVAILABLE")

            # 2. Owner intent / generation change between pre-gate and systemctl MUST reject stale authorization!
            # Seed a real snapshots.sqlite3 coordinator sidecar so note_owner_event() advances real owner_epoch!
            from src.broker.snapshot_coordinator import CrossProcessReadCoordinator, EnvironmentObservation
            _coord = CrossProcessReadCoordinator(
                td, ACCT,
                observe=lambda: EnvironmentObservation((1, 1, 1, 1), "1" * 64, "2" * 64, "0" * 64, _make_snapshot(snapshot_id="BOOT").session, True),
                collect=lambda *a, **kw: None,
            )
            _coord.observe_generation()
            calls = []
            def _safe_gate_with_midflight_owner_event(*args, **kwargs):
                # Simulate concurrent owner event occurring during authorization
                note_owner_event(td, "OWNER_INTENT_CHANGED")
                return True, "safe", "", 0

            with patch.object(tracker, "evaluate_trading_safety_gate_detailed", side_effect=_safe_gate_with_midflight_owner_event):
                res_drift = tracker.control_service(
                    action="stop",
                    env_path=env_path,
                    db_path=db_path,
                    stop_state_path=stop_path,
                    systemctl_fn=lambda a, s: (calls.append((a, s)) or True),
                    check_service_fn=lambda: "active",
                )
            self.assertFalse(res_drift.get("ok"))
            self.assertEqual(res_drift.get("blocked_reason"), "STALE_AUTHORIZATION_GENERATION_CHANGED")
            self.assertEqual(calls, [], "systemctl stop must NOT be called when generation drifted")

    def test_c08_missing_unified_verdict_blocks_and_10s_ttl_enforced_at_transport(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            broker = MagicMock()
            broker.cfg = cfg

            watcher = MagicMock(); watcher._is_running = True
            gate = StartupReconciliationGate(broker, db, watcher, stop_mgr)
            # Default TTL must be <= 10.0s (not 300s!)
            self.assertLessEqual(gate._unified_verdict_ttl_sec, 10.0)

            # Missing unified verdict when mandatory -> MUST block entry AND transport revalidation
            gate.enable_mandatory_unified_verdict()
            gate._accepting_entries = True
            self.assertFalse(gate.is_accepting_entries)
            ok_rv, rv_msg = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_rv)
            self.assertIn("UNIFIED_VERDICT_MISSING", rv_msg)

            # Passing ttl_sec=300.0 to publish_unified_verdict MUST be clamped to <= 10.0s
            jv = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            from tests._phase2d_live_harness import persist_synthetic_snapshot_to_sidecar
            snap = _make_snapshot(snapshot_id="C08-SNAP", holdings=[], positions=[], journal_digest=jv.digest_sha256)
            persist_synthetic_snapshot_to_sidecar(td, snap, ttl_sec=10.0)
            ok_u, msg_u = gate.run_unified_startup_verification(data_dir=td, broker_snapshot=snap, ttl_sec=300.0)
            self.assertTrue(ok_u, msg_u)
            self.assertLessEqual(gate._unified_verdict_ttl_sec, 10.0)
            self.assertTrue(gate.revalidate_for_transport_submission()[0])

            # Simulate >10s expiry -> transport revalidation MUST block!
            gate._unified_verdict_at = time.monotonic() - 11.0
            ok_exp, exp_msg = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_exp)
            self.assertIn("UNIFIED_VERDICT_STALE", exp_msg)

    def test_c09_fractional_boolean_negative_and_conflicting_observations_block(self) -> None:
        # Direct C09 fractional / bool / negative / malformed rejection
        for bad_row in (
            {"orderStatus": "CANCELLED", "filledQty": 0.9, "remainingQuantity": 0.9},
            {"orderStatus": "CANCELLED", "filledQty": 0.0, "remainingQuantity": 0.0},
            {"orderStatus": "CANCELLED", "filledQty": "0.9", "remainingQuantity": "0"},
            {"orderStatus": "CANCELLED", "filledQty": False, "remainingQuantity": 0},
            {"orderStatus": "CANCELLED", "filledQty": -1, "remainingQuantity": 0},
            {"orderStatus": "CANCELLED", "filledQty": None, "remainingQuantity": 0},
        ):
            ok, _ = _strict_zero_fill_terminal_dead(bad_row)
            self.assertFalse(ok, f"C09 violated for row: {bad_row}")

        # Exact integer 0/0 passes
        ok_good, st_good = _strict_zero_fill_terminal_dead(
            {"orderStatus": "CANCELLED", "filledQty": 0, "remainingQuantity": 0}
        )
        self.assertTrue(ok_good)
        self.assertEqual(st_good, "CANCELLED")

        # Conflicting order observations across books -> MUST BLOCK
        now_m = time.monotonic_ns()
        rec = {
            "order_id": "ORD-CONF",
            "security_id": "3045",
            "segment": "NSE_EQ",
            "product_type": "CNC",
        }
        row_canc = {
            "dhanClientId": ACCT,
            "orderId": "ORD-CONF",
            "securityId": "3045",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "remainingQuantity": 0,
        }
        row_rej = dict(row_canc, orderStatus="REJECTED")
        s1 = _make_snapshot(snapshot_id="C09-S1", order_book=[row_canc], forever_orders=[row_rej], mono_ns=now_m)
        s2 = _make_snapshot(snapshot_id="C09-S2", order_book=[row_canc], forever_orders=[row_canc], mono_ns=now_m + 2_000_000_000)
        out = resolve_ordinary_zero_fill_cancellation(rec, s1, s2, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertFalse(out.resolved)
        self.assertIn("CONFLICTING_ORDER_OBSERVATIONS", out.reason_code)

    def test_c10_nonexistent_opening_rejected_and_valid_event_passes_phase2c_replay(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            now = time.time()
            # Seed a real parent order + fill so Phase 2C read_durable_issues execution ref verification can pass
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO order_intents (correlation_id, account_id, symbol, security_id, exchange_segment, product_type, order_type, price, quantity, state, broker_order_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("C-P10", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5, "TRADED", "P10", now, now),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("P10", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 5, 5, 0, 5, now),
                )
                cur = con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("P10", "EID-P10-1", "SBIN-EQ", 800.0, 5, now, "BUY", "3045", "NSE_EQ", "CNC", "P10", 1),
                )
                fill_id = int(cur.lastrowid)

            ck = ContractKey(ACCT, "3045", "NSE_EQ", "CNC")
            reason = "LEGACY_UNCLASSIFIED_RISK:persistence_uncertain"
            ident = issue_identity(ck, reason, "PARENT", "P10")
            jv0 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())

            # 1. Nonexistent opening MUST be rejected and MUST NOT corrupt ledger
            fake_res = {
                "version": 1,
                "issue_id": ident,
                "contract": {"account_id": ACCT, "security_id": "3045", "exchange_segment": "NSE_EQ", "product_type": "CNC"},
                "reason_code": reason,
                "scope": "PARENT",
                "subject_id": "P10",
                "snapshot_id": "SNAP-RES-1",
                "journal_digest": jv0.digest_sha256,
                "opening_event_id": 99999,
                "opening_sha256": "a" * 64,
                "resolved_utc_ns": time.time_ns(),
                "proof_kind": "EXACT_PARENT_LEDGER_READBACK",
                "parent_readback_sha256": "b" * 64,
                "evidence_refs": [
                    {"snapshot_id": "SNAP-RES-1", "book_key": "order_book", "row_index": 0, "payload_sha256": "c" * 64, "journal_ref": f"fills:{fill_id}"}
                ],
            }
            res_fake = commit_durable_issue_resolution(db_path, account_id=ACCT, resolution=fake_res)
            self.assertFalse(res_fake.ok)
            self.assertEqual(res_fake.error, "OPENING_OCCURRENCE_NOT_FOUND")
            self.assertFalse(read_durable_issues(read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())).malformed)

            # 2. Valid opening commits with description == issue_id and passes Phase 2C read_durable_issues() replay!
            t0 = time.time_ns()
            opening = {
                "version": 1,
                "issue_id": ident,
                "contract": {"account_id": ACCT, "security_id": "3045", "exchange_segment": "NSE_EQ", "product_type": "CNC"},
                "reason_code": reason,
                "classification": "DURABLE_UNRESOLVED",
                "scope": "PARENT",
                "subject_id": "P10",
                "source": "watcher",
                "first_seen_utc_ns": t0,
                "snapshot_id": "SNAP-OPEN-1",
                "journal_digest": jv0.digest_sha256,
                "evidence_refs": [
                    {"snapshot_id": "SNAP-OPEN-1", "book_key": "journal:view", "row_index": None, "payload_sha256": jv0.digest_sha256, "journal_ref": "journal:view"}
                ],
                "observed": None,
            }
            res_open = commit_durable_issue_open(db_path, account_id=ACCT, opening=opening)
            self.assertTrue(res_open.ok, res_open)
            jv1 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            st1 = read_durable_issues(jv1)
            self.assertFalse(st1.malformed)
            self.assertEqual(len(st1.unresolved), 1)
            self.assertEqual(st1.unresolved[0].event_id, res_open.event_id)

            # 3. Valid resolution linked to actual opening_event_id + opening_sha256 passes Phase 2C replay cleanly!
            valid_res = dict(
                fake_res,
                journal_digest=jv1.digest_sha256,
                opening_event_id=res_open.event_id,
                opening_sha256=res_open.payload_sha256,
                resolved_utc_ns=t0 + 1000,
            )
            res_ok = commit_durable_issue_resolution(db_path, account_id=ACCT, resolution=valid_res)
            self.assertTrue(res_ok.ok, res_ok)
            jv2 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            st2 = read_durable_issues(jv2)
            self.assertFalse(st2.malformed)
            self.assertEqual(len(st2.unresolved), 0)

    def test_c11_settlement_verifier_invoked_during_runtime_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            JournalDB(db_path)
            jv = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            snap = _make_snapshot(
                snapshot_id="C11-SNAP",
                session_phase=SessionPhase.SETTLED,
                holdings=[],
                positions=[],
                journal_digest=jv.digest_sha256,
            )
            from src.engine.broker_session import VerifiedCalendar
            cal = VerifiedCalendar(holidays=frozenset(), covered_years=frozenset({2026}))
            runtime = UnifiedAccountRuntime(data_dir=td, db_path=db_path, account_id=ACCT)
            policy = build_verified_policy_context(
                mode="LIVE", purpose=Purpose.STARTUP, snapshot=snap, journal_view=jv
            )
            res = runtime.evaluate_snapshot(snap, policy, calendar=cal)
            self.assertIsNotNone(res.settlement_result, "C11: verify_settled_session_evidence must be invoked!")
            self.assertIsNotNone(getattr(runtime, "last_settlement_result", None))

    def test_webui_displays_existing_valid_snapshot_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            acct_k = account_key(ACCT)
            sc_dir = Path(td) / "broker_evidence" / acct_k
            sc_dir.mkdir(parents=True, exist_ok=True)
            sc_path = sc_dir / "snapshots.sqlite3"
            now_m = time.monotonic_ns()
            now_u = time.time_ns()
            with closing(sqlite3.connect(str(sc_path))) as con:
                con.execute(
                    "CREATE TABLE account_state (account_key TEXT PRIMARY KEY, boot_id TEXT, token_epoch INT, mutation_epoch INT, owner_epoch INT, journal_epoch INT, session_epoch INT)"
                )
                con.execute("INSERT INTO account_state VALUES (?, 'boot-1', 1, 2, 3, 4, 5)", (acct_k,))
                con.execute(
                    """CREATE TABLE snapshots (
                        snapshot_id TEXT PRIMARY KEY, account_key TEXT NOT NULL, boot_id TEXT NOT NULL,
                        token_epoch INTEGER NOT NULL, mutation_epoch INTEGER NOT NULL, owner_epoch INTEGER NOT NULL,
                        journal_epoch INTEGER NOT NULL, session_epoch INTEGER NOT NULL, purpose TEXT NOT NULL,
                        required_books_json TEXT NOT NULL, capture_started_utc_ns INTEGER NOT NULL,
                        capture_finished_utc_ns INTEGER NOT NULL, capture_started_mono_ns INTEGER NOT NULL,
                        capture_finished_mono_ns INTEGER NOT NULL, session_date TEXT, session_phase TEXT NOT NULL,
                        calendar_sha256 TEXT, calendar_verified INTEGER NOT NULL, profile_account_id TEXT,
                        profile_verified INTEGER NOT NULL, state TEXT NOT NULL, invalid_reasons_json TEXT NOT NULL,
                        expires_mono_ns INTEGER NOT NULL, producer_pid INTEGER NOT NULL,
                        producer_starttime INTEGER NOT NULL, journal_digest TEXT
                    )"""
                )
                con.execute(
                    """INSERT INTO snapshots VALUES (
                        'WEBUI-SNAP-1', ?, 'boot-1', 1, 2, 3, 4, 5, 'UI', '[]',
                        ?, ?, ?, ?, '2026-11-26', 'SETTLED', ?, 1, ?, 1, 'COMPLETE', '[]', ?, 100, 1, ?
                    )""",
                    (acct_k, now_u, now_u, now_m, now_m, "0" * 64, ACCT, now_m + 10_000_000_000, "f" * 64),
                )
                con.commit()

            mtime_before = sc_path.stat().st_mtime_ns
            summary = peek_display_snapshot_summary(td, ACCT, now_mono_ns=now_m + 1_000_000_000)
            mtime_after = sc_path.stat().st_mtime_ns
            self.assertTrue(summary["available"], summary)
            self.assertEqual(summary["status"], "VERIFIED")
            self.assertTrue(summary["display_only"])
            self.assertFalse(summary["authoritative_for_mutation"])
            self.assertEqual(summary["snapshot_id"], "WEBUI-SNAP-1")
            self.assertEqual(mtime_before, mtime_after, "WebUI read-only peek must NEVER mutate snapshots.sqlite3")

    def test_permissive_policy_defaults_removed(self) -> None:
        p = build_policy_context()
        self.assertFalse(p.mutation_fence_verified)
        self.assertFalse(p.service_identity_verified)
        self.assertFalse(p.watcher_liveness_verified)
        self.assertFalse(p.risk_funds_mode_checks_passed)
        self.assertFalse(p.journal_readback_verified)


if __name__ == "__main__":
    unittest.main()
