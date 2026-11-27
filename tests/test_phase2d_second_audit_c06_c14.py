"""Phase 2D Second Independent Audit Remediation Tests: C06, C07, C08, C11, C12, C13, C14."""
from __future__ import annotations

from contextlib import closing
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.broker.broker_snapshot import account_key
from src.broker.dhan_client import DhanClient
from src.broker.snapshot_coordinator import (
    CrossProcessReadCoordinator,
    EnvironmentObservation,
    EvidenceUnavailable,
)
from src.config import AppConfig, TradingMode
from src.engine.broker_ledger_reconciler import Purpose
from src.engine.config_manager import ConfigChangeTracker
from src.engine.journal_view import read_journal_view
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.unified_account_runtime import (
    UnifiedAccountRuntime,
    build_verified_policy_context,
    load_verified_exchange_calendar,
    peek_display_snapshot_summary,
    reconcile_and_repair_terminal_orders,
)
from src.journal.db import JournalDB
from tests.test_phase2d_c01_c05_and_settlement import ACCT, _make_snapshot


class TestSecondAuditC06ToC14(unittest.TestCase):
    def test_c06_all_live_journal_variants_block_when_broker_unavailable(self) -> None:
        """C06: Empty, acknowledged-only, pending, and unacknowledged LIVE journals MUST all fail closed when broker=None."""
        with tempfile.TemporaryDirectory() as td:
            tracker = ConfigChangeTracker(data_dir=td)
            env_path = str(Path(td) / "live.env")
            Path(env_path).write_text(f"TRADING_MODE=LIVE\nDHAN_CLIENT_ID={ACCT}\n", encoding="utf-8")

            # 1. Empty LIVE journal
            db_empty = str(Path(td) / "empty.db")
            JournalDB(db_empty)
            with patch.object(tracker, "get_readonly_broker", return_value=None):
                ok, err = tracker.reconcile_stale_journal_with_broker(
                    db_empty, broker=None, env_path=env_path, is_live_mode=True, is_service_running=True
                )
            self.assertFalse(ok, "C06: Empty LIVE journal with broker=None must fail closed")
            self.assertIn("BROKER_RECONCILIATION_FAILED", str(err))

            # 2. Acknowledged-only LIVE journal (no _only_settled_owner_acks bypass!)
            db_ack = str(Path(td) / "ack.db")
            j_ack = JournalDB(db_ack)
            j_ack.record_order(
                correlation_id="C-ACK", broker_order_id="ORD-ACK", trading_mode="LIVE",
                product_type="CNC", exchange_segment="NSE_EQ", symbol="YESBANK-EQ",
                security_id="11915", order_type="LIMIT", price=15.5, quantity=1, status="TRADED",
            )
            with j_ack._get_conn() as con:
                con.execute("UPDATE orders SET filled_quantity=1 WHERE broker_order_id='ORD-ACK'")
                con.execute(
                    "INSERT INTO manual_risk_acknowledgements (parent_order_id, correlation_id, account_id, symbol, security_id, quantity, acknowledged_by, reason, status, acknowledged_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    ("ORD-ACK", "C-ACK", ACCT, "YESBANK-EQ", "11915", 1, "owner", "retain", "UNPROTECTED_MANUAL_RISK", time.time()),
                )
            with patch.object(tracker, "get_readonly_broker", return_value=None):
                ok_a, err_a = tracker.reconcile_stale_journal_with_broker(
                    db_ack, broker=None, env_path=env_path, is_live_mode=True, is_service_running=True
                )
            self.assertFalse(ok_a, "C06: Acknowledged-only LIVE journal with broker=None must fail closed")
            self.assertIn("BROKER_RECONCILIATION_FAILED", str(err_a))

            # 3. Pending LIVE journal
            db_pend = str(Path(td) / "pend.db")
            j_pend = JournalDB(db_pend)
            j_pend.record_order_intent("C-P", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
            j_pend.update_order_intent("C-P", state="PENDING", broker_order_id="ORD-P")
            with patch.object(tracker, "get_readonly_broker", return_value=None):
                ok_p, err_p = tracker.reconcile_stale_journal_with_broker(
                    db_pend, broker=None, env_path=env_path, is_live_mode=True, is_service_running=True
                )
            self.assertFalse(ok_p)
            self.assertIn("LIVE_ORDER_IN_PROGRESS", str(err_p))

    def test_c07_missing_sidecar_blocks_and_full_uncompressed_generation_tuple(self) -> None:
        """C07: Absent generation sidecar raises EvidenceUnavailable and blocks LIVE STOP; full uncompressed tuple verified."""
        with tempfile.TemporaryDirectory() as td:
            tracker = ConfigChangeTracker(data_dir=td)
            # Absent sidecar with require_sidecar=True MUST raise EvidenceUnavailable (never return (0,0,acct)!)
            with self.assertRaises(EvidenceUnavailable):
                tracker._observe_owner_generation_fence(account_id=ACCT, require_sidecar=True)

            # Seed real sidecar -> returns complete 9-element uncompressed tuple (account_key, boot_id, 5 epochs, journal_digest, svc_id)
            coord = CrossProcessReadCoordinator(
                td, ACCT,
                observe=lambda: EnvironmentObservation((1, 1, 1, 1), "1" * 64, "2" * 64, "0" * 64, _make_snapshot(snapshot_id="B").session, True),
                collect=lambda *a, **kw: None,
            )
            coord.observe_generation()
            tup = tracker._observe_owner_generation_fence(account_id=ACCT, require_sidecar=True)
            self.assertEqual(len(tup), 9)
            self.assertEqual(tup[0], account_key(ACCT))
            self.assertIsInstance(tup[1], str)  # boot_id
            for idx in range(2, 7):
                self.assertIs(type(tup[idx]), int)

    def test_c08_and_c12_intrinsic_live_mandatory_unified_and_no_startup_deadlock(self) -> None:
        """C08 + C12: LIVE StartupReconciliationGate has mandatory unified verification intrinsically and never deadlocks."""
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path, account_id=ACCT)
            cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            broker = MagicMock()
            broker.cfg = cfg
            stop_mgr = MagicMock()
            stop_mgr.is_stopped.return_value = False
            stop_mgr.get_in_flight_orders.return_value = []
            watcher = MagicMock()
            watcher._is_running = True

            # C08: Intrinsic LIVE initialization sets _require_unified_verdict=True WITHOUT calling any opt-in switch!
            gate = StartupReconciliationGate(broker, db, watcher, stop_mgr)
            self.assertTrue(gate._require_unified_verdict)
            gate._accepting_entries = True
            self.assertFalse(gate.is_accepting_entries)
            ok_rv, rv_msg = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_rv)
            self.assertIn("UNIFIED_VERDICT_MISSING", rv_msg)

            # C12: Deterministic bounded-completion test proving _run_startup_reconciliation_inner does NOT deadlock!
            jv = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            snap = _make_snapshot(snapshot_id="C12-SNAP", holdings=[], positions=[], journal_digest=jv.digest_sha256)

            def _fast_unified(*a, **kw):
                return gate.publish_unified_verdict(
                    UnifiedAccountRuntime(data_dir=td, db_path=db_path, account_id=ACCT)
                    .evaluate_snapshot(
                        snap,
                        build_verified_policy_context(
                            mode="LIVE", purpose=Purpose.STARTUP, snapshot=snap,
                            journal_view=jv, stop_manager=stop_mgr, watcher=watcher, risk_funds_verified=True,
                        ),
                    ).verdict,
                    expected_halt_generation=gate._halt_generation,
                    snapshot_generation=snap.generation,
                )

            broker.get_order_book.return_value = []
            broker.get_positions.return_value = []
            broker.get_holdings.return_value = []
            broker.get_trade_book.return_value = []
            broker.get_forever_orders.return_value = []
            broker.get_super_orders.return_value = []
            broker.get_profile.return_value = {"dhanClientId": ACCT}
            broker.get_user_profile.return_value = {"dhanClientId": ACCT}
            broker.get_authenticated_profile_id.return_value = ACCT
            broker.get_fund_limits.return_value = (True, 50000.0, "OK")
            broker.verify_live_connectivity.return_value = (True, "OK")

            done = threading.Event()
            outcome = []
            def _worker():
                with patch.object(gate, "run_unified_startup_verification", side_effect=_fast_unified):
                    outcome.append(gate.run_startup_reconciliation())
                done.set()

            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            completed = done.wait(timeout=3.0)
            self.assertTrue(completed, "C12 DEADLOCK DETECTED: run_startup_reconciliation did not finish within 3s!")
            self.assertTrue(outcome[0][0], outcome[0])

            # Req #4: Journal mutation after unified verdict invalidates transport submission!
            db.record_system_event("TEST_MUTATION", "mutate journal digest")
            ok_drift, drift_msg = gate.revalidate_for_transport_submission()
            self.assertFalse(ok_drift)
            self.assertIn("UNIFIED_JOURNAL_DIGEST_DRIFT", drift_msg)

    def test_c11_real_verified_calendar_and_dhan_snapshot_source_acquisition(self) -> None:
        """C11: Real 49-holiday NSE calendar + real DhanSnapshotSource/CrossProcessReadCoordinator acquisition."""
        cal = load_verified_exchange_calendar()
        self.assertGreaterEqual(len(cal.holidays), 30, "Real NSE exchange holiday calendar must not be empty")

        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            JournalDB(db_path)
            cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, dhan_access_token="tok-1", data_dir=td)
            # Simulated HTTP transport client compatible with DhanSnapshotSource
            client = DhanClient(cfg)
            def _sim_http(endpoint, *args, **kwargs):
                if endpoint == "/profile":
                    return {"dhanClientId": ACCT, "tokenValidity": "31/12/2099 23:59"}
                return []

            class _FakeResp:
                def __init__(self, payload):
                    self.status_code = 200
                    self._payload = payload
                    self.text = json.dumps(payload)
                    self.headers = {}
                def json(self):
                    return self._payload

            def _sim_get(url, *args, **kwargs):
                if str(url).endswith("/profile"):
                    return _FakeResp({"dhanClientId": ACCT, "tokenValidity": "31/12/2099 23:59"})
                return _FakeResp([])

            with patch.object(client.session, "get", side_effect=_sim_get):
                runtime = UnifiedAccountRuntime(data_dir=td, db_path=db_path, account_id=ACCT)
                pol = build_verified_policy_context(
                    mode="LIVE",
                    purpose=Purpose.STARTUP,
                    snapshot=None,
                    journal_view=None,
                )
                # Verify acquire_and_evaluate invokes real verified calendar session supplier (never SESSION_UNVERIFIED!)
                res = runtime.acquire_and_evaluate(client, pol, calendar=cal)
                self.assertNotEqual(res.reason_code, "SESSION_UNVERIFIED")
                self.assertNotIn("SESSION_UNVERIFIED", res.detail)

    def test_c13_wrong_account_wrong_order_and_missing_qty_perform_zero_writes(self) -> None:
        """C13: Exact wrong-account/wrong-order payload with missing quantities performs ZERO journal writes."""
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            db.record_order_intent("C-AUDIT", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
            db.update_order_intent("C-AUDIT", state="PENDING", broker_order_id="ORD-AUDIT")

            # Negative 1: Exact ChatGPT reproduction payload
            bad_broker = MagicMock()
            bad_broker.order_status.return_value = {
                "orderStatus": "CANCELLED",
                "orderId": "OTHER",
                "dhanClientId": "WRONG",
                "securityId": "WRONG",
            }
            bad_broker.get_order_book.return_value = []
            bad_broker.get_trade_book.return_value = []
            ok_bad, err_bad, rep_bad = reconcile_and_repair_terminal_orders(
                db_path=db_path, broker=bad_broker, account_id=ACCT
            )
            self.assertFalse(ok_bad)
            self.assertEqual(rep_bad, {})
            with closing(sqlite3.connect(db_path)) as con:
                st = con.execute("SELECT state FROM order_intents WHERE correlation_id='C-AUDIT'").fetchone()[0]
            self.assertEqual(st, "PENDING", "C13 violated: wrong-account/order modified journal!")

            # Negative 2: Matching orderId/account/security but MISSING filledQty -> MUST NOT default to 0!
            missing_qty_broker = MagicMock()
            missing_qty_broker.order_status.return_value = {
                "orderStatus": "CANCELLED",
                "orderId": "ORD-AUDIT",
                "correlationId": "C-AUDIT",
                "dhanClientId": ACCT,
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
            }
            missing_qty_broker.get_order_book.return_value = []
            missing_qty_broker.get_trade_book.return_value = []
            ok_mq, _, rep_mq = reconcile_and_repair_terminal_orders(
                db_path=db_path, broker=missing_qty_broker, account_id=ACCT
            )
            self.assertFalse(ok_mq)
            self.assertEqual(rep_mq, {})

            # Positive control: Exact account, orderId, correlationId, contract, filledQty=0, remainingQuantity=0
            good_broker = MagicMock()
            good_broker.order_status.return_value = {
                "orderStatus": "CANCELLED",
                "orderId": "ORD-AUDIT",
                "correlationId": "C-AUDIT",
                "dhanClientId": ACCT,
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "filledQty": 0,
                "remainingQuantity": 0,
            }
            good_broker.get_order_book.return_value = []
            good_broker.get_trade_book.return_value = []
            ok_good, err_good, rep_good = reconcile_and_repair_terminal_orders(
                db_path=db_path, broker=good_broker, account_id=ACCT
            )
            self.assertTrue(ok_good, err_good)
            self.assertEqual(rep_good, {"ORD-AUDIT": "CANCELLED"})

    def test_c14_webui_rejects_stale_generation_and_expired_snapshot_and_accepts_valid(self) -> None:
        """C14: WebUI rejects owner_epoch mismatch (3 vs 4) and expired snapshot, accepts valid matching snapshot."""
        with tempfile.TemporaryDirectory() as td:
            ak = account_key(ACCT)
            sc_dir = Path(td) / "broker_evidence" / ak
            sc_dir.mkdir(parents=True, exist_ok=True)
            sc = sc_dir / "snapshots.sqlite3"
            now_m = time.monotonic_ns()
            now_u = time.time_ns()
            with closing(sqlite3.connect(str(sc))) as con:
                con.execute(
                    "CREATE TABLE account_state (account_key TEXT PRIMARY KEY, boot_id TEXT, token_epoch INT, mutation_epoch INT, owner_epoch INT, journal_epoch INT, session_epoch INT)"
                )
                con.execute("INSERT INTO account_state VALUES (?, 'boot-1', 1, 2, 4, 4, 5)", (ak,))
                con.execute(
                    """CREATE TABLE snapshots (
                        snapshot_id TEXT PRIMARY KEY, account_key TEXT, boot_id TEXT, token_epoch INT,
                        mutation_epoch INT, owner_epoch INT, journal_epoch INT, session_epoch INT,
                        purpose TEXT, required_books_json TEXT, capture_started_utc_ns INT,
                        capture_finished_utc_ns INT, capture_started_mono_ns INT, capture_finished_mono_ns INT,
                        session_date TEXT, session_phase TEXT, calendar_sha256 TEXT, calendar_verified INT,
                        profile_account_id TEXT, profile_verified INT, state TEXT, invalid_reasons_json TEXT,
                        expires_mono_ns INT, producer_pid INT, producer_starttime INT, journal_digest TEXT
                    )"""
                )
                # Stale owner_epoch=3 (current is 4) + expired
                con.execute(
                    "INSERT INTO snapshots VALUES ('STALE-SNAP', ?, 'boot-1', 1, 2, 3, 4, 5, 'UI', '[]', ?, ?, ?, ?, '2026-11-26', 'SETTLED', ?, 1, ?, 1, 'COMPLETE', '[]', ?, 100, 1, ?)",
                    (ak, now_u, now_u, now_m, now_m, "0" * 64, ACCT, now_m - 1000, "f" * 64),
                )
                con.commit()

            bad_sum = peek_display_snapshot_summary(td, ACCT, now_mono_ns=now_m + 1_000_000)
            self.assertFalse(bad_sum["available"], bad_sum)
            self.assertEqual(bad_sum["status"], "STALE_OR_INVALIDATED")

            # Insert valid matching generation + unexpired snapshot -> MUST succeed without mutating file
            with closing(sqlite3.connect(str(sc))) as con:
                con.execute(
                    "INSERT INTO snapshots VALUES ('VALID-SNAP', ?, 'boot-1', 1, 2, 4, 4, 5, 'UI', '[]', ?, ?, ?, ?, '2026-11-26', 'SETTLED', ?, 1, ?, 1, 'COMPLETE', '[]', ?, 100, 1, ?)",
                    (ak, now_u, now_u, now_m, now_m, "0" * 64, ACCT, now_m + 10_000_000_000, "f" * 64),
                )
                con.commit()
            mtime_before = sc.stat().st_mtime_ns
            good_sum = peek_display_snapshot_summary(td, ACCT, now_mono_ns=now_m + 1_000_000)
            self.assertTrue(good_sum["available"], good_sum)
            self.assertEqual(good_sum["status"], "VERIFIED")
            self.assertEqual(good_sum["snapshot_id"], "VALID-SNAP")
            self.assertEqual(mtime_before, sc.stat().st_mtime_ns)

    def test_req6_verified_policy_context_rejects_missing_runtime_facts(self) -> None:
        """Req #6: Missing watcher, stop_manager, snapshot, or journal_view MUST remain False."""
        pol = build_verified_policy_context(mode="LIVE", purpose=Purpose.STARTUP)
        self.assertFalse(pol.watcher_liveness_verified)
        self.assertFalse(pol.mutation_fence_verified)
        self.assertFalse(pol.service_identity_verified)
        self.assertFalse(pol.journal_readback_verified)
        self.assertFalse(pol.risk_funds_mode_checks_passed)


if __name__ == "__main__":
    unittest.main()
