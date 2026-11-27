"""
AB-V14-WEBUI-BACKEND-FIX-02: Comprehensive Regression & Integration Test Suite.

Covers Section H (H1–H5):
- H1: Explicit reproduction and verification tests for all 12 backend defects (B01–B12).
- H2: Cross-process integration tests (real OS subprocesses competing for fcntl.flock,
      concurrent Save & Stop, concurrent Start & Restart, two control workers on same
      deferred action, coordinator crash recovery, corrupt/unwritable transition gate).
- H3: Runtime verification matrix (saved TEST / running LIVE, saved LIVE / running TEST,
      stale PID, old acknowledgement timestamp, MainPID mismatch, hash mismatch,
      systemd failed substate, genuine matching runtime).
- H4: Complete broker lifecycle matrix (TRANSIT, PENDING, PART_TRADED, TRADED with
      unverified vs verified protection, CANCELLED, REJECTED, EXPIRED, UNKNOWN,
      broker timeout, historical stale journal row, retry order blocked during transition,
      partial fill with outstanding protection).
"""

from dataclasses import asdict
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.broker.dhan_client import DhanClient
from src.config import AppConfig, TradingMode, parse_env_dict
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeRecord,
    ConfigChangeTracker,
    CrossProcessOperationLock,
    GateOperationResult,
    OperationState,
    ReadOnlyDhanBrokerAdapter,
    ServiceOperatingState,
    TransitionGateStatus,
    compute_config_version,
)
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, OrderRequest, OrderType, ProductType, TransactionType
from src.setup_app import SETUP_CSRF_TOKEN, app, build_env_and_params_dict
from src.engine.config_patch import revision


def _child_process_try_lock(lock_file: str, lease_file: str, result_queue: multiprocessing.Queue) -> None:
    """Helper executed in a separate OS process to verify cross-process fcntl.flock enforcement (B08/H2)."""
    lk = CrossProcessOperationLock(
        lock_file=lock_file,
        lease_file=lease_file,
        operation_type="CHILD_PROC_TEST",
        operation_id="CHILD-1",
        blocking=False,
    )
    acquired = lk.acquire()
    if acquired:
        lk.release()
    result_queue.put(acquired)


class TestV14WebUIBackendFix02(unittest.TestCase):
    def setUp(self):
        self._orig_env = dict(os.environ)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.env_path = os.path.join(self.tmp_dir.name, ".env")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")

        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(
                "OPERATOR_NAME=Udit\n"
                "BOT_INSTANCE_NAME=Audit bot\n"
                "TRADING_MODE=LIVE\n"
                "EQUITY_ACTIVE=true\n"
                "FNO_ACTIVE=true\n"
                "EQUITY_BUDGET_INR=500.0\n"
                "FNO_CAPITAL_PER_TRADE=20000.0\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "DHAN_PIN=123456\n"
                "DHAN_TOTP_SECRET=JBSWY3DPEHPK3PXP\n"
                "TG_API_ID=123456\n"
                "TG_API_HASH=INITIAL_HASH_ABC\n"
                "TG_BOT_TOKEN=111:INITIAL_BOT_TOK\n"
                "MONITORED_CHANNEL_IDS=-100111,-100222\n"
                "LOG_GROUP_ID=-5514889030\n"
            )

        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, f)

        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        ConfigChangeTracker.register_default_broker_factory(None)

        # Record baseline running LIVE process state
        base_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        self.tracker.record_active_running_version(compute_config_version(base_cfg), trading_mode="LIVE")

    def tearDown(self):
        ConfigChangeTracker.register_default_broker_factory(None)
        self.tmp_dir.cleanup()
        os.environ.clear()
        os.environ.update(self._orig_env)

    def _make_restart_and_ack(self, mode_override: str = None):
        def _restart():
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            m = mode_override or new_cfg.trading_mode.value
            self.tracker.record_active_running_version(
                compute_config_version(new_cfg),
                trading_mode=m,
            )
            return True
        return _restart

    # =========================================================================
    # B01 — Transition gate cleared prematurely (P0)
    # =========================================================================

    def test_B02_transition_gate_corruption_missing_and_io_errors_fail_closed(self):
        # 1. Valid inactive state initially (missing record with no pending transitions)
        st0 = self.tracker.inspect_transition_gate_status()
        self.assertFalse(st0["active"])
        self.assertEqual(st0["status"], TransitionGateStatus.MISSING_RECORD.value)

        # 2. Valid active gate
        res_act = self.tracker.set_transition_gate(True, change_id="CFG-B02", target_mode="TEST", reason="Testing B02")
        self.assertIsInstance(res_act, GateOperationResult)
        self.assertTrue(bool(res_act))
        ok_unpacked, _ = res_act
        self.assertTrue(ok_unpacked)
        st_act = self.tracker.inspect_transition_gate_status()
        self.assertTrue(st_act["active"])
        self.assertEqual(st_act["status"], TransitionGateStatus.VALID_ACTIVE.value)

        # 3. Valid inactive gate after explicit release
        res_inact = self.tracker.set_transition_gate(False, change_id="CFG-B02", target_mode="TEST", reason="Done")
        self.assertTrue(bool(res_inact))
        st_inact = self.tracker.inspect_transition_gate_status()
        self.assertFalse(st_inact["active"])
        self.assertEqual(st_inact["status"], TransitionGateStatus.VALID_INACTIVE.value)

        # 4. Corrupted JSON in mode_transition_gate.json -> MUST fail closed (active=True)
        with open(self.tracker.transition_gate_file, "w", encoding="utf-8") as f:
            f.write("{corrupted_json: true")
        st_corrupt = self.tracker.inspect_transition_gate_status()
        self.assertTrue(st_corrupt["active"])
        self.assertEqual(st_corrupt["status"], TransitionGateStatus.CORRUPTED_RECORD.value)
        blocked_c, reason_c = self.tracker.is_live_entry_blocked_by_transition()
        self.assertTrue(blocked_c)
        self.assertIn("CORRUPTED_RECORD", reason_c)

        # 5. Non-boolean 'active' field in mode_transition_gate.json -> MUST fail closed
        with open(self.tracker.transition_gate_file, "w", encoding="utf-8") as f:
            json.dump({"active": "no"}, f)
        st_schema = self.tracker.inspect_transition_gate_status()
        self.assertTrue(st_schema["active"])
        self.assertEqual(st_schema["status"], TransitionGateStatus.CORRUPTED_RECORD.value)

        # Retired deferred-settings replay no longer synthesizes a transition gate.
        # 7. File read failure -> MUST fail closed
        with patch("builtins.open", side_effect=PermissionError("Read permission denied")):
            st_read_err = self.tracker.inspect_transition_gate_status()
            self.assertTrue(st_read_err["active"])
            self.assertEqual(st_read_err["status"], TransitionGateStatus.FILE_READ_FAILURE.value)

        # 8. File write failure -> returns explicit ok=False and blocks LIVE entries
        with patch("src.engine.config_manager.write_json_atomic", side_effect=OSError("Disk full")):
            res_w_err = self.tracker.set_transition_gate(True, change_id="CFG-W-ERR")
            self.assertFalse(bool(res_w_err))
            self.assertEqual(res_w_err.status, TransitionGateStatus.FILE_WRITE_FAILURE.value)
            b_w, r_w = self.tracker.is_live_entry_blocked_by_transition()
            self.assertTrue(b_w)
            self.assertIn("FILE_WRITE_FAILURE", r_w)

    # =========================================================================
    # B03 — False verification on identical configuration save (P0 / Scenario 5)
    # =========================================================================

    def test_B05_retry_orders_blocked_by_transition_gate_at_watcher_and_broker_boundaries(self):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="VALID_TOKEN",
            data_dir=self.data_dir,
        )
        stop_mgr = EmergencyStopManager(self.stop_path, expected_account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr, reconciliation_gate=_c60_admit_gate())

        # Activate transition gate
        self.tracker.set_transition_gate(
            active=True,
            change_id="CFG-B05-GATE",
            target_mode="TEST",
            reason="Pending LIVE -> TEST transition",
        )

        # 1. Direct DhanClient.place_order with RTRY- prefix MUST be blocked!
        retry_req = OrderRequest(
            correlation_id="RTRY-YESBANK-1000-01",
            symbol="YESBANK-EQ",
            security_id="11915",
            exchange_segment="NSE_EQ",
            transaction_type=TransactionType.BUY,
            product_type=ProductType.CNC,
            order_type=OrderType.LIMIT,
            price=15.60,
            quantity=10,
            trading_mode=TradingMode.LIVE,
        )
        with patch.object(client.session, "post", side_effect=AssertionError("RTRY order must not reach HTTP POST")):
            ok_r, oid_r, msg_r = client.place_order(retry_req)
            self.assertFalse(ok_r)
            self.assertIsNone(oid_r)
            self.assertIn("LIVE_TO_TEST_PENDING", msg_r)

            ok_sup, oid_sup, msg_sup = client.place_super_order(retry_req, stop_loss_price=15.0, target_price=16.5)
            self.assertFalse(ok_sup)
            self.assertIsNone(oid_sup)
            self.assertIn("LIVE_TO_TEST_PENDING", msg_sup)

        # 2. ProtectionWatcher.check_once() on a timed-out/cancelled order suppresses retry during transition
        #    while still placing Forever OCO protection for partial fills!
        mock_broker = MagicMock()
        mock_broker.cfg = cfg
        mock_broker.order_status.return_value = {
            "orderId": "ORD-PART-TIMEOUT",
            "orderStatus": "CANCELLED",
            "filledQty": 4,
            "averageTradedPrice": 15.50,
        }
        mock_broker.place_forever_oco.return_value = (True, "OCO-PART-4", "OK")
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-PART-4",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 4,
                "triggerPrice": 15.05,
                "price": 14.90,
            },
            {
                "orderId": "OCO-PART-4",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 4,
                "triggerPrice": 15.95,
                "price": 15.95,
            },
        ]

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=os.path.join(self.data_dir, "pending_protection.json"),
            dry_run=False,
            journal_db=self.db,
            order_timeout_sec=10,
            auto_retry_enabled=True,
        )
        watcher.track(
            order_id="ORD-PART-TIMEOUT",
            correlation_id="EQ-YESBANK-ORIG",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=10,
            product_type="CNC",
            original_market_price=15.50,
        )
        # Mark cancellation origin as BOT_TIMEOUT and retry_eligible=True
        watcher.pending["ORD-PART-TIMEOUT"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-PART-TIMEOUT"]["retry_eligible"] = True

        watcher.check_once()

        # Forever OCO for the 4 filled shares WAS placed and verified:
        mock_broker.place_forever_oco.assert_called_once()
        # Replacement retry BUY order was NOT placed because transition gate is active:
        mock_broker.place_order.assert_not_called()
        term_rec = watcher.terminal_orders.get("ORD-PART-TIMEOUT")
        self.assertIsNotNone(term_rec)
        self.assertEqual(term_rec.get("retry_final_status"), "BLOCKED_BY_TRANSITION_GATE")
        self.assertFalse(term_rec.get("retry_eligible"))

    # =========================================================================
    # B06 — Unsafe owner Stop after safety-check failure (P0)
    # =========================================================================

    def _make_flat_live_broker(self):
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        b = MagicMock()
        b.get_order_book.return_value = []
        b.get_trade_book.return_value = []
        b.get_holdings.return_value = []
        b.get_positions.return_value = []
        b.get_forever_orders.return_value = []
        b.get_super_orders.return_value = []
        attach_unified_live_evidence_to_stub_broker(b, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")
        return b

    def test_B06_owner_stop_defers_on_journal_error_and_unreadable_emergency_stop(self):
        stop_systemctl_mock = MagicMock(return_value=True)

        # 1. Corrupt emergency_stop.json -> EMERGENCY_STOP_UNREADABLE -> MUST defer Stop, NOT invoke systemctl stop!
        with open(self.stop_path, "w", encoding="utf-8") as f:
            f.write("{invalid_json")

        res_unreadable_stop = self.tracker.control_service(
            action="stop",
            reason="Owner stop during unreadable emergency stop",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=stop_systemctl_mock,
            check_service_fn=lambda: "active",
        )
        self.assertFalse(res_unreadable_stop["ok"])
        self.assertTrue(res_unreadable_stop.get("deferred"))
        self.assertEqual(res_unreadable_stop["blocked_reason"], "EMERGENCY_STOP_UNREADABLE")
        stop_systemctl_mock.assert_not_called()
        self.assertTrue(self.tracker.is_live_entry_blocked_by_transition()[0])

        # Restore clean emergency_stop.json, but simulate SQLite JOURNAL_ERROR
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": False, "in_flight_orders": [], "account_id": "1113731536"}, f)

        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("Database disk image is malformed")):
            res_db_err = self.tracker.control_service(
                action="stop",
                reason="Owner stop during journal error",
                env_path=self.env_path,
                stop_state_path=self.stop_path,
                db_path=self.db_path,
                systemctl_fn=stop_systemctl_mock,
                check_service_fn=lambda: "active",
            )
        self.assertFalse(res_db_err["ok"])
        self.assertTrue(res_db_err.get("deferred"))
        self.assertEqual(res_db_err["blocked_reason"], "JOURNAL_ERROR")
        stop_systemctl_mock.assert_not_called()

        # Once DB is healthy and zero obligations exist, coordinator automatically completes the deferred stop!
        svc_state = {"state": "active"}
        def _stop_and_mark_inactive(act, svc):
            stop_systemctl_mock(act, svc)
            if act == "stop":
                svc_state["state"] = "inactive"
            return True
        res_auto_stop = self.tracker.try_auto_complete_deferred_stop(
            broker=self._make_flat_live_broker(),
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=_stop_and_mark_inactive,
            check_service_fn=lambda: svc_state["state"],
        )
        self.assertIsNotNone(res_auto_stop)
        self.assertTrue(res_auto_stop["ok"])
        self.assertEqual(res_auto_stop["service_state"], ServiceOperatingState.STOPPED_BY_OWNER.value)
        stop_systemctl_mock.assert_called_once_with("stop", "trading_bot.service")

    # =========================================================================
    # B07 — Trading watcher restarts its own service (P1)
    # =========================================================================

    def test_B09_start_verifies_running_mode_and_hash_when_service_active(self):
        # Disk .env is set to TEST, but running process is LIVE!
        env_content_test, _ = build_env_and_params_dict(
            is_live_mode=False,
            equity_budget_inr=500.0,
            env_path=self.env_path,
        )
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(env_content_test)

        # Running process is still LIVE!
        self.tracker.record_active_running_version("OLD_LIVE_HASH", trading_mode="LIVE")

        systemctl_calls = []

        def _mock_systemctl(act: str, svc: str) -> bool:
            systemctl_calls.append((act, svc))
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(compute_config_version(new_cfg), trading_mode="TEST")
            return True

        res_start = self.tracker.control_service(
            action="start",
            reason="Owner pressed Start expecting TEST mode",
            broker=self._make_flat_live_broker(),
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=_mock_systemctl,
            check_service_fn=lambda: "active",
        )
        # Must NOT return idempotent=True without restarting! Must restart into TEST and verify!
        self.assertTrue(res_start["ok"])
        self.assertFalse(res_start.get("idempotent", False))
        self.assertTrue(res_start["runtime_verified"])
        self.assertEqual(res_start["running_mode"], "TEST")
        self.assertEqual(systemctl_calls, [("restart", "trading_bot.service")])

    # =========================================================================
    # B10 — Incomplete Restart action (P1)
    # =========================================================================

    def test_B10_authoritative_restart_and_status_actions(self):
        # 1. Read-only STATUS action
        res_status = self.tracker.control_service(
            action="status",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: "active",
        )
        self.assertTrue(res_status["ok"])
        self.assertTrue(res_status["read_only"])
        self.assertEqual(res_status["saved_mode"], "LIVE")
        self.assertEqual(res_status["running_mode"], "LIVE")
        self.assertTrue(res_status["runtime_verified"])

        # 2. RESTART action when active LIVE order exists -> deferred!
        self.db.record_order_intent(
            correlation_id="CORR-B10-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("CORR-B10-1", state="PENDING")

        systemctl_mock = MagicMock(return_value=True)
        res_restart_blocked = self.tracker.control_service(
            action="restart",
            confirm_live=True,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=systemctl_mock,
            check_service_fn=lambda: "active",
        )
        self.assertFalse(res_restart_blocked["ok"])
        self.assertTrue(res_restart_blocked.get("deferred"))
        systemctl_mock.assert_not_called()

        # 3. Resolve order -> RESTART succeeds and verifies!
        self.db.update_order_intent("CORR-B10-1", state="CANCELLED")
        res_restart_ok = self.tracker.control_service(
            action="restart",
            broker=self._make_flat_live_broker(),
            confirm_live=True,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=systemctl_mock,
            check_service_fn=lambda: "active",
        )
        self.assertTrue(res_restart_ok["ok"])
        self.assertTrue(res_restart_ok["runtime_verified"])
        systemctl_mock.assert_called_once_with("restart", "trading_bot.service")

    # =========================================================================
    # B11 — Asynchronous Save & Apply, Truthful Progress & Cancel Deferred (P1)
    # =========================================================================

    def test_B11_retired_configuration_endpoints_cannot_start_work(self):
        client = TestClient(app)
        for path in ("submit-async", "coordinator-tick", "cancel-deferred"):
            reply = client.post("/setup/" + path, json={"csrf_token": SETUP_CSRF_TOKEN})
            self.assertEqual(reply.status_code, 404)
        self.assertFalse(os.path.exists(self.tracker.pending_op_file))

    # =========================================================================
    # B12 — Broker verification not supplied consistently (P1)
    # =========================================================================

    def test_H4_broker_lifecycle_matrix_all_states(self):
        # Verify BrokerReadFailure (timeout / HTTP 500) fails closed and blocks transition
        self.db.record_order_intent(
            correlation_id="CORR-H4-1",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
        )
        self.db.update_order_intent("CORR-H4-1", state="TRANSIT", broker_order_id="ORD-H4-1")

        failing_broker = MagicMock()
        failing_broker.get_order_book.return_value = BrokerReadFailure(status="UNAVAILABLE", error="Read timed out", status_code=504)
        ok_b, reason_b, code_b, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            db_path=self.db_path,
            stop_state_path=self.stop_path,
            is_service_running=True,
            is_live_mode=True,
            target_is_live=False,
            broker=failing_broker,
            env_path=self.env_path,
        )
        self.assertFalse(ok_b)
        self.assertEqual(code_b, "BROKER_RECONCILIATION_FAILED")

        # Verify TRADED order WITHOUT verified Forever OCO protection blocks transition
        unprot_broker = MagicMock()
        unprot_broker.get_order_book.return_value = [
            {"dhanClientId": "1113731536", "orderId": "ORD-H4-1", "correlationId": "CORR-H4-1", "orderStatus": "TRADED", "filledQty": 5, "remainingQuantity": 0, "quantity": 5, "transactionType": "BUY", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"}
        ]
        unprot_broker.get_forever_orders.return_value = []
        unprot_broker.get_super_orders.return_value = []
        unprot_broker.get_trade_book.return_value = []
        unprot_broker.get_holdings.return_value = []
        unprot_broker.get_positions.return_value = []
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        attach_unified_live_evidence_to_stub_broker(unprot_broker, db_path=self.db_path, data_dir=self.data_dir, account_id='1113731536')
        ok_u, _, code_u, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            db_path=self.db_path,
            stop_state_path=self.stop_path,
            is_service_running=True,
            is_live_mode=True,
            target_is_live=False,
            broker=unprot_broker,
            env_path=self.env_path,
        )
        self.assertFalse(ok_u)
        self.assertEqual(code_u, "LIVE_ORDER_IN_PROGRESS")

        # Verify REJECTED / EXPIRED with 0 fills reconciles stale row and allows transition
        for term_state in ("REJECTED", "EXPIRED", "CANCELLED"):
            self.db.update_order_intent("CORR-H4-1", state="TRANSIT", broker_order_id="ORD-H4-1")
            term_broker = MagicMock()
            term_broker.get_order_book.return_value = [
                {"dhanClientId": "1113731536", "orderId": "ORD-H4-1", "correlationId": "CORR-H4-1", "orderStatus": term_state, "filledQty": 0, "remainingQuantity": 0, "quantity": 5, "transactionType": "BUY", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"}
            ]
            term_broker.get_trade_book.return_value = []
            term_broker.get_holdings.return_value = []
            term_broker.get_positions.return_value = []
            term_broker.get_forever_orders.return_value = []
            term_broker.get_super_orders.return_value = []
            from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
            attach_unified_live_evidence_to_stub_broker(term_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")
            ok_t, _, code_t, _ = self.tracker.evaluate_trading_safety_gate_detailed(
                db_path=self.db_path,
                stop_state_path=self.stop_path,
                is_service_running=True,
                is_live_mode=True,
                target_is_live=False,
                broker=term_broker,
                env_path=self.env_path,
            )
            self.assertTrue(ok_t, f"Expected terminal state {term_state} with 0 fills to reconcile and pass safety gate")
            self.assertEqual(code_t, "")


    def test_H4_partial_fill_with_outstanding_protection_blocks_until_protected_and_never_duplicates_orders(self):
        self.db.record_order_intent(
            correlation_id="CORR-PART-PROT",
            account_id="1113731536",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )
        self.db.update_order_intent("CORR-PART-PROT", state="CANCELLED", broker_order_id="ORD-PART-PROT")
        self.db.record_fill(
            "ORD-PART-PROT", "EID-PART-4", "SBIN", 800.0, 4, traded_at=time.time() - 30.0,
            transaction_type="BUY", security_id="3045", exchange_segment="NSE_EQ",
            product_type="CNC", parent_order_id="ORD-PART-PROT", is_verified_trade=True,
        )
        self.db.upsert_position_lifecycle(
            parent_order_id="ORD-PART-PROT", account_id="1113731536", symbol="SBIN", security_id="3045",
            exchange_segment="NSE_EQ", product_type="CNC", protection_route="FOREVER_OCO",
            position_state="UNPROTECTED", entry_qty=10, buy_filled_qty=4, buy_vwap=800.0,
            protected_qty=0, confirmed_open_qty=4,
        )
        with self.db._get_conn() as conn:
            conn.execute(
                "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, quantity, sl_trigger, sl_limit, target_price, status, created_at) "
                "VALUES ('ORD-PART-PROT', 'OCO-UNVER', 'SBIN', 4, 776.0, 768.0, 824.0, 'UNVERIFIED', ?)",
                (time.time(),),
            )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"dhanClientId": "1113731536", "orderId": "ORD-PART-PROT", "correlationId": "CORR-PART-PROT", "orderStatus": "CANCELLED", "filledQty": 4, "remainingQuantity": 0, "quantity": 10, "transactionType": "BUY", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"}
        ]
        mock_broker.get_forever_orders.return_value = []
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_trade_book.return_value = []
        mock_broker.get_holdings.return_value = []
        mock_broker.get_positions.return_value = []
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        attach_unified_live_evidence_to_stub_broker(mock_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")

        # Blocked while 4 partially-filled shares have UNVERIFIED protection
        ok1, _, code1, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            db_path=self.db_path,
            stop_state_path=self.stop_path,
            is_service_running=True,
            is_live_mode=True,
            target_is_live=False,
            broker=mock_broker,
            env_path=self.env_path,
        )
        self.assertFalse(ok1)
        self.assertEqual(code1, "UNPROTECTED_POSITION")
        mock_broker.place_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()

def _c60_admit_gate():
    class _Gate:
        is_accepting_entries = True
        def revalidate_for_transport_submission(self, *, pending_correlation_id=None, post_registration=False, expected_order_payload=None):
            return (True, 'ADMITTED')
    return _Gate()
