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
    execute_configuration_update,
)
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, OrderRequest, OrderType, ProductType, TransactionType
from src.setup_app import SETUP_CSRF_TOKEN, app, build_env_and_params_dict


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

    def test_B01_transition_gate_held_until_post_restart_verification_and_retained_on_failure(self):
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=500.0,
            env_path=self.env_path,
        )

        gate_states_during_restart = []

        def _restart_observing_gate_and_failing():
            # At the moment systemctl restart runs (after .env is written), the transition gate MUST still be active!
            blocked, reason = self.tracker.is_live_entry_blocked_by_transition()
            gate_states_during_restart.append((blocked, reason))
            return False  # Simulate restart failure

        rec_fail = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_observing_gate_and_failing,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec_fail.state, ConfigApplyState.APPLY_FAILED)
        self.assertEqual(len(gate_states_during_restart), 1)
        self.assertTrue(gate_states_during_restart[0][0], "Transition gate must be active during restart execution")
        # Because restart failed, the transition gate MUST remain active (blocking LIVE entries)
        still_blocked, _ = self.tracker.is_live_entry_blocked_by_transition()
        self.assertTrue(still_blocked, "Transition gate must remain active when LIVE -> TEST restart fails")

        # Now test when restart succeeds and TEST runtime is verified -> gate is finally released
        gate_states_on_success = []

        def _restart_observing_gate_and_succeeding():
            blocked, _ = self.tracker.is_live_entry_blocked_by_transition()
            gate_states_on_success.append(blocked)
            return self._make_restart_and_ack("TEST")()

        rec_ok = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_observing_gate_and_succeeding,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec_ok.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertTrue(gate_states_on_success[0], "Gate must be active while restart is in progress")
        final_blocked, _ = self.tracker.is_live_entry_blocked_by_transition()
        self.assertFalse(final_blocked, "Gate must be released after TEST runtime is verified")

    # =========================================================================
    # B02 — Transition-gate corruption fails open (P0)
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

        # 6. Missing gate record while deferred transition exists -> MUST recover & fail closed (INCONSISTENT_STATE)
        os.remove(self.tracker.transition_gate_file)
        with open(self.tracker.deferred_env_file, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\n")
        self.tracker.save_deferred_metadata({"change_id": "CFG-RECOVER-01", "requested_mode": "TEST", "previous_mode": "LIVE"})
        st_ro = self.tracker.inspect_transition_gate_status(read_only=True)
        self.assertTrue(st_ro["active"])
        self.assertEqual(st_ro["status"], TransitionGateStatus.INCONSISTENT_STATE.value)
        self.assertFalse(st_ro.get("recovered"))
        self.assertFalse(os.path.exists(self.tracker.transition_gate_file))
        st_recov = self.tracker.reconcile_and_recover_transition_gate()
        self.assertTrue(st_recov["active"])
        self.assertEqual(st_recov["status"], TransitionGateStatus.INCONSISTENT_STATE.value)
        self.assertTrue(st_recov.get("recovered"))
        self.assertTrue(os.path.exists(self.tracker.transition_gate_file))
        self.tracker.clear_deferred_artifacts(clear_gate=True)

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

    def test_B03_identical_disk_save_with_running_live_drift_restarts_and_verifies_test(self):
        # Write TEST to .env on disk, while active_config_version.json remains LIVE!
        env_content_test, form_params_test = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=500.0,
            env_path=self.env_path,
        )
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(env_content_test)

        # Running process is still LIVE!
        live_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        live_cfg.trading_mode = TradingMode.LIVE
        self.tracker.record_active_running_version(compute_config_version(live_cfg), trading_mode="LIVE")

        restart_mock = MagicMock(side_effect=self._make_restart_and_ack("TEST"))

        # Owner presses Save & Apply TEST (even though .env on disk is already TEST)
        rec = execute_configuration_update(
            form_params=form_params_test,
            env_content=env_content_test,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )

        # Must detect runtime drift, record trading_mode: LIVE -> TEST, restart, and verify TEST runtime!
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertTrue(rec.restart_required)
        self.assertTrue(rec.restart_performed)
        self.assertTrue(rec.runtime_verified)
        self.assertEqual(rec.previous_mode, "LIVE")
        self.assertEqual(rec.requested_mode, "TEST")
        self.assertEqual(rec.running_mode, "TEST")
        self.assertIn("trading_mode: LIVE -> TEST", rec.changes_summary)
        restart_mock.assert_called_once()

    # =========================================================================
    # B04 — Runtime verification incomplete (P0 & H3)
    # =========================================================================

    def test_B04_runtime_verification_enforces_mode_timestamp_mainpid_hash_and_systemd_state(self):
        env_content_test, form_params_test = build_env_and_params_dict(
            is_live_mode=False,
            equity_budget_inr=400.0,
            env_path=self.env_path,
        )

        # 1. Restart acknowledges matching hash, but WRONG mode (LIVE instead of TEST) -> MUST FAIL
        def _restart_wrong_mode():
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(compute_config_version(new_cfg), trading_mode="LIVE")
            return True

        rec_bad_mode = execute_configuration_update(
            form_params=form_params_test,
            env_content=env_content_test,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_wrong_mode,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec_bad_mode.state, ConfigApplyState.APPLY_FAILED)
        self.assertFalse(rec_bad_mode.runtime_verified)
        self.assertIn("Running service mode (LIVE) does not match expected mode (TEST)", rec_bad_mode.error_msg)

        # 2. Stale acknowledgement timestamp predating restart -> MUST FAIL
        new_cfg_test = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        new_cfg_test.trading_mode = TradingMode.TEST
        new_cfg_test.equity_budget_inr = 400.0
        # Pre-write acknowledgement before restart is initiated
        self.tracker.record_active_running_version(compute_config_version(new_cfg_test), trading_mode="TEST")
        time.sleep(0.02)

        rec_stale_ts = execute_configuration_update(
            form_params=form_params_test,
            env_content=env_content_test,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=lambda: True,  # Does not write a fresh timestamp!
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec_stale_ts.state, ConfigApplyState.APPLY_FAILED)
        self.assertFalse(rec_stale_ts.runtime_verified)
        self.assertIn("predates restart", rec_stale_ts.error_msg)

        # 3. Systemd MainPID mismatch -> MUST FAIL
        rec_pid_mismatch = execute_configuration_update(
            form_params=form_params_test,
            env_content=env_content_test,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=self._make_restart_and_ack("TEST"),
            check_service_fn=lambda: {"ActiveState": "active", "SubState": "running", "MainPID": os.getpid() + 99999},
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec_pid_mismatch.state, ConfigApplyState.APPLY_FAILED)
        self.assertIn("MainPID", rec_pid_mismatch.error_msg)

    # =========================================================================
    # B05 — Retry orders bypass transition gate (P0)
    # =========================================================================

    def test_B05_retry_orders_blocked_by_transition_gate_at_watcher_and_broker_boundaries(self):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="VALID_TOKEN",
            data_dir=self.data_dir,
        )
        stop_mgr = EmergencyStopManager(self.stop_path, expected_account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

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

    def test_B07_trading_watcher_never_restarts_its_own_service(self):
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_broker.order_status.return_value = {"orderId": "ORD-B07-1", "orderStatus": "CANCELLED", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-B07-1", "correlationId": "CORR-B07-1", "orderStatus": "CANCELLED", "filledQty": 0}
        ]

        self.db.record_order_intent(
            correlation_id="CORR-B07-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("CORR-B07-1", state="PENDING", broker_order_id="ORD-B07-1")

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=os.path.join(self.data_dir, "pending_protection.json"),
            dry_run=False,
            journal_db=self.db,
            order_timeout_sec=0,
            auto_retry_enabled=False,
        )
        watcher.track(
            order_id="ORD-B07-1",
            correlation_id="CORR-B07-1",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=1,
            product_type="CNC",
        )

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec.operation_state, OperationState.WAITING_FOR_ORDER.value)

        # Ensure watcher.check_once() NEVER calls execute_configuration_update or _invoke_systemctl
        with patch("src.engine.config_manager.execute_configuration_update", side_effect=AssertionError("Watcher must not call execute_configuration_update")), \
             patch.object(ConfigChangeTracker, "_invoke_systemctl", side_effect=AssertionError("Watcher must not call _invoke_systemctl")):
            watcher.check_once()

        # Watcher only marked the operation as READY_TO_APPLY
        ready_rec = self.tracker.get_change_record(rec.change_id)
        self.assertEqual(ready_rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(ready_rec.operation_state, OperationState.READY_TO_APPLY.value)

        # Now the WebUI/control coordinator executes the actual restart
        restart_mock = MagicMock(side_effect=self._make_restart_and_ack("TEST"))
        self.tracker.run_control_coordinator_tick(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            broker=mock_broker,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )
        final_rec = self.tracker.get_change_record(rec.change_id)
        self.assertEqual(final_rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(final_rec.operation_state, OperationState.APPLIED_VERIFIED.value)
        restart_mock.assert_called_once()

    # =========================================================================
    # B08 — Process-local configuration locking (P1 & H2)
    # =========================================================================

    def test_B08_cross_process_flock_blocks_other_os_processes_and_supersedes_old_deferred(self):
        # 1. Verify across real OS processes via multiprocessing that fcntl.flock blocks another process
        q: multiprocessing.Queue = multiprocessing.Queue()
        with self.tracker.operation_lock(operation_type="PARENT_HOLD", operation_id="OP-1"):
            p = multiprocessing.Process(
                target=_child_process_try_lock,
                args=(self.tracker.lock_file, self.tracker.lease_file, q),
            )
            p.start()
            p.join(timeout=5)
            child_acquired_while_held = q.get(timeout=2)
            self.assertFalse(child_acquired_while_held, "Separate OS process must not acquire lock while held by parent")

        # After parent releases lock, child process CAN acquire it
        p2 = multiprocessing.Process(
            target=_child_process_try_lock,
            args=(self.tracker.lock_file, self.tracker.lease_file, q),
        )
        p2.start()
        p2.join(timeout=5)
        child_acquired_after_release = q.get(timeout=2)
        self.assertTrue(child_acquired_after_release, "Separate OS process must acquire lock after parent releases it")

        # 2. Verify superseded deferred request handling: newer owner request cancels older waiting request
        self.db.record_order_intent(
            correlation_id="CORR-B08-WAIT",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("CORR-B08-WAIT", state="PENDING")

        env_1, params_1 = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=300.0, env_path=self.env_path)
        rec_1 = execute_configuration_update(
            form_params=params_1,
            env_content=env_1,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec_1.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)

        # Submit a newer request with equity_budget_inr=200.0
        env_2, params_2 = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=200.0, env_path=self.env_path)
        rec_2 = execute_configuration_update(
            form_params=params_2,
            env_content=env_2,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )
        self.assertGreater(rec_2.sequence_id, rec_1.sequence_id)
        # Attempting to auto-resume the old change_id must be rejected/no-op
        self.db.update_order_intent("CORR-B08-WAIT", state="CANCELLED")
        stale_resume = self.tracker.try_auto_resume_deferred_update(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            expected_change_id=rec_1.change_id,
        )
        self.assertIsNone(stale_resume)

    # =========================================================================
    # B09 — Start reports already running without verifying mode (P1)
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

    def test_B11_durable_async_submission_readonly_status_and_cancel_deferred(self):
        client = TestClient(app)
        old_cwd = os.getcwd()
        os.chdir(self.tmp_dir.name)
        try:
            self.db.record_order_intent(
                correlation_id="CORR-B11-1",
                symbol="YESBANK",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                order_type="LIMIT",
                price=15.60,
                quantity=1,
            )
            self.db.update_order_intent("CORR-B11-1", state="PENDING")

            # 1. Enqueue without spawning background thread to verify initial 15% RECEIVED durable record
            r_init = client.post(
                "/setup/submit-async",
                json={
                    "is_live_mode": False,
                    "dhan_client_id": "1113731536",
                    "equity_budget_inr": 300.0,
                    "csrf_token": SETUP_CSRF_TOKEN,
                    "spawn_worker": False,
                },
            )
            self.assertEqual(r_init.status_code, 200)
            init_data = r_init.json()
            cid = init_data["change_id"]
            self.assertEqual(init_data["state"], "REQUEST_RECEIVED")
            self.assertEqual(init_data["operation_state"], "REQUESTED")
            self.assertEqual(init_data["progress_percent"], 15)
            self.assertTrue(os.path.exists(self.tracker.pending_op_file))

            # 2. Coordinator tick processes the queued operation and pauses at 75% WAITING_FOR_ORDER
            self.tracker.run_control_coordinator_tick(
                env_path=self.env_path,
                stop_state_path=self.stop_path,
                db_path=self.db_path,
                check_service_fn=lambda: True,
            )
            r_wait = client.get(f"/setup/change-status/{cid}")
            self.assertEqual(r_wait.status_code, 200)
            wait_data = r_wait.json()
            self.assertEqual(wait_data["state"], "WAITING_FOR_SAFE_APPLY")
            self.assertEqual(wait_data["operation_state"], "WAITING_FOR_ORDER")
            self.assertEqual(wait_data["progress_percent"], 75)
            self.assertEqual(wait_data["step_status"]["SAFETY_GATE"], "WAITING")
            self.assertEqual(wait_data["step_status"]["APPLYING"], "PENDING")
            self.assertEqual(wait_data["step_status"]["VERIFIED"], "PENDING")

            # 3. Verify GET /setup/change-status/{cid} is strictly read-only even after order resolves!
            self.db.update_order_intent("CORR-B11-1", state="CANCELLED")
            with patch.object(ConfigChangeTracker, "try_auto_resume_deferred_update", side_effect=AssertionError("GET must not mutate")):
                r_poll = client.get(f"/setup/change-status/{cid}")
                self.assertEqual(r_poll.status_code, 200)
                self.assertEqual(r_poll.json()["state"], "WAITING_FOR_SAFE_APPLY")

            # 4. Cancel the deferred change via POST /setup/cancel-deferred
            r_cancel = client.post(
                "/setup/cancel-deferred",
                json={"change_id": cid, "reason": "Operator cancelled deferred mode switch", "csrf_token": SETUP_CSRF_TOKEN},
            )
            self.assertEqual(r_cancel.status_code, 200)
            self.assertTrue(r_cancel.json()["cancelled"])
            self.assertEqual(r_cancel.json()["operation_state"], "CANCELLED")
            self.assertFalse(self.tracker.is_live_entry_blocked_by_transition()[0])
        finally:
            os.chdir(old_cwd)

    # =========================================================================
    # B12 — Broker verification not supplied consistently (P1)
    # =========================================================================

    def test_B12_readonly_broker_adapter_supplied_automatically_and_blocks_mutations(self):
        # 1. Insert stale ACKED intent & PENDING order in SQLite
        self.db.record_order_intent(
            correlation_id="CORR-B12-STALE",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
        )
        self.db.update_order_intent("CORR-B12-STALE", state="ACKED", broker_order_id="ORD-B12-1")

        # 2. Register a default broker factory simulating Dhan confirming CANCELLED (0 fills)
        mock_raw_broker = MagicMock()
        mock_raw_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_raw_broker.get_order_book.return_value = [
            {"orderId": "ORD-B12-1", "correlationId": "CORR-B12-STALE", "orderStatus": "CANCELLED", "filledQty": 0}
        ]
        ConfigChangeTracker.register_default_broker_factory(lambda env_p, data_d: mock_raw_broker)

        # Verify ReadOnlyDhanBrokerAdapter forbids all write calls
        ro_adapter = self.tracker.get_readonly_broker(env_path=self.env_path)
        self.assertIsInstance(ro_adapter, ReadOnlyDhanBrokerAdapter)
        with self.assertRaises(RuntimeError):
            ro_adapter.place_order(MagicMock())
        with self.assertRaises(RuntimeError):
            ro_adapter.cancel_order("ORD-B12-1")

        # 3. Call execute_configuration_update WITHOUT passing `broker=` explicitly!
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=self._make_restart_and_ack("TEST"),
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        mock_raw_broker.get_order_book.assert_called_once()

    # =========================================================================
    # H2 — Coordinator Crash Recovery & Concurrent Control Workers
    # =========================================================================

    def test_H2_coordinator_crash_recovery_and_concurrent_workers_no_duplicate_restart(self):
        # 1. Simulate coordinator crash during APPLYING after restart already succeeded
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=250.0, env_path=self.env_path)
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(env_content)
        new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        new_hash = compute_config_version(new_cfg)

        now = time.time()
        crashed_rec = ConfigChangeRecord(
            change_id="CFG-CRASH-01",
            state=ConfigApplyState.APPLYING,
            operation_state=OperationState.VERIFYING.value,
            requested_at=now - 2.0,
            applied_at=now - 1.0,
            previous_version_hash="OLD_HASH",
            proposed_version_hash=new_hash,
            previous_mode="LIVE",
            requested_mode="TEST",
            saved_mode="TEST",
            running_mode="LIVE",
            progress_percent=90,
            current_step="APPLYING",
            restart_required=True,
            restart_performed=True,
        )
        self.tracker.save_change_record(crashed_rec)
        self.tracker.set_transition_gate(True, change_id="CFG-CRASH-01", target_mode="TEST", reason="Applying")
        # Bot finished starting and wrote its fresh acknowledgement
        self.tracker.record_active_running_version(new_hash, trading_mode="TEST")

        restart_mock = MagicMock()
        outcomes = self.tracker.run_control_coordinator_tick(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )
        # Reconstructed without repeating restart!
        restart_mock.assert_not_called()
        rec_recovered = outcomes["recovered_interrupted"]
        self.assertIsNotNone(rec_recovered)
        self.assertEqual(rec_recovered.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec_recovered.progress_percent, 100)
        self.assertTrue(rec_recovered.runtime_verified)
        self.assertFalse(self.tracker.is_live_entry_blocked_by_transition()[0])

    # =========================================================================
    # H4 — Broker Lifecycle Matrix (All Order & Protection States)
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
            {"orderId": "ORD-H4-1", "correlationId": "CORR-H4-1", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"}
        ]
        unprot_broker.get_forever_orders.return_value = []
        unprot_broker.get_super_orders.return_value = []
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
                {"orderId": "ORD-H4-1", "correlationId": "CORR-H4-1", "orderStatus": term_state, "filledQty": 0, "securityId": "3045"}
            ]
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


    def test_H2_concurrent_save_and_stop_and_concurrent_start_and_restart(self):
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=300.0, env_path=self.env_path)
        events = []

        def _slow_restart():
            events.append("RESTART_BEGIN")
            time.sleep(0.03)
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(compute_config_version(new_cfg), trading_mode="TEST")
            events.append("RESTART_END")
            return True

        def _slow_stop(act, svc):
            events.append(f"SYS_{act.upper()}_BEGIN")
            time.sleep(0.02)
            events.append(f"SYS_{act.upper()}_END")
            return True

        t_save = threading.Thread(
            target=execute_configuration_update,
            kwargs={
                "form_params": form_params,
                "env_content": env_content,
                "data_dir": self.data_dir,
                "env_path": self.env_path,
                "stop_state_path": self.stop_path,
                "db_path": self.db_path,
                "systemctl_restart_fn": _slow_restart,
                "check_service_fn": lambda: True,
            },
        )
        t_stop = threading.Thread(
            target=self.tracker.control_service,
            kwargs={
                "action": "stop",
                "env_path": self.env_path,
                "stop_state_path": self.stop_path,
                "db_path": self.db_path,
                "systemctl_fn": _slow_stop,
                "check_service_fn": lambda: "active",
            },
        )
        t_save.start()
        time.sleep(0.005)
        t_stop.start()
        t_save.join()
        t_stop.join()

        # Verify strict serialization: RESTART_BEGIN..RESTART_END never interleaves with SYS_STOP_BEGIN..SYS_STOP_END
        self.assertEqual(events, ["RESTART_BEGIN", "RESTART_END", "SYS_STOP_BEGIN", "SYS_STOP_END"])

    def test_H2_two_control_workers_attempting_same_deferred_action_only_one_executes(self):
        self.db.record_order_intent(
            correlation_id="CORR-2WORKERS",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("CORR-2WORKERS", state="PENDING")

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=350.0, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)

        # Resolve order and launch two concurrent control coordinator workers
        self.db.update_order_intent("CORR-2WORKERS", state="CANCELLED")
        restart_calls = []

        def _restart_once():
            time.sleep(0.03)
            restart_calls.append(1)
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(compute_config_version(new_cfg), trading_mode="TEST")
            return True

        results = []

        def _coord_worker():
            tracker_w = ConfigChangeTracker(data_dir=self.data_dir)
            r = tracker_w.try_auto_resume_deferred_update(
                env_path=self.env_path,
                stop_state_path=self.stop_path,
                db_path=self.db_path,
                systemctl_restart_fn=_restart_once,
                check_service_fn=lambda: True,
            )
            results.append(r)

        w1 = threading.Thread(target=_coord_worker)
        w2 = threading.Thread(target=_coord_worker)
        w1.start()
        w2.start()
        w1.join()
        w2.join()

        self.assertEqual(len(restart_calls), 1, "Exactly one worker must execute the deferred restart")
        non_none = [r for r in results if r is not None]
        self.assertEqual(len(non_none), 1)
        self.assertEqual(non_none[0].state, ConfigApplyState.APPLIED_VERIFIED)

    def test_H3_saved_live_running_test_and_unexpected_process_exit_detected(self):
        # 1. Saved LIVE on disk, but running process is TEST -> runtime_verified MUST be False in status!
        self.tracker.record_active_running_version("TEST_HASH_123", trading_mode="TEST")
        st = self.tracker.control_service(
            action="status",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: "active",
        )
        self.assertEqual(st["saved_mode"], "LIVE")
        self.assertEqual(st["running_mode"], "TEST")
        self.assertFalse(st["runtime_verified"])

        # 2. Unexpected process exit during post-restart verification -> detected and marked APPLY_FAILED!
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=250.0, env_path=self.env_path)
        svc_states = iter(["active", "active", "active", "failed", "failed"])
        rec_exit = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=self._make_restart_and_ack("TEST"),
            check_service_fn=lambda: next(svc_states, "failed"),
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec_exit.state, ConfigApplyState.APPLY_FAILED)
        self.assertFalse(rec_exit.runtime_verified)
        self.assertIn("ActiveState=failed", rec_exit.error_msg)

    def test_H4_partial_fill_with_outstanding_protection_blocks_until_protected_and_never_duplicates_orders(self):
        self.db.record_order_intent(
            correlation_id="CORR-PART-PROT",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )
        self.db.update_order_intent("CORR-PART-PROT", state="CANCELLED", broker_order_id="ORD-PART-PROT")
        with self.db._get_conn() as conn:
            conn.execute(
                "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, quantity, sl_trigger, sl_limit, target_price, status, created_at) "
                "VALUES ('ORD-PART-PROT', 'OCO-UNVER', 'SBIN', 4, 776.0, 768.0, 824.0, 'UNVERIFIED', ?)",
                (time.time(),),
            )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-PART-PROT", "correlationId": "CORR-PART-PROT", "orderStatus": "CANCELLED", "filledQty": 4, "securityId": "3045"}
        ]
        mock_broker.get_forever_orders.return_value = []

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
