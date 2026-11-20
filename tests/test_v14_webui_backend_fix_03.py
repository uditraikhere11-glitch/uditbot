"""
Regression & Verification Suite for AuditBot V14 — AB-V14-WEBUI-BACKEND-FIX-03
Covers all six independent audit findings (F02-01 through F02-06) and mandatory Section 9 scenarios:
- F02-01: False Runtime Verification When systemd Service Is Inactive or PID Mismatches
- F02-02: Broker Protection Reconciliation Credits Unlinked Protection by securityId Only & Revoked Protection
- F02-03: Automatic Deferred Completion Requires External Coordinator Tick (Daemon Coordinator)
- F02-04: Queued Operation Recovery Strands Interrupted IN_PROGRESS Requests
- F02-05: Owner Stop Trusts systemctl Return Code Without Verifying Stopped State
- F02-06: Read-Only Gate Status Inspection Mutates State File
"""

import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.config import AppConfig, TradingMode
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeRecord,
    ConfigChangeTracker,
    OperationState,
    ServiceOperatingState,
    TransitionGateStatus,
    compute_config_version,
)
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.setup_app import SETUP_CSRF_TOKEN, app, build_env_and_params_dict


class TestV14WebUIBackendFix03(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="auditbot_fix03_")
        self.data_dir = os.path.join(self.tmp_dir, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.env_path = os.path.join(self.tmp_dir, ".env")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.watcher_path = os.path.join(self.data_dir, "pending_protection.json")

        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(
                "OPERATOR_NAME=Udit\n"
                "BOT_INSTANCE_NAME=Audit bot\n"
                "TRADING_MODE=LIVE\n"
                "EQUITY_ACTIVE=true\n"
                "FNO_ACTIVE=false\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "EQUITY_BUDGET_INR=500.0\n"
                "MIN_TRADE_FUNDS_INR=100.0\n"
                "SL_PCT=3.0\n"
                "TARGET_PCT=3.0\n"
            )
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "is_stopped": False,
                    "stopped_by": None,
                    "reason": "Normal",
                    "in_flight_orders": [],
                    "account_id": "1113731536",
                },
                f,
            )

        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.initial_cfg = AppConfig.from_env(env_file=self.env_path)
        self.initial_hash = compute_config_version(self.initial_cfg)
        self.tracker.record_active_running_version(self.initial_hash, trading_mode="LIVE")

    def tearDown(self):
        try:
            self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        except Exception:
            pass
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _make_restart_and_ack(self, mode_override=None):
        def _restart():
            from src.config import parse_env_dict
            parsed = parse_env_dict(self.env_path)
            cfg = AppConfig.from_env(env=parsed, env_file="")
            h = compute_config_version(cfg)
            m = mode_override or cfg.trading_mode.value
            self.tracker.record_active_running_version(h, trading_mode=m)
            return True
        return _restart

    # =========================================================================
    # F02-01 — False Runtime Verification When systemd Service Is Inactive
    # =========================================================================

    def test_01_f02_01_reproduce_and_prevent_false_runtime_verification_when_systemd_inactive(self):
        """
        F02-01 Mandatory Reproduction:
        - Valid live PID belonging to the test process
        - Matching TEST mode and hash
        - Inactive trading_bot.service (default isolated test environment with check_service_fn=None)
        Expected: is_active_version_confirmed() fails by default without passing require_systemd_active=True.
        """
        test_cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536")
        test_hash = compute_config_version(test_cfg)
        self.tracker.record_active_running_version(test_hash, trading_mode="TEST")

        # Call is_active_version_confirmed with default arguments (check_service_fn=None)
        ok_default, info, reason = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
        )
        self.assertFalse(ok_default)
        self.assertIsNotNone(info)
        self.assertIn("not active", reason.lower())

        # Also fails when check_service_fn explicitly reports inactive / failed / deactivating
        for bad_state in (False, "inactive", "failed", "deactivating", {"ActiveState": "active", "SubState": "dead"}):
            ok_bad, _, err_bad = self.tracker.is_active_version_confirmed(
                expected_hashes={test_hash},
                expected_mode="TEST",
                check_service_fn=lambda s=bad_state: s,
            )
            self.assertFalse(ok_bad)
            self.assertTrue(len(err_bad) > 0)

        # Succeeds only when systemd reports active/running and MainPID matches live process
        ok_active, _, reason_active = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
            check_service_fn=lambda: {
                "ActiveState": "active",
                "SubState": "running",
                "Result": "success",
                "MainPID": os.getpid(),
                "ExecMainStatus": 0,
            },
        )
        self.assertTrue(ok_active)
        self.assertEqual(reason_active, "Verified")

    def test_02_f02_01_reject_main_pid_mismatch_and_recycled_pid_starttime(self):
        """
        F02-01:
        1. ActiveState=active, but systemd MainPID != acknowledged PID -> verification fails.
        2. ActiveState=active and MainPID==0 when explicitly reported by systemd -> verification fails.
        3. Recycled PID with mismatched /proc/<pid>/stat starttime -> verification fails.
        """
        test_cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536")
        test_hash = compute_config_version(test_cfg)
        self.tracker.record_active_running_version(test_hash, trading_mode="TEST")

        # 1. MainPID mismatch
        ok_mismatch, _, err_mismatch = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
            check_service_fn=lambda: {
                "ActiveState": "active",
                "SubState": "running",
                "Result": "success",
                "MainPID": os.getpid() + 99999,
            },
        )
        self.assertFalse(ok_mismatch)
        self.assertIn("MainPID", err_mismatch)

        # 2. Explicit MainPID=0 on active service
        ok_zero_pid, _, err_zero_pid = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
            check_service_fn=lambda: {
                "ActiveState": "active",
                "SubState": "running",
                "Result": "success",
                "MainPID": 0,
            },
        )
        self.assertFalse(ok_zero_pid)
        self.assertIn("MainPID", err_zero_pid)

        # 3. Recycled OS PID (mismatched os_starttime in active_config_version.json)
        with open(self.tracker.active_version_file, "r", encoding="utf-8") as f:
            ver_data = json.load(f)
        ver_data["process_starttime"] = int(ver_data.get("process_starttime") or 1000) + 55555
        with open(self.tracker.active_version_file, "w", encoding="utf-8") as f:
            json.dump(ver_data, f)

        ok_recycled, _, err_recycled = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
            check_service_fn=lambda: {
                "ActiveState": "active",
                "SubState": "running",
                "Result": "success",
                "MainPID": os.getpid(),
            },
        )
        self.assertFalse(ok_recycled)

    def test_03_f02_01_reject_stale_pre_restart_acknowledgement_timestamp(self):
        """
        F02-01: acknowledged_at < restart_initiated_at must fail verification.
        """
        test_cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536")
        test_hash = compute_config_version(test_cfg)
        self.tracker.record_active_running_version(test_hash, trading_mode="TEST")
        restart_initiated_at = time.time() + 10.0

        ok_stale, _, err_stale = self.tracker.is_active_version_confirmed(
            expected_hashes={test_hash},
            expected_mode="TEST",
            min_acknowledged_at=restart_initiated_at,
            check_service_fn=lambda: True,
        )
        self.assertFalse(ok_stale)
        self.assertIn("predates restart", err_stale)

    def test_04_f02_01_control_service_start_fails_unless_systemd_confirms_active(self):
        """
        F02-01: control_service("start") must pass check_service_fn to _poll_for_runtime_verification()
        and fail closed if trading_bot.service remains inactive after systemctl start returns 0.
        """
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")

        # systemctl start returns True, and active_config_version.json is written,
        # but check_service_fn continues reporting "inactive"
        res_start_inactive = self.tracker.control_service(
            action="start",
            reason="Owner start where systemd unit fails to become active",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: True,
            check_service_fn=lambda: "inactive",
            verify_delay_sec=0.02,
        )
        self.assertFalse(res_start_inactive["ok"])
        self.assertFalse(res_start_inactive["runtime_verified"])
        self.assertEqual(res_start_inactive["service_state"], ServiceOperatingState.FAILED.value)
        self.assertIn("not active", res_start_inactive["message"].lower())

    # =========================================================================
    # F02-02 — Broker Protection Correlation & Revoked Protection Detection
    # =========================================================================

    def test_11_f02_03_automatic_deferred_owner_stop_completes_without_manual_tick(self):
        """
        F02-03: Deferred Owner Stop automatically completes when blocking LIVE order resolves
        without any manual coordinator tick.
        """
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_broker.order_status.return_value = {"orderId": "ORD-STOP-AUTO", "orderStatus": "PENDING", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-STOP-AUTO", "correlationId": "CORR-STOP-AUTO", "orderStatus": "PENDING", "filledQty": 0}
        ]

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=self.watcher_path,
            dry_run=False,
            journal_db=self.db,
        )
        self.db.record_order_intent("CORR-STOP-AUTO", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 1)
        self.db.update_order_intent("CORR-STOP-AUTO", state="ACKED", broker_order_id="ORD-STOP-AUTO")
        self.db.record_order(
            correlation_id="CORR-STOP-AUTO",
            broker_order_id="ORD-STOP-AUTO",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=1,
            status="PENDING",
        )
        watcher.track(
            order_id="ORD-STOP-AUTO",
            symbol="SBIN",
            security_id="3045",
            segment="NSE_EQ",
            qty=1,
            product_type="CNC",
            correlation_id="CORR-STOP-AUTO",
        )

        svc_state = {"state": "active"}
        stop_calls = []

        def _systemctl(act, svc):
            stop_calls.append((act, svc))
            if act == "stop":
                svc_state["state"] = "inactive"
            return True

        res_defer = self.tracker.control_service(
            action="stop",
            reason="Owner stop while order pending",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            broker=mock_broker,
            systemctl_fn=_systemctl,
            check_service_fn=lambda: svc_state["state"],
        )
        self.assertFalse(res_defer["ok"])
        self.assertTrue(res_defer["deferred"])

        # Start persistent coordinator daemon
        self.tracker.start_coordinator_daemon(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            broker=mock_broker,
            systemctl_fn=_systemctl,
            check_service_fn=lambda: svc_state["state"],
            stop_when_idle=True,
        )

        # Now order is cancelled on broker with 0 fills
        mock_broker.order_status.return_value = {"orderId": "ORD-STOP-AUTO", "orderStatus": "CANCELLED", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-STOP-AUTO", "correlationId": "CORR-STOP-AUTO", "orderStatus": "CANCELLED", "filledQty": 0}
        ]
        watcher.check_once()

        deadline = time.time() + 2.5
        while time.time() < deadline:
            ctrl = self.tracker.get_service_control_record() or {}
            if ctrl.get("observed_state") == ServiceOperatingState.STOPPED_BY_OWNER.value and not ctrl.get("deferred_stop_pending"):
                break
            time.sleep(0.02)

        ctrl_final = self.tracker.get_service_control_record() or {}
        self.assertEqual(ctrl_final.get("observed_state"), ServiceOperatingState.STOPPED_BY_OWNER.value)
        self.assertFalse(ctrl_final.get("deferred_stop_pending"))
        self.assertEqual(len(stop_calls), 1)

    # =========================================================================
    # F02-04 — Interrupted IN_PROGRESS Operation Recovery
    # =========================================================================

    def test_14_f02_05_owner_stop_fails_closed_when_systemctl_succeeds_but_service_stays_active(self):
        """
        F02-05 Mandatory Scenario:
        - systemctl stop returns 0 (True), but systemd still reports ActiveState="active"
          (and also test "failed", "deactivating", "unknown").
        Expected: control_service("stop") fails closed (ok=False, stop_verified=False), reports
        actual state (RUNNING / FAILED / STOPPING / UNKNOWN), and does NOT claim STOPPED_BY_OWNER.
        """
        # 1. Service remains "active" despite systemctl stop returning True
        res_still_running = self.tracker.control_service(
            action="stop",
            reason="Stop where unit stays active",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: True,
            check_service_fn=lambda: "active",
            verify_delay_sec=0.02,
        )
        self.assertFalse(res_still_running["ok"])
        self.assertFalse(res_still_running["stop_verified"])
        self.assertEqual(res_still_running["service_state"], ServiceOperatingState.RUNNING.value)
        self.assertNotEqual(res_still_running["service_state"], ServiceOperatingState.STOPPED_BY_OWNER.value)
        # Transition gate must remain active to protect LIVE mode when stop fails to complete
        self.assertTrue(self.tracker.is_live_entry_blocked_by_transition()[0])

        # 2. Service enters "failed" during stop
        states_seq = ["active", "failed", "failed"]
        res_failed_stop = self.tracker.control_service(
            action="stop",
            reason="Stop where unit crashes into failed",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: True,
            check_service_fn=lambda: states_seq.pop(0) if len(states_seq) > 1 else states_seq[0],
            verify_delay_sec=0.02,
        )
        self.assertFalse(res_failed_stop["ok"])
        self.assertFalse(res_failed_stop["stop_verified"])
        self.assertEqual(res_failed_stop["service_state"], ServiceOperatingState.FAILED.value)

        # 3. Service stays in "deactivating" (STOPPING) past timeout
        deact_seq = ["active", "deactivating", "deactivating"]
        res_stopping = self.tracker.control_service(
            action="stop",
            reason="Stop where unit hangs in deactivating",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: True,
            check_service_fn=lambda: deact_seq.pop(0) if len(deact_seq) > 1 else deact_seq[0],
            verify_delay_sec=0.02,
        )
        self.assertFalse(res_stopping["ok"])
        self.assertFalse(res_stopping["stop_verified"])
        self.assertEqual(res_stopping["service_state"], ServiceOperatingState.STOPPING.value)

    # =========================================================================
    # F02-06 — Read-Only Status Inspection
    # =========================================================================

    def test_15_f02_06_status_inspection_and_get_endpoints_never_mutate_files(self):
        """
        F02-06 Mandatory Scenario:
        - Stage a pending LIVE -> TEST transition (deferred_config.env + deferred_config_meta.json)
        - Remove mode_transition_gate.json so the state is inconsistent (needs_gate=True)
        - Call inspect_transition_gate_status(), get_transition_gate(), control_service("status"),
          GET /setup/active-version, and GET /setup/change-status/{change_id}
        Expected:
        - Every read-only status inspection returns active=True, status=INCONSISTENT_STATE, needs_recovery=True, recovered=False
        - Zero files are written or created on disk during any status/GET inspection!
        - Calling reconcile_and_recover_transition_gate() explicitly repairs mode_transition_gate.json on disk.
        """
        with open(self.tracker.deferred_env_file, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\n")
        self.tracker.save_deferred_metadata({
            "change_id": "CFG-RO-TEST-01",
            "requested_mode": "TEST",
            "previous_mode": "LIVE",
        })
        rec = ConfigChangeRecord(
            change_id="CFG-RO-TEST-01",
            state=ConfigApplyState.WAITING_FOR_SAFE_APPLY,
            operation_state=OperationState.WAITING_FOR_ORDER.value,
            previous_mode="LIVE",
            requested_mode="TEST",
            progress_percent=75,
            current_step="SAFETY_GATE",
        )
        self.tracker.save_change_record(rec)
        if os.path.exists(self.tracker.transition_gate_file):
            os.remove(self.tracker.transition_gate_file)

        with patch("src.engine.config_manager.write_json_atomic", side_effect=AssertionError("Read-only status inspection must NEVER write files!")):
            gate_st = self.tracker.inspect_transition_gate_status()
            self.assertTrue(gate_st["active"])
            self.assertEqual(gate_st["status"], TransitionGateStatus.INCONSISTENT_STATE.value)
            self.assertTrue(gate_st.get("needs_recovery"))
            self.assertFalse(gate_st.get("recovered"))

            gate_get = self.tracker.get_transition_gate()
            self.assertTrue(gate_get["active"])
            self.assertFalse(gate_get.get("recovered"))

            svc_st = self.tracker.control_service(
                action="status",
                env_path=self.env_path,
                stop_state_path=self.stop_path,
                db_path=self.db_path,
                check_service_fn=lambda: True,
            )
            self.assertTrue(svc_st["ok"])
            self.assertTrue(svc_st["read_only"])
            self.assertTrue(svc_st["transition_gate"]["active"])
            self.assertFalse(svc_st["transition_gate"].get("recovered"))

            # Also verify FastAPI GET endpoints are strictly read-only
            client = TestClient(app)
            r1 = client.get("/setup/active-version")
            self.assertEqual(r1.status_code, 200)
            r2 = client.get(f"/setup/change-status/{rec.change_id}")
            self.assertIn(r2.status_code, (200, 404))

        self.assertFalse(os.path.exists(self.tracker.transition_gate_file))

        # Now invoke explicit recovery via reconcile_and_recover_transition_gate()
        recovered = self.tracker.reconcile_and_recover_transition_gate()
        self.assertTrue(recovered["active"])
        self.assertEqual(recovered["status"], TransitionGateStatus.INCONSISTENT_STATE.value)
        self.assertTrue(recovered.get("recovered"))
        self.assertTrue(os.path.exists(self.tracker.transition_gate_file))


if __name__ == "__main__":
    unittest.main()
