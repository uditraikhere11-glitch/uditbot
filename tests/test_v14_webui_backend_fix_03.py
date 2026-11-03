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
    execute_configuration_update,
    prepare_configuration_change_record,
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

    def test_05_f02_02_two_trades_same_security_cannot_share_unlinked_protection(self):
        """
        F02-02 Mandatory Scenario 1:
        - Trade A (ORD-SBIN-A): 5 units of SBIN (3045), unprotected
        - Trade B (ORD-SBIN-B): 5 units of SBIN (3045), protected by OCO-SBIN-B (linked to ORD-SBIN-B)
        - Broker returns 5 OCO units on OCO-SBIN-B
        Expected: Trade A remains unprotected; LIVE -> TEST stays at 75% WAITING_FOR_SAFE_APPLY.
        """
        now = time.time()
        # Trade A: unprotected
        self.db.record_order_intent("CORR-SBIN-A", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
        self.db.update_order_intent("CORR-SBIN-A", state="ACKED", broker_order_id="ORD-SBIN-A")
        self.db.record_order(
            correlation_id="CORR-SBIN-A",
            broker_order_id="ORD-SBIN-A",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
            status="PENDING",
        )

        # Trade B: protected by OCO-SBIN-B
        self.db.record_order_intent("CORR-SBIN-B", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
        self.db.update_order_intent("CORR-SBIN-B", state="TRADED", broker_order_id="ORD-SBIN-B")
        self.db.record_order(
            correlation_id="CORR-SBIN-B",
            broker_order_id="ORD-SBIN-B",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
            status="TRADED",
        )
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_orders (
                    parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("ORD-SBIN-B", "OCO-SBIN-B", "SBIN", 776.0, 768.0, 824.0, 5, "ACTIVE", now),
            )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-SBIN-A", "correlationId": "CORR-SBIN-A", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"},
            {"orderId": "ORD-SBIN-B", "correlationId": "CORR-SBIN-B", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"},
        ]
        # Broker has only OCO-SBIN-B (5 units)
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-SBIN-B",
                "parentOrderId": "ORD-SBIN-B",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-SBIN-B",
                "parentOrderId": "ORD-SBIN-B",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec.operation_state, OperationState.WAITING_FOR_ORDER.value)
        self.assertEqual(rec.progress_percent, 75)
        self.assertEqual(rec.blocked_reason, "UNPROTECTED_POSITION")
        self.assertIn("ORD-SBIN-A", rec.reason)

    def test_06_f02_02_unlinked_forever_oco_same_security_rejected(self):
        """
        F02-02 Mandatory Scenario 2:
        - Trade A (ORD-UNLINKED-PARENT) filled 5 units of SBIN (3045)
        - Broker returns a Forever OCO for SBIN (3045) with 5 units, but NO parent linkage exists
          (neither in protection_orders, protection_attempts, watcher_orders, nor broker parentOrderId/correlationId).
        Expected: Unlinked Forever OCO is NOT credited; LIVE -> TEST stays blocked at 75%.
        """
        self.db.record_order_intent("CORR-UNLINKED-1", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
        self.db.update_order_intent("CORR-UNLINKED-1", state="ACKED", broker_order_id="ORD-UNLINKED-PARENT")
        self.db.record_order(
            correlation_id="CORR-UNLINKED-1",
            broker_order_id="ORD-UNLINKED-PARENT",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
            status="PENDING",
        )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-UNLINKED-PARENT", "correlationId": "CORR-UNLINKED-1", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"}
        ]
        # Unlinked manual/other OCO on the same securityId "3045"
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-FOREIGN-999",
                "correlationId": "MANUAL-WEB-OCO",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-FOREIGN-999",
                "correlationId": "MANUAL-WEB-OCO",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec.blocked_reason, "UNPROTECTED_POSITION")
        self.assertIn("protected=0/5", rec.reason)

    def test_07_f02_02_partial_fill_protection_deficit_blocks_transition_until_full_coverage(self):
        """
        F02-02 Mandatory Scenario 3:
        - Parent order ORD-PART-10 filled 10 units of SBIN (3045)
        - Linked OCO-PART-1 protects only 4 units -> blocked (protected=4/10)
        - Second linked OCO-PART-2 protects remaining 6 units -> transition succeeds (100% APPLIED_VERIFIED)
        """
        now = time.time()
        self.db.record_order_intent("CORR-PART-10", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 10)
        self.db.update_order_intent("CORR-PART-10", state="ACKED", broker_order_id="ORD-PART-10")
        self.db.record_order(
            correlation_id="CORR-PART-10",
            broker_order_id="ORD-PART-10",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
            status="PENDING",
        )
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_orders (
                    parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("ORD-PART-10", "OCO-PART-1", "SBIN", 776.0, 768.0, 824.0, 4, "ACTIVE", now),
            )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-PART-10", "correlationId": "CORR-PART-10", "orderStatus": "TRADED", "filledQty": 10, "securityId": "3045"}
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-PART-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 4,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-PART-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 4,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec_blocked = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec_blocked.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec_blocked.blocked_reason, "UNPROTECTED_POSITION")
        self.assertIn("protected=4/10", rec_blocked.reason)

        # Now add second linked Forever OCO covering remaining 6 units
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_orders (
                    parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("ORD-PART-10", "OCO-PART-2", "SBIN", 776.0, 768.0, 824.0, 6, "ACTIVE", now + 1),
            )
        mock_broker.get_forever_orders.return_value.extend([
            {
                "orderId": "OCO-PART-2",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 6,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-PART-2",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 6,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ])

        resumed = self.tracker.try_auto_resume_deferred_update(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            broker=mock_broker,
            systemctl_restart_fn=self._make_restart_and_ack(),
            check_service_fn=lambda: True,
        )
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(resumed.progress_percent, 100)

    def test_08_f02_02_revoked_or_missing_broker_protection_blocks_live_to_test(self):
        """
        F02-02: If local watcher_orders / protection_orders claim an already-TRADED position is protected
        (status='ACTIVE', protected_qty=5), but live broker readback shows the Forever OCO was cancelled
        or is missing on the broker, reconciliation MUST detect the revoked protection and block LIVE -> TEST.
        """
        now = time.time()
        self.db.record_order_intent("CORR-REV-1", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
        self.db.update_order_intent("CORR-REV-1", state="TRADED", broker_order_id="ORD-REV-1")
        self.db.record_order(
            correlation_id="CORR-REV-1",
            broker_order_id="ORD-REV-1",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
            status="TRADED",
        )
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_orders (
                    parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("ORD-REV-1", "OCO-REV-1", "SBIN", 776.0, 768.0, 824.0, 5, "ACTIVE_CONFIRMED", now),
            )
            conn.execute(
                """
                INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at)
                VALUES (?, 1, ?, ?)
                """,
                (
                    "ORD-REV-1",
                    json.dumps({
                        "order_id": "ORD-REV-1",
                        "symbol": "SBIN",
                        "security_id": "3045",
                        "segment": "NSE_EQ",
                        "product_type": "CNC",
                        "qty": 5,
                        "filled_qty": 5,
                        "protected_qty": 5,
                        "protection_status": "ACTIVE_CONFIRMED",
                        "confirmed_protection_ids": ["OCO-REV-1"],
                        "state": "TRADED",
                    }),
                    now,
                ),
            )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-REV-1", "correlationId": "CORR-REV-1", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"}
        ]
        # Broker shows OCO-REV-1 was CANCELLED externally!
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-REV-1",
                "parentOrderId": "ORD-REV-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CANCELLED",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-REV-1",
                "parentOrderId": "ORD-REV-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CANCELLED",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec.blocked_reason, "UNPROTECTED_POSITION")
        self.assertIn("ORD-REV-1", rec.reason)

    def test_09_f02_02_linked_forever_oco_and_linked_super_order_verified_pass(self):
        """
        F02-02: Both a parent-linked Equity Forever OCO and a parent-linked F&O Margin Super Order
        with verified active SL and Target legs pass reconciliation and allow LIVE -> TEST.
        """
        self.db.record_order_intent("CORR-SUPER-1", "RELIANCE 2500 CE", "55001", "NSE_FNO", "MARGIN", "LIMIT", 100.0, 250)
        self.db.update_order_intent("CORR-SUPER-1", state="TRADED", broker_order_id="ORD-SUPER-1")
        self.db.record_order(
            correlation_id="CORR-SUPER-1",
            broker_order_id="ORD-SUPER-1",
            trading_mode="LIVE",
            product_type="MARGIN",
            exchange_segment="NSE_FNO",
            symbol="RELIANCE 2500 CE",
            security_id="55001",
            order_type="LIMIT",
            price=100.0,
            quantity=250,
            status="TRADED",
        )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {
                "orderId": "ORD-SUPER-1",
                "correlationId": "CORR-SUPER-1",
                "orderStatus": "TRADED",
                "filledQty": 250,
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
            }
        ]
        mock_broker.get_forever_orders.return_value = []
        mock_broker.get_super_orders.return_value = [
            {
                "orderId": "ORD-SUPER-1",
                "correlationId": "CORR-SUPER-1",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "orderStatus": "TRADED",
                "quantity": 250,
                "filledQty": 250,
                "legDetails": [
                    {"legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "quantity": 250, "price": 97.0, "triggerPrice": 97.0},
                    {"legName": "TARGET_LEG", "orderStatus": "PENDING", "quantity": 250, "price": 103.0, "triggerPrice": 103.0},
                ],
            }
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=self._make_restart_and_ack(),
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.progress_percent, 100)

    # =========================================================================
    # F02-03 — Automatic Deferred Transition Without Manual Coordinator Tick
    # =========================================================================

    def test_10_f02_03_automatic_deferred_transition_completes_without_manual_coordinator_tick(self):
        """
        F02-03 Mandatory Scenario:
        - Active LIVE order exists
        - User submits LIVE -> TEST via enqueue_durable_configuration_update
        - Request reaches 75% WAITING_FOR_SAFE_APPLY (WAITING_FOR_ORDER)
        - Order later completes and protection is verified in watcher.check_once()
        - Do NOT call /setup/coordinator-tick or run_control_coordinator_tick() manually!
        Expected: Persistent background coordinator daemon automatically completes transition to 100% APPLIED_VERIFIED.
        """
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_broker.order_status.return_value = {"orderId": "ORD-AUTO-F03", "orderStatus": "PENDING", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-AUTO-F03", "correlationId": "CORR-AUTO-F03", "orderStatus": "PENDING", "filledQty": 0, "securityId": "11915"}
        ]
        mock_broker.get_forever_orders.return_value = []

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=self.watcher_path,
            dry_run=False,
            journal_db=self.db,
        )
        self.db.record_order_intent("CORR-AUTO-F03", "YESBANK", "11915", "NSE_EQ", "CNC", "LIMIT", 15.60, 1)
        self.db.update_order_intent("CORR-AUTO-F03", state="ACKED", broker_order_id="ORD-AUTO-F03")
        self.db.record_order(
            correlation_id="CORR-AUTO-F03",
            broker_order_id="ORD-AUTO-F03",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="YESBANK",
            security_id="11915",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
            status="PENDING",
        )
        watcher.track(
            order_id="ORD-AUTO-F03",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=1,
            product_type="CNC",
            correlation_id="CORR-AUTO-F03",
        )

        restart_spy = MagicMock(side_effect=self._make_restart_and_ack())
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)

        init_rec = self.tracker.enqueue_durable_configuration_update(
            form_params=form_params,
            env_content=env_content,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_spy,
            check_service_fn=lambda: True,
            broker=mock_broker,
            spawn_worker=True,
        )
        self.assertEqual(init_rec.progress_percent, 15)

        # Wait briefly for coordinator daemon to process the queued request into 75% WAITING_FOR_SAFE_APPLY
        deadline_wait = time.time() + 2.0
        cur_rec = None
        while time.time() < deadline_wait:
            cur_rec = self.tracker.get_change_record(init_rec.change_id)
            if cur_rec and cur_rec.state == ConfigApplyState.WAITING_FOR_SAFE_APPLY:
                break
            time.sleep(0.02)

        self.assertIsNotNone(cur_rec)
        self.assertEqual(cur_rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(cur_rec.operation_state, OperationState.WAITING_FOR_ORDER.value)
        self.assertEqual(cur_rec.progress_percent, 75)
        self.assertTrue(self.tracker.is_live_entry_blocked_by_transition()[0])
        restart_spy.assert_not_called()

        # Now order fills and Forever OCO is confirmed in watcher.check_once()
        mock_broker.order_status.return_value = {
            "orderId": "ORD-AUTO-F03",
            "orderStatus": "TRADED",
            "filledQty": 1,
            "averageTradedPrice": 15.55,
        }
        mock_broker.trades_avg_price.return_value = 15.55
        mock_broker.place_forever_oco.return_value = (True, "OCO-AUTO-F03", "OK")
        mock_broker.get_order_book.return_value = [
            {"orderId": "ORD-AUTO-F03", "correlationId": "CORR-AUTO-F03", "orderStatus": "TRADED", "filledQty": 1, "securityId": "11915"}
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-AUTO-F03",
                "parentOrderId": "ORD-AUTO-F03",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 1,
                "triggerPrice": 15.08,
                "price": 14.93,
            },
            {
                "orderId": "OCO-AUTO-F03",
                "parentOrderId": "ORD-AUTO-F03",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 1,
                "triggerPrice": 16.02,
                "price": 16.02,
            },
        ]

        # Watcher resolves order and wakes coordinator daemon — NO manual coordinator tick called!
        watcher.check_once()

        deadline_complete = time.time() + 2.5
        final_rec = None
        while time.time() < deadline_complete:
            final_rec = self.tracker.get_change_record(init_rec.change_id)
            if final_rec and final_rec.state == ConfigApplyState.APPLIED_VERIFIED:
                break
            time.sleep(0.02)

        self.assertIsNotNone(final_rec)
        self.assertEqual(final_rec.change_id, init_rec.change_id)
        self.assertEqual(final_rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(final_rec.operation_state, OperationState.APPLIED_VERIFIED.value)
        self.assertEqual(final_rec.progress_percent, 100)
        self.assertTrue(final_rec.runtime_verified)
        self.assertEqual(final_rec.running_mode, "TEST")
        self.assertFalse(self.tracker.is_live_entry_blocked_by_transition()[0])
        restart_spy.assert_called_once()

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

    def test_12_f02_04_recover_interrupted_in_progress_envelope_before_restart(self):
        """
        F02-04 Mandatory Scenario 1:
        - Create pending_config_operation.json with status="IN_PROGRESS" (simulating worker crash
          after setting status="IN_PROGRESS" before completing staging/restart).
        - Run coordinator recovery.
        Expected: Operation is recovered, executed to 100% APPLIED_VERIFIED, and pending_op_file removed.
        """
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        _, init_rec, _ = prepare_configuration_change_record(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            check_service_fn=lambda: True,
        )
        op_payload = {
            "change_id": init_rec.change_id,
            "sequence_id": init_rec.sequence_id,
            "status": "IN_PROGRESS",
            "worker_pid": 999999,
            "started_at": time.time() - 5.0,
            "form_params": form_params,
            "env_content": env_content,
            "env_path": self.env_path,
            "stop_state_path": self.stop_path,
            "db_path": self.db_path,
            "created_at": time.time() - 6.0,
        }
        with open(self.tracker.pending_op_file, "w", encoding="utf-8") as f:
            json.dump(op_payload, f)

        restart_spy = MagicMock(side_effect=self._make_restart_and_ack())
        outcomes = self.tracker.run_control_coordinator_tick(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_spy,
            check_service_fn=lambda: True,
        )
        self.assertIsNotNone(outcomes["recovered_interrupted"])
        rec_after = self.tracker.get_change_record(init_rec.change_id)
        self.assertEqual(rec_after.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec_after.progress_percent, 100)
        self.assertFalse(os.path.exists(self.tracker.pending_op_file))
        restart_spy.assert_called_once()

    def test_13_f02_04_recover_interrupted_applying_without_duplicate_restart(self):
        """
        F02-04 Mandatory Scenario 2:
        - Worker initiated systemctl restart (state=APPLYING, restart_performed=True) and crashed
          while pending_config_operation.json still had status="IN_PROGRESS".
        - The restarted bot process acknowledged the new TEST hash and mode.
        Expected: Coordinator recovery verifies the running process, marks 100% APPLIED_VERIFIED,
        removes pending_config_operation.json, and does NOT call systemctl restart a second time.
        """
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(env_content)
        _, init_rec, _ = prepare_configuration_change_record(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            check_service_fn=lambda: True,
        )
        restart_ts = time.time()
        init_rec.state = ConfigApplyState.APPLYING
        init_rec.operation_state = OperationState.VERIFYING.value
        init_rec.current_step = "APPLYING"
        init_rec.progress_percent = 90
        init_rec.applied_at = restart_ts
        init_rec.restart_performed = True
        self.tracker.save_change_record(init_rec)

        with open(self.tracker.pending_op_file, "w", encoding="utf-8") as f:
            json.dump({"change_id": init_rec.change_id, "status": "IN_PROGRESS"}, f)

        # Simulate new bot process acknowledging proposed hash after restart_ts
        self.tracker.record_active_running_version(init_rec.proposed_version_hash, trading_mode="TEST")

        restart_spy = MagicMock(return_value=True)
        outcomes = self.tracker.run_control_coordinator_tick(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_spy,
            check_service_fn=lambda: True,
        )
        restart_spy.assert_not_called()
        self.assertIsNotNone(outcomes["recovered_interrupted"])
        rec_final = self.tracker.get_change_record(init_rec.change_id)
        self.assertEqual(rec_final.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec_final.progress_percent, 100)
        self.assertTrue(rec_final.runtime_verified)
        self.assertFalse(os.path.exists(self.tracker.pending_op_file))

    # =========================================================================
    # F02-05 — Owner Stop Verification
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
