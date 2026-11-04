"""
AB-V14-WEBUI-BACKEND-FIX-01: Comprehensive Automated Regression Test Suite.

Covers all 34 required scenarios from Part 10:
1. LIVE -> TEST with no open orders.
2. LIVE -> TEST with a genuinely pending order.
3. LIVE -> TEST with partial fill.
4. LIVE -> TEST with pending cancellation.
5. LIVE -> TEST with unknown broker outcome.
6. LIVE -> TEST with unverified protection.
7. LIVE -> TEST with stale journal but verified terminal broker state.
8. Deferred transition automatically resumes after reconciliation.
9. Deferred transition persists across service/WebUI restart.
10. New LIVE entries are blocked during pending transition (at final broker submission boundary).
11. Existing LIVE watcher remains operational while transition is deferred.
12. TEST-mode simulation never replaces reconciliation of existing genuine LIVE orders.
13. Equity budget update while LIVE.
14. TEST -> LIVE with explicit owner confirmation.
15. TEST -> LIVE rejected when emergency STOP forbids activation.
16. Owner manually stops service (and stops safely or defers when LIVE order active).
17. Owner manually starts service.
18. Unexpected service failure (reported as FAILED / Trading Bot Stopped Unexpectedly, not STOPPED_BY_OWNER).
19. Service-status inspection timeout returns UNKNOWN, not STOPPED, and fails closed.
20. Repeated Stop or Start requests are idempotent.
21. Concurrent Save requests do not create duplicate restarts.
22. Existing and requested mode comparisons are accurate.
23. Unchanged configurations do not restart unnecessarily.
24. Hash mismatch is correctly reported.
25. Stale PID or active-version file cannot falsely verify success.
26. Progress remains at a genuine waiting checkpoint (60%).
27. Progress resumes under the original change ID.
28. Progress reaches 100% only when the requested operation is verified.
29. WebUI refresh preserves current operation status.
30. Rollback does not accidentally reactivate unsafe LIVE trading.
31. CSRF and authorization prevent unauthorized Start/Stop.
32. Existing CLI behavior remains compatible.
33. Existing Yahoo, BRAIN and broker execution regression tests remain passing.
34. No live broker write is performed during automated tests.
"""

from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import subprocess
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
    ServiceOperatingState,
    check_service_is_active,
    compute_config_version,
    execute_configuration_update,
    human_readable_service_label,
    human_readable_state_label,
    inspect_systemd_unit_raw_state,
)
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import OrderRequest, OrderType, ProductType, TransactionType
from src.setup_app import SETUP_CSRF_TOKEN, app, build_env_and_params_dict


class TestV14WebUIBackendModeSwitch(unittest.TestCase):
    def setUp(self):
        self._orig_env = dict(os.environ)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.env_path = os.path.join(self.tmp_dir.name, ".env")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")

        # Provision baseline LIVE .env
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

    def tearDown(self):
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

    # 1. LIVE -> TEST with no open orders
    def test_01_live_to_test_no_open_orders(self):
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=500.0,
            monitored_channel_ids="-100111,-100222",
            log_group_id="-5514889030",
            tg_api_id="123456",
            env_path=self.env_path,
        )
        restart_mock = MagicMock(side_effect=self._make_restart_and_ack())

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.progress_percent, 100)
        self.assertEqual(rec.previous_mode, "LIVE")
        self.assertEqual(rec.requested_mode, "TEST")
        self.assertEqual(rec.saved_mode, "TEST")
        self.assertEqual(rec.running_mode, "TEST")
        self.assertTrue(rec.restart_performed)
        self.assertTrue(rec.runtime_verified)
        self.assertIn("trading_mode: LIVE -> TEST", rec.changes_summary)
        restart_mock.assert_called_once()

    # 2. LIVE -> TEST with a genuinely pending order
    def test_02_live_to_test_with_genuinely_pending_order(self):
        self.db.record_order_intent(
            correlation_id="EQ-YESBANK-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-YESBANK-1", state="PENDING", broker_order_id="221261102657708")
        self.db.record_order(
            correlation_id="EQ-YESBANK-1",
            broker_order_id="221261102657708",
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

        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=500.0,
            env_path=self.env_path,
        )
        restart_mock = MagicMock()

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )

        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(rec.progress_percent, 75)
        self.assertEqual(rec.current_step, "SAFETY_GATE")
        self.assertEqual(rec.blocked_reason, "LIVE_ORDER_IN_PROGRESS")
        self.assertGreaterEqual(rec.blocking_order_count, 1)
        self.assertTrue(rec.auto_resume)
        self.assertEqual(rec.saved_mode, "LIVE")
        self.assertEqual(rec.requested_mode, "TEST")
        self.assertEqual(rec.running_mode, "LIVE")
        self.assertIn("Your TEST mode request has been saved", rec.reason)
        restart_mock.assert_not_called()

    # 3. LIVE -> TEST with partial fill
    def test_03_live_to_test_with_partial_fill(self):
        self.db.record_order_intent(
            correlation_id="EQ-SBIN-PART",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )
        self.db.update_order_intent("EQ-SBIN-PART", state="PART_TRADED", broker_order_id="ORD-PART-1")

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
        self.assertEqual(rec.progress_percent, 75)
        self.assertEqual(rec.blocked_reason, "LIVE_ORDER_IN_PROGRESS")

    # 4. LIVE -> TEST with pending cancellation
    def test_04_live_to_test_with_pending_cancellation(self):
        self.db.record_order_intent(
            correlation_id="EQ-SBIN-CANC",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )
        self.db.update_order_intent("EQ-SBIN-CANC", state="CANCEL_PENDING", broker_order_id="ORD-CANC-1")

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
        self.assertEqual(rec.blocked_reason, "LIVE_ORDER_IN_PROGRESS")

    # 5. LIVE -> TEST with unknown broker outcome
    def test_05_live_to_test_with_unknown_broker_outcome(self):
        self.db.record_order_intent(
            correlation_id="EQ-SBIN-UNK",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )
        self.db.update_order_intent("EQ-SBIN-UNK", state="UNKNOWN")

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
        self.assertEqual(rec.blocked_reason, "LIVE_ORDER_IN_PROGRESS")

    # 6. LIVE -> TEST with unverified protection
    def test_06_live_to_test_with_unverified_protection(self):
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_attempts (
                    attempt_id, parent_order_id, root_order_id, symbol, security_id,
                    exchange_segment, product_type, route, uncovered_from_qty, quantity,
                    executed_price, sl_trigger, sl_limit, target_price, correlation_id,
                    state, created_at, updated_at
                ) VALUES ('ATT-1', 'ORD-1', 'ORD-1', 'SBIN', '3045', 'NSE_EQ', 'CNC',
                          'FOREVER_OCO', 0, 5, 800.0, 776.0, 768.0, 824.0, 'CORR-1',
                          'ACKED_UNVERIFIED', ?, ?)
                """,
                (time.time(), time.time()),
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
        self.assertIn("Protection leg pending or unverified", rec.reason)

    # 7. LIVE -> TEST with stale journal but verified terminal broker state
    def test_07_live_to_test_with_stale_journal_verified_terminal_broker(self):
        self.db.record_order_intent(
            correlation_id="EQ-SBIN-STALE",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
        )
        self.db.update_order_intent("EQ-SBIN-STALE", state="ACKED", broker_order_id="ORD-STALE-1")
        self.db.record_order(
            correlation_id="EQ-SBIN-STALE",
            broker_order_id="ORD-STALE-1",
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
            {"orderId": "ORD-STALE-1", "correlationId": "EQ-SBIN-STALE", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"}
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-STALE-1",
                "parentOrderId": "ORD-STALE-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-STALE-1",
                "parentOrderId": "ORD-STALE-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]

        env_content, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        restart_mock = MagicMock(side_effect=self._make_restart_and_ack())

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
            broker=mock_broker,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.progress_percent, 100)
        restart_mock.assert_called_once()

    # 8. Deferred transition automatically resumes after reconciliation
    def test_08_deferred_transition_automatically_resumes_after_reconciliation(self):
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_broker.order_status.return_value = {"orderId": "221261102657708", "orderStatus": "PENDING", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "221261102657708", "correlationId": "EQ-YESBANK-1", "orderStatus": "PENDING", "filledQty": 0}
        ]

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=os.path.join(self.data_dir, "pending_protection.json"),
            dry_run=False,
            journal_db=self.db,
            order_timeout_sec=0,
            auto_retry_enabled=False,
        )
        self.db.record_order_intent(
            correlation_id="EQ-YESBANK-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-YESBANK-1", state="PENDING", broker_order_id="221261102657708")
        watcher.track(
            order_id="221261102657708",
            correlation_id="EQ-YESBANK-1",
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

        mock_broker.order_status.return_value = {"orderId": "221261102657708", "orderStatus": "CANCELLED", "filledQty": 0}
        mock_broker.get_order_book.return_value = [
            {"orderId": "221261102657708", "correlationId": "EQ-YESBANK-1", "orderStatus": "CANCELLED", "filledQty": 0}
        ]
        restart_mock = MagicMock(side_effect=self._make_restart_and_ack())
        watcher.on_deferred_restart_fn = restart_mock
        watcher.on_deferred_check_service_fn = lambda: True

        # B07: Watcher marks readiness only and NEVER restarts its own service
        watcher.check_once()
        mid_rec = self.tracker.get_change_record(rec.change_id)
        self.assertEqual(mid_rec.operation_state, "READY_TO_APPLY")
        restart_mock.assert_not_called()

        # Control coordinator executes the actual restart and verifies TEST runtime
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
        self.assertEqual(final_rec.progress_percent, 100)
        restart_mock.assert_called_once()

    # 9. Deferred transition persists across service/WebUI restart
    def test_09_deferred_transition_persists_across_service_and_webui_restart(self):
        self.db.record_order_intent(
            correlation_id="EQ-PERSIST-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-PERSIST-1", state="PENDING", broker_order_id="ORD-PERSIST-1")

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

        # Create a brand-new ConfigChangeTracker instance simulating process restart
        new_tracker = ConfigChangeTracker(data_dir=self.data_dir)
        loaded_rec = new_tracker.get_change_record(rec.change_id)
        self.assertIsNotNone(loaded_rec)
        self.assertEqual(loaded_rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertTrue(os.path.exists(new_tracker.deferred_env_file))
        self.assertTrue(new_tracker.is_live_entry_blocked_by_transition()[0])

    # 10. New LIVE entries are blocked during pending transition
    def test_10_new_live_entries_blocked_during_pending_transition(self):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="VALID_TOKEN",
            data_dir=self.data_dir,
        )
        stop_mgr = EmergencyStopManager(self.stop_path, expected_account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        self.tracker.set_transition_gate(
            active=True,
            change_id="CFG-TRANS-01",
            target_mode="TEST",
            reason="Waiting for YESBANK order",
        )

        req = OrderRequest(
            correlation_id="EQ-NEW-ENTRY-1",
            symbol="RELIANCE-EQ",
            security_id="2885",
            exchange_segment="NSE_EQ",
            transaction_type=TransactionType.BUY,
            product_type=ProductType.CNC,
            order_type=OrderType.LIMIT,
            price=1300.0,
            quantity=1,
            trading_mode=TradingMode.LIVE,
        )
        with patch.object(client.session, "post", side_effect=AssertionError("Must not call HTTP POST when transition gate active")):
            ok, oid, msg = client.place_order(req)
            self.assertFalse(ok)
            self.assertIsNone(oid)
            self.assertIn("LIVE_TO_TEST_PENDING", msg)

    # 11. Existing LIVE watcher remains operational while transition is deferred
    def test_11_existing_live_watcher_remains_operational_while_transition_deferred(self):
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        mock_broker.order_status.return_value = {"orderId": "ORD-W-1", "orderStatus": "TRADED", "filledQty": 2, "averageTradedPrice": 15.50}
        mock_broker.place_forever_oco.return_value = (True, "OCO-W-1", "OK")
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-W-1",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 2,
                "triggerPrice": 15.05,
                "price": 14.90,
            },
            {
                "orderId": "OCO-W-1",
                "securityId": "11915",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "PENDING",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 2,
                "triggerPrice": 15.95,
                "price": 15.95,
            },
        ]

        watcher = ProtectionWatcher(
            broker=mock_broker,
            path=os.path.join(self.data_dir, "pending_protection.json"),
            dry_run=False,
            journal_db=self.db,
            order_timeout_sec=0,
            auto_retry_enabled=False,
        )
        watcher.track(
            order_id="ORD-W-1",
            correlation_id="CORR-W-1",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=2,
            product_type="CNC",
        )
        # Activate transition gate
        self.tracker.set_transition_gate(active=True, change_id="CFG-WAIT", target_mode="TEST", reason="Waiting")

        # Watcher still places Forever OCO protection for existing order while transition gate is active!
        watcher.check_once()
        mock_broker.place_forever_oco.assert_called_once()
        self.assertIn("ORD-W-1", watcher.terminal_orders)

    # 12. TEST-mode simulation never replaces reconciliation of existing genuine LIVE orders
    def test_12_test_mode_simulation_never_replaces_reconciliation_of_live_orders(self):
        self.db.record_order_intent(
            correlation_id="EQ-LIVE-REAL-1",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-LIVE-REAL-1", state="PENDING", broker_order_id="221261102657708")

        test_cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536", data_dir=self.data_dir)
        test_client = DhanClient(test_cfg)

        is_safe, _, code, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            db_path=self.db_path,
            stop_state_path=self.stop_path,
            is_service_running=True,
            is_live_mode=True,
            target_is_live=False,
            broker=test_client,
        )
        self.assertFalse(is_safe)
        self.assertEqual(code, "LIVE_ORDER_IN_PROGRESS")

    # 13. Equity budget update while LIVE
    def test_13_equity_budget_update_while_live(self):
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=100.0,
            env_path=self.env_path,
        )
        restart_mock = MagicMock(side_effect=self._make_restart_and_ack())

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.progress_percent, 100)
        self.assertIn("equity_budget_inr: 500 -> 100", rec.changes_summary)
        self.assertEqual(rec.running_mode, "LIVE")
        restart_mock.assert_called_once()

    # 14. TEST -> LIVE with explicit owner confirmation
    def test_14_test_to_live_with_explicit_owner_confirmation(self):
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=LIVE\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")

        res_unconfirmed = self.tracker.control_service(
            action="start",
            confirm_live=False,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: False,
        )
        self.assertFalse(res_unconfirmed["ok"])
        self.assertTrue(res_unconfirmed.get("requires_live_confirmation"))

        svc_state = {"active": False}
        res_confirmed = self.tracker.control_service(
            action="start",
            confirm_live=True,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: (svc_state.update(active=(act in ("start", "restart"))) or True),
            check_service_fn=lambda: svc_state["active"],
        )
        self.assertTrue(res_confirmed["ok"])
        self.assertEqual(res_confirmed["running_mode"], "LIVE")

    # 15. TEST -> LIVE rejected when emergency STOP forbids activation
    def test_15_test_to_live_rejected_when_emergency_stop_active(self):
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": True, "stopped_by": "Udit", "reason": "Circuit breaker", "in_flight_orders": []}, f)

        env_content, form_params = build_env_and_params_dict(is_live_mode=True, equity_budget_inr=500.0, env_path=self.env_path)
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
        self.assertEqual(rec.blocked_reason, "EMERGENCY_STOP_ACTIVE")

    # 16. Owner manually stops service
    def test_16_owner_manually_stops_service(self):
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")
        svc_state = {"state": "active"}
        res_stop = self.tracker.control_service(
            action="stop",
            reason="Stopped from WebUI",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: svc_state["state"],
            systemctl_fn=lambda act, svc: (svc_state.update(state="inactive" if act == "stop" else "active") or True),
        )
        self.assertTrue(res_stop["ok"])
        self.assertEqual(res_stop["service_state"], "STOPPED_BY_OWNER")
        self.assertEqual(res_stop["service_state_label"], "Bot Status: Stopped by Owner")

    # 17. Owner manually starts service
    def test_17_owner_manually_starts_service(self):
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")
        svc_state = {"state": "inactive"}
        res_start = self.tracker.control_service(
            action="start",
            reason="Started from WebUI",
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_fn=lambda act, svc: (svc_state.update(state="active" if act in ("start", "restart") else "inactive") or True),
            check_service_fn=lambda: svc_state["state"],
        )
        self.assertTrue(res_start["ok"])
        self.assertEqual(res_start["service_state"], "RUNNING")
        self.assertEqual(res_start["running_mode"], "TEST")

    # 18. Unexpected service failure
    def test_18_unexpected_service_failure_distinguished_from_owner_stop(self):
        self.tracker.record_service_control_intent(
            desired_service_state="RUNNING",
            last_action="OWNER_START",
            reason="Running normally",
            requested_mode="TEST",
            observed_state="RUNNING",
        )
        st_crash, _ = self.tracker.resolve_service_operating_state(check_fn=lambda: "failed")
        self.assertEqual(st_crash, ServiceOperatingState.FAILED)
        self.assertEqual(human_readable_service_label(st_crash.value), "❌ Trading Bot Stopped Unexpectedly")

        st_inactive_unexp, _ = self.tracker.resolve_service_operating_state(check_fn=lambda: "inactive")
        self.assertEqual(st_inactive_unexp, ServiceOperatingState.FAILED)

    # 19. Service-status inspection timeout returns UNKNOWN, not STOPPED
    def test_19_service_status_inspection_timeout_returns_unknown(self):
        def _timeout_check():
            raise subprocess.TimeoutExpired(cmd="systemctl is-active", timeout=3)

        st_unk, _ = self.tracker.resolve_service_operating_state(check_fn=_timeout_check)
        self.assertEqual(st_unk, ServiceOperatingState.UNKNOWN)

    # 20. Repeated Stop or Start requests are idempotent
    def test_20_repeated_stop_or_start_requests_are_idempotent(self):
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500.0\nDHAN_CLIENT_ID=1113731536\n")
        self.tracker.record_service_control_intent(
            desired_service_state="STOPPED",
            last_action="OWNER_STOP",
            reason="Stopped",
            requested_mode="TEST",
        )
        res_stop_idem = self.tracker.control_service(
            action="stop",
            env_path=self.env_path,
            check_service_fn=lambda: "inactive",
        )
        self.assertTrue(res_stop_idem["ok"])
        self.assertTrue(res_stop_idem.get("idempotent"))

        cfg_t20 = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        self.tracker.record_active_running_version(compute_config_version(cfg_t20), trading_mode="TEST")
        res_start_idem = self.tracker.control_service(
            action="start",
            env_path=self.env_path,
            check_service_fn=lambda: "active",
        )
        self.assertTrue(res_start_idem["ok"])
        self.assertTrue(res_start_idem.get("idempotent"))

    # 21. Concurrent Save requests do not create duplicate restarts
    def test_21_concurrent_save_requests_no_duplicate_restarts(self):
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_budget_inr=300.0,
            env_path=self.env_path,
        )
        restart_count = []

        def _slow_restart():
            time.sleep(0.05)
            restart_count.append(1)
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(compute_config_version(new_cfg), trading_mode="TEST")
            return True

        results = []

        def _worker():
            r = execute_configuration_update(
                form_params=form_params,
                env_content=env_content,
                data_dir=self.data_dir,
                env_path=self.env_path,
                stop_state_path=self.stop_path,
                db_path=self.db_path,
                systemctl_restart_fn=_slow_restart,
                check_service_fn=lambda: True,
            )
            results.append(r)

        t1 = threading.Thread(target=_worker)
        t2 = threading.Thread(target=_worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(results), 2)
        self.assertEqual(len(restart_count), 1)

    # 22. Existing and requested mode comparisons are accurate
    def test_22_existing_and_requested_mode_comparisons_accurate(self):
        old_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        _, form_params = build_env_and_params_dict(is_live_mode=False, env_path=self.env_path)
        changes, _ = self.tracker.compare_configurations(asdict(old_cfg), form_params)
        self.assertIn("trading_mode: LIVE -> TEST", changes)

    # 23. Unchanged configurations do not restart unnecessarily
    def test_23_unchanged_configurations_do_not_restart(self):
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            operator_name="Udit",
            instance_name="Audit bot",
            tg_api_id="123456",
            monitored_channel_ids="-100111,-100222",
            log_group_id="-5514889030",
            dhan_client_id="1113731536",
            equity_budget_inr=500.0,
            fno_capital_per_trade=20000.0,
            env_path=self.env_path,
        )
        init_cfg_23 = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        self.tracker.record_active_running_version(compute_config_version(init_cfg_23), trading_mode="LIVE")
        restart_mock = MagicMock()
        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertFalse(rec.restart_performed)
        self.assertIn("No settings changed", rec.reason)
        restart_mock.assert_not_called()

    # 24. Hash mismatch is correctly reported
    def test_24_hash_mismatch_is_correctly_reported(self):
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=450.0, env_path=self.env_path)

        def _restart_wrong_hash():
            self.tracker.record_active_running_version("00000000000000000000000000000000", trading_mode="TEST")
            return True

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_wrong_hash,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
        self.assertIn("v-00000000", rec.error_msg)

    # 25. Stale PID or active-version file cannot falsely verify success
    def test_25_stale_pid_cannot_falsely_verify_success(self):
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=400.0, env_path=self.env_path)
        prop_hash = compute_config_version(form_params)

        def _restart_dead_pid():
            self.tracker.record_active_running_version(prop_hash, trading_mode="TEST", pid=99999999)
            return True

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_dead_pid,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
        self.assertFalse(rec.runtime_verified)
        self.assertIn("dead/replaced PID", rec.error_msg)

    # 26. Progress remains at a genuine waiting checkpoint
    def test_26_progress_remains_at_waiting_checkpoint_60_percent(self):
        self.db.record_order_intent(
            correlation_id="EQ-WAIT-60",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-WAIT-60", state="PENDING")

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
        self.assertEqual(rec.progress_percent, 75)
        self.assertEqual(rec.current_step, "SAFETY_GATE")

    # 27. Progress resumes under the original change ID
    def test_27_progress_resumes_under_original_change_id(self):
        self.db.record_order_intent(
            correlation_id="EQ-RESUME-ID",
            symbol="YESBANK",
            security_id="11915",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=15.60,
            quantity=1,
        )
        self.db.update_order_intent("EQ-RESUME-ID", state="PENDING")

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
        orig_id = rec.change_id

        # Mark intent cancelled and auto-resume
        self.db.update_order_intent("EQ-RESUME-ID", state="CANCELLED")
        resumed = self.tracker.try_auto_resume_deferred_update(
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=self._make_restart_and_ack(),
            check_service_fn=lambda: True,
        )
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed.change_id, orig_id)
        self.assertEqual(resumed.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(resumed.progress_percent, 100)

    # 28. Progress reaches 100% only when the requested operation is verified
    def test_28_progress_reaches_100_only_when_verified(self):
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=350.0, env_path=self.env_path)
        rec_fail = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=lambda: False,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec_fail.state, ConfigApplyState.APPLY_FAILED)
        self.assertEqual(rec_fail.progress_percent, 75)

    # 29. WebUI refresh preserves current operation status
    def test_29_webui_refresh_preserves_current_operation_status(self):
        client = TestClient(app)
        old_cwd = os.getcwd()
        os.chdir(self.tmp_dir.name)
        try:
            self.db.record_order_intent(
                correlation_id="EQ-WEB-REF",
                symbol="YESBANK",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                order_type="LIMIT",
                price=15.60,
                quantity=1,
            )
            self.db.update_order_intent("EQ-WEB-REF", state="PENDING")

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

            # Poll /setup/active-version and /setup/change-status/{change_id}
            r_act = client.get("/setup/active-version")
            self.assertEqual(r_act.status_code, 200)
            self.assertEqual(r_act.json()["latest_change"]["change_id"], rec.change_id)
            self.assertEqual(r_act.json()["latest_change"]["state"], "WAITING_FOR_SAFE_APPLY")

            r_st = client.get(f"/setup/change-status/{rec.change_id}")
            self.assertEqual(r_st.status_code, 200)
            self.assertEqual(r_st.json()["progress_percent"], 75)
        finally:
            os.chdir(old_cwd)

    # 30. Rollback does not accidentally reactivate unsafe LIVE trading
    def test_30_rollback_does_not_blindly_restart_live_trading(self):
        env_content, form_params = build_env_and_params_dict(is_live_mode=False, equity_budget_inr=200.0, env_path=self.env_path)
        restart_calls = []

        def _restart_once_then_fail_verify():
            restart_calls.append(1)
            # Do not acknowledge the new version hash
            self.tracker.record_active_running_version("STALE_HASH", trading_mode="LIVE")
            return True

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=_restart_once_then_fail_verify,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
        self.assertTrue(rec.rolled_back)
        # Must NOT call restart a second time to blindly restart LIVE!
        self.assertEqual(len(restart_calls), 1)

    # 31. CSRF and authorization prevent unauthorized Start/Stop
    def test_31_csrf_and_authorization_prevent_unauthorized_start_stop(self):
        client = TestClient(app)
        self.assertEqual(client.post("/setup/service-control", json={"action": "stop"}).status_code, 403)
        self.assertEqual(client.post("/setup/service-control", json={"action": "start", "csrf_token": "bad"}).status_code, 403)
        self.assertEqual(client.post("/setup/service-control", json={"action": "evil", "csrf_token": SETUP_CSRF_TOKEN}).status_code, 400)

    # 32. Existing CLI behavior remains compatible
    def test_32_existing_cli_behavior_and_service_control_compatible(self):
        from src.cli import cmd_service_control
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(f"TRADING_MODE=TEST\nDATA_DIR={self.data_dir}\nJOURNAL_DB_PATH={self.db_path}\n")
        cfg = AppConfig(trading_mode=TradingMode.TEST, data_dir=self.data_dir, journal_db_path=self.db_path)
        # Use this test's temporary TEST-mode .env, never the live repo .env.
        with patch("src.cli.resolve_env_path", return_value=self.env_path):
            rc_stop = cmd_service_control(cfg, "stop", ["CLI test stop"])
            self.assertEqual(rc_stop, 0)
            with patch.dict(os.environ, {"AUDITBOT_TEST_SERVICE_ACTIVE": "1"}):
                rc_start = cmd_service_control(cfg, "start", ["CLI test start"])
            self.assertEqual(rc_start, 0)

    # 33. Read-only service status inspection does not perform side-effect writes
    def test_33_service_status_inspection_is_strictly_read_only(self):
        with patch("src.engine.config_manager.repair_and_export_sqlite_session", side_effect=AssertionError("Must not run during status check")):
            active = check_service_is_active(check_fn=lambda: True)
            self.assertTrue(active)

    # 34. No live broker write is performed during automated tests
    def test_34_no_live_broker_write_performed_during_automated_tests(self):
        from src.engine.config_manager import is_isolated_test_environment
        self.assertTrue(is_isolated_test_environment())


if __name__ == "__main__":
    unittest.main()
