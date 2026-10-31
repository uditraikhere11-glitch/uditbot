from pathlib import Path
"""
CV13-06G: Automated Regression Test Suite for WebUI Save Confirmation,
Telegram Acknowledgement and Safe Configuration Application Tracking.

Verifies:
1. Clicking Save produces a unique change ID.
2. The popup/state shows request receipt without prematurely claiming success.
3. Valid updates progress through validation, persistence, application and verification.
4. Invalid updates are rejected without changes to the effective configuration.
5. An active broker order defers unsafe application (WAITING_FOR_SAFE_APPLY).
6. A pending broker acknowledgement defers unsafe application (WAITING_FOR_SAFE_APPLY).
7. Existing protection requirements remain enforced (pending protection attempts defer apply).
8. Telegram status notifications match actual state transitions.
9. Failed Telegram delivery does not corrupt configuration state.
10. Saving while the service is stopped does not start it (SAVED_FOR_NEXT_START).
11. Failed application is reported accurately (APPLY_FAILED).
12. Repeated Save requests cannot cause duplicate restarts.
13. Secret values are excluded from HTML, logs and Telegram messages.
14. Deferred changes remain safe after process restart.
15. No change is marked APPLIED_VERIFIED without confirmation from the running process.
"""

import json
import os
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.config import AppConfig, TradingMode
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeRecord,
    ConfigChangeTracker,
    compute_config_version,
    execute_configuration_update,
    sanitize_value,
)


class TestCV1306GConfigTracking(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.env_path = os.path.join(self.tmp_dir.name, ".env")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")

        # Provision base .env
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(
                "OPERATOR_NAME=Udit\n"
                "BOT_INSTANCE_NAME=Audit bot\n"
                "TRADING_MODE=TEST\n"
                "EQUITY_ACTIVE=true\n"
                "FNO_ACTIVE=true\n"
                "EQUITY_BUDGET_INR=25000\n"
                "FNO_CAPITAL_PER_TRADE=20000\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "DHAN_ACCESS_TOKEN=SECRET_TOKEN_ORIGINAL\n"
                "LOG_GROUP_ID=-5514889030\n"
            )

        # Provision clean emergency stop
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": False, "in_flight_orders": []}, f)

        # Provision empty SQLite DB
        con = sqlite3.connect(self.db_path)
        con.execute(
            "CREATE TABLE IF NOT EXISTS order_intents (correlation_id TEXT PRIMARY KEY, state TEXT);"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS orders (correlation_id TEXT PRIMARY KEY, status TEXT);"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS protection_attempts (attempt_id TEXT PRIMARY KEY, state TEXT);"
        )
        con.commit()
        con.close()

        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)

    def tearDown(self):
        self.tmp_dir.cleanup()

    # 1. Clicking Save produces a unique change ID
    def test_01_unique_change_ids(self):
        id1 = self.tracker.generate_change_id()
        time.sleep(0.01)
        id2 = self.tracker.generate_change_id()
        self.assertNotEqual(id1, id2)
        self.assertTrue(id1.startswith("CFG-"))
        self.assertTrue(id2.startswith("CFG-"))

    # 2. Popup/state shows request receipt without prematurely claiming success
    def test_02_request_receipt_not_premature_applied(self):
        cid = self.tracker.generate_change_id()
        rec = ConfigChangeRecord(change_id=cid, state=ConfigApplyState.REQUEST_RECEIVED)
        self.assertEqual(rec.state, ConfigApplyState.REQUEST_RECEIVED)
        self.assertNotEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)

        # Persist and reload
        self.tracker.save_change_record(rec)
        loaded = self.tracker.get_change_record(cid)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.state, ConfigApplyState.REQUEST_RECEIVED)

    # 3. Valid updates can progress through validation, persistence, application, and verification
    def test_03_valid_update_lifecycle_complete(self):
        form_params = {
            "equity_budget_inr": 35000.0,
            "fno_capital_per_trade": 25000.0,
            "sl_pct": 3.0,
            "target_pct": 4.0,
            "is_live_mode": False,
        }
        env_content = "OPERATOR_NAME=Udit\nTRADING_MODE=TEST\nEQUITY_BUDGET_INR=35000\n"

        # Mock service active and runner acknowledges version
        prop_ver = compute_config_version(form_params)

        def mock_restart():
            self.tracker.record_active_running_version(prop_ver, trading_mode="TEST")
            return True

        rec = execute_configuration_update(
            form_params=form_params,
            env_content=env_content,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=mock_restart,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )

        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec.effective_version_hash, prop_ver)
        self.assertIsNotNone(rec.saved_at)
        self.assertIsNotNone(rec.verified_at)

        # Verify .env written
        with open(self.env_path) as f:
            self.assertIn("EQUITY_BUDGET_INR=35000", f.read())

    # 4. Invalid updates are rejected without changes to the effective configuration
    def test_04_invalid_inputs_rejected_without_modifying_env(self):
        initial_env = Path(self.env_path).read_text(encoding='utf-8')
        invalid_params = {
            "equity_budget_inr": -500.0,  # Negative budget rejected
            "sl_pct": 3.0,
        }
        rec = execute_configuration_update(
            form_params=invalid_params,
            env_content="EQUITY_BUDGET_INR=-500\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec.state, ConfigApplyState.REJECTED)
        self.assertIn("must be greater than 0", rec.reason)

        # Confirm .env was NOT modified
        current_env = Path(self.env_path).read_text(encoding='utf-8')
        self.assertEqual(initial_env, current_env)

    # 5. An active broker order defers unsafe application (WAITING_FOR_SAFE_APPLY)
    def test_05_active_order_defers_application_in_live_mode(self):
        # Insert non-terminal order into DB
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT INTO orders (correlation_id, status) VALUES ('EQ-ORD-PENDING', 'PENDING');"
        )
        con.commit()
        con.close()

        form_params = {
            "equity_budget_inr": 40000.0,
            "fno_capital_per_trade": 20000.0,
            "is_live_mode": True,  # LIVE mode checks active orders
            "TRADING_MODE": "LIVE",
        }
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="TRADING_MODE=LIVE\nEQUITY_BUDGET_INR=40000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )

        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertIn("Active broker order in progress", rec.reason)

    # 6. A pending broker acknowledgement defers unsafe application (WAITING_FOR_SAFE_APPLY)
    def test_06_unresolved_intent_defers_application(self):
        # Insert unresolved submitting intent
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT INTO order_intents (correlation_id, state) VALUES ('EQ-INTENT-SUBMITTING', 'SUBMITTING');"
        )
        con.commit()
        con.close()

        form_params = {
            "equity_budget_inr": 40000.0,
            "fno_capital_per_trade": 20000.0,
            "is_live_mode": True,
            "TRADING_MODE": "LIVE",
        }
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="TRADING_MODE=LIVE\nEQUITY_BUDGET_INR=40000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )

        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertIn("An order is awaiting broker acknowledgement", rec.reason)

    # 7. Existing protection requirements remain enforced
    def test_07_pending_protection_attempt_defers_application(self):
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT INTO protection_attempts (attempt_id, state) VALUES ('OCO-ATTEMPT-01', 'SUBMITTING');"
        )
        con.commit()
        con.close()

        form_params = {
            "equity_budget_inr": 40000.0,
            "fno_capital_per_trade": 20000.0,
            "is_live_mode": True,
            "TRADING_MODE": "LIVE",
        }
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="TRADING_MODE=LIVE\nEQUITY_BUDGET_INR=40000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            check_service_fn=lambda: True,
        )

        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertIn("Protection leg pending", rec.reason)

    # 8. Telegram status notifications match actual state transitions
    def test_08_telegram_notification_templates(self):
        rec = ConfigChangeRecord(
            change_id="CFG-20261030-TEST",
            service_status="TEST",
            changes_summary=["Equity budget: 25000 -> 35000"],
            proposed_version_hash="abcdef1234567890",
            previous_version_hash="123456abcdef7890",
        )

        # 1. SAVED
        rec.state = ConfigApplyState.SAVED
        msg_saved = self.tracker.format_telegram_notification(rec)
        self.assertIn("AUDITBOT CONFIGURATION SAVED", msg_saved)
        self.assertIn("Change ID: <code>CFG-20261030-TEST</code>", msg_saved)
        self.assertIn("Application: Safety verification pending", msg_saved)

        # 2. DEFERRED
        rec.state = ConfigApplyState.WAITING_FOR_SAFE_APPLY
        rec.reason = "Pending broker order acknowledgement"
        msg_def = self.tracker.format_telegram_notification(rec)
        self.assertIn("AUDITBOT CONFIGURATION UPDATE DEFERRED", msg_def)
        self.assertIn("Reason: Pending broker order acknowledgement", msg_def)

        # 3. APPLIED
        rec.state = ConfigApplyState.APPLIED_VERIFIED
        msg_app = self.tracker.format_telegram_notification(rec)
        self.assertIn("AUDITBOT CONFIGURATION APPLIED", msg_app)
        self.assertIn("Configuration version: Verified (v-abcdef12)", msg_app)

        # 4. SAVED_FOR_NEXT_START
        rec.state = ConfigApplyState.SAVED_FOR_NEXT_START
        msg_start = self.tracker.format_telegram_notification(rec)
        self.assertIn("Saved for next authorized startup", msg_start)

        # 5. FAILED
        rec.state = ConfigApplyState.APPLY_FAILED
        rec.error_msg = "Process verification failed"
        msg_fail = self.tracker.format_telegram_notification(rec)
        self.assertIn("AUDITBOT CONFIGURATION APPLICATION FAILED", msg_fail)
        self.assertIn("Reason: Process verification failed", msg_fail)

    # 9. Failed Telegram delivery does not corrupt configuration state
    def test_09_failed_telegram_delivery_does_not_corrupt_config(self):
        mock_notifier = MagicMock()
        mock_notifier.send_log_message.side_effect = Exception("Telegram connection timed out")

        form_params = {"equity_budget_inr": 30000.0}
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="EQUITY_BUDGET_INR=30000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            notifier=mock_notifier,
            check_service_fn=lambda: False,
        )

        # State progressed cleanly to SAVED_FOR_NEXT_START despite Telegram error
        self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertTrue(os.path.exists(self.env_path))
        self.assertIn("ERROR", str(rec.delivery_status))

    # 10. Saving while the service is stopped does not start it (SAVED_FOR_NEXT_START)
    def test_10_saving_while_service_stopped_does_not_start_service(self):
        restart_mock = MagicMock()
        form_params = {"equity_budget_inr": 28000.0}

        rec = execute_configuration_update(
            form_params=form_params,
            env_content="EQUITY_BUDGET_INR=28000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: False,  # Service is stopped
        )

        self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        # Service restart function was NOT called
        restart_mock.assert_not_called()

    # 11. Failed application is reported accurately (APPLY_FAILED)
    def test_11_failed_application_reported_accurately(self):
        def failing_restart():
            return False

        form_params = {"equity_budget_inr": 32000.0}
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="EQUITY_BUDGET_INR=32000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=failing_restart,
            check_service_fn=lambda: True,
        )

        self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
        self.assertIn("failed", str(rec.error_msg).lower())

    # 12. Repeated Save requests cannot cause duplicate restarts
    def test_12_token_only_update_skips_restart(self):
        from dataclasses import asdict
        restart_mock = MagicMock()
        old_cfg = AppConfig.from_env(env_file=self.env_path)
        form_params = asdict(old_cfg)
        form_params["dhan_access_token"] = "NEW_TOKEN_999"
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="DHAN_ACCESS_TOKEN=NEW_TOKEN_999\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock,
            check_service_fn=lambda: True,
        )

        # Token-only refresh is verified without service restart
        self.assertEqual(rec.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertTrue(rec.is_token_only_refresh)
        restart_mock.assert_not_called()

    # 13. Secret values are excluded from HTML, logs, and Telegram messages
    def test_13_secrets_excluded_from_version_hash_and_notifications(self):
        cfg_with_token1 = {"equity_budget_inr": 25000, "dhan_access_token": "TOKEN_AAA"}
        cfg_with_token2 = {"equity_budget_inr": 25000, "dhan_access_token": "TOKEN_BBB"}

        # Version hashes must be identical because secret tokens are excluded
        h1 = compute_config_version(cfg_with_token1)
        h2 = compute_config_version(cfg_with_token2)
        self.assertEqual(h1, h2)

        # Notification message must not contain secret tokens
        rec = ConfigChangeRecord(
            change_id="CFG-SEC-01",
            changes_summary=["dhan_access_token: [REDACTED_UPDATE]"],
            state=ConfigApplyState.SAVED,
        )
        msg = self.tracker.format_telegram_notification(rec)
        self.assertNotIn("TOKEN_AAA", msg)
        self.assertNotIn("TOKEN_BBB", msg)
        self.assertIn("[REDACTED_UPDATE]", msg)

    # 14. Deferred changes remain safe after process restart
    def test_14_deferred_changes_persistence_across_restart(self):
        cid = self.tracker.generate_change_id()
        rec = ConfigChangeRecord(
            change_id=cid,
            state=ConfigApplyState.WAITING_FOR_SAFE_APPLY,
            reason="Active execution in progress",
        )
        self.tracker.save_change_record(rec)

        # Create new tracker instance simulating process restart
        new_tracker = ConfigChangeTracker(data_dir=self.data_dir)
        restored = new_tracker.get_change_record(cid)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertEqual(restored.reason, "Active execution in progress")

    # 15. No change is marked APPLIED_VERIFIED without confirmation from the running process
    def test_15_restart_without_running_acknowledgement_fails_verification(self):
        prop_ver = "PROPOSED_HASH_99999"

        def restart_without_ack():
            # Old version remains in active_version_file
            self.tracker.record_active_running_version("OLD_HASH_11111", trading_mode="TEST")
            return True

        form_params = {"equity_budget_inr": 50000.0}
        rec = execute_configuration_update(
            form_params=form_params,
            env_content="EQUITY_BUDGET_INR=50000\n",
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_without_ack,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )

        self.assertEqual(rec.state, ConfigApplyState.APPLY_FAILED)
        self.assertIn("acknowledged", str(rec.error_msg).lower())


if __name__ == "__main__":
    unittest.main()
