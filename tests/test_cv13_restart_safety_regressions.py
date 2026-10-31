"""
AuditBot V13 — Targeted Restart-Safety Regression Tests (`auditbot_cv13_restart_safety_regressions.py`).

Verifies the 5 targeted restart-safety corrections + immediate pre-restart service activity recheck:
1. LIVE->TEST configuration changes must not trigger disruptive restarts while any LIVE broker operation remains unresolved.
2. Emergency STOP must never bypass restart safety checks.
3. Protection attempts in ACKED_UNVERIFIED must be included in restart safety evaluation.
4. Deferred configuration changes must not unexpectedly become active following a later service restart.
5. An identical configuration Save must not restart the service, while genuine credential changes must still be applied and restarted properly (and stopping the service before the restart step transitions to SAVED_FOR_NEXT_START without starting it).
"""

import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock

from src.config import AppConfig, TradingMode, parse_env_dict
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeTracker,
    compute_config_version,
    execute_configuration_update,
)
from src.setup_app import build_env_and_params_dict


class TestCV13RestartSafetyRegressions(unittest.TestCase):
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
                "EQUITY_BUDGET_INR=25000\n"
                "FNO_CAPITAL_PER_TRADE=20000\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "DHAN_PIN=123456\n"
                "DHAN_TOTP_SECRET=JBSWY3DPEHPK3PXP\n"
                "TG_API_ID=123456\n"
                "TG_API_HASH=INITIAL_HASH_ABC\n"
                "TG_BOT_TOKEN=111:INITIAL_BOT_TOK\n"
                "LOG_GROUP_ID=-5514889030\n"
            )

        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump({"is_stopped": False, "in_flight_orders": []}, f)

        con = sqlite3.connect(self.db_path)
        con.execute("CREATE TABLE IF NOT EXISTS order_intents (correlation_id TEXT PRIMARY KEY, state TEXT);")
        con.execute("CREATE TABLE IF NOT EXISTS orders (correlation_id TEXT PRIMARY KEY, status TEXT);")
        con.execute("CREATE TABLE IF NOT EXISTS protection_attempts (attempt_id TEXT PRIMARY KEY, state TEXT);")
        con.commit()
        con.close()

        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)

    def tearDown(self):
        self.tmp_dir.cleanup()
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_01_live_to_test_transition_deferred_when_live_broker_operation_unresolved(self):
        """
        Defect 1: LIVE->TEST configuration changes must not trigger disruptive restarts
        while any LIVE broker operation remains unresolved.
        """
        con = sqlite3.connect(self.db_path)
        con.execute("INSERT INTO orders (correlation_id, status) VALUES ('LIVE-ORD-TRANSIT-1', 'TRANSIT');")
        con.commit()
        con.close()

        restart_mock = MagicMock(return_value=True)
        # Propose switching from LIVE -> TEST while LIVE-ORD-TRANSIT-1 is still in TRANSIT
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,  # Switching LIVE -> TEST
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=25000.0,
            env_path=self.env_path,
        )

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
        self.assertIn("Active broker order in progress", rec.reason)
        restart_mock.assert_not_called()

    def test_02_emergency_stop_never_bypasses_restart_safety_checks(self):
        """
        Defect 2: Emergency STOP must never bypass restart safety checks (even when svc_status is BLOCKED).
        """
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "is_stopped": True,
                    "stopped_by": "Udit",
                    "reason": "Manual emergency halt",
                    "in_flight_orders": [],
                },
                f,
            )

        restart_mock = MagicMock(return_value=True)
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=30000.0,
            env_path=self.env_path,
        )

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

        self.assertEqual(rec.service_status, "BLOCKED")
        self.assertEqual(rec.state, ConfigApplyState.WAITING_FOR_SAFE_APPLY)
        self.assertIn("Emergency STOP active", rec.reason)
        restart_mock.assert_not_called()

    def test_03_acked_unverified_protection_attempt_blocks_restart(self):
        """
        Defect 3: Protection attempts in ACKED_UNVERIFIED must be included in restart safety evaluation.
        """
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT INTO protection_attempts (attempt_id, state) VALUES ('PATM-OCO-UNVERIFIED-1', 'ACKED_UNVERIFIED');"
        )
        con.commit()
        con.close()

        restart_mock = MagicMock(return_value=True)
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=35000.0,
            env_path=self.env_path,
        )

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
        self.assertIn("Protection leg pending or unverified", rec.reason)
        restart_mock.assert_not_called()

    def test_04_deferred_configuration_does_not_activate_on_later_service_restart(self):
        """
        Defect 4: Deferred configuration changes must not unexpectedly become active
        following a later service restart.
        """
        con = sqlite3.connect(self.db_path)
        con.execute("INSERT INTO order_intents (correlation_id, state) VALUES ('INTENT-INFLIGHT-1', 'SUBMITTING');")
        con.commit()
        con.close()

        # Submit a change from EQUITY_BUDGET_INR=25000 (LIVE) -> 45000 (TEST) while intent is SUBMITTING
        env_content, form_params = build_env_and_params_dict(
            is_live_mode=False,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=45000.0,
            env_path=self.env_path,
        )

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

        # Simulate a later service restart loading from self.env_path:
        # Active .env must still have the original active config (LIVE, 25000), NOT the deferred config (TEST, 45000)
        restarted_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
        self.assertEqual(restarted_cfg.trading_mode, TradingMode.LIVE)
        self.assertEqual(restarted_cfg.equity_budget_inr, 25000.0)
        # Deferred config is staged separately
        self.assertTrue(os.path.exists(self.tracker.deferred_env_file))
        self.assertIn("EQUITY_BUDGET_INR=45000", Path(self.tracker.deferred_env_file).read_text(encoding="utf-8"))

    def test_05_identical_save_skips_restart_and_genuine_credential_change_restarts_safely(self):
        """
        Defect 5 + Pre-Restart Recheck:
        - An identical configuration Save must not restart the service.
        - Genuine credential changes (e.g. DHAN_PIN, DHAN_TOTP_SECRET, TG_API_HASH, TG_BOT_TOKEN)
          must still update .env and restart the service when running.
        - If the service is stopped right before the restart step, it must not be started accidentally.
        """
        # 1. Identical configuration Save (with blank secret fields in WebUI preserving existing secrets)
        env_content_same, form_params_same = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            operator_name="Udit",
            instance_name="Audit bot",
            tg_api_id="123456",
            tg_api_hash="",          # Blank -> preserves INITIAL_HASH_ABC
            tg_bot_token="",         # Blank -> preserves 111:INITIAL_BOT_TOK
            log_group_id="-5514889030",
            dhan_client_id="1113731536",
            dhan_pin="",             # Blank -> preserves 123456
            dhan_totp_secret="",     # Blank -> preserves JBSWY3DPEHPK3PXP
            equity_budget_inr=25000.0,
            fno_capital_per_trade=20000.0,
            env_path=self.env_path,
        )

        restart_mock_same = MagicMock(return_value=True)
        rec_same = execute_configuration_update(
            form_params=form_params_same,
            env_content=env_content_same,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock_same,
            check_service_fn=lambda: True,
        )
        self.assertEqual(rec_same.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(rec_same.changes_summary, [])
        restart_mock_same.assert_not_called()

        # 2. Genuine credential change (updating DHAN_PIN and TG_BOT_TOKEN) -> MUST restart service
        env_content_cred, form_params_cred = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            operator_name="Udit",
            instance_name="Audit bot",
            tg_api_id="123456",
            tg_api_hash="",
            tg_bot_token="222:ROTATED_BOT_TOKEN",
            log_group_id="-5514889030",
            dhan_client_id="1113731536",
            dhan_pin="654321",
            dhan_totp_secret="",
            equity_budget_inr=25000.0,
            fno_capital_per_trade=20000.0,
            env_path=self.env_path,
        )

        def restart_and_ack():
            new_cfg = AppConfig.from_env(env=parse_env_dict(self.env_path), env_file="")
            self.tracker.record_active_running_version(
                compute_config_version(new_cfg),
                trading_mode="LIVE",
            )
            return True

        restart_mock_cred = MagicMock(side_effect=restart_and_ack)
        rec_cred = execute_configuration_update(
            form_params=form_params_cred,
            env_content=env_content_cred,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock_cred,
            check_service_fn=lambda: True,
            verify_delay_sec=0.01,
        )
        self.assertEqual(rec_cred.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertFalse(rec_cred.is_token_only_refresh)
        self.assertIn("dhan_pin: [REDACTED_UPDATE]", rec_cred.changes_summary)
        self.assertIn("tg_bot_token: [REDACTED_UPDATE]", rec_cred.changes_summary)
        restart_mock_cred.assert_called_once()

        # 3. Immediate pre-restart service activity recheck:
        #    Service is active on initial check, but stopped before restart step -> SAVED_FOR_NEXT_START, no restart!
        env_content_stop, form_params_stop = build_env_and_params_dict(
            is_live_mode=True,
            equity_active=True,
            fno_active=True,
            dhan_client_id="1113731536",
            equity_budget_inr=38000.0,
            env_path=self.env_path,
        )
        service_states = iter([True, False])
        restart_mock_stop = MagicMock(return_value=True)
        rec_stop = execute_configuration_update(
            form_params=form_params_stop,
            env_content=env_content_stop,
            data_dir=self.data_dir,
            env_path=self.env_path,
            stop_state_path=self.stop_path,
            db_path=self.db_path,
            systemctl_restart_fn=restart_mock_stop,
            check_service_fn=lambda: next(service_states, False),
        )
        self.assertEqual(rec_stop.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        restart_mock_stop.assert_not_called()


if __name__ == "__main__":
    unittest.main()
