"""PR84 Gemini F-03/F-05/F-06/F-08/F-09/F-10 offline acceptance.

All data is synthetic; never call Dhan, Telegram, or systemd.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from src.setup_app import SETUP_CSRF_TOKEN, app
from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, patch_env, revision, validate_effective_config
from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState, ConfigApplyState, write_json_atomic
from src.engine.webui_update import submit_webui_patch

ENV = (
    "TRADING_MODE=TEST\n"
    "DHAN_CLIENT_ID=FAKE_ACCOUNT\n"
    "EQUITY_SL_PCT=3\n"
    "EQUITY_BUDGET_INR=125000.5\n"
    "MIN_TRADE_FUNDS_INR=123456.75\n"
    "TG_API_ID=\nLOG_GROUP_ID=\nTEST_GROUP_ID=\n"
    "CUSTOM_CONFIG_KEY=keep-this\n"
)


class WebUIRegression(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        self.addCleanup(os.chdir, self.cwd)
        self.path = Path(".env")
        self.path.write_text(ENV, encoding="utf-8")
        self.client = TestClient(app)

    def test_f03_optional_blank_telegram_numbers_remain_legal(self):
        validate_effective_config(ENV)
        new, changed = patch_env(ENV.replace("LOG_GROUP_ID=\n", "LOG_GROUP_ID=-100111222\n"),
                                 {"log_group_id": "", "test_group_id": "", "tg_api_id": ""})
        self.assertEqual(changed, {"LOG_GROUP_ID": ("-100111222", "")})
        self.assertIn("LOG_GROUP_ID=\n", new)
        validate_effective_config(new)
        with self.assertRaises(ConfigurationRejected):
            patch_env(ENV, {"max_daily_trades": ""})

    def test_f08_can_repair_invalid_existing_env_without_process_environ_pollution(self):
        invalid = ENV.replace("EQUITY_SL_PCT=3\n", "EQUITY_SL_PCT=0\n")
        self.path.write_text(invalid, encoding="utf-8")
        old_process = os.environ.get("EQUITY_SL_PCT")
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.STOPPED_BY_OWNER, "mocked")), \
             patch.object(ConfigChangeTracker, "get_readonly_broker",
                          side_effect=AssertionError("stopped repair cannot contact broker")):
            response = self.client.post("/setup/submit-async", json={
                "equity_sl_pct": 3.0,
                "expected_revision": revision(invalid),
                "csrf_token": SETUP_CSRF_TOKEN,
                "request_id": "OFFLINE-REPAIR-0001",
                "spawn_worker": False,
            })
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("EQUITY_SL_PCT=3", self.path.read_text())
        self.assertEqual(os.environ.get("EQUITY_SL_PCT"), old_process)

    def test_f09_unknown_sync_form_field_is_rejected(self):
        result = self.client.post("/setup", data={
            "csrf_token": SETUP_CSRF_TOKEN,
            "expected_revision": revision(ENV),
            "unknown_configuration_field": "should-never-accept",
        })
        self.assertEqual(result.status_code, 422)
        self.assertEqual(self.path.read_text(), ENV)

    def test_f10_control_center_is_mounted_and_stopped_amo_save_does_not_use_broker(self):
        self.assertEqual(self.client.get("/control").status_code, 200)
        self.assertEqual(self.client.get("/setup").status_code, 200)
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.STOPPED_BY_OWNER, "mocked")), \
             patch.object(ConfigChangeTracker, "get_readonly_broker",
                          side_effect=AssertionError("No Dhan adapter for stopped AMO setting")):
            body = {
                "enabled": True,
                "csrf_token": SETUP_CSRF_TOKEN,
                "expected_revision": revision(ENV),
                "request_id": "OFFLINE-AMO-TOGGLE-0001",
            }
            result = self.client.post("/control/amo/fno", json=body)
            self.assertEqual(result.status_code, 200, result.text)
            self.assertEqual(result.json()["state"], "SAVED_FOR_NEXT_START")
            self.assertIn("FNO_AMO_ENABLED=true", self.path.read_text())
            repeated = self.client.post("/control/amo/fno", json=body)
            self.assertEqual(repeated.status_code, 200)
            self.assertEqual(repeated.json()["change_id"], result.json()["change_id"])

    def test_f06_safety_enqueued_request_survives_crash_before_returning_receipt(self):
        tracker = ConfigChangeTracker(data_dir=str(Path(self.tmp.name) / "data"))
        accepted = []
        original = tracker.enqueue_durable_configuration_update
        def crash_after_enqueue(**kwargs):
            rec = original(**kwargs)
            accepted.append(rec.change_id)
            raise RuntimeError("synthetic crash AFTER queue and request index are durable")
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.RUNNING, "mocked")), \
             patch.object(tracker, "enqueue_durable_configuration_update", side_effect=crash_after_enqueue):
            with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                submit_webui_patch(tracker, {"equity_active": False}, str(self.path),
                                   expected_revision=revision(ENV),
                                   request_id="F06-IDEMPOTENT-REPLAY-01", spawn_worker=False)
        self.assertEqual(len(accepted), 1)
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          side_effect=AssertionError("retry may not re-enter service state")):
            retried = submit_webui_patch(tracker, {"equity_active": False}, str(self.path),
                                         expected_revision=revision(ENV),
                                         request_id="F06-IDEMPOTENT-REPLAY-01", spawn_worker=False)
        self.assertEqual(retried.change_id, accepted[0])
        self.assertEqual(len(accepted), 1)
        self.assertEqual(self.path.read_text(), ENV)

    def test_f05_coordinator_recovers_prepared_save_before_legacy_timeout(self):
        tracker = ConfigChangeTracker(data_dir=str(Path(self.tmp.name) / "data"))
        target, _ = patch_env(ENV, {"dhan_response_group_name": "-5514889030"})
        change_id = tracker.generate_change_id()
        txn_dir = Path(tracker.data_dir) / "config_field_transactions"
        txn_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(str(txn_dir / (change_id + ".json")), {
            "change_id": change_id, "previous_sha256": revision(ENV),
            "target_sha256": revision(target), "status": "PREPARED",
            "backup_path": str(txn_dir / "original.env"), "env_path": str(self.path.resolve()),
            "policy": "NEXT_START",
        })
        from src.engine.config_manager import ConfigChangeRecord, OperationState
        tracker.save_change_record(ConfigChangeRecord(
            change_id=change_id, state=ConfigApplyState.REQUEST_RECEIVED,
            operation_state=OperationState.REQUESTED.value,
            application_method="SAVE_FOR_CONTROLLED_START",
            requested_file_revision=revision(target),
            requested_at=1.0,
        ))
        self.path.write_text(target, encoding="utf-8")
        tracker.run_control_coordinator_tick(env_path=str(self.path.resolve()),
                                             db_path=str(Path(self.tmp.name) / "missing.db"))
        rec = tracker.get_change_record(change_id)
        self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertFalse(rec.runtime_verified)
        self.assertEqual(self.path.read_text(), target)


class ManualRiskGateRegression(WebUIRegression):
    def test_f02_two_exact_owner_acknowledged_filled_holdings_do_not_block_safe_stop(self):
        from src.journal.db import JournalDB
        import time

        db_path = Path(self.tmp.name) / "data" / "trade_journal.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db = JournalDB(str(db_path), account_id="FAKE_ACCOUNT")
        refs = (
            ("SYNTHETIC-ENTRY-001", "SYN-CORR-001"),
            ("SYNTHETIC-ENTRY-002", "SYN-CORR-002"),
        )
        with db._get_conn() as conn:
            for oid, cid in refs:
                conn.execute(
                    "INSERT INTO order_intents "
                    "(correlation_id, account_id, symbol, security_id, exchange_segment, "
                    "product_type, order_type, price, quantity, state, broker_order_id, "
                    "created_at, updated_at) "
                    "VALUES (?, 'FAKE_ACCOUNT', 'YESBANK', '11915', 'NSE_EQ', "
                    "'CNC', 'LIMIT', 15.6, 1, 'TRADED', ?, ?, ?)",
                    (cid, oid, time.time(), time.time()),
                )
                conn.execute(
                    "INSERT INTO orders "
                    "(correlation_id, broker_order_id, trading_mode, product_type, "
                    "exchange_segment, symbol, security_id, order_type, price, "
                    "quantity, filled_quantity, status, created_at, updated_at) "
                    "VALUES (?, ?, 'LIVE', 'CNC', 'NSE_EQ', 'YESBANK', '11915', "
                    "'LIMIT', 15.6, 1, 1, 'TRADED', ?, ?)",
                    (cid, oid, time.time(), time.time()),
                )
                conn.execute(
                    "INSERT INTO watcher_orders "
                    "(order_id, is_terminal, record_json, updated_at) VALUES (?, 0, ?, ?)",
                    (oid, json.dumps({
                        "order_id": oid, "correlation_id": cid, "security_id": "11915",
                        "symbol": "YESBANK-EQ", "segment": "NSE_EQ", "product_type": "CNC",
                        "state": "TRADED", "filled_qty": 1, "protected_qty": 0,
                    }), time.time()),
                )
        for oid, cid in refs:
            db.acknowledge_manual_risk_holding(
                parent_order_id=oid, correlation_id=cid, account_id="FAKE_ACCOUNT",
                symbol="YESBANK", security_id="11915", quantity=1,
                acknowledged_by="SYNTHETIC_OWNER", reason="Controlled test holding",
            )
        tracker = ConfigChangeTracker(data_dir=str(db_path.parent))
        with patch.object(tracker, "get_readonly_broker", return_value=None):
            ok, reason, code, count = tracker.evaluate_trading_safety_gate_detailed(
                db_path=str(db_path), stop_state_path=str(db_path.parent / "emergency_stop.json"),
                env_path=str(self.path.resolve()), is_service_running=True,
                is_live_mode=True, target_is_live=False,
            )
        self.assertTrue(ok, (reason, code, count))

        # Changing just one share's filled quantity invalidates the waiver,
        # and must fail closed rather than crediting another parent's ack.
        with db._get_conn() as conn:
            conn.execute("UPDATE orders SET filled_quantity=2 WHERE broker_order_id=?", (refs[0][0],))
        with patch.object(tracker, "get_readonly_broker", return_value=None):
            ok, reason, code, count = tracker.evaluate_trading_safety_gate_detailed(
                db_path=str(db_path), stop_state_path=str(db_path.parent / "emergency_stop.json"),
                env_path=str(self.path.resolve()), is_service_running=True,
                is_live_mode=True, target_is_live=True,
            )
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
