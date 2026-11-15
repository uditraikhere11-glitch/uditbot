"""WEBUI-R02: broker-network-isolated transactional configuration integration acceptance.

Use only synthetic .env, SQLite, tracker files and mocks; these tests never
start systemd, call Dhan, or send Telegram messages.
"""
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, REGISTRY, patch_env, revision,
    validate_effective_config,
)
from src.engine.config_manager import (
    ConfigApplyState, ConfigChangeRecord, ConfigChangeTracker,
    OperationState, ServiceOperatingState, write_env_atomic,
)
from src.engine.webui_update import (
    recover_field_transactions, recover_legacy_notification_deferred,
    submit_webui_patch,
)

BASE = (
    "# Synthetic UditBot config; no production credentials.\n"
    "TRADING_MODE=LIVE\n"
    "EQUITY_ACTIVE=true\n"
    "FNO_ACTIVE=true\n"
    "DHAN_RESPONSE_GROUP_NAME=-100551111111\n"
    "DHAN_CLIENT_ID=FAKE_ACCOUNT\n"
    "DHAN_PIN=FAKE_PIN\n"
    "TG_API_HASH=FAKE_API_HASH\n"
    "DHAN_TOTP_SECRET=FAKE_TOTP\n"
    "EQUITY_BUDGET_INR=500\n"
    "CUSTOM_SETTING=preserve-me\n"
)


class FieldRegistryAudit(unittest.TestCase):
    def test_ac03_every_registered_field_valid_and_isolated(self):
        from src.setup_app import HTML_TEMPLATE
        from src.control_center_ui import render_control_center
        self.assertEqual(len(REGISTRY), 51)
        for form_name, spec in REGISTRY.items():
            with self.subTest(form=form_name):
                if spec.value_type == "mode":
                    value = "TEST"
                elif spec.value_type == "enum":
                    value = spec.choices[0]
                elif spec.value_type == "bool":
                    value = "false"
                elif spec.value_type == "integer":
                    value = "1"
                elif spec.value_type == "number":
                    value = "1"
                elif spec.value_type == "groups":
                    value = "-100111999"
                elif spec.value_type == "response":
                    value = "-5514889030"
                else:
                    value = "SYNTHETIC_OWNER_VALUE"
                new, changed = patch_env(BASE, {form_name: value})
                self.assertIn("CUSTOM_SETTING=preserve-me", new)
                self.assertIn("DHAN_PIN=FAKE_PIN", new) if spec.key != "DHAN_PIN" else None
                self.assertTrue(set(changed).issubset({spec.key}))
                validate_effective_config(new)
                if form_name not in ("equity_amo_enabled", "fno_amo_enabled"):
                    self.assertIn('name="' + form_name + '"', HTML_TEMPLATE)

    def test_ac14_dom_never_injects_change_descriptions_as_html(self):
        from src.setup_app import HTML_TEMPLATE
        self.assertIn("changesList.replaceChildren()", HTML_TEMPLATE)
        self.assertIn("row.textContent = '• ' + String(change)", HTML_TEMPLATE)
        self.assertNotIn("rec.changes_summary.map(c => '<div>• ' + c", HTML_TEMPLATE)


class CoordinatorIntegrationAudit(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env_path = str(Path(self.temp.name) / ".env")
        Path(self.env_path).write_text(BASE, encoding="utf-8")
        self.tracker = ConfigChangeTracker(data_dir=str(Path(self.temp.name) / "data"))

    def _save(self, data, request_id="TEST-REQ-00000001", mode=ServiceOperatingState.RUNNING):
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(mode, "synthetic")), \
             patch.object(ConfigChangeTracker, "get_readonly_broker",
                          side_effect=AssertionError("notification save must not read broker")):
            return submit_webui_patch(
                self.tracker, data, self.env_path, request_id=request_id,
                expected_revision=revision(Path(self.env_path).read_text(encoding="utf-8")),
                spawn_worker=False,
            )

    def test_ac01_ac07_notification_saved_not_falsely_verified(self):
        rec = self._save({"dhan_response_group_name": "-5514889030"})
        self.assertEqual(rec.changed_fields, ["DHAN_RESPONSE_GROUP_NAME"])
        self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertFalse(rec.runtime_verified)
        self.assertFalse(rec.restart_performed)
        self.assertFalse(rec.broker_checks_required)
        self.assertTrue(rec.application_method.startswith("SAVE_FOR_"))
        self.assertEqual(Path(self.env_path).read_text(encoding="utf-8").count("DHAN_RESPONSE_GROUP_NAME="), 1)

    def test_ac02_ac11_yesbank_legacy_notification_recovery_no_broker_or_journal_edit(self):
        sqlite_path = Path(self.temp.name) / "data" / "synthetic_yesbank.db"
        con = sqlite3.connect(sqlite_path)
        con.execute("CREATE TABLE orders (broker_order_id TEXT, status TEXT, acknowledgment TEXT)")
        con.executemany("INSERT INTO orders VALUES (?, ?, ?)", [
            ("221261102657708", "TRADED", "UNPROTECTED_MANUAL_RISK"),
            ("23126110439808", "TRADED", "UNPROTECTED_MANUAL_RISK")
        ])
        con.commit()
        con.close()
        before = sqlite_path.read_bytes()
        # Legacy staged env differs only in the final response group, not secrets.
        staged, changed = patch_env(BASE, {"dhan_response_group_name": "-5514889030"})
        self.assertEqual(set(changed), {"DHAN_RESPONSE_GROUP_NAME"})
        write_env_atomic(self.tracker.deferred_env_file, staged)
        rec = ConfigChangeRecord(
            change_id="CFG-20261115-081814-EEF65F",
            state=ConfigApplyState.WAITING_FOR_SAFE_APPLY,
            operation_state=OperationState.WAITING_FOR_ORDER.value,
            progress_percent=75, blocked_reason="UNPROTECTED_POSITION",
            previous_mode="LIVE", requested_mode="LIVE",
        )
        self.tracker.save_change_record(rec)
        self.tracker.save_deferred_metadata({
            "change_id": rec.change_id, "env_path": self.env_path,
            "db_path": str(sqlite_path),
        })
        with patch.object(ConfigChangeTracker, "evaluate_trading_safety_gate_detailed",
                          side_effect=AssertionError("NO broker checks for group change")), \
             patch.object(ConfigChangeTracker, "get_readonly_broker",
                          side_effect=AssertionError("NO Dhan GET")):
            self.assertTrue(recover_legacy_notification_deferred(self.tracker, self.env_path))
        after = self.tracker.get_change_record(rec.change_id)
        self.assertEqual(after.state, ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertFalse(after.runtime_verified)
        self.assertEqual(sqlite_path.read_bytes(), before)
        self.assertFalse(Path(self.tracker.deferred_env_file).exists())

    def test_ac06_confirmation_required_before_live_transition(self):
        path = Path(self.env_path)
        original = path.read_text(encoding="utf-8")
        path.write_text(original.replace("TRADING_MODE=LIVE", "TRADING_MODE=TEST"), encoding="utf-8")
        with self.assertRaises(ConfigurationRejected):
            self._save({"is_live_mode": True}, request_id="TEST-REQ-00000002")
        self.assertIn("TRADING_MODE=TEST", path.read_text(encoding="utf-8"))

    def test_ac04_uncertain_service_refuses_save(self):
        for state in (ServiceOperatingState.UNKNOWN, ServiceOperatingState.STARTING,
                      ServiceOperatingState.STOPPING):
            with self.subTest(state=state), self.assertRaises(ConfigurationConflict):
                self._save({"dhan_response_group_name": "-5514889030"},
                           request_id="TEST-REQ-STATE-" + state.value, mode=state)

    def test_ac08_failed_atomic_write_keeps_previous_config(self):
        import src.engine.webui_update as candidate
        original = Path(self.env_path).read_text(encoding="utf-8")
        original_write = candidate.write_env_atomic
        def fail_target(path, content):
            if os.path.abspath(path) == os.path.abspath(self.env_path):
                raise OSError("Injected disk-write failure")
            return original_write(path, content)
        with patch.object(candidate, "write_env_atomic", side_effect=fail_target):
            with self.assertRaises(ConfigurationConflict):
                self._save({"dhan_response_group_name": "-5514889030"})
        self.assertEqual(Path(self.env_path).read_text(encoding="utf-8"), original)

    def test_ac09_simultaneous_stale_revision_never_loses_update(self):
        previous = revision(Path(self.env_path).read_text(encoding="utf-8"))
        results = []
        start = threading.Barrier(2)
        def worker(index, field, value):
            start.wait()
            try:
                with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                                  return_value=(ServiceOperatingState.RUNNING, "synthetic")):
                    rec = submit_webui_patch(
                        self.tracker, {field: value}, self.env_path,
                        expected_revision=previous,
                        request_id="TEST-RACE-0000000" + str(index),
                        spawn_worker=False,
                    )
                    results.append(("ok", rec.change_id))
            except ConfigurationConflict:
                results.append(("conflict", index))
        threads = [
            threading.Thread(target=worker, args=(1, "operator_name", "A_TEST")),
            threading.Thread(target=worker, args=(2, "dhan_response_group_name", "-5514889030")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertCountEqual([x[0] for x in results], ["ok", "conflict"])

    def test_ac12_ac18_without_telegram_ack_do_not_claim_runtime(self):
        rec = self._save({"dhan_response_group_name": "-5514889030"})
        self.assertFalse(rec.runtime_verified)
        self.assertEqual(rec.application_method, "SAVE_FOR_CONTROLLED_START")
        self.assertIn("running settings remain active", rec.reason.lower())

    def test_ac13_governor_is_never_touched_for_destination_save(self):
        from src.broker.read_governor import ReadGovernor
        with patch.object(ReadGovernor, "admit", side_effect=AssertionError("NO governor admission")):
            self._save({"dhan_response_group_name": "-5514889030"})

    def test_ac10_repeat_client_request_no_new_change(self):
        data = {"dhan_response_group_name": "-5514889030"}
        first = self._save(data)
        second = self._save(data)
        self.assertEqual(first.change_id, second.change_id)


if __name__ == "__main__":
    unittest.main()
