"""WEBUI-R01: fully offline, field-scoped configuration acceptance tests.

No real broker, Telegram, systemctl or network resources are used.
"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, ENV_REGISTRY, REGISTRY,
    classify, normalize, parse_env_strict, patch_env, redact, revision,
    validate_effective_config,
)
from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
from src.engine.webui_update import submit_webui_patch


BASE = (
    "# Existing production-like configuration\n"
    "TRADING_MODE=LIVE\n"
    "EQUITY_ACTIVE=true\n"
    "FNO_ACTIVE=false\n"
    "DHAN_RESPONSE_GROUP_NAME=-1234567890  # old destination\n"
    "DHAN_PIN=7261\n"
    "DHAN_TOTP_SECRET=MYPRIVATESECRET\n"
    "TG_API_HASH=FAKESECRET0123456\n"
    "DHAN_CLIENT_ID=NOT_A_REAL_ACCOUNT\n"
    "CUSTOM_UNKNOWN_KEY=untouched \n"
    "# Preserve operator note exactly\n"
)


class ConfigSchemaReliabilityTests(unittest.TestCase):
    def test_r01_registry_has_every_writable_setting(self):
        self.assertEqual(len(REGISTRY), 51)
        self.assertEqual(len(ENV_REGISTRY), 51)
        for name, spec in REGISTRY.items():
            self.assertEqual(spec.form_name, name)
            self.assertIn(spec.policy, {"HOT_NOTIFICATION", "NEXT_START", "FUTURE_ENTRY", "SAFETY_CONTROLLED"})
            self.assertTrue(spec.consumer)
            self.assertTrue(spec.verifier)

    def test_ac01_only_destination_changes_preserving_all_other_bytes(self):
        new, changes = patch_env(BASE, {"dhan_response_group_name": "-5514889030"})
        self.assertEqual(set(changes), {"DHAN_RESPONSE_GROUP_NAME"})
        self.assertEqual(new.replace("DHAN_RESPONSE_GROUP_NAME=-5514889030", "DHAN_RESPONSE_GROUP_NAME=-1234567890  # old destination"), BASE)
        old_kv, _ = parse_env_strict(BASE)
        new_kv, _ = parse_env_strict(new)
        for k, v in old_kv.items():
            if k != "DHAN_RESPONSE_GROUP_NAME":
                self.assertEqual(new_kv.get(k), v)
        self.assertEqual(classify(changes), "NEXT_START")

    def test_ac03_field_types_reject_nan_bool_as_int_unexpected_enum(self):
        for req in (
            {"equity_budget_inr": "nan"},
            {"fno_max_lots": "3.7"},
            {"fno_max_lots": True},
            {"equity_active": "sometimes"},
            {"equity_sizing_mode": "SOMETHING_ELSE"},
            {"dhan_response_group_name": "new\nINJECTED=1"},
            {"dhan_access_token": "NOT_ALLOWED"},
            {"made_up_setting": "x"},
        ):
            with self.subTest(req=req), self.assertRaises(ConfigurationRejected):
                patch_env(BASE, req)

    def test_ac03_each_setting_invalid_type_and_preserves_unrelated(self):
        for name, spec in REGISTRY.items():
            with self.subTest(field=name):
                if spec.value_type in {"number", "integer"}:
                    invalid = "not-a-number"
                elif spec.value_type in {"bool", "mode", "enum"}:
                    invalid = "not-supported"
                elif spec.value_type == "groups":
                    invalid = "a-bad-group"
                elif spec.value_type == "response":
                    invalid = "x\nMODIFY=1"
                else:
                    invalid = "x\nINJECTED=1"
                with self.assertRaises(ConfigurationRejected):
                    patch_env(BASE, {name: invalid})

    def test_ac03_same_value_does_not_write_unrelated_defaults(self):
        original = "EQUITY_ACTIVE=true\nTRADING_MODE=TEST\n"
        new, diff = patch_env(original, {"equity_active": True, "is_live_mode": False})
        self.assertEqual(new, original)
        self.assertEqual(diff, {})

    def test_ac09_optimistic_revision_and_duplicate_keys(self):
        with self.assertRaises(ConfigurationConflict):
            patch_env(BASE, {"dhan_response_group_name": "-5514889030"}, expected_revision="bad")
        for content in (BASE + "dhan_response_group_name=overridden\n", "FLAG=x\nflag=y\n"):
            with self.assertRaises(ConfigurationConflict):
                patch_env(content, {"operator_name": "test"})

    def test_ac14_secrets_redacted(self):
        for key in ("DHAN_PIN", "TG_API_HASH", "DHAN_TOTP_SECRET", "TG_BOT_TOKEN"):
            self.assertEqual(redact(key, "canary-secret"), "[REDACTED]")
        self.assertEqual(redact("DHAN_RESPONSE_GROUP_NAME", "-5514889030"), "-5514889030")

    def test_ac17_unknown_custom_key_and_crlf_preserved(self):
        original = "# custom\r\nUNKNOWN_FIELD=z\r\nDHAN_RESPONSE_GROUP_NAME=old\r\n"
        new, _ = patch_env(original, {"dhan_response_group_name": "-5514889030"})
        self.assertEqual(new, "# custom\r\nUNKNOWN_FIELD=z\r\nDHAN_RESPONSE_GROUP_NAME=-5514889030\r\n")

    def test_ac03_existing_invalid_mode_fails_closed_on_validation(self):
        with self.assertRaises(ConfigurationRejected):
            validate_effective_config("TRADING_MODE=MAYBE\n")


class ConfigurationCoordinatorReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, ".env")
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(BASE)
        self.tracker = ConfigChangeTracker(data_dir=os.path.join(self.tmp.name, "data"))

    def _submit(self, patch, req_id="DEMO-TRANSACTION-0001", service=ServiceOperatingState.RUNNING, **extras):
        with mock.patch.object(ConfigChangeTracker, "resolve_service_operating_state", return_value=(service, "mocked")), \
             mock.patch.object(ConfigChangeTracker, "get_readonly_broker", side_effect=AssertionError("Broker must not be used")):
            return submit_webui_patch(
                self.tracker, patch, self.path, expected_revision=revision(Path(self.path).read_text(encoding="utf-8")),
                request_id=req_id, spawn_worker=False, **extras,
            )

    def test_ac01_ac02_notification_does_not_use_broker_or_restart(self):
        orig = Path(self.path).read_text(encoding="utf-8")
        rec = self._submit({"dhan_response_group_name": "-5514889030"})
        self.assertEqual(rec.state.value, "SAVED_FOR_NEXT_START")
        self.assertFalse(rec.runtime_verified)
        self.assertFalse(rec.restart_performed)
        self.assertFalse(rec.broker_checks_required)
        self.assertEqual(rec.changed_fields, ["DHAN_RESPONSE_GROUP_NAME"])
        self.assertEqual(rec.application_method, "SAVE_FOR_CONTROLLED_START")
        now = Path(self.path).read_text(encoding="utf-8")
        self.assertEqual(now.replace("DHAN_RESPONSE_GROUP_NAME=-5514889030", "DHAN_RESPONSE_GROUP_NAME=-1234567890  # old destination"), orig)

    def test_ac09_stale_form_conflict_and_no_write(self):
        old = Path(self.path).read_text(encoding="utf-8")
        with mock.patch.object(ConfigChangeTracker, "resolve_service_operating_state", return_value=(ServiceOperatingState.RUNNING, "mocked")):
            with self.assertRaises(ConfigurationConflict):
                submit_webui_patch(self.tracker, {"dhan_response_group_name": "-5514889030"}, self.path,
                                   expected_revision="stale", request_id="STALE-REQUEST-0001", spawn_worker=False)
        self.assertEqual(Path(self.path).read_text(encoding="utf-8"), old)

    def test_ac10_same_request_cannot_replay(self):
        request = {"dhan_response_group_name": "-5514889030"}
        rec1 = self._submit(request)
        rec2 = self._submit(request)
        self.assertEqual(rec1.change_id, rec2.change_id)
        with self.assertRaises(ConfigurationConflict):
            self._submit({"dhan_response_group_name": "-5514889031"})

    def test_ac04_stopped_save_never_starts(self):
        rec = self._submit({"dhan_response_group_name": "-5514889030"}, service=ServiceOperatingState.STOPPED_BY_OWNER)
        self.assertEqual(rec.state.value, "SAVED_FOR_NEXT_START")
        self.assertFalse(rec.restart_performed)

    def test_ac08_recovery_without_network(self):
        self._submit({"dhan_response_group_name": "-5514889030"})
        from src.engine.webui_update import recover_field_transactions
        self.assertEqual(recover_field_transactions(self.tracker), {})


if __name__ == "__main__":
    unittest.main()
