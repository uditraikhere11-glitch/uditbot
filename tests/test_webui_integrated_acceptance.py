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
        self.assertIn('id="simpleSaveStatus"', HTML_TEMPLATE)
        self.assertIn("toastMessage.textContent = String(value)", HTML_TEMPLATE)
        self.assertIn("result.status === 'RESTARTED' && result.restart_performed === true", HTML_TEMPLATE)
        self.assertIn("fetch('/setup/save-apply'", HTML_TEMPLATE)
        self.assertNotIn("changesList.replaceChildren()", HTML_TEMPLATE)
        self.assertNotIn("modalApplicationFacts", HTML_TEMPLATE)
        self.assertNotIn("rec.changes_summary.map(c => '<div>• ' + c", HTML_TEMPLATE)




if __name__ == "__main__":
    unittest.main()
