"""Offline acceptance for the streamlined one-click Save & Apply handler."""
import os
import tempfile
import unittest
from pathlib import Path

from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, revision
from src.engine.simple_save_apply import apply_settings


class SimpleSaveApplyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = Path(self.tmp.name) / ".env"
        self.env.write_text(
            "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\nDHAN_CLIENT_ID=TEST_ACCOUNT\n",
            encoding="utf-8",
        )
        os.chmod(self.env, 0o600)
        self.restarts = 0

    def restart(self):
        self.restarts += 1

    def save(self, values, *, state="inactive", rev=None, live=False, restart=None):
        return apply_settings(
            values, env_path=self.env,
            expected_revision=rev or revision(self.env.read_text()),
            confirm_live=live, state_reader=lambda: state,
            restart=restart or self.restart,
        )

    def test_stopped_save_does_not_start_or_restart(self):
        result = self.save({"equity_budget_inr": "750"}, state="inactive")
        self.assertEqual(result.status, "SAVED_STOPPED")
        self.assertFalse(result.restart_performed)
        self.assertEqual(self.restarts, 0)
        self.assertIn("EQUITY_BUDGET_INR=750", self.env.read_text())
        self.assertEqual(self.env.stat().st_mode & 0o777, 0o600)

    def test_running_change_requests_exactly_one_restart(self):
        result = self.save({"equity_budget_inr": "800"}, state="active")
        self.assertTrue(result.restart_performed)
        self.assertEqual(result.status, "RESTARTED")
        self.assertEqual(self.restarts, 1)
        self.assertEqual(result.changed_keys, ("EQUITY_BUDGET_INR",))

    def test_no_change_never_restarts(self):
        result = self.save({"equity_budget_inr": "500"}, state="active")
        self.assertEqual(result.status, "UNCHANGED")
        self.assertEqual(self.restarts, 0)

    def test_revision_conflict_keeps_settings(self):
        before = self.env.read_text()
        with self.assertRaises(ConfigurationConflict):
            self.save({"equity_budget_inr": "800"}, rev="1" * 64, state="active")
        self.assertEqual(self.env.read_text(), before)
        self.assertEqual(self.restarts, 0)

    def test_invalid_field_keeps_settings(self):
        before = self.env.read_text()
        with self.assertRaises(ConfigurationRejected):
            self.save({"unsafe_unknown_field": "1"}, state="active")
        self.assertEqual(self.env.read_text(), before)

    def test_test_to_live_requires_explicit_confirmation(self):
        before = self.env.read_text()
        with self.assertRaises(ConfigurationRejected):
            self.save({"is_live_mode": "true"}, state="active")
        self.assertEqual(self.env.read_text(), before)
        self.assertEqual(self.restarts, 0)

    def test_explicit_live_change_can_be_saved_while_stopped(self):
        result = self.save({"is_live_mode": "true"}, state="inactive", live=True)
        self.assertEqual(result.status, "SAVED_STOPPED")
        self.assertIn("TRADING_MODE=LIVE", self.env.read_text())
        self.assertEqual(self.restarts, 0)

    def test_unknown_service_state_does_not_write_or_restart(self):
        before = self.env.read_text()
        with self.assertRaises(ConfigurationConflict):
            self.save({"equity_budget_inr": "900"}, state="activating")
        self.assertEqual(self.env.read_text(), before)
        self.assertEqual(self.restarts, 0)

    def test_owner_stop_winning_race_is_never_restarted(self):
        readings = iter(("active", "inactive"))
        result = apply_settings(
            {"equity_budget_inr": "801"}, env_path=self.env,
            expected_revision=revision(self.env.read_text()),
            state_reader=lambda: next(readings), restart=self.restart,
        )
        self.assertEqual(result.status, "SAVED_STOPPED")
        self.assertEqual(self.restarts, 0)

    def test_real_systemd_restart_only_targets_already_running_service(self):
        from src.engine import simple_save_apply as module
        from unittest.mock import patch
        from subprocess import CompletedProcess
        with patch.object(module.subprocess, "run",
                          return_value=CompletedProcess([], 0, "", "")) as called:
            module.restart_trading_service()
        self.assertEqual(
            called.call_args.args[0],
            ["sudo", "-n", "/usr/bin/systemctl", "try-restart", "trading_bot.service"],
        )

    def test_restart_exception_does_not_falsely_claim_success(self):
        def failed_restart():
            raise ConfigurationConflict("systemctl failed")
        with self.assertRaises(ConfigurationConflict):
            self.save({"equity_budget_inr": "900"}, state="active", restart=failed_restart)
        self.assertIn("EQUITY_BUDGET_INR=900", self.env.read_text())

    def test_old_v2_transaction_is_not_written_by_save(self):
        journal = self.env.parent / "config_restart_v2" / "current_transaction.json"
        journal.parent.mkdir()
        journal.write_text('{"phase":"ACTION_REQUIRED"}')
        result = self.save({"equity_budget_inr": "800"}, state="inactive")
        self.assertTrue(result.saved)
        self.assertEqual(journal.read_text(), '{"phase":"ACTION_REQUIRED"}')

    def test_form_uses_only_single_save_apply_endpoint(self):
        source = (Path(__file__).parents[1] / "src" / "setup_app.py").read_text()
        template = source.split('HTML_TEMPLATE = """', 1)[1].split('"""', 1)[0]
        self.assertIn("Save & Apply", template)
        self.assertIn("fetch('/setup/save-apply'", template)
        self.assertNotIn("Configuration Change Tracker", template)
        self.assertNotIn("startPolling(", template)
        self.assertNotIn("CFG-PENDING", template)

if __name__ == "__main__":
    unittest.main()
