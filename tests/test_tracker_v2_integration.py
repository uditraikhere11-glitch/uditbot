"""Actual WebUI V2 route guard tests, with NO network or systemd calls."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, revision
from src.engine.tracker_v2_integration import submit_v2_webui_patch

OLD = "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\n"


class WebuiV2IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / "data"
        self.data.mkdir(mode=0o700)
        self.env = self.root / ".env"
        self.env.write_text(OLD, encoding="utf8")
        self.tracker = ConfigChangeTracker(data_dir=str(self.data))
        self.env_patch = patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "1",
                                                "AUDITBOT_TEST_ISOLATION": "1"})
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.notifier = patch.object(self.tracker, "dispatch_telegram_update")
        self.notifier.start()
        self.addCleanup(self.notifier.stop)

    def submit(self, **kw):
        values = dict(tracker=self.tracker, form_data={"equity_budget_inr": "1000"},
                      env_path=str(self.env), expected_revision=revision(OLD),
                      request_id="request-0001")
        values.update(kw)
        return submit_v2_webui_patch(**values)

    def test_flag_disabled_never_edits_env(self):
        with patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "0"}):
            with self.assertRaises(ConfigurationConflict):
                self.submit()
        self.assertEqual(OLD, self.env.read_text())

    def test_invalid_request_id_blocks(self):
        with self.assertRaises(ConfigurationConflict):
            self.submit(request_id="x")
        self.assertEqual(OLD, self.env.read_text())

    def test_invalid_revision_blocks(self):
        with self.assertRaises(ConfigurationConflict):
            self.submit(expected_revision="a"*64)
        self.assertEqual(OLD, self.env.read_text())

    def test_stopped_service_not_started(self):
        with patch.object(self.tracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.STOPPED_OTHER, "stopped")):
            rec = self.submit()
        self.assertFalse(rec.runtime_verified)
        self.assertFalse(rec.restart_performed)
        self.assertIn("1000", self.env.read_text())
        self.assertEqual("SAVED_FOR_NEXT_START", rec.state.value)

    def test_running_without_real_ack_blocks_before_writing(self):
        with patch.object(self.tracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.RUNNING, "running")):
            with patch.object(self.tracker, "is_active_version_confirmed",
                              return_value=(False, None, "ACK missing")):
                with self.assertRaises(ConfigurationConflict):
                    self.submit()
        self.assertEqual(OLD, self.env.read_text())

    def test_owner_stop_priority(self):
        self.tracker.record_service_control_intent(
            desired_service_state="STOPPED", action="OWNER_STOP", observed_state="STOPPED_BY_OWNER",
        )
        with self.assertRaises(ConfigurationConflict):
            self.submit()
        self.assertEqual(OLD, self.env.read_text())

    def test_live_requires_explicit_confirmation(self):
        with self.assertRaises(ConfigurationRejected):
            self.submit(form_data={"trading_mode": "LIVE"})
        self.assertEqual(OLD, self.env.read_text())


if __name__ == "__main__":
    unittest.main()
