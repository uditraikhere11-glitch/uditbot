"""Offline retirement regression: old config is never replayed or erased.

No Dhan, Telegram, systemd or production data are accessed.
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.engine.config_manager import ConfigChangeTracker


class RetiredSettingsOwnerStopTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name)
        self.tracker = ConfigChangeTracker(data_dir=str(self.data))
        self.owner = self.data / "service_control_state.json"
        self.pending = self.data / "pending_config_operation.json"
        self.deferred = self.data / "deferred_config.env"
        self.meta = self.data / "deferred_config_meta.json"
        self.pending.write_text('{"status": "QUEUED", "env_content": "TRADING_MODE=LIVE"}')
        self.deferred.write_text('TRADING_MODE=LIVE\n')
        self.meta.write_text('{"change_id": "OLD"}')

    def _owner(self, deferred=False, desired="RUNNING"):
        self.owner.write_text(json.dumps({
            "desired_service_state": desired, "observed_state": "RUNNING",
            "deferred_stop_pending": deferred,
        }))

    def test_historical_queued_update_is_not_replayed_or_removed(self):
        self._owner(False)
        original = [(f, f.read_bytes()) for f in (self.pending, self.deferred, self.meta)]
        with mock.patch.object(self.tracker, "try_auto_complete_deferred_stop", side_effect=AssertionError("No owner STOP")):
            status = self.tracker.run_control_coordinator_tick()
        self.assertIsNone(status["queued_executed"])
        self.assertIsNone(status["resumed_deferred"])
        for p, before in original:
            self.assertEqual(p.read_bytes(), before)

    def test_stale_historical_state_never_awakens_worker(self):
        self._owner(False)
        self.assertFalse(self.tracker._has_active_or_deferred_work())

    def test_owner_stop_tick_does_not_touch_pending_config(self):
        self._owner(True, "STOPPED")
        original = [(f, f.read_bytes()) for f in (self.pending, self.deferred, self.meta)]
        with mock.patch.object(self.tracker, "try_auto_complete_deferred_stop", return_value={"ok": True}) as stop:
            status = self.tracker.run_control_coordinator_tick()
        stop.assert_called_once()
        self.assertEqual(status["completed_stop"], {"ok": True})
        for p, before in original:
            self.assertEqual(p.read_bytes(), before)

    def test_watcher_marks_only_owner_stop(self):
        self._owner(True, "STOPPED")
        before = [(f, f.read_bytes()) for f in (self.pending, self.deferred, self.meta)]
        self.assertTrue(self.tracker.record_watcher_obligations_resolved())
        self.assertTrue(json.loads(self.owner.read_text())["stop_ready_to_apply"])
        for p, old in before:
            self.assertEqual(p.read_bytes(), old)

    def test_watcher_does_not_mark_old_config_without_owner_stop(self):
        self._owner(False)
        self.assertFalse(self.tracker.record_watcher_obligations_resolved())
        self.assertEqual(json.loads(self.owner.read_text())["deferred_stop_pending"], False)

    def test_corrupt_owner_record_never_advances(self):
        self.owner.write_text('{')
        self.assertFalse(self.tracker.record_watcher_obligations_resolved())
        self.assertFalse(self.tracker._has_active_or_deferred_work())

    def test_wrong_owner_desired_state_cannot_autostop(self):
        self._owner(True, "RUNNING")
        self.assertFalse(self.tracker._has_active_or_deferred_work())
        with mock.patch.object(self.tracker, "control_service", side_effect=AssertionError("Not authorized")):
            self.assertIsNone(self.tracker.try_auto_complete_deferred_stop())

    def test_owner_stop_completed_only_by_existing_control_service(self):
        self._owner(True, "STOPPED")
        with mock.patch.object(self.tracker, "control_service", return_value={"ok": False,"deferred": True}) as action:
            result = self.tracker.try_auto_complete_deferred_stop(env_path="test.env")
        self.assertTrue(action.called)
        self.assertEqual(action.call_args.kwargs["action"], "stop")
        self.assertEqual(result["deferred"], True)

    def test_old_writer_methods_are_gone(self):
        for name in (
            "try_auto_resume_deferred_update", "enqueue_durable_configuration_update",
            "reconcile_stale_control_records", "save_deferred_metadata",
        ):
            self.assertFalse(hasattr(self.tracker, name), name)


if __name__ == "__main__":
    unittest.main()
