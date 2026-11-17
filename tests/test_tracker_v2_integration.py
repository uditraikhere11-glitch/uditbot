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
            desired_service_state="STOPPED", last_action="OWNER_STOP", reason="offline test", observed_state="STOPPED_BY_OWNER",
        )
        with self.assertRaises(ConfigurationConflict):
            self.submit()
        self.assertEqual(OLD, self.env.read_text())

    def test_live_requires_explicit_confirmation(self):
        with self.assertRaises(ConfigurationRejected):
            self.submit(form_data={"trading_mode": "LIVE"})
        self.assertEqual(OLD, self.env.read_text())

    def _simulate_running_v2(self, *, restart_result: bool = False):
        """An actual live-PID handoff, with only the broker GET gate stubbed."""
        import os
        import multiprocessing
        from src.config import AppConfig
        from src.engine.config_manager import compute_config_version, parse_env_content_string
        from src.engine.tracker_credentials import fingerprint
        from src.engine.broker_handoff import register_producer, _paths

        original = OLD + "DHAN_CLIENT_ID=DEMOACCOUNT\n"
        self.env.write_text(original)
        cfg = AppConfig.from_env(env=parse_env_content_string(original), env_file="")
        register_producer(self.data, cfg.dhan_client_id)
        old_fp = fingerprint(cfg, str(self.data))
        self.tracker.record_active_running_version(
            compute_config_version(cfg), trading_mode="TEST",
            credential_fingerprint=old_fp,
        )
        self.assertEqual(os.getpid(), self.tracker.get_active_running_version()["pid"])
        holder = {"pid": os.getpid(), "calls": 0}
        release = multiprocessing.Event()
        child = None

        def child_runner(data_dir, env_path, ready, release_event):
            from src.config import AppConfig, parse_env_dict
            from src.engine.config_manager import ConfigChangeTracker, compute_config_version
            from src.engine.tracker_credentials import fingerprint
            from src.engine.broker_handoff import register_producer
            import os
            try:
                # A child forked by the full test suite inherits unrelated
                # os.environ values from earlier tests. Model the isolated
                # systemd service's actual .env snapshot, not test pollution.
                cfg_new = AppConfig.from_env(env=parse_env_dict(env_path), env_file="")
                register_producer(data_dir, cfg_new.dhan_client_id)
                ConfigChangeTracker(data_dir=data_dir).record_active_running_version(
                    compute_config_version(cfg_new), trading_mode="TEST",
                    credential_fingerprint=fingerprint(cfg_new, data_dir),
                )
                ready.put(("OK", os.getpid()))
                release_event.wait(timeout=5)
            except BaseException as exc:
                ready.put(("ERR", str(exc)))

        def restart():
            nonlocal child
            holder["calls"] += 1
            if not restart_result:
                return False
            ready = multiprocessing.Queue()
            child = multiprocessing.Process(
                target=child_runner,
                args=(str(self.data), str(self.env), ready, release),
                daemon=True,
            )
            child.start()
            status, data = ready.get(timeout=5)
            self.assertEqual("OK", status, data)
            holder["pid"] = data
            return True

        def status():
            return {
                "ActiveState": "active", "SubState": "running", "Result": "success",
                "MainPID": holder["pid"], "ExecMainStatus": 0,
            }

        try:
            with patch.object(self.tracker, "evaluate_trading_safety_gate_detailed",
                              return_value=(True, "offline fake GET evidence", "", 0)):
                rec = self.submit(
                    expected_revision=revision(original),
                    systemctl_restart_fn=restart,
                    check_service_fn=status,
                )
            return rec, holder
        finally:
            release.set()
            if child is not None:
                child.join(timeout=5)
                if child.is_alive():
                    child.terminate()
                    child.join(timeout=5)

    def test_running_restart_unknown_preserves_env_and_barrier(self):
        from src.engine.broker_handoff import _paths
        rec, ctx = self._simulate_running_v2(restart_result=False)
        self.assertEqual(1, ctx["calls"])
        self.assertFalse(rec.runtime_verified)
        self.assertTrue(rec.restart_performed)
        self.assertEqual("ACK_UNCERTAIN", rec.blocked_reason)
        self.assertIn("1000", self.env.read_text())
        barrier = _paths(self.data)[2]
        self.assertTrue(__import__("json").loads(barrier.read_text())["active"])

    def test_running_positive_new_pid_ack_single_restart(self):
        from src.engine.broker_handoff import _paths
        rec, ctx = self._simulate_running_v2(restart_result=True)
        self.assertEqual(1, ctx["calls"])
        journal = self.data / "config_restart_v2" / "current_transaction.json"
        self.assertTrue(rec.runtime_verified, (rec.reason, journal.read_text() if journal.exists() else "no journal"))
        self.assertEqual("APPLIED_VERIFIED", rec.state.value)
        barrier = _paths(self.data)[2]
        self.assertFalse(__import__("json").loads(barrier.read_text())["active"])



if __name__ == "__main__":
    unittest.main()
