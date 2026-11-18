"""PR98: stopped/verified-running stale status recovery and one-restart safety.

All observations are simulated and no Dhan, Telegram, or systemd calls occur.
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.config import AppConfig, parse_env_dict
from src.engine.config_patch import ConfigurationConflict, revision
from src.engine.config_manager import (
    ConfigApplyState, ConfigChangeRecord, ConfigChangeTracker,
    OperationState, compute_config_version, write_json_atomic,
)
from src.engine import simple_save_apply as direct
from src.engine.tracker_v2_safety import SafetyDecision


class StaleServiceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.data.mkdir()
        self.env = self.root / ".env"
        self.env.write_text(
            "TRADING_MODE=TEST\nDHAN_CLIENT_ID=FAKE_ID\n"
            f"DATA_DIR={self.data}\nEQUITY_AMO_ENABLED=false\n"
        )
        self.tracker = ConfigChangeTracker(data_dir=str(self.data))
        self.cfg = AppConfig.from_env(env=parse_env_dict(str(self.env)), env_file="")
        self.hash = compute_config_version(self.cfg)
        self.control = {
            "desired_service_state": "RUNNING",
            "last_action": "OWNER_START",
            "observed_state": "STARTING",
            "deferred_stop_pending": False,
        }
        write_json_atomic(self.tracker.service_control_file, self.control)
        self.record = ConfigChangeRecord(
            change_id="TEST-STUCK-PR98",
            state=ConfigApplyState.APPLYING,
            operation_state=OperationState.APPLYING.value,
            requested_file_revision=revision(self.env.read_text()),
            proposed_version_hash=self.hash,
            previous_mode="TEST",
            requested_mode="TEST",
        )
        self.tracker.save_change_record(self.record)

    def stopped(self):
        with patch("src.engine.config_manager.inspect_systemd_service_properties",
                   return_value={"ActiveState": "inactive", "MainPID": 0}):
            return self.tracker.reconcile_stale_control_records(str(self.env))

    def running(self, *, gate=True, pid=201):
        decision = SafetyDecision(
            gate, "NO_ACTIVE_BROKER_OPERATION" if gate else "UNRESOLVED_BROKER_MUTATION",
            "simulated", {"producer_pid": pid, "producer_starttime": 555},
        )
        with patch("src.engine.config_manager.inspect_systemd_service_properties",
                   return_value={"ActiveState": "active", "MainPID": 201}), \
             patch.object(self.tracker, "is_active_version_confirmed",
                          return_value=(True, {"pid": 201, "process_starttime": 555}, "Verified")), \
             patch("src.engine.tracker_v2_safety.evaluate_active_broker_operation_gate",
                   return_value=decision), \
             patch("src.engine.tracker_v2_safety.read_private_json",
                   return_value={"account_id": "FAKE_ID", "active": [], "unresolved": []}):
            return self.tracker.reconcile_stale_control_records(str(self.env))

    def test_stopped_process_resolves_stale_states_without_restart(self):
        out = self.stopped()
        self.assertTrue(out["resolved"], out)
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.SAVED_FOR_NEXT_START)
        self.assertEqual(self.tracker.get_service_control_record()["observed_state"],
                         "STOPPED_OTHER")
        self.assertEqual(self.tracker.get_service_control_record()["desired_service_state"],
                         "RUNNING")

    def test_running_exact_pid_hash_and_empty_handoff_recovers(self):
        out = self.running()
        self.assertTrue(out["resolved"], out)
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(self.tracker.get_service_control_record()["observed_state"],
                         "RUNNING")

    def test_unresolved_broker_operation_blocks_repair(self):
        out = self.running(gate=False)
        self.assertFalse(out["resolved"])
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.APPLYING)

    def test_wrong_broker_producer_pid_blocks_repair(self):
        out = self.running(pid=299)
        self.assertFalse(out["resolved"])
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.APPLYING)

    def test_mismatched_requested_file_never_verified(self):
        self.record.requested_file_revision = "0" * 64
        self.record.proposed_version_hash = "f" * 64
        self.tracker.save_change_record(self.record)
        self.stopped()
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.APPLYING)

    def test_v2_stopped_journal_becomes_terminal_without_second_restart(self):
        target = self.data / "config_restart_v2"
        target.mkdir()
        write_json_atomic(str(target / "current_transaction.json"), {
            "phase": "ACK_UNCERTAIN",
            "request_id": "previous-restart",
            "restart_issued": True,
            "requested_file_revision": revision(self.env.read_text()),
        })
        out = self.stopped()
        self.assertNotIn("v2", out)
        self.assertEqual(
            json.loads((target / "current_transaction.json").read_text())["phase"],
            "ACK_UNCERTAIN",
        )
        self.assertEqual(
            json.loads((target / "current_transaction.json").read_text())["restart_issued"],
            True,
        )

    def test_v2_active_barrier_never_auto_cleared(self):
        target = self.data / "config_restart_v2"
        target.mkdir()
        write_json_atomic(str(target / "current_transaction.json"), {
            "phase": "ACK_UNCERTAIN",
            "requested_file_revision": revision(self.env.read_text()),
        })
        write_json_atomic(str(target / "apply_barrier.json"), {
            "active": True, "owner_pid": 101, "owner_starttime": 123,
        })
        out = self.running()
        self.assertNotIn("v2", out)
        self.assertTrue(json.loads((target / "apply_barrier.json").read_text())["active"])
        self.assertEqual(
            json.loads((target / "current_transaction.json").read_text())["phase"],
            "ACK_UNCERTAIN",
        )

    def test_v2_mismatched_env_never_auto_resolved(self):
        target = self.data / "config_restart_v2"
        target.mkdir()
        write_json_atomic(str(target / "current_transaction.json"), {
            "phase": "ACK_UNCERTAIN",
            "requested_file_revision": "9" * 64,
        })
        self.stopped()
        self.assertEqual(
            json.loads((target / "current_transaction.json").read_text())["phase"],
            "ACK_UNCERTAIN",
        )

    def test_terminal_saved_for_next_start_not_busy(self):
        self.record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        self.record.operation_state = OperationState.SAVED_FOR_NEXT_START.value
        self.tracker.save_change_record(self.record)
        self.stopped()
        self.assertEqual(self.tracker.get_change_record().state,
                         ConfigApplyState.SAVED_FOR_NEXT_START)

    def test_saved_for_next_start_matching_pending_envelope_is_terminal(self):
        self.record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        self.record.operation_state = OperationState.SAVED_FOR_NEXT_START.value
        self.tracker.save_change_record(self.record)
        write_json_atomic(self.tracker.pending_op_file, {
            "change_id": self.record.change_id, "status": "QUEUED",
        })
        self.assertFalse(self.tracker.has_unresolved_pending_operation())
        write_json_atomic(self.tracker.pending_op_file, {
            "change_id": "OTHER-CHANGE", "status": "QUEUED",
        })
        self.assertTrue(self.tracker.has_unresolved_pending_operation())

    def test_active_owner_stop_intent_is_not_overwritten(self):
        self.control["desired_service_state"] = "STOPPED"
        write_json_atomic(self.tracker.service_control_file, self.control)
        self.running()
        self.assertEqual(
            self.tracker.get_service_control_record()["desired_service_state"], "STOPPED",
        )
        self.assertEqual(
            self.tracker.get_service_control_record()["observed_state"], "STARTING",
        )


class DirectBrokerRestartTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = Path(self.temp.name) / ".env"
        self.env.write_text("TRADING_MODE=TEST\nDHAN_CLIENT_ID=FAKE_ID\n"
                            "EQUITY_AMO_ENABLED=false\n")
        self.before = self.env.read_text()
        self.proof = {
            "data": Path(self.temp.name) / "data", "account": "FAKE_ID",
            "pid": 101, "starttime": 123,
        }
        self.proof["data"].mkdir()
        self.restarts = 0

    def change(self):
        def restart():
            self.restarts += 1
        return direct.apply_settings(
            {"equity_amo_enabled": True}, env_path=self.env,
            expected_revision=revision(self.before),
            state_reader=lambda: "active", restart=restart,
        )

    def test_broker_handoff_denial_blocks_before_env_write(self):
        with patch.object(direct, "is_isolated_test_environment", return_value=False), \
             patch.object(direct, "_positive_running_restart_proof",
                          return_value=dict(self.proof)), \
             patch.object(direct, "acquire_apply_barrier", return_value=False):
            with self.assertRaises(ConfigurationConflict):
                self.change()
        self.assertEqual(self.env.read_text(), self.before)
        self.assertEqual(self.restarts, 0)

    def test_one_barrier_one_restart_verified_new_runtime(self):
        with patch.object(direct, "is_isolated_test_environment", return_value=False), \
             patch.object(direct, "_positive_running_restart_proof",
                          return_value=dict(self.proof)), \
             patch.object(direct, "acquire_apply_barrier", return_value=True) as fence, \
             patch.object(direct, "_verify_new_runtime") as verified:
            out = self.change()
        self.assertEqual(out.status, "RESTARTED")
        self.assertEqual(self.restarts, 1)
        fence.assert_called_once()
        verified.assert_called_once()
        self.assertIn("EQUITY_AMO_ENABLED=true", self.env.read_text())

    def test_terminal_v2_journal_tracks_direct_save_without_trusting_external_writes(self):
        from src.engine.tracker_v2_safety import inspect_transaction_recovery
        from hashlib import sha256
        directory = Path(self.temp.name) / "data" / "config_restart_v2"
        directory.mkdir(parents=True)
        before_hash = sha256(self.before.encode()).hexdigest()
        write_json_atomic(str(directory / "current_transaction.json"), {
            "phase": "VERIFIED",
            "requested_file_revision": before_hash,
            "previous_file_revision": before_hash,
        })
        result = direct.apply_settings(
            {"equity_amo_enabled": True}, env_path=self.env,
            expected_revision=revision(self.before),
            state_reader=lambda: "inactive", restart=lambda: self.fail("No restart"),
        )
        self.assertEqual(result.status, "SAVED_STOPPED")
        self.assertEqual(
            inspect_transaction_recovery(Path(self.temp.name) / "data", self.env).code,
            "DIRECT_SAVE_SUPERSEDED",
        )
        self.env.write_text(self.env.read_text() + "UNTRUSTED_MANUAL=YES\n")
        self.assertEqual(
            inspect_transaction_recovery(Path(self.temp.name) / "data", self.env).code,
            "EXTERNAL_ENV_WRITER",
        )

    def test_exact_noop_retry_does_not_restart_again(self):
        first = direct.apply_settings(
            {"equity_amo_enabled": True}, env_path=self.env,
            expected_revision=revision(self.before), state_reader=lambda: "inactive",
        )
        second = direct.apply_settings(
            {"equity_amo_enabled": True}, env_path=self.env,
            expected_revision=revision(self.before), state_reader=lambda: "active",
            restart=lambda: self.fail("No-op retry must not restart"),
        )
        self.assertEqual(first.status, "SAVED_STOPPED")
        self.assertEqual(second.status, "UNCHANGED")


if __name__ == "__main__":
    unittest.main()
