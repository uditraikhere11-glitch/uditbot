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
from src.engine.runtime_evidence import SafetyDecision


class RetiredStaleStatusSafetyTests(unittest.TestCase):
    """Stale historical records must remain unchanged and block new writes."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.data.mkdir(mode=0o700)
        self.env = self.root / ".env"
        self.env.write_text(
            "TRADING_MODE=TEST\nDHAN_CLIENT_ID=FAKE_ID\n"
            f"DATA_DIR={self.data}\nEQUITY_AMO_ENABLED=false\n"
        )
        self.tracker = ConfigChangeTracker(data_dir=str(self.data))
        self.record = ConfigChangeRecord(
            change_id="RETIRED-UNRESOLVED-RECORD",
            state=ConfigApplyState.APPLYING,
            operation_state=OperationState.APPLYING.value,
        )
        self.tracker.save_change_record(self.record)

    def test_old_stale_coordinator_is_not_callable(self):
        self.assertFalse(hasattr(self.tracker, "reconcile_stale_control_records"))

    def test_old_record_is_immutable_during_owner_stop_tick(self):
        before = Path(self.tracker.state_file).read_bytes()
        result = self.tracker.run_control_coordinator_tick()
        self.assertIsNone(result["queued_executed"])
        self.assertEqual(Path(self.tracker.state_file).read_bytes(), before)

    def test_old_applying_record_cannot_be_silently_accepted_as_clean(self):
        from src.engine.tracker_v2_safety import preview_legacy_tracker_migration
        decision = preview_legacy_tracker_migration(self.data)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.code, "LEGACY_CHANGE_IN_PROGRESS")

    def test_terminal_saved_for_next_start_not_busy(self):
        self.record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        self.record.operation_state = OperationState.SAVED_FOR_NEXT_START.value
        self.tracker.save_change_record(self.record)
        self.assertFalse(self.tracker.has_unresolved_pending_operation())

    def test_different_pending_envelope_is_not_discarded(self):
        self.record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        self.tracker.save_change_record(self.record)
        write_json_atomic(self.tracker.pending_op_file, {
            "change_id": "OTHER-CHANGE", "status": "QUEUED",
        })
        self.assertTrue(self.tracker.has_unresolved_pending_operation())
        before = Path(self.tracker.pending_op_file).read_bytes()
        self.tracker.run_control_coordinator_tick()
        self.assertEqual(Path(self.tracker.pending_op_file).read_bytes(), before)


class AbortedBrokerFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        from src.engine.broker_handoff import _paths, _write
        _, self.ledger, self.barrier = _paths(self.root)
        self.ledger.parent.mkdir(parents=True, exist_ok=True)
        _write(self.ledger, {
            "schema_version": 2, "handoff_verified": True,
            "account_id": "FAKE_ID", "producer_pid": 101,
            "producer_starttime": 123, "active": [], "unresolved": [],
        })
        _write(self.barrier, {
            "active": True, "change_id": "SAVE-ONE",
            "account_id": "FAKE_ID", "owner_pid": 42,
        })

    def release(self, stopped=True):
        from src.engine.broker_handoff import release_aborted_apply_barrier
        return release_aborted_apply_barrier(
            self.root, "FAKE_ID", "SAVE-ONE", 101, 123,
            service_stopped=lambda: stopped,
        )

    def test_prepared_migration_requires_old_file_and_empty_ledger(self):
        from src.engine.broker_handoff import _write
        env = self.root / ".env"
        env.write_text("DHAN_CLIENT_ID=FAKE_ID\n")
        rev = revision(env.read_text())
        tx = self.barrier.parent / "current_transaction.json"
        _write(tx, dict(phase="PREPARED", previous_file_revision=rev, requested_file_revision="f"*64,
                        restart_issued=False, previous_pid=101, previous_starttime=123, change_id="SAVE-ONE"))
        values = {"DHAN_CLIENT_ID": "FAKE_ID"}
        with patch.object(direct, "inspect_systemd_service_properties", return_value={"MainPID":0, "ActiveState":"inactive"}):
            with self.assertRaises(ConfigurationConflict):
                direct._retire_previous_transaction(self.root, env, values, "0"*64)
            self.assertTrue(json.loads(self.barrier.read_text())["active"])
            direct._retire_previous_transaction(self.root, env, values, rev)
        self.assertFalse(tx.exists())
        self.assertFalse(json.loads(self.barrier.read_text())["active"])

    def test_stopped_empty_ledger_releases_exact_precommit_fence(self):
        self.assertTrue(self.release())
        self.assertFalse(json.loads(self.barrier.read_text())["active"])
        self.assertEqual(json.loads(self.barrier.read_text())["release_reason"],
                         "ABORTED_BEFORE_ENV_COMMIT_SYSTEMD_STOPPED")

    def test_running_service_never_releases(self):
        self.assertFalse(self.release(stopped=False))
        self.assertTrue(json.loads(self.barrier.read_text())["active"])

    def test_unknown_mutation_never_releases(self):
        from src.engine.broker_handoff import _write
        ledger = json.loads(self.ledger.read_text())
        ledger["unresolved"] = [{"id": "UNKNOWN"}]
        _write(self.ledger, ledger)
        self.assertFalse(self.release())
        self.assertTrue(json.loads(self.barrier.read_text())["active"])

    def test_wrong_change_id_never_releases(self):
        from src.engine.broker_handoff import release_aborted_apply_barrier
        self.assertFalse(release_aborted_apply_barrier(
            self.root, "FAKE_ID", "OTHER", 101, 123,
            service_stopped=lambda: True,
        ))
        self.assertTrue(json.loads(self.barrier.read_text())["active"])

    def test_wrong_producer_never_releases(self):
        from src.engine.broker_handoff import release_aborted_apply_barrier
        self.assertFalse(release_aborted_apply_barrier(
            self.root, "FAKE_ID", "SAVE-ONE", 999, 123,
            service_stopped=lambda: True,
        ))
        self.assertTrue(json.loads(self.barrier.read_text())["active"])


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

        import sqlite3
        with sqlite3.connect(self.proof["data"] / "trade_journal.db") as db:
            rows = db.execute("select payload from outbox where event_type='SETTINGS_RESTART_VERIFIED'").fetchall()
        queued = [json.loads(raw)["text"] for raw, in rows]
        self.assertEqual(len(queued), 1)
        self.assertIn("<b>Settings Updated</b>", queued[0])
        self.assertIn("Trading bot: Restarted", queued[0])
        self.assertIn("Configuration: Verified", queued[0])
        self.assertIn("Changed Settings (1):", queued[0])
        self.assertIn("Disabled → Enabled", queued[0])
        # The user-facing WebUI message is retained for API compatibility.
        self.assertIn("Settings updated. Trading bot restarted", out.message)

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
            "NO_V2_TRANSACTION",
        )
        self.assertEqual(len(list(directory.glob("retired-*.json"))), 1)
        self.env.write_text(self.env.read_text() + "UNTRUSTED_MANUAL=YES\n")
        self.assertEqual(
            inspect_transaction_recovery(Path(self.temp.name) / "data", self.env).code,
            "NO_V2_TRANSACTION",
        )

    def test_amotoggle_admission_runs_under_shared_save_lock(self):
        calls = []
        def admission():
            calls.append("admitted")
        result = direct.apply_settings(
            {"equity_amo_enabled": True}, env_path=self.env,
            expected_revision=revision(self.before),
            state_reader=lambda: "inactive", admission_check=admission,
        )
        self.assertEqual(result.status, "SAVED_STOPPED")
        self.assertEqual(calls, ["admitted"])

    def test_amotoggle_denied_admission_keeps_env_intact(self):
        def admission():
            raise ConfigurationConflict("Concurrent STOP")
        with self.assertRaises(ConfigurationConflict):
            direct.apply_settings(
                {"equity_amo_enabled": True}, env_path=self.env,
                expected_revision=revision(self.before),
                state_reader=lambda: "inactive", admission_check=admission,
            )
        self.assertEqual(self.env.read_text(), self.before)

    def test_stop_race_before_env_commit_reports_failure(self):
        reads = iter(("active", "inactive"))
        with patch.object(direct, "is_isolated_test_environment", return_value=False), \
             patch.object(direct, "_positive_running_restart_proof",
                          return_value=dict(self.proof)), \
             patch.object(direct, "acquire_apply_barrier", return_value=True), \
             patch.object(direct, "inspect_systemd_service_properties",
                          return_value={"ActiveState": "inactive", "MainPID": 0}), \
             patch.object(direct, "release_aborted_apply_barrier", return_value=True) as release:
            with self.assertRaises(ConfigurationConflict):
                direct.apply_settings(
                    {"equity_amo_enabled": True}, env_path=self.env,
                    expected_revision=revision(self.before),
                    state_reader=lambda: next(reads), restart=lambda: self.fail("No restart"),
                )
        self.assertEqual(self.env.read_text(), self.before)
        release.assert_called_once()

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
