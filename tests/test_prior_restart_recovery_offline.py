"""Historical restart recovery: offline durable-fixture cases without broker/systemd IO."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from src.engine.config_manager import ConfigChangeRecord, ConfigChangeTracker, ConfigApplyState
from src.engine.config_patch import ConfigurationConflict

class FakeConfig:
    def __init__(self):
        self.dhan_client_id = "ACCOUNT"
        self.trading_mode = types.SimpleNamespace(value="TEST")
    @classmethod
    def from_env(cls, env=None, env_file=""):
        config = cls()
        config.dhan_client_id = (env or {}).get("DHAN_CLIENT_ID", "")
        config.trading_mode = types.SimpleNamespace(value=(env or {}).get("TRADING_MODE", "TEST"))
        return config
from src.engine import prior_restart_recovery as recovery

ID = "V2-0123456789abcdef"
HASH = "h" * 64
FINGERPRINT = "f" * 64


def private_json(path, obj):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(obj))
    path.chmod(0o600)


class PriorRestartOfflineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name)
        self.tracker = ConfigChangeTracker(data_dir=str(self.data))
        self.env = self.data / ".env"
        self.env.write_text("DHAN_CLIENT_ID=ACCOUNT\nTRADING_MODE=TEST\n")
        self.folder = self.data / "config_restart_v2"
        self.folder.mkdir(mode=0o700)
        self.tx_path = self.folder / "current_transaction.json"
        self.barrier_path = self.folder / "apply_barrier.json"
        self.ledger_path = self.data / "active_broker_mutations.json"
        self.tx = {
            "change_id": ID, "phase": "ACK_UNCERTAIN", "restart_issued": True,
            "previous_pid": 10, "previous_starttime": 100,
            "restart_initiated_at": 110.0,
            "previous_file_revision": "a" * 64,
            "requested_file_revision": hashlib.sha256(self.env.read_bytes()).hexdigest(),
            "expected_runtime_hash": HASH, "target_mode": "TEST",
            "credential_fingerprint": FINGERPRINT,
        }
        self.barrier = {"active": True, "change_id": ID, "account_id": "ACCOUNT"}
        self.ledger = {"schema_version": 2, "account_id": "ACCOUNT",
                       "handoff_verified": True, "producer_pid": 20,
                       "producer_starttime": 200, "active": [], "unresolved": []}
        self.ack = {"pid": 20, "process_starttime": 200,
                    "version_hash": HASH, "trading_mode": "TEST",
                    "credential_fingerprint": FINGERPRINT,
                    "acknowledged_at": 120.0}
        self.systemd = {"ActiveState": "active", "Result": "success", "MainPID": 20}
        private_json(self.tx_path, self.tx)
        private_json(self.barrier_path, self.barrier)
        private_json(self.ledger_path, self.ledger)
        self.tracker.save_change_record(ConfigChangeRecord(change_id=ID, state=ConfigApplyState.APPLY_FAILED))
        self.notify = mock.patch.object(self.tracker, "dispatch_telegram_update")
        self.notify.start()
        self.addCleanup(self.notify.stop)
        self.config_hash = mock.patch.object(recovery, "compute_config_version", return_value=HASH)
        self.config_hash.start()
        self.addCleanup(self.config_hash.stop)
        cfg_patch = mock.patch.object(recovery, "AppConfig", FakeConfig)
        cfg_patch.start()
        self.addCleanup(cfg_patch.stop)

    def write(self):
        private_json(self.tx_path, self.tx)
        private_json(self.barrier_path, self.barrier)
        private_json(self.ledger_path, self.ledger)

    def send(self, fn=None):
        def release(cid, pid, start):
            self.assertEqual((cid,pid,start), (ID,20,200))
            b = json.loads(self.barrier_path.read_text())
            b.update(active=False, released_at=123.0)
            private_json(self.barrier_path,b)
            return True
        with mock.patch.object(recovery, "fingerprint", return_value=FINGERPRINT):
            return recovery.recover_prior_restart(
                self.tracker, str(self.env), requested_change_id=ID,
                ack_probe=lambda: (self.ack,self.systemd),
                starttime_reader=lambda pid: 200 if pid == 20 else None,
                release_fence=release if fn is None else fn,
            )

    def test_original_late_ack_released_without_restart(self):
        out = self.send()
        self.assertTrue(out.runtime_verified)
        self.assertEqual(out.state, ConfigApplyState.APPLIED_VERIFIED)
        self.assertEqual(json.loads(self.tx_path.read_text())["phase"], "VERIFIED")
        self.assertFalse(json.loads(self.barrier_path.read_text())["active"])

    def test_repeat_verified_does_not_release_fence_twice(self):
        self.send()
        out = self.send(fn=lambda *_: self.fail("duplicate barrier release"))
        self.assertTrue(out.runtime_verified)

    def test_crash_after_barrier_release_before_journal(self):
        self.barrier.update(active=False, released_at=124.0)
        self.write()
        out = self.send(fn=lambda *_: self.fail("must not re-release"))
        self.assertTrue(out.runtime_verified)
        self.assertEqual(json.loads(self.tx_path.read_text())["phase"], "VERIFIED")

    def test_crash_after_journal_verified_before_ui_ack(self):
        self.tx["phase"] = "VERIFIED"
        self.barrier.update(active=False, released_at=124.0)
        self.write()
        out = self.send(fn=lambda *_: self.fail("must not re-release"))
        self.assertTrue(out.runtime_verified)
        self.assertTrue(self.tracker.get_change_record(ID).runtime_verified)

    def test_flag_disabled_not_a_dependency(self):
        with mock.patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "0"}):
            self.assertTrue(self.send().runtime_verified)

    def test_wrong_credential_does_not_release(self):
        self.tx["credential_fingerprint"] = "a"*64
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()
        self.assertTrue(json.loads(self.barrier_path.read_text())["active"])

    def test_wrong_account_does_not_release(self):
        self.barrier["account_id"] = "ANOTHER"
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_missing_unverified_ledger_blocks(self):
        self.ledger_path.unlink()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_active_broker_operation_blocks(self):
        self.ledger["active"] = [{"id":"busy"}]
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_unresolved_broker_operation_blocks(self):
        self.ledger["unresolved"] = [{"id":"unknown"}]
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_wrong_pid_blocks(self):
        self.ack["pid"] = 21
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_wrong_starttime_blocks(self):
        self.ack["process_starttime"] = 201
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_wrong_mode_blocks(self):
        self.ack["trading_mode"] = "LIVE"
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_wrong_hash_blocks(self):
        self.ack["version_hash"] = "z"*64
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_old_process_not_replaced_blocks(self):
        self.tx.update(previous_pid=20, previous_starttime=200)
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_saved_env_changed_blocks(self):
        self.env.write_text("DHAN_CLIENT_ID=ANOTHER\nTRADING_MODE=TEST\n")
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_wrong_change_id_blocks(self):
        self.tx["change_id"] = "V2-fedcba9876543210"
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_unrelated_inactive_barrier_blocks(self):
        self.barrier.update(active=False, released_at=124.0, change_id="WRONG")
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_inactive_barrier_missing_release_evidence_blocks(self):
        self.barrier["active"] = False
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_owner_stop_blocks(self):
        private_json(self.data / "service_control_state.json", {
            "deferred_stop_pending":True,"desired_service_state":"STOPPED"
        })
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_unissued_restart_cannot_recover(self):
        self.tx["restart_issued"] = False
        self.write()
        with self.assertRaises(ConfigurationConflict): self.send()

    def test_failed_release_preserves_record(self):
        before = self.tx_path.read_bytes()
        with self.assertRaises(ConfigurationConflict): self.send(fn=lambda *_: False)
        self.assertEqual(self.tx_path.read_bytes(), before)
        self.assertTrue(json.loads(self.barrier_path.read_text())["active"])


if __name__ == "__main__":
    unittest.main()
