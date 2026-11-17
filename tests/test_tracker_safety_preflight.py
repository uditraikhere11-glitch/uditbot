"""Fully offline positive and negative proofs for tracker preflight (no mocks of live APIs)."""
import json
from pathlib import Path
import tempfile
import unittest

from src.engine.tracker_safety_preflight import inspect_snapshot, verify_ack

HASH = "a" * 64
FP = "fingerprint-not-secret"
ACK = {"pid": 321, "pid_starttime": 12345, "version_hash": HASH,
       "trading_mode": "LIVE", "credential_fingerprint": FP}


class TrackerSafetyPreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.put("emergency_stop.json", {"is_stopped": False, "in_flight_orders": []})
        self.put("active_config_version.json", ACK)

    def put(self, name, value):
        (self.root / name).write_text(json.dumps(value), encoding="utf-8")

    def run_preflight(self, **updates):
        kw = dict(service_state="active", main_pid=321, pid_starttime=12345,
                  effective_hash=HASH, effective_mode="LIVE", credential_fingerprint=FP)
        kw.update(updates)
        return inspect_snapshot(self.root, **kw)

    def test_positive_exact_identity(self):
        x = self.run_preflight()
        self.assertTrue(x.runtime_verified)
        self.assertTrue(x.safe_to_propose)

    def test_noop_missing_ack_must_not_invent_verification(self):
        (self.root / "active_config_version.json").unlink()
        self.assertFalse(self.run_preflight().runtime_verified)

    def test_wrong_pid(self):
        self.assertFalse(self.run_preflight(main_pid=999).runtime_verified)

    def test_wrong_starttime(self):
        self.assertFalse(self.run_preflight(pid_starttime=999).runtime_verified)

    def test_wrong_hash(self):
        self.assertFalse(self.run_preflight(effective_hash="b"*64).runtime_verified)

    def test_wrong_mode(self):
        self.assertFalse(self.run_preflight(effective_mode="TEST").runtime_verified)

    def test_wrong_credentials(self):
        self.assertFalse(self.run_preflight(credential_fingerprint="different").runtime_verified)

    def test_owner_stop_does_not_disappear(self):
        self.put("service_control_state.json", {"desired_service_state": "STOPPED", "deferred_stop_pending": True})
        self.assertIn("OWNER_STOP_PENDING", " ".join(self.run_preflight().blockers))

    def test_pending_operation_not_deleted(self):
        p = self.root / "pending_config_operation.json"
        self.put(p.name, {"state":"IN_PROGRESS"})
        self.assertFalse(self.run_preflight().safe_to_propose)
        self.assertTrue(p.exists())

    def test_corrupt_emergency_stop_blocks(self):
        (self.root / "emergency_stop.json").write_text("{bad", encoding="utf-8")
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_symlinked_emergency_stop_blocks(self):
        path = self.root / "emergency_stop.json"
        path.unlink()
        path.symlink_to(self.root / "active_config_version.json")
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_missing_emergency_stop_running_blocks(self):
        (self.root / "emergency_stop.json").unlink()
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_inflight_orders_block(self):
        self.put("emergency_stop.json", {"is_stopped":False,"in_flight_orders":[{"id":"a"}]})
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_active_gate_held_regardless_of_age(self):
        self.put("mode_transition_gate.json", {"active": True,"created_at":1})
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_malformed_gate_blocks(self):
        self.put("mode_transition_gate.json", {"unexpected": "payload"})
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_stopped_never_runtime_verified(self):
        x = self.run_preflight(service_state="inactive")
        self.assertFalse(x.runtime_verified)
        self.assertEqual(x.state,"STOPPED")

    def test_unsafe_input_state(self):
        self.assertFalse(self.run_preflight(service_state="unknown").safe_to_propose)

    def test_broker_mutations_block(self):
        self.put("active_broker_mutations.json", {"mutations":[{"type":"place"}]})
        self.assertFalse(self.run_preflight().safe_to_propose)

    def test_invalid_ack_bool_pid_is_not_valid(self):
        a = dict(ACK,pid=True)
        self.assertFalse(verify_ack(a,main_pid=1,pid_starttime=12345,effective_hash=HASH,
                                   effective_mode="LIVE",credential_fingerprint=FP))

    def test_read_only_bytes_unchanged(self):
        before={p.name:p.read_bytes() for p in self.root.iterdir()}
        self.run_preflight()
        after={p.name:p.read_bytes() for p in self.root.iterdir()}
        self.assertEqual(before,after)


if __name__ == "__main__":
    unittest.main()
