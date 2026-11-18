import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# The checkout-only package test harness has no entire production repository.
# On GitHub CI, import the real read_private_json implementation unchanged.
try:
    import src.engine.tracker_v2_safety  # noqa: F401
except ModuleNotFoundError:
    import types
    safety = types.ModuleType("src.engine.tracker_v2_safety")
    def read_private_json(path, *, missing_ok=False):
        try:
            p = Path(path)
            if p.is_symlink() or p.stat().st_mode & 0o022:
                raise ValueError("Unsafe record")
            data = json.loads(p.read_text())
            if not isinstance(data, dict):
                raise ValueError("Expected object")
            return data
        except FileNotFoundError:
            if missing_ok:
                return None
            raise
    safety.read_private_json = read_private_json
    sys.modules["src.engine.tracker_v2_safety"] = safety

from src.engine.tracker_v2_dual_service import (
    assert_consistent_save_route, DualServiceMismatch,
)


class DualServiceRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.p = Path(self.tmp.name)

    def ledger(self, **fields):
        p = self.p/'active_broker_mutations.json'
        payload = {'schema_version': 2, 'handoff_verified': True,
                   'producer_pid': 44, 'active': [], 'unresolved': []}
        payload.update(fields)
        p.write_text(json.dumps(payload))
        p.chmod(0o600)
        return p

    def owner_stopped(self, **fields):
        owner = {'desired_service_state': 'STOPPED',
                 'observed_state': 'STOPPED_BY_OWNER',
                 'deferred_stop_pending': False}
        owner.update(fields)
        p = self.p/'service_control_state.json'
        p.write_text(json.dumps(owner))
        p.chmod(0o600)
        return p

    def guard(self, web_v2, pid=44, flag=True, state='active'):
        return assert_consistent_save_route(
            self.p, web_v2=web_v2, pid_reader=lambda: pid,
            process_flag=lambda _pid: flag,
            state_reader=lambda: state)

    def test_fully_legacy_is_unchanged(self):
        self.guard(False, flag=False)

    def test_legacy_web_blocks_v2_worker_without_ledger(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(False, flag=True)

    def test_legacy_web_blocks_unverifiable_process(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(False, pid=-1, flag=False)

    def test_legacy_web_allows_verified_stopped_service_without_v2(self):
        self.guard(False, pid=0, flag=False, state='inactive')

    def test_legacy_web_blocks_active_service_with_zero_pid(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(False, pid=0, flag=False, state='active')

    def test_legacy_web_rejects_v2_ledger(self):
        self.ledger()
        with self.assertRaises(DualServiceMismatch):
            self.guard(False)

    def test_legacy_web_rejects_v2_transaction(self):
        p = self.p/'config_restart_v2'
        p.mkdir()
        (p/'current_transaction.json').write_text('{}')
        with self.assertRaises(DualServiceMismatch):
            self.guard(False)

    def test_legacy_web_rejects_v2_barrier(self):
        p = self.p/'config_restart_v2'
        p.mkdir()
        (p/'apply_barrier.json').write_text('{}')
        with self.assertRaises(DualServiceMismatch):
            self.guard(False)

    def test_web_enabled_running_bot_flag_off_blocks(self):
        self.ledger()
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, flag=False)

    def test_web_enabled_running_bot_missing_handoff_blocks(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(True)

    def test_web_enabled_running_bot_wrong_producer_blocks(self):
        self.ledger(producer_pid=55)
        with self.assertRaises(DualServiceMismatch):
            self.guard(True)

    def test_web_enabled_running_bot_bad_schema_blocks(self):
        self.ledger(schema_version=1)
        with self.assertRaises(DualServiceMismatch):
            self.guard(True)

    def test_both_enabled_verified_producer_allows_next_gate(self):
        self.ledger()
        self.guard(True)

    def test_stopped_bot_eligible_for_separate_stopped_save_gate(self):
        self.owner_stopped()
        self.guard(True, pid=0, state='inactive')

    def test_systemd_activating_with_pid_zero_must_block(self):
        self.owner_stopped()
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, pid=0, state='activating')

    def test_missing_owner_stop_with_pid_zero_must_block(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, pid=0, state='inactive')

    def test_owner_stop_pending_with_pid_zero_must_block(self):
        self.owner_stopped(deferred_stop_pending=True)
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, pid=0, state='inactive')

    def test_stopped_with_unresolved_broker_handoff_must_block(self):
        self.owner_stopped()
        self.ledger(unresolved=[{'id': 'unknown'}])
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, pid=0, state='inactive')

    def test_corrupt_ledger_fails_closed(self):
        p = self.ledger()
        p.write_text('{broken')
        with self.assertRaises(DualServiceMismatch):
            self.guard(True)

    def test_legacy_corrupt_ledger_fails_closed(self):
        p = self.ledger()
        p.write_text('{broken')
        with self.assertRaises(DualServiceMismatch):
            self.guard(False)

    def test_invalid_pid_type_fails_closed(self):
        with self.assertRaises(DualServiceMismatch):
            self.guard(True, pid='invalid')

if __name__ == '__main__':
    unittest.main()
