"""Pure Control Center health behavior, without touching production services."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.engine.tracker_v2_health import owner_intent_health


class OwnerIntentHealthTests(unittest.TestCase):
    def test_verified_running_but_owner_stopped_is_warning(self):
        result = owner_intent_health('RUNNING', {
            'desired_service_state': 'STOPPED',
            'observed_state': 'STOPPED_BY_OWNER',
            'deferred_stop_pending': False,
        })
        self.assertEqual(result.status, 'WARN')

    def test_running_record_agrees(self):
        r = owner_intent_health('RUNNING', {
            'desired_service_state': 'RUNNING',
            'observed_state': 'RUNNING',
            'deferred_stop_pending': False,
        })
        self.assertEqual(r.status, 'PASS')

    def test_stop_is_not_warn_when_owner_and_systemd_agree(self):
        r = owner_intent_health('STOPPED_BY_OWNER', {
            'desired_service_state': 'STOPPED',
            'observed_state': 'STOPPED_BY_OWNER',
            'deferred_stop_pending': False,
        })
        self.assertEqual(r.status, 'PASS')

    def test_deferred_stop_is_warning_even_when_running(self):
        r = owner_intent_health('RUNNING', {
            'desired_service_state': 'STOPPED',
            'observed_state': 'RUNNING',
            'deferred_stop_pending': True,
        })
        self.assertEqual(r.status, 'WARN')

    def test_corrupt_record_does_not_pass(self):
        self.assertEqual(owner_intent_health('RUNNING', {'_corrupted': True}).status, 'UNKNOWN')

    def test_empty_record_does_not_pass(self):
        self.assertEqual(owner_intent_health('RUNNING', {}).status, 'UNKNOWN')

    def test_missing_deferred_proof_does_not_pass(self):
        self.assertEqual(owner_intent_health('RUNNING', {
            'desired_service_state': 'RUNNING', 'observed_state': 'RUNNING'
        }).status, 'UNKNOWN')

    def test_unknown_service_does_not_pass(self):
        self.assertEqual(owner_intent_health('UNKNOWN', {
            'desired_service_state': 'RUNNING', 'observed_state': 'RUNNING',
            'deferred_stop_pending': False,
        }).status, 'UNKNOWN')

    def test_owner_running_but_service_failed_is_warning(self):
        self.assertEqual(owner_intent_health('FAILED', {
            'desired_service_state': 'RUNNING', 'observed_state': 'RUNNING',
            'deferred_stop_pending': False,
        }).status, 'WARN')

if __name__ == '__main__':
    unittest.main()
