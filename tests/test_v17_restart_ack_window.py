"""Regression for the bounded runtime acknowledgement window after a LIVE restart.

The tests never call systemctl, Dhan or Telegram.  They exercise the coordinator
poll only, with deterministic time and a mocked existing strict verifier.
"""

import tempfile
import unittest
from unittest.mock import patch

from src.engine.config_manager import ConfigChangeTracker


class FakeClock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, interval):
        self.elapsed += interval


class RestartAckWindowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tracker = ConfigChangeTracker(data_dir=self.tmp.name)

    def _poll(self, clock, verifier):
        with (
            patch("src.engine.config_manager.is_isolated_test_environment", return_value=False),
            patch("src.engine.config_manager.time.monotonic", side_effect=clock.monotonic),
            patch("src.engine.config_manager.time.sleep", side_effect=clock.sleep),
            patch.object(self.tracker, "is_active_version_confirmed", side_effect=verifier),
        ):
            return self.tracker._poll_for_runtime_verification(
                expected_hashes={"saved-config-sha"},
                expected_mode="LIVE",
                min_acknowledged_at=1234.0,
                check_service_fn=lambda: {"ActiveState": "active"},
            )

    def test_late_valid_acknowledgement_is_accepted_after_previous_six_second_limit(self):
        clock = FakeClock()
        calls = []

        def verify(**kwargs):
            calls.append(kwargs)
            if clock.elapsed >= 7.87:
                return True, {"pid": 1756899}, "Verified"
            return False, None, "New runtime acknowledgement pending"

        ok, info, detail = self._poll(clock, verify)
        self.assertTrue(ok)
        self.assertEqual(info, {"pid": 1756899})
        self.assertEqual(detail, "Verified")
        self.assertGreater(clock.elapsed, 6.0)
        self.assertLess(clock.elapsed, 20.0)
        self.assertTrue(all(c["require_systemd_active"] is True for c in calls))
        self.assertTrue(all(c["min_acknowledged_at"] == 1234.0 for c in calls))
        self.assertTrue(all(c["expected_hashes"] == {"saved-config-sha"} for c in calls))
        self.assertTrue(all(c["expected_mode"] == "LIVE" for c in calls))

    def test_no_valid_acknowledgement_still_fails_closed_after_bounded_wait(self):
        clock = FakeClock()
        ok, info, detail = self._poll(
            clock,
            lambda **kwargs: (False, {"pid": 777}, "MainPID/starttime mismatch"),
        )
        self.assertFalse(ok)
        self.assertEqual(info, {"pid": 777})
        self.assertEqual(detail, "MainPID/starttime mismatch")
        self.assertGreaterEqual(clock.elapsed, 20.0)
        self.assertLess(clock.elapsed, 20.5)

    def test_wrong_identity_never_turns_into_success_just_from_elapsed_time(self):
        clock = FakeClock()
        ok, _, detail = self._poll(
            clock,
            lambda **kwargs: (False, None, "Runtime version hash mismatch"),
        )
        self.assertFalse(ok)
        self.assertEqual(detail, "Runtime version hash mismatch")

    def test_isolated_test_environment_keeps_short_custom_poll_window(self):
        clock = FakeClock()
        with (
            patch("src.engine.config_manager.is_isolated_test_environment", return_value=True),
            patch("src.engine.config_manager.time.monotonic", side_effect=clock.monotonic),
            patch("src.engine.config_manager.time.sleep", side_effect=clock.sleep),
            patch.object(
                self.tracker, "is_active_version_confirmed",
                return_value=(False, None, "Unit inactive"),
            ),
        ):
            ok, _, detail = self.tracker._poll_for_runtime_verification(
                {"hash"}, "TEST", 100.0, verify_delay_sec=0.02
            )
        self.assertFalse(ok)
        self.assertEqual(detail, "Unit inactive")
        self.assertGreaterEqual(clock.elapsed, 0.02)
        self.assertLess(clock.elapsed, 0.04)


if __name__ == "__main__":
    unittest.main()
