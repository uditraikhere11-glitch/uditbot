"""Verify injection is uniquely anchored and refuses altered source."""
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from patches.build_source_replacements import (
    replace_exact, WEBUI_OLD, HEALTH_OLD,
)


class SourcePatchTests(unittest.TestCase):
    def test_integration_injected_only_once(self):
        sample = ('header\n' + WEBUI_OLD + 'footer\n').encode()
        revised = replace_exact('src/engine/webui_update.py', sample, check_blob=False)
        self.assertIn(b'assert_consistent_save_route(', revised)
        self.assertIn(b'raise ConfigurationConflict(', revised)
        self.assertEqual(revised.count(b'assert_consistent_save_route('), 1)

    def test_health_reports_owner_state(self):
        sample = ('header\n' + HEALTH_OLD + 'footer\n').encode()
        revised = replace_exact('src/control_center_backend.py', sample, check_blob=False)
        self.assertIn(b'owner_intent_health(', revised)
        self.assertIn(b'check("Owner service intent"', revised)

    def test_duplicate_anchor_is_rejected(self):
        with self.assertRaises(ValueError):
            replace_exact('src/engine/webui_update.py', (WEBUI_OLD * 2).encode(), check_blob=False)

    def test_drift_rejected_with_blob(self):
        with self.assertRaises(ValueError):
            replace_exact('src/control_center_backend.py', HEALTH_OLD.encode())

    def test_missing_anchor_is_rejected(self):
        with self.assertRaises(ValueError):
            replace_exact('src/control_center_backend.py', b'unrelated', check_blob=False)

if __name__ == '__main__':
    unittest.main()
