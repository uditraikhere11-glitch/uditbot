"""Negative source policy tests for the stopped-service systemd rollout."""
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT/'deploy/tracker_v2_activation.sh'


class ActivationScriptSafetyTests(unittest.TestCase):
    def test_bash_syntax(self):
        p = subprocess.run(['bash', '-n', str(SCRIPT)], capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr.decode())

    def test_no_live_trade_service_mutation_commands(self):
        script = SCRIPT.read_text()
        self.assertNotIn('systemctl restart "$TRADING"', script)
        self.assertNotIn('systemctl stop "$TRADING"', script)
        self.assertNotIn('systemctl start "$TRADING"', script)
        self.assertNotIn('rm -rf "$ROOT/data"', script)
        self.assertNotIn('rm -f "$ROOT/.env"', script)
        self.assertNotIn('dhanc', script.lower())

    def test_initial_activation_requires_stopped_state_and_backup(self):
        s = SCRIPT.read_text()
        self.assertIn('preflight stopped', s)
        self.assertIn('stopped_now || block', s)
        self.assertIn('I_HAVE_OFF_VM_BACKUP_AND_AUTHORIZE_STOPPED_V2', s)

    def test_partial_install_cannot_downgrade_if_v2_evidence_exists(self):
        s = SCRIPT.read_text()
        self.assertIn('v2_recovery_clear', s)
        self.assertIn('Flags PRESERVED', s)
        self.assertIn('if ledger.exists() or ledger.is_symlink()', s)

    def test_both_units_get_same_root_owned_flag(self):
        s = SCRIPT.read_text()
        self.assertIn('for unit in "$TRADING" "$WEB"', s)
        self.assertIn('EnvironmentFile=%s', s)
        self.assertIn('install -o root -g root -m 0644', s)

if __name__ == '__main__':
    unittest.main()

class StagedPreflightRegressionTests(unittest.TestCase):
    def test_requires_staged_validation_after_install(self):
        s = SCRIPT.read_text()
        segment = s.split('    sudo systemctl daemon-reload\n    stopped_now', 1)[1].split('    sudo systemctl restart "$WEB"', 1)[0]
        self.assertIn('verify_unit_config', segment)
        self.assertIn('preflight staged', segment)
        self.assertNotIn('preflight stopped', segment)

    def test_staged_preflight_requires_managed_flags_on_both_units(self):
        source = (ROOT/'deploy/tracker_v2_preflight.py').read_text()
        self.assertIn('managed_stage=stage == "staged"', source)
        self.assertIn('MANAGED_V2_UNIT_CONFIGURATION_UNVERIFIED_', source)
        self.assertIn('"stopped", "staged", "running"', source)

class ModuleInvocationTests(unittest.TestCase):
    def test_preflight_uses_package_module_import(self):
        source = SCRIPT.read_text()
        self.assertIn('-m deploy.tracker_v2_preflight', source)
        self.assertNotIn('python3 deploy/tracker_v2_preflight.py', source)
