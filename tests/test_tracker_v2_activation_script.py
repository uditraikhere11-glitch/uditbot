"""Negative source policy tests for the stopped-service systemd rollout."""
from pathlib import Path
import subprocess
import tempfile
import os
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT/'deploy/tracker_v2_activation.sh'


class ActivationScriptSafetyTests(unittest.TestCase):
    def test_bash_syntax(self):
        p = subprocess.run(['bash', '-n', str(SCRIPT)], capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr.decode())

    def test_unprivileged_activation_and_running_checks_share_safe_proc_fallback(self):
        script = SCRIPT.read_text()
        preflight = (ROOT / 'deploy/tracker_v2_preflight.py').read_text()
        self.assertIn('flag = process_flag(pid)', script)
        self.assertIn('preflight running', script)
        self.assertIn('["sudo", "-n", "/usr/bin/cat", str(target)]', preflight)
        self.assertNotIn('sudo env PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3', script)

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


class WebuiReadinessRetryTests(unittest.TestCase):
    """Execute the real Bash helper with isolated shell stubs; no systemctl calls."""

    def check_ready(self, codes, *, stopped=True, web_state="active"):
        script = SCRIPT.read_text()
        body = script.split("webui_login_ready() {\n", 1)[1].split("\n}\n", 1)[0]
        with tempfile.TemporaryDirectory() as temp:
            counter = Path(temp) / "calls"
            counter.write_text("0")
            harness = r"""
set -Eeuo pipefail
WEB=auditbot-web.service
stopped_now() { [[ "$MOCK_STOPPED" == yes ]]; }
block() { echo "BLOCK: $*" >&2; exit 2; }
systemctl() { printf '%s\n' "$MOCK_WEB_STATE"; }
sleep() { :; }
curl() {
  local n reply
  n=$(cat "$MOCK_COUNTER")
  printf '%s' "$(( n + 1 ))" > "$MOCK_COUNTER"
  IFS=',' read -r -a values <<< "$MOCK_CODES"
  if (( n >= ${#values[@]} )); then
    reply="${values[${#values[@]}-1]}"
  else
    reply="${values[n]}"
  fi
  printf '%s' "$reply"
  [[ "$reply" != '000' ]]
}
webui_login_ready() {
""" + body + "\n}\nwebui_login_ready\n"
            env = os.environ.copy()
            env.update(MOCK_STOPPED="yes" if stopped else "no",
                       MOCK_WEB_STATE=web_state,
                       MOCK_CODES=",".join(codes), MOCK_COUNTER=str(counter))
            result = subprocess.run(["bash", "-c", harness], env=env,
                                    capture_output=True, text=True, timeout=5)
            return result, int(counter.read_text())

    def test_transient_connection_refusal_then_303_is_accepted(self):
        result, attempts = self.check_ready(["000", "000", "303"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(attempts, 3)

    def test_http_200_is_never_accepted(self):
        result, attempts = self.check_ready(["200", "303"])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(attempts, 1)
        self.assertIn("unexpected HTTP (200)", result.stderr)

    def test_unauthenticated_503_is_not_retried_as_connection_failure(self):
        result, attempts = self.check_ready(["503", "303"])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(attempts, 1)

    def test_persistent_connection_failure_has_hard_bound(self):
        result, attempts = self.check_ready(["000"])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(attempts, 15)
        self.assertIn("never produced HTTP 303", result.stderr)

    def test_trading_state_change_aborts_before_http(self):
        result, attempts = self.check_ready(["303"], stopped=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(attempts, 0)

    def test_webui_service_failure_aborts_before_http(self):
        result, attempts = self.check_ready(["303"], web_state="inactive")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(attempts, 0)

    def test_running_pid_flag_is_reverified_after_ready(self):
        source = SCRIPT.read_text()
        after = source.split("    webui_login_ready\n", 1)[1]
        self.assertIn("flag = process_flag(pid)", after)
        self.assertIn("WebUI V2 process flag changed during readiness", after)


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
