"""Static, side-effect-free safety checks for the production WebUI cutover utility.

A real systemd cutover must be validated after independent audit, not in CI.
"""
from pathlib import Path
import subprocess
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "migrate_existing_webui_to_control_center.sh"


class WebuiMigrationScriptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = SCRIPT.read_text(encoding="utf-8")

    def test_bash_syntax(self):
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True, capture_output=True)

    def test_commands_are_opt_in_and_reversible(self):
        for marker in (
            "--check-public-http", "--apply-public-http", "--rollback",
            "I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK",
            "zzzz-uditbot-control-center-pr78.conf",
            "rollback_our_override", "daemon-reload",
        ):
            self.assertIn(marker, self.source)

    def test_only_webui_service_targeted(self):
        self.assertIn("readonly SERVICE='auditbot-web.service'", self.source)
        self.assertNotIn('"$SYSTEMCTL" restart trading_bot.service', self.source)
        self.assertNotIn('"$SYSTEMCTL" stop trading_bot.service', self.source)
        self.assertNotIn('"$SYSTEMCTL" start trading_bot.service', self.source)
        self.assertNotIn('sed -i', self.source)
        self.assertNotIn('trade_journal.db', self.source)

    def test_health_validation_and_no_credential_post(self):
        self.assertIn("http://127.0.0.1:80/login", self.source)
        self.assertIn("/control/status", self.source)
        self.assertIn("PREVIOUS_START", self.source)
        self.assertNotIn('curl -X POST', self.source)

    def test_help_requires_no_root(self):
        r = subprocess.run(["bash", str(SCRIPT), "--help"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0)
        self.assertIn("--apply-public-http", r.stdout)

    def test_unknown_mode_rejected(self):
        r = subprocess.run(["bash", str(SCRIPT), "--oops"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main()
