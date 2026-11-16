"""Offline, isolated tests for the existing-VM WebUI service migration.

Never opens ports or invokes real sudo/systemd, Dhan, Telegram or production files.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1] / 'deploy' / 'upgrade_webui_control_center.sh'


class WebUIServiceMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='webui-service-offline-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for path in ('deploy', 'src', 'venv/bin', 'data', 'systemd', 'fakes'):
            (self.root / path).mkdir(parents=True, exist_ok=True)
        (self.root / 'data').chmod(0o700)
        (self.root / '.env').write_text('TRADING_MODE=TEST\n')
        for name in ('control_center_entry.py', 'webui_password.py'):
            (self.root / 'src' / name).write_text('# dummy fixture\n')
        self.script = self.root / 'deploy' / SOURCE.name
        shutil.copy2(SOURCE, self.script)
        (self.root / 'venv/bin/python3').write_text('''#!/bin/sh
case "$*" in
  *password_configured*) test -f "$PWD/data/webui_password.json" ;;
  *) exit 0 ;;
esac
''')
        (self.root / 'venv/bin/python3').chmod(0o755)
        self.unit = self.root / 'systemd/auditbot-web.service'
        self.unit.write_text('[Service]\nExecStart=/usr/bin/python3 -m src.setup_app --host 0.0.0.0 --port 80\n')
        self.dropins = self.root / 'systemd/auditbot-web.service.d'
        self.dropins.mkdir()
        (self.dropins / '10-original-access.conf').write_text('[Service]\nExecStart=\nExecStart=/usr/bin/python3 -m src.setup_app --host 0.0.0.0 --port 80\n')
        (self.root / 'systemd/active').touch()
        self.override = self.dropins / '99-github-authenticated-control.conf'

        self._fake('sudo', '''#!/bin/sh
exec "$@"
''')
        self._fake('systemctl', '''#!/bin/sh
set -eu
D="$AUDITBOT_SYSTEMD_DIR"
OV="$D/auditbot-web.service.d/99-github-authenticated-control.conf"
case "$1" in
  is-active) if [ -e "$D/active" ]; then echo active; exit 0; else echo inactive; exit 3; fi ;;
  stop) rm -f "$D/active" ;;
  restart) touch "$D/active" ;;
  daemon-reload) : ;;
  cat)
    cat "$D/auditbot-web.service"
    cat "$D/auditbot-web.service.d/10-original-access.conf"
    if [ -e "$OV" ]; then cat "$OV"; fi ;;
  show)
    if [ "${FAKE_EFFECTIVE_OLD:-}" = 1 ]; then
       echo 'argv[]=/usr/bin/python3 -m src.setup_app --host 0.0.0.0 --port 80'
    elif [ -e "$OV" ]; then
       grep '^ExecStart=' "$OV" | tail -n 1
    else
       echo 'argv[]=/usr/bin/python3 -m src.setup_app --host 0.0.0.0 --port 80'
    fi ;;
  *) echo "Unexpected systemctl command: $*" >&2; exit 2 ;;
esac
''')
        self._fake('curl', '''#!/bin/sh
for LAST; do :; done
if [ "${FAKE_CURL_HTTP_FAIL:-}" = 1 ]; then
    printf '200'; exit 0
fi
if [ "${FAKE_CURL_CONNECTION_REFUSED_ONCE:-}" = 1 ] && [ ! -e "$AUDITBOT_SYSTEMD_DIR/curl-tried" ]; then
    touch "$AUDITBOT_SYSTEMD_DIR/curl-tried"
    exit 7
fi
case "$LAST" in
  */login) printf '200' ;;
  *) printf '303' ;;
esac
''')
        self.env = dict(os.environ)
        self.env.update({
            'AUDITBOT_TEST_ISOLATION': '1',
            'AUDITBOT_SYSTEMD_DIR': str(self.root / 'systemd'),
            'AUDITBOT_SYSTEMCTL_BIN': str(self.root / 'fakes/systemctl'),
            'AUDITBOT_SUDO_BIN': str(self.root / 'fakes/sudo'),
            'AUDITBOT_CURL_BIN': str(self.root / 'fakes/curl'),
            'HOME': str(self.root),
        })

    def _fake(self, name, data):
        file = self.root / 'fakes' / name
        file.write_text(data)
        file.chmod(0o755)

    def run_cmd(self, *args, env=None):
        return subprocess.run(['bash', str(self.script), *args], cwd=self.root,
                              env=env or self.env, text=True, capture_output=True)

    def assert_active(self, active=True):
        self.assertEqual((self.root / 'systemd/active').exists(), active)

    def test_plan_is_read_only_even_if_password_missing(self):
        original = self.unit.read_bytes()
        result = self.run_cmd('--plan')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('READ-ONLY', result.stdout)
        self.assertIn('missing', result.stdout)
        self.assertFalse(self.override.exists())
        self.assert_active()
        self.assertEqual(self.unit.read_bytes(), original)

    def test_missing_password_prevents_update_and_restart(self):
        result = self.run_cmd('--apply')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Provision a private password', result.stderr)
        self.assertFalse(self.override.exists())
        self.assert_active()

    def test_private_migration_uses_correct_entrypoint_and_does_not_change_old_unit(self):
        (self.root / 'data/webui_password.json').touch()
        old = self.unit.read_bytes()
        old_dropin = (self.dropins / '10-original-access.conf').read_bytes()
        result = self.run_cmd('--apply')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('SUCCESS:', result.stdout)
        self.assertIn('127.0.0.1 --port 80', self.override.read_text())
        self.assertIn('Environment=UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=\n', self.override.read_text())
        self.assertEqual(self.unit.read_bytes(), old)
        self.assertEqual((self.dropins / '10-original-access.conf').read_bytes(), old_dropin)
        self.assert_active()
        self.assertTrue(list((self.root / 'test-backups').glob('*/UDITBOT_WEBUI_BACKUP_V1')))
        again = self.run_cmd('--apply')
        self.assertNotEqual(again.returncode, 0)
        self.assertIn('already exists', again.stderr)

    def test_public_requires_two_explicit_flags(self):
        (self.root / 'data/webui_password.json').touch()
        res = self.run_cmd('--apply', '--public-http')
        self.assertNotEqual(res.returncode, 0)
        self.assertFalse(self.override.exists())
        res = self.run_cmd('--apply', '--public-http', '--accept-plaintext-risk')
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assertIn('--host 0.0.0.0', self.override.read_text())
        self.assertIn('I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK', self.override.read_text())
        self.assertIn('WARNING:', res.stdout)

    def test_http_auth_failure_removes_override_and_leaves_webui_stopped(self):
        (self.root / 'data/webui_password.json').touch()
        env = dict(self.env, FAKE_CURL_HTTP_FAIL='1')
        res = self.run_cmd('--apply', env=env)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn('Verification failed', res.stderr)
        self.assertFalse(self.override.exists())
        self.assert_active(False)
        self.assertIn('src.setup_app', self.unit.read_text())

    def test_bounded_retry_tolerates_uvicorn_bind_delay(self):
        (self.root / 'data/webui_password.json').touch()
        env = dict(self.env, FAKE_CURL_CONNECTION_REFUSED_ONCE='1')
        res = self.run_cmd('--apply', env=env)
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assert_active()
        self.assertTrue(self.override.exists())
        self.assertTrue((self.root / 'systemd/curl-tried').exists())

    def test_wrong_effective_entrypoint_fails_closed(self):
        (self.root / 'data/webui_password.json').touch()
        res = self.run_cmd('--apply', env=dict(self.env, FAKE_EFFECTIVE_OLD='1'))
        self.assertNotEqual(res.returncode, 0)
        self.assertIn('ExecStart', res.stderr)
        self.assertFalse(self.override.exists())
        self.assert_active(False)

    def test_stopped_webui_remains_stopped_after_migration_and_rollback(self):
        (self.root / 'data/webui_password.json').touch()
        (self.root / 'systemd/active').unlink()
        res = self.run_cmd('--apply')
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assert_active(False)
        self.assertTrue(self.override.exists())
        backups = sorted((self.root / 'test-backups').glob('*/UDITBOT_WEBUI_BACKUP_V1'))
        self.assertEqual(len(backups), 1)
        rb = self.run_cmd('--rollback', str(backups[0].parent))
        self.assertEqual(rb.returncode, 0, rb.stdout + rb.stderr)
        self.assert_active(False)
        self.assertFalse(self.override.exists())
        self.assertIn('src.setup_app', self.unit.read_text())


if __name__ == '__main__':
    unittest.main()
