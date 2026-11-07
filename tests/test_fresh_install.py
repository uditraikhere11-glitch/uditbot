"""Offline clean-install regression tests. Never contact Telegram or Dhan."""
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from deploy.fresh_config import SetupError, channel_ids, create_config, optional_chat_id

ROOT = Path(__file__).resolve().parents[1]


class FreshInstallTests(unittest.TestCase):
    def test_shell_syntax_and_preflight(self):
        script = ROOT / 'deploy' / 'fresh_install.sh'
        self.assertEqual(subprocess.run(['bash', '-n', str(script)], capture_output=True).returncode, 0)
        result = subprocess.run(['bash', str(script), '--preflight'], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_explicit_sources_not_wildcard(self):
        with self.assertRaises(SetupError):
            channel_ids('', required=True)
        with self.assertRaises(SetupError):
            channel_ids('0', required=True)
        with self.assertRaises(SetupError):
            channel_ids('-1001,-1001', required=True)
        with self.assertRaises(SetupError):
            optional_chat_id('-123,-456')
        self.assertEqual(channel_ids('-100123,-987', required=True), '-100123,-987')

    def test_no_overwrite_existing_configuration(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'uditbot'
            home = Path(d) / 'home'
            root.mkdir()
            home.mkdir()
            (root / '.env').write_text('PREEXISTING=PRIVATE')
            with self.assertRaises(SetupError):
                create_config(root, home)
            self.assertEqual((root / '.env').read_text(), 'PREEXISTING=PRIVATE')

    def test_private_new_config_test_mode_and_empty_runtime(self):
        answers = [
            'Operator', 'BrandNewBot', '1234567', 'a' * 32,
            '-10012345678', '-9999', '', '', '', '', '1105508885',
            '', '', 'synthetic.testing.token'
        ]
        iterator = iter(answers)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'uditbot'
            home = Path(d) / 'home'
            root.mkdir()
            home.mkdir()
            (root / 'data').mkdir()
            (root / 'data' / 'holidays.json').write_text('[]')
            with patch('deploy.fresh_config.input', side_effect=lambda *_: next(iterator), create=True), \
                 patch('deploy.fresh_config.getpass.getpass', side_effect=lambda *_: next(iterator)):
                create_config(root, home)
            env = root / '.env'
            body = env.read_text()
            self.assertIn('TRADING_MODE=TEST\n', body)
            self.assertIn('FNO_AMO_ENABLED=false\n', body)
            self.assertIn('EQUITY_AMO_ENABLED=false\n', body)
            self.assertIn('MONITORED_CHANNEL_IDS=-10012345678\n', body)
            self.assertNotIn('DHAN_ACCESS_TOKEN=', body)
            self.assertEqual((home / '.dhan_token').read_text(), 'synthetic.testing.token\n')
            self.assertEqual(stat.S_IMODE(env.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((home / '.dhan_token').stat().st_mode), 0o600)
            self.assertFalse((root / 'data' / 'trade_journal.db').exists())
            with self.assertRaises(SetupError):
                create_config(root, home)

    def test_preexisting_journal_refused(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'uditbot'
            home = Path(d) / 'home'
            (root / 'data').mkdir(parents=True)
            home.mkdir()
            (root / 'data' / 'trade_journal.db').write_bytes(b'private')
            with self.assertRaises(SetupError):
                create_config(root, home)

    def test_installer_no_live_autostart(self):
        script = (ROOT / 'deploy' / 'fresh_install.sh').read_text()
        self.assertIn('deploy/install_vm_services.sh', script)
        self.assertIn('AUDITBOT_SKIP_TOKEN_TIMER_START=1', script)
        self.assertIn('TRADING_MODE=TEST', script)
        self.assertIn('systemctl is-active --quiet trading_bot.service', script)
        self.assertNotIn('systemctl start trading_bot.service', script)
        self.assertNotIn('systemctl enable --now trading_bot.service', script)


if __name__ == '__main__':
    unittest.main()
