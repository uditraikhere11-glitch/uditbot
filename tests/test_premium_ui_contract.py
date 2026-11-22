"""Offline premium WebUI contracts; no broker, trading or systemd mutation."""
from pathlib import Path
import ast
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    content = (ROOT / path).read_text(encoding='utf-8')
    if path.endswith('.py'):
        ast.parse(content)
    return content


class PremiumThemeContracts(unittest.TestCase):
    def test_navigation_has_only_real_authenticated_pages(self):
        from src.premium_ui import premium_nav
        for page in ('control', 'setup', 'designer'):
            markup = premium_nav(page)
            for route in ('/', '/setup', '/setup/telegram-designer'):
                self.assertIn('href="' + route + '"', markup)
            self.assertEqual(markup.count('aria-current="page"'), 1)
            self.assertIn('aria-label="AuditBot navigation"', markup)
        with self.assertRaises(ValueError):
            premium_nav('unknown')

    def test_styles_are_loaded_from_local_bundled_file(self):
        from src.premium_ui import premium_styles
        html = premium_styles()
        self.assertIn('premium-setup', html)
        self.assertIn('premium-control', html)
        self.assertIn('premium-designer', html)
        self.assertIn('#simpleSaveStatus[hidden]', html)

    @unittest.skipUnless(shutil.which('node'), 'Node.js not installed')
    def test_navigation_js_syntax(self):
        result = subprocess.run(['node', '--check', str(ROOT / 'src/premium_ui.js')],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)


class PresentationIntegrationContracts(unittest.TestCase):
    def test_setup_save_apply_truthful_and_safe(self):
        content = read('src/setup_app.py')
        self.assertIn('HTML_TEMPLATE', content)
        self.assertNotIn('Next Operational Steps:', content)
        self.assertNotIn('guide-card', content)
        self.assertNotIn('window.alert(result.message)', content)
        self.assertNotIn('status.scrollIntoView', content)
        for fragment in ('<!--PREMIUM_NAV-->', '<!--PREMIUM_STYLES-->', '/*PREMIUM_NAV_JS*/',
                         'id="premNavOpen"', 'id="toastMessage"', 'id="toastDismiss"',
                         "fetch('/setup/save-apply'", "result.status === 'RESTARTED' && result.restart_performed === true",
                         'toastMessage.textContent = String(value)', "'error', 0", 'name="csrf_token"',
                         'name="expected_revision"', 'name="confirm_live"',
                         '<body>\n  <div class="container">\n'):
            self.assertIn(fragment, content, fragment)

    def test_control_center_unchanged_service_contract(self):
        content = read('src/control_center_ui.py')
        for fragment in ('id="start"', 'id="stop"', 'id="v17-health-run"',
                         'id="equity-amo"', 'id="fno-amo"', '/setup/service-control',
                         'requestJSON(\'/control/status\')', '/control/amo/fno',
                         'premium_nav("control")'):
            self.assertIn(fragment, content)
        from src.control_center_ui import render_control_center
        rendered = render_control_center('TEST-&"<>')
        self.assertIn('href="/setup/telegram-designer"', rendered)
        self.assertIn('aria-current="page"', rendered)
        self.assertIn('TEST-&amp;&quot;&lt;&gt;', rendered)
        self.assertNotIn('<!--PREMIUM_NAV-->', rendered)

    def test_designer_broker_labels_and_opt_in_unchanged(self):
        content = read('src/notifier/message_designer_ui.py')
        for fragment in ('Templates are opt-in', '/setup/telegram-designer/save',
                         '/setup/telegram-designer/config', 'textContent', 'premium_nav("designer")'):
            self.assertIn(fragment, content)
        from src.notifier.message_designer_ui import render_page
        rendered = render_page('TOKEN-&"<>')
        self.assertIn('aria-current="page"', rendered)
        self.assertIn('href="/setup/telegram-designer"', rendered)
        self.assertIn('TOKEN-&amp;&quot;&lt;&gt;', rendered)
        self.assertNotIn('<!--PREMIUM_STYLES-->', rendered)


if __name__ == '__main__':
    unittest.main()