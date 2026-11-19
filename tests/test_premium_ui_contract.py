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
        for fragment in ('id="start"', 'id="stop"', 'id="health"',
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


class ApprovedScreenshotReplicaContracts(unittest.TestCase):
    """The approved 2x2 cards must wrap the real existing form fields."""
    def test_four_screenshot_cards_and_advanced_preservation(self):
        content = read('src/setup_app.py')
        for title in ('Bot Configuration', 'Risk &amp; Protection', 'AMO Settings',
                      'Market &amp; Data', 'Advanced Configuration'):
            self.assertIn(title, content)
        for field in ('rp-mode', 'rp-sources', 'rp-order-cap', 'rp-fno-budget',
                      'rp-equity-sl', 'rp-equity-tp', 'rp-quote-age',
                      'rp-price-policy', 'rp-scrip-refresh'):
            self.assertIn('id="' + field + '"', content)
        # Original full 6 sections and all known safety inputs stay in the template.
        for section in ('setup-execution','setup-telegram','setup-broker','setup-sizing',
                        'setup-risk','setup-protection'):
            self.assertIn('id="' + section + '"', content)
        for field in ('is_live_mode', 'equity_active', 'fno_active',
                      'monitored_channel_ids', 'max_order_amount_inr',
                      'equity_budget_inr', 'fno_capital_per_trade',
                      'equity_sl_pct', 'equity_target_pct', 'fno_sl_pct',
                      'fno_target_pct', 'dhan_pin', 'dhan_totp_secret',
                      'csrf_token','expected_revision'):
            self.assertIn('name="' + field + '"', content)
        self.assertIn('/*PREMIUM_REPLICA_JS*/', content)
        self.assertIn('.replace("/*PREMIUM_REPLICA_JS*/", premium_replica_js())', content)

    def test_no_auto_trade_or_fabricated_verified_states(self):
        script = read('src/premium_replica.js')
        self.assertIn("json('/control/status')", script)
        self.assertIn('data.runtime_verified===true', script)
        self.assertIn('Reconciliation: not verified', script)
        self.assertIn('OCO Guard: not verified', script)
        self.assertIn("target.disabled=true", script)
        self.assertIn('expected_revision:verifiedStatus.config_file_revision', script)
        self.assertIn('csrf_token:form.elements.namedItem', script)
        self.assertNotIn('/v2/orders', script)
        self.assertNotIn('systemctl', script)
        self.assertNotIn('trading_bot.service', script)

    def test_existing_control_renderer_is_unchanged(self):
        content = read('src/control_center_ui.py')
        self.assertIn("requestJSON('/control/status')", content)
        self.assertIn('/setup/service-control', content)

    @unittest.skipUnless(shutil.which('node'), 'Node.js unavailable')
    def test_replica_js_syntax(self):
        result = subprocess.run(['node', '--check', str(ROOT / 'src/premium_replica.js')],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()