"""Offline premium WebUI contracts; no broker, trading or systemd mutation."""
from pathlib import Path
import ast
import re
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


class FocusedV17PresentationContracts(unittest.TestCase):
    def test_six_setup_views_keep_one_native_54_field_form(self):
        tree = ast.parse(read('src/setup_app.py'))
        html = next(ast.literal_eval(node.value) for node in tree.body
                    if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == 'HTML_TEMPLATE'
                            for target in node.targets))
        self.assertEqual(html.count('<form method="POST" action="/setup">'), 1)
        form = html.split('<form method="POST" action="/setup">')[1].split('</form>')[0]
        names = re.findall(r'<(?:input|select|textarea)[^>]*name="([^"]+)"', form)
        self.assertEqual(len(names), 54)
        self.assertEqual(len(set(names)), 54)
        self.assertTrue({'csrf_token','expected_revision','full_setup_form','confirm_live',
                         'is_live_mode','equity_active','fno_active'}.issubset(names))
        for key in ('is_live_mode','equity_active','fno_active'):
            tag = re.search(r'<input[^>]*name="' + key + r'"[^>]*>', form)
            self.assertIsNotNone(tag)
            self.assertIn('type="checkbox"', tag.group())
            self.assertIn('value="true"', tag.group())
        self.assertNotIn('class="slider"', html)
        self.assertNotIn('class="switch"', html)
        self.assertEqual(html.count('class="section-card"'), 6)

    def test_single_column_sticky_free_setup_desktop(self):
        css = read('src/premium_ui.css')
        self.assertIn('max-width:1040px', css)
        self.assertIn('column-count:1', css)
        self.assertNotIn('column-count:2', css)
        self.assertNotIn('backdrop-filter:blur(9px)', css)
        self.assertIn('.setup-section-jumps{position:static;', css)
        self.assertIn('grid-template-columns:repeat(3,minmax(0,1fr))', css)
        self.assertIn('@media(max-width:700px)', css)
        self.assertNotIn('transform:skewX(-24deg)', css)
        js = read('src/premium_ui.js')
        for section in ('execution','telegram','broker','sizing','risk','protection'):
            self.assertIn("'setup-" + section + "'", js)
        self.assertIn("root.classList.toggle('v17p-setup-single',ids.includes(selected))", js)

    def test_accessible_type_floor_weights_and_iphone_controls(self):
        css = read('src/premium_ui.css')
        px = [float(x) for x in re.findall(r'font-size\\s*:\\s*([\\d.]+)px', css)]
        self.assertTrue(px)
        self.assertGreaterEqual(min(px), 12)
        weights = {int(x) for x in re.findall(r'font-weight\\s*:\\s*(\\d+)', css)}
        self.assertTrue(weights.issubset({400,500,600,700}), weights)
        self.assertIn('font-variant-numeric:tabular-nums', css)
        self.assertIn('font-size:16px!important;line-height:1.45', css)
        self.assertIn('.setup-choice:focus-within', css)

    def test_svg_marks_favicon_and_six_category_icons(self):
        from src.premium_ui import _UB_MARK, premium_nav, premium_styles
        import xml.etree.ElementTree as ET
        ET.fromstring(_UB_MARK)
        self.assertIn('data:image/svg+xml,', premium_styles())
        self.assertNotIn('⌁', read('src/premium_ui.py'))
        self.assertEqual(premium_nav('setup').count('class="v17p-ub-mark"'), 2)
        js = read('src/premium_ui.js')
        for icon in ('setup-execution','setup-telegram','setup-broker',
                     'setup-sizing','setup-risk','setup-protection'):
            self.assertIn("'" + icon + "':[", js)
        self.assertIn('document.createElementNS', js)
        for old in ("'♧'", "'◈'", "'▤'", "'⬡'", "'♡'"):
            self.assertNotIn(old, js)

    def test_verified_runtime_controls_all_running_and_mode_visuals(self):
        cc = read('src/control_center_ui.py')
        css = read('src/premium_ui.css')
        self.assertIn("const verifiedRunning=state==='RUNNING'&&s.runtime_verified===true", cc)
        self.assertIn("const mode=verifiedRunning&&['LIVE','TEST'].includes(reportedMode)?reportedMode:'UNVERIFIED'", cc)
        self.assertIn("setText('v17-db-service',serviceReadout)", cc)
        self.assertIn("classList.toggle('is-test',verifiedRunning&&mode==='TEST')", cc)
        self.assertIn("classList.toggle('mode-test',verifiedRunning&&mode==='TEST')", cc)
        self.assertIn('html.v17p-preview-shell.premium-control #v17-control>.buttons #restart{background:#d97706;color:#fff', css)
        self.assertIn('#v17p-mode-live.selected{background:#16a34a', css)
        self.assertIn('#v17p-mode-test.selected{background:#2563eb', css)
        self.assertIn('v17p-verified-beacon 1.35s ease-in-out infinite', css)
        self.assertIn('@media(prefers-reduced-motion:reduce)', css)
        self.assertNotIn('#v17-control #mode{position:absolute', css)



if __name__ == '__main__':
    unittest.main()