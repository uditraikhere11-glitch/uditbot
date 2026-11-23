"""Approved production UI: six native cards; no proxy controls or broker writes."""
from __future__ import annotations
import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


def source(rel):
    path = ROOT / rel
    raw = path.read_bytes()
    ast.parse(raw.decode('utf-8')) if path.suffix == '.py' else None
    return raw.decode('utf-8')


def html_template():
    tree = ast.parse(source('src/setup_app.py'))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(t, ast.Name) and t.id == 'HTML_TEMPLATE' for t in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError('Setup HTML template not found')


class ApprovedVisualOnlyContract(unittest.TestCase):
    def test_single_native_setup_form_and_live_confirmation_contract(self):
        # Markup can improve without freezing the historical visual hash.
        page = html_template()
        self.assertEqual(page.count('<form method="POST" action="/setup">'), 1)
        self.assertEqual(page.count('</form>'), 1)
        for name in ('csrf_token', 'expected_revision', 'full_setup_form',
                     'confirm_live', 'is_live_mode', 'equity_active', 'fno_active'):
            self.assertEqual(len(re.findall(r'\\bname="' + name + r'"', page)), 1, name)
        for name in ('is_live_mode', 'equity_active', 'fno_active'):
            self.assertRegex(page, r'<input type="checkbox" name="' + name + r'" value="true"')
        self.assertNotIn('class="slider"', page)
        self.assertNotIn('class="switch"', page)

    def test_six_existing_cards_and_exact_54_unique_field_names(self):
        page = html_template()
        self.assertEqual(page.count('class="section-card"'), 6)
        for section in ('execution', 'telegram', 'broker', 'sizing', 'risk', 'protection'):
            self.assertEqual(page.count('id="setup-' + section + '"'), 1)
        form = page.split('<form method="POST" action="/setup">', 1)[1].split('</form>', 1)[0]
        names = re.findall(r'<(?:input|select|textarea)\b[^>]*\bname="([^"]+)"', form)
        self.assertEqual(len(names), 54)
        self.assertEqual(len(names), len(set(names)))
        self.assertIn('csrf_token', names)
        self.assertIn('expected_revision', names)
        self.assertIn('confirm_live', names)
        self.assertIn('max_quote_age_sec', names)
        self.assertNotIn('data-mirror', form)
        self.assertNotIn('replicaAdvanced', page)
        self.assertNotIn('replicaBoard', page)

    def test_quote_2_valid_under_existing_rules(self):
        page = html_template()
        tag = re.search(r'<input\b[^>]*name="max_quote_age_sec"[^>]*>', page).group()
        self.assertIn('step="0.5"', tag)
        self.assertNotIn('min=', tag)
        self.assertIn("fetch('/setup/save-apply'", page)

    def test_six_visual_tabs_use_only_three_authenticated_page_routes(self):
        # V17's approved preview has six visible tabs, not six new FastAPI
        # pages. Dashboard/Control/AMO/Health are hash-only native views.
        from urllib.parse import urlsplit
        helper = source('src/premium_ui.py')
        tree = ast.parse(helper)
        pages_decl = next(node for node in tree.body
                          if isinstance(node, ast.Assign) and
                          any(isinstance(target, ast.Name) and target.id == '_PAGES'
                              for target in node.targets))
        self.assertEqual(set(ast.literal_eval(pages_decl.value.args[0])),
                         {'control', 'setup', 'designer'})
        nav_fn = next(node for node in tree.body
                      if isinstance(node, ast.FunctionDef) and node.name == 'premium_nav')
        entries = next(ast.literal_eval(node.value) for node in nav_fn.body
                       if isinstance(node, ast.Assign) and
                       any(isinstance(target, ast.Name) and target.id == 'destinations'
                           for target in node.targets))
        hrefs = {key: href for key, href, _icon, _label in entries}
        self.assertEqual(hrefs, {
            'dashboard': '/#dashboard', 'control': '/#control',
            'setup': '/setup', 'telegram': '/setup/telegram-designer',
            'amo': '/#amo', 'health': '/#health',
        })
        self.assertEqual({urlsplit(href).path for href in hrefs.values()},
                         {'/', '/setup', '/setup/telegram-designer'})
        for forbidden in ('replica-extra-nav', 'Trading &amp; Orders',
                          'Logs &amp; Monitoring', 'approved_replica_'):
            self.assertNotIn(forbidden, helper)

    def test_presentation_cannot_rewrite_or_mirror_inputs(self):
        css = source('src/premium_ui.css')
        self.assertIn('AuditBot approved professional dashboard', css)
        self.assertNotIn('column-count:2', css)
        self.assertIn('column-count:1', css)
        self.assertIn('.premium-designer', css)
        self.assertIn('.premium-control', css)
        js = source('src/premium_ui.js')
        for forbidden in ('FormData(', '.value=', 'requestSubmit(', '/setup/save-apply', '/control/amo'):
            self.assertNotIn(forbidden, js)

    def test_actual_control_center_and_designer_contracts(self):
        cc = source('src/control_center_ui.py')
        for token in ('id="start"', 'id="stop"', 'id="v17-health-run"',
                      'id="equity-amo"', 'id="fno-amo"', 'id="amo-orders"',
                      'id="system-health"', '/setup/service-control', '/control/status'):
            self.assertIn(token, cc)
        designer = source('src/notifier/message_designer_ui.py')
        for token in ('id="event"', 'id="enabled"', 'id="selected"',
                      'id="available"', 'id="preview"', 'id="save"',
                      'id="export"', 'id="import"'):
            self.assertIn(token, designer)


if __name__ == '__main__':
    unittest.main()
