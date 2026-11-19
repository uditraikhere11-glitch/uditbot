"""Approved production UI: six native cards; no proxy controls or broker writes."""
from __future__ import annotations
import ast
import hashlib
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
KNOWN_WORKING_SETUP_BLOB = 'c8a81e63d3045229c97cbe7e5f58085b2395e6bf'


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
    def test_exact_previous_working_setup(self):
        raw = (ROOT / 'src/setup_app.py').read_bytes()
        data = b'blob ' + str(len(raw)).encode() + b'\0' + raw
        self.assertEqual(hashlib.sha1(data).hexdigest(), KNOWN_WORKING_SETUP_BLOB)

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

    def test_navigation_is_three_real_pages_only(self):
        helper = source('src/premium_ui.py')
        self.assertIn("('control', '/',", helper)
        self.assertIn("('setup', '/setup',", helper)
        self.assertIn("('designer', '/setup/telegram-designer',", helper)
        self.assertNotIn('replica-extra-nav', helper)
        self.assertNotIn('Trading &amp; Orders', helper)
        self.assertNotIn('Logs &amp; Monitoring', helper)
        self.assertNotIn('approved_replica_', helper)

    def test_presentation_cannot_rewrite_or_mirror_inputs(self):
        css = source('src/premium_ui.css')
        self.assertIn('AuditBot approved professional dashboard', css)
        self.assertIn('column-count:2', css)
        self.assertIn('column-count:1', css)
        self.assertIn('.premium-designer', css)
        self.assertIn('.premium-control', css)
        js = source('src/premium_ui.js')
        for forbidden in ('FormData(', '.value=', 'requestSubmit(', '/setup/save-apply', '/control/amo'):
            self.assertNotIn(forbidden, js)

    def test_actual_control_center_and_designer_contracts(self):
        cc = source('src/control_center_ui.py')
        for token in ('id="start"', 'id="stop"', 'id="health"',
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
