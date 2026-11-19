"""System-wide presentation contracts. No network, broker or systemd calls."""
from __future__ import annotations

import ast
from html.parser import HTMLParser
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    content = (ROOT / path).read_text(encoding='utf-8')
    if path.endswith('.py'):
        ast.parse(content)
    return content


def template_constant(name):
    tree = ast.parse(read('src/setup_app.py'))
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return node.value.value
    raise AssertionError('Missing template constant: ' + name)


class InputNames(HTMLParser):
    def __init__(self):
        super().__init__()
        self.names = []

    def handle_starttag(self, tag, attrs):
        if tag in ('input', 'select', 'textarea'):
            name = dict(attrs).get('name')
            if name:
                self.names.append(name)


class SystemwideUIContracts(unittest.TestCase):
    def test_complete_configuration_is_visible_without_advanced_drawer(self):
        html = template_constant('HTML_TEMPLATE')
        self.assertIn('<section id="replicaAdvanced"', html)
        self.assertNotIn('<details id="replicaAdvanced"', html)
        self.assertIn('All Trading &amp; System Settings', html)
        self.assertEqual(html.count('class="section-card"'), 6)
        self.assertIn('id="replicaApply"', html)
        self.assertIn('name="confirm_live"', html)
        self.assertIn("fetch('/setup/save-apply'", html)

    def test_all_authoritative_form_fields_are_unique(self):
        fields = InputNames()
        fields.feed(template_constant('HTML_TEMPLATE'))
        self.assertEqual(len(fields.names), 54)
        self.assertEqual(len(set(fields.names)), 54)
        for name in ('csrf_token', 'expected_revision', 'full_setup_form', 'is_live_mode',
                     'confirm_live', 'fno_capital_per_trade', 'equity_budget_inr',
                     'tg_api_hash', 'dhan_totp_secret', 'max_daily_loss_inr',
                     'fno_target_pct', 'equity_cmp_execution_policy'):
            self.assertIn(name, fields.names)
        self.assertNotIn('name="equity_budget_inr"', template_constant('APPROVED_REFERENCE_BOARD'))

    def test_all_pages_use_exact_shared_header_component(self):
        from src.premium_ui import unified_header, systemwide_styles, systemwide_script
        for name in ('control', 'designer'):
            rendered = unified_header(name)
            self.assertIn('class="replica-head system-unified-head"', rendered)
            self.assertIn('Reconciliation: Not verified', rendered)
            self.assertIn('OCO Guard: Not verified', rendered)
            self.assertIn('◌ UNVERIFIED', rendered)
        with self.assertRaises(ValueError):
            unified_header('unsupported')
        self.assertIn('replica-advanced-body', systemwide_styles())
        self.assertIn("fetch('/control/status'", systemwide_script())

    def test_control_service_action_and_order_controls_not_replaced(self):
        cc = read('src/control_center_ui.py')
        for fragment in ('<!--UNIFIED_HEADER-->', 'premium_nav("control")',
                         '/setup/service-control', '/control/amo/fno',
                         "requestJSON('/control/status')", "requestJSON('/control/health')",
                         'id="start"', 'id="stop"', 'id="health"',
                         'id="equity-amo"', 'id="fno-amo"', 'id="refresh-orders"'):
            self.assertIn(fragment, cc)

    def test_designer_editor_and_submission_are_preserved(self):
        designer = read('src/notifier/message_designer_ui.py')
        for fragment in ('<!--UNIFIED_HEADER-->', 'premium_nav("designer")',
                         '/setup/telegram-designer/save',
                         '/setup/telegram-designer/config', 'id="preview"',
                         'id="selected"', 'id="available"', 'id="save"',
                         'Templates are opt-in'):
            self.assertIn(fragment, designer)

    def test_advanced_assets_do_not_contact_broker_or_mutate_services(self):
        js = read('src/systemwide_ui.js')
        for fragment in ('/control/amo/equity', '/control/amo/fno',
                         '/setup/service-control', '/setup/save-apply',
                         '/orders', 'systemctl'):
            self.assertNotIn(fragment, js)
        self.assertIn("state.runtime_verified === true", js)
        self.assertIn("'UNVERIFIED'", js)

    def test_css_responsive_and_no_status_fabrication(self):
        styles = read('src/systemwide_ui.css')
        self.assertIn('@media(max-width:760px)', styles)
        self.assertIn('columns:2', styles)
        self.assertIn('system-unified-head', styles)
        self.assertIn('.premium-control', styles)
        self.assertIn('.premium-designer', styles)
        self.assertNotIn('Reconciliation: Healthy', styles)
        script = read('src/approved_replica.js')
        self.assertNotIn('advanced.open = false', script)
        self.assertIn("get('setup-telegram').scrollIntoView", script)
        self.assertIn('original.value = input.value', script)


if __name__ == '__main__':
    unittest.main()
