"""Approved-screenshot WebUI contracts. Pure presentation checks; no broker calls."""
from __future__ import annotations
import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def template_constants():
    tree = ast.parse((ROOT / 'src/setup_app.py').read_text(encoding='utf-8'))
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    values[target.id] = node.value.value
    return values


class ApprovedReplicaContract(unittest.TestCase):
    def test_four_card_reference_structure(self):
        values = template_constants()
        board = values['APPROVED_REFERENCE_BOARD']
        self.assertEqual(board.count('class="replica-card replica-'), 4)
        for title in ('Bot Configuration', 'Risk &amp; Protection',
                      'AMO Settings', 'Market &amp; Data'):
            self.assertIn(title, board)
        self.assertIn('id="replicaLive"', board)
        self.assertIn('id="replicaTest"', board)
        self.assertIn('id="replicaAMOEquity"', board)
        self.assertIn('id="replicaAMOFno"', board)

    def test_legacy_forms_and_safety_still_available(self):
        page = template_constants()['HTML_TEMPLATE']
        for name in ('is_live_mode','confirm_live','csrf_token','expected_revision',
                     'equity_budget_inr','fno_capital_per_trade','fno_max_lots',
                     'equity_sl_pct','equity_target_pct','fno_sl_pct','fno_target_pct',
                     'max_quote_age_sec','equity_cmp_execution_policy',
                     'scrip_master_max_age_sec'):
            self.assertIn('name="' + name + '"', page)
        self.assertIn('fetch(\'/setup/save-apply\'', page)
        self.assertIn('id="replicaAdvanced"', page)
        self.assertIn('id="replicaApply"', page)
        self.assertIn('name="full_setup_form"', page)

    def test_mirror_fields_are_not_submitted_twice(self):
        values = template_constants()
        board = values['APPROVED_REFERENCE_BOARD']
        self.assertNotIn('name="equity_budget_inr"', board)
        self.assertNotIn('name="fno_sl_pct"', board)
        self.assertNotIn('name="fno_capital_per_trade"', board)
        script = (ROOT / 'src/approved_replica.js').read_text(encoding='utf-8')
        self.assertIn('original.value = input.value', script)
        self.assertIn('refreshMirrors()', script)
        self.assertIn("form.requestSubmit(get('replicaApply'))", script)
        self.assertIn("if (!brokerState || dirty())", script)

    def test_no_invented_verified_reconciliation_or_oco(self):
        board = template_constants()['HTML_TEMPLATE']
        self.assertIn('Reconciliation: Not verified', board)
        self.assertIn('OCO Guard: Not verified', board)
        script = (ROOT / 'src/approved_replica.js').read_text(encoding='utf-8')
        self.assertIn("state.runtime_verified === true", script)
        self.assertIn("state.equity_amo_can_edit !== true", script)
        self.assertIn("state.fno_amo_can_edit !== true", script)
        self.assertIn("'/control/amo/fno'", script)
        self.assertIn('csrf_token:csrf', script)
        self.assertIn('expected_revision:brokerState.config_file_revision', script)

    def test_css_desktop_and_mobile(self):
        css = (ROOT / 'src/approved_replica.css').read_text(encoding='utf-8')
        self.assertIn('grid-template-columns:repeat(2,minmax(0,1fr))', css)
        self.assertIn('max-width:760px', css)
        self.assertIn('.replica-footer', css)
        self.assertIn('.replica-advanced', css)
        self.assertIn('approved', css)

    def test_asset_loading_and_existing_routes(self):
        module = (ROOT / 'src/premium_ui.py').read_text(encoding='utf-8')
        self.assertIn('approved_replica_styles', module)
        self.assertIn('approved_replica_js', module)
        cc = (ROOT / 'src/control_center_ui.py').read_text(encoding='utf-8')
        self.assertIn('id="amo-orders"', cc)
        self.assertIn('id="system-health"', cc)
        self.assertIn("requestJSON('/control/status')", cc)
        self.assertIn('/setup/service-control', cc)


if __name__ == '__main__':
    unittest.main()


class CSSOnlyReliabilityContracts(unittest.TestCase):
    """Lock original configuration semantics while skinning the three pages."""

    def test_exact_54_form_control_contract(self):
        import hashlib
        import json
        from html.parser import HTMLParser

        class SavedFields(HTMLParser):
            def __init__(self):
                super().__init__()
                self.fields = []
            def handle_starttag(self, tag, attrs):
                if tag in {'input', 'select', 'textarea'} and dict(attrs).get('name'):
                    self.fields.append((tag, tuple(sorted(attrs))))

        parser = SavedFields()
        parser.feed(template_constants()['HTML_TEMPLATE'])
        encoded = json.dumps(parser.fields, ensure_ascii=False, separators=(',', ':'))
        self.assertEqual(len(parser.fields), 54)
        self.assertEqual(hashlib.sha256(encoded.encode()).hexdigest(),
                         '89f72336971cddc6a4cee394aaf75a98e1b94908ff74b5d3fb4e591b8dfcbf80',
                         'An original field name/type/constraint/default changed; UI-only contract violated')

    def test_only_existing_advanced_disclosure_initial_state_changed(self):
        from hashlib import sha256
        original_script = (ROOT / 'src/approved_replica.js').read_text(encoding='utf-8')
        target = 'advanced.open = true;  // Display the unchanged working Setup controls by default.'
        self.assertEqual(original_script.count(target), 1)
        normalized = original_script.replace(target, 'advanced.open = false;')
        self.assertEqual(sha256(normalized.encode()).hexdigest(),
                         'b163edc8212f1db80c99ba1049cda9deb1fe68667e978d2a7da08cddde4eacc7',
                         'A nonvisual action, request or edit listener was changed')

    def test_all_original_configuration_cards_are_visible(self):
        page = template_constants()['HTML_TEMPLATE']
        for key in ('setup-execution', 'setup-telegram', 'setup-broker',
                    'setup-sizing', 'setup-risk', 'setup-protection'):
            self.assertIn('id="'+key+'"', page)
        self.assertIn('<details id="replicaAdvanced"', page)
        self.assertEqual(page.count('class="section-card"'), 6)
        css = (ROOT / 'src/approved_replica.css').read_text(encoding='utf-8')
        self.assertIn('#replicaAdvanced.replica-advanced[open]>.replica-advanced-body', css)
        self.assertIn('grid-template-columns:repeat(2,minmax(0,1fr))', css)

    def test_no_new_script_or_backend_api_in_global_skin(self):
        css = (ROOT / 'src/premium_ui.css').read_text(encoding='utf-8')
        self.assertIn('Shared approved header and card geometry', css)
        self.assertIn('.premium-control main>header', css)
        self.assertIn('.premium-designer main>.top', css)
        self.assertNotIn('@import', css)
        # Setup's real backend remains one Save & Apply endpoint, not a new API.
        page = template_constants()['HTML_TEMPLATE']
        self.assertEqual(page.count("fetch('/setup/save-apply'"), 1)
        self.assertIn('name="csrf_token"', page)
        self.assertIn('name="expected_revision"', page)
        self.assertIn('name="confirm_live"', page)
