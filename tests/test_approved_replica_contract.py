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
