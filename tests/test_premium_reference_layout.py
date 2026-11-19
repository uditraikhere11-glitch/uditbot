"""Offline contracts for screenshot-aligned Setup UI; no systemd, Dhan or network."""
import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


def src(name):
    return (ROOT / name).read_text(encoding="utf-8")


class ScreenshotReferenceContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.setup_source = src("src/setup_app.py")
        ast.parse(cls.setup_source)
        tree = ast.parse(cls.setup_source)
        cls.template = next(node.value.value for node in tree.body
                            if isinstance(node, ast.Assign)
                            and any(isinstance(t, ast.Name) and t.id == "HTML_TEMPLATE"
                                    for t in node.targets))
        cls.css = src("src/premium_reference.css")
        cls.js = src("src/premium_reference.js")

    def test_exact_four_cards_in_visual_order(self):
        labels = ["Bot Configuration", "Risk &amp; Protection", "AMO Settings", "Market &amp; Data"]
        positions = [self.template.index(label) for label in labels]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('class="dash-board-grid"', self.template)
        self.assertEqual(self.template.count('class="dash-card dash-'), 4)
        self.assertIn('grid-template-rows:296px 232px', self.css)
        self.assertIn('grid-template-columns:minmax(0,1fr) minmax(0,1fr)', self.css)

    def test_one_form_one_submission_path_preserved(self):
        self.assertEqual(self.template.count('<form method="POST" action="/setup">'), 1)
        self.assertEqual(self.template.split('</form>',1)[0].count('name="csrf_token"'), 1)
        self.assertEqual(self.template.split('</form>',1)[0].count('name="expected_revision"'), 1)
        self.assertEqual(self.template.split('</form>',1)[0].count('name="full_setup_form"'), 1)
        self.assertEqual(self.template.count('type="submit" class="btn-submit"'), 1)
        self.assertIn("fetch('/setup/save-apply'", self.template)
        self.assertIn('new FormData(form)', self.template)
        self.assertIn("result.status === 'RESTARTED' && result.restart_performed === true", self.template)
        self.assertNotIn('status.scrollIntoView', self.template)

    def test_no_duplicate_security_or_sizing_inputs(self):
        for key in ('is_live_mode','equity_active','fno_active','equity_sl_pct',
                    'equity_target_pct','equity_budget_inr','fno_capital_per_trade',
                    'fno_max_lots','equity_cmp_execution_policy','max_quote_age_sec',
                    'scrip_master_max_age_sec','dhan_pin','dhan_totp_secret'):
            self.assertEqual(len(re.findall(r'name="' + key + r'"',self.template.split('</form>',1)[0])), 1, key)
        self.assertIn("slot.append(group)",self.js)
        self.assertNotIn('cloneNode(',self.js)

    def test_amo_fail_closed_and_no_auto_health_poll(self):
        self.assertIn('id="dashEquityAmo"',self.template)
        self.assertIn('id="dashFnoAmo"',self.template)
        self.assertIn('type="checkbox" role="switch" disabled',self.template)
        self.assertIn("fetch('/control/status'",self.js)
        self.assertIn("fetch('/control/amo/'+which",self.js)
        self.assertIn('expected_revision:current.config_file_revision',self.js)
        self.assertIn("csrf_token:single('csrf_token').value",self.js)
        self.assertIn('NO automatic SL or target',self.js)
        self.assertNotIn('fetch(\'/control/health\'',self.js)
        self.assertNotIn('window.alert(',self.js)

    def test_fallback_does_not_hide_legacy_form_without_js(self):
        self.assertIn('id="dashOverview" class="dash-overview" hidden',self.template)
        self.assertIn('id="dashAdvanced" class="dash-advanced" hidden',self.template)
        self.assertIn("querySelectorAll(':scope > .section-card').length !== 6",self.js)
        self.assertIn('if (fields.length !== 1)',self.js)
        self.assertIn('overview.hidden = false',self.js)
        self.assertIn('Advanced Configuration',self.template)
        self.assertIn("form.addEventListener('invalid'",self.js)

    def test_all_original_secrets_remain_existing_form_fields(self):
        for key in ('tg_api_hash','tg_bot_token','dhan_pin','dhan_totp_secret','dhan_access_token'):
            self.assertIn('name="' + key + '"',self.template)
        self.assertIn('label class="toggle-row"',self.template)
        self.assertIn('name="confirm_live"',self.template)
        self.assertIn('Audit bot Setup',self.template)

    def test_shipped_presentation_files_only(self):
        ast.parse(src('src/premium_ui.py'))
        from src.premium_ui import premium_reference_styles,premium_reference_js,premium_nav
        self.assertIn('dash-board-grid',premium_reference_styles())
        self.assertIn('DOMContentLoaded',premium_reference_js())
        self.assertIn('/#amo-order-status',premium_nav('setup'))
        self.assertIn('/#system-health',premium_nav('setup'))
        self.assertEqual(premium_nav('setup').count('aria-current="page"'),1)
        control = src('src/control_center_ui.py')
        self.assertIn('id="amo-order-status"',control)
        self.assertIn('id="system-health"',control)


if __name__ == '__main__':
    unittest.main()
