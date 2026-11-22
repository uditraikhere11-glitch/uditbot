"""Source and native Setup tests: six category views, same 54-form schema.

No live broker, Telegram, systemd or configuration write in these tests.
"""
from contextlib import contextmanager
from pathlib import Path
import ast
import os
import re
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def saved_settings(source):
    prior = os.getcwd()
    with tempfile.TemporaryDirectory(prefix='v17-setup-contract-') as temporary:
        os.chdir(temporary)
        try:
            Path('.env').write_text(source, encoding='utf-8')
            yield
        finally:
            os.chdir(prior)


class SetupCategoryContract(unittest.TestCase):
    def test_shared_header_masks_saved_account_only(self):
        from src.premium_ui import premium_nav, premium_configured_dhan_label
        with saved_settings('OPERATOR_NAME=Udit\nDHAN_CLIENT_ID=1234567890\n'):
            self.assertEqual(premium_configured_dhan_label(), '***7890')
            for page in ('setup', 'control', 'designer'):
                html = premium_nav(page)
                self.assertIn('Dhan <strong>***7890</strong>', html)
                self.assertIn('connection not verified here', html)
                self.assertNotIn('1234567890', html)
                self.assertNotIn('Dhan: Connected', html)
        with saved_settings('OPERATOR_NAME=Udit\n'):
            self.assertEqual(premium_configured_dhan_label(), 'Not configured')
        with saved_settings('DHAN_CLIENT_ID=<script>alert(1)</script>\n'):
            self.assertEqual(premium_configured_dhan_label(), 'Configured')
            self.assertNotIn('<script>', premium_nav('setup'))

    def test_setup_native_form_unchanged_54_names(self):
        from src.setup_app import HTML_TEMPLATE, get_setup_page
        with saved_settings('OPERATOR_NAME=Udit\nDHAN_CLIENT_ID=1234567890\n'):
            html = get_setup_page().body.decode('utf-8')
        native = HTML_TEMPLATE.split('<form method="POST" action="/setup">', 1)[1].split('</form>', 1)[0]
        names = re.findall(r'<(?:input|select|textarea)\b[^>]*\bname="([^"]+)"', native)
        self.assertEqual(len(names), 54)
        self.assertEqual(len(set(names)), 54)
        for key in ('csrf_token','expected_revision','full_setup_form','confirm_live','is_live_mode','dhan_client_id','max_open_positions','sl_pct'):
            self.assertIn(key, names)
        self.assertIn('fetch(\'/setup/save-apply\'', html)
        self.assertIn('View all settings and Save', (ROOT/'src/premium_ui.js').read_text())
        # Duplicated runtime status is now removed rather than using saved mode
        # and saved Dhan credentials to imply running or broker-connected state.
        self.assertNotIn('<div class="system-status-panel">', html)
        self.assertIn('Dhan <strong>***7890</strong>', html)

    def test_every_landing_category_maps_to_existing_native_section(self):
        from src.setup_app import HTML_TEMPLATE
        js=(ROOT/'src/premium_ui.js').read_text(encoding='utf-8')
        targets=['setup-execution','setup-telegram','setup-broker','setup-sizing','setup-risk','setup-protection']
        for target in targets:
            self.assertIn(f'id="{target}"',HTML_TEMPLATE)
            self.assertIn(f"['{target}'",js)  # exact configured button target
        self.assertIn("amo.href='/#amo'",js)
        self.assertNotIn('AMO &amp; Orders</b>',js)
        self.assertIn("event.preventDefault();navigate(id)",js)
        self.assertIn("section.hidden=",js)
        self.assertIn('v17p-setup-amo-link',js)
        for forbidden in ('new FormData(', 'requestSubmit(', '/setup/save-apply', '/control/amo'):
            self.assertNotIn(forbidden,js)

    def test_native_backend_save_apply_route_preserved(self):
        source=(ROOT/'src/setup_app.py').read_text(encoding='utf-8')
        ast.parse(source)
        self.assertIn('@app.get("/", response_class=HTMLResponse)',source)
        self.assertIn('fetch(\'/setup/save-apply\'',source)
        self.assertIn('expected_revision',source)
        self.assertIn('result.status === \'RESTARTED\'',source)


if __name__=='__main__': unittest.main()
