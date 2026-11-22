"""Offline V17 visual integration contracts; only display-name and CSS changes."""
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from src.premium_ui import premium_instance_name, premium_nav
from src.control_center_ui import render_control_center
from src.setup_app import HTML_TEMPLATE


class V17InstanceAndNativePreservation(TestCase):
    def _with_env(self, content, assertion):
        former = os.getcwd()
        with TemporaryDirectory(prefix='v17_display_') as tmp:
            try:
                os.chdir(tmp)
                Path('.env').write_text(content, encoding='utf-8')
                os.chmod('.env',0o600)
                assertion()
            finally:
                os.chdir(former)

    def test_distinct_bot_instance_names_are_not_cached(self):
        def udit():
            self.assertEqual(premium_instance_name(),'Udit')
            self.assertIn('<strong>UditBot</strong>',premium_nav('control'))
            self.assertIn('Powered by AuditBot · Trading System',premium_nav('control'))
            self.assertIn('<h1>Udit</h1>',render_control_center('fake_csrf'))
        def aman():
            self.assertEqual(premium_instance_name(),'Aman')
            self.assertIn('<strong>AmanBot</strong>',premium_nav('control'))
            self.assertIn('Powered by AuditBot · Trading System',premium_nav('control'))
            self.assertIn('<h1>Aman</h1>',render_control_center('fake_csrf'))
        self._with_env('OPERATOR_NAME=Udit\nBOT_INSTANCE_NAME=UditBot\n',udit)
        self._with_env('OPERATOR_NAME=Aman\nBOT_INSTANCE_NAME=AmanBot\n',aman)

    def test_fallback_and_default(self):
        self._with_env('BOT_INSTANCE_NAME=Replica\n',lambda:self.assertEqual(premium_instance_name(),'Replica'))
        self._with_env('OPERATOR_NAME=\nBOT_INSTANCE_NAME=\n',lambda:self.assertEqual(premium_instance_name(),'UDITBOT'))

    def test_untrusted_label_is_escaped_and_not_executable(self):
        def check():
            html=render_control_center('fake_csrf')
            self.assertIn('&lt;script&gt;',html)
            self.assertNotIn('<h1><script>',html)
            self.assertIn('&lt;script&gt;',premium_nav('control'))
            self.assertNotIn('<script>alert(1)</script>',premium_nav('control'))
        self._with_env('OPERATOR_NAME=<script>alert(1)</script>\n',check)

    def test_long_label_is_bounded(self):
        self._with_env('OPERATOR_NAME='+'A'*1000+'\n',lambda:self.assertEqual(len(premium_instance_name()),96))

    def test_exact_existing_setup_template_untouched(self):
        import hashlib
        self.assertEqual(hashlib.sha256(HTML_TEMPLATE.encode()).hexdigest(),
                         '4993337e7676744596d82c85bc057ebad6cfd74096086395349db37d667e6a17')

    def test_existing_routes_and_native_actions_still_present(self):
        html=render_control_center('fake_csrf')
        for marker in ('id="start"','id="stop"','id="v17-health-run"','id="equity-amo"','id="fno-amo"',
                       '/control/status','/control/health','/control/amo/equity',
                       '/control/amo/fno','/control/amo/fno/cancel','/setup/service-control'):
            with self.subTest(marker=marker):self.assertIn(marker,html)

    def test_mobile_changes_scoped_and_do_not_hide_controls(self):
        css=(Path(__file__).resolve().parents[1]/'src/premium_ui.css').read_text()
        self.assertIn('/* V17 iPhone refinements.',css)
        refined=css.split('/* V17 iPhone refinements.',1)[1].split('/* V17 reference-layout restoration;',1)[0]
        self.assertIn('@media (max-width:700px)',refined)
        self.assertNotIn('display:none',refined)
        self.assertIn('grid-template-columns:repeat(3,minmax(0,1fr))',refined)
