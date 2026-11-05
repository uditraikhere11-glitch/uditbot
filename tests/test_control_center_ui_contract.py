"""Standalone read-only proposal smoke tests; no production imports or network."""
import importlib.util
from pathlib import Path
import unittest

P = Path(__file__).resolve().parents[1] / "src" / "control_center_ui.py"
spec = importlib.util.spec_from_file_location("control_center_proposal", P)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ControlCenterPresentationTests(unittest.TestCase):
    def setUp(self):
        self.html = module.render_control_center('TOKEN-&"<>')

    def test_setup_navigation(self):
        self.assertIn('href="/setup"', self.html)
        self.assertIn('UDITBOT', self.html)

    def test_reuses_existing_service_control_endpoint(self):
        self.assertIn('/setup/service-control', self.html)
        self.assertIn('confirm_live', self.html)

    def test_fno_switch_starts_disabled(self):
        self.assertIn('id="fno-amo"', self.html)
        self.assertRegex(self.html, r'id="fno-amo"[^>]*disabled')

    def test_equity_switch_requires_server_authorization(self):
        self.assertIn('equity_amo_can_edit', self.html)
        self.assertRegex(self.html, r'id="equity-amo"[^>]*disabled')

    def test_no_static_assumption_of_live_service_status(self):
        self.assertIn('id="service" class="value">Checking…', self.html)
        self.assertIn('id="mode" class="value">UNKNOWN', self.html)

    def test_escapes_csrf_attribute(self):
        self.assertIn('TOKEN-&amp;&quot;&lt;&gt;', self.html)
        self.assertNotIn('content="TOKEN-&"<>"', self.html)

    def test_preserves_remote_setup_and_trading_files(self):
        self.assertNotIn('systemctl start', self.html)
        self.assertNotIn('systemctl stop', self.html)
        self.assertNotIn('/super/orders', self.html)
        self.assertNotIn('/orders"', self.html)

    def test_health_is_read_only(self):
        self.assertIn("requestJSON('/control/health')", self.html)
        self.assertIn('Health Check', self.html)


if __name__ == '__main__':
    unittest.main()