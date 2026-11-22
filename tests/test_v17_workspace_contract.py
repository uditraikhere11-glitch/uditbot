"""Static contracts for V17 browser-native view navigation, no broker calls."""
import re
import unittest
from src.control_center_ui import render_control_center
from src.premium_ui import premium_nav, premium_nav_js


class V17WorkspaceContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = render_control_center('offline-test-token')

    def test_four_derived_views_use_existing_root_response(self):
        for key in ('dashboard', 'control', 'amo', 'health'):
            with self.subTest(key=key):
                self.assertIn(f'data-v17-view="{key}"', self.html)
        for target in ('v17-dashboard', 'v17-control', 'v17-amo-settings', 'amo-orders', 'system-health'):
            with self.subTest(panel=target):
                self.assertEqual(len(re.findall(fr'id="{target}"',self.html)),1)
        self.assertIn('window.addEventListener(\'hashchange\',v17ShowView)',self.html)
        self.assertNotIn('window.location.href=',self.html)

    def test_dashboard_values_are_source_backed_or_unverified(self):
        for key in ('v17-db-service','v17-db-mode','v17-db-equity','v17-db-fno','v17-db-verified'):
            with self.subTest(key=key):
                self.assertIn(f'id="{key}"',self.html)
        self.assertIn("setText('v17-db-verified',s.runtime_verified===true&&state==='RUNNING'?'VERIFIED':'UNVERIFIED')",self.html)
        self.assertIn("setText(id,'UNVERIFIED')",self.html)
        self.assertNotIn('Account Balance',self.html)
        self.assertNotIn('Realized P&L',self.html)

    def test_service_controls_only_one_copy(self):
        for field in ('start','stop','restart','v17-health-run','equity-amo','fno-amo'):
            with self.subTest(field=field):
                self.assertEqual(len(re.findall(fr'id="{field}"',self.html)),1)
        self.assertNotIn('id="health"', self.html)
        self.assertIn("$('v17-health-run').addEventListener('click',async()=>{", self.html)
        self.assertIn('/setup/service-control',self.html)
        self.assertIn('/control/amo/fno',self.html)
        self.assertIn('/control/health',self.html)

    def test_theme_toggle_is_only_visual_and_reversible(self):
        html=premium_nav('control')
        js=premium_nav_js()
        self.assertIn('id="premiumThemeToggle"',html)
        self.assertIn("window.localStorage.getItem(key)",js)
        self.assertIn("window.localStorage.setItem(key,light?'light':'dark')",js)
        for bad in ('dhan_client_id','access_token','csrf_token','systemctl','order_id'):
            self.assertNotIn(bad,js)

    def test_no_misleading_emergency_stop_button(self):
        self.assertNotIn('id="emergency-stop"',self.html)
        self.assertNotIn('POST /control/emergency-stop',self.html)


if __name__=='__main__': unittest.main()
