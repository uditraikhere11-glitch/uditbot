"""Regression tests for authenticated, opt-in facade; original Setup is never edited."""
import base64
import hashlib
import os
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient

from src.control_center_entry import app


class ControlCenterFacadeTests(unittest.TestCase):
    def setUp(self):
        self.password = 'dummy-only-sample-not-real'
        self.hashval = hashlib.sha256(self.password.encode()).hexdigest()
        self.envpatch = patch.dict(os.environ, {'UDITBOT_WEBUI_PASSWORD_SHA256': self.hashval})
        self.envpatch.start()
        self.addCleanup(self.envpatch.stop)
        self.client = TestClient(app)
        self.auth = {'Authorization': 'Basic ' + base64.b64encode(
            ('uditbot:' + self.password).encode('utf-8')).decode('ascii')}

    def test_login_required_on_homepage_and_legacy_mutations(self):
        self.assertEqual(self.client.get('/').status_code, 401)
        self.assertEqual(self.client.get('/setup').status_code, 401)
        self.assertEqual(self.client.post('/setup/service-control').status_code, 401)
        self.assertEqual(self.client.post('/setup').status_code, 401)
        self.assertEqual(self.client.post('/control/amo/equity', json={'enabled':True}).status_code, 401)

    def test_authorized_homepage_and_existing_setup(self):
        home=self.client.get('/',headers=self.auth)
        self.assertEqual(home.status_code,200)
        self.assertIn('href="/setup"',home.text)
        setup=self.client.get('/setup',headers=self.auth)
        self.assertEqual(setup.status_code,200)
        self.assertIn('Back to Control Center', setup.text)
        self.assertIn('Audit bot Setup',setup.text)

    def test_authorized_legacy_route_delegation(self):
        # Use read-only existing route; no live service restart should occur.
        self.assertNotEqual(self.client.post('/setup/service-control').status_code,200)
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'forged'},headers=self.auth)
        self.assertEqual(r.status_code,403)

    def test_wrong_password_and_user_blocked(self):
        bad={'Authorization':'Basic '+base64.b64encode(('uditbot:wrong').encode()).decode()}
        wronguser={'Authorization':'Basic '+base64.b64encode(('other:'+self.password).encode()).decode()}
        self.assertEqual(self.client.get('/',headers=bad).status_code,401)
        self.assertEqual(self.client.get('/',headers=wronguser).status_code,401)
        self.assertEqual(self.client.get('/',headers={'Authorization':'Basic not-base64!'}).status_code,401)

    def test_missing_password_hash_defaults_fail_closed(self):
        with patch('src.control_center_entry._password_hash',return_value=''):
            r=self.client.get('/',headers=self.auth)
        self.assertEqual(r.status_code,503)
        self.assertIn('inactive',r.text)

    def test_nonce_invalid_no_amo_mutation(self):
        r=self.client.post('/control/amo/equity',json={'enabled':True,'csrf_token':'bad'},headers=self.auth)
        self.assertEqual(r.status_code,403)


if __name__=='__main__':
    unittest.main()