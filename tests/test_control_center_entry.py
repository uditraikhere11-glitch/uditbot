"""Control Center one-password security and legacy route regression tests."""
import os
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from src import webui_password as auth
from src.control_center_entry import app
from src.setup_app import SETUP_CSRF_TOKEN

PASSWORD = "StrongSingleOwnerPass2026!"


class ControlCenterFacadeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.env = patch.dict(os.environ, {
            "UDITBOT_WEBUI_AUTH_DIR": self.directory.name,
            "AUDITBOT_TEST_ISOLATION": "1",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        auth.provision(PASSWORD)
        auth._failures.clear()  # isolate per-test rate-limit state
        self.client = TestClient(app, base_url="https://control.example", follow_redirects=False)

    def login(self, password=PASSWORD):
        return self.client.post('/login', data={'password': password})

    def test_all_routes_private_before_login(self):
        for path in ('/', '/setup', '/control/status', '/control/health',
                     '/control/amo/orders', '/setup/telegram-designer',
                     '/setup/telegram-designer/config'):
            r = self.client.get(path)
            self.assertEqual(r.status_code, 303, path)
            self.assertEqual(r.headers.get('location'), '/login')
        for path in ('/setup', '/setup/submit-async', '/setup/service-control',
                     '/control/amo/equity', '/control/amo/fno',
                     '/setup/telegram-designer/save'):
            self.assertEqual(self.client.post(path).status_code, 401, path)

    def test_login_home_setup_and_logout(self):
        r = self.login()
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers['location'], '/')
        self.assertIn('HttpOnly', r.headers['set-cookie'])
        self.assertIn('samesite=strict', r.headers['set-cookie'].lower())
        self.assertEqual(self.client.get('/').status_code, 200)
        self.assertIn('Control Center', self.client.get('/').text)
        r = self.client.get('/setup')
        self.assertEqual(r.status_code, 200)
        self.assertIn('Back to Control Center', r.text)
        self.assertIn('Audit bot Setup', r.text)
        self.assertEqual(self.client.post('/control/amo/equity', json={
            'enabled': True, 'csrf_token': 'forged',
        }).status_code, 403)
        self.assertEqual(self.client.post('/logout', data={'csrf_token': 'wrong'}).status_code, 403)
        self.assertEqual(self.client.post('/logout', data={'csrf_token': SETUP_CSRF_TOKEN}).status_code, 303)
        self.assertEqual(self.client.get('/setup').status_code, 303)

    def test_setup_post_is_still_original_fastapi_form_route(self):
        # Invalid CSRF must reach legacy POST /setup and return 403, not a 500
        # caused by forwarding raw form strings as Python kwargs.
        self.login()
        r = self.client.post('/setup', data={
            'csrf_token': 'invalid', 'equity_budget_inr': '1500.00',
            'equity_active': 'true',
        })
        self.assertEqual(r.status_code, 403)

    def test_private_http_and_secure_https_cookie_policy(self):
        from starlette.requests import Request
        def req(host, client='127.0.0.1'):
            return Request({'type':'http','method':'GET','scheme':'http','path':'/',
                'root_path':'', 'headers':[(b'host',host.encode())],
                'client':(client,5555),'server':('127.0.0.1',80)})
        self.assertFalse(auth.cookie_secure(req('127.0.0.1')))
        self.assertTrue(auth.cookie_secure(req('control.example')))
        self.assertTrue(auth.permitted_transport(req('control.example')))
        self.assertFalse(auth.permitted_transport(req('control.example','8.8.8.8')))

    def test_insecure_public_http_requires_exact_operator_optin(self):
        from starlette.requests import Request
        def remote(scope_scheme="http"):
            return Request({
                'type':'http', 'method':'GET', 'scheme':scope_scheme,
                'path':'/login', 'root_path':'',
                'headers':[(b'host',b'public.example'),
                           (b'x-forwarded-for',b'127.0.0.1')],
                'client':('203.0.113.25', 12345),
                'server':('0.0.0.0',80),
            })
        key = 'UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP'
        with patch.dict(os.environ, {key:'true'}):
            self.assertFalse(auth.public_http_explicitly_enabled())
            self.assertFalse(auth.permitted_transport(remote()))
            self.assertTrue(auth.cookie_secure(remote()))
        with patch.dict(os.environ, {key:'I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK'}):
            self.assertTrue(auth.public_http_explicitly_enabled())
            self.assertTrue(auth.permitted_transport(remote()))
            self.assertFalse(auth.cookie_secure(remote()))
            self.assertTrue(auth.cookie_secure(remote('https')))
            self.assertTrue(auth.authenticate(PASSWORD, '203.0.113.25'))
        self.assertFalse(auth.permitted_transport(remote()))

    def test_login_via_public_http_optin_warns_and_cookie_is_not_secure(self):
        key='UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP'
        with patch.dict(os.environ, {key:'I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK'}):
            client = TestClient(app,base_url='http://public.example',follow_redirects=False)
            warning = client.get('/login')
            self.assertEqual(warning.status_code, 200)
            self.assertIn('Public HTTP does not encrypt',warning.text)
            response = client.post('/login',data={'password': PASSWORD})
            self.assertEqual(response.status_code, 303)
            self.assertIn('httponly',response.headers['set-cookie'].lower())
            self.assertNotIn('; secure',response.headers['set-cookie'].lower())
            self.assertEqual(client.get('/').status_code,200)
            self.assertEqual(client.get('/setup').status_code,200)
            self.assertEqual(client.get('/control/status').status_code,200)

    def test_expired_session_is_rejected(self):
        self.login()
        self.assertEqual(self.client.get('/').status_code, 200)
        with auth._lock:
            auth._sessions.clear()  # model expired/revoked server-side state
        self.assertEqual(self.client.get('/').status_code, 303)

    def test_wrong_password_no_basic_auth_bypass(self):
        self.assertEqual(self.login('wrong').status_code, 401)
        self.assertEqual(self.client.get('/', headers={
            'Authorization': 'Basic dWRpdGJvdDpmb28='
        }).status_code, 303)
        self.assertEqual(self.client.get('/login').status_code, 200)

    def test_missing_verifier_fails_closed(self):
        os.remove(auth.password_file())
        self.assertEqual(self.client.get('/login').status_code, 503)

    def test_limit_attempts(self):
        for _ in range(5):
            self.assertEqual(self.login('incorrect').status_code, 401)
        self.assertEqual(self.login(PASSWORD).status_code, 429)

    def test_password_rotation_revokes_current_session(self):
        self.login()
        self.assertEqual(self.client.get('/').status_code, 200)
        auth.provision('AnotherStrongSecret2026!')
        self.assertEqual(self.client.get('/').status_code, 303)

    def test_spoofed_external_headers_rejected(self):
        from starlette.requests import Request
        scope = {
            'type': 'http', 'method': 'GET', 'scheme': 'http',
            'path': '/login', 'root_path': '', 'headers': [
                (b'host', b'127.0.0.1'),
                (b'x-forwarded-for', b'127.0.0.1'),
                (b'x-goog-iap-jwt-assertion', b'fake'),
            ], 'client': ('203.0.113.99', 1234), 'server': ('127.0.0.1', 80),
        }
        self.assertFalse(auth.permitted_transport(Request(scope)))


if __name__ == '__main__':
    unittest.main()
