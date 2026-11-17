"""End-to-end password/CSRF/Setup V2 acceptance through actual ASGI routes.

No network, no live Dhan, no systemctl, no VM access. Uses a disposable
private .env and password verifier. The outer Control Center is not bypassed.
"""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from src import webui_password as auth
from src.control_center_entry import app
from src.setup_app import SETUP_CSRF_TOKEN
from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
from src.engine.config_patch import revision

PASSWORD = "SyntheticPrivateOwner2026!"
BASE = "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\nDHAN_CLIENT_ID=TESTONLY1234\n"


class TrackerV2AuthenticatedAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data_dir = self.root / "trading-data"
        self.data_dir.mkdir(mode=0o700)
        self.config_path = self.root / ".env"
        self.initial = BASE + f"DATA_DIR={self.data_dir}\n"
        self.config_path.write_text(self.initial, encoding="utf8")
        self.oldcwd = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.oldcwd)
        env = patch.dict(os.environ, {
            "UDITBOT_TRACKER_V2_ENABLE": "1",
            "AUDITBOT_TEST_ISOLATION": "1",
            "UDITBOT_WEBUI_AUTH_DIR": str(self.root / "auth"),
        })
        env.start()
        self.addCleanup(env.stop)
        auth.provision(PASSWORD)
        auth._failures.clear()
        auth._sessions.clear()
        status = patch.object(
            ConfigChangeTracker, "resolve_service_operating_state",
            return_value=(ServiceOperatingState.STOPPED_OTHER, "offline staging"),
        )
        status.start()
        self.addCleanup(status.stop)
        notifier = patch.object(ConfigChangeTracker, "dispatch_telegram_update")
        notifier.start()
        self.addCleanup(notifier.stop)
        self.client = TestClient(
            app, base_url="https://control.example", follow_redirects=False
        )

    def _login(self):
        reply = self.client.post("/login", data={"password": PASSWORD})
        self.assertEqual(303, reply.status_code, reply.text)
        self.assertIn("HttpOnly", reply.headers.get("set-cookie", ""))

    def _save(self, **values):
        params = {
            "csrf_token": SETUP_CSRF_TOKEN,
            "request_id": "v2-auth-req-001",
            "expected_revision": revision(self.config_path.read_text()),
            "equity_budget_inr": "750",
        }
        params.update(values)
        return self.client.post("/setup/submit-async", json=params)

    def test_password_required_for_every_v2_write(self):
        for path in (
            "/setup/submit-async", "/control/amo/fno",
            "/control/tracker-v2/recheck",
        ):
            with self.subTest(path=path):
                reply = self.client.post(path, json={"csrf_token": SETUP_CSRF_TOKEN})
                self.assertEqual(401, reply.status_code)
        self.assertEqual(self.initial, self.config_path.read_text())

    def test_real_login_setup_csrf_save_status_and_amotoggle(self):
        self._login()
        page = self.client.get("/setup")
        self.assertEqual(200, page.status_code)
        self.assertIn('name="expected_revision"', page.text)
        self.assertIn('name="csrf_token"', page.text)
        self.assertIn("Control Center", self.client.get("/").text)
        denied = self._save(csrf_token="forged")
        self.assertEqual(403, denied.status_code)
        self.assertEqual(self.initial, self.config_path.read_text())
        saved = self._save()
        self.assertEqual(200, saved.status_code, saved.text)
        body = saved.json()
        self.assertEqual("SAVED_FOR_NEXT_START", body["state"])
        self.assertFalse(body["restart_performed"])
        self.assertFalse(body["runtime_verified"])
        self.assertIn("EQUITY_BUDGET_INR=750", self.config_path.read_text())
        status = self.client.get(f"/setup/change-status/{body['change_id']}")
        self.assertEqual(200, status.status_code)
        self.assertFalse(status.json()["runtime_verified"])
        active = self.client.get("/control/status")
        self.assertEqual(200, active.status_code)
        amo = self.client.post("/control/amo/fno", json={
            "csrf_token": SETUP_CSRF_TOKEN, "enabled": True,
            "request_id": "v2-auth-amo-002",
            "expected_revision": revision(self.config_path.read_text()),
        })
        self.assertEqual(200, amo.status_code, amo.text)
        self.assertIn("FNO_AMO_ENABLED=true", self.config_path.read_text())

    def test_live_needs_explicit_confirmation(self):
        self._login()
        resp = self._save(trading_mode="LIVE")
        self.assertEqual(422, resp.status_code)
        self.assertEqual(self.initial, self.config_path.read_text())

    def test_late_ack_recovery_route_is_auth_and_csrf_protected(self):
        self.assertEqual(
            401, self.client.post("/control/tracker-v2/recheck", json={
                "csrf_token": SETUP_CSRF_TOKEN, "change_id": "V2-" + "a"*16
            }).status_code,
        )
        self._login()
        denied = self.client.post("/control/tracker-v2/recheck", json={
            "csrf_token": "wrong", "change_id": "V2-" + "a"*16,
        })
        self.assertEqual(403, denied.status_code)
        invalid = self.client.post("/control/tracker-v2/recheck", json={
            "csrf_token": SETUP_CSRF_TOKEN, "change_id": "INVALID",
        })
        self.assertEqual(409, invalid.status_code)
        self.assertEqual(self.initial, self.config_path.read_text())


if __name__ == "__main__":
    unittest.main()
