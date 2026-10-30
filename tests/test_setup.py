"""
Unit tests for Setup System and Web Configuration App.
Gracefully skips if httpx/TestClient is not installed in the environment.
"""

import os
import tempfile
import unittest

try:
    from fastapi.testclient import TestClient
    HAS_TESTCLIENT = True
except Exception:
    TestClient = None
    HAS_TESTCLIENT = False

from src.setup_app import app


class TestSetupApp(unittest.TestCase):
    def setUp(self):
        if not HAS_TESTCLIENT:
            self.skipTest("TestClient unavailable (install httpx via: pip install httpx)")
        self.client = TestClient(app)

    def test_get_setup_page(self):
        if not HAS_TESTCLIENT:
            self.skipTest("TestClient unavailable")
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Audit bot Setup", resp.text)
        self.assertIn("Exchange Credentials", resp.text)
        self.assertIn("Bot Configuration", resp.text)
        self.assertIn("Risk Management", resp.text)
        self.assertIn("Execution Mode", resp.text)

    def test_post_setup_configuration(self):
        if not HAS_TESTCLIENT:
            self.skipTest("TestClient unavailable")
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_cwd = os.getcwd()
            os.chdir(tmp_dir)
            try:
                data = {
                    "dhan_client_id": "1100998877",
                    "dhan_access_token": "test_token_123",
                    "instance_name": "SapphireAlgo",
                    "log_group_id": "-10012345678",
                    "max_drawdown_pct": 5.0,
                    "daily_trade_limit": 20,
                    "is_live_mode": False,
                }
                resp = self.client.post("/setup", data=data)
                self.assertEqual(resp.status_code, 200)
                self.assertTrue(os.path.exists(".env"))
                with open(".env") as f:
                    content = f.read()
                self.assertIn("DHAN_CLIENT_ID=1100998877", content)
                self.assertIn("TRADING_MODE=TEST", content)
                self.assertIn("BOT_INSTANCE_NAME=SapphireAlgo", content)
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
