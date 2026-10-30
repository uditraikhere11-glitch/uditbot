"""
Unit tests for Dhan automated TOTP authentication and /fundlimit integration.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.broker.dhan_client import DhanClient, compute_totp


class TestDhanAuthAndFunds(unittest.TestCase):
    def test_compute_totp_rfc6238(self):
        """Verifies pure standard library RFC 6238 TOTP generates 6-digit code."""
        secret = "JBSWY3DPEHPK3PXP"
        code = compute_totp(secret)
        self.assertEqual(len(code), 6)
        self.assertTrue(code.isdigit())

    @patch("requests.Session.get")
    def test_get_fund_limits_fundlimit_endpoint(self, mock_get):
        """Verifies fund limits are queried from /fundlimit and parsed properly."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dhanClientId": "1113731536",
            "availabelBalance": 45250.75,
            "sodLimit": 50000.0,
            "collateralAmount": 0.0,
            "utilizedAmount": 4749.25,
        }
        mock_get.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_access_token = "mock_test_token"
        client = DhanClient(cfg)

        ok, bal, msg = client.get_fund_limits(retry=False)
        self.assertTrue(ok)
        self.assertEqual(bal, 45250.75)
        self.assertIn("Funds retrieved successfully", msg)
        mock_get.assert_called_once()
        self.assertIn("/fundlimit", mock_get.call_args[0][0])

    @patch("requests.Session.post")
    def test_generate_token_with_totp_success(self, mock_post):
        """Verifies generate_token_with_totp calls auth service and stores token."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dhanClientId": "1113731536",
            "accessToken": "eyJmock_new_access_token",
            "expiryTime": "2026-10-30 08:00:00",
        }
        mock_post.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_pin = "123456"
        cfg.dhan_totp_secret = "JBSWY3DPEHPK3PXP"
        cfg.dhan_token_file = "/tmp/test_dhan_token.txt"

        client = DhanClient(cfg)
        ok, tok, msg = client.generate_token_with_totp()
        self.assertTrue(ok)
        self.assertEqual(tok, "eyJmock_new_access_token")
        self.assertEqual(client.token, "eyJmock_new_access_token")


if __name__ == "__main__":
    unittest.main()
