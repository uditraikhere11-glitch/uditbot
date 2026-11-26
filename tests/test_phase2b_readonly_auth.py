"""Regression: safety probes never mint a token, including on HTTP 401."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.broker.dhan_client import DhanClient
from src.config import AppConfig
from src.models import BrokerReadFailure, TradingMode


class ReadOnlyAuthTests(unittest.TestCase):
    def test_readonly_client_does_not_mint_on_missing_or_rejected_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AppConfig(trading_mode=TradingMode.LIVE, data_dir=tmp,
                            dhan_client_id="9000000001", dhan_pin="123456",
                            dhan_totp_secret="JBSWY3DPEHPK3PXP",
                            dhan_token_file=str(Path(tmp) / "missing_token"))
            with patch.object(DhanClient, "generate_token_with_totp") as renew:
                client = DhanClient(cfg, auto_generate_token=False,
                                    allow_read_auth_recovery=False)
                self.addCleanup(client.session.close)
                response = Mock(status_code=401, text='{"errorCode":"DH-901"}', headers={})
                with patch.object(client.session, "get", return_value=response) as transport:
                    read = client.get_holdings()
                self.assertIsInstance(read, BrokerReadFailure)
                self.assertEqual(read.status_code, 401)
                transport.assert_called_once()
                renew.assert_not_called()
                self.assertFalse(Path(cfg.dhan_token_file).exists())

    def test_readonly_can_adopt_an_independently_rotated_token_without_minting(self):
        with tempfile.TemporaryDirectory() as tmp:
            from src.broker.dhan_token import write_token_atomic
            token_path = str(Path(tmp) / "token")
            write_token_atomic("old-test-token", token_path)
            cfg = AppConfig(trading_mode=TradingMode.LIVE, data_dir=tmp,
                            dhan_client_id="9000000001", dhan_pin="123456",
                            dhan_totp_secret="JBSWY3DPEHPK3PXP", dhan_token_file=token_path)
            with patch.object(DhanClient, "generate_token_with_totp") as renew:
                client = DhanClient(cfg, auto_generate_token=False,
                                    allow_read_auth_recovery=False)
                self.addCleanup(client.session.close)
                calls = []
                def get(url, *, headers, timeout):
                    calls.append(headers["access-token"])
                    if len(calls) == 1:
                        write_token_atomic("new-test-token", token_path)
                        return Mock(status_code=401, text='{"errorCode":"DH-901"}', headers={})
                    r = Mock(status_code=200, text='[]', headers={})
                    r.json.return_value = []
                    return r
                with patch.object(client.session, "get", side_effect=get):
                    read = client.get_holdings()
                self.assertTrue(read.ok)
                self.assertEqual(calls, ["old-test-token", "new-test-token"])
                renew.assert_not_called()
