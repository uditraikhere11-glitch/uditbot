"""PR130 classification of local admission vs Dhan broker historical HTTP 429."""
import unittest
from unittest.mock import MagicMock, patch
from src.broker.dhan_client import DhanClient


class GovernorDiagnostics(unittest.TestCase):
    def test_local_historical_429_is_not_misattributed_to_dhan(self):
        from requests import Response
        from src.models import BrokerReadStatus
        client = object.__new__(DhanClient)
        cfg = MagicMock()
        cfg.dhan_client_id = "1113731536"
        cfg.trading_mode.value = "LIVE"
        client.cfg = cfg
        response = Response()
        response.status_code = 429
        response._content = b"Local admission deferred"
        response.headers["X-AuditBot-Local-Deferral"] = "1"
        with patch.object(client, "_get_with_auth_recovery", return_value=response):
            failure = client.get_historical_trades_page("2026-11-20", "2026-11-23", 0)
        self.assertEqual(failure.status_code, 429)
        self.assertIn("LOCAL_DHAN_GET_DEFERRAL", failure.error)
        self.assertNotIn("BROKER_HTTP_429", failure.error)

    def test_actual_historical_429_is_labelled_broker(self):
        from requests import Response
        client = object.__new__(DhanClient)
        cfg = MagicMock()
        cfg.dhan_client_id = "1113731536"
        cfg.trading_mode.value = "LIVE"
        client.cfg = cfg
        response = Response()
        response.status_code = 429
        response._content = b"Broker capacity exceeded"
        with patch.object(client, "_get_with_auth_recovery", return_value=response):
            failure = client.get_historical_trades_page("2026-11-20", "2026-11-23", 0)
        self.assertIn("BROKER_HTTP_429", failure.error)
        self.assertNotIn("LOCAL_DHAN_GET_DEFERRAL", failure.error)
