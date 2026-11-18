"""R07 offline regression: startup funds reserve, identity retry, local 429.

Only synthetic broker responses; no Dhan network calls, orders, VM or journal
mutations. Keep routine signal-side funds checks at NORMAL priority.
"""
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock

from src.broker.dhan_client import DhanClient, BASE_URL
from src.broker.read_governor import ReadDeferred, ReadGovernor
from src.engine.reconciliation import StartupReconciliationGate
from src.models import BrokerReadFailure, BrokerReadStatus, TradingMode


def live_client(response):
    client = object.__new__(DhanClient)
    client.cfg = NS(trading_mode=TradingMode.LIVE, dhan_client_id="SYNTHETIC")
    client._get_with_auth_recovery = Mock(return_value=response)
    return client


class StartupReadReserveTests(unittest.TestCase):
    def test_reserved_identity_read_available_after_normal_is_exhausted(self):
        with tempfile.TemporaryDirectory() as root:
            gov = ReadGovernor(root, "SYNTHETIC", clock=lambda: 100000.0)
            gov.LIMITS = ((86400, 100),)
            for _ in range(95):
                gov.admit(priority="normal")
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="normal")
            gov.admit(priority="identity")  # one of five reserved slots
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="normal")
            for _ in range(4):
                gov.admit(priority="identity")
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="identity")  # hard cap is NEVER bypassed

    def test_startup_funds_get_uses_identity_but_normal_get_does_not(self):
        response = NS(
            status_code=200, text="", headers={},
            json=lambda: {"dhanClientId": "SYNTHETIC", "availabelBalance": 1234.5},
        )
        broker = live_client(response)
        ok, balance, _ = broker.get_reconciliation_fund_limits()
        self.assertTrue(ok)
        self.assertEqual(1234.5, balance)
        broker._get_with_auth_recovery.assert_called_once_with(
            f"{BASE_URL}/fundlimit", allow_refresh=True, priority="identity"
        )
        broker._get_with_auth_recovery.reset_mock()
        ok, balance, _ = broker.get_fund_limits(retry=False)
        self.assertTrue(ok)
        self.assertEqual(1234.5, balance)
        broker._get_with_auth_recovery.assert_called_once_with(
            f"{BASE_URL}/fundlimit", allow_refresh=False
        )

    def test_local_denial_is_not_falsely_reported_as_broker_http429(self):
        response = NS(
            status_code=429,
            text="Dhan GET deferred locally for 999s: local GET budget exhausted",
            headers={"X-AuditBot-Local-Deferral": "1", "Retry-After": "999"},
        )
        broker = live_client(response)
        ok, balance, message = broker.get_fund_limits(retry=False)
        self.assertFalse(ok)
        self.assertEqual(0.0, balance)
        self.assertIn("no broker request sent", message)
        self.assertIn("999s", message)
        self.assertNotIn("Dhan funds API error", message)

    def test_auth_refresh_retry_retains_original_identity_priority(self):
        broker = object.__new__(DhanClient)
        broker.cfg = NS(
            dhan_client_id="SYNTHETIC", dhan_pin="", dhan_totp_secret=""
        )
        broker.token = "new"
        broker._headers = Mock(side_effect=[
            {"access-token": "old"}, {"access-token": "new"}
        ])
        broker._governed_get = Mock(side_effect=[
            NS(status_code=401, text="unauthorized"),
            NS(status_code=200, text="success"),
        ])
        broker._requires_token_refresh = Mock(return_value=True)
        broker._sync_shared_token_from_disk = Mock()
        response = broker._get_with_auth_recovery(
            f"{BASE_URL}/fundlimit", allow_refresh=True, priority="identity"
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(2, broker._governed_get.call_count)
        for call in broker._governed_get.call_args_list:
            self.assertEqual("identity", call.kwargs.get("priority"))

    def test_gate_uses_concrete_startup_probe_and_does_not_assume_green(self):
        class Broker:
            def __init__(self):
                self.startup_count = 0
                self.cfg = NS(dhan_client_id="SYNTHETIC")

            def get_reconciliation_fund_limits(self):
                self.startup_count += 1
                return True, 1234.0, "verified"

            def get_fund_limits(self):
                raise AssertionError("Normal funds path used at startup")

            def get_trade_book(self):
                return BrokerReadFailure(
                    status=BrokerReadStatus.UNAVAILABLE.value,
                    error="synthetic tradebook unavailable",
                    status_code=503,
                )

        broker = Broker()
        gate = StartupReconciliationGate(
            broker=broker, journal_db=None, watcher=None,
            stop_manager=NS(is_stopped=lambda: False),
        )
        ok, _ = gate.run_startup_reconciliation()
        self.assertFalse(ok)  # another required broker read is still missing
        self.assertEqual(1, broker.startup_count)


if __name__ == "__main__":
    unittest.main()
