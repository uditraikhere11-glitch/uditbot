"""Offline DhanClient typed lookup tests. All transport is mocked."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from src.broker.dhan_client import DhanClient
from src.models import TradingMode


class TestDhanTypedLookup(unittest.TestCase):
    def setUp(self):
        self.broker=object.__new__(DhanClient)
        self.broker.cfg=SimpleNamespace(trading_mode=TradingMode.LIVE,dhan_client_id="ACC")
        self.broker._get_with_auth_recovery=Mock()

    def test_test_mode_does_not_call_network(self):
        self.broker.cfg.trading_mode=TradingMode.TEST
        x=self.broker.diagnose_external_order_lookup("CID")
        self.assertEqual(x.kind,"NOT_QUERIED_TEST_MODE")
        self.broker._get_with_auth_recovery.assert_not_called()

    def test_http_404_never_permits_resubmit(self):
        self.broker._get_with_auth_recovery.return_value=SimpleNamespace(status_code=404)
        x=self.broker.diagnose_external_order_lookup("CID")
        self.assertEqual(x.kind,"NOT_FOUND_UNVERIFIED")
        self.assertFalse(x.may_resubmit)

    def test_200_identity_validated_but_fill_unverified(self):
        self.broker._get_with_auth_recovery.return_value=SimpleNamespace(
            status_code=200,json=lambda:{"orderId":"OID","correlationId":"CID",
                    "dhanClientId":"ACC","securityId":"SEC","orderStatus":"TRADED"})
        x=self.broker.diagnose_external_order_lookup("CID",expected_security_id="SEC")
        self.assertEqual(x.kind,"FOUND_UNVERIFIED_STATUS")
        self.assertFalse(x.may_mark_filled)

    def test_timeout_keeps_unknown(self):
        self.broker._get_with_auth_recovery.side_effect=TimeoutError()
        x=self.broker.diagnose_external_order_lookup("CID")
        self.assertEqual(x.kind,"TRANSPORT_UNKNOWN")
        self.assertFalse(x.may_resubmit)

    def test_invalid_id_does_not_call_network(self):
        self.broker._get_with_auth_recovery.reset_mock()
        x=self.broker.diagnose_external_order_lookup("CID/../../path")
        self.assertEqual(x.kind,"INVALID_LOOKUP")
        self.broker._get_with_auth_recovery.assert_not_called()

    def test_unreadable_json_is_malformed(self):
        self.broker._get_with_auth_recovery.return_value=SimpleNamespace(
            status_code=200,json=Mock(side_effect=ValueError()))
        self.assertEqual(self.broker.diagnose_external_order_lookup("CID").kind,"MALFORMED")
