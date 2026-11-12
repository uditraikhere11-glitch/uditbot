"""Isolated regression tests for persistent Dhan DH-901 errors and token-file sharing.

These tests use only mocked HTTP and temporary token paths: zero real broker
credentials, remote API calls, trade orders or modifications to the user's home.
"""
import os
import stat
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.broker.dhan_client import DhanClient
from src.broker.dhan_token import read_token, write_token_atomic
from src.models import BrokerReadFailure, BrokerReadList, TradingMode


def response(status, text="", records=None):
    mock = Mock()
    mock.status_code = status
    mock.text = text
    mock.json.return_value = records
    return mock


class TestSharedAuthFailureCooldown(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "udit-token")
        write_token_atomic("expired-token", self.path)
        self.client = object.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(
            dhan_token_file=self.path,
            dhan_client_id="udit-account",
            dhan_pin="pin",
            dhan_totp_secret="totp-secret",
        )
        self.client.token = "expired-token"
        self.client._token_mtime = 0.0

    def test_failed_login_creates_private_marker_not_repeated_every_poll(self):
        self.client._perform_totp_http_login = Mock(
            return_value=(False, "", "Dhan auth endpoint timed out")
        )
        ok, token, msg = self.client.generate_token_with_totp(
            force=False, rejected_token="expired-token"
        )
        self.assertFalse(ok)
        self.assertEqual(token, "")
        self.assertEqual(read_token(self.path), "expired-token")
        self.assertEqual(self.client._perform_totp_http_login.call_count, 1)
        marker = self.path + ".auth_retry_after"
        self.assertTrue(os.path.isfile(marker))
        self.assertEqual(stat.S_IMODE(os.stat(marker).st_mode), 0o600)
        with open(marker, encoding="ascii") as f:
            state = f.read()
        self.assertNotIn("pin", state)
        self.assertNotIn("expired-token", state)

        for _ in range(4):
            ok2, token2, msg2 = self.client.generate_token_with_totp(
                force=False, rejected_token="expired-token"
            )
            self.assertFalse(ok2)
            self.assertEqual(token2, "")
            self.assertIn("cooldown", msg2)
        self.client._perform_totp_http_login.assert_called_once()

    def test_scheduled_force_login_can_bypass_marker_and_clear_it(self):
        self.client._perform_totp_http_login = Mock(
            side_effect=[
                (False, "", "Dhan auth endpoint timed out"),
                (True, "fresh-token", "Token created"),
            ]
        )
        self.assertFalse(self.client.generate_token_with_totp(
            force=False, rejected_token="expired-token"
        )[0])
        self.assertTrue(os.path.exists(self.path + ".auth_retry_after"))
        ok, tok, _ = self.client.generate_token_with_totp(force=True)
        self.assertTrue(ok)
        self.assertEqual(tok, "fresh-token")
        self.assertEqual(read_token(self.path), "fresh-token")
        self.assertFalse(os.path.exists(self.path + ".auth_retry_after"))
        self.assertEqual(self.client._perform_totp_http_login.call_count, 2)

    def test_shared_backoff_respects_distinct_account_token_paths(self):
        another = os.path.join(self.tmp.name, "aman-token")
        write_token_atomic("expired-aman", another)
        self.client._perform_totp_http_login = Mock(
            return_value=(False, "", "Dhan timeout")
        )
        self.client.generate_token_with_totp(
            force=False, rejected_token="expired-token"
        )
        second_client = object.__new__(DhanClient)
        second_client.cfg = SimpleNamespace(
            dhan_token_file=another,
            dhan_client_id="aman-account",
            dhan_pin="pin", dhan_totp_secret="secret",
        )
        second_client.token = "expired-aman"
        second_client._perform_totp_http_login = Mock(
            return_value=(True, "fresh-aman", "OK")
        )
        ok, token, _ = second_client.generate_token_with_totp(
            force=False, rejected_token="expired-aman"
        )
        self.assertTrue(ok)
        self.assertEqual(token, "fresh-aman")
        self.assertEqual(read_token(another), "fresh-aman")
        self.assertEqual(read_token(self.path), "expired-token")


class TestAtomicTokenFileAdoption(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "shared-token")
        write_token_atomic("expired", self.path)
        self.client = object.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(dhan_token_file=self.path)
        self.client.token = "expired"
        old_stat = os.stat(self.path)
        self.old_stamp = old_stat
        self.client._token_mtime = old_stat.st_mtime
        self.client._token_file_signature = DhanClient._token_stat_signature(old_stat)
        self.client._auth_read_pause_token = "expired"
        self.client._auth_read_pause_until = 1e30

    def test_atomic_replace_adopted_even_if_mtime_goes_backwards(self):
        write_token_atomic("fresh", self.path)
        old_time = self.old_stamp.st_mtime_ns - 1000000000
        os.utime(self.path, ns=(old_time, old_time))
        self.assertLess(os.stat(self.path).st_mtime_ns, self.old_stamp.st_mtime_ns)
        self.client._sync_shared_token_from_disk()
        self.assertEqual(self.client.token, "fresh")
        self.assertEqual(self.client._auth_read_pause_until, 0.0)
        self.assertEqual(self.client._auth_read_pause_token, "")

    def test_unchanged_file_does_not_reopen_or_overwrite_memory(self):
        with patch("src.broker.dhan_client.read_token",
                   side_effect=AssertionError("unchanged file re-read")):
            self.client._sync_shared_token_from_disk()
        self.assertEqual(self.client.token, "expired")


class TestReadCircuitAndFailClosed(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="test-account",
            dhan_pin="test-pin",
            dhan_totp_secret="test-secret",
        )
        self.client.token = "expired"
        self.client._headers = Mock(
            side_effect=lambda: {"access-token": self.client.token}
        )
        self.client._sync_shared_token_from_disk = Mock()
        self.client.generate_token_with_totp = Mock(
            return_value=(False, "", "Auth endpoint unreachable")
        )
        self.client.session = Mock()
        self.client._auth_log_at = {}

    def test_outage_skips_repeated_broker_reads_but_returns_failure(self):
        self.client.session.get.return_value = response(
            401, '{"errorCode":"DH-901"}'
        )
        with patch("src.broker.dhan_client.logger.error") as log_error:
            first = self.client.get_order_book()
            second = self.client.get_order_book()
            third = self.client.get_order_book()
        self.assertIsInstance(first, BrokerReadFailure)
        self.assertIsInstance(second, BrokerReadFailure)
        self.assertIsInstance(third, BrokerReadFailure)
        self.assertEqual(first.status_code, 401)
        self.assertEqual(second.status_code, 401)
        self.assertEqual(self.client.session.get.call_count, 1)
        self.client.generate_token_with_totp.assert_called_once()
        self.assertEqual(log_error.call_count, 1)

    def test_new_token_bypasses_pause_and_resumes_broker_reads_immediately(self):
        self.client.session.get.side_effect = [
            response(401, '{"errorCode":"DH-901"}'),
            response(200, "", [{"orderId": "123"}]),
        ]
        failed = self.client.get_order_book()
        self.assertIsInstance(failed, BrokerReadFailure)
        self.client.token = "new-token-from-scheduled-timer"
        good = self.client.get_order_book()
        self.assertIsInstance(good, BrokerReadList)
        self.assertEqual(good[0]["orderId"], "123")
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_new_token_rejected_twice_is_not_refreshed_in_a_tight_loop(self):
        def renew(*, force, rejected_token):
            self.client.token = "new-but-rejected"
            return True, "new-but-rejected", "OK"
        self.client.generate_token_with_totp.side_effect = renew
        self.client.session.get.side_effect = [
            response(401, "DH-901"),
            response(401, "DH-901"),
        ]
        first = self.client.get_positions()
        self.assertIsInstance(first, BrokerReadFailure)
        second = self.client.get_positions()
        self.assertIsInstance(second, BrokerReadFailure)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_non_auth_service_unavailable_still_reaches_broker_again(self):
        self.client.session.get.return_value = response(503, "Unavailable")
        self.assertIsInstance(self.client.get_order_book(), BrokerReadFailure)
        self.assertIsInstance(self.client.get_order_book(), BrokerReadFailure)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_not_called()


if __name__ == "__main__":
    unittest.main()
