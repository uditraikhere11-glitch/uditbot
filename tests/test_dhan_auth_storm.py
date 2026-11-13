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
        # A fresh token gets a brief propagation grace marker rather than
        # having its old failed-login marker removed unconditionally.
        import hashlib
        with open(self.path + ".auth_retry_after", encoding="ascii") as f:
            marker = f.read().split()
        self.assertEqual(len(marker), 2)
        self.assertEqual(
            marker[1], hashlib.sha256(b"fresh-token").hexdigest()
        )
        self.assertEqual(self.client._perform_totp_http_login.call_count, 2)

    def test_newly_issued_token_401_does_not_cause_instant_relogin(self):
        import hashlib
        from src.broker.dhan_client import NEW_TOKEN_PROPAGATION_GRACE_SEC
        self.client._perform_totp_http_login = Mock(
            side_effect=[
                (True, "just-issued-token", "OK"),
                (True, "later-token", "OK"),
            ]
        )
        ok, token, _ = self.client.generate_token_with_totp(force=True)
        self.assertTrue(ok)
        self.assertEqual(token, "just-issued-token")
        # Dhan can briefly respond DH-901 while API nodes propagate a newly
        # issued token. Continue broker reads; do NOT issue a fresh login yet.
        for _ in range(3):
            ok2, new_tok, reason = self.client.generate_token_with_totp(
                force=False, rejected_token="just-issued-token"
            )
            self.assertFalse(ok2)
            self.assertEqual(new_tok, "")
            self.assertIn("cooldown", reason)
        self.client._perform_totp_http_login.assert_called_once()
        with open(self.path + ".auth_retry_after", encoding="ascii") as f:
            marker = f.read().split()
        self.assertEqual(
            marker[1], hashlib.sha256(b"just-issued-token").hexdigest()
        )
        # The grace is shorter than a failed-auth cooldown.
        until = float(marker[0])
        self.assertGreater(until, 0)

        # Once that short grace expires, a still-rejected token may be
        # regenerated once through the normal shared-file coordinator.
        with open(self.path + ".auth_retry_after", "w", encoding="ascii") as f:
            f.write(f"0 {marker[1]}")
        ok3, final, _ = self.client.generate_token_with_totp(
            force=False, rejected_token="just-issued-token"
        )
        self.assertTrue(ok3)
        self.assertEqual(final, "later-token")
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


    def test_new_rejected_token_is_not_blocked_by_old_token_cooldown(self):
        self.client._perform_totp_http_login = Mock(
            side_effect=[
                (False, "", "Old token: auth server unavailable"),
                (True, "valid-final-token", "OK"),
            ]
        )
        old = self.client.generate_token_with_totp(
            force=False, rejected_token="expired-token"
        )
        self.assertFalse(old[0])
        # An external process issued a DIFFERENT token, which later gets
        # rejected. Its recovery must not inherit the previous token's marker.
        write_token_atomic("new-but-invalid", self.path)
        self.client.token = "new-but-invalid"
        ok, tok, _ = self.client.generate_token_with_totp(
            force=False, rejected_token="new-but-invalid"
        )
        self.assertTrue(ok)
        self.assertEqual(tok, "valid-final-token")
        self.assertEqual(read_token(self.path), "valid-final-token")
        self.assertEqual(self.client._perform_totp_http_login.call_count, 2)
        # The old rejected token no longer owns the marker: the newly issued
        # token gets its own short grace marker.
        import hashlib
        with open(self.path + ".auth_retry_after", encoding="ascii") as f:
            marker = f.read().split()
        self.assertEqual(marker[1], hashlib.sha256(b"valid-final-token").hexdigest())

    def test_legacy_float_only_marker_is_honored_to_avoid_upgrade_stampede(self):
        import time
        marker = self.path + ".auth_retry_after"
        with open(marker, "w", encoding="ascii") as f:
            f.write(str(time.time() + 60))
        os.chmod(marker, 0o600)
        self.client._perform_totp_http_login = Mock(
            side_effect=AssertionError("login must respect legacy marker")
        )
        ok, token, reason = self.client.generate_token_with_totp(
            force=False, rejected_token="expired-token"
        )
        self.assertFalse(ok)
        self.assertEqual(token, "")
        self.assertIn("cooldown", reason)
        self.client._perform_totp_http_login.assert_not_called()


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

    def test_atomic_replace_adopted_even_if_mtime_goes_backwards(self):
        write_token_atomic("fresh", self.path)
        old_time = self.old_stamp.st_mtime_ns - 1000000000
        os.utime(self.path, ns=(old_time, old_time))
        self.assertLess(os.stat(self.path).st_mtime_ns, self.old_stamp.st_mtime_ns)
        self.client._sync_shared_token_from_disk()
        self.assertEqual(self.client.token, "fresh")
        self.assertEqual(
            self.client._token_file_signature,
            self.client._token_stat_signature(os.stat(self.path)),
        )

    def test_unchanged_file_does_not_reopen_or_overwrite_memory(self):
        with patch("src.broker.dhan_client.read_token",
                   side_effect=AssertionError("unchanged file re-read")):
            self.client._sync_shared_token_from_disk()
        self.assertEqual(self.client.token, "expired")


class TestReadContinuityAndFailClosed(unittest.TestCase):
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

    def test_outage_keeps_read_probes_active_but_throttles_duplicate_logs(self):
        # Failed authentication must not make the protection watcher blind for
        # 15 seconds. The shared token-file cooldown prevents repeated LOGIN,
        # while read-only order status checks continue every watcher cycle.
        self.client.session.get.return_value = response(
            401, '{"errorCode":"DH-901"}'
        )
        with patch("src.broker.dhan_client.logger.error") as log_error:
            outcomes = [self.client.get_order_book() for _ in range(3)]
        for result in outcomes:
            self.assertIsInstance(result, BrokerReadFailure)
            self.assertEqual(result.status_code, 401)
            self.assertNotEqual(result, [])
        self.assertEqual(self.client.session.get.call_count, 3)
        self.assertEqual(log_error.call_count, 1)

    def test_new_token_is_used_on_next_broker_read(self):
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

    def test_two_rejected_tokens_trigger_only_one_login_in_a_single_read(self):
        def renew(*, force, rejected_token):
            self.client.token = "new-but-rejected"
            return True, "new-but-rejected", "OK"
        self.client.generate_token_with_totp.side_effect = renew
        self.client.session.get.side_effect = [
            response(401, "DH-901"),
            response(401, "DH-901"),
        ]
        result = self.client.get_positions()
        self.assertIsInstance(result, BrokerReadFailure)
        self.assertEqual(result.status_code, 401)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_next_read_recovers_immediately_after_transient_401(self):
        # No token-file modification is needed when a broker-side temporary
        # authentication rejection clears. PR #74's 15s block failed this.
        self.client.session.get.side_effect = [
            response(401, "DH-901"),
            response(200, "", [{"orderId": "OK-NOW"}]),
        ]
        first = self.client.get_order_book()
        second = self.client.get_order_book()
        self.assertIsInstance(first, BrokerReadFailure)
        self.assertIsInstance(second, BrokerReadList)
        self.assertEqual(second[0]["orderId"], "OK-NOW")
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_non_auth_service_unavailable_still_reaches_broker_again(self):
        self.client.session.get.return_value = response(503, "Unavailable")
        self.assertIsInstance(self.client.get_order_book(), BrokerReadFailure)
        self.assertIsInstance(self.client.get_order_book(), BrokerReadFailure)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_not_called()


if __name__ == "__main__":
    unittest.main()
