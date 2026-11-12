"""Offline regression coverage for read-only Dhan DH-901 recovery (PR #72 baseline)."""

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from src.broker.dhan_client import DhanClient
from src.models import BrokerReadFailure, BrokerReadList, TradingMode


def _response(status, payload=None, text=""):
    r = Mock()
    r.status_code = status
    r.text = text
    r.json.return_value = payload
    return r


class TestDhanReadAuthRecovery(unittest.TestCase):
    def setUp(self):
        # No broker session, token file, or network is created by these tests.
        self.client = object.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="test-account-1",
            dhan_pin="test-pin",
            dhan_totp_secret="test-secret",
        )
        self.client.token = "expired"
        self.client.session = Mock()
        self.client._headers = Mock(side_effect=lambda: {"access-token": self.client.token})
        self.client._sync_shared_token_from_disk = Mock()

        def refresh(*, force, rejected_token):
            self.assertFalse(force)
            self.assertEqual(rejected_token, "expired")
            self.client.token = "fresh"
            return True, "fresh", "OK"

        self.client.generate_token_with_totp = Mock(side_effect=refresh)

    def test_order_book_401_then_200_adopts_new_token_and_keeps_records(self):
        self.client.session.get.side_effect = [
            _response(401, text='{"errorCode":"DH-901"}'),
            _response(200, [{"orderId": "AB", "orderStatus": "PENDING"}]),
        ]
        result = self.client.get_order_book()
        self.assertIsInstance(result, BrokerReadList)
        self.assertEqual(result[0]["orderId"], "AB")
        self.assertEqual(self.client.session.get.call_count, 2)
        used = [c.kwargs["headers"]["access-token"] for c in self.client.session.get.call_args_list]
        self.assertEqual(used, ["expired", "fresh"])
        self.client.generate_token_with_totp.assert_called_once()

    def test_positions_401_then_200_uses_same_read_recovery(self):
        self.client.session.get.side_effect = [
            _response(401, text="Invalid_Authentication DH-901"),
            _response(200, [{"securityId": "123", "netQty": 1}]),
        ]
        self.assertEqual(len(self.client.get_positions()), 1)
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_second_401_fails_closed_without_second_login(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"), _response(401, text="DH-901")
        ]
        result = self.client.get_order_book()
        self.assertIsInstance(result, BrokerReadFailure)
        self.assertEqual(result.status_code, 401)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_refresh_failure_never_turns_401_into_empty_orders(self):
        self.client.generate_token_with_totp = Mock(return_value=(False, "", "bad TOTP"))
        self.client.session.get.return_value = _response(401, text="DH-901")
        result = self.client.get_order_book()
        self.assertIsInstance(result, BrokerReadFailure)
        self.assertNotEqual(result, [])
        self.assertEqual(self.client.session.get.call_count, 1)

    def test_missing_totp_never_retries_or_mutates(self):
        self.client.cfg.dhan_pin = ""
        self.client.session.get.return_value = _response(401, text="DH-901")
        self.assertIsInstance(self.client.get_positions(), BrokerReadFailure)
        self.client.generate_token_with_totp.assert_not_called()
        self.assertEqual(self.client.session.get.call_count, 1)

    def test_external_token_rotation_adopted_without_new_totp(self):
        self.client._sync_shared_token_from_disk.side_effect = lambda: setattr(
            self.client, "token", "already-refreshed-by-timer"
        )
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"), _response(200, [])
        ]
        self.assertIsInstance(self.client.get_order_book(), BrokerReadList)
        self.client.generate_token_with_totp.assert_not_called()
        self.assertEqual(
            self.client.session.get.call_args_list[1].kwargs["headers"]["access-token"],
            "already-refreshed-by-timer",
        )

    def test_non_auth_failure_has_no_totp_retry(self):
        self.client.session.get.return_value = _response(503, text="Unavailable")
        self.assertIsInstance(self.client.get_order_book(), BrokerReadFailure)
        self.assertEqual(self.client.session.get.call_count, 1)
        self.client.generate_token_with_totp.assert_not_called()

    def test_single_order_401_then_200_requires_exact_order_id(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"),
            _response(200, {"orderId": "ABC", "orderStatus": "PENDING"}),
        ]
        self.assertEqual(self.client.order_status("ABC")["orderId"], "ABC")
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_single_order_wrong_id_is_not_authenticated_as_expected_order(self):
        self.client.session.get.return_value = _response(
            200, {"orderId": "DIFFERENT", "orderStatus": "TRADED"}
        )
        self.client.get_order_book = Mock(return_value=BrokerReadList([]))
        self.assertIsNone(self.client.order_status("EXPECTED"))
        self.client.get_order_book.assert_called_once()

    def test_single_order_repeated_401_skips_second_refresh_via_fallback(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"), _response(401, text="DH-901")
        ]
        self.client.get_order_book = Mock()
        self.assertIsNone(self.client.order_status("ABC"))
        self.client.get_order_book.assert_not_called()
        self.client.generate_token_with_totp.assert_called_once()

    def test_manual_cancelled_order_book_fallback_preserved(self):
        self.client.session.get.return_value = _response(404, text="Not found")
        self.client.get_order_book = Mock(return_value=BrokerReadList([
            {"orderId": "ABC", "orderStatus": "CANCELLED"}
        ]))
        self.assertEqual(self.client.order_status("ABC")["orderStatus"], "CANCELLED")

    def test_test_mode_bypass_remains_real_read(self):
        self.client.cfg.trading_mode = TradingMode.TEST
        self.client.session.get.return_value = _response(200, [
            {"orderId": "ABC", "orderStatus": "CANCELLED"}
        ])
        self.assertEqual(self.client.get_order_book(), [])
        self.assertEqual(self.client.session.get.call_count, 0)
        self.assertEqual(len(self.client.get_order_book(bypass_test_mode=True)), 1)
        self.assertEqual(
            self.client.order_status("ABC", bypass_test_mode=True)["orderId"], "ABC"
        )


    def test_live_funds_401_then_200_recovers_once(self):
        self.client.session.get.side_effect = [
            _response(401, text='{"errorCode":"DH-901"}'),
            _response(200, {"availabelBalance": 775.50}),
        ]
        ok, balance, _ = self.client.get_fund_limits()
        self.assertTrue(ok)
        self.assertEqual(balance, 775.50)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_live_funds_repeated_401_remains_auth_error(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"), _response(401, text="DH-901")
        ]
        ok, balance, message = self.client.get_fund_limits()
        self.assertFalse(ok)
        self.assertEqual(balance, 0.0)
        self.assertIn("auth error", message)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_token_verification_funds_retry_false_never_rotates(self):
        self.client.session.get.return_value = _response(401, text="DH-901")
        ok, _, _ = self.client.get_fund_limits(retry=False)
        self.assertFalse(ok)
        self.client.generate_token_with_totp.assert_not_called()
        self.assertEqual(self.client.session.get.call_count, 1)


class TestDhanTokenAccountIsolation(unittest.TestCase):
    def test_separate_account_token_paths_never_cross(self):
        from src.broker.dhan_token import fresh_token, read_token
        with tempfile.TemporaryDirectory() as directory:
            udit = os.path.join(directory, "udit-account.token")
            aman = os.path.join(directory, "aman-account.token")
            self.assertEqual(fresh_token("", lambda: "udit-token", path=udit), "udit-token")
            self.assertEqual(fresh_token("", lambda: "aman-token", path=aman), "aman-token")
            self.assertEqual(read_token(udit), "udit-token")
            self.assertEqual(read_token(aman), "aman-token")
            wrong_login = Mock(side_effect=AssertionError("must adopt existing token"))
            self.assertEqual(
                fresh_token("expired-token", wrong_login, path=udit), "udit-token"
            )
            wrong_login.assert_not_called()


class TestScheduledRefreshStartup(unittest.TestCase):
    def test_missing_file_does_not_implicitly_login_before_scheduled_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = SimpleNamespace(
                dhan_client_id="test-account-1", dhan_pin="123456",
                dhan_totp_secret="test-secret", dhan_access_token="",
                dhan_token_file=os.path.join(directory, "nonexistent-token"),
                data_dir=directory,
            )
            with patch.object(
                DhanClient, "generate_token_with_totp",
                side_effect=AssertionError("implicit login would rotate token twice"),
            ):
                client = DhanClient(
                    cfg, stop_manager=Mock(), auto_generate_token=False
                )
            self.assertEqual(client.token, "")


class TestScheduledTokenVerification(unittest.TestCase):
    def _cfg(self, path):
        return SimpleNamespace(
            dhan_client_id="test-account-1", dhan_pin="pin",
            dhan_totp_secret="secret", dhan_token_file=path,
        )

    def test_transient_401_waits_and_verifies_without_repeat_login(self):
        from src.cli import cmd_refresh_token
        with tempfile.TemporaryDirectory() as directory:
            cfg = self._cfg(os.path.join(directory, "token"))
            fake = Mock()
            fake.generate_token_with_totp.return_value = (True, "fresh", "OK")
            fake.verify_token_authenticated.side_effect = [
                (False, 0.0, "Dhan auth error (401): DH-901"),
                (False, 0.0, "Dhan auth error (401): DH-901"),
                (True, 100.0, "Funds retrieved successfully"),
            ]
            with patch("src.broker.dhan_client.DhanClient", return_value=fake), patch(
                "src.cli.time.sleep"
            ) as sleep:
                self.assertTrue(cmd_refresh_token(cfg, object()))
            fake.generate_token_with_totp.assert_called_once_with(force=True)
            self.assertEqual(sleep.call_args_list, [call(2), call(3)])

    def test_failed_verification_retains_new_token_without_rollback(self):
        from src.cli import cmd_refresh_token
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "token")
            with open(path, "w", encoding="utf-8") as f:
                f.write("old")
            cfg = self._cfg(path)
            fake = Mock()

            def generate(*, force):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("new")
                return True, "new", "OK"

            fake.generate_token_with_totp.side_effect = generate
            fake.verify_token_authenticated.return_value = (False, 0.0, "Wrong account")
            with patch("src.broker.dhan_client.DhanClient", return_value=fake), patch(
                "src.cli.time.sleep"
            ) as sleep:
                self.assertFalse(cmd_refresh_token(cfg, object()))
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read(), "new")
            fake.generate_token_with_totp.assert_called_once()
            sleep.assert_not_called()

    def test_daily_health_never_marks_test_fallback_as_verified(self):
        from src.cli import cmd_daily_health_check
        cfg = SimpleNamespace(data_dir="/tmp", scrip_master_max_age_sec=3600)
        fake = Mock()
        fake.get_fund_limits.return_value = (True, 100000.0, "Test mode fallback")
        fake.last_funds_is_fallback = True
        with patch("src.cli.cmd_refresh_token", return_value=True), patch(
            "src.broker.dhan_client.DhanClient", return_value=fake
        ), patch(
            "src.instruments.resolver.InstrumentResolver.load_dhan_master",
            return_value=True
        ):
            code = cmd_daily_health_check(cfg, Mock(), object())
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
