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


class TestDhanTotpTimeoutRecovery(unittest.TestCase):
    def setUp(self):
        import requests
        self.requests = requests
        self.client = object.__new__(DhanClient)
        self.client.cfg = SimpleNamespace(
            dhan_client_id="test-account-1",
            dhan_pin="test-pin",
            dhan_totp_secret="test-secret",
        )
        self.client.session = Mock()

    def test_read_timeout_one_retry_then_success_no_order_post(self):
        from src.broker.dhan_client import AUTH_URL
        self.client.session.post.side_effect = [
            self.requests.exceptions.ReadTimeout("Dhan auth read timed out"),
            _response(200, {
                "dhanClientId": "test-account-1",
                "accessToken": "verified-token",
            }),
        ]
        with patch("src.broker.dhan_client.compute_totp",
            side_effect=["111111", "222222"]) as totp, patch(
            "src.broker.dhan_client.time.sleep"
        ) as sleep, patch.object(
            self.client, "verify_profile_and_token_expiry",
            return_value={"identity_verified": True, "identity_status": "VERIFIED", "token_expired": False},
        ) as verified_profile:
            ok, token, message = self.client._perform_totp_http_login()
            self.assertTrue(ok, message)
            self.assertEqual(token, "verified-token")
            verified_profile.assert_called_once()
            self.assertEqual(verified_profile.call_args.kwargs["candidate_token"], "verified-token")
            self.assertEqual(self.client.session.post.call_count, 2)
            for call_args in self.client.session.post.call_args_list:
                self.assertEqual(call_args.args[0], AUTH_URL)
                self.assertEqual(call_args.kwargs["timeout"], (5, 25))
                self.assertEqual(
                    [call_args.kwargs["params"]["totp"]
                    for call_args in self.client.session.post.call_args_list],
                    ["111111", "222222"],
                )
            self.assertEqual(totp.call_count, 2)
            sleep.assert_called_once_with(2)

    def test_second_timeout_is_failure_with_at_most_two_calls(self):
        self.client.session.post.side_effect = [
            self.requests.exceptions.ReadTimeout("attempt one"),
            self.requests.exceptions.ReadTimeout("attempt two"),
        ]
        with patch("src.broker.dhan_client.compute_totp",
            side_effect=["111111", "222222"]), patch(
            "src.broker.dhan_client.time.sleep"
        ):
            ok, token, message = self.client._perform_totp_http_login()
            self.assertFalse(ok)
            self.assertEqual(token, "")
            self.assertIn("unavailable", message)
            self.assertNotIn("test-pin", message)
            self.assertEqual(self.client.session.post.call_count, 2)

    def test_wrong_pin_http_403_does_not_generate_another_totp(self):
        self.client.session.post.return_value = _response(
            403, text='{"errorMessage":"Incorrect PIN"}'
        )
        with patch("src.broker.dhan_client.compute_totp", return_value="111111"), patch(
            "src.broker.dhan_client.time.sleep"
        ) as sleep:
            ok, token, message = self.client._perform_totp_http_login()
            self.assertFalse(ok)
            self.assertEqual(token, "")
            self.assertIn("403", message)
            self.assertEqual(self.client.session.post.call_count, 1)
            sleep.assert_not_called()

    def test_tls_certificate_error_is_not_retried(self):
        self.client.session.post.side_effect = self.requests.exceptions.SSLError(
            "certificate verify failed"
        )
        with patch("src.broker.dhan_client.compute_totp", return_value="111111"), patch(
            "src.broker.dhan_client.time.sleep"
        ) as sleep:
            ok, token, message = self.client._perform_totp_http_login()
            self.assertFalse(ok)
            self.assertEqual(token, "")
            self.assertIn("TLS", message)
            self.assertEqual(self.client.session.post.call_count, 1)
            sleep.assert_not_called()


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

    def test_unrelated_403_permission_denial_never_rotates_token(self):
        self.client.session.get.return_value = _response(403, text="Permission denied")
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


    def test_explicit_403_invalid_authentication_recovers(self):
        self.client.session.get.side_effect = [
            _response(403, text='{"errorCode":"DH-901","errorMessage":"Invalid_Authentication"}'),
            _response(200, [{"orderId": "A", "orderStatus": "PENDING"}]),
        ]
        self.assertEqual(len(self.client.get_order_book()), 1)
        self.assertEqual(self.client.session.get.call_count, 2)
        self.client.generate_token_with_totp.assert_called_once()

    def test_403_expired_access_token_recovers_without_dh901_code(self):
        self.client.session.get.side_effect = [
            _response(403, text="Access token expired"),
            _response(200, []),
        ]
        self.assertIsInstance(self.client.get_order_book(), BrokerReadList)
        self.client.generate_token_with_totp.assert_called_once()

    def test_correlation_id_read_uses_one_recovery(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"),
            _response(200, {"orderId": "OID-1", "correlationId": "C-1"}),
        ]
        self.assertEqual(
            self.client.get_order_by_correlation_id("C-1")["orderId"], "OID-1"
        )
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_trade_fill_read_401_then_valid_empty_book(self):
        self.client.session.get.side_effect = [
            _response(401, text="DH-901"), _response(200, []),
        ]
        result = self.client.get_trades_for_order("OID-1")
        self.assertIsInstance(result, BrokerReadList)
        self.assertEqual(len(result), 0)
        self.client.generate_token_with_totp.assert_called_once()

    def test_account_profile_403_token_expired_uses_recovery(self):
        self.client.cfg.dhan_client_id = "1234567"
        self.client.session.get.side_effect = [
            _response(403, text="invalid token"),
            _response(200, {"dhanClientId": "1234567"}),
        ]
        self.assertEqual(self.client.get_authenticated_profile_id(), "1234567")
        self.assertEqual(self.client.session.get.call_count, 2)

    def test_test_mode_funds_auth_recovery_is_not_paper_fallback(self):
        self.client.cfg.trading_mode = TradingMode.TEST
        self.client.session.get.side_effect = [
            _response(403, text="token expired"),
            _response(200, {"availabelBalance": 135.25}),
        ]
        ok, bal, _ = self.client.get_fund_limits()
        self.assertTrue(ok)
        self.assertEqual(bal, 135.25)
        self.assertFalse(self.client.last_funds_is_fallback)


    def test_read_only_marketfeed_post_403_token_expired_recovers_once(self):
        self.client.session.post.side_effect = [
            _response(403, text="Access token expired"),
            _response(200, {
                "data": {"NSE_FNO": {"777": {"last_price": 28.5}}}
            }),
        ]
        ok, ltp, msg = self.client.get_ltp(
            "NSE_FNO", "777", symbol=None,
        )
        self.assertTrue(ok, msg)
        self.assertEqual(ltp, 28.5)
        self.assertEqual(self.client.session.post.call_count, 2)
        used = [
            call.kwargs["headers"]["access-token"]
            for call in self.client.session.post.call_args_list
        ]
        self.assertEqual(used, ["expired", "fresh"])
        self.client.generate_token_with_totp.assert_called_once()

    def test_read_only_marketfeed_post_permission_403_does_not_rotate(self):
        self.client.session.post.return_value = _response(403, text="Permission denied")
        ok, ltp, _ = self.client.get_ltp("NSE_FNO", "777", symbol=None)
        self.assertFalse(ok)
        self.assertEqual(ltp, 0.0)
        self.assertEqual(self.client.session.post.call_count, 1)
        self.client.generate_token_with_totp.assert_not_called()


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


class TestDailyTelegramHealthReport(unittest.TestCase):
    def setUp(self):
        from src.config import AppConfig
        from src.journal.db import JournalDB
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1234567890",
            data_dir=self.tempdir.name,
            tg_bot_token="123456:OFFLINE_DUMMY",
            log_group_id=-100987654321,
            instance_name="AuditBot Udit",
        )
        self.db = JournalDB(
            os.path.join(self.tempdir.name, "journal.db"),
            account_id=self.cfg.dhan_client_id,
        )
        # Fixed time is representative of the 17:00 IST daily systemd job.
        self.fixed_ts = 1794483005.0

    def test_healthy_report_includes_account_mode_timestamp_and_real_funds(self):
        from src.cli import format_morning_health_report
        card = format_morning_health_report(
            self.cfg, now_ts=self.fixed_ts, token_verified=True,
            funds_verified=True, fallback_funds=False,
            available_balance=1200.5, master_loaded=True, is_stopped=False,
            account_identity_status="VERIFIED",
            token_expiration_str="2026-11-13 17:00:05 IST",
        )
        self.assertIn("AUDITBOT — 05:00 PM DHAN HEALTH CHECK", card)
        self.assertIn("17:00:", card)
        self.assertIn("***7890", card)
        self.assertIn("LIVE", card)
        self.assertIn("₹1,200.50", card)
        self.assertIn("VERIFIED CHECKS", card)
        self.assertIn("SUCCESS", card)
        self.assertNotIn("1234567890", card)

    def test_stop_master_warning_and_paper_balance_never_claim_ready(self):
        from src.cli import format_morning_health_report
        for stop, master, paper in (
            (True, True, False), (False, False, False), (False, True, True)
        ):
            card = format_morning_health_report(
                self.cfg, now_ts=self.fixed_ts, token_verified=True,
                funds_verified=not paper, fallback_funds=paper,
                available_balance=100000.0, master_loaded=master, is_stopped=stop,
            )
            self.assertIn("REVIEW REQUIRED", card)
            self.assertIn("ACTION REQUIRED", card)
            self.assertNotIn("VERIFIED CHECKS", card)
            self.assertNotIn("Overall result: ✅ SUCCESS", card)
            if paper:
                self.assertIn("TEST FALLBACK", card)

    def test_direct_bot_api_sends_only_once_per_ist_day(self):
        from src.cli import dispatch_morning_health_report_once
        with patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            return_value=True,
        ) as sender:
            a = dispatch_morning_health_report_once(
                self.cfg, self.db, "<b>morning report</b>", now_ts=self.fixed_ts
            )
            b = dispatch_morning_health_report_once(
                self.cfg, self.db, "<b>morning report</b>", now_ts=self.fixed_ts + 60
            )
            self.assertIn("DELIVERED", a)
            self.assertIn("ALREADY", b)
            sender.assert_called_once()
            self.assertEqual(self.db.fetch_pending_outbox(), [])

    def test_bot_api_failure_is_queued_once_and_not_claimed_delivered(self):
        from src.cli import dispatch_morning_health_report_once
        with patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            return_value=False,
        ) as sender:
            a = dispatch_morning_health_report_once(
                self.cfg, self.db, "Health report", now_ts=self.fixed_ts
            )
            b = dispatch_morning_health_report_once(
                self.cfg, self.db, "Health report", now_ts=self.fixed_ts + 10
            )
            self.assertIn("QUEUED", a)
            self.assertIn("ALREADY", b)
            sender.assert_called_once()
            messages = self.db.fetch_pending_outbox()
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["event_type"], "ORDER_STATUS_UPDATE")

    def test_without_bot_token_queues_for_running_telethon_worker(self):
        from src.cli import dispatch_morning_health_report_once
        self.cfg.tg_bot_token = ""
        with patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            side_effect=AssertionError("Bot API must not be contacted"),
        ):
            msg = dispatch_morning_health_report_once(
                self.cfg, self.db, "morning", now_ts=self.fixed_ts
            )
            self.assertIn("QUEUED", msg)
            self.assertEqual(len(self.db.fetch_pending_outbox()), 1)

    def test_failed_auth_still_sends_truthful_morning_alert(self):
        from src.cli import cmd_daily_health_check
        broker = Mock()
        broker.get_fund_limits.return_value = (
            False, 0.0, "Dhan auth error (401): DH-901",
        )
        broker.last_funds_is_fallback = False
        stop_mgr = Mock()
        stop_mgr.get_state.return_value = {"is_stopped": True}
        cards = []
        with patch("src.cli.cmd_refresh_token", return_value=False), patch(
            "src.broker.dhan_client.DhanClient", return_value=broker
        ), patch(
            "src.instruments.resolver.InstrumentResolver.load_dhan_master",
            return_value=False
        ), patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            side_effect=lambda body: cards.append(body) or True
        ):
            rc = cmd_daily_health_check(self.cfg, self.db, stop_mgr)
            self.assertEqual(rc, 1)
            self.assertEqual(len(cards), 1)
            self.assertIn("Dhan authentication: ❌ FAILED", cards[0])
            self.assertIn("Emergency STOP: 🔴 ACTIVE", cards[0])
            self.assertIn("REVIEW REQUIRED", cards[0])
            broker.generate_token_with_totp.assert_not_called()

    def test_unconfigured_log_group_never_claims_delivered(self):
        from src.cli import dispatch_morning_health_report_once
        self.cfg.log_group_id = 0
        status = dispatch_morning_health_report_once(
            self.cfg, self.db, "morning", now_ts=self.fixed_ts
        )
        self.assertIn("NOT_CONFIGURED", status)
        self.assertEqual(self.db.fetch_pending_outbox(), [])


if __name__ == "__main__":
    unittest.main()
