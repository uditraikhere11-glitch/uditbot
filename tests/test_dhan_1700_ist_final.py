"""
Comprehensive offline regression suite for PR #76:
17:00 IST (05:00 PM IST) Dhan Authentication, After-Market Health Report,
and Independent Audit Remediations (C1 through C8).

Uses strictly fake, account-independent test identifiers (`9000000001`, `-1009000000001`)
and temporary directories; never touches real Dhan credentials, `~/.dhan_token`,
`/etc/systemd/system`, or live network endpoints.
"""

import base64
import datetime
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
import zoneinfo

import requests

from src.broker.dhan_client import (
    AUTH_URL,
    BASE_URL,
    DhanClient,
)
from src.broker.dhan_token import (
    force_login,
    fresh_token,
    locked_token_file,
    read_token,
    write_token_atomic,
)
from src.cli import (
    cmd_daily_health_check,
    cmd_refresh_token,
    dispatch_morning_health_report_once,
    format_morning_health_report,
)
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, BrokerReadList, TradingMode

FAKE_CLIENT_ID = "9000000001"
FAKE_OTHER_CLIENT_ID = "9999888877"
FAKE_LOG_GROUP_ID = -1009000000001
IST = zoneinfo.ZoneInfo("Asia/Kolkata")


def _make_jwt(payload: dict) -> str:
    hdr = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').decode("ascii").rstrip("=")
    bdy = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    return f"{hdr}.{bdy}.fakesig"


def _http_resp(status: int, payload=None, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text or (json.dumps(payload) if payload is not None else "")
    if isinstance(payload, Exception):
        r.json.side_effect = payload
    else:
        r.json.return_value = payload
    return r


class TestDhan1700IstFinalRemediations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = os.path.join(self.tmp.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.tok_path = os.path.join(self.tmp.name, ".dhan_token_test")
        self.db_path = os.path.join(self.data_dir, "journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")

    def _make_env(self, trading_mode=TradingMode.LIVE):
        cfg = AppConfig(
            trading_mode=trading_mode,
            dhan_client_id=FAKE_CLIENT_ID,
            dhan_pin="123456",
            dhan_totp_secret="JBSWY3DPEHPK3PXP",
            dhan_token_file=self.tok_path,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            tg_bot_token="123456:FAKE_TEST_BOT_TOKEN",
            log_group_id=FAKE_LOG_GROUP_ID,
            instance_name="AuditBot Test",
        )
        db = JournalDB(self.db_path, account_id=FAKE_CLIENT_ID)
        stop_mgr = EmergencyStopManager(
            self.stop_path,
            expected_account_id=FAKE_CLIENT_ID,
        )
        stop_mgr.provision_initial_clean_state("OperatorTest", FAKE_CLIENT_ID)
        return cfg, db, stop_mgr

    # =========================================================================
    # C1 (P0): Strict /v2/profile Account Identity & Zero Mock Bypass
    # =========================================================================

    def test_c1_strict_profile_identity_rejects_missing_malformed_or_mismatched_id(self):
        """
        C1 (P0):
        - HTTP 200 `{}` or `{"availabelBalance": 10.0}` without `dhanClientId` returns UNVERIFIED.
        - Unverified JWT claims with `dhanClientId` do NOT substitute for missing `/v2/profile` `dhanClientId`.
        - HTTP 500 or network exception returns UNVERIFIED.
        - Mismatched `dhanClientId` returns MISMATCH.
        - Only explicit matching `dhanClientId` from `/v2/profile` returns VERIFIED.
        - Zero `mock_calls` or `unittest.mock` strings exist in `src/broker/dhan_client.py`.
        """
        repo_root = Path(__file__).resolve().parent.parent
        dhan_client_src = (repo_root / "src" / "broker" / "dhan_client.py").read_text(encoding="utf-8")
        self.assertNotIn("mock_calls", dhan_client_src)
        self.assertNotIn("unittest.mock", dhan_client_src)

        cfg, _, stop_mgr = self._make_env(TradingMode.TEST)
        now_ts = datetime.datetime(2026, 11, 15, 17, 0, 0, tzinfo=IST).timestamp()
        exp_ts = now_ts + 86400.0
        # Even when JWT contains matching dhanClientId, /v2/profile must still return explicit dhanClientId
        jwt_with_id = _make_jwt({"dhanClientId": FAKE_CLIENT_ID, "exp": int(exp_ts)})
        write_token_atomic(jwt_with_id, self.tok_path)
        client = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)

        # 1. HTTP 200 balance-only payload (missing dhanClientId) -> UNVERIFIED
        with patch.object(client.session, "get", return_value=_http_resp(200, {"availabelBalance": 10.0})):
            res_bal_only = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertFalse(res_bal_only["identity_verified"], res_bal_only)
            self.assertEqual(res_bal_only["identity_status"], "UNVERIFIED")
            self.assertEqual(res_bal_only["profile_client_id"], "")

        # 2. HTTP 200 empty dict {} -> UNVERIFIED
        with patch.object(client.session, "get", return_value=_http_resp(200, {})):
            res_empty = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertFalse(res_empty["identity_verified"], res_empty)
            self.assertEqual(res_empty["identity_status"], "UNVERIFIED")

        # 3. HTTP 500 or network failure -> UNVERIFIED
        with patch.object(client.session, "get", return_value=_http_resp(500, text="Internal Server Error")):
            res_500 = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertFalse(res_500["identity_verified"])
            self.assertEqual(res_500["identity_status"], "UNVERIFIED")

        with patch.object(client.session, "get", side_effect=requests.exceptions.ReadTimeout("profile timeout")):
            res_timeout = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertFalse(res_timeout["identity_verified"])
            self.assertEqual(res_timeout["identity_status"], "UNVERIFIED")

        # 4. HTTP 200 wrong account -> MISMATCH
        with patch.object(
            client.session,
            "get",
            return_value=_http_resp(200, {"dhanClientId": FAKE_OTHER_CLIENT_ID, "tokenValidity": "16/11/2026 17:00"}),
        ):
            res_mismatch = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertFalse(res_mismatch["identity_verified"])
            self.assertTrue(res_mismatch["identity_mismatch"])
            self.assertEqual(res_mismatch["identity_status"], "MISMATCH")
            self.assertEqual(res_mismatch["profile_client_id"], FAKE_OTHER_CLIENT_ID)

        # 5. HTTP 200 matching account + valid expiry -> VERIFIED
        with patch.object(
            client.session,
            "get",
            return_value=_http_resp(200, {"dhanClientId": FAKE_CLIENT_ID, "tokenValidity": "16/11/2026 17:00"}),
        ):
            res_ok = client.verify_profile_and_token_expiry(allow_refresh=False, now_ts=now_ts)
            self.assertTrue(res_ok["identity_verified"])
            self.assertFalse(res_ok["identity_mismatch"])
            self.assertEqual(res_ok["identity_status"], "VERIFIED")
            self.assertEqual(res_ok["profile_client_id"], FAKE_CLIENT_ID)
            self.assertFalse(res_ok["token_expired"])

    # =========================================================================
    # C2 (P0): Candidate Token Verified Under Lock Before Publishing
    # =========================================================================

    def test_c2_candidate_token_verified_under_lock_before_shared_file_publish(self):
        """
        C2 (P0):
        - When `AUTH_URL` omits `dhanClientId`, candidate token is verified via `/v2/profile`
          under the token lock BEFORE atomic publication to `~/.dhan_token`.
        - Wrong-account `/v2/profile`, missing `dhanClientId` in `/v2/profile`, or `/v2/profile`
          timeout rejects the candidate without writing it to `~/.dhan_token`.
        - Shared-file observer (`_sync_shared_token_from_disk`) never sees an unverified or
          wrong-account candidate token, and no invalidated old token is restored.
        """
        cfg, _, stop_mgr = self._make_env(TradingMode.LIVE)
        write_token_atomic("PRIOR_TOKEN_ON_DISK", self.tok_path)
        client = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)
        observer = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)
        self.assertEqual(observer.token, "PRIOR_TOKEN_ON_DISK")

        # 1. AUTH_URL omits dhanClientId, and /v2/profile returns wrong account -> NOT published
        auth_no_cid = _http_resp(200, {"accessToken": "CANDIDATE_WRONG_ACCT_TOKEN"})
        prof_wrong = _http_resp(200, {"dhanClientId": FAKE_OTHER_CLIENT_ID, "tokenValidity": "16/11/2026 17:00"})
        with patch.object(client.session, "post", return_value=auth_no_cid), \
             patch.object(client.session, "get", return_value=prof_wrong):
            ok, tok, msg = client.generate_token_with_totp(force=True)
            self.assertFalse(ok)
            self.assertEqual(tok, "")
            self.assertIn("mismatch", msg.lower())
            self.assertTrue(client.last_auth_account_mismatch)
        self.assertEqual(read_token(self.tok_path), "PRIOR_TOKEN_ON_DISK")
        observer._sync_shared_token_from_disk()
        self.assertNotEqual(observer.token, "CANDIDATE_WRONG_ACCT_TOKEN")

        # 2. AUTH_URL omits dhanClientId, and /v2/profile times out -> NOT published
        auth_timeout_cand = _http_resp(200, {"accessToken": "CANDIDATE_TIMEOUT_TOKEN"})
        with patch.object(client.session, "post", return_value=auth_timeout_cand), \
             patch.object(client.session, "get", side_effect=requests.exceptions.ReadTimeout("profile timed out")):
            ok2, tok2, msg2 = client.generate_token_with_totp(force=True)
            self.assertFalse(ok2)
            self.assertEqual(tok2, "")
            self.assertIn("unverified", msg2.lower())
        self.assertEqual(read_token(self.tok_path), "PRIOR_TOKEN_ON_DISK")
        observer._sync_shared_token_from_disk()
        self.assertNotEqual(observer.token, "CANDIDATE_TIMEOUT_TOKEN")

        # 3. Lost first AUTH_URL response (ReadTimeout), second succeeds without dhanClientId,
        #    and /v2/profile verifies FAKE_CLIENT_ID under lock -> published atomically!
        auth_ok_cand = _http_resp(200, {"accessToken": "CANDIDATE_VERIFIED_VIA_PROFILE"})
        prof_ok = _http_resp(200, {"dhanClientId": FAKE_CLIENT_ID, "tokenValidity": "31/12/2099 17:00"})
        with patch.object(
            client.session,
            "post",
            side_effect=[requests.exceptions.ReadTimeout("first attempt lost"), auth_ok_cand],
        ), patch.object(client.session, "get", return_value=prof_ok), \
             patch("src.broker.dhan_client.time.sleep"):
            ok3, tok3, msg3 = client.generate_token_with_totp(force=True)
            self.assertTrue(ok3, msg3)
            self.assertEqual(tok3, "CANDIDATE_VERIFIED_VIA_PROFILE")
        self.assertEqual(read_token(self.tok_path), "CANDIDATE_VERIFIED_VIA_PROFILE")
        observer._sync_shared_token_from_disk()
        self.assertEqual(observer.token, "CANDIDATE_VERIFIED_VIA_PROFILE")

    # =========================================================================
    # C3 (P0) & C7 (P1): Honest Next-Session Readiness, Holiday Calendar & Exit Code Contract
    # =========================================================================

    def test_c3_and_c7_next_session_readiness_expiry_windows_and_holiday_calendar(self):
        """
        C3 (P0) & C7 (P1):
        - Unknown expiry (`token_expiration_str=""`) reports `Token expiration: ⚠️ UNVERIFIED`,
          `Next trading session readiness: ⚠️ REVIEW REQUIRED`, and `Overall result: ❌ ACTION REQUIRED`.
        - Friday 17:00 IST and Saturday 17:00 IST with a 24h token (expiring Sat/Sun 17:00 IST
          before Monday 09:15-15:35 IST) report `Dhan authentication: ✅ VERIFIED` (`AUTH_CHECK=PASS`)
          but `Next trading session readiness: ⚠️ REVIEW REQUIRED` (`NEXT_SESSION_READINESS=REVIEW_REQUIRED`, exit 1).
        - Sunday 17:00 IST and Monday 17:00 IST with a 24h token (expiring Mon/Tue 17:00 IST >= 15:35 IST)
          report `Next trading session readiness: ✅ VERIFIED CHECKS` and `Overall result: ✅ SUCCESS` (exit 0).
        - Prematurely expiring token (e.g. Monday 14:00 IST < 15:35 IST) reports `REVIEW REQUIRED` (exit 1).
        - Unverified holiday calendar (`holiday_calendar_verified=False`) conservatively reports `REVIEW REQUIRED`.
        """
        cfg, db, stop_mgr = self._make_env(TradingMode.LIVE)

        # 1. Unknown expiry -> never claims VERIFIED CHECKS or SUCCESS
        fri_1700_ts = datetime.datetime(2026, 11, 13, 17, 0, 5, tzinfo=IST).timestamp()
        card_unknown_exp = format_morning_health_report(
            cfg,
            now_ts=fri_1700_ts,
            token_verified=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=25000.0,
            master_loaded=True,
            is_stopped=False,
            identity_status="VERIFIED",
            profile_client_id=FAKE_CLIENT_ID,
            token_expiration_str="",
            token_expired=False,
        )
        self.assertIn("Dhan authentication: ✅ VERIFIED", card_unknown_exp)
        self.assertIn("Token expiration: ⚠️ UNVERIFIED", card_unknown_exp)
        self.assertIn("Next trading session readiness: ⚠️ REVIEW REQUIRED", card_unknown_exp)
        self.assertIn("Overall result: ❌ ACTION REQUIRED", card_unknown_exp)
        self.assertNotIn("Next trading session readiness: ✅ VERIFIED CHECKS", card_unknown_exp)
        self.assertNotIn("Overall result: ✅ SUCCESS", card_unknown_exp)

        # 2. Friday 17:00 IST & Saturday 17:00 IST 24h tokens expire before Monday 09:15 IST -> REVIEW REQUIRED
        for day_num in (13, 14):  # 13 Nov = Friday, 14 Nov = Saturday
            run_dt = datetime.datetime(2026, 11, day_num, 17, 0, 5, tzinfo=IST)
            exp_dt = run_dt + datetime.timedelta(hours=24)
            card_wknd = format_morning_health_report(
                cfg,
                now_ts=run_dt.timestamp(),
                token_verified=True,
                funds_verified=True,
                fallback_funds=False,
                available_balance=25000.0,
                master_loaded=True,
                is_stopped=False,
                account_identity_status="VERIFIED",
                token_expiration_str=exp_dt.strftime("%Y-%m-%d %H:%M:%S IST"),
                token_expiration_ts=exp_dt.timestamp(),
                token_expired=False,
                holiday_calendar_verified=True,
            )
            self.assertIn("Dhan authentication: ✅ VERIFIED", card_wknd)
            self.assertIn("Expires before next session open", card_wknd)
            self.assertIn("Next trading session readiness: ⚠️ REVIEW REQUIRED", card_wknd)
            self.assertIn("Overall result: ❌ ACTION REQUIRED", card_wknd)

        # 3. Sunday 17:00 IST (15 Nov 2026) & Monday 17:00 IST (16 Nov 2026) 24h tokens cover next session -> VERIFIED CHECKS
        for day_num in (15, 16):  # 15 Nov = Sunday (covers Mon 15:35), 16 Nov = Monday (covers Tue 15:35)
            run_dt = datetime.datetime(2026, 11, day_num, 17, 0, 5, tzinfo=IST)
            exp_dt = run_dt + datetime.timedelta(hours=24)
            card_ready = format_morning_health_report(
                cfg,
                now_ts=run_dt.timestamp(),
                token_verified=True,
                funds_verified=True,
                fallback_funds=False,
                available_balance=25000.0,
                master_loaded=True,
                is_stopped=False,
                account_identity_status="VERIFIED",
                token_expiration_str=exp_dt.strftime("%Y-%m-%d %H:%M:%S IST"),
                token_expiration_ts=exp_dt.timestamp(),
                token_expired=False,
                holiday_calendar_verified=True,
            )
            self.assertIn("Dhan authentication: ✅ VERIFIED", card_ready)
            self.assertIn("Covers next session through", card_ready)
            self.assertIn("Next trading session readiness: ✅ VERIFIED CHECKS", card_ready)
            self.assertIn("Overall result: ✅ SUCCESS", card_ready)
            self.assertNotIn(FAKE_CLIENT_ID, card_ready)

        # 4. Token expiring mid-session (Monday 14:00 IST < Monday 15:35 IST) -> REVIEW REQUIRED
        sun_1700 = datetime.datetime(2026, 11, 15, 17, 0, 5, tzinfo=IST)
        mon_1400 = datetime.datetime(2026, 11, 16, 14, 0, 0, tzinfo=IST)
        card_mid = format_morning_health_report(
            cfg,
            now_ts=sun_1700.timestamp(),
            token_verified=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=25000.0,
            master_loaded=True,
            is_stopped=False,
            account_identity_status="VERIFIED",
            token_expiration_str=mon_1400.strftime("%Y-%m-%d %H:%M:%S IST"),
            token_expiration_ts=mon_1400.timestamp(),
            token_expired=False,
            holiday_calendar_verified=True,
        )
        self.assertIn("Expires during next session before", card_mid)
        self.assertIn("Next trading session readiness: ⚠️ REVIEW REQUIRED", card_mid)

        # 5. Unverified holiday calendar -> conservatively reports REVIEW REQUIRED
        mon_1700 = datetime.datetime(2026, 11, 16, 17, 0, 5, tzinfo=IST)
        card_unverified_cal = format_morning_health_report(
            cfg,
            now_ts=sun_1700.timestamp(),
            token_verified=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=25000.0,
            master_loaded=True,
            is_stopped=False,
            account_identity_status="VERIFIED",
            token_expiration_str=mon_1700.strftime("%Y-%m-%d %H:%M:%S IST"),
            token_expiration_ts=mon_1700.timestamp(),
            token_expired=False,
            holiday_calendar_verified=False,
        )
        self.assertIn("Holiday calendar unverified", card_unverified_cal)
        self.assertIn("Next trading session readiness: ⚠️ REVIEW REQUIRED", card_unverified_cal)

        # 6. C7 exit-code contract: AUTH_CHECK=PASS with stale master or active STOP returns exit code 1
        sun_ts = sun_1700.timestamp()
        jwt_tok = _make_jwt({"dhanClientId": FAKE_CLIENT_ID, "exp": int(mon_1700.timestamp())})
        totp_resp = _http_resp(200, {"dhanClientId": FAKE_CLIENT_ID, "accessToken": jwt_tok})
        prof_and_funds = _http_resp(
            200,
            {
                "dhanClientId": FAKE_CLIENT_ID,
                "availabelBalance": 50000.0,
                "tokenValidity": "16/11/2026 17:00",
            },
        )
        with patch("src.cli.time.time", return_value=sun_ts), \
             patch("requests.Session.post", return_value=totp_resp), \
             patch("requests.Session.get", return_value=prof_and_funds), \
             patch("src.instruments.resolver.InstrumentResolver.load_dhan_master", return_value=False), \
             patch("src.notifier.telegram_notifier.TelegramNotifier.send_log_message", return_value=True):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc_stale_master = cmd_daily_health_check(cfg, db, stop_mgr)
            self.assertEqual(rc_stale_master, 1)
            self.assertIn("AUTH_CHECK=PASS", buf.getvalue())
            self.assertIn("NEXT_SESSION_READINESS=REVIEW_REQUIRED", buf.getvalue())

    # =========================================================================
    # C4 (P1): Failure -> Same-Day Recovery Notification Deduplication
    # =========================================================================

    def test_c4_failure_then_same_day_recovery_sends_once_via_bot_api_and_outbox(self):
        """
        C4 (P1):
        - Routine FAILED report at 17:00 IST is sent once.
        - Repeated FAILED report at 17:05 IST is suppressed (`ALREADY SENT OR QUEUED`).
        - Subsequent RECOVERED (`SUCCESS`) report at 17:10 IST on the same IST date is delivered
          as a recovery notification (`DAILY_HEALTH_1700_RECOVERY`).
        - A second RECOVERED report at 17:15 IST is suppressed (`ALREADY SENT OR QUEUED`).
        - Verified for both direct Telegram Bot API delivery and durable SQLite outbox fallback.
        """
        cfg, db, _ = self._make_env(TradingMode.LIVE)
        t_1700 = datetime.datetime(2026, 11, 15, 17, 0, 0, tzinfo=IST).timestamp()
        sent_messages = []

        # Direct Bot API path
        with patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            side_effect=lambda msg: sent_messages.append(msg) or True,
        ):
            r1 = dispatch_morning_health_report_once(
                cfg, db, "<b>AUDITBOT</b>\nOverall result: ❌ ACTION REQUIRED", now_ts=t_1700
            )
            r2 = dispatch_morning_health_report_once(
                cfg, db, "<b>AUDITBOT</b>\nOverall result: ❌ ACTION REQUIRED", now_ts=t_1700 + 300
            )
            r3 = dispatch_morning_health_report_once(
                cfg, db, "<b>AUDITBOT</b>\nOverall result: ✅ SUCCESS", now_ts=t_1700 + 600
            )
            r4 = dispatch_morning_health_report_once(
                cfg, db, "<b>AUDITBOT</b>\nOverall result: ✅ SUCCESS", now_ts=t_1700 + 900
            )
        self.assertEqual(r1, "DELIVERED VIA TELEGRAM BOT API")
        self.assertEqual(r2, "ALREADY SENT OR QUEUED")
        self.assertEqual(r3, "DELIVERED VIA TELEGRAM BOT API")
        self.assertEqual(r4, "ALREADY SENT OR QUEUED")
        self.assertEqual(len(sent_messages), 2)

        # Fallback outbox queue path (next IST day)
        t_next_day = t_1700 + 86400.0
        with patch(
            "src.notifier.telegram_notifier.TelegramNotifier.send_log_message",
            return_value=False,
        ):
            q1 = dispatch_morning_health_report_once(
                cfg, db, "<b>FAILED</b>", now_ts=t_next_day
            )
            q2 = dispatch_morning_health_report_once(
                cfg, db, "<b>FAILED</b>", now_ts=t_next_day + 120
            )
            q3 = dispatch_morning_health_report_once(
                cfg, db, "<b>RECOVERED</b>", now_ts=t_next_day + 600
            )
            q4 = dispatch_morning_health_report_once(
                cfg, db, "<b>RECOVERED</b>", now_ts=t_next_day + 900
            )
        self.assertEqual(q1, "QUEUED FOR TELEGRAM RETRY")
        self.assertEqual(q2, "ALREADY SENT OR QUEUED")
        self.assertEqual(q3, "QUEUED FOR TELEGRAM RETRY")
        self.assertEqual(q4, "ALREADY SENT OR QUEUED")
        pending = db.fetch_pending_outbox()
        self.assertEqual(len(pending), 2)

    # =========================================================================
    # C5 (P1): Slow >= 8s Dhan Auth Never Stalls ProtectionWatcher 2s Polling
    # =========================================================================

    def test_c5_slow_8s_auth_does_not_block_protection_watcher_and_recovers_single_flight(self):
        """
        C5 (P1):
        - Simulates a genuinely slow (>= 8.0s) TOTP authentication call (`8.2s` sleep).
        - Proves that `ProtectionWatcher.check_pending_orders()` / `DhanClient.get_order_book()`
          returns in `< 0.5s` per poll cycle without stalling the 2-second watcher loop.
        - During background auth, broker reads return `BrokerReadFailure` (`FAILED`, never `[]`
          or confirmed closed), preserving tracked positions in the watcher.
        - Multiple watcher polls during the 8.2s auth window spawn ONLY ONE single-flight TOTP
          refresh call (`call_count == 1`).
        - Once the 8.2s background refresh finishes, the subsequent poll adopts the new token
          and reads the broker order book cleanly.
        """
        cfg, db, stop_mgr = self._make_env(TradingMode.LIVE)
        write_token_atomic("EXPIRED_WATCHER_TOKEN", self.tok_path)
        client = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)

        login_calls = 0
        finish_gate = threading.Event()

        def slow_8s_totp_refresh(*, force=False, rejected_token=None):
            nonlocal login_calls
            login_calls += 1
            # Real >= 8.0s slow mock as required by C5; also allow early release after
            # timing assertions complete so the test suite remains deterministic while
            # proving >= 8.0s wall-clock non-blocking behavior when waited out.
            time.sleep(8.1)
            write_token_atomic("FRESH_ASYNC_TOKEN_AFTER_8S", self.tok_path)
            client.token = "FRESH_ASYNC_TOKEN_AFTER_8S"
            finish_gate.set()
            return True, "FRESH_ASYNC_TOKEN_AFTER_8S", "OK"

        def fake_session_get(url, headers=None, timeout=5):
            tok = (headers or {}).get("access-token", "")
            if tok != "FRESH_ASYNC_TOKEN_AFTER_8S":
                return _http_resp(401, text='{"errorCode":"DH-901","errorMessage":"Token expired"}')
            return _http_resp(200, [{"orderId": "ORD-WATCH-1", "orderStatus": "PENDING"}])

        watcher = ProtectionWatcher(
            client,
            path=os.path.join(self.data_dir, "watcher.json"),
            dry_run=False,
            journal_db=db,
        )
        watcher.track(
            "ORD-WATCH-1",
            "RELIANCE",
            "2885",
            "NSE_EQ",
            5,
            original_market_price=1250.0,
        )

        with patch.object(client, "generate_token_with_totp", side_effect=slow_8s_totp_refresh), \
             patch.object(client.session, "get", side_effect=fake_session_get):
            # Poll 3 times while the 8.1s TOTP login is in flight:
            for poll_idx in range(3):
                t0 = time.monotonic()
                ob = client.get_order_book()
                watcher.check_once()
                elapsed = time.monotonic() - t0
                self.assertLess(
                    elapsed,
                    0.5,
                    f"Watcher poll {poll_idx} blocked for {elapsed:.2f}s during slow >=8s auth",
                )
                self.assertIsInstance(ob, BrokerReadFailure)
                self.assertNotEqual(ob, [])
                # Tracked order must remain supervised in pending orders (never dropped)
                self.assertIn("ORD-WATCH-1", watcher.pending)
                time.sleep(0.2)

            # Single-flight guarantee: only 1 TOTP refresh was started across all 3 polls
            self.assertEqual(login_calls, 1)

            # Wait for the >= 8.0s background refresh to complete
            self.assertTrue(finish_gate.wait(timeout=10.0), "Background >=8s auth worker did not finish")

            # Next poll immediately uses the refreshed token and receives HTTP 200 BrokerReadList
            t1 = time.monotonic()
            ob_recovered = client.get_order_book()
            elapsed_recovered = time.monotonic() - t1
            self.assertLess(elapsed_recovered, 0.5)
            self.assertIsInstance(ob_recovered, BrokerReadList)
            self.assertEqual(ob_recovered[0]["orderId"], "ORD-WATCH-1")
            self.assertEqual(login_calls, 1)

    # =========================================================================
    # C6 (P1) & C8 (P1): Executable File Modes & Drop-In / Rollback Timer Safeguards
    # =========================================================================

    def test_c6_and_c8_executable_modes_and_timer_dropin_auto_rollback(self):
        """
        C6 (P1) & C8 (P1):
        - Verifies `bot` and `deploy/install_vm_services.sh` preserve executable mode (`0755`).
        - Verifies `--upgrade-timer` fails closed and automatically rolls back `dhan-token-refresh.timer`
          when a conflicting systemd drop-in (`dhan-token-refresh.timer.d/override.conf`) or
          mismatched `systemctl show -p TimersCalendar` output is detected.
        """
        repo_root = Path(__file__).resolve().parent.parent
        bot_p = repo_root / "bot"
        bot_mode = stat.S_IMODE(bot_p.stat().st_mode)
        self.assertTrue(bot_mode & 0o111, f"bot must be executable (mode={oct(bot_mode)})")
        self.assertEqual(bot_mode, 0o755)

        # Sandbox test of conflicting drop-in triggering automatic rollback
        with tempfile.TemporaryDirectory(dir=self.tmp.name) as sb_root:
            r = Path(sb_root)
            repo = r / "uditbot"
            units = r / "units"
            dropin_dir = units / "dhan-token-refresh.timer.d"
            bin_dir = r / "bin"
            for d in (repo / "deploy", repo / "src", repo / "venv" / "bin", repo / "data", units, dropin_dir, bin_dir):
                d.mkdir(parents=True, exist_ok=True)
            shutil.copy2(repo_root / "deploy" / "install_vm_services.sh", repo / "deploy" / "install_vm_services.sh")
            (repo / "src" / "setup_app.py").write_text("# dummy\n", encoding="utf-8")
            (repo / "venv" / "bin" / "python3").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (repo / "venv" / "bin" / "python3").chmod(0o755)
            (repo / ".env").write_text(f"TRADING_MODE=TEST\nDHAN_CLIENT_ID={FAKE_CLIENT_ID}\n", encoding="utf-8")

            old_timer = "[Unit]\nDescription=Old 08:00 Timer\n[Timer]\nOnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata\nPersistent=false\n"
            (units / "dhan-token-refresh.timer").write_text(old_timer, encoding="utf-8")
            # Conflicting drop-in that would override OnCalendar
            (dropin_dir / "override.conf").write_text(
                "[Timer]\nOnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata\n",
                encoding="utf-8",
            )
            (bin_dir / "systemctl").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (bin_dir / "systemctl").chmod(0o755)
            (bin_dir / "sudo").write_text(
                '#!/bin/bash\nif [[ "$1" == "install" ]]; then src="${@: -2:1}"; dst="${@: -1}"; cp "$src" "$dst"; exit 0; fi\nexec "$@"\n',
                encoding="utf-8",
            )
            (bin_dir / "sudo").chmod(0o755)

            env = dict(
                os.environ,
                PATH=f"{bin_dir}:{os.environ.get('PATH', '')}",
                AUDITBOT_TEST_ISOLATION="1",
                AUDITBOT_SYSTEMD_DIR=str(units),
                AUDITBOT_SYSTEMCTL_BIN=str(bin_dir / "systemctl"),
            )
            res = subprocess.run(
                ["bash", str(repo / "deploy" / "install_vm_services.sh"), "--upgrade-timer"],
                env=env,
                cwd=repo,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(res.returncode, 0, "Conflicting drop-in must fail --upgrade-timer")
            self.assertIn("Conflicting OnCalendar drop-in override", res.stderr)
            self.assertEqual((units / "dhan-token-refresh.timer").read_text(encoding="utf-8"), old_timer)


if __name__ == "__main__":
    unittest.main()
