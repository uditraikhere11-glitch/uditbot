"""PR76 final owner corrections. Offline-only; never touches real broker credentials.

Includes calendar fail-closed, calendar year boundary, same-day transitions,
account-scoped singleflight, and candidate-token identity verification.
"""
import datetime as dt
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
from zoneinfo import ZoneInfo

from src.cli import format_morning_health_report, dispatch_morning_health_report_once
from src.broker.dhan_client import DhanClient, BASE_URL
from src.broker.dhan_token import read_token, write_token_atomic
from src.models import TradingMode

IST = ZoneInfo("Asia/Kolkata")
FAKE_ACCOUNT = "9000000001"


class FakeJournal:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE order_notification_events (order_id TEXT, event_key TEXT, created_at REAL, UNIQUE(order_id,event_key))")
        self.queued = []
    def _get_conn(self):
        return self.db
    def enqueue_order_notification_once(self, identity, event_key, report):
        if (identity, event_key) not in [(x[0], x[1]) for x in self.queued]:
            self.queued.append((identity, event_key, report))
            return True
        return False


class TestAfterMarketSafety(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = SimpleNamespace(
            dhan_client_id=FAKE_ACCOUNT, instance_name="AuditBot Offline",
            data_dir=self.tmp.name, log_group_id=-1009000000001,
            trading_mode=TradingMode.LIVE,
        )
        self.sunday = dt.datetime(2026, 11, 15, 17, 0, tzinfo=IST).timestamp()
    def report(self, now, expiry):
        return format_morning_health_report(
            self.cfg, now_ts=now, token_verified=True,
            funds_verified=True, fallback_funds=False,
            available_balance=100.0, master_loaded=True,
            is_stopped=False, account_identity_status="VERIFIED",
            token_expiration_ts=expiry,
            token_expiration_str=dt.datetime.fromtimestamp(expiry, IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        )
    def test_calendar_loader_exception_never_claims_ready(self):
        with patch("src.engine.market.load_holidays_verified", side_effect=RuntimeError("offline calendar")):
            card = self.report(self.sunday, self.sunday+86400)
        self.assertIn("Overall result: ❌ ACTION REQUIRED", card)
        self.assertIn("Next trading session readiness: ⚠️ REVIEW REQUIRED", card)
    def test_missing_next_year_data_never_claims_ready(self):
        now = dt.datetime(2026, 12, 31, 17, 0, tzinfo=IST).timestamp()
        def loader(*, required_year=None):
            if required_year == 2026:
                return True, {dt.date(2026,12,25)}, ""
            return False, set(), "2027 holiday calendar missing"
        with patch("src.engine.market.load_holidays_verified", side_effect=loader):
            card = self.report(now, now+86400*5)
        self.assertIn("Overall result: ❌ ACTION REQUIRED", card)
        self.assertIn("Holiday calendar UNVERIFIED", card)
    def test_verified_new_year_holiday_skipped(self):
        now = dt.datetime(2026, 12, 31, 17, 0, tzinfo=IST).timestamp()
        def loader(*, required_year=None):
            if required_year == 2026:
                return True, {dt.date(2026,12,25)}, ""
            return True, {dt.date(2027,1,1)}, ""
        with patch("src.engine.market.load_holidays_verified", side_effect=loader):
            card = self.report(now, now+86400*5)
        self.assertIn("Mon 04 Jan 15:35 IST", card)
        self.assertIn("Overall result: ✅ SUCCESS", card)
    def test_notification_ready_failed_recovered_and_dedup(self):
        db = FakeJournal()
        notifier=SimpleNamespace(enabled=True,send_log_message=lambda report: True)
        with patch("src.notifier.telegram_notifier.TelegramNotifier", return_value=notifier):
            r1=dispatch_morning_health_report_once(self.cfg,db,"Overall result: ✅ SUCCESS",now_ts=self.sunday)
            r2=dispatch_morning_health_report_once(self.cfg,db,"Overall result: ❌ ACTION REQUIRED",now_ts=self.sunday+600)
            r3=dispatch_morning_health_report_once(self.cfg,db,"Overall result: ✅ SUCCESS",now_ts=self.sunday+1200)
            r4=dispatch_morning_health_report_once(self.cfg,db,"Overall result: ❌ ACTION REQUIRED",now_ts=self.sunday+1800)
        self.assertEqual((r1,r2,r3),("DELIVERED VIA TELEGRAM BOT API",)*3)
        self.assertEqual(r4,"ALREADY SENT OR QUEUED")
        keys={k for (k,) in db.db.execute("SELECT event_key FROM order_notification_events")}
        self.assertIn("DAILY_HEALTH_1700_DETERIORATION",keys)
        self.assertIn("DAILY_HEALTH_1700_RECOVERY",keys)
    def test_notification_failure_recovered_failed_and_queued(self):
        db=FakeJournal()
        notifier=SimpleNamespace(enabled=False,send_log_message=lambda _:False)
        with patch("src.notifier.telegram_notifier.TelegramNotifier", return_value=notifier):
            statuses=[dispatch_morning_health_report_once(self.cfg,db,s,now_ts=self.sunday+i*600)
            for i,s in enumerate(("Overall result: ❌ ACTION REQUIRED","Overall result: ✅ SUCCESS","Overall result: ❌ ACTION REQUIRED"))]
        self.assertEqual(statuses,["QUEUED FOR TELEGRAM RETRY"]*3)
        self.assertEqual(len(db.queued),3)


class TestTokenCandidateAndSingleflight(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=os.path.join(self.tmp.name,"shared-token")
    def client(self, account=FAKE_ACCOUNT):
        cl=object.__new__(DhanClient)
        cl.cfg=SimpleNamespace(dhan_client_id=account,dhan_pin="FAKE_PIN",dhan_totp_secret="JBSWY3DPEHPK3PXP",dhan_token_file=self.path,trading_mode=TradingMode.LIVE)
        cl.token="old-token"
        cl.session=Mock()
        cl._token_mtime=0
        cl._token_file_signature=None
        cl._auth_log_at={}
        return cl
    def test_matching_auth_claim_alone_cannot_publish_token(self):
        cl=self.client()
        cl.session.post.return_value=MagicMock(status_code=200)
        cl.session.post.return_value.json.return_value={"accessToken":"unverified-candidate","dhanClientId":FAKE_ACCOUNT}
        cl.verify_profile_and_token_expiry=Mock(return_value={"identity_status":"UNVERIFIED","identity_verified":False})
        with patch("src.broker.dhan_client.compute_totp",return_value="000000"):
            ok,token,_=cl.generate_token_with_totp(force=True)
        self.assertFalse(ok)
        self.assertEqual(token,"")
        self.assertEqual(read_token(self.path),"")
        cl.verify_profile_and_token_expiry.assert_called_once()
        self.assertEqual(cl.verify_profile_and_token_expiry.call_args.kwargs["candidate_token"],"unverified-candidate")
    def test_valid_candidate_checked_then_published(self):
        cl=self.client()
        cl.session.post.return_value=MagicMock(status_code=200)
        cl.session.post.return_value.json.return_value={"accessToken":"verified-new-token","dhanClientId":FAKE_ACCOUNT}
        cl.verify_profile_and_token_expiry=Mock(return_value={"identity_status":"VERIFIED","identity_verified":True,"token_expired":False})
        with patch("src.broker.dhan_client.compute_totp",return_value="000000"):
            ok,token,_=cl.generate_token_with_totp(force=True)
        self.assertTrue(ok)
        self.assertEqual(token,"verified-new-token")
        self.assertEqual(read_token(self.path),token)
    def test_two_accounts_same_path_never_share_async_token(self):
        A=self.client("ACCOUNT-A")
        B=self.client("ACCOUNT-B")
        A.token="old-A";B.token="old-B"
        for cl in (A,B):
            cl._headers=lambda cl=cl:{"access-token":cl.token}
            cl._sync_shared_token_from_disk=lambda:None
            cl._requires_token_refresh=lambda status,body:True
            cl.session.get.return_value=MagicMock(status_code=401,text="DH-901")
        active=threading.Event();release=threading.Event()
        def renew_a(**kwargs):
            active.set()
            release.wait(3)
            A.token="token-for-A"
            return True,"token-for-A","OK"
        A.generate_token_with_totp=renew_a
        B.generate_token_with_totp=Mock(side_effect=AssertionError("wrong account recovery should not start"))
        try:
            self.assertEqual(A._get_with_auth_recovery(BASE_URL+"/orders").status_code,401)
            self.assertTrue(active.wait(2))
            self.assertEqual(B._get_with_auth_recovery(BASE_URL+"/orders").status_code,401)
        finally:
            release.set()
            time.sleep(0.08)
        self.assertEqual(B.token,"old-B")
        B.generate_token_with_totp.assert_not_called()
    def test_broker_get_no_longer_waits_for_slow_login(self):
        cl=self.client()
        cl._headers=lambda:{"access-token":cl.token}
        cl._sync_shared_token_from_disk=lambda:None
        cl._requires_token_refresh=lambda status,body:True
        cl.session.get.return_value=MagicMock(status_code=401,text="DH-901")
        release=threading.Event()
        cl.generate_token_with_totp=lambda **kwargs:(release.wait(3) or False,"","offline")
        begin=time.monotonic()
        try:
            result=cl._get_with_auth_recovery(BASE_URL+"/orders")
        finally:
            release.set()
        self.assertEqual(result.status_code,401)
        self.assertLess(time.monotonic()-begin,0.5)


if __name__ == "__main__":
    unittest.main()
