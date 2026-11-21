"""Offline regression for scheduled Dhan TOTP retry and Telegram recovery notice.

Synthetic account only; no Dhan API, Telegram API, systemd or production secrets.
"""
import sqlite3
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from src.broker.totp_retry import retry_scheduled_invalid_totp_once
from src.notifier.health_recovery import queue_verified_dhan_recovery


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class TotpRetryTests(unittest.TestCase):
    def retry(self, response, *, on_demand=False, session=None, clock=None, sleep=None):
        return retry_scheduled_invalid_totp_once(
            response, on_demand=on_demand, session=session or Mock(),
            auth_url="https://example.invalid/dhan", params={"totp": "000000"},
            timeout=(5, 25), generate_code=lambda: "111111",
            clock=clock or (lambda: 3605.0), sleeper=sleep or Mock(),
        )

    def test_one_fresh_window_retry(self):
        response = FakeResponse({"status": "error", "message": "Invalid TOTP"})
        new_response = FakeResponse({"accessToken": "synthetic"})
        session, sleep = Mock(), Mock()
        session.post.return_value = new_response
        self.assertIs(self.retry(response, session=session, sleep=sleep), new_response)
        sleep.assert_called_once_with(25.75)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(session.post.call_args.kwargs["params"]["totp"], "111111")

    def test_second_rejection_does_not_retry_again(self):
        first = FakeResponse({"status": "error", "message": "Invalid TOTP"})
        second = FakeResponse({"status": "error", "message": "Invalid TOTP"})
        session = Mock()
        session.post.return_value = second
        self.assertIs(self.retry(first, session=session), second)
        session.post.assert_called_once()

    def test_runtime_and_other_errors_never_retry(self):
        for on_demand, body in (
            (True, {"status": "error", "message": "Invalid TOTP"}),
            (False, {"status": "error", "message": "Invalid PIN"}),
            (False, {"status": "error", "message": "Account mismatch"}),
            (False, {"status": "success", "message": "Invalid TOTP"}),
            (False, {"status": "error", "message": "Invalid TOTP", "accessToken": "synthetic"}),
        ):
            with self.subTest(on_demand=on_demand, body=body):
                original, session, sleep = FakeResponse(body), Mock(), Mock()
                self.assertIs(self.retry(original, on_demand=on_demand, session=session, sleep=sleep), original)
                session.post.assert_not_called()
                sleep.assert_not_called()


class MockJournal:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            "CREATE TABLE system_events(event_type TEXT, description TEXT, created_at REAL);"
            "CREATE TABLE notice(identity TEXT, event_key TEXT, text TEXT,"
            "UNIQUE(identity, event_key));"
        )

    def close(self):
        self.conn.close()

    def _get_conn(self):
        class ConnCtx:
            def __enter__(inner):
                return self.conn

            def __exit__(inner, exc_type, exc, tb):
                if exc_type is None:
                    self.conn.commit()
        return ConnCtx()

    def enqueue_order_notification_once(self, identity, event_key, text):
        try:
            self.conn.execute("INSERT INTO notice VALUES(?,?,?)", (identity, event_key, text))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


class HealthRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.db = MockJournal()
        self.addCleanup(self.db.close)
        self.now = datetime(2026, 11, 21, 17, 10, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
        cfg = SimpleNamespace(
            dhan_client_id="9000001536", instance_name="Auditbot", log_group_id="-123"
        )
        self.gate = SimpleNamespace(
            db=self.db, broker=SimpleNamespace(cfg=cfg),
            state=SimpleNamespace(value="HEALTHY"), is_accepting_entries=True,
            stop_manager=SimpleNamespace(is_stopped=lambda: False),
        )

    def record_health(self, message, at=None):
        self.db.conn.execute(
            "INSERT INTO system_events VALUES('DAILY_HEALTH_CHECK', ?, ?)",
            (message, at if at is not None else self.now - 600),
        )
        self.db.conn.commit()

    def test_failed_auth_then_reconciliation_queues_once(self):
        self.record_health("AUTH_CHECK=FAIL, NEXT_SESSION_READINESS=REVIEW_REQUIRED")
        self.assertTrue(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        event_key, msg = self.db.conn.execute("SELECT event_key,text FROM notice").fetchone()
        self.assertEqual(event_key, "DAILY_HEALTH_1700_RECOVERY")
        self.assertIn("Reconciliation: ✅ HEALTHY", msg)
        self.assertIn("Next-session readiness: Not independently verified", msg)
        self.assertNotIn("9000001536", msg)

    def test_no_notification_without_same_day_auth_failure(self):
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        self.record_health("AUTH_CHECK=PASS")
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        self.record_health("AUTH_CHECK=FAIL", self.now - 86400)
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))

    def test_recovery_notice_contains_verified_exchange_holiday(self):
        # Official 2026 calendar: Tuesday 24 November is a trading holiday.
        self.now = datetime(2026, 11, 23, 17, 10,
                            tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
        self.record_health("AUTH_CHECK=FAIL")
        with patch("src.engine.market.load_holidays_verified",
                   return_value=(True, {datetime(2026, 11, 24).date(),
                                       datetime(2026, 1, 26).date()}, "OK")):
            self.assertTrue(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        msg = self.db.conn.execute("SELECT text FROM notice").fetchone()[0]
        self.assertIn("Market tomorrow (Tue 24 Nov 2026): 🚫 CLOSED (Exchange holiday)", msg)
        self.assertIn("Market today (Mon 23 Nov 2026): ✅ SCHEDULED TRADING DAY", msg)
        self.assertIn("Next scheduled session: Wed 25 Nov 2026", msg)
        self.assertIn("Next-session readiness: Not independently verified", msg)

    def test_recovery_notice_fails_closed_when_calendar_unverified(self):
        self.record_health("AUTH_CHECK=FAIL")
        with patch("src.engine.market.load_holidays_verified",
                   return_value=(False, set(), "Missing calendar")):
            self.assertTrue(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        msg = self.db.conn.execute("SELECT text FROM notice").fetchone()[0]
        self.assertIn("Market tomorrow (Sun 22 Nov 2026): 🚫 CLOSED (Weekend)", msg)
        self.assertIn("Next scheduled session: ⚠️ UNVERIFIED", msg)
        self.assertNotIn("Next scheduled session: ✅", msg)

    def test_no_notification_if_stop_or_gate_blocked(self):
        self.record_health("AUTH_CHECK=FAIL")
        self.gate.state.value = "BLOCKED"
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        self.gate.state.value = "HEALTHY"
        self.gate.is_accepting_entries = False
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))
        self.gate.is_accepting_entries = True
        self.gate.stop_manager = SimpleNamespace(is_stopped=lambda: True)
        self.assertFalse(queue_verified_dhan_recovery(self.gate, now_ts=self.now))


if __name__ == "__main__":
    unittest.main()
