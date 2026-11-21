"""Offline tests: AmanBot 08:00 report and Udit-compatible 17:00 report coexist.

No real broker/Telegram access, services, environment secrets, or VM files.
"""
from contextlib import nullcontext
from datetime import datetime, date
from pathlib import Path
import os
import sqlite3
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from src.cli import (
    _is_morning_health_slot, dispatch_morning_health_report_once,
    format_morning_health_report,
)
from src.notifier.health_recovery import queue_verified_dhan_recovery

IST = ZoneInfo("Asia/Kolkata")


class MemoryJournal:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            "CREATE TABLE order_notification_events("
            "order_id TEXT, event_key TEXT, created_at REAL, "
            "UNIQUE(order_id,event_key));"
            "CREATE TABLE system_events("
            "event_type TEXT, description TEXT, created_at REAL);"
        )

    def _get_conn(self):
        return self.conn

    def enqueue_order_notification_once(self, identity, event_key, text):
        raise AssertionError("Bot API is mocked as successful; outbox not required")


class AmanBotDualScheduleTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.db = MemoryJournal()
        self.addCleanup(self.db.conn.close)
        self.cfg = SimpleNamespace(
            dhan_client_id="9000009999",
            instance_name="AmanBot",
            log_group_id=-1009000001234,
            tg_bot_token="90000:synthetic-only",
            data_dir=self.work.name,
            trading_mode="LIVE",
        )
        self.when = datetime(2026, 11, 23, 8, 0, tzinfo=IST).timestamp()

    def test_slot_is_opt_in_and_default_remains_17(self):
        with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": "AM_0800"}):
            self.assertTrue(_is_morning_health_slot())
        with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": ""}):
            self.assertFalse(_is_morning_health_slot())
        with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": "invalid"}):
            self.assertFalse(_is_morning_health_slot())

    def test_same_day_08_and_17_deliver_independently_no_duplicates(self):
        notifier = Mock()
        notifier.enabled = True
        notifier.send_log_message.return_value = True
        with patch("src.notifier.telegram_notifier.TelegramNotifier", return_value=notifier), \
             patch("src.broker.dhan_token.locked_token_file",
                   side_effect=lambda _path: nullcontext()):
            with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": "AM_0800"}):
                self.assertEqual(
                    "DELIVERED VIA TELEGRAM BOT API",
                    dispatch_morning_health_report_once(
                        self.cfg, self.db, "Overall result: ✅ SUCCESS", now_ts=self.when,
                    ),
                )
                self.assertEqual(
                    "ALREADY SENT OR QUEUED",
                    dispatch_morning_health_report_once(
                        self.cfg, self.db, "Overall result: ✅ SUCCESS", now_ts=self.when,
                    ),
                )
            with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": ""}):
                self.assertEqual(
                    "DELIVERED VIA TELEGRAM BOT API",
                    dispatch_morning_health_report_once(
                        self.cfg, self.db, "Overall result: ✅ SUCCESS",
                        now_ts=self.when + 9 * 3600,
                    ),
                )
                self.assertEqual(
                    "ALREADY SENT OR QUEUED",
                    dispatch_morning_health_report_once(
                        self.cfg, self.db, "Overall result: ✅ SUCCESS",
                        now_ts=self.when + 9 * 3600,
                    ),
                )
        self.assertEqual(notifier.send_log_message.call_count, 2)
        identities = self.db.conn.execute(
            "SELECT order_id FROM order_notification_events "
            "WHERE event_key='DAILY_HEALTH_1700_REPORT' ORDER BY order_id"
        ).fetchall()
        self.assertEqual(len(identities), 2)
        self.assertTrue(any(row[0].endswith(":AM_0800") for row in identities))
        self.assertTrue(any(not row[0].endswith(":AM_0800") for row in identities))

    def test_morning_and_evening_heading_and_no_calendar_rows(self):
        next_day = datetime(2026, 11, 24, 8, 0, tzinfo=IST)
        kwargs = dict(
            now_ts=self.when,
            token_verified=True,
            token_renewed=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=1000.0,
            master_loaded=True,
            is_stopped=False,
            account_identity_status="VERIFIED",
            token_expiration_str="2026-11-24 08:00:00 IST",
            token_expiration_ts=next_day.timestamp(),
        )
        with patch("src.engine.market.load_holidays_verified",
                   return_value=(True, {date(2026, 11, 24),
                                        date(2026, 1, 26)}, "verified")), \
             patch("src.notifier.message_designer.render", return_value=None):
            with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": "AM_0800"}):
                am = format_morning_health_report(self.cfg, **kwargs)
            with patch.dict(os.environ, {"AUDITBOT_HEALTH_SLOT": ""}):
                pm = format_morning_health_report(
                    self.cfg, **{**kwargs, "now_ts": self.when + 9 * 3600}
                )
        self.assertIn("08:00 AM DHAN HEALTH CHECK", am)
        self.assertNotIn("05:00 PM DHAN HEALTH CHECK", am)
        self.assertIn("05:00 PM DHAN HEALTH CHECK", pm)
        self.assertNotIn("08:00 AM DHAN HEALTH CHECK", pm)
        for report in (am, pm):
            self.assertNotIn("📅 Market today", report)
            self.assertNotIn("📅 Market tomorrow", report)
            self.assertNotIn("📆 Next scheduled session", report)

    def test_morning_auth_failure_cannot_trigger_5pm_recovery_card(self):
        self.db.conn.execute(
            "INSERT INTO system_events VALUES(?,?,?)",
            ("MORNING_HEALTH_CHECK", "AUTH_CHECK=FAIL", self.when),
        )
        self.db.conn.commit()
        gate = SimpleNamespace(
            db=self.db, broker=SimpleNamespace(cfg=self.cfg),
            stop_manager=SimpleNamespace(is_stopped=lambda: False),
            state=SimpleNamespace(value="HEALTHY"), is_accepting_entries=True,
        )
        self.assertFalse(queue_verified_dhan_recovery(gate, now_ts=self.when+9*3600))

    def test_deployment_script_parses_without_execution(self):
        root = Path(__file__).resolve().parents[1]
        file = root / "deploy" / "amanbot_dual_health_schedule.sh"
        check = subprocess.run(["bash", "-n", str(file)], capture_output=True, text=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        source = file.read_text(encoding="utf-8")
        self.assertIn("AUDITBOT_HEALTH_SLOT=AM_0800", source)
        self.assertIn("--upgrade-timer", source)
        self.assertIn("auditbot-morning-health.timer", source)


if __name__ == "__main__":
    unittest.main()
