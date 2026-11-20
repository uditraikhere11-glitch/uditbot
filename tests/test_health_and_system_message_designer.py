"""Offline regression: optional system Telegram cards and next-day holiday reporting.

Uses synthetic account identifiers, temp directories and a patched holiday calendar.
Never contacts Dhan/Telegram or accesses production configuration/databases.
"""
import datetime as dt
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src.cli import format_morning_health_report, format_startup_online_card
from src.notifier import message_designer as designer

IST = ZoneInfo("Asia/Kolkata")
ACCOUNT = "9000000001"
SYSTEM_TYPES = ("settings_update", "dhan_health", "startup")


class HealthAndSystemDesignerTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = tmp.name
        self.cfg = SimpleNamespace(
            data_dir=self.data_dir,
            instance_name="Synthetic AuditBot",
            dhan_client_id=ACCOUNT,
            trading_mode="LIVE",
        )
        self.holidays = {dt.date(2026, 1, 26), dt.date(2026, 11, 25)}

    def _enable(self, event, **kwargs):
        settings = designer.load(self.data_dir)
        settings["templates"][event].update(enabled=True, **kwargs)
        designer.save(settings, self.data_dir)

    def _health(self, day, expiry, **kwargs):
        now = dt.datetime.combine(day, dt.time(17, 0, 5), tzinfo=IST)
        payload = dict(
            now_ts=now.timestamp(),
            token_verified=True,
            token_renewed=True,
            funds_verified=True,
            fallback_funds=False,
            available_balance=50000.0,
            master_loaded=True,
            is_stopped=False,
            account_identity_status="VERIFIED",
            profile_client_id=ACCOUNT,
            token_expiration_str=expiry.strftime("%Y-%m-%d %H:%M:%S IST"),
            token_expiration_ts=expiry.timestamp(),
            token_expired=False,
            reconciliation_ok=True,
        )
        payload.update(kwargs)
        with patch(
            "src.engine.market.load_holidays_verified",
            return_value=(True, self.holidays, "OK"),
        ):
            return format_morning_health_report(self.cfg, **payload)

    def test_new_system_categories_are_opt_in(self):
        settings = designer.defaults()
        for event in SYSTEM_TYPES:
            self.assertIn(event, designer.EVENTS)
            self.assertFalse(settings["templates"][event]["enabled"])
        msg = self._health(
            dt.date(2026, 11, 20),
            dt.datetime(2026, 11, 21, 17, 0, 5, tzinfo=IST),
        )
        self.assertTrue(msg.startswith("🕔 <b>AUDITBOT — 05:00 PM DHAN HEALTH CHECK</b>"))

    def test_friday_saturday_closed_next_day_is_not_action_required(self):
        for day in (dt.date(2026, 11, 20), dt.date(2026, 11, 21)):
            with self.subTest(day=day):
                expiry = dt.datetime.combine(day + dt.timedelta(days=1),
                                             dt.time(17, 0, 5), tzinfo=IST)
                msg = self._health(day, expiry)
                self.assertIn("⏳ RENEWAL PENDING (NEXT DAY MARKET CLOSED)", msg)
                self.assertIn("Overall result: ✅ ACTION NOT REQUIRED — NEXT DAY MARKET CLOSED", msg)
                self.assertNotIn("Next trading session readiness: ✅ VERIFIED CHECKS", msg)

    def test_verified_midweek_holiday_is_not_action_required(self):
        msg = self._health(
            dt.date(2026, 11, 24),
            dt.datetime(2026, 11, 25, 9, 0, tzinfo=IST),
        )
        self.assertIn("Overall result: ✅ ACTION NOT REQUIRED — NEXT DAY MARKET CLOSED", msg)

    def test_normal_next_day_still_requires_action(self):
        msg = self._health(
            dt.date(2026, 11, 23),
            dt.datetime(2026, 11, 24, 8, 0, tzinfo=IST),
        )
        self.assertIn("Overall result: ❌ ACTION REQUIRED", msg)

    def test_closed_next_day_does_not_suppress_actual_failures(self):
        day = dt.date(2026, 11, 20)
        expiry = dt.datetime(2026, 11, 21, 17, 0, 5, tzinfo=IST)
        for kwargs in (
            {"token_verified": False}, {"token_renewed": False},
            {"funds_verified": False}, {"fallback_funds": True},
            {"master_loaded": False}, {"is_stopped": True},
            {"reconciliation_ok": False}, {"unresolved_intents_count": 1},
            {"unprotected_positions_count": 1},
            {"token_expiration_ts": 0, "token_expiration_str": ""},
        ):
            with self.subTest(kwargs=kwargs):
                self.assertIn("Overall result: ❌ ACTION REQUIRED",
                              self._health(day, expiry, **kwargs))

    def test_unverified_calendar_never_grants_holiday_exception(self):
        day = dt.date(2026, 11, 20)
        expiry = dt.datetime(2026, 11, 21, 17, 0, 5, tzinfo=IST)
        now = dt.datetime(2026, 11, 20, 17, 0, 5, tzinfo=IST)
        with patch("src.engine.market.load_holidays_verified",
                   return_value=(False, set(), "unverified")):
            msg = format_morning_health_report(
                self.cfg, now_ts=now.timestamp(), token_verified=True,
                token_renewed=True, funds_verified=True, fallback_funds=False,
                available_balance=50000.0, master_loaded=True, is_stopped=False,
                account_identity_status="VERIFIED",
                token_expiration_str=expiry.strftime("%Y-%m-%d %H:%M:%S IST"),
                token_expiration_ts=expiry.timestamp(), token_expired=False,
            )
        self.assertIn("Overall result: ❌ ACTION REQUIRED", msg)

    def test_health_designer_changes_heading_not_authoritative_statuses(self):
        self._enable(
            "dhan_health", title="<Custom Health>", title_emoji="🪄",
            status_emoji="🟢", footer="Review pending", fields=[],
        )
        msg = self._health(
            dt.date(2026, 11, 20),
            dt.datetime(2026, 11, 21, 17, 0, 5, tzinfo=IST),
            reconciliation_ok=False,
        )
        self.assertTrue(msg.startswith("🪄 <b>&lt;Custom Health&gt;</b>"))
        self.assertIn("🔑 Dhan authentication: ✅ VERIFIED", msg)
        self.assertIn("🚦 Next trading session readiness: ⚠️ REVIEW REQUIRED", msg)
        self.assertIn("📊 <b>Overall result: ❌ ACTION REQUIRED</b>", msg)
        self.assertIn("🧾 Reconciliation: ⚠️ NEW ENTRIES BLOCKED", msg)
        self.assertNotIn("🟢 <b>Overall result", msg)

    def test_startup_designer_preserves_new_entry_gate(self):
        self._enable(
            "startup", title="Custom startup", title_emoji="🚀",
            status_emoji="🟢", fields=[],
        )
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=3, reconciliation_ok=False,
            reconciliation_msg="Unresolved protection", unresolved_intents_count=1,
            unprotected_positions_count=1, is_stopped=False,
        )
        self.assertTrue(card.startswith("🚀 <b>Custom startup</b>"))
        self.assertIn("New Entries Gate: <code>NEW ENTRIES BLOCKED</code>", card)
        self.assertIn("Reconciliation: <code>RECONCILIATION PENDING (1)", card)
        self.assertIn("Protection Watcher: <code>PROTECTION UNVERIFIED</code>", card)

    def test_startup_default_remains_unchanged_when_disabled(self):
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=2, reconciliation_ok=True,
        )
        self.assertTrue(card.startswith("🟢 <b>Synthetic AuditBot Online</b>"))
        self.assertIn("New Entries Gate: <code>OPEN (ACCEPTING SIGNALS)</code>", card)

    def test_settings_notification_cosmetic_only(self):
        self._enable(
            "settings_update", title="<Updated>", title_emoji="📣",
            fields=[{"key": "changed_keys", "label": "Settings", "emoji": "⚙️"}],
        )
        required = "Settings updated. Trading bot restarted and the new configuration was verified."
        card = designer.render(
            "settings_update", {"changed_keys": "EQUITY_SL_PCT"}, [required],
            self.data_dir,
        )
        self.assertTrue(card.startswith("📣 <b>&lt;Updated&gt;</b>"))
        self.assertIn("⚙️ Settings: EQUITY_SL_PCT", card)
        self.assertIn(required, card)

    def test_verified_save_apply_queues_designed_telegram_confirmation(self):
        import pathlib
        import sqlite3
        from src.engine import simple_save_apply as direct
        from src.engine.config_patch import revision

        env = pathlib.Path(self.data_dir) / ".env"
        env.write_text(
            chr(10).join([
                "TRADING_MODE=TEST", "DHAN_CLIENT_ID=FAKE_ID",
                f"DATA_DIR={self.data_dir}", "EQUITY_AMO_ENABLED=false",
            ]) + chr(10), encoding="utf-8",
        )
        self._enable(
            "settings_update", title="Verified Settings", fields=[],
        )
        proof = dict(
            data=pathlib.Path(self.data_dir), account="FAKE_ID",
            pid=101, starttime=123,
        )
        with (
            patch.object(direct, "is_isolated_test_environment", return_value=False),
            patch.object(direct, "_positive_running_restart_proof", return_value=proof),
            patch.object(direct, "acquire_apply_barrier", return_value=True),
            patch.object(direct, "_verify_new_runtime"),
        ):
            outcome = direct.apply_settings(
                {"equity_amo_enabled": True},
                env_path=env, expected_revision=revision(env.read_text()),
                state_reader=lambda: "active", restart=lambda: None,
            )
        self.assertEqual(outcome.status, "RESTARTED")
        with sqlite3.connect(pathlib.Path(self.data_dir) / "trade_journal.db") as conn:
            rows = conn.execute(
                "SELECT payload FROM outbox WHERE event_type='SETTINGS_RESTART_VERIFIED'"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        queued = json.loads(rows[0][0])["text"]
        self.assertIn("<b>Verified Settings</b>", queued)
        self.assertIn(
            "Settings updated. Trading bot restarted and the new configuration was verified.",
            queued,
        )

    def test_existing_saved_templates_migrate_without_reset(self):
        old = designer.defaults()
        old["templates"]["fno_amo"]["enabled"] = True
        old["templates"]["fno_amo"]["title"] = "My retained format"
        for new_event in SYSTEM_TYPES:
            old["templates"].pop(new_event)
        with open(designer.file_path(self.data_dir), "w", encoding="utf-8") as stream:
            json.dump(old, stream)
        loaded = designer.load(self.data_dir)
        self.assertTrue(loaded["templates"]["fno_amo"]["enabled"])
        self.assertEqual(loaded["templates"]["fno_amo"]["title"], "My retained format")
        self.assertTrue(all(not loaded["templates"][kind]["enabled"] for kind in SYSTEM_TYPES))

    def test_unknown_template_category_remains_rejected(self):
        settings = designer.defaults()
        settings["templates"]["token_secret_override"] = {"enabled": True}
        with self.assertRaises(ValueError):
            designer.validate(settings)


if __name__ == "__main__":
    unittest.main()
