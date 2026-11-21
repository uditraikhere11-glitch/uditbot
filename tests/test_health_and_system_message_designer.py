"""Offline regression: optional system Telegram cards and next-day holiday reporting.

Uses synthetic account identifiers, temp directories and a patched holiday calendar.
Never contacts Dhan/Telegram or accesses production configuration/databases.
"""
import datetime as dt
import json
from html import unescape
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
        self.holidays = {dt.date(2026, 1, 26), dt.date(2026, 11, 24)}

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
                self.assertIn("Market tomorrow (", msg)
                self.assertIn("🚫 CLOSED (Weekend)", msg)

    def test_verified_midweek_holiday_is_not_action_required(self):
        msg = self._health(
            dt.date(2026, 11, 23),
            dt.datetime(2026, 11, 24, 9, 0, tzinfo=IST),
        )
        self.assertIn("Overall result: ✅ ACTION NOT REQUIRED — NEXT DAY MARKET CLOSED", msg)
        self.assertIn("Market tomorrow (Tue 24 Nov 2026): 🚫 CLOSED (Exchange holiday)", msg)
        self.assertIn("Next scheduled session: Wed 25 Nov 2026", msg)

    def test_exchange_holiday_today_explicitly_identified(self):
        msg = self._health(
            dt.date(2026, 11, 24),
            dt.datetime(2026, 11, 25, 17, 0, 5, tzinfo=IST),
        )
        self.assertIn("Market today (Tue 24 Nov 2026): 🚫 CLOSED (Exchange holiday)", msg)
        self.assertIn("Market tomorrow (Wed 25 Nov 2026): ✅ SCHEDULED TRADING DAY", msg)
        self.assertIn("Next scheduled session: Wed 25 Nov 2026", msg)

    def test_normal_next_day_still_requires_action(self):
        msg = self._health(
            dt.date(2026, 11, 25),
            dt.datetime(2026, 11, 26, 8, 0, tzinfo=IST),
        )
        self.assertIn("Overall result: ❌ ACTION REQUIRED", msg)

    def test_failed_auth_still_shows_holiday_information(self):
        now = dt.date(2026, 11, 21)  # Saturday; tomorrow Sunday closed.
        expiry = dt.datetime(2026, 11, 21, 17, 0, 6, tzinfo=IST)
        msg = self._health(now, expiry, token_verified=False, token_renewed=False)
        self.assertIn("Market tomorrow (Sun 22 Nov 2026): 🚫 CLOSED (Weekend)", msg)
        self.assertIn("Next scheduled session: Mon 23 Nov 2026", msg)
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
        self.assertIn("New Entries Gate: Blocked", card)
        self.assertIn("Reconciliation: Pending (1)", card)
        self.assertIn("Protection Watcher: Unverified", card)

    def test_startup_default_remains_unchanged_when_disabled(self):
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=2, reconciliation_ok=True,
        )
        self.assertTrue(card.startswith("🟢 <b>Synthetic AuditBot Online</b>"))
        self.assertIn("New Entries Gate: Open", card)

    def test_settings_notification_cosmetic_only(self):
        self._enable(
            "settings_update", title="<Updated>", title_emoji="📣",
            fields=[{"key": "changed_keys", "label": "Settings", "emoji": "⚙️"}],
        )
        required = [
            "Trading bot: Restarted", "Configuration: Verified",
            "Changed Settings (1):", "• Equity SL: 3% → 4%",
        ]
        card = designer.render(
            "settings_update", {"changed_keys": "EQUITY_SL_PCT"}, required,
            self.data_dir,
        )
        self.assertTrue(card.startswith("📣 <b>&lt;Updated&gt;</b>"))
        self.assertIn("⚙️ Settings (1):", card)
        self.assertEqual(card.count("Equity SL: 3% → 4%"), 1)
        self.assertIn("Configuration: Verified", card)

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
        self.assertIn("Trading bot: Restarted", queued)
        self.assertIn("Configuration: Verified", queued)
        self.assertIn("Changed Settings (1):", queued)
        self.assertIn("Equity AMO: Disabled → Enabled", queued)
        self.assertNotIn("Settings updated. Trading bot restarted", queued)

    def test_startup_default_is_readable_and_not_repeated(self):
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=2, reconciliation_ok=True,
        )
        self.assertEqual(card.count("Listener:"), 1)
        self.assertEqual(card.count("Reconciliation:"), 1)
        self.assertEqual(card.count("New Entries Gate:"), 1)
        self.assertIn("Listener: Active 2 channels", card)
        self.assertIn("Mode: LIVE", card)
        self.assertNotIn("<code>", card)
        self.assertNotIn("Status: Actively listening", card)
        self.assertNotIn("channels (monitored channels only)", card)

    def test_startup_fields_customize_existing_rows_without_duplicate(self):
        self._enable(
            "startup", title="Status Overview", fields=[
                {"key": "mode", "label": "Bot Mode", "emoji": "⚙️"},
                {"key": "channels", "label": "Signal Channels", "emoji": "📡"},
            ],
        )
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=2, reconciliation_ok=True,
        )
        self.assertIn("⚙️ Bot Mode: LIVE", card)
        self.assertIn("📡 Signal Channels: Active 2 channels", card)
        self.assertEqual(card.count("LIVE"), 1)
        self.assertEqual(card.count("Active 2 channels"), 1)
        self.assertNotIn("<code>", card)

    def test_startup_legacy_selected_fields_do_not_duplicate_essential_status(self):
        self._enable(
            "startup", title="Synthetic AuditBot Online",
            fields=[
                {"key": "instance", "label": "Bot Instance", "emoji": "•"},
                {"key": "mode", "label": "Mode", "emoji": "•"},
                {"key": "channels", "label": "Monitored", "emoji": "•"},
            ],
        )
        card = format_startup_online_card(
            self.cfg, allowed_chats_count=2, reconciliation_ok=False,
            reconciliation_msg="Review required", is_stopped=True,
        )
        self.assertEqual(card.count("LIVE"), 1)
        self.assertNotIn("Bot Instance:", card)
        self.assertIn("New Entries Gate: Blocked", card)
        self.assertNotIn("OPEN (ACCEPTING SIGNALS)", card)
        self.assertNotIn("<code>", card)

    def test_settings_default_notification_no_long_repeated_sentence(self):
        from src.notifier import message_designer as md
        self.assertIsNone(md.render(
            "settings_update", {}, ["Trading bot: Restarted", "Configuration: Verified"],
            self.data_dir,
        ))
        # Disabled templates preserve the code-owned concise default.
        default_card = chr(10).join([
            "✅ <b>Settings Updated</b>",
            "Trading bot: Restarted",
            "Configuration: Verified",
        ])
        self.assertEqual(default_card.count("Settings Updated"), 1)
        self.assertNotIn("Settings updated. Trading bot restarted", default_card)

    def test_verified_setting_diff_is_recorded_and_secrets_are_redacted(self):
        changed = {
            "EQUITY_SL_PCT": ("3", "4"),
            "FNO_TARGET_PCT": ("3", "5"),
            "DHAN_TOTP_SECRET": ("OLD_TOTP_SECRET", "NEW_TOTP_SECRET"),
            "DHAN_CLIENT_ID": ("9000000001", "9000000002"),
            "TG_API_ID": ("123456", "456789"),
            "TG_BOT_TOKEN": ("OLD_BOT_TOKEN", "NEW_BOT_TOKEN"),
            "BOT_INSTANCE_NAME": ("OLD_NAME", "NEW_NAME"),
        }
        lines = designer.format_changed_settings(changed)
        message = chr(10).join(lines)
        self.assertIn("Changed Settings (7):", message)
        self.assertIn("• Equity SL: 3% → 4%", message)
        self.assertIn("• F&amp;O Target: 3% → 5%", message)
        self.assertIn("• F&O Target: 3% → 5%", unescape(message))
        for secret in (
            "OLD_TOTP_SECRET", "NEW_TOTP_SECRET", "9000000001",
            "9000000002", "123456", "456789", "OLD_BOT_TOKEN",
            "NEW_BOT_TOKEN", "OLD_NAME", "NEW_NAME",
        ):
            self.assertNotIn(secret, message)
        self.assertIn("Dhan TOTP Secret: Updated (values hidden)", message)

    def test_setting_diff_rejects_invalid_previous_value_without_leak(self):
        lines = designer.format_changed_settings({
            "EQUITY_SL_PCT": ("not-a-valid-number", "4")
        })
        message = chr(10).join(lines)
        self.assertNotIn("not-a-valid-number", message)
        self.assertIn("Updated (values hidden)", message)

    def test_bool_setting_diff_uses_readable_values(self):
        lines = designer.format_changed_settings({
            "EQUITY_AMO_ENABLED": ("false", "true"),
        })
        self.assertTrue(any("Disabled → Enabled" in line for line in lines))

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
