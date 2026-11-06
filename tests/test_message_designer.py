"""Offline tests for opt-in Telegram Message Designer and critical status integrity."""
import copy
import os
import stat
import tempfile
import unittest
from unittest.mock import patch

from src.notifier import message_designer as md
from src.notifier.execution_updates import (
    format_amo_acknowledged, format_order_failed, format_order_outcome,
    format_order_submitted,
)


class TestMessageDesigner(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.folder = tmp.name
        env = patch.dict(os.environ, {"DATA_DIR": self.folder})
        env.start()
        self.addCleanup(env.stop)

    def _enable(self, event, **changes):
        settings = md.defaults()
        settings["templates"][event]["enabled"] = True
        settings["templates"][event].update(changes)
        md.save(settings, self.folder)
        return settings

    def test_defaults_dont_change_existing_amo(self):
        self.assertFalse(md.load(self.folder)["templates"]["fno_amo"]["enabled"])
        t = format_amo_acknowledged(
            {"symbol": "HAL4750CE", "limit_price": 120.45,
             "order_id": "ID-123", "entry_only_amo": True,
             "data_dir": self.folder}, 75, "TRANSIT", allow_unverified=True)
        self.assertIn("🌙 <b>Dhan AMO Acknowledgement</b>", t)
        self.assertIn("No automatic SL/target", t)
        self.assertIn("NOT EXECUTED", t)

    def test_fno_custom_emojis_order_labels_and_safe_notice(self):
        self._enable(
            "fno_amo", title="My custom F&O card", title_emoji="🦄",
            status_emoji="🌟", footer="Custom note",
            fields=[
                {"key": "order_id", "label": "Broker Ref", "emoji": "🧿"},
                {"key": "contract", "label": "My Option", "emoji": "🪄"},
                {"key": "limit_price", "label": "Buy Limit", "emoji": "💰"},
            ],
        )
        t = format_amo_acknowledged(
            {"symbol": "HAL4750CE", "limit_price": 120.45,
             "order_id": "ID-123", "entry_only_amo": True,
             "data_dir": self.folder}, 75, "PENDING", allow_unverified=True)
        self.assertTrue(t.startswith("🦄 <b>My custom F&amp;O card</b>"))
        self.assertLess(t.index("🧿 Broker Ref"), t.index("🪄 My Option"))
        self.assertIn("💰 Buy Limit: ₹120.45", t)
        self.assertIn("Custom note", t)
        self.assertIn("🌟 <b>AMO Accepted by Dhan</b>", t)
        self.assertIn("⏳ Queued for market OPEN — NOT EXECUTED", t)
        self.assertIn("⚠️ F&O AMO: No automatic SL/target.", t)

    def test_fno_unknown_status_cannot_claim_queued(self):
        self._enable("fno_amo", fields=[])
        t = format_amo_acknowledged(
            {"symbol": "HAL", "limit_price": 120,
             "order_id": "ID-123", "entry_only_amo": True,
             "data_dir": self.folder}, 75, "TRADED", allow_unverified=True)
        self.assertIn("Broker status needs verification", t)
        self.assertNotIn("Queued for market OPEN", t)
        self.assertNotIn("AMO Accepted by Dhan", t)
        self.assertIn("No automatic SL/target", t)

    def test_html_escape_for_all_operator_content_and_data(self):
        self._enable("fno_amo", title="<h1>bad</h1>", title_emoji="<unsafe>",
                     footer="<img src=x>", fields=[{"key": "order_id", "label": "<id>",
                     "emoji": "<script>"}])
        t = md.render("fno_amo", {"order_id": "<BROKER>&"}, ["✅ <b>Fixed Status</b>"], self.folder)
        self.assertIn("&lt;h1&gt;bad&lt;/h1&gt;", t)
        self.assertIn("&lt;BROKER&gt;&amp;", t)
        self.assertNotIn("<script>", t)
        self.assertNotIn("<img", t)
        self.assertIn("Fixed Status", t)

    def test_reject_extra_fields_duplicate_and_invalid_emoji(self):
        s = md.defaults()
        s["templates"]["fno_amo"]["fields"].append(
            copy.deepcopy(s["templates"]["fno_amo"]["fields"][0]))
        with self.assertRaises(ValueError):
            md.validate(s)
        s = md.defaults()
        s["templates"]["fno_amo"]["fields"][0]["key"] = "access_token"
        with self.assertRaises(ValueError):
            md.validate(s)
        s = md.defaults()
        s["templates"]["fno_amo"]["fields"][0]["emoji"] = "🚀" * 40
        with self.assertRaises(ValueError):
            md.validate(s)

    def test_atomic_permissions_and_live_reload(self):
        cfg = self._enable("fno_amo", title="Before")
        path = md.file_path(self.folder)
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        self.assertIn("Before", md.render("fno_amo", {}, ["✅ Fixed"], self.folder))
        cfg["templates"]["fno_amo"]["title"] = "After"
        md.save(cfg, self.folder)
        self.assertIn("After", md.render("fno_amo", {}, ["✅ Fixed"], self.folder))
        self.assertNotIn("Before", md.render("fno_amo", {}, ["✅ Fixed"], self.folder))

    def test_standard_and_compact_styles(self):
        self._enable("submitted", style="compact",
                     fields=[
                         {"key": "symbol", "label": "Ticker", "emoji": "🌟"},
                         {"key": "quantity", "label": "Qty", "emoji": "🔢"},
                     ])
        msg = format_order_submitted(
            {"symbol": "HAL", "quantity": 75, "limit_price": 120,
             "data_dir": self.folder, "source": "Signal"}, 75)
        self.assertIn("🌟 Ticker: HAL | 🔢 Qty: 75", msg)
        self.assertIn("Status: Order Submitted", msg)

    def test_terminal_status_and_partial_never_hidden(self):
        self._enable("partial", fields=[], title="Custom")
        msg = format_order_outcome(
            {"symbol": "HAL", "data_dir": self.folder}, 75, 25,
            "PART_TRADED", 1800000000.0, 120.5, False)
        self.assertIn("Partially Executed", msg)
        self.assertIn("Filled: 25/75", msg)
        self.assertIn("Pending Broker Verification", msg)

    def test_invalid_file_returns_disabled_defaults(self):
        with open(md.file_path(self.folder), "w", encoding="utf-8") as f:
            f.write('{"version": 999}')
        self.assertFalse(md.load(self.folder)["templates"]["fno_amo"]["enabled"])

    def test_operator_cannot_overwrite_unrelated_event_schema(self):
        cfg = md.defaults()
        cfg["templates"]["injected-event"] = {"enabled": True}
        with self.assertRaises(ValueError):
            md.save(cfg, self.folder)

    def test_disabled_rejected_notification_unchanged(self):
        self.assertIn("Order Rejected", format_order_failed(
            {"symbol": "HAL", "segment": "FNO", "mode": "LIVE"}, 75))


if __name__ == "__main__":
    unittest.main()
