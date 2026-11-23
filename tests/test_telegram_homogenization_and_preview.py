"""Offline acceptance tests for Telegram homogenization & server-side Live Preview sync."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.notifier import execution_updates as eu
from src.notifier import message_designer as md
from src.notifier.message_designer_routes import register_routes


class TelegramHomogenizationAndPreviewTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.csrf = "test-csrf-token"
        register_routes(self.app, self.csrf)
        self.client = TestClient(self.app)

    def test_zero_vwap_across_all_events_default_and_custom(self):
        for event in md.EVENTS:
            default_msg = md.render_event_preview(event, md._default_template(event))
            self.assertNotIn("VWAP", default_msg, f"VWAP leaked in default {event}")

            custom_tpl = md._default_template(event)
            custom_tpl["enabled"] = True
            custom_msg = md.render_event_preview(event, custom_tpl)
            self.assertNotIn("VWAP", custom_msg, f"VWAP leaked in custom {event}")

        for label in md.FIELDS.values():
            self.assertNotIn("VWAP", label)

    def test_all_protection_and_exit_events_present_and_preview_matches_production_byte_for_byte(self):
        expected_new = {
            "protection_pending", "protection_verified", "protection_unverified", "protection_rejected",
            "target_triggered", "sl_triggered", "exit_partial_filled", "position_closed",
            "reconciliation_required",
        }
        self.assertTrue(expected_new.issubset(md.EVENTS.keys()))

        for event in md.EVENTS:
            for enabled in (False, True):
                tpl = md._default_template(event)
                tpl["enabled"] = enabled
                resp = self.client.post(
                    "/setup/telegram-designer/preview",
                    headers={"X-CSRF-Token": self.csrf},
                    json={"event": event, "template": tpl},
                )
                self.assertEqual(resp.status_code, 200, f"Preview failed for {event} enabled={enabled}: {resp.text}")
                data = resp.json()
                direct = md.render_event_preview(event, tpl)
                self.assertEqual(data["html"], direct, f"Preview drifted from production for {event} enabled={enabled}")

    def test_dhan_health_default_preview_matches_actual_telegram_report_fields(self):
        tpl = md._default_template("dhan_health")
        tpl["enabled"] = False
        resp = self.client.post(
            "/setup/telegram-designer/preview",
            headers={"X-CSRF-Token": self.csrf},
            json={"event": "dhan_health", "template": tpl},
        )
        self.assertEqual(resp.status_code, 200)
        plain = resp.json()["plain"]
        self.assertIn("AUDITBOT — 05:00 PM DHAN HEALTH CHECK", plain)
        self.assertIn("Instance: Auditbot", plain)
        self.assertIn("Trading mode: LIVE", plain)
        self.assertIn("Broker funds: ✅ VERIFIED — ₹949.21", plain)
        self.assertEqual(md.EVENTS["dhan_health"]["allowed"], [])

    def test_vedpower_and_pdsl_full_lifecycle_homogeneity_and_safety(self):
        for sym, entry_p, exit_p, pnl in (("VEDPOWER", 34.69, 35.06, 0.37), ("PDSL", 402.50, 417.75, 15.25)):
            meta = {
                "symbol": sym, "action": "BUY", "segment": "EQUITY", "mode": "LIVE",
                "source": "Received RDHS", "yahoo_cmp": entry_p, "limit_price": entry_p + 0.10,
                "quote_age_sec": 3.5, "signal_received_at": 1795405500.0,
            }
            c_sub = eu.format_order_submitted(meta, 1)
            c_exe = eu.format_order_outcome(meta, 1, 1, "TRADED", 1795405502.0, entry_p, False, protection_route="FOREVER_OCO")
            c_prt = eu.format_protection_lifecycle_card(meta, "PROTECTION_VERIFIED_ACTIVE", protected_qty=1, total_qty=1, sl_price=entry_p * 0.99, target_price=exit_p, protection_route="FOREVER_OCO")
            c_trg = eu.format_exit_lifecycle_card(meta, "TARGET_TRIGGERED", triggered_leg="TARGET_LEG", sell_qty=0, total_qty=1, buy_vwap=entry_p, protection_route="FOREVER_OCO")
            c_cls = eu.format_exit_lifecycle_card(meta, "POSITION_CLOSED", triggered_leg="TARGET_LEG", sell_qty=1, total_qty=1, buy_vwap=entry_p, sell_vwap=exit_p, realized_pnl=pnl, protection_route="FOREVER_OCO")

            for card in (c_sub, c_exe, c_prt, c_trg, c_cls):
                self.assertNotIn("VWAP", card)
                self.assertNotIn("321261120279408", card)

            self.assertIn("Target Leg Triggered (Awaiting Fill)", c_trg)
            self.assertNotIn("Position Closed", c_trg)
            self.assertIn(f"Entry Price: ₹{entry_p:.2f}", c_cls)
            self.assertIn(f"Exit Price: ₹{exit_p:.2f}", c_cls)
            self.assertIn("Position Closed (Target Hit)", c_cls)

    def test_legacy_8_and_11_template_saved_json_migrates_cleanly(self):
        full = md.defaults()
        # Legacy 8-template payload (only trade events)
        legacy8 = {
            "version": 1,
            "templates": {k: full["templates"][k] for k in md.LEGACY_TRADE_EVENTS},
        }
        legacy8["templates"]["executed"]["enabled"] = True
        legacy8["templates"]["executed"]["title"] = "Custom Executed"
        validated = md.validate(legacy8)
        self.assertTrue(validated["templates"]["executed"]["enabled"])
        self.assertEqual(validated["templates"]["executed"]["title"], "Custom Executed")
        self.assertIn("position_closed", validated["templates"])
        self.assertFalse(validated["templates"]["position_closed"]["enabled"])

        # Legacy 11-template payload with old dhan_health dead fields
        legacy11 = {
            "version": 1,
            "templates": {k: full["templates"][k] for k in (md.LEGACY_TRADE_EVENTS | md.SYSTEM_EVENTS)},
        }
        legacy11["templates"]["dhan_health"]["fields"] = [{"key": "instance", "label": "Bot", "emoji": "•"}]
        validated11 = md.validate(legacy11)
        self.assertEqual(validated11["templates"]["dhan_health"]["fields"], [])
        self.assertIn("target_triggered", validated11["templates"])


if __name__ == "__main__":
    unittest.main()
