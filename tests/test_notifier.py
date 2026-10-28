"""
Unit tests for visual Telegram trade log message formatting.
"""

import unittest
from src.notifier.formatter import format_trade_log, format_exit_log


class TestLogFormatter(unittest.TestCase):
    def test_format_trade_log_live(self):
        text = format_trade_log(
            symbol="TMPV",
            source_name="Test group",
            original_message="#TMPV 280CE @8.5-9 Sl-5 Target-16,20 #Positional",
            segment="FNO",
            status="Message sent to Dhan",
            dhan_order_details="[Order ID/Status will appear here]",
            heads_up="Instrument unverified",
            is_live=True,
            is_processed=True,
            timestamp=1729987259.0,
        )
        self.assertIn("✅ Trade Signal Processed: TMPV (Source: Test group)", text)
        self.assertIn("📩 The Trade Details:", text)
        self.assertIn("o   Original Message: #TMPV 280CE @8.5-9 Sl-5 Target-16,20 #Positional", text)
        self.assertIn("🤖 Bot Action Taken:", text)
        self.assertIn("o   Segment: FNO", text)
        self.assertIn("o   Status: Message sent to Dhan", text)
        self.assertIn("o   Dhan Order Details: [Order ID/Status will appear here]", text)
        self.assertIn("⚠️ Heads Up: Instrument unverified", text)
        self.assertIn("_________________________", text)
        self.assertIn("IST", text)
        self.assertIn("🟢 Bot - LIVE", text)

    def test_format_trade_log_test_mode(self):
        text = format_trade_log(
            symbol="SBIN",
            source_name="VIP Cash",
            original_message="Buy #SBIN @ 805",
            segment="EQUITY",
            status="TEST Mode: Validated & Sized",
            dhan_order_details="[TEST Mode / Qty: 30 shares / Limit: 809.00]",
            heads_up="TEST Mode active - zero broker orders placed",
            is_live=False,
            is_processed=True,
        )
        self.assertIn("✅ Trade Signal Processed: SBIN (Source: VIP Cash)", text)
        self.assertIn("🟡 Bot - TEST", text)
        self.assertIn("o   Segment: EQUITY", text)

    def test_format_exit_log(self):
        text = format_exit_log(
            symbol="POLYCAB",
            source_name="Test group",
            original_message="Exit POLYCAB 8000 PE",
            segment="FNO",
            reason="Exit signal classified (BUY-ONLY Phase)",
            is_live=False,
        )
        self.assertIn("ℹ️ Exit Signal Logged: POLYCAB", text)
        self.assertIn("o   Status: Logged for Audit (BUY-ONLY Phase - No broker exit order placed)", text)
        self.assertIn("🟡 Bot - TEST", text)


if __name__ == "__main__":
    unittest.main()
