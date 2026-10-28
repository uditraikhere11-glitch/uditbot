"""
End-to-End Pipeline Integration Tests.
Verifies complete flow in TEST mode: zero live orders, zero fake fills, full audit journal.
"""

from datetime import date
import os
import tempfile
import unittest
from src.config import AppConfig
from src.main import TradingBotPipeline
from src.models import SignalAction, SignalType, TradingMode


class TestPipelineE2E(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "e2e_journal.db")

        self.cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            equity_active=True,
            fno_active=True,
            journal_db_path=self.db_path,
            equity_budget_inr=25000.0,
            equity_price_markup_pct=0.5,
            fno_capital_per_trade=20000.0,
            fno_max_lots=3,
            dedup_window_sec=20.0,
            max_message_age_sec=60.0,
        )
        self.pipeline = TradingBotPipeline(self.cfg)

        # Pre-load test scrip master entries
        self.pipeline.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ")
        self.pipeline.resolver.load_equity("TATAMOTORS", security_id="3456", exchange_segment="NSE_EQ")
        self.pipeline.resolver.load_option(
            symbol="POLYCAB",
            security_id="54231",
            strike=8000.0,
            option_type="PE",
            expiry=date(2026, 10, 29),
            lot_size=100,
            trading_symbol="POLYCAB-29OCT26-8000-PE",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_e2e_equity_buy_test_mode(self):
        res = self.pipeline.process_raw_message(
            text="Buy #SBIN @ 805",
            channel_id=-1001,
            message_id=1,
            msg_timestamp=1000.0,
            current_time=1005.0,
        )
        self.assertTrue(res.is_valid)
        self.assertEqual(res.signal_type, SignalType.EQUITY)
        self.assertEqual(res.symbol, "SBIN")
        self.assertEqual(res.security_id, "3045")
        # CMP 805 + 0.5% markup = 809.025 -> nearest tick 809.0
        self.assertEqual(res.limit_price, 809.0)
        # Budget 25,000 / 809.0 = 30 shares
        self.assertEqual(res.quantity_or_lots, 30)

        # Verify DB order record status is TEST_VALIDATED and fills table is empty
        with self.pipeline.db._get_conn() as conn:
            cur = conn.execute("SELECT status, price, quantity FROM orders WHERE symbol = 'SBIN'")
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "TEST_VALIDATED")
            self.assertEqual(row["quantity"], 30)

            cur2 = conn.execute("SELECT COUNT(*) FROM fills")
            self.assertEqual(cur2.fetchone()[0], 0, "TEST mode must never create simulated/fake fills")

    def test_e2e_option_buy_test_mode(self):
        res = self.pipeline.process_raw_message(
            text="#POLYCAB 8000 PE OCT @90-105",
            channel_id=-1002,
            message_id=2,
            msg_timestamp=2000.0,
            current_time=2005.0,
        )
        self.assertTrue(res.is_valid)
        self.assertEqual(res.signal_type, SignalType.OPTION)
        self.assertEqual(res.symbol, "POLYCAB")
        self.assertEqual(res.security_id, "54231")
        # Ceiling: 105 * 1.04 = 109.20
        self.assertEqual(res.limit_price, 109.20)
        # Lot cost = 100 * 109.20 = 10,920 -> Budget 20,000 / 10,920 = 1 lot (100 shares)
        self.assertEqual(res.quantity_or_lots, 1)

    def test_e2e_duplicate_cross_group_blocked(self):
        # 1. Group A at t=3000
        res1 = self.pipeline.process_raw_message(
            text="Buy #TATAMOTORS @ 950",
            channel_id=-1001,
            message_id=10,
            msg_timestamp=3000.0,
            current_time=3000.0,
        )
        self.assertTrue(res1.is_valid)

        # 2. Group B at t=3010 (elapsed 10s <= 20s window) -> Blocked as duplicate
        res2 = self.pipeline.process_raw_message(
            text="Buy #TATAMOTORS @ 955",
            channel_id=-1002,
            message_id=20,
            msg_timestamp=3010.0,
            current_time=3010.0,
        )
        self.assertFalse(res2.is_valid)
        self.assertIn("Duplicate signal for TATAMOTORS", res2.reason)

    def test_e2e_index_rejection(self):
        res = self.pipeline.process_raw_message(
            text="Buy #NIFTY 24500 CE @ 120",
            channel_id=-1001,
            message_id=30,
            msg_timestamp=4000.0,
            current_time=4002.0,
        )
        self.assertFalse(res.is_valid)
        self.assertIn("Index option rejected", res.reason)

    def test_e2e_exit_classification_no_order(self):
        res = self.pipeline.process_raw_message(
            text="Book profit in POLYCAB 8000 PE",
            channel_id=-1001,
            message_id=40,
            msg_timestamp=5000.0,
            current_time=5002.0,
        )
        self.assertFalse(res.is_valid)
        self.assertEqual(res.action, SignalAction.EXIT_SIGNAL)

        # Confirm 0 orders were created
        with self.pipeline.db._get_conn() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM orders WHERE symbol = 'POLYCAB'")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
