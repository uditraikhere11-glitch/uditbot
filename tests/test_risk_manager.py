"""
Unit tests for Risk Management limits and Emergency Stop.
"""

import unittest
from src.config import AppConfig
from src.engine.risk_manager import RiskManager


class TestRiskManager(unittest.TestCase):
    def setUp(self):
        self.cfg = AppConfig(
            max_order_amount_inr=50000.0,
            min_trade_funds_inr=10000.0,
            max_daily_loss_inr=15000.0,
            max_total_exposure_inr=150000.0,
            max_open_positions=5,
            max_daily_trades=10,
        )
        self.rm = RiskManager(self.cfg)

    def test_normal_order_passes(self):
        passed, reason = self.rm.validate_order_risk(
            order_cost_inr=20000.0,
            available_funds_inr=50000.0,
        )
        self.assertTrue(passed)
        self.assertIn("Passed risk validation", reason)

    def test_emergency_stop_blocks_order(self):
        self.rm.trigger_emergency_stop("Test alert")
        self.assertTrue(self.rm.is_emergency_stopped)

        passed, reason = self.rm.validate_order_risk(20000.0, 50000.0)
        self.assertFalse(passed)
        self.assertIn("EMERGENCY STOP is active", reason)

        # Resume trading
        self.rm.resume_trading()
        self.assertFalse(self.rm.is_emergency_stopped)
        passed2, _ = self.rm.validate_order_risk(20000.0, 50000.0)
        self.assertTrue(passed2)

    def test_min_trade_funds_breach(self):
        # Available funds 8,000 < 10,000 minimum
        passed, reason = self.rm.validate_order_risk(order_cost_inr=5000.0, available_funds_inr=8000.0)
        self.assertFalse(passed)
        self.assertIn("below minimum required threshold", reason)

    def test_max_order_amount_breach(self):
        # Order cost 60,000 > 50,000 max order limit
        passed, reason = self.rm.validate_order_risk(order_cost_inr=60000.0, available_funds_inr=100000.0)
        self.assertFalse(passed)
        self.assertIn("MAX_ORDER_AMOUNT_INR", reason)

    def test_max_daily_trades_breach(self):
        self.rm.update_account_state(current_exposure=0, open_positions=0, daily_trades=10, daily_pnl=0)
        passed, reason = self.rm.validate_order_risk(order_cost_inr=10000.0, available_funds_inr=50000.0)
        self.assertFalse(passed)
        self.assertIn("Daily trade limit reached", reason)

    def test_max_open_positions_breach(self):
        self.rm.update_account_state(current_exposure=0, open_positions=5, daily_trades=2, daily_pnl=0)
        passed, reason = self.rm.validate_order_risk(order_cost_inr=10000.0, available_funds_inr=50000.0)
        self.assertFalse(passed)
        self.assertIn("Max open positions reached", reason)

    def test_max_total_exposure_breach(self):
        # Current exposure 140,000 + 20,000 = 160,000 > 150,000
        self.rm.update_account_state(current_exposure=140000.0, open_positions=3, daily_trades=3, daily_pnl=0)
        passed, reason = self.rm.validate_order_risk(order_cost_inr=20000.0, available_funds_inr=50000.0)
        self.assertFalse(passed)
        self.assertIn("MAX_TOTAL_EXPOSURE_INR", reason)

    def test_daily_loss_circuit_breaker(self):
        # Daily loss -16,000 >= 15,000 limit
        self.rm.update_account_state(current_exposure=0, open_positions=0, daily_trades=2, daily_pnl=-16000.0)
        passed, reason = self.rm.validate_order_risk(order_cost_inr=10000.0, available_funds_inr=50000.0)
        self.assertFalse(passed)
        self.assertIn("Daily loss limit breached", reason)


if __name__ == "__main__":
    unittest.main()
