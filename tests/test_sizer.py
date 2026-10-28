"""
Unit tests for Sizing, Price Ceilings, and Protection Levels.
"""

import unittest
from src.engine.sizer import (
    size_equity,
    size_option,
    calculate_equity_limit_price,
    calculate_option_limit_price,
    calculate_protection_levels,
    round_to_tick,
)


class TestSizer(unittest.TestCase):
    def test_round_to_tick(self):
        # 104.128 rounded down to 0.05 tick -> 104.10
        self.assertEqual(round_to_tick(104.128, 0.05, mode="down"), 104.10)
        # 104.128 rounded up to 0.05 tick -> 104.15
        self.assertEqual(round_to_tick(104.128, 0.05, mode="up"), 104.15)
        # 104.128 nearest to 0.05 tick -> 104.15
        self.assertEqual(round_to_tick(104.128, 0.05, mode="nearest"), 104.15)

    def test_equity_limit_price(self):
        # CMP 800 with 0.5% markup -> 804.00
        p = calculate_equity_limit_price(cmp=800.0, markup_pct=0.5, tick_size=0.05)
        self.assertEqual(p, 804.0)

    def test_option_limit_ceiling_never_exceeded(self):
        # entry_max = 105 -> 105 * 1.04 = 109.20
        p1 = calculate_option_limit_price(105.0, tick_size=0.05)
        self.assertEqual(p1, 109.20)
        self.assertLessEqual(p1, 105.0 * 1.04)

        # entry_max = 92.31 -> 92.31 * 1.04 = 96.0024 -> floored to 96.00
        p2 = calculate_option_limit_price(92.31, tick_size=0.05)
        self.assertEqual(p2, 96.00)
        self.assertLessEqual(p2, 92.31 * 1.04)

    def test_equity_sizing_budget(self):
        # Budget 25,000 INR at price 805 -> floor(25000 / 805) = 31 shares
        res = size_equity(sizing_mode="BUDGET", budget_inr=25000.0, fixed_qty=10, limit_price=805.0)
        self.assertTrue(res.is_valid)
        self.assertEqual(res.units, 31)
        self.assertEqual(res.total_quantity, 31)
        self.assertEqual(res.total_cost, 24955.0)

    def test_equity_sizing_insufficient_budget(self):
        # Budget 500 INR at price 805 -> 0 shares -> Rejected
        res = size_equity(sizing_mode="BUDGET", budget_inr=500.0, fixed_qty=10, limit_price=805.0)
        self.assertFalse(res.is_valid)
        self.assertEqual(res.units, 0)
        self.assertIn("Insufficient budget", res.error_reason)

    def test_option_sizing_budget(self):
        # Budget 20,000 INR, lot size 250, limit price 25.0 -> cost/lot = 6,250 -> 3 lots (qty 750)
        res = size_option(
            sizing_mode="BUDGET",
            capital_per_trade=20000.0,
            fixed_lots=1,
            max_lots=0,
            lot_size=250,
            limit_price=25.0,
        )
        self.assertTrue(res.is_valid)
        self.assertEqual(res.units, 3)
        self.assertEqual(res.total_quantity, 750)
        self.assertEqual(res.total_cost, 18750.0)

    def test_option_sizing_max_lots_cap(self):
        # Budget affords 3 lots, but max_lots = 2 -> Capped to 2 lots
        res = size_option(
            sizing_mode="BUDGET",
            capital_per_trade=20000.0,
            fixed_lots=1,
            max_lots=2,
            lot_size=250,
            limit_price=25.0,
        )
        self.assertTrue(res.is_valid)
        self.assertEqual(res.units, 2)
        self.assertEqual(res.total_quantity, 500)

    def test_option_sizing_insufficient_capital(self):
        # Budget 5,000 INR, cost/lot = 6,250 -> 0 lots -> Rejected
        res = size_option(
            sizing_mode="BUDGET",
            capital_per_trade=5000.0,
            fixed_lots=1,
            max_lots=0,
            lot_size=250,
            limit_price=25.0,
        )
        self.assertFalse(res.is_valid)
        self.assertIn("Insufficient capital", res.error_reason)

    def test_protection_levels_calculation(self):
        # Executed fill price = 100.0, SL=3%, Target=3%
        levels = calculate_protection_levels(
            executed_price=100.0,
            quantity=50,
            sl_pct=3.0,
            target_pct=3.0,
            sl_limit_buffer_pct=1.0,
            tick_size=0.05,
        )
        self.assertEqual(levels.executed_price, 100.0)
        self.assertEqual(levels.sl_trigger, 97.00)
        self.assertEqual(levels.target_price, 103.00)
        self.assertEqual(levels.quantity, 50)
        self.assertLess(levels.sl_limit, levels.sl_trigger)


if __name__ == "__main__":
    unittest.main()
