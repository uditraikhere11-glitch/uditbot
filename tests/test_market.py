"""
Unit tests for Market Hours, Holiday Calendar, and Timing Engine.
"""

import unittest
from datetime import date, datetime
import zoneinfo

from src.engine.market import (
    IST,
    MARKET_OPEN,
    ENTRY_CUTOFF,
    MARKET_CLOSE,
    is_market_open,
    market_status,
    load_holidays,
)


class TestMarketHours(unittest.TestCase):
    def setUp(self):
        self.sample_holidays = {date(2026, 1, 26), date(2026, 8, 15)}

    def test_open_during_regular_hours(self):
        # Wednesday 2026-10-28 10:30:00 IST
        dt = datetime(2026, 10, 28, 10, 30, 0, tzinfo=IST)
        is_open, reason = market_status(dt, holidays=self.sample_holidays)
        self.assertTrue(is_open)
        self.assertTrue(is_market_open(dt, holidays=self.sample_holidays))
        self.assertIn("Market Open", reason)

    def test_closed_on_saturday_and_sunday(self):
        # Saturday 2026-10-31 11:00:00 IST
        dt_sat = datetime(2026, 10, 31, 11, 0, 0, tzinfo=IST)
        is_open, reason = market_status(dt_sat, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Saturday", reason)

        # Sunday 2026-11-01 11:00:00 IST
        dt_sun = datetime(2026, 11, 1, 11, 0, 0, tzinfo=IST)
        is_open, reason = market_status(dt_sun, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Sunday", reason)

    def test_closed_on_exchange_holiday(self):
        # Monday 2026-01-26 (Republic Day) at 10:30:00 IST
        dt_holiday = datetime(2026, 1, 26, 10, 30, 0, tzinfo=IST)
        is_open, reason = market_status(dt_holiday, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Exchange Holiday", reason)

    def test_pre_market_hours_rejected(self):
        # Wednesday 2026-10-28 09:00:00 IST (pre-market)
        dt = datetime(2026, 10, 28, 9, 0, 0, tzinfo=IST)
        is_open, reason = market_status(dt, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Pre-Open", reason)

    def test_entry_cutoff_rejected(self):
        # Wednesday 2026-10-28 15:26:00 IST (after 15:25 entry cutoff)
        dt = datetime(2026, 10, 28, 15, 26, 0, tzinfo=IST)
        is_open, reason = market_status(dt, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Entry Cutoff Exceeded", reason)

    def test_post_market_hours_rejected(self):
        # Wednesday 2026-10-28 18:00:00 IST (evening)
        dt = datetime(2026, 10, 28, 18, 0, 0, tzinfo=IST)
        is_open, reason = market_status(dt, holidays=self.sample_holidays)
        self.assertFalse(is_open)
        self.assertIn("Post-Hours", reason)

    def test_load_holidays_from_file(self):
        holidays = load_holidays()
        self.assertIsInstance(holidays, set)
        # Verify Republic Day 2026 is loaded
        self.assertIn(date(2026, 1, 26), holidays)


if __name__ == "__main__":
    unittest.main()
