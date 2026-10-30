"""
Unit tests for Scrip Master Instrument Resolver & STEP 13 exact contract resolution.
"""

from datetime import date
import os
import tempfile
import unittest
from src.instruments.resolver import InstrumentResolver


class TestInstrumentResolver(unittest.TestCase):
    def setUp(self):
        self.resolver = InstrumentResolver()
        # Load sample cash equities
        self.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ")
        self.resolver.load_equity("TATAMOTORS", security_id="3456", exchange_segment="NSE_EQ")
        self.resolver.load_equity("NIFTY", security_id="999", exchange_segment="NSE_EQ")  # Index should be ignored

        # Load sample stock options (two expiries in same month to test deterministic resolution)
        self.resolver.load_option(
            symbol="POLYCAB",
            security_id="54230",
            strike=8000.0,
            option_type="PE",
            expiry=date(2026, 10, 22),
            lot_size=100,
            trading_symbol="POLYCAB-22OCT26-8000-PE",
        )
        self.resolver.load_option(
            symbol="POLYCAB",
            security_id="54231",
            strike=8000.0,
            option_type="PE",
            expiry=date(2026, 10, 29),
            lot_size=100,
            trading_symbol="POLYCAB-29OCT26-8000-PE",
        )
        self.resolver.load_option(
            symbol="INFY",
            security_id="54232",
            strike=1500.0,
            option_type="CE",
            expiry=date(2026, 10, 29),
            lot_size=300,
            trading_symbol="INFY-29OCT26-1500-CE",
        )

    def test_equity_resolution(self):
        inst, msg = self.resolver.resolve_equity("SBIN")
        self.assertIsNotNone(inst)
        self.assertEqual(inst.security_id, "3045")
        self.assertEqual(inst.exchange_segment, "NSE_EQ")

    def test_index_equity_rejected(self):
        inst, msg = self.resolver.resolve_equity("NIFTY")
        self.assertIsNone(inst)
        self.assertIn("Index trade rejected", msg)

    def test_13_b_exact_expiry_resolution(self):
        """13-B: Exact expiry date matching takes precedence."""
        inst, msg = self.resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            exact_expiry=date(2026, 10, 22),
            reference_date=date(2026, 10, 1),
        )
        self.assertIsNotNone(inst)
        self.assertEqual(inst.security_id, "54230")
        self.assertEqual(inst.expiry, date(2026, 10, 22))

    def test_13_b_multi_expiry_deterministic_monthly_selection(self):
        """13-B: Month-only callout deterministically picks monthly expiry (furthest in month)."""
        inst, msg = self.resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            expiry_month="OCT",
            reference_date=date(2026, 10, 1),
        )
        self.assertIsNotNone(inst)
        # Picks 2026-10-29 (monthly) over 2026-10-22
        self.assertEqual(inst.security_id, "54231")
        self.assertEqual(inst.expiry, date(2026, 10, 29))

    def test_13_e_strict_index_rejection(self):
        """13-E: Rejects all index callouts pre-broker."""
        for idx in ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"]:
            inst, msg = self.resolver.resolve_option(symbol=idx, strike=25000.0, option_type="CE")
            self.assertIsNone(inst)
            self.assertIn("Index options rejected", msg)

    def test_expired_option_rejection(self):
        # Reference date after expiry date (2026-10-30 > 2026-10-29) in same month
        inst, msg = self.resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            expiry_month="OCT",
            reference_date=date(2026, 10, 30),
        )
        self.assertIsNone(inst)
        self.assertIn("No option contract found", msg)


if __name__ == "__main__":
    unittest.main()
