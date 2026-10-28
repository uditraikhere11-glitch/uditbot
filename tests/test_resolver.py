"""
Unit tests for Scrip Master Instrument Resolver.
"""

from datetime import date
import unittest
from src.instruments.resolver import InstrumentResolver


class TestInstrumentResolver(unittest.TestCase):
    def setUp(self):
        self.resolver = InstrumentResolver()
        # Load sample cash equities
        self.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ")
        self.resolver.load_equity("TATAMOTORS", security_id="3456", exchange_segment="NSE_EQ")
        self.resolver.load_equity("NIFTY", security_id="999", exchange_segment="NSE_EQ")  # Index should be ignored

        # Load sample stock options
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

    def test_option_resolution_exact_and_default_month(self):
        # Explicit month OCT
        inst, msg = self.resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            expiry_month="OCT",
            reference_date=date(2026, 10, 1),
        )
        self.assertIsNotNone(inst)
        self.assertEqual(inst.security_id, "54231")
        self.assertEqual(inst.lot_size, 100)

        # Omitted month (defaults to current month: OCT 2026)
        inst2, msg2 = self.resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            expiry_month=None,
            reference_date=date(2026, 10, 1),
        )
        self.assertIsNotNone(inst2)
        self.assertEqual(inst2.security_id, "54231")

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
        self.assertIn("Contract expired", msg)


if __name__ == "__main__":
    unittest.main()
