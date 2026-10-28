"""
Unit tests for Persistent Partial-Fill Protection Watcher.
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

from src.engine.protection_watcher import ProtectionWatcher


class TestProtectionWatcher(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.pending_file = os.path.join(self.temp_dir.name, "pending_protection.json")
        self.mock_broker = MagicMock()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_track_and_persist(self):
        watcher = ProtectionWatcher(
            broker=self.mock_broker,
            sl_pct=3.0,
            target_pct=3.0,
            path=self.pending_file,
            dry_run=True,
            clock=lambda: 1000.0,
        )

        oid = watcher.track(
            order_id="TEST-ORD-1",
            symbol="RELIANCE",
            security_id="2885",
            segment="NSE_EQ",
            qty=50,
            product_type="CNC",
            dry_price=2500.0,
        )

        self.assertTrue(os.path.exists(self.pending_file))
        with open(self.pending_file, "r") as f:
            data = json.load(f)
        self.assertIn(oid, data)
        self.assertEqual(data[oid]["qty"], 50)
        self.assertEqual(data[oid]["symbol"], "RELIANCE")

        # Resume with new instance
        watcher2 = ProtectionWatcher(
            broker=self.mock_broker,
            path=self.pending_file,
            dry_run=True,
        )
        self.assertIn(oid, watcher2.pending)
        self.assertEqual(watcher2.pending[oid]["qty"], 50)

    def test_partial_fill_protection_chunk_by_chunk(self):
        watcher = ProtectionWatcher(
            broker=self.mock_broker,
            sl_pct=3.0,
            target_pct=5.0,
            sl_limit_buffer_pct=1.0,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
        )

        watcher.track(
            order_id="ORD-101",
            symbol="TCS",
            security_id="11536",
            segment="NSE_EQ",
            qty=100,
            product_type="CNC",
        )

        # 1. First partial fill: 40 shares executed @ 3000.0
        self.mock_broker.order_status.return_value = {
            "orderStatus": "PART_TRADED",
            "filledQty": 40,
            "averageTradedPrice": 3000.0,
        }
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-40", "Success")

        changed = watcher.check_once()
        self.assertTrue(changed)
        rec = watcher.pending["ORD-101"]
        self.assertEqual(rec["protected_qty"], 40)
        self.assertEqual(rec["protected_value"], 120000.0)
        self.mock_broker.place_forever_oco.assert_called_once()

        # Verify arguments passed to place_forever_oco
        call_kwargs = self.mock_broker.place_forever_oco.call_args.kwargs
        levels = call_kwargs["levels"]
        self.assertEqual(levels.quantity, 40)
        self.assertEqual(levels.executed_price, 3000.0)
        self.assertEqual(levels.sl_trigger, 2910.0)  # -3%
        self.assertEqual(levels.target_price, 3150.0)  # +5%

        # 2. Second fill completing the order: 100 shares executed total @ 3010.0 average
        # Total value = 100 * 3010 = 301,000. Protected value = 120,000.
        # Remaining 60 shares executed at (301,000 - 120,000) / 60 = 181,000 / 60 = 3016.6667
        self.mock_broker.place_forever_oco.reset_mock()
        self.mock_broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 100,
            "averageTradedPrice": 3010.0,
        }
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-60", "Success")

        changed2 = watcher.check_once()
        self.assertTrue(changed2)
        # Order reached terminal status TRADED and all 100 shares protected -> popped from pending
        self.assertNotIn("ORD-101", watcher.pending)
        self.mock_broker.place_forever_oco.assert_called_once()

        call_kwargs2 = self.mock_broker.place_forever_oco.call_args.kwargs
        levels2 = call_kwargs2["levels"]
        self.assertEqual(levels2.quantity, 60)
        self.assertAlmostEqual(levels2.executed_price, 3016.6667, places=3)

    def test_cancelled_order_dropped_without_protection(self):
        watcher = ProtectionWatcher(
            broker=self.mock_broker,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
        )

        watcher.track(
            order_id="ORD-CANCEL",
            symbol="INFY",
            security_id="1594",
            segment="NSE_EQ",
            qty=20,
        )

        self.mock_broker.order_status.return_value = {
            "orderStatus": "CANCELLED",
            "filledQty": 0,
        }

        watcher.check_once()
        # Order cancelled with 0 fills -> dropped without placing OCO
        self.assertNotIn("ORD-CANCEL", watcher.pending)
        self.mock_broker.place_forever_oco.assert_not_called()


if __name__ == "__main__":
    unittest.main()
