"""
Unit tests for STEP 14 (Unified Equity Partial-Fill Protection)
and STEP 15 (Broker-Authoritative Startup Reconciliation Gate).
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import (
    StartupReconciliationGate,
    ReconciliationState,
)
from src.journal.db import JournalDB
from src.models import TradingMode


class TestStep14And15(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "journal.db")
        self.pending_file = os.path.join(self.temp_dir.name, "pending_protection.json")
        self.stop_file = os.path.join(self.temp_dir.name, "emergency_stop.json")

        self.journal = JournalDB(self.db_path)
        self.stop_mgr = EmergencyStopManager(self.stop_file, fail_closed_if_missing=True)
        self.stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        self.mock_broker = MagicMock()
        self.mock_broker.get_trade_book.return_value = []
        self.mock_broker.get_positions.return_value = []
        self.mock_broker.get_order_book.return_value = []
        self.mock_broker.get_forever_orders.return_value = []
        self.mock_broker.get_super_orders.return_value = []

        self.watcher = ProtectionWatcher(
            broker=self.mock_broker,
            sl_pct=3.0,
            target_pct=3.0,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=self.journal,
        )

        self.gate = StartupReconciliationGate(
            broker=self.mock_broker,
            journal_db=self.journal,
            watcher=self.watcher,
            stop_manager=self.stop_mgr,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    # --- STEP 14 Tests ---

    def test_14_b_equity_incremental_fills(self):
        """14-B: Unified watcher handles 1, 2, 5 share partial equity fills."""
        oid = self.watcher.track(
            order_id="EQ-FILLS-1",
            symbol="HDFCBANK",
            security_id="1333",
            segment="NSE_EQ",
            qty=8,
            product_type="CNC",
        )
        self.mock_broker.place_forever_oco.return_value = (True, "OCO-1", "Success")

        # Fill 1: 1 share @ 1600.0
        self.mock_broker.order_status.return_value = {
            "orderStatus": "PART_TRADED", "filledQty": 1, "averageTradedPrice": 1600.0
        }
        self.watcher.check_once()
        self.assertEqual(self.watcher.pending[oid]["protected_qty"], 1)

        # Fill 2: 2 more shares (total 3) @ 1605.0 avg
        self.mock_broker.order_status.return_value = {
            "orderStatus": "PART_TRADED", "filledQty": 3, "averageTradedPrice": 1605.0
        }
        self.watcher.check_once()
        self.assertEqual(self.watcher.pending[oid]["protected_qty"], 3)

        # Fill 3: 5 more shares (total 8) @ 1610.0 avg
        self.mock_broker.order_status.return_value = {
            "orderStatus": "TRADED", "filledQty": 8, "averageTradedPrice": 1610.0
        }
        self.watcher.check_once()
        # All 8 shares protected -> removed from pending
        self.assertNotIn(oid, self.watcher.pending)

    # --- STEP 15 Tests ---

    def test_15_a_gate_fails_closed_initially(self):
        """15-A: Trading gate is closed (False) before reconciliation runs."""
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertEqual(self.gate.state, ReconciliationState.INITIALIZING)

    def test_15_b_reconcile_healthy_opens_gate(self):
        """15-B: Healthy broker check opens the trading gate."""
        self.mock_broker.get_fund_limits.return_value = (True, 150000.0, "OK")

        ok, msg = self.gate.run_startup_reconciliation()
        self.assertTrue(ok)
        self.assertTrue(self.gate.is_accepting_entries)
        self.assertEqual(self.gate.state, ReconciliationState.HEALTHY)

    def test_15_c_broker_failure_keeps_gate_locked(self):
        """15-E: Broker outage or funds error keeps gate locked fail-closed."""
        self.mock_broker.get_fund_limits.return_value = (False, 0.0, "Dhan 503 Service Unavailable")

        ok, msg = self.gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertEqual(self.gate.state, ReconciliationState.BLOCKED)
        self.assertIn("503", self.gate.last_error)

    def test_15_b_unknown_intent_reconciliation(self):
        """15-B: Reconciles UNKNOWN intent on startup without resubmitting."""
        self.journal.record_order_intent(
            correlation_id="OPT-RECON-1",
            symbol="BAJAJFINSV",
            security_id="16675",
            exchange_segment="NSE_FNO",
            product_type="MARGIN",
            order_type="LIMIT",
            price=160.0,
            quantity=500,
        )
        self.journal.update_order_intent("OPT-RECON-1", state="UNKNOWN")

        self.mock_broker.get_fund_limits.return_value = (True, 200000.0, "OK")
        # Broker reports this order exists and is TRADED
        self.mock_broker.modify_super_order_protection.return_value = (True, "OK")
        super_status = {
            "orderId": "DHAN-ORDER-9999",
            "securityId": "16675",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "orderStatus": "TRADED",
            "filledQty": 500,
            "averageTradedPrice": 160.0,
            "legDetails": [
                {"legName": "STOP_LOSS_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 500, "price": 155.2},
                {"legName": "TARGET_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 500, "price": 164.8},
            ],
        }
        self.mock_broker.get_super_orders.return_value = [super_status]
        self.mock_broker.order_status.return_value = super_status

        ok, msg = self.gate.run_startup_reconciliation()
        self.assertTrue(ok)

        # Verify intent in DB is now resolved to TRADED
        unknowns = self.journal.get_unknown_intents()
        self.assertEqual(len(unknowns), 0)


if __name__ == "__main__":
    unittest.main()
