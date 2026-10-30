"""
Unit tests for STEP 04: Durable Emergency STOP and Mutation Gate.
Tests crash/restart persistence, fail-closed corrupt & missing file handling,
Dhan adapter mutation boundary blocking, and risk-reducing exit exemptions.
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.risk_manager import RiskManager
from src.broker.dhan_client import DhanClient
from src.models import OrderRequest, ProtectionLevels, ProductType, TradingMode


class TestEmergencyStop(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.stop_file = os.path.join(self.temp_dir.name, "emergency_stop.json")
        self.manager = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_04_a_missing_file_fails_closed(self):
        """04-A / H-04: Clean start with no file must FAIL CLOSED (is_stopped=True) until provisioned."""
        self.assertTrue(self.manager.is_stopped())
        state = self.manager.get_state()
        self.assertTrue(state["is_stopped"])
        self.assertEqual(state["stopped_by"], "SYSTEM_FAIL_CLOSED")

        # Explicit authorized provisioning opens the gate
        self.manager.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        self.assertFalse(self.manager.is_stopped())
        st2 = self.manager.get_state()
        self.assertFalse(st2["is_stopped"])
        self.assertEqual(st2["resumed_by"], "Udit")

    def test_04_a_crash_restart_persistence(self):
        """Emergency STOP survives process restart (new instance on same file)."""
        self.manager.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        self.manager.trigger_stop(operator="Udit", reason="Market crash alert", account_id="1113731536")
        self.assertTrue(self.manager.is_stopped())

        # Simulate restart: create new manager pointing to the same file
        restarted_manager = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        self.assertTrue(restarted_manager.is_stopped())
        st = restarted_manager.get_state()
        self.assertEqual(st["stopped_by"], "Udit")
        self.assertEqual(st["reason"], "Market crash alert")
        self.assertEqual(st["account_id"], "1113731536")

    def test_04_a_fail_closed_on_corrupt_file(self):
        """FAIL-CLOSED: Corrupted state file blocks mutations (is_stopped=True)."""
        with open(self.stop_file, "w") as f:
            f.write("{corrupt_json_trash!!")

        corrupt_manager = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        self.assertTrue(corrupt_manager.is_stopped())
        st = corrupt_manager.get_state()
        self.assertTrue(st["is_stopped"])
        self.assertEqual(st["stopped_by"], "SYSTEM_FAIL_CLOSED")

    def test_04_b_dhan_client_blocks_mutation_when_stopped(self):
        """DhanClient adapter boundary rejects place_order when STOP is active."""
        self.manager.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        cfg = AppConfig.from_env()
        cfg.trading_mode = TradingMode.LIVE  # Test live path gate
        client = DhanClient(cfg, stop_manager=self.manager)

        self.manager.trigger_stop(operator="Udit", reason="Manual circuit breaker")

        order = OrderRequest(
            correlation_id="TEST-CORR-1",
            symbol="TATAMOTORS",
            security_id="3456",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=800.0,
            quantity=10,
            trading_mode=TradingMode.LIVE,
        )

        ok, order_id, msg = client.place_order(order)
        self.assertFalse(ok)
        self.assertIsNone(order_id)
        self.assertIn("MUTATION BLOCKED: Durable EMERGENCY STOP active", msg)

    def test_04_c_explicit_resume_required(self):
        """STOP requires explicit resume; restart does NOT auto-resume."""
        self.manager.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        self.manager.trigger_stop(operator="Udit", reason="Test stop")
        self.assertTrue(self.manager.is_stopped())

        # Restart
        m2 = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        self.assertTrue(m2.is_stopped())

        # Explicit resume
        m2.resume(operator="Udit", reason="Market stabilized")
        self.assertFalse(m2.is_stopped())

        # Restart again to verify resume persisted
        m3 = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        self.assertFalse(m3.is_stopped())

    def test_04_d_in_flight_tracking(self):
        """Tracks in-flight requests and clears them upon completion."""
        self.assertEqual(self.manager.get_in_flight(), [])
        self.manager.register_in_flight("CORR-999")
        self.assertIn("CORR-999", self.manager.get_in_flight())

        self.manager.deregister_in_flight("CORR-999")
        self.assertEqual(self.manager.get_in_flight(), [])

    def test_04_f_risk_reducing_exemptions_permitted(self):
        """
        DEC-01 / 04-F / H-05: Cancellations and protective OCO orders for existing positions
        remain permitted during STOP so positions are not left unhedged.
        """
        self.manager.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        cfg = AppConfig.from_env()
        client = DhanClient(cfg, stop_manager=self.manager)
        self.manager.trigger_stop(operator="Udit", reason="Halt")

        # 1. Cancel resting order: permitted
        cancel_ok, cancel_msg = client.cancel_order("RESTING-123")
        self.assertTrue(cancel_ok)

        # 2. Protective Forever OCO placement: not blocked by emergency stop when position exists
        levels = ProtectionLevels(
            executed_price=100.0,
            sl_trigger=97.0,
            sl_limit=96.0,
            target_price=103.0,
            quantity=50,
        )
        # In TEST mode, place_forever_oco returns simulated block, but NOT emergency stop rejection
        oco_ok, oco_id, oco_msg = client.place_forever_oco(
            correlation_id="OCO-1",
            security_id="1234",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=levels,
        )
        self.assertNotIn("EMERGENCY STOP", oco_msg)


if __name__ == "__main__":
    unittest.main()
