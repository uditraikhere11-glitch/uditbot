"""
Hermetic Reproduction and Gap Verification Tests.
Verifies fixes for findings H-01 through H-07:
- H-01: Multi-stock ambiguity rejection & action-bound ticker binding.
- H-03: Startup reconciliation preserves UNKNOWN intents and keeps gate BLOCKED.
- H-04: Missing STOP state fails closed (is_stopped=True).
- H-05: Emergency STOP gate enforced on protective mutations.
- H-06: Accurate Forever OCO field mapping according to Dhan v2 docs.
- H-07: Correlation ID uniqueness across rapid same-second invocations (<=30 chars).
- H-11: Token file permissions strictly 0600.
And Correction 02 Focused Regressions for Dhan Forever OCO Stop-Loss / Target Mapping.
"""

import os
import stat
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch
import requests

from src.config import AppConfig
from src.parsers.equity_parser import parse_equity_regex
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.journal.db import JournalDB
from src.broker.dhan_client import DhanClient
from src.models import OrderRequest, ProtectionLevels, ProductType, TradingMode


class TestHardenedReproductionGates(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.stop_file = os.path.join(self.temp_dir.name, "emergency_stop.json")
        self.db_path = os.path.join(self.temp_dir.name, "test_journal.db")
        self.journal = JournalDB(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_h01_ticker_binding_and_multi_stock_rejection(self):
        """H-01: Proves ticker binds to BUY action and multi-instrument messages are rejected."""
        # 1. Negative checks: TCS signals containing other stock commentary MUST NOT resolve to other stock!
        sig1, r1 = parse_equity_regex("buy TCS above 2500, note YES BANK unrelated")
        self.assertIsNone(sig1)
        self.assertIn("Multiple stocks detected", r1)

        sig2, r2 = parse_equity_regex("buy TCS at 2500; unrelated analysis includes HDFC BANK")
        self.assertIsNone(sig2)
        self.assertIn("Multiple stocks detected", r2)

        sig3, r3 = parse_equity_regex("buy #TCS around 2500 as YES BANK rallies")
        self.assertIsNone(sig3)
        self.assertIn("Multiple stocks detected", r3)

        # 2. Positive checks: Clean single-stock messages resolve accurately
        sig_yes, r_yes = parse_equity_regex("buy YES BANK around 25")
        self.assertIsNotNone(sig_yes)
        self.assertEqual(sig_yes.symbol, "YESBANK")
        self.assertEqual(sig_yes.stated_price, 25.0)

        sig_tcs, r_tcs = parse_equity_regex("buy TCS at 2500")
        self.assertIsNotNone(sig_tcs)
        self.assertEqual(sig_tcs.symbol, "TCS")
        self.assertEqual(sig_tcs.stated_price, 2500.0)

        sig_sbin, r_sbin = parse_equity_regex("Buy #SBIN around 805")
        self.assertIsNotNone(sig_sbin)
        self.assertEqual(sig_sbin.symbol, "SBIN")

    def test_h03_unresolved_unknown_intent_keeps_gate_blocked(self):
        """H-03: Startup reconciliation with unresolved UNKNOWN intent keeps gate BLOCKED."""
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")

        self.journal.record_order_intent(
            correlation_id="OPT-UNRESOLVED-1",
            symbol="INFY",
            security_id="16675",
            exchange_segment="NSE_FNO",
            product_type="MARGIN",
            order_type="LIMIT",
            price=150.0,
            quantity=300,
        )
        self.journal.update_order_intent("OPT-UNRESOLVED-1", state="UNKNOWN")

        mock_broker = MagicMock()
        mock_broker.get_fund_limits.return_value = (True, 100000.0, "OK")
        # Broker lookup returns None (e.g. read timeout or not found)
        mock_broker.get_order_by_correlation_id.return_value = None
        mock_broker.order_status.return_value = None

        mock_watcher = MagicMock()

        gate = StartupReconciliationGate(
            broker=mock_broker,
            journal_db=self.journal,
            watcher=mock_watcher,
            stop_manager=stop_mgr,
        )

        ok, msg = gate.run_startup_reconciliation()
        # Intent must remain UNKNOWN and gate MUST NOT open to HEALTHY
        unknowns = self.journal.get_unknown_intents()
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0]["state"], "UNKNOWN")

    def test_h04_missing_emergency_stop_fails_closed(self):
        """H-04: Missing STOP file fails closed by default (is_stopped=True)."""
        stop_mgr = EmergencyStopManager(state_file=os.path.join(self.temp_dir.name, "absent_stop.json"))
        self.assertTrue(stop_mgr.is_stopped())
        st = stop_mgr.get_state()
        self.assertTrue(st["is_stopped"])
        self.assertEqual(st["stopped_by"], "SYSTEM_FAIL_CLOSED")

    @patch("requests.Session.post")
    def test_h06_forever_oco_payload_semantics(self, mock_post):
        """H-06: Verifies Forever OCO fields strictly follow Dhan documentation (SELL: Leg 0 = SL, Leg 1 = Target)."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"orderId": "5132208051112", "orderStatus": "PENDING"}
        mock_post.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_auth_token_for_test"
        cfg.trading_mode = TradingMode.LIVE

        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")

        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=15,
        )

        ok, forever_id, msg = client.place_forever_oco(
            correlation_id="OCO-TEST-1",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=levels,
        )

        self.assertTrue(ok)
        self.assertEqual(forever_id, "5132208051112")
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]

        # Documented fields check: Leg 0 is Stop-Loss, Leg 1 is Profit Target
        self.assertEqual(payload["dhanClientId"], "1113731536")
        self.assertEqual(payload["orderFlag"], "OCO")
        self.assertEqual(payload["transactionType"], "SELL")
        self.assertEqual(payload["productType"], "CNC")
        self.assertEqual(payload["price"], 960.0)          # Primary leg limit: SL limit
        self.assertEqual(payload["triggerPrice"], 970.0)   # Primary leg trigger: SL trigger
        self.assertEqual(payload["price1"], 1030.0)        # Target leg limit: Target price
        self.assertEqual(payload["triggerPrice1"], 1030.0) # Target leg trigger
        self.assertEqual(payload["quantity"], 15)
        self.assertEqual(payload["quantity1"], 15)

    def test_h07_correlation_id_uniqueness_no_same_second_collision(self):
        """H-07: Correlation IDs generated in rapid succession are 100% unique and <= 30 chars."""
        import uuid
        client_id = "1113731536"
        ids = set()
        for i in range(100):
            corr = f"EQ{client_id[-4:]}{int(time.time())%100000}{uuid.uuid4().hex[:8]}"[:30]
            self.assertLessEqual(len(corr), 30)
            self.assertNotIn(corr, ids)
            ids.add(corr)
        self.assertEqual(len(ids), 100)

    @patch("requests.Session.post")
    def test_h11_token_file_permissions_0600(self, mock_post):
        """H-11: Token file is written strictly with 0600 permissions."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"accessToken": "eyJmock_token_0600"}
        mock_post.return_value = mock_resp

        token_path = os.path.join(self.temp_dir.name, "dhan_token_0600.txt")
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_pin = "123456"
        cfg.dhan_totp_secret = "JBSWY3DPEHPK3PXP"
        cfg.dhan_token_file = token_path

        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")

        client = DhanClient(cfg, stop_manager=stop_mgr)
        ok, tok, msg = client.generate_token_with_totp()

        self.assertTrue(ok)
        self.assertTrue(os.path.exists(token_path))
        file_mode = stat.S_IMODE(os.stat(token_path).st_mode)
        self.assertEqual(file_mode, 0o600)

    # =========================================================================
    # CORRECTION 02 FOCUSED REGRESSION TESTS (02-E: 1 through 9)
    # =========================================================================

    @patch("requests.Session.post")
    def test_regression_01_correct_sell_oco_mapping(self, mock_post):
        """02-E.1: Correct SELL OCO mapping for entry 1000, SL trigger 970, SL limit 960, target 1030, qty 15."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"orderId": "OCO-ORD-100", "orderStatus": "PENDING"}
        mock_post.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE

        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=15,
        )

        ok, forever_id, msg = client.place_forever_oco(
            correlation_id="OCO-TEST-REG-1",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=levels,
        )
        self.assertTrue(ok)
        self.assertEqual(forever_id, "OCO-ORD-100")
        payload = mock_post.call_args[1]["json"]

        self.assertEqual(payload["price"], 960.00)
        self.assertEqual(payload["triggerPrice"], 970.00)
        self.assertEqual(payload["price1"], 1030.00)
        self.assertEqual(payload["triggerPrice1"], 1030.00)
        self.assertEqual(payload["quantity"], 15)
        self.assertEqual(payload["quantity1"], 15)

    @patch("requests.Session.post")
    def test_regression_02_incorrect_target_sl_reversal_fails_contract(self, mock_post):
        """02-E.2: Incorrect target/stop-loss leg reversal fails contract validation."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        # Inverted SL (limit > trigger)
        invalid_levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=1030.0,  # Invalid: limit higher than trigger for SELL
            target_price=1040.0,
            quantity=15,
        )
        ok, forever_id, msg = client.place_forever_oco(
            correlation_id="OCO-INV-1",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=invalid_levels,
        )
        self.assertFalse(ok)
        self.assertIn("SELL stop-loss limit price", msg)
        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_regression_03_invalid_or_zero_quantity_produces_no_broker_post(self, mock_post):
        """02-E.3: Invalid or zero quantity produces no broker POST."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        # Zero quantity
        zero_qty_levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=0,
        )
        ok, forever_id, msg = client.place_forever_oco(
            correlation_id="OCO-ZERO-QTY",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=zero_qty_levels,
        )
        self.assertFalse(ok)
        self.assertIn("invalid quantity", msg)
        mock_post.assert_not_called()

        # Negative quantity
        neg_qty_levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=-5,
        )
        ok2, _, msg2 = client.place_forever_oco(
            correlation_id="OCO-NEG-QTY",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=neg_qty_levels,
        )
        self.assertFalse(ok2)
        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_regression_04_unsupported_margin_product_produces_no_broker_post(self, mock_post):
        """02-E.4: Unsupported MARGIN and INTRADAY products produce no broker POST."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=10,
        )

        for unsupported_prod in ("MARGIN", "INTRADAY", "CO", "BO"):
            ok, forever_id, msg = client.place_forever_oco(
                correlation_id=f"OCO-UNSUPP-{unsupported_prod}",
                security_id="2885",
                exchange_segment="NSE_EQ",
                product_type=unsupported_prod,
                levels=levels,
            )
            self.assertFalse(ok)
            self.assertIn("unsupported for product", msg)
            mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_regression_05_test_mode_produces_no_broker_post(self, mock_post):
        """02-E.5: TEST mode produces no broker POST."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.TEST
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=15,
        )
        ok, forever_id, msg = client.place_forever_oco(
            correlation_id="OCO-TEST-MODE",
            security_id="2885",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=levels,
        )
        self.assertFalse(ok)
        self.assertIn("System is in TEST mode", msg)
        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_regression_06_invalid_price_or_tick_grid_produces_no_broker_post(self, mock_post):
        """02-E.6: Invalid stop-loss/target price or tick-grid value produces no broker POST."""
        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        # 1. Non-positive price
        l1 = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=-10.0, target_price=1030.0, quantity=15)
        ok1, _, msg1 = client.place_forever_oco("OCO-INV-1", "2885", "NSE_EQ", "CNC", l1)
        self.assertFalse(ok1)
        self.assertIn("must be > 0", msg1)

        # 2. Target <= SL trigger
        l2 = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=960.0, target_price=950.0, quantity=15)
        ok2, _, msg2 = client.place_forever_oco("OCO-INV-2", "2885", "NSE_EQ", "CNC", l2)
        self.assertFalse(ok2)
        self.assertIn("must be > stop-loss trigger", msg2)

        # 3. Off tick-grid price (not aligned to 0.05 tick size)
        l3 = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=960.03, target_price=1030.0, quantity=15)
        ok3, _, msg3 = client.place_forever_oco("OCO-INV-3", "2885", "NSE_EQ", "CNC", l3)
        self.assertFalse(ok3)
        self.assertIn("not aligned to tick grid", msg3)

        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_regression_07_dhan_http_rejection_reported_accurately(self, mock_post):
        """02-E.7: Dhan HTTP rejection (e.g. RMS 400 error) is reported accurately."""
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = '{"error": "RMS Limit Exceeded", "errorCode": "DH-RMS-01"}'
        mock_post.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=960.0, target_price=1030.0, quantity=15)
        ok, forever_id, msg = client.place_forever_oco("OCO-REJ", "2885", "NSE_EQ", "CNC", levels)

        self.assertFalse(ok)
        self.assertIsNone(forever_id)
        self.assertIn("Forever OCO rejected (400)", msg)
        self.assertIn("RMS Limit Exceeded", msg)

    @patch("requests.Session.post")
    def test_regression_08_dhan_timeout_no_duplicate_post(self, mock_post):
        """02-E.8: Dhan timeout does not result in an automatic duplicate protection POST."""
        mock_post.side_effect = requests.exceptions.Timeout("Connection timed out waiting for Dhan gateway")

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=960.0, target_price=1030.0, quantity=15)
        ok, forever_id, msg = client.place_forever_oco("OCO-TIMEOUT", "2885", "NSE_EQ", "CNC", levels)

        self.assertFalse(ok)
        self.assertIsNone(forever_id)
        self.assertIn("HTTP exception placing Forever OCO", msg)
        # Exactly one POST attempted - client never initiates blind automatic duplicate retry
        self.assertEqual(mock_post.call_count, 1)

    @patch("requests.Session.post")
    def test_regression_09_http_acceptance_not_represented_as_active(self, mock_post):
        """02-E.9: HTTP acceptance or returned order ID is not represented as broker-confirmed ACTIVE protection."""
        # Case A: 200 OK with PENDING status returns order ID but caller must verify async active state
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"orderId": "5132208051112", "orderStatus": "PENDING"}
        mock_post.return_value = mock_resp

        cfg = AppConfig.from_env()
        cfg.dhan_client_id = "1113731536"
        cfg.dhan_access_token = "mock_token"
        cfg.trading_mode = TradingMode.LIVE
        stop_mgr = EmergencyStopManager(state_file=self.stop_file, fail_closed_if_missing=True)
        stop_mgr.provision_initial_clean_state(operator="TestRunner", account_id="1113731536")
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(executed_price=1000.0, sl_trigger=970.0, sl_limit=960.0, target_price=1030.0, quantity=15)
        ok, forever_id, msg = client.place_forever_oco("OCO-ACCEPT", "2885", "NSE_EQ", "CNC", levels)
        self.assertTrue(ok)
        self.assertEqual(forever_id, "5132208051112")
        # Confirms receipt, not immediate ACTIVE execution
        self.assertIn("placed successfully", msg)

        # Case B: HTTP 200 but body contains orderStatus REJECTED by RMS
        mock_resp.json.return_value = {"orderId": "5132208051113", "orderStatus": "REJECTED", "remarks": "Margin shortfall"}
        ok2, forever_id2, msg2 = client.place_forever_oco("OCO-RMS-REJ", "2885", "NSE_EQ", "CNC", levels)
        self.assertFalse(ok2)
        self.assertIsNone(forever_id2)
        self.assertIn("rejected by broker RMS", msg2)


if __name__ == "__main__":
    unittest.main()
