"""
Regression Suite for Auditbot V13 Confirmed Corrections CV13-01 through CV13-05.
Covers all required acceptance tests:
- CV13-01 (High): Recover an unresolved Super protection attempt
- CV13-02 (High): Retry only cancellations eligible under the approved policy
- CV13-03 (High): Continue cancellation recovery after a failed timeout request
- CV13-04 (Medium): Revalidate retry eligibility immediately before submission
- CV13-05 (Medium): Reject incomplete position records at startup
"""

from datetime import date
import os
import tempfile
import unittest
from unittest.mock import MagicMock, Mock

from src.config import AppConfig
from src.models import QuoteResult, TradingMode
from src.journal.db import JournalDB
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher, OrderLifecycleState
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.broker.dhan_client import DhanClient


class TestCV13ConfirmedCorrections(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            data_dir=self.tmp.name,
            journal_db_path=os.path.join(self.tmp.name, "journal.db"),
        )
        self.db = JournalDB(self.cfg.journal_db_path, account_id="1113731536")
        self.stop = EmergencyStopManager(
            os.path.join(self.tmp.name, "emergency_stop.json"),
            expected_account_id="1113731536",
        )
        self.stop.provision_initial_clean_state("Operator", "1113731536")
        self.pending_path = os.path.join(self.tmp.name, "pending_protection.json")

    def _make_watcher(self, broker, clock_fn=None, **kwargs):
        return ProtectionWatcher(
            broker=broker,
            sl_pct=3.0,
            target_pct=3.0,
            sl_limit_buffer_pct=1.0,
            path=self.pending_path,
            dry_run=False,
            clock=clock_fn or (lambda: 1000.0),
            journal_db=self.db,
            require_broker_leg_confirmation=True,
            **kwargs,
        )

    # =========================================================================
    # CV13-01 (High): Recover an unresolved Super protection attempt
    # =========================================================================

    def test_cv13_01_a_first_readback_unverified_second_valid_confirms_protection(self):
        """
        CV13-01 Acceptance 1:
        First readback unverified; second valid (even without advancing clock by 60s):
        protection becomes CONFIRMED without repeating modification or hitting UNIQUE constraint.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        watcher.track(
            "SUP-CV01-A",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=100,
            product_type="MARGIN",
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
        )

        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "Super legs modified"))
        client.get_super_orders = Mock(return_value=[])

        # Pass 1: modification succeeds, readback 1 is unverified
        watcher.check_once()
        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertEqual(watcher.pending["SUP-CV01-A"]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries)
        unresolved = self.db.get_unresolved_protection_attempts("SUP-CV01-A")
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]["state"], "ACKED_UNVERIFIED")
        self.assertEqual(unresolved[0]["route"], "SUPER_ORDER")

        # Pass 2 (only 2 seconds later, well inside 60s backoff!): readback 2 is valid
        now_val[0] = 1002.0
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-CV01-A",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 100,
                    "averageTradedPrice": 100.0,
                    "legDetails": [
                        {
                            "legName": "STOP_LOSS_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 97.0,
                        },
                        {
                            "legName": "TARGET_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 103.0,
                        },
                    ],
                }
            ]
        )
        watcher.check_once()

        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertIn("SUP-CV01-A", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-CV01-A"]["protected_qty"], 100)
        attempts = self.db.get_protection_attempts("SUP-CV01-A")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["state"], "CONFIRMED")

    def test_cv13_01_b_same_sequence_succeeds_after_restart(self):
        """
        CV13-01 Acceptance 2:
        Unresolved SUPER_ORDER attempt is restored during startup (including expected quantity,
        prices, broker identity, and state) and reconciled to CONFIRMED on the next pass.
        """
        client = DhanClient(self.cfg, self.stop)
        watcher1 = self._make_watcher(client, clock_fn=lambda: 1000.0)
        watcher1.track(
            "SUP-CV01-B",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=100,
            product_type="MARGIN",
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
        )

        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "Super legs modified"))
        client.get_super_orders = Mock(return_value=[])

        watcher1.check_once()
        self.assertEqual(client.modify_super_order_protection.call_count, 1)

        # Simulate restart into a brand-new watcher instance
        watcher2 = self._make_watcher(client, clock_fn=lambda: 1005.0)
        restored_att = watcher2.pending["SUP-CV01-B"].get("pending_oco")
        self.assertIsInstance(restored_att, dict)
        self.assertEqual(restored_att["route"], "SUPER_ORDER")
        self.assertEqual(restored_att["qty"], 100)
        self.assertEqual(restored_att["sl_trigger"], 97.0)
        self.assertEqual(restored_att["target_price"], 103.0)
        self.assertEqual(restored_att["oco_id"], "SUP-CV01-B")
        self.assertEqual(restored_att["state"], "ACKED_UNVERIFIED")

        # Broker readback now confirms active legs
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-CV01-B",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 100,
                    "averageTradedPrice": 100.0,
                    "legDetails": [
                        {
                            "legName": "STOP_LOSS_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 97.0,
                        },
                        {
                            "legName": "TARGET_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 103.0,
                        },
                    ],
                }
            ]
        )
        watcher2.check_once()

        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertIn("SUP-CV01-B", watcher2.terminal_orders)
        self.assertEqual(watcher2.terminal_orders["SUP-CV01-B"]["protected_qty"], 100)
        attempts = self.db.get_protection_attempts("SUP-CV01-B")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["state"], "CONFIRMED")

    def test_cv13_01_c_modification_timeout_followed_by_valid_readback_recovers_without_blind_repetition(self):
        """
        CV13-01 Acceptance 3:
        Modification timeout leaves attempt in UNKNOWN state, blocks fresh entries, does not
        blindly repeat modification while uncertain, and recovers to CONFIRMED once readback validates.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        watcher.track(
            "SUP-CV01-C",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=100,
            product_type="MARGIN",
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
        )

        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(
            return_value=(False, "HTTP exception modifying super order SUP-CV01-C: Read timed out")
        )
        client.get_super_orders = Mock(return_value=[])

        # Pass 1: modification times out -> state=UNKNOWN
        watcher.check_once()
        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertFalse(gate.is_accepting_entries)
        unresolved = self.db.get_unresolved_protection_attempts("SUP-CV01-C")
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]["state"], "UNKNOWN")

        # Pass 2: readback still unavailable -> retains UNKNOWN, zero blind repetition
        now_val[0] = 1002.0
        watcher.check_once()
        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertEqual(self.db.get_unresolved_protection_attempts("SUP-CV01-C")[0]["state"], "UNKNOWN")

        # Pass 3: readback confirms modification took effect on broker -> recovers to CONFIRMED
        now_val[0] = 1004.0
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-CV01-C",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 100,
                    "averageTradedPrice": 100.0,
                    "legDetails": [
                        {
                            "legName": "STOP_LOSS_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 97.0,
                        },
                        {
                            "legName": "TARGET_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 103.0,
                        },
                    ],
                }
            ]
        )
        watcher.check_once()

        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertIn("SUP-CV01-C", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-CV01-C"]["protected_qty"], 100)
        attempts = self.db.get_protection_attempts("SUP-CV01-C")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["state"], "CONFIRMED")

    def test_cv13_01_d_additional_partial_fill_during_uncertainty_reconciled_without_losing_earlier_coverage(self):
        """
        CV13-01 Acceptance 4:
        Additional partial fill during uncertainty is handled separately after reconciling
        the earlier attempt:
        - Pass 1: 100 of 200 filled @ 100.0 -> Attempt 1 (SL=97.0, Target=103.0) unverified.
        - Pass 2: filled becomes 200 @ 105.0 cumulative VWAP. Readback confirms Attempt 1 (100 units @ 97/103),
          and Attempt 2 (200 units @ SL=101.85, Target=108.15) is submitted at uncovered_from_qty=100.
          Even while Attempt 2 readback is delayed on Pass 2, earlier confirmed coverage (protected_qty=100) is preserved!
        - Pass 3: Readback confirms Attempt 2 (200 units @ 101.85/108.15) -> protected_qty=200, both attempts CONFIRMED.
        """
        client = DhanClient(self.cfg, self.stop)
        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        watcher.track(
            "SUP-CV01-D",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=200,
            product_type="MARGIN",
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
        )

        # Pass 1: 100 units filled @ 100.0; modification 1 succeeds, readback delayed
        client.order_status = Mock(
            return_value={"orderStatus": "PART_TRADED", "filledQty": 100, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        mod_calls = []

        def record_mod(**kwargs):
            mod_calls.append(kwargs)
            return True, "Modified"

        client.modify_super_order_protection = Mock(side_effect=record_mod)
        client.get_super_orders = Mock(return_value=[])

        watcher.check_once()
        self.assertEqual(len(mod_calls), 1)
        self.assertEqual(mod_calls[0]["stop_loss_price"], 97.0)
        self.assertEqual(mod_calls[0]["target_price"], 103.0)
        self.assertEqual(watcher.pending["SUP-CV01-D"]["protected_qty"], 0)

        # Pass 2: cumulative fill increases to 200 @ 105.0 VWAP.
        # Broker readback confirms Attempt 1 (100 units @ 97.0 / 103.0), while Attempt 2's readback is not yet updated.
        now_val[0] = 1002.0
        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 200, "averageTradedPrice": 105.0}
        )
        client.trades_avg_price = Mock(return_value=105.0)
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-CV01-D",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 200,
                    "averageTradedPrice": 105.0,
                    "legDetails": [
                        {
                            "legName": "STOP_LOSS_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 97.0,
                        },
                        {
                            "legName": "TARGET_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 100,
                            "price": 103.0,
                        },
                    ],
                }
            ]
        )
        watcher.check_once()

        # Attempt 1 must now be CONFIRMED and protected_qty=100 preserved while Attempt 2 is ACKED_UNVERIFIED!
        self.assertEqual(len(mod_calls), 2)
        self.assertEqual(mod_calls[1]["stop_loss_price"], 101.85)
        self.assertEqual(mod_calls[1]["target_price"], 108.15)
        self.assertIn("SUP-CV01-D", watcher.pending)
        self.assertEqual(watcher.pending["SUP-CV01-D"]["protected_qty"], 100)
        attempts_p2 = self.db.get_protection_attempts("SUP-CV01-D")
        self.assertEqual(len(attempts_p2), 2)
        self.assertEqual(attempts_p2[0]["state"], "CONFIRMED")
        self.assertEqual(attempts_p2[0]["uncovered_from_qty"], 0)
        self.assertEqual(attempts_p2[1]["state"], "ACKED_UNVERIFIED")
        self.assertEqual(attempts_p2[1]["uncovered_from_qty"], 100)

        # Pass 3: Broker readback confirms Attempt 2 (200 units @ 101.85 / 108.15)
        now_val[0] = 1004.0
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-CV01-D",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 200,
                    "averageTradedPrice": 105.0,
                    "legDetails": [
                        {
                            "legName": "STOP_LOSS_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 200,
                            "price": 101.85,
                        },
                        {
                            "legName": "TARGET_LEG",
                            "transactionType": "SELL",
                            "orderStatus": "PENDING",
                            "remainingQuantity": 200,
                            "price": 108.15,
                        },
                    ],
                }
            ]
        )
        watcher.check_once()

        self.assertEqual(len(mod_calls), 2)
        self.assertIn("SUP-CV01-D", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-CV01-D"]["protected_qty"], 200)
        attempts_p3 = self.db.get_protection_attempts("SUP-CV01-D")
        self.assertEqual([a["state"] for a in attempts_p3], ["CONFIRMED", "CONFIRMED"])

    # =========================================================================
    # CV13-02 (High): Retry only cancellations eligible under the approved policy
    # =========================================================================

    def test_cv13_02_a_non_timeout_and_legacy_cancellations_submit_zero_replacement_orders(self):
        """
        CV13-02 Acceptance 1:
        Manual/operator, external/unknown, signal withdrawal, emergency stop, and legacy
        cancellations submit ZERO replacement BUY orders while still reconciling and protecting fills.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        watcher = self._make_watcher(client, clock_fn=lambda: 1000.0)
        gate.watcher = watcher

        client.cancel_order = Mock(return_value=(True, "HTTP 202 Accepted"))
        client.place_order = Mock(return_value=(True, "UNWANTED-RETRY", "Placed"))
        client.get_yahoo_quote_details = Mock(
            return_value=QuoteResult(
                is_valid=True,
                price=100.3,
                source="YAHOO_FINANCE",
                ticker="SBIN.NS",
                venue="NSE",
                security_id="3045",
                currency="INR",
                market_state="REGULAR",
                quote_timestamp=1000.0,
                age_sec=0.0,
            )
        )

        # 1. Manual/operator cancellation (default request_cancel)
        watcher.track("ORD-MAN-1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.request_cancel("ORD-MAN-1")
        self.assertEqual(watcher.pending["ORD-MAN-1"]["cancel_origin"], "MANUAL_OPERATOR")
        self.assertFalse(watcher.pending["ORD-MAN-1"]["retry_eligible"])

        # 2. Signal withdrawal cancellation
        watcher.track("ORD-SIG-1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.request_cancel("ORD-SIG-1", origin="SIGNAL_WITHDRAWAL", reason="Channel withdrew signal")
        self.assertEqual(watcher.pending["ORD-SIG-1"]["cancel_origin"], "SIGNAL_WITHDRAWAL")
        self.assertFalse(watcher.pending["ORD-SIG-1"]["retry_eligible"])

        # 3. Emergency stop cancellation
        watcher.track("ORD-STOP-1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.request_cancel("ORD-STOP-1", origin="EMERGENCY_STOP", reason="Operator emergency stop")
        self.assertEqual(watcher.pending["ORD-STOP-1"]["cancel_origin"], "EMERGENCY_STOP")
        self.assertFalse(watcher.pending["ORD-STOP-1"]["retry_eligible"])

        # 4. External/unknown cancellation (broker reports CANCELLED without any bot timeout request)
        watcher.track("ORD-EXT-1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)

        # 5. Legacy record without cancel_origin loaded from persistence
        watcher.track("ORD-LEG-1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-LEG-1"].pop("cancel_origin", None)
        watcher.pending["ORD-LEG-1"].pop("retry_eligible", None)
        watcher._save()
        watcher._load()
        self.assertFalse(watcher.pending["ORD-LEG-1"].get("retry_eligible", True))

        client.order_status = Mock(
            return_value={"orderStatus": "CANCELLED", "filledQty": 0, "averageTradedPrice": 0.0}
        )
        watcher.check_once()

        # Zero replacement orders placed across all 5 non-timeout cancellations!
        client.place_order.assert_not_called()
        for oid in ("ORD-MAN-1", "ORD-SIG-1", "ORD-STOP-1", "ORD-EXT-1", "ORD-LEG-1"):
            self.assertIn(oid, watcher.terminal_orders)
            can_r, _, _ = watcher.evaluate_retry(oid, current_market_price=100.3, quote_timestamp=1000.0)
            self.assertFalse(can_r)
            exec_r, _, _ = watcher.execute_retry(oid, current_market_price=100.3, quote_timestamp=1000.0)
            self.assertFalse(exec_r)
        client.place_order.assert_not_called()

    def test_cv13_02_b_restart_preserves_cancellation_reason_and_retry_eligibility(self):
        """
        CV13-02 Acceptance 4:
        Restart preserves cancellation reason and retry eligibility for both prohibited
        and eligible timeout cancellations.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher1 = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher1

        watcher1.track("ORD-MAN-RST", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher1.track("ORD-TMO-RST", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)

        client.cancel_order = Mock(return_value=(True, "HTTP 202 Accepted"))
        # Manual cancel on ORD-MAN-RST at t=1010
        now_val[0] = 1010.0
        watcher1.request_cancel("ORD-MAN-RST", origin="MANUAL_OPERATOR", reason="User clicked cancel")

        # Timeout cancel on ORD-TMO-RST at t=1061 via check_once()
        now_val[0] = 1061.0
        client.order_status = Mock(
            return_value={"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
        )
        watcher1.check_once()

        # Restart into watcher2 before broker confirms terminal CANCELLED
        watcher2 = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher2
        self.assertEqual(watcher2.pending["ORD-MAN-RST"]["cancel_origin"], "MANUAL_OPERATOR")
        self.assertEqual(watcher2.pending["ORD-MAN-RST"]["cancel_reason"], "User clicked cancel")
        self.assertFalse(watcher2.pending["ORD-MAN-RST"]["retry_eligible"])

        self.assertEqual(watcher2.pending["ORD-TMO-RST"]["cancel_origin"], "BOT_TIMEOUT")
        self.assertTrue(watcher2.pending["ORD-TMO-RST"]["retry_eligible"])

        # Now broker confirms CANCELLED for both orders
        now_val[0] = 1062.0
        client.order_status = Mock(
            side_effect=lambda oid: (
                {"orderStatus": "CANCELLED", "filledQty": 3, "averageTradedPrice": 100.0}
                if oid == "ORD-TMO-RST"
                else {"orderStatus": "CANCELLED", "filledQty": 0, "averageTradedPrice": 0.0}
            )
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 3}]
        )
        client.place_forever_oco = Mock(return_value=(True, "OCO-TMO-3", "Placed"))
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-TMO-3",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "transactionType": "SELL",
                    "quantity": 3,
                    "quantity1": 3,
                    "triggerPrice": 97.0,
                    "price1": 103.0,
                }
            ]
        )
        client.get_yahoo_quote_details = Mock(
            return_value=QuoteResult(
                is_valid=True,
                price=100.4,
                source="YAHOO_FINANCE",
                ticker="SBIN.NS",
                venue="NSE",
                security_id="3045",
                currency="INR",
                market_state="REGULAR",
                quote_timestamp=1062.0,
                age_sec=0.0,
            )
        )
        client. get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.place_order = Mock(return_value=(True, "ORD-TMO-CHILD", "Placed"))

        watcher2.check_once()

        # Only ORD-TMO-RST retried (for remaining 7 shares); ORD-MAN-RST did NOT retry!
        client.place_order.assert_called_once()
        placed_req = client.place_order.call_args[0][0]
        self.assertEqual(placed_req.quantity, 7)
        self.assertIsNone(watcher2.terminal_orders["ORD-MAN-RST"].get("retry_order_id"))
        self.assertEqual(watcher2.terminal_orders["ORD-TMO-RST"].get("retry_order_id"), "ORD-TMO-CHILD")
        self.assertFalse(watcher2.pending["ORD-TMO-CHILD"].get("retry_eligible", True))

    # =========================================================================
    # CV13-03 (High): Continue cancellation recovery after a failed timeout request
    # =========================================================================

    def test_cv13_03_a_http_429_cancel_rejection_retries_after_backoff_and_protects_intervening_fill(self):
        """
        CV13-03 Acceptance 1, 3 & 4:
        - Timeout reached at t=1061 -> first cancel rejected with HTTP 429.
        - Separate flags tracked: entry_timeout_elapsed=True, cancel_requested=True,
          cancel_acknowledged=False, cancel_uncertain=False, terminal_confirmed=False.
        - At t=1062 (before 2.0s backoff): no second cancel call yet; 4 shares fill and get protected.
        - At t=1063 (after 2.0s backoff): broker status still PART_TRADED -> second cancel attempt occurs and is acknowledged (202).
        - While only acknowledged (not terminal CANCELLED), zero replacement BUY orders occur.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        watcher.track("ORD-CV03-A", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)

        cancel_responses = [
            (False, "Dhan rate limit exceeded (429): Too Many Requests"),
            (True, "Order cancellation request accepted by broker (HTTP 202)"),
        ]
        client.cancel_order = Mock(side_effect=lambda oid: cancel_responses.pop(0))
        client.order_status = Mock(
            return_value={"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
        )
        client.place_order = Mock(return_value=(True, "CHILD-1", "Placed"))

        # Pass 1 at t=1061: timeout elapses, first cancel fails with HTTP 429
        now_val[0] = 1061.0
        watcher.check_once()
        self.assertEqual(client.cancel_order.call_count, 1)
        rec = watcher.pending["ORD-CV03-A"]
        self.assertTrue(rec["entry_timeout_elapsed"])
        self.assertTrue(rec["cancel_requested"])
        self.assertFalse(rec["cancel_acknowledged"])
        self.assertFalse(rec["cancel_uncertain"])
        self.assertFalse(rec["terminal_confirmed"])

        # Pass 2 at t=1062 (inside 2.0s backoff): 4 shares fill between cancellation attempts
        now_val[0] = 1062.0
        client.order_status = Mock(
            return_value={"orderStatus": "PART_TRADED", "filledQty": 4, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 4}]
        )
        client.place_forever_oco = Mock(return_value=(True, "OCO-CV03-4", "Placed"))
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-CV03-4",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "transactionType": "SELL",
                    "quantity": 4,
                    "quantity1": 4,
                    "triggerPrice": 97.0,
                    "price1": 103.0,
                }
            ]
        )
        watcher.check_once()
        # Fill of 4 is protected, still waiting for cancel backoff
        self.assertEqual(watcher.pending["ORD-CV03-A"]["protected_qty"], 4)
        self.assertEqual(client.cancel_order.call_count, 1)

        # Pass 3 at t=1063.5 (backoff elapsed): second cancel attempt occurs and is acknowledged (202)
        now_val[0] = 1063.5
        watcher.check_once()
        self.assertEqual(client.cancel_order.call_count, 2)
        self.assertTrue(watcher.pending["ORD-CV03-A"]["cancel_acknowledged"])
        self.assertFalse(watcher.pending["ORD-CV03-A"]["terminal_confirmed"])
        # Acknowledgement without terminal confirmation never triggers replacement BUY!
        client.place_order.assert_not_called()

    def test_cv13_03_b_cancellation_timeout_then_confirmed_readback_or_exhausted_attempts(self):
        """
        CV13-03 Acceptance 2, 5 & 6:
        - Case 1: Cancellation times out (uncertain), but next readback confirms CANCELLED -> no second cancel call.
        - Case 2: Cancellation fails repeatedly until automatic attempts are exhausted -> order stays in pending
          (not marked cancelled or closed) and emits an actionable alert.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(
            client,
            clock_fn=lambda: now_val[0],
            max_cancel_attempts=2,
            cancel_backoff_sec=2.0,
            auto_retry_enabled=False,
        )
        gate.watcher = watcher

        # Case 1: Timeout on DELETE, then readback confirms CANCELLED
        watcher.track("ORD-CV03-B1", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC")
        client.cancel_order = Mock(return_value=(False, "HTTP exception cancelling order: Read timed out"))
        client.order_status = Mock(
            return_value={"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
        )

        now_val[0] = 1061.0
        watcher.check_once()
        self.assertEqual(client.cancel_order.call_count, 1)
        self.assertTrue(watcher.pending["ORD-CV03-B1"]["cancel_uncertain"])

        # Next poll: broker readback confirms CANCELLED -> no unnecessary second DELETE!
        now_val[0] = 1065.0
        client.order_status = Mock(
            return_value={"orderStatus": "CANCELLED", "filledQty": 0, "averageTradedPrice": 0.0}
        )
        watcher.check_once()
        self.assertEqual(client.cancel_order.call_count, 1)
        self.assertIn("ORD-CV03-B1", watcher.terminal_orders)
        self.assertTrue(watcher.terminal_orders["ORD-CV03-B1"]["terminal_confirmed"])

        # Case 2: Automatic cancel attempts exhausted -> stays monitored in pending + actionable alert
        watcher.track("ORD-CV03-B2", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC")
        client.cancel_order = Mock(return_value=(False, "Dhan rate limit exceeded (429)"))
        client.order_status = Mock(
            return_value={"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
        )

        now_val[0] = 1130.0
        watcher.check_once()  # Attempt 1
        now_val[0] = 1135.0
        watcher.check_once()  # Attempt 2 (reaches max_cancel_attempts=2)
        now_val[0] = 1145.0
        watcher.check_once()  # Exhausted -> alerts, does NOT call cancel_order a 3rd time, keeps in pending

        self.assertEqual(client.cancel_order.call_count, 2)
        self.assertIn("ORD-CV03-B2", watcher.pending)
        self.assertNotIn("ORD-CV03-B2", watcher.terminal_orders)
        self.assertTrue(watcher.pending["ORD-CV03-B2"].get("cancel_exhausted"))
        outbox = self.db.fetch_pending_outbox(now=2000.0)
        self.assertTrue(any("ORD-CV03-B2" in str(ev.get("payload")) for ev in outbox))

    # =========================================================================
    # CV13-04 (Medium): Revalidate retry eligibility immediately before submission
    # =========================================================================

    def test_cv13_04_a_quote_stale_after_delayed_funds_check_submits_zero_orders(self):
        """
        CV13-04 Acceptance 1:
        Quote is valid initially (age=0.2s <= 2.0s), but funds check delays clock by 2.5s
        (final quote age = 2.7s > 2.0s). Zero broker submissions occur, intent is recorded as
        NOT_SUBMITTED (distinct from UNKNOWN), and root's one-retry guarantee is preserved.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        watcher.track("ORD-CV04-A", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-CV04-A"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["ORD-CV04-A"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-CV04-A"]["retry_eligible"] = True

        def slow_funds():
            now_val[0] += 2.5
            return True, 100000.0, "OK"

        client.get_fund_limits = Mock(side_effect=slow_funds)
        client.place_order = Mock(return_value=(True, "SHOULD-NOT-SUBMIT", "Placed"))

        ok, child_id, msg = watcher.execute_retry(
            "ORD-CV04-A",
            current_market_price=100.4,
            quote_timestamp=999.8,  # age=0.2s initially, 2.7s after slow_funds()
            max_quote_age_sec=2.0,
        )
        self.assertFalse(ok)
        self.assertIsNone(child_id)
        self.assertIn("stale", msg.lower())
        client.place_order.assert_not_called()

        # Recorded distinctly as NOT_SUBMITTED (never UNKNOWN) while preserving one-retry guarantee
        with self.db._get_conn() as conn:
            intent_row = conn.execute("SELECT state FROM order_intents ORDER BY id DESC LIMIT 1").fetchone()
            self.assertIsNotNone(intent_row)
            self.assertEqual(intent_row["state"], "NOT_SUBMITTED")
        self.assertTrue(self.db.has_retry_reservation("ORD-CV04-A"))
        self.assertTrue(gate.is_accepting_entries)

    def test_cv13_04_b_quote_becomes_stale_during_persistence_submits_zero_orders(self):
        """
        CV13-04 Acceptance 2:
        Quote is still fresh after funds check, but becomes stale during persistence work
        right before broker submission -> zero broker submissions, recorded as NOT_SUBMITTED.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        watcher.track("ORD-CV04-B", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-CV04-B"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["ORD-CV04-B"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-CV04-B"]["retry_eligible"] = True

        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.place_order = Mock(return_value=(True, "SHOULD-NOT-SUBMIT", "Placed"))

        orig_save_strict = watcher._save_strict

        def slow_save_strict():
            orig_save_strict()
            now_val[0] += 2.4  # Makes quote stale during persistence

        watcher._save_strict = slow_save_strict

        ok, child_id, msg = watcher.execute_retry(
            "ORD-CV04-B",
            current_market_price=100.4,
            quote_timestamp=999.9,
            max_quote_age_sec=2.0,
        )
        self.assertFalse(ok)
        self.assertIsNone(child_id)
        self.assertIn("stale", msg.lower())
        client.place_order.assert_not_called()

        with self.db._get_conn() as conn:
            intent_row = conn.execute("SELECT state FROM order_intents ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(intent_row["state"], "NOT_SUBMITTED")

    def test_cv13_04_c_future_timestamp_stop_or_gate_change_and_fno_timestamp_distinction(self):
        """
        CV13-04 Acceptance 3, 4 & 5:
        - Future timestamp -> zero submissions.
        - Emergency stop or entry gate closing before submission -> zero submissions (recorded as NOT_SUBMITTED).
        - F&O distinguishes local retrieval timestamp from broker/exchange source timestamp (does not fabricate source timestamp).
        - Fresh quote with all checks satisfied -> retry succeeds.
        """
        client = DhanClient(self.cfg, self.stop)
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.reconciliation_gate = gate

        now_val = [1000.0]
        watcher = self._make_watcher(client, clock_fn=lambda: now_val[0])
        gate.watcher = watcher

        # 1. Future quote timestamp -> zero submissions
        watcher.track("ORD-CV04-FUT", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-CV04-FUT"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["ORD-CV04-FUT"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-CV04-FUT"]["retry_eligible"] = True
        client.place_order = Mock(return_value=(True, "NO", "NO"))

        ok_fut, _, msg_fut = watcher.execute_retry(
            "ORD-CV04-FUT",
            current_market_price=100.4,
            quote_timestamp=1001.5,  # Future timestamp!
            max_quote_age_sec=2.0,
        )
        self.assertFalse(ok_fut)
        self.assertIn("future", msg_fut.lower())
        client.place_order.assert_not_called()

        # 2. Gate closes during funds check -> zero submissions, NOT_SUBMITTED
        watcher.track("ORD-CV04-GATE", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-CV04-GATE"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["ORD-CV04-GATE"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["ORD-CV04-GATE"]["retry_eligible"] = True

        def close_gate_during_funds():
            gate.halt_new_entries("Concurrent exposure alert")
            return True, 100000.0, "OK"

        client.get_fund_limits = Mock(side_effect=close_gate_during_funds)
        ok_gate, _, msg_gate = watcher.execute_retry(
            "ORD-CV04-GATE",
            current_market_price=100.4,
            quote_timestamp=999.8,
            max_quote_age_sec=2.0,
        )
        self.assertFalse(ok_gate)
        client.place_order.assert_not_called()
        with self.db._get_conn() as conn:
            intent_row = conn.execute("SELECT state FROM order_intents ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(intent_row["state"], "NOT_SUBMITTED")

        # Re-open gate and test F&O local retrieval timestamp vs source timestamp + fresh success
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_ltp = Mock(return_value=(True, 100.4, "LTP retrieved from Dhan"))
        client.place_super_order = Mock(return_value=(True, "SUP-CV04-OK", "Placed"))

        watcher.track(
            "SUP-CV04-FNO",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=100,
            product_type="MARGIN",
            original_market_price=100.0,
            is_super_order=True,
            tick_size=0.05,
            lot_size=100,
            option_type="PE",
            strike=8000.0,
            expiry=date(2099, 10, 29),
            trading_symbol="POLYCAB-29OCT99-8000-PE",
        )
        watcher.pending["SUP-CV04-FNO"]["state"] = OrderLifecycleState.CANCELLED.value
        watcher.pending["SUP-CV04-FNO"]["cancel_origin"] = "BOT_TIMEOUT"
        watcher.pending["SUP-CV04-FNO"]["retry_eligible"] = True

        ok_fno, child_fno, _ = watcher.execute_retry("SUP-CV04-FNO", max_quote_age_sec=2.0)
        self.assertTrue(ok_fno)
        self.assertEqual(child_fno, "SUP-CV04-OK")
        parent_rec = watcher.pending.get("SUP-CV04-FNO") or watcher.terminal_orders.get("SUP-CV04-FNO")
        self.assertEqual(parent_rec.get("retry_quote_timestamp_type"), "LOCAL_RETRIEVAL")
        self.assertIsNone(parent_rec.get("retry_source_quote_timestamp"))

    # =========================================================================
    # CV13-05 (Medium): Reject incomplete position records at startup
    # =========================================================================

    def test_cv13_05_a_malformed_netqty_and_missing_identity_block_startup(self):
        """
        CV13-05 Acceptance 1 & 5:
        - Missing, null, boolean, non-finite, fractional, negative, and non-numeric netQty -> startup blocked.
        - Missing required instrument, segment, or product identity -> startup blocked.
        """
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_forever_orders = Mock(return_value=[])
        client.get_super_orders = Mock(return_value=[])

        malformed_positions = [
            # Missing or invalid netQty
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": None}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": True}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": False}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": float("nan")}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": float("inf")}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 5.5}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0.5}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": "5.5"}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": "abc"}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": ""}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": -5}],
            # Missing required instrument / segment / product identity
            [{"exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0}],
            [{"securityId": "", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0}],
            [{"securityId": "3045", "productType": "CNC", "netQty": 5}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "netQty": 5}],
            [{"securityId": "3045", "exchangeSegment": "INVALID_SEG", "productType": "CNC", "netQty": 0}],
            [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "INVALID_PROD", "netQty": 0}],
        ]

        for bad_list in malformed_positions:
            client.get_positions = Mock(return_value=bad_list)
            ok, msg = gate.run_startup_reconciliation()
            self.assertFalse(ok, f"Expected startup to be blocked for {bad_list!r}, but got ok=True ({msg})")
            self.assertFalse(gate.is_accepting_entries)

    def test_cv13_05_b_empty_list_explicit_zero_and_protected_valid_position_succeed(self):
        """
        CV13-05 Acceptance 2, 3 & 4:
        - Successful empty list -> startup succeeds.
        - Valid explicit zero (0 and "0") -> startup succeeds.
        - Correctly protected valid position -> startup succeeds.
        """
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_forever_orders = Mock(return_value=[])
        client.get_super_orders = Mock(return_value=[])

        # 1. Empty list
        client.get_positions = Mock(return_value=[])
        ok_empty, msg_empty = gate.run_startup_reconciliation()
        self.assertTrue(ok_empty, msg_empty)
        self.assertTrue(gate.is_accepting_entries)

        # 2. Valid explicit zero (both int 0 and string "0")
        client.get_positions = Mock(
            return_value=[
                {"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0},
                {"securityId": "1594", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": "0"},
            ]
        )
        ok_zero, msg_zero = gate.run_startup_reconciliation()
        self.assertTrue(ok_zero, msg_zero)
        self.assertTrue(gate.is_accepting_entries)

        # 3. Correctly protected valid position (int 5 and string "5")
        client.get_positions = Mock(
            return_value=[
                {"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": "5"},
            ]
        )
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-CV05-5",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "transactionType": "SELL",
                    "quantity": 5,
                    "quantity1": 5,
                    "triggerPrice": 97.0,
                    "price1": 103.0,
                }
            ]
        )
        ok_prot, msg_prot = gate.run_startup_reconciliation()
        self.assertTrue(ok_prot, msg_prot)
        self.assertTrue(gate.is_accepting_entries)


if __name__ == "__main__":
    unittest.main()
