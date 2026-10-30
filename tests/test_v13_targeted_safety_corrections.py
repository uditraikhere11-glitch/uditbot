"""
Auditbot V13 Targeted Safety Corrections — Reproduction & Regression Suite.
Covers all required tests across:
- Safety Correction A: Durable protection-attempt lifecycle, pre-POST commit fail-closed,
  restored SUBMITTING/UNKNOWN recovery without blind resubmission, 5 -> 10 incremental
  partial-fill verification against stored attempt qty/prices, and concurrency protection.
- Safety Correction B: Single authoritative Super Order & Forever OCO protection verification
  contract (rejects two TRADED legs, missing leg status/quantity, single leg, wrong identity/qty/price,
  top-level prices without active leg evidence, and prevents watcher fallback override).
- Safety Correction C: Startup coverage from fresh broker evidence (distinguishes OK_EMPTY,
  OK_RECORDS, FAILED/MALFORMED/UNAVAILABLE reads; rejects historical local coverage without
  active broker protection; prevents local+broker double counting; counts 5-share OCO pair as 5;
  rejects wrong product/segment coverage; blocks startup on failed/malformed reads; allows
  genuine empty and properly protected accounts).
- Section 6: Production wiring of automatic retry inside ProtectionWatcher.check_once() after
  broker-confirmed terminal cancellation with partial-fill reconciliation, durable reservation,
  Super Order route preservation, and single-retry lineage enforcement.
"""

from datetime import date
import os
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock

from src.config import AppConfig
from src.models import ProtectionLevels, QuoteResult, TradingMode, ProductType
from src.journal.db import JournalDB
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher, OrderLifecycleState
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.broker.dhan_client import DhanClient


class TestV13TargetedSafetyCorrections(unittest.TestCase):
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
    # SAFETY CORRECTION A: Durable Protection-Attempt Lifecycle
    # =========================================================================

    def test_A1_persistence_failure_prevents_broker_post(self):
        """A.1: If pre-POST persistence fails, place_forever_oco must NOT be called."""
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 5,
            "averageTradedPrice": 100.0,
        }
        broker.trades_avg_price.return_value = 100.0
        broker.get_positions.return_value = [{"securityId": "3045", "netQty": 5}]
        broker.place_forever_oco.return_value = (True, "OCO-SHOULD-NOT-FIRE", "OK")

        watcher = self._make_watcher(broker)
        watcher.track("ORD-A1", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")

        # Simulate SQLite failure on pre-POST commit
        orig_get_conn = self.db._get_conn

        def broken_conn():
            raise sqlite3.OperationalError("disk I/O error")

        self.db._get_conn = broken_conn
        try:
            watcher.check_once()
        finally:
            self.db._get_conn = orig_get_conn

        broker.place_forever_oco.assert_not_called()
        self.assertEqual(watcher.pending["ORD-A1"]["protected_qty"], 0)

    def test_A2_restart_after_durable_reservation_before_submission_does_not_blindly_resubmit(self):
        """
        A.2: A restored SUBMITTING attempt without a broker order ID (and empty broker lookup)
        is treated as potentially accepted and NEVER falls through to a second submission.
        """
        broker = MagicMock()
        broker.cfg = self.cfg
        gate = StartupReconciliationGate(broker, self.db, None, self.stop)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        broker.reconciliation_gate = gate

        # Create watcher and track parent order
        watcher1 = self._make_watcher(broker)
        watcher1.track("ORD-A2", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")

        # Simulate crash right after pre-POST durable reservation (state="SUBMITTING", oco_id=None)
        if hasattr(self.db, "create_protection_attempt"):
            self.db.create_protection_attempt(
                attempt_id="PATM-A2-1",
                parent_order_id="ORD-A2",
                root_order_id="ORD-A2",
                account_id="1113731536",
                symbol="SBIN",
                security_id="3045",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                route="FOREVER_OCO",
                uncovered_from_qty=0,
                quantity=5,
                executed_price=100.0,
                sl_trigger=97.0,
                sl_limit=96.05,
                target_price=103.0,
                tick_size=0.05,
                correlation_id="OCO-SBIN-CRASH-0001",
            )
        else:
            watcher1.pending["ORD-A2"]["pending_oco"] = {
                "correlation_id": "OCO-SBIN-CRASH-0001",
                "oco_id": None,
                "qty": 5,
                "sl_trigger": 97.0,
                "sl_limit": 96.05,
                "target_price": 103.0,
                "state": "SUBMITTING",
            }
            watcher1._save()

        # Restart into a fresh watcher instance; broker Forever book is currently empty
        broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 5,
            "averageTradedPrice": 100.0,
        }
        broker.trades_avg_price.return_value = 100.0
        broker.get_forever_orders.return_value = []
        broker.get_order_by_correlation_id.return_value = None
        broker.get_order_book.return_value = []
        broker.place_forever_oco.return_value = (True, "OCO-DUP", "Placed")

        watcher2 = self._make_watcher(broker, clock_fn=lambda: 1010.0)
        watcher2.check_once()

        # Must NOT have submitted a second Forever OCO!
        broker.place_forever_oco.assert_not_called()
        self.assertEqual(watcher2.pending["ORD-A2"]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries)
        self.assertGreaterEqual(len(self.db.get_unprotected_orders()), 1)

    def test_A3_restart_after_broker_acceptance_before_saving_response_reconciles_without_duplicate(self):
        """
        A.3: Broker accepted the Forever OCO before crash, but local state only has SUBMITTING
        with correlation_id (and oco_id=None). On restart, watcher reconciles via correlationId,
        verifies active legs, confirms protection, and places zero duplicate OCOs.
        """
        client = DhanClient(self.cfg, self.stop)
        watcher1 = self._make_watcher(client)
        watcher1.track("ORD-A3", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")

        def crash_after_broker_accept(**kwargs):
            corr = kwargs.get("correlation_id")
            # Simulate broker accepting the order into its forever book before process dies
            client.get_forever_orders = Mock(
                return_value=[
                    {
                        "orderId": "OCO-ACCEPTED-99",
                        "correlationId": corr,
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
            raise RuntimeError("Process killed before response could be saved")

        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 5, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 5}])
        client.place_forever_oco = Mock(side_effect=crash_after_broker_accept)

        # Pass 1 crashes inside place_forever_oco after pre-POST commit
        watcher1.check_once()
        self.assertEqual(client.place_forever_oco.call_count, 1)

        # Restart into a brand-new watcher instance WITHOUT calling watcher1._save() after the crash
        watcher2 = self._make_watcher(client, clock_fn=lambda: 1070.0)
        watcher2.check_once()

        # Must reconcile OCO-ACCEPTED-99 from SQLite pre-POST record without calling place_forever_oco again!
        self.assertEqual(client.place_forever_oco.call_count, 1)
        self.assertIn("ORD-A3", watcher2.terminal_orders)
        self.assertEqual(watcher2.terminal_orders["ORD-A3"]["protected_qty"], 5)

    def test_A4_http_uncertainty_preserves_original_attempt_and_identity(self):
        """A.4: HTTP timeout / 504 during OCO placement preserves attempt and correlation_id as UNKNOWN."""
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 5,
            "averageTradedPrice": 100.0,
        }
        broker.trades_avg_price.return_value = 100.0
        broker.get_positions.return_value = [{"securityId": "3045", "netQty": 5}]
        broker.place_forever_oco.return_value = (False, None, "HTTP exception placing Forever OCO: Read timed out (504)")
        broker.get_forever_orders.return_value = []
        broker.get_order_by_correlation_id.return_value = None

        watcher = self._make_watcher(broker, clock_fn=lambda: 1000.0)
        watcher.track("ORD-A4", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")
        watcher.check_once()

        first_corr = watcher.pending["ORD-A4"]["pending_oco"]["correlation_id"]
        self.assertTrue(first_corr.startswith("OCO-"))
        self.assertEqual(watcher.pending["ORD-A4"]["pending_oco"]["state"], "UNKNOWN")

        # Advance clock past retry_at and poll again; original correlation_id & attempt must be preserved
        watcher.clock = lambda: 1070.0
        watcher.check_once()
        self.assertEqual(broker.place_forever_oco.call_count, 1)
        self.assertEqual(watcher.pending["ORD-A4"]["pending_oco"]["correlation_id"], first_corr)

    def test_A5_delayed_verification_with_5_to_10_cumulative_fill_confirms_first_5_and_protects_remainder_5(self):
        """
        A.5 Required Example:
        - Entry fills 5 shares @ 100.0.
        - Protection attempt A submits coverage for 5 shares (SL=97.0, Target=103.0).
        - Verification is delayed.
        - Entry fills another 5 shares @ 110.0 (cumulative 10 shares @ 105.0 VWAP).
        - Attempt A is verified against its original 5-share request (qty=5, SL=97.0, Target=103.0).
        - Once A is confirmed, protection Attempt B is submitted ONLY for the remaining 5 shares (@ 110.0 marginal price).
        """
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client, clock_fn=lambda: 1000.0)
        watcher.track("ORD-A5", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC")

        # Step 1: 5 shares filled @ 100.0, OCO-A placed for 5 shares, but Forever book is delayed (empty)
        client.order_status = Mock(
            return_value={"orderStatus": "PART_TRADED", "filledQty": 5, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 5}])

        oco_submissions = []

        def fake_place_oco(**kwargs):
            oco_submissions.append(kwargs)
            idx = len(oco_submissions)
            return True, f"OCO-A5-{idx}", "Placed"

        client.place_forever_oco = Mock(side_effect=fake_place_oco)
        client.get_forever_orders = Mock(return_value=[])

        watcher.check_once()
        self.assertEqual(len(oco_submissions), 1)
        self.assertEqual(oco_submissions[0]["levels"].quantity, 5)
        self.assertEqual(oco_submissions[0]["levels"].executed_price, 100.0)
        self.assertEqual(watcher.pending["ORD-A5"]["protected_qty"], 0)

        # Step 2: Entry fills another 5 shares @ 110.0 -> cumulative filledQty=10, VWAP=105.0.
        # Broker Forever book now shows OCO-A5-1 for the FIRST 5 shares (@ SL 97.0, Target 103.0),
        # and after OCO-A5-2 is placed, shows both OCO-A5-1 and OCO-A5-2.
        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 105.0}
        )
        client.trades_avg_price = Mock(return_value=105.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 10}])

        def dynamic_forever_book():
            rows = [
                {
                    "orderId": "OCO-A5-1",
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
            if len(oco_submissions) >= 2:
                rows.append(
                    {
                        "orderId": "OCO-A5-2",
                        "orderStatus": "PENDING",
                        "securityId": "3045",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "transactionType": "SELL",
                        "quantity": 5,
                        "quantity1": 5,
                        "triggerPrice": 106.7,
                        "price1": 113.3,
                    }
                )
            return rows

        client.get_forever_orders = Mock(side_effect=dynamic_forever_book)
        watcher.clock = lambda: 1065.0
        watcher.check_once()

        # Attempt A (5 shares @ 100.0) must be verified against qty=5, SL=97.0, Target=103.0,
        # and Attempt B must be submitted ONLY for the remaining 5 shares @ marginal price 110.0!
        self.assertEqual(len(oco_submissions), 2)
        self.assertEqual(oco_submissions[1]["levels"].quantity, 5)
        self.assertEqual(oco_submissions[1]["levels"].executed_price, 110.0)
        self.assertEqual(oco_submissions[1]["levels"].sl_trigger, 106.7)
        self.assertEqual(oco_submissions[1]["levels"].target_price, 113.3)
        self.assertIn("ORD-A5", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["ORD-A5"]["protected_qty"], 10)

    def test_A6_concurrent_checks_cannot_submit_duplicate_protection(self):
        """A.6: Concurrent watcher passes cannot create competing attempts for the same uncovered fill."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        watcher.track("ORD-A6", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")

        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 5, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 5}])

        barrier = threading.Barrier(2)
        post_calls = []

        def slow_place_oco(**kwargs):
            post_calls.append(kwargs)
            return True, "OCO-CONC-1", "Placed"

        client.place_forever_oco = Mock(side_effect=slow_place_oco)
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-CONC-1",
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

        def worker():
            barrier.wait()
            watcher.check_once()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(post_calls), 1)

    def test_A7_healthy_normal_submission_completes_successfully(self):
        """A.7: Healthy normal fill + Forever OCO submission + active leg confirmation succeeds."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        watcher.track("ORD-A7", "SBIN", "3045", "NSE_EQ", qty=5, product_type="CNC")
        client.order_status = Mock(
            return_value={"orderStatus": "TRADED", "filledQty": 5, "averageTradedPrice": 200.0}
        )
        client.trades_avg_price = Mock(return_value=200.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 5}])
        client.place_forever_oco = Mock(return_value=(True, "OCO-OK-7", "Placed"))
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-OK-7",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "transactionType": "SELL",
                    "quantity": 5,
                    "quantity1": 5,
                    "triggerPrice": 194.0,
                    "price1": 206.0,
                }
            ]
        )
        watcher.check_once()
        self.assertIn("ORD-A7", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["ORD-A7"]["protected_qty"], 5)

    # =========================================================================
    # SAFETY CORRECTION B: Authoritative Super Protection Verification
    # =========================================================================

    def test_B1_two_traded_legs_never_produce_confirmed_protection(self):
        """B.1 & B.7: Two TRADED legs fail verification and cannot be overridden by watcher fallback."""
        client = DhanClient(self.cfg, self.stop)
        traded_super = {
            "orderId": "SUP-B1",
            "securityId": "54231",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "orderStatus": "TRADED",
            "filledQty": 100,
            "averageTradedPrice": 100.0,
            "legDetails": [
                {"legName": "STOP_LOSS_LEG", "orderStatus": "TRADED", "remainingQuantity": 100, "price": 97.0},
                {"legName": "TARGET_LEG", "orderStatus": "TRADED", "remainingQuantity": 100, "price": 103.0},
            ],
        }
        client.get_super_orders = Mock(return_value=[traded_super])
        client.order_status = Mock(return_value=traded_super)
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "Updated"))

        ok, _ = client.verify_super_order_legs_active("SUP-B1", "54231", 100, 97.0, 103.0)
        self.assertFalse(ok)

        watcher = self._make_watcher(client)
        watcher.track("SUP-B1", "POLYCAB", "54231", "NSE_FNO", qty=100, product_type="MARGIN", is_super_order=True)
        watcher.check_once()

        self.assertEqual(watcher.pending["SUP-B1"]["protected_qty"], 0)
        self.assertEqual(watcher.pending["SUP-B1"]["reanchor_status"], "UNVERIFIED")

    def test_B2_missing_leg_status_or_missing_quantity_evidence_does_not_confirm_coverage(self):
        """B.2: Missing leg orderStatus or missing leg quantity evidence must fail verification."""
        client = DhanClient(self.cfg, self.stop)

        # Case 1: Missing leg orderStatus
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-B2",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "legDetails": [
                        {"legName": "STOP_LOSS_LEG", "orderStatus": "", "remainingQuantity": 100, "price": 97.0},
                        {"legName": "TARGET_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
                    ],
                }
            ]
        )
        self.assertFalse(client.verify_super_order_legs_active("SUP-B2", "54231", 100, 97.0, 103.0)[0])

        # Case 2: Missing leg quantity evidence
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-B2",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "legDetails": [
                        {"legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "price": 97.0},
                        {"legName": "TARGET_LEG", "orderStatus": "PENDING", "price": 103.0},
                    ],
                }
            ]
        )
        self.assertFalse(client.verify_super_order_legs_active("SUP-B2", "54231", 100, 97.0, 103.0)[0])

    def test_B3_stop_loss_only_or_target_only_response_fails_verification(self):
        """B.3: A stop-loss-only or target-only response fails verification."""
        client = DhanClient(self.cfg, self.stop)
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-B3",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "legDetails": [
                        {"legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 97.0},
                    ],
                }
            ]
        )
        self.assertFalse(client.verify_super_order_legs_active("SUP-B3", "54231", 100, 97.0, 103.0)[0])

        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-B3",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "legDetails": [
                        {"legName": "TARGET_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
                    ],
                }
            ]
        )
        self.assertFalse(client.verify_super_order_legs_active("SUP-B3", "54231", 100, 97.0, 103.0)[0])

    def test_B4_wrong_instrument_wrong_parent_insufficient_qty_and_incorrect_prices_fail_verification(self):
        """B.4: Wrong securityId, wrong parent orderId, insufficient quantity, or wrong prices fail verification."""
        client = DhanClient(self.cfg, self.stop)
        base_record = {
            "orderId": "SUP-B4",
            "dhanClientId": "1113731536",
            "securityId": "54231",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "legDetails": [
                {"legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 97.0},
                {"legName": "TARGET_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
            ],
        }
        client.get_super_orders = Mock(return_value=[base_record])
        client.order_status = Mock(return_value=None)

        # Wrong parent orderId
        self.assertFalse(client.verify_super_order_legs_active("WRONG-ID", "54231", 100, 97.0, 103.0)[0])
        # Wrong securityId
        self.assertFalse(client.verify_super_order_legs_active("SUP-B4", "99999", 100, 97.0, 103.0)[0])
        # Insufficient quantity (need 150, have 100)
        self.assertFalse(client.verify_super_order_legs_active("SUP-B4", "54231", 150, 97.0, 103.0)[0])
        # Incorrect SL price
        self.assertFalse(client.verify_super_order_legs_active("SUP-B4", "54231", 100, 95.0, 103.0)[0])
        # Incorrect Target price
        self.assertFalse(client.verify_super_order_legs_active("SUP-B4", "54231", 100, 97.0, 108.0)[0])

    def test_B5_matching_top_level_prices_with_no_active_leg_evidence_remain_unverified(self):
        """B.5: Top-level stopLossPrice and targetPrice alone without active legDetails evidence fail verification."""
        client = DhanClient(self.cfg, self.stop)
        top_level_only = {
            "orderId": "SUP-B5",
            "securityId": "54231",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "orderStatus": "TRADED",
            "filledQty": 100,
            "averageTradedPrice": 100.0,
            "stopLossPrice": 97.0,
            "targetPrice": 103.0,
        }
        client.get_super_orders = Mock(return_value=[top_level_only])
        client.order_status = Mock(return_value=top_level_only)
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "OK"))

        self.assertFalse(client.verify_super_order_legs_active("SUP-B5", "54231", 100, 97.0, 103.0)[0])

        watcher = self._make_watcher(client)
        watcher.track("SUP-B5", "POLYCAB", "54231", "NSE_FNO", qty=100, product_type="MARGIN", is_super_order=True)
        watcher.check_once()
        self.assertEqual(watcher.pending["SUP-B5"]["protected_qty"], 0)
        self.assertEqual(watcher.pending["SUP-B5"]["reanchor_status"], "UNVERIFIED")

    def test_B6_valid_documented_active_pair_passes(self):
        """B.6: Valid documented active pair of STOP_LOSS_LEG and TARGET_LEG passes verification."""
        client = DhanClient(self.cfg, self.stop)
        valid_super = {
            "orderId": "SUP-B6",
            "dhanClientId": "1113731536",
            "securityId": "54231",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "orderStatus": "TRADED",
            "filledQty": 100,
            "averageTradedPrice": 100.0,
            "legDetails": [
                {
                    "orderId": "LEG-SL-1",
                    "legName": "STOP_LOSS_LEG",
                    "transactionType": "SELL",
                    "orderStatus": "PENDING",
                    "remainingQuantity": 100,
                    "price": 97.0,
                },
                {
                    "orderId": "LEG-TP-1",
                    "legName": "TARGET_LEG",
                    "transactionType": "SELL",
                    "orderStatus": "PENDING",
                    "remainingQuantity": 100,
                    "price": 103.0,
                },
            ],
        }
        client.get_super_orders = Mock(return_value=[valid_super])
        client.order_status = Mock(return_value=valid_super)
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "OK"))

        ok, msg = client.verify_super_order_legs_active("SUP-B6", "54231", 100, 97.0, 103.0)
        self.assertTrue(ok, msg)

        watcher = self._make_watcher(client)
        watcher.track("SUP-B6", "POLYCAB", "54231", "NSE_FNO", qty=100, product_type="MARGIN", is_super_order=True)
        watcher.check_once()
        self.assertIn("SUP-B6", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-B6"]["protected_qty"], 100)
        self.assertEqual(watcher.terminal_orders["SUP-B6"]["reanchor_status"], "CONFIRMED")

    # =========================================================================
    # SAFETY CORRECTION C: Startup Coverage From Fresh Broker Evidence
    # =========================================================================

    def test_C1_historical_local_coverage_of_10_with_no_active_broker_protection_blocks_startup(self):
        """C.1: Historical local protected_qty=10 with no active broker protection cannot cover a position of 10."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        watcher.track("ORD-HIST-10", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC")
        watcher.pending["ORD-HIST-10"]["protected_qty"] = 10
        watcher.terminal_orders["ORD-HIST-OLD"] = dict(watcher.pending["ORD-HIST-10"])
        watcher._save()

        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}]
        )
        client.get_forever_orders = Mock(return_value=[])
        client.get_super_orders = Mock(return_value=[])
        client.order_status = Mock(return_value={"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 100.0})

        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)
        ok, msg = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(gate.is_accepting_entries)
        self.assertIn("3045", msg)

    def test_C2_local_coverage_of_5_plus_same_broker_coverage_of_5_cannot_cover_position_of_10(self):
        """C.2: Local protected_qty=5 plus the same broker Forever OCO of 5 cannot cover a position of 10."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        watcher.track("ORD-C2", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC")
        watcher.pending["ORD-C2"]["protected_qty"] = 5
        watcher.pending["ORD-C2"]["filled_qty"] = 5
        watcher._save()

        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}]
        )
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-C2-5",
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
        client.get_super_orders = Mock(return_value=[])
        client.order_status = Mock(return_value={"orderStatus": "PART_TRADED", "filledQty": 5, "averageTradedPrice": 100.0})

        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)
        ok, msg = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(gate.is_accepting_entries)
        self.assertIn("verified_protected_qty=5", msg)

    def test_C3_five_share_oco_pair_counts_as_five(self):
        """C.3: A two-leg Forever OCO pair protecting 5 shares counts as 5, not 10."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)

        two_leg_oco_rows = [
            {
                "orderId": "OCO-PAIR-5",
                "legName": "STOP_LOSS_LEG",
                "orderStatus": "PENDING",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 97.0,
                "price": 96.05,
            },
            {
                "orderId": "OCO-PAIR-5",
                "legName": "TARGET_LEG",
                "orderStatus": "PENDING",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 5,
                "price": 103.0,
                "triggerPrice": 103.0,
            },
        ]
        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_forever_orders = Mock(return_value=two_leg_oco_rows)
        client.get_super_orders = Mock(return_value=[])

        # Against a 10-share position, the 5-share OCO pair must only count as 5 and block startup!
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}]
        )
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)
        ok_10, msg_10 = gate.run_startup_reconciliation()
        self.assertFalse(ok_10)
        self.assertIn("verified_protected_qty=5", msg_10)

        # Against a 5-share position, the 5-share OCO pair counts as 5 and allows startup!
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 5}]
        )
        ok_5, msg_5 = gate.run_startup_reconciliation()
        self.assertTrue(ok_5, msg_5)

    def test_C4_coverage_for_wrong_product_or_segment_is_rejected(self):
        """C.4: Coverage for a different productType or exchangeSegment cannot cover an open position."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        client.get_fund_limits = Mock(return_value=(True, 100000.0, "OK"))
        client.get_trade_book = Mock(return_value=[])
        client.get_positions = Mock(
            return_value=[{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 5}]
        )
        # Forever OCO is for MTF / BSE_EQ instead of CNC / NSE_EQ
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-WRONG-PROD",
                    "orderStatus": "PENDING",
                    "securityId": "3045",
                    "exchangeSegment": "BSE_EQ",
                    "productType": "MTF",
                    "transactionType": "SELL",
                    "quantity": 5,
                    "quantity1": 5,
                    "triggerPrice": 97.0,
                    "price1": 103.0,
                }
            ]
        )
        client.get_super_orders = Mock(return_value=[])

        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)
        ok, msg = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertIn("verified_protected_qty=0", msg)

    def test_C5_failed_positions_trade_book_or_required_protection_reads_keep_startup_blocked(self):
        """C.5: Failed HTTP 500 or network exception on positions, trade_book, or protection reads blocks startup."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        # 1. Verify DhanClient.get_positions() on HTTP 500 does NOT return []
        client.session.get = Mock(return_value=SimpleNamespace(status_code=500, text="Internal Server Error", json=lambda: {}))
        pos_fail = client.get_positions()
        self.assertNotEqual(pos_fail, [])
        self.assertFalse(getattr(pos_fail, "ok", True))

        # 2. Failed get_positions() blocks startup reconciliation
        def get_router_pos_fail(url, **kwargs):
            if "/fundlimit" in url:
                return SimpleNamespace(status_code=200, text="{}", json=lambda: {"availabelBalance": 100000.0})
            if "/trades" in url:
                return SimpleNamespace(status_code=200, text="[]", json=lambda: [])
            if "/positions" in url:
                return SimpleNamespace(status_code=503, text="Service Unavailable", json=lambda: {})
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = Mock(side_effect=get_router_pos_fail)
        ok_pos, msg_pos = gate.run_startup_reconciliation()
        self.assertFalse(ok_pos)
        self.assertFalse(gate.is_accepting_entries)

        # 3. Failed get_trade_book() blocks startup reconciliation
        def get_router_tb_fail(url, **kwargs):
            if "/fundlimit" in url:
                return SimpleNamespace(status_code=200, text="{}", json=lambda: {"availabelBalance": 100000.0})
            if "/trades" in url:
                return SimpleNamespace(status_code=500, text="Error", json=lambda: {})
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = Mock(side_effect=get_router_tb_fail)
        ok_tb, _ = gate.run_startup_reconciliation()
        self.assertFalse(ok_tb)

    def test_C6_malformed_responses_keep_startup_blocked(self):
        """C.6: Malformed JSON dict (e.g. {'error': 'bad'}) instead of list on positions/trades blocks startup."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        def get_router_malformed(url, **kwargs):
            if "/fundlimit" in url:
                return SimpleNamespace(status_code=200, text="{}", json=lambda: {"availabelBalance": 100000.0})
            if "/trades" in url:
                return SimpleNamespace(status_code=200, text="[]", json=lambda: [])
            if "/positions" in url:
                return SimpleNamespace(status_code=200, text='{"error":"malformed"}', json=lambda: {"error": "malformed"})
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = Mock(side_effect=get_router_malformed)
        pos_malformed = client.get_positions()
        self.assertNotEqual(pos_malformed, [])
        self.assertEqual(getattr(pos_malformed, "status", ""), "MALFORMED")

        ok, _ = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(gate.is_accepting_entries)

    def test_C7_genuinely_empty_account_with_successful_reads_can_start(self):
        """C.7: A genuinely empty account with HTTP 200 [] reads starts cleanly."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        def get_router_empty(url, **kwargs):
            if "/fundlimit" in url:
                return SimpleNamespace(status_code=200, text="{}", json=lambda: {"availabelBalance": 100000.0})
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = Mock(side_effect=get_router_empty)
        ok, msg = gate.run_startup_reconciliation()
        self.assertTrue(ok, msg)
        self.assertTrue(gate.is_accepting_entries)

    def test_C8_correctly_protected_account_can_start(self):
        """C.8: Account with open position and matching verified active broker protection starts cleanly."""
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client)
        gate = StartupReconciliationGate(client, self.db, watcher, self.stop)

        def get_router_protected(url, **kwargs):
            if "/fundlimit" in url:
                return SimpleNamespace(status_code=200, text="{}", json=lambda: {"availabelBalance": 100000.0})
            if "/positions" in url:
                return SimpleNamespace(
                    status_code=200,
                    text="[...]",
                    json=lambda: [{"securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 5}],
                )
            if "/forever/all" in url:
                return SimpleNamespace(
                    status_code=200,
                    text="[...]",
                    json=lambda: [
                        {
                            "orderId": "OCO-VALID-5",
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
                    ],
                )
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = Mock(side_effect=get_router_protected)
        ok, msg = gate.run_startup_reconciliation()
        self.assertTrue(ok, msg)
        self.assertTrue(gate.is_accepting_entries)

    # =========================================================================
    # SECTION 6: Automatic Retry Reached Through Real Production Watcher Path
    # =========================================================================

    def test_R1_production_watcher_timeout_cancel_and_confirmed_cancelled_triggers_single_retry(self):
        """
        Section 6: Reaches automatic retry through the real ProtectionWatcher.check_once() path
        (without calling execute_retry() directly):
        1. Order fills 4 of 10 shares before 60s timeout.
        2. At t=1061s, watcher protects the 4 filled shares AND requests cancellation of the remainder.
           While broker status is still PART_TRADED / CANCEL_PENDING, no retry is placed.
        3. At t=1063s, broker confirms terminal CANCELLED with filledQty=4.
           Watcher reconciles that all 4 filled shares are protected, fetches a fresh quote (<1.00% move),
           reserves the single retry slot, and places a retry order for the remaining 6 shares.
        4. When the child retry order later times out and cancels, zero further retries occur.
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
            "ORD-PARENT-1",
            "SBIN",
            "3045",
            "NSE_EQ",
            qty=10,
            product_type="CNC",
            original_market_price=100.0,
        )

        # Pass 1 at t=1061s: timeout reached (61s >= 60s), 4 shares filled @ 100.0, status still PART_TRADED
        now_val[0] = 1061.0
        client.cancel_order = Mock(return_value=(True, "Cancel ACK 202"))
        client.order_status = Mock(
            return_value={"orderStatus": "PART_TRADED", "filledQty": 4, "averageTradedPrice": 100.0}
        )
        client.trades_avg_price = Mock(return_value=100.0)
        client.get_positions = Mock(return_value=[{"securityId": "3045", "netQty": 4}])
        client.place_forever_oco = Mock(return_value=(True, "OCO-PART-4", "Placed"))
        client.get_forever_orders = Mock(
            return_value=[
                {
                    "orderId": "OCO-PART-4",
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
        client.place_order = Mock(return_value=(True, "ORD-CHILD-RETRY-1", "Placed"))
        client.get_yahoo_quote_details = Mock(
            return_value=QuoteResult(
                is_valid=True,
                price=100.5,
                source="YAHOO_FINANCE",
                ticker="SBIN.NS",
                venue="NSE",
                security_id="3045",
                currency="INR",
                market_state="REGULAR",
                quote_timestamp=1063.0,
                age_sec=0.1,
            )
        )

        watcher.check_once()
        client.cancel_order.assert_called_once_with("ORD-PARENT-1")
        self.assertEqual(watcher.pending["ORD-PARENT-1"]["protected_qty"], 4)
        # Cancel was only acknowledged (CANCEL_PENDING), not yet terminal CANCELLED -> no retry yet!
        client.place_order.assert_not_called()

        # Pass 2 at t=1063s: Broker confirms terminal CANCELLED with filledQty=4
        now_val[0] = 1063.0
        client.order_status = Mock(
            side_effect=lambda oid: (
                {"orderStatus": "CANCELLED", "filledQty": 4, "averageTradedPrice": 100.0}
                if oid == "ORD-PARENT-1"
                else {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
            )
        )
        watcher.check_once()

        # Automatic retry must have been triggered through check_once() for the remaining 6 shares!
        client.place_order.assert_called_once()
        placed_req = client.place_order.call_args[0][0]
        self.assertEqual(placed_req.quantity, 6)
        self.assertEqual(placed_req.security_id, "3045")
        self.assertIn("ORD-CHILD-RETRY-1", watcher.pending)
        self.assertEqual(watcher.pending["ORD-CHILD-RETRY-1"]["retry_count"], 1)

        # Pass 3 at t=1066s: Child retry order times out after retry_timeout_sec (2.0s) and cancels -> NO second retry!
        now_val[0] = 1066.0
        watcher.check_once()
        now_val[0] = 1067.0
        client.order_status = Mock(
            return_value={"orderStatus": "CANCELLED", "filledQty": 0, "averageTradedPrice": 0.0}
        )
        watcher.check_once()
        self.assertEqual(client.place_order.call_count, 1)


    def test_B7_rejected_strict_result_cannot_be_overridden_by_watcher(self):
        """B.7: When strict verify_super_order_legs_active returns False, watcher never overrides it via order_status."""
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.trades_avg_price.return_value = 100.0
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.verify_super_order_legs_active.return_value = (False, "Strict verifier rejected Super legs")
        # Even if order_status returns two PENDING legs and matching top-level prices, strict False cannot be overridden!
        broker.order_status.return_value = {
            "orderId": "SUP-B7",
            "securityId": "54231",
            "orderStatus": "TRADED",
            "filledQty": 100,
            "averageTradedPrice": 100.0,
            "stopLossPrice": 97.0,
            "targetPrice": 103.0,
            "legDetails": [
                {"legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 97.0},
                {"legName": "TARGET_LEG", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
            ],
        }
        watcher = self._make_watcher(broker)
        watcher.track("SUP-B7", "POLYCAB", "54231", "NSE_FNO", qty=100, product_type="MARGIN", is_super_order=True)
        watcher.check_once()
        self.assertEqual(watcher.pending["SUP-B7"]["protected_qty"], 0)
        self.assertEqual(watcher.pending["SUP-B7"]["reanchor_status"], "UNVERIFIED")

    def test_R2_production_watcher_super_order_timeout_cancel_retries_via_place_super_order(self):
        """
        Section 6: F&O Super Order reaching retry through the real ProtectionWatcher.check_once() path
        routes through place_super_order and preserves Super Order metadata.
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
            "SUP-PARENT-1",
            "POLYCAB",
            "54231",
            "NSE_FNO",
            qty=200,
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

        # Pass 1 at t=1185s: F&O 180s timeout reached; requests ENTRY_LEG cancel
        now_val[0] = 1185.0
        client.cancel_super_order_leg = Mock(return_value=(True, "Super cancel ACK 202"))
        client.order_status = Mock(return_value={"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0})
        client.place_super_order = Mock(return_value=(True, "SUP-CHILD-RETRY-1", "Placed"))
        client.place_order = Mock(return_value=(True, "WRONG-ROUTE", "Placed"))
        client.get_ltp = Mock(return_value=(True, 100.4, "Dhan LTP OK"))

        watcher.check_once()
        client.cancel_super_order_leg.assert_called_once_with("SUP-PARENT-1", "ENTRY_LEG")
        client.place_super_order.assert_not_called()

        # Pass 2 at t=1187s: Broker confirms terminal CANCELLED with 0 fills -> triggers Super Order retry!
        now_val[0] = 1187.0
        client.order_status = Mock(
            side_effect=lambda oid: (
                {"orderStatus": "CANCELLED", "filledQty": 0, "averageTradedPrice": 0.0}
                if oid == "SUP-PARENT-1"
                else {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0.0}
            )
        )
        watcher.check_once()

        client.place_super_order.assert_called_once()
        client.place_order.assert_not_called()
        self.assertIn("SUP-CHILD-RETRY-1", watcher.pending)
        self.assertTrue(watcher.pending["SUP-CHILD-RETRY-1"]["is_super_order"])
        self.assertEqual(watcher.pending["SUP-CHILD-RETRY-1"]["retry_count"], 1)


    def test_D1_super_order_delayed_verification_reconciles_on_second_pass_without_duplicate_put(self):
        """
        Discrepancy Fix D1: When a Super Order reanchor PUT succeeds on pass 1 but broker leg readback
        is initially delayed, pass 2 reconciles the existing protection_attempt and confirms active legs
        without issuing a duplicate modify_super_order_protection PUT or hitting a UNIQUE constraint error.
        """
        client = DhanClient(self.cfg, self.stop)
        watcher = self._make_watcher(client, clock_fn=lambda: 1000.0)
        watcher.track("SUP-D1", "POLYCAB", "54231", "NSE_FNO", qty=100, product_type="MARGIN", is_super_order=True)

        client.order_status = Mock(return_value={"orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 100.0})
        client.trades_avg_price = Mock(return_value=100.0)
        client.modify_super_order_protection = Mock(return_value=(True, "Updated"))
        # Pass 1: Super order book readback not yet updated (empty)
        client.get_super_orders = Mock(return_value=[])

        watcher.check_once()
        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertEqual(watcher.pending["SUP-D1"]["protected_qty"], 0)
        self.assertEqual(watcher.pending["SUP-D1"]["reanchor_status"], "UNVERIFIED")
        self.assertEqual(self.db.get_dashboard_summary()["unresolved_protection_attempts"], 1)

        # Pass 2: Broker Super Order book now shows both active legs
        client.get_super_orders = Mock(
            return_value=[
                {
                    "orderId": "SUP-D1",
                    "dhanClientId": "1113731536",
                    "securityId": "54231",
                    "exchangeSegment": "NSE_FNO",
                    "productType": "MARGIN",
                    "orderStatus": "TRADED",
                    "filledQty": 100,
                    "averageTradedPrice": 100.0,
                    "legDetails": [
                        {"legName": "STOP_LOSS_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 97.0},
                        {"legName": "TARGET_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
                    ],
                }
            ]
        )
        watcher.clock = lambda: 1065.0
        watcher.check_once()

        # Must NOT have called modify_super_order_protection a second time!
        self.assertEqual(client.modify_super_order_protection.call_count, 1)
        self.assertIn("SUP-D1", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-D1"]["protected_qty"], 100)
        self.assertEqual(watcher.terminal_orders["SUP-D1"]["reanchor_status"], "CONFIRMED")
        self.assertEqual(self.db.get_dashboard_summary()["unresolved_protection_attempts"], 0)

    def test_D2_retry_records_in_orders_table_and_env_example_parity(self):
        """
        Discrepancy Fix D2: execute_retry records the child order in the `orders` table (for risk/dashboard
        visibility), and `.env.example` documents all key V13 settings.
        """
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.stop_manager = self.stop
        broker.place_order.return_value = (True, "ORD-RETRY-DB", "OK")

        watcher = self._make_watcher(broker)
        watcher.track("ORD-PARENT-DB", "SBIN", "3045", "NSE_EQ", qty=10, product_type="CNC", original_market_price=100.0)
        watcher.pending["ORD-PARENT-DB"]["state"] = "CANCELLED"

        ok, child_id, _ = watcher.execute_retry("ORD-PARENT-DB", current_market_price=100.4, quote_timestamp=1000.0)
        self.assertTrue(ok)
        self.assertEqual(child_id, "ORD-RETRY-DB")

        with self.db._get_conn() as conn:
            row = conn.execute("SELECT * FROM orders WHERE broker_order_id = 'ORD-RETRY-DB'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["quantity"], 10)

        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        candidates = [
            os.path.join(root_dir, ".env.example"),
            os.path.join(os.getcwd(), ".env.example"),
            os.path.join(root_dir, ".env"),
            os.path.join(os.getcwd(), ".env"),
        ]
        target_env = next((p for p in candidates if os.path.exists(p)), None)
        if target_env:
            with open(target_env, "r", encoding="utf-8") as f:
                env_ex = f.read()
            for key in (
                "TG_BOT_TOKEN",
                "MAX_DRAWDOWN_PCT",
                "MAX_QUOTE_AGE_SEC",
                "PARTIAL_FILL_RETRY_TIMEOUT_SEC",
                "SCRIP_MASTER_MAX_AGE_SEC",
                "ALLOW_NEXT_MONTH_EXPIRY_FALLBACK",
            ):
                self.assertIn(key, env_ex)


if __name__ == "__main__":
    unittest.main()
