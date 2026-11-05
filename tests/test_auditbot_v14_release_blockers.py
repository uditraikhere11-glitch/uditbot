"""Independent offline regression tests for AuditBot V14.

Copy this file to tests/test_auditbot_v14_release_blockers.py and run from
the repository root:
    python -m unittest -v tests.test_auditbot_v14_release_blockers

Expected against the supplied 2026-11-04 V14 ZIP: 3 test failures, plus a
Pydantic initialization error in the installed Python 3.13/Pydantic 2.13 env.
No broker credentials or real HTTP are needed; all broker methods are mocked.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from src.config import AppConfig
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import TradingMode, PositionLifecycleState
from src.parsers.gemini_fallback import GeminiFnOCandidate


def _super_row(order_id: str, *, status: str = "TRADED", target: str = "PENDING", stop: str = "PENDING") -> dict:
    def leg(name, state, suffix, price):
        return {"orderId":f"{order_id}-{suffix}","legName":name,"transactionType":"SELL",
                "orderStatus":state,"price":price,"remainingQuantity":10 if state=="PENDING" else 0,
                "triggeredQuantity":0}
    return {"orderId":order_id, "orderStatus":status, "dhanClientId":"1100000001",
            "securityId":"55001", "exchangeSegment":"NSE_FNO", "productType":"MARGIN",
            "transactionType":"BUY", "quantity":10, "filledQty":10,
            "legDetails":[leg("TARGET_LEG",target,"TGT",103.0),leg("STOP_LOSS_LEG",stop,"SL",97.0)]}


class AuditBotV14IndependentRegressions(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        d=os.path.join(self.tmp.name,"data")
        os.makedirs(d,exist_ok=True)
        self.cfg=AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="OFFLINE_TEST_ONLY",
            dhan_token_file=os.path.join(self.tmp.name,"unused.token"),
            data_dir=d,
            journal_db_path=os.path.join(d,"journal.db"),
            monitored_channel_ids=[-1001],
            log_group_id=-1002,
        )
        self.db=JournalDB(self.cfg.journal_db_path, account_id=self.cfg.dhan_client_id)
        self.broker=MagicMock()
        self.broker.cfg=self.cfg
        self.broker.order_status.return_value={"orderId":"ORDER-A","orderStatus":"TRADED","filledQty":10,"averageTradedPrice":100.0}
        self.broker.trades_avg_price.return_value=100.0
        self.broker.modify_super_order_protection.return_value=(True,"OK")
        self.broker.get_super_orders.return_value=[_super_row("ORDER-A")]
        self.broker.get_trade_book.return_value=[]
        self.broker.get_trades_for_order.return_value=[]
        self.broker.get_forever_orders.return_value=[]
        self.broker.get_positions.return_value=[{"securityId":"55001","exchangeSegment":"NSE_FNO","productType":"MARGIN","netQty":10}]
        self.broker.get_holdings.return_value=[]
        self.watcher=ProtectionWatcher(
            broker=self.broker,path=os.path.join(d,"pending.json"),
            dry_run=False,journal_db=self.db,clock=lambda:1000.0,
        )
        self.watcher.track(order_id="ORDER-A",symbol="POLYCAB",security_id="55001",
                           segment="NSE_FNO",qty=10,product_type="MARGIN",
                           is_super_order=True,protection_route="SUPER_ORDER")
        self.watcher.check_once()

    def tearDown(self):
        self.tmp.cleanup()

    def test_unrelated_same_instrument_sell_cannot_reduce_this_position(self):
        self.broker.get_trade_book.return_value=[{
            "orderId":"UNRELATED-ORDER-B","exchangeTradeId":"TRADE-B","transactionType":"SELL",
            "securityId":"55001","exchangeSegment":"NSE_FNO","productType":"MARGIN",
            "tradedQuantity":5,"tradedPrice":103.0,
        }]
        self.watcher.check_once()
        r=self.watcher.terminal_orders["ORDER-A"]
        self.assertEqual(int(r.get("exit_filled_qty") or 0),0,
                         "An unrelated SELL must not be credited to this parent order")
        self.assertNotEqual(r.get("position_state"),PositionLifecycleState.EXIT_PART_FILLED.value)

    def test_cancel_one_leg_success_other_failure_is_not_complete(self):
        self.broker.get_positions.return_value=[{
            "securityId":"55001","exchangeSegment":"NSE_FNO","productType":"MARGIN","netQty":0,
        }]
        self.broker.cancel_super_order_leg.side_effect=[(True,"TARGET cancelled"),(False,"STOP failed")]
        self.watcher.check_once()
        r=self.watcher.terminal_orders["ORDER-A"]
        self.assertFalse(r.get("orphaned_legs_cancelled",False),
                         "Cancellation remains unresolved until both legs are broker-confirmed terminal")

    def test_parent_closed_without_leg_confirmation_must_not_fabricate_target(self):
        self.broker.get_super_orders.return_value=[_super_row("ORDER-A",status="CLOSED")]
        self.watcher.check_once()
        r=self.watcher.terminal_orders["ORDER-A"]
        self.assertFalse(r.get("triggered_leg"),
                         "Parent CLOSED without identified trigger is ambiguous, not a TARGET event")
        self.assertNotEqual(r.get("position_state"),PositionLifecycleState.POSITION_CLOSED.value)

    def test_gemini_candidate_initialization_is_compatible_with_pydantic(self):
        c=GeminiFnOCandidate(is_option_buy_entry=True,symbol="POLYCAB",strike=8000.0,
                            option_type="PE",expiry_month="OCT",entry_min=95.0,entry_max=100.0)
        self.assertTrue(c.is_strictly_valid()[0])


from src.engine.reconciliation import StartupReconciliationGate
from src.engine.emergency_stop import EmergencyStopManager
from src.notifier.execution_updates import OrderStatusReporter
from src.parsers.gemini_fallback import GeminiEquityCandidate
from src.version import (
    APP_RELEASE_VERSION,
    PARSER_RULE_VERSION,
    compare_semver,
    get_app_version,
    get_release_summary,
    is_valid_semver,
    verify_release_version_governance,
)


class AuditBotV14ExtendedReleaseGateTests(unittest.TestCase):
    """Extended independent regression tests covering P0-A, P0-B, P0-C, P1-D, allowlist isolation, and release versioning."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="OFFLINE_TEST_ONLY",
            dhan_token_file=os.path.join(self.tmp.name, "unused.token"),
            data_dir=self.data_dir,
            journal_db_path=os.path.join(self.data_dir, "journal.db"),
            monitored_channel_ids=[-1001],
            log_group_id=-1002,
            test_group_id=-1003,
        )
        self.db = JournalDB(self.cfg.journal_db_path, account_id=self.cfg.dhan_client_id)
        self.stop_mgr = EmergencyStopManager(os.path.join(self.data_dir, "emergency_stop.json"), expected_account_id=self.cfg.dhan_client_id)
        self.stop_mgr.provision_initial_clean_state("Udit", self.cfg.dhan_client_id)

        self.broker = MagicMock()
        self.broker.cfg = self.cfg
        self.gate = StartupReconciliationGate(self.broker, self.db, None, self.stop_mgr)
        self.gate._accepting_entries = True
        self.broker.reconciliation_gate = self.gate

        self.broker.order_status.side_effect = lambda oid, **kw: {
            "orderId": oid,
            "orderStatus": "TRADED",
            "filledQty": 10,
            "averageTradedPrice": 100.0,
        }
        self.broker.trades_avg_price.return_value = 100.0
        self.broker.modify_super_order_protection.return_value = (True, "OK")
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A"), _super_row("ORDER-B")]
        self.broker.get_trade_book.return_value = []
        self.broker.get_trades_for_order.return_value = []
        self.broker.get_forever_orders.return_value = []
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 20}
        ]
        self.broker.get_holdings.return_value = []

        self.watcher = ProtectionWatcher(
            broker=self.broker,
            path=os.path.join(self.data_dir, "pending.json"),
            dry_run=False,
            journal_db=self.db,
            clock=lambda: 1000.0,
        )
        self.watcher.execution_reporter = OrderStatusReporter(self.db, self.broker)

    def tearDown(self):
        self.tmp.cleanup()

    def test_p0_a_multiple_same_symbol_parents_and_restart_attribution(self):
        """P0-A: Overlapping bot positions in the same contract attribute fills strictly to the verified parent leg."""
        meta_a = {"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 990.0}
        meta_b = {"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 995.0}
        self.watcher.track(order_id="ORDER-A", symbol="POLYCAB", security_id="55001", segment="NSE_FNO", qty=10, product_type="MARGIN", is_super_order=True, protection_route="SUPER_ORDER", notification_meta=meta_a)
        self.watcher.track(order_id="ORDER-B", symbol="POLYCAB", security_id="55001", segment="NSE_FNO", qty=10, product_type="MARGIN", is_super_order=True, protection_route="SUPER_ORDER", notification_meta=meta_b)
        self.watcher.check_once()

        # 1. Unlinked manual SELL order + unlinked order-specific tradebook data + missing exchangeTradeId
        self.broker.get_trade_book.return_value = [
            {
                "orderId": "MANUAL-SELL-99",
                "exchangeTradeId": "TRD-MANUAL-99",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 4,
                "tradedPrice": 103.0,
            },
            # Partial fill of 4 units specifically on ORDER-B's target leg (with duplicate row in tradebook)
            {
                "orderId": "ORDER-B-TGT",
                "exchangeTradeId": "TRD-B-PART-1",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 4,
                "tradedPrice": 103.0,
            },
            {
                "orderId": "ORDER-B-TGT",
                "exchangeTradeId": "TRD-B-PART-1",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 4,
                "tradedPrice": 103.0,
            },
        ]
        # Per-order endpoint returns unlinked orderId when queried for ORDER-A-TGT
        def mock_trades_for_order(oid, **kw):
            if oid == "ORDER-A-TGT":
                return [
                    {
                        "orderId": "ORDER-B-TGT",
                        "exchangeTradeId": "TRD-B-PART-1",
                        "transactionType": "SELL",
                        "securityId": "55001",
                        "exchangeSegment": "NSE_FNO",
                        "productType": "MARGIN",
                        "tradedQuantity": 4,
                        "tradedPrice": 103.0,
                    }
                ]
            return []

        self.broker.get_trades_for_order.side_effect = mock_trades_for_order
        self.broker.get_super_orders.return_value = [
            _super_row("ORDER-A"),
            _super_row("ORDER-B", target="PART_TRADED"),
        ]
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 16}
        ]
        self.watcher.check_once()

        rec_a = self.watcher.terminal_orders["ORDER-A"]
        rec_b = self.watcher.terminal_orders["ORDER-B"]
        self.assertEqual(int(rec_a.get("exit_filled_qty") or 0), 0)
        self.assertEqual(rec_a.get("position_state"), PositionLifecycleState.PROTECTED.value)
        self.assertEqual(int(rec_b.get("exit_filled_qty") or 0), 4)
        self.assertEqual(rec_b.get("position_state"), PositionLifecycleState.EXIT_PART_FILLED.value)
        self.assertEqual(rec_b.get("realized_pnl"), 12.0)

        # 2. Restart recovery preserves attribution and completes ORDER-B exit without touching ORDER-A
        restarted_watcher = ProtectionWatcher(
            broker=self.broker,
            path=os.path.join(self.data_dir, "pending.json"),
            dry_run=False,
            journal_db=self.db,
            clock=lambda: 1010.0,
        )
        restarted_watcher.execution_reporter = OrderStatusReporter(self.db, self.broker)
        self.broker.get_trade_book.return_value.append(
            {
                "orderId": "ORDER-B-TGT",
                "exchangeTradeId": "TRD-B-PART-2",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 6,
                "tradedPrice": 103.0,
            }
        )
        self.broker.get_super_orders.return_value = [
            _super_row("ORDER-A"),
            _super_row("ORDER-B", status="CLOSED", target="TRADED"),
        ]
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 10}
        ]
        restarted_watcher.check_once()

        self.assertEqual(int(restarted_watcher.terminal_orders["ORDER-A"].get("exit_filled_qty") or 0), 0)
        self.assertEqual(restarted_watcher.terminal_orders["ORDER-A"].get("position_state"), PositionLifecycleState.PROTECTED.value)
        self.assertEqual(int(restarted_watcher.terminal_orders["ORDER-B"].get("exit_filled_qty") or 0), 10)
        self.assertEqual(restarted_watcher.terminal_orders["ORDER-B"].get("realized_pnl"), 30.0)

    def test_p0_b_per_leg_cancellation_states_and_eventual_confirmation(self):
        """P0-B: Tests TARGET/STOP cancel combinations (failure/success, pending/timeout, eventual broker confirmation)."""
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A")]
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 10}
        ]
        self.watcher.track(order_id="ORDER-A", symbol="POLYCAB", security_id="55001", segment="NSE_FNO", qty=10, product_type="MARGIN", is_super_order=True, protection_route="SUPER_ORDER")
        self.watcher.check_once()

        # Pass 1: Manual exit flattens position; TARGET cancel fails, STOP cancel succeeds (ACK)
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 0}
        ]
        self.broker.cancel_super_order_leg.side_effect = [(False, "TARGET timeout"), (True, "STOP ACK")]
        self.watcher.check_once()
        r = self.watcher.terminal_orders["ORDER-A"]
        self.assertFalse(r.get("orphaned_legs_cancelled", False))
        self.assertEqual(r["orphaned_leg_cancel_states"]["TARGET_LEG"], "REJECTED")
        self.assertEqual(r["orphaned_leg_cancel_states"]["STOP_LOSS_LEG"], "ACK_UNKNOWN")

        # Pass 2 (after retry backoff window): Broker book confirms STOP_LOSS_LEG is CANCELLED, TARGET_LEG still PENDING; retry cancels TARGET_LEG only
        self.watcher.clock = lambda: 1010.0
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A", target="PENDING", stop="CANCELLED")]
        self.broker.cancel_super_order_leg.side_effect = [(True, "TARGET ACK")]
        self.broker.cancel_super_order_leg.reset_mock()
        self.watcher.check_once()
        self.assertEqual(self.broker.cancel_super_order_leg.call_count, 1)
        self.assertEqual(self.broker.cancel_super_order_leg.call_args[0], ("ORDER-A", "TARGET_LEG"))
        self.assertFalse(r.get("orphaned_legs_cancelled", False))
        self.assertEqual(r["orphaned_leg_cancel_states"]["STOP_LOSS_LEG"], "CONFIRMED_TERMINAL")
        self.assertEqual(r["orphaned_leg_cancel_states"]["TARGET_LEG"], "ACK_UNKNOWN")

        # Pass 3: Broker book confirms both TARGET_LEG and STOP_LOSS_LEG are CANCELLED -> aggregate complete = True
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A", target="CANCELLED", stop="CANCELLED")]
        self.broker.cancel_super_order_leg.reset_mock()
        self.watcher.check_once()
        self.assertEqual(self.broker.cancel_super_order_leg.call_count, 0)
        self.assertTrue(r.get("orphaned_legs_cancelled", False))

    def test_p0_c_super_order_closed_states_and_notification_accuracy(self):
        """P0-C: Tests CLOSED with no leg, contradictory legs, SL-only trigger, TARGET-only trigger, and verified SELL fill."""
        import json as _json
        meta = {"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 990.0}
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A")]
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 10}
        ]
        self.watcher.track(order_id="ORDER-A", symbol="POLYCAB", security_id="55001", segment="NSE_FNO", qty=10, product_type="MARGIN", is_super_order=True, protection_route="SUPER_ORDER", notification_meta=meta)
        self.watcher.check_once()

        # 1. Contradictory legs (both TRIGGERED) with parent CLOSED -> ambiguous, no target fabrication
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A", status="CLOSED", target="TRIGGERED", stop="TRIGGERED")]
        self.watcher.check_once()
        r = self.watcher.terminal_orders["ORDER-A"]
        self.assertFalse(r.get("triggered_leg"))
        self.assertTrue(r.get("ambiguous_super_closed"))
        self.assertFalse(self.gate.is_accepting_entries)

        # 2. SL-only trigger -> STOP_LOSS_LEG identified, EXIT_TRIGGERED card emitted (never Target Hit)
        self.broker.get_super_orders.return_value = [_super_row("ORDER-A", status="CLOSED", target="CANCELLED", stop="TRIGGERED")]
        self.watcher.check_once()
        self.assertEqual(r.get("triggered_leg"), "STOP_LOSS_LEG")
        self.assertEqual(r.get("position_state"), PositionLifecycleState.EXIT_TRIGGERED.value)

        # 3. Verified SELL fill on ORDER-A-SL -> POSITION_CLOSED (Stop-Loss Hit)
        self.broker.get_trade_book.return_value = [
            {
                "orderId": "ORDER-A-SL",
                "exchangeTradeId": "TRD-SL-10",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 10,
                "tradedPrice": 97.0,
            }
        ]
        self.broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 0}
        ]
        self.watcher.check_once()
        self.assertEqual(r.get("position_state"), PositionLifecycleState.POSITION_CLOSED.value)
        self.assertEqual(r.get("realized_pnl"), -30.0)
        cards = [_json.loads(row["payload"])["text"] for row in self.db.fetch_pending_outbox(limit=20)]
        self.assertTrue(any("Position Closed (Stop-Loss Hit)" in c for c in cards))
        self.assertFalse(any("Target Hit" in c for c in cards))

    def test_p1_d_pydantic_candidate_validation_init_model_validate_and_json(self):
        """P1-D: Validates GeminiFnOCandidate and GeminiEquityCandidate across __init__, model_validate, and JSON."""
        ok_fno = GeminiFnOCandidate.model_validate(
            {
                "is_option_buy_entry": True,
                "symbol": "POLYCAB",
                "strike": 8000.0,
                "option_type": "PE",
                "expiry_month": "OCT",
                "entry_min": 95.0,
                "entry_max": 100.0,
            }
        )
        self.assertTrue(ok_fno.is_strictly_valid()[0])

        ok_eq = GeminiEquityCandidate.model_validate_json(
            '{"is_buy_entry": true, "stock_symbol": "SBIN", "entry_price": 805.0}'
        )
        self.assertTrue(ok_eq.is_strictly_valid()[0])

        bad_bool_fno = GeminiFnOCandidate(
            is_option_buy_entry=True,
            symbol="POLYCAB",
            strike=True,
            option_type="PE",
            entry_min=95.0,
            entry_max=100.0,
        )
        self.assertFalse(bad_bool_fno.is_strictly_valid()[0])

        bad_bool_eq = GeminiEquityCandidate(
            is_buy_entry=True,
            stock_symbol="SBIN",
            entry_price=True,
        )
        self.assertFalse(bad_bool_eq.is_strictly_valid()[0])

    def test_source_allowlist_overlap_fail_closed_validation(self):
        """Verifies LOG_GROUP_ID cannot overlap with MONITORED_CHANNEL_IDS or TEST_GROUP_ID."""
        with self.assertRaises(ValueError):
            AppConfig(monitored_channel_ids=[-1001, -1002], log_group_id=-1002)
        with self.assertRaises(ValueError):
            AppConfig(monitored_channel_ids=[-1001], test_group_id=-1002, log_group_id=-1002)
        with self.assertRaises(ValueError):
            AppConfig(monitored_channel_ids=[-1001, -1002], test_group_id=-1002, log_group_id=-1003)

    def test_release_version_governance_and_namespace_isolation(self):
        """Verifies root VERSION, CHANGELOG.md, RELEASE_MANIFEST.json, and namespace separation from config hash."""
        ver = get_app_version()
        self.assertTrue(is_valid_semver(ver))
        self.assertEqual(ver, APP_RELEASE_VERSION)
        self.assertEqual(compare_semver("14.0.1-rc.1", "14.0.0"), 1)
        self.assertEqual(compare_semver("14.0.1", "14.0.1-rc.1"), 1)
        summary = get_release_summary(config_fingerprint="v-12345678")
        self.assertEqual(summary["app_release_version"], ver)
        self.assertEqual(summary["config_fingerprint"], "v-12345678")
        self.assertEqual(summary["parser_rule_version"], PARSER_RULE_VERSION)
        ok, errs = verify_release_version_governance()
        self.assertTrue(ok, f"Release version governance check failed: {errs}")


if __name__=="__main__":
    unittest.main()
