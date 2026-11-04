"""
AB-V14-PROTECTION-NOTIFY-FINAL-01 — Offline Regression Suite & Full-Day Simulator.
Covers all mandatory P0-01 through P1-13 requirements plus the full-day timestamped
YESBANK CNC and ₹100 F&O Stock Option lifecycle simulation with zero live broker mutations.
"""

from datetime import date
import io
import json
import os
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient

from src.broker.dhan_client import DhanClient
from src.cli import cmd_check_dhan_orders, cmd_order_details
from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker, compute_config_version
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.sizer import calculate_option_limit_price, calculate_protection_levels
from src.journal.db import JournalDB
from src.main import TradingBotPipeline
from src.models import (
    BrokerReadFailure,
    BrokerReadList,
    OrderRequest,
    OrderType,
    PositionLifecycleState,
    ProductType,
    ProtectionRoute,
    TradingMode,
    TransactionType,
    infer_record_protection_route,
    resolve_protection_route_capability,
)
from src.notifier.execution_updates import (
    OrderStatusReporter,
    format_exit_lifecycle_card,
    format_order_outcome,
    format_order_submitted,
    format_protection_lifecycle_card,
    format_signal_accepted,
)
from src.notifier.outbox_worker import OutboxWorker
from src.notifier.telegram_notifier import TelegramNotifier
from src.setup_app import SETUP_CSRF_TOKEN, app, build_env_and_params_dict, get_setup_page


def _active_super_order_record(
    order_id: str,
    security_id: str,
    segment: str,
    product_type: str,
    qty: int,
    sl_price: float,
    target_price: float,
    account_id: str = "1100000001",
    parent_status: str = "TRADED",
    target_status: str = "PENDING",
    sl_status: str = "PENDING",
    target_triggered_qty: int = 0,
    sl_triggered_qty: int = 0,
) -> dict:
    return {
        "dhanClientId": account_id,
        "orderId": order_id,
        "orderStatus": parent_status,
        "transactionType": "BUY",
        "exchangeSegment": segment,
        "productType": product_type,
        "securityId": security_id,
        "quantity": qty,
        "filledQty": qty,
        "legDetails": [
            {
                "orderId": f"{order_id}-TGT",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "orderStatus": target_status,
                "price": target_price,
                "targetPrice": target_price,
                "remainingQuantity": 0 if target_status in ("TRIGGERED", "TRADED", "CANCELLED") else qty,
                "triggeredQuantity": target_triggered_qty,
            },
            {
                "orderId": f"{order_id}-SL",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "orderStatus": sl_status,
                "price": sl_price,
                "stopLossPrice": sl_price,
                "remainingQuantity": 0 if sl_status in ("TRIGGERED", "TRADED", "CANCELLED") else qty,
                "triggeredQuantity": sl_triggered_qty,
            },
        ],
    }


class TestV14ProtectionNotifyFinal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.tmp.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.pending_path = os.path.join(self.data_dir, "pending_protection.json")
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="TEST_TOKEN_LOCAL",
            dhan_token_file=os.path.join(self.tmp.name, ".dhan_token"),
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            monitored_channel_ids=[-1001],
            log_group_id=-1002,
            tg_bot_token="123456:ABCDEF_TEST_BOT_TOKEN",
        )
        self.db = JournalDB(self.db_path, account_id=self.cfg.dhan_client_id)
        self.stop = EmergencyStopManager(self.stop_path, expected_account_id=self.cfg.dhan_client_id)
        self.stop.provision_initial_clean_state("Udit", self.cfg.dhan_client_id)

    def tearDown(self):
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # Scenario 1 (P0-01 & P0-02): Same-day CNC/MTF Super Order eligibility & capability matrix
    # ------------------------------------------------------------------
    def test_01_p0_01_p0_02_cnc_mtf_super_order_capability_and_protection_route(self):
        for seg in ("NSE_EQ", "BSE_EQ"):
            for prod in ("CNC", "MTF"):
                ok_sup, route_sup, _ = resolve_protection_route_capability(seg, prod, preferred_route="SUPER_ORDER")
                self.assertTrue(ok_sup)
                self.assertEqual(route_sup, "SUPER_ORDER")
                ok_fo, route_fo, _ = resolve_protection_route_capability(seg, prod, preferred_route="FOREVER_OCO")
                self.assertTrue(ok_fo)
                self.assertEqual(route_fo, "FOREVER_OCO")

        ok_fno, route_fno, _ = resolve_protection_route_capability("NSE_FNO", "MARGIN")
        self.assertTrue(ok_fno)
        self.assertEqual(route_fno, "SUPER_ORDER")
        ok_bad, route_bad, _ = resolve_protection_route_capability("NSE_FNO", "MARGIN", preferred_route="FOREVER_OCO")
        self.assertFalse(ok_bad)
        self.assertEqual(route_bad, "UNKNOWN")

        # Verify CNC Super Order re-anchors via SUPER_ORDER route (not FOREVER_OCO) when product_type is CNC!
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.trades_avg_price.return_value = 15.55
        broker.order_status.return_value = {"orderId": "SUP-EQ-CNC-1", "orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 15.55}
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.last_super_leg_mod_results = {
            "SUP-EQ-CNC-1": {"TARGET_LEG": {"state": "ACKED"}, "STOP_LOSS_LEG": {"state": "ACKED"}}
        }
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-EQ-CNC-1", "11915", "NSE_EQ", "CNC", 100, 15.05, 16.05)
        ]

        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.track(
            order_id="SUP-EQ-CNC-1",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=100,
            product_type="CNC",
            is_super_order=True,
            protection_route="SUPER_ORDER",
        )
        watcher.check_once()
        broker.modify_super_order_protection.assert_called_once()
        broker.place_forever_oco.assert_not_called()
        self.assertIn("SUP-EQ-CNC-1", watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders["SUP-EQ-CNC-1"]["protected_qty"], 100)
        self.assertEqual(watcher.terminal_orders["SUP-EQ-CNC-1"]["protection_route"], "SUPER_ORDER")

    # ------------------------------------------------------------------
    # Scenario 2 (P0-01): Equity Super Order rejection NEVER falls back to unprotected place_order
    # ------------------------------------------------------------------
    def test_02_p0_01_equity_super_order_failure_never_places_unprotected_buy(self):
        pipe = TradingBotPipeline(self.cfg)
        pipe.stop_manager.provision_initial_clean_state("Udit", self.cfg.dhan_client_id)
        pipe.reconciliation_gate._accepting_entries = True
        pipe.resolver.load_equity("YESBANK", "11915", "NSE_EQ", 0.05)
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 100000.0, "OK"))
        pipe.broker.get_positions = MagicMock(return_value=BrokerReadList([]))
        pipe.broker.place_super_order = MagicMock(return_value=(False, None, "Dhan super order rejected by RMS"))
        pipe.broker.place_order = MagicMock(return_value=(True, "UNPROTECTED-ORD", "Should never be called"))

        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res = pipe.process_raw_message(
                "Buy #YESBANK @ 15.50",
                channel_id=-1001,
                message_id=201,
                msg_timestamp=1730200000.0,
                current_time=1730200001.0,
            )
        self.assertFalse(res.is_valid)
        pipe.broker.place_super_order.assert_called_once()
        pipe.broker.place_order.assert_not_called()

    # ------------------------------------------------------------------
    # Scenario 3 (P0-02): Legacy route migration & UNKNOWN route fail-closed halt
    # ------------------------------------------------------------------
    def test_03_p0_02_legacy_route_inference_and_unknown_route_halts(self):
        self.assertEqual(infer_record_protection_route({"is_super_order": True, "product_type": "CNC", "segment": "NSE_EQ"}), "SUPER_ORDER")
        self.assertEqual(infer_record_protection_route({"is_super_order": False, "product_type": "CNC", "segment": "NSE_EQ"}), "FOREVER_OCO")
        self.assertEqual(infer_record_protection_route({"is_super_order": None, "product_type": "MARGIN", "segment": "NSE_FNO"}), "UNKNOWN")

        broker = MagicMock()
        broker.cfg = self.cfg
        gate = StartupReconciliationGate(broker, self.db, None, self.stop)
        gate._accepting_entries = True
        broker.reconciliation_gate = gate
        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.track(
            order_id="ORD-UNK-1",
            symbol="SBIN",
            security_id="3045",
            segment="NSE_EQ",
            qty=10,
            product_type="CNC",
            protection_route="UNKNOWN",
        )
        watcher.pending["ORD-UNK-1"]["protection_route"] = "UNKNOWN"
        watcher._protect(watcher.pending["ORD-UNK-1"], {"averageTradedPrice": 800.0}, 10, 10)
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(watcher.pending["ORD-UNK-1"]["protection_status"], "UNPROTECTED")

    # ------------------------------------------------------------------
    # Scenario 4 (P0-04 & P1-11): ₹100 Option + 4% Markup -> ₹104 Limit -> ₹100 Fill -> ₹97/₹103 Re-Anchor
    # ------------------------------------------------------------------
    def test_04_p0_04_p1_11_fno_initial_limit_relative_and_fill_relative_reanchor(self):
        limit_p = calculate_option_limit_price(100.0, tick_size=0.05, markup_pct=4.0)
        self.assertEqual(limit_p, 104.00)
        init_lvls = calculate_protection_levels(limit_p, quantity=125, sl_pct=3.0, target_pct=3.0, tick_size=0.05)
        self.assertEqual(init_lvls.sl_trigger, 100.85)
        self.assertEqual(init_lvls.target_price, 107.15)

        fill_lvls = calculate_protection_levels(100.0, quantity=125, sl_pct=3.0, target_pct=3.0, tick_size=0.05)
        self.assertEqual(fill_lvls.sl_trigger, 97.00)
        self.assertEqual(fill_lvls.target_price, 103.00)

    # ------------------------------------------------------------------
    # Scenario 5 (P0-04): Partial/non-atomic Super Order leg modification failure
    # ------------------------------------------------------------------
    def test_05_p0_04_partial_super_order_leg_modification_persists_per_leg_state(self):
        client = DhanClient(self.cfg, self.stop)
        put_calls = []

        def fake_put(url, headers=None, json=None, timeout=5):
            put_calls.append(json)
            if json.get("legName") == "TARGET_LEG":
                return SimpleNamespace(status_code=200, text='{"orderId":"SUP-PART-1","orderStatus":"PENDING"}', json=lambda: {"orderId": "SUP-PART-1", "orderStatus": "PENDING"})
            return SimpleNamespace(status_code=504, text="Gateway Timeout on STOP_LOSS_LEG", json=lambda: {})

        client.session.put = fake_put
        client.session.get = MagicMock(return_value=SimpleNamespace(status_code=200, text="[]", json=lambda: []))
        client.trades_avg_price = MagicMock(return_value=100.0)
        client.order_status = MagicMock(return_value={"orderId": "SUP-PART-1", "orderStatus": "TRADED", "filledQty": 125, "averageTradedPrice": 100.0})
        gate = StartupReconciliationGate(client, self.db, None, self.stop)
        gate._accepting_entries = True
        client.reconciliation_gate = gate

        watcher = ProtectionWatcher(broker=client, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.track(
            order_id="SUP-PART-1",
            symbol="POLYCAB",
            security_id="55001",
            segment="NSE_FNO",
            qty=125,
            product_type="MARGIN",
            is_super_order=True,
            protection_route="SUPER_ORDER",
        )
        watcher.check_once()
        self.assertEqual(len(put_calls), 2)
        self.assertEqual(watcher.pending["SUP-PART-1"]["target_leg_mod_state"], "ACKED")
        self.assertEqual(watcher.pending["SUP-PART-1"]["sl_leg_mod_state"], "UNKNOWN")
        self.assertEqual(watcher.pending["SUP-PART-1"]["reanchor_status"], "FAILED")
        self.assertEqual(watcher.pending["SUP-PART-1"]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries)
        attempts = self.db.get_protection_attempts("SUP-PART-1")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["target_leg_state"], "ACKED")
        self.assertEqual(attempts[0]["sl_leg_state"], "UNKNOWN")
        self.assertEqual(attempts[0]["state"], "UNKNOWN")

    # ------------------------------------------------------------------
    # Scenario 6 (P0-03): Super Order CLOSED / Leg TRIGGERED without SELL fill is EXIT_TRIGGERED, NOT closed
    # ------------------------------------------------------------------
    def test_06_p0_03_super_order_closed_or_leg_triggered_requires_sell_tradebook_fill(self):
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.trades_avg_price.return_value = 100.0
        broker.order_status.return_value = {"orderId": "SUP-EXIT-1", "orderStatus": "TRADED", "filledQty": 125, "averageTradedPrice": 100.0}
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-EXIT-1", "55001", "NSE_FNO", "MARGIN", 125, 97.0, 103.0)
        ]
        broker.get_trade_book.return_value = [
            {
                "orderId": "SUP-EXIT-1",
                "exchangeTradeId": "ET-BUY-1001",
                "transactionType": "BUY",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 125,
                "tradedPrice": 100.0,
            }
        ]
        broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 125}
        ]

        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.execution_reporter = OrderStatusReporter(self.db, broker)
        watcher.track(
            order_id="SUP-EXIT-1",
            symbol="POLYCAB",
            security_id="55001",
            segment="NSE_FNO",
            qty=125,
            product_type="MARGIN",
            is_super_order=True,
            protection_route="SUPER_ORDER",
            notification_meta={"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 999.0},
        )
        watcher.check_once()
        self.assertEqual(watcher.terminal_orders["SUP-EXIT-1"]["position_state"], PositionLifecycleState.PROTECTED.value)

        # Now Dhan Super Order parent becomes CLOSED and TARGET_LEG is TRIGGERED, but SELL fill is NOT in tradebook yet!
        broker.get_super_orders.return_value = [
            _active_super_order_record(
                "SUP-EXIT-1", "55001", "NSE_FNO", "MARGIN", 125, 97.0, 103.0,
                parent_status="CLOSED", target_status="TRIGGERED", target_triggered_qty=125,
            )
        ]
        watcher.check_once()
        rec = watcher.terminal_orders["SUP-EXIT-1"]
        self.assertEqual(rec["position_state"], PositionLifecycleState.EXIT_TRIGGERED.value)
        self.assertEqual(rec["confirmed_position_open_qty"], 125)
        outbox_rows = self.db.fetch_pending_outbox(limit=20)
        texts = [json.loads(r["payload"])["text"] for r in outbox_rows]
        self.assertTrue(any("Target Leg Triggered (Awaiting Fill)" in t for t in texts))
        self.assertFalse(any("Position Closed" in t for t in texts))

    # ------------------------------------------------------------------
    # Scenario 7 (P0-03 & P0-09): Partial exit fill -> Full exit fill & gross realized P&L
    # ------------------------------------------------------------------
    def test_07_p0_03_p0_09_partial_and_full_exit_fills_and_realized_pnl(self):
        broker = MagicMock()
        broker.cfg = self.cfg
        broker.trades_avg_price.return_value = 100.0
        broker.order_status.return_value = {"orderId": "SUP-PNL-1", "orderStatus": "TRADED", "filledQty": 100, "averageTradedPrice": 100.0}
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-PNL-1", "55001", "NSE_FNO", "MARGIN", 100, 97.0, 103.0)
        ]
        broker.get_trades_for_order.return_value = [
            {
                "orderId": "SUP-PNL-1",
                "exchangeTradeId": "TRD-BUY-1",
                "transactionType": "BUY",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 100,
                "tradedPrice": 100.0,
            }
        ]
        broker.get_trade_book.return_value = []
        broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 100}
        ]

        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.execution_reporter = OrderStatusReporter(self.db, broker)
        watcher.track(
            order_id="SUP-PNL-1",
            symbol="POLYCAB",
            security_id="55001",
            segment="NSE_FNO",
            qty=100,
            product_type="MARGIN",
            is_super_order=True,
            protection_route="SUPER_ORDER",
            notification_meta={"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 999.0},
        )
        watcher.check_once()

        # Partial SELL fill of 40 units @ ₹103.00
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-PNL-1", "55001", "NSE_FNO", "MARGIN", 100, 97.0, 103.0, parent_status="CLOSED", target_status="TRIGGERED", target_triggered_qty=100)
        ]
        broker.get_trade_book.return_value = [
            {
                "orderId": "SUP-PNL-1-TGT",
                "exchangeTradeId": "TRD-SELL-PART-1",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 40,
                "tradedPrice": 103.0,
            }
        ]
        broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 60}
        ]
        watcher.check_once()
        rec = watcher.terminal_orders["SUP-PNL-1"]
        self.assertEqual(rec["position_state"], PositionLifecycleState.EXIT_PART_FILLED.value)
        self.assertEqual(rec["confirmed_position_open_qty"], 60)
        self.assertEqual(rec["realized_pnl"], 120.00)

        # Remaining SELL fill of 60 units @ ₹103.00
        broker.get_trade_book.return_value.append(
            {
                "orderId": "SUP-PNL-1-TGT",
                "exchangeTradeId": "TRD-SELL-PART-2",
                "transactionType": "SELL",
                "securityId": "55001",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "tradedQuantity": 60,
                "tradedPrice": 103.0,
            }
        )
        broker.get_positions.return_value = [
            {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 0}
        ]
        watcher.check_once()
        self.assertEqual(rec["position_state"], PositionLifecycleState.POSITION_CLOSED.value)
        self.assertEqual(rec["confirmed_position_open_qty"], 0)
        self.assertEqual(rec["realized_pnl"], 300.00)
        acct = self.db.get_position_accounting("SUP-PNL-1")
        self.assertEqual(acct["buy_filled_qty"], 100)
        self.assertEqual(acct["sell_filled_qty"], 100)
        self.assertEqual(acct["realized_pnl"], 300.00)

    # ------------------------------------------------------------------
    # Scenario 8 (P0-05): Manual external exit with orphaned resting protective legs
    # ------------------------------------------------------------------
    def test_08_p0_05_manual_exit_detects_and_cancels_orphaned_protective_legs(self):
        broker = MagicMock()
        broker.cfg = self.cfg
        gate = StartupReconciliationGate(broker, self.db, None, self.stop)
        gate._accepting_entries = True
        broker.reconciliation_gate = gate
        broker.trades_avg_price.return_value = 15.55
        broker.order_status.return_value = {"orderId": "SUP-MAN-1", "orderStatus": "TRADED", "filledQty": 50, "averageTradedPrice": 15.55}
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.cancel_super_order_leg.return_value = (True, "Cancelled")
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-MAN-1", "11915", "NSE_EQ", "CNC", 50, 15.05, 16.05)
        ]
        broker.get_trade_book.return_value = []
        broker.get_positions.return_value = [
            {"securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 50}
        ]

        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.track(
            order_id="SUP-MAN-1",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=50,
            product_type="CNC",
            is_super_order=True,
            protection_route="SUPER_ORDER",
        )
        watcher.check_once()
        self.assertTrue(gate.is_accepting_entries)

        # Operator manually exits all 50 shares on Dhan app; netQty becomes 0 while Super Order legs are still PENDING
        broker.get_positions.return_value = [
            {"securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0}
        ]
        watcher.check_once()
        rec = watcher.terminal_orders["SUP-MAN-1"]
        self.assertTrue(rec["manual_exit_detected"])
        self.assertTrue(rec["orphaned_legs_detected"])
        # P0-B: Cancellation ACK alone is not terminal confirmation; orphaned_legs_cancelled stays False until broker book confirms CANCELLED
        self.assertFalse(rec["orphaned_legs_cancelled"])
        self.assertEqual(rec["orphaned_leg_cancel_states"]["TARGET_LEG"], "ACK_UNKNOWN")
        self.assertEqual(rec["orphaned_leg_cancel_states"]["STOP_LOSS_LEG"], "ACK_UNKNOWN")
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(broker.cancel_super_order_leg.call_count, 2)

        # Next pass: broker book confirms both legs CANCELLED -> orphaned_legs_cancelled becomes True without duplicate cancel calls
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-MAN-1", "11915", "NSE_EQ", "CNC", 50, 15.05, 16.05, target_status="CANCELLED", sl_status="CANCELLED")
        ]
        watcher.check_once()
        self.assertTrue(rec["orphaned_legs_cancelled"])
        self.assertEqual(rec["orphaned_leg_cancel_states"]["TARGET_LEG"], "CONFIRMED_TERMINAL")
        self.assertEqual(rec["orphaned_leg_cancel_states"]["STOP_LOSS_LEG"], "CONFIRMED_TERMINAL")
        self.assertEqual(broker.cancel_super_order_leg.call_count, 2)

    # ------------------------------------------------------------------
    # Scenario 9 (P0-05): Protective legs externally cancelled while position remains open
    # ------------------------------------------------------------------
    def test_09_p0_05_protective_legs_cancelled_while_position_open_marks_unprotected(self):
        broker = MagicMock()
        broker.cfg = self.cfg
        gate = StartupReconciliationGate(broker, self.db, None, self.stop)
        gate._accepting_entries = True
        broker.reconciliation_gate = gate
        broker.trades_avg_price.return_value = 15.55
        broker.order_status.return_value = {"orderId": "SUP-DEAD-1", "orderStatus": "TRADED", "filledQty": 50, "averageTradedPrice": 15.55}
        broker.modify_super_order_protection.return_value = (True, "OK")
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-DEAD-1", "11915", "NSE_EQ", "CNC", 50, 15.05, 16.05)
        ]
        broker.get_trade_book.return_value = []
        broker.get_positions.return_value = [
            {"securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 50}
        ]

        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.track(
            order_id="SUP-DEAD-1",
            symbol="YESBANK",
            security_id="11915",
            segment="NSE_EQ",
            qty=50,
            product_type="CNC",
            is_super_order=True,
            protection_route="SUPER_ORDER",
        )
        watcher.check_once()

        # Both protective legs become CANCELLED while netQty is still 50
        broker.get_super_orders.return_value = [
            _active_super_order_record("SUP-DEAD-1", "11915", "NSE_EQ", "CNC", 50, 15.05, 16.05, target_status="CANCELLED", sl_status="CANCELLED")
        ]
        watcher.check_once()
        self.assertEqual(watcher.terminal_orders["SUP-DEAD-1"]["position_state"], PositionLifecycleState.UNPROTECTED.value)
        self.assertFalse(gate.is_accepting_entries)

    # ------------------------------------------------------------------
    # Scenario 10 (P0-06): Dhan /v2/trades/{order-id} accepts both single-dict and list-of-dicts
    # ------------------------------------------------------------------
    def test_10_p0_06_dhan_trades_for_order_accepts_dict_and_list_and_validates(self):
        client = DhanClient(self.cfg, self.stop)

        # 1. Documented Dhan v2 single dict response
        single_trade_dict = {
            "dhanClientId": "1100000001",
            "orderId": "112111182045",
            "exchangeOrderId": "15112111182045",
            "exchangeTradeId": "ET-998877",
            "transactionType": "BUY",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "tradingSymbol": "YESBANK",
            "securityId": "11915",
            "tradedQuantity": 40,
            "tradedPrice": 15.55,
        }
        client.session.get = MagicMock(return_value=SimpleNamespace(status_code=200, text=json.dumps(single_trade_dict), json=lambda: single_trade_dict))
        self.assertEqual(client.trades_avg_price("112111182045"), 15.55)
        trades_res = client.get_trades_for_order("112111182045", expected_security_id="11915", expected_transaction_type="BUY", require_exchange_trade_id=True)
        self.assertIsInstance(trades_res, BrokerReadList)
        self.assertEqual(len(trades_res), 1)
        self.assertEqual(trades_res[0]["exchangeTradeId"], "ET-998877")

        # 2. List of dicts response (multi-fill VWAP)
        multi_trades_list = [
            dict(single_trade_dict, exchangeTradeId="ET-1", tradedQuantity=20, tradedPrice=100.0),
            dict(single_trade_dict, exchangeTradeId="ET-2", tradedQuantity=20, tradedPrice=102.0),
        ]
        client.session.get = MagicMock(return_value=SimpleNamespace(status_code=200, text=json.dumps(multi_trades_list), json=lambda: multi_trades_list))
        self.assertEqual(client.trades_avg_price("112111182045"), 101.0)

        # 3. Mismatched orderId or invalid tradedQuantity rejected
        bad_trade = dict(single_trade_dict, orderId="WRONG-OID")
        client.session.get = MagicMock(return_value=SimpleNamespace(status_code=200, text=json.dumps(bad_trade), json=lambda: bad_trade))
        self.assertIsNone(client.trades_avg_price("112111182045"))
        bad_res = client.get_trades_for_order("112111182045")
        self.assertIsInstance(bad_res, BrokerReadFailure)

    # ------------------------------------------------------------------
    # Scenario 11 (P0-06): get_holdings() and get_forever_orders() v2 endpoint fallback
    # ------------------------------------------------------------------
    def test_11_p0_06_holdings_and_forever_orders_v2_fallback(self):
        client = DhanClient(self.cfg, self.stop)

        def router(url, **kwargs):
            if url.endswith("/holdings"):
                return SimpleNamespace(status_code=200, text="[...]", json=lambda: [{"securityId": "11915", "tradingSymbol": "YESBANK", "totalQty": 100}])
            if url.endswith("/forever/orders"):
                return SimpleNamespace(status_code=404, text="Not Found", json=lambda: {})
            if url.endswith("/forever/orders"):
                return SimpleNamespace(status_code=200, text="[...]", json=lambda: [{"orderId": "FO-V2-1", "orderStatus": "PENDING"}])
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        client.session.get = MagicMock(side_effect=router)
        h = client.get_holdings()
        self.assertEqual(len(h), 1)
        self.assertEqual(h[0]["securityId"], "11915")
        fo = client.get_forever_orders()
        self.assertEqual(len(fo), 1)
        self.assertEqual(fo[0]["orderId"], "FO-V2-1")

    # ------------------------------------------------------------------
    # Scenario 12 (P0-06): Read-only CLI order-details & check-dhan-orders in TEST/STOP mode
    # ------------------------------------------------------------------
    def test_12_p0_06_readonly_cli_commands_work_in_test_and_stop_mode_with_redaction(self):
        test_cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            dhan_client_id="1100998877",
            dhan_access_token="RO_TOKEN",
            dhan_token_file=os.path.join(self.tmp.name, ".dhan_token"),
            data_dir=self.data_dir,
        )
        self.stop.trigger_stop("Udit", "Testing read-only CLI while stopped", account_id="1100998877")

        def ro_get(url, **kwargs):
            if "/orders/221261102657708" in url:
                return SimpleNamespace(
                    status_code=200,
                    text="{}",
                    json=lambda: {
                        "orderId": "221261102657708",
                        "tradingSymbol": "YESBANK",
                        "orderStatus": "TRADED",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "quantity": 10,
                        "filledQty": 10,
                        "price": 15.60,
                        "averageTradedPrice": 15.55,
                    },
                )
            if "/trades/221261102657708" in url:
                return SimpleNamespace(
                    status_code=200,
                    text="{}",
                    json=lambda: {
                        "orderId": "221261102657708",
                        "exchangeTradeId": "9988776655",
                        "transactionType": "BUY",
                        "tradedQuantity": 10,
                        "tradedPrice": 15.55,
                    },
                )
            return SimpleNamespace(status_code=200, text="[]", json=lambda: [])

        buf = io.StringIO()
        with patch("requests.Session.get", side_effect=ro_get), patch("sys.stdout", buf):
            rc1 = cmd_order_details(test_cfg, self.stop, ["221261102657708", "--read-only"])
            rc2 = cmd_check_dhan_orders(test_cfg, self.stop, ["--read-only"])
        out = buf.getvalue()
        self.assertEqual(rc1, 0)
        self.assertEqual(rc2, 0)
        self.assertIn("…7708", out)
        self.assertNotIn("221261102657708", out)
        self.assertIn("VWAP=15.55", out)

    # ------------------------------------------------------------------
    # Scenario 13 (P0-07): Lifecycle notification cards & zero Order ID leakage
    # ------------------------------------------------------------------
    def test_13_p0_07_notification_lifecycle_cards_and_no_order_id_leakage(self):
        fno_meta = {
            "symbol": "POLYCAB",
            "segment": "FNO",
            "action": "BUY",
            "mode": "LIVE",
            "source": "VIP Options",
            "stated_price": 100.0,
            "markup_pct": 4.0,
            "limit_price": 104.0,
            "strike": 5000.0,
            "option_type": "CE",
            "expiry": "2026-11-26",
            "lot_size": 125,
            "lots": 1,
            "protection_route": "SUPER_ORDER",
            "initial_sl_price": 100.85,
            "initial_target_price": 107.15,
            "signal_received_at": 1730200000.0,
        }
        c_acc = format_signal_accepted(fno_meta, 125)
        self.assertIn("Status: Signal Accepted", c_acc)
        self.assertIn("Contract: POLYCAB 5000 CE (2026-11-26)", c_acc)
        self.assertIn("Lots: 1 (125 Qty @ 125/lot)", c_acc)

        c_sub = format_order_submitted(fno_meta, 125)
        self.assertIn("Initial Limit-Relative SL / Target: ₹100.85 / ₹107.15", c_sub)

        c_exec = format_order_outcome(
            fno_meta, 125, 125, "TRADED", 1730200001.2, 100.0, True,
            sl_price=97.0, target_price=103.0, protection_route="SUPER_ORDER",
        )
        self.assertIn("Verified SL / Target: ₹97.00 / ₹103.00", c_exec)

        c_prot_rej = format_protection_lifecycle_card(
            fno_meta, "PROTECTION_REJECTED", protected_qty=0, total_qty=125,
            sl_price=97.0, target_price=103.0, protection_route="SUPER_ORDER", reason="RMS rejected SL leg",
        )
        self.assertIn("Protection Rejected / Halted", c_prot_rej)

        c_closed = format_exit_lifecycle_card(
            fno_meta, "POSITION_CLOSED", triggered_leg="TARGET_LEG",
            sell_qty=125, total_qty=125, buy_vwap=100.0, sell_vwap=103.0,
            realized_pnl=375.0, sl_price=97.0, target_price=103.0,
            protection_route="SUPER_ORDER", observed_at=1730200600.0,
        )
        self.assertIn("Status: Position Closed (Target Hit)", c_closed)
        self.assertIn("Entry BUY VWAP: ₹100.00", c_closed)
        self.assertIn("Exit SELL VWAP: ₹103.00", c_closed)
        self.assertIn("Gross Realized P&amp;L: +₹375.00", c_closed)

    # ------------------------------------------------------------------
    # Scenario 14 (P0-08): Atomic outbox claiming, Telegram JSON check & dead-letter visibility
    # ------------------------------------------------------------------
    def test_14_p0_08_atomic_outbox_claiming_and_telegram_delivery_verification(self):
        self.db.enqueue_outbox("ORDER_STATUS_UPDATE", {"text": "Card 1"})
        claimed_1 = self.db.claim_pending_outbox(limit=10, now=1000.0)
        claimed_2 = self.db.claim_pending_outbox(limit=10, now=1001.0)
        self.assertEqual(len(claimed_1), 1)
        self.assertEqual(len(claimed_2), 0, "Second concurrent worker must not claim already-claimed row")

        # TelegramNotifier rejects HTTP 200 when JSON ok == False or message_id missing
        notifier = TelegramNotifier(self.cfg)
        with patch("requests.post", return_value=SimpleNamespace(status_code=200, text='{"ok":false}', json=lambda: {"ok": False})):
            self.assertFalse(notifier.send_log_message("Test card"))
        with patch("requests.post", return_value=SimpleNamespace(status_code=200, text='{"ok":true,"result":{"message_id":42}}', json=lambda: {"ok": True, "result": {"message_id": 42}})):
            self.assertTrue(notifier.send_log_message("Test card"))
            self.assertEqual(notifier.last_message_id, 42)

        # Exhausting retries records OUTBOX_DEAD_LETTER system event
        worker = OutboxWorker(self.db, dispatch_fn=lambda _: False, max_retries=2)
        self.db.mark_outbox_retry(claimed_1[0]["id"], error_msg="Fail 1", max_retries=2, now=1002.0)
        st = self.db.mark_outbox_retry(claimed_1[0]["id"], error_msg="Fail 2", max_retries=2, now=1005.0)
        self.assertEqual(st, "DEAD_LETTER")
        with self.db._get_conn() as conn:
            ev = conn.execute("SELECT * FROM system_events WHERE event_type = 'OUTBOX_DEAD_LETTER'").fetchone()
            self.assertIsNotNone(ev)

    # ------------------------------------------------------------------
    # Scenario 15 (P1-10): Overnight & after-hours open-position continuity
    # ------------------------------------------------------------------
    def test_15_p1_10_open_position_continues_polling_after_market_close_and_restart(self):
        broker = MagicMock()
        broker.cfg = self.cfg
        watcher = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 1000.0)
        watcher.terminal_orders["SUP-OVN-1"] = {
            "order_id": "SUP-OVN-1",
            "symbol": "YESBANK",
            "security_id": "11915",
            "segment": "NSE_EQ",
            "product_type": "CNC",
            "protection_route": "SUPER_ORDER",
            "is_super_order": True,
            "qty": 100,
            "filled_qty": 100,
            "protected_qty": 100,
            "position_state": PositionLifecycleState.PROTECTED.value,
            "confirmed_position_open_qty": 100,
        }
        watcher._save()
        with patch("src.engine.protection_watcher.is_lifecycle_open", return_value=False):
            self.assertTrue(watcher.has_open_monitored_positions())
            self.assertTrue(watcher.should_poll(is_market_open_fn=lambda: False))

        # Reload watcher from SQLite after restart -> still tracks open position
        watcher2 = ProtectionWatcher(broker=broker, path=self.pending_path, dry_run=False, journal_db=self.db, clock=lambda: 2000.0)
        self.assertTrue(watcher2.has_open_monitored_positions())

    # ------------------------------------------------------------------
    # Scenario 16 (P1-11 & P1-12): FNO_PRICE_MARKUP_PCT validation & WebUI section header
    # ------------------------------------------------------------------
    def test_16_p1_11_p1_12_fno_price_markup_pct_config_webui_and_validation(self):
        # 1. Default is 4.0%
        c_def = AppConfig()
        self.assertEqual(c_def.fno_price_markup_pct, 4.0)

        # 2. Custom valid values: 0.0%, 2.5%, 20.0%
        for m_val, exp_limit in [(0.0, 100.00), (2.5, 102.50), (4.0, 104.00), (20.0, 120.00)]:
            c = AppConfig(fno_price_markup_pct=m_val)
            self.assertEqual(c.fno_price_markup_pct, m_val)
            self.assertEqual(calculate_option_limit_price(100.0, 0.05, markup_pct=c.fno_price_markup_pct), exp_limit)

        # 3. Reject negative, >20, boolean, NaN, Inf
        for bad_val in (-0.1, 20.1, True, False, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                AppConfig(fno_price_markup_pct=bad_val)

        tracker = ConfigChangeTracker(data_dir=self.data_dir)
        ok_neg, _ = tracker.validate_inputs({"FNO_PRICE_MARKUP_PCT": -1.0})
        self.assertFalse(ok_neg)
        ok_high, _ = tracker.validate_inputs({"FNO_PRICE_MARKUP_PCT": 25.0})
        self.assertFalse(ok_high)
        ok_bool, _ = tracker.validate_inputs({"FNO_PRICE_MARKUP_PCT": True})
        self.assertFalse(ok_bool)
        ok_valid, _ = tracker.validate_inputs({"FNO_PRICE_MARKUP_PCT": 2.5})
        self.assertTrue(ok_valid)

        # 4. WebUI page renders F&O Price Markup (%) and route-neutral section header
        html = get_setup_page().body.decode("utf-8")
        self.assertIn('name="fno_price_markup_pct"', html)
        self.assertIn("Protection & Timing Rules (Super Order & Forever OCO)", html)
        self.assertNotIn("Protection & Timing Rules (Forever OCO)", html)
        self.assertIn("Protection Status", html)

        # 5. build_env_and_params_dict & config version hash sensitivity
        env_str_4, params_4 = build_env_and_params_dict(fno_price_markup_pct=4.0, env_path=os.path.join(self.tmp.name, ".env"))
        env_str_25, params_25 = build_env_and_params_dict(fno_price_markup_pct=2.5, env_path=os.path.join(self.tmp.name, ".env"))
        self.assertIn("FNO_PRICE_MARKUP_PCT=2.5", env_str_25)
        self.assertNotEqual(compute_config_version(params_4), compute_config_version(params_25))

    # ------------------------------------------------------------------
    # Scenario 17: Full-Day Timestamped Equity (YESBANK CNC) & F&O (₹100 Option) Simulation
    # ------------------------------------------------------------------
    def test_17_full_day_equity_and_fno_simulation(self):
        sim_clock = [1730266500.0]  # 09:45:00 IST
        pipe = TradingBotPipeline(self.cfg)
        pipe.watcher.clock = lambda: sim_clock[0]
        pipe.stop_manager.provision_initial_clean_state("Udit", self.cfg.dhan_client_id)
        pipe.reconciliation_gate._accepting_entries = True
        pipe.resolver.load_equity("YESBANK", "11915", "NSE_EQ", 0.05)
        pipe.resolver.load_option(
            symbol="POLYCAB",
            security_id="55001",
            exchange_segment="NSE_FNO",
            strike=5000.0,
            option_type="CE",
            expiry=date(2024, 11, 28),
            lot_size=125,
            tick_size=0.05,
            trading_symbol="POLYCAB-Nov2024-5000-CE",
        )

        # Simulated broker state books
        super_book = {}
        trade_book = []
        positions_book = {}
        order_status_book = {}
        dispatched_cards = []

        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 250000.0, "OK"))
        pipe.broker.get_positions = MagicMock(side_effect=lambda *a, **kw: BrokerReadList(list(positions_book.values())))
        pipe.broker.get_holdings = MagicMock(return_value=BrokerReadList([]))
        pipe.broker.get_super_orders = MagicMock(side_effect=lambda *a, **kw: BrokerReadList(list(super_book.values())))
        pipe.broker.get_forever_orders = MagicMock(return_value=BrokerReadList([]))
        pipe.broker.get_trade_book = MagicMock(side_effect=lambda *a, **kw: BrokerReadList(list(trade_book)))
        pipe.broker.order_status = MagicMock(side_effect=lambda oid, **kw: order_status_book.get(oid))

        def sim_trades_for_order(oid, **kw):
            matches = [t for t in trade_book if t["orderId"] == oid]
            return BrokerReadList(matches)

        def sim_trades_avg(oid, **kw):
            matches = [t for t in trade_book if t["orderId"] == oid and t["transactionType"] == "BUY"]
            if not matches:
                return None
            val = sum(t["tradedPrice"] * t["tradedQuantity"] for t in matches)
            qty = sum(t["tradedQuantity"] for t in matches)
            return round(val / qty, 4) if qty > 0 else None

        pipe.broker.get_trades_for_order = MagicMock(side_effect=sim_trades_for_order)
        pipe.broker.trades_avg_price = MagicMock(side_effect=sim_trades_avg)

        seq = [0]

        def sim_place_super(order_req, stop_loss_price, target_price, trailing_jump=0.0, retry=False):
            seq[0] += 1
            oid = f"SIM-SUP-{seq[0]}"
            order_status_book[oid] = {
                "orderId": oid,
                "orderStatus": "PENDING",
                "quantity": order_req.quantity,
                "filledQty": 0,
                "price": order_req.price,
                "averageTradedPrice": 0.0,
                "securityId": order_req.security_id,
                "exchangeSegment": order_req.exchange_segment,
                "productType": order_req.product_type.value,
            }
            super_book[oid] = _active_super_order_record(
                oid, order_req.security_id, order_req.exchange_segment, order_req.product_type.value,
                order_req.quantity, stop_loss_price, target_price, parent_status="PENDING",
            )
            pipe.broker.last_order_broker_status = "PENDING"
            return True, oid, "Super order acknowledged by Dhan (status=PENDING)"

        def sim_mod_super(order_id, stop_loss_price, target_price, trailing_jump=0.0, legs_to_modify=None):
            so = super_book[order_id]
            for lg in so["legDetails"]:
                if lg["legName"] == "TARGET_LEG":
                    lg["price"] = target_price
                    lg["targetPrice"] = target_price
                elif lg["legName"] == "STOP_LOSS_LEG":
                    lg["price"] = stop_loss_price
                    lg["stopLossPrice"] = stop_loss_price
            pipe.broker.last_super_leg_mod_results[order_id] = {
                "TARGET_LEG": {"state": "ACKED", "price": target_price},
                "STOP_LOSS_LEG": {"state": "ACKED", "price": stop_loss_price},
            }
            return True, "Updated"

        pipe.broker.place_super_order = MagicMock(side_effect=sim_place_super)
        pipe.broker.modify_super_order_protection = MagicMock(side_effect=sim_mod_super)
        worker = OutboxWorker(pipe.db, dispatch_fn=lambda txt: (dispatched_cards.append(txt) or True))

        # --- LEG A: 09:45:00 IST — Buy #YESBANK @ 15.52 -> Limit 15.60 (1,602 shares) ---
        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res_eq = pipe.process_raw_message(
                "Buy #YESBANK @ 15.52",
                channel_id=-1001,
                message_id=1001,
                msg_timestamp=sim_clock[0],
                current_time=sim_clock[0],
                source_name="Equity Channel",
            )
        self.assertTrue(res_eq.is_valid)
        self.assertEqual(res_eq.limit_price, 15.60)
        eq_oid = "SIM-SUP-1"
        worker.process_pending_events(now=sim_clock[0])

        # 09:45:02 IST — YESBANK fills at ₹15.55 -> re-anchors SL to ₹15.05, Target to ₹16.05
        sim_clock[0] += 2.0
        qty_eq = order_status_book[eq_oid]["quantity"]
        order_status_book[eq_oid].update({"orderStatus": "TRADED", "filledQty": qty_eq, "averageTradedPrice": 15.55})
        super_book[eq_oid]["orderStatus"] = "TRADED"
        trade_book.append({
            "orderId": eq_oid,
            "exchangeTradeId": "ET-YB-BUY-1",
            "transactionType": "BUY",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "tradedQuantity": qty_eq,
            "tradedPrice": 15.55,
        })
        positions_book["11915"] = {"securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": qty_eq}
        pipe.watcher.check_once()
        worker.process_pending_events(now=sim_clock[0])
        self.assertEqual(pipe.watcher.terminal_orders[eq_oid]["confirmed_sl_price"], 15.05)
        self.assertEqual(pipe.watcher.terminal_orders[eq_oid]["confirmed_target_price"], 16.05)

        # 10:15:00 IST — YESBANK Target Leg triggers and fills at ₹16.05
        sim_clock[0] += 1800.0
        super_book[eq_oid] = _active_super_order_record(
            eq_oid, "11915", "NSE_EQ", "CNC", qty_eq, 15.05, 16.05,
            parent_status="CLOSED", target_status="TRADED", target_triggered_qty=qty_eq,
        )
        trade_book.append({
            "orderId": f"{eq_oid}-TGT",
            "exchangeTradeId": "ET-YB-SELL-1",
            "transactionType": "SELL",
            "securityId": "11915",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "tradedQuantity": qty_eq,
            "tradedPrice": 16.05,
        })
        positions_book["11915"]["netQty"] = 0
        pipe.watcher.check_once()
        worker.process_pending_events(now=sim_clock[0])
        self.assertEqual(pipe.watcher.terminal_orders[eq_oid]["position_state"], PositionLifecycleState.POSITION_CLOSED.value)
        self.assertEqual(pipe.watcher.terminal_orders[eq_oid]["realized_pnl"], round((16.05 - 15.55) * qty_eq, 2))

        # --- LEG B: 11:00:00 IST — POLYCAB 5000 CE @ 98-100 -> Limit ₹104.00 (1 lot = 125 qty) ---
        sim_clock[0] += 2700.0
        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res_fno = pipe.process_raw_message(
                "POLYCAB NOV 5000 CE BUY AT 98-100 SL 90 TGT 115",
                channel_id=-1001,
                message_id=1002,
                msg_timestamp=sim_clock[0],
                current_time=sim_clock[0],
                source_name="Options Channel",
            )
        self.assertTrue(res_fno.is_valid)
        self.assertEqual(res_fno.limit_price, 104.00)
        fno_oid = "SIM-SUP-2"
        # Initial Super Order legs submitted at limit-relative ₹100.85 SL / ₹107.15 Target
        self.assertEqual(pipe.watcher.pending[fno_oid]["initial_sl_price"], 100.85)
        self.assertEqual(pipe.watcher.pending[fno_oid]["initial_target_price"], 107.15)
        worker.process_pending_events(now=sim_clock[0])

        # 11:00:02 IST — POLYCAB 5000 CE fills at ₹100.00 -> re-anchors to fill-relative ₹97.00 SL / ₹103.00 Target
        sim_clock[0] += 2.0
        order_status_book[fno_oid].update({"orderStatus": "TRADED", "filledQty": 125, "averageTradedPrice": 100.0})
        super_book[fno_oid]["orderStatus"] = "TRADED"
        trade_book.append({
            "orderId": fno_oid,
            "exchangeTradeId": "ET-PC-BUY-1",
            "transactionType": "BUY",
            "securityId": "55001",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "tradedQuantity": 125,
            "tradedPrice": 100.0,
        })
        positions_book["55001"] = {"securityId": "55001", "exchangeSegment": "NSE_FNO", "productType": "MARGIN", "netQty": 125}
        pipe.watcher.check_once()
        worker.process_pending_events(now=sim_clock[0])
        self.assertEqual(pipe.watcher.terminal_orders[fno_oid]["confirmed_sl_price"], 97.00)
        self.assertEqual(pipe.watcher.terminal_orders[fno_oid]["confirmed_target_price"], 103.00)

        # 11:30:00 IST — POLYCAB Target Leg triggers on exchange (CLOSED / TRIGGERED), awaiting SELL tradebook fill
        sim_clock[0] += 1800.0
        super_book[fno_oid] = _active_super_order_record(
            fno_oid, "55001", "NSE_FNO", "MARGIN", 125, 97.00, 103.00,
            parent_status="CLOSED", target_status="TRIGGERED", target_triggered_qty=125,
        )
        pipe.watcher.check_once()
        worker.process_pending_events(now=sim_clock[0])
        self.assertEqual(pipe.watcher.terminal_orders[fno_oid]["position_state"], PositionLifecycleState.EXIT_TRIGGERED.value)

        # 11:30:01 IST — POLYCAB Target SELL fill arrives in tradebook @ ₹103.00 -> POSITION_CLOSED (+₹375.00 P&L)
        sim_clock[0] += 1.0
        trade_book.append({
            "orderId": f"{fno_oid}-TGT",
            "exchangeTradeId": "ET-PC-SELL-1",
            "transactionType": "SELL",
            "securityId": "55001",
            "exchangeSegment": "NSE_FNO",
            "productType": "MARGIN",
            "tradedQuantity": 125,
            "tradedPrice": 103.0,
        })
        positions_book["55001"]["netQty"] = 0
        pipe.watcher.check_once()
        worker.process_pending_events(now=sim_clock[0])
        self.assertEqual(pipe.watcher.terminal_orders[fno_oid]["position_state"], PositionLifecycleState.POSITION_CLOSED.value)
        self.assertEqual(pipe.watcher.terminal_orders[fno_oid]["realized_pnl"], 375.00)
        self.assertTrue(any("Gross Realized P&amp;L: +₹375.00" in c for c in dispatched_cards))


if __name__ == "__main__":
    unittest.main()
