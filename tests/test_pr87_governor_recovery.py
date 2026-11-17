"""PR87: regression protection for two settled YESBANK fills and safe gate recovery.

All Dhan reads are synthetic. No network, token refresh, orders or VM changes.
The historical evidence parser/ledger from PR79/PR80 is intentionally untouched.
"""
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock

from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState


def yesbank_row(oid):
    return {
        "parent_order_id": oid,
        "account_id": "ACCOUNT",
        "symbol": "YESBANK",
        "security_id": "11915",
        "exchange_segment": "NSE_EQ",
        "product_type": "CNC",
        "position_state": "RECONCILIATION_REQUIRED",
        "entry_qty": 1,
        "buy_filled_qty": 1,
        "sell_filled_qty": 0,
        "buy_vwap": 15.52,
        "confirmed_open_qty": 1,
    }


class PR87SettledHoldingsTests(unittest.TestCase):
    def setUp(self):
        self.holdings = [{"securityId": "11915", "totalQty": 2, "dpQty": 2}]
        self.broker = NS(
            cfg=NS(dhan_client_id="ACCOUNT"),
            get_holdings=Mock(side_effect=lambda: self.holdings),
            get_forever_orders=Mock(return_value=[]),
        )
        self.db = NS(
            account_id="ACCOUNT",
            get_open_positions_lifecycle=Mock(return_value=[
                yesbank_row("YESBANK-NOV02"), yesbank_row("YESBANK-NOV04")
            ]),
            is_manual_risk_acknowledged=Mock(return_value=True),
        )
        self.gate = StartupReconciliationGate(
            self.broker, self.db, None, NS(is_stopped=Mock(return_value=False))
        )

    def test_both_existing_verified_fills_remain_owner_acknowledged(self):
        ok, msg = self.gate._verify_bot_owned_holdings_protected([])
        self.assertTrue(ok, msg)
        self.assertEqual(self.db.is_manual_risk_acknowledged.call_count, 2)
        self.broker.get_holdings.assert_called_once()
        self.assertEqual(
            [x["confirmed_open_qty"] for x in self.db.get_open_positions_lifecycle()],
            [1, 1],
        )

    def test_reconciliation_required_must_not_hide_missing_broker_shares(self):
        self.holdings = [{"securityId": "11915", "totalQty": 0, "dpQty": 0}]
        ok, msg = self.gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        self.assertIn("sumOfBotOpenQty=2", msg)

    def test_missing_owner_ack_must_not_claim_broker_protection(self):
        self.db.is_manual_risk_acknowledged.return_value = False
        ok, msg = self.gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        self.assertIn("Unprotected bot-owned settled holding", msg)


class PR87GateCannotIgnoreWatcherUncertaintyTests(unittest.TestCase):
    def setUp(self):
        self.broker = NS(
            cfg=NS(dhan_client_id="ACCOUNT"),
            get_fund_limits=Mock(return_value=(True, 5000.0, "OK")),
            get_trade_book=Mock(return_value=[]),
            get_positions=Mock(return_value=[]),
        )
        self.db = NS(
            get_non_terminal_intents=Mock(return_value=[]),
            get_unprotected_orders=Mock(return_value=[]),
            get_unresolved_protection_attempts=Mock(return_value=[]),
        )
        self.record = {
            "order_id": "YESBANK-NOV02", "symbol": "YESBANK",
            "filled_qty": 1, "exit_filled_qty": 0,
            "position_state": "RECONCILIATION_REQUIRED",
            "reconciliation_required": True,
            "unreadable_broker_response": True,
            "manual_exit_detected": True,
        }
        self.watcher = NS(
            terminal_orders={"YESBANK-NOV02": self.record},
            pending={},
            check_once=Mock(),
        )
        self.stop = NS(is_stopped=Mock(return_value=False))
        self.gate = StartupReconciliationGate(self.broker, self.db, self.watcher, self.stop)
        self.gate._restore_committed_historical_watcher_records = Mock()
        self.gate._verify_open_positions_protected = Mock(return_value=(True, "OK"))
        self.gate._verify_bot_owned_holdings_protected = Mock(return_value=(True, "OK"))

    def test_clean_broker_books_do_not_auto_erase_legacy_read_uncertainty(self):
        ok, msg = self.gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertEqual(self.gate.state, ReconciliationState.BLOCKED)
        self.assertIn("verified lifecycle repair required", msg)
        self.assertTrue(self.record["manual_exit_detected"])
        self.assertTrue(self.record["reconciliation_required"])
        self.assertTrue(self.record["unreadable_broker_response"])

    def test_gate_can_open_only_when_all_watcher_uncertainty_resolved(self):
        self.record.update(
            position_state="UNPROTECTED_MANUAL_RISK",
            manual_exit_detected=False,
            reconciliation_required=False,
            unreadable_broker_response=False,
        )
        ok, msg = self.gate.run_startup_reconciliation()
        self.assertTrue(ok, msg)
        self.assertEqual(self.gate.state, ReconciliationState.HEALTHY)


if __name__ == "__main__":
    unittest.main()
