"""T+0 signed CNC delivery 1+1 owner-managed, 28-share *closed* sibling; using
an actual ProtectionWatcher and an isolated durable JournalDB. No live APIs.

The test intentionally begins with persisted RECONCILIATION_REQUIRED rows and
requires broker-authoritative history, current Demat quantity, exact protection
binding, and two account/quantity-qualified owner acknowledgements. Startup
remains fail-closed until watcher recovery has been written to SQLite.
"""
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

from src.engine.owner_lot_evidence import GATE_BOOKS, prepare_owner_lots
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate
from src.journal.db import JournalDB


class _ThreeParentFixture:
    ACCOUNT = "SYNTHETIC"
    SECURITY = "11915"
    FIRST = "221261102657708"
    SECOND = "23126110439808"
    THIRD = "221261123606908"
    GROUP = "SYNTHETIC-OCO-28"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.now = time.time()
        self.prior = self.now - 8 * 86400
        self.db = JournalDB(str(self.root / "isolated-test-journal.db"), account_id=self.ACCOUNT)
        self.parents = [(self.FIRST, 1), (self.SECOND, 1), (self.THIRD, 28)]
        self.history = []
        self.records = {}
        for index, (parent, qty) in enumerate(self.parents):
            corr = parent  # Exact durable owner ACK binding; never symbol-only.
            self.db.record_order_intent(
                correlation_id=corr, account_id=self.ACCOUNT, symbol="YESBANK-EQ",
                security_id=self.SECURITY, exchange_segment="NSE_EQ",
                product_type="CNC", order_type="LIMIT", price=15.0,
                quantity=qty, protection_route="FOREVER_OCO",
            )
            self.db.update_order_intent(corr, "TRADED", broker_order_id=parent)
            with self.db._get_conn() as con:
                con.execute("UPDATE order_intents SET created_at=?, updated_at=? WHERE correlation_id=?",
                            (self.prior, self.prior, corr))
            execution = "SYNTHETIC-EXEC-" + str(index + 1)
            self.assertTrue(self.db.record_fill(
                broker_order_id=parent, exchange_trade_id=execution,
                symbol="YESBANK-EQ", traded_price=15.0, traded_quantity=qty,
                traded_at=self.prior, transaction_type="BUY", security_id=self.SECURITY,
                exchange_segment="NSE_EQ", product_type="CNC",
                parent_order_id=parent, is_verified_trade=True,
            ))
            protected = parent == self.THIRD
            self.db.upsert_position_lifecycle(
                parent_order_id=parent, account_id=self.ACCOUNT,
                symbol="YESBANK-EQ", security_id=self.SECURITY,
                exchange_segment="NSE_EQ", product_type="CNC",
                protection_route="FOREVER_OCO",
                position_state="PROTECTED" if protected else "RECONCILIATION_REQUIRED",
                entry_qty=qty, buy_filled_qty=qty, buy_vwap=15.0,
                protected_qty=28 if protected else 0, confirmed_open_qty=qty,
                manual_exit_detected=not protected,
            )
            if not protected:
                self.db.acknowledge_manual_risk_holding(
                    parent_order_id=parent, correlation_id=corr,
                    account_id=self.ACCOUNT, symbol="YESBANK-EQ",
                    security_id=self.SECURITY, quantity=1,
                    acknowledged_by="offline synthetic owner", reason="retain test share",
                )
            rec = dict(
                order_id=parent, correlation_id=corr, symbol="YESBANK-EQ",
                security_id=self.SECURITY, segment="NSE_EQ", product_type="CNC",
                protection_route="FOREVER_OCO", state="TRADED", qty=qty,
                filled_qty=qty, exit_filled_qty=0, confirmed_position_open_qty=qty,
                buy_vwap=15.0, position_state="PROTECTED" if protected else "RECONCILIATION_REQUIRED",
                protected_qty=28 if protected else 0,
                manual_risk_acknowledged=not protected,
                manual_exit_detected=not protected,
                reconciliation_required=not protected,
                unreadable_broker_response=not protected,
            )
            if protected:
                rec["confirmed_protection_ids"] = [self.GROUP]
                rec["forever_order_id"] = self.GROUP
                rec["confirmed_sl_price"] = 14.0
                rec["confirmed_target_price"] = 16.0
            self.records[parent] = rec
            self.history.append(dict(
                dhanClientId=self.ACCOUNT, securityId=self.SECURITY,
                isin="SYNTHETIC-YESBANK-ISIN", exchangeSegment="NSE_EQ",
                productType="CNC", orderId=parent,
                exchangeTradeId=execution, transactionType="BUY",
                tradedQuantity=qty, tradedPrice=15.0,
            ))
        with self.db._get_conn() as con:
            con.execute(
                "INSERT INTO protection_orders "
                "(parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, "
                "target_price, quantity, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (self.THIRD, self.GROUP, "YESBANK-EQ", 14.0, 13.95, 16.0,
                 28, "ACTIVE_CONFIRMED", self.prior),
            )
        self.forever = [dict(
            orderId=self.GROUP, orderStatus="PENDING", securityId=self.SECURITY,
            dhanClientId=self.ACCOUNT, exchangeSegment="NSE_EQ", productType="CNC",
            transactionType="SELL", quantity=28, quantity1=28,
            triggerPrice=14.0, price1=16.0,
        )]
        self.broker = NS(
            cfg=NS(dhan_client_id=self.ACCOUNT),
            get_authenticated_profile_id=Mock(return_value=self.ACCOUNT),
            get_super_orders=Mock(return_value=[]),
            get_forever_orders=Mock(side_effect=lambda: list(self.forever)),
            get_trade_book=Mock(return_value=[]),
            get_trades_for_order=Mock(side_effect=lambda parent: [dict(x) for x in self.history if x["orderId"] == parent]),
            get_historical_trades=Mock(side_effect=lambda *a, **kw: [dict(x) for x in self.history]),
            get_positions=Mock(return_value=[]),
            get_holdings=Mock(return_value=[dict(
                securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
                isin="SYNTHETIC-YESBANK-ISIN", totalQty=30, dpQty=30, t1Qty=0,
            )]),
            get_order_book=Mock(return_value=[]),
            get_edis_owned_quantity=Mock(return_value=None),
            place_forever_oco=Mock(side_effect=AssertionError("LIVE mutation is forbidden")),
            place_super_order=Mock(side_effect=AssertionError("LIVE mutation is forbidden")),
            cancel_order=Mock(side_effect=AssertionError("LIVE mutation is forbidden")),
            cancel_forever_order=Mock(side_effect=AssertionError("LIVE mutation is forbidden")),
            reconciliation_gate=NS(halt_new_entries=Mock()),
        )
        self.watcher = ProtectionWatcher(
            broker=self.broker, journal_db=self.db,
            path=str(self.root / "pending_protection.json"),
            dry_run=True, clock=lambda: self.now,
        )
        self.watcher.terminal_orders = dict(self.records)
        self.assertTrue(self.watcher._save())

    def _gate(self):
        # Exercise the actual startup holdings verification method, not an AST
        # copy or a mock returning HEALTHY. Full startup's other broker reads
        # are independently covered in the existing CI suite.
        gate = object.__new__(StartupReconciliationGate)
        gate.db = self.db
        gate.broker = self.broker
        gate.watcher = self.watcher
        gate._in_startup_reconciliation_pass = False
        gate._read_required_broker_collection = lambda name, label: (True, list(self.forever), "OK")
        token = GATE_BOOKS.set({"verified": {}, "errors": {}, "share": False})
        try:
            from src.engine.owner_lot_evidence import prepare_owner_lots
            b = GATE_BOOKS.get()
            prepare_owner_lots(self.broker, self.db, b, self.broker.get_positions(),
                               self.broker.get_trade_book(), self.now)
            return gate._verify_bot_owned_holdings_protected([], ())
        finally:
            GATE_BOOKS.reset(token)

    def _full_watcher_pass(self):
        batch = {"verified": {}, "errors": {}, "share": False}
        token = GATE_BOOKS.set(batch)
        try:
            prepare_owner_lots(self.broker, self.db, batch,
                               self.broker.get_positions(), self.broker.get_trade_book(), self.now)
            self.watcher.supervise_open_positions(current_time=self.now)
            return batch
        finally:
            GATE_BOOKS.reset(token)

    def _states(self):
        return {p: self.db.get_position_lifecycle(p) for p, _ in self.parents}

    def _assert_no_mutation(self):
        for key in ("place_forever_oco", "place_super_order", "cancel_order", "cancel_forever_order"):
            getattr(self.broker, key).assert_not_called()
        self.assertEqual(self.db.get_protection_order_ids_for_parent(self.FIRST), [])
        self.assertEqual(self.db.get_protection_order_ids_for_parent(self.SECOND), [])
        self.assertEqual(self.db.get_protection_order_ids_for_parent(self.THIRD), [self.GROUP])


from datetime import datetime, timezone
import json

from src.engine.holding_quantity import observed_open_quantity
from src.engine.signed_cnc_delivery import (
    SignedCNCDeliveryUnverified, verify_signed_cnc_delivery_quantity,
)
from src.engine.reconciliation import ReconciliationState
from src.engine.unified_exit_reconciliation import ReconciliationEvidenceError


class SignedCNCT0ClosedSibling(_ThreeParentFixture, unittest.TestCase):
    TARGET_CHILD = "SYNTHETIC-TARGET-CHILD-28"
    STOP_CHILD = "SYNTHETIC-STOP-CHILD-28"
    SELL_TRADE = "SYNTHETIC-EXIT-TRADE-28"

    def setUp(self):
        super().setUp()
        self.broker.cfg = NS(
            dhan_client_id=self.ACCOUNT, trading_mode="LIVE",
            data_dir=str(self.root),
        )
        self.broker.get_fund_limits = Mock(return_value=(True, 20000.0, "Synthetic funds"))
        self.broker.get_positions.return_value = [dict(
            securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
            exchangeSegment="NSE_EQ", productType="CNC",
            tradingSymbol="YESBANK-EQ", netQty=-28,
            dayBuyQty=0, daySellQty=28,
            carryForwardBuyQty=0, carryForwardSellQty=0,
        )]
        self.broker.get_holdings.return_value = [dict(
            securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
            isin="SYNTHETIC-YESBANK-ISIN", totalQty=30, dpQty=30, t1Qty=0,
        )]
        self.sell = dict(
            dhanClientId=self.ACCOUNT, securityId=self.SECURITY,
            isin="SYNTHETIC-YESBANK-ISIN",
            exchangeSegment="NSE_EQ", productType="CNC",
            orderId=self.TARGET_CHILD, exchangeTradeId=self.SELL_TRADE,
            exchangeOrderId="SYNTHETIC-EXCHANGE-SELL",
            exchangeTime=datetime.fromtimestamp(self.now, __import__("zoneinfo").ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S"),
            transactionType="SELL", tradedQuantity=28, tradedPrice=17.93,
        )
        self.broker.get_trade_book.return_value = [dict(self.sell)]
        for buy in self.history:
            # A separate historical BUY cannot be exempted without its exact
            # broker date and exchange execution identity.
            buy["exchangeTime"] = datetime.fromtimestamp(
                self.prior, __import__("zoneinfo").ZoneInfo("Asia/Kolkata")
            ).strftime("%Y-%m-%d %H:%M:%S")
            buy["exchangeOrderId"] = "SYNTHETIC-EXCHANGE-" + buy["orderId"]
        self.history.append(dict(self.sell))
        self.broker.get_order_book.return_value = [dict(
            orderId=self.TARGET_CHILD, algoId=self.GROUP,
            dhanClientId=self.ACCOUNT, securityId=self.SECURITY,
            exchangeSegment="NSE_EQ", productType="CNC",
            transactionType="SELL", orderStatus="TRADED",
            quantity=28, filledQty=28, remainingQuantity=0,
        )]
        self.forever[:] = [
            dict(orderId=self.GROUP, orderFlag="OCO", orderType="LIMIT",
                 legName="TARGET_LEG", orderStatus="TRADED",
                 securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
                 exchangeSegment="NSE_EQ", productType="CNC",
                 transactionType="SELL", quantity=28, quantity1=28,
                 price1=16.0, triggerPrice=14.0,
                 childOrderId=self.TARGET_CHILD),
            dict(orderId=self.GROUP, orderFlag="OCO", orderType="LIMIT",
                 legName="STOP_LOSS_LEG", orderStatus="CANCELLED",
                 securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
                 exchangeSegment="NSE_EQ", productType="CNC",
                 transactionType="SELL", quantity=28, quantity1=28,
                 price1=16.0, triggerPrice=14.0),
        ]
        self.assertTrue(self.db.record_fill(
            broker_order_id=self.TARGET_CHILD, exchange_trade_id=self.SELL_TRADE,
            symbol="YESBANK-EQ", traded_price=17.93, traded_quantity=28,
            traded_at=self.now, transaction_type="SELL",
            security_id=self.SECURITY, exchange_segment="NSE_EQ",
            product_type="CNC", parent_order_id=self.THIRD,
            is_verified_trade=True,
        ))
        self.db.upsert_position_lifecycle(
            parent_order_id=self.THIRD, account_id=self.ACCOUNT,
            symbol="YESBANK-EQ", security_id=self.SECURITY,
            exchange_segment="NSE_EQ", product_type="CNC",
            protection_route="FOREVER_OCO", position_state="POSITION_CLOSED",
            entry_qty=28, buy_filled_qty=28, buy_vwap=15.0,
            sell_filled_qty=28, sell_vwap=17.93,
            confirmed_open_qty=0, protected_qty=0,
        )
        self.db.mark_protection_orders_closed(self.THIRD)
        self.records[self.THIRD].update(
            position_state="POSITION_CLOSED", protected_qty=0,
            confirmed_position_open_qty=0, exit_filled_qty=28,
            exit_vwap=17.93, tp_order_id=self.TARGET_CHILD,
            sl_order_id=self.STOP_CHILD, position_closed_notified=True,
            reconciliation_required=False,
        )
        self.watcher.terminal_orders[self.THIRD] = self.records[self.THIRD]
        self.assertTrue(self.watcher._save())

    def proof(self):
        return verify_signed_cnc_delivery_quantity(
            db=self.db, positions=self.broker.get_positions(),
            holdings=self.broker.get_holdings(),
            trade_book=self.broker.get_trade_book(),
            account=self.ACCOUNT, security=self.SECURITY,
            segment="NSE_EQ", product="CNC",
            expected_open_qty=2, now=self.now,
        )

    def assert_retained(self):
        for oid in (self.FIRST, self.SECOND):
            row = self.db.get_position_lifecycle(oid)
            self.assertEqual((row["confirmed_open_qty"], row["sell_filled_qty"]), (1, 0))
            self.assertTrue(self.db.is_manual_risk_acknowledged(
                oid, oid, expected_account_id=self.ACCOUNT,
                expected_security_id=self.SECURITY, required_qty=1,
            ))
        closed = self.db.get_position_lifecycle(self.THIRD)
        self.assertEqual(
            (closed["position_state"], closed["sell_filled_qty"], closed["confirmed_open_qty"]),
            ("POSITION_CLOSED", 28, 0),
        )
        self._assert_no_mutation()

    def test_signed_delivery_proof_and_generic_two_book_fail_closed(self):
        observed, status = observed_open_quantity(
            self.broker.get_positions(), self.broker.get_holdings(),
            security_id=self.SECURITY, segment="NSE_EQ",
            product="CNC", account_id=self.ACCOUNT,
        )
        self.assertIsNone(observed)
        self.assertEqual(status, "NEGATIVE_CNC_POSITION_REVIEW")
        self.assertEqual(self.proof(), 2)
        self.assert_retained()

    def test_actual_watcher_uses_closed_sibling_not_per_record_sell(self):
        # Owner history must be verified in this *same* pass; no stale
        # boolean, old quote or owner-label is accepted as proof.
        batch = self._full_watcher_pass()
        self.assertEqual(set(batch["verified"]), {self.FIRST, self.SECOND}, batch)
        for oid in (self.FIRST, self.SECOND):
            rec = self.watcher.terminal_orders[oid]
            self.assertEqual(rec["broker_quantity_evidence"], "VERIFIED_SIGNED_CNC_DELIVERY")
            self.assertEqual(rec["position_state"], "UNPROTECTED_MANUAL_RISK")
            self.assertEqual(rec["confirmed_position_open_qty"], 1)
            self.assertEqual(rec["exit_filled_qty"], 0)
        self.broker.reconciliation_gate.halt_new_entries.assert_not_called()
        self.assert_retained()

    def test_real_startup_restart_open_only_after_independent_validation(self):
        self.broker.reconciliation_gate = NS(halt_new_entries=Mock())
        gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher, NS(is_stopped=lambda: False),
        )
        self.broker.reconciliation_gate = gate
        success, reason = gate.run_startup_reconciliation()
        self.assertTrue(success, reason)
        self.assertEqual(gate.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate.is_accepting_entries)
        self.assert_retained()

        reopened = JournalDB(str(self.root / "isolated-test-journal.db"),
                             account_id=self.ACCOUNT)
        restarted = ProtectionWatcher(
            broker=self.broker, journal_db=reopened,
            path=str(self.root / "pending_protection.json"),
            dry_run=True, clock=lambda: self.now,
        )
        again = StartupReconciliationGate(
            self.broker, reopened, restarted, NS(is_stopped=lambda: False),
        )
        self.broker.reconciliation_gate = again
        ok, detail = again.run_startup_reconciliation()
        self.assertTrue(ok, detail)
        self.assertTrue(again.is_accepting_entries)
        self.assert_retained()

    def test_unchanged_owner_stop_fences_healthy_signed_delivery(self):
        gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher, NS(is_stopped=lambda: True),
        )
        self.broker.reconciliation_gate = gate
        ok, detail = gate.run_startup_reconciliation()
        self.assertFalse(ok, detail)
        self.assertFalse(gate.is_accepting_entries)
        self.assert_retained()

    def test_unjournaled_or_unverified_sell_rejected(self):
        with self.db._get_conn() as conn:
            conn.execute("UPDATE fills SET is_verified_trade=0 WHERE exchange_trade_id=?",
                         (self.SELL_TRADE,))
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_unattributed_manual_sell_rejected(self):
        self.sell["orderId"] = "EXTERNAL-SELL"
        self.broker.get_trade_book.return_value = [dict(self.sell)]
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_holdings_or_net_mismatch_rejected(self):
        self.broker.get_holdings.return_value[0]["totalQty"] = 29
        self.broker.get_holdings.return_value[0]["dpQty"] = 29
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_net_minus_27_rejected(self):
        self.broker.get_positions.return_value[0]["netQty"] = -27
        self.broker.get_positions.return_value[0]["daySellQty"] = 27
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_missing_or_extra_trade_rejected(self):
        self.broker.get_trade_book.return_value = []
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_price_mismatch_rejected(self):
        self.broker.get_trade_book.return_value[0]["tradedPrice"] = 17.94
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_cross_segment_or_product_rejected(self):
        self.broker.get_trade_book.return_value[0]["exchangeSegment"] = "BSE_EQ"
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_account_mismatch_rejected(self):
        self.broker.get_trade_book.return_value[0]["dhanClientId"] = "OTHER-ACCOUNT"
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_multi_lot_allocated_fill_rejected(self):
        with self.db._get_conn() as conn:
            conn.execute("UPDATE fills SET parent_order_id='MULTI_LOT_ALLOCATED' WHERE exchange_trade_id=?",
                         (self.SELL_TRADE,))
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_trade_book_unreadable_rejected(self):
        self.broker.get_trade_book.return_value = None
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_owner_ack_missing_rejected(self):
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM manual_risk_acknowledgements WHERE parent_order_id=?",
                         (self.SECOND,))
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()

    def test_bogus_closed_sibling_cannot_whitelist_historical_buy(self):
        # A closed-sibling BUY is exempt only when its OCO and corresponding
        # child SELL are both durably, account-bound and fully reconciled.
        with self.db._get_conn() as conn:
            conn.execute("UPDATE positions_lifecycle SET sell_filled_qty=27 WHERE parent_order_id=?",
                         (self.THIRD,))
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()


if __name__ == "__main__":
    unittest.main()
