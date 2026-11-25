"""Synthetic 1+1 owner-managed and 28-share protected YESBANK recovery, using
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
from src.journal.db import JournalDB, write_protection_order


class ThreeParentRealWatcher(unittest.TestCase):
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
            write_protection_order(
                con, parent_id=self.THIRD, protection_id=self.GROUP,
                symbol="YESBANK-EQ", sl_trigger=14.0, sl_limit=13.95,
                target_price=16.0, quantity=28, status="ACTIVE_CONFIRMED",
                created_at=self.prior,
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

    def test_real_watcher_recovers_exact_two_lots_and_preserves_protected_sibling(self):
        ok, reason = self._gate()
        self.assertFalse(ok)
        self.assertIn("reconciliation", reason.lower())
        batch = self._full_watcher_pass()
        self.assertEqual(set(batch["verified"]), {self.FIRST, self.SECOND}, batch)
        rows = self._states()
        for parent in (self.FIRST, self.SECOND):
            self.assertEqual(rows[parent]["position_state"], "UNPROTECTED_MANUAL_RISK", rows[parent])
            self.assertEqual(rows[parent]["confirmed_open_qty"], 1)
            self.assertEqual(rows[parent]["protected_qty"], 0)
            self.assertEqual(self.watcher.terminal_orders[parent]["position_state"], "UNPROTECTED_MANUAL_RISK")
        self.assertEqual(rows[self.THIRD]["position_state"], "PROTECTED")
        self.assertEqual(rows[self.THIRD]["confirmed_open_qty"], 28)
        self.assertEqual(rows[self.THIRD]["protected_qty"], 28)
        self._assert_no_mutation()
        ok, reason = self._gate()
        self.assertTrue(ok, reason)
        # Repeated passes do not manufacture a SELL, remove an ACK, or borrow
        # the protected sibling's group.
        self._full_watcher_pass()
        self.assertEqual(self._states()[self.THIRD]["protected_qty"], 28)
        self._assert_no_mutation()

    def test_missing_owner_ack_remains_blocked(self):
        with self.db._get_conn() as con:
            con.execute("DELETE FROM manual_risk_acknowledgements WHERE parent_order_id=?", (self.SECOND,))
        self._full_watcher_pass()
        self.assertEqual(self._states()[self.SECOND]["position_state"], "RECONCILIATION_REQUIRED")
        ok, reason = self._gate()
        self.assertFalse(ok)
        self._assert_no_mutation()

    def test_short_holdings_cannot_repair_stale_lots(self):
        self.broker.get_holdings.return_value = [dict(
            securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
            isin="SYNTHETIC-YESBANK-ISIN", totalQty=29, dpQty=29, t1Qty=0,
        )]
        self._full_watcher_pass()
        self.assertTrue(any(self._states()[p]["position_state"] == "RECONCILIATION_REQUIRED"
                            for p in (self.FIRST, self.SECOND)))
        ok, reason = self._gate()
        self.assertFalse(ok)
        self._assert_no_mutation()

    def test_unattributed_same_security_sell_cannot_repair_lots(self):
        self.broker.get_trade_book.return_value = [dict(
            dhanClientId=self.ACCOUNT, securityId=self.SECURITY,
            exchangeSegment="NSE_EQ", productType="CNC", transactionType="SELL",
            orderId="UNLINKED-SELL", exchangeTradeId="UNLINKED-EXEC",
            tradedQuantity=1, tradedPrice=16.0,
        )]
        self._full_watcher_pass()
        self.assertTrue(any(self._states()[p]["position_state"] == "RECONCILIATION_REQUIRED"
                            for p in (self.FIRST, self.SECOND)))
        self._assert_no_mutation()


if __name__ == "__main__":
    unittest.main()
