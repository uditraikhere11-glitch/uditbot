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

    def _live_schema(self):
        """Production shape with synthetic identifiers and no live API access."""
        self.broker.get_holdings.return_value = [dict(
            exchange="ALL", tradingSymbol="YESBANK", securityId=self.SECURITY,
            isin="SYNTHETIC-YESBANK-ISIN", totalQty=30, dpQty=0, t1Qty=2,
            availableQty=2, mtf_qty=0, mtf_t1_qty=0, collateralQty=0,
        )]
        self.broker.get_account_bound_holdings = Mock(side_effect=lambda: (
            self.ACCOUNT, [dict(r) for r in self.broker.get_holdings.return_value],
        ))
        self.broker.get_positions.return_value[0].update(
            positionType="SHORT", buyQty=0, sellQty=28,
        )
        self.forever[:] = [dict(
            orderId=self.GROUP, orderStatus="TRIGGERED", orderType="OCO",
            legName="TARGET_LEG", securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
            exchangeSegment="NSE_EQ", productType="CNC", transactionType="SELL",
            quantity=28, price=17.9, triggerPrice=0.0,
        )]
        self.broker.get_order_book.return_value[0].update(legName="NA", orderType="LIMIT")
        self.broker.get_trade_book.return_value[0].pop("isin", None)
        rec = self.watcher.terminal_orders[self.THIRD]
        rec.pop("tp_order_id", None)
        rec.pop("sl_order_id", None)
        rec.update(triggered_leg="TARGET_LEG", protected_qty=28,
                   verified_leg_order_ids=[self.TARGET_CHILD],
                   verified_leg_map={self.GROUP: "TARGET_LEG", self.TARGET_CHILD: "TARGET_LEG"})
        with self.db._get_conn() as conn:
            conn.execute("UPDATE positions_lifecycle SET triggered_leg=?, protected_qty=28 WHERE parent_order_id=?",
                         ("TARGET_LEG", self.THIRD))
        self.assertTrue(self.watcher._save())

    def _live_schema_with_legacy_attested_buys_and_split_closed_buy(self):
        self._live_schema()
        import hashlib
        # The actual retained BUYs have no exchange execution IDs; their
        # independent original broker attestations must survive recovery.
        for parent in (self.FIRST, self.SECOND):
            trade = next(r for r in self.history if r["orderId"] == parent)
            trade["exchangeTradeId"] = None
            evidence = {k: trade[k] for k in (
                "dhanClientId", "orderId", "exchangeOrderId", "exchangeTradeId",
                "securityId", "exchangeSegment", "productType", "transactionType",
                "isin", "exchangeTime", "tradedQuantity", "tradedPrice",
            )}
            evidence["source"] = "BROKER_HISTORICAL_NO_EXECUTION_ID"
            canonical = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
            with self.db._get_conn() as conn:
                conn.execute("UPDATE fills SET exchange_trade_id=NULL, evidence_source=? WHERE broker_order_id=?",
                             (evidence["source"], parent))
                conn.execute("INSERT INTO broker_attested_historical_fills "
                             "(account_id, broker_order_id, correlation_id, exchange_order_id, "
                             "evidence_fingerprint, evidence_json, created_at) VALUES (?,?,?,?,?,?,?)",
                             (self.ACCOUNT, parent, parent, trade["exchangeOrderId"],
                              hashlib.sha256(canonical.encode()).hexdigest(), canonical, self.prior))
        third = next(r for r in self.history if r["orderId"] == self.THIRD)
        third["tradedQuantity"] = 16
        with self.db._get_conn() as conn:
            conn.execute("UPDATE fills SET traded_quantity=16 WHERE exchange_trade_id=?",
                         (third["exchangeTradeId"],))
        extra = dict(third, exchangeTradeId="SYNTHETIC-SPLIT-12", tradedQuantity=12)
        self.assertTrue(self.db.record_fill(
            broker_order_id=self.THIRD, exchange_trade_id=extra["exchangeTradeId"],
            symbol="YESBANK-EQ", traded_price=extra["tradedPrice"], traded_quantity=12,
            traded_at=self.prior, transaction_type="BUY", security_id=self.SECURITY,
            exchange_segment="NSE_EQ", product_type="CNC", parent_order_id=self.THIRD,
            is_verified_trade=True,
        ))
        self.history.append(extra)

    def test_live_shape_with_legacy_attested_buys_and_split_closed_buy(self):
        self._live_schema_with_legacy_attested_buys_and_split_closed_buy()
        batch = self._full_watcher_pass()
        self.assertEqual(set(batch["verified"]), {self.FIRST, self.SECOND}, batch)
        self.assert_retained()
        for parent in (self.FIRST, self.SECOND):
            with self.db._get_conn() as conn:
                self.assertIsNone(conn.execute("SELECT exchange_trade_id FROM fills WHERE broker_order_id=?",
                                               (parent,)).fetchone()[0])

    def _replace_split_buy_with_archive(self):
        third = sorted((r for r in self.history if r["orderId"] == self.THIRD),
                       key=lambda r: r["tradedQuantity"], reverse=True)
        self.assertEqual([r["tradedQuantity"] for r in third], [16, 12])
        # Exact observed execution IDs, quantity, price and exchange order ID.
        # Account, other identifiers and clock remain offline fixture values.
        with self.db._get_conn() as conn:
            for row, eid in zip(third, ("609978137", "609978138")):
                conn.execute("UPDATE fills SET exchange_trade_id=?, traded_price=? "
                             "WHERE parent_order_id=? AND exchange_trade_id=?",
                             (eid, 17.72, self.THIRD, row["exchangeTradeId"]))
                row.update(exchangeTradeId=eid, tradedPrice=17.72,
                           exchangeOrderId="1300000097782851")
            conn.execute("UPDATE positions_lifecycle SET buy_vwap=? WHERE parent_order_id=?",
                         (17.72, self.THIRD))
        self.watcher.terminal_orders[self.THIRD]["buy_vwap"] = 17.72
        self.history[:] = [r for r in self.history if r["orderId"] != self.THIRD]
        aggregate = dict(third[0], exchangeTradeId="", tradedQuantity=28)
        self.history.append(aggregate)
        self.assertTrue(self.watcher._save())
        return aggregate, third

    def _archived_rollup_schema(self):
        self._live_schema_with_legacy_attested_buys_and_split_closed_buy()
        aggregate, third = self._replace_split_buy_with_archive()
        # Dhan's historical response contained the original BUYs only.
        # The current-day SELL remains independently verified in tradebook.
        self.history[:] = [r for r in self.history if r["orderId"] != self.TARGET_CHILD]
        for oid in (self.FIRST, self.SECOND):
            self.watcher.terminal_orders[oid].update(
                external_unlinked_sell_observed=True,
                unreadable_broker_response=True,
                reconciliation_required=True,
                position_state="RECONCILIATION_REQUIRED",
            )
        self.assertTrue(self.watcher._save())
        return aggregate, third

    def _assert_archived_rollup_blocked(self):
        gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher, NS(is_stopped=lambda: False),
        )
        self.broker.reconciliation_gate = gate
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok, reason)
        self.assertFalse(gate.is_accepting_entries)
        self.assert_retained()

    def test_archived_rollup_full_startup_restart_and_durable_recovery(self):
        self._archived_rollup_schema()
        self.test_real_startup_restart_open_only_after_independent_validation()
        reopened = JournalDB(str(self.root / "isolated-test-journal.db"),
                             account_id=self.ACCOUNT)
        _, terminal = reopened.load_watcher_state()
        for oid in (self.FIRST, self.SECOND):
            self.assertEqual(terminal[oid]["position_state"], "UNPROTECTED_MANUAL_RISK")
            for flag in ("external_unlinked_sell_observed", "reconciliation_required",
                         "broker_quantity_unverified"):
                self.assertFalse(terminal[oid].get(flag))
        with reopened._get_conn() as conn:
            fills = conn.execute(
                "SELECT exchange_trade_id, traded_quantity FROM fills WHERE parent_order_id=? "
                "AND transaction_type='BUY' ORDER BY traded_quantity", (self.THIRD,)
            ).fetchall()
            self.assertEqual([(r[0], r[1]) for r in fills],
                             [("609978138", 12), ("609978137", 16)])
        for oid in (self.FIRST, self.SECOND):
            self.assertEqual(reopened.get_position_lifecycle(oid)["position_state"],
                             "UNPROTECTED_MANUAL_RISK")

    def test_archived_rollup_settled_next_day_and_restart(self):
        self._settled_rollover_schema()
        self._replace_split_buy_with_archive()
        self._assert_rollover_recovered()

    def test_archived_rollup_normalizer_missing_sentinels_and_no_dedup(self):
        from src.broker.dhan_client import DhanClient
        row, _ = self._archived_rollup_schema()
        for sentinel in ("", None, "0", "NONE", "NULL"):
            with self.subTest(sentinel=sentinel):
                raw = dict(row, exchangeTradeId=sentinel)
                ok, normalized, reason = DhanClient.validate_historical_trade_record(
                    raw, expected_account_id=self.ACCOUNT,
                    allow_missing_exchange_trade_id=True,
                )
                self.assertTrue(ok, reason)
                self.assertIsNone(normalized["exchangeTradeId"])
                self.assertTrue(normalized["_broker_attested_no_execution_id"])
                valid, _, _ = DhanClient.validate_historical_trade_record(
                    raw, expected_account_id=self.ACCOUNT,
                    allow_missing_exchange_trade_id=False,
                )
                self.assertFalse(valid)
        # Real pagination must preserve duplicate ID-less rows so the
        # shared matcher, rather than pagination dedup, can reject them.
        client = object.__new__(DhanClient)
        client.cfg = NS(trading_mode="LIVE")
        client.get_historical_trades_page = Mock(side_effect=[
            [dict(normalized), dict(normalized)], [],
        ])
        day = datetime.fromtimestamp(self.prior, timezone.utc).date().isoformat()
        result = client.get_historical_trades(day, day, page_delay_sec=0)
        self.assertEqual(len(result), 2)

    def test_archived_rollup_wrong_quantity_low(self):
        row, _ = self._archived_rollup_schema()
        row["tradedQuantity"] = 27
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_quantity_high(self):
        row, _ = self._archived_rollup_schema()
        row["tradedQuantity"] = 29
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_price(self):
        row, _ = self._archived_rollup_schema()
        row["tradedPrice"] += 0.03
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_blank_live_sell_id(self):
        self._archived_rollup_schema()
        self.broker.get_trade_book.return_value[0]["exchangeTradeId"] = ""
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_duplicate(self):
        row, _ = self._archived_rollup_schema()
        self.history.append(dict(row))
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_mixed_exact_and_aggregate(self):
        _, originals = self._archived_rollup_schema()
        self.history.append(dict(originals[0]))
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_unknown_buy(self):
        row, _ = self._archived_rollup_schema()
        self.history.append(dict(row, orderId="UNKNOWN-BUY"))
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_segment(self):
        row, _ = self._archived_rollup_schema()
        row["exchangeSegment"] = "BSE_EQ"
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_product(self):
        row, _ = self._archived_rollup_schema()
        row["productType"] = "INTRADAY"
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_date(self):
        row, _ = self._archived_rollup_schema()
        row["exchangeTime"] = self.sell["exchangeTime"]
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_wrong_account(self):
        row, _ = self._archived_rollup_schema()
        row["dhanClientId"] = "OTHER"
        self._assert_archived_rollup_blocked()

    def test_archived_rollup_missing_exchange_order_identity(self):
        row, _ = self._archived_rollup_schema()
        row["exchangeOrderId"] = ""
        self._assert_archived_rollup_blocked()

    def test_pinned_broker_method_uses_one_credential_snapshot(self):
        from src.broker.dhan_client import DhanClient, TradingMode
        client = object.__new__(DhanClient)
        client.cfg = NS(dhan_client_id=self.ACCOUNT, trading_mode=TradingMode.LIVE)
        headers = {"client-id": self.ACCOUNT, "access-token": "synthetic-no-live-token"}
        client._headers = Mock(return_value=headers)
        profile = NS(status_code=200, json=lambda: {"dhanClientId": self.ACCOUNT})
        holdings = NS(status_code=200, json=lambda: [{"securityId": self.SECURITY}])
        client._governed_get = Mock(side_effect=[profile, holdings])
        self.assertEqual(client.get_account_bound_holdings(), (self.ACCOUNT, [{"securityId": self.SECURITY}]))
        client._headers.assert_called_once()
        calls = client._governed_get.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0].args[0].endswith("/profile"))
        self.assertTrue(calls[1].args[0].endswith("/holdings"))
        self.assertIs(calls[0].kwargs["headers"], calls[1].kwargs["headers"])
        self.assertEqual(calls[0].kwargs["headers"], headers)
        client._governed_get.reset_mock()
        client._governed_get.side_effect = [NS(status_code=200, json=lambda: {"dhanClientId": "OTHER"})]
        self.assertEqual(client.get_account_bound_holdings(), (None, None))
        client._governed_get.assert_called_once()

    def test_live_shape_requires_pinned_account_and_recovers_watcher(self):
        self._live_schema()
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()  # Missing row account is never accepted from raw data alone.
        batch = self._full_watcher_pass()
        self.assertEqual(set(batch["verified"]), {self.FIRST, self.SECOND}, batch)
        self.broker.get_account_bound_holdings.assert_called_once()
        for oid in (self.FIRST, self.SECOND):
            self.assertEqual(self.watcher.terminal_orders[oid]["position_state"], "UNPROTECTED_MANUAL_RISK")
        self.broker.reconciliation_gate.halt_new_entries.assert_not_called()
        self.assert_retained()

    def test_combined_release_live_shape_stale_flags_legacy_buys_full_restart(self):
        self._live_schema_with_legacy_attested_buys_and_split_closed_buy()
        for oid in (self.FIRST, self.SECOND):
            rec = self.watcher.terminal_orders[oid]
            rec.update(external_unlinked_sell_observed=True,
                       unreadable_broker_response=True,
                       reconciliation_required=True,
                       position_state="RECONCILIATION_REQUIRED")
        self.assertTrue(self.watcher._save())
        self.test_real_startup_restart_open_only_after_independent_validation()
        reopened = JournalDB(str(self.root / "isolated-test-journal.db"),
                             account_id=self.ACCOUNT)
        _, terminal = reopened.load_watcher_state()
        for oid in (self.FIRST, self.SECOND):
            rec = terminal[oid]
            self.assertFalse(rec.get("external_unlinked_sell_observed"), rec)
            self.assertFalse(rec.get("reconciliation_required"), rec)
            self.assertEqual(rec["position_state"], "UNPROTECTED_MANUAL_RISK", rec)
            self.assertEqual(reopened.get_position_lifecycle(oid)["position_state"],
                             "UNPROTECTED_MANUAL_RISK")
        self.assert_retained()

    def _settled_rollover_schema(self, *, triggered=True):
        """Simulate deployment after settlement with original stale flags."""
        self._live_schema_with_legacy_attested_buys_and_split_closed_buy()
        old_sell_at = self.now - 86400
        old_sell_time = datetime.fromtimestamp(
            old_sell_at, __import__("zoneinfo").ZoneInfo("Asia/Kolkata")
        ).strftime("%Y-%m-%d %H:%M:%S")
        for trade in self.history:
            if trade["exchangeTradeId"] == self.SELL_TRADE:
                trade["exchangeTime"] = old_sell_time
        with self.db._get_conn() as conn:
            conn.execute("UPDATE fills SET traded_at=? WHERE exchange_trade_id=?",
                         (old_sell_at, self.SELL_TRADE))
        self.broker.get_positions.return_value = []
        self.broker.get_trade_book.return_value = []
        self.broker.get_holdings.return_value[0].update(
            totalQty=2, dpQty=2, t1Qty=0, availableQty=2,
        )
        child = dict(self.broker.get_order_book.return_value[0])
        self.broker.get_order_book.return_value = []
        self.broker.order_status = Mock(side_effect=lambda oid: (
            dict(child) if oid == self.TARGET_CHILD else None
        ))
        if not triggered:
            self.forever[:] = []
        for oid in (self.FIRST, self.SECOND):
            self.watcher.terminal_orders[oid].update(
                external_unlinked_sell_observed=True,
                unreadable_broker_response=True,
                reconciliation_required=True,
                position_state="RECONCILIATION_REQUIRED",
            )
        self.assertTrue(self.watcher._save())

    def _assert_rollover_recovered(self):
        self.test_real_startup_restart_open_only_after_independent_validation()
        reopened = JournalDB(str(self.root / "isolated-test-journal.db"),
                             account_id=self.ACCOUNT)
        _, terminal = reopened.load_watcher_state()
        for oid in (self.FIRST, self.SECOND):
            self.assertFalse(terminal[oid].get("external_unlinked_sell_observed"))
            self.assertFalse(terminal[oid].get("reconciliation_required"))
            self.assertEqual(terminal[oid]["position_state"], "UNPROTECTED_MANUAL_RISK")
            self.assertEqual(reopened.get_position_lifecycle(oid)["position_state"],
                             "UNPROTECTED_MANUAL_RISK")
        self.assert_retained()

    def test_rollover_persistent_triggered_child_readback_and_restart(self):
        self._settled_rollover_schema()
        self._assert_rollover_recovered()
        self.broker.order_status.assert_any_call(self.TARGET_CHILD)

    def test_rollover_retired_group_and_restart(self):
        self._settled_rollover_schema(triggered=False)
        self._assert_rollover_recovered()

    def test_rollover_missing_child_status_remains_blocked(self):
        self._settled_rollover_schema()
        self.broker.order_status.side_effect = None
        self.broker.order_status.return_value = None
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assertTrue(self.watcher.terminal_orders[self.FIRST]["external_unlinked_sell_observed"])
        self.assert_retained()

    def test_rollover_pending_sibling_remains_blocked(self):
        self._settled_rollover_schema()
        self.forever.append(dict(self.forever[0], legName="STOP_LOSS_LEG", orderStatus="PENDING"))
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_rollover_fresh_unknown_sell_remains_blocked(self):
        self._settled_rollover_schema(triggered=False)
        unknown = dict(self.sell, orderId="UNKNOWN-SELL", exchangeTradeId="UNKNOWN-EXEC",
                       tradedQuantity=1)
        self.broker.get_trade_book.return_value = [unknown]
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assertTrue(self.watcher.terminal_orders[self.FIRST]["external_unlinked_sell_observed"])
        self.assert_retained()

    def test_rollover_history_missing_closed_sell_remains_blocked(self):
        self._settled_rollover_schema(triggered=False)
        self.history[:] = [t for t in self.history if t["exchangeTradeId"] != self.SELL_TRADE]
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_rollover_unverified_holding_account_remains_blocked(self):
        self._settled_rollover_schema(triggered=False)
        self.broker.get_account_bound_holdings.side_effect = None
        self.broker.get_account_bound_holdings.return_value = ("OTHER", self.broker.get_holdings.return_value)
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_signed_position_rejects_offsetting_omitted_executions(self):
        self.broker.get_positions.return_value[0].update(dayBuyQty=5, daySellQty=33)
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_signed_position_rejects_partial_day_totals(self):
        row = self.broker.get_positions.return_value[0]
        row.pop("daySellQty")
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        row.pop("dayBuyQty")
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_signed_position_rejects_malformed_other_contract(self):
        original = dict(self.broker.get_positions.return_value[0])
        for value in (None, False, 0.0, "", "bad"):
            with self.subTest(netQty=value):
                self.broker.get_positions.return_value = [
                    dict(original), dict(original, productType="MTF", netQty=value)
                ]
                with self.assertRaises(SignedCNCDeliveryUnverified):
                    self.proof()
        self.assert_retained()

    def test_signed_position_rejects_null_carry_forward(self):
        self.broker.get_positions.return_value[0]["carryForwardSellQty"] = None
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_live_shape_full_startup_and_restart(self):
        self._live_schema()
        self.test_real_startup_restart_open_only_after_independent_validation()

    def test_live_shape_startup_wrong_pinned_profile_blocks(self):
        self._live_schema()
        self.broker.get_account_bound_holdings.side_effect = None
        self.broker.get_account_bound_holdings.return_value = (
            "OTHER", self.broker.get_holdings.return_value,
        )
        gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher, NS(is_stopped=lambda: False),
        )
        self.broker.reconciliation_gate = gate
        ok, detail = gate.run_startup_reconciliation()
        self.assertFalse(ok, detail)
        self.assertFalse(gate.is_accepting_entries)
        self.assertIn("Signed delivery holdings or closed sibling proof unavailable", detail)
        self.assert_retained()

    def test_live_shape_owner_stop_blocks(self):
        self._live_schema()
        self.test_unchanged_owner_stop_fences_healthy_signed_delivery()

    def test_live_shape_wrong_pinned_profile_blocks(self):
        self._live_schema()
        self.broker.get_account_bound_holdings.side_effect = None
        self.broker.get_account_bound_holdings.return_value = ("OTHER", self.broker.get_holdings.return_value)
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_changed_pinned_holdings_blocks(self):
        self._live_schema()
        self.broker.get_account_bound_holdings.side_effect = None
        changed = [dict(self.broker.get_holdings.return_value[0], totalQty=31)]
        self.broker.get_account_bound_holdings.return_value = (self.ACCOUNT, changed)
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_explicit_holdings_account_mismatch_blocks(self):
        self._live_schema()
        self.broker.get_holdings.return_value[0]["dhanClientId"] = "OTHER"
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_triggered_with_pending_sibling_blocks(self):
        self._live_schema()
        self.forever.append(dict(self.forever[0], legName="STOP_LOSS_LEG", orderStatus="PENDING"))
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_triggered_with_wrong_child_algo_blocks(self):
        self._live_schema()
        self.broker.get_order_book.return_value[0]["algoId"] = "OTHER-GROUP"
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_triggered_with_unfilled_child_blocks(self):
        self._live_schema()
        self.broker.get_order_book.return_value[0]["remainingQuantity"] = 1
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_triggered_with_conflicting_leg_map_blocks(self):
        self._live_schema()
        self.watcher.terminal_orders[self.THIRD]["verified_leg_map"][self.TARGET_CHILD] = "STOP_LOSS_LEG"
        self.assertTrue(self.watcher._save())
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

    def test_live_shape_triggered_with_unknown_group_blocks(self):
        self._live_schema()
        self.forever[0]["orderId"] = "UNBOUND-GROUP"
        self.assertFalse(self._full_watcher_pass()["verified"])
        self.assert_retained()

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
            self.assertEqual(rec["position_state"], "UNPROTECTED_MANUAL_RISK",
                             {"rec": rec, "gate_halts": self.broker.reconciliation_gate.halt_new_entries.call_args_list,
                              "batch": batch})
            self.assertEqual(rec["confirmed_position_open_qty"], 1)
            self.assertEqual(rec["exit_filled_qty"], 0)
        self.broker.reconciliation_gate.halt_new_entries.assert_not_called()
        self.assert_retained()

    def test_durable_stale_external_sell_flag_recovered_only_after_exact_proof(self):
        # Recreate the production halt's sticky watcher state: the closed
        # sibling child was previously labelled an external/unlinked SELL.
        # Recovery cannot merely accept the signed quantity while retaining
        # this stale flag: otherwise every later pass remains BLOCKED.
        for oid in (self.FIRST, self.SECOND):
            rec = self.watcher.terminal_orders[oid]
            rec["external_unlinked_sell_observed"] = True
            rec["unreadable_broker_response"] = True
            rec["reconciliation_required"] = True
            rec["position_state"] = "RECONCILIATION_REQUIRED"
        self.assertTrue(self.watcher._save())
        batch = self._full_watcher_pass()
        self.assertEqual(set(batch["verified"]), {self.FIRST, self.SECOND}, batch)
        for oid in (self.FIRST, self.SECOND):
            rec = self.watcher.terminal_orders[oid]
            self.assertFalse(rec.get("external_unlinked_sell_observed"), rec)
            self.assertEqual(rec["position_state"], "UNPROTECTED_MANUAL_RISK", rec)
            self.assertFalse(rec.get("reconciliation_required"), rec)
        self.broker.reconciliation_gate.halt_new_entries.assert_not_called()
        self.assert_retained()

    def test_closed_child_id_claimed_by_other_parent_blocks_recovery(self):
        # C8: A forged/stale active owner claim for the closed sibling's
        # child SELL order ID cannot be excused by otherwise valid 30-28 math.
        first = self.watcher.terminal_orders[self.FIRST]
        first["verified_leg_order_ids"] = [self.TARGET_CHILD]
        self.assertTrue(self.watcher._save())
        self._full_watcher_pass()
        reasons = [
            str(call.args[0]) for call in
            self.broker.reconciliation_gate.halt_new_entries.call_args_list
        ]
        self.assertTrue(any("conflicting bot owner claim" in x for x in reasons), reasons)
        self.assertEqual(
            self.watcher.terminal_orders[self.FIRST]["position_state"],
            "RECONCILIATION_REQUIRED",
        )
        self.assert_retained()

    def test_real_startup_restart_open_only_after_independent_validation(self):
        self.broker.reconciliation_gate = NS(halt_new_entries=Mock())
        gate = StartupReconciliationGate(
            self.broker, self.db, self.watcher, NS(is_stopped=lambda: False),
        )
        self.broker.reconciliation_gate = gate
        success, reason = gate.run_startup_reconciliation()
        self.assertTrue(success, {"reason": reason,
                                  "gate": gate.last_error,
                                  "first": self.watcher.terminal_orders.get(self.FIRST),
                                  "second": self.watcher.terminal_orders.get(self.SECOND)})
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

    def test_invalid_negative_or_boolean_day_component_rejected(self):
        self.broker.get_positions.return_value[0]["dayBuyQty"] = -1
        self.broker.get_positions.return_value[0]["daySellQty"] = 27
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.broker.get_positions.return_value[0]["dayBuyQty"] = True
        self.broker.get_positions.return_value[0]["daySellQty"] = 29
        with self.assertRaises(SignedCNCDeliveryUnverified):
            self.proof()
        self.assert_retained()

    def test_broker_exchange_time_from_prior_day_not_current_sell(self):
        self.broker.get_trade_book.return_value[0]["exchangeTime"] = datetime.fromtimestamp(
            self.prior, __import__("zoneinfo").ZoneInfo("Asia/Kolkata")
        ).strftime("%Y-%m-%d %H:%M:%S")
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
