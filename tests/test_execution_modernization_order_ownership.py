"""Adversarial real-schema Dhan order ownership and shadow persistence tests.

Exercises the actual UditBot JournalDB schema, not a simplified fake. All
broker messages are synthetic; no account credentials or network access.
"""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.engine.dhan_order_observer import (
    JournalOwnership, OwnedOrder, ShadowOrderObserver,
)
from src.engine.shadow_event_ledger import ShadowEventLedger
from src.engine.shadow_execution_events import BrokerEvent
from src.journal.db import JournalDB


RAW = {
    "Type": "order_alert",
    "Data": {
        "ClientId": "SYNTHETIC", "OrderNo": "BUY-1", "SecurityId": "SEC",
        "Exchange": "NSE", "Segment": "E", "Product": "C",
        "ProductName": "CNC", "Status": "PENDING",
        "Quantity": 5, "TradedQty": 2, "RemainingQuantity": 3,
        "TxnType": "B", "LastUpdatedTime": "2026-11-21 10:00:00",
    },
}


def message(**changes):
    result = copy.deepcopy(RAW)
    result["Data"].update(changes)
    return result


class TestJournalAccountBoundOwnership(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "real-journal.sqlite3"
        self.shadow_path = Path(self.tmp.name) / "events.shadow.sqlite3"
        self.journal = JournalDB(str(self.db_path), account_id="SYNTHETIC")
        with self.journal._get_conn() as conn:
            conn.execute(
                "INSERT INTO order_intents ("
                "correlation_id, account_id, symbol, security_id, "
                "exchange_segment, product_type, order_type, price, quantity, "
                "state, broker_order_id, created_at, updated_at) "
                "VALUES ('BUY-CORR','SYNTHETIC','SYN','SEC','NSE_EQ','CNC',"
                "'LIMIT',100,5,'PENDING','BUY-1',1,1)"
            )
            conn.execute(
                "INSERT INTO protection_attempts ("
                "attempt_id,parent_order_id,root_order_id,account_id,symbol,"
                "security_id,exchange_segment,product_type,route,quantity,"
                "executed_price,sl_trigger,sl_limit,target_price,correlation_id,"
                "state,broker_order_id,created_at,updated_at) "
                "VALUES ('ATT-1','BUY-1','BUY-1','SYNTHETIC','SYN',"
                "'SEC','NSE_EQ','CNC','FOREVER_OCO',5,100,90,89,110,"
                "'SELL-CORR','ACKED_UNVERIFIED','SELL-1',1,1)"
            )
        self.owner = JournalOwnership(str(self.db_path))

    def tearDown(self):
        self.tmp.cleanup()

    def test_real_journal_identifies_buy_and_protective_sell(self):
        buy = self.owner.find("BUY-1")
        sell = self.owner.find("SELL-1")
        self.assertEqual((buy.account_id, buy.transaction_side, buy.requested_qty),
                         ("SYNTHETIC", "BUY", 5))
        self.assertEqual((sell.account_id, sell.transaction_side, sell.requested_qty),
                         ("SYNTHETIC", "SELL", 5))
        self.assertIsNone(self.owner.find("UNKNOWN-ORDER"))

    def test_account_bound_broker_buy_accepted_without_authorizing_trades(self):
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        self.assertEqual(obs.observe(message()), "OWNED_APPLIED")
        self.assertFalse(obs.summary()["may_authorize_orders"])
        self.assertTrue(obs.summary()["needs_authoritative_rest_sync"])

    def test_buy_identity_rejects_broker_sell_side(self):
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        self.assertEqual(obs.observe(message(TxnType="S")), "REJECTED")
        self.assertIn("OWNED_ORDER_SIDE_CONFLICT", obs.reasons)
        self.assertEqual(len(obs.projections), 0)

    def test_broker_quantity_mismatch_rejected_even_if_cumulative_is_small(self):
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        self.assertEqual(obs.observe(message(Quantity=8)), "REJECTED")
        self.assertIn("OWNED_ORDER_QUANTITY_CONFLICT", obs.reasons)
        self.assertEqual(len(obs.projections), 0)

    def test_valid_sell_cannot_be_confused_with_entry(self):
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        self.assertEqual(obs.observe(message(OrderNo="SELL-1", TxnType="S")),
                         "OWNED_APPLIED")
        self.assertEqual(obs.observe(message(OrderNo="SELL-1", TxnType="B",
                                             LastUpdatedTime="2026-11-21 10:01:00")),
                         "REJECTED")

    def test_missing_side_is_not_accepted_for_durable_bot_owned_order(self):
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        event = message()
        del event["Data"]["TxnType"]
        self.assertEqual(obs.observe(event), "REJECTED")
        self.assertIn("OWNED_ORDER_SIDE_CONFLICT", obs.reasons)

    def test_double_claimed_broker_order_blocks(self):
        with self.journal._get_conn() as conn:
            conn.execute(
                "UPDATE protection_attempts SET broker_order_id='BUY-1' "
                "WHERE attempt_id='ATT-1'"
            )
        with self.assertRaises(ValueError):
            self.owner.find("BUY-1")
        obs = ShadowOrderObserver("SYNTHETIC", self.owner)
        self.assertEqual(obs.observe(message()), "REJECTED")
        self.assertIn("OWNERSHIP_LOOKUP_UNAVAILABLE", obs.reasons)

    def test_rejected_shadow_projection_never_poison_persisted_replay(self):
        with ShadowEventLedger(str(self.shadow_path),
                               account_id="SYNTHETIC") as ledger:
            obs = ShadowOrderObserver("SYNTHETIC", self.owner,
                                      shadow_ledger=ledger)
            self.assertEqual(obs.observe(message()), "OWNED_APPLIED")
            self.assertEqual(ledger.count(), 1)
            evil = BrokerEvent(
                event_id="INVALID-TRADE-1", account_id="SYNTHETIC",
                broker_order_id="BUY-1", security_id="SEC",
                exchange_segment="NSE_EQ", product_type="CNC",
                kind="TRADE", cumulative_filled_qty=2,
                exchange_trade_id="EX-FAIL", traded_qty=99,
            )
            with patch(
                "src.engine.dhan_order_observer.normalize_dhan_order_update",
                return_value=evil
            ):
                self.assertEqual(obs.observe(message()), "REJECTED")
            self.assertEqual(ledger.count(), 1)
            self.assertEqual(obs.projections["BUY-1"].cumulative_filled_qty, 2)
            self.assertIn("PROJECTOR_EVIDENCE_CONFLICT", obs.reasons)
        # New process reconstructs the original valid evidence without a
        # poisoned invalid event, and still requires authoritative REST sync.
        with ShadowEventLedger(str(self.shadow_path),
                               account_id="SYNTHETIC") as ledger:
            recovered = ShadowOrderObserver("SYNTHETIC", self.owner,
                                            shadow_ledger=ledger)
            self.assertEqual(recovered.observe(message()), "OWNED_DUPLICATE")
            self.assertEqual(ledger.count(), 1)
            self.assertTrue(recovered.needs_authoritative_rest_sync)
            self.assertFalse(recovered.summary()["may_assert_verified_fills"])
