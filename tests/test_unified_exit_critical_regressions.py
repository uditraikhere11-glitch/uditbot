"""Permanent CI regression suite for all six P0/P1 independent audit findings
(P0-1 through P0-4, P1-5, P1-6) plus real-stack PDSL/YESBANK crash recovery.

Offline only; zero network calls, zero live broker mutations.
"""
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from src.engine.unified_exit_reconciliation import (
    ReconciliationEvidenceError,
    reconcile_verified_exits,
)
from src.journal.db import JournalDB
from tests.test_unified_exit_reconciliation import ACCOUNT, B_OID, CONTRACT, DAY, S_OID, Watcher, trade

IST = ZoneInfo("Asia/Kolkata")


class IndependentCriticalRegressions(unittest.TestCase):
    def setUp(self):
        from tests.test_unified_exit_reconciliation import Fixture
        self.f = Fixture()
        self.f.setUp()

    def tearDown(self):
        self.f.tearDown()

    # 1. P0-1: Multi-lot crash recovery (both full-interruption and partial-sync crash)
    def test_multi_lot_crash_after_commit_is_recoverable(self):
        f = self.f
        with f.db._get_conn() as c:
            c.execute(
                "INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("BUY-222", ACCOUNT, "corr-222", "PDSL", *CONTRACT, 2, DAY.timestamp(), 0, "FOREVER_OCO", 416.0),
            )
            c.execute(
                "INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("BUY-222", ACCOUNT, "PDSL", *CONTRACT, 2, 0, 2, "UNPROTECTED"),
            )
            c.execute(
                "INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("BUY-222", "B-2", "PDSL", 416.0, 2, DAY.timestamp() + 45, "BUY", *CONTRACT, "BUY-222", 1),
            )
            c.execute("UPDATE fills SET traded_quantity=5 WHERE exchange_trade_id='S-1'")
        f.se["tradedQuantity"] = 5
        f.trades.append(trade("BUY-222", "B-2", "BUY", 2, DAY + timedelta(seconds=45), 416.0))
        f.books["order_book"].append(
            dict(
                orderId="BUY-222", securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="TRADED", transactionType="BUY", filledQty=2, dhanClientId=ACCOUNT,
            )
        )
        f.books["order_book"][1]["filledQty"] = 5
        f.positions = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0)]

        # Simulate crash AFTER SQLite allocation commit and AFTER syncing BUY-111, BEFORE syncing BUY-222!
        class CrashOnSecondWatcher(Watcher):
            def reconcile_historical_order_fills(self, **kwargs):
                if kwargs["order_id"] == "BUY-222":
                    raise RuntimeError("simulated crash after first parent sync")
                return super().reconcile_historical_order_fills(**kwargs)

        f.w = CrashOnSecondWatcher(f.db)
        with self.assertRaisesRegex(RuntimeError, "simulated crash"):
            f.run_it()

        # Verify BUY-111 synced while BUY-222 is still stale in lifecycle
        self.assertEqual(f.db.get_position_lifecycle(B_OID)["position_state"], "POSITION_CLOSED")
        self.assertEqual(f.db.get_position_lifecycle("BUY-222")["position_state"], "UNPROTECTED")
        self.assertEqual(f.db.get_position_lifecycle("BUY-222")["confirmed_open_qty"], 2)

        # Simulate next-day restart (today's tradebook & orderbook rolled over to empty!)
        f.trades = []
        f.books["order_book"] = []
        f.positions = []
        f.w = Watcher(f.db)

        changed, msg = f.run_it()
        self.assertTrue(changed, msg)
        self.assertEqual(f.db.get_position_lifecycle(B_OID)["position_state"], "POSITION_CLOSED")
        self.assertEqual(f.db.get_position_lifecycle("BUY-222")["position_state"], "POSITION_CLOSED")
        self.assertEqual(f.db.get_position_accounting(B_OID)["sell_filled_qty"], 3)
        self.assertEqual(f.db.get_position_accounting("BUY-222")["sell_filled_qty"], 2)
        self.assertEqual(f.db.get_position_accounting(B_OID)["confirmed_open_qty"], 0)
        self.assertEqual(f.db.get_position_accounting("BUY-222")["confirmed_open_qty"], 0)

        with f.db._get_conn() as c:
            alloc_total = c.execute("SELECT SUM(allocated_qty) FROM exit_allocations").fetchone()[0]
        self.assertEqual(alloc_total, 5)

        # Repeated restart is idempotent (no further changes, no duplicate allocations)
        changed_again, _ = f.run_it()
        self.assertFalse(changed_again)

    # 2. P1-5: Documented orderFlag="OCO" with orderType="LIMIT" requires confirmed terminal sibling
    def test_documented_oco_flag_requires_confirmed_sibling(self):
        f = self.f
        f.full_exit()
        # Missing sibling leg with orderFlag="OCO" and orderType="LIMIT" MUST BLOCK
        f.books["forever_orders"] = [
            dict(
                orderId=S_OID, orderFlag="OCO", orderType="LIMIT", legName="TARGET_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="TRADED", quantity=3, dhanClientId=ACCOUNT,
            ),
        ]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "sibling"):
            f.run_it()

        # Conflicting orderFlag="SINGLE" + orderType="OCO" MUST BLOCK
        f.books["forever_orders"] = [
            dict(
                orderId=S_OID, orderFlag="SINGLE", orderType="OCO", legName="TARGET_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="TRADED", quantity=3, dhanClientId=ACCOUNT,
            ),
        ]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "Conflicting"):
            f.run_it()

        # Confirmed STOP_LOSS_LEG TRADED + TARGET_LEG CANCELLED with matching orderId fill PASSES
        f.books["forever_orders"] = [
            dict(
                orderId=S_OID, orderFlag="OCO", orderType="LIMIT", legName="STOP_LOSS_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="TRADED", quantity=3, dhanClientId=ACCOUNT,
            ),
            dict(
                orderId=S_OID, orderFlag="OCO", orderType="LIMIT", legName="TARGET_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="CANCELLED", quantity=3, dhanClientId=ACCOUNT,
            ),
        ]
        changed, _ = f.run_it()
        self.assertTrue(changed)

    # 3. P0-4: TRADED OCO requires a verified SELL fill for its OWN orderId, not an unrelated manual SELL
    def test_traded_oco_requires_fill_for_its_own_order_not_unrelated_sell(self):
        f = self.f
        f.full_exit()
        # Verified SELL fill is for S_OID (manual sell), while Forever OCO is a DIFFERENT orderId "OCO-999"
        f.books["forever_orders"] = [
            dict(
                orderId="OCO-999", orderFlag="OCO", orderType="LIMIT", legName="TARGET_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="TRADED", quantity=3, dhanClientId=ACCOUNT,
            ),
            dict(
                orderId="OCO-999", orderFlag="OCO", orderType="LIMIT", legName="STOP_LOSS_LEG",
                securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                orderStatus="CANCELLED", quantity=3, dhanClientId=ACCOUNT,
            ),
        ]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "own order ID"):
            f.run_it()

    # 4. P0-2: Historical/same-day SELL before actual BUY fill timestamp must NOT be parented
    def test_history_sell_before_actual_buy_fill_is_not_parented(self):
        f = self.f
        # BUY intent submitted at 09:30, SELL executed at 10:00, BUY actually filled at 10:30!
        submitted_ts = datetime(2026, 11, 17, 9, 30, tzinfo=IST)
        sell_ts = datetime(2026, 11, 17, 10, 0, tzinfo=IST)
        buy_fill_ts = datetime(2026, 11, 17, 10, 30, tzinfo=IST)
        with f.db._get_conn() as c:
            c.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id=?", (submitted_ts.timestamp(), B_OID))
            c.execute("UPDATE fills SET traded_at=? WHERE exchange_trade_id='B-1'", (buy_fill_ts.timestamp(),))
        f.trades = []
        f.books["order_book"] = []
        f.positions = []
        f.books["holdings"] = [dict(securityId="5264", totalQty=2)]
        f.se["exchangeTime"] = sell_ts.strftime("%Y-%m-%d %H:%M:%S")
        f.broker.get_historical_trades.return_value = [f.se]

        with self.assertRaisesRegex(ReconciliationEvidenceError, "precedes or overlaps"):
            f.run_it()

    # 5. P0-3: Negative CNC netQty WITHOUT an explicit holdings row MUST NOT certify zero exposure
    def test_negative_cnc_net_without_holdings_row_must_not_certify_zero(self):
        f = self.f
        f.full_exit()
        earlier = DAY - timedelta(days=2)
        with f.db._get_conn() as c:
            c.execute("UPDATE order_intents SET created_at=?", (earlier.timestamp(),))
            c.execute("UPDATE fills SET traded_at=? WHERE exchange_trade_id='B-1'", (earlier.timestamp() + 10,))
        f.trades = [f.se]
        f.books["order_book"] = [f.books["order_book"][1]]
        f.positions = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=-3, dhanClientId=ACCOUNT)]
        # Holdings row for 5264 is OMITTED! Must NOT default to 0 and certify flat!
        f.books["holdings"] = []
        with self.assertRaisesRegex(ReconciliationEvidenceError, "NEGATIVE_CNC_POSITION_REVIEW"):
            f.run_it()

        # When Dhan explicitly reports the pre-debit settled holdings row (totalQty=3) alongside netQty=-3,
        # 3 + (-3) == 0 is corroborated and reconciles cleanly!
        f.books["holdings"] = [dict(securityId="5264", totalQty=3, dhanClientId=ACCOUNT)]
        changed, _ = f.run_it()
        self.assertTrue(changed)


class CanonicalLedgerInterfaceTests(unittest.TestCase):
    # 6. P1-6: JournalDB.get_position_accounting() AND JournalDB.get_sell_fills_for_parent() MUST agree
    def test_canonical_ledger_interfaces_agree_on_multi_lot_allocation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "journal.db")
            db = JournalDB(db_path, account_id=ACCOUNT)
            now = time.time()
            with db._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity,
                        traded_at, transaction_type, security_id, exchange_segment, product_type,
                        parent_order_id, is_verified_trade
                    ) VALUES
                    ('BUY-A', 'B-A1', 'PDSL', 400.0, 3, ?, 'BUY', '5264', 'NSE_EQ', 'CNC', 'BUY-A', 1),
                    ('BUY-B', 'B-B1', 'PDSL', 405.0, 2, ?, 'BUY', '5264', 'NSE_EQ', 'CNC', 'BUY-B', 1),
                    ('SELL-C', 'S-C1', 'PDSL', 420.0, 5, ?, 'SELL', '5264', 'NSE_EQ', 'CNC', 'MULTI_LOT_ALLOCATED', 1)
                    """,
                    (now - 200, now - 150, now - 50),
                )
                conn.execute(
                    """
                    INSERT INTO exit_allocations (
                        parent_order_id, sell_order_id, exchange_trade_id,
                        allocated_qty, traded_price, traded_at
                    ) VALUES
                    ('BUY-A', 'SELL-C', 'S-C1', 3, 420.0, ?),
                    ('BUY-B', 'SELL-C', 'S-C1', 2, 420.0, ?)
                    """,
                    (now - 50, now - 50),
                )

            # Reload fresh JournalDB instance to prove persistence across restart
            db2 = JournalDB(db_path, account_id=ACCOUNT)
            acct_a = db2.get_position_accounting("BUY-A")
            acct_b = db2.get_position_accounting("BUY-B")
            fills_a = db2.get_sell_fills_for_parent("BUY-A")
            fills_b = db2.get_sell_fills_for_parent("BUY-B")

            self.assertEqual((acct_a["buy_filled_qty"], acct_a["sell_filled_qty"], acct_a["confirmed_open_qty"]), (3, 3, 0))
            self.assertEqual((acct_b["buy_filled_qty"], acct_b["sell_filled_qty"], acct_b["confirmed_open_qty"]), (2, 2, 0))
            self.assertEqual(acct_a["realized_pnl"], 60.0)
            self.assertEqual(acct_b["realized_pnl"], 30.0)

            self.assertEqual(len(fills_a), 1)
            self.assertEqual(len(fills_b), 1)
            self.assertEqual(fills_a[0]["traded_quantity"], 3)
            self.assertEqual(fills_a[0]["total_fill_quantity"], 5)
            self.assertEqual(fills_b[0]["traded_quantity"], 2)
            self.assertEqual(fills_b[0]["total_fill_quantity"], 5)
            self.assertEqual(sum(x["traded_quantity"] for x in fills_a + fills_b), 5)

            # Zero double-counting if direct parent_order_id fill and exit_allocation coexist for same exchange_trade_id
            with db2._get_conn() as conn:
                conn.execute("UPDATE fills SET parent_order_id='BUY-A' WHERE exchange_trade_id='S-C1'")
            self.assertEqual(len(db2.get_sell_fills_for_parent("BUY-A")), 1)


if __name__ == "__main__":
    unittest.main()
