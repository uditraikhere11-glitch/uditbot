"""R01 offline safety tests: historical Dhan rows with exchangeTradeId=0.

All account/order/exchange values below are synthetic. Never contact Dhan or Telegram.
"""
import datetime
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from src.broker.dhan_client import DhanClient
from src.engine.reconciliation import StartupReconciliationGate
from src.journal.db import JournalDB
from src.models import BrokerReadList, BrokerReadFailure, TradingMode

ACCOUNT = "9000000088"
IST = ZoneInfo("Asia/Kolkata")
NOW = datetime.datetime(2026, 11, 14, 2, 0, tzinfo=IST).timestamp()


def timestamp(day, clock):
    return datetime.datetime.fromisoformat(f"{day}T{clock}").replace(tzinfo=IST).timestamp()


def trade(order_id, exchange_order_id, day, clock, price):
    return {
        "dhanClientId": ACCOUNT,
        "orderId": order_id,
        "exchangeOrderId": exchange_order_id,
        "exchangeTradeId": 0,
        "exchangeTime": f"{day}T{clock}",
        "securityId": "11915",
        "exchangeSegment": "NSE_EQ",
        "productType": "CNC",
        "transactionType": "BUY",
        "isin": "INE528G01035",
        "tradedQuantity": 1,
        "tradedPrice": price,
    }


def as_read_list(rows):
    return BrokerReadList(rows, status="OK_RECORDS" if rows else "OK_EMPTY", status_code=200)


class R01HistoricalZeroTradeIdTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = JournalDB(str(Path(self.tmp.name) / "journal.db"), account_id=ACCOUNT)
        self.first_oid = "900001102657701"
        self.second_oid = "900001104398802"
        self.first = trade(self.first_oid, "EX-ALPHA-01", "2026-11-02", "14:01:19", 15.52)
        self.second = trade(self.second_oid, "EX-BETA-02", "2026-11-04", "11:25:21", 15.51)
        self.broker = MagicMock()
        self.broker.cfg = SimpleNamespace(dhan_client_id=ACCOUNT)
        self.broker.get_authenticated_profile_id.return_value = ACCOUNT
        self.broker.get_holdings.return_value = as_read_list([{
            "securityId": "11915", "isin": "INE528G01035",
            "dpQty": 2, "totalQty": 2,
        }])
        self.broker.get_forever_orders.return_value = as_read_list([])
        self.broker.get_historical_trades.return_value = as_read_list(
            [self._normalized(self.first), self._normalized(self.second)]
        )
        self.watcher = MagicMock()
        self.watcher.pending = {}
        self.watcher.terminal_orders = {}
        self.gate = StartupReconciliationGate(
            broker=self.broker, journal_db=self.db,
            watcher=self.watcher, stop_manager=MagicMock(),
        )

    def _normalized(self, row):
        ok, norm, msg = DhanClient.validate_historical_trade_record(
            row, expected_account_id=ACCOUNT, expected_transaction_type="BUY",
            allow_missing_exchange_trade_id=True,
        )
        self.assertTrue(ok, msg)
        return norm

    def _seed(self, oid, day, clock, cid):
        created = timestamp(day, clock)
        with self.db._get_conn() as conn:
            conn.execute(
                "INSERT INTO order_intents "
                "(correlation_id, account_id, symbol, security_id, exchange_segment, "
                "product_type, order_type, price, quantity, state, broker_order_id, created_at, updated_at) "
                "VALUES (?, ?, 'YESBANK', '11915', 'NSE_EQ', 'CNC', "
                "'LIMIT', 15.60, 1, 'ACKED', ?, ?, ?)",
                (cid, ACCOUNT, oid, created, created),
            )
            conn.execute(
                "INSERT INTO orders "
                "(correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, "
                "symbol, security_id, order_type, price, quantity, status, created_at, updated_at) "
                "VALUES (?, ?, 'LIVE', 'CNC', 'NSE_EQ', 'YESBANK', '11915', 'LIMIT', "
                "15.60, 1, 'PENDING', ?, ?)",
                (cid, oid, created, created),
            )
        return self.db.get_non_terminal_intents()[-1]

    def _seed_two(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        self._seed(self.second_oid, "2026-11-04", "11:25:20", "EQ-BETA-02")
        return self.db.get_non_terminal_intents()

    def _assert_no_verified_fills(self):
        with self.db._get_conn() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM fills WHERE is_verified_trade=1").fetchone()[0], 0
            )

    def test_r01_01_original_real_trade_id_validator_remains_strict(self):
        ok, _, _ = DhanClient.validate_historical_trade_record(self.first)
        self.assertFalse(ok)
        ok, norm, msg = DhanClient.validate_historical_trade_record(
            self.first, allow_missing_exchange_trade_id=True,
        )
        self.assertTrue(ok, msg)
        self.assertIsNone(norm["exchangeTradeId"])
        self.assertTrue(norm["_broker_attested_no_execution_id"])

    def test_r01_02_two_distinct_orders_reconcile_without_invented_ids(self):
        rows = self._seed_two()
        resolved, remaining, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
            rows, now_ts=NOW,
        )
        self.assertIsNone(fatal)
        self.assertEqual(remaining, [])
        self.assertEqual(set(resolved), {"EQ-ALPHA-01", "EQ-BETA-02"})
        self.assertEqual(self.db.get_non_terminal_intents(), [])
        self.assertEqual(self.db.get_position_accounting(self.first_oid)["buy_filled_qty"], 1)
        self.assertEqual(self.db.get_position_accounting(self.second_oid)["buy_filled_qty"], 1)
        with self.db._get_conn() as conn:
            rows = conn.execute(
                "SELECT exchange_trade_id, evidence_source FROM fills ORDER BY broker_order_id"
            ).fetchall()
            self.assertEqual(len(rows), 2)
            for row in rows:
                self.assertIsNone(row[0])
                self.assertEqual(row[1], "BROKER_HISTORICAL_NO_EXECUTION_ID")
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM broker_attested_historical_fills").fetchone()[0], 2
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM system_events WHERE event_type='HISTORICAL_FILL_RECONCILED'"
                ).fetchone()[0], 2
            )
        self.broker.get_holdings.assert_called_once()
        self.broker.get_authenticated_profile_id.assert_called_once()

    def test_r01_03_repeated_evidence_is_idempotent(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        kwargs = dict(
            correlation_id="EQ-ALPHA-01", broker_order_id=self.first_oid,
            expected_account_id=ACCOUNT, symbol="YESBANK", security_id="11915",
            exchange_segment="NSE_EQ", product_type="CNC", requested_quantity=1,
            attested_trade=self._normalized(self.first), now=NOW,
        )
        for _ in range(5):
            result = self.db.apply_attested_historical_order_fill(**kwargs)
            self.assertEqual(result["filled_quantity"], 1)
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM broker_attested_historical_fills").fetchone()[0], 1)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM system_events WHERE event_type='HISTORICAL_FILL_RECONCILED'"
                ).fetchone()[0], 1
            )

    def test_r01_04_insufficient_aggregated_holdings_rejected(self):
        rows = self._seed_two()
        self.broker.get_holdings.return_value = as_read_list([{
            "securityId": "11915", "isin": "INE528G01035", "dpQty": 1
        }])
        resolved, remaining, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
            rows, now_ts=NOW,
        )
        self.assertFalse(resolved)
        self.assertIn("Insufficient verified settled", fatal)
        self._assert_no_verified_fills()

    def test_r01_05_profile_mismatch_fails_closed(self):
        rows = self._seed_two()
        self.broker.get_authenticated_profile_id.return_value = "9999999999"
        resolved, remaining, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
            rows, now_ts=NOW,
        )
        self.assertFalse(resolved)
        self.assertIn("/profile account", fatal)
        self._assert_no_verified_fills()

    def test_r01_06_duplicate_missing_id_rows_are_not_deduplicated(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        self.broker.get_historical_trades.return_value = as_read_list([
            self._normalized(self.first), self._normalized(self.first)
        ])
        _, _, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
            self.db.get_non_terminal_intents(), now_ts=NOW,
        )
        self.assertIn("multiple/mixed historical records", fatal)
        self._assert_no_verified_fills()

    def test_r01_07_cross_order_exchange_order_id_collision_rejected(self):
        rows = self._seed_two()
        bad = {**self.second, "exchangeOrderId": "EX-ALPHA-01"}
        self.broker.get_historical_trades.return_value = as_read_list([
            self._normalized(self.first), self._normalized(bad)
        ])
        _, _, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
            rows, now_ts=NOW,
        )
        self.assertIn("reused across different", fatal)
        self._assert_no_verified_fills()

    def test_r01_08_missing_isin_or_date_fails_closed(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        for bad in (
            {**self.first, "isin": ""},
            {**self.first, "exchangeTime": "2026-11-05T14:01:19"},
        ):
            self.broker.get_historical_trades.return_value = as_read_list([self._normalized(bad)])
            _, _, fatal = self.gate._reconcile_unresolved_intents_via_historical_trades(
                self.db.get_non_terminal_intents(), now_ts=NOW,
            )
            self.assertIsNotNone(fatal)
            self._assert_no_verified_fills()

    def test_r01_09_conflicting_proof_rejected_without_extra_fill(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        common = dict(
            correlation_id="EQ-ALPHA-01", broker_order_id=self.first_oid,
            expected_account_id=ACCOUNT, symbol="YESBANK", security_id="11915",
            exchange_segment="NSE_EQ", product_type="CNC", requested_quantity=1,
        )
        self.db.apply_attested_historical_order_fill(
            **common, attested_trade=self._normalized(self.first),
        )
        bad = self._normalized({**self.first, "tradedPrice": 15.99})
        with self.assertRaisesRegex(ValueError, "Conflicting repeat"):
            self.db.apply_attested_historical_order_fill(**common, attested_trade=bad)
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)

    def test_r01_10_page_completion_and_rate_failure_remain_fail_closed(self):
        obj = DhanClient.__new__(DhanClient)
        obj.cfg = SimpleNamespace(trading_mode=TradingMode.LIVE)
        obj.get_historical_trades_page = MagicMock(side_effect=[
            as_read_list([self._normalized(self.first)]),
            as_read_list([]),
        ])
        result = obj.get_historical_trades("2026-11-02", "2026-11-14")
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["exchangeTradeId"])
        obj.get_historical_trades_page = MagicMock(side_effect=[
            as_read_list([self._normalized(self.first)]),
            BrokerReadFailure(status="FAILED", error="HTTP 429", status_code=429),
        ])
        failed = obj.get_historical_trades("2026-11-02", "2026-11-14")
        self.assertIsInstance(failed, BrokerReadFailure)

    def test_r01_11_later_real_exchange_id_never_double_counts(self):
        self._seed(self.first_oid, "2026-11-02", "14:01:18", "EQ-ALPHA-01")
        self.db.apply_attested_historical_order_fill(
            correlation_id="EQ-ALPHA-01", broker_order_id=self.first_oid,
            expected_account_id=ACCOUNT, symbol="YESBANK", security_id="11915",
            exchange_segment="NSE_EQ", product_type="CNC", requested_quantity=1,
            attested_trade=self._normalized(self.first),
        )
        with self.assertRaisesRegex(ValueError, "source-upgrade review"):
            self.db.record_fill(
                broker_order_id=self.first_oid, exchange_trade_id="REAL-EX-001",
                symbol="YESBANK", traded_price=15.52, traded_quantity=1,
                security_id="11915", exchange_segment="NSE_EQ", product_type="CNC",
                is_verified_trade=True,
            )
        self.assertEqual(self.db.get_position_accounting(self.first_oid)["buy_filled_qty"], 1)


if __name__ == "__main__":
    unittest.main()
