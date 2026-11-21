"""Phase 2 broker-tradebook preflight: no partial fill persistence.

All rows synthetic; no broker calls. This tests the exact batch preflight
now invoked before the canonical startup reconciliation fill loop.
"""
import unittest
from src.engine.broker_tradebook_preflight import (
    TradebookPreflightError, preflight_tradebook,
)

ROW = {
    "dhanClientId": "ACC", "orderId": "OID", "exchangeTradeId": "EX1",
    "securityId": "SEC", "exchangeSegment": "NSE_EQ", "productType": "CNC",
    "transactionType": "BUY", "tradedQuantity": 2, "tradedPrice": 100.5,
    "exchangeTime": "2026-11-21 09:35:00",
}


class TestTradebookPreflight(unittest.TestCase):
    def test_valid_unique_buy_and_sell(self):
        sell = {**ROW, "orderId": "SELL", "exchangeTradeId": "EX2",
                "transactionType": "SELL", "tradedQuantity": 1}
        self.assertEqual(preflight_tradebook([ROW, sell], expected_account_id="ACC"), 2)

    def test_exact_duplicate_is_counted_once(self):
        self.assertEqual(preflight_tradebook([ROW, dict(ROW)], expected_account_id="ACC"), 1)

    def test_conflicting_execution_id_never_authorizes_partial_batch(self):
        for key, value in (
            ("orderId", "OTHER"), ("dhanClientId", "OTHER"),
            ("securityId", "OTHER"), ("exchangeSegment", "NSE_FNO"),
            ("productType", "MTF"), ("transactionType", "SELL"),
            ("tradedQuantity", 3), ("tradedPrice", 98.5),
            ("exchangeTime", "2026-11-21 09:35:01"),
        ):
            with self.subTest(key=key):
                conflicting={**ROW,key:value}
                with self.assertRaises(TradebookPreflightError):
                    preflight_tradebook(
                        [ROW, conflicting], expected_account_id="ACC",
                    )

    def test_invalid_late_row_blocks_all_records(self):
        for key, bad in (
            ("exchangeTradeId", ""), ("exchangeTradeId", "0"),
            ("exchangeTradeId", "TRD-FILL-1"), ("orderId", ""),
            ("transactionType", "HOLD"), ("tradedQuantity", 0),
            ("tradedQuantity", True), ("tradedQuantity", -1),
            ("tradedQuantity", 2.5), ("tradedPrice", float("nan")),
            ("tradedPrice", False), ("tradedPrice", "inf"),
        ):
            with self.subTest(key=key):
                with self.assertRaises(TradebookPreflightError):
                    preflight_tradebook(
                        [ROW, {**ROW,"exchangeTradeId": "EX-LATE",key:bad}],
                        expected_account_id="ACC",
                    )

    def test_duplicate_no_side_effects_on_source_rows(self):
        rows=[dict(ROW),{**ROW,"exchangeTradeId":"EX2","tradedQuantity":1}]
        before=[dict(v) for v in rows]
        self.assertEqual(preflight_tradebook(rows,expected_account_id="ACC"),2)
        self.assertEqual(rows,before)

    def test_malformed_collection_is_rejected(self):
        for bad in (None, (), {}, [ROW, None], ["not a row"]):
            with self.subTest(value=type(bad).__name__):
                with self.assertRaises(TradebookPreflightError):
                    preflight_tradebook(bad,expected_account_id="ACC")

    def test_missing_account_row_preserves_previous_acceptance(self):
        self.assertEqual(preflight_tradebook([{**ROW,"dhanClientId":None}],
                                              expected_account_id="ACC"),1)

    def test_only_one_completed_ledger_identity_per_execution(self):
        rows=[{**ROW,"exchangeTradeId":f"EX{i}"} for i in range(300)]
        self.assertEqual(preflight_tradebook(rows,expected_account_id="ACC"),300)
        rows.append({**ROW,"exchangeTradeId":"EX299","tradedQuantity":4})
        with self.assertRaises(TradebookPreflightError):
            preflight_tradebook(rows,expected_account_id="ACC")
