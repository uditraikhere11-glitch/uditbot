"""Phase 2D Targeted Release-Blocking Corrections Certification Tests (C63-C68).

Covers:
- C63: Pydantic v2 + v1 compatibility (no unsupported legacy `field` arg; imports & validation)
- C64: SELL execution contract binding (wrong security/segment/product SELL never clears risk)
- C65: Contradictory allocation evidence (replaces max(direct, alloc) with exact execution-to-allocation linkage)
- C66: Semantic authorization & wire binding (canonical vs wire mismatch rejected; mandatory LIVE op on POST/PUT/DELETE)
- C67: Complete snapshot freshness on BOTH s1 and s2 (29s old s1 + 1s old s2 rejected, future/reversed/missing ID/reused issue)
- C68: Numeric range & OverflowError handling (10**30 quantity, 10**400 price cleanly rejected without unhandled OverflowError)
"""
import ast
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from src.broker.broker_snapshot import Generation, account_key
from src.broker.dhan_client import (
    AuthorizedBrokerOperation,
    DhanClient,
    _validate_raw_strict_int,
    _validate_raw_strict_number,
)
from src.config import AppConfig, TradingMode
from src.engine.broker_handoff import BrokerMutationNotSubmitted, proc_starttime
from src.engine.durable_issue_writer import verify_operation_readback_and_clear_persistence_risk
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import OrderRequest, OrderType, ProductType, ProtectionLevels, TransactionType

ACCT = "1113731536"


def _admit_gate():
    gate = MagicMock()
    gate.is_accepting_entries = True
    gate.revalidate_for_transport_submission.return_value = (True, "ADMITTED")
    return gate


def _fake_resp(status=200, payload=None):
    r = MagicMock()
    r.status_code = status
    r.headers = {}
    body = payload if payload is not None else {"orderId": "88000111", "orderStatus": "PENDING"}
    r.text = json.dumps(body)
    r.json.return_value = body
    return r


def _make_live_client(td: str, gate=None):
    cfg = AppConfig(
        trading_mode=TradingMode.LIVE,
        dhan_client_id=ACCT,
        dhan_access_token="dummy-token",
        data_dir=td,
    )
    Path(td, "active_broker_mutations.json").write_text(json.dumps({
        "schema_version": 2, "account_id": ACCT, "handoff_verified": True,
        "producer_pid": os.getpid(), "producer_starttime": proc_starttime(os.getpid()),
        "active": [], "unresolved": [],
    }), encoding="utf-8")
    Path(td, "emergency_stop.json").write_text(
        json.dumps({"is_stopped": False, "in_flight_orders": [], "account_id": ACCT}), encoding="utf-8"
    )
    sm = EmergencyStopManager(f"{td}/emergency_stop.json")
    client = DhanClient(cfg, stop_manager=sm, reconciliation_gate=gate)
    client._get_active_read_governor = lambda: None
    return client, cfg, sm


class TestC63PydanticV2Compatibility(unittest.TestCase):
    """C63: src/models.py uses no unsupported Pydantic v2 legacy `field` validator argument."""

    def test_models_source_has_no_legacy_field_validator_signature_and_imports_cleanly(self):
        src_text = Path("src/models.py").read_text(encoding="utf-8")
        tree = ast.parse(src_text)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_validate_raw_"):
                arg_names = [a.arg for a in node.args.args]
                self.assertNotIn(
                    "field", arg_names,
                    f"Legacy Pydantic v1 `field` parameter breaks Pydantic v2 in {node.name}({arg_names})",
                )
        # Smoke imports of runtime, broker client, and models
        import src.models
        import src.broker.dhan_client
        import src.engine.reconciliation
        import src.engine.protection_watcher
        self.assertTrue(hasattr(src.models, "OrderRequest"))
        self.assertTrue(hasattr(src.models, "ProtectionLevels"))

        # Valid instantiation & malformed rejection
        req = OrderRequest(
            correlation_id="C63-OK", symbol="YESBANK-EQ", security_id="11915",
            exchange_segment="NSE_EQ", transaction_type=TransactionType.BUY,
            product_type=ProductType.CNC, order_type=OrderType.LIMIT, price=20.0, quantity=1,
        )
        self.assertEqual(req.quantity, 1)
        pl = ProtectionLevels(executed_price=20.0, sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=1)
        self.assertEqual(pl.target_price, 20.4)
        with self.assertRaises(ValidationError):
            ProtectionLevels(executed_price=float("nan"), sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=1)
        with self.assertRaises(ValidationError):
            ProtectionLevels(executed_price=20.0, sl_trigger=True, sl_limit=19.7, target_price=20.4, quantity=1)


class TestC64AndC65SellContractAndAllocationLinkage(unittest.TestCase):
    """C64 + C65: Exact SELL contract binding and execution-to-allocation linkage (zero max() substitution)."""

    def _base_db(self, db_path: Path, *, sell_filled_qty: int = 5, open_qty: int = 0):
        db = JournalDB(str(db_path))
        now = time.time()
        with db._get_conn() as con:
            con.execute(
                "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                ("PAR-64", 0, json.dumps({"order_id": "PAR-64", "position_state": "POSITION_CLOSED", "filled_qty": 5}), now),
            )
            con.execute(
                "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("PAR-64", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "POSITION_CLOSED", 5, 5, sell_filled_qty, open_qty, now),
            )
            con.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                ("PAR-64", "ET-BUY-64", "SBIN-EQ", 800.0, 5, now, "BUY", "3045", "NSE_EQ", "CNC", "PAR-64"),
            )
        return db

    def test_c64_wrong_security_segment_or_product_sell_fails_closed(self):
        """Exact ChatGPT C64 reproduction: wrong security/segment/product SELL for expected parent."""
        for mismatch_field, sec, seg, prod in [
            ("security_id", "9999", "NSE_EQ", "CNC"),
            ("exchange_segment", "3045", "BSE_EQ", "CNC"),
            ("product_type", "3045", "NSE_EQ", "INTRADAY"),
            ("all_three", "9999", "BSE_EQ", "INTRADAY"),
        ]:
            with self.subTest(mismatch=mismatch_field), tempfile.TemporaryDirectory() as td:
                p = Path(td) / "c64.db"
                db = self._base_db(p, sell_filled_qty=5, open_qty=0)
                with db._get_conn() as con:
                    con.execute(
                        "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                        ("SELL-BAD", "ET-SELL-BAD", "OTHER", 810.0, 5, time.time(), "SELL", sec, seg, prod, "PAR-64"),
                    )
                rec = {"order_id": "PAR-64", "persistence_uncertain": True}
                ok, reason = verify_operation_readback_and_clear_persistence_risk(
                    p, rec,
                    expected_order_id="PAR-64", expected_position_state="POSITION_CLOSED",
                    expected_filled_qty=5, expected_sell_filled_qty=5, expected_confirmed_open_qty=0,
                    expected_account_id=ACCT, expected_security_id="3045", expected_segment="NSE_EQ", expected_product_type="CNC",
                )
                self.assertFalse(ok, f"Wrong-contract SELL ({mismatch_field}) MUST fail closed")
                self.assertIn("CONTRACT_MISMATCH", reason)
                self.assertTrue(rec["persistence_uncertain"])

    def test_c65_contradictory_direct_vs_allocated_sell_fails_closed_and_external_alloc_works(self):
        """Exact ChatGPT C65 reproduction: direct SELL=2 vs alloc SELL=5 MUST fail closed;
        legitimate 2 direct + 3 external verified SELL allocation MUST pass without double-counting."""
        with tempfile.TemporaryDirectory() as td:
            # 1. Exact ChatGPT reproduction: direct=2, alloc=5 (contradictory/over-allocated)
            p1 = Path(td) / "c65_bad.db"
            db1 = self._base_db(p1, sell_filled_qty=5, open_qty=0)
            now = time.time()
            with db1._get_conn() as con:
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("SELL-1", "ET-S1", "SBIN-EQ", 810.0, 2, now, "SELL", "3045", "NSE_EQ", "CNC", "PAR-64"),
                )
                con.execute(
                    "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
                    ("PAR-64", "SELL-1", "ET-S1", 5, 810.0, now),
                )
            rec1 = {"order_id": "PAR-64", "persistence_uncertain": True}
            ok1, reason1 = verify_operation_readback_and_clear_persistence_risk(
                p1, rec1,
                expected_order_id="PAR-64", expected_position_state="POSITION_CLOSED",
                expected_filled_qty=5, expected_sell_filled_qty=5, expected_confirmed_open_qty=0,
                expected_account_id=ACCT, expected_security_id="3045", expected_segment="NSE_EQ", expected_product_type="CNC",
            )
            self.assertFalse(ok1)
            self.assertIn("EXIT_ALLOCATION_EXCEEDS_FILL_QTY", reason1)
            self.assertTrue(rec1["persistence_uncertain"])

            # 2. Unsupported allocation (no backing verified SELL fill)
            p2 = Path(td) / "c65_unbacked.db"
            db2 = self._base_db(p2, sell_filled_qty=5, open_qty=0)
            with db2._get_conn() as con:
                con.execute(
                    "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
                    ("PAR-64", "SELL-GHOST", "ET-GHOST", 5, 810.0, now),
                )
            rec2 = {"order_id": "PAR-64", "persistence_uncertain": True}
            ok2, reason2 = verify_operation_readback_and_clear_persistence_risk(
                p2, rec2,
                expected_order_id="PAR-64", expected_position_state="POSITION_CLOSED",
                expected_filled_qty=5, expected_sell_filled_qty=5, expected_confirmed_open_qty=0,
                expected_account_id=ACCT, expected_security_id="3045", expected_segment="NSE_EQ", expected_product_type="CNC",
            )
            self.assertFalse(ok2)
            self.assertIn("UNVERIFIED_OR_AMBIGUOUS_EXIT_ALLOCATION_FILL", reason2)
            self.assertTrue(rec2["persistence_uncertain"])

            # 3. Positive control: 2 direct SELL + 3 external SELL allocation (parent_order_id=NULL) = 5 total (no double-counting)
            p3 = Path(td) / "c65_ok.db"
            db3 = self._base_db(p3, sell_filled_qty=5, open_qty=0)
            with db3._get_conn() as con:
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("SELL-DIR", "ET-DIR-2", "SBIN-EQ", 810.0, 2, now, "SELL", "3045", "NSE_EQ", "CNC", "PAR-64"),
                )
                con.execute(
                    "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
                    ("PAR-64", "SELL-DIR", "ET-DIR-2", 2, 810.0, now),
                )
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("SELL-EXT", "ET-EXT-3", "SBIN-EQ", 812.0, 3, now, "SELL", "3045", "NSE_EQ", "CNC", None),
                )
                con.execute(
                    "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
                    ("PAR-64", "SELL-EXT", "ET-EXT-3", 3, 812.0, now),
                )
            rec3 = {"order_id": "PAR-64", "persistence_uncertain": True}
            ok3, reason3 = verify_operation_readback_and_clear_persistence_risk(
                p3, rec3,
                expected_order_id="PAR-64", expected_position_state="POSITION_CLOSED",
                expected_filled_qty=5, expected_sell_filled_qty=5, expected_confirmed_open_qty=0,
                expected_account_id=ACCT, expected_security_id="3045", expected_segment="NSE_EQ", expected_product_type="CNC",
            )
            self.assertTrue(ok3, reason3)
            self.assertNotIn("persistence_uncertain", rec3)


class TestC66SemanticAuthAndWireBinding(unittest.TestCase):
    """C66: Semantic equivalence enforced in AuthorizedBrokerOperation.build() + mandatory LIVE op on POST/PUT/DELETE."""

    def test_canonical_vs_wire_mismatch_rejected_at_build(self):
        """Exact ChatGPT C66 reproduction: canonical quantity=1 vs wire quantity=999 MUST fail."""
        with self.assertRaises(ValueError) as ctx:
            AuthorizedBrokerOperation.build(
                method="POST", url="https://api.dhan.co/v2/orders",
                account_id=ACCT, operation_type="PLACE_ORDER", correlation_id="C66-1",
                canonical_fields={"quantity": 1, "price": 20.0},
                wire_payload={"quantity": 999, "price": 20.0},
            )
        self.assertIn("SEMANTIC_WIRE_MISMATCH", str(ctx.exception))

        # Also catch mapped camelCase mismatch (account_id vs dhanClientId, security_id vs securityId)
        with self.assertRaises(ValueError):
            AuthorizedBrokerOperation.build(
                method="POST", url="https://api.dhan.co/v2/orders",
                account_id=ACCT, operation_type="PLACE_ORDER", correlation_id="C66-2",
                canonical_fields={"account_id": ACCT, "security_id": "11915"},
                wire_payload={"dhanClientId": "9999999999", "securityId": "11915"},
            )

    def test_missing_authorized_op_in_live_mode_blocked_across_post_put_delete(self):
        with tempfile.TemporaryDirectory() as td:
            client, _, _ = _make_live_client(td, gate=_admit_gate())
            http_spy = MagicMock(side_effect=AssertionError("ZERO HTTP calls allowed without AuthorizedBrokerOperation"))
            with patch.object(client.session, "post", http_spy), \
                 patch.object(client.session, "put", http_spy), \
                 patch.object(client.session, "delete", http_spy):
                for meth, url in [
                    ("POST", "https://api.dhan.co/v2/orders"),
                    ("PUT", "https://api.dhan.co/v2/super/orders/ORD-1"),
                    ("DELETE", "https://api.dhan.co/v2/orders/ORD-1"),
                ]:
                    with self.subTest(method=meth, url=url):
                        with self.assertRaises(BrokerMutationNotSubmitted) as ctx:
                            client._broker_mutating_http(meth, url, headers={}, timeout=5)
                        self.assertIn("MISSING_AUTHORIZED_OPERATION", str(ctx.exception))
            http_spy.assert_not_called()

            # Public DELETE methods bind explicit AuthorizedBrokerOperation and succeed even when entries halted
            halted_gate = MagicMock()
            halted_gate.is_accepting_entries = False
            halted_gate.revalidate_for_transport_submission.return_value = (False, "HALTED")
            client.reconciliation_gate = halted_gate
            with patch.object(client.session, "delete", return_value=_fake_resp(202)):
                ok_c, _ = client.cancel_order("ORD-DEL-1")
                self.assertTrue(ok_c)
                ok_f, _ = client.cancel_forever_order("FO-DEL-1")
                self.assertTrue(ok_f)
                ok_s, _ = client.cancel_super_order_leg("SUP-DEL-1", "TARGET_LEG")
                self.assertTrue(ok_s)


class TestC67CompleteSnapshotFreshness(unittest.TestCase):
    """C67: BOTH s1 and s2 must satisfy <=10s freshness, chronology, non-future, identity, and non-reused issue."""

    def _snap(self, sid: str, finish_ns: int, gen_num: int = 1, acct: str = ACCT):
        gen = Generation(account_key(acct), "boot", gen_num, 1, 1, 1, 1)
        return SimpleNamespace(
            snapshot_id=sid, account_id=acct, capture_finished_mono_ns=finish_ns,
            generation=gen, journal_digest="d1", complete=True,
        )

    def test_29s_old_first_snapshot_with_1s_old_second_snapshot_rejected(self):
        """Exact ChatGPT C67 reproduction: 29s-old s1 + 1s-old s2 MUST be rejected."""
        with tempfile.TemporaryDirectory() as td:
            db = JournalDB(f"{td}/trade_journal.db")
            broker = MagicMock()
            broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            pw = ProtectionWatcher(broker, journal_db=db, path=f"{td}/pending.json")
            now_ns = time.monotonic_ns()
            rec = {"order_id": "ORD-67", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC", "cancel_uncertain": True, "durable_issue_id": "ISS-67"}
            key = ("ORD-67", pw._resolution_operation_scope(rec))

            def _seed_and_consume(s1, s2):
                pw._pending_resolution_snapshots_by_order = {
                    key: {
                        **pw._resolution_binding_metadata(rec, expected_account_id=ACCT, operation_scope=key[1]),
                        "snapshot_1": s1, "snapshot_2": s2, "generation": s2.generation,
                    }
                }
                return pw._consume_bound_resolution_snapshots(rec, expected_account_id=ACCT, now_mono_ns=now_ns)

            # 1. 29s-old s1 + 1s-old s2 -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1", now_ns - 29_000_000_000), self._snap("S2", now_ns - 1_000_000_000)))
            # 2. Stale s2 (15s old) -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1", now_ns - 18_000_000_000), self._snap("S2", now_ns - 15_000_000_000)))
            # 3. Future s2 -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1", now_ns - 2_000_000_000), self._snap("S2", now_ns + 5_000_000_000)))
            # 4. Reversed chronology -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1", now_ns - 1_000_000_000), self._snap("S2", now_ns - 3_000_000_000)))
            # 5. Missing capture identity -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("", now_ns - 4_000_000_000), self._snap("S2", now_ns - 1_000_000_000)))
            # 6. Generation rollover -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1", now_ns - 4_000_000_000, gen_num=1), self._snap("S2", now_ns - 1_000_000_000, gen_num=2)))
            # 7. Fresh valid pair (4s old s1 + 1s old s2) -> ACCEPTED once
            s1_ok = self._snap("S1-OK", now_ns - 4_000_000_000)
            s2_ok = self._snap("S2-OK", now_ns - 1_000_000_000)
            self.assertEqual(_seed_and_consume(s1_ok, s2_ok), (s1_ok, s2_ok))
            # 8. Reused durable issue occurrence -> REJECTED
            self.assertIsNone(_seed_and_consume(self._snap("S1-R", now_ns - 4_000_000_000), self._snap("S2-R", now_ns - 1_000_000_000)))


class TestC68NumericRangeAndOverflow(unittest.TestCase):
    """C68: Controlled rejection of 10**30 quantity and 10**400 price with zero unhandled OverflowError."""

    def test_overflow_and_huge_int_rejected_in_helpers_models_and_dhan_client(self):
        ok_q, v_q, msg_q = _validate_raw_strict_int(10**30, "quantity")
        self.assertFalse(ok_q)
        self.assertIsNone(v_q)
        self.assertIn("INVALID_NUMERIC_FIELD", msg_q)

        # Must NOT raise unhandled OverflowError!
        ok_p, v_p, msg_p = _validate_raw_strict_number(10**400, "price")
        self.assertFalse(ok_p)
        self.assertIsNone(v_p)
        self.assertIn("overflow", msg_p.lower())

        # Model construction rejects 10**30 qty and 10**400 price via ValidationError (not OverflowError)
        with self.assertRaises(ValidationError):
            OrderRequest(
                correlation_id="C68-M1", symbol="YESBANK-EQ", security_id="11915",
                exchange_segment="NSE_EQ", product_type=ProductType.CNC, price=20.0, quantity=10**30,
            )
        with self.assertRaises(ValidationError):
            OrderRequest(
                correlation_id="C68-M2", symbol="YESBANK-EQ", security_id="11915",
                exchange_segment="NSE_EQ", product_type=ProductType.CNC, price=10**400, quantity=1,
            )

        # Public DhanClient.place_order rejects raw 10**30 and 10**400 with zero HTTP calls
        with tempfile.TemporaryDirectory() as td:
            client, _, _ = _make_live_client(td, gate=_admit_gate())
            post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP calls allowed"))
            for bad_q, bad_p in [(10**30, 20.0), (1, 10**400)]:
                raw_req = SimpleNamespace(
                    correlation_id="C68-RAW", symbol="YESBANK-EQ", security_id="11915",
                    exchange_segment="NSE_EQ", transaction_type=TransactionType.BUY,
                    product_type=ProductType.CNC, order_type=OrderType.LIMIT,
                    price=bad_p, quantity=bad_q, trigger_price=0.0, disclosed_quantity=0,
                    after_market_order=False, amo_time="OPEN",
                )
                with patch.object(client.session, "post", post_spy):
                    ok, oid, msg = client.place_order(raw_req)
                    self.assertFalse(ok)
                    self.assertIsNone(oid)
                    self.assertIn("INVALID_NUMERIC_FIELD", msg)
            post_spy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
