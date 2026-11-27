"""Phase 2D Final Corrective Closure Certification Tests (C58-C62 + C54 Re-Verification).

Covers:
- C58: Raw numeric validation before conversion (OrderRequest, ProtectionLevels, DhanClient public methods)
- C59: Mandatory persistence evidence readback (zero table-absence bypass)
- C60: Single explicit LIVE authorization interface (zero reduced-argument TypeError fallback)
- C61: Complete per-order/per-operation recovery snapshot isolation (zero global fallback)
- C62: Immutable AuthorizedBrokerOperation binding authorized & wire payloads + method + endpoint
- C54: Full Forever Order and Super Order sibling-leg residual risk re-verification
"""
import datetime
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from src.broker.broker_snapshot import (
    BookEvidence,
    BookStatus,
    BrokerSnapshot,
    Generation,
    Purpose,
    SessionContext,
    SessionPhase,
    account_key,
    payload_digest,
)
from src.broker.dhan_client import AuthorizedBrokerOperation, DhanClient
from src.config import AppConfig, TradingMode
from src.engine.broker_handoff import BrokerMutationNotSubmitted
from src.engine.durable_issue_writer import verify_operation_readback_and_clear_persistence_risk
from src.engine.durable_risk_resolution import resolve_durable_orphan_legs
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
    body = payload if payload is not None else {"orderId": "99000111", "orderStatus": "PENDING"}
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
    import os as _os
    from src.engine.broker_handoff import proc_starttime as _ps
    Path(td, "active_broker_mutations.json").write_text(json.dumps({
        "schema_version": 2, "account_id": ACCT, "handoff_verified": True,
        "producer_pid": _os.getpid(), "producer_starttime": _ps(_os.getpid()),
        "active": [], "unresolved": [],
    }), encoding="utf-8")
    Path(td, "emergency_stop.json").write_text(json.dumps({"is_stopped": False, "in_flight_orders": [], "account_id": ACCT}), encoding="utf-8")
    sm = EmergencyStopManager(f"{td}/emergency_stop.json")
    client = DhanClient(cfg, stop_manager=sm, reconciliation_gate=gate)
    client._get_active_read_governor = lambda: None
    return client, cfg, sm


def _valid_order_req(cid="C58-VALID-1", qty=2, price=100.0, order_type=OrderType.LIMIT):
    return OrderRequest(
        correlation_id=cid,
        symbol="YESBANK-EQ",
        security_id="11915",
        exchange_segment="NSE_EQ",
        transaction_type=TransactionType.BUY,
        product_type=ProductType.CNC,
        order_type=order_type,
        price=price,
        quantity=qty,
        trading_mode=TradingMode.LIVE,
    )


class TestC58RawNumericValidation(unittest.TestCase):
    """C58: Raw numeric validation before Pydantic/int()/float() conversion."""

    def test_order_request_rejects_lossy_and_invalid_raw_inputs(self):
        base = dict(
            correlation_id="C58-M1",
            symbol="YESBANK-EQ",
            security_id="11915",
            exchange_segment="NSE_EQ",
            transaction_type=TransactionType.BUY,
            product_type=ProductType.CNC,
            order_type=OrderType.LIMIT,
            price=20.0,
            quantity=1,
            trading_mode=TradingMode.LIVE,
        )
        for field, bad_val in [
            ("quantity", 1.9),
            ("quantity", 1.0),
            ("quantity", True),
            ("quantity", False),
            ("quantity", 0),
            ("quantity", -3),
            ("quantity", "1"),
            ("disclosed_quantity", 1.5),
            ("disclosed_quantity", True),
            ("disclosed_quantity", -1),
            ("price", True),
            ("price", float("nan")),
            ("price", float("inf")),
            ("price", float("-inf")),
            ("price", 0.0),
            ("price", -10.0),
            ("price", "20.0"),
            ("trigger_price", True),
            ("trigger_price", float("nan")),
            ("trigger_price", float("inf")),
            ("trigger_price", -0.5),
        ]:
            kw = dict(base)
            kw[field] = bad_val
            with self.subTest(field=field, bad_val=bad_val):
                with self.assertRaises(ValidationError):
                    OrderRequest(**kw)

        # Positive controls: valid LIMIT, valid MARKET with price=0.0, valid trigger_price=0.0
        lim = OrderRequest(**base)
        self.assertEqual(lim.quantity, 1)
        mkt = OrderRequest(**{**base, "order_type": OrderType.MARKET, "price": 0.0})
        self.assertEqual(mkt.price, 0.0)

    def test_dhan_client_place_order_rejects_raw_invalid_values_zero_http(self):
        with tempfile.TemporaryDirectory() as td:
            client, _, _ = _make_live_client(td, gate=_admit_gate())
            post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed for invalid numeric inputs"))
            for bad_kw in [
                {"quantity": 1.9},
                {"quantity": True},
                {"quantity": False},
                {"quantity": 0},
                {"price": float("nan")},
                {"price": float("inf")},
                {"price": float("-inf")},
                {"price": True},
                {"price": 0.0},
                {"trigger_price": float("nan")},
                {"trigger_price": True},
                {"disclosed_quantity": 1.5},
                {"disclosed_quantity": True},
                {"disclosed_quantity": 5},  # > quantity=1
            ]:
                raw_req = SimpleNamespace(
                    correlation_id="C58-RAW-1",
                    symbol="YESBANK-EQ",
                    security_id="11915",
                    exchange_segment="NSE_EQ",
                    transaction_type=TransactionType.BUY,
                    product_type=ProductType.CNC,
                    order_type=OrderType.LIMIT,
                    price=20.0,
                    quantity=1,
                    trigger_price=0.0,
                    disclosed_quantity=0,
                    after_market_order=False,
                    amo_time="OPEN",
                )
                for k, v in bad_kw.items():
                    setattr(raw_req, k, v)
                with self.subTest(bad_kw=bad_kw), patch.object(client.session, "post", post_spy):
                    ok, oid, msg = client.place_order(raw_req)
                    self.assertFalse(ok)
                    self.assertIsNone(oid)
                    self.assertIn("INVALID_NUMERIC_FIELD", msg)
            post_spy.assert_not_called()

    def test_dhan_client_super_order_modify_and_forever_oco_raw_validation(self):
        with tempfile.TemporaryDirectory() as td:
            client, _, _ = _make_live_client(td, gate=_admit_gate())
            http_spy = MagicMock(side_effect=AssertionError("ZERO HTTP mutations allowed"))
            with patch.object(client.session, "post", http_spy), patch.object(client.session, "put", http_spy):
                # Super Order bad raw values
                bad_req = SimpleNamespace(
                    correlation_id="C58-SUP-1", symbol="YESBANK-EQ", security_id="11915",
                    exchange_segment="NSE_EQ", transaction_type=TransactionType.BUY,
                    product_type=ProductType.CNC, order_type=OrderType.LIMIT,
                    price=20.0, quantity=1.9,
                )
                ok, oid, msg = client.place_super_order(bad_req, stop_loss_price=19.8, target_price=20.4)
                self.assertFalse(ok)
                self.assertIsNone(oid)
                self.assertIn("INVALID_NUMERIC_FIELD", msg)

                ok, oid, msg = client.place_super_order(_valid_order_req("C58-SUP-2", qty=1, price=20.0), stop_loss_price=True, target_price=20.4)
                self.assertFalse(ok)
                self.assertIn("INVALID_NUMERIC_FIELD", msg)

                ok, oid, msg = client.place_super_order(_valid_order_req("C58-SUP-3", qty=1, price=20.0), stop_loss_price=19.8, target_price=float("nan"))
                self.assertFalse(ok)
                self.assertIn("INVALID_NUMERIC_FIELD", msg)

                ok, oid, msg = client.place_super_order(_valid_order_req("C58-SUP-4", qty=1, price=20.0), stop_loss_price=19.8, target_price=20.4, trailing_jump=float("inf"))
                self.assertFalse(ok)
                self.assertIn("INVALID_NUMERIC_FIELD", msg)

                # Modify Super Order bad raw values
                ok_m, msg_m, _ = client.modify_super_order_leg("ORD-1", "TARGET_LEG", price=True)
                self.assertFalse(ok_m)
                self.assertIn("INVALID_NUMERIC_FIELD", msg_m)

                ok_m2, msg_m2 = client.modify_super_order_protection("ORD-1", stop_loss_price=float("nan"), target_price=21.0)
                self.assertFalse(ok_m2)
                self.assertIn("INVALID_NUMERIC_FIELD", msg_m2)

                # Forever OCO bad raw values
                bad_levels = SimpleNamespace(executed_price=20.0, sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=1.9, target_trigger=20.4)
                ok_f, fid, msg_f = client.place_forever_oco("OCO-1", "11915", "NSE_EQ", "CNC", bad_levels)
                self.assertFalse(ok_f)
                self.assertIsNone(fid)
                self.assertIn("INVALID_NUMERIC_FIELD", msg_f)

                bad_levels_bool = SimpleNamespace(executed_price=20.0, sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=True, target_trigger=20.4)
                ok_fb, fidb, msg_fb = client.place_forever_oco("OCO-2", "11915", "NSE_EQ", "CNC", bad_levels_bool)
                self.assertFalse(ok_fb)
                self.assertIn("INVALID_NUMERIC_FIELD", msg_fb)
            http_spy.assert_not_called()

            # Positive controls
            with patch.object(client.session, "post", return_value=_fake_resp(200, {"orderId": "777001", "orderStatus": "PENDING"})), \
                 patch.object(client.session, "put", return_value=_fake_resp(200, {"orderId": "777001", "orderStatus": "PENDING"})):
                ok1, oid1, _ = client.place_order(_valid_order_req("C58-POS-1", qty=2, price=20.0))
                self.assertTrue(ok1)
                self.assertEqual(oid1, "777001")
                ok2, oid2, _ = client.place_super_order(_valid_order_req("C58-POS-2", qty=2, price=20.0), stop_loss_price=19.8, target_price=20.4, trailing_jump=0.0)
                self.assertTrue(ok2)
                self.assertEqual(oid2, "777001")
                ok3, _, _ = client.modify_super_order_leg("777001", "TARGET_LEG", price=20.5)
                self.assertTrue(ok3)
                good_levels = ProtectionLevels(executed_price=20.0, sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=2, target_trigger=20.4)
                ok4, fid4, _ = client.place_forever_oco("OCO-POS", "11915", "NSE_EQ", "CNC", good_levels)
                self.assertTrue(ok4)
                self.assertEqual(fid4, "777001")


class TestC59MandatoryPersistenceReadback(unittest.TestCase):
    """C59: Missing tables/columns/rows MUST fail closed — never clear persistence_uncertain."""

    def _seed_complete_db(self, db_path: Path):
        db = JournalDB(str(db_path))
        now = time.time()
        with db._get_conn() as con:
            con.execute(
                "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                ("ORD-59", 0, json.dumps({"order_id": "ORD-59", "position_state": "PROTECTED_OPEN", "filled_qty": 5}), now),
            )
            con.execute(
                "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("ORD-59", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 5, 5, 2, 3, now),
            )
            con.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                ("ORD-59", "ET-59-BUY", "SBIN-EQ", 800.0, 5, now, "BUY", "3045", "NSE_EQ", "CNC", "ORD-59"),
            )
            con.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                ("SELL-59", "ET-59-SELL", "SBIN-EQ", 810.0, 2, now, "SELL", "3045", "NSE_EQ", "CNC", "ORD-59"),
            )
            con.execute(
                "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
                ("ORD-59", "SELL-59", "ET-59-SELL", 2, 810.0, now),
            )
            con.execute(
                "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                ("ORD-59", "FO-59", "SBIN-EQ", 790.0, 789.0, 820.0, 3, "ACTIVE", now),
            )
            con.execute(
                "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?,?,?,?)",
                ("PROTECTION_RISK", "ISS-59", json.dumps({"issue_id": "ISS-59", "order_id": "ORD-59"}), now),
            )
        return db

    def test_watcher_orders_only_db_must_fail_closed(self):
        """Exact ChatGPT C59 adversarial reproduction: DB with ONLY watcher_orders."""
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "minimal.db"
            with closing(sqlite3.connect(str(p))) as con:
                con.execute("CREATE TABLE watcher_orders (order_id TEXT PRIMARY KEY, record_json TEXT)")
                con.execute(
                    "INSERT INTO watcher_orders VALUES (?, ?)",
                    ("ORD-ONLY", json.dumps({"order_id": "ORD-ONLY", "position_state": "PROTECTED_OPEN", "filled_qty": 5})),
                )
                con.commit()
            rec = {"order_id": "ORD-ONLY", "persistence_uncertain": True}
            ok, reason = verify_operation_readback_and_clear_persistence_risk(
                p, rec,
                expected_order_id="ORD-ONLY",
                expected_position_state="PROTECTED_OPEN",
                expected_filled_qty=5,
            )
            self.assertFalse(ok)
            self.assertEqual(reason, "MANDATORY_TABLE_MISSING:positions_lifecycle")
            self.assertTrue(rec["persistence_uncertain"])

    def test_every_mandatory_table_independently_missing_fails_closed(self):
        for drop_tbl in ("watcher_orders", "positions_lifecycle", "fills", "exit_allocations", "protection_orders", "system_events"):
            with self.subTest(dropped_table=drop_tbl), tempfile.TemporaryDirectory() as td:
                p = Path(td) / "t.db"
                self._seed_complete_db(p)
                with closing(sqlite3.connect(str(p))) as con:
                    con.execute(f"DROP TABLE {drop_tbl}")
                    con.commit()
                rec = {"order_id": "ORD-59", "persistence_uncertain": True}
                ok, reason = verify_operation_readback_and_clear_persistence_risk(
                    p, rec,
                    expected_order_id="ORD-59",
                    expected_position_state="PROTECTED_OPEN",
                    expected_filled_qty=5,
                    expected_account_id=ACCT,
                    expected_security_id="3045",
                    expected_segment="NSE_EQ",
                    expected_product_type="CNC",
                    expected_fill_ids=["ET-59-BUY"],
                    expected_sell_filled_qty=2,
                    expected_confirmed_open_qty=3,
                    expected_protection_status="ACTIVE",
                    expected_protection_order_id="FO-59",
                    expected_event_type="PROTECTION_RISK",
                    expected_issue_id="ISS-59",
                )
                self.assertFalse(ok)
                self.assertEqual(reason, f"MANDATORY_TABLE_MISSING:{drop_tbl}")
                self.assertTrue(rec["persistence_uncertain"])

    def test_missing_column_wrong_contract_malformed_json_and_positive_control(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "full.db"
            self._seed_complete_db(p)

            # Wrong contract security_id fails closed
            rec_bad = {"order_id": "ORD-59", "persistence_uncertain": True}
            ok, reason = verify_operation_readback_and_clear_persistence_risk(
                p, rec_bad,
                expected_order_id="ORD-59", expected_position_state="PROTECTED_OPEN", expected_filled_qty=5,
                expected_account_id=ACCT, expected_security_id="9999", expected_segment="NSE_EQ", expected_product_type="CNC",
            )
            self.assertFalse(ok)
            self.assertIn("TARGET_LIFECYCLE_CONTRACT_MISMATCH", reason)
            self.assertTrue(rec_bad["persistence_uncertain"])

            # Issue ID substring collision fails closed
            rec_sub = {"order_id": "ORD-59", "persistence_uncertain": True}
            ok_sub, reason_sub = verify_operation_readback_and_clear_persistence_risk(
                p, rec_sub,
                expected_order_id="ORD-59", expected_position_state="PROTECTED_OPEN", expected_filled_qty=5,
                expected_event_type="PROTECTION_RISK", expected_issue_id="ISS-5",
            )
            self.assertFalse(ok_sub)
            self.assertIn("TARGET_ISSUE_LINKAGE_NOT_COMMITTED", reason_sub)

            # Complete evidence positive control passes and clears flag
            rec_ok = {"order_id": "ORD-59", "persistence_uncertain": True}
            ok_pos, reason_pos = verify_operation_readback_and_clear_persistence_risk(
                p, rec_ok,
                expected_order_id="ORD-59", expected_position_state="PROTECTED_OPEN", expected_filled_qty=5,
                expected_account_id=ACCT, expected_security_id="3045", expected_segment="NSE_EQ", expected_product_type="CNC",
                expected_fill_ids=["ET-59-BUY"], expected_sell_filled_qty=2, expected_confirmed_open_qty=3,
                expected_protection_status="ACTIVE", expected_protection_order_id="FO-59",
                expected_event_type="PROTECTION_RISK", expected_issue_id="ISS-59",
            )
            self.assertTrue(ok_pos, reason_pos)
            self.assertEqual(reason_pos, "EXACT_READBACK_VERIFIED")
            self.assertNotIn("persistence_uncertain", rec_ok)


class TestC60NoWeakerAuthorizationFallbacks(unittest.TestCase):
    """C60: Single explicit LIVE authorization contract with zero reduced-argument TypeError fallback."""

    def test_internal_type_error_not_retried_and_blocks_with_zero_http(self):
        with tempfile.TemporaryDirectory() as td:
            calls = []
            class _TypeErrGate:
                is_accepting_entries = True
                def revalidate_for_transport_submission(self, *, pending_correlation_id=None, post_registration=False, expected_order_payload=None):
                    calls.append((pending_correlation_id, post_registration))
                    raise TypeError("internal authorization type bug")

            client, _, _ = _make_live_client(td, gate=_TypeErrGate())
            post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed"))
            with patch.object(client.session, "post", post_spy):
                ok, oid, msg = client.place_order(_valid_order_req("C60-TE-1"))
            self.assertFalse(ok)
            self.assertIsNone(oid)
            self.assertIn("AUTHORIZATION_CALLBACK_ERROR:TypeError", msg)
            self.assertEqual(len(calls), 1, "MUST invoke callback exactly once with ZERO reduced-arg retries")
            post_spy.assert_not_called()

    def test_legacy_signature_value_error_missing_gate_invalid_return_all_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed"))

            # 1. Legacy 0-arg callback
            legacy_calls = []
            class _LegacyGate:
                is_accepting_entries = True
                def revalidate_for_transport_submission(self):
                    legacy_calls.append(1)
                    return True, "LEGACY_OK"
            c1, _, _ = _make_live_client(td, gate=_LegacyGate())
            with patch.object(c1.session, "post", post_spy):
                ok, _, msg = c1.place_order(_valid_order_req("C60-LEG-1"))
            self.assertFalse(ok)
            self.assertIn("AUTHORIZATION_CALLBACK_ERROR:TypeError", msg)
            self.assertEqual(len(legacy_calls), 0)

            # 2. ValueError in callback
            class _ValErrGate:
                is_accepting_entries = True
                def revalidate_for_transport_submission(self, **kw):
                    raise ValueError("corrupt state")
            c2, _, _ = _make_live_client(td, gate=_ValErrGate())
            with patch.object(c2.session, "post", post_spy):
                ok, _, msg = c2.place_order(_valid_order_req("C60-VE-1"))
            self.assertFalse(ok)
            self.assertIn("AUTHORIZATION_CALLBACK_ERROR:ValueError", msg)

            # 3. Missing reconciliation_gate in LIVE mode
            c3, _, _ = _make_live_client(td, gate=None)
            with patch.object(c3.session, "post", post_spy):
                ok, _, msg = c3.place_order(_valid_order_req("C60-NONE-1"))
            self.assertFalse(ok)
            self.assertIn("RECONCILIATION_GATE_MISSING", msg)

            # 4. Non-callable callback
            c4, _, _ = _make_live_client(td, gate=SimpleNamespace(is_accepting_entries=True, revalidate_for_transport_submission="not-callable"))
            with patch.object(c4.session, "post", post_spy):
                ok, _, msg = c4.place_order(_valid_order_req("C60-NC-1"))
            self.assertFalse(ok)
            self.assertIn("AUTHORIZATION_CALLBACK_MISSING", msg)

            # 5. Invalid return type
            c5, _, _ = _make_live_client(td, gate=SimpleNamespace(is_accepting_entries=True, revalidate_for_transport_submission=lambda **kw: ("not_bool", "OK")))
            with patch.object(c5.session, "post", post_spy):
                ok, _, msg = c5.place_order(_valid_order_req("C60-INV-1"))
            self.assertFalse(ok)
            self.assertIn("AUTHORIZATION_INVALID_RESULT", msg)

            post_spy.assert_not_called()

    def test_protective_operations_permitted_while_entries_halted(self):
        with tempfile.TemporaryDirectory() as td:
            halted_gate = MagicMock()
            halted_gate.is_accepting_entries = False
            halted_gate.revalidate_for_transport_submission.return_value = (False, "ENTRIES_HALTED")
            client, _, _ = _make_live_client(td, gate=halted_gate)
            with patch.object(client.session, "post", return_value=_fake_resp(200, {"orderId": "FO-999", "orderStatus": "PENDING"})), \
                 patch.object(client.session, "delete", return_value=_fake_resp(202, {"orderId": "ORD-999"})):
                levels = ProtectionLevels(executed_price=20.0, sl_trigger=19.8, sl_limit=19.7, target_price=20.4, quantity=1, target_trigger=20.4)
                ok_f, fid, _ = client.place_forever_oco("OCO-HALT", "11915", "NSE_EQ", "CNC", levels)
                self.assertTrue(ok_f)
                self.assertEqual(fid, "FO-999")
                ok_c, _ = client.cancel_order("ORD-999")
                self.assertTrue(ok_c)


class TestC61PerOrderRecoverySnapshotIsolation(unittest.TestCase):
    """C61: Strict per-order, per-operation recovery snapshot isolation with zero global fallback."""

    def _snap(self, sid: str, finish_ns: int, gen_num: int = 1):
        gen = Generation(account_key(ACCT), "boot", gen_num, 1, 1, 1, 1)
        return SimpleNamespace(
            snapshot_id=sid,
            capture_finished_mono_ns=finish_ns,
            generation=gen,
            journal_digest="dig-1",
            complete=True,
        )

    def test_global_fallback_ignored_and_cross_order_isolation_enforced(self):
        with tempfile.TemporaryDirectory() as td:
            db = JournalDB(f"{td}/trade_journal.db")
            broker = MagicMock()
            broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id=ACCT, data_dir=td)
            pw = ProtectionWatcher(broker, journal_db=db, path=f"{td}/pending.json")
            now_ns = time.monotonic_ns()
            s1 = self._snap("S1", now_ns - 3_000_000_000)
            s2 = self._snap("S2", now_ns - 500_000_000)

            # Even if legacy global attribute is injected, C61 MUST ignore it completely
            pw._pending_resolution_snapshots = (s1, s2)
            rec_a = {"order_id": "ORD-A", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC", "cancel_uncertain": True}
            rec_b = {"order_id": "ORD-B", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC", "cancel_uncertain": True}
            self.assertIsNone(pw._consume_bound_resolution_snapshots(rec_a, expected_account_id=ACCT, now_mono_ns=now_ns))

            # Store bound snapshot for ORD-A only -> ORD-B MUST NOT consume it
            key_a = ("ORD-A", pw._resolution_operation_scope(rec_a))
            pw._pending_resolution_snapshots_by_order = {
                key_a: {
                    **pw._resolution_binding_metadata(rec_a, expected_account_id=ACCT, operation_scope=key_a[1]),
                    "snapshot_1": s1, "snapshot_2": s2, "generation": s2.generation,
                }
            }
            self.assertIsNone(pw._consume_bound_resolution_snapshots(rec_b, expected_account_id=ACCT, now_mono_ns=now_ns))
            # Different operation scope on same parent ORD-A MUST NOT consume CANCEL scope snapshot
            rec_a_orphan = {**rec_a, "cancel_uncertain": False, "orphaned_legs_detected": True}
            self.assertIsNone(pw._consume_bound_resolution_snapshots(rec_a_orphan, expected_account_id=ACCT, now_mono_ns=now_ns))
            # Wrong account MUST NOT consume and cleans up entry
            self.assertIsNone(pw._consume_bound_resolution_snapshots(rec_a, expected_account_id="9999999999", now_mono_ns=now_ns))
            self.assertNotIn(key_a, pw._pending_resolution_snapshots_by_order)

            # Generation rollover between s1 and s2 rejected
            s2_roll = self._snap("S2-ROLL", now_ns - 500_000_000, gen_num=2)
            pw._pending_resolution_snapshots_by_order[key_a] = {
                **pw._resolution_binding_metadata(rec_a, expected_account_id=ACCT, operation_scope=key_a[1]),
                "snapshot_1": s1, "snapshot_2": s2_roll, "generation": s2_roll.generation,
            }
            self.assertIsNone(pw._consume_bound_resolution_snapshots(rec_a, expected_account_id=ACCT, now_mono_ns=now_ns))

            # Matching binding positive control succeeds and cleans up
            pw._pending_resolution_snapshots_by_order[key_a] = {
                **pw._resolution_binding_metadata(rec_a, expected_account_id=ACCT, operation_scope=key_a[1]),
                "snapshot_1": s1, "snapshot_2": s2, "generation": s2.generation,
            }
            got = pw._consume_bound_resolution_snapshots(rec_a, expected_account_id=ACCT, now_mono_ns=now_ns)
            self.assertEqual(got, (s1, s2))
            self.assertNotIn(key_a, pw._pending_resolution_snapshots_by_order)


class TestC62WirePayloadBinding(unittest.TestCase):
    """C62: Any economic or endpoint mutation between authorization and HTTP send yields ZERO HTTP POSTs."""

    def test_tampered_wire_payload_or_method_or_url_blocked_with_zero_http(self):
        with tempfile.TemporaryDirectory() as td:
            client, _, _ = _make_live_client(td, gate=_admit_gate())
            url = "https://api.dhan.co/v2/orders"
            wire = {
                "dhanClientId": ACCT,
                "correlationId": "C62-BIND-1",
                "transactionType": "BUY",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderType": "LIMIT",
                "securityId": "11915",
                "quantity": 2,
                "price": 20.0,
                "triggerPrice": 0.0,
                "nested": {"tag": "orig"},
            }
            op = AuthorizedBrokerOperation.build(
                method="POST", url=url, account_id=ACCT, operation_type="PLACE_ORDER",
                correlation_id="C62-BIND-1", canonical_fields=wire, wire_payload=wire,
            )
            post_spy = MagicMock(side_effect=AssertionError("ZERO HTTP POSTs allowed on tampered wire payload"))

            mutations = [
                ("POST", url, {**wire, "quantity": 200}),
                ("POST", url, {**wire, "price": 25.0}),
                ("POST", url, {**wire, "triggerPrice": 19.5}),
                ("POST", url, {**wire, "transactionType": "SELL"}),
                ("POST", url, {**wire, "securityId": "99999"}),
                ("POST", url, {**wire, "productType": "INTRADAY"}),
                ("POST", url, {**wire, "dhanClientId": "9999999999"}),
                ("POST", url, {**wire, "correlationId": "C62-TAMPER"}),
                ("POST", url, {**wire, "nested": {"tag": "tampered"}}),
                ("PUT", url, dict(wire)),
                ("POST", "https://api.dhan.co/v2/super/orders", dict(wire)),
            ]
            for meth, target_url, mutated_json in mutations:
                with self.subTest(meth=meth, target_url=target_url, mutated_json=mutated_json), \
                     patch.object(client.session, "post", post_spy), \
                     patch.object(client.session, "put", post_spy):
                    with self.assertRaises(BrokerMutationNotSubmitted) as ctx:
                        client._broker_mutating_http(meth, target_url, headers={}, json=mutated_json, timeout=5, authorized_op=op)
                    self.assertIn("WIRE PAYLOAD BINDING", str(ctx.exception))
            post_spy.assert_not_called()

            # Positive control: exact unmodified wire payload succeeds
            with patch.object(client.session, "post", return_value=_fake_resp(200)) as ok_spy:
                resp = client._broker_mutating_http("POST", url, headers={}, json=op.wire_payload_copy(), timeout=5, authorized_op=op)
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(ok_spy.call_count, 1)


class TestC54SuperAndForeverSiblingReverification(unittest.TestCase):
    """C54 Re-Verification: Both Forever Order and Super Order sibling-leg residual checks."""

    def _make_snap(self, sid: str, mono_ns: int, orders, forever_orders=(), super_orders=()):
        from src.broker.broker_snapshot import HistoryCoverage, freeze_json
        gen = Generation(account_key(ACCT), "boot", 1, 1, 1, 1, 1)
        utc_ns = int(time.time() * 1_000_000_000)
        def _bk(k, p):
            fr = freeze_json(p)
            st = BookStatus.OK_RECORDS if p else BookStatus.OK_EMPTY
            return BookEvidence(
                book_key=k, endpoint=f"/v2/{k}", request_params=freeze_json({}),
                status=st, http_status=200, broker_error_code=None,
                retry_after_utc_ns=None, transport_sent=True,
                request_started_utc_ns=utc_ns - 100, received_utc_ns=utc_ns,
                request_started_mono_ns=mono_ns - 10, received_mono_ns=mono_ns,
                account_id=ACCT, token_epoch=1,
                payload=fr, payload_sha256=payload_digest(fr), complete=True,
            )
        return BrokerSnapshot(
            snapshot_id=sid,
            generation=gen,
            account_id=ACCT,
            profile_account_id=ACCT,
            profile_verified=True,
            session=SessionContext(SessionPhase.CURRENT_SIGNED, datetime.date(2026, 11, 27), utc_ns, "a" * 64, True),
            capture_started_utc_ns=utc_ns - 100,
            capture_finished_utc_ns=utc_ns,
            capture_started_mono_ns=mono_ns - 100,
            capture_finished_mono_ns=mono_ns,
            holdings=_bk("holdings", []),
            positions=_bk("positions", []),
            trade_book=_bk("trade_book", []),
            order_book=_bk("order_book", list(orders)),
            forever_orders=_bk("forever_orders", list(forever_orders)),
            super_orders=_bk("super_orders", list(super_orders)),
            historical_trades=(),
            order_status=freeze_json({}),
            trades_for_order=freeze_json({}),
            profile=_bk("profile", {"dhanClientId": ACCT}),
            funds=_bk("funds", {"availabelBalance": 50000.0}),
            edis=_bk("edis", {"status": "OK"}),
            history_coverage=(),
            complete=True,
            invalid_reasons=(),
            producer_pid=123,
            producer_starttime=1,
            journal_digest="b" * 64,
            profile_valid_until_mono_ns=mono_ns + 60_000_000_000,
        )

    def test_super_order_live_sibling_fails_closed_even_with_explicit_order_linkage(self):
        now_ns = time.monotonic_ns()
        dead_leg = {
            "dhanClientId": ACCT, "orderId": "LEG-1", "parentOrderId": "PAR-1", "algoId": "SUP-1",
            "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "orderStatus": "CANCELLED", "filledQty": 0, "remainingQuantity": 0,
        }
        live_super = {
            "dhanClientId": ACCT, "orderId": "PAR-1", "securityId": "11915",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "orderStatus": "PENDING",
            "legDetails": [
                {"orderId": "LEG-1", "legName": "TARGET_LEG", "orderStatus": "CANCELLED", "remainingQuantity": 0},
                {"orderId": "LEG-2", "legName": "STOP_LOSS_LEG", "orderStatus": "PENDING", "remainingQuantity": 5},
            ],
        }
        s1 = self._make_snap("S1", now_ns - 3_000_000_000, [dead_leg], super_orders=[live_super])
        s2 = self._make_snap("S2", now_ns - 100_000_000, [dead_leg], super_orders=[live_super])
        rec1 = {
            "order_id": "PAR-1", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC",
            "orphaned_leg_cancel_details": {"LEG-1": {"order_id": "LEG-1", "status": "UNCERTAIN"}},
        }
        res = resolve_durable_orphan_legs(rec1, s1, s2, expected_account_id=ACCT, now_mono_ns=now_ns)
        self.assertFalse(res.resolved, f"Live Super Order sibling leg MUST fail closed (got {res})")

    def test_forever_order_unverified_triggered_fails_closed(self):
        now_ns = time.monotonic_ns()
        dead_leg = {
            "dhanClientId": ACCT, "orderId": "FO-LEG-1", "parentOrderId": "PAR-2", "algoId": "FO-GRP-2",
            "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "orderStatus": "CANCELLED", "filledQty": 0, "remainingQuantity": 0,
        }
        triggered_fo = {
            "dhanClientId": ACCT, "orderId": "FO-GRP-2", "securityId": "11915",
            "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "orderStatus": "TRIGGERED", "remainingQuantity": 3, "triggeredOrderId": "OTHER-ORD",
        }
        s1 = self._make_snap("S1", now_ns - 3_000_000_000, [dead_leg], forever_orders=[triggered_fo])
        s2 = self._make_snap("S2", now_ns - 100_000_000, [dead_leg], forever_orders=[triggered_fo])
        rec2 = {
            "order_id": "PAR-2", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC",
            "orphaned_leg_cancel_details": {"FO-LEG-1": {"order_id": "FO-LEG-1", "status": "UNCERTAIN"}},
        }
        res = resolve_durable_orphan_legs(rec2, s1, s2, expected_account_id=ACCT, now_mono_ns=now_ns)
        self.assertFalse(res.resolved, f"Unverified TRIGGERED Forever Order MUST fail closed (got {res})")


if __name__ == "__main__":
    unittest.main()
