"""Phase 2D Adversarial Tests: C01-C05 Defect Prevention & Generalized 6-Point Settled-Session Acquisition."""
from __future__ import annotations

import datetime
import json
from contextlib import closing
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from src.broker.broker_snapshot import (
    BookEvidence,
    BookStatus,
    account_key,
    payload_digest,
    BrokerSnapshot,
    Generation,
    HistoryCoverage,
    SessionContext,
    SessionPhase,
)
from src.engine.broker_session import VerifiedCalendar
from src.engine.durable_issue_writer import (
    commit_durable_issue_open,
    commit_durable_issue_resolution,
    verify_operation_readback_and_clear_persistence_risk,
)
from src.engine.durable_risk_resolution import (
    resolve_durable_orphan_legs,
    resolve_ordinary_zero_fill_cancellation,
)
from src.engine.journal_view import read_journal_view
from src.engine.settled_session_acquisition import verify_settled_session_evidence
from src.journal.db import JournalDB

IST = ZoneInfo("Asia/Kolkata")
ACCT = "1105678901"


def _book(name: str, rows: Any, *, ok: bool = True, error: Optional[str] = None, account_id: str = ACCT, mono_ns: Optional[int] = None, utc_ns: Optional[int] = None, request_params: Optional[Dict[str, Any]] = None, endpoint: Optional[str] = None) -> BookEvidence:
    now_u = int(utc_ns if utc_ns is not None else time.time_ns())
    now_m = int(mono_ns if mono_ns is not None else time.monotonic_ns())
    digest = payload_digest(rows)
    return BookEvidence(
        book_key=name,
        endpoint=(endpoint or f"/v2/{name}"),
        request_params=(request_params or {}),
        status=(BookStatus.OK_RECORDS if (ok and rows) else (BookStatus.OK_EMPTY if ok else BookStatus.UNAVAILABLE)),
        http_status=200 if ok else 500,
        broker_error_code=error,
        retry_after_utc_ns=None,
        transport_sent=True,
        request_started_utc_ns=now_u,
        received_utc_ns=now_u,
        request_started_mono_ns=now_m,
        received_mono_ns=now_m,
        account_id=account_id,
        token_epoch=1,
        payload=rows,
        payload_sha256=digest,
        complete=ok,
    )


def _make_snapshot(
    *,
    snapshot_id: str,
    account_id: str = ACCT,
    session_date: str = "2026-11-26",
    session_phase: SessionPhase = SessionPhase.CURRENT_SIGNED,
    holdings: Optional[List[Dict[str, Any]]] = None,
    positions: Optional[List[Dict[str, Any]]] = None,
    order_book: Optional[List[Dict[str, Any]]] = None,
    trade_book: Optional[List[Dict[str, Any]]] = None,
    forever_orders: Optional[List[Dict[str, Any]]] = None,
    super_orders: Optional[List[Dict[str, Any]]] = None,
    historical_trades: Optional[List[BookEvidence]] = None,
    order_status: Optional[Dict[str, BookEvidence]] = None,
    history_coverage: Optional[Tuple[HistoryCoverage, ...]] = None,
    mono_ns: Optional[int] = None,
    utc_ns: Optional[int] = None,
    gen_num: int = 1,
    journal_digest: Optional[str] = None,
) -> BrokerSnapshot:
    now_m = int(mono_ns if mono_ns is not None else time.monotonic_ns())
    now_u = int(utc_ns if utc_ns is not None else time.time_ns())
    gen = Generation(
        account_key=account_key(account_id),
        boot_id="test-boot",
        token_epoch=1,
        mutation_epoch=gen_num,
        owner_epoch=1,
        journal_epoch=gen_num,
        session_epoch=1,
    )
    sess = SessionContext(
        phase=session_phase,
        trading_session_date=datetime.date.fromisoformat(session_date),
        observed_utc_ns=now_u,
        calendar_sha256="0" * 64,
        calendar_verified=True,
    )
    return BrokerSnapshot(
        snapshot_id=snapshot_id,
        generation=gen,
        account_id=account_id,
        profile_account_id=account_id,
        profile_verified=True,
        session=sess,
        capture_started_utc_ns=now_u,
        capture_finished_utc_ns=now_u,
        capture_started_mono_ns=now_m,
        capture_finished_mono_ns=now_m,
        holdings=_book("holdings", holdings or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        positions=_book("positions", positions or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        trade_book=_book("trade_book", trade_book or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        order_book=_book("order_book", order_book or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        forever_orders=_book("forever_orders", forever_orders or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        super_orders=_book("super_orders", super_orders or [], mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        historical_trades=tuple(historical_trades or ()),
        order_status=dict(order_status or {}),
        trades_for_order={},
        profile=_book("profile", {"dhanClientId": account_id}, mono_ns=now_m, utc_ns=now_u, account_id=account_id),
        funds=None,
        edis=None,
        history_coverage=tuple(history_coverage or ()),
        complete=True,
        invalid_reasons=(),
        producer_pid=100,
        producer_starttime=1,
        journal_digest=journal_digest,
        profile_valid_until_mono_ns=now_m + 60_000_000_000,
    )


def _seed_yesbank_db(db_path: str) -> None:
    """Seed synthetic YESBANK 28-share closed sibling + 2 retained 1-share acknowledged lots."""
    db = JournalDB(db_path)
    now = time.time()
    buy_ts = datetime.datetime(2026, 11, 24, 10, 0, tzinfo=IST).timestamp()
    sell_ts = datetime.datetime(2026, 11, 25, 11, 15, tzinfo=IST).timestamp()
    with db._get_conn() as con:
        # Retained Lot 1 & Lot 2 (UNPROTECTED_MANUAL_RISK, owner-acknowledged, NEVER touched)
        for oid, price in (("221261102657708", 15.52), ("23126110439808", 15.51)):
            con.execute(
                "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (oid, oid, "LIVE", "CNC", "NSE_EQ", "YESBANK-EQ", "11915", "LIMIT", price, 1, 1, "TRADED", buy_ts, now, "MANUAL"),
            )
            con.execute(
                "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                (oid, 1, json.dumps({"order_id": oid, "symbol": "YESBANK-EQ", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC", "filled_qty": 1, "position_state": "UNPROTECTED_MANUAL_RISK", "manual_risk_acknowledged": True}), now),
            )
            con.execute(
                "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (oid, ACCT, "YESBANK-EQ", "11915", "NSE_EQ", "CNC", "MANUAL", "UNPROTECTED_MANUAL_RISK", 1, 1, 0, 1, now),
            )
            con.execute(
                "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (oid, f"EID-{oid}", "YESBANK-EQ", price, 1, buy_ts, "BUY", "11915", "NSE_EQ", "CNC", oid, 1),
            )
            con.execute(
                "INSERT INTO manual_risk_acknowledgements (parent_order_id, correlation_id, account_id, symbol, security_id, quantity, acknowledged_by, reason, status, acknowledged_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (oid, oid, ACCT, "YESBANK-EQ", "11915", 1, "owner", "retain 1-share lot", "UNPROTECTED_MANUAL_RISK", buy_ts),
            )

        # Closed sibling: 28 shares BUY (16 + 12 @ 17.72) closed via Forever OCO 23132611231775 -> child SELL 22126112566908 (28 @ 17.93)
        con.execute(
            "INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, filled_quantity, status, created_at, updated_at, protection_route) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("221261123606908", "221261123606908", "LIVE", "CNC", "NSE_EQ", "YESBANK-EQ", "11915", "LIMIT", 17.72, 28, 28, "TRADED", buy_ts, sell_ts, "FOREVER_OCO"),
        )
        con.execute(
            "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
            ("221261123606908", 1, json.dumps({"order_id": "221261123606908", "symbol": "YESBANK-EQ", "security_id": "11915", "segment": "NSE_EQ", "product_type": "CNC", "filled_qty": 28, "exit_filled_qty": 28, "position_state": "POSITION_CLOSED"}), sell_ts),
        )
        con.execute(
            "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, triggered_leg, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("221261123606908", ACCT, "YESBANK-EQ", "11915", "NSE_EQ", "CNC", "FOREVER_OCO", "POSITION_CLOSED", 28, 28, 28, 0, "TARGET_LEG", sell_ts),
        )
        con.execute(
            "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("221261123606908", "609978137", "YESBANK-EQ", 17.72, 16, buy_ts, "BUY", "11915", "NSE_EQ", "CNC", "221261123606908", 1),
        )
        con.execute(
            "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("221261123606908", "609978138", "YESBANK-EQ", 17.72, 12, buy_ts, "BUY", "11915", "NSE_EQ", "CNC", "221261123606908", 1),
        )
        con.execute(
            "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("22126112566908", "600252830", "YESBANK-EQ", 17.93, 28, sell_ts, "SELL", "11915", "NSE_EQ", "CNC", "221261123606908", 1),
        )
        con.execute(
            "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            ("221261123606908", "23132611231775", "YESBANK-EQ", 17.10, 17.00, 17.93, 28, "TRIGGERED", buy_ts),
        )
        con.execute(
            "INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?,?,?,?,?,?)",
            ("221261123606908", "22126112566908", "600252830", 28, 17.93, sell_ts),
        )


class TestC01DurableIssueWriter(unittest.TestCase):
    def test_c01_real_system_events_schema_write_readback_and_dedup(self) -> None:
        from src.broker.broker_snapshot import ContractKey
        from src.engine.durable_ledger_issues import issue_identity, read_durable_issues

        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            now = time.time()
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO order_intents (correlation_id, account_id, symbol, security_id, exchange_segment, product_type, order_type, price, quantity, state, broker_order_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("C-100", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5, "TRADED", "ORD-100", now, now),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ORD-100", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 5, 5, 0, 5, now),
                )
                cur = con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ORD-100", "EID-100", "SBIN-EQ", 800.0, 5, now, "BUY", "3045", "NSE_EQ", "CNC", "ORD-100", 1),
                )
                fill_id = int(cur.lastrowid)

            jv0 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            ck = ContractKey(ACCT, "3045", "NSE_EQ", "CNC")
            reason = "LEGACY_UNCLASSIFIED_RISK:persistence_uncertain"
            ident = issue_identity(ck, reason, "PARENT", "ORD-100")
            t0 = time.time_ns()
            opening = {
                "version": 1,
                "issue_id": ident,
                "contract": {"account_id": ACCT, "security_id": "3045", "exchange_segment": "NSE_EQ", "product_type": "CNC"},
                "reason_code": reason,
                "classification": "DURABLE_UNRESOLVED",
                "scope": "PARENT",
                "subject_id": "ORD-100",
                "source": "watcher",
                "first_seen_utc_ns": t0,
                "snapshot_id": "SNAP-C01-OPEN",
                "journal_digest": jv0.digest_sha256,
                "evidence_refs": [
                    {"snapshot_id": "SNAP-C01-OPEN", "book_key": "journal:view", "row_index": None, "payload_sha256": jv0.digest_sha256, "journal_ref": "journal:view"}
                ],
                "observed": None,
            }
            res1 = commit_durable_issue_open(db_path, account_id=ACCT, opening=opening)
            self.assertTrue(res1.ok, res1)
            self.assertEqual(res1.event_type, "OWNER_LOT_LEDGER_ISSUE_OPENED")
            self.assertTrue(res1.readback_verified)

            with closing(sqlite3.connect(db_path)) as con:
                cols = [r[1] for r in con.execute("PRAGMA table_info(system_events)").fetchall()]
                self.assertIn("payload", cols)
                self.assertNotIn("details", cols)
                rows = con.execute("SELECT event_type, description, payload FROM system_events").fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], "OWNER_LOT_LEDGER_ISSUE_OPENED")
                self.assertEqual(rows[0][1], ident)

            res_dup = commit_durable_issue_open(db_path, account_id=ACCT, opening=opening)
            self.assertTrue(res_dup.ok)
            self.assertTrue(res_dup.duplicate)
            with closing(sqlite3.connect(db_path)) as con:
                count = con.execute("SELECT COUNT(*) FROM system_events").fetchone()[0]
                self.assertEqual(count, 1)

            jv1 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            resolution = {
                "version": 1,
                "issue_id": ident,
                "contract": {"account_id": ACCT, "security_id": "3045", "exchange_segment": "NSE_EQ", "product_type": "CNC"},
                "reason_code": reason,
                "scope": "PARENT",
                "subject_id": "ORD-100",
                "snapshot_id": "SNAP-C01-RES",
                "journal_digest": jv1.digest_sha256,
                "opening_event_id": res1.event_id,
                "opening_sha256": res1.payload_sha256,
                "resolved_utc_ns": t0 + 1000,
                "proof_kind": "EXACT_PARENT_LEDGER_READBACK",
                "parent_readback_sha256": "b" * 64,
                "evidence_refs": [
                    {"snapshot_id": "SNAP-C01-RES", "book_key": "order_book", "row_index": 0, "payload_sha256": "c" * 64, "journal_ref": f"fills:{fill_id}"}
                ],
            }
            res2 = commit_durable_issue_resolution(db_path, account_id=ACCT, resolution=resolution)
            self.assertTrue(res2.ok, res2)
            self.assertEqual(res2.event_type, "OWNER_LOT_LEDGER_ISSUE_RESOLVED")
            jv2 = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            replay = read_durable_issues(jv2)
            self.assertFalse(replay.malformed)
            self.assertEqual(len(replay.unresolved), 0)


class TestC05PersistenceRiskReadback(unittest.TestCase):
    def test_c05_requires_exact_committed_record_readback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            db = JournalDB(db_path)
            rec = {
                "order_id": "ORD-500",
                "position_state": "PROTECTED_OPEN",
                "filled_qty": 10,
                "persistence_uncertain": True,
                "last_persistence_error": "disk busy",
                "owner_readback_failed": True,
            }
            # Missing row in watcher_orders -> MUST FAIL CLOSED
            ok_missing, msg_missing = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec, expected_order_id="ORD-500", expected_position_state="PROTECTED_OPEN", expected_filled_qty=10
            )
            self.assertFalse(ok_missing)
            self.assertTrue(rec.get("persistence_uncertain"))

            # Insert row with mismatched filled_qty -> MUST FAIL CLOSED
            with db._get_conn() as con:
                con.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?,?,?,?)",
                    ("ORD-500", 0, json.dumps({"order_id": "ORD-500", "position_state": "PROTECTED_OPEN", "filled_qty": 5}), time.time()),
                )
            ok_bad_qty, _ = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec, expected_order_id="ORD-500", expected_position_state="PROTECTED_OPEN", expected_filled_qty=10
            )
            self.assertFalse(ok_bad_qty)

            # Update watcher_orders + commit positions_lifecycle & verified fills -> MUST PASS & clear persistence flags
            with db._get_conn() as con:
                con.execute(
                    "UPDATE watcher_orders SET record_json = ? WHERE order_id = ?",
                    (json.dumps({"order_id": "ORD-500", "position_state": "PROTECTED_OPEN", "filled_qty": 10}), "ORD-500"),
                )
                con.execute(
                    "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ORD-500", ACCT, "SBIN-EQ", "3045", "NSE_EQ", "CNC", "FOREVER_OCO", "PROTECTED_OPEN", 10, 10, 0, 10, time.time()),
                )
                con.execute(
                    "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                    ("ORD-500", "ET-500", "SBIN-EQ", 800.0, 10, time.time(), "BUY", "3045", "NSE_EQ", "CNC", "ORD-500"),
                )
            ok_match, _ = verify_operation_readback_and_clear_persistence_risk(
                db_path, rec, expected_order_id="ORD-500", expected_position_state="PROTECTED_OPEN", expected_filled_qty=10
            )
            self.assertTrue(ok_match)
            self.assertFalse(rec.get("persistence_uncertain", False))
            self.assertFalse(rec.get("owner_readback_failed", False))


class TestC02C03OrphanAndZeroFillCancellation(unittest.TestCase):
    def _base_record(self) -> Dict[str, Any]:
        return {
            "order_id": "PARENT-1",
            "symbol": "SBIN-EQ",
            "security_id": "3045",
            "segment": "NSE_EQ",
            "product_type": "CNC",
            "orphaned_leg_cancel_details": {"LEG-99": {"order_id": "LEG-99", "status": "UNCERTAIN"}},
            "orphaned_leg_cancel_states": {"LEG-99": "UNCERTAIN"},
            "orphan_leg_risk": True,
            "protection_cancel_uncertain": True,
        }

    def test_c02_orphan_rejects_same_snapshot_and_missing_qty_and_accepts_two_independent(self) -> None:
        now_m = time.monotonic_ns()
        rec = self._base_record()
        clean_leg = {
            "dhanClientId": ACCT,
            "orderId": "LEG-99",
            "parentOrderId": "PARENT-1",
            "algoId": "GROUP-1",
            "securityId": "3045",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "remainingQuantity": 0,
        }
        snap1 = _make_snapshot(snapshot_id="SNAP-1", order_book=[clean_leg], mono_ns=now_m)
        snap2_same = _make_snapshot(snapshot_id="SNAP-1", order_book=[clean_leg], mono_ns=now_m)
        snap2_diff = _make_snapshot(snapshot_id="SNAP-2", order_book=[clean_leg], mono_ns=now_m + 2_000_000_000)

        # Identical snapshot_id -> BLOCKED
        out_same = resolve_durable_orphan_legs(rec, snap1, snap2_same, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertFalse(out_same.resolved)

        # Missing remainingQuantity (None) -> BLOCKED (never defaulted to 0)
        bad_leg = dict(clean_leg)
        del bad_leg["remainingQuantity"]
        snap_missing = _make_snapshot(snapshot_id="SNAP-3", order_book=[bad_leg], mono_ns=now_m + 2_000_000_000)
        out_missing = resolve_durable_orphan_legs(rec, snap1, snap_missing, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertFalse(out_missing.resolved)

        # Two genuinely independent fresh snapshots with explicit zero filled + zero remaining -> RESOLVED
        out_ok = resolve_durable_orphan_legs(rec, snap1, snap2_diff, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertTrue(out_ok.resolved, out_ok)

    def test_c03_zero_fill_cancel_rejects_missing_qty_and_trade_fill_and_accepts_two_independent(self) -> None:
        now_m = time.monotonic_ns()
        rec = self._base_record()
        canc_row = {
            "dhanClientId": ACCT,
            "orderId": "PARENT-1",
            "securityId": "3045",
            "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderStatus": "CANCELLED",
            "filledQty": 0,
            "remainingQuantity": 0,
        }
        snap1 = _make_snapshot(snapshot_id="S-A", order_book=[canc_row], trade_book=[], mono_ns=now_m)
        snap2 = _make_snapshot(snapshot_id="S-B", order_book=[canc_row], trade_book=[], mono_ns=now_m + 2_000_000_000)

        # Fill present in trade_book -> BLOCKED
        snap_with_fill = _make_snapshot(
            snapshot_id="S-C",
            order_book=[canc_row],
            trade_book=[{"orderId": "PARENT-1", "tradedQuantity": 1, "securityId": "3045"}],
            mono_ns=now_m + 2_000_000_000,
        )
        out_fill = resolve_ordinary_zero_fill_cancellation(rec, snap1, snap_with_fill, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertFalse(out_fill.resolved)

        # Missing filledQty -> BLOCKED
        canc_missing = dict(canc_row)
        canc_missing["filledQty"] = None
        snap_null = _make_snapshot(snapshot_id="S-D", order_book=[canc_missing], trade_book=[], mono_ns=now_m + 2_000_000_000)
        out_null = resolve_ordinary_zero_fill_cancellation(rec, snap1, snap_null, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertFalse(out_null.resolved)

        # Two independent snapshots with explicit 0/0 and empty trade_book -> RESOLVED
        out_ok = resolve_ordinary_zero_fill_cancellation(rec, snap1, snap2, expected_account_id=ACCT, now_mono_ns=now_m + 3_000_000_000)
        self.assertTrue(out_ok.resolved, out_ok)


class TestC04SettledSessionAndYesbankRollover(unittest.TestCase):
    def _calendar(self) -> VerifiedCalendar:
        return VerifiedCalendar(
            holidays=frozenset(),
            covered_years=frozenset({2026}),
        )

    def _hist_books(self, mono_ns: int, utc_ns: int) -> List[BookEvidence]:
        return [
            _book(
                "history:2026-11-24:2026-11-26:0",
                [
                    {
                        "dhanClientId": ACCT,
                        "orderId": "221261123606908",
                        "exchangeTradeId": "609978137",
                        "securityId": "11915",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "transactionType": "BUY",
                        "tradedQuantity": 16,
                        "tradedPrice": 17.72,
                        "exchangeTime": "2026-11-24 10:00:00",
                    },
                    {
                        "dhanClientId": ACCT,
                        "orderId": "221261123606908",
                        "exchangeTradeId": "609978138",
                        "securityId": "11915",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "transactionType": "BUY",
                        "tradedQuantity": 12,
                        "tradedPrice": 17.72,
                        "exchangeTime": "2026-11-24 10:00:01",
                    },
                    {
                        "dhanClientId": ACCT,
                        "orderId": "22126112566908",
                        "exchangeTradeId": "600252830",
                        "securityId": "11915",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "transactionType": "SELL",
                        "tradedQuantity": 28,
                        "tradedPrice": 17.93,
                        "exchangeTime": "2026-11-25 11:15:00",
                    },
                ],
                mono_ns=mono_ns,
                utc_ns=utc_ns,
                request_params={"from_date": "2026-11-24", "to_date": "2026-11-26", "page": 0},
                endpoint="/v2/trades/2026-11-24/2026-11-26/0",
            ),
            _book(
                "history:2026-11-24:2026-11-26:1",
                [],
                mono_ns=mono_ns,
                utc_ns=utc_ns,
                request_params={"from_date": "2026-11-24", "to_date": "2026-11-26", "page": 1},
                endpoint="/v2/trades/2026-11-24/2026-11-26/1",
            ),
        ]

    def _child_order_status(self, mono_ns: int, utc_ns: int) -> Dict[str, BookEvidence]:
        return {
            "22126112566908": _book(
                "order_status",
                {
                        "dhanClientId": ACCT,
                        "orderId": "22126112566908",
                        "securityId": "11915",
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "transactionType": "SELL",
                        "orderStatus": "TRADED",
                        "filledQty": 28,
                        "remainingQuantity": 0,
                    },
                request_params={"order_id": "22126112566908"},
                mono_ns=mono_ns,
                utc_ns=utc_ns,
            )
        }

    def test_c04_yesbank_4_row_rollover_matrix_and_no_metadata_only_certification(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            _seed_yesbank_db(db_path)
            jv = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            cal = self._calendar()
            now_m = time.monotonic_ns()
            now_u = time.time_ns()
            cov = (HistoryCoverage(datetime.date(2026, 11, 24), datetime.date(2026, 11, 26), ("history:2026-11-24:2026-11-26:0", "history:2026-11-24:2026-11-26:1"), True, True, now_m, False),)

            # 4-row YESBANK rollover matrix:
            # (30, -28) -> BLOCKED; (30, 0) -> BLOCKED; (2, -28) -> BLOCKED; (2, 0) -> SETTLED
            cases = [
                (30, -28, False),
                (30, 0, False),
                (2, -28, False),
                (2, 0, True),
            ]
            for hold_qty, net_pos, expected in cases:
                snap = _make_snapshot(
                    snapshot_id=f"YB-{hold_qty}-{net_pos}",
                    session_date="2026-11-26",
                    holdings=[{"dhanClientId": ACCT, "securityId": "11915", "tradingSymbol": "YESBANK-EQ", "totalQty": hold_qty, "dpQty": hold_qty, "t1Qty": 0, "availableQty": hold_qty, "collateralQty": 0}],
                    positions=([{"dhanClientId": ACCT, "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": net_pos}] if net_pos != 0 else []),
                    historical_trades=self._hist_books(now_m, now_u),
                    order_status=self._child_order_status(now_m, now_u),
                    history_coverage=cov,
                    mono_ns=now_m,
                    utc_ns=now_u,
                )
                res = verify_settled_session_evidence(snap, jv, calendar=cal, now_mono_ns=now_m + 1_000_000_000)
                self.assertEqual(res.settled, expected, f"Case holdings={hold_qty}, net={net_pos}: {res}")

            # C04 adversarial check: history_coverage metadata WITHOUT actual historical_trades payloads -> BLOCKED!
            snap_meta_only = _make_snapshot(
                snapshot_id="YB-META-ONLY",
                session_date="2026-11-26",
                holdings=[{"dhanClientId": ACCT, "securityId": "11915", "tradingSymbol": "YESBANK-EQ", "totalQty": 2, "dpQty": 2, "t1Qty": 0, "availableQty": 2, "collateralQty": 0}],
                positions=[],
                historical_trades=[],
                order_status=self._child_order_status(now_m, now_u),
                history_coverage=cov,
                mono_ns=now_m,
                utc_ns=now_u,
            )
            res_meta = verify_settled_session_evidence(snap_meta_only, jv, calendar=cal, now_mono_ns=now_m + 1_000_000_000)
            self.assertFalse(res_meta.settled, "C04 violated: metadata-only history_coverage must NEVER certify settlement")

            # Stale settled snapshot (>10s) -> BLOCKED!
            stale_m = now_m - 15_000_000_000
            snap_stale = _make_snapshot(
                snapshot_id="YB-STALE",
                session_date="2026-11-26",
                holdings=[{"dhanClientId": ACCT, "securityId": "11915", "tradingSymbol": "YESBANK-EQ", "totalQty": 2, "dpQty": 2, "t1Qty": 0, "availableQty": 2, "collateralQty": 0}],
                positions=[],
                historical_trades=self._hist_books(stale_m, now_u),
                order_status=self._child_order_status(stale_m, now_u),
                history_coverage=(HistoryCoverage(datetime.date(2026, 11, 24), datetime.date(2026, 11, 26), ("history:2026-11-24:2026-11-26:0", "history:2026-11-24:2026-11-26:1"), True, True, stale_m, False),),
                mono_ns=stale_m,
                utc_ns=now_u,
            )
            res_stale = verify_settled_session_evidence(snap_stale, jv, calendar=cal, now_mono_ns=now_m)
            self.assertFalse(res_stale.settled)

    def test_c04_generalized_flat_account_settled_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "trade_journal.db")
            JournalDB(db_path)
            jv = read_journal_view(db_path, account_id=ACCT, journal_epoch=1, read_utc_ns=time.time_ns())
            cal = self._calendar()
            now_m = time.monotonic_ns()
            snap = _make_snapshot(
                snapshot_id="FLAT-1",
                session_date="2026-11-26",
                holdings=[],
                positions=[],
                mono_ns=now_m,
            )
            res = verify_settled_session_evidence(
                snap, jv, calendar=cal, now_mono_ns=now_m + 1_000_000_000, require_history_for_flat=False
            )
            self.assertTrue(res.settled, res)


if __name__ == "__main__":
    unittest.main()
