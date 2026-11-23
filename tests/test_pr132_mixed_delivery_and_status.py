"""PR132 isolated offline acceptance: exact YESBANK 2 settled + 28 current CNC.

No live Dhan, Telegram, network access or writes to trading databases.
"""
import ast
from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock
from zoneinfo import ZoneInfo

# The GitHub checkout has src.engine.market; this reduced CI review bundle
# omits the unchanged file. The fallback below is test-only and conditional.
if importlib.util.find_spec("src.engine.market") is None:
    market = types.ModuleType("src.engine.market")
    market.IST = ZoneInfo("Asia/Kolkata")
    sys.modules["src.engine.market"] = market

from src.engine.holding_quantity import observed_open_quantity
from src.engine.mixed_cnc_evidence import MixedCNCUnverified, verify_mixed_cnc_split
from src.notifier.runtime_status import RuntimeStatusTransitions, format_runtime_status

ROOT = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")


def stamp(day, hour=14, minute=54, second=59):
    return datetime(2026, 11, day, hour, minute, second, tzinfo=IST).timestamp()


class FakeDB:
    account_id = "SYNTHETIC"
    def __init__(self):
        self.parents = {
            "OLD1": (stamp(2), [("old-fill1", 1, 15.1)]),
            "OLD2": (stamp(4), [("old-fill2", 1, 15.2)]),
            "NEW28": (stamp(23), [("today-fill16", 16, 21.1), ("today-fill12", 12, 21.1)]),
        }
        self.rows = []
        self.fills = {}
        for oid, (created, fills) in self.parents.items():
            qty = sum(q for _, q, _ in fills)
            self.rows.append({
                "parent_order_id": oid, "security_id": "11915",
                "exchange_segment": "NSE_EQ", "product_type": "CNC",
                "confirmed_open_qty": qty, "buy_filled_qty": qty,
                "entry_qty": qty, "sell_filled_qty": 0,
                "position_state": "PROTECTED" if oid == "NEW28" else "UNPROTECTED_MANUAL_RISK",
            })
            for etid, quantity, price in fills:
                self.fills[etid] = {
                    "exchange_trade_id": etid, "broker_order_id": oid,
                    "parent_order_id": oid, "security_id": "11915",
                    "exchange_segment": "NSE_EQ", "product_type": "CNC",
                    "transaction_type": "BUY", "is_verified_trade": 1,
                    "traded_quantity": quantity, "traded_price": price,
                    "traded_at": created,
                }
        self.revoked = set()
        self.events = []
        self.conflict = set()

    def owner_lot_basis(self, account, security, segment, product, oid):
        assert (account, security, segment, product) == ("SYNTHETIC", "11915", "NSE_EQ", "CNC")
        created, fills = self.parents[oid]
        return created, [dict(exchange_trade_id=e, traded_quantity=q, traded_price=p)
                         for e, q, p in fills]

    def get_fill_by_exchange_trade_id(self, etid):
        return self.fills.get(etid)

    def is_manual_risk_acknowledged(self, oid, cid, **kwargs):
        return oid in ("OLD1", "OLD2") and oid not in self.revoked and cid == oid and kwargs.get("required_qty") == 1

    def owner_lot_review_reason(self, account, oid):
        return "CONFIRMED_SELL_CONFLICT" if oid in self.conflict else None

    def get_open_positions_lifecycle(self):
        return self.rows

    def record_system_event(self, typ, description, payload=""):
        self.events.append((typ, description, json.loads(payload) if payload else {}))


class BrokerFixture:
    def __init__(self, db):
        self.db = db
        self.positions = [{
            "securityId": "11915", "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 28, "dhanClientId": "SYNTHETIC",
            "sellQty": 0,
        }]
        self.holdings = [{
            "securityId": "11915", "dhanClientId": "SYNTHETIC",
            "isin": "INE528G01035", "totalQty": 2, "dpQty": 2, "t1Qty": 0,
        }]
        self.trades = [
            {"dhanClientId": "SYNTHETIC", "securityId": "11915", "isin": "INE528G01035",
             "exchangeSegment": "NSE_EQ", "productType": "CNC", "orderId": "NEW28",
             "exchangeTradeId": etid, "transactionType": "BUY", "tradedQuantity": qty,
             "tradedPrice": 21.1}
            for etid, qty in (("today-fill16", 16), ("today-fill12", 12))
        ]
        self.cfg = NS(dhan_client_id="SYNTHETIC")
        self.orders = [dict(securityId="11915", orderId="NEW28", transactionType="BUY",
                            orderStatus="TRADED", filledQty=28)]
        self.forever = [dict(securityId="11915", orderId="FOREVER28", orderStatus="PENDING",
                             transactionType="SELL")]
        self.get_authenticated_profile_id = Mock(return_value="SYNTHETIC")
        self.get_order_book = Mock(side_effect=lambda: list(self.orders))
        self.get_super_orders = Mock(return_value=[])
        self.get_forever_orders = Mock(side_effect=lambda: list(self.forever))
        self.get_trade_book = Mock(side_effect=lambda: list(self.trades))
        self.get_positions = Mock(side_effect=lambda: list(self.positions))
        self.get_holdings = Mock(side_effect=lambda: list(self.holdings))
        self.get_edis_owned_quantity = Mock(return_value=None)

    def proof(self):
        return verify_mixed_cnc_split(
            db=self.db, positions=self.positions, holdings=self.holdings,
            trade_book=self.trades, account="SYNTHETIC",
            security="11915", segment="NSE_EQ", product="CNC",
            parent_quantities={k: v[1] for k, v in [
                (r["parent_order_id"], (r["security_id"], r["confirmed_open_qty"]))
                for r in self.db.rows]},
            now=stamp(23, 15, 10),
        )


class MixedCNCSafety(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.fx = BrokerFixture(self.db)

    def test_two_old_one_new_verified_exact_total(self):
        proof = self.fx.proof()
        self.assertEqual((proof.settled_qty, proof.intraday_qty, proof.total_qty), (2, 28, 30))
        self.assertEqual(set(proof.settled_parent_ids), {"OLD1", "OLD2"})
        self.assertEqual(proof.intraday_parent_ids, ("NEW28",))
        self.assertEqual(len(proof.intraday_trade_keys), 2)
        self.assertEqual((None, "CNC_POSITIONS_HOLDINGS_CONTRADICT"),
                         observed_open_quantity(self.fx.positions, self.fx.holdings,
                             security_id="11915", segment="NSE_EQ", product="CNC", account_id="SYNTHETIC"))

    def test_missing_one_current_buy_fill(self):
        self.fx.trades.pop()
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_duplicate_current_buy_fill(self):
        self.fx.trades.append(dict(self.fx.trades[0]))
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_unattributed_sell_halts(self):
        self.fx.trades.append(dict(self.fx.trades[0], transactionType="SELL", exchangeTradeId="external"))
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_unattributed_external_buy_halts(self):
        self.fx.trades.append(dict(self.fx.trades[0], orderId="EXTERNAL", exchangeTradeId="other"))
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_wrong_broker_account_halts(self):
        self.fx.trades[0]["dhanClientId"] = "OTHER"
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_wrong_broker_price_halts(self):
        self.fx.trades[0]["tradedPrice"] = 90
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_current_day_quantity_mismatch_halts(self):
        self.fx.positions[0]["netQty"] = 29
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_settled_holding_shortfall_halts(self):
        self.fx.holdings[0]["totalQty"] = 1
        self.fx.holdings[0]["dpQty"] = 1
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_extra_settled_holding_halts(self):
        self.fx.holdings[0]["totalQty"] = 3
        self.fx.holdings[0]["dpQty"] = 3
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_old_owner_ack_missing_halts(self):
        self.db.revoked.add("OLD2")
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_old_owner_sell_conflict_halts(self):
        self.db.conflict.add("OLD1")
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_new_buy_parent_id_mismatch_halts(self):
        self.db.fills["today-fill12"]["parent_order_id"] = "OLD2"
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_unverified_durable_fill_halts(self):
        self.db.fills["today-fill16"]["is_verified_trade"] = 0
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_missing_isin_halts(self):
        del self.fx.holdings[0]["isin"]
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_other_exchange_open_position_halts(self):
        self.fx.positions.append(dict(self.fx.positions[0], exchangeSegment="BSE_EQ", netQty=1))
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_mixed_fill_dates_in_one_parent_halts(self):
        self.db.fills["today-fill16"]["traded_at"] = stamp(22)
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_same_day_sell_in_journal_cannot_be_counted_as_buy(self):
        self.db.fills["today-fill12"]["transaction_type"] = "SELL"
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()


class ExtractedOwnerIntegration(unittest.TestCase):
    """Exercise unmodified function AST within an isolated source-only fixture."""
    def extract(self, fn_name, *, history_fn=None):
        class ExecutionConflict(ValueError): pass
        class DematReview(ValueError): pass
        source = ROOT / "src/engine/owner_lot_evidence.py"
        tree = ast.parse(source.read_text())
        f = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == fn_name)
        module = ast.fix_missing_locations(ast.Module(body=[f], type_ignores=[]))
        namespace = dict(
            observed_open_quantity=observed_open_quantity,
            verify_mixed_cnc_split=verify_mixed_cnc_split,
            verify_owner_history=history_fn or Mock(),
            ExecutionConflict=ExecutionConflict, DematReview=DematReview, json=json,
        )
        exec(compile(module, str(source), "exec"), namespace)
        return namespace[fn_name], namespace["verify_owner_history"]

    def setUp(self):
        self.db = FakeDB(); self.fx = BrokerFixture(self.db)

    def test_gate_owner_proof_records_verified_2_plus_28(self):
        f, hist = self.extract("prepare_owner_lots")
        batch = {"account": "SYNTHETIC", "verified": {}, "errors": {}, "share": False}
        f(self.fx, self.db, batch, self.fx.positions, self.fx.trades,
          stamp(23, 15, 10), self.fx.holdings, self.fx.forever)
        self.assertEqual({}, batch["errors"])
        self.assertEqual(set(batch["verified"]), {"OLD1", "OLD2"})
        self.assertEqual(batch["mixed_verified"][("11915", "NSE_EQ", "CNC")].total_qty, 30)
        self.assertEqual(self.db.events[-1][0], "OWNER_LOT_EVIDENCE")
        self.assertEqual(self.db.events[-1][2]["quantity_source"], "VERIFIED_MIXED_CNC_DELIVERY")
        hist.assert_called_once()
        self.assertEqual(set(hist.call_args.args[6]), {"OLD1", "OLD2", "NEW28"})
        self.assertEqual(len(hist.call_args.kwargs["current_day_live_keys"]), 2)

    def test_no_owner_proof_if_live_sell_unattributed(self):
        f, hist = self.extract("prepare_owner_lots")
        self.fx.trades.append(dict(self.fx.trades[0], transactionType="SELL", exchangeTradeId="other"))
        batch = {"account": "SYNTHETIC", "verified": {}, "errors": {}, "share": False}
        f(self.fx, self.db, batch, self.fx.positions, self.fx.trades,
          stamp(23, 15, 10), self.fx.holdings, self.fx.forever)
        self.assertEqual(set(batch["errors"]), {"OLD1", "OLD2"})
        self.assertNotIn("mixed_verified", batch)
        hist.assert_not_called()

    def test_no_owner_proof_if_full_history_fails(self):
        f, hist = self.extract("prepare_owner_lots", history_fn=Mock(side_effect=ValueError("Historical pagination incomplete")))
        batch = {"account": "SYNTHETIC", "verified": {}, "errors": {}, "share": False}
        f(self.fx, self.db, batch, self.fx.positions, self.fx.trades,
          stamp(23, 15, 10), self.fx.holdings, self.fx.forever)
        self.assertEqual(set(batch["errors"]), {"OLD1", "OLD2"})
        self.assertNotIn("mixed_verified", batch)


class RuntimeStatusSemantics(unittest.TestCase):
    @staticmethod
    def snapshot(*, open=True, verified=False, at=None):
        return {"created_at": at or stamp(23, 15, 15), "reconciliation": "HEALTHY" if open else "BLOCKED",
                "entries_open": open, "watcher_running": True,
                "watcher_supervision_verified": verified}

    def test_startup_yellow_then_stable_green_update_once(self):
        trans = RuntimeStatusTransitions()
        trans.arm(self.snapshot(verified=False))
        self.assertIsNone(trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE"))
        card = trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE")
        self.assertIn("🟢", card)
        self.assertIn("Broker Supervision Read: VERIFIED", card)
        self.assertIn("New Entries Gate: OPEN", card)
        trans.confirm_queued(self.snapshot(verified=True))
        self.assertIsNone(trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE"))

    def test_failed_outbox_enqueue_never_silently_drops_status(self):
        trans = RuntimeStatusTransitions()
        trans.arm(self.snapshot(verified=False))
        self.assertIsNone(trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE"))
        first = trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE")
        # Simulate a failed SQLite INSERT: no confirm_queued call.
        retry = trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE")
        self.assertEqual(first, retry)
        trans.confirm_queued(self.snapshot(verified=True))
        self.assertIsNone(trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE"))

    def test_blocked_update_not_green(self):
        trans = RuntimeStatusTransitions()
        trans.arm(self.snapshot(verified=True))
        trans.observe(self.snapshot(open=False, verified=True), instance="Test", mode="LIVE")
        card = trans.observe(self.snapshot(open=False, verified=True), instance="Test", mode="LIVE")
        self.assertIn("🔴", card)
        self.assertIn("New Entries Gate: BLOCKED", card)

    def test_single_watcher_transient_not_notification(self):
        trans = RuntimeStatusTransitions()
        trans.arm(self.snapshot(verified=True))
        self.assertIsNone(trans.observe(self.snapshot(verified=False), instance="Test", mode="LIVE"))
        self.assertIsNone(trans.observe(self.snapshot(verified=True), instance="Test", mode="LIVE"))

    def test_open_but_unverified_never_green(self):
        s = format_runtime_status(self.snapshot(open=True, verified=False), instance="Test", mode="LIVE")
        self.assertIn("🟡", s)
        self.assertIn("New Entries Gate: OPEN", s)
        self.assertNotIn("🟢", s)


class HistoricalWindowBoundary(unittest.TestCase):
    """Today's broker tradebook may precede historical pagination; no old proof bypass."""
    def setUp(self):
        self.db = FakeDB()
        self.fx = BrokerFixture(self.db)
        self.broker = NS(get_historical_trades=lambda start, end: [
            dict(dhanClientId="SYNTHETIC", securityId="11915", isin="INE528G01035",
                 exchangeSegment="NSE_EQ", productType="CNC", orderId=oid,
                 exchangeTradeId=etid, transactionType="BUY", tradedQuantity=1, tradedPrice=price)
            for oid, etid, price in (("OLD1", "old-fill1", 15.1), ("OLD2", "old-fill2", 15.2))
        ])
        class StubDhan:
            @staticmethod
            def validate_historical_trade_record(row, **kwargs):
                if row.get("dhanClientId") != kwargs.get("expected_account_id"):
                    return False, None, "ACCOUNT_MISMATCH"
                return True, row, ""
        self.source = ROOT / "src/engine/owner_lot_evidence.py"
        tree = ast.parse(self.source.read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "verify_owner_history")
        class ExecutionConflict(ValueError): pass
        ns = dict(datetime=datetime, timedelta=timedelta, json=json,
                  IST=IST, DhanClient=StubDhan, ExecutionConflict=ExecutionConflict,
                  MAX_HISTORICAL_TRADE_WINDOW_DAYS=90)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                     str(self.source), "exec"), ns)
        self.verify = ns["verify_owner_history"]
        self.conflict = ExecutionConflict
        self.parents = {r["parent_order_id"]: r["confirmed_open_qty"] for r in self.db.rows}
        self.holding = self.fx.holdings[0]

    def run_history(self, today_keys=()):
        evidence = {}
        self.verify(self.broker, self.db, "SYNTHETIC", "11915", "NSE_EQ", "CNC",
                    self.parents, self.holding, self.fx.trades, stamp(23, 15, 10), evidence,
                    current_day_live_keys=today_keys)
        return evidence

    def test_prior_day_verified_and_today_live_is_sufficient(self):
        evidence = self.run_history((("NEW28", "today-fill16"), ("NEW28", "today-fill12")))
        self.assertEqual(evidence["to_date"], "2026-11-23")
        self.assertTrue(evidence["windows"])

    def test_live_fill_cannot_substitute_old_history(self):
        self.broker.get_historical_trades = lambda start, end: []
        with self.assertRaisesRegex(ValueError, "Complete history"):
            self.run_history((("NEW28", "today-fill16"), ("NEW28", "today-fill12")))

    def test_historical_today_missing_without_live_proof_stays_blocked(self):
        with self.assertRaisesRegex(ValueError, "Complete history"):
            self.run_history()

    def test_historical_sell_is_never_overridden_by_live_buys(self):
        old = self.broker.get_historical_trades
        self.broker.get_historical_trades = lambda start, end: old(start, end) + [
            dict(dhanClientId="SYNTHETIC", securityId="11915", isin="INE528G01035",
                 exchangeSegment="NSE_EQ", productType="CNC", orderId="SELLX",
                 exchangeTradeId="sell-1", transactionType="SELL", tradedQuantity=1, tradedPrice=16.)
        ]
        with self.assertRaises(self.conflict):
            self.run_history((("NEW28", "today-fill16"), ("NEW28", "today-fill12")))


class RuntimeSnapshotContract(unittest.TestCase):
    def setUp(self):
        import hashlib
        import os
        import time
        file = ROOT / "src/engine/operational_snapshot.py"
        tree = ast.parse(file.read_text())
        names = ("collect_operational_snapshot", "read_verified_snapshot")
        code = ast.fix_missing_locations(ast.Module(
            body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names],
            type_ignores=[]))
        self.at = stamp(23, 15, 10)
        os_fake = NS(getpid=lambda: 77777, path=os.path)
        time_fake = NS(time=lambda: self.at + 1)
        ns = dict(os=os_fake, time=time_fake, Any=object, proc_starttime=lambda pid: 53,
                  account_fingerprint=lambda a: hashlib.sha256(a.encode()).hexdigest(),
                  MAX_AGE_SECONDS=35, MAX_SUPERVISION_AGE_SECONDS=300,
                  FILE_NAME="runtime_operational_snapshot.json")
        exec(compile(code, str(file), "exec"), ns)
        self.ns = ns
        self.pipeline = NS(
            cfg=NS(dhan_client_id="SYNTHETIC"),
            reconciliation_gate=NS(state=NS(value="HEALTHY"),
                                   is_accepting_entries=True, last_reconciled_at=self.at),
            watcher=NS(_is_running=True, _supervision_read_complete=True,
                       _supervision_success_at=self.at),
        )
    def test_live_snapshot_both_readers_agree(self):
        truth = self.ns["collect_operational_snapshot"](self.pipeline, telegram_connected=True)
        self.assertEqual(2, truth["schema"])
        self.assertTrue(truth["watcher_supervision_verified"])
        self.ns["read_private_json"] = lambda *args, **kwargs: truth
        got = self.ns["read_verified_snapshot"]("/tmp", expected_pid=77777,
             expected_starttime=53, expected_account_id="SYNTHETIC", now_ts=self.at + 1)
        self.assertTrue(got["watcher_supervision_verified"])
        self.assertTrue(got["entries_open"])

    def test_stale_supervision_never_verified(self):
        self.pipeline.watcher._supervision_success_at = self.at - 900
        truth = self.ns["collect_operational_snapshot"](self.pipeline)
        self.assertFalse(truth["watcher_supervision_verified"])

    def test_old_schema_never_assumed_verified(self):
        truth = self.ns["collect_operational_snapshot"](self.pipeline)
        truth.pop("watcher_supervision_verified")
        truth.pop("watcher_supervision_at")
        truth["schema"] = 1
        self.ns["read_private_json"] = lambda *args, **kwargs: truth
        got = self.ns["read_verified_snapshot"]("/tmp", expected_pid=77777,
             expected_starttime=53, expected_account_id="SYNTHETIC", now_ts=self.at + 1)
        self.assertFalse(got["watcher_supervision_verified"])

    def test_wrong_pid_rejected(self):
        truth = self.ns["collect_operational_snapshot"](self.pipeline)
        self.ns["read_private_json"] = lambda *args, **kwargs: truth
        got = self.ns["read_verified_snapshot"]("/tmp", expected_pid=77778,
             expected_starttime=53, expected_account_id="SYNTHETIC", now_ts=self.at + 1)
        self.assertIsNone(got)

    def test_wrong_account_rejected(self):
        truth = self.ns["collect_operational_snapshot"](self.pipeline)
        self.ns["read_private_json"] = lambda *args, **kwargs: truth
        got = self.ns["read_verified_snapshot"]("/tmp", expected_pid=77777,
             expected_starttime=53, expected_account_id="OTHER", now_ts=self.at + 1)
        self.assertIsNone(got)

    def test_open_gate_with_blocked_state_rejected(self):
        truth = self.ns["collect_operational_snapshot"](self.pipeline)
        truth["reconciliation"] = "BLOCKED"
        self.ns["read_private_json"] = lambda *args, **kwargs: truth
        got = self.ns["read_verified_snapshot"]("/tmp", expected_pid=77777,
             expected_starttime=53, expected_account_id="SYNTHETIC", now_ts=self.at + 1)
        self.assertIsNone(got)



class DurableOwnerHistoryProof(unittest.TestCase):
    def setUp(self):
        import sqlite3
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("CREATE TABLE system_events (id INTEGER PRIMARY KEY, event_type TEXT, description TEXT, payload TEXT, created_at REAL)")
        f = ROOT / "src/journal/db.py"
        tree = ast.parse(f.read_text())
        cl = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "JournalDB")
        fn = next(n for n in cl.body if isinstance(n, ast.FunctionDef)
                  and n.name == "has_verified_mixed_cnc_history_today")
        exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                     str(f), "exec"), {"sqlite3": sqlite3, "json": json}, ns := {})
        self.method = ns["has_verified_mixed_cnc_history_today"]
        self.fake = NS(_get_conn=lambda: self.context())
        self.parents = {"OLD1": 1, "OLD2": 1, "NEW28": 28}

    def context(self):
        from contextlib import nullcontext
        return nullcontext(self.conn)

    def add_event(self, at=None, parent_quantities=None):
        parents = parent_quantities or self.parents
        payload = {
            "account": "SYNTHETIC", "security": "11915", "segment": "NSE_EQ", "product": "CNC",
            "parents": parents, "to_date": "2026-11-23", "windows": [["2026-11-02", "2026-11-23", 4]],
            "quantity_source": "VERIFIED_MIXED_CNC_DELIVERY",
            "mixed_split": {"account": "SYNTHETIC", "security": "11915", "segment": "NSE_EQ", "product": "CNC",
                            "settled_qty": 2, "intraday_qty": 28,
                            "settled_parent_ids": ["OLD1", "OLD2"], "intraday_parent_ids": ["NEW28"]},
        }
        self.conn.execute("INSERT INTO system_events(event_type, description,payload,created_at) VALUES(?,?,?,?)",
                          ("OWNER_LOT_EVIDENCE", "Verified original purchases and settled ownership",
                           json.dumps(payload), at if at is not None else stamp(23, 15, 12)))
        return payload

    def verify(self, **overrides):
        kwargs = dict(account="SYNTHETIC", security="11915", segment="NSE_EQ",
                      product="CNC", parent_quantities=self.parents, now=stamp(23, 15, 15))
        kwargs.update(overrides)
        return self.method(self.fake, **kwargs)

    def test_event_missing_fails_closed(self):
        self.assertFalse(self.verify())

    def test_exact_immutable_event_matches(self):
        self.add_event()
        self.assertTrue(self.verify())

    def test_old_event_fails_closed(self):
        self.add_event(at=stamp(22, 15, 12))
        self.assertFalse(self.verify())

    def test_account_mismatch_fails_closed(self):
        self.add_event()
        self.assertFalse(self.verify(account="OTHER"))

    def test_changed_parent_qty_fails_closed(self):
        self.add_event()
        self.assertFalse(self.verify(parent_quantities={"OLD1": 1, "OLD2": 1, "NEW28": 27}))

    def test_tampered_split_parent_mapping_fails_closed(self):
        self.add_event()
        event = self.conn.execute("SELECT payload FROM system_events WHERE id=1").fetchone()
        payload = json.loads(event["payload"])
        payload["mixed_split"]["settled_parent_ids"] = ["NEW28", "OLD2"]
        self.conn.execute("UPDATE system_events SET payload=? WHERE id=1", (json.dumps(payload),))
        self.assertFalse(self.verify())


class DashboardStatusParity(unittest.TestCase):
    def setUp(self):
        p = ROOT / "src/control_center_backend.py"
        tree = ast.parse(p.read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "_operational_evidence")
        self.snapshot = {
            "reconciliation": "HEALTHY", "watcher_running": True,
            "watcher_supervision_verified": False,
            "entries_open": True, "telegram_connected": True, "created_at": stamp(23,15,10),
        }
        ns = dict(Dict=dict, Any=object, ConfigChangeTracker=object, AppConfig=object,
                  read_verified_snapshot=lambda *a,**k: self.snapshot)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                     str(p), "exec"), ns)
        self.read = ns["_operational_evidence"]
        self.tracker = NS(get_active_running_version=lambda **k: {"pid": 777, "process_starttime": 12})
        self.raw = {"runtime_verified": True, "service_state": "RUNNING"}
        self.cfg = NS(dhan_client_id="SYNTHETIC")

    def test_watcher_loop_is_not_supervision_verified(self):
        out = self.read(self.tracker, self.raw, self.cfg, "/tmp")
        self.assertEqual("ACTIVE", out["protection_watcher"])
        self.assertEqual("UNVERIFIED", out["protection_supervision"])
        self.assertEqual("OPEN", out["new_entries_gate"])

    def test_verified_must_be_separate_fact(self):
        self.snapshot["watcher_supervision_verified"] = True
        out = self.read(self.tracker, self.raw, self.cfg, "/tmp")
        self.assertEqual("VERIFIED", out["protection_supervision"])

    def test_no_runtime_snapshot_is_unverified(self):
        self.raw["runtime_verified"] = False
        out = self.read(self.tracker, self.raw, self.cfg, "/tmp")
        self.assertEqual("UNVERIFIED", out["protection_supervision"])
        self.assertEqual("UNVERIFIED", out["new_entries_gate"])

    def test_ui_green_requires_verified_supervision(self):
        js = (ROOT / "src/control_center_ui.py").read_text()
        self.assertIn("value==='ACTIVE'&&supervision==='VERIFIED'", js)
        self.assertIn("'ACTIVE · '+supervision:value", js)


class AuthoringSafetyGuards(unittest.TestCase):
    def test_original_ambiguous_quantity_still_unknown(self):
        q, source = observed_open_quantity(
            [{"securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 28}],
            [{"securityId": "11915", "totalQty": 2, "dpQty": 2}],
            security_id="11915", segment="NSE_EQ", product="CNC", account_id="SYNTHETIC")
        self.assertIsNone(q)
        self.assertEqual("CNC_POSITIONS_HOLDINGS_CONTRADICT", source)

    def test_no_entry_gate_or_broker_order_mutation_added_to_pure_modules(self):
        for f in ("src/engine/mixed_cnc_evidence.py", "src/notifier/runtime_status.py"):
            src = (ROOT / f).read_text()
            t = ast.parse(src)
            forbidden = {"place_order", "cancel_order", "submit_order", "halt_new_entries", "_accepting_entries"}
            for item in ast.walk(t):
                if isinstance(item, ast.Attribute):
                    self.assertNotIn(item.attr, forbidden, (f, item.lineno))

    def test_runtime_status_notification_does_not_use_dhan(self):
        src = (ROOT / "src/notifier/runtime_status.py").read_text()
        self.assertNotIn("DhanClient", src)
        self.assertNotIn("requests.", src)


class TelegramStartupSnapshotParity(unittest.TestCase):
    def setUp(self):
        p = ROOT / "src/cli.py"
        tree = ast.parse(p.read_text())
        f = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == "format_startup_online_card")
        ns = dict(AppConfig=object)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[f], type_ignores=[])),
                     str(p), "exec"), ns)
        self.render = ns["format_startup_online_card"]
        self.cfg = NS(instance_name="Synthetic", trading_mode=NS(value="LIVE"), data_dir="/nonexistent")
        self.render_mod = types.ModuleType("src.notifier.message_designer")
        self.render_mod.render = lambda *args, **kwargs: None

    def card(self, **kwargs):
        from unittest.mock import patch
        with patch.dict(sys.modules, {"src.notifier.message_designer": self.render_mod}):
            return self.render(self.cfg, allowed_chats_count=2,
                               reconciliation_ok=True, snapshot_at=stamp(23, 15, 10), **kwargs)

    def test_open_but_watcher_not_verified_yellow(self):
        msg = self.card(watcher_verified=False)
        self.assertIn("🟡", msg)
        self.assertIn("Open (watcher unverified)", msg)
        self.assertIn("Startup snapshot: 15:10:59", msg)
        self.assertNotIn("🟢", msg)

    def test_green_only_when_both_verified(self):
        msg = self.card(watcher_verified=True)
        self.assertIn("🟢", msg)
        self.assertIn("Protection Watcher: Active and Verified", msg)

    def test_legacy_default_preserved_but_runner_supplies_explicit_state(self):
        # Backward compatibility for existing formatter users. run.py passes
        # the canonical watcher_verified flag explicitly.
        msg = self.card()
        self.assertIn("🟢", msg)
        runner = (ROOT / "run.py").read_text()
        self.assertIn("watcher_verified=watcher_live", runner)


if __name__ == "__main__":
    unittest.main()

class LegacyNULLTradeIdMixedCNC(unittest.TestCase):
    """Production owner-lot recovery explicitly supports NULL-ID prior BUY attestations."""
    def setUp(self):
        self.db = FakeDB()
        self.fx = BrokerFixture(self.db)
        date, _ = self.db.parents['OLD1']
        original = {
            'dhanClientId': 'SYNTHETIC', 'orderId': 'OLD1',
            'securityId': '11915', 'isin': 'INE528G01035',
            'exchangeSegment': 'NSE_EQ', 'productType': 'CNC',
            'transactionType': 'BUY', 'exchangeTradeId': None,
            'tradedQuantity': 1, 'tradedPrice': 15.1,
        }
        self.db.parents['OLD1'] = (date, [(None, 1, 15.1)])
        self.db.fills.pop('old-fill1')
        self.old_attestation = original
        original_basis = self.db.owner_lot_basis
        def basis(account, security, segment, product, oid):
            created, fills = original_basis(account,security,segment,product,oid)
            if oid == 'OLD1':
                fills[0]['evidence_json'] = json.dumps(self.old_attestation)
            return created, fills
        self.db.owner_lot_basis = basis

    def test_legacy_attested_old_buy_and_exact_new_fills_verified(self):
        proof = self.fx.proof()
        self.assertEqual((proof.settled_qty, proof.intraday_qty), (2,28))

    def test_legacy_unattested_null_id_rejected(self):
        self.old_attestation['orderId'] = 'UNRELATED'
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_legacy_null_id_today_not_accepted(self):
        self.db.parents['NEW28'] = (stamp(23), [(None,16,21.1),('today-fill12',12,21.1)])
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()

    def test_legacy_attestation_broker_account_mismatch_rejected(self):
        self.old_attestation['dhanClientId']='OTHER'
        with self.assertRaises(MixedCNCUnverified): self.fx.proof()
