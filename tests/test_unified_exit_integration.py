"""UREC-006: Full production-stack integration tests for unified BUY/SELL reconciliation.

Uses REAL JournalDB (SQLite WAL), REAL ProtectionWatcher (JSON + SQLite persistence
and restart reload), REAL StartupReconciliationGate.run_startup_reconciliation(),
REAL holding_quantity, and REAL ReadGovernor — zero network calls, zero live mutations.
"""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from src.broker.read_governor import ReadGovernor
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.journal.db import JournalDB
from src.models import BrokerReadList

IST = ZoneInfo("Asia/Kolkata")
ACCOUNT = "1113731536"


def _trade(oid, eid, side, qty, ts, price, *, sec_id="5264", seg="NSE_EQ", prod="CNC"):
    return {
        "orderId": oid,
        "exchangeTradeId": eid,
        "transactionType": side,
        "tradedQuantity": qty,
        "tradedPrice": price,
        "exchangeTime": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "exchangeOrderId": f"EXO-{oid}",
        "dhanClientId": ACCOUNT,
        "securityId": sec_id,
        "exchangeSegment": seg,
        "productType": prod,
    }


class UnifiedExitProductionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db_path = str(self.root / "trade_journal.db")
        self.watcher_path = str(self.root / "pending_protection.json")
        self.db = JournalDB(self.db_path, account_id=ACCOUNT)
        self.gov = ReadGovernor(self.tmp.name, ACCOUNT, clock=time.time)
        self.stop_mgr = NS(is_stopped=lambda: False)

    def _make_broker(self, *, trade_book, order_book, positions, holdings, forever=None, super_orders=None, hist_trades=None):
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        b = NS(
            cfg=NS(dhan_client_id=ACCOUNT, data_dir=self.tmp.name, journal_db_path=self.db_path, trading_mode="LIVE"),
            get_authenticated_profile_id=Mock(return_value=ACCOUNT),
            _read_governor=self.gov,
            get_fund_limits=Mock(return_value=(True, 25000.0, "Funds retrieved successfully")),
            get_trade_book=Mock(return_value=BrokerReadList(trade_book)),
            get_order_book=Mock(return_value=BrokerReadList(order_book)),
            get_positions=Mock(return_value=BrokerReadList(positions)),
            get_holdings=Mock(return_value=BrokerReadList(holdings)),
            get_forever_orders=Mock(return_value=BrokerReadList(forever or [])),
            get_super_orders=Mock(return_value=BrokerReadList(super_orders or [])),
            get_trades_for_order=Mock(return_value=BrokerReadList([])),
            get_historical_trades=Mock(return_value=BrokerReadList(hist_trades or [])),
        )
        for _r in order_book:
            if isinstance(_r, dict):
                _r.setdefault("dhanClientId", ACCOUNT)
                if _r.get("orderStatus") == "TRADED" and "filledQty" in _r:
                    _r.setdefault("quantity", _r["filledQty"])
                    _r.setdefault("remainingQuantity", 0)
        for _p in positions:
            if isinstance(_p, dict):
                _p.setdefault("dhanClientId", ACCOUNT)
                _sec = str(_p.get("securityId") or "")
                _seg = str(_p.get("exchangeSegment") or "NSE_EQ")
                _prod = str(_p.get("productType") or "CNC")
                _dbq = sum(int(t.get("tradedQuantity") or 0) for t in trade_book if isinstance(t, dict) and str(t.get("securityId") or "") == _sec and str(t.get("exchangeSegment") or "") == _seg and str(t.get("productType") or "") == _prod and str(t.get("transactionType") or "").upper() == "BUY")
                _dsq = sum(int(t.get("tradedQuantity") or 0) for t in trade_book if isinstance(t, dict) and str(t.get("securityId") or "") == _sec and str(t.get("exchangeSegment") or "") == _seg and str(t.get("productType") or "") == _prod and str(t.get("transactionType") or "").upper() == "SELL")
                _p.setdefault("dayBuyQty", _dbq)
                _p.setdefault("daySellQty", _dsq)
                _p.setdefault("carryForwardBuyQty", 0)
                _p.setdefault("carryForwardSellQty", 0)
        for _h in holdings:
            if isinstance(_h, dict):
                _h.setdefault("dhanClientId", ACCOUNT)
                _h.setdefault("t1Qty", 0)
                _h.setdefault("isin", f"ISIN-{_h.get('securityId', '0')}")
        _hist_by_oid = {}
        for _ht in (hist_trades or []):
            if isinstance(_ht, dict) and _ht.get("orderId"):
                _oid = str(_ht["orderId"])
                _prev = _hist_by_oid.get(_oid)
                _q = int(_ht.get("tradedQuantity") or 0)
                if _prev is None:
                    _hist_by_oid[_oid] = {
                        "dhanClientId": ACCOUNT,
                        "orderId": _oid,
                        "exchangeOrderId": str(_ht.get("exchangeOrderId") or f"EXO-{_oid}"),
                        "securityId": str(_ht.get("securityId") or ""),
                        "exchangeSegment": str(_ht.get("exchangeSegment") or "NSE_EQ"),
                        "productType": str(_ht.get("productType") or "CNC"),
                        "transactionType": str(_ht.get("transactionType") or "SELL"),
                        "orderStatus": "TRADED",
                        "quantity": _q,
                        "filledQty": _q,
                        "remainingQuantity": 0,
                    }
                else:
                    _prev["quantity"] += _q
                    _prev["filledQty"] += _q
        if _hist_by_oid:
            b.order_status = Mock(side_effect=lambda oid: dict(_hist_by_oid[str(oid)]) if str(oid) in _hist_by_oid else None)
        attach_unified_live_evidence_to_stub_broker(b, db_path=self.db_path, data_dir=self.tmp.name, account_id=ACCOUNT)
        return b

    def _seed_pdsl_open_buy(self, *, buy_oid="23126111920108", buy_ts=None, buy_qty=1, buy_price=402.5):
        ts = buy_ts or (datetime.now(IST) - timedelta(minutes=30))
        self.db.record_order_intent(
            correlation_id=f"EQ-PDSL-{buy_oid}",
            symbol="PDSL-EQ",
            trading_symbol="PDSL-EQ",
            security_id="5264",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=404.05,
            quantity=buy_qty,
            is_super_order=False,
            is_amo=False,
            protection_route="FOREVER_OCO",
            original_market_price=402.05,
        )
        self.db.update_order_intent(f"EQ-PDSL-{buy_oid}", state="TRADED", broker_order_id=buy_oid)
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id=?", (ts.timestamp(), buy_oid))
            conn.execute(
                """
                INSERT INTO fills (
                    broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity,
                    traded_at, transaction_type, security_id, exchange_segment, product_type,
                    parent_order_id, is_verified_trade
                ) VALUES (?, ?, 'PDSL', ?, ?, ?, 'BUY', '5264', 'NSE_EQ', 'CNC', ?, 1)
                """,
                (buy_oid, f"B-{buy_oid}", buy_price, buy_qty, ts.timestamp() + 2, buy_oid),
            )
            conn.execute(
                """
                INSERT INTO positions_lifecycle (
                    parent_order_id, account_id, symbol, trading_symbol, security_id,
                    exchange_segment, product_type, protection_route, position_state,
                    entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price, target_price,
                    sell_filled_qty, sell_vwap, confirmed_open_qty, realized_pnl,
                    triggered_leg, manual_exit_detected, orphaned_legs_detected, updated_at
                ) VALUES (?, ?, 'PDSL', 'PDSL-EQ', '5264', 'NSE_EQ', 'CNC', 'FOREVER_OCO',
                          'UNPROTECTED', ?, ?, ?, 0, 0.0, 0.0, 0, 0.0, ?, 0.0, '', 0, 0, ?)
                """,
                (buy_oid, ACCOUNT, buy_qty, buy_qty, buy_price, buy_qty, time.time()),
            )
        return ts

    def test_real_stack_pdsl_same_day_manual_sell_recovers_on_second_independent_pass(self):
        _now_sd = datetime.now(IST).replace(microsecond=0)
        _sod_sd = _now_sd.replace(hour=0, minute=0, second=10)
        buy_ts = self._seed_pdsl_open_buy(buy_ts=max(_sod_sd, _now_sd - timedelta(minutes=30)))
        sell_ts = min(_now_sd - timedelta(seconds=5), buy_ts + timedelta(minutes=15))
        sell_oid = "331261119150608"
        broker = self._make_broker(
            trade_book=[
                _trade("23126111920108", "B-23126111920108", "BUY", 1, buy_ts + timedelta(seconds=2), 402.5),
                _trade(sell_oid, "S-PDSL-1", "SELL", 1, sell_ts, 417.75),
            ],
            order_book=[
                {"orderId": "23126111920108", "securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                 "orderStatus": "TRADED", "transactionType": "BUY", "filledQty": 1, "dhanClientId": ACCOUNT},
                {"orderId": sell_oid, "securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                 "orderStatus": "TRADED", "transactionType": "SELL", "filledQty": 1, "dhanClientId": ACCOUNT},
            ],
            positions=[{"securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0, "dhanClientId": ACCOUNT}],
            holdings=[dict(securityId="5264", totalQty=0, dpQty=0)],
        )
        watcher = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=broker, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate

        # Block a required broker read using the real persisted Dhan read governor.
        clock = [1000.0]
        self.gov.clock = lambda: clock[0]
        self.gov.monotonic_clock = lambda: clock[0]
        self.gov.throttled(30)
        holdings = broker.get_holdings.return_value
        def read_holdings():
            # Synthetic transport emits an actual validated admission receipt,
            # and the independent captures are spaced by the locked recovery rate.
            receipt = self.gov.admit()
            self.gov.record_success(receipt=receipt)
            clock[0] += 0.5
            return holdings
        broker.get_holdings.side_effect = read_holdings
        self.assertFalse(gate.run_startup_reconciliation()[0])
        self.assertEqual(self.db.get_position_lifecycle("23126111920108")["confirmed_open_qty"], 1)
        clock[0] += 600
        broker.get_authenticated_profile_id.reset_mock()
        ok1, msg1 = gate.run_startup_reconciliation()
        self.assertTrue(ok1, msg1)
        self.assertEqual(broker.get_authenticated_profile_id.call_count, 2)
        lc = self.db.get_position_lifecycle("23126111920108")
        self.assertEqual(lc["position_state"], "POSITION_CLOSED")
        self.assertEqual(lc["confirmed_open_qty"], 0)
        self.assertEqual(lc["sell_filled_qty"], 1)

        # Simulate process restart: reload JournalDB + ProtectionWatcher from SQLite WAL & JSON files
        db_restarted = JournalDB(self.db_path, account_id=ACCOUNT)
        watcher_restarted = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=db_restarted)
        gate_restarted = StartupReconciliationGate(broker=broker, journal_db=db_restarted, watcher=watcher_restarted, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate_restarted

        # Pass 2: independent whole-gate verification succeeds -> HEALTHY
        ok2, msg2 = gate_restarted.run_startup_reconciliation()
        self.assertTrue(ok2, msg2)
        self.assertEqual(gate_restarted.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate_restarted.is_accepting_entries)

    def test_real_stack_historical_manual_sell_discovered_and_coexists_with_yesbank_manual_risk(self):
        # 1. Seed prior-day PDSL buy (sold offline yesterday; absent from today's tradebook/orderbook & local fills)
        buy_ts = self._seed_pdsl_open_buy(buy_ts=datetime.now(IST) - timedelta(days=3))
        hist_sell_ts = datetime.now(IST) - timedelta(days=1)

        # 2. Seed two-share YESBANK acknowledged manual-risk settled holdings (must remain intact!)
        owner_trades = []
        for idx, yb_oid in enumerate(("910000000000001", "910000000000002"), start=1):
            self.db.record_order_intent(
                correlation_id=f"EQ-YESBANK-{yb_oid}",
                symbol="YESBANK-EQ",
                trading_symbol="YESBANK-EQ",
                security_id="11915",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                order_type="LIMIT",
                price=18.5,
                quantity=1,
                is_super_order=False,
                is_amo=False,
                protection_route="FOREVER_OCO",
                original_market_price=18.0,
            )
            self.db.update_order_intent(f"EQ-YESBANK-{yb_oid}", state="TRADED", broker_order_id=yb_oid)
            with self.db._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO positions_lifecycle (
                        parent_order_id, account_id, symbol, trading_symbol, security_id,
                        exchange_segment, product_type, protection_route, position_state,
                        entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price, target_price,
                        sell_filled_qty, sell_vwap, confirmed_open_qty, realized_pnl,
                        triggered_leg, manual_exit_detected, orphaned_legs_detected, updated_at
                    ) VALUES (?, ?, 'YESBANK', 'YESBANK-EQ', '11915', 'NSE_EQ', 'CNC', 'FOREVER_OCO',
                              'UNPROTECTED_MANUAL_RISK', 1, 1, 18.0, 0, 0.0, 0.0, 0, 0.0, 1, 0.0, '', 0, 0, ?)
                    """,
                    (yb_oid, ACCOUNT, time.time()),
                )
            yb_ts = datetime.now(IST) - timedelta(days=2)
            with self.db._get_conn() as conn:
                conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id=?", (yb_ts.timestamp(), yb_oid))
            fill = _trade(yb_oid, f"Y-{idx}", "BUY", 1, yb_ts, 18.0, sec_id="11915")
            fill.update(isin="INE528G01035", exchangeOrderId=f"EX-{idx}")
            owner_trades.append(fill)
            self.db.record_fill(yb_oid, fill["exchangeTradeId"], "YESBANK", 18., 1,
                                traded_at=int(yb_ts.timestamp()),
                                security_id="11915", transaction_type="BUY", exchange_segment="NSE_EQ", product_type="CNC")
            self.db.acknowledge_manual_risk_holding(
                parent_order_id=yb_oid,
                correlation_id=f"EQ-YESBANK-{yb_oid}",
                account_id=ACCOUNT,
                symbol="YESBANK",
                security_id="11915",
                quantity=1,
                acknowledged_by="Udit",
                reason="Audited manual risk",
            )

        pdsl_buy_hist = _trade("23126111920108", "B-23126111920108", "BUY", 1, buy_ts + timedelta(seconds=2), 402.5)
        broker = self._make_broker(
            trade_book=[],
            order_book=[],
            positions=[],
            holdings=[{"securityId": "11915", "totalQty": 2, "dpQty": 2, "t1Qty": 0, "availableQty": 2, "isin": "INE528G01035", "dhanClientId": ACCOUNT}],
            hist_trades=[pdsl_buy_hist, *owner_trades, _trade("HIST-SELL-1", "S-HIST-1", "SELL", 1, hist_sell_ts, 420.0)],
        )
        broker.get_order_status = lambda oid: {
            "dhanClientId": ACCOUNT, "orderId": oid, "securityId": "5264",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL",
            "orderStatus": "TRADED", "quantity": 1, "filledQty": 1, "remainingQuantity": 0,
        } if oid == "HIST-SELL-1" else None
        watcher = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=broker, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate

        ok1, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok1, reason)
        self.assertEqual(self.db.get_position_lifecycle("23126111920108")["position_state"], "POSITION_CLOSED")
        self.assertEqual(self.db.get_position_lifecycle("910000000000001")["position_state"], "UNPROTECTED_MANUAL_RISK")
        self.assertEqual(self.db.get_position_lifecycle("910000000000002")["position_state"], "UNPROTECTED_MANUAL_RISK")

        ok2, msg2 = gate.run_startup_reconciliation()
        self.assertTrue(ok2, msg2)
        self.assertEqual(gate.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate.is_accepting_entries)

    def test_historical_manual_exit_with_unclassified_competing_fill_preserves_parent(self):
        parent = "PDSL-BUY"
        buy_time = datetime.now(IST) - timedelta(days=2)
        self._seed_pdsl_open_buy(buy_oid=parent, buy_ts=buy_time)
        ambiguous = _trade("OTHER-ORDER", "OTHER-EXEC", "UNKNOWN", 1,
                           buy_time + timedelta(minutes=3), 403.0)
        sell = _trade("SELL", "SELL-EXEC", "SELL", 1,
                      buy_time + timedelta(days=1), 420.0)
        broker = self._make_broker(trade_book=[], order_book=[], positions=[],
                                   holdings=[], hist_trades=[ambiguous, sell])
        for _ in range(2):
            self.db = JournalDB(self.db_path, account_id=ACCOUNT)
            gate = self._gate(broker)
            ok, reason = gate.run_startup_reconciliation()
            self.assertFalse(ok, reason)
            self.assertEqual(self.db.get_position_lifecycle(parent)["confirmed_open_qty"], 1)
            self.assertEqual(self.db.get_sell_fills_for_parent(parent), [])

    def _gate(self, broker):
        watcher = ProtectionWatcher(broker, path=self.watcher_path, journal_db=self.db)
        gate = StartupReconciliationGate(broker, self.db, watcher, self.stop_mgr)
        broker.reconciliation_gate = gate
        return gate

    def _round_trip(self, parent, ts, qty, exit_order):
        self._seed_pdsl_open_buy(buy_oid=parent, buy_ts=ts, buy_qty=qty)
        trades = [_trade(parent, f"B-{parent}", "BUY", qty, ts + timedelta(seconds=2), 402.5),
                  _trade(exit_order, f"S-{parent}", "SELL", qty, ts + timedelta(seconds=5), 415)]
        return trades, [dict(row, orderStatus="TRADED", filledQty=qty, quantity=qty, remainingQuantity=0) for row in trades]

    def test_reentry_ignores_only_verified_closed_lot(self):
        _now_ist = datetime.now(IST).replace(microsecond=0)
        ts = _now_ist.replace(hour=9, minute=0, second=0) if _now_ist.hour >= 11 else (_now_ist - timedelta(minutes=2))
        trades, orders = self._round_trip("B1", ts, 2, "S1")
        pos_row = dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0, dayBuyQty=2, daySellQty=2, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)
        broker = self._make_broker(trade_book=trades, order_book=orders, positions=[pos_row], holdings=[dict(securityId="5264", totalQty=0, dpQty=0)])
        gate = self._gate(broker)
        self.assertEqual(gate.run_startup_reconciliation()[0], True)
        t2, o2 = self._round_trip("B2", ts + timedelta(seconds=30), 3, "S2")
        pos_row["dayBuyQty"] = 5
        pos_row["daySellQty"] = 5
        broker.get_trade_book.return_value.extend(t2)
        broker.get_order_book.return_value.extend(o2)
        broker.get_trade_book.return_value.append(dict(t2[0], orderId="manual", exchangeTradeId="unknown"))
        self.assertFalse(gate.run_startup_reconciliation()[0])
        broker.get_trade_book.return_value.pop()
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM fills WHERE broker_order_id='manual' AND exchange_trade_id='unknown'")
        ok, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok, reason)
        for oid in ("B1", "B2"):
            self.assertEqual(self.db.get_position_lifecycle(oid)["confirmed_open_qty"], 0)

    def test_four_simultaneous_gate_checks_link_once(self):
        trades, orders = self._round_trip("CONCURRENT", datetime.now(IST) - timedelta(minutes=1), 2, "EXIT")
        pos3 = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0, dayBuyQty=2, daySellQty=2, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)]
        gate = self._gate(self._make_broker(trade_book=trades, order_book=orders, positions=pos3, holdings=[dict(securityId="5264", totalQty=0, dpQty=0)]))
        barrier = Barrier(4)

        def check():
            barrier.wait(timeout=5)
            return gate.run_startup_reconciliation()

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(check) for _ in range(4)]
            outcomes = [future.result(timeout=15) for future in futures]
        self.assertTrue(any(ok for ok, reason in outcomes), outcomes)
        self.assertTrue(gate.run_startup_reconciliation()[0])
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM system_events WHERE event_type='UNIFIED_EXIT_LINKED' "
                "AND description='CONCURRENT'").fetchone()[0], 1)
        self.assertEqual(self.db.get_position_accounting("CONCURRENT")["sell_filled_qty"], 2)

    def test_shared_super_id_requires_matching_terminal_leg(self):
        trades, orders = self._round_trip("SUP1", datetime.now(IST) - timedelta(minutes=1), 1, "SUP1-SL")
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET is_super_order=1 WHERE broker_order_id='SUP1'")
            conn.execute("UPDATE positions_lifecycle SET protection_route='SUPER_ORDER', triggered_leg='STOP_LOSS_LEG' WHERE parent_order_id='SUP1'")
            conn.execute(
                "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, quantity, status, created_at) VALUES ('SUP1', 'SUP1', 'PDSL', 1, 'POSITION_CLOSED_LOCAL', ?)",
                (time.time(),),
            )
        pos4 = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0, dayBuyQty=1, daySellQty=1, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)]
        broker = self._make_broker(trade_book=trades, order_book=orders, positions=pos4, holdings=[dict(securityId="5264", totalQty=0, dpQty=0)])
        gate = self._gate(broker)
        self.assertFalse(gate.run_startup_reconciliation()[0])
        super_row = dict(orders[0], orderStatus="CLOSED", legDetails=[
            dict(orderId="SUP1-SL", legName="STOP_LOSS_LEG", orderStatus="TRADED", transactionType="SELL", quantity=1, remainingQuantity=0),
            dict(orderId="SUP1-TP", legName="TARGET_LEG", orderStatus="CANCELLED", transactionType="SELL", quantity=1, remainingQuantity=0)])
        with self.db._get_conn() as conn:
            conn.execute("UPDATE positions_lifecycle SET protection_route='SUPER_ORDER', triggered_leg='STOP_LOSS_LEG' WHERE parent_order_id='SUP1'")
            row_w = conn.execute("SELECT record_json FROM watcher_orders WHERE order_id='SUP1'").fetchone()
            if row_w:
                import json as _json
                rw = _json.loads(row_w[0])
                rw["is_super_order"] = True
                rw["protection_route"] = "SUPER_ORDER"
                conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='SUP1'", (_json.dumps(rw),))
        broker2 = self._make_broker(trade_book=trades, order_book=orders, positions=pos4, holdings=[dict(securityId="5264", totalQty=0, dpQty=0)], super_orders=[super_row])
        (Path(self.tmp.name) / "snapshots.sqlite3").unlink(missing_ok=True)
        ok, reason = self._gate(broker2).run_startup_reconciliation()
        self.assertTrue(ok, reason)
        self.assertEqual(self.db.get_position_lifecycle("SUP1")["sell_filled_qty"], 1)

    def test_cancelled_partial_buy_fully_exited_persists(self):
        trades, orders = self._round_trip("PART", datetime.now(IST) - timedelta(minutes=1), 4, "SELL4")
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET quantity=10, state='CANCELLED' WHERE broker_order_id='PART'")
        orders[0].update(orderStatus="CANCELLED", quantity=10, remainingQuantity=6)
        pos5 = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0, dayBuyQty=4, daySellQty=4, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)]
        broker = self._make_broker(trade_book=trades, order_book=orders, positions=pos5, holdings=[dict(securityId="5264", totalQty=0, dpQty=0)])
        for _ in range(2):
            ok, reason = self._gate(broker).run_startup_reconciliation()
            self.assertTrue(ok, reason)
            self.assertEqual(self.db.get_position_lifecycle("PART")["sell_filled_qty"], 4)
            pending, terminal = self.db.load_watcher_state()
            self.assertNotIn("PART", pending)
            self.assertEqual(terminal["PART"]["state"], "CANCELLED")

    def test_historical_mtf_full_exit_requires_absent_position(self):
        ts = datetime.now(IST) - timedelta(days=3)
        self._seed_pdsl_open_buy(buy_oid="MTFB", buy_ts=ts, buy_qty=5)
        with self.db._get_conn() as conn:
            for table in ("order_intents", "fills", "positions_lifecycle"):
                conn.execute(f"UPDATE {table} SET product_type='MTF'")
        buy_h = _trade("MTFB", "B-MTFB", "BUY", 5, ts + timedelta(seconds=2), 402.5, prod="MTF")
        sell = _trade("MTFS", "MTF-EX1", "SELL", 5, ts + timedelta(days=1), 410, prod="MTF")
        row = dict(securityId="5264", exchangeSegment="NSE_EQ", productType="MTF", netQty=0)
        broker = self._make_broker(trade_book=[], order_book=[], positions=[row], holdings=[], hist_trades=[buy_h, sell])
        broker.get_order_status = lambda oid: {
            "dhanClientId": ACCOUNT, "orderId": oid, "securityId": "5264",
            "exchangeSegment": "NSE_EQ", "productType": "MTF", "transactionType": "SELL",
            "orderStatus": "TRADED", "quantity": 5, "filledQty": 5, "remainingQuantity": 0,
        } if oid == "MTFS" else None
        gate = self._gate(broker)
        self.assertFalse(gate.run_startup_reconciliation()[0])
        broker.get_positions.return_value.clear()
        ok, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok, reason)

    def test_split_dp_and_t1_delivery_sale_on_second_pass(self):
        ts = datetime.now(IST) - timedelta(days=2)
        self._seed_pdsl_open_buy(buy_oid="B5", buy_ts=ts, buy_qty=5)
        buy5_h = _trade("B5", "B-B5", "BUY", 5, ts + timedelta(seconds=2), 402.5)
        sell = _trade("S5", "S5EX1", "SELL", 5, datetime.now(IST), 410)
        position = dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=-5, dayBuyQty=0, daySellQty=5, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)
        broker = self._make_broker(trade_book=[sell], order_book=[dict(sell, orderStatus="TRADED", filledQty=5)],
                                   positions=[position], holdings=[dict(securityId="5264", dpQty=2, t1Qty=3, totalQty=5, availableQty=0, dhanClientId=ACCOUNT, isin="INE000P01001")],
                                   hist_trades=[buy5_h])
        for _ in range(2):
            ok, reason = self._gate(broker).run_startup_reconciliation()
            self.assertTrue(ok, reason)
        broker.get_trade_book.return_value.clear()
        self.assertFalse(self._gate(broker).run_startup_reconciliation()[0])

    def test_open_fno_lot_with_zero_or_missing_position_blocks(self):
        self._seed_pdsl_open_buy(buy_oid="OPTION", buy_qty=75)
        with self.db._get_conn() as conn:
            for table in ("order_intents", "fills", "positions_lifecycle"):
                conn.execute(f"UPDATE {table} SET exchange_segment='NSE_FNO', product_type='MARGIN'")
        position = dict(securityId="5264", exchangeSegment="NSE_FNO", productType="MARGIN", netQty=0)
        for snapshot in ([], [position]):
            broker = self._make_broker(trade_book=[], order_book=[], positions=snapshot, holdings=[])
            ok, reason = self._gate(broker).run_startup_reconciliation()
            self.assertFalse(ok)
            self.assertIn("exposure mismatch", reason)

    def test_delivery_sale_requires_sufficient_explicit_holdings_after_restart(self):
        buy_ts_d = datetime.now(IST) - timedelta(days=2)
        self._seed_pdsl_open_buy(buy_ts=buy_ts_d)
        buy_d_h = _trade("23126111920108", "B-23126111920108", "BUY", 1, buy_ts_d + timedelta(seconds=2), 402.5)
        sell = _trade("SELL-D", "EXEC-D", "SELL", 1, datetime.now(IST), 417.75)
        position = dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                        netQty=-1, sellQty=1, buyQty=0, dayBuyQty=0, daySellQty=1, carryForwardBuyQty=0, carryForwardSellQty=0, dhanClientId=ACCOUNT)
        broker = self._make_broker(trade_book=[sell], order_book=[dict(sell, orderStatus="TRADED", filledQty=1)],
                                   positions=[position], holdings=[], hist_trades=[buy_d_h])
        watcher = ProtectionWatcher(broker, path=self.watcher_path, journal_db=self.db)
        gate = StartupReconciliationGate(broker, self.db, watcher, self.stop_mgr)
        broker.reconciliation_gate = gate
        self.assertFalse(gate.run_startup_reconciliation()[0])
        self.assertEqual(self.db.get_position_lifecycle("23126111920108")["confirmed_open_qty"], 1)
        broker.get_holdings.return_value = BrokerReadList([dict(securityId="5264", totalQty=0, dpQty=0)])
        self.assertFalse(gate.run_startup_reconciliation()[0])
        broker.get_holdings.return_value = BrokerReadList([dict(securityId="5264", dpQty=0, t1Qty=1, totalQty=1, availableQty=0, dhanClientId=ACCOUNT, isin="INE000P01001")])
        ok, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok, reason)
        self.assertEqual(self.db.get_position_lifecycle("23126111920108")["confirmed_open_qty"], 0)
        # A fresh independent pass must still verify outstanding protection.
        broker.get_forever_orders.return_value = BrokerReadList([dict(position, orderId="OCO", orderStatus="PENDING")])
        self.assertFalse(gate.run_startup_reconciliation()[0])

    def test_real_stack_stays_blocked_when_holdings_missing_without_verified_sell(self):
        self._seed_pdsl_open_buy(buy_ts=datetime.now(IST) - timedelta(days=2))
        broker = self._make_broker(
            trade_book=[],
            order_book=[],
            positions=[],
            holdings=[],
            hist_trades=[],
        )
        watcher = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=broker, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate

        ok1, msg1 = gate.run_startup_reconciliation()
        self.assertFalse(ok1)
        self.assertEqual(gate.state, ReconciliationState.BLOCKED)
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(self.db.get_position_lifecycle("23126111920108")["confirmed_open_qty"], 1)



    def test_real_stack_two_buy_lots_single_sell_restart_accounting_and_gate_healthy(self):
        _n_ist = datetime.now(IST).replace(microsecond=0)
        _sod = _n_ist.replace(hour=0, minute=0, second=10)
        buy_ts1 = self._seed_pdsl_open_buy(buy_oid="BUY-PDSL-1", buy_ts=max(_sod, _n_ist - timedelta(minutes=40)), buy_qty=3, buy_price=400.0)
        buy_ts2 = self._seed_pdsl_open_buy(buy_oid="BUY-PDSL-2", buy_ts=max(_sod + timedelta(seconds=10), _n_ist - timedelta(minutes=30)), buy_qty=2, buy_price=405.0)
        sell_ts = max(buy_ts2 + timedelta(seconds=10), _n_ist - timedelta(minutes=10))
        sell_oid = "SELL-PDSL-ALL-5"
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO fills (
                    broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity,
                    traded_at, transaction_type, security_id, exchange_segment, product_type,
                    parent_order_id, is_verified_trade
                ) VALUES (?, 'S-PDSL-5', 'PDSL', 420.0, 5, ?, 'SELL', '5264', 'NSE_EQ', 'CNC', ?, 1)
                """,
                (sell_oid, sell_ts.timestamp(), sell_oid),
            )
        broker = self._make_broker(
            trade_book=[
                _trade("BUY-PDSL-1", "B-BUY-PDSL-1", "BUY", 3, buy_ts1 + timedelta(seconds=2), 400.0),
                _trade("BUY-PDSL-2", "B-BUY-PDSL-2", "BUY", 2, buy_ts2 + timedelta(seconds=2), 405.0),
                _trade(sell_oid, "S-PDSL-5", "SELL", 5, sell_ts, 420.0),
            ],
            order_book=[
                {"orderId": "BUY-PDSL-1", "securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                 "orderStatus": "TRADED", "transactionType": "BUY", "filledQty": 3, "dhanClientId": ACCOUNT},
                {"orderId": "BUY-PDSL-2", "securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                 "orderStatus": "TRADED", "transactionType": "BUY", "filledQty": 2, "dhanClientId": ACCOUNT},
                {"orderId": sell_oid, "securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC",
                 "orderStatus": "TRADED", "transactionType": "SELL", "filledQty": 5, "dhanClientId": ACCOUNT},
            ],
            positions=[{"securityId": "5264", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0, "dhanClientId": ACCOUNT}],
            # Explicit zero settled holdings is required when relative timestamps
            # straddle IST midnight; an omitted holding is not verified zero.
            holdings=[{"securityId": "5264", "totalQty": 0, "dpQty": 0}],
        )
        broker.place_forever_oco = Mock(side_effect=AssertionError("Must never place OCO on closed lot"))
        watcher = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=self.db)
        gate = StartupReconciliationGate(broker=broker, journal_db=self.db, watcher=watcher, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate

        # Pass 1: allocates 3+2=5 across both parents, closes both lifecycles, blocks for independent recheck
        ok1, reason = gate.run_startup_reconciliation()
        self.assertTrue(ok1, reason)

        # Simulate full process restart: reload JournalDB + ProtectionWatcher from SQLite WAL + JSON files
        db_restarted = JournalDB(self.db_path, account_id=ACCOUNT)
        watcher_restarted = ProtectionWatcher(broker=broker, path=self.watcher_path, dry_run=False, journal_db=db_restarted)
        gate_restarted = StartupReconciliationGate(broker=broker, journal_db=db_restarted, watcher=watcher_restarted, stop_manager=self.stop_mgr)
        broker.reconciliation_gate = gate_restarted

        # Canonical per-parent accounting MUST agree with lifecycle after restart
        a1 = db_restarted.get_position_accounting("BUY-PDSL-1")
        a2 = db_restarted.get_position_accounting("BUY-PDSL-2")
        self.assertEqual((a1["buy_filled_qty"], a1["sell_filled_qty"], a1["confirmed_open_qty"]), (3, 3, 0))
        self.assertEqual((a2["buy_filled_qty"], a2["sell_filled_qty"], a2["confirmed_open_qty"]), (2, 2, 0))
        self.assertEqual(db_restarted.get_position_lifecycle("BUY-PDSL-1")["position_state"], "POSITION_CLOSED")
        self.assertEqual(db_restarted.get_position_lifecycle("BUY-PDSL-2")["position_state"], "POSITION_CLOSED")

        # Pass 2: whole startup gate succeeds -> HEALTHY, zero new OCOs
        ok2, msg2 = gate_restarted.run_startup_reconciliation()
        self.assertTrue(ok2, msg2)
        self.assertEqual(gate_restarted.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate_restarted.is_accepting_entries)
        broker.place_forever_oco.assert_not_called()



    def test_real_stack_multi_lot_crash_after_partial_watcher_sync_recovers_after_rollover(self):
        """P0-1 real-stack: crash after BUY-1 syncs and before BUY-2 syncs recovers after restart + day rollover."""
        cr_ts1 = self._seed_pdsl_open_buy(buy_oid="BUY-CR-1", buy_ts=datetime.now(IST) - timedelta(days=2, hours=2), buy_qty=3, buy_price=400.0)
        cr_ts2 = self._seed_pdsl_open_buy(buy_oid="BUY-CR-2", buy_ts=datetime.now(IST) - timedelta(days=2, hours=1), buy_qty=2, buy_price=405.0)
        sell_ts = datetime.now(IST) - timedelta(days=1)
        sell_oid = "SELL-CR-ALL-5"
        with self.db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO fills (
                    broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity,
                    traded_at, transaction_type, security_id, exchange_segment, product_type,
                    parent_order_id, is_verified_trade
                ) VALUES (?, 'S-CR-5', 'PDSL', 420.0, 5, ?, 'SELL', '5264', 'NSE_EQ', 'CNC', ?, 1)
                """,
                (sell_oid, sell_ts.timestamp(), sell_oid),
            )
        broker_day1 = self._make_broker(
            trade_book=[],
            order_book=[],
            positions=[],
            holdings=[dict(securityId="5264", totalQty=0, dpQty=0)],
            hist_trades=[_trade(sell_oid, "S-CR-5", "SELL", 5, sell_ts, 420.0)],
        )
        watcher1 = ProtectionWatcher(broker=broker_day1, path=self.watcher_path, dry_run=False, journal_db=self.db)
        orig_reconcile = watcher1.reconcile_historical_order_fills
        calls = {"n": 0}
        def crash_on_second(**kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated crash after BUY-CR-1 synced and before BUY-CR-2 synced")
            return orig_reconcile(**kwargs)
        watcher1.reconcile_historical_order_fills = crash_on_second
        gate1 = StartupReconciliationGate(broker=broker_day1, journal_db=self.db, watcher=watcher1, stop_manager=self.stop_mgr)
        broker_day1.reconciliation_gate = gate1

        ok_crash, reason = gate1.run_startup_reconciliation()
        self.assertFalse(ok_crash)
        self.assertIn("RuntimeError", reason)
        self.assertEqual(self.db.get_position_lifecycle("BUY-CR-1")["position_state"], "POSITION_CLOSED")
        self.assertEqual(self.db.get_position_lifecycle("BUY-CR-2")["position_state"], "UNPROTECTED")

        # Process restarts after day rollover: empty tradebook, empty orderbook, empty positions, empty holdings
        db2 = JournalDB(self.db_path, account_id=ACCOUNT)
        broker_day2 = self._make_broker(
            trade_book=[], order_book=[], positions=[], holdings=[],
            hist_trades=[
                _trade("BUY-CR-1", "B-BUY-CR-1", "BUY", 3, cr_ts1 + timedelta(seconds=2), 400.0),
                _trade("BUY-CR-2", "B-BUY-CR-2", "BUY", 2, cr_ts2 + timedelta(seconds=2), 405.0),
                _trade(sell_oid, "S-CR-5", "SELL", 5, sell_ts, 420.0),
            ],
        )
        broker_day2.get_order_status = lambda oid: {
            "dhanClientId": ACCOUNT, "orderId": oid, "securityId": "5264",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL",
            "orderStatus": "TRADED", "quantity": 5, "filledQty": 5, "remainingQuantity": 0,
        } if oid == sell_oid else None
        watcher2 = ProtectionWatcher(broker=broker_day2, path=self.watcher_path, dry_run=False, journal_db=db2)
        gate2 = StartupReconciliationGate(broker=broker_day2, journal_db=db2, watcher=watcher2, stop_manager=self.stop_mgr)
        broker_day2.reconciliation_gate = gate2

        ok_rec1, reason = gate2.run_startup_reconciliation()
        self.assertTrue(ok_rec1, reason)
        self.assertEqual(db2.get_position_lifecycle("BUY-CR-1")["position_state"], "POSITION_CLOSED")
        self.assertEqual(db2.get_position_lifecycle("BUY-CR-2")["position_state"], "POSITION_CLOSED")
        # Canonical ledger interfaces agree 100%
        for oid, expected_qty in (("BUY-CR-1", 3), ("BUY-CR-2", 2)):
            acct = db2.get_position_accounting(oid)
            fills = db2.get_sell_fills_for_parent(oid)
            self.assertEqual(acct["sell_filled_qty"], expected_qty)
            self.assertEqual(acct["confirmed_open_qty"], 0)
            self.assertEqual(sum(int(f["traded_quantity"]) for f in fills), expected_qty)

        ok_rec2, msg_rec2 = gate2.run_startup_reconciliation()
        self.assertTrue(ok_rec2, msg_rec2)
        self.assertEqual(gate2.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate2.is_accepting_entries)

if __name__ == "__main__":
    unittest.main()
