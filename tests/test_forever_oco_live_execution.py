"""Regression tests for live Dhan Forever OCO execution & prior-day policy.

Uses a synthetic-ID, structurally equivalent Dhan snapshot for VEDPOWER (2026-11-23 09:15:18 IST):
- /v2/forever/all: single row with orderStatus='TRIGGERED', legName='TARGET_LEG' (cancelled sibling omitted)
- /v2/orders: spawned child SELL order (orderId='92026112357408', correlationId='NR', algoId='99032611201560')
- /v2/trades: fill under child orderId='92026112357408' @ 35.06
- /v2/positions: CNC netQty=-1 (same-day delivery sale)
- /v2/holdings: totalQty=1, availableQty=0 (unsettled T+0 Demat balance)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from src.engine.market import IST
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.settled_owner_read_policy import may_omit_protected_prior_day_buy_trade_read
from src.engine.unified_exit_reconciliation import (
    ReconciliationEvidenceError,
    _verify_protective_orders_safe,
    reconcile_verified_exits,
    verified_flat_delivery_day,
)
from src.journal.db import JournalDB


ACCOUNT_ID = "TEST_ACCOUNT_001"
PARENT_OID = "920261120279408"
FOREVER_OID = "99032611201560"
CHILD_SELL_OID = "92026112357408"
SEC_ID = "750518"


def _today_str(hm: str = "09:15:18") -> str:
    return f"{datetime.now(IST).date().isoformat()} {hm}"


def _live_vedpower_fixture(*, leg_name: str = "TARGET_LEG", traded_price: float = 35.06):
    today_exec = _today_str("09:15:18")
    today_create = _today_str("09:15:13")
    return {
        "trades": [
            {
                "dhanClientId": ACCOUNT_ID,
                "orderId": CHILD_SELL_OID,
                "exchangeOrderId": "TEST_EXCHANGE_ORDER_001",
                "exchangeTradeId": "TEST_TRADE_ID_001",
                "transactionType": "SELL",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderType": "LIMIT",
                "tradingSymbol": "VEDPOWER",
                "customSymbol": None,
                "securityId": SEC_ID,
                "tradedQuantity": 1,
                "tradedPrice": traded_price,
                "createTime": today_exec,
                "updateTime": today_exec,
                "exchangeTime": today_exec,
            }
        ],
        "orders": [
            {
                "dhanClientId": ACCOUNT_ID,
                "orderId": CHILD_SELL_OID,
                "exchangeOrderId": "TEST_EXCHANGE_ORDER_001",
                "correlationId": "NR",
                "orderStatus": "TRADED",
                "transactionType": "SELL",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderType": "LIMIT",
                "validity": "DAY",
                "tradingSymbol": "VEDPOWER",
                "securityId": SEC_ID,
                "quantity": 1,
                "price": 35.04 if leg_name == "TARGET_LEG" else 33.99,
                "triggerPrice": 0.0,
                "afterMarketOrder": True,
                "legName": "NA",
                "createTime": today_create,
                "updateTime": today_exec,
                "exchangeTime": today_exec,
                "omsErrorCode": "0",
                "omsErrorDescription": "TRADE CONFIRMED",
                "algoId": FOREVER_OID,
                "remainingQuantity": 0,
                "averageTradedPrice": traded_price,
                "filledQty": 1,
            }
        ],
        "positions": [
            {
                "dhanClientId": ACCOUNT_ID,
                "tradingSymbol": "VEDPOWER",
                "securityId": SEC_ID,
                "positionType": "SHORT",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "buyAvg": 0.0,
                "costPrice": traded_price,
                "buyQty": 0,
                "sellAvg": traded_price,
                "sellQty": 1,
                "netQty": -1,
                "dayBuyQty": 0,
                "daySellQty": 1,
                "daySellValue": traded_price,
            }
        ],
        "holdings": [
            {
                "exchange": "ALL",
                "tradingSymbol": "VEDPOWER",
                "securityId": SEC_ID,
                "isin": "INE694L01019",
                "totalQty": 1,
                "dpQty": 0,
                "t1Qty": 0,
                "availableQty": 0,
                "collateralQty": 0,
                "avgCostPrice": 34.69,
                "lastTradedPrice": 34.62,
            }
        ],
        "forever": [
            {
                "dhanClientId": ACCOUNT_ID,
                "orderId": FOREVER_OID,
                "orderStatus": "TRIGGERED",
                "transactionType": "SELL",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderType": "OCO",
                "tradingSymbol": "VEDPOWER",
                "securityId": SEC_ID,
                "quantity": 1,
                "price": 35.04 if leg_name == "TARGET_LEG" else 33.99,
                "triggerPrice": 0.0 if leg_name == "TARGET_LEG" else 34.34,
                "legName": leg_name,
                "createTime": today_create,
            }
        ],
    }


class _GateSpy:
    def __init__(self):
        self.halts = []

    def halt_new_entries(self, reason: str):
        self.halts.append(str(reason))


class _FakeCfg:
    dhan_client_id = ACCOUNT_ID


class _FakeBroker:
    def __init__(self, snap: dict, gate: _GateSpy):
        self.cfg = _FakeCfg()
        self.reconciliation_gate = gate
        self._snap = snap
        self.trades_for_order_calls = []

    def get_trade_book(self):
        return list(self._snap["trades"])

    def get_order_book(self):
        return list(self._snap["orders"])

    def get_positions(self):
        return list(self._snap["positions"])

    def get_holdings(self):
        return list(self._snap["holdings"])

    def get_forever_orders(self):
        return list(self._snap["forever"])

    def get_super_orders(self):
        return []

    def get_trades_for_order(self, oid: str):
        self.trades_for_order_calls.append(str(oid))
        return [t for t in self._snap["trades"] if str(t.get("orderId")) == str(oid)]


class ForeverOcoLiveExecutionTests(unittest.TestCase):
    def _seed_db(self, db_path: str) -> JournalDB:
        db = JournalDB(db_path, account_id=ACCOUNT_ID)
        prior_ts = (datetime.now(IST) - timedelta(days=3)).timestamp()
        with db._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO order_intents (
                    correlation_id, symbol, trading_symbol, security_id, exchange_segment, product_type,
                    order_type, price, quantity, protection_route, state, broker_order_id, account_id,
                    created_at, updated_at
                ) VALUES (?, 'VEDPOWER', 'VEDPOWER', ?, 'NSE_EQ', 'CNC', 'LIMIT', 34.69, 1, 'FOREVER_OCO', 'TRADED', ?, ?, ?, ?)
                """,
                ("BUY-VED-01", SEC_ID, PARENT_OID, ACCOUNT_ID, prior_ts, prior_ts),
            )
            conn.execute(
                """
                INSERT INTO fills (
                    broker_order_id, exchange_trade_id, symbol, traded_price,
                    traded_quantity, traded_at, transaction_type, security_id,
                    exchange_segment, product_type, parent_order_id, is_verified_trade
                ) VALUES (?, '500110001', 'VEDPOWER', 34.69, 1, ?, 'BUY', ?, 'NSE_EQ', 'CNC', ?, 1)
                """,
                (PARENT_OID, prior_ts, SEC_ID, PARENT_OID),
            )
            conn.execute(
                """
                INSERT INTO positions_lifecycle (
                    parent_order_id, account_id, symbol, trading_symbol, security_id,
                    exchange_segment, product_type, protection_route, position_state,
                    entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price,
                    target_price, sell_filled_qty, sell_vwap, confirmed_open_qty,
                    realized_pnl, updated_at
                ) VALUES (?, ?, 'VEDPOWER', 'VEDPOWER', ?, 'NSE_EQ', 'CNC', 'FOREVER_OCO',
                          'PROTECTED', 1, 1, 34.69, 1, 34.34, 35.04, 0, 0.0, 1, 0.0, ?)
                """,
                (PARENT_OID, ACCOUNT_ID, SEC_ID, prior_ts),
            )
            conn.execute(
                """
                INSERT INTO protection_orders (
                    parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit,
                    target_price, quantity, status, created_at, protection_route
                ) VALUES (?, ?, 'VEDPOWER', 34.34, 33.99, 35.04, 1, 'ACTIVE_CONFIRMED', ?, '')
                """,
                (PARENT_OID, FOREVER_OID, prior_ts),
            )
        return db

    def _make_watcher_record(self) -> dict:
        return {
            "order_id": PARENT_OID,
            "correlation_id": "BUY-VED-01",
            "symbol": "VEDPOWER",
            "security_id": SEC_ID,
            "segment": "NSE_EQ",
            "product_type": "CNC",
            "account_id": ACCOUNT_ID,
            "state": "TRADED",
            "position_state": "PROTECTED",
            "protection_route": "FOREVER_OCO",
            "qty": 1,
            "filled_qty": 1,
            "protected_qty": 1,
            "exit_filled_qty": 0,
            "buy_vwap": 34.69,
            "sl_order_id": None,
            "tp_order_id": None,
            "exit_order_id": None,
        }

    def test_1_and_2_live_vedpower_target_hit_closes_and_verifies_flat_delivery(self):
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / "journal.db"))
            snap = _live_vedpower_fixture(leg_name="TARGET_LEG", traded_price=35.06)
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / "pending.json"),
                journal_db=db,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()

            watcher.supervise_open_positions()

            rec = watcher.terminal_orders[PARENT_OID]
            self.assertEqual(rec["position_state"], "POSITION_CLOSED")
            self.assertEqual(rec["exit_filled_qty"], 1)
            self.assertAlmostEqual(rec["exit_vwap"], 35.06, places=2)
            self.assertAlmostEqual(rec["realized_pnl"], 0.37, places=2)
            self.assertEqual(rec["triggered_leg"], "TARGET_LEG")
            self.assertEqual(gate.halts, [], f"Unexpected gate halts: {gate.halts}")

            acct = db.get_position_accounting(PARENT_OID)
            self.assertEqual(acct["confirmed_open_qty"], 0)
            self.assertEqual(acct["sell_filled_qty"], 1)

            with db._get_conn() as conn:
                prot_status = conn.execute(
                    "SELECT status FROM protection_orders WHERE parent_order_id=?", (PARENT_OID,)
                ).fetchone()[0]
            self.assertEqual(prot_status, "POSITION_CLOSED_LOCAL")

            # Subsequent T+0 restart: verified_flat_delivery_day must pass with netQty=-1
            def _read_req(method_name, _label):
                return True, getattr(broker, method_name)(), "OK"

            flat_ok = verified_flat_delivery_day(
                snap["positions"][0],
                snap["positions"],
                snap["holdings"],
                snap["trades"],
                db,
                ACCOUNT_ID,
                _read_req,
            )
            self.assertTrue(flat_ok)

    def test_3_stop_loss_leg_hit_closes_cleanly(self):
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / "journal.db"))
            snap = _live_vedpower_fixture(leg_name="STOP_LOSS_LEG", traded_price=34.30)
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / "pending.json"),
                journal_db=db,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()

            watcher.supervise_open_positions()

            rec = watcher.terminal_orders[PARENT_OID]
            self.assertEqual(rec["position_state"], "POSITION_CLOSED")
            self.assertEqual(rec["triggered_leg"], "STOP_LOSS_LEG")
            self.assertAlmostEqual(rec["realized_pnl"], -0.39, places=2)
            self.assertEqual(gate.halts, [])

    def test_4_triggered_forever_with_pending_or_rejected_child_stays_fail_closed(self):
        for child_status in ("PENDING", "REJECTED"):
            with self.subTest(child_status=child_status), tempfile.TemporaryDirectory() as td:
                db = self._seed_db(str(Path(td) / "journal.db"))
                snap = _live_vedpower_fixture()
                snap["trades"] = []
                snap["positions"] = []
                snap["holdings"][0]["availableQty"] = 1
                snap["orders"][0]["orderStatus"] = child_status
                snap["orders"][0]["filledQty"] = 0
                snap["orders"][0]["remainingQuantity"] = 1
                gate = _GateSpy()
                broker = _FakeBroker(snap, gate)
                watcher = ProtectionWatcher(
                    broker=broker,
                    dry_run=False, path=str(Path(td) / "pending.json"),
                    journal_db=db,
                )
                watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()

                watcher.supervise_open_positions()
                self.assertNotEqual(watcher.terminal_orders[PARENT_OID]["position_state"], "POSITION_CLOSED")

                with self.assertRaises(ReconciliationEvidenceError):
                    _verify_protective_orders_safe(
                        forever=snap["forever"],
                        super_orders=[],
                        key=(SEC_ID, "NSE_EQ", "CNC"),
                        account=ACCOUNT_ID,
                        broker_evidence={},
                        orders=snap["orders"],
                    )

    def test_5_unexplained_negative_cnc_or_missing_holdings_still_halts(self):
        for mode in ("manual_unlinked_sell", "missing_holdings"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as td:
                db = self._seed_db(str(Path(td) / "journal.db"))
                snap = _live_vedpower_fixture()
                if mode == "manual_unlinked_sell":
                    snap["orders"][0]["algoId"] = ""
                    snap["forever"] = []
                else:
                    snap["holdings"] = []
                gate = _GateSpy()
                broker = _FakeBroker(snap, gate)
                watcher = ProtectionWatcher(
                    broker=broker,
                    dry_run=False, path=str(Path(td) / "pending.json"),
                    journal_db=db,
                )
                watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()

                watcher.supervise_open_positions()

                self.assertTrue(
                    any("NEGATIVE_CNC_POSITION_REVIEW" in h or "SETTLED_HOLDINGS" in h for h in gate.halts),
                    f"Expected fail-closed halt for {mode}, got {gate.halts}",
                )
                if mode == "manual_unlinked_sell":
                    self.assertNotEqual(watcher.terminal_orders[PARENT_OID]["position_state"], "POSITION_CLOSED")

    def test_6_prior_day_open_protected_lot_omits_old_buy_trade_read_via_db_forever_id(self):
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / "journal.db"))
            now_ts = datetime.now(IST).timestamp()
            rec = self._make_watcher_record()
            rec["qty"] = 0  # exercise qty -> filled_qty fallback
            active_forever = [
                {
                    "dhanClientId": ACCOUNT_ID,
                    "orderId": FOREVER_OID,
                    "orderStatus": "ACTIVE",
                    "transactionType": "SELL",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "orderType": "OCO",
                    "tradingSymbol": "VEDPOWER",
                    "securityId": SEC_ID,
                    "quantity": 1,
                    "price": 35.04,
                    "triggerPrice": 35.04,
                    "legName": "TARGET_LEG",
                },
                {
                    "dhanClientId": ACCOUNT_ID,
                    "orderId": FOREVER_OID,
                    "orderStatus": "ACTIVE",
                    "transactionType": "SELL",
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "orderType": "OCO",
                    "tradingSymbol": "VEDPOWER",
                    "securityId": SEC_ID,
                    "quantity": 1,
                    "price": 33.99,
                    "triggerPrice": 34.34,
                    "legName": "STOP_LOSS_LEG",
                },
            ]
            holdings = [
                {
                    "dhanClientId": ACCOUNT_ID,
                    "tradingSymbol": "VEDPOWER",
                    "securityId": SEC_ID,
                    "isin": "INE694L01019",
                    "totalQty": 1,
                    "dpQty": 1,
                    "t1Qty": 0,
                    "availableQty": 1,
                }
            ]
            rejection = {}
            ok = may_omit_protected_prior_day_buy_trade_read(
                record=rec,
                all_open_records=[rec],
                leg_order_ids={PARENT_OID},  # forever_order_id absent from leg_order_ids
                journal_db=db,
                gate_books={"protected_verified": {PARENT_OID: (ACCOUNT_ID, SEC_ID, 1)}, "errors": {}},
                current_trades=[],
                current_positions=[],
                current_holdings=holdings,
                current_forever=active_forever,
                account_id=ACCOUNT_ID,
                now=now_ts,
                rejection=rejection,
            )
            self.assertTrue(ok, f"Expected policy pass via DB forever_order_id, got {rejection}")
            self.assertEqual(rejection.get("reason"), "VERIFIED")

    def test_7_reconcile_verified_exits_accepts_live_vedpower_forever_oco_fixture(self):
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / "journal.db"))
            snap = _live_vedpower_fixture()
            sell_ts = datetime.now(IST).timestamp()
            with db._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price,
                        traded_quantity, traded_at, transaction_type, security_id,
                        exchange_segment, product_type, parent_order_id, is_verified_trade
                    ) VALUES (?, 'TEST_TRADE_ID_001', 'VEDPOWER', 35.06, 1, ?, 'SELL', ?, 'NSE_EQ', 'CNC', NULL, 1)
                    """,
                    (CHILD_SELL_OID, sell_ts, SEC_ID),
                )
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / "pending.json"),
                journal_db=db,
            )

            def _read_req(method_name, _label):
                return True, getattr(broker, method_name)(), "OK"

            ok, msg = reconcile_verified_exits(
                broker=broker,
                db=db,
                watcher=watcher,
                trade_book=snap["trades"],
                positions=snap["positions"],
                read_required=_read_req,
                initial_holdings=snap["holdings"],
            )
            self.assertTrue(ok, f'reconcile_verified_exits failed: {msg}')
            acct = db.get_position_accounting(PARENT_OID)
            self.assertEqual(acct["confirmed_open_qty"], 0)
            self.assertEqual(acct["sell_filled_qty"], 1)


    def test_8_preexisting_self_parented_sell_fill_in_sqlite_reconciles_and_notifies_once(self):
        class _ReporterSpy:
            def __init__(self):
                self.events = []
            def process_exit_event(self, rec, event_type, **kwargs):
                self.events.append((rec.get('order_id'), event_type, kwargs))

        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            sell_ts = datetime.now(IST).timestamp()
            with db._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price,
                        traded_quantity, traded_at, transaction_type, security_id,
                        exchange_segment, product_type, parent_order_id, is_verified_trade
                    ) VALUES (?, 'TEST_TRADE_ID_001', 'VEDPOWER', 35.06, 1, ?, 'SELL', ?, 'NSE_EQ', 'CNC', ?, 1)
                    """,
                    (CHILD_SELL_OID, sell_ts, SEC_ID, CHILD_SELL_OID),
                )
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / 'pending.json'),
                journal_db=db,
            )
            reporter = _ReporterSpy()
            watcher.execution_reporter = reporter
            rec = self._make_watcher_record()
            rec['position_state'] = 'EXIT_TRIGGERED'
            rec['triggered_leg'] = 'TARGET_LEG'
            watcher.terminal_orders[PARENT_OID] = rec

            watcher.supervise_open_positions()

            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')
            self.assertEqual(gate.halts, [], f'Unexpected halts: {gate.halts}')
            with db._get_conn() as conn:
                rows = conn.execute(
                    "SELECT parent_order_id FROM fills WHERE exchange_trade_id='TEST_TRADE_ID_001'"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], PARENT_OID)
            closed_events = [e for e in reporter.events if e[1] == 'POSITION_CLOSED']
            self.assertEqual(len(closed_events), 1)
            self.assertAlmostEqual(closed_events[0][2]['realized_pnl'], 0.37, places=2)

    def test_9_unified_reconciliation_emits_position_closed_notification_on_recovery(self):
        class _ReporterSpy:
            def __init__(self):
                self.events = []
            def process_exit_event(self, rec, event_type, **kwargs):
                self.events.append((rec.get('order_id'), event_type, kwargs))

        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            sell_ts = datetime.now(IST).timestamp()
            with db._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price,
                        traded_quantity, traded_at, transaction_type, security_id,
                        exchange_segment, product_type, parent_order_id, is_verified_trade
                    ) VALUES (?, 'TEST_TRADE_ID_001', 'VEDPOWER', 35.06, 1, ?, 'SELL', ?, 'NSE_EQ', 'CNC', ?, 1)
                    """,
                    (CHILD_SELL_OID, sell_ts, SEC_ID, CHILD_SELL_OID),
                )
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / 'pending.json'),
                journal_db=db,
            )
            reporter = _ReporterSpy()
            watcher.execution_reporter = reporter
            rec = self._make_watcher_record()
            rec['position_state'] = 'EXIT_TRIGGERED'
            rec['triggered_leg'] = 'TARGET_LEG'
            watcher.terminal_orders[PARENT_OID] = rec

            def _read_req(method_name, _label):
                return True, getattr(broker, method_name)(), 'OK'

            ok, msg = reconcile_verified_exits(
                broker=broker,
                db=db,
                watcher=watcher,
                trade_book=snap['trades'],
                positions=snap['positions'],
                read_required=_read_req,
                initial_holdings=snap['holdings'],
            )
            self.assertTrue(ok, f'reconcile_verified_exits failed: {msg}')
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')
            closed_events = [e for e in reporter.events if e[1] == 'POSITION_CLOSED']
            self.assertEqual(len(closed_events), 1)
            self.assertAlmostEqual(closed_events[0][2]['realized_pnl'], 0.37, places=2)
            with db._get_conn() as conn:
                prot_status = conn.execute(
                    'SELECT status FROM protection_orders WHERE parent_order_id=?', (PARENT_OID,)
                ).fetchone()[0]
            self.assertEqual(prot_status, 'POSITION_CLOSED_LOCAL')

    def test_10_contradictory_oco_child_payloads_rejected_by_both_reconciler_and_watcher(self):
        """Independent audit scenarios 5-8:
        - child filledQty=0 despite TRADED
        - child requested quantity differs from OCO quantity
        - child remainingQuantity=1 despite TRADED
        - extra TRADED child without its own verified fill
        Must be rejected by BOTH _verify_protective_orders_safe AND ProtectionWatcher.
        """
        cases = [
            ('child_filled_qty_zero', lambda snap: snap['orders'][0].update({'filledQty': 0})),
            ('child_req_qty_differs', lambda snap: snap['orders'][0].update({'quantity': 2})),
            ('child_remaining_qty_nonzero', lambda snap: snap['orders'][0].update({'remainingQuantity': 1})),
            ('extra_traded_child_without_fill', lambda snap: snap['orders'].append(
                dict(snap['orders'][0], orderId='32126112357499', exchangeOrderId='1300000000603999')
            )),
        ]
        for name, mutate in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as td:
                db = self._seed_db(str(Path(td) / 'journal.db'))
                snap = _live_vedpower_fixture()
                mutate(snap)

                # 1) Reconciler helper must reject
                evidence = {
                    'TEST_TRADE_ID_001': {
                        'oid': CHILD_SELL_OID,
                        'eid': 'TEST_TRADE_ID_001',
                        'side': 'SELL',
                        'qty': 1,
                        'price': 35.06,
                        'when': datetime.now(IST).timestamp(),
                    }
                }
                with self.assertRaises(ReconciliationEvidenceError, msg=f'Reconciler accepted {name}'):
                    _verify_protective_orders_safe(
                        forever=snap['forever'],
                        super_orders=[],
                        key=(SEC_ID, 'NSE_EQ', 'CNC'),
                        account=ACCOUNT_ID,
                        broker_evidence=evidence,
                        orders=snap['orders'],
                    )

                # 2) Watcher must also reject (must NOT close position and must halt gate)
                gate = _GateSpy()
                broker = _FakeBroker(snap, gate)
                watcher = ProtectionWatcher(
                    broker=broker,
                    dry_run=False, path=str(Path(td) / 'pending.json'),
                    journal_db=db,
                )
                watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
                watcher.supervise_open_positions()
                self.assertNotEqual(
                    watcher.terminal_orders[PARENT_OID]['position_state'],
                    'POSITION_CLOSED',
                    f'Watcher wrongly closed position on {name}',
                )
                self.assertTrue(len(gate.halts) > 0, f'Watcher failed to halt gate on {name}')

    def test_11_full_integration_vedpower_yesbank_preserved_restarts_and_sqlite_write_failure(self):
        """Full offline integration:
        1) VEDPOWER (self-parented SELL TEST_TRADE_ID_001, EXIT_TRIGGERED) + BOTH YESBANK-EQ
           UNPROTECTED_MANUAL_RISK lots coexist -> VEDPOWER closes with exactly 1 notification,
           repeated restarts emit 0 duplicate notifications and keep verified_flat_delivery_day=True,
           and both YESBANK lots remain untouched.
        2) Simulated SQLite record_fill write failure -> gate stays fail-closed (NEGATIVE_CNC_POSITION_REVIEW
           NOT waived, position NOT closed, 0 POSITION_CLOSED notifications).
        """
        class _ReporterSpy:
            def __init__(self):
                self.events = []
            def process_exit_event(self, rec, event_type, **kwargs):
                self.events.append((rec.get('order_id'), event_type, kwargs))

        # Part A: VEDPOWER + both YESBANK lots + repeated restart
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            prior_ts = (datetime.now(IST) - timedelta(days=16)).timestamp()
            sell_ts = datetime.now(IST).timestamp()
            with db._get_conn() as conn:
                # Seed pre-existing self-parented VEDPOWER SELL fill
                conn.execute(
                    """
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price,
                        traded_quantity, traded_at, transaction_type, security_id,
                        exchange_segment, product_type, parent_order_id, is_verified_trade
                    ) VALUES (?, 'TEST_TRADE_ID_001', 'VEDPOWER', 35.06, 1, ?, 'SELL', ?, 'NSE_EQ', 'CNC', ?, 1)
                    """,
                    (CHILD_SELL_OID, sell_ts, SEC_ID, CHILD_SELL_OID),
                )
                # Seed both YESBANK-EQ UNPROTECTED_MANUAL_RISK lots
                for yb_oid, yb_etid in (('920261102657708', 'YB-1'), ('93026110439808', 'YB-2')):
                    conn.execute(
                        """
                        INSERT INTO positions_lifecycle (
                            parent_order_id, account_id, symbol, trading_symbol, security_id,
                            exchange_segment, product_type, protection_route, position_state,
                            entry_qty, buy_filled_qty, buy_vwap, protected_qty, sl_price,
                            target_price, sell_filled_qty, sell_vwap, confirmed_open_qty,
                            realized_pnl, updated_at
                        ) VALUES (?, ?, 'YESBANK-EQ', 'YESBANK-EQ', '11915', 'NSE_EQ', 'CNC',
                                  'FOREVER_OCO', 'UNPROTECTED_MANUAL_RISK', 1, 1, 20.0, 0, 0.0,
                                  0.0, 0, 0.0, 1, 0.0, ?)
                        """,
                        (yb_oid, ACCOUNT_ID, prior_ts),
                    )
            snap = _live_vedpower_fixture()
            snap['holdings'].append({
                'dhanClientId': ACCOUNT_ID,
                'tradingSymbol': 'YESBANK-EQ',
                'securityId': '11915',
                'isin': 'INE528G01035',
                'totalQty': 2,
                'dpQty': 2,
                't1Qty': 0,
                'availableQty': 2,
            })
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / 'pending.json'),
                journal_db=db,
            )
            reporter = _ReporterSpy()
            watcher.execution_reporter = reporter
            rec = self._make_watcher_record()
            rec['position_state'] = 'EXIT_TRIGGERED'
            rec['triggered_leg'] = 'TARGET_LEG'
            watcher.terminal_orders[PARENT_OID] = rec

            # Pass 1: closes VEDPOWER and emits 1 notification
            watcher.supervise_open_positions()
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')
            self.assertEqual(len([e for e in reporter.events if e[1] == 'POSITION_CLOSED']), 1)
            self.assertEqual(gate.halts, [])

            # Pass 2 (simulated restart): idempotent, 0 duplicate notifications, verified_flat_delivery_day=True
            watcher.supervise_open_positions()
            self.assertEqual(len([e for e in reporter.events if e[1] == 'POSITION_CLOSED']), 1)
            flat_ok = verified_flat_delivery_day(
                snap['positions'][0],
                snap['positions'],
                snap['holdings'],
                snap['trades'],
                db,
                ACCOUNT_ID,
                lambda m, _l: (True, getattr(broker, m)(), 'OK'),
            )
            self.assertTrue(flat_ok)

            # Both YESBANK-EQ lots remain completely untouched
            for yb_oid in ('920261102657708', '93026110439808'):
                yb = db.get_position_lifecycle(yb_oid)
                self.assertEqual(yb['position_state'], 'UNPROTECTED_MANUAL_RISK')
                self.assertEqual(yb['confirmed_open_qty'], 1)

        # Part B: SQLite write failure during record_fill -> must stay fail-closed
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker,
                dry_run=False, path=str(Path(td) / 'pending.json'),
                journal_db=db,
            )
            reporter = _ReporterSpy()
            watcher.execution_reporter = reporter
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()

            def _fail_record_fill(*args, **kwargs):
                raise RuntimeError('simulated sqlite disk I/O error')

            db.record_fill = _fail_record_fill
            watcher.supervise_open_positions()
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'RECONCILIATION_REQUIRED')
            self.assertTrue(watcher.terminal_orders[PARENT_OID].get('persistence_uncertain'))
            self.assertEqual([e for e in reporter.events if e[1] == 'POSITION_CLOSED'], [])
            self.assertTrue(any('Journal persistence failure' in h for h in gate.halts))
            self.assertTrue(any('NEGATIVE_CNC_POSITION_REVIEW' in h for h in gate.halts))

    def test_12_v4_audit_hardening_missing_account_traded_oco_cross_contract_and_unjournaled_sell(self):
        """Covers all 4 independent v3 audit findings:
        1) Missing child dhanClientId rejected.
        2) TRADED Forever OCO with contradictory/extra child rejected via shared validator.
        3) JournalDB.record_fill refuses cross-contract reparenting and pre-existing exit_allocations.
        4) Signed CNC waiver rejects when a same-security tradebook SELL is not durably journaled.
        """
        # (1) Missing child account ID
        snap = _live_vedpower_fixture()
        snap['orders'][0].pop('dhanClientId', None)
        ev = {'TEST_TRADE_ID_001': {'oid': CHILD_SELL_OID, 'side': 'SELL', 'qty': 1, 'price': 35.06}}
        with self.assertRaises(ReconciliationEvidenceError):
            _verify_protective_orders_safe(
                forever=snap['forever'], super_orders=[], key=(SEC_ID, 'NSE_EQ', 'CNC'),
                account=ACCOUNT_ID, broker_evidence=ev, orders=snap['orders'],
            )

        # (2) TRADED Forever OCO with contradictory or extra unproven child
        snap2 = _live_vedpower_fixture()
        f_traded = dict(snap2['forever'][0], orderStatus='TRADED')
        f_cancel = dict(snap2['forever'][0], orderStatus='CANCELLED', legName='STOP_LOSS_LEG')
        parent_ev = {'TEST_TRADE_ID_001': {'oid': FOREVER_OID, 'side': 'SELL', 'qty': 1, 'price': 35.06}}
        with self.assertRaises(ReconciliationEvidenceError):
            _verify_protective_orders_safe(
                forever=[f_traded, f_cancel], super_orders=[], key=(SEC_ID, 'NSE_EQ', 'CNC'),
                account=ACCOUNT_ID, broker_evidence=parent_ev,
                orders=[dict(snap2['orders'][0], filledQty=0)],
            )
        with self.assertRaises(ReconciliationEvidenceError):
            _verify_protective_orders_safe(
                forever=[f_traded, f_cancel], super_orders=[], key=(SEC_ID, 'NSE_EQ', 'CNC'),
                account=ACCOUNT_ID, broker_evidence=parent_ev,
                orders=[snap2['orders'][0], dict(snap2['orders'][0], orderId='EXTRA-CHILD')],
            )

        # (3) JournalDB.record_fill cross-contract & double-allocation guards
        with tempfile.TemporaryDirectory() as td:
            db = JournalDB(str(Path(td) / 'j.db'), account_id=ACCOUNT_ID)
            db.record_fill(
                broker_order_id='CHILD-SELL', exchange_trade_id='E-CROSS', symbol='OTHER',
                traded_price=35.06, traded_quantity=1, transaction_type='SELL',
                security_id='OTHER-SEC', exchange_segment='NSE_EQ', product_type='CNC',
                parent_order_id='CHILD-SELL', is_verified_trade=True,
            )
            ok_cross = db.record_fill(
                broker_order_id='CHILD-SELL', exchange_trade_id='E-CROSS', symbol='VEDPOWER',
                traded_price=35.06, traded_quantity=1, transaction_type='SELL',
                security_id=SEC_ID, exchange_segment='NSE_EQ', product_type='CNC',
                parent_order_id=PARENT_OID, is_verified_trade=True,
            )
            self.assertFalse(ok_cross)
            self.assertEqual(db.get_fill_by_exchange_trade_id('E-CROSS')['parent_order_id'], 'CHILD-SELL')

            db.record_fill(
                broker_order_id='CHILD-SELL', exchange_trade_id='E-ALLOC', symbol='VEDPOWER',
                traded_price=35.06, traded_quantity=1, transaction_type='SELL',
                security_id=SEC_ID, exchange_segment='NSE_EQ', product_type='CNC',
                parent_order_id='CHILD-SELL', is_verified_trade=True,
            )
            with db._get_conn() as conn:
                conn.execute(
                    'INSERT INTO exit_allocations (parent_order_id, sell_order_id, exchange_trade_id, allocated_qty, traded_price, traded_at) VALUES (?, ?, ?, ?, ?, ?)',
                    ('OTHER-BUY', 'CHILD-SELL', 'E-ALLOC', 1, 35.06, datetime.now(IST).timestamp()),
                )
            ok_alloc = db.record_fill(
                broker_order_id='CHILD-SELL', exchange_trade_id='E-ALLOC', symbol='VEDPOWER',
                traded_price=35.06, traded_quantity=1, transaction_type='SELL',
                security_id=SEC_ID, exchange_segment='NSE_EQ', product_type='CNC',
                parent_order_id=PARENT_OID, is_verified_trade=True,
            )
            self.assertFalse(ok_alloc)

        # (4) Signed CNC waiver rejects when a second same-security SELL is unjournaled
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap4 = _live_vedpower_fixture()
            snap4['positions'][0]['netQty'] = -2
            snap4['holdings'][0]['totalQty'] = 2
            snap4['trades'].append(dict(snap4['trades'][0], orderId='UNJOURNALED-SELL-2', exchangeTradeId='UNVERIFIED-EID'))
            gate = _GateSpy()
            broker = _FakeBroker(snap4, gate)
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'), journal_db=db,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
            watcher.terminal_orders['OTHER-PARENT-B'] = dict(
                self._make_watcher_record(), order_id='OTHER-PARENT-B', verified_leg_order_ids=['UNJOURNALED-SELL-2'],
                position_state='POSITION_CLOSED', exit_filled_qty=1,
            )
            watcher.supervise_open_positions()
            self.assertTrue(any('NEGATIVE_CNC_POSITION_REVIEW' in h for h in gate.halts), f'Expected halt, got {gate.halts}')

    def test_13_c5_forever_group_id_never_queried_via_get_trades_for_order(self):
        """C5: Even if get_trades_for_order(FOREVER_OID) returns BrokerReadFailure (429/error),
        watcher must NEVER call get_trades_for_order on the Forever parent group ID;
        child 92026112357408 is verified cleanly and VEDPOWER closes with 0 halts.
        """
        from src.broker.dhan_client import BrokerReadFailure
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            orig_get = broker.get_trades_for_order
            def _fail_on_forever(oid):
                broker.trades_for_order_calls.append(str(oid))
                if str(oid) == FOREVER_OID:
                    return BrokerReadFailure('429/unavailable on Forever parent ID')
                return [t for t in snap['trades'] if str(t.get('orderId')) == str(oid)]
            broker.get_trades_for_order = _fail_on_forever
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'), journal_db=db,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
            watcher.supervise_open_positions()
            self.assertNotIn(FOREVER_OID, broker.trades_for_order_calls)
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')
            self.assertEqual(gate.halts, [], f'Unexpected halts: {gate.halts}')

    def test_14_c1_c6_c7_c8_and_fixed_nov23_equivalent_replay(self):
        """Covers C1 (postcondition price mismatch), C6 (strict parser rejects float/missing acct),
        C7 (notification failure retry after SQLite commit), C8 (multi-owner collision halt),
        and C9 (fixed 2026-11-23 09:15:18 IST fixed timestamp replay).
        """
        from src.engine.unified_exit_reconciliation import parse_verified_broker_trade

        # C6: strict shared trade parser rejects float quantity and missing account
        base_tr = _live_vedpower_fixture()['trades'][0]
        parsed_ok = parse_verified_broker_trade(
            base_tr, expected_account_id=ACCOUNT_ID, expected_security_id=SEC_ID,
            expected_segment='NSE_EQ', expected_product_type='CNC', expected_side='SELL',
        )
        self.assertEqual(parsed_ok['qty'], 1)
        with self.assertRaises(ReconciliationEvidenceError):
            parse_verified_broker_trade(
                dict(base_tr, tradedQuantity=1.5), expected_account_id=ACCOUNT_ID,
                expected_security_id=SEC_ID, expected_segment='NSE_EQ', expected_product_type='CNC',
            )
        tr_no_acct = dict(base_tr)
        tr_no_acct.pop('dhanClientId', None)
        with self.assertRaises(ReconciliationEvidenceError):
            parse_verified_broker_trade(
                tr_no_acct, expected_account_id=ACCOUNT_ID,
                expected_security_id=SEC_ID, expected_segment='NSE_EQ', expected_product_type='CNC',
            )

        # C1: pre-existing fill with contradictory price fails persistence cleanly
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            db.record_fill(
                broker_order_id=CHILD_SELL_OID, exchange_trade_id='TEST_TRADE_ID_001', symbol='VEDPOWER',
                traded_price=35.06, traded_quantity=1, traded_at=datetime.now(IST).timestamp(),
                transaction_type='SELL', security_id=SEC_ID, exchange_segment='NSE_EQ',
                product_type='CNC', parent_order_id=PARENT_OID, is_verified_trade=True,
            )
            snap = _live_vedpower_fixture(traded_price=39.99)
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'), journal_db=db,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
            watcher.supervise_open_positions()
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'RECONCILIATION_REQUIRED')
            self.assertTrue(watcher.terminal_orders[PARENT_OID].get('persistence_uncertain'))
            self.assertTrue(any('postcondition mismatch' in h for h in gate.halts))

        # C7: Telegram notification failure after SQLite commit retries on next pass with 0 duplicates
        class _FlakyReporter:
            def __init__(self):
                self.fail_first = True
                self.delivered = []
            def process_exit_event(self, rec, event_type, **kwargs):
                if self.fail_first:
                    self.fail_first = False
                    raise RuntimeError('transient telegram outbox error')
                self.delivered.append((rec.get('order_id'), event_type))

        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'), journal_db=db,
            )
            flaky = _FlakyReporter()
            watcher.execution_reporter = flaky
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
            # Pass 1: closes in SQLite, reporter throws -> position_closed_notified stays False
            watcher.reconcile_historical_order_fills(
                order_id=PARENT_OID, correlation_id='BUY-VED-01', symbol='VEDPOWER',
                security_id=SEC_ID, segment='NSE_EQ', product_type='CNC',
                requested_qty=1, filled_qty=1, avg_fill_price=34.69,
                verified_closed_qty=1, verified_sell_vwap=35.06, verified_realized_pnl=0.37,
            )
            self.assertFalse(watcher.terminal_orders[PARENT_OID].get('position_closed_notified', False))
            self.assertEqual(len(flaky.delivered), 0)
            # Pass 2: replay retries notification and marks position_closed_notified=True
            watcher.reconcile_historical_order_fills(
                order_id=PARENT_OID, correlation_id='BUY-VED-01', symbol='VEDPOWER',
                security_id=SEC_ID, segment='NSE_EQ', product_type='CNC',
                requested_qty=1, filled_qty=1, avg_fill_price=34.69,
                verified_closed_qty=1, verified_sell_vwap=35.06, verified_realized_pnl=0.37,
            )
            self.assertTrue(watcher.terminal_orders[PARENT_OID].get('position_closed_notified', False))
            self.assertEqual(len(flaky.delivered), 1)
            # Pass 3: already acknowledged -> 0 duplicates
            watcher.reconcile_historical_order_fills(
                order_id=PARENT_OID, correlation_id='BUY-VED-01', symbol='VEDPOWER',
                security_id=SEC_ID, segment='NSE_EQ', product_type='CNC',
                requested_qty=1, filled_qty=1, avg_fill_price=34.69,
                verified_closed_qty=1, verified_sell_vwap=35.06, verified_realized_pnl=0.37,
            )
            self.assertEqual(len(flaky.delivered), 1)

        # C8: two distinct parents claiming the same Forever OCO ID halts fail-closed
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap = _live_vedpower_fixture()
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'), journal_db=db,
            )
            r1 = self._make_watcher_record()
            r1['forever_order_id'] = FOREVER_OID
            r2 = dict(r1, order_id='999999999999999', correlation_id='BUY-VED-02')
            watcher.terminal_orders[PARENT_OID] = r1
            watcher.terminal_orders['999999999999999'] = r2
            watcher.supervise_open_positions()
            self.assertTrue(any('conflicting bot owner claim' in h for h in gate.halts), f'Expected collision halt, got {gate.halts}')
            self.assertNotEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')

        # C9: Fixed 2026-11-23 09:15:18 IST fixed timestamp replay
        fixed_ts = datetime(2026, 11, 23, 9, 55, 0, tzinfo=IST).timestamp()
        with tempfile.TemporaryDirectory() as td:
            db = self._seed_db(str(Path(td) / 'journal.db'))
            snap = _live_vedpower_fixture()
            snap['trades'][0]['exchangeTime'] = '2026-11-23 09:15:18'
            snap['trades'][0]['createTime'] = '2026-11-23 09:15:18'
            snap['trades'][0]['updateTime'] = '2026-11-23 09:15:18'
            snap['orders'][0]['createTime'] = '2026-11-23 09:15:13'
            snap['orders'][0]['exchangeTime'] = '2026-11-23 09:15:18'
            snap['forever'][0]['createTime'] = '2026-11-23 09:15:13'
            gate = _GateSpy()
            broker = _FakeBroker(snap, gate)
            watcher = ProtectionWatcher(
                broker=broker, dry_run=False, path=str(Path(td) / 'pending.json'),
                journal_db=db, clock=lambda: fixed_ts,
            )
            watcher.terminal_orders[PARENT_OID] = self._make_watcher_record()
            watcher.supervise_open_positions(current_time=fixed_ts)
            self.assertEqual(watcher.terminal_orders[PARENT_OID]['position_state'], 'POSITION_CLOSED')
            self.assertAlmostEqual(watcher.terminal_orders[PARENT_OID]['realized_pnl'], 0.37, places=2)
            self.assertEqual(gate.halts, [])

if __name__ == "__main__":
    unittest.main()
