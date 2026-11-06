"""Independent fail-first merge gate & PR #49 Final Scope Freeze acceptance suite.
Offline only: uses mocked Dhan responses and a temporary SQLite journal.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from src.broker.dhan_client import DhanClient, BrokerReadList, BrokerReadFailure, BrokerReadStatus
from src.config import AppConfig
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.main import TradingBotPipeline
from src.models import PositionLifecycleState, QuoteResult, TradingMode
from src.notifier.execution_updates import OrderStatusReporter
from src.parsers.fno_parser import parse_fno_regex_detailed
from tests import test_auditbot_v14_release_blockers as fixture


class AdditionalMergeGate(unittest.TestCase):
    def test_db_write_failure_must_not_confirm_position_closed(self):
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'EXIT-DURABILITY-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 10,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]
            case.db.record_fill = MagicMock(side_effect=OSError('injected SQLite disk write failure'))
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            accounting = case.db.get_position_accounting('ORDER-A')
            self.assertEqual(accounting['sell_filled_qty'], 0, 'Injected failure should leave no durable SELL')
            self.assertNotEqual(
                rec.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value,
                'Do not confirm POSITION_CLOSED when the broker SELL fill cannot be durably journaled',
            )
            gate.halt_new_entries.assert_called()
        finally:
            case.tearDown()

    def test_p0_01_sqlite_operational_error_and_idempotent_restart_recovery(self):
        """P0-01: SQLite OperationalError/disk-full prevents false closure; recovery + restart replays fill idempotently."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            meta = {"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 990.0}
            case.watcher.terminal_orders['ORDER-A']['notification_meta'] = meta
            case.watcher.execution_reporter = OrderStatusReporter(case.db, case.broker)
            gate = MagicMock()
            case.broker.reconciliation_gate = gate

            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'EXIT-SQLITE-LOCK-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 10,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]

            real_record_fill = case.db.record_fill
            case.db.record_fill = MagicMock(side_effect=sqlite3.OperationalError('database or disk is full'))
            case.watcher.check_once()

            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)
            self.assertTrue(rec.get('reconciliation_required'))
            self.assertTrue(rec.get('persistence_uncertain'))
            self.assertEqual(int(rec.get('exit_filled_qty') or 0), 0)
            self.assertEqual(int(rec.get('broker_observed_exit_filled_qty') or 0), 10)
            self.assertTrue(rec.get('broker_exit_already_executed'))
            gate.halt_new_entries.assert_called()

            outbox_1 = [json.loads(r['payload'])['text'] for r in case.db.fetch_pending_outbox(limit=20)]
            self.assertFalse(any('Status: Position Closed' in c for c in outbox_1))
            self.assertTrue(any('Reconciliation Required' in c for c in outbox_1))

            # Restore SQLite write capability and simulate watcher restart
            case.db.record_fill = real_record_fill
            restarted = ProtectionWatcher(
                broker=case.broker,
                path=case.watcher.path,
                dry_run=False,
                journal_db=case.db,
                clock=lambda: 1020.0,
            )
            restarted.execution_reporter = OrderStatusReporter(case.db, case.broker)
            restarted.check_once()

            rec_after = restarted.terminal_orders['ORDER-A']
            acct_after = case.db.get_position_accounting('ORDER-A')
            self.assertEqual(acct_after['sell_filled_qty'], 10)
            self.assertEqual(acct_after['confirmed_open_qty'], 0)
            self.assertEqual(rec_after.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value)
            self.assertEqual(rec_after.get('exit_filled_qty'), 10)
            self.assertEqual(rec_after.get('realized_pnl'), 30.0)
            self.assertFalse(rec_after.get('reconciliation_required'))
            self.assertFalse(rec_after.get('persistence_uncertain'))

            outbox_2 = [json.loads(r['payload'])['text'] for r in case.db.fetch_pending_outbox(limit=20)]
            closed_cards = [c for c in outbox_2 if 'Status: Position Closed (Target Hit)' in c]
            self.assertEqual(len(closed_cards), 1)
        finally:
            case.tearDown()

    def test_p0_01_partial_fill_commit_then_second_fill_db_error_and_lifecycle_upsert_failure(self):
        """P0-01: Partial fill 1 commits, fill 2 fails DB write -> preserves durable partial accounting; lifecycle upsert failure also blocks closure."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='TRADED', target='PART_TRADED', stop='PENDING')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'PART-FILL-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 4,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 6,
            }]
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.EXIT_PART_FILLED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 4)

            # Second fill arrives (6 shares), but record_fill fails on PART-FILL-2
            real_record_fill = case.db.record_fill
            def fail_on_second_fill(*args, **kwargs):
                if kwargs.get('exchange_trade_id') == 'PART-FILL-2':
                    raise OSError('disk full on second fill')
                return real_record_fill(*args, **kwargs)

            case.db.record_fill = MagicMock(side_effect=fail_on_second_fill)
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value.append({
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'PART-FILL-2',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 6,
                'tradedPrice': 103.0,
            })
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 4)
            self.assertEqual(rec.get('confirmed_position_open_qty'), 6)
            self.assertEqual(rec.get('broker_observed_exit_filled_qty'), 10)
            gate.halt_new_entries.assert_called()

            # Restore record_fill, but inject failure on upsert_position_lifecycle -> still must not confirm POSITION_CLOSED
            case.db.record_fill = real_record_fill
            real_upsert = case.db.upsert_position_lifecycle
            case.db.upsert_position_lifecycle = MagicMock(side_effect=OSError('lifecycle table locked'))
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)

            # Restore upsert_position_lifecycle -> converges to POSITION_CLOSED without double counting
            case.db.upsert_position_lifecycle = real_upsert
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 10)
            self.assertEqual(rec.get('realized_pnl'), 30.0)
        finally:
            case.tearDown()

    def test_p1_02_ack_unknown_timeout_retry_and_escalation_at_1000_1100_2000_10000(self):
        """P1-02: Verifies bounded retries, escalation to MANUAL_INTERVENTION_REQUIRED across 1000/1100/2000/10000, and eventual terminal recovery."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            now_box = [1000.0]
            case.watcher.clock = lambda: now_box[0]
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.cancel_super_order_leg.return_value = (True, 'Cancel ACK received')
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]

            # t=1000: Initial cancel request -> ACK_UNKNOWN (attempt 1)
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'ACK_UNKNOWN')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'ACK_UNKNOWN')
            self.assertEqual(rec['orphaned_leg_cancel_details']['TARGET_LEG']['attempt_count'], 1)
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, 2)

            # t=1015: Past 10s ACK timeout, still within 60s deadline -> bounded retry (attempt 2)
            now_box[0] = 1015.0
            case.watcher.check_once()
            self.assertEqual(rec['orphaned_leg_cancel_details']['TARGET_LEG']['attempt_count'], 2)
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, 4)

            # t=1100: Past 60s deadline while broker still reports PENDING -> escalates to MANUAL_INTERVENTION_REQUIRED
            now_box[0] = 1100.0
            case.watcher.check_once()
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))
            self.assertTrue(rec.get('orphaned_legs_manual_intervention_required'))
            self.assertTrue(rec.get('orphaned_legs_escalation_alerted'))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'MANUAL_INTERVENTION_REQUIRED')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'MANUAL_INTERVENTION_REQUIRED')
            calls_at_1100 = case.broker.cancel_super_order_leg.call_count

            # t=2000 and t=10000: Does not issue unlimited duplicate cancel requests or duplicate alerts
            for sim_t in (2000.0, 10000.0):
                now_box[0] = sim_t
                case.watcher.check_once()
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, calls_at_1100)
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))

            # t=10500: Broker finally confirms both legs CANCELLED -> resolves to CONFIRMED_TERMINAL
            now_box[0] = 10500.0
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='TRADED', target='CANCELLED', stop='CANCELLED')
            ]
            case.watcher.check_once()
            self.assertTrue(rec.get('orphaned_legs_cancelled', False))
            self.assertFalse(rec.get('orphaned_legs_manual_intervention_required', False))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'CONFIRMED_TERMINAL')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'CONFIRMED_TERMINAL')
        finally:
            case.tearDown()

    def test_p1_02_forever_oco_and_readback_failure_cancellation_liveness(self):
        """P1-02: Tests Forever OCO orphaned leg cancellation, readback failure handling, and restart persistence."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            now_box = [2000.0]
            case.watcher.clock = lambda: now_box[0]
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.order_status.return_value = {"orderId": "ORD-FO-1", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 100.0}
            case.broker.place_forever_oco.return_value = (True, "FO-LEG-1", "OK")
            case.broker.get_forever_orders.return_value = [{
                "orderId": "FO-LEG-1",
                "orderStatus": "CONFIRM",
                "orderFlag": "OCO",
                "dhanClientId": "1100000001",
                "securityId": "55001",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 10,
                "price": 96.05,
                "triggerPrice": 97.0,
                "price1": 103.0,
                "triggerPrice1": 103.0,
                "orderType": "OCO",
            }]
            case.broker.get_positions.return_value = [{"securityId": "55001", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}]
            case.watcher.track(
                order_id="ORD-FO-1",
                symbol="POLYCAB",
                security_id="55001",
                segment="NSE_EQ",
                qty=10,
                product_type="CNC",
                is_super_order=False,
                protection_route="FOREVER_OCO",
            )
            case.watcher.check_once()

            # Manual exit flattens position while FO-LEG-1 is still CONFIRM; first cancel rejected
            case.broker.get_positions.return_value = [{"securityId": "55001", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0}]
            case.broker.cancel_forever_order.return_value = (False, "Temporary 503")
            case.watcher.check_once()
            rec = case.watcher.terminal_orders["ORD-FO-1"]
            self.assertFalse(rec.get("orphaned_legs_cancelled", False))
            self.assertEqual(rec["orphaned_leg_cancel_states"]["FO-LEG-1"], "REJECTED")

            # Empty/unreadable forever book on next pass does not fabricate completion or issue blind cancel
            now_box[0] = 2010.0
            case.broker.get_forever_orders.return_value = []
            case.broker.cancel_forever_order.reset_mock()
            case.watcher.check_once()
            self.assertEqual(case.broker.cancel_forever_order.call_count, 0)
            self.assertFalse(rec.get("orphaned_legs_cancelled", False))

            # Book returns with CANCELLED -> resolves to CONFIRMED_TERMINAL
            now_box[0] = 2020.0
            case.broker.get_forever_orders.return_value = [{
                "orderId": "FO-LEG-1",
                "orderStatus": "CANCELLED",
                "orderFlag": "OCO",
                "dhanClientId": "1100000001",
                "securityId": "55001",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 10,
            }]
            case.watcher.check_once()
            self.assertTrue(rec.get("orphaned_legs_cancelled", False))
            self.assertEqual(rec["orphaned_leg_cancel_states"]["FO-LEG-1"], "CONFIRMED_TERMINAL")
        finally:
            case.tearDown()

    def test_p0_02_overridden_order_status_cannot_suppress_get_trades_for_order_in_live_mode(self):
        """P0-02: Overriding order_status/trades_avg_price on a LIVE DhanClient never suppresses get_trades_for_order() or fabricates OK_EMPTY."""
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="live_jwt_token",
        )
        client = DhanClient(cfg)
        client.order_status = MagicMock(return_value={"orderId": "ORD-LIVE-901", "orderStatus": "TRADED", "filledQty": 10})
        client.trades_avg_price = MagicMock(return_value=102.5)
        client.get_trade_book = MagicMock(return_value=[])

        # 1. When transport raises an error, LIVE get_trades_for_order returns BrokerReadFailure(UNAVAILABLE), NEVER OK_EMPTY
        client.session.get = MagicMock(side_effect=OSError("Simulated transport unavailable"))
        unperformed_res = client.get_trades_for_order("ORD-LIVE-901")
        self.assertIsInstance(unperformed_res, BrokerReadFailure)
        self.assertFalse(unperformed_res.ok)
        self.assertEqual(unperformed_res.status, BrokerReadStatus.UNAVAILABLE.value)

        # 2. With mocked session.get HTTP 200, LIVE get_trades_for_order executes GET /v2/trades/{order_id} and validates fields
        trade_payload = [{
            "orderId": "ORD-LIVE-901",
            "exchangeTradeId": "ET-LIVE-901",
            "dhanClientId": "1100000001",
            "securityId": "55001",
            "transactionType": "BUY",
            "tradedQuantity": 10,
            "tradedPrice": 102.5,
        }]
        client.session.get = MagicMock(
            return_value=SimpleNamespace(
                status_code=200,
                text='[{"orderId":"ORD-LIVE-901"}]',
                json=lambda: trade_payload,
            )
        )
        live_res = client.get_trades_for_order(
            "ORD-LIVE-901",
            expected_security_id="55001",
            expected_transaction_type="BUY",
            require_exchange_trade_id=True,
        )
        client.session.get.assert_called_once()
        called_url = client.session.get.call_args[0][0]
        self.assertTrue(called_url.endswith("/trades/ORD-LIVE-901"))
        self.assertIsInstance(live_res, BrokerReadList)
        self.assertTrue(live_res.ok)
        self.assertEqual(live_res.status, BrokerReadStatus.OK_RECORDS.value)
        self.assertEqual(len(live_res), 1)
        self.assertEqual(live_res[0]["exchangeTradeId"], "ET-LIVE-901")

    def test_p0_02_genuine_empty_broker_response_distinguishable_from_failed_or_unperformed_requests(self):
        """P0-02: Genuine HTTP 200 empty trade list (OK_EMPTY) is strictly distinguishable from HTTP failures and malformed payloads."""
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="live_jwt_token",
        )
        client = DhanClient(cfg)

        # Case A: HTTP 503 failure -> BrokerReadFailure(FAILED)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(status_code=503, text="Service Unavailable", json=lambda: {})
        )
        res_http_fail = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_http_fail, BrokerReadFailure)
        self.assertFalse(res_http_fail.ok)
        self.assertEqual(res_http_fail.status, BrokerReadStatus.FAILED.value)
        self.assertEqual(res_http_fail.status_code, 503)

        # Case B: Malformed / mismatched orderId payload -> BrokerReadFailure(MALFORMED)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(
                status_code=200,
                text='[{"orderId":"WRONG-ORDER"}]',
                json=lambda: [{"orderId": "WRONG-ORDER", "tradedQuantity": 10, "tradedPrice": 100.0}],
            )
        )
        res_malformed = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_malformed, BrokerReadFailure)
        self.assertFalse(res_malformed.ok)
        self.assertEqual(res_malformed.status, BrokerReadStatus.MALFORMED.value)

        # Case C: Genuine HTTP 200 empty list -> BrokerReadList(OK_EMPTY)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(status_code=200, text="[]", json=lambda: [])
        )
        res_empty = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_empty, BrokerReadList)
        self.assertTrue(res_empty.ok)
        self.assertEqual(res_empty.status, BrokerReadStatus.OK_EMPTY.value)
        self.assertEqual(len(res_empty), 0)

    def test_sanitized_historical_db_schema_upgrade_and_startup_recovery(self):
        """Scope Freeze Section B: Verifies schema upgrade and startup recovery on a copied sanitized pre-PR49 SQLite DB."""
        with tempfile.TemporaryDirectory() as tmp:
            hist_db_path = os.path.join(tmp, "sanitized_historical.db")
            con = sqlite3.connect(hist_db_path)
            with con:
                con.executescript(
                    """
                    CREATE TABLE order_intents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        correlation_id TEXT UNIQUE NOT NULL,
                        parent_correlation_id TEXT DEFAULT '',
                        account_id TEXT DEFAULT '',
                        symbol TEXT NOT NULL,
                        trading_symbol TEXT DEFAULT '',
                        security_id TEXT NOT NULL,
                        exchange_segment TEXT NOT NULL,
                        product_type TEXT NOT NULL,
                        order_type TEXT NOT NULL,
                        price REAL NOT NULL,
                        quantity INTEGER NOT NULL,
                        is_super_order INTEGER DEFAULT 0,
                        is_amo INTEGER DEFAULT 0,
                        original_market_price REAL DEFAULT 0.0,
                        tick_size REAL DEFAULT 0.05,
                        lot_size INTEGER DEFAULT 1,
                        option_type TEXT DEFAULT '',
                        strike REAL DEFAULT 0.0,
                        expiry TEXT DEFAULT '',
                        state TEXT NOT NULL,
                        broker_order_id TEXT,
                        error_msg TEXT,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    );
                    CREATE TABLE fills (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        broker_order_id TEXT NOT NULL,
                        exchange_trade_id TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        traded_price REAL NOT NULL,
                        traded_quantity INTEGER NOT NULL,
                        traded_at REAL NOT NULL
                    );
                    CREATE TABLE watcher_orders (
                        order_id TEXT PRIMARY KEY,
                        is_terminal INTEGER NOT NULL DEFAULT 0,
                        record_json TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    );
                    CREATE TABLE outbox (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'PENDING',
                        retry_count INTEGER DEFAULT 0,
                        next_retry_at REAL DEFAULT 0.0,
                        last_error TEXT,
                        created_at REAL NOT NULL,
                        processed_at REAL
                    );
                    INSERT INTO order_intents (
                        correlation_id, account_id, symbol, security_id, exchange_segment,
                        product_type, order_type, price, quantity, is_super_order, is_amo,
                        state, broker_order_id, created_at, updated_at
                    ) VALUES (
                        'HIST-EQ-1', '1100000001', 'YESBANK', '11915', 'NSE_EQ',
                        'CNC', 'LIMIT', 15.60, 10, 0, 0,
                        'ACKED', 'ORD-HIST-1', 1730000000.0, 1730000001.0
                    );
                    INSERT INTO fills (
                        broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at
                    ) VALUES (
                        'ORD-HIST-1', 'ORD-HIST-1-FILL-10', 'YESBANK', 15.55, 10, 1730000002.0
                    );
                    INSERT INTO watcher_orders (
                        order_id, is_terminal, record_json, updated_at
                    ) VALUES (
                        'ORD-HIST-1', 0,
                        '{"order_id":"ORD-HIST-1","symbol":"YESBANK","security_id":"11915","segment":"NSE_EQ","product_type":"CNC","qty":10,"is_super_order":false}',
                        1730000002.0
                    );
                    """
                )
            con.close()

            # Upgrade historical DB via JournalDB initialization
            db = JournalDB(hist_db_path, account_id="1100000001")
            pending_w, _ = db.load_watcher_state()
            self.assertEqual(pending_w["ORD-HIST-1"]["protection_route"], "FOREVER_OCO")

            # Recording a verified exchange tradebook fill for ORD-HIST-1 replaces the legacy synthetic -FILL-10 row without double counting!
            inserted = db.record_fill(
                broker_order_id="ORD-HIST-1",
                exchange_trade_id="REAL-EXCH-TRD-1",
                symbol="YESBANK",
                traded_price=15.55,
                traded_quantity=10,
                transaction_type="BUY",
                parent_order_id="ORD-HIST-1",
                is_verified_trade=True,
            )
            self.assertTrue(inserted)
            acct = db.get_position_accounting("ORD-HIST-1")
            self.assertEqual(acct["buy_filled_qty"], 10)
            self.assertEqual(acct["confirmed_open_qty"], 10)

    def test_pr49_default_cash_equity_forever_oco_and_amo_and_pr51_preserved(self):
        """Verifies default regular-session equity and AMO both use place_order() + FOREVER_OCO, explicit SUPER_ORDER works when set, and PR #51 is preserved."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100998877",
            dhan_access_token="LIVE_TOK",
            dhan_token_file=os.path.join(tmp.name, ".dhan_token"),
            data_dir=tmp.name,
            journal_db_path=os.path.join(tmp.name, "amo_reconcile.db"),
            equity_amo_enabled=True,
            monitored_channel_ids=[-100111222333],
            log_group_id=-100999888777,
        )
        self.assertEqual(cfg.equity_protection_route, "FOREVER_OCO")
        pipe = TradingBotPipeline(cfg)
        pipe.stop_manager.provision_initial_clean_state("Operator", "1100998877")
        pipe.reconciliation_gate._accepting_entries = True
        pipe.resolver.load_equity("YESBANK", "11915", "NSE_EQ", 0.05)
        pipe.broker.get_fund_limits = MagicMock(return_value=(True, 250000.0, "OK"))
        pipe.broker.get_positions = MagicMock(return_value=[])
        pipe.broker.get_quote_result = MagicMock(
            return_value=QuoteResult(
                is_valid=True,
                price=20.0,
                quote_timestamp=1762340000.0,
                age_sec=10.0,
                source="Yahoo Finance",
                symbol="YESBANK",
                http_latency_ms=12.0,
                parse_latency_ms=1.0,
                freshness_verified=True,
                quote_age_status="VERIFIED_FRESH",
                quote_freshness_status="FRESH",
            )
        )
        pipe.broker.last_quote_result = pipe.broker.get_quote_result.return_value
        pipe.broker.place_order = MagicMock(return_value=(True, "AMO-ORD-101", "AMO queued"))
        pipe.broker.place_super_order = MagicMock(return_value=(True, "SUP-ORD-102", "Super Order placed"))
        pipe.broker.last_order_broker_status = "TRANSIT"

        # 1. Closed session (after-hours) with equity_amo_enabled=True -> uses place_order(afterMarketOrder=True), NEVER place_super_order
        with patch("src.main.market_status", return_value=(False, "Market Closed Post-Hours (16:00:00 IST > 15:30:00)")):
            res_amo = pipe.process_raw_message(
                "Buy #YESBANK @ 20.00",
                channel_id=-100111222333,
                message_id=701,
                msg_timestamp=1762340005.0,
                current_time=1762340006.0,
                source_name="Channel",
            )
        self.assertTrue(res_amo.is_valid, res_amo.reason)
        self.assertEqual(pipe.broker.place_order.call_count, 1)
        self.assertEqual(pipe.broker.place_super_order.call_count, 0)
        placed_req = pipe.broker.place_order.call_args[0][0]
        self.assertTrue(placed_req.after_market_order)
        self.assertEqual(placed_req.amo_time, "OPEN")
        self.assertEqual(res_amo.protection_route, "FOREVER_OCO")

        # 2. Regular session with default config (unset EQUITY_PROTECTION_ROUTE) -> ALSO uses place_order() + FOREVER_OCO, NEVER place_super_order!
        pipe.broker.place_order.reset_mock()
        pipe.broker.place_order.return_value = (True, "REG-ORD-102", "Regular order placed")
        pipe.broker.place_super_order.reset_mock()
        pipe.broker.get_quote_result.return_value.age_sec = 0.5
        pipe.broker.get_quote_result.return_value.quote_timestamp = 1762340100.0
        with patch("src.main.market_status", return_value=(True, "Market Open")):
            res_reg = pipe.process_raw_message(
                "Buy #YESBANK @ 20.50",
                channel_id=-100111222333,
                message_id=702,
                msg_timestamp=1762340100.0,
                current_time=1762340100.5,
                source_name="Channel",
            )
        self.assertTrue(res_reg.is_valid, res_reg.reason)
        self.assertEqual(pipe.broker.place_order.call_count, 1)
        self.assertEqual(pipe.broker.place_super_order.call_count, 0)
        self.assertFalse(pipe.broker.place_order.call_args[0][0].after_market_order)
        self.assertEqual(res_reg.protection_route, "FOREVER_OCO")
        self.assertFalse(pipe.watcher.pending["REG-ORD-102"]["is_amo"])
        self.assertFalse(pipe.watcher.pending["REG-ORD-102"]["is_super_order"])
        self.assertEqual(pipe.watcher.pending["REG-ORD-102"]["protection_route"], "FOREVER_OCO")

        # 3. PR #51 unanchored immediate F&O option premium range remains intact
        parsed_fno, reason_fno, action_fno, _, _, _ = parse_fno_regex_detailed("BUY LAURUSLABS 1100 CE 18-20 SL 14 TGT 28")
        self.assertIsNotNone(parsed_fno, reason_fno)
        self.assertEqual(action_fno, "BUY_ENTRY")
        self.assertEqual((parsed_fno.entry_min, parsed_fno.entry_max), (18.0, 20.0))

        # 4. .env.example template exists and documents FOREVER_OCO default
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env_example_path = os.path.join(root_dir, ".env.example")
        self.assertTrue(os.path.exists(env_example_path), ".env.example must exist at repository root")
        with open(env_example_path, "r", encoding="utf-8") as ef_ex:
            env_example_txt = ef_ex.read()
        for req_key in (
            "EQUITY_PROTECTION_ROUTE=FOREVER_OCO",
            "EQUITY_AMO_ENABLED=false",
            "EQUITY_AMO_MAX_REFERENCE_AGE_SEC=259200.0",
            "DHAN_RESPONSE_GROUP_NAME=",
        ):
            self.assertIn(req_key, env_example_txt)


if __name__ == '__main__':
    unittest.main()
