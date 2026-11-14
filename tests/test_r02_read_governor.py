"""Offline R01/R02 integration & independent audit regression tests: transport counts, shared cooldown, real SQLite."""
from concurrent.futures import ThreadPoolExecutor
from email.utils import formatdate
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock

import requests

from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadDeferred, ReadGovernor
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.journal.db import JournalDB
from src.models import BrokerReadFailure, TradingMode


def response(code, body='[]', retry=None):
    r = requests.Response()
    r.status_code = code
    r._content = body.encode()
    if retry is not None:
        r.headers['Retry-After'] = retry
    return r


class ReadControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100000.
        self.g = ReadGovernor(self.tmp.name, 'ACCOUNT_A', clock=lambda: self.now)

    def client(self, governor=None, account_id='ACCOUNT_A'):
        c = DhanClient.__new__(DhanClient)
        c.cfg = NS(trading_mode=TradingMode.LIVE, dhan_client_id=account_id, data_dir=self.tmp.name)
        c._read_governor = governor or self.g
        c._headers = lambda: {}
        c.session = NS(get=Mock())
        c.generate_token_with_totp = Mock()
        return c

    def test_429_prevents_fallback_and_other_endpoint_reads(self):
        c = self.client()
        c.session.get.return_value = response(429, retry='120')
        c.get_order_book = Mock()
        self.assertIsNone(c.order_status('A'))
        c.get_order_book.assert_not_called()
        c.get_holdings()
        self.assertEqual(c.session.get.call_count, 1)
        c.generate_token_with_totp.assert_not_called()

    def test_shared_cooldown_survives_new_instance(self):
        self.g.throttled('120')
        other = ReadGovernor(self.tmp.name, 'ACCOUNT_A', clock=lambda: self.now)
        with self.assertRaises(ReadDeferred):
            other.admit()
        self.now += 121
        other.admit()

    def test_separate_accounts_are_isolated(self):
        self.g.throttled('120')
        ReadGovernor(self.tmp.name, 'ACCOUNT_B', clock=lambda: self.now).admit()

    def test_retry_after_http_date_and_long_window(self):
        self.g.throttled(formatdate(self.now + 7200, usegmt=True))
        self.now += 3601
        with self.assertRaises(ReadDeferred):
            self.g.admit()
        self.now += 3600
        self.g.admit()

    def test_invalid_header_uses_exponential_cooldown(self):
        for header in ('nonsense', 'nan', '-1'):
            self.g.throttled(header)
        self.now += 239
        with self.assertRaises(ReadDeferred):
            self.g.admit()
        self.now += 2
        self.g.admit()

    def test_hourly_budget_preserves_history_reserve(self):
        self.g.LIMITS = ((3600, 10),)
        for _ in range(8):
            self.g.admit(historical=True)
        with self.assertRaises(ReadDeferred):
            self.g.admit(historical=True)
        self.g.admit()
        self.g.admit()
        with self.assertRaises(ReadDeferred):
            self.g.admit()

    def test_dh904_never_becomes_empty_success(self):
        c = self.client()
        c.session.get.return_value = response(200, '{"errorCode":"DH-904"}')
        result = c.get_holdings()
        self.assertIsInstance(result, BrokerReadFailure)
        self.assertEqual(result.status_code, 429)
        c.generate_token_with_totp.assert_not_called()

    def test_r01_partial_history_throttle_never_commits_complete_result(self):
        c = self.client()
        c.get_historical_trades_page = Mock(side_effect=[
            [{'_broker_attested_no_execution_id': True}],
            BrokerReadFailure(status='FAILED', error='HTTP 429', status_code=429)])
        result = c.get_historical_trades('2026-11-02', '2026-11-14', page_delay_sec=0)
        self.assertIsInstance(result, BrokerReadFailure)

    def test_404_retains_exact_id_fallback(self):
        c = self.client()
        c.session.get.return_value = response(404)
        c.get_order_book = Mock(return_value=[{'orderId': 'A', 'orderStatus': 'CANCELLED'}])
        self.assertEqual(c.order_status('A')['orderStatus'], 'CANCELLED')

    def test_candidate_token_profile_cannot_bypass_cooldown(self):
        c = self.client()
        self.g.throttled('120')
        self.assertIsNone(c.get_authenticated_profile(candidate_token='synthetic'))
        c.session.get.assert_not_called()

    def test_store_failure_fails_closed_without_http_or_fallback(self):
        c = self.client()
        c._read_governor = NS(admit=Mock(side_effect=OSError('disk full')))
        c.get_order_book = Mock()
        self.assertIsNone(c.order_status('A'))
        c.session.get.assert_not_called()
        c.get_order_book.assert_not_called()

    def test_recovery_after_cooldown_uses_fresh_http_evidence(self):
        c = self.client()
        c.session.get.side_effect = [response(429, retry='60'), response(200, '[]')]
        self.assertIsInstance(c.get_holdings(), BrokerReadFailure)
        self.now += 61
        result = c.get_holdings()
        self.assertEqual(result, [])
        self.assertTrue(result.ok)
        self.assertEqual(c.session.get.call_count, 2)

    def test_shared_cooldown_is_visible_to_another_process(self):
        self.g.throttled('120')
        code = """import sys
from src.broker.read_governor import ReadGovernor, ReadDeferred
try:
    ReadGovernor(sys.argv[1], 'ACCOUNT_A', clock=lambda: 100000).admit()
except ReadDeferred:
    sys.exit(0)
sys.exit(1)
"""
        result = subprocess.run([sys.executable, '-c', code, self.tmp.name], timeout=10)
        self.assertEqual(result.returncode, 0)

    def test_429_halts_previously_open_entry_gate(self):
        c = self.client()
        c.reconciliation_gate = NS(is_accepting_entries=True, halt_new_entries=Mock())
        c.session.get.return_value = response(429)
        c.get_holdings()
        c.reconciliation_gate.halt_new_entries.assert_called_once()

    def test_r01_holdings_throttle_commits_nothing_then_reconciles(self):
        from tests.test_historical_zero_trade_id_r01 import R01HistoricalZeroTradeIdTests, NOW
        fixture = R01HistoricalZeroTradeIdTests()
        fixture.setUp()
        try:
            intents = fixture._seed_two()
            c = self.client()
            c.session.get.side_effect = [response(429, retry='60'), response(200,
                '[{"securityId":"11915","isin":"INE528G01035","dpQty":2,"totalQty":2}]')]
            fixture.broker.get_holdings.side_effect = c.get_holdings
            resolved, _, fatal = fixture.gate._reconcile_unresolved_intents_via_historical_trades(intents, now_ts=NOW)
            self.assertFalse(resolved)
            self.assertIsNotNone(fatal)
            fixture._assert_no_verified_fills()
            self.assertEqual(len(fixture.db.get_non_terminal_intents()), 2)
            self.now += 61
            resolved, remaining, fatal = fixture.gate._reconcile_unresolved_intents_via_historical_trades(intents, now_ts=NOW+61)
            self.assertIsNone(fatal)
            self.assertEqual(len(resolved), 2)
            self.assertFalse(remaining)
            self.assertEqual(c.session.get.call_count, 2)
        finally:
            fixture.doCleanups()

    def test_watcher_rate_halt_is_not_overwritten_by_startup(self):
        db = JournalDB(str(Path(self.tmp.name) / 'journal.db'), account_id='ACCOUNT_A')
        broker = NS(cfg=NS(dhan_client_id='ACCOUNT_A'), get_fund_limits=lambda: (True, 100, 'ok'),
                    get_trade_book=lambda: [], get_positions=lambda: [], get_holdings=lambda: [])
        watcher = NS(pending={}, terminal_orders={})
        gate = StartupReconciliationGate(broker, db, watcher, NS(is_stopped=lambda: False))
        watcher.check_once = lambda: gate.halt_new_entries('rate limited during supervision')
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(gate.is_accepting_entries)
        self.assertIn('rate limited', reason)
        watcher.check_once = lambda: None
        self.assertTrue(gate.run_startup_reconciliation()[0])

    # ------------------------------------------------------------------
    # Additional Independent Audit Regression Tests (Findings 1–9)
    # ------------------------------------------------------------------

    def test_strike_counter_resets_after_cooldown_recovery_http_200(self):
        """Finding 1: Strikes reset to 0 after cooldown expires and a genuine HTTP 200 succeeds."""
        c = self.client()
        # 3 consecutive throttles without Retry-After -> strikes=3 (240s cooldown)
        c.session.get.return_value = response(429)
        c.get_holdings()
        self.g.throttled(None)
        self.g.throttled(None)
        self.now += 241
        # Genuine HTTP 200 after cooldown -> resets strikes to 0
        c.session.get.return_value = response(200, '[]')
        res = c.get_holdings()
        self.assertEqual(res, [])
        # Next single 429 without Retry-After starts fresh at strike 1 (60s), not strike 4 (480s)
        c.session.get.return_value = response(429)
        c.get_holdings()
        self.now += 61
        c.session.get.return_value = response(200, '[]')
        self.assertEqual(c.get_holdings(), [])

    def test_concurrent_multithread_admission_no_false_sqlite_lock_halt(self):
        """Finding 2: Concurrent WAL admission across threads never triggers false SQLite lock halts."""
        def worker(i):
            gov = ReadGovernor(self.tmp.name, 'ACCOUNT_A', clock=lambda: self.now + i * 0.001)
            gov.admit()
            return True

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(worker, range(40)))
        self.assertEqual(len(results), 40)
        self.assertTrue(all(results))

    def test_low_priority_polling_preserves_top_10_percent_critical_reserve(self):
        """Finding 3A: Low-priority background polling stops at 90% while normal/critical reads use remaining 10%."""
        self.g.LIMITS = ((3600, 10),)
        for _ in range(9):
            self.g.admit(priority='low')
        with self.assertRaises(ReadDeferred):
            self.g.admit(priority='low')
        # Normal/critical read still admitted using the reserved 10th slot
        self.g.admit(priority='normal')
        with self.assertRaises(ReadDeferred):
            self.g.admit(priority='normal')

    def _check_startup_reconciliation_snapshots(self, drop_holdings=False):
        """Finding 3B: Reconciling 2 historical R01 orders inside run_startup_reconciliation refreshes holdings and protection after the watcher pass."""
        from tests.test_historical_zero_trade_id_r01 import R01HistoricalZeroTradeIdTests, ACCOUNT
        fixture = R01HistoricalZeroTradeIdTests()
        fixture.setUp()
        try:
            fixture._seed_two()
            fixture.broker.get_fund_limits = Mock(return_value=(True, 5000.0, 'OK'))
            fixture.broker.get_trade_book = Mock(return_value=[])
            fixture.broker.get_positions = Mock(return_value=[])
            fixture.broker.get_order_book = Mock(return_value=[])
            fixture.broker.order_status = Mock(return_value=None)
            fixture.gate.stop_manager = NS(is_stopped=lambda: False)
            fixture.broker.get_order_by_correlation_id = Mock(return_value=None)
            fixture.db.acknowledge_manual_risk_holding(
                parent_order_id='900001102657701',
                correlation_id='EQ-ALPHA-01',
                account_id=ACCOUNT,
                symbol='YESBANK-EQ',
                security_id='11915',
                quantity=1,
                acknowledged_by='owner',
                reason='Controlled test share 1',
            )
            fixture.db.acknowledge_manual_risk_holding(
                parent_order_id='900001104398802',
                correlation_id='EQ-BETA-02',
                account_id=ACCOUNT,
                symbol='YESBANK-EQ',
                security_id='11915',
                quantity=1,
                acknowledged_by='owner',
                reason='Controlled test share 2',
            )
            def restore_lifecycle(**rec):
                fixture.db.upsert_position_lifecycle(
                    parent_order_id=rec['order_id'], symbol=rec['symbol'],
                    security_id=rec['security_id'], exchange_segment=rec['segment'],
                    product_type=rec['product_type'], protection_route='FOREVER_OCO',
                    position_state='OPEN', entry_qty=1, buy_filled_qty=1,
                    buy_vwap=15.52, account_id=ACCOUNT,
                )
            fixture.watcher.reconcile_historical_order_fills.side_effect = restore_lifecycle
            def watcher_pass():
                if drop_holdings:
                    fixture.broker.get_holdings.return_value = []
            fixture.watcher.check_once = watcher_pass
            ok, msg = fixture.gate.run_startup_reconciliation()
            if drop_holdings:
                self.assertFalse(ok, msg)
                self.assertFalse(fixture.gate.is_accepting_entries)
            else:
                self.assertTrue(ok, msg)
                self.assertEqual(fixture.broker.get_forever_orders.call_count, 2)
            self.assertEqual(fixture.broker.get_holdings.call_count, 2)
        finally:
            fixture.doCleanups()

    def test_startup_reconciliation_refreshes_proof_after_watcher(self):
        self._check_startup_reconciliation_snapshots()

    def test_stale_pre_watcher_holdings_cannot_clear_gate(self):
        self._check_startup_reconciliation_snapshots(drop_holdings=True)

    def test_dynamic_account_change_requires_new_verified_client(self):
        """Account changes cannot bypass cooldown with an old client/token."""
        c = self.client(account_id='ACCOUNT_A')
        c.session.get.return_value = response(429, retry='120')
        c.get_holdings()
        # Changing account on an existing client must fail without sending HTTP.
        c.cfg.dhan_client_id = 'ACCOUNT_B'
        c.session.get.return_value = response(200, '[]')
        res = c.get_holdings()
        self.assertIsInstance(res, BrokerReadFailure)
        self.assertEqual(c.session.get.call_count, 1)

    def test_funds_account_mismatch_and_nonfinite_balance_rejected(self):
        """Findings 7 & 8: get_fund_limits rejects mismatched dhanClientId and NaN/non-finite balances."""
        c = self.client(account_id='ACCOUNT_A')
        c.token = 'tok-1'
        c.is_token_error = lambda code, txt: False
        c._requires_token_refresh = lambda code, txt: False
        c.session.get.return_value = response(200, '{"dhanClientId":"ACCOUNT_B","availabelBalance":5000.0}')
        ok, bal, msg = c.get_fund_limits(retry=False)
        self.assertFalse(ok)
        self.assertIn('mismatch', msg.lower())

        c.session.get.return_value = response(200, '{"dhanClientId":"ACCOUNT_A","availabelBalance":"NaN"}')
        ok2, bal2, msg2 = c.get_fund_limits(retry=False)
        self.assertFalse(ok2)
        self.assertIn('non-finite', msg2.lower())

    def test_current_day_tradebook_zero_or_synthetic_exchange_trade_id_blocks_startup(self):
        """Finding 9: Current-day trade_book row with exchangeTradeId='0' fails closed during startup reconciliation."""
        db = JournalDB(str(Path(self.tmp.name) / 'journal_tb.db'), account_id='ACCOUNT_A')
        bad_trade = [{
            'dhanClientId': 'ACCOUNT_A',
            'orderId': 'ORD-TODAY-1',
            'exchangeTradeId': '0',
            'tradingSymbol': 'YESBANK-EQ',
            'securityId': '11915',
            'exchangeSegment': 'NSE_EQ',
            'productType': 'CNC',
            'transactionType': 'BUY',
            'tradedQuantity': 1,
            'tradedPrice': 15.52,
        }]
        broker = NS(
            cfg=NS(dhan_client_id='ACCOUNT_A'),
            get_fund_limits=lambda: (True, 1000.0, 'ok'),
            get_trade_book=lambda: bad_trade,
            get_positions=lambda: [],
            get_holdings=lambda: [],
        )
        watcher = NS(pending={}, terminal_orders={})
        gate = StartupReconciliationGate(broker, db, watcher, NS(is_stopped=lambda: False))
        ok, reason = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertEqual(gate.state, ReconciliationState.BLOCKED)
        self.assertIn('exchangeTradeId', reason)

    def test_error_code_805_and_nested_dh904_are_throttle_not_auth(self):
        for payload in ('{"internalErrorCode":805}', '{"data":{"errorCode":"DH-904"}}'):
            with self.subTest(payload=payload):
                c = self.client()
                c.session.get.return_value = response(200, payload)
                result = c.get_holdings()
                self.assertIsInstance(result, BrokerReadFailure)
                self.assertEqual(result.status_code, 429)
                c.generate_token_with_totp.assert_not_called()
                self.now += 10000

    def test_order_book_reads_use_low_priority_reserve(self):
        self.g.LIMITS = ((3600, 10),)
        c = self.client()
        c.session.get.return_value = response(200)
        for _ in range(9):
            self.assertEqual(c.get_order_book(), [])
        self.assertIsInstance(c.get_order_book(), BrokerReadFailure)
        self.assertEqual(c.session.get.call_count, 9)
        self.assertEqual(c.get_holdings(), [])
        self.assertEqual(c.session.get.call_count, 10)

    def test_sqlite_contention_has_bounded_wait_and_no_http(self):
        import sqlite3
        import time
        self.g.admit()
        conn = sqlite3.connect(self.g.path)
        try:
            conn.execute('BEGIN IMMEDIATE')
            c = self.client()
            start = time.monotonic()
            self.assertIsInstance(c.get_holdings(), BrokerReadFailure)
            self.assertLess(time.monotonic() - start, 0.75)
            c.session.get.assert_not_called()
        finally:
            conn.rollback()
            conn.close()

    def test_blocked_enum_does_not_repeat_halt_events(self):
        c = self.client()
        c.reconciliation_gate = NS(state=ReconciliationState.BLOCKED,
                                    is_accepting_entries=False, halt_new_entries=Mock())
        self.g.throttled('120')
        for _ in range(3):
            c.get_holdings()
        c.reconciliation_gate.halt_new_entries.assert_not_called()

    def test_current_day_missing_id_fractional_quantity_and_boolean_price_block(self):
        base = dict(orderId='O1', exchangeTradeId='T1', tradedQuantity=1,
                    tradedPrice=10, transactionType='BUY', dhanClientId='ACCOUNT_A')
        for bad in ({'exchangeTradeId':None}, {'exchangeTradeId':'0.0'},
                    {'exchangeTradeId':'NULL'}, {'tradedQuantity':1.5},
                    {'tradedQuantity':True}, {'tradedPrice':True}):
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as temp:
                db = JournalDB(str(Path(temp)/'journal.db'), account_id='ACCOUNT_A')
                broker = NS(cfg=NS(dhan_client_id='ACCOUNT_A'), get_fund_limits=lambda:(True,100,'ok'),
                            get_trade_book=lambda:[dict(base, **bad)], get_positions=lambda:[], get_holdings=lambda:[])
                gate = StartupReconciliationGate(broker, db, NS(pending={},terminal_orders={}), NS(is_stopped=lambda:False))
                self.assertFalse(gate.run_startup_reconciliation()[0])
                with db._get_conn() as conn:
                    self.assertEqual(conn.execute('SELECT COUNT(*) FROM fills').fetchone()[0],0)
