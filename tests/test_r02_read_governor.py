"""Offline R01/R02 integration: transport counts, shared cooldown, real SQLite."""
import tempfile
import unittest
from email.utils import formatdate
from types import SimpleNamespace as NS
from unittest.mock import Mock
import requests
from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor, ReadDeferred
from src.models import TradingMode, BrokerReadFailure


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

    def client(self, governor=None):
        c = DhanClient.__new__(DhanClient)
        c.cfg = NS(trading_mode=TradingMode.LIVE)
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
        import subprocess
        import sys
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
        from src.journal.db import JournalDB
        from src.engine.reconciliation import StartupReconciliationGate
        from pathlib import Path
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
