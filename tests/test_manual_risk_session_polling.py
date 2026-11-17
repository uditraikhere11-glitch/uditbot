"""Offline deterministic clock tests for owner-approved manual-risk supervision."""
import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

from src.engine.protection_watcher import ProtectionWatcher
from src.engine.watch_poll_policy import watcher_cadence_seconds, manual_risk_only


def holding():
    return dict(order_id='O1', security_id='100', correlation_id='C1', symbol='SYNTH',
                filled_qty=1, exit_filled_qty=0, position_state='UNPROTECTED_MANUAL_RISK',
                state='TRADED', _schedule_ack_verified=True)


class EligibilityTests(unittest.TestCase):
    def test_acknowledgement_and_no_legs_required(self):
        r = holding()
        self.assertTrue(manual_risk_only({}, [r]))
        for field, value in [('sl_order_id', 'SL1'), ('pending_oco', {'state': 'UNKNOWN'}),
                             ('_schedule_ack_verified', False), ('reconciliation_required', True),
                             ('cancel_requested', True), ('verified_leg_order_ids', ['TP1'])]:
            with self.subTest(field=field):
                self.assertFalse(manual_risk_only({}, [{**r, field: value}]))
        self.assertFalse(manual_risk_only({'N': {}}, [r]))

    def test_terminal_uncertainty_is_bounded_outside_hours(self):
        r = {**holding(), 'cancel_uncertain': True}
        self.assertEqual(watcher_cadence_seconds(pending={}, open_records=[r], now=1000,
                                               market_open=False), 300)


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = 1000.0
        self.market = False
        self.w = ProtectionWatcher(broker=NS(cfg=NS(dhan_client_id='ACCOUNT')), journal_db=None,
                                   path=str(Path(self.tmp.name) / 'watcher.json'))
        self.w.journal_db = NS(is_manual_risk_acknowledged=Mock(return_value=True))
        self.w.terminal_orders = {'O1': holding()}
        self.w.should_poll = lambda *_: True
        self.calls = []
        self.on_check = None
        self.complete = True

        def check():
            self.calls.append(self.clock)
            if self.on_check:
                self.on_check()
            self.w._supervision_read_complete = self.complete
            return False  # no state changes != failed broker evidence
        self.w.check_once = check

    async def drive(self, seconds, event=None):
        end = self.clock + seconds
        class Finished(Exception):
            pass
        async def sleep(delay):
            self.clock += delay
            if event:
                event()
            if self.clock >= end:
                raise Finished
        with patch('src.engine.protection_watcher.time', NS(monotonic=lambda: self.clock, time=time.time)), \
             patch('src.engine.protection_watcher.is_lifecycle_open', side_effect=lambda: self.market), \
             patch('src.engine.protection_watcher.asyncio.sleep', new=sleep):
            with self.assertRaises(Finished):
                await self.w.run()

    async def test_one_closed_session_check_even_after_hours_of_heartbeats(self):
        await self.drive(3600)
        self.assertEqual(self.calls, [1000])
        self.w.journal_db.is_manual_risk_acknowledged.assert_called_with(
            'O1', 'C1', expected_account_id='ACCOUNT', expected_security_id='100', required_qty=1)

    async def test_market_checks_exactly_120_seconds_apart(self):
        self.market = True
        await self.drive(242)
        self.assertEqual(self.calls, [1000, 1120, 1240])

    async def test_market_transition_wakes_immediately_and_close_gets_one_check(self):
        def event():
            if self.clock == 1010: self.market = True
            if self.clock == 1020: self.market = False
        await self.drive(400, event)
        self.assertEqual(self.calls, [1000, 1010, 1020])

    async def test_restart_takes_fresh_snapshot(self):
        await self.drive(10)
        await self.drive(10)
        self.assertEqual(self.calls, [1000, 1010])

    async def test_failed_snapshot_retries_and_does_not_consume_once(self):
        self.complete = False
        def event():
            if self.clock == 1060: self.complete = True
        await self.drive(400, event)
        self.assertEqual(self.calls, [1000, 1060])

    async def test_explicit_operator_recheck_wakes_once(self):
        def event():
            if self.clock == 1010: self.w.request_supervision_check()
        await self.drive(400, event)
        self.assertEqual(self.calls, [1000, 1010])

    async def test_new_order_arriving_during_pass_is_not_swallowed(self):
        def add():
            if len(self.calls) == 1:
                self.w.pending['NEW'] = dict(order_id='NEW', created_at=self.w.clock(), state='ACKED')
        self.on_check = add
        await self.drive(6)
        self.assertGreaterEqual(len(self.calls), 2)
        self.assertEqual(self.calls[1], 1002)

    async def test_ack_revoked_wakes_periodic_checks(self):
        def event():
            if self.clock == 1010:
                self.w.journal_db.is_manual_risk_acknowledged.return_value = False
        await self.drive(330, event)
        self.assertEqual(self.calls, [1000, 1010, 1310])

    async def test_ack_lookup_failure_does_not_get_one_shot(self):
        self.w.journal_db.is_manual_risk_acknowledged.side_effect = RuntimeError('offline fixture')
        await self.drive(302)
        self.assertEqual(self.calls, [1000, 1300])

    async def test_terminal_protection_uncertainty_wakes_urgent_check(self):
        def event():
            if self.clock == 1010:
                self.w.terminal_orders['O1']['pending_oco'] = {'state': 'UNKNOWN'}
        await self.drive(32, event)
        self.assertEqual(self.calls, [1000, 1010, 1020, 1030])

    async def test_terminal_uncertainty_uses_urgent_governor_reserve(self):
        from src.broker.read_governor import ReadGovernor
        governor = ReadGovernor(self.tmp.name, 'ACCOUNT')
        governor.seconds_until_capacity = Mock(return_value=0)
        self.w.broker._read_governor = governor
        self.w.terminal_orders['O1']['cancel_uncertain'] = True
        await self.drive(4)
        governor.seconds_until_capacity.assert_called_once_with(required=8, priority='urgent')

    async def test_throttled_closed_session_is_not_consumed(self):
        from src.broker.read_governor import ReadGovernor
        governor = ReadGovernor(self.tmp.name, 'ACCOUNT')
        governor.seconds_until_capacity = Mock(side_effect=[30, 0])
        self.w.broker._read_governor = governor
        await self.drive(400)
        self.assertEqual(self.calls, [1030])
        self.assertEqual(governor.seconds_until_capacity.call_count, 2)

    async def test_snapshot_failure_has_bounded_retry(self):
        self.w._poll_snapshot = Mock(side_effect=ValueError('fixture'))
        await self.drive(62)
        self.assertEqual(self.w._poll_snapshot.call_count, 2)
        self.assertFalse(self.calls)


class BrokerSnapshotTests(unittest.TestCase):
    def exercise(self, holdings, trades=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        gate = NS(halt_new_entries=Mock())
        broker = NS(cfg=NS(dhan_client_id='ACCOUNT'), reconciliation_gate=gate,
                    get_super_orders=Mock(return_value=[]),
                    get_forever_orders=Mock(return_value=[]),
                    get_trade_book=Mock(return_value=trades or []),
                    get_positions=Mock(return_value=[]),
                    get_holdings=Mock(return_value=holdings))
        w = ProtectionWatcher(broker=broker, journal_db=None,
                              path=str(Path(tmp.name) / 'watcher.json'))
        r = {**holding(), 'protection_route': 'FOREVER_OCO', 'product_type': 'CNC',
             'segment': 'NSE_EQ', 'qty': 1}
        w.terminal_orders = {'O1': r}
        w.supervise_open_positions()
        return w, r, gate

    def test_readable_unchanged_holding_completes_snapshot_and_remains_manual(self):
        w, r, gate = self.exercise([{'securityId': '100', 'totalQty': 1}])
        self.assertTrue(w._supervision_read_complete)
        self.assertEqual(r['position_state'], 'UNPROTECTED_MANUAL_RISK')
        gate.halt_new_entries.assert_not_called()

    def test_reduced_or_absent_holding_requires_reconciliation_not_fake_close(self):
        for rows in ([], [{'securityId': '100', 'totalQty': 0}]):
            with self.subTest(rows=rows):
                w, r, gate = self.exercise(rows)
                self.assertEqual(r['position_state'], 'RECONCILIATION_REQUIRED')
                self.assertTrue(r['reconciliation_required'])
                self.assertEqual(r.get('exit_filled_qty', 0), 0)
                gate.halt_new_entries.assert_called()

    def test_unreadable_book_is_not_a_completed_snapshot(self):
        w, r, gate = self.exercise({'error': 'unavailable'})
        self.assertFalse(w._supervision_read_complete)
        gate.halt_new_entries.assert_called()


if __name__ == '__main__':
    unittest.main()
