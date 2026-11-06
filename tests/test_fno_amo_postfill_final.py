"""Offline native AMO lifecycle, truthful notices and durable ACK regressions."""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.models import infer_record_protection_route
from src.notifier.execution_updates import format_order_outcome, OrderStatusReporter
from src.notifier import message_designer as designer
from tests import test_fno_amo_protected_queue as fixtures


class PostfillTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.folder = temp.name
        self.broker = MagicMock()
        self.broker.reconciliation_gate = MagicMock()
        for name in ('get_super_orders', 'get_forever_orders', 'get_trade_book', 'get_positions', 'get_holdings'):
            getattr(self.broker, name).return_value = []
        self.db = JournalDB(os.path.join(self.folder, 'journal.db'))
        self.path = os.path.join(self.folder, 'watcher.json')
        self.watcher = ProtectionWatcher(broker=self.broker, path=self.path,
                                        clock=lambda: 1000, journal_db=self.db)
        self.watcher.track(order_id='AMO-1', symbol='HAL', security_id='123',
                           segment='NSE_FNO', product_type='MARGIN', qty=75,
                           is_amo=True, entry_only_amo=True, protection_route='ENTRY_ONLY_AMO')

    def observe(self, status='TRADED', qty=75):
        self.broker.order_status.return_value = {
            'orderId': 'AMO-1', 'orderStatus': status,
            'filledQty': qty, 'averageTradedPrice': 90,
        }
        self.broker.get_positions.return_value = [dict(securityId='123', exchangeSegment='NSE_FNO',
                                                      productType='MARGIN', netQty=qty)]
        with patch('src.engine.protection_watcher.is_market_open', return_value=True):
            self.watcher.check_once()

    def test_full_fill_supervised_and_restored_without_protection_or_retry(self):
        self.observe()
        self.assertNotIn('AMO-1', self.watcher.pending)
        rec = self.watcher.terminal_orders['AMO-1']
        self.assertEqual(rec['protection_route'], 'ENTRY_ONLY_AMO')
        self.assertEqual(rec['position_state'], 'UNPROTECTED')
        self.assertEqual(rec['protected_qty'], 0)
        self.assertTrue(self.watcher.has_open_monitored_positions())
        self.broker.get_positions.assert_called()
        self.broker.place_forever_oco.assert_not_called()
        self.broker.place_super_order.assert_not_called()
        reloaded = ProtectionWatcher(broker=self.broker, path=self.path, journal_db=self.db)
        self.assertTrue(reloaded.has_open_monitored_positions())
        self.assertFalse(reloaded._evaluate_retry_locked('AMO-1', 90)[0])

    def test_partial_fill_supervised_without_finishing_entry(self):
        self.observe('PART_TRADED', 25)
        self.assertIn('AMO-1', self.watcher.pending)
        self.assertTrue(self.watcher.has_open_monitored_positions())
        self.broker.get_positions.assert_called()
        self.broker.place_forever_oco.assert_not_called()

    def test_cancelled_partial_fill_retains_open_position(self):
        self.observe('CANCELLED', 25)
        self.assertIn('AMO-1', self.watcher.terminal_orders)
        self.assertTrue(self.watcher.has_open_monitored_positions())
        self.assertNotEqual(self.watcher.terminal_orders['AMO-1']['position_state'], 'POSITION_CLOSED')

    def test_lifecycle_commit_failure_keeps_reconciliation_obligation(self):
        with patch.object(self.db, 'upsert_position_lifecycle', side_effect=OSError('disk full')):
            self.observe()
        self.assertIn('AMO-1', self.watcher.pending)
        self.assertEqual(self.watcher.pending['AMO-1']['position_state'], 'RECONCILIATION_REQUIRED')
        self.broker.reconciliation_gate.halt_new_entries.assert_called()

    def test_fill_commit_failure_never_finishes_entry(self):
        with patch.object(self.db, 'record_fill', side_effect=OSError('disk full')):
            self.observe()
        self.assertIn('AMO-1', self.watcher.pending)
        self.assertEqual(self.watcher.pending['AMO-1']['position_state'], 'RECONCILIATION_REQUIRED')
        self.broker.reconciliation_gate.halt_new_entries.assert_called()

    def test_native_only_does_not_poll_nonexistent_protective_books(self):
        self.observe()
        self.broker.get_super_orders.assert_not_called()
        self.broker.get_forever_orders.assert_not_called()
        self.broker.get_holdings.assert_not_called()

    def test_manual_flat_without_parent_sell_evidence_requires_reconciliation(self):
        self.observe()
        self.broker.get_positions.return_value = [dict(securityId='123', exchangeSegment='NSE_FNO',
                                                      productType='MARGIN', netQty=0)]
        self.watcher.supervise_open_positions()
        self.assertEqual(self.watcher.terminal_orders['AMO-1']['position_state'], 'RECONCILIATION_REQUIRED')
        self.broker.reconciliation_gate.halt_new_entries.assert_called()

    def linked_exit(self, quantity=75):
        self.observe()
        self.watcher.terminal_orders['AMO-1']['exit_order_id'] = 'EXIT-1'
        self.broker.get_trade_book.return_value = [dict(
            orderId='EXIT-1', parentOrderId='AMO-1', exchangeTradeId='EX-1',
            transactionType='SELL', securityId='123', exchangeSegment='NSE_FNO',
            productType='MARGIN', tradedQuantity=quantity, tradedPrice=93)] * 2
        self.broker.get_positions.return_value = [dict(securityId='123', exchangeSegment='NSE_FNO',
                                                      productType='MARGIN', netQty=0)]

    def test_verified_linked_sell_closes_once_despite_duplicate_trade_rows(self):
        self.linked_exit()
        self.watcher.supervise_open_positions()
        rec = self.watcher.terminal_orders['AMO-1']
        self.assertEqual(rec['position_state'], 'POSITION_CLOSED')
        self.assertEqual(rec['exit_filled_qty'], 75)
        self.assertFalse(self.watcher.has_open_monitored_positions())

    def test_partial_linked_sell_does_not_hide_unattributed_remaining_sale(self):
        self.linked_exit(25)
        self.watcher.supervise_open_positions()
        self.assertEqual(self.watcher.terminal_orders['AMO-1']['position_state'], 'RECONCILIATION_REQUIRED')

    def test_exit_persistence_failure_never_claims_closed(self):
        self.linked_exit()
        with patch.object(self.db, 'record_fill', side_effect=OSError('disk full')):
            self.watcher.supervise_open_positions()
        self.assertEqual(self.watcher.terminal_orders['AMO-1']['position_state'], 'RECONCILIATION_REQUIRED')
        self.broker.reconciliation_gate.halt_new_entries.assert_called()

    def test_route_validation_rejects_non_amo_and_wrong_segments(self):
        rec = dict(self.watcher.pending['AMO-1'])
        rec['protection_route'] = 'UNKNOWN'
        self.assertEqual(infer_record_protection_route(rec), 'ENTRY_ONLY_AMO')
        for changed in ({'is_amo': False}, {'segment': 'NSE_EQ'}, {'product_type': 'CNC'}, {'is_super_order': True}):
            self.assertEqual(infer_record_protection_route(dict(rec, **changed)), 'UNKNOWN')

    def test_risk_warning_survives_custom_designer_and_stale_levels(self):
        settings = designer.defaults()
        for event in ('executed', 'partial', 'final'):
            settings['templates'][event].update(enabled=True, fields=[])
        designer.save(settings, self.folder)
        for status in ('TRADED', 'PART_TRADED', 'CANCELLED'):
            text = format_order_outcome(dict(symbol='HAL', entry_only_amo=True, data_dir=self.folder),
                                        75, 75, status, 1000, 90, True,
                                        sl_price=87, target_price=93)
            self.assertIn('No automatic SL/target', text)
            self.assertNotIn('Pending Broker Verification', text)
            self.assertNotIn('Verified Active', text)
            self.assertNotIn('87.00', text)

    def test_reporter_clears_phantom_protection_pending(self):
        rec = self.watcher.pending['AMO-1']
        rec['notification_meta'] = dict(symbol='HAL', mode='LIVE')
        rec['notification_protection_pending'] = True
        reporter = OrderStatusReporter(self.db, self.broker, dhan_response_enabled=True)
        self.assertTrue(reporter.process(rec, dict(orderStatus='TRADED', filledQty=75, averageTradedPrice=90), 1000))
        self.assertFalse(rec['notification_protection_pending'])


class AckIntegrationTests(unittest.TestCase):
    def test_verified_ack_deduplicates_and_unverified_is_not_mirrored(self):
        fixture = fixtures.TestFnoAmoExecutor()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        pipe = fixture.make_pipeline(fixture.t)
        pipe.db = JournalDB(os.path.join(fixture.tmp.name, 'ack.db'))
        pipe.watcher.execution_reporter.dhan_response_enabled = True
        pipe.broker.last_order_broker_status = 'PENDING'
        pipe.broker.place_order.return_value = (True, 'AMO-ID', 'Dhan order acknowledged (status=PENDING)')
        with patch('src.main.market_status', return_value=(False, 'Market Closed Post-Hours')), \
             patch('src.engine.config_manager.ConfigChangeTracker') as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value = (False, '')
            for msg in (10, 11):
                result = pipe._handle_fno_signal(fixtures.signal(msg=msg, timestamp=fixture.t), 'trusted', fixture.t, injected_clock=fixture.t)
                self.assertTrue(result.is_valid, result.reason)
            rows = [r for r in pipe.db.fetch_pending_outbox() if r['event_type'] == 'DHAN_RESPONSE_ACK']
            self.assertEqual(len(rows), 1)
            self.assertIn('No automatic SL/target', rows[0]['payload'])
            pipe.broker.place_order.return_value = (True, 'AMO-UNKNOWN', 'ack received')
            pipe._handle_fno_signal(fixtures.signal(msg=12, timestamp=fixture.t), 'trusted', fixture.t, injected_clock=fixture.t)
            rows = [r for r in pipe.db.fetch_pending_outbox() if r['event_type'] == 'DHAN_RESPONSE_ACK']
            self.assertEqual(len(rows), 1)


if __name__ == '__main__':
    unittest.main()
