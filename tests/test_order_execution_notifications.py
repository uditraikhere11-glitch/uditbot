"""AB-ORDER-CONFIRM-01: offline tests. No Dhan/Telegram network writes."""
import unittest
from src.notifier.execution_updates import (
    OrderStatusReporter, format_order_failed, format_order_outcome, format_order_submitted,
)


META = {
    'symbol': 'YESBANK', 'segment': 'EQUITY', 'action': 'BUY',
    'mode': 'LIVE', 'source': 'RDHS 💬', 'yahoo_cmp': 15.52,
    'quote_age_sec': 3.88, 'quote_threshold_sec': 2.0,
    'quote_accepted': True, 'limit_price': 15.60,
    'signal_received_at': 1793608278.0,
}


class FakeDB:
    def __init__(self):
        self.events = {}

    def enqueue_order_notification_once(self, oid, event_key, text):
        key = (oid, event_key)
        if key in self.events:
            return False
        self.events[key] = text
        return True


class FakeBroker:
    def __init__(self, avg=None):
        self.avg = avg
        self.reads = 0

    def trades_avg_price(self, oid):
        self.reads += 1
        return self.avg


class TestOrderNotifications(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.broker = FakeBroker()
        self.reporter = OrderStatusReporter(self.db, self.broker)
        self.rec = {
            'order_id': '221261102657708', 'symbol': 'YESBANK', 'qty': 1,
            'filled_qty': 0, 'protected_qty': 0, 'pending_oco': None,
            'notification_meta': dict(META),
        }
        self.observed = META['signal_received_at'] + 1.42

    def test_submitted_clock_and_no_id(self):
        text = format_order_submitted(META, 1)
        self.assertIn('✅ <b>Trade Signal Processed: YESBANK</b>', text)
        self.assertIn('⏳ <b>Status: Order Submitted</b>', text)
        self.assertIn('Awaiting Dhan execution confirmation', text)
        self.assertIn('Yahoo Quote Age: 3.88s (Accepted under configured policy)', text)
        self.assertNotIn('221261102657708', text)
        self.assertNotIn('Order ID', text)
        self.assertNotIn('Execution Confirmed:', text)

    def test_warning_only_above_threshold(self):
        meta = dict(META, quote_age_sec=1.75)
        self.assertNotIn('⚠️ Yahoo', format_order_submitted(meta, 2))
        meta['quote_age_sec'] = 2.00
        self.assertNotIn('⚠️ Yahoo', format_order_submitted(meta, 2))
        meta['quote_age_sec'] = None
        self.assertNotIn('⚠️ Yahoo', format_order_submitted(meta, 2))

    def test_executed_separate_notification_and_elapsed(self):
        rec = self.rec
        rec['protected_qty'] = 1
        self.assertTrue(self.reporter.process(rec, {'orderStatus': 'TRADED', 'filledQty': 1, 'averageTradedPrice': 15.55}, self.observed))
        self.assertEqual(len(self.db.events), 1)
        text = self.db.events[(rec['order_id'], 'TRADED')]
        self.assertIn('✅ <b>Status: Order Executed</b>', text)
        self.assertIn('₹15.55', text)
        self.assertIn('₹15.55', text)
        self.assertIn('⏱️ Signal → Execution Confirmed: 1.42 s', text)
        self.assertIn('🛡️ Protection: ✅ Verified Active', text)
        self.assertNotIn(rec['order_id'], text)
        self.assertNotIn('Dhan Order', text)

    def test_pending_poll_never_confirms(self):
        self.assertTrue(self.reporter.process(self.rec, {'orderStatus': 'TRANSIT','filledQty':0}, self.observed))
        self.assertTrue(self.reporter.process(self.rec, {'orderStatus': 'PENDING','filledQty':0}, self.observed))
        self.assertFalse(self.db.events)

    def test_partial_fill_deduplicated(self):
        self.rec['qty'] = 3
        order = {'orderStatus': 'PART_TRADED', 'filledQty': 1, 'averageTradedPrice': 15.55}
        self.assertTrue(self.reporter.process(self.rec, order, self.observed))
        self.assertTrue(self.reporter.process(self.rec, order, self.observed + 1))
        self.assertEqual(len(self.db.events), 1)
        self.assertIn('Filled: 1/3', next(iter(self.db.events.values())))
        self.assertNotIn('Status: Order Executed', next(iter(self.db.events.values())))

    def test_terminal_failure_reports_actual_fills(self):
        for status in ('REJECTED', 'CANCELLED', 'EXPIRED'):
            self.assertTrue(self.reporter.process(self.rec, {'orderStatus': status, 'filledQty': 0}, self.observed))
            self.assertIn(f'FINAL_{status}_0', {key[1] for key in self.db.events})
        self.assertEqual(len(self.db.events), 3)

    def test_unverified_price_not_called_executed(self):
        self.assertFalse(self.reporter.process(self.rec, {'orderStatus': 'TRADED', 'filledQty': 1}, self.observed))
        text = self.db.events[(self.rec['order_id'], 'UNVERIFIED_EXECUTION')]
        self.assertIn('Fill Verification Pending', text)
        self.assertNotIn('Status: Order Executed', text)

    def test_fallback_broker_trade_avg(self):
        self.broker.avg = 15.58
        self.rec['protected_qty'] = 1
        self.assertTrue(self.reporter.process(self.rec, {'orderStatus': 'TRADED', 'filledQty': 1}, self.observed))
        self.assertEqual(self.broker.reads, 1)
        self.assertIn('₹15.58', self.db.events[(self.rec['order_id'], 'TRADED')])

    def test_protection_followup_only_after_pending(self):
        order = {'orderStatus': 'TRADED', 'filledQty': 1, 'averageTradedPrice': 15.53}
        self.reporter.process(self.rec, order, self.observed)
        self.assertIn('Pending Broker Verification', self.db.events[(self.rec['order_id'], 'TRADED')])
        self.rec['protected_qty'] = 1
        self.reporter.process(self.rec, order, self.observed + 1)
        self.assertIn((self.rec['order_id'], 'PROTECTION_VERIFIED'), self.db.events)
        self.reporter.process(self.rec, order, self.observed + 2)
        self.assertEqual(len(self.db.events), 2)

    def test_super_closed_requires_tradebook_evidence(self):
        self.rec['is_super_order'] = True
        self.assertFalse(self.reporter.process(self.rec, {'orderStatus': 'CLOSED', 'filledQty': 1}, self.observed))
        self.assertNotIn((self.rec['order_id'], 'TRADED'), self.db.events)
        self.broker.avg = 15.57
        self.rec['protected_qty'] = 1
        self.assertTrue(self.reporter.process(self.rec, {'orderStatus': 'CLOSED', 'filledQty': 1}, self.observed))
        self.assertIn('Status: Order Executed', self.db.events[(self.rec['order_id'], 'TRADED')])

    def test_html_escape(self):
        meta = dict(META, symbol='<script>', source='RDHS & group')
        text = format_order_submitted(meta, 1)
        self.assertNotIn('<script>', text)
        self.assertIn('&lt;script&gt;', text)
        self.assertIn('RDHS &amp; group', text)

    def test_incorrect_filled_qty_never_confirmed(self):
        self.assertFalse(self.reporter.process(self.rec, {'orderStatus': 'TRADED','filledQty': 2, 'averageTradedPrice': 15}, self.observed))
        self.assertFalse(self.db.events)

    def test_broker_traded_with_inconsistent_quantity_is_not_executed(self):
        self.rec['qty'] = 3
        self.assertFalse(self.reporter.process(
            self.rec, {'orderStatus': 'TRADED', 'filledQty': 1, 'averageTradedPrice': 15.55}, self.observed
        ))
        text = self.db.events[(self.rec['order_id'], 'UNVERIFIED_EXECUTION')]
        self.assertNotIn('Status: Order Executed', text)
        self.assertIn('Fill Verification Pending', text)

    def test_partially_filled_then_cancelled_reports_fill_value(self):
        self.rec['qty'] = 3
        self.assertTrue(self.reporter.process(
            self.rec, {'orderStatus': 'CANCELLED', 'filledQty': 1, 'averageTradedPrice': 15.55}, self.observed
        ))
        text = self.db.events[(self.rec['order_id'], 'FINAL_CANCELLED_1')]
        self.assertIn('Average Filled Price: ₹15.55', text)
        self.assertIn('Filled: 1/3', text)

    def test_unknown_transport_message(self):
        text = format_order_failed(META, 1, uncertain=True)
        self.assertIn('Submission Unconfirmed', text)
        self.assertNotIn('Order Rejected', text)

    def test_no_metadata_no_notice(self):
        self.rec['notification_meta'] = None
        self.assertTrue(self.reporter.process(self.rec, {'orderStatus': 'TRADED','filledQty':1,'averageTradedPrice':15}, self.observed))
        self.assertEqual(len(self.db.events), 0)


class TestRepositoryIntegration(unittest.TestCase):
    """These run against the actual repo after patch application.

    The standalone bundle skips them because it intentionally does not copy
    all production modules into this delivery artifact.
    """
    def test_sqlite_outbox_unique_claim(self):
        try:
            from src.journal.db import JournalDB
        except ImportError:
            self.skipTest("Run this integration test from the AuditBot checkout")
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            db = JournalDB(str(Path(tmp) / "trade_journal.db"))
            self.assertTrue(db.enqueue_order_notification_once("oid-1", "TRADED", "Executed"))
            self.assertFalse(db.enqueue_order_notification_once("oid-1", "TRADED", "Duplicate"))
            rows = db.fetch_pending_outbox()
            self.assertEqual(len(rows), 1)
            self.assertIn('Executed', rows[0]['payload'])

    def test_watcher_reuses_existing_polling_path(self):
        try:
            from src.engine.protection_watcher import ProtectionWatcher
        except ImportError:
            self.skipTest("Run this integration test from the AuditBot checkout")
        import tempfile
        from pathlib import Path
        broker = FakeBroker()
        broker.order_status = lambda oid: {'orderStatus': 'TRADED', 'filledQty': 1, 'averageTradedPrice': 15.55}
        with tempfile.TemporaryDirectory() as tmp:
            watcher = ProtectionWatcher(
                broker, path=str(Path(tmp) / 'watcher.json'), clock=lambda: META['signal_received_at'] + 1.42,
                dry_run=False, order_timeout_sec=0, auto_retry_enabled=False,
            )
            watcher.track(
                order_id='oid-2', symbol='YESBANK', security_id='1', segment='NSE_EQ',
                qty=1, product_type='CNC', notification_meta=dict(META),
            )
            # Suppress protection writes (not part of notification tests).
            def pretend_protected(rec, status, filled, new_qty):
                rec['protected_qty'] = filled
                return True
            watcher._protect = pretend_protected
            db = FakeDB()
            watcher.execution_reporter = OrderStatusReporter(db, broker)
            watcher.check_once()
            self.assertIn(('oid-2', 'TRADED'), db.events)
            self.assertNotIn('oid-2', watcher.pending)
            watcher.check_once()
            self.assertEqual(len(db.events), 1)


if __name__ == '__main__':
    unittest.main()
