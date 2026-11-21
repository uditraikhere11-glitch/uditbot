"""Offline adversarial event replay for shadow-only order projections."""
import unittest
from src.engine.shadow_execution_events import BrokerEvent, EvidenceConflict, OrderProjection


def ev(**kwargs):
    d=dict(event_id="e1",account_id="ACC",broker_order_id="OID",security_id="SEC",
           exchange_segment="NSE_EQ",product_type="CNC",kind="STATUS",status="OPEN")
    d.update(kwargs)
    return BrokerEvent(**d)


class TestEventProjection(unittest.TestCase):
    def setUp(self):
        self.p=OrderProjection("ACC","OID","SEC","NSE_EQ","CNC",5)

    def test_exact_duplicates(self):
        x=ev()
        self.assertEqual(self.p.apply(x),"APPLIED")
        self.assertEqual(self.p.apply(x),"DUPLICATE")
        self.assertEqual(self.p.cumulative_filled_qty,0)

    def test_event_identity_conflict(self):
        self.p.apply(ev())
        with self.assertRaises(EvidenceConflict):
            self.p.apply(ev(status="CANCELLED"))
        self.assertTrue(self.p.reconciliation_required)

    def test_snapshot_and_trades_not_double_counted(self):
        self.p.apply(ev(cumulative_filled_qty=3))
        self.p.apply(ev(event_id="t1",kind="TRADE",status=None,exchange_trade_id="X1",traded_qty=2))
        self.p.apply(ev(event_id="t2",kind="TRADE",status=None,exchange_trade_id="X2",traded_qty=1))
        self.assertEqual(self.p.cumulative_filled_qty,3)
        self.assertEqual(self.p.status,"PARTIAL_FILL")

    def test_same_trade_two_sources_deduped(self):
        self.p.apply(ev(event_id="ws",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=2))
        self.p.apply(ev(event_id="rest",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=2))
        self.assertEqual(self.p.unique_traded_qty,2)

    def test_conflicting_trade_requires_reconciliation(self):
        self.p.apply(ev(event_id="ws",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=2))
        with self.assertRaises(EvidenceConflict):
            self.p.apply(ev(event_id="rest",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=3))
        self.assertTrue(self.p.reconciliation_required)

    def test_late_fill_after_cancel_stays_unresolved(self):
        self.p.apply(ev(status="CANCELLED",cumulative_filled_qty=0))
        self.p.apply(ev(event_id="late",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=2))
        self.assertEqual(self.p.status,"PARTIAL_TERMINAL")
        self.assertEqual(self.p.unfilled_qty,3)
        self.assertTrue(self.p.reconciliation_required)

    def test_invalid_cumulative_values(self):
        for n in (-1,6,True,1.5):
            with self.subTest(n=n), self.assertRaises(EvidenceConflict):
                self.p.apply(ev(cumulative_filled_qty=n))

    def test_identity_mismatch(self):
        with self.assertRaises(EvidenceConflict):
            self.p.apply(ev(account_id="WRONG"))
        self.assertTrue(self.p.reconciliation_required)

    def test_unknown_remains_unresolved_after_ack(self):
        self.p.apply(ev(kind="UNKNOWN",status=None))
        self.p.apply(ev(event_id="ack"))
        self.assertTrue(self.p.reconciliation_required)

    def test_late_open_does_not_reopen_cancelled(self):
        self.p.apply(ev(status="CANCELLED"))
        self.p.apply(ev(event_id="late",status="OPEN"))
        self.assertEqual(self.p.status,"CANCELLED")
        self.assertTrue(self.p.reconciliation_required)

    def test_conflicting_terminal(self):
        self.p.apply(ev(status="CANCELLED"))
        self.p.apply(ev(event_id="late",status="REJECTED"))
        self.assertTrue(self.p.reconciliation_required)

    def test_full_report_does_not_close_account_position(self):
        self.p.apply(ev(status="TRADED",cumulative_filled_qty=5))
        self.assertEqual(self.p.status,"FILLED_UNVERIFIED_POSITION")

    def test_invalid_starting_quantity(self):
        with self.assertRaises(ValueError):
            OrderProjection("ACC","OID","SEC","NSE_EQ","CNC",0)

    def test_overfill(self):
        self.p.apply(ev(event_id="t1",kind="TRADE",status=None,exchange_trade_id="X",traded_qty=4))
        with self.assertRaises(EvidenceConflict):
            self.p.apply(ev(event_id="t2",kind="TRADE",status=None,exchange_trade_id="Y",traded_qty=3))

    def test_terminal_fill_qty_contradictions(self):
        self.p.apply(ev(status="FILLED",cumulative_filled_qty=0))
        self.assertTrue(self.p.reconciliation_required)

    def test_terminal_cancellation_after_fill_requires_review(self):
        self.p.apply(ev(status="TRADED",cumulative_filled_qty=5))
        self.p.apply(ev(event_id="cancel",status="CANCELLED"))
        self.assertTrue(self.p.reconciliation_required)
