"""Offline crash, duplicate, conflicting evidence and replay tests."""
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path

from src.engine.shadow_event_ledger import ShadowEventLedger, ShadowLedgerConflict
from src.engine.shadow_execution_events import BrokerEvent


def status(event_id="A", status="OPEN", qty=0):
    return BrokerEvent(
        event_id=event_id, account_id="ACC", broker_order_id="OID",
        security_id="SEC", exchange_segment="NSE_EQ", product_type="CNC",
        kind="STATUS", status=status, cumulative_filled_qty=qty,
    )


class TestShadowLedger(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.p=Path(self.tmp.name)/"events.shadow.sqlite3"

    def tearDown(self):
        self.tmp.cleanup()

    def test_requires_separate_explicit_shadow_filename(self):
        with self.assertRaises(ValueError):
            ShadowEventLedger(str(Path(self.tmp.name)/"live.sqlite3"),account_id="ACC")

    def test_commit_and_restart_replay_same_state(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            self.assertEqual(ledger.append(status(qty=2)),"PERSISTED")
            self.assertEqual(ledger.append(status(event_id="B",status="TRADED",qty=5)),
                             "PERSISTED")
            self.assertEqual(ledger.count(),2)
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            proj,n=ledger.replay_order(
                broker_order_id="OID",security_id="SEC",exchange_segment="NSE_EQ",
                product_type="CNC",requested_qty=5)
            self.assertEqual(n,2)
            self.assertEqual(proj.cumulative_filled_qty,5)
            self.assertEqual(proj.status,"FILLED_UNVERIFIED_POSITION")

    def test_existing_event_exact_duplicate_is_idempotent(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            self.assertEqual(ledger.append(status()),"PERSISTED")
            self.assertEqual(ledger.append(status()),"DUPLICATE")
            self.assertEqual(ledger.count(),1)

    def test_changed_event_id_same_payload_fails_closed(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            ledger.append(status(status="OPEN"))
            with self.assertRaises(ShadowLedgerConflict):
                ledger.append(status(status="CANCELLED"))
            self.assertEqual(ledger.count(),1)

    def test_power_loss_before_commit_atomic_rollback(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            ledger.append(status())
            def die():
                raise OSError("simulated crash before durable commit")
            with self.assertRaises(OSError):
                ledger.append(status(event_id="B",qty=2),before_commit=die)
            self.assertEqual(ledger.count(),1)
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            self.assertEqual(ledger.count(),1)
            self.assertEqual(ledger.append(status(event_id="B",qty=2)),"PERSISTED")
            self.assertEqual(ledger.count(),2)

    def test_wrong_account_never_mixes_ledgers(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            ledger.append(status())
        with self.assertRaises(ShadowLedgerConflict):
            ShadowEventLedger(str(self.p),account_id="OTHER")

    def test_incoming_event_wrong_account_rejected(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            event=BrokerEvent(event_id="T",account_id="OTHER",broker_order_id="OID",
                security_id="SEC",exchange_segment="NSE_EQ",product_type="CNC",kind="UNKNOWN")
            with self.assertRaises(ShadowLedgerConflict):
                ledger.append(event)
            self.assertEqual(ledger.count(),0)

    def test_replay_inconsistent_journal_identity_fails_closed(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            ledger.append(status())
            with self.assertRaises(ShadowLedgerConflict):
                ledger.replay_order(broker_order_id="OID",security_id="OTHER",
                    exchange_segment="NSE_EQ",product_type="CNC",requested_qty=5)

    def test_replay_bound_is_mandatory(self):
        with ShadowEventLedger(str(self.p),account_id="ACC") as ledger:
            ledger.append(status())
            ledger.append(status(event_id="B",qty=2))
            with self.assertRaises(ShadowLedgerConflict):
                ledger.replay_order(broker_order_id="OID",security_id="SEC",
                    exchange_segment="NSE_EQ",product_type="CNC",
                    requested_qty=5,max_events=1)

    def test_shadow_file_permissions_are_restrictive(self):
        with ShadowEventLedger(str(self.p),account_id="ACC"):
            pass
        self.assertEqual(stat.S_IMODE(self.p.stat().st_mode),0o600)
