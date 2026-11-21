"""Restart and crash regressions for private synthetic SQLite evidence stores."""
from contextlib import closing
from multiprocessing import get_context
from pathlib import Path
import os
import sqlite3
import tempfile
import unittest

from src.engine.durable_ingress_audit import DurableIngressAudit, IngressAuditConflict
from src.engine.shadow_event_ledger import ShadowEventLedger, ShadowLedgerConflict
from tests.test_execution_modernization_shadow_ledger import status


def interrupt_append(path, committed):
    with ShadowEventLedger(str(path), account_id="ACC") as db:
        db.append(status(), before_commit=None if committed else lambda: os._exit(31))
        os._exit(32)


class LedgerBinding(unittest.TestCase):
    def test_crash_before_and_after_commit(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "test.shadow.sqlite3"
            for committed, expected in [(False, 31), (True, 32)]:
                child = get_context("spawn").Process(target=interrupt_append, args=(path, committed))
                child.start()
                try:
                    child.join(10)
                    self.assertEqual(child.exitcode, expected)
                finally:
                    if child.is_alive():
                        child.kill()
                        child.join()
                    child.close()
                with ShadowEventLedger(str(path), account_id="ACC") as db:
                    self.assertEqual(db.count(), int(committed))

    def test_damaged_store_is_never_claimed_or_repaired(self):
        for factory, meta, evidence, conflict in [
            (DurableIngressAudit, "ingress_meta", "ingress_messages", IngressAuditConflict),
            (ShadowEventLedger, "shadow_metadata", "shadow_events", ShadowLedgerConflict),
        ]:
            for damage in ("empty_file", "empty_unbound", "partial_schema", "lost_binding",
                           "lost_evidence", "wrong_account"):
                with self.subTest(store=meta, damage=damage), tempfile.TemporaryDirectory() as root:
                    path = Path(root) / ("test" + factory.SUFFIX)
                    if damage == "empty_file":
                        path.touch(mode=0o600)
                    else:
                        with factory(str(path), account_id="ACC") as db:
                            if damage != "empty_unbound":
                                if factory is ShadowEventLedger:
                                    db.append(status())
                                else:
                                    db.admit(channel_id=-123, message_id=1, text="fixture",
                                             message_timestamp=1700000000)
                        with closing(sqlite3.connect(path)) as conn:
                            if damage in ("empty_unbound", "lost_binding"):
                                conn.execute("DELETE FROM " + meta)
                            elif damage == "partial_schema":
                                conn.execute("DROP TABLE " + meta)
                            elif damage == "lost_evidence":
                                conn.execute("DROP TABLE " + evidence)
                            conn.commit()
                    before = path.read_bytes()
                    accounts = ["OTHER"] if damage == "wrong_account" else ["ACC", "OTHER"]
                    for account in accounts:
                        with self.assertRaises(conflict):
                            factory(str(path), account_id=account)
                        self.assertEqual(path.read_bytes(), before)
