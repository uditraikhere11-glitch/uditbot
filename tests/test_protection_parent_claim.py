import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from src.journal.db import JournalDB, write_protection_order


class ProtectionParentClaimTests(unittest.TestCase):
    def test_concurrent_claim_and_replay_preserve_first_parent_and_row(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = JournalDB(str(Path(directory) / "journal.db"))
            def claim(parent, timestamp, prices=(90, 89, 110)):
                with journal._get_conn() as conn:
                    write_protection_order(conn, parent_id=parent, protection_id="G", symbol="S",
                                           sl_trigger=prices[0], sl_limit=prices[1], target_price=prices[2], quantity=1,
                                           status="ACTIVE", created_at=timestamp)

            def try_claim(parent):
                try:
                    claim(parent, 1)
                    return parent
                except ValueError:
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(try_claim, ("A", "B")))
            self.assertEqual(results.count(None), 1)
            winner = next(parent for parent in results if parent)
            claim(winner, 2, prices=(0, 0, 0))
            with journal._get_conn() as conn:
                rows = conn.execute("SELECT parent_order_id, created_at, sl_trigger, sl_limit, target_price FROM protection_orders").fetchall()
            self.assertEqual([tuple(row) for row in rows], [(winner, 1, 90, 89, 110)])

    def test_watcher_refuses_to_credit_or_repeat_group_claim(self):
        from tests.test_step14_step15 import TestStep14And15

        fixture = TestStep14And15()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        watcher = fixture.watcher
        fixture.mock_broker.reconciliation_gate = fixture.gate
        for parent in ("A", "B"):
            watcher.track(order_id=parent, symbol="S", security_id="1", segment="NSE_EQ",
                          qty=1, product_type="CNC")
            watcher._record_confirmed_protection(
                watcher.pending[parent], "G", 1, 100, 100, 1,
                SimpleNamespace(sl_trigger=90, sl_limit=89, target_price=110), "ACTIVE_CONFIRMED", False,
            )
        self.assertEqual(watcher.pending["A"]["protected_qty"], 1)
        rec = watcher.pending["B"]
        self.assertEqual(rec["protected_qty"], 0)
        self.assertTrue(rec["reconciliation_required"])
        self.assertNotIn("G", rec.get("confirmed_protection_ids", []))
        self.assertEqual(fixture.gate.state.value, "BLOCKED")
        self.assertFalse(watcher._protect(rec, {}, 1, 1))
        fixture.mock_broker.place_forever_oco.assert_not_called()
