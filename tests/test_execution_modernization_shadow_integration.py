"""Crash-consistent Dhan observer replay against isolated shadow store."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.engine.dhan_order_observer import OwnedOrder, ShadowOrderObserver
from src.engine.shadow_event_ledger import ShadowEventLedger


RAW={"Type":"order_alert","Data":{
    "ClientId":"ACC","OrderNo":"OID","SecurityId":"SEC",
    "Exchange":"NSE","Segment":"E","Product":"C","ProductName":"CNC",
    "Status":"PENDING","Quantity":5,"TradedQty":2,"RemainingQuantity":3,
    "LastUpdatedTime":"2026-11-21 11:00:00",
}}


class Owner:
    def find(self,oid):
        if oid=="OID":
            return OwnedOrder("ACC","OID","SEC","NSE_EQ","CNC",5)
        return None


class TestPersistentShadowObserver(unittest.TestCase):
    def test_replayed_status_is_idempotent_after_process_restart(self):
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/"status.shadow.sqlite3"
            with ShadowEventLedger(str(path),account_id="ACC") as ledger:
                obs=ShadowOrderObserver("ACC",Owner(),shadow_ledger=ledger)
                self.assertEqual(obs.observe(RAW),"OWNED_APPLIED")
                self.assertEqual(ledger.count(),1)
            with ShadowEventLedger(str(path),account_id="ACC") as ledger:
                restored=ShadowOrderObserver("ACC",Owner(),shadow_ledger=ledger)
                self.assertEqual(restored.observe(RAW),"OWNED_DUPLICATE")
                self.assertEqual(restored.projections["OID"].cumulative_filled_qty,2)
                self.assertEqual(ledger.count(),1)
                self.assertTrue(restored.needs_authoritative_rest_sync)

    def test_shadow_persist_error_stops_projection(self):
        class FailingLedger:
            account_id="ACC"
            def replay_order(self,**kw):
                from src.engine.shadow_execution_events import OrderProjection
                return OrderProjection("ACC","OID","SEC","NSE_EQ","CNC",5),0
            def append(self,event):
                raise OSError("shadow disk full")
        obs=ShadowOrderObserver("ACC",Owner(),shadow_ledger=FailingLedger())
        self.assertEqual(obs.observe(RAW),"REJECTED")
        self.assertEqual(obs.projections["OID"].cumulative_filled_qty,0)
        self.assertIn("SHADOW_PERSISTENCE_UNVERIFIED",obs.reasons)

    def test_shadow_account_mismatch_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            ShadowOrderObserver("ACC",Owner(),
                                shadow_ledger=SimpleNamespace(account_id="OTHER"))
