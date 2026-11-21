"""Adversarial read-only Dhan stream observer tests; no network access."""
import asyncio
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from src.engine.dhan_order_observer import (
    DhanShadowWebSocket, JournalOwnership, OwnedOrder, ShadowOrderObserver,
)


BASE={"Type":"order_alert","Data":{
    "ClientId":"ACC","OrderNo":"OID","SecurityId":"SEC",
    "Exchange":"NSE","Segment":"E","Product":"C","ProductName":"CNC",
    "Status":"PENDING","Quantity":5,"TradedQty":2,"RemainingQuantity":3,
    "LastUpdatedTime":"2026-11-21 10:00:00",
}}


def msg(**updates):
    data=dict(BASE["Data"])
    data.update(updates)
    return {"Type":"order_alert","Data":data}


class Owner:
    def __init__(self, entry=None, exc=None):
        self.entry=entry or OwnedOrder("ACC","OID","SEC","NSE_EQ","CNC",5)
        self.exc=exc
    def find(self, oid):
        if self.exc:
            raise self.exc
        return self.entry if oid=="OID" else None


class TestObserver(unittest.TestCase):
    def setUp(self):
        self.obs=ShadowOrderObserver("ACC",Owner())

    def test_matching_order_can_only_update_shadow(self):
        self.assertEqual(self.obs.observe(BASE),"OWNED_APPLIED")
        self.assertEqual(self.obs.projections["OID"].cumulative_filled_qty,2)
        summary=self.obs.summary()
        self.assertTrue(summary["needs_authoritative_rest_sync"])
        for key in ("may_authorize_orders","may_modify_reconciliation_gate",
                    "may_assert_verified_fills"):
            self.assertFalse(summary[key])

    def test_duplicate_snapshot_does_not_double_count(self):
        self.obs.observe(BASE)
        self.assertEqual(self.obs.observe(BASE),"OWNED_DUPLICATE")
        self.assertEqual(self.obs.duplicate_events,1)
        self.assertEqual(self.obs.projections["OID"].cumulative_filled_qty,2)

    def test_unowned_accounts_stay_ignored(self):
        self.assertEqual(self.obs.observe(msg(OrderNo="OTHER")),"UNOWNED_IGNORED")
        self.assertEqual(len(self.obs.projections),0)

    def test_account_spoof_rejected(self):
        self.assertEqual(self.obs.observe(msg(ClientId="WRONG")),"REJECTED")
        self.assertEqual(len(self.obs.projections),0)

    def test_journal_identity_conflict(self):
        self.obs.ownership=Owner(entry=OwnedOrder("ACC","OID","OTHER","NSE_EQ","CNC",5))
        self.assertEqual(self.obs.observe(BASE),"REJECTED")

    def test_journal_read_failure_is_not_an_empty_account(self):
        self.obs.ownership=Owner(exc=sqlite3.OperationalError("db read failed"))
        self.assertEqual(self.obs.observe(BASE),"REJECTED")
        self.assertIn("OWNERSHIP_LOOKUP_UNAVAILABLE",self.obs.summary()["reason_codes"])

    def test_out_of_order_broker_status_requires_review(self):
        self.obs.observe(msg(Status="CANCELLED"))
        self.obs.observe(msg(Status="PENDING",LastUpdatedTime="2026-11-21 10:01:00"))
        self.assertTrue(self.obs.projections["OID"].reconciliation_required)

    def test_unknown_message_type_no_trade_mutation(self):
        self.assertEqual(self.obs.observe({"Type":"ping"}),"NON_ORDER_MESSAGE")
        self.assertEqual(self.obs.received,1)
        self.assertEqual(len(self.obs.projections),0)

    def test_bad_cumulative_rejected(self):
        self.assertEqual(self.obs.observe(msg(TradedQty=9)),"REJECTED")

    def test_order_count_capacity_fail_closed(self):
        self.obs=ShadowOrderObserver("ACC",Owner(),max_orders=1)
        self.obs.observe(BASE)
        class TwoOwner:
            def find(self,oid):
                return OwnedOrder("ACC",oid,"SEC","NSE_EQ","CNC",5)
        self.obs.ownership=TwoOwner()
        self.assertEqual(self.obs.observe(msg(OrderNo="OID2")),"REJECTED")
        self.assertTrue(self.obs.needs_authoritative_rest_sync)

    def test_event_capacity_no_silent_eviction(self):
        self.obs=ShadowOrderObserver("ACC",Owner(),max_events_per_order=1)
        self.obs.observe(BASE)
        self.assertEqual(self.obs.observe(msg(TradedQty=3,RemainingQuantity=2)),"REJECTED")
        self.assertEqual(self.obs.projections["OID"].cumulative_filled_qty,2)

    def test_connection_loss_requires_reconciliation(self):
        before=self.obs.stream_gaps
        self.obs.connection_lost("NETWORK")
        self.assertEqual(self.obs.stream_gaps,before+1)
        self.assertTrue(self.obs.needs_authoritative_rest_sync)


class TestJournalOwner(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.db=Path(self.temp.name)/"journal.sqlite"
        with sqlite3.connect(self.db) as con:
            con.executescript("""
                CREATE TABLE order_intents (broker_order_id TEXT,account_id TEXT,
                  security_id TEXT,exchange_segment TEXT,product_type TEXT,quantity INTEGER);
                CREATE TABLE protection_attempts (broker_order_id TEXT,account_id TEXT,
                  security_id TEXT,exchange_segment TEXT,product_type TEXT,quantity INTEGER);
                INSERT INTO order_intents VALUES ('OID','ACC','SEC','NSE_EQ','CNC',5);
            """)

    def tearDown(self):
        self.temp.cleanup()

    def test_reads_durable_account_scoped_intent(self):
        before=self.db.read_bytes()
        o=JournalOwnership(str(self.db)).find("OID")
        self.assertEqual((o.account_id,o.security_id,o.requested_qty),("ACC","SEC",5))
        self.assertEqual(before,self.db.read_bytes())

    def test_missing_order_returns_none(self):
        self.assertIsNone(JournalOwnership(str(self.db)).find("OTHER"))

    def test_duplicate_claim_is_ambiguous(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO protection_attempts VALUES ('OID','ACC','SEC','NSE_EQ','CNC',5)")
        with self.assertRaises(ValueError):
            JournalOwnership(str(self.db)).find("OID")

    def test_missing_database_is_fail_closed(self):
        with self.assertRaises(FileNotFoundError):
            JournalOwnership(str(Path(self.temp.name)/"missing.sqlite"))


class TestNetworkOptIn(unittest.IsolatedAsyncioTestCase):
    async def test_network_is_off_by_default(self):
        obs=ShadowOrderObserver("ACC",Owner())
        ws=DhanShadowWebSocket(obs)
        with self.assertRaises(PermissionError):
            await ws.run_once(access_token="FAKE")
        self.assertEqual(obs.received,0)

    async def test_missing_token_never_connects(self):
        obs=ShadowOrderObserver("ACC",Owner())
        ws=DhanShadowWebSocket(obs)
        with self.assertRaises(ValueError):
            await ws.run_once(access_token="",network_opt_in=True)

    async def test_fake_websocket_auth_and_read_only_message(self):
        observer=ShadowOrderObserver("ACC",Owner())
        client=DhanShadowWebSocket(observer)
        sent=[]
        class FakeConnection:
            async def __aenter__(self):
                return self
            async def __aexit__(self,*args):
                return False
            async def send(self,payload):
                sent.append(json.loads(payload))
            async def recv(self):
                return json.dumps(BASE)
        m=types.SimpleNamespace(connect=lambda *args,**kwargs:FakeConnection())
        with patch.dict(sys.modules,{"websockets":m}):
            result=await client.run_once(access_token="TEMP_TOKEN",
                                         network_opt_in=True,max_messages=1)
        self.assertEqual(len(sent),1)
        self.assertEqual(sent[0]["LoginReq"]["MsgCode"],42)
        self.assertEqual(sent[0]["LoginReq"]["Token"],"TEMP_TOKEN")
        self.assertEqual(result["owned_observed_orders"],1)
        self.assertEqual(result["stream_gaps"],1)
        self.assertNotIn("TEMP_TOKEN",json.dumps(result))

    async def test_socket_failure_is_gap_and_no_credentials_in_report(self):
        obs=ShadowOrderObserver("ACC",Owner())
        client=DhanShadowWebSocket(obs)
        def failing(*args,**kwargs):
            raise RuntimeError("payload with secret SECRET")
        with patch.dict(sys.modules,{"websockets":types.SimpleNamespace(connect=failing)}):
            result=await client.run_once(access_token="SECRET",
                                         network_opt_in=True,max_messages=1)
        self.assertTrue(result["needs_authoritative_rest_sync"])
        self.assertNotIn("SECRET",json.dumps(result))
