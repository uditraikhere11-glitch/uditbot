"""Read-only WebSocket reconnect, silence and credential-isolation gates.

All tests are offline. Every reconnect remains non-authoritative, and
an application-silent WebSocket is a gap even if ping/pong stays healthy.
"""
import asyncio
import json
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from src.engine.dhan_order_observer import (
    DhanShadowWebSocket, OwnedOrder, ShadowOrderObserver,
)


class Owner:
    def find(self, oid):
        if oid == "OID":
            return OwnedOrder("ACC", "OID", "SEC", "NSE_EQ", "CNC", 5)
        return None


class TestSupervisedReadOnlyStream(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.observer = ShadowOrderObserver("ACC", Owner())
        self.client = DhanShadowWebSocket(self.observer)

    async def test_supervised_mode_cannot_connect_without_opt_in(self):
        with patch.object(self.client,"run_once",new_callable=AsyncMock) as run:
            with self.assertRaises(PermissionError):
                await self.client.run_supervised(access_token="TEST_TOKEN")
            run.assert_not_called()

    async def test_bounded_reconnect_never_clears_authoritative_gap(self):
        calls=[]
        async def fake_once(**kwargs):
            self.observer.connection_lost("OFFLINE_SIMULATED_DISCONNECT")
            calls.append(kwargs)
            return self.observer.summary()
        with (patch.object(self.client,"run_once",side_effect=fake_once),
              patch("src.engine.dhan_order_observer.asyncio.sleep",
                    new_callable=AsyncMock) as sleeper):
            result=await self.client.run_supervised(
                access_token="TEST_TOKEN",network_opt_in=True,
                max_sessions=4,max_messages_per_session=6,
                initial_backoff_sec=1,max_backoff_sec=2,
            )
        self.assertEqual(len(calls),4)
        self.assertEqual(result["stream_gaps"],4)
        self.assertEqual(result["completed_observation_sessions"],4)
        self.assertEqual(result["reconnect_attempts"],3)
        self.assertEqual([x.args[0] for x in sleeper.await_args_list],[1,2,2])
        self.assertTrue(result["needs_authoritative_rest_sync"])
        self.assertFalse(result["may_authorize_orders"])
        self.assertFalse(result["may_modify_reconciliation_gate"])
        self.assertFalse(result["may_assert_verified_fills"])
        self.assertNotIn("TEST_TOKEN",json.dumps(result))

    async def test_idle_websocket_times_out_as_gap(self):
        class FakeConnection:
            async def __aenter__(self): return self
            async def __aexit__(self,*args): return False
            async def send(self,body): pass
            async def recv(self):
                await asyncio.sleep(10)
                return '{"Type":"ping"}'
        seen=[]
        async def timeout(awaitable,timeout):
            seen.append(timeout)
            awaitable.close()
            raise asyncio.TimeoutError()
        with (patch.dict(sys.modules,{"websockets":types.SimpleNamespace(
                connect=lambda *args,**kwargs: FakeConnection())}),
              patch("src.engine.dhan_order_observer.asyncio.wait_for",
                    side_effect=timeout)):
            result=await self.client.run_once(
                access_token="NEVER_PRINT_TOKEN",network_opt_in=True,
                max_messages=5,idle_timeout_sec=3,
            )
        self.assertEqual(seen,[3])
        self.assertEqual(result["stream_gaps"],1)
        self.assertIn("STREAM_GAP_TIMEOUTERROR",
                      [x.upper() for x in result["reason_codes"]])
        self.assertTrue(result["needs_authoritative_rest_sync"])
        self.assertNotIn("NEVER_PRINT_TOKEN",json.dumps(result))

    async def test_invalid_limits_do_not_connect(self):
        invalid=(
            {"max_sessions":0}, {"max_sessions":101},
            {"initial_backoff_sec":0}, {"max_backoff_sec":float("nan")},
            {"initial_backoff_sec":6,"max_backoff_sec":2},
            {"idle_timeout_sec":float("inf")},
            {"max_messages_per_session":0},
        )
        with patch.object(self.client,"run_once",new_callable=AsyncMock) as run:
            for kwargs in invalid:
                with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):
                    await self.client.run_supervised(
                        access_token="FAKE",network_opt_in=True,**kwargs,
                    )
            run.assert_not_called()

    async def test_single_socket_idle_limits_rejected_before_import(self):
        for value in (True,0,float("inf"),float("nan"),601):
            with self.subTest(value=value),self.assertRaises(ValueError):
                await self.client.run_once(
                    access_token="FAKE",network_opt_in=True,
                    idle_timeout_sec=value,
                )
