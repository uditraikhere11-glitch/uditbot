"""LIVE mutation safety: remote 429 cannot prove no broker side effect.

All requests are mocked before transport; no broker calls, credentials or
production DB. In particular, local governor deferrals are *known* no-send,
whereas a received 429 after HTTP POST/DELETE/PUT is an uncertain outcome.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests

from src.broker.dhan_client import DhanClient
from src.models import (
    OrderRequest, OrderType, ProductType, ProtectionLevels,
    TradingMode, TransactionType,
)


def response(status=429, *, local=False):
    r=requests.Response()
    r.status_code=status
    r._content=b'{"errorCode":"DH-904","errorMessage":"rate limited"}'
    if local:
        r.headers["X-AuditBot-Local-Deferral"]="1"
    return r


def request(product=ProductType.CNC,segment="NSE_EQ"):
    return OrderRequest(
        correlation_id="M1-TEST-429",symbol="SYNTHETIC-EQ",
        security_id="123",exchange_segment=segment,
        transaction_type=TransactionType.BUY,product_type=product,
        order_type=OrderType.LIMIT,price=100.0,quantity=5,
        trading_mode=TradingMode.LIVE,
    )


class TestRemote429Mutation(unittest.TestCase):
    def setUp(self):
        self.broker=object.__new__(DhanClient)
        self.broker.cfg=SimpleNamespace(
            dhan_client_id="SYNTHETIC", data_dir="/offline",
            trading_mode=TradingMode.LIVE,
            equity_amo_enabled=False, fno_amo_enabled=False,
        )
        self.broker.stop_manager=Mock()
        self.broker.stop_manager.try_begin_mutation.return_value=(True,{})
        self.broker.reconciliation_gate=SimpleNamespace(is_accepting_entries=True)
        self.broker._broker_mutating_http=Mock(return_value=response())
        self.broker._headers=Mock(return_value={})
        self.broker.is_token_error=Mock(return_value=False)
        self.broker._reconcile_uncertain_submission=Mock(
            side_effect=AssertionError("remote 429 should not trigger immediate GET storm")
        )
        self.patch=patch(
            "src.engine.config_manager.ConfigChangeTracker"
        )
        tracker=self.patch.start()
        self.addCleanup(self.patch.stop)
        tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,"OK")

    def test_regular_entry_remote_429_retains_unknown_fence(self):
        ok,order_id,msg=self.broker.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(order_id)
        self.assertIn("UNKNOWN",msg)
        self.assertIn("429",msg)
        self.assertNotIn("rate limited",msg.lower())
        self.broker.stop_manager.finalize_mutation.assert_called_once_with(
            "M1-TEST-429",keep_persisted_if_uncertain=True,
        )
        self.broker._broker_mutating_http.assert_called_once()

    def test_super_order_remote_429_retains_unknown_fence(self):
        ok,order_id,msg=self.broker.place_super_order(
            request(product=ProductType.MARGIN,segment="NSE_FNO"),
            stop_loss_price=95,target_price=105,
        )
        self.assertFalse(ok)
        self.assertIsNone(order_id)
        self.assertIn("UNKNOWN",msg)
        self.broker.stop_manager.finalize_mutation.assert_called_once_with(
            "M1-TEST-429",keep_persisted_if_uncertain=True,
        )
        self.broker._broker_mutating_http.assert_called_once()

    def test_local_429_is_different_known_no_send(self):
        self.broker._broker_mutating_http.return_value=response(local=True)
        ok,oid,msg=self.broker.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertIn("NOT_SUBMITTED",msg)
        self.assertNotIn("outcome UNKNOWN",msg)
        self.broker.stop_manager.finalize_mutation.assert_called_once_with(
            "M1-TEST-429",keep_persisted_if_uncertain=False,
        )

    def test_remote_400_remains_definitive_rejection(self):
        self.broker._broker_mutating_http.return_value=response(status=400)
        ok,oid,msg=self.broker.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertIn("rejected",msg)
        self.broker.stop_manager.finalize_mutation.assert_called_once_with(
            "M1-TEST-429",keep_persisted_if_uncertain=False,
        )

    def test_forever_oco_sell_remote_429_never_claims_no_send(self):
        levels=ProtectionLevels(
            executed_price=100,sl_trigger=95,sl_limit=94.95,
            target_price=105,quantity=5,target_trigger=105,
        )
        ok,oid,msg=self.broker.place_forever_oco(
            correlation_id="M1-PROTECT",security_id="123",
            exchange_segment="NSE_EQ",product_type="CNC",
            levels=levels,max_protectable_qty=5,
        )
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertIn("UNKNOWN",msg)
        self.assertEqual(self.broker._broker_mutating_http.call_count,1)

    def test_super_leg_modification_remote_429_has_typed_unknown(self):
        ok,msg,data=self.broker.modify_super_order_leg(
            "SUPER-ONE","STOP_LOSS_LEG",price=95,
        )
        self.assertFalse(ok)
        self.assertEqual(data["state"],"UNKNOWN")
        self.assertEqual(data["status_code"],429)
        self.broker._broker_mutating_http.assert_called_once()

    def test_regular_cancel_remote_429_requires_readback(self):
        ok,msg=self.broker.cancel_order("ORDER-ONE")
        self.assertFalse(ok)
        self.assertIn("UNKNOWN",msg)
        self.assertEqual(self.broker._broker_mutating_http.call_count,1)

    def test_super_cancel_remote_429_requires_readback(self):
        ok,msg=self.broker.cancel_super_order_leg("SUPER-ONE","ENTRY_LEG")
        self.assertFalse(ok)
        self.assertIn("UNKNOWN",msg)
        self.assertEqual(self.broker._broker_mutating_http.call_count,1)

    def test_forever_cancel_remote_429_requires_readback(self):
        ok,msg=self.broker.cancel_forever_order("FOREVER-ONE")
        self.assertFalse(ok)
        self.assertIn("UNKNOWN",msg)
        self.assertEqual(self.broker._broker_mutating_http.call_count,1)
