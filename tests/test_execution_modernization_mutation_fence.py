"""Real durable STOP-file regression: unresolved correlation cannot re-POST.

No live broker access. All mutations are mocked; actual EmergencyStopManager
file locks and on-disk JSON survive simulated process restarts.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace as NS
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

from src.broker.dhan_client import DhanClient
from src.engine.emergency_stop import EmergencyStopManager
from src.models import (
    OrderRequest,OrderType,ProductType,TradingMode,TransactionType,
)


ACCOUNT="123456"


def stop(path):
    return EmergencyStopManager(
        state_file=str(path),expected_account_id=ACCOUNT,
        fail_closed_if_missing=True,
    )


def order(product=ProductType.CNC,segment="NSE_EQ"):
    return OrderRequest(
        correlation_id="CORR-UNKNOWN-429",symbol="TEST-EQ",
        security_id="123",exchange_segment=segment,
        transaction_type=TransactionType.BUY,
        product_type=product,order_type=OrderType.LIMIT,
        price=100,quantity=1,trading_mode=TradingMode.LIVE,
    )


def remote429():
    r=requests.Response()
    r.status_code=429
    r._content=b'{"errorCode":"DH-904","errorMessage":"remote rate limit"}'
    return r


def client(st):
    c=object.__new__(DhanClient)
    c.cfg=NS(dhan_client_id=ACCOUNT,data_dir="/offline",
             trading_mode=TradingMode.LIVE,
             equity_amo_enabled=False,fno_amo_enabled=False)
    c.stop_manager=st
    c.reconciliation_gate=NS(is_accepting_entries=True)
    c._broker_mutating_http=Mock(return_value=remote429())
    c._headers=Mock(return_value={})
    c.is_token_error=Mock(return_value=False)
    c._reconcile_uncertain_submission=Mock(
        side_effect=AssertionError("remote 429 cannot trigger immediate retry/read storm")
    )
    return c


class TestDurableCorrelationFence(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/"emergency-stop.json"
        self.first=stop(self.path)
        self.first.provision_initial_clean_state(
            operator="OfflineTest",account_id=ACCOUNT,
        )

    def test_one_correlation_cannot_be_acquired_twice(self):
        accepted,state=self.first.try_begin_mutation("CORR-ONE")
        self.assertTrue(accepted)
        blocked,reason=self.first.try_begin_mutation("CORR-ONE")
        self.assertFalse(blocked)
        self.assertEqual(reason["blocked_reason"],
                         "IN_FLIGHT_CORRELATION_UNRESOLVED")
        self.assertIn("CORR-ONE",self.first.get_in_flight())

    def test_restarted_process_sees_still_unresolved_mutation(self):
        self.assertTrue(self.first.try_begin_mutation("CORR-RESTART")[0])
        second=stop(self.path)
        ok,st=second.try_begin_mutation("CORR-RESTART")
        self.assertFalse(ok)
        self.assertEqual(st["blocked_reason"],
                         "IN_FLIGHT_CORRELATION_UNRESOLVED")
        self.assertFalse(second.is_stopped())  # Not a fabricated STOP state.

    def test_broker_verified_release_allows_future_authorized_admission(self):
        self.assertTrue(self.first.try_begin_mutation("CORR-CLEAR")[0])
        self.first.finalize_mutation(
            "CORR-CLEAR",keep_persisted_if_uncertain=True
        )
        second=stop(self.path)
        self.assertFalse(second.try_begin_mutation("CORR-CLEAR")[0])
        # Only explicit verified correlation recovery may release the fence.
        self.first.deregister_in_flight("CORR-CLEAR")
        self.assertTrue(second.try_begin_mutation("CORR-CLEAR")[0])

    def test_concurrent_same_correlation_one_wins(self):
        with ThreadPoolExecutor(max_workers=10) as pool:
            decisions=list(pool.map(
                lambda _: self.first.try_begin_mutation("CORR-RACE")[0],
                range(24),
            ))
        self.assertEqual(sum(decisions),1)
        self.assertEqual(self.first.get_in_flight().count("CORR-RACE"),1)

    def test_regular_entry_http429_persists_fence_across_restart(self):
        first_client=client(self.first)
        with patch("src.engine.config_manager.ConfigChangeTracker") as t:
            t.return_value.is_live_entry_blocked_by_transition.return_value=(False,"OK")
            ok,oid,msg=first_client.place_order(order())
            self.assertFalse(ok)
            self.assertIsNone(oid)
            self.assertIn("UNKNOWN",msg)
            self.assertIn("CORR-UNKNOWN-429",self.first.get_in_flight())
            # Replay before restart must not send a second broker mutation.
            again=first_client.place_order(order())
            self.assertFalse(again[0])
            self.assertIn("unresolved in-flight correlation",again[2])
            self.assertEqual(first_client._broker_mutating_http.call_count,1)
            # Simulated independent process restart with same durable STOP file.
            second_client=client(stop(self.path))
            after_restart=second_client.place_order(order())
            self.assertFalse(after_restart[0])
            self.assertIn("unresolved in-flight correlation",after_restart[2])
            second_client._broker_mutating_http.assert_not_called()

    def test_super_order_http429_persists_fence_across_restart(self):
        first_client=client(self.first)
        with patch("src.engine.config_manager.ConfigChangeTracker") as t:
            t.return_value.is_live_entry_blocked_by_transition.return_value=(False,"OK")
            args=(order(ProductType.MARGIN,"NSE_FNO"),)
            kwargs={"stop_loss_price":95,"target_price":105}
            first=first_client.place_super_order(*args,**kwargs)
            self.assertFalse(first[0])
            self.assertIn("UNKNOWN",first[2])
            second_client=client(stop(self.path))
            replay=second_client.place_super_order(*args,**kwargs)
            self.assertFalse(replay[0])
            self.assertIn("unresolved in-flight correlation",replay[2])
            second_client._broker_mutating_http.assert_not_called()
