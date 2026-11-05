"""F&O AMO goes to documented /orders; never mutates a live broker in tests."""
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from src.broker.dhan_client import DhanClient
from src.models import OrderRequest, ProductType, OrderType, TransactionType, TradingMode


def request(segment="NSE_FNO", product=ProductType.MARGIN, amo=True):
    return OrderRequest(correlation_id="OPT-HAL-123", symbol="HAL28NOV4750CE",
                        security_id="49201", exchange_segment=segment,
                        transaction_type=TransactionType.BUY, product_type=product,
                        order_type=OrderType.LIMIT, price=90.0, quantity=75,
                        trading_mode=TradingMode.LIVE, after_market_order=amo, amo_time="OPEN")


def client(fno=True, equity=False, status="PENDING", oid="112111182198"):
    c=object.__new__(DhanClient)
    c.cfg=SimpleNamespace(dhan_client_id="ACCT",fno_amo_enabled=fno,
                          equity_amo_enabled=equity,trading_mode=TradingMode.LIVE,
                          data_dir="/tmp/not-used")
    c.stop_manager=MagicMock()
    c.stop_manager.try_begin_mutation.return_value=(True,{})
    c.reconciliation_gate=MagicMock()
    c.reconciliation_gate.is_accepting_entries=True
    c._headers=MagicMock(return_value={})
    c.session=MagicMock()
    resp=c.session.post.return_value
    resp.status_code=201
    resp.text="ack"
    resp.json.return_value={"orderId":oid, "orderStatus":status}
    c.last_order_broker_status=None
    return c


class FrozenBrokerClock(unittest.TestCase):
    """Make after-hours tests independent of the CI runner's real wall clock."""
    def setUp(self):
        clock = patch("src.broker.dhan_client.datetime")
        frozen = clock.start()
        frozen.now.return_value = datetime(2026, 11, 5, 17, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
        self.addCleanup(clock.stop)


class NativeBrokerFnoAmoTests(FrozenBrokerClock):
    def test_posts_exact_fno_amo_schema(self):
        c=client()
        with patch("src.engine.market.market_status",return_value=(False,"Market Closed Post-Hours")), \
             patch("src.engine.config_manager.ConfigChangeTracker") as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,"")
            ok,oid,note=c.place_order(request())
        self.assertTrue(ok,note)
        self.assertEqual(oid,"112111182198")
        c.session.post.assert_called_once()
        args=c.session.post.call_args
        self.assertEqual(args.args[0],"https://api.dhan.co/v2/orders")
        payload=args.kwargs["json"]
        self.assertEqual(payload["exchangeSegment"],"NSE_FNO")
        self.assertEqual(payload["productType"],"MARGIN")
        self.assertEqual(payload["orderType"],"LIMIT")
        self.assertTrue(payload["afterMarketOrder"])
        self.assertEqual(payload["amoTime"],"OPEN")
        self.assertEqual(payload["quantity"],75)
        self.assertNotIn("stopLossPrice",payload)
        self.assertNotIn("targetPrice",payload)
        c.stop_manager.finalize_mutation.assert_called_once()

    def test_disabled_switch_never_calls_broker(self):
        c=client(fno=False)
        ok,oid,note=c.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(oid)
        c.session.post.assert_not_called()

    def test_bad_fno_product_blocked(self):
        c=client()
        ok,_,_=c.place_order(request(product=ProductType.CNC))
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_equity_switch_cannot_enable_fno(self):
        c=client(fno=False,equity=True)
        ok,_,_=c.place_order(request())
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_rejected_response_not_reported_as_acknowledged(self):
        c=client(status="REJECTED")
        with patch("src.engine.market.market_status",return_value=(False,"Market Closed Post-Hours")), \
             patch("src.engine.config_manager.ConfigChangeTracker") as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,"")
            ok,oid,_=c.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(oid)

    def test_market_open_is_not_an_amo_submission_window(self):
        c=client()
        with patch("src.engine.market.market_status",return_value=(True,"Market Open")):
            ok,_,_=c.place_order(request())
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_unknown_broker_id_does_not_resubmit(self):
        c=client(oid="")
        c._reconcile_uncertain_submission=MagicMock(return_value=(False,None,"outcome UNKNOWN"))
        with patch("src.engine.market.market_status",return_value=(False,"Market Closed Post-Hours")), \
             patch("src.engine.config_manager.ConfigChangeTracker") as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,"")
            ok,oid,_=c.place_order(request())
        self.assertFalse(ok)
        self.assertIsNone(oid)
        c.session.post.assert_called_once()
        c.stop_manager.finalize_mutation.assert_called_once()
        self.assertTrue(c.stop_manager.finalize_mutation.call_args.kwargs["keep_persisted_if_uncertain"])

    def test_entry_only_amo_never_retries_even_after_restart(self):
        from src.engine.protection_watcher import ProtectionWatcher
        w=object.__new__(ProtectionWatcher)
        rec={"entry_only_amo":True,"retry_eligible":True,"max_retries":0}
        w.pending={"O-1":rec}
        w.terminal_orders={}
        ok,qty,note=w._evaluate_retry_locked("O-1", 90.0)
        self.assertFalse(ok)
        self.assertEqual(qty,0)
        self.assertFalse(rec["retry_eligible"])

    def test_entry_only_amo_never_uses_margin_oco(self):
        import threading
        from src.engine.protection_watcher import ProtectionWatcher
        w=object.__new__(ProtectionWatcher)
        w._lock=threading.RLock()
        w.broker=MagicMock()
        changed=w._protect({"entry_only_amo":True},{},75,75)
        self.assertFalse(changed)
        w.broker.place_forever_oco.assert_not_called()

class FnoAmoEdgeSafetyTests(FrozenBrokerClock):
    def invoke(self,c):
        with patch('src.engine.market.market_status',return_value=(False,'Market Closed Post-Hours')), \
             patch('src.engine.config_manager.ConfigChangeTracker') as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,'')
            return c.place_order(request())

    def test_null_order_id_never_reports_success(self):
        c=client();c.session.post.return_value.json.return_value={'orderId':None,'orderStatus':'PENDING'}
        c._reconcile_uncertain_submission=MagicMock(return_value=(False,None,'UNKNOWN'))
        ok,oid,_=self.invoke(c)
        self.assertFalse(ok,'None was accepted as broker order ID')
        self.assertIsNone(oid)

    def test_cancelled_or_expired_status_never_reports_success(self):
        for status in ('CANCELLED','EXPIRED','FAILED'):
            with self.subTest(status=status):
                c=client(status=status)
                ok,oid,_=self.invoke(c)
                self.assertFalse(ok,f'{status} wrongly labeled ACK')

    def test_fno_1527_ist_rejected_despite_pipeline_close_gate(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        c=client()
        frozen=datetime(2026, 11, 5, 15, 27, tzinfo=ZoneInfo('Asia/Kolkata'))
        with patch('src.broker.dhan_client.datetime') as dt:
            dt.now.return_value=frozen
            ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_fno_1530_ist_allowed(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        c=client()
        frozen=datetime(2026, 11, 5, 15, 30, tzinfo=ZoneInfo('Asia/Kolkata'))
        with patch('src.broker.dhan_client.datetime') as dt:
            dt.now.return_value=frozen
            ok,_,_=self.invoke(c)
        self.assertTrue(ok)
        c.session.post.assert_called_once()

    def test_test_mode_blocked(self):
        c=client();c.cfg.trading_mode=TradingMode.TEST
        ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_missing_or_invalid_order_ids_are_uncertain(self):
        for bad_id in (None, '', 'None', 'ABCDEF', '123', '१२३४५६७८'):
            with self.subTest(bad_id=bad_id):
                c=client(oid=bad_id)
                c._reconcile_uncertain_submission=MagicMock(return_value=(True,'112111182198','broker read'))
                ok,oid,_=self.invoke(c)
                self.assertFalse(ok)
                self.assertIsNone(oid)
                c._reconcile_uncertain_submission.assert_not_called()
                c.session.post.assert_called_once()
                self.assertTrue(c.stop_manager.finalize_mutation.call_args.kwargs['keep_persisted_if_uncertain'])

    def test_http5xx_reconciliation_does_not_false_ack(self):
        c=client();c.session.post.return_value.status_code=503
        c._reconcile_uncertain_submission=MagicMock(return_value=(True,'112111182198','broker read possibly terminal'))
        ok,oid,_=self.invoke(c)
        self.assertFalse(ok)
        self.assertIsNone(oid)
        c._reconcile_uncertain_submission.assert_not_called()

    def test_broker_status_missing_is_unknown_not_accepted(self):
        c=client(status='')
        ok,oid,_=self.invoke(c)
        self.assertFalse(ok)
        self.assertEqual(oid,'112111182198')
        self.assertTrue(c.stop_manager.finalize_mutation.call_args.kwargs['keep_persisted_if_uncertain'])

    def test_fno_0914_ist_allowed(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        c=client()
        frozen=datetime(2026, 11, 5, 9, 14, tzinfo=ZoneInfo('Asia/Kolkata'))
        with patch('src.broker.dhan_client.datetime') as dt:
            dt.now.return_value=frozen
            ok,_,_=self.invoke(c)
        self.assertTrue(ok)

    def test_fno_0915_ist_blocked(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        c=client()
        frozen=datetime(2026, 11, 5, 9, 15, tzinfo=ZoneInfo('Asia/Kolkata'))
        with patch('src.broker.dhan_client.datetime') as dt:
            dt.now.return_value=frozen
            ok,_,_=self.invoke(c)
        self.assertFalse(ok)

    def test_equity_amo_unaffected_by_fno_toggle(self):
        from src.models import ProductType
        c=client(fno=False,equity=True)
        r=request(segment='NSE_EQ',product=ProductType.CNC)
        with patch('src.engine.market.market_status',return_value=(False,'Market Closed Post-Hours')), \
             patch('src.engine.config_manager.ConfigChangeTracker') as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(False,'')
            ok,_,_=c.place_order(r)
        self.assertTrue(ok)

    def test_emergency_stop_blocks(self):
        c=client();c.stop_manager.try_begin_mutation.return_value=(False,{'stopped_by':'OWNER','reason':'manual'})
        ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_reconciliation_gate_blocks(self):
        c=client();c.reconciliation_gate.is_accepting_entries=False
        ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_transition_gate_blocks(self):
        c=client()
        with patch('src.engine.market.market_status',return_value=(False,'Market Closed Post-Hours')), \
             patch('src.engine.config_manager.ConfigChangeTracker') as tracker:
            tracker.return_value.is_live_entry_blocked_by_transition.return_value=(True,'pending transition')
            ok,_,_=c.place_order(request())
        self.assertFalse(ok)
        c.session.post.assert_not_called()

    def test_http_5xx_never_resubmits(self):
        c=client();c.session.post.return_value.status_code=503
        c._reconcile_uncertain_submission=MagicMock(return_value=(False,None,'outcome UNKNOWN'))
        ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_called_once()
        self.assertTrue(c.stop_manager.finalize_mutation.call_args.kwargs['keep_persisted_if_uncertain'])

    def test_network_timeout_never_resubmits(self):
        c=client();c.session.post.side_effect=TimeoutError('timeout')
        ok,_,_=self.invoke(c)
        self.assertFalse(ok)
        c.session.post.assert_called_once()
        self.assertTrue(c.stop_manager.finalize_mutation.call_args.kwargs['keep_persisted_if_uncertain'])


if __name__=="__main__":
    unittest.main()
