"""Offline DhanClient v2 request admission across GET, QUOTE and ORDER routes."""
from types import SimpleNamespace as NS
import tempfile
import unittest
from unittest.mock import Mock, patch
import requests
from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor


def response(code=200, payload='[]', retry=None):
    r = requests.Response()
    r.status_code = code
    r._content = payload.encode()
    if retry is not None:
        r.headers['Retry-After'] = str(retry)
    return r


class DhanCategoryTransportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100000.
        self.g = ReadGovernor(self.tmp.name, 'ACCOUNT_A', clock=lambda: self.now)
        c = DhanClient.__new__(DhanClient)
        c.cfg = NS(dhan_client_id='ACCOUNT_A', data_dir=self.tmp.name)
        c._read_governor = self.g
        c.session = NS(
            get=Mock(return_value=response(200)),
            post=Mock(return_value=response(200,'{}')),
            put=Mock(return_value=response(200,'{}')),
            delete=Mock(return_value=response(202,'{}')),
        )
        self.c = c
        patcher = patch.dict('os.environ', {'UDITBOT_TRACKER_V2_ENABLE':'0'})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_order_book_get_is_non_trading_not_order_daily_limit(self):
        url='https://api.dhan.co/v2/orders'
        # Routine order-book polling retains two short-window slots for
        # urgent safety probes. There is no artificial daily GET ceiling.
        for _ in range(18):
            self.assertEqual(self.c._governed_get(url,headers={},timeout=1).status_code,200)
        blocked=self.c._governed_get(url,headers={},timeout=1)
        self.assertEqual(blocked.status_code,429)
        self.assertEqual(blocked.headers['X-AuditBot-Local-Deferral'],'1')
        self.assertEqual(self.c.session.get.call_count,18)
        for _ in range(2):
            self.assertEqual(self.c._governed_get(url,headers={},timeout=1,priority='urgent').status_code,200)
        self.assertEqual(self.c._governed_get(url,headers={},timeout=1,priority='urgent').status_code,429)
        self.assertEqual(self.c.session.get.call_count,20)
        self.now+=1.02
        self.assertEqual(self.c._governed_get(url,headers={},timeout=1).status_code,200)

    def test_order_post_and_quote_post_have_independent_limits(self):
        url='https://api.dhan.co/v2/orders'
        for _ in range(10):
            self.assertEqual(self.c._broker_mutating_http('POST',url,timeout=1).status_code,200)
        local=self.c._broker_mutating_http('POST',url,timeout=1)
        self.assertEqual(local.status_code,429)
        self.assertEqual(self.c.session.post.call_count,10)
        quote='https://api.dhan.co/v2/marketfeed/ltp'
        self.assertEqual(self.c._governed_quote_post(quote,headers={},json_payload={}).status_code,200)
        blocked=self.c._governed_quote_post(quote,headers={},json_payload={})
        self.assertEqual(blocked.status_code,429)
        self.assertEqual(self.c.session.post.call_count,11)

    def test_broker_429_cools_only_offending_category(self):
        self.c.session.post.return_value=response(429,retry=120)
        quote='https://api.dhan.co/v2/marketfeed/ltp'
        self.assertEqual(self.c._governed_quote_post(quote,headers={},json_payload={}).status_code,429)
        self.c.session.get.return_value=response(200)
        self.assertEqual(self.c._governed_get('https://api.dhan.co/v2/orders',headers={},timeout=1).status_code,200)
        self.assertEqual(self.c.session.get.call_count,1)

    def test_modification_is_accounted_once_before_network_send(self):
        self.c.session.put.return_value=response(200,'{}')
        url='https://api.dhan.co/v2/orders/ORD-001'
        for _ in range(25):
            result=self.c._broker_mutating_http('PUT',url,timeout=1)
            self.assertEqual(result.status_code,200)
            self.now+=1.1
        result=self.c._broker_mutating_http('PUT',url,timeout=1)
        self.assertEqual(result.status_code,429)
        self.assertEqual(self.c.session.put.call_count,25)

    def test_failed_admission_never_invokes_v2_broker_handoff(self):
        for _ in range(10):self.g.admit(category='ORDER')
        with patch('src.engine.broker_handoff.guarded_request') as handoff:
            with patch.dict('os.environ', {'UDITBOT_TRACKER_V2_ENABLE':'1'}):
                response_local=self.c._broker_mutating_http('POST','https://api.dhan.co/v2/orders')
            self.assertEqual(response_local.status_code,429)
            handoff.assert_not_called()


if __name__=='__main__':unittest.main()
