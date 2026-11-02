"""Strict Yahoo-only patch regression tests: no network, no broker orders."""
import json
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import requests
from requests.adapters import HTTPAdapter

from src.broker.dhan_client import DhanClient
from src.broker.yahoo_observer import (
    YahooHTTPAdapter,
    YahooObserver,
    YAHOO_HTTP_TIMEOUT,
    configure_yahoo_http,
    parse_yahoo_response,
)
from src.config import AppConfig, TradingMode
from src.models import QuoteResult


class TestYahooOnlyScopedPatch(unittest.TestCase):
    def make_client(self):
        with patch.object(DhanClient, "_load_token", return_value=""):
            return DhanClient(AppConfig(trading_mode=TradingMode.TEST))

    def test_adapter_is_yahoo_scoped_and_dhan_is_unchanged(self):
        client = self.make_client()
        self.assertIsInstance(client.session.get_adapter("https://query1.finance.yahoo.com/v8/finance/chart/SBIN.NS"), YahooHTTPAdapter)
        self.assertIsInstance(client.session.get_adapter("https://query2.finance.yahoo.com/v8/finance/chart/SBIN.NS"), YahooHTTPAdapter)
        self.assertIs(type(client.session.get_adapter("https://api.dhan.co/v2/orders")), HTTPAdapter)
        self.assertIs(type(client.session.get_adapter("https://auth.dhan.co/app/generateAccessToken")), HTTPAdapter)
        self.assertTrue(client.session.trust_env)
        client.session.close()

    def test_no_global_https_override(self):
        session = requests.Session()
        initial_https_adapter = session.get_adapter("https://another.example/test")
        configure_yahoo_http(session)
        self.assertIs(initial_https_adapter, session.get_adapter("https://another.example/test"))
        session.close()

    def test_request_local_quote_metadata_does_not_cross_threads(self):
        client = self.make_client()
        qa = QuoteResult(is_valid=True, price=99.0, ticker="SBIN.NS")
        qb = QuoteResult(is_valid=True, price=102.0, ticker="TCS.NS")
        client.last_quote_result = qa
        seen = []
        def worker():
            seen.append(client.last_quote_result)
            client.last_quote_result = qb
            seen.append(client.last_quote_result)
        th = threading.Thread(target=worker)
        th.start()
        th.join(timeout=1)
        self.assertFalse(th.is_alive())
        self.assertEqual(seen, [None, qb])
        self.assertIs(client.last_quote_result, qa)
        client.session.close()

    def test_normal_yahoo_request_uses_socket_timeout_pair(self):
        client = self.make_client()
        response = MagicMock(status_code=503)
        client.session.get = MagicMock(return_value=response)
        result = client.get_yahoo_quote_details("SBIN", exchange_segment="NSE_EQ")
        self.assertFalse(result.is_valid)
        self.assertIn("HTTP 503", result.failure_reason)
        self.assertEqual(client.session.get.call_args.kwargs['timeout'], YAHOO_HTTP_TIMEOUT)
        client.session.close()

    def test_yahoo_json_bytes_fast_path_and_mock_fallback(self):
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({'chart': {'result': []}}).encode('utf-8')
        self.assertEqual(parse_yahoo_response(response)['chart'], {'result': []})
        mock_response = MagicMock()
        mock_response.json.return_value = {'chart': {'result': []}}
        self.assertIn('chart',parse_yahoo_response(mock_response))
        with self.assertRaises(ValueError):
            parse_yahoo_response(MagicMock(json=lambda: [1,2]))

    def test_observer_never_executable_and_keeps_injected_session(self):
        session = MagicMock()
        observer = YahooObserver(session=session)
        self.assertIs(observer.session, session)
        self.assertFalse(observer.observe('', 'NSE_EQ').executable)

    def test_strict_rejects_stale_and_fno_fails_closed(self):
        client = self.make_client()
        now=1_730_200_000.0
        response=MagicMock(status_code=200)
        response.json.return_value={'chart': {'result': [{'meta': {
            'regularMarketPrice':812.50,
            'regularMarketTime': now-5.0,
            'symbol':'SBIN.NS',
            'currency':'INR',
            'exchangeName':'NSI',
            'marketState':'REGULAR',
            'exchangeDataDelayedBy':0,
        }}]}}
        client.session.get = MagicMock(return_value=response)
        res=client.get_yahoo_quote_details('SBIN','NSE_EQ',now_utc=now,max_quote_age_sec=2.0,cmp_policy='STRICT_TIMESTAMP')
        self.assertFalse(res.is_valid)
        self.assertIn('Stale',res.failure_reason)
        fno=client.get_yahoo_quote_details('SBIN','NSE_FNO')
        self.assertFalse(fno.is_valid)
        self.assertEqual(client.session.get.call_count,1)
        client.session.close()

    def test_unchanged_yahoo_observer_missing_metadata_behavior(self):
        response=MagicMock(status_code=200)
        response.json.return_value={'chart': {'result': [{'meta': {'regularMarketPrice': 125.0, 'regularMarketTime': 1730200000}}]}}
        observer=YahooObserver(session=MagicMock())
        observer.session.get.return_value=response
        result=observer.observe('SBIN', now_utc=1730200001)
        self.assertTrue(result.is_success)
        self.assertEqual(result.market_state,'UNKNOWN')
        self.assertIsNone(result.delayed_by_sec)
        self.assertFalse(result.executable)

if __name__=='__main__':
    unittest.main()
