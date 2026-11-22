"""Reproduce an obsolete order HTTP-429 poisoning the next holdings GET.

Only local fake HTTP responses and real account-scoped sqlite ReadGovernor.
"""
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import requests
from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor


class VEDPOWERLocalGovernorChain(unittest.TestCase):
    def test_broker_429_arms_60_second_cooldown_for_following_holdings(self):
        with tempfile.TemporaryDirectory() as temp:
            timestamp=[1_795_000_000.]
            governor=ReadGovernor(temp,"SYNTHETIC-VED",clock=lambda:timestamp[0])
            client=DhanClient.__new__(DhanClient)
            client._get_active_read_governor=lambda:governor
            client._halt_for_read_deferral=Mock()
            def response(status, text, headers=None):
                r=requests.Response()
                r.status_code=status
                r._content=text.encode()
                if headers:r.headers.update(headers)
                return r
            client.session=SimpleNamespace(get=Mock(side_effect=[
                response(429,'{"errorCode":"DH-904","errorMessage":"Rate limited"}',{"Retry-After":"30"}),
                response(200,'[]'),
            ]))
            bad=client._governed_get(
                'https://api.dhan.co/v2/trades/321261120279408',headers={},timeout=3,
            )
            self.assertEqual(bad.status_code,429)
            self.assertGreaterEqual(governor.seconds_until_capacity(),59)
            blocked=client._governed_get('https://api.dhan.co/v2/holdings',
                                        headers={},timeout=3)
            self.assertEqual(blocked.status_code,429)
            self.assertEqual(blocked.headers.get('X-AuditBot-Local-Deferral'),'1')
            self.assertEqual(client.session.get.call_count,1,
                             'Holdings never left VM after old order GET poisoned the category')
            timestamp[0]+=61
            good=client._governed_get('https://api.dhan.co/v2/holdings',
                                     headers={},timeout=3)
            self.assertEqual(good.status_code,200)
            self.assertEqual(client.session.get.call_count,2)

if __name__=='__main__':unittest.main()
