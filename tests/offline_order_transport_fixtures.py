"""Offline-only mock transport for pre-existing broker PAYLOAD tests.

These tests deliberately assert payloads/HTTP outcomes, not mutation-fence
admission. Only their explicitly decorated test methods may send to a mocked
requests session. Real fence acceptance is tested separately with actual
broker_handoff and a disposable ledger. Never import this helper in src/.
"""
import os
from unittest.mock import Mock


def mocked_order_transport(broker, method, url, **kwargs):
    if os.environ.get("AUDITBOT_TEST_ISOLATION") != "1":
        raise AssertionError("Mocked order transport requires explicit offline test isolation")
    method = str(method).lower()
    if method not in ("post", "put", "delete"):
        raise AssertionError("Not a broker ORDER mutation")
    sender = getattr(broker.session, method, None)
    if not isinstance(sender, Mock):
        raise AssertionError("Refusing unmocked network transport in offline test")
    return sender(url, **kwargs)
