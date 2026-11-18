"""R07 offline regression: startup funds reserve, identity retry, local 429.

Only synthetic broker responses; no Dhan network calls, orders, VM or journal
mutations. Keep routine signal-side funds checks at NORMAL priority.
"""
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from src.broker.dhan_client import DhanClient, BASE_URL
from src.broker.read_governor import ReadDeferred, ReadGovernor, RECONCILIATION_PACING
from src.engine.reconciliation import StartupReconciliationGate
from src.models import BrokerReadFailure, BrokerReadStatus, TradingMode


def live_client(response):
    client = object.__new__(DhanClient)
    client.cfg = NS(trading_mode=TradingMode.LIVE, dhan_client_id="SYNTHETIC")
    client._get_with_auth_recovery = Mock(return_value=response)
    return client


class StartupReadReserveTests(unittest.TestCase):
    def test_reserved_identity_read_available_after_normal_is_exhausted(self):
        with tempfile.TemporaryDirectory() as root:
            gov = ReadGovernor(root, "SYNTHETIC", clock=lambda: 100000.0)
            gov.LIMITS = ((86400, 100),)
            for _ in range(95):
                gov.admit(priority="normal")
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="normal")
            gov.admit(priority="identity")  # one of five reserved slots
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="normal")
            for _ in range(4):
                gov.admit(priority="identity")
            with self.assertRaises(ReadDeferred):
                gov.admit(priority="identity")  # hard cap is NEVER bypassed

    def test_startup_funds_get_uses_identity_but_normal_get_does_not(self):
        response = NS(
            status_code=200, text="", headers={},
            json=lambda: {"dhanClientId": "SYNTHETIC", "availabelBalance": 1234.5},
        )
        broker = live_client(response)
        del broker._get_with_auth_recovery
        broker.session = NS(get=Mock(return_value=response))
        broker._headers = Mock(return_value={})
        governor = Mock()
        broker._get_active_read_governor = Mock(return_value=governor)
        token = RECONCILIATION_PACING.set(True)
        try:
            self.assertTrue(broker.get_fund_limits(retry=False)[0])
        finally:
            RECONCILIATION_PACING.reset(token)
        self.assertTrue(broker.get_fund_limits(retry=False)[0])
        self.assertEqual(["identity", "normal"], [c.kwargs["priority"] for c in governor.admit.call_args_list])

    def test_local_denial_is_not_falsely_reported_as_broker_http429(self):
        response = NS(
            status_code=429,
            text="Dhan GET deferred locally for 999s: local GET budget exhausted",
            headers={"X-AuditBot-Local-Deferral": "1", "Retry-After": "999"},
        )
        broker = live_client(response)
        ok, balance, message = broker.get_fund_limits(retry=False)
        self.assertFalse(ok)
        self.assertEqual(0.0, balance)
        self.assertIn("no broker request sent", message)
        self.assertIn("999s", message)
        self.assertNotIn("Dhan funds API error", message)

    def test_concurrent_signal_and_gate_keep_separate_funds_priorities(self):
        broker = object.__new__(DhanClient)
        governor = Mock()
        broker._get_active_read_governor = Mock(return_value=governor)
        broker.session = NS(get=Mock(return_value=NS(status_code=200, text="ok")))
        barrier = Barrier(2)
        def read(in_gate):
            scope = RECONCILIATION_PACING.set(in_gate)
            try:
                barrier.wait(timeout=3)
                broker._governed_get(f"{BASE_URL}/fundlimit", headers={}, timeout=1)
            finally:
                RECONCILIATION_PACING.reset(scope)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(read, [True, False]))
        self.assertCountEqual(["identity", "normal"], [c.kwargs["priority"] for c in governor.admit.call_args_list])

    def test_empty_journal_blocked_gate_rechecks_once_per_cooldown(self):
        from src.engine.reconciliation import ReconciliationState
        gate = StartupReconciliationGate(broker=NS(), journal_db=NS(), watcher=None,
                                         stop_manager=NS(is_stopped=lambda: False))
        gate.state = ReconciliationState.BLOCKED
        gate.recheck_and_recover_if_verified = Mock(return_value=(False, "funds unavailable"))
        with patch("src.engine.reconciliation.threading.Thread") as worker:
            worker.side_effect = lambda target, **kwargs: NS(start=target)
            with patch("src.engine.reconciliation.time.monotonic", return_value=1000):
                gate._schedule_historical_background_recheck()
                gate._schedule_historical_background_recheck()
            with patch("src.engine.reconciliation.time.monotonic", return_value=1601):
                gate._schedule_historical_background_recheck()
        self.assertEqual(2, gate.recheck_and_recover_if_verified.call_count)

    def test_auth_refresh_retry_retains_original_identity_priority(self):
        broker = object.__new__(DhanClient)
        broker.cfg = NS(
            dhan_client_id="SYNTHETIC", dhan_pin="", dhan_totp_secret=""
        )
        broker.token = "new"
        broker._headers = Mock(side_effect=[
            {"access-token": "old"}, {"access-token": "new"}
        ])
        broker.session = NS(get=Mock(side_effect=[
            NS(status_code=401, text="unauthorized"),
            NS(status_code=200, text="success"),
        ]))
        governor = Mock()
        broker._get_active_read_governor = Mock(return_value=governor)
        broker._requires_token_refresh = Mock(return_value=True)
        broker._sync_shared_token_from_disk = Mock()
        token = RECONCILIATION_PACING.set(True)
        try:
            response = broker._get_with_auth_recovery(f"{BASE_URL}/fundlimit")
        finally:
            RECONCILIATION_PACING.reset(token)
        self.assertEqual(200, response.status_code)
        self.assertEqual(["identity", "identity"], [c.kwargs["priority"] for c in governor.admit.call_args_list])

    def test_gate_uses_concrete_startup_probe_and_does_not_assume_green(self):
        class Broker:
            def __init__(self):
                self.startup_count = 0
                self.cfg = NS(dhan_client_id="SYNTHETIC")

            def get_fund_limits(self):
                assert RECONCILIATION_PACING.get()
                self.startup_count += 1
                return True, 1234.0, "verified"


            def get_trade_book(self):
                return BrokerReadFailure(
                    status=BrokerReadStatus.UNAVAILABLE.value,
                    error="synthetic tradebook unavailable",
                    status_code=503,
                )

        broker = Broker()
        gate = StartupReconciliationGate(
            broker=broker, journal_db=None, watcher=None,
            stop_manager=NS(is_stopped=lambda: False),
        )
        ok, _ = gate.run_startup_reconciliation()
        self.assertFalse(ok)  # another required broker read is still missing
        self.assertEqual(1, broker.startup_count)


if __name__ == "__main__":
    unittest.main()
