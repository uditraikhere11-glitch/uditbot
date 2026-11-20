"""Offline mutation admission checks; no Dhan, network, trading worker, or data access."""
from __future__ import annotations
import ast
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "src/broker/dhan_client.py"


def _isolated_mutator():
    """Compile only the real source method to prove its transport order."""
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    class_def = next(x for x in tree.body if isinstance(x, ast.ClassDef) and x.name == "DhanClient")
    method = next(x for x in class_def.body if isinstance(x, ast.FunctionDef) and x.name == "_broker_mutating_http")
    isolated = ast.Module(body=[method], type_ignores=[])
    ast.fix_missing_locations(isolated)
    context = {"sqlite3": sqlite3}
    exec(compile(isolated, str(SOURCE), "exec"), context)
    return context["_broker_mutating_http"]


class AlwaysOnFenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mutator = staticmethod(_isolated_mutator())

    def setUp(self):
        self.events = []
        self.reject = False
        self.finish_known = None
        self.response = SimpleNamespace(status_code=202, headers={})
        self.session = SimpleNamespace(delete=Mock(side_effect=self.send))
        self.client = SimpleNamespace(
            cfg=SimpleNamespace(data_dir="SYNTHETIC_ONLY", dhan_client_id="FAKE_ACCOUNT"),
            session=self.session,
            _get_active_read_governor=lambda: None,
            _read_deferral_response=Mock(),
            _is_rate_limited_response=lambda _: False,
            _halt_for_read_deferral=Mock(),
        )
        class FakeGovernor:
            @staticmethod
            def classify(method, url):
                return "ORDER" if method == "DELETE" else "NON_TRADING"
            @staticmethod
            def modification_order_id(method, url):
                return None
        governor = ModuleType("src.broker.read_governor")
        governor.ReadGovernor = FakeGovernor
        governor.ReadDeferred = RuntimeError
        handoff = ModuleType("src.engine.broker_handoff")
        class BrokerHandoffBlocked(RuntimeError):
            pass
        class BrokerMutationNotSubmitted(BrokerHandoffBlocked):
            pass
        @contextmanager
        def broker_mutation(data, account, method):
            self.events.append("admission")
            if self.reject:
                raise BrokerHandoffBlocked("active fence")
            state = {"known": None}
            def finish(known):
                state["known"] = known
            try:
                yield finish
            finally:
                self.finish_known = state["known"]
                self.events.append("finalization")
        handoff.broker_mutation = broker_mutation
        handoff.BrokerHandoffBlocked = BrokerHandoffBlocked
        handoff.BrokerMutationNotSubmitted = BrokerMutationNotSubmitted
        self.handoff_error = BrokerMutationNotSubmitted
        self.modules = patch.dict(sys.modules, {
            "src.broker.read_governor": governor,
            "src.engine.broker_handoff": handoff,
        })
        self.modules.start()
        self.addCleanup(self.modules.stop)

    def send(self, url, **kwargs):
        self.events.append("transport")
        return self.response

    def test_active_barrier_blocks_without_network(self):
        self.reject = True
        with self.assertRaises(self.handoff_error):
            self.mutator(self.client, "DELETE", "https://api.dhan.co/v2/forever/orders/FAKE")
        self.session.delete.assert_not_called()
        self.assertEqual(self.events, ["admission"])

    def test_admission_precedes_http_transport(self):
        self.mutator(self.client, "DELETE", "https://api.dhan.co/v2/forever/orders/FAKE")
        self.assertEqual(self.events, ["admission", "transport", "finalization"])
        self.assertTrue(self.finish_known)
        self.session.delete.assert_called_once()

    def test_ambiguous_http_status_remains_unresolved(self):
        self.response.status_code = 500
        self.mutator(self.client, "DELETE", "https://api.dhan.co/v2/forever/orders/FAKE")
        self.assertFalse(self.finish_known)

    def test_network_exception_finalizes_as_unknown(self):
        self.session.delete.side_effect = ConnectionError("offline synthetic timeout")
        with self.assertRaises(ConnectionError):
            self.mutator(self.client, "DELETE", "https://api.dhan.co/v2/forever/orders/FAKE")
        self.assertIsNone(self.finish_known)
        self.assertEqual(self.events, ["admission", "finalization"])

    def test_unclassified_mutation_never_transports(self):
        with self.assertRaises(ValueError):
            self.mutator(self.client, "GET", "https://api.dhan.co/v2/forever/orders/FAKE")
        self.session.delete.assert_not_called()
        self.assertEqual(self.events, [])


if __name__ == "__main__":
    unittest.main()
