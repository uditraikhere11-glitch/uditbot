"""Regression tests for the actual systemd entrypoint: run.py.

PRs #45-47 added response dispatch only to src.main's alternate listener.
The deployed trading_bot.service executes run.py, so runtime wiring must be
tested separately from helper-only or standalone listener tests.
"""
import ast
import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

from src.journal.db import JournalDB
from src.notifier.dhan_response import configure_dhan_response_group
from src.notifier.outbox_worker import OutboxWorker


def _group(name, chat_id):
    return SimpleNamespace(
        name=name, id=chat_id, is_group=True,
        entity=SimpleNamespace(id=chat_id),
    )


class _FakeClient:
    def __init__(self, groups, send_exception=None):
        self.groups = groups
        self.sent = []
        self.send_exception = send_exception

    async def iter_dialogs(self):
        for group in self.groups:
            yield group

    async def send_message(self, entity, message, parse_mode=None):
        if self.send_exception:
            raise self.send_exception
        self.sent.append((entity.id, message, parse_mode))
        return SimpleNamespace(id=1)


class TestProductionEntrypointWiring(unittest.TestCase):
    def test_real_systemd_runner_connects_response_group_and_outbox(self):
        source = (Path(__file__).resolve().parents[1] / "run.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        runner = next(
            node for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"
        )
        calls = [node for node in ast.walk(runner) if isinstance(node, ast.Call)]
        setup_calls = [
            node for node in calls
            if isinstance(node.func, ast.Name) and node.func.id == "configure_dhan_response_group"
        ]
        self.assertEqual(len(setup_calls), 1, "Production run.py must actually set up the Dhan response group")
        setup_keys = {x.arg for x in setup_calls[0].keywords}
        self.assertTrue({"client", "group_name", "signal_source_ids", "reporter", "loop"} <= setup_keys)
        workers = [
            node for node in calls
            if isinstance(node.func, ast.Name) and node.func.id == "OutboxWorker"
        ]
        self.assertEqual(len(workers), 1)
        dispatch = [
            kw for kw in workers[0].keywords if kw.arg == "dhan_response_dispatch_fn"
        ]
        self.assertEqual(len(dispatch), 1)
        self.assertIsInstance(dispatch[0].value, ast.Name)
        self.assertEqual(dispatch[0].value.id, "dhan_response_dispatch_fn")


class TestProductionDelivery(unittest.IsolatedAsyncioTestCase):
    async def test_signed_id_connects_and_delivers_acknowledgement(self):
        target = -5254821642
        client = _FakeClient([_group("Dhan Response", target)])
        reporter = SimpleNamespace(dhan_response_enabled=False)
        dispatch = await configure_dhan_response_group(
            client, str(target), [-1004417022099], reporter, asyncio.get_running_loop()
        )
        self.assertIsNotNone(dispatch)
        self.assertTrue(reporter.dhan_response_enabled)
        self.assertTrue(await asyncio.to_thread(dispatch, "AMO Accepted — NOT EXECUTED"))
        self.assertEqual(client.sent, [(target, "AMO Accepted — NOT EXECUTED", "html")])

    async def test_acked_dhan_response_flows_through_production_outbox(self):
        target = -5254821642
        client = _FakeClient([_group("Dhan Response", target)])
        reporter = SimpleNamespace(dhan_response_enabled=False)
        dispatch = await configure_dhan_response_group(
            client, str(target), [-1004417022099], reporter, asyncio.get_running_loop()
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal = JournalDB(os.path.join(tmp, "journal.db"))
            journal.enqueue_dhan_response_ack_once("ORDER-1", "🌙 AMO queued for OPEN")
            logs = []
            worker = OutboxWorker(
                journal,
                dispatch_fn=lambda txt: logs.append(txt) or True,
                dhan_response_dispatch_fn=dispatch,
            )
            self.assertEqual(await asyncio.to_thread(worker.process_pending_events), 1)
            self.assertEqual(logs, [])
            self.assertEqual(client.sent, [(target, "🌙 AMO queued for OPEN", "html")])
            self.assertEqual(journal.fetch_pending_outbox(), [])

    async def test_missing_group_does_not_enable_mirroring(self):
        client = _FakeClient([])
        reporter = SimpleNamespace(dhan_response_enabled=True)
        with self.assertLogs("src.notifier.dhan_response", level="WARNING") as log:
            dispatch = await configure_dhan_response_group(
                client, "-5254821642", [], reporter, asyncio.get_running_loop()
            )
        self.assertIsNone(dispatch)
        self.assertFalse(reporter.dhan_response_enabled)
        self.assertIn("group unavailable", " ".join(log.output))

    async def test_monitored_source_is_never_a_response_destination(self):
        target = -5254821642
        client = _FakeClient([_group("Dhan Response", target)])
        reporter = SimpleNamespace(dhan_response_enabled=False)
        dispatch = await configure_dhan_response_group(
            client, str(target), [target], reporter, asyncio.get_running_loop()
        )
        self.assertIsNone(dispatch)
        self.assertFalse(reporter.dhan_response_enabled)

    async def test_failed_send_is_retryable_by_outbox(self):
        client = _FakeClient(
            [_group("Dhan Response", -5254821642)],
            send_exception=RuntimeError("Fake network failure"),
        )
        reporter = SimpleNamespace(dhan_response_enabled=False)
        dispatch = await configure_dhan_response_group(
            client, "-5254821642", [], reporter, asyncio.get_running_loop()
        )
        self.assertFalse(await asyncio.to_thread(dispatch, "AMO accepted"))
        self.assertTrue(reporter.dhan_response_enabled)

    async def test_unconfigured_group_never_enables_response_mirroring(self):
        client = _FakeClient([_group("Dhan Response", -5254821642)])
        reporter = SimpleNamespace(dhan_response_enabled=True)
        dispatch = await configure_dhan_response_group(
            client, "", [], reporter, asyncio.get_running_loop()
        )
        self.assertIsNone(dispatch)
        self.assertFalse(reporter.dhan_response_enabled)


if __name__ == "__main__":
    unittest.main()
