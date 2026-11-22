"""Offline regression for the actual systemd production runner's V17 status publisher.

run.py cannot be imported normally: importing starts a live trading pipeline.
Extract only the pure async publisher function via AST and inject a fake
snapshot module. No broker, Telegram or service action is performed.
"""
import ast
import asyncio
import copy
import logging
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


RUNNER = Path(__file__).resolve().parents[1] / "run.py"


def _ast():
    return ast.parse(RUNNER.read_text(encoding="utf-8"), filename=str(RUNNER))


def _runner():
    return next(
        obj for obj in _ast().body
        if isinstance(obj, ast.AsyncFunctionDef) and obj.name == "main"
    )


def _publisher():
    return next(
        obj for obj in ast.walk(_runner())
        if isinstance(obj, ast.AsyncFunctionDef)
        and obj.name == "_publish_ui_runtime_evidence"
    )


class ProductionRunnerSnapshotTests(unittest.TestCase):
    def test_systemd_installer_launches_the_runner_under_test(self):
        installer = RUNNER.parent / "deploy" / "install_vm_services.sh"
        content = installer.read_text(encoding="utf-8")
        self.assertIn("ExecStart=$REPO_DIR/venv/bin/python3 run.py", content)

    def test_real_runner_schedules_and_cancels_snapshot_task(self):
        runner = _runner()
        self.assertEqual(
            sum(isinstance(x, ast.AsyncFunctionDef) and x.name == "_publish_ui_runtime_evidence"
                for x in ast.walk(runner)), 1,
        )
        source = RUNNER.read_text(encoding="utf-8")
        self.assertIn("runtime_evidence_task = asyncio.create_task(_publish_ui_runtime_evidence())", source)
        self.assertIn("runtime_evidence_task.cancel()", source)
        self.assertLess(source.index("pipeline.watcher.run(is_lifecycle_open)"),
                        source.index("runtime_evidence_task = asyncio.create_task(_publish_ui_runtime_evidence())"))
        self.assertLess(source.index("runtime_evidence_task = asyncio.create_task(_publish_ui_runtime_evidence())"),
                        source.index("await client.run_until_disconnected()"))

    def _exercise(self, should_raise=False):
        events = []
        pipeline = object()

        class Client:
            def is_connected(self):
                return True

        def write_snapshot(target, *, telegram_connected):
            events.append((target is pipeline, telegram_connected))
            if should_raise:
                raise ValueError("test-only-failure")

        snap = types.ModuleType("src.engine.operational_snapshot")
        snap.publish_operational_snapshot = write_snapshot

        async def one_sleep(seconds):
            self.assertEqual(seconds, 10)
            raise asyncio.CancelledError()

        fake_asyncio = types.SimpleNamespace(sleep=one_sleep)
        function = copy.deepcopy(_publisher())
        function.decorator_list = []
        namespace = dict(pipeline=pipeline, client=Client(), asyncio=fake_asyncio, logging=logging)
        tree = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
        exec(compile(tree, str(RUNNER), "exec"), namespace)
        with patch.dict(sys.modules, {"src.engine.operational_snapshot": snap}):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(namespace[function.name]())
        self.assertEqual(events, [(True, True)])

    def test_publishes_live_connection_evidence(self):
        self._exercise(should_raise=False)

    def test_writer_failure_does_not_break_trading_listener(self):
        self._exercise(should_raise=True)


if __name__ == "__main__":
    unittest.main()
