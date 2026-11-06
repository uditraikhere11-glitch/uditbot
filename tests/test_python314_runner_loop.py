"""Regression tests for Python 3.14 Telethon event-loop initialization and teardown in run.py (PR #50).

Verifies:
1. AST check that run.py contains zero top-level TelegramClient(...) calls, constructs
   TelegramClient(...) inside async def main(), creates no manual second event loop, and
   isolates outbox_worker.stop(), await client.disconnect(), and lock.release() in nested
   try/finally blocks.
2. Runtime verification where a fake TelegramClient asserts asyncio.get_running_loop()
   succeeds during __init__ inside asyncio.run(main()), and normal exit executes
   outbox_stop -> disconnect -> lock_release in order.
3. Genuine disconnected Telethon smoke test (TelegramClient(StringSession(), 12345, '0123456789abcdef0123456789abcdef')
   instantiated inside asyncio.run(...) and disconnected cleanly without network calls).
   Requires the real `telethon` dependency to be installed (never substitutes a fake client).
4. Lock-release and cleanup guarantees when TelegramClient(...) raises on construction or when
   client.disconnect() raises during shutdown.
5. Lock-release and client.disconnect() guarantees when outbox_worker.stop() raises an exception
   (or when both outbox_worker.stop() and client.disconnect() raise).
"""
import ast
import asyncio
import logging
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock


RUN_PY_PATH = Path(__file__).resolve().parents[1] / "run.py"


def _load_run_py_ast() -> ast.Module:
    return ast.parse(RUN_PY_PATH.read_text(encoding="utf-8"), filename=str(RUN_PY_PATH))


def _compile_main_function(env_globals: dict):
    """Extracts and compiles only `async def main()` from `run.py` against `env_globals`."""
    tree = _load_run_py_ast()
    main_node = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"
    )
    mod = ast.Module(body=[main_node], type_ignores=[])
    ast.fix_missing_locations(mod)
    code = compile(mod, str(RUN_PY_PATH), "exec")
    ns = dict(env_globals)
    exec(code, ns)
    return ns["main"]


class TestPython314RunnerLoop(unittest.TestCase):
    def test_01_ast_no_top_level_telegram_client_and_nested_finally_cleanup(self):
        """1. AST check that run.py has no top-level TelegramClient(...) call, constructs it inside async def main(), and nests finally cleanup."""
        tree = _load_run_py_ast()

        top_level_calls = []
        main_fn = None
        for stmt in tree.body:
            if isinstance(stmt, ast.AsyncFunctionDef) and stmt.name == "main":
                main_fn = stmt
                continue
            for sub in ast.walk(stmt):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "TelegramClient":
                    top_level_calls.append(sub)

        self.assertEqual(
            len(top_level_calls),
            0,
            "run.py must not instantiate TelegramClient at module top level outside an active asyncio event loop",
        )
        self.assertIsNotNone(main_fn, "run.py must define async def main()")

        main_tg_calls = [
            sub for sub in ast.walk(main_fn)
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "TelegramClient"
        ]
        self.assertEqual(
            len(main_tg_calls),
            1,
            "async def main() must construct TelegramClient exactly once inside the running event loop",
        )

        # Ensure no manual second loop creation (new_event_loop / set_event_loop / run_until_complete)
        manual_loop_calls = [
            sub.func.attr for sub in ast.walk(tree)
            if isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr in {"new_event_loop", "set_event_loop", "run_until_complete"}
        ]
        self.assertEqual(manual_loop_calls, [], "run.py must use asyncio.run(main()) without manual loop creation")

        # Verify main() outer Try has nested Try/finalbody isolating outbox_worker.stop(), client.disconnect(), and lock.release()
        outer_try = next(node for node in main_fn.body if isinstance(node, ast.Try))
        self.assertEqual(len(outer_try.finalbody), 1)
        self.assertIsInstance(outer_try.finalbody[0], ast.Try)
        mid_try = outer_try.finalbody[0]
        self.assertEqual(len(mid_try.finalbody), 1)
        self.assertIsInstance(mid_try.finalbody[0], ast.Try)

    def test_02_runtime_telegram_client_constructed_with_active_running_loop(self):
        """2. Runtime test where a fake TelegramClient asserts asyncio.get_running_loop() succeeds during __init__."""
        events_recorded = []

        class LoopCheckingTelegramClient:
            def __init__(self, session_arg, api_id_arg, api_hash_arg):
                running_loop = asyncio.get_running_loop()
                self.assert_running = running_loop.is_running()
                events_recorded.append(("init", session_arg, api_id_arg, api_hash_arg, self.assert_running))
                self.handlers = []

            async def start(self):
                events_recorded.append(("start", asyncio.get_running_loop().is_running()))

            async def get_me(self):
                return SimpleNamespace(first_name="AuditBot", username="auditbot_runner")

            def on(self, event_builder):
                def _decorator(fn):
                    self.handlers.append((event_builder, fn))
                    return fn
                return _decorator

            async def run_until_disconnected(self):
                events_recorded.append(("run_until_disconnected", len(self.handlers)))

            async def disconnect(self):
                events_recorded.append(("disconnect", asyncio.get_running_loop().is_running()))

        class FakeOutboxWorker:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            async def run(self):
                return None

            def stop(self):
                events_recorded.append(("outbox_stop",))

        lock_mock = MagicMock()
        lock_mock.release.side_effect = lambda: events_recorded.append(("lock_release",))

        fake_pipeline = SimpleNamespace(
            reconciliation_gate=SimpleNamespace(run_startup_reconciliation=lambda: (True, "OK")),
            watcher=SimpleNamespace(
                execution_reporter=SimpleNamespace(dhan_response_enabled=False),
                run=lambda _pred: asyncio.sleep(0),
            ),
            notifier=SimpleNamespace(enabled=True, send_log_message=lambda _t: True),
            db=MagicMock(),
            dispatch_notification=lambda *a, **kw: None,
        )
        fake_cfg = SimpleNamespace(
            monitored_channel_ids=[-1001234567890],
            trading_mode=SimpleNamespace(value="TEST"),
            test_group_id=None,
            dhan_response_group_name="",
            log_group_id=None,
            instance_name="AuditBot",
        )

        async def fake_configure_dhan_response_group(**kwargs):
            events_recorded.append(("configure_dhan_response_group", kwargs["loop"].is_running()))
            return None

        main_coro = _compile_main_function({
            "asyncio": asyncio,
            "TelegramClient": LoopCheckingTelegramClient,
            "session": "test_session",
            "tg_api_id": 12345,
            "tg_api_hash": "0123456789abcdef0123456789abcdef",
            "cfg": fake_cfg,
            "pipeline": fake_pipeline,
            "configure_dhan_response_group": fake_configure_dhan_response_group,
            "is_lifecycle_open": lambda: False,
            "OutboxWorker": FakeOutboxWorker,
            "events": SimpleNamespace(
                NewMessage=lambda **kw: ("NewMessage", kw),
                MessageEdited=lambda **kw: ("MessageEdited", kw),
            ),
            "format_trade_log": lambda **kw: "",
            "lock": lock_mock,
            "logging": logging,
            "sys": sys,
            "time": time,
        })

        asyncio.run(main_coro())

        self.assertEqual(events_recorded[0], ("init", "test_session", 12345, "0123456789abcdef0123456789abcdef", True))
        self.assertIn(("start", True), events_recorded)
        self.assertIn(("configure_dhan_response_group", True), events_recorded)
        self.assertIn(("run_until_disconnected", 2), events_recorded)
        idx_outbox = events_recorded.index(("outbox_stop",))
        idx_disc = events_recorded.index(("disconnect", True))
        idx_lock = events_recorded.index(("lock_release",))
        self.assertLess(idx_outbox, idx_disc)
        self.assertLess(idx_disc, idx_lock)

    def test_03_disconnected_telethon_smoke_test_inside_asyncio_run(self):
        """3. Disconnected Telethon smoke test: requires real installed telethon package (never substitutes a fake client)."""
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        async def _smoke():
            client = TelegramClient(StringSession(), 12345, "0123456789abcdef0123456789abcdef")
            self.assertIsNotNone(client)
            self.assertFalse(client.is_connected())
            await client.disconnect()

        asyncio.run(_smoke())

    def test_04_lock_released_when_constructor_or_disconnect_raises(self):
        """4. Lock-release test when TelegramClient(...) constructor raises or client.disconnect() raises."""
        # Case A: TelegramClient constructor raises RuntimeError before client exists
        lock_a = MagicMock()

        def raising_constructor(*args, **kwargs):
            raise RuntimeError("Simulated Telethon constructor error")

        main_a = _compile_main_function({
            "asyncio": asyncio,
            "TelegramClient": raising_constructor,
            "session": "s",
            "tg_api_id": 12345,
            "tg_api_hash": "hash",
            "lock": lock_a,
        })
        with self.assertRaisesRegex(RuntimeError, "Simulated Telethon constructor error"):
            asyncio.run(main_a())
        lock_a.release.assert_called_once()

        # Case B: client.start() raises AND client.disconnect() also raises -> lock.release() still runs
        order_b = []
        lock_b = MagicMock()
        lock_b.release.side_effect = lambda: order_b.append("lock_released")

        class DisconnectFailingClient:
            def __init__(self, *args, **kwargs):
                order_b.append("constructed")

            async def start(self):
                order_b.append("start_failed")
                raise RuntimeError("Simulated start failure")

            async def disconnect(self):
                order_b.append("disconnect_failed")
                raise ConnectionError("Simulated disconnect failure")

        fake_cfg = SimpleNamespace(
            monitored_channel_ids=[-1001234567890],
            trading_mode=SimpleNamespace(value="TEST"),
            test_group_id=None,
        )
        main_b = _compile_main_function({
            "asyncio": asyncio,
            "TelegramClient": DisconnectFailingClient,
            "session": "s",
            "tg_api_id": 12345,
            "tg_api_hash": "hash",
            "cfg": fake_cfg,
            "lock": lock_b,
        })
        with self.assertRaisesRegex(ConnectionError, "Simulated disconnect failure"):
            asyncio.run(main_b())
        self.assertEqual(order_b, ["constructed", "start_failed", "disconnect_failed", "lock_released"])
        lock_b.release.assert_called_once()

    def test_05_outbox_stop_exception_still_disconnects_client_and_releases_lock(self):
        """5. Regression for P1 audit finding: outbox_worker.stop() raising must never skip client.disconnect() or lock.release()."""
        # Case A: outbox_worker.stop() raises RuntimeError -> client.disconnect() and lock.release() still run
        state = []

        class FakeClient:
            def __init__(self, *a, **kw):
                state.append("constructed")

            async def start(self):
                pass

            async def get_me(self):
                return SimpleNamespace(first_name="test", username="test")

            def on(self, *a, **kw):
                return lambda f: f

            async def run_until_disconnected(self):
                pass

            async def disconnect(self):
                state.append("disconnect")

        class FailOutbox:
            def __init__(self, **kw):
                pass

            async def run(self):
                pass

            def stop(self):
                state.append("stop_raises")
                raise RuntimeError("simulated stop error")

        lock = MagicMock()
        lock.release.side_effect = lambda: state.append("lock_released")
        cfg = SimpleNamespace(
            monitored_channel_ids=[-1001234567890],
            trading_mode=SimpleNamespace(value="TEST"),
            test_group_id=0,
            dhan_response_group_name="",
            log_group_id=0,
        )
        pipe = SimpleNamespace(
            reconciliation_gate=SimpleNamespace(run_startup_reconciliation=lambda: (True, "OK")),
            watcher=SimpleNamespace(execution_reporter=None, run=lambda _: asyncio.sleep(0)),
            notifier=SimpleNamespace(enabled=False),
            db=MagicMock(),
            dispatch_notification=lambda *a, **kw: None,
        )

        async def cg(**kwargs):
            return None

        func = _compile_main_function({
            "asyncio": asyncio,
            "TelegramClient": FakeClient,
            "session": "x",
            "tg_api_id": 1,
            "tg_api_hash": "a" * 32,
            "lock": lock,
            "cfg": cfg,
            "pipeline": pipe,
            "configure_dhan_response_group": cg,
            "is_lifecycle_open": lambda: False,
            "OutboxWorker": FailOutbox,
            "events": SimpleNamespace(NewMessage=lambda **k: None, MessageEdited=lambda **k: None),
            "format_trade_log": lambda **kw: "",
            "logging": logging,
            "sys": sys,
            "time": time,
        })
        with self.assertRaisesRegex(RuntimeError, "simulated stop error"):
            asyncio.run(func())
        self.assertEqual(state, ["constructed", "stop_raises", "disconnect", "lock_released"])
        self.assertEqual(lock.release.call_count, 1)

        # Case B: BOTH outbox_worker.stop() AND client.disconnect() raise -> lock.release() still runs
        state_both = []

        class FailDisconnectClient(FakeClient):
            def __init__(self, *a, **kw):
                state_both.append("constructed")

            async def disconnect(self):
                state_both.append("disconnect_raises")
                raise ConnectionError("simulated disconnect error")

        class FailOutboxBoth(FailOutbox):
            def stop(self):
                state_both.append("stop_raises")
                raise RuntimeError("simulated stop error")

        lock_both = MagicMock()
        lock_both.release.side_effect = lambda: state_both.append("lock_released")
        func_both = _compile_main_function({
            "asyncio": asyncio,
            "TelegramClient": FailDisconnectClient,
            "session": "x",
            "tg_api_id": 1,
            "tg_api_hash": "a" * 32,
            "lock": lock_both,
            "cfg": cfg,
            "pipeline": pipe,
            "configure_dhan_response_group": cg,
            "is_lifecycle_open": lambda: False,
            "OutboxWorker": FailOutboxBoth,
            "events": SimpleNamespace(NewMessage=lambda **k: None, MessageEdited=lambda **k: None),
            "format_trade_log": lambda **kw: "",
            "logging": logging,
            "sys": sys,
            "time": time,
        })
        with self.assertRaisesRegex(ConnectionError, "simulated disconnect error"):
            asyncio.run(func_both())
        self.assertEqual(state_both, ["constructed", "stop_raises", "disconnect_raises", "lock_released"])
        self.assertEqual(lock_both.release.call_count, 1)


if __name__ == "__main__":
    unittest.main()
