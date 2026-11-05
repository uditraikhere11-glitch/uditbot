"""Offline regression gate for production Telethon event-loop initialization.

Python 3.14 no longer creates an implicit event loop. The production entrypoint
must instantiate TelegramClient *after* asyncio.run(main()) owns a running loop,
not by synthesizing a second unrelated loop in global module scope.
"""
import ast
import asyncio
from pathlib import Path
import unittest

from telethon import TelegramClient
from telethon.sessions import StringSession


RUN_PY = Path(__file__).resolve().parents[1] / "run.py"


class Python314RunnerLoopTests(unittest.TestCase):
    def test_production_runner_constructs_client_inside_async_main(self):
        module = ast.parse(RUN_PY.read_text(encoding="utf-8"))
        main = next(
            (n for n in module.body
             if isinstance(n, ast.AsyncFunctionDef) and n.name == "main"),
            None,
        )
        self.assertIsNotNone(main)

        # No eager top-level TelegramClient constructor before a loop exists.
        toplevel_expressions = [
            n for n in module.body
            if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        ]
        self.assertFalse(any(
            isinstance(node, ast.Call) and
            isinstance(node.func, ast.Name) and
            node.func.id == "TelegramClient"
            for statement in toplevel_expressions
            for node in ast.walk(statement)
        ), "Telethon constructor must not run at module-import time")

        constructors = [
            n for n in ast.walk(main)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "TelegramClient"
        ]
        self.assertEqual(len(constructors), 1)

    def test_runner_uses_single_asyncio_run_and_disconnects(self):
        module = ast.parse(RUN_PY.read_text(encoding="utf-8"))
        main = next(n for n in module.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "main")
        self.assertTrue(any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "asyncio"
            and n.func.attr == "run"
            and n.args
            and isinstance(n.args[0], ast.Call)
            and isinstance(n.args[0].func, ast.Name)
            and n.args[0].func.id == "main"
            for n in ast.walk(module)
        ))
        self.assertFalse(any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr in {"new_event_loop", "run_until_complete", "set_event_loop"}
            for n in ast.walk(module)
        ), "Do not create a parallel, manually managed event loop")
        self.assertTrue(any(
            isinstance(n, ast.Await)
            and isinstance(n.value, ast.Call)
            and isinstance(n.value.func, ast.Attribute)
            and n.value.func.attr == "disconnect"
            for n in ast.walk(main)
        ), "Telethon must disconnect during shutdown")

    def test_telethon_instantiates_inside_running_loop_without_network(self):
        async def smoke():
            self.assertIs(asyncio.get_running_loop(), asyncio.get_event_loop())
            client = TelegramClient(StringSession(), 12345, "0" * 32)
            try:
                self.assertFalse(client.is_connected())
            finally:
                await client.disconnect()
        asyncio.run(smoke())


if __name__ == "__main__":
    unittest.main()
