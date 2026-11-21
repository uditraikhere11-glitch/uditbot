"""Cached Telegram event metadata tests, no network calls."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from src.engine.telegram_event_metadata import source_name_for_event


class TestTelegramSourceLabel(unittest.IsolatedAsyncioTestCase):
    async def test_cached_chat_avoids_lookup(self):
        read=AsyncMock(side_effect=AssertionError("unnecessary get_chat"))
        event=SimpleNamespace(chat=SimpleNamespace(title="Signal Source"),
                              chat_id=-1001,get_chat=read)
        self.assertEqual(await source_name_for_event(event),"Signal Source")
        read.assert_not_awaited()

    async def test_uncached_chat_uses_original_lookup(self):
        read=AsyncMock(return_value=SimpleNamespace(title="Signal Source"))
        event=SimpleNamespace(chat=None,chat_id=-1001,get_chat=read)
        self.assertEqual(await source_name_for_event(event),"Signal Source")
        read.assert_awaited_once()

    async def test_missing_title_uses_chat_id(self):
        event=SimpleNamespace(chat=SimpleNamespace(title=None),
                              chat_id=-1001,get_chat=AsyncMock())
        self.assertEqual(await source_name_for_event(event),"-1001")
