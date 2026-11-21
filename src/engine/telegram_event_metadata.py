"""Resolve the Telegram source label without an avoidable get_chat request.

Pure metadata access when Telethon has the chat entity cached; preserves
existing await event.get_chat() fallback when it is not cached. No trading
state, side effects or broker calls.
"""
from __future__ import annotations


async def source_name_for_event(event) -> str:
    chat=getattr(event,"chat",None)
    if chat is None:
        chat=await event.get_chat()
    name=getattr(chat,"title",None)
    return str(name if name else event.chat_id)
