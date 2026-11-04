"""Final Dhan response group routing over the existing authenticated Telegram session.

The group title is never used directly as an API destination: the authenticated
user's dialog list must contain exactly one matching *group*. Duplicates,
missing groups and monitored signal-source groups are rejected.
"""
from __future__ import annotations

import logging
import unicodedata
from typing import Any, Iterable

logger = logging.getLogger(__name__)


def normalize_group_name(value: Any) -> str:
    name = unicodedata.normalize("NFKC", str(value or "").strip())
    if len(name) > 100 or any(c in name for c in "\r\n\x00=#"):
        raise ValueError("Dhan Response group name must be at most 100 characters, without newlines, '=' or '#'.")
    return name


async def resolve_dhan_response_group(client: Any, name: str, forbidden_chat_ids: Iterable[int] = ()) -> Any:
    """Return one exact-name Telegram group entity, never a title-based send target."""
    wanted = normalize_group_name(name)
    if not wanted:
        raise ValueError("Dhan Response group is not configured.")
    matches = []
    async for dialog in client.iter_dialogs():
        if not getattr(dialog, "is_group", False):
            continue
        if normalize_group_name(getattr(dialog, "name", "")).casefold() == wanted.casefold():
            matches.append(dialog)
    if len(matches) != 1:
        raise ValueError(
            "Dhan Response group must match exactly one joined Telegram group "
            f"(found {len(matches)}). Check the name or duplicate group titles."
        )
    dialog = matches[0]
    if int(dialog.id) in {int(x) for x in forbidden_chat_ids}:
        raise ValueError("Dhan Response group cannot also be a monitored trade-signal source.")
    return dialog.entity
