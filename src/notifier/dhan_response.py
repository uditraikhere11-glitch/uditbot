"""Final Dhan response group routing over the existing authenticated Telegram session.

Neither the group title nor an unverified numeric chat ID is sent directly.
The authenticated user's Telegram dialog list must contain exactly one matching
group; missing groups, duplicate titles, and monitored signal-source groups
are rejected.
"""
from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from typing import Any, Callable, Iterable, Optional

logger = logging.getLogger(__name__)


def normalize_group_name(value: Any) -> str:
    name = unicodedata.normalize("NFKC", str(value or "").strip())
    if len(name) > 100 or any(c in name for c in "\r\n\x00=#"):
        raise ValueError("Dhan Response group name or ID must be at most 100 characters, without newlines, '=' or '#'.")
    return name


async def resolve_dhan_response_group(client: Any, name: str, forbidden_chat_ids: Iterable[int] = ()) -> Any:
    """Resolve a joined Telegram group by exact title or its peer chat ID.

    Numeric IDs must be group peers present in the authenticated account's own
    dialogs. Never treat an arbitrary integer as a send target. The forbidden
    signal-source group list is checked for both name and ID lookup.
    """
    wanted = normalize_group_name(name)
    if not wanted:
        raise ValueError("Dhan Response group is not configured.")

    # Telethon dialog.id is already the signed Telegram peer ID used in bot status
    # and WebUI. A negative signed ID disambiguates groups from users/channels.
    by_id = bool(re.fullmatch(r"-[0-9]{5,20}", wanted))
    wanted_id = int(wanted) if by_id else None
    forbidden = {int(x) for x in forbidden_chat_ids}

    matches = []
    async for dialog in client.iter_dialogs():
        if not getattr(dialog, "is_group", False):
            continue
        if by_id:
            matched = int(dialog.id) == wanted_id
        else:
            matched = normalize_group_name(getattr(dialog, "name", "")).casefold() == wanted.casefold()
        if matched:
            matches.append(dialog)

    if len(matches) != 1:
        raise ValueError(
            "Dhan Response destination must match exactly one joined Telegram group "
            f"(found {len(matches)}). Verify the group name or chat ID and account membership."
        )
    dialog = matches[0]
    if int(dialog.id) in forbidden:
        raise ValueError("Dhan Response group cannot also be a monitored trade-signal source.")
    return dialog.entity


async def configure_dhan_response_group(
    client: Any,
    group_name: str,
    signal_source_ids: Iterable[int],
    reporter: Any,
    loop: Any,
) -> Optional[Callable[[str], bool]]:
    """Bind the existing watcher and outbox to a verified Telegram destination.

    This is notification-only. A group lookup failure must not affect the
    trading listener, source allowlist, STOP, orders or protection watcher.
    """
    if reporter is not None:
        reporter.dhan_response_enabled = False
    if not group_name:
        logger.info("Dhan Response group not configured; response mirroring disabled.")
        return None
    if reporter is None:
        logger.warning("Dhan Response group unavailable; execution reporter is not initialized.")
        return None
    try:
        entity = await asyncio.wait_for(
            resolve_dhan_response_group(
                client, group_name, forbidden_chat_ids=signal_source_ids
            ), timeout=15.0,
        )
    except Exception as exc:
        logger.warning("Dhan Response group unavailable; mirroring disabled: %s", exc)
        return None

    def dispatch_dhan_response(message: str) -> bool:
        """Invoked only from the existing outbox worker's background thread."""
        future = asyncio.run_coroutine_threadsafe(
            client.send_message(entity, message, parse_mode="html"), loop
        )
        try:
            future.result(timeout=12.0)
            return True
        except Exception as exc:
            future.cancel()
            logger.warning("Dhan Response Telegram delivery unsuccessful: %s", type(exc).__name__)
            return False

    reporter.dhan_response_enabled = True
    logger.info("Dhan final response group resolved uniquely: %s", group_name)
    return dispatch_dhan_response
