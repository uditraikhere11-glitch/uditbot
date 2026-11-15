"""Fail-closed, broker-independent Dhan Response destination activation.

Never routes trade signals, repairs Telegram sessions or submits orders.
A saved previous destination is only a fallback *candidate*; Telegram group
identity, membership, uniqueness and signal-source exclusion must be verified.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Iterable, Optional, Tuple

from src.notifier import dhan_response
from src.notifier.dhan_response import normalize_group_name
from src.engine.config_manager import write_json_atomic

logger = logging.getLogger(__name__)


def _pending_file(data_dir: str) -> str:
    return os.path.join(data_dir, 'dhan_response_pending_activation.json')


def _activation_file(data_dir: str) -> str:
    return os.path.join(data_dir, 'dhan_response_activation.json')


def stage_destination_change(data_dir: str, previous: str, requested: str) -> None:
    """Preserve original fallback if the operator makes repeated pending edits."""
    old = normalize_group_name(previous)
    new = normalize_group_name(requested)
    try:
        with open(_pending_file(data_dir), encoding='utf-8') as fh:
            pending = json.load(fh)
        if (isinstance(pending, dict) and pending.get('requested_name') == old
                and pending.get('target_verified') is not True):
            candidate = normalize_group_name(pending.get('previous_name', ''))
            if candidate and candidate != new:
                old = candidate
    except (OSError, ValueError, TypeError):
        pass
    write_json_atomic(_pending_file(data_dir), {
        'previous_name': old, 'requested_name': new,
        'target_verified': False, 'staged_at': time.time(),
    }, mode=0o600)


def _fallback_for(data_dir: str, requested: str) -> str:
    try:
        with open(_pending_file(data_dir), encoding='utf-8') as fh:
            pending = json.load(fh)
        if not isinstance(pending, dict) or pending.get('requested_name') != requested:
            return ''
        old = normalize_group_name(pending.get('previous_name', ''))
        return old if old != requested else ''
    except (OSError, ValueError, TypeError):
        return ''


def read_active_destination(data_dir: str) -> Optional[dict]:
    try:
        with open(_activation_file(data_dir), encoding='utf-8') as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError, TypeError):
        return None


async def resolve_with_fallback(
    client: Any, requested: str, forbidden_chat_ids: Iterable[int], data_dir: str,
) -> Tuple[Optional[Any], str, bool]:
    """Return (verified entity, active group, requested destination verified).

    Only a resolved dialog from the authenticated session can become active.
    An intentional blank value disables response mirroring without fallback.
    """
    wanted = normalize_group_name(requested)
    entity = None
    active = ''
    verified = False
    reason = 'REQUESTED_DESTINATION_UNRESOLVED'
    if not wanted:
        reason, verified = 'INTENTIONALLY_DISABLED', True
    else:
        try:
            entity = await dhan_response.resolve_dhan_response_group(client, wanted, forbidden_chat_ids)
            active, verified, reason = wanted, True, 'REQUESTED_DESTINATION_RESOLVED'
        except Exception as exc:
            logger.warning('Dhan Response target verification failed: %s', type(exc).__name__)
            fallback = _fallback_for(data_dir, wanted)
            if fallback:
                try:
                    entity = await dhan_response.resolve_dhan_response_group(client, fallback, forbidden_chat_ids)
                    active, reason = fallback, 'PREVIOUS_DESTINATION_RESTORED'
                except Exception as err:
                    logger.warning('Dhan Response fallback unavailable: %s', type(err).__name__)
    # Distinct consumer acknowledgement, never inferred from saved .env alone.
    try:
        write_json_atomic(_activation_file(data_dir), {
            'requested_name': wanted, 'active_name': active,
            'target_verified': bool(verified),
            'fallback_active': bool(active and active != wanted),
            'reason_code': reason, 'pid': os.getpid(),
            'acknowledged_at': time.time(),
        }, mode=0o600)
    except OSError:
        logger.warning('Dhan Response activation acknowledgement could not be persisted')
    if verified and wanted:
        try:
            write_json_atomic(_pending_file(data_dir), {
                'previous_name': wanted, 'requested_name': wanted,
                'target_verified': True, 'staged_at': time.time(),
            }, mode=0o600)
        except OSError:
            logger.warning('Dhan Response known-good checkpoint failed')
    return entity, active, verified
