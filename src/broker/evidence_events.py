"""Publish non-secret generation invalidation after existing durable events.

No sidecar is created by legacy writers. An initialized sidecar is invalidated
in its own short transaction, never inside a journal write transaction or
network call. Existing order and Emergency STOP outcomes are not rewritten.
"""
from __future__ import annotations

import logging
import os
import sqlite3

logger=logging.getLogger(__name__)


def note_event(data_dir, account, reason):
    from src.broker.snapshot_coordinator import EvidenceUnavailable, invalidate_existing
    if not isinstance(data_dir,(str,os.PathLike)) or not data_dir or type(account) is not str or not account:
        return True
    try:
        invalidate_existing(data_dir,account,reason)
        return True
    except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError):
        logger.error("Broker evidence generation invalidation failed (%s)",reason)
        return False


def note_owner_event(data_dir, reason):
    from src.broker.snapshot_coordinator import EvidenceUnavailable, invalidate_owner_scope
    if not isinstance(data_dir,(str,os.PathLike)) or not data_dir: return True
    try:
        invalidate_owner_scope(data_dir,reason)
        return True
    except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError):
        logger.error("Broker owner evidence invalidation failed (%s)",reason)
        return False
