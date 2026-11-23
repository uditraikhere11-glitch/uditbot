"""Fail-closed owner STOP readout shared with broker entry admission."""
from __future__ import annotations
from src.engine.owner_service_admission import owner_entries_permitted

def owner_intent_blocks_new_entries(data_dir: str) -> bool:
    # Retain the public diagnostic API; new entry decisions are lock-coordinated.
    return not owner_entries_permitted(data_dir)
