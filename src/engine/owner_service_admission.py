"""Read-only Owner STOP admission fence; protects new entries, not exits."""
from __future__ import annotations
import fcntl
import json
import os
from pathlib import Path

def _record(path: Path):
    if path.is_symlink():
        raise ValueError("symlinked owner-control record")
    try:
        with path.open(encoding="utf-8") as f:
            result = json.load(f)
    except FileNotFoundError:
        return None
    if not isinstance(result, dict):
        raise ValueError("malformed owner-control record")
    return result

def owner_entries_permitted(data_dir: str) -> bool:
    """Respect pending STOP and active transitions under the coordinator flock.

    Legacy installations with neither record may start as before. A partially
    committed or malformed owner-control record never enables trading entries.
    """
    if not isinstance(data_dir, (str, os.PathLike)) or not str(data_dir):
        return False
    root = Path(data_dir)
    lock_path = root / "config_operation.lock"
    fd = None
    try:
        if lock_path.is_symlink():
            return False
        try:
            fd = os.open(str(lock_path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            pass
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                return False
        owner = _record(root / "service_control_state.json")
        gate = _record(root / "mode_transition_gate.json")
        if owner is None and gate is None:
            # Once the coordinator lock exists, missing owner records are
            # suspicious and must not reopen an entry gate.
            return fd is None
        if owner is None:
            return False
        if (owner.get("desired_service_state") != "RUNNING"
                or owner.get("observed_state") != "RUNNING"
                or owner.get("deferred_stop_pending") is not False):
            return False
        if gate is not None and gate.get("active") is not False:
            return False
        return True
    except (OSError, ValueError, TypeError):
        return False
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
