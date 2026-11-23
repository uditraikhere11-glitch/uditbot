"""Read-only fail-closed Owner STOP and transition admission evidence."""
from __future__ import annotations
import json
from pathlib import Path

def owner_intent_blocks_new_entries(data_dir: str) -> bool:
    if not data_dir:
        return False
    root = Path(data_dir)
    for path, name in ((root / "service_control_state.json", "owner"),
                       (root / "mode_transition_gate.json", "transition")):
        try:
            if path.is_symlink():
                return True
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError, ValueError, TypeError):
            return True
        if not isinstance(data, dict):
            return True
        if name == "owner":
            if (data.get("deferred_stop_pending") is True
                    or str(data.get("desired_service_state") or "").upper() == "STOPPED"):
                return True
            if (type(data.get("deferred_stop_pending")) is not bool
                    or str(data.get("desired_service_state") or "").upper() != "RUNNING"
                    or str(data.get("observed_state") or "").upper() != "RUNNING"):
                return True
        elif type(data.get("active")) is not bool or data["active"]:
            return True
    return False
