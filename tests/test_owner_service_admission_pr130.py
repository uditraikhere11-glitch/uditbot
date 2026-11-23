"""Owner STOP admission tests: no Dhan, Telegram or service mutations."""
import fcntl
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from src.engine.owner_service_admission import owner_entries_permitted

def save(root, name, data):
    (root/name).write_text(json.dumps(data), encoding="utf-8")

def running(root):
    save(root,"service_control_state.json",{"desired_service_state":"RUNNING","observed_state":"RUNNING","deferred_stop_pending":False})
    save(root,"mode_transition_gate.json",{"active":False})

def test_running_and_empty_legacy():
    with TemporaryDirectory() as d:
        assert owner_entries_permitted(d)
        running(Path(d))
        assert owner_entries_permitted(d)

def test_deferred_stop_and_active_gate():
    with TemporaryDirectory() as d:
        p=Path(d);running(p)
        save(p,"mode_transition_gate.json",{"active":True})
        assert not owner_entries_permitted(d)
        save(p,"mode_transition_gate.json",{"active":False})
        save(p,"service_control_state.json",{"desired_service_state":"STOPPED","observed_state":"RUNNING","deferred_stop_pending":True})
        assert not owner_entries_permitted(d)

def test_interrupted_cancel_stays_blocked():
    with TemporaryDirectory() as d:
        p=Path(d);running(p)
        save(p,"mode_transition_gate.json",{"active":True})
        assert not owner_entries_permitted(d)
        save(p,"mode_transition_gate.json",{"active":False})
        assert owner_entries_permitted(d)

def test_invalid_control_fails_closed():
    with TemporaryDirectory() as d:
        p=Path(d);running(p)
        (p/"service_control_state.json").write_text("corrupt")
        assert not owner_entries_permitted(d)
        running(p)
        (p/"mode_transition_gate.json").write_text("corrupt")
        assert not owner_entries_permitted(d)

def test_flock_contention_fails_closed():
    with TemporaryDirectory() as d:
        p=Path(d);running(p)
        with (p/"config_operation.lock").open("w+") as f:
            fcntl.flock(f.fileno(),fcntl.LOCK_EX)
            assert not owner_entries_permitted(d)
            fcntl.flock(f.fileno(),fcntl.LOCK_UN)
        assert owner_entries_permitted(d)

def test_symlink_gate_fails_closed():
    with TemporaryDirectory() as d:
        p=Path(d);running(p)
        (p/"mode_transition_gate.json").unlink()
        (p/"mode_transition_gate.json").symlink_to(p/"service_control_state.json")
        assert not owner_entries_permitted(d)

def test_missing_owner_with_gate_fails_closed():
    with TemporaryDirectory() as d:
        p=Path(d)
        save(p,"mode_transition_gate.json",{"active":False})
        assert not owner_entries_permitted(d)

def test_missing_owner_records_with_existing_control_lock_block():
    with TemporaryDirectory() as d:
        p=Path(d)
        (p/"config_operation.lock").write_text("")
        assert not owner_entries_permitted(d)
