"""PR130 explicit owner-cancellation tests using isolated temporary control files."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from src.engine.config_manager import ConfigChangeTracker


def _prepare(root):
    t = ConfigChangeTracker(data_dir=str(root))
    assert t.set_transition_gate(
        True, change_id="OWNER-STOP-PENDING",
        target_mode="STOPPED", reason="Test deferred Stop"
    ).ok
    t.record_service_control_intent(
        desired_service_state="STOPPED", last_action="OWNER_STOP",
        reason="Test deferred Stop", requested_mode="TEST",
        observed_state="RUNNING", deferred_stop_pending=True,
    )
    return t


def _fake_status(tracker):
    return {
        "service_state": "RUNNING",
        "runtime_verified": True,
        "saved_mode": "TEST",
        "systemd_properties": {"MainPID": 10001},
        "owner_intent": tracker.get_service_control_record(),
        "transition_gate": tracker.inspect_transition_gate_status(read_only=True),
    }


def test_explicit_owner_cancellation_does_not_restart():
    with TemporaryDirectory() as d:
        t = _prepare(Path(d))
        with patch.object(t, "control_service", side_effect=lambda **kw: _fake_status(t)):
            result = t.cancel_deferred_owner_stop()
        assert result["ok"] is True, result
        assert result["restart_performed"] is False
        assert t.get_service_control_record()["deferred_stop_pending"] is False
        assert t.inspect_transition_gate_status()["active"] is False


def test_unrelated_transition_not_cancelled():
    with TemporaryDirectory() as d:
        t = _prepare(Path(d))
        t.set_transition_gate(True, change_id="ANOTHER-OPERATION", target_mode="TEST")
        with patch.object(t, "control_service", side_effect=lambda **kw: _fake_status(t)):
            result = t.cancel_deferred_owner_stop()
        assert result["ok"] is False
        assert t.get_service_control_record()["deferred_stop_pending"] is True
        assert t.inspect_transition_gate_status()["active"] is True


def test_wrong_runtime_does_not_cancel_owner_stop():
    with TemporaryDirectory() as d:
        t = _prepare(Path(d))
        with patch.object(t, "control_service", return_value={
            "service_state": "RUNNING", "runtime_verified": False,
            "saved_mode": "TEST", "systemd_properties": {"MainPID": 10001}
        }):
            result = t.cancel_deferred_owner_stop()
        assert result["ok"] is False
        assert t.get_service_control_record()["deferred_stop_pending"] is True


def test_interrupted_cancellation_can_retry_without_restart():
    with TemporaryDirectory() as d:
        t = _prepare(Path(d))
        t.record_service_control_intent(
            desired_service_state="RUNNING", last_action="OWNER_CANCEL_STOP",
            reason="Interrupted after owner write", requested_mode="TEST",
            observed_state="RUNNING", deferred_stop_pending=False,
        )
        with patch.object(t, "control_service", side_effect=lambda **kw: _fake_status(t)):
            result = t.cancel_deferred_owner_stop()
        assert result["ok"] is True, result
        assert t.inspect_transition_gate_status()["active"] is False
