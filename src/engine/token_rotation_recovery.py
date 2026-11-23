"""Fast serialized reconciliation recovery when a rotated Dhan token is adopted.

Strictly advisory scheduling hook:
- Never opens the entry gate directly.
- Never bypasses Emergency STOP or ReadGovernor admission.
- Rate-limited to at most one trigger per 30 seconds.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from src.engine.reconciliation import ReconciliationState

logger = logging.getLogger(__name__)


def notify_token_rotated(gate: Any, *, min_interval_sec: float = 30.0) -> bool:
    """Clears the 600s background retry backoff and schedules a serialized recheck when BLOCKED."""
    if gate is None:
        return False
    if getattr(gate, "_in_startup_reconciliation_pass", False):
        return False
    if getattr(gate, "state", None) != ReconciliationState.BLOCKED:
        return False
    if not getattr(gate, "db", None):
        return False
    stop_mgr = getattr(gate, "stop_manager", None)
    if stop_mgr is None or not hasattr(stop_mgr, "is_stopped"):
        return False
    try:
        if stop_mgr.is_stopped():
            return False
    except Exception:
        return False

    now = time.monotonic()
    last_trigger = float(getattr(gate, "_last_token_rotation_recheck_at", 0.0) or 0.0)
    if last_trigger > 0.0 and (now - last_trigger) < float(min_interval_sec):
        return False

    schedule = getattr(gate, "_schedule_historical_background_recheck", None)
    if not callable(schedule):
        return False

    gate._last_token_rotation_recheck_at = now
    gate._historical_background_retry_after = 0.0
    logger.info(
        "[RECONCILIATION] Rotated Dhan access token adopted while gate is BLOCKED; "
        "scheduling immediate serialized verification recheck."
    )
    schedule()
    return True


def install_token_rotation_fast_recovery(broker: Any, gate: Any) -> bool:
    """Idempotently wraps `broker._sync_shared_token_from_disk` to notify `gate` on token change."""
    orig = getattr(broker, "_sync_shared_token_from_disk", None)
    if not callable(orig):
        return False
    if getattr(orig, "_token_rotation_recovery_wrapped", False):
        return True

    def _wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        prev_token = getattr(broker, "token", None)
        result = orig(*args, **kwargs)
        curr_token = getattr(broker, "token", None)
        if curr_token and curr_token != prev_token:
            try:
                notify_token_rotated(gate)
            except Exception as exc:
                logger.warning(
                    "Post-rotation reconciliation trigger skipped: %s",
                    type(exc).__name__,
                )
        return result

    _wrapped_sync._token_rotation_recovery_wrapped = True  # type: ignore[attr-defined]
    broker._sync_shared_token_from_disk = _wrapped_sync
    return True
