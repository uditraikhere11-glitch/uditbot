"""R02 deterministic broker supervision cadence; never certifies protection."""
import math

POLL_HEARTBEAT_SECONDS = 2.0
NEW_ORDER_FAST_WINDOW_SECONDS = 45.0
NORMAL_OPEN_SECONDS = 45.0
MANUAL_RISK_OPEN_SECONDS = 120.0
STALE_PENDING_SECONDS = 75.0
AFTER_HOURS_SECONDS = 300.0
IDLE_MARKET_SECONDS = 90.0


def watcher_cadence_seconds(*, pending, open_records, now, market_open=False):
    """Schedule checks while observing closed-session 300s cadence.

    State changes independently wake the watcher. A stale reconciliation flag
    must not force a continuous 10s GET loop while the exchange is closed.
    """
    pending_records = list((pending or {}).values())
    urgent_records = [rec for rec in pending_records + list(open_records or [])
                      if has_urgent_obligation(rec)]
    if urgent_records:
        # The only safe after-hours downgrade is a *stale read-only* mismatch,
        # never an uncertain cancellation, OCO placement, pending retry or
        # broker mutation whose protection can be affected by delay.
        if not market_open and all(_stale_read_reconciliation_only(rec, now)
                                   for rec in urgent_records):
            return AFTER_HOURS_SECONDS
        return 10.0
    if pending_records:
        for rec in pending_records:
            if not isinstance(rec, dict):
                continue
            try:
                created = float(rec.get("created_at") or 0)
            except (TypeError, ValueError, OverflowError):
                created = 0
            if math.isfinite(created) and created > 0 and 0 <= now - created <= NEW_ORDER_FAST_WINDOW_SECONDS:
                return POLL_HEARTBEAT_SECONDS
        if not market_open:
            return AFTER_HOURS_SECONDS
        return max(STALE_PENDING_SECONDS, supervision_read_requirement(len(pending_records)) * 7.5)

    open_recs = [
        rec for rec in (open_records or [])
        if isinstance(rec, dict)
        and not str(rec.get("order_id") or "").startswith("DRY-")
        and int(rec.get("filled_qty") or 0) > 0
        and str(rec.get("position_state") or "") != "POSITION_CLOSED"
    ]
    if open_recs:
        if not market_open:
            return AFTER_HOURS_SECONDS
        if manual_risk_only(pending, open_recs):
            return MANUAL_RISK_OPEN_SECONDS
        return NORMAL_OPEN_SECONDS
    return IDLE_MARKET_SECONDS if market_open else AFTER_HOURS_SECONDS



def _stale_read_reconciliation_only(rec, now):
    if not isinstance(rec, dict):
        return False
    if not (rec.get("reconciliation_required") or rec.get("unreadable_broker_response")
            or str(rec.get("position_state") or "").upper() == "RECONCILIATION_REQUIRED"):
        return False
    if str(rec.get("position_state") or "").upper() in (
        "PERSISTENCE_UNCERTAIN", "EXIT_TRIGGERED", "EXIT_PART_FILLED", "MANUAL_INTERVENTION_REQUIRED",
    ):
        return False
    if any(rec.get(key) for key in (
        "pending_oco", "cancel_uncertain", "cancel_requested", "entry_timeout_elapsed",
        "retry_reserved", "persistence_uncertain", "orphaned_legs_detected",
        "orphaned_leg_cancel_states", "exit_order_id", "sl_order_id", "tp_order_id",
    )):
        return False
    created = rec.get("created_at")
    if created is not None:
        try:
            age = float(now) - float(created)
            if not math.isfinite(age) or age < NEW_ORDER_FAST_WINDOW_SECONDS:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
    return True


def supervision_read_requirement(pending_count):
    """Conservative planning allowance; final Dhan GETs remain atomically admitted."""
    return min(40, 8 + 2 * max(0, int(pending_count or 0)))


def has_urgent_obligation(rec):
    if not isinstance(rec, dict):
        return True
    attempt = rec.get("pending_oco")
    return (
        str(rec.get("position_state") or "").upper() in (
            "RECONCILIATION_REQUIRED", "PERSISTENCE_UNCERTAIN", "EXIT_TRIGGERED",
            "EXIT_PART_FILLED", "MANUAL_INTERVENTION_REQUIRED",
        )
        or bool(attempt)
        or any(bool(rec.get(key)) for key in (
            "cancel_uncertain", "cancel_requested", "entry_timeout_elapsed",
            "retry_reserved", "persistence_uncertain", "reconciliation_required",
            "unreadable_broker_response",
        ))
    )


def manual_risk_only(pending, records):
    """Only account-bound acknowledged risk may use manual-risk cadence."""
    if pending:
        return False
    opened = [r for r in records if isinstance(r, dict)
              and not str(r.get("order_id") or "").startswith("DRY-")
              and int(r.get("filled_qty") or 0) > 0
              and str(r.get("position_state") or "") != "POSITION_CLOSED"]
    return bool(opened) and all(
        r.get("_schedule_ack_verified") is True
        and r.get("position_state") == "UNPROTECTED_MANUAL_RISK"
        and not has_urgent_obligation(r)
        and not any(r.get(key) for key in (
            "sl_order_id", "tp_order_id", "exit_order_id", "confirmed_protection_ids",
            "verified_leg_order_ids", "verified_leg_map", "orphaned_leg_cancel_states",
        ))
        for r in opened
    )
