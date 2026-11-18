"""R02 deterministic polling policy. Does not place orders or certify protection.

Routine broker supervision must coexist with Dhan's durable hourly GET allowance.
An acknowledged manual-risk holding remains OPEN and is still supervised; only
its polling cadence differs from a pending/uncertain order. Broker admission is
always enforced independently by ReadGovernor.
"""
import math

POLL_HEARTBEAT_SECONDS = 2.0
NEW_ORDER_FAST_WINDOW_SECONDS = 45.0
NORMAL_OPEN_SECONDS = 45.0
MANUAL_RISK_OPEN_SECONDS = 120.0
STALE_PENDING_SECONDS = 75.0
AFTER_HOURS_SECONDS = 300.0
IDLE_MARKET_SECONDS = 90.0


def is_new_order_window(rec, now):
    """Use an actual placement timestamp, not stale reconciliation markers."""
    if not isinstance(rec, dict):
        return False
    try:
        created = float(rec.get("created_at") or 0)
        age = float(now) - created
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        math.isfinite(created) and created > 0
        and math.isfinite(age) and 0 <= age <= NEW_ORDER_FAST_WINDOW_SECONDS
    )


def is_amo_record(rec):
    """Recognize the existing watcher metadata, never infer order validity."""
    return isinstance(rec, dict) and (
        rec.get("is_amo") is True or rec.get("entry_only_amo") is True
        or str(rec.get("protection_route") or "").upper() == "ENTRY_ONLY_AMO"
    )


def watcher_cadence_seconds(*, pending, open_records, now, market_open=False):
    """Schedule broker GETs, independently of the fail-closed entry gate.

    Closed market: stale/unreadable/reconciliation flags cannot force 10s loops.
    Only an actually recent order may temporarily accelerate its first 45s.
    A newly placed after-hours AMO uses 10s (not 2s) to protect GET headroom.
    """
    pending_records = list((pending or {}).values())
    open_all = list(open_records or [])

    if not market_open:
        # Closed/DRY historical records are not newly placed live orders.
        # Ignore them even when they have a recent saved timestamp.
        recent = [
            rec for rec in pending_records
            if is_new_order_window(rec, now)
            and not str(rec.get("order_id") or "").startswith("DRY-")
        ]
        for rec in open_all:
            if not is_new_order_window(rec, now):
                continue
            if str(rec.get("order_id") or "").startswith("DRY-"):
                continue
            if str(rec.get("position_state") or "").upper() == "POSITION_CLOSED":
                continue
            try:
                active_filled = int(rec.get("filled_qty") or 0) > 0
            except (TypeError, ValueError, OverflowError):
                active_filled = False
            if active_filled:
                recent.append(rec)
        if recent:
            return 10.0 if any(
                is_amo_record(rec) or has_urgent_obligation(rec)
                for rec in recent
            ) else POLL_HEARTBEAT_SECONDS
        return AFTER_HOURS_SECONDS

    # Existing urgent market-hours supervision is deliberately unchanged.
    if any(has_urgent_obligation(rec) for rec in pending_records + open_all):
        return 10.0
    if pending_records:
        if any(is_new_order_window(rec, now) for rec in pending_records):
            return POLL_HEARTBEAT_SECONDS
        # Stale pending orders cannot exhaust the hourly broker GET budget.
        return max(STALE_PENDING_SECONDS, supervision_read_requirement(len(pending_records)) * 7.5)

    open_recs = [
        rec for rec in open_all
        if isinstance(rec, dict)
        and not str(rec.get("order_id") or "").startswith("DRY-")
        and int(rec.get("filled_qty") or 0) > 0
        and str(rec.get("position_state") or "") != "POSITION_CLOSED"
    ]
    if open_recs:
        if manual_risk_only(pending, open_recs):
            return MANUAL_RISK_OPEN_SECONDS
        return NORMAL_OPEN_SECONDS

    return IDLE_MARKET_SECONDS


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
    """Eligibility is computed from a fresh local, account-bound DB ack check.

    A status label alone is never an owner acknowledgement. Recorded exit
    legs or uncertain obligations exclude a holding from one-shot monitoring.
    """
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
