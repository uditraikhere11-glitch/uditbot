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
MANUAL_RISK_OPEN_SECONDS = 60.0
STALE_PENDING_SECONDS = 75.0
AFTER_HOURS_SECONDS = 300.0
IDLE_MARKET_SECONDS = 90.0


def watcher_cadence_seconds(*, pending, open_records, now, market_open=False):
    """Return a scheduled GET cycle interval; never suppress an open record."""
    pending_records = list((pending or {}).values())
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
        # Repeated stale pending reads are never allowed to consume the
        # entire hourly budget just because their status is unresolved.
        return max(STALE_PENDING_SECONDS, supervision_read_requirement(len(pending_records)) * 7.5)

    open_recs = [
        rec for rec in (open_records or [])
        if isinstance(rec, dict)
        and not str(rec.get("order_id") or "").startswith("DRY-")
        and int(rec.get("filled_qty") or 0) > 0
        and str(rec.get("position_state") or "") != "POSITION_CLOSED"
    ]
    if open_recs:
        # Broker-side SL/Target orders remain live outside exchange hours;
        # slower verification still monitors holdings without burning the
        # durable 24-hour GET quota overnight.
        if not market_open:
            return AFTER_HOURS_SECONDS
        if all(
            str(rec.get("position_state") or "") == "UNPROTECTED_MANUAL_RISK"
            for rec in open_recs
        ):
            return MANUAL_RISK_OPEN_SECONDS
        return NORMAL_OPEN_SECONDS

    return IDLE_MARKET_SECONDS if market_open else AFTER_HOURS_SECONDS


def supervision_read_requirement(pending_count):
    """Conservative planning allowance; final Dhan GETs remain atomically admitted."""
    return min(40, 8 + 2 * max(0, int(pending_count or 0)))
