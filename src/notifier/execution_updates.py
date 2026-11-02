"""AB-ORDER-CONFIRM-01: presentation-only notifications from existing broker watcher.

No order placement, price sourcing, trading gates or protection mutations occur here.
The received-to-confirmed duration is an *observed confirmation interval*, NOT
claimed to be the precise exchange fill time.
"""
from __future__ import annotations

from datetime import datetime, timezone
from html import escape
import logging
import math
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _safe(value: Any) -> str:
    return escape(str(value or ""), quote=False)


def _money(value: Any) -> str:
    try:
        val = float(value)
        return f"₹{val:,.2f}" if math.isfinite(val) else "Unavailable"
    except (ValueError, TypeError):
        return "Unavailable"


def _ist(timestamp: Any) -> str:
    try:
        t = float(timestamp)
        if math.isfinite(t) and t > 0:
            return datetime.fromtimestamp(t, tz=timezone.utc).astimezone(IST).strftime("%H:%M:%S IST")
    except (TypeError, ValueError, OverflowError, OSError):
        pass
    return "Unavailable"


def _quote_warning(meta: Mapping[str, Any]) -> Optional[str]:
    """Never confuse policy acceptance with passing strict quote freshness."""
    try:
        age = float(meta.get("quote_age_sec"))
        threshold = float(meta.get("quote_threshold_sec", 2.0))
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(age) and math.isfinite(threshold)) or age <= threshold:
        return None
    qualifier = "Accepted under configured policy" if meta.get("quote_accepted", True) else "Freshness threshold exceeded"
    return f"⚠️ Yahoo Quote Age: {age:.2f}s ({qualifier})"


def _top(meta: Mapping[str, Any], qty: int, *, execution_price: Optional[float] = None) -> list[str]:
    symbol = _safe(meta.get("symbol", "UNKNOWN"))
    segment = _safe(meta.get("segment", "EQUITY"))
    action = _safe(meta.get("action", "BUY"))
    lines = [
        f"✅ <b>Trade Signal Processed: {symbol}</b>", "", "📊 <b>Trade Details</b>",
        f"• Action: {action} | {segment}",
        f"• Quantity: {qty} {'Share' if qty == 1 else 'Shares'}" if segment == "EQUITY" else f"• Quantity: {qty}",
    ]
    if meta.get("yahoo_cmp") is not None:
        lines.append(f"• Yahoo CMP: {_money(meta['yahoo_cmp'])}")
    lines.append(f"• Limit Price: {_money(meta.get('limit_price'))}")
    if execution_price is not None:
        lines.extend((f"• Execution Price: {_money(execution_price)}", f"• Trade Value: {_money(execution_price * qty)}"))
    return lines


def _tail(meta: Mapping[str, Any], timestamp: Any, label: str) -> list[str]:
    lines = []
    warning = _quote_warning(meta)
    if warning:
        lines.extend(("", warning))
    lines.extend((
        "", f"📩 Source: {_safe(meta.get('source', 'Unknown'))}",
        f"{'🟢' if str(meta.get('mode', '')).upper() == 'LIVE' else '🟡'} Mode: {_safe(meta.get('mode', 'TEST'))}",
        f"🕒 {label}: {_ist(timestamp)}",
    ))
    return lines


def format_order_submitted(meta: Mapping[str, Any], quantity: int) -> str:
    lines = _top(meta, quantity)
    lines.extend(("", "⏳ <b>Status: Order Submitted</b>", "Awaiting Dhan execution confirmation."))
    lines.extend(_tail(meta, meta.get("signal_received_at"), "Signal Received"))
    return "\n".join(lines)


def format_order_failed(meta: Mapping[str, Any], quantity: int, *, uncertain: bool = False) -> str:
    lines = _top(meta, quantity)
    if uncertain:
        lines.extend(("", "⚠️ <b>Status: Submission Unconfirmed</b>", "Broker response uncertain; automatic re-entry is not implied."))
    else:
        lines.extend(("", "❌ <b>Status: Order Rejected</b>", "Dhan did not acknowledge the order."))
    lines.extend(_tail(meta, meta.get("signal_received_at"), "Signal Received"))
    return "\n".join(lines)


def format_order_outcome(meta: Mapping[str, Any], requested_qty: int, filled_qty: int,
                         status: str, observed_at: float, execution_price: Optional[float],
                         protection_verified: bool) -> str:
    status = str(status).upper()
    complete = status == "TRADED" and filled_qty == requested_qty and execution_price is not None and execution_price > 0
    lines = _top(meta, requested_qty)
    if complete:
        lines.extend((
            f"• Execution Price: {_money(execution_price)}",
            f"• Trade Value: {_money(execution_price * filled_qty)}",
            "", "✅ <b>Status: Order Executed</b>",
            f"Filled: {filled_qty}/{requested_qty}",
        ))
    elif status == "PART_TRADED":
        if filled_qty > 0 and execution_price is not None and execution_price > 0:
            lines.extend((f"• Average Filled Price: {_money(execution_price)}",
                          f"• Executed Value: {_money(execution_price * filled_qty)}"))
        lines.extend(("", "⏳ <b>Status: Partially Executed</b>", f"Filled: {filled_qty}/{requested_qty}"))
    elif status == "UNVERIFIED_EXECUTION":
        lines.extend(("", "⚠️ <b>Status: Fill Verification Pending</b>", f"Broker-reported filled quantity: {filled_qty}/{requested_qty}"))
    else:
        if filled_qty > 0 and execution_price is not None and execution_price > 0:
            lines.extend((f"• Average Filled Price: {_money(execution_price)}",
                          f"• Executed Value: {_money(execution_price * filled_qty)}"))
        lines.extend(("", f"❌ <b>Status: Order {_safe(status.title())}</b>", f"Filled: {filled_qty}/{requested_qty}"))

    start = meta.get("signal_received_at")
    if complete and start is not None:
        try:
            elapsed = float(observed_at) - float(start)
            if math.isfinite(elapsed) and 0 <= elapsed < 86400 * 7:
                duration = f"{elapsed * 1000:.0f} ms" if elapsed < 1 else f"{elapsed:.2f} s"
                lines.extend(("", f"⏱️ Signal → Execution Confirmed: {duration}"))
        except (TypeError, ValueError):
            pass

    if filled_qty > 0:
        protected = "✅ Verified Active" if protection_verified else "⏳ Pending Broker Verification"
        lines.extend(("", f"🛡️ Protection: {protected}"))
    lines.extend(_tail(meta, observed_at, "Execution Confirmed" if complete else "Status Checked"))
    return "\n".join(lines)


class OrderStatusReporter:
    """Convert existing watcher observations to durable, deduplicated outbox events.

    Caller remains responsible for polling broker; reporter may read trade avg via
    existing read-only broker API but NEVER places/modifies/cancels orders.
    """
    def __init__(self, journal_db: Any, broker: Any):
        self.db = journal_db
        self.broker = broker

    def process(self, rec: dict, broker_order: dict, observed_at: float) -> bool:
        meta = rec.get("notification_meta")
        if not meta or str(meta.get("mode", "")).upper() != "LIVE":
            return True
        oid = str(rec.get("order_id") or "")
        if not oid:
            return True
        requested = int(rec.get("qty") or 0)
        filled = max(0, int(broker_order.get("filledQty") or 0))
        status = str(broker_order.get("orderStatus") or "").upper()
        if requested <= 0 or filled > requested:
            LOGGER.warning("Skipping invalid broker quantity for order %s", oid)
            return False
        protected = filled > 0 and int(rec.get("protected_qty") or 0) >= filled and not rec.get("pending_oco")

        price = None
        if filled > 0:
            try:
                p = float(broker_order.get("averageTradedPrice") or 0)
                if math.isfinite(p) and p > 0:
                    price = p
            except (TypeError, ValueError):
                pass

        # Full broker TRADED (or Super Order CLOSED with independently confirmed
        # trade price/quantity) is required before saying 'Order Executed'.
        closed_super = status == "CLOSED" and bool(rec.get("is_super_order"))
        if (status == "TRADED" or closed_super) and filled != requested:
            # Inconsistent broker status/quantity: never confirm execution or
            # silently terminalize a partially reported order.
            text = format_order_outcome(meta, requested, filled, "UNVERIFIED_EXECUTION", observed_at, price, protected)
            self.db.enqueue_order_notification_once(oid, "UNVERIFIED_EXECUTION", text)
            return False
        if filled == requested and (status == "TRADED" or closed_super):
            if price is None or closed_super:
                try:
                    broker_avg = self.broker.trades_avg_price(oid)
                    p = float(broker_avg) if broker_avg is not None else 0
                    if math.isfinite(p) and p > 0:
                        price = p
                except Exception as exc:
                    LOGGER.warning("Trade detail read failed for %s: %s", oid, exc)
            if price is None:
                text = format_order_outcome(meta, requested, filled, "UNVERIFIED_EXECUTION", observed_at, None, protected)
                self.db.enqueue_order_notification_once(oid, "UNVERIFIED_EXECUTION", text)
                return False  # keep terminal record under watcher until price verification
            event_key = "TRADED"
            text = format_order_outcome(meta, requested, filled, "TRADED", observed_at, price, protected)
            self.db.enqueue_order_notification_once(oid, event_key, text)
            if not protected:
                rec["notification_protection_pending"] = True
            elif rec.get("notification_protection_pending"):
                self.db.enqueue_order_notification_once(
                    oid, "PROTECTION_VERIFIED", "🛡️ <b>Protection Verified:</b> " + _safe(meta.get("symbol", "")) +
                    "\n✅ Verified active broker protection for confirmed filled quantity."
                )
                rec["notification_protection_pending"] = False
            return True

        if status == "PART_TRADED" and filled > 0:
            text = format_order_outcome(meta, requested, filled, status, observed_at, price, protected)
            self.db.enqueue_order_notification_once(oid, f"PART_TRADED_{filled}", text)
            return True

        if status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            text = format_order_outcome(meta, requested, filled, status, observed_at, price, protected)
            self.db.enqueue_order_notification_once(oid, f"FINAL_{status}_{filled}", text)
            return True
        # Pending/Transit/Triggered or indeterminate: leave initial awaiting card.
        return True
