"""AB-ORDER-CONFIRM-01 & P0-07/P0-08: presentation-only notifications from existing broker watcher.

No order placement, price sourcing, trading gates or protection mutations occur here.
The received-to-confirmed duration is an *observed confirmation interval*, NOT
claimed to be the precise exchange fill time.
Never exposes broker Order IDs in normal Telegram cards, and never claims a fill,
active protection, or closed position without authoritative broker evidence.
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


def _signed_money(value: Any) -> str:
    try:
        val = float(value)
        if not math.isfinite(val):
            return "Unavailable"
        sign = "+" if val >= 0 else "-"
        return f"{sign}₹{abs(val):,.2f}"
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


def _contract_details(meta: Mapping[str, Any], qty: int) -> list[str]:
    """Renders option contract details (symbol, strike, CE/PE, expiry, lots, qty) when present."""
    segment = str(meta.get("segment", "EQUITY")).upper()
    if segment not in ("FNO", "OPTION", "NSE_FNO", "BSE_FNO"):
        return []
    out: list[str] = []
    strike = meta.get("strike")
    opt_type = str(meta.get("option_type") or "").strip().upper()
    expiry = str(meta.get("expiry") or "").strip()
    lot_size = int(meta.get("lot_size") or 0)
    lots = int(meta.get("lots") or (qty // lot_size if lot_size > 0 else 0))
    contract_parts: list[str] = [str(meta.get("symbol") or "")]
    if strike is not None:
        try:
            s_val = float(strike)
            if s_val > 0:
                contract_parts.append(f"{s_val:g}")
        except (ValueError, TypeError):
            pass
    if opt_type in ("CE", "PE"):
        contract_parts.append(opt_type)
    if expiry:
        contract_parts.append(f"({expiry})")
    if len(contract_parts) > 1:
        out.append(f"• Contract: {_safe(' '.join(contract_parts))}")
    if lots > 0 and lot_size > 0:
        out.append(f"• Lots: {lots} ({qty} Qty @ {lot_size}/lot)")
    return out


def _top(meta: Mapping[str, Any], qty: int, *, execution_price: Optional[float] = None) -> list[str]:
    symbol = _safe(meta.get("symbol", "UNKNOWN"))
    segment = _safe(meta.get("segment", "EQUITY"))
    action = _safe(meta.get("action", "BUY"))
    lines = [
        f"✅ <b>Trade Signal Processed: {symbol}</b>", "", "📊 <b>Trade Details</b>",
        f"• Action: {action} | {segment}",
    ]
    lines.extend(_contract_details(meta, qty))
    lines.append(
        f"• Quantity: {qty} {'Share' if qty == 1 else 'Shares'}" if segment == "EQUITY" else f"• Quantity: {qty}"
    )
    if meta.get("yahoo_cmp") is not None:
        lines.append(f"• Yahoo CMP: {_money(meta['yahoo_cmp'])}")
    if meta.get("stated_price") is not None:
        try:
            sp = float(meta["stated_price"])
            if sp > 0:
                mk = meta.get("markup_pct")
                mk_str = f" (+{float(mk):g}% Markup)" if mk is not None else ""
                lines.append(f"• Stated Price: {_money(sp)}{mk_str}")
        except (ValueError, TypeError):
            pass
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


def format_signal_accepted(meta: Mapping[str, Any], quantity: int) -> str:
    """P0-07: SIGNAL_ACCEPTED card before broker submission."""
    lines = _top(meta, quantity)
    lines.extend(("", "🟢 <b>Status: Signal Accepted</b>", "Validated against sizing and risk controls; preparing broker submission."))
    lines.extend(_tail(meta, meta.get("signal_received_at"), "Signal Received"))
    return "\n".join(lines)


def format_order_submitted(meta: Mapping[str, Any], quantity: int) -> str:
    lines = _top(meta, quantity)
    if meta.get("is_amo"):
        lines.extend(("", "🌙 <b>Status: AMO Submitted</b>",
                      "Dhan acknowledged the after-market order; queued for market OPEN, not executed."))
    else:
        lines.extend(("", "⏳ <b>Status: Order Submitted</b>", "Awaiting Dhan execution confirmation."))
    if meta.get("protection_route"):
        lines.append(f"• Protection Route: {_safe(meta.get('protection_route'))}")
    if meta.get("initial_sl_price") and meta.get("initial_target_price"):
        lines.append(
            f"• Initial Limit-Relative SL / Target: {_money(meta.get('initial_sl_price'))} / {_money(meta.get('initial_target_price'))} (Re-anchors on fill)"
        )
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


def format_order_outcome(
    meta: Mapping[str, Any],
    requested_qty: int,
    filled_qty: int,
    status: str,
    observed_at: float,
    execution_price: Optional[float],
    protection_verified: bool,
    *,
    sl_price: Optional[float] = None,
    target_price: Optional[float] = None,
    protection_route: Optional[str] = None,
) -> str:
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
        if protection_route:
            lines.append(f"• Protection Route: {_safe(protection_route)}")
        if sl_price and target_price and sl_price > 0 and target_price > 0:
            lbl = "Verified SL / Target" if protection_verified else "Pending SL / Target"
            lines.append(f"• {lbl}: {_money(sl_price)} / {_money(target_price)}")
    lines.extend(_tail(meta, observed_at, "Execution Confirmed" if complete else "Status Checked"))
    return "\n".join(lines)


def format_protection_lifecycle_card(
    meta: Mapping[str, Any],
    state_key: str,
    *,
    protected_qty: int = 0,
    total_qty: int = 0,
    sl_price: Optional[float] = None,
    target_price: Optional[float] = None,
    protection_route: Optional[str] = None,
    observed_at: Optional[float] = None,
    reason: str = "",
) -> str:
    """P0-07: Formats PROTECTION_PENDING, PROTECTION_VERIFIED_ACTIVE, PROTECTION_UNVERIFIED, and PROTECTION_REJECTED cards."""
    sym = _safe(meta.get("symbol", "UNKNOWN"))
    st = str(state_key or "").upper()
    if st in ("PROTECTION_VERIFIED", "PROTECTION_VERIFIED_ACTIVE"):
        lines = [
            f"🛡️ <b>Protection Verified:</b> {sym}",
            "✅ Verified active broker protection for confirmed filled quantity.",
        ]
    elif st == "PROTECTION_PENDING":
        lines = [
            f"⏳ <b>Protection Pending:</b> {sym}",
            "Submitting/modifying protective SL & Target legs on Dhan.",
        ]
    elif st == "PROTECTION_UNVERIFIED":
        lines = [
            f"⚠️ <b>Protection Unverified:</b> {sym}",
            f"Broker readback has not yet confirmed active SL/Target legs ({_safe(reason or 'awaiting readback')}).",
        ]
    else:
        lines = [
            f"🚨 <b>Protection Rejected / Halted:</b> {sym}",
            f"Protective leg failed verification ({_safe(reason or 'rejected by broker')}). New entries halted.",
        ]
    if protection_route:
        lines.append(f"• Protection Route: {_safe(protection_route)}")
    if total_qty > 0:
        lines.append(f"• Protected Quantity: {protected_qty}/{total_qty}")
    if sl_price and target_price and sl_price > 0 and target_price > 0:
        lines.append(f"• SL / Target Levels: {_money(sl_price)} / {_money(target_price)}")
    if observed_at:
        lines.append(f"🕒 Verified At: {_ist(observed_at)}")
    return "\n".join(lines)


def format_exit_lifecycle_card(
    meta: Mapping[str, Any],
    state_key: str,
    *,
    triggered_leg: str = "",
    sell_qty: int = 0,
    total_qty: int = 0,
    buy_vwap: Optional[float] = None,
    sell_vwap: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    sl_price: Optional[float] = None,
    target_price: Optional[float] = None,
    protection_route: Optional[str] = None,
    observed_at: Optional[float] = None,
) -> str:
    """
    P0-03 & P0-07: Formats TARGET_TRIGGERED, SL_TRIGGERED, EXIT_PARTIAL_FILLED, EXIT_FILLED, and POSITION_CLOSED cards.
    Never claims an exit fill when only a trigger has occurred, and never exposes broker Order IDs.
    """
    sym = _safe(meta.get("symbol", "UNKNOWN"))
    st = str(state_key or "").upper()
    lines = _top(meta, total_qty)
    if buy_vwap is not None and buy_vwap > 0:
        lines.append(f"• Entry BUY VWAP: {_money(buy_vwap)}")
    if protection_route:
        lines.append(f"• Protection Route: {_safe(protection_route)}")
    if sl_price and target_price and sl_price > 0 and target_price > 0:
        lines.append(f"• Active SL / Target: {_money(sl_price)} / {_money(target_price)}")

    if st == "TARGET_TRIGGERED":
        lines.extend((
            "",
            "🎯 <b>Status: Target Leg Triggered (Awaiting Fill)</b>",
            "Target leg was triggered on the exchange; awaiting tradebook SELL fill confirmation.",
            f"• Exit Filled: {sell_qty}/{total_qty}",
        ))
    elif st == "SL_TRIGGERED":
        lines.extend((
            "",
            "🛑 <b>Status: Stop-Loss Leg Triggered (Awaiting Fill)</b>",
            "Stop-Loss leg was triggered on the exchange; awaiting tradebook SELL fill confirmation.",
            f"• Exit Filled: {sell_qty}/{total_qty}",
        ))
    elif st == "EXIT_PARTIAL_FILLED":
        rem_qty = max(0, total_qty - sell_qty)
        lines.extend((
            "",
            "⏳ <b>Status: Exit Partially Executed</b>",
            f"• Exit Filled: {sell_qty}/{total_qty} (Remaining Open: {rem_qty})",
        ))
        if sell_vwap is not None and sell_vwap > 0:
            lines.append(f"• Exit SELL VWAP: {_money(sell_vwap)}")
        if realized_pnl is not None:
            lines.append(f"• Realized Gross P&amp;L (Partial): {_signed_money(realized_pnl)}")
    elif st in ("RECONCILIATION_REQUIRED", "PERSISTENCE_UNCERTAIN"):
        lines.extend((
            "",
            "⚠️ <b>Status: Reconciliation Required (Persistence Uncertain)</b>",
            "Broker exit fill was observed, but local SQLite journal commit did not succeed. New entries halted until durable reconciliation completes.",
            f"• Durable Exit Recorded: {sell_qty}/{total_qty}",
        ))
    elif st in ("EXIT_FILLED", "POSITION_CLOSED"):
        leg_label = "Target Hit" if "TARGET" in str(triggered_leg).upper() else ("Stop-Loss Hit" if "STOP" in str(triggered_leg).upper() else "Exit Confirmed")
        lines.extend((
            "",
            f"🏁 <b>Status: Position Closed ({_safe(leg_label)})</b>",
            f"• Exit Filled: {sell_qty}/{total_qty} (Open Quantity: 0)",
        ))
        if sell_vwap is not None and sell_vwap > 0:
            lines.append(f"• Exit SELL VWAP: {_money(sell_vwap)}")
            lines.append(f"• Exit Trade Value: {_money(sell_vwap * sell_qty)}")
        if realized_pnl is not None:
            lines.append(f"• Gross Realized P&amp;L: {_signed_money(realized_pnl)}")
    lines.extend(_tail(meta, observed_at, "Exit Observed"))
    return "\n".join(lines)


def format_amo_acknowledged(meta: Mapping[str, Any], quantity: int, broker_status: str) -> str:
    """Format a confirmed Dhan acceptance, explicitly not an executed trade."""
    status = str(broker_status).upper().strip()
    if status not in {"TRANSIT", "PENDING"}:
        raise ValueError("AMO queued acknowledgement requires TRANSIT or PENDING")
    return "\n".join((
        "🌙 <b>Dhan AMO Acknowledgement</b>",
        f"• Symbol: {_safe(meta.get('symbol', 'UNKNOWN'))}",
        f"• Quantity: {int(quantity)}",
        f"• Limit Price: {_money(meta.get('limit_price'))}",
        f"• Dhan Status: {_safe(status)}",
        "✅ <b>AMO Accepted by Dhan</b>",
        "⏳ Queued for market OPEN — NOT EXECUTED",
        "A separate confirmation will follow only after a broker-verified fill or final rejection/cancellation.",
    ))


class OrderStatusReporter:
    """Convert existing watcher observations to durable, deduplicated outbox events.

    Caller remains responsible for polling broker; reporter may read trade avg via
    existing read-only broker API but NEVER places/modifies/cancels orders.
    """
    def __init__(self, journal_db: Any, broker: Any, dhan_response_enabled: bool = False):
        self.db = journal_db
        self.broker = broker
        self.dhan_response_enabled = dhan_response_enabled

    def _queue_final(self, order_id: str, key: str, text: str) -> bool:
        """Mirror only verified terminal Dhan outcomes; legacy log path is unchanged."""
        if self.dhan_response_enabled:
            return self.db.enqueue_order_notification_once(order_id, key, text, mirror_final=True)
        return self.db.enqueue_order_notification_once(order_id, key, text)

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
        route_val = str(rec.get("protection_route") or "").strip().upper() or None
        sl_val = rec.get("confirmed_sl_price") or rec.get("sl_trigger")
        tgt_val = rec.get("confirmed_target_price") or rec.get("target_price")

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
        closed_super = status == "CLOSED" and bool(rec.get("is_super_order") or route_val == "SUPER_ORDER")
        if (status == "TRADED" or closed_super) and filled != requested:
            # Inconsistent broker status/quantity: never confirm execution or
            # silently terminalize a partially reported order.
            text = format_order_outcome(
                meta, requested, filled, "UNVERIFIED_EXECUTION", observed_at, price, protected,
                sl_price=sl_val, target_price=tgt_val, protection_route=route_val,
            )
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
                text = format_order_outcome(
                    meta, requested, filled, "UNVERIFIED_EXECUTION", observed_at, None, protected,
                    sl_price=sl_val, target_price=tgt_val, protection_route=route_val,
                )
                self.db.enqueue_order_notification_once(oid, "UNVERIFIED_EXECUTION", text)
                return False  # keep terminal record under watcher until price verification
            event_key = "TRADED"
            text = format_order_outcome(
                meta, requested, filled, "TRADED", observed_at, price, protected,
                sl_price=sl_val, target_price=tgt_val, protection_route=route_val,
            )
            self._queue_final(oid, event_key, text)
            if not protected:
                rec["notification_protection_pending"] = True
            elif rec.get("notification_protection_pending"):
                prot_card = format_protection_lifecycle_card(
                    meta,
                    "PROTECTION_VERIFIED_ACTIVE",
                    protected_qty=int(rec.get("protected_qty") or filled),
                    total_qty=filled,
                    sl_price=sl_val,
                    target_price=tgt_val,
                    protection_route=route_val,
                    observed_at=observed_at,
                )
                self.db.enqueue_order_notification_once(oid, "PROTECTION_VERIFIED", prot_card)
                rec["notification_protection_pending"] = False
            return True

        if status == "PART_TRADED" and filled > 0:
            text = format_order_outcome(
                meta, requested, filled, status, observed_at, price, protected,
                sl_price=sl_val, target_price=tgt_val, protection_route=route_val,
            )
            self.db.enqueue_order_notification_once(oid, f"PART_TRADED_{filled}", text)
            return True

        if status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            text = format_order_outcome(
                meta, requested, filled, status, observed_at, price, protected,
                sl_price=sl_val, target_price=tgt_val, protection_route=route_val,
            )
            self._queue_final(oid, f"FINAL_{status}_{filled}", text)
            return True
        # Pending/Transit/Triggered or indeterminate: leave initial awaiting card.
        return True

    def process_exit_event(
        self,
        rec: dict,
        event_state: str,
        observed_at: float,
        *,
        triggered_leg: str = "",
        sell_qty: int = 0,
        buy_vwap: Optional[float] = None,
        sell_vwap: Optional[float] = None,
        realized_pnl: Optional[float] = None,
    ) -> bool:
        """
        P0-03 & P0-07: Enqueues deduplicated exit lifecycle notifications
        (`TARGET_TRIGGERED`, `SL_TRIGGERED`, `EXIT_PARTIAL_FILLED_<qty>`, `POSITION_CLOSED`).
        """
        meta = rec.get("notification_meta")
        if not meta or str(meta.get("mode", "")).upper() != "LIVE":
            return False
        oid = str(rec.get("order_id") or "")
        if not oid:
            return False
        total_qty = int(rec.get("filled_qty") or rec.get("qty") or 0)
        route_val = str(rec.get("protection_route") or "").strip().upper() or None
        sl_val = rec.get("confirmed_sl_price") or rec.get("sl_trigger")
        tgt_val = rec.get("confirmed_target_price") or rec.get("target_price")

        st_key = str(event_state or "").upper()
        dedup_key = f"{st_key}_{sell_qty}" if st_key == "EXIT_PARTIAL_FILLED" else st_key
        card = format_exit_lifecycle_card(
            meta,
            st_key,
            triggered_leg=triggered_leg,
            sell_qty=sell_qty,
            total_qty=total_qty,
            buy_vwap=buy_vwap,
            sell_vwap=sell_vwap,
            realized_pnl=realized_pnl,
            sl_price=sl_val,
            target_price=tgt_val,
            protection_route=route_val,
            observed_at=observed_at,
        )
        try:
            return bool(self.db.enqueue_order_notification_once(oid, dedup_key, card))
        except Exception as exc:
            LOGGER.error("Failed to enqueue exit notification %s for %s: %s", dedup_key, oid, exc)
            return False
