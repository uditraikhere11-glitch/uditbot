"""
Telegram Trade Log Message Formatter.
Matches the exact visual card layout from reference specification and CV13-07
structured CMP & latency telemetry extensions:
- Header: ✅ Trade Signal Processed / ❌ Trade Signal Rejected
- Section 1: 📩 The Trade Details (o   Original Message)
- Section 2: 🤖 Bot Action Taken (o   Segment, o   Status, o   Dhan Order Details)
- Section 3 (Optional CV13-07): 📊 CMP & Execution Telemetry (Price Source, Yahoo CMP,
  Yahoo HTTP Latency, Reported Quote Age, Quote Freshness, CMP Policy, Calculated Limit Price,
  Quantity, Mode, Order Submission, Broker Order ID, Execution Status, Protection Status)
- Section 4 (Optional CV13-07): ⏱️ Latency Breakdown (Telegram reception → CMP,
  CMP → Dhan submission, Dhan request → acknowledgement, Total to broker acknowledgement)
- P1 Safeguard: Never labels TRANSIT, PENDING, or broker-acknowledged orders as EXECUTED or FILLED.
  Explicitly displays source quote age and indicates when DARVAS_HTTP_SUCCESS accepts a quote > 2.0s
  or with an omitted source timestamp.
"""

from datetime import datetime
from typing import Any, Dict, Optional
import pytz

IST = pytz.timezone("Asia/Kolkata")


def get_current_ist_time(epoch_time: Optional[float] = None) -> str:
    """Returns formatted HH:MM:SS string in IST timezone."""
    if epoch_time is not None:
        dt = datetime.fromtimestamp(epoch_time, tz=pytz.UTC).astimezone(IST)
    else:
        dt = datetime.now(IST)
    return dt.strftime("%H:%M:%S")


def _fmt_ms(val: Optional[float]) -> str:
    if val is None or not isinstance(val, (int, float)):
        return "N/A"
    return f"{float(val):.2f} ms"


def _sanitize_unfilled_execution_status(raw_status: str) -> str:
    """
    P1 Guard: Never label TRANSIT, PENDING, or broker-acknowledged orders as EXECUTED or FILLED.
    """
    st = str(raw_status or "Pending").strip()
    upper = st.upper()
    if any(k in upper for k in ("TRANSIT", "PENDING", "ACKED", "ACKNOWLEDGED", "NOT YET FILLED", "AWAITING FILL")):
        if "TRADED" not in upper:
            # Strip any misleading EXECUTED or FILLED label if caller passed one
            if upper in ("EXECUTED", "FILLED"):
                return "PENDING (Broker Acknowledged — Not Yet Filled)"
    return st


def format_trade_log(
    symbol: str,
    source_name: str,
    original_message: str,
    segment: str,  # 'EQUITY' or 'FNO'
    status: str,
    dhan_order_details: str,
    heads_up: Optional[str] = None,
    is_live: bool = False,
    is_processed: bool = True,
    timestamp: Optional[float] = None,
    equity_cmp_details: Optional[Dict[str, Any]] = None,
    latency_breakdown: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Constructs the Telegram log message matching the visual specification and
    CV13-07 latency/CMP telemetry block when provided.
    """
    time_str = get_current_ist_time(timestamp)
    header_icon = "✅" if is_processed else "❌"
    header_title = "Trade Signal Processed" if is_processed else "Trade Signal Rejected"

    mode_icon = "🟢" if is_live else "🟡"
    mode_text = "Bot - LIVE" if is_live else "Bot - TEST"

    lines = [
        f"{header_icon} {header_title}: {symbol} (Source: {source_name})",
        "",
        "📩 The Trade Details:",
        "",
        f"o   Original Message: {original_message.strip()}",
        "",
        "🤖 Bot Action Taken:",
        "",
        f"o   Segment: {segment.upper()}",
        f"o   Status: {status}",
        f"o   Dhan Order Details: {dhan_order_details}",
    ]

    if equity_cmp_details:
        raw_exec_st = str(equity_cmp_details.get("execution_status", "Pending"))
        safe_exec_st = _sanitize_unfilled_execution_status(raw_exec_st)
        q_age_str = str(equity_cmp_details.get("reported_quote_age", "QUOTE_AGE_UNKNOWN"))
        q_fresh_str = str(equity_cmp_details.get("quote_freshness_status", "")).strip()
        cmp_pol = str(equity_cmp_details.get("cmp_policy", "STRICT_TIMESTAMP"))

        lines.append("")
        lines.append("AUDITBOT — EQUITY SIGNAL PROCESSED" if is_processed else "AUDITBOT — EQUITY SIGNAL EVALUATION")
        lines.append(f"Symbol: {equity_cmp_details.get('symbol', symbol)}")
        lines.append(f"Source: {equity_cmp_details.get('source', source_name)}")
        lines.append(f"Signal: {equity_cmp_details.get('signal', 'BUY')}")
        lines.append(f"Price Source: {equity_cmp_details.get('price_source', 'Yahoo Finance')}")
        if equity_cmp_details.get("yahoo_cmp") is not None:
            lines.append(f"Yahoo CMP: {float(equity_cmp_details['yahoo_cmp']):.2f}")
        else:
            lines.append(f"Yahoo CMP: {equity_cmp_details.get('yahoo_cmp_str', 'N/A')}")
        lines.append(f"Yahoo HTTP Latency: {_fmt_ms(equity_cmp_details.get('yahoo_http_latency_ms'))}")
        lines.append(f"Reported Quote Age: {q_age_str}")
        if q_fresh_str:
            lines.append(f"Quote Freshness: {q_fresh_str}")
        elif cmp_pol == "DARVAS_HTTP_SUCCESS":
            if q_age_str == "QUOTE_AGE_UNKNOWN":
                lines.append("Quote Freshness: ACCEPTED_UNVERIFIED_TIMESTAMP (Pricing accepted by DARVAS_HTTP_SUCCESS; source regularMarketTime omitted)")
            else:
                try:
                    age_num = float(q_age_str.rstrip("s"))
                    if age_num > 2.0:
                        lines.append(f"Quote Freshness: ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S (Quote age {age_num:.2f}s > 2.00s strict threshold; pricing accepted)")
                    else:
                        lines.append("Quote Freshness: FRESHNESS_VERIFIED (<= 2.00s)")
                except Exception:
                    lines.append("Quote Freshness: ACCEPTED_BY_DARVAS_POLICY")
        lines.append(f"CMP Policy: {cmp_pol}")
        if equity_cmp_details.get("reference_label"):
            lines.append(f"Reference Type: {equity_cmp_details['reference_label']}")
        if equity_cmp_details.get("limit_price") is not None:
            lines.append(f"Calculated Limit Price: {float(equity_cmp_details['limit_price']):.2f}")
        if equity_cmp_details.get("quantity") is not None:
            lines.append(f"Quantity: {equity_cmp_details['quantity']}")
        lines.append(f"Mode: {'LIVE' if is_live else 'TEST'}")
        lines.append(f"Order Submission: {equity_cmp_details.get('order_submission', 'N/A')}")
        lines.append(f"Broker Order ID: {equity_cmp_details.get('broker_order_id', 'None')}")
        lines.append(f"Execution Status: {safe_exec_st}")
        lines.append(f"Protection Status: {equity_cmp_details.get('protection_status', 'Not Applicable')}")

    if latency_breakdown:
        lines.append("")
        lines.append("Latency breakdown")
        lines.append(f"Telegram reception → CMP: {_fmt_ms(latency_breakdown.get('telegram_to_cmp_ms'))}")
        lines.append(f"CMP → Dhan submission: {_fmt_ms(latency_breakdown.get('cmp_to_submission_ms'))}")
        lines.append(f"Dhan request → acknowledgement: {_fmt_ms(latency_breakdown.get('dhan_request_to_ack_ms'))}")
        lines.append(f"Total to broker acknowledgement: {_fmt_ms(latency_breakdown.get('telegram_to_ack_ms'))}")
        if latency_breakdown.get("ack_to_fill_ms") is not None:
            lines.append(f"Broker acknowledgement → fill: {_fmt_ms(latency_breakdown.get('ack_to_fill_ms'))}")
        if latency_breakdown.get("fill_to_protection_ms") is not None:
            lines.append(f"Fill → protection verified: {_fmt_ms(latency_breakdown.get('fill_to_protection_ms'))}")
        if latency_breakdown.get("total_processing_ms") is not None:
            lines.append(f"Total measured processing: {_fmt_ms(latency_breakdown.get('total_processing_ms'))}")

    if heads_up:
        lines.append("")
        lines.append(f"⚠️ Heads Up: {heads_up}")

    lines.append("_________________________")
    lines.append(f"⏰ {time_str} IST")
    lines.append(f"{mode_icon} {mode_text}")

    return "\n".join(lines)


def format_exit_log(
    symbol: str,
    source_name: str,
    original_message: str,
    segment: str,
    reason: str,
    is_live: bool = False,
    timestamp: Optional[float] = None,
) -> str:
    """
    Constructs formatted notification for EXIT / SELL signals (logged without execution).
    """
    time_str = get_current_ist_time(timestamp)
    mode_icon = "🟢" if is_live else "🟡"
    mode_text = "Bot - LIVE" if is_live else "Bot - TEST"

    lines = [
        f"ℹ️ Exit Signal Logged: {symbol} (Source: {source_name})",
        "",
        "📩 The Trade Details:",
        "",
        f"o   Original Message: {original_message.strip()}",
        "",
        "🤖 Bot Action Taken:",
        "",
        f"o   Segment: {segment.upper()}",
        "o   Status: Logged for Audit (BUY-ONLY Phase - No broker exit order placed)",
        "o   Dhan Order Details: None (Monitoring Only)",
        "",
        f"⚠️ Heads Up: {reason}",
        "_________________________",
        f"⏰ {time_str} IST",
        f"{mode_icon} {mode_text}",
    ]
    return "\n".join(lines)
