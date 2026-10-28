"""
Telegram Trade Log Message Formatter.
Matches the exact visual card layout from reference specification:
- Header: ✅ Trade Signal Processed / ❌ Trade Signal Rejected
- Section 1: 📩 The Trade Details (o   Original Message)
- Section 2: 🤖 Bot Action Taken (o   Segment, o   Status, o   Dhan Order Details)
- Warning: ⚠️ Heads Up (optional)
- Divider: _________________________
- Footer: ⏰ HH:MM:SS IST, 🟢 Bot - LIVE or 🟡 Bot - TEST
"""

from datetime import datetime
from typing import Optional
import pytz

IST = pytz.timezone("Asia/Kolkata")


def get_current_ist_time(epoch_time: Optional[float] = None) -> str:
    """Returns formatted HH:MM:SS string in IST timezone."""
    if epoch_time is not None:
        dt = datetime.fromtimestamp(epoch_time, tz=pytz.UTC).astimezone(IST)
    else:
        dt = datetime.now(IST)
    return dt.strftime("%H:%M:%S")


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
) -> str:
    """
    Constructs the exact Telegram log message matching the visual specification.
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
