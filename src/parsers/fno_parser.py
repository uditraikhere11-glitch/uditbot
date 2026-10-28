"""
Deterministic regex parser & gatekeeper for Stock Options (F&O) signals.
Restricted to BUY Long Options only; index options and option writing strictly rejected.
"""

import re
import time
from typing import Optional, Tuple
from src.models import FnOSignal, SignalAction

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
_MONTH_RE = "|".join(MONTHS)
_NUM = r"\d+(?:\.\d+)?"

# Index underlyings: strictly blocked
INDEX_UNDERLYINGS = frozenset({
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYBANK"
})

# Exit keywords
EXIT_PATTERN = re.compile(
    r'\b(?:exit|book|booked|booking|sell|square\s*off|sl\s*hit|target\s*hit|close)\b',
    re.IGNORECASE,
)

# Core option regex pattern: matches symbol, strike, CE/PE/CALL/PUT, optional expiry, and @ price
CORE_FNO_PATTERN = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)\s*"
    r"(?P<strike>" + _NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b"
    r"(?:\s+(?:(?P<day>\d{1,2})\s*)?(?P<mon>" + _MONTH_RE + r")[A-Z]*)?"
    r"[^@\n]*?@\s*(?P<e1>" + _NUM + r")(?:\s*(?:-|–|to)\s*(?P<e2>" + _NUM + r"))?",
    re.IGNORECASE,
)

SL_PATTERN = re.compile(r"\b(?:SL|STOP\s*-?\s*LOSS)\s*[-_:=]*\s*(" + _NUM + r")", re.IGNORECASE)
TARGET_PATTERN = re.compile(r"\b(?:TARGETS?|TGT|TP)\s*[-_:=]*\s*([^\n]*)", re.IGNORECASE)


def parse_fno_regex(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[FnOSignal], str, SignalAction]:
    """
    Parses incoming text for Stock Options BUY signals.
    Returns: (FnOSignal or None, reason_str, action)
    """
    if not text or not isinstance(text, str):
        return None, "Empty message", SignalAction.BLOCKED

    now = current_time if current_time is not None else time.time()

    # 1. Message age gatekeeper
    if msg_timestamp > 0 and (now - msg_timestamp) > max_age_sec:
        age = now - msg_timestamp
        return None, f"Message too old ({age:.1f}s > {max_age_sec}s)", SignalAction.BLOCKED

    # 2. Exit / Square-off detection
    if EXIT_PATTERN.search(text):
        return None, "Exit/Sell wording detected (Logged, BUY-ONLY mode)", SignalAction.EXIT_SIGNAL

    # 3. Match Core Option Pattern
    m = CORE_FNO_PATTERN.search(text)
    if not m:
        return None, "No option trade pattern matched", SignalAction.IGNORED

    symbol = m.group("sym").upper()
    strike = float(m.group("strike"))
    opt_raw = m.group("opt").upper()
    opt_type = "CE" if opt_raw in ("CE", "CALL") else "PE"

    # 4. Index Option Blocker
    if symbol in INDEX_UNDERLYINGS:
        return None, f"Index option rejected ({symbol}); single-stock options only", SignalAction.BLOCKED

    # 5. Expiry resolution (month / day)
    mon = m.group("mon").upper() if m.group("mon") else None
    day = int(m.group("day")) if m.group("day") else None

    # 6. Price Extraction & Validation
    e1_str = m.group("e1")
    e2_str = m.group("e2")
    if not e1_str:
        return None, "Missing mandatory entry price for options", SignalAction.BLOCKED

    e1 = float(e1_str)
    e2 = float(e2_str) if e2_str else e1
    entry_min = min(e1, e2)
    entry_max = max(e1, e2)

    if entry_min <= 0:
        return None, "Invalid option entry price <= 0", SignalAction.BLOCKED

    # 7. Informational SL / Targets (ignored for broker orders, logged for audit)
    sl_val = None
    sl_m = SL_PATTERN.search(text)
    if sl_m:
        try:
            sl_val = float(sl_m.group(1))
        except ValueError:
            pass

    targets = []
    tgt_m = TARGET_PATTERN.search(text)
    if tgt_m:
        for num_str in re.findall(_NUM, tgt_m.group(1)):
            try:
                targets.append(float(num_str))
            except ValueError:
                pass

    signal = FnOSignal(
        symbol=symbol,
        strike=strike,
        option_type=opt_type,
        expiry_month=mon,
        expiry_day=day,
        entry_min=entry_min,
        entry_max=entry_max,
        stop_loss=sl_val,
        targets=targets,
        action=SignalAction.BUY_ENTRY,
        raw_text=text,
        channel_id=channel_id,
        message_id=message_id,
        message_timestamp=msg_timestamp or now,
    )
    return signal, "Successfully parsed stock option buy signal", SignalAction.BUY_ENTRY
