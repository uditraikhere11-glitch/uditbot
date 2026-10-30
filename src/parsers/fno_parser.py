"""
Deterministic regex parser for Stock Options (NSE_FNO / BSE_FNO) signals.
Implements STEP 20 additive regex aliases + Darvas Script enhancements:
- `looks_like_option(text)` (~1µs option gatekeeper so equity exits are never hijacked as F&O exits)
- `is_exit_message(text)`
- `_validate_fno_levels` (drops invalid SL >= entry_min or targets <= entry_max, extracts caution & hold_hint)
Runs in < 1ms per message.
"""

import re
import time
from typing import List, Optional, Tuple
from src.models import FnOSignal, SignalAction

_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
_MONTH_RE = "(?:" + "|".join(_MONTHS) + ")"
_NUM = r"\d+(?:\.\d+)?"

# Index underlyings: strictly blocked
INDEX_UNDERLYINGS = frozenset({
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYBANK",
    "NIFTYIT", "NIFTYAUTO", "NIFTYPHARMA", "NIFTYFMCG", "NIFTYMETAL", "SENSEX50"
})

_SYMBOL_ALIASES = {
    "SENSEX": "SENSEX",
    "BANK NIFTY": "BANKNIFTY",
    "NIFTYBANK": "BANKNIFTY",
}

# Cheap gate (~1µs) from darvas_script/services/fno_parser.py:
# A strike glued to CE/PE, or the words CALL/PUT. Decides whether a message belongs to the FnO path.
_OPTION_HINT_RE = re.compile(r"\d\s*(?:CE|PE)\b|\b(?:CALL|PUT)\b", re.IGNORECASE)

# Exit keywords
EXIT_PATTERN = re.compile(
    r'\b(?:exit|book|booked|booking|sell|square\s*-?\s*off|sl\s*hit|target\s*hit|close)\b',
    re.IGNORECASE,
)

# Caution & Hold hints from darvas_script/services/fno_parser.py
CAUTION_PATTERN = re.compile(
    r"limited\s+qty|add\s+slowly|small\s+qty|low\s+liquidity|avg|average",
    re.IGNORECASE,
)
HOLD_PATTERN = re.compile(
    r"month\s*end|\d+\s*weeks?|positional|hold(?:ing)?\s+till[^\n]*",
    re.IGNORECASE,
)

# 1. Primary core option regex pattern: matches symbol, strike, CE/PE/CALL/PUT, optional expiry, and @ price
CORE_FNO_PATTERN = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)\s*"
    r"(?P<strike>" + _NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b"
    r"(?:\s+(?:(?P<day>\d{1,2})\s*)?(?P<mon>" + _MONTH_RE + r")[A-Z]*)?"
    r"[^@\n]*?@\s*(?P<e1>" + _NUM + r")(?:\s*(?:-|–|to)\s*(?P<e2>" + _NUM + r"))?",
    re.IGNORECASE,
)

# 2. Additive secondary alias (STEP 20): matches price callouts using 'at', 'buy', 'around', or 'cmp'
SECONDARY_FNO_PATTERN = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)\s*"
    r"(?P<strike>" + _NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b"
    r"(?:\s+(?:(?P<day>\d{1,2})\s*)?(?P<mon>" + _MONTH_RE + r")[A-Z]*)?"
    r"[^@\n]*?\b(?:at|buy|around|cmp\s*@?)\s*(?P<e1>" + _NUM + r")(?:\s*(?:-|–|to)\s*(?P<e2>" + _NUM + r"))?",
    re.IGNORECASE,
)

SL_PATTERN = re.compile(r"\b(?:SL|STOP\s*-?\s*LOSS)\s*[-_:=]*\s*(" + _NUM + r")", re.IGNORECASE)
TARGET_PATTERN = re.compile(r"\b(?:TARGETS?|TGT|TP)\s*[-_:=]*\s*([^\n]*)", re.IGNORECASE)


def looks_like_option(text: str) -> bool:
    """
    True if the message mentions an option contract (strike + CE/PE or CALL/PUT).
    Used to route messages cleanly between FnO and Equity paths without hijacking equity exits.
    """
    return bool(_OPTION_HINT_RE.search(text or ""))


def is_exit_message(text: str) -> bool:
    """True if the message contains exit / profit-booking / square-off wording."""
    return bool(EXIT_PATTERN.search(text or ""))


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

    # 2. Option Hint Gatekeeper (from darvas_script):
    # Do NOT classify plain equity messages or general chat as F&O exits if no option hint is present.
    if not looks_like_option(text):
        return None, "No option trade pattern matched", SignalAction.IGNORED

    # 3. Exit / Square-off detection (only evaluated for option-looking messages)
    if is_exit_message(text):
        return None, "Exit/Sell wording detected (Logged, BUY-ONLY mode)", SignalAction.EXIT_SIGNAL

    # 4. Match Core Option Pattern or Additive Secondary Pattern
    m = CORE_FNO_PATTERN.search(text) or SECONDARY_FNO_PATTERN.search(text)
    if not m:
        return None, "No option trade pattern matched", SignalAction.IGNORED

    raw_sym = m.group("sym").upper()
    symbol = _SYMBOL_ALIASES.get(raw_sym, raw_sym)
    strike = float(m.group("strike"))
    opt_raw = m.group("opt").upper()
    opt_type = "CE" if opt_raw in ("CE", "CALL") else "PE"

    # 5. Index Option Blocker
    if symbol in INDEX_UNDERLYINGS:
        return None, f"Index option rejected ({symbol}); single-stock options only", SignalAction.BLOCKED

    # 6. Expiry resolution (month / day)
    mon = m.group("mon").upper()[:3] if m.group("mon") else None
    if mon and mon not in _MONTHS:
        mon = None
    day = int(m.group("day")) if m.group("day") else None

    # 7. Price Extraction & Validation
    e1_str = m.group("e1")
    e2_str = m.group("e2")
    if not e1_str:
        return None, "Missing mandatory entry price for options", SignalAction.BLOCKED

    e1 = float(e1_str)
    e2 = float(e2_str) if e2_str else e1
    entry_min = min(e1, e2)
    entry_max = max(e1, e2)

    if strike <= 0 or entry_min <= 0:
        return None, "Invalid option strike or entry price <= 0", SignalAction.BLOCKED

    # 8. Informational SL / Targets (sanitized using darvas_script _validate rules)
    sl_val = None
    sl_m = SL_PATTERN.search(text, m.end()) or SL_PATTERN.search(text)
    if sl_m:
        try:
            cand_sl = float(sl_m.group(1))
            if 0 < cand_sl < entry_min:
                sl_val = cand_sl
        except ValueError:
            pass

    targets: List[float] = []
    tgt_m = TARGET_PATTERN.search(text, m.end()) or TARGET_PATTERN.search(text)
    if tgt_m:
        for num_str in re.findall(_NUM, tgt_m.group(1)):
            try:
                val = float(num_str)
                if val > entry_max:
                    targets.append(val)
            except ValueError:
                pass
    targets = sorted(targets)

    hold_m = HOLD_PATTERN.search(text)
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
        caution=bool(CAUTION_PATTERN.search(text)),
        hold_hint=hold_m.group(0).strip() if hold_m else None,
        raw_text=text,
        channel_id=channel_id,
        message_id=message_id,
        message_timestamp=msg_timestamp or (current_time or time.time()),
    )
    return signal, "Successfully parsed F&O option buy signal", SignalAction.BUY_ENTRY
