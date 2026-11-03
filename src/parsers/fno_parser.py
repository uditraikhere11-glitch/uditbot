"""
Deterministic regex parser for Stock Options (NSE_FNO / BSE_FNO) signals (BRAIN v3.1).
Implements STEP 20 additive regex aliases + Darvas Script enhancements + AuditBot BRAIN v3.1 Hardening:
- `looks_like_option(text)` (~1µs option gatekeeper so equity exits are never hijacked as F&O exits)
- `is_exit_message(text)`
- Supports both standard contract order (`POLYCAB 8000 PE OCT @100`) and pre-strike expiry order
  (`BUY SBIN NOV 800 CE @20`, `BUY SBIN 28 NOV 800CE @20`), preventing month names (`NOV`) from being
  misparsed as stock symbols (`BRAIN-V3-02`).
- Masks SL and Target clauses (`SL @30`, `SL: @30`, `Target: @150`) BEFORE entry extraction so
  `POLYCAB 8000 PE OCT SL: @30` is rejected with `NO_ENTRY_PREMIUM`.
- Rejects malformed or negative explicit prices (`@100-abc`, `@-100`, `@12.3.4`, `@NaN`, `@Infinity`) with `SignalAction.BLOCKED`.
- Rejects conflicting explicit expiries (`POLYCAB 8000 PE OCT / NOV @100`, `SBIN NOV 800 CE DEC @20`),
  invalid month tokens (`OCTOBAD`), explicit unsupported years (`OCT 2027`), shorthand multi-strike/multi-side
  contracts (`SBIN 800/820 CE`), and cross-phrase conflicting underlyings (`BUY SBIN? RELIANCE 800 PE @100`).
- Routes all extracted candidates through `validate_fno_candidate`.
Runs in < 1ms per message.
"""

import re
import time
from typing import Any, Dict, List, Optional, Tuple
from src.models import FnOSignal, SignalAction
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    VALID_MONTH_MAP,
    detect_multiple_option_contracts,
    extract_explicit_option_expiry_from_text,
    extract_fno_entry_bounds,
    is_historical_callout,
    is_negated_or_non_actionable_callout,
    normalize_signal_text,
    parse_clean_number_token,
    strip_clock_times,
    validate_fno_candidate,
)

_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
_MONTH_RE = "(?:" + "|".join(_MONTHS) + ")"
_FULL_MONTH_RE = (
    r"(?:JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY|JUN(?:E)?|"
    r"JUL(?:Y)?|AUG(?:UST)?|SEP(?:T|TEMBER)?|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)"
)
_NUM = r"\d+(?:\.\d+)?"
_RAW_NUM = r"\d[\d,.]*"

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

_NON_SYMBOL_WORDS = frozenset({
    "BUY", "BOUGHT", "ADD", "ADDED", "AT", "IN", "ON", "FOR", "AND", "OR", "WITH",
    "THE", "THIS", "OPTION", "OPTIONS", "STRIKE", "LOT", "LOTS", "ONLY", "FROM",
    "FRESH", "CALL", "PUT", "CE", "PE", "IDEA", "TRADE", "LONG", "SHORT",
    "DO", "NOT", "DONT", "AVOID", "WATCH", "WATCHING", "WATCHLIST", "WAIT",
    "LEARNING", "EDUCATION", "EDUCATIONAL", "STUDY", "PAPER", "NEVER", "NO",
    *VALID_MONTH_MAP.keys(),
})

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

# Contract header pattern supporting optional pre-strike expiry (`#POLYCAB 8000 PE`, `BUY SBIN NOV 800 CE`, `SBIN 28 NOV 800CE`)
CONTRACT_HEADER_PATTERN = re.compile(
    r"(?<![A-Za-z0-9&\-])(?:(?:buy|bought|add|added|in|fresh)\s+)*#?\s*"
    r"(?P<sym>[A-Za-z][A-Za-z&\-]*)"
    r"(?:\s+(?P<pre_exp>(?:\d{1,2}(?:st|nd|rd|th)?\s+)?" + _FULL_MONTH_RE + r"(?:\s+\d{1,2}(?:st|nd|rd|th)?)?))?"
    r"\s+(?P<strike>" + _RAW_NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b",
    re.IGNORECASE,
)

# Glue pattern where strike is attached to symbol without space (e.g. `#TMPV 280CE`, `#TMPV280CE`)
CONTRACT_GLUED_PATTERN = re.compile(
    r"(?<![A-Za-z0-9&\-])(?:(?:buy|bought|add|added|in|fresh)\s+)*#?\s*"
    r"(?P<sym>[A-Za-z][A-Za-z&\-]*?)"
    r"(?:\s+(?P<pre_exp>(?:\d{1,2}(?:st|nd|rd|th)?\s+)?" + _FULL_MONTH_RE + r"(?:\s+\d{1,2}(?:st|nd|rd|th)?)?))?"
    r"\s*(?P<strike>" + _RAW_NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b",
    re.IGNORECASE,
)

# Legacy pattern constants kept for backward compatibility
CORE_FNO_PATTERN = CONTRACT_GLUED_PATTERN
SECONDARY_FNO_PATTERN = CONTRACT_GLUED_PATTERN

SL_PATTERN = re.compile(r"\b(?:SL|STOP\s*-?\s*LOSS)\s*(?:at|@|is|[-_:=])*\s*(" + _RAW_NUM + r")", re.IGNORECASE)
TARGET_PATTERN = re.compile(r"\b(?:TARGETS?|TGT|TP)\s*(?:at|@|is|[-_:=])*\s*([^\n]*)", re.IGNORECASE)


def looks_like_option(text: str) -> bool:
    """
    True if the message mentions an option contract (strike + CE/PE or CALL/PUT).
    Used to route messages cleanly between FnO and Equity paths without hijacking equity exits.
    """
    return bool(_OPTION_HINT_RE.search(normalize_signal_text(text or "")))


def is_exit_message(text: str) -> bool:
    """True if the message contains exit / profit-booking / square-off wording."""
    return bool(EXIT_PATTERN.search(normalize_signal_text(text or "")))


def parse_fno_regex_detailed(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[FnOSignal], str, SignalAction, str, PriceStatus, Dict[str, Any]]:
    """
    Detailed F&O option regex parser returning:
        `(FnOSignal or None, reason, action, reason_code, price_status, evidence_dict)`
    """
    if not text or not isinstance(text, str) or not text.strip():
        return None, "Empty message", SignalAction.BLOCKED, DecisionReasonCode.EMPTY_MESSAGE.value, PriceStatus.INVALID, {}

    norm = normalize_signal_text(text)
    now = current_time if current_time is not None else time.time()

    # 1. Message age gatekeeper
    if msg_timestamp > 0 and (now - msg_timestamp) > max_age_sec:
        age = now - msg_timestamp
        return (
            None,
            f"Message too old ({age:.1f}s > {max_age_sec}s)",
            SignalAction.BLOCKED,
            DecisionReasonCode.MESSAGE_TOO_OLD.value,
            PriceStatus.ABSENT,
            {},
        )

    # 2. Option Hint Gatekeeper:
    if not looks_like_option(norm):
        return (
            None,
            "No option trade pattern matched",
            SignalAction.IGNORED,
            DecisionReasonCode.UNMATCHED_PATTERN.value,
            PriceStatus.ABSENT,
            {},
        )

    # 3. Exit / Square-off detection
    if is_exit_message(norm):
        return (
            None,
            "Exit/Sell wording detected (Logged, BUY-ONLY mode)",
            SignalAction.EXIT_SIGNAL,
            DecisionReasonCode.EXIT_SIGNAL.value,
            PriceStatus.ABSENT,
            {},
        )

    # 4. Historical / retrospective blocker (A06)
    is_hist, hist_reason = is_historical_callout(norm)
    if is_hist:
        return (
            None,
            hist_reason,
            SignalAction.BLOCKED,
            DecisionReasonCode.HISTORICAL_CALLOUT.value,
            PriceStatus.ABSENT,
            {},
        )

    # 4b. Negated, avoidance, watchlist/wait, and educational commentary blocker (P0-01)
    is_neg, neg_reason = is_negated_or_non_actionable_callout(norm)
    if is_neg:
        return (
            None,
            neg_reason,
            SignalAction.BLOCKED,
            DecisionReasonCode.ANALYSIS_COMMENTARY.value,
            PriceStatus.ABSENT,
            {},
        )

    # 5. Multiple / mixed / shorthand option contracts & conflicting underlyings blocker (A07 + BRAIN-V3-02)
    is_multi_opt, _, multi_reason = detect_multiple_option_contracts(norm)
    if is_multi_opt:
        return (
            None,
            multi_reason,
            SignalAction.BLOCKED,
            DecisionReasonCode.AMBIGUOUS_CONTRACT.value,
            PriceStatus.ABSENT,
            {},
        )

    cleaned_text = strip_clock_times(norm)

    # 6. Locate the Option Contract Header (`SYM [EXPIRY] STRIKE CE/PE`)
    m = CONTRACT_HEADER_PATTERN.search(cleaned_text) or CONTRACT_GLUED_PATTERN.search(cleaned_text)
    if not m:
        return (
            None,
            "No option trade pattern matched",
            SignalAction.IGNORED,
            DecisionReasonCode.UNMATCHED_PATTERN.value,
            PriceStatus.ABSENT,
            {},
        )

    raw_sym = m.group("sym").upper()
    if raw_sym in _NON_SYMBOL_WORDS:
        return (
            None,
            "No option trade pattern matched",
            SignalAction.IGNORED,
            DecisionReasonCode.UNMATCHED_PATTERN.value,
            PriceStatus.ABSENT,
            {},
        )
    symbol = _SYMBOL_ALIASES.get(raw_sym, raw_sym)

    # 7. Index Option Blocker
    if symbol in INDEX_UNDERLYINGS:
        return (
            None,
            f"Index option rejected ({symbol}); single-stock options only",
            SignalAction.BLOCKED,
            DecisionReasonCode.INDEX_REJECTED.value,
            PriceStatus.ABSENT,
            {},
        )

    strike_raw = m.group("strike").rstrip(".")
    strike = parse_clean_number_token(strike_raw)
    if strike is None or strike <= 0:
        return (
            None,
            f"Invalid or malformed option strike: {m.group('strike')!r}",
            SignalAction.BLOCKED,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            PriceStatus.INVALID,
            {},
        )

    opt_raw = m.group("opt").upper()
    opt_type = "CE" if opt_raw in ("CE", "CALL") else "PE"
    pre_exp_seg = (m.group("pre_exp") or "").strip() if "pre_exp" in m.groupdict() else ""

    # 8. Validate explicit expiry tokens (both pre-strike and post-strike: `OCT`, `OCT / NOV`, `OCTOBAD`, `OCT 2027`)
    exp_ok, exp_day, exp_mon, exp_code, exp_info = extract_explicit_option_expiry_from_text(
        cleaned_text,
        after_opt_pos=m.end("opt"),
        pre_strike_seg=pre_exp_seg,
    )
    if not exp_ok:
        return (
            None,
            exp_info,
            SignalAction.BLOCKED,
            exp_code,
            PriceStatus.INVALID,
            {},
        )

    # 9. Extract and validate entry premium from dedicated entry clause (with SL/Target masked out!)
    price_res = extract_fno_entry_bounds(cleaned_text, after_pos=m.end("opt"))
    if price_res.status == PriceStatus.INVALID:
        return (
            None,
            price_res.reason,
            SignalAction.BLOCKED,
            price_res.reason_code,
            PriceStatus.INVALID,
            {"price_span": price_res.source_span},
        )

    if price_res.status == PriceStatus.ABSENT:
        if re.search(r"\b(?:sl|stop\s*-?\s*loss|targets?|tgt|tp)\b\s*(?:(?:at|@|is|[:\-=])\s*)*@", cleaned_text, flags=re.IGNORECASE) or re.search(
            r"\b(?:sl|stop\s*-?\s*loss|targets?|tgt|tp)\b\s*(?:at|@|is|[:\-=])\s*\d",
            cleaned_text,
            flags=re.IGNORECASE,
        ):
            return (
                None,
                price_res.reason,
                SignalAction.BLOCKED,
                DecisionReasonCode.NO_ENTRY_PREMIUM.value,
                PriceStatus.ABSENT,
                {},
            )
        return (
            None,
            "No option trade pattern matched",
            SignalAction.IGNORED,
            DecisionReasonCode.NO_ENTRY_PREMIUM.value,
            PriceStatus.ABSENT,
            {},
        )

    # Standard regex path only matches `@`, `at`, `buy`, `around`, `cmp` anchors (leaving `premium`/`entry` to AI/adaptive)
    anchor_prefix = price_res.source_span.split()[0].upper() if price_res.source_span else ""
    if not (anchor_prefix.startswith("@") or anchor_prefix in ("AT", "BUY", "AROUND", "CMP")):
        return (
            None,
            "No standard regex option price anchor (@/at/buy/around/cmp) matched",
            SignalAction.IGNORED,
            DecisionReasonCode.UNMATCHED_PATTERN.value,
            PriceStatus.VALID,
            {},
        )

    # 10. Informational SL / Targets
    sl_val: Optional[float] = None
    sl_m = SL_PATTERN.search(cleaned_text, m.end())
    if sl_m:
        cand_sl = parse_clean_number_token(sl_m.group(1).rstrip("."))
        if cand_sl is not None and 0 < cand_sl < price_res.entry_min:
            sl_val = cand_sl

    targets: List[float] = []
    tgt_m = TARGET_PATTERN.search(cleaned_text, m.end())
    if tgt_m:
        for num_str in re.findall(_NUM, tgt_m.group(1)):
            try:
                val = float(num_str)
                if val > price_res.entry_max:
                    targets.append(val)
            except ValueError:
                pass
    targets = sorted(targets)

    hold_m = HOLD_PATTERN.search(norm)
    eff_ts = msg_timestamp or (current_time or time.time())

    # 11. Pass through Unified Final F&O Candidate Validator
    sig, p_status, evidence, v_code, v_reason = validate_fno_candidate(
        symbol=symbol,
        strike=strike,
        option_type=opt_type,
        entry_min=price_res.entry_min,
        entry_max=price_res.entry_max,
        raw_text=text,
        expiry_month=exp_mon,
        expiry_day=exp_day,
        stop_loss=sl_val,
        targets=targets,
        caution=bool(CAUTION_PATTERN.search(norm)),
        hold_hint=hold_m.group(0).strip() if hold_m else None,
        channel_id=channel_id,
        message_id=message_id,
        msg_timestamp=eff_ts,
        index_underlyings=set(INDEX_UNDERLYINGS),
    )
    if sig is None:
        return None, v_reason, SignalAction.BLOCKED, v_code, p_status, evidence

    return sig, v_reason, SignalAction.BUY_ENTRY, DecisionReasonCode.OK.value, p_status, evidence


def parse_fno_regex(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[FnOSignal], str, SignalAction]:
    """
    Backward-compatible 3-tuple F&O regex parser returning `(FnOSignal or None, reason_str, action)`.
    """
    sig, reason, action, _, _, _ = parse_fno_regex_detailed(
        text,
        channel_id=channel_id,
        message_id=message_id,
        msg_timestamp=msg_timestamp,
        max_age_sec=max_age_sec,
        current_time=current_time,
    )
    return sig, reason, action
