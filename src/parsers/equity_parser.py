"""
Deterministic regex parser & gatekeeper for Cash Equity trading signals.
Implements STEP 20 additive aliases + Darvas Script Company-Name Resolution (`resolve_company_name`)
+ AuditBot BRAIN v2 Unified Candidate Validation & PriceStatus (`ABSENT`, `VALID`, `INVALID`):
- Resolves full or prefix company names from Dhan's scrip master (e.g. "Bought MANIPAL PAYMENT at 450" -> MPIMANIPAL).
- Rejects ambiguous single-word prefixes ("Bought MANIPAL", "Bought SBI", "Bought Bajaj") when scrip master is loaded.
- Distinguishes `PriceStatus.ABSENT` (`stated_price=0.0`, `OrderType.LIMIT`), `PriceStatus.VALID` (`stated_price=entry_max`),
  and `PriceStatus.INVALID` (negative `@-800`, incomplete `@800-abc`, `@800 to`, malformed `@12.3.4` -> `BLOCKED`).
- Preserves baseline priority, multi-stock ambiguity blocker (H-01 / A05), and index rejection.
Runs in < 1ms per message.
"""

import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from src.models import EquitySignal, SignalAction, OrderType
from src.instruments.resolver import (
    VALID_EQUITY_SYMBOLS,
    _NAME_INDEX,
    normalize_name,
    resolve_company_name,
)
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    extract_equity_price_bounds_detailed,
    is_historical_callout,
    is_negated_or_non_actionable_callout,
    mask_sl_and_target_clauses,
    normalize_signal_text,
    strip_clock_times,
    validate_equity_candidate,
)

# Meta hashtags that are commentary/formatting, not ticker symbols
META_TAGS = frozenset({
    "LEARNING", "LEARN", "EDUCATION", "EDUCATIONAL", "CHART", "CHARTS",
    "BREAKOUT", "RADAR", "SETUP", "DISCLAIMER", "UPDATE", "ALERT", "INTRADAY",
    "SWING", "POSITIONAL", "HOLDING", "TARGET", "SL", "ENTRY", "CMP", "NEWS",
    "IPO", "VIEW", "LEVELS", "STUDY", "WATCHLIST", "KNOWLEDGE", "INFO", "MOMENTUM",
    "BUY", "AT", "ABOVE", "AROUND", "PRICE", "NOTE", "UNRELATED", "RALLIES",
})

# Index keywords: hard block (we trade single stocks only)
INDEX_KEYWORDS = frozenset({
    "nifty", "banknifty", "sensex", "finnifty", "finifty",
    "bankex", "midcpnifty", "midcap", "niftyit", "niftybank",
})

# Multi-word stock name aliases mapped to canonical NSE tickers (fast fallback when scrip master not loaded)
COMMON_STOCK_ALIASES = {
    "STATE BANK OF INDIA": "SBIN",
    "STATE BANK": "SBIN",
    "YES BANK": "YESBANK",
    "CANARA BANK": "CANBK",
    "AXIS BANK": "AXISBANK",
    "ICICI BANK": "ICICIBANK",
    "HDFC BANK": "HDFCBANK",
    "KOTAK BANK": "KOTAKBANK",
    "TATA MOTORS": "TATAMOTORS",
    "TATA STEEL": "TATASTEEL",
    "TATA POWER": "TATAPOWER",
}

# Words that end a company-name phrase ("Bought Manipal Payment at 450 for positional")
PHRASE_STOP_WORDS = frozenset({
    "AT", "CMP", "SL", "TARGET", "TGT", "ENTRY", "FOR", "NOW", "TODAY", "ABOVE", "BELOW",
    "AROUND", "NEAR", "RS", "WITH", "ON", "IN", "QTY", "LEVELS", "LEVEL", "ZONE", "BUY",
    "AND", "OR",
})

# Common English filler words that are NOT stock symbols (adopted from darvas_script/services/regex_service.py)
NOISE_WORDS = frozenset({
    "THEM", "IT", "THIS", "THE", "SOME", "SOMETHING", "NOW", "TODAY", "MORE", "ONE", "STOCK", "COUNTER", "SHARE", "LETS", "LET'S", "SIGNAL", "RECEIVED", "ALLOCATION", "ONLY", "ALL",
    "BUY", "SELL", "BOUGHT", "ADDED", "ADD", "IN", "AT", "FOR", "AND", "OR", "WITH",
    "TO", "FROM", "ONLY", "CARE", "GOOD", "SAFE", "FAST", "SLOW", "HIGH", "LOW",
    "BIY", "BOGHT", "BOUGTH", "BOT", "BAUGHT", "ASD", "ADED",
})

# Exit / Sell keywords: classified as EXIT_SIGNAL, never submitted as BUY orders
EXIT_PATTERN = re.compile(
    r'\b(?:sell|short|exit|book|booked|booking|close|square\s*off|sl\s*hit|target\s*hit)\b',
    re.IGNORECASE,
)

# Analysis / Vague chatter keywords
ANALYSIS_KEYWORDS = frozenset({
    "tomorrow", "next week", "next month", "mid term", "long term", "short term",
    "monday", "tuesday", "wednesday", "thursday", "friday",
    "ema", "sma", "dma", "support", "resistance", "trendline",
    "watching", "watch", "view",
    "sitting", "holding", "loss", "not gud to add", "not good to add",
})

# Buy intent keywords
BUY_INTENT_PATTERN = re.compile(
    r'\b(buy|biy|bought|boght|bougth|bot|baught|added|aded|add|asd|accumulate|accumulated|sl|target|entry|cmp)\b',
    re.IGNORECASE,
)

# Option hint: if present, message belongs to F&O parser, not Equity
OPTION_HINT_PATTERN = re.compile(
    r'\d+\s*(?:CE|PE)\b|\b(?:CALL|PUT)\b',
    re.IGNORECASE,
)

# Hashtag extraction: #SBIN, # RELIANCE
HASHTAG_PATTERN = re.compile(r'#\s*([a-zA-Z]+)')

# Action + symbol: "buy SBIN", "bought in RELIANCE", "added TATAMOTORS"
ACTION_SYMBOL_PATTERN = re.compile(
    r'(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+(?:(?:one|more|stock|counter|share|lets|let\'s|signal|received)\s+)*(?:in\s+)?(?:#\s*)?([a-zA-Z]+)',
    re.IGNORECASE,
)

# Symbol anchored directly before an explicit price marker (e.g. "and RELIANCE @1400", "RELIANCE at 1400") (A05)
SYMBOL_BEFORE_PRICE_PATTERN = re.compile(
    r'\b([A-Za-z][A-Za-z0-9&\-]{1,14})\s+(?:@|(?:at|around|above|cmp)\s+(?:rs\.?\s*)?\d)',
    re.IGNORECASE,
)

# Conjunctive second symbol after an action verb (e.g. "Buy SBIN and RELIANCE", "Buy SBIN & TCS") (A05)
CONJUNCTIVE_SYMBOL_PATTERN = re.compile(
    r'\b(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+(?:in\s+)?(?:#\s*)?[A-Za-z][A-Za-z0-9&\-]*'
    r'(?:\s+(?:@|at|around|above)\s*[\d,.\-–]+)?\s+(?:and|or|&|/)\s+(?:#\s*)?([A-Za-z][A-Za-z0-9&\-]{1,14})\b',
    re.IGNORECASE,
)

# Multi-word phrase patterns for company-name extraction (from darvas_script/services/regex_service.py)
_WORD = r"[A-Za-z&][A-Za-z&.\-]*"
HASHTAG_PHRASE_PATTERN = re.compile(r'#\s*(' + _WORD + r'(?:[ \t]+' + _WORD + r'){0,5})')
ACTION_PHRASE_PATTERN = re.compile(
    r'(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+(?:(?:one|more|stock|counter|share|lets|let\'s|signal|received)\s+)*(?:in\s+)?(?:#\s*)?(' + _WORD + r'(?:[ \t]+' + _WORD + r'){0,5})',
    re.IGNORECASE,
)

# Entry price pattern kept for backward-compatible direct imports
PRICE_PATTERN = re.compile(
    r'(?:@|at|cmp|around|above)\s*(?:rs\.?)?\s*[:\-]?\s*([\d,\.]+)',
    re.IGNORECASE,
)


def _phrase_tokens(phrase: str, skip_noise: bool) -> List[str]:
    """Words of a candidate company-name phrase: optional leading noise dropped, cut at the first stop word."""
    tokens = phrase.split()
    if skip_noise:
        while tokens and tokens[0].upper() in NOISE_WORDS:
            tokens.pop(0)
    for i, tok in enumerate(tokens):
        if tok.upper().strip(".-") in PHRASE_STOP_WORDS:
            return tokens[:i]
    return tokens


def _resolve_by_name(tokens: List[str]) -> Optional[str]:
    """Company-name lookup ('MANIPAL PAYMENT' -> 'MPIMANIPAL'); None if unknown or ambiguous."""
    return resolve_company_name(normalize_name(" ".join(tokens))) if tokens else None


def _is_valid_candidate_symbol(cand: str) -> bool:
    if not cand or len(cand) < 2:
        return False
    u = cand.upper()
    if u in META_TAGS or u in NOISE_WORDS or u in PHRASE_STOP_WORDS or u.lower() in INDEX_KEYWORDS:
        return False
    if VALID_EQUITY_SYMBOLS and u not in VALID_EQUITY_SYMBOLS:
        return False
    return True


def get_unique_stocks_in_message(text: str) -> Set[str]:
    """
    Identifies all unique stock symbols referenced in a message, accounting for
    multi-word aliases, scrip-master company names, hashtags, all action-verb occurrences,
    conjunctive lists ('BUY SBIN @800 and RELIANCE @1400'), and price-anchored tickers (H-01, A05).
    """
    stocks: Set[str] = set()
    cleaned = mask_sl_and_target_clauses(text or "")

    # 1. Detect multi-word aliases first and consume them
    for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
        if re.search(r'\b' + re.escape(alias_phrase) + r'\b', cleaned, re.IGNORECASE):
            stocks.add(canonical)
            cleaned = re.sub(r'\b' + re.escape(alias_phrase) + r'\b', ' ', cleaned, flags=re.IGNORECASE)

    # 2. If company name index is loaded, check if action/hashtag phrase resolves to a company name
    if _NAME_INDEX or VALID_EQUITY_SYMBOLS:
        for pm in list(HASHTAG_PHRASE_PATTERN.finditer(cleaned)) + list(ACTION_PHRASE_PATTERN.finditer(cleaned)):
            p_tokens = _phrase_tokens(pm.group(1), skip_noise=True)
            resolved = _resolve_by_name(p_tokens)
            if resolved:
                stocks.add(resolved)
                phrase_str = " ".join(p_tokens)
                if phrase_str:
                    cleaned = re.sub(re.escape(phrase_str), ' ', cleaned, flags=re.IGNORECASE)

    # 3. Detect hashtags
    for h in HASHTAG_PATTERN.findall(cleaned):
        cand = h.upper()
        if _is_valid_candidate_symbol(cand):
            stocks.add(cand)

    # 4. Detect all action-bound single tickers
    for m in ACTION_SYMBOL_PATTERN.finditer(cleaned):
        cand = m.group(1).upper()
        if _is_valid_candidate_symbol(cand):
            stocks.add(cand)

    # 5. Detect symbols anchored directly before a price marker ('RELIANCE @1400', 'TCS at 2500') (A05)
    for m in SYMBOL_BEFORE_PRICE_PATTERN.finditer(cleaned):
        cand = m.group(1).upper()
        if _is_valid_candidate_symbol(cand):
            stocks.add(cand)

    # 6. Detect conjunctive second symbols ('BUY SBIN and RELIANCE') (A05)
    for m in CONJUNCTIVE_SYMBOL_PATTERN.finditer(cleaned):
        cand = m.group(1).upper()
        if _is_valid_candidate_symbol(cand):
            stocks.add(cand)

    return stocks


def filter_equity_message_detailed(
    text: str,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[bool, str, SignalAction, str]:
    """
    Applies message-level filters and returns `(is_passed, reason, action, reason_code)`.
    Distinguishes hard safety rejections (`BLOCKED`) from non-actionable chatter (`IGNORED`)
    and exit notifications (`EXIT_SIGNAL`).
    """
    if not text or not isinstance(text, str) or not text.strip():
        return False, "Empty message", SignalAction.BLOCKED, DecisionReasonCode.EMPTY_MESSAGE.value

    norm = normalize_signal_text(text)
    now = current_time if current_time is not None else time.time()

    # Message age gatekeeper
    if msg_timestamp > 0 and (now - msg_timestamp) > max_age_sec:
        age = now - msg_timestamp
        return False, f"Message too old ({age:.1f}s > {max_age_sec}s)", SignalAction.BLOCKED, DecisionReasonCode.MESSAGE_TOO_OLD.value

    # Message length gatekeeper (> 300 chars)
    if len(norm) > 300:
        return False, "Message too long (> 300 chars)", SignalAction.BLOCKED, DecisionReasonCode.MESSAGE_TOO_LONG.value

    msg_lower = norm.lower()

    # Historical / retrospective blocker (A06)
    is_hist, hist_reason = is_historical_callout(norm)
    if is_hist:
        return False, hist_reason, SignalAction.BLOCKED, DecisionReasonCode.HISTORICAL_CALLOUT.value

    # Negated / avoidance / watch / wait / learning commentary blocker (P0-01)
    is_neg, neg_reason = is_negated_or_non_actionable_callout(norm)
    if is_neg:
        return False, neg_reason, SignalAction.BLOCKED, DecisionReasonCode.ANALYSIS_COMMENTARY.value

    # Index rejection
    if any(idx in msg_lower for idx in INDEX_KEYWORDS):
        return False, "Index trade detected: single-stock equity only", SignalAction.BLOCKED, DecisionReasonCode.INDEX_REJECTED.value

    # Exit signal check
    if EXIT_PATTERN.search(norm):
        return False, "Exit/Sell signal detected (Logged, BUY-ONLY mode)", SignalAction.EXIT_SIGNAL, DecisionReasonCode.EXIT_SIGNAL.value

    # Option hint check: redirect to F&O parser
    if OPTION_HINT_PATTERN.search(norm):
        return False, "Derivative / Option signal detected (routed to F&O)", SignalAction.IGNORED, DecisionReasonCode.ROUTED_TO_FNO.value

    # Multi-stock ambiguity check (H-01 / A05): Rejects messages mentioning multiple distinct instruments
    unique_stocks = get_unique_stocks_in_message(norm)
    if len(unique_stocks) > 1:
        return False, f"Multiple stocks detected: {sorted(unique_stocks)}", SignalAction.BLOCKED, DecisionReasonCode.MULTIPLE_INSTRUMENTS.value

    # Analysis chatter blocker
    if any(kw in msg_lower for kw in ANALYSIS_KEYWORDS):
        return False, "Analysis or future planning commentary", SignalAction.BLOCKED, DecisionReasonCode.ANALYSIS_COMMENTARY.value

    # Buy intent filter
    if not BUY_INTENT_PATTERN.search(norm) and "@" not in norm:
        return False, "No BUY intent detected (casual chatter)", SignalAction.IGNORED, DecisionReasonCode.NO_BUY_INTENT.value

    # Explicit price validation (A01, A02, A03, A04 + negative/incomplete ranges):
    # Reject malformed explicit prices with BLOCKED before any AI fallback!
    price_res = extract_equity_price_bounds_detailed(norm)
    if price_res.status == PriceStatus.INVALID:
        return False, price_res.reason, SignalAction.BLOCKED, price_res.reason_code

    return True, "Passed filters", SignalAction.BUY_ENTRY, DecisionReasonCode.OK.value


def filter_equity_message(
    text: str,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[bool, str, SignalAction]:
    """
    Backward-compatible 3-tuple filter returning `(is_passed, reason, action)`.
    """
    passed, reason, action, _ = filter_equity_message_detailed(
        text, msg_timestamp=msg_timestamp, max_age_sec=max_age_sec, current_time=current_time
    )
    return passed, reason, action


def parse_equity_regex_detailed(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[EquitySignal], str, SignalAction, str, PriceStatus, Dict[str, Any]]:
    """
    Detailed equity regex parser returning:
        `(EquitySignal or None, reason, action, reason_code, price_status, evidence_dict)`
    """
    passed, reason, action, r_code = filter_equity_message_detailed(
        text, msg_timestamp=msg_timestamp, max_age_sec=max_age_sec, current_time=current_time
    )
    if not passed:
        p_stat = PriceStatus.INVALID if r_code in (
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            DecisionReasonCode.CONFLICTING_ENTRY_PRICE.value,
        ) else PriceStatus.ABSENT
        return None, reason, action, r_code, p_stat, {}

    cleaned_text = strip_clock_times(text)
    symbol: Optional[str] = None
    phrase: List[str] = []

    # --- Stage 1: Hashtag extraction ---
    hashtags = HASHTAG_PATTERN.findall(cleaned_text)
    candidate_tags = [
        h.upper() for h in hashtags
        if h.upper() not in META_TAGS and h.lower() not in INDEX_KEYWORDS
    ]
    if candidate_tags:
        symbol = candidate_tags[0]
        phrase_match = next(
            (m for m in HASHTAG_PHRASE_PATTERN.finditer(cleaned_text)
             if m.group(1).split()[0].upper().startswith(symbol)),
            None,
        )
        phrase = _phrase_tokens(phrase_match.group(1), skip_noise=False) if phrase_match else []
        if len(phrase) >= 2:
            resolved_by_name = _resolve_by_name(phrase)
            resolved_alias = COMMON_STOCK_ALIASES.get(" ".join(normalize_name(" ".join(phrase))))
            if resolved_by_name or resolved_alias:
                symbol = resolved_by_name or resolved_alias
            else:
                return (
                    None, "Unresolved or ambiguous multi-word company name",
                    SignalAction.BLOCKED, DecisionReasonCode.UNRESOLVED_SYMBOL.value,
                    PriceStatus.ABSENT, {},
                )

    # --- Stage 2: Action pattern fallback (company name phrase or ticker) ---
    if not symbol:
        phrase_match = ACTION_PHRASE_PATTERN.search(cleaned_text)
        if phrase_match:
            phrase = _phrase_tokens(phrase_match.group(1), skip_noise=True)
            if len(phrase) >= 2:
                symbol = _resolve_by_name(phrase)

        if not symbol:
            for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
                pat = (
                    r'(?:\b(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+'
                    r'(?:more\s+|in\s+)*(?:#\s*)?)' + re.escape(alias_phrase) + r'\b'
                )
                if re.search(pat, cleaned_text, re.IGNORECASE):
                    symbol = canonical
                    break

        # Do not trade a first-word ticker if a supplied second word could
        # belong to a different company (including when the master is absent).
        if not symbol and len(phrase) >= 2:
            return (
                None, "Unresolved or ambiguous multi-word company name",
                SignalAction.BLOCKED, DecisionReasonCode.UNRESOLVED_SYMBOL.value,
                PriceStatus.ABSENT, {},
            )

        if not symbol:
            m = ACTION_SYMBOL_PATTERN.search(cleaned_text)
            if m:
                cand = m.group(1).upper()
                if cand not in META_TAGS and cand not in NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
                    symbol = cand

    # --- Stage 3: Validate against Scrip Master / Resolve Company Name ---
    if VALID_EQUITY_SYMBOLS and symbol not in VALID_EQUITY_SYMBOLS:
        symbol = _resolve_by_name(phrase)

    if not symbol:
        return (
            None,
            "No valid stock symbol extracted",
            SignalAction.IGNORED,
            DecisionReasonCode.UNRESOLVED_SYMBOL.value,
            PriceStatus.ABSENT,
            {},
        )

    if symbol in COMMON_STOCK_ALIASES:
        symbol = COMMON_STOCK_ALIASES[symbol]

    # --- Stage 4: Pass through Unified Final Equity Validator ---
    eff_ts = msg_timestamp or (current_time or time.time())
    sig, p_status, evidence, v_code, v_reason = validate_equity_candidate(
        symbol=symbol,
        raw_text=text,
        candidate_price=None,
        channel_id=channel_id,
        message_id=message_id,
        msg_timestamp=eff_ts,
        common_aliases=COMMON_STOCK_ALIASES,
        index_keywords=set(INDEX_KEYWORDS),
    )
    if sig is None:
        act = SignalAction.BLOCKED if p_status == PriceStatus.INVALID else SignalAction.IGNORED
        return None, v_reason, act, v_code, p_status, evidence

    return sig, v_reason, SignalAction.BUY_ENTRY, DecisionReasonCode.OK.value, p_status, evidence


def parse_equity_regex(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[EquitySignal], str]:
    """
    Backward-compatible 2-tuple equity parser returning `(EquitySignal or None, reason_str)`.
    """
    sig, reason, _, _, _, _ = parse_equity_regex_detailed(
        text,
        channel_id=channel_id,
        message_id=message_id,
        msg_timestamp=msg_timestamp,
        max_age_sec=max_age_sec,
        current_time=current_time,
    )
    return sig, reason


# --- Darvas Script Compatibility Wrappers ---

def filter_message(text: str) -> Optional[str]:
    """Darvas-compatible wrapper: returns None if passed, or block reason string."""
    passed, reason, _ = filter_equity_message(text)
    return None if passed else reason


def extract_trade(text: str) -> Optional[dict]:
    """
    Darvas-compatible wrapper around `parse_equity_regex`:
    Returns `{stock_symbol, entry_price, order_type, source, raw_text, timestamp}` or None.
    """
    sig, _ = parse_equity_regex(text)
    if not sig:
        return None
    return {
        "stock_symbol": sig.symbol,
        "entry_price": sig.stated_price,
        "order_type": "LIMIT" if sig.stated_price > 0 else "MARKET",
        "source": "REGEX",
        "raw_text": text,
        "timestamp": "",
    }
