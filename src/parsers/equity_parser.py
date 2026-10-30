"""
Deterministic regex parser & gatekeeper for Cash Equity trading signals.
Implements STEP 20 additive aliases + Darvas Script Company-Name Resolution (`resolve_company_name`):
- Resolves full or prefix company names from Dhan's scrip master (e.g. "Bought MANIPAL PAYMENT at 450" -> MPIMANIPAL).
- Rejects ambiguous single-word prefixes ("Bought MANIPAL", "Bought SBI", "Bought Bajaj") when scrip master is loaded.
- Preserves baseline priority, multi-stock ambiguity blocker (H-01), and index rejection.
Runs in < 1ms per message.
"""

import re
import time
from typing import List, Optional, Set, Tuple
from src.models import EquitySignal, SignalAction, OrderType
from src.instruments.resolver import (
    VALID_EQUITY_SYMBOLS,
    _NAME_INDEX,
    normalize_name,
    resolve_company_name,
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
})

# Common English filler words that are NOT stock symbols (adopted from darvas_script/services/regex_service.py)
NOISE_WORDS = frozenset({
    "THEM", "IT", "THIS", "THE", "SOME", "SOMETHING", "NOW", "TODAY", "MORE", "ALL",
    "BUY", "SELL", "BOUGHT", "ADDED", "ADD", "IN", "AT", "FOR",
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
    r'(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+(?:in\s+)?(?:#\s*)?([a-zA-Z]+)',
    re.IGNORECASE,
)

# Multi-word phrase patterns for company-name extraction (from darvas_script/services/regex_service.py)
_WORD = r"[A-Za-z&][A-Za-z&.\-]*"
HASHTAG_PHRASE_PATTERN = re.compile(r'#\s*(' + _WORD + r'(?:[ \t]+' + _WORD + r'){0,5})')
ACTION_PHRASE_PATTERN = re.compile(
    r'(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+(?:in\s+)?(?:#\s*)?(' + _WORD + r'(?:[ \t]+' + _WORD + r'){0,5})',
    re.IGNORECASE,
)

# Entry price pattern: "@ 120", "at Rs. 2500", "CMP 120", "@120-125", "around 120", "above 120"
PRICE_PATTERN = re.compile(
    r'(?:@|at|cmp|around|above)\s*(?:rs\.?)?\s*[:\-]?\s*([\d\.]+)',
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


def get_unique_stocks_in_message(text: str) -> Set[str]:
    """
    Identifies all unique stock symbols referenced in a message, accounting for
    multi-word aliases, scrip-master company names, and hashtags to detect ambiguous multi-stock messages (H-01).
    """
    stocks: Set[str] = set()
    cleaned = text

    # 1. Detect multi-word aliases first and consume them
    for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
        if re.search(r'\b' + re.escape(alias_phrase) + r'\b', cleaned, re.IGNORECASE):
            stocks.add(canonical)
            cleaned = re.sub(r'\b' + re.escape(alias_phrase) + r'\b', ' ', cleaned, flags=re.IGNORECASE)

    # 2. If company name index is loaded, check if action/hashtag phrase resolves to a company name
    if _NAME_INDEX:
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
        if cand not in META_TAGS and cand not in NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
            if not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS:
                stocks.add(cand)

    # 4. Detect action-bound single tickers
    m = ACTION_SYMBOL_PATTERN.search(cleaned)
    if m:
        cand = m.group(1).upper()
        if cand not in META_TAGS and cand not in NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
            if not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS:
                stocks.add(cand)

    return stocks


def filter_equity_message(
    text: str,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[bool, str, SignalAction]:
    """
    Applies message-level filters:
    Returns: (is_passed, reason, action)
    """
    if not text or not isinstance(text, str):
        return False, "Empty message", SignalAction.BLOCKED

    now = current_time if current_time is not None else time.time()

    # Message age gatekeeper
    if msg_timestamp > 0 and (now - msg_timestamp) > max_age_sec:
        age = now - msg_timestamp
        return False, f"Message too old ({age:.1f}s > {max_age_sec}s)", SignalAction.BLOCKED

    # Message length gatekeeper (> 300 chars)
    if len(text) > 300:
        return False, "Message too long (> 300 chars)", SignalAction.BLOCKED

    msg_lower = text.lower()

    # Index rejection
    if any(idx in msg_lower for idx in INDEX_KEYWORDS):
        return False, "Index trade detected: single-stock equity only", SignalAction.BLOCKED

    # Exit signal check
    if EXIT_PATTERN.search(text):
        return False, "Exit/Sell signal detected (Logged, BUY-ONLY mode)", SignalAction.EXIT_SIGNAL

    # Option hint check: redirect to F&O parser
    if OPTION_HINT_PATTERN.search(text):
        return False, "Derivative / Option signal detected (routed to F&O)", SignalAction.IGNORED

    # Multi-stock ambiguity check (H-01): Rejects messages mentioning multiple distinct instruments
    unique_stocks = get_unique_stocks_in_message(text)
    if len(unique_stocks) > 1:
        return False, f"Multiple stocks detected: {unique_stocks}", SignalAction.BLOCKED

    # Analysis chatter blocker
    if any(kw in msg_lower for kw in ANALYSIS_KEYWORDS):
        return False, "Analysis or future planning commentary", SignalAction.BLOCKED

    # Buy intent filter
    if not BUY_INTENT_PATTERN.search(text) and "@" not in text:
        return False, "No BUY intent detected (casual chatter)", SignalAction.BLOCKED

    return True, "Passed filters", SignalAction.BUY_ENTRY


def parse_equity_regex(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Tuple[Optional[EquitySignal], str]:
    """
    Parses incoming message for Equity Cash buy signals.
    Binds the ticker strictly to the approved BUY action, resolves company names via
    `resolve_company_name`, and rejects ambiguous multi-stock texts.
    Returns (EquitySignal or None, reason_str).
    """
    passed, reason, action = filter_equity_message(
        text, msg_timestamp=msg_timestamp, max_age_sec=max_age_sec, current_time=current_time
    )
    if not passed:
        return None, reason

    symbol: Optional[str] = None
    phrase: List[str] = []

    # --- Stage 1: Hashtag extraction ---
    hashtags = HASHTAG_PATTERN.findall(text)
    candidate_tags = [
        h.upper() for h in hashtags
        if h.upper() not in META_TAGS and h.lower() not in INDEX_KEYWORDS
    ]
    if candidate_tags:
        symbol = candidate_tags[0]
        phrase_match = next(
            (m for m in HASHTAG_PHRASE_PATTERN.finditer(text)
             if m.group(1).split()[0].upper().startswith(symbol)),
            None,
        )
        phrase = _phrase_tokens(phrase_match.group(1), skip_noise=False) if phrase_match else []
        # If hashtag is followed by multi-word company name (e.g. "#MANIPAL PAYMENT buy")
        if len(phrase) >= 2 and _NAME_INDEX:
            resolved_By_name = _resolve_by_name(phrase)
            if resolved_By_name:
                symbol = resolved_By_name

    # --- Stage 2: Action pattern fallback (company name phrase or ticker) ---
    if not symbol:
        # Check multi-word phrase first
        phrase_match = ACTION_PHRASE_PATTERN.search(text)
        if phrase_match:
            phrase = _phrase_tokens(phrase_match.group(1), skip_noise=True)
            if _NAME_INDEX and len(phrase) >= 2:
                symbol = _resolve_by_name(phrase)

        # Check built-in common stock aliases (when _NAME_INDEX not loaded or didn't match)
        if not symbol:
            for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
                pat = (
                    r'(?:\b(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+'
                    r'(?:more\s+|in\s+)*(?:#\s*)?)' + re.escape(alias_phrase) + r'\b'
                )
                if re.search(pat, text, re.IGNORECASE):
                    symbol = canonical
                    break

        # Check single action-bound symbol
        if not symbol:
            m = ACTION_SYMBOL_PATTERN.search(text)
            if m:
                cand = m.group(1).upper()
                if cand not in META_TAGS and cand not in NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
                    symbol = cand

    # --- Stage 3: Validate against Scrip Master / Resolve Company Name (from darvas_script) ---
    if VALID_EQUITY_SYMBOLS and symbol not in VALID_EQUITY_SYMBOLS:
        symbol = _resolve_by_name(phrase)

    if not symbol:
        return None, "No valid stock symbol extracted"

    # Canonical alias translation
    if symbol in COMMON_STOCK_ALIASES:
        symbol = COMMON_STOCK_ALIASES[symbol]

    # --- Extract Price (if specified) ---
    stated_price = 0.0
    price_match = PRICE_PATTERN.search(text)
    if price_match:
        try:
            stated_price = float(price_match.group(1))
        except ValueError:
            stated_price = 0.0

    signal = EquitySignal(
        symbol=symbol,
        action=SignalAction.BUY_ENTRY,
        stated_price=stated_price,
        order_type=OrderType.LIMIT,
        raw_text=text,
        channel_id=channel_id,
        message_id=message_id,
        message_timestamp=msg_timestamp or (current_time or time.time()),
    )
    return signal, "Successfully parsed equity buy signal"


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
