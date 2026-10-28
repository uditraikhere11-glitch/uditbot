"""
Deterministic regex parser & gatekeeper for Cash Equity trading signals.
Runs in < 1ms per message.
"""

import re
import time
from typing import Optional, Tuple
from src.models import EquitySignal, SignalAction, OrderType

# Meta hashtags that are commentary/formatting, not ticker symbols
META_TAGS = frozenset({
    "LEARNING", "LEARN", "EDUCATION", "EDUCATIONAL", "CHART", "CHARTS",
    "BREAKOUT", "RADAR", "SETUP", "DISCLAIMER", "UPDATE", "ALERT", "INTRADAY",
    "SWING", "POSITIONAL", "HOLDING", "TARGET", "SL", "ENTRY", "CMP", "NEWS",
    "IPO", "VIEW", "LEVELS", "STUDY", "WATCHLIST", "KNOWLEDGE", "INFO", "MOMENTUM",
})

# Index keywords: hard block (we trade single stocks only)
INDEX_KEYWORDS = frozenset({
    "nifty", "banknifty", "sensex", "finnifty", "finifty",
    "bankex", "midcpnifty", "midcap",
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

# Entry price pattern: "@ 120", "at Rs. 2500", "CMP 120", "@120-125"
PRICE_PATTERN = re.compile(
    r'(?:@|at|cmp)\s*(?:rs\.?)?\s*[:\-]?\s*([\d\.]+)',
    re.IGNORECASE,
)


def filter_equity_message(text: str, msg_timestamp: float = 0.0, max_age_sec: float = 60.0, current_time: Optional[float] = None) -> Tuple[bool, str, SignalAction]:
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

    # Index trade blocker
    if any(kw in msg_lower for kw in INDEX_KEYWORDS):
        return False, "Index trade detected (NIFTY/BANKNIFTY/SENSEX)", SignalAction.BLOCKED

    # Exit / Sell classification
    if EXIT_PATTERN.search(text):
        return False, "Exit/Sell signal detected (Logged, BUY-ONLY mode)", SignalAction.EXIT_SIGNAL

    # Option hint check: redirect to F&O parser
    if OPTION_HINT_PATTERN.search(text):
        return False, "Derivative / Option signal detected (routed to F&O)", SignalAction.IGNORED

    # Multi-stock check
    hashtags = HASHTAG_PATTERN.findall(text)
    unique_symbols = set(h.upper() for h in hashtags if h.upper() not in META_TAGS)
    if len(unique_symbols) > 1:
        return False, f"Multiple stocks detected: {unique_symbols}", SignalAction.BLOCKED

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
    Returns (EquitySignal or None, reason_str).
    """
    passed, reason, action = filter_equity_message(
        text, msg_timestamp=msg_timestamp, max_age_sec=max_age_sec, current_time=current_time
    )
    if not passed:
        return None, reason

    # 1. Extract Symbol
    symbol = None
    hashtags = HASHTAG_PATTERN.findall(text)
    candidate_tags = [h.upper() for h in hashtags if h.upper() not in META_TAGS]
    if candidate_tags:
        symbol = candidate_tags[0]
    else:
        m = ACTION_SYMBOL_PATTERN.search(text)
        if m:
            cand = m.group(1).upper()
            if cand not in META_TAGS and cand.lower() not in INDEX_KEYWORDS:
                symbol = cand

    if not symbol:
        return None, "No valid stock symbol extracted"

    # 2. Extract Price (if specified)
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
