"""
Shared validation, conservative text normalization, token-boundary evidence,
semantic role binding, and unified final candidate validators for AuditBot BRAIN (Module 2 — v3.1).

Closes A01–A10, all P1/P2 findings from the Independent Patch Review, and BRAIN-V3-01..03:
- Conservative Unicode normalization (`normalize_signal_text`): handles nonbreaking spaces (`\u00a0`),
  zero-width spaces, Unicode dashes (`–`, `—`, `−`), currency prefixes (`₹`, `Rs.`, `INR`), and `\r\n`
  while preserving `raw_text`, negative signs, decimals, and punctuation.
- Length-preserving clock-time and SL/Target masking (`strip_clock_times`, `mask_sl_and_target_clauses`):
  replaces matched clauses with spaces of identical character length so source-span character offsets
  `[start, end]` are preserved exactly against `normalize_signal_text(raw_text)`.
- `BRAIN-V3-01` (`PriceStatus.ABSENT` vs `PriceStatus.INVALID`):
  * When `raw_text` genuinely contains no entry price (`PriceStatus.ABSENT`, e.g., `Bought SBIN`),
    interprets the documented Gemini `0` / `0.0` sentinel (as well as `None`) as `PriceStatus.ABSENT`
    (`stated_price=0.0`, `OrderType.LIMIT`).
  * Rejects explicit invalid prices in text (`@0`, `@-800`, `@NaN`, `@Infinity`, `at NaN`, `at Infinity`,
    `@800-abc`, `@800 to`, `@12.3.4`) across `REGEX`, `ADAPTIVE_REGEX`, and `GEMINI`.
  * Rejects `candidate_price=0` when `raw_text` contains an explicit valid price (e.g. `Buy SBIN @800`).
- `BRAIN-V3-02` (Atomic F&O contract grounding):
  * Grounds `symbol`, optional pre/post `expiry`, `strike`, and `option_type` (`CE`/`PE`) to one coherent
    atomic contract expression with verified character offsets.
  * Rejects cross-phrase symbol/strike stitching (`BUY SBIN? RELIANCE 800 PE @100`), multiple underlyings,
    mixed `CE`/`PE` tokens anywhere in the message, strike-as-premium reuse, and SL/Target-as-entry (`SL: @30`).
  * Supports pre-strike expiry (`BUY SBIN NOV 800 CE @20`, `BUY SBIN 28 NOV 800CE @20`) as well as post-strike
    expiry (`BUY POLYCAB 8000 PE OCT @100`) and rejects conflicting pre/post expiries (`BUY SBIN NOV 800 CE DEC @20`).
- `BRAIN-V3-03` (Strict F&O AI numeric validation):
  * Validates all candidate numeric fields (`strike`, `entry_min`, `entry_max`, `expiry_day`, `stop_loss`, `targets`)
    BEFORE comparing against text-derived values, rejecting `NaN`, `±Infinity`, booleans (`True`/`False`),
    non-positive values, malformed strings, and inverted bounds (`entry_min > entry_max`).
"""

from dataclasses import dataclass, field
from enum import Enum
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from src.models import EquitySignal, FnOSignal, OrderType, SignalAction
from src.instruments.resolver import (
    VALID_EQUITY_SYMBOLS,
    _NAME_INDEX,
    normalize_name,
    resolve_company_name,
)

PARSER_VERSION = "BRAIN-V14.3.2"


class PriceStatus(str, Enum):
    ABSENT = "ABSENT"
    VALID = "VALID"
    INVALID = "INVALID"


class DecisionReasonCode(str, Enum):
    OK = "OK"
    EMPTY_MESSAGE = "EMPTY_MESSAGE"
    MESSAGE_TOO_OLD = "MESSAGE_TOO_OLD"
    MESSAGE_TOO_LONG = "MESSAGE_TOO_LONG"
    HISTORICAL_CALLOUT = "HISTORICAL_CALLOUT"
    INDEX_REJECTED = "INDEX_REJECTED"
    EXIT_SIGNAL = "EXIT_SIGNAL"
    ROUTED_TO_FNO = "ROUTED_TO_FNO"
    MULTIPLE_INSTRUMENTS = "MULTIPLE_INSTRUMENTS"
    AMBIGUOUS_CONTRACT = "AMBIGUOUS_CONTRACT"
    CONFLICTING_EXPIRY = "CONFLICTING_EXPIRY"
    INVALID_EXPIRY_TOKEN = "INVALID_EXPIRY_TOKEN"
    UNSUPPORTED_EXPIRY_YEAR = "UNSUPPORTED_EXPIRY_YEAR"
    OMITTED_EXPLICIT_EXPIRY = "OMITTED_EXPLICIT_EXPIRY"
    INVALID_EXPLICIT_PRICE = "INVALID_EXPLICIT_PRICE"
    CONFLICTING_ENTRY_PRICE = "CONFLICTING_ENTRY_PRICE"
    NO_ENTRY_PREMIUM = "NO_ENTRY_PREMIUM"
    NO_BUY_INTENT = "NO_BUY_INTENT"
    ANALYSIS_COMMENTARY = "ANALYSIS_COMMENTARY"
    UNRESOLVED_SYMBOL = "UNRESOLVED_SYMBOL"
    UNMATCHED_PATTERN = "UNMATCHED_PATTERN"
    ADAPTIVE_RULE_TIMEOUT = "ADAPTIVE_RULE_TIMEOUT"
    ADAPTIVE_WORKER_ERROR = "ADAPTIVE_WORKER_ERROR"
    AI_UNCONFIGURED = "AI_UNCONFIGURED"
    AI_TIMEOUT = "AI_TIMEOUT"
    AI_NON_ENTRY = "AI_NON_ENTRY"
    AI_GROUNDING_FAILED = "AI_GROUNDING_FAILED"
    AI_TRANSPORT_ERROR = "AI_TRANSPORT_ERROR"
    PARSER_EXCEPTION = "PARSER_EXCEPTION"


@dataclass
class PriceExtractionResult:
    status: PriceStatus
    entry_min: float = 0.0
    entry_max: float = 0.0
    source_span: str = ""
    span_offsets: Optional[Tuple[int, int]] = None
    reason_code: str = DecisionReasonCode.OK.value
    reason: str = ""

    @property
    def is_valid(self) -> bool:
        return self.status in (PriceStatus.ABSENT, PriceStatus.VALID)

    @property
    def has_explicit_price(self) -> bool:
        return self.status == PriceStatus.VALID


# Valid month tokens (3-letter abbreviations and full English month names)
VALID_MONTH_MAP: Dict[str, str] = {
    "JAN": "JAN", "JANUARY": "JAN",
    "FEB": "FEB", "FEBRUARY": "FEB",
    "MAR": "MAR", "MARCH": "MAR",
    "APR": "APR", "APRIL": "APR",
    "MAY": "MAY",
    "JUN": "JUN", "JUNE": "JUN",
    "JUL": "JUL", "JULY": "JUL",
    "AUG": "AUG", "AUGUST": "AUG",
    "SEP": "SEP", "SEPT": "SEP", "SEPTEMBER": "SEP",
    "OCT": "OCT", "OCTOBER": "OCT",
    "NOV": "NOV", "NOVEMBER": "NOV",
    "DEC": "DEC", "DECEMBER": "DEC",
}

# Historical / retrospective markers that disqualify a message from being a fresh live entry (A06)
HISTORICAL_RETROSPECTIVE_PATTERN = re.compile(
    r"\b(?:"
    r"yesterday|last\s+week|last\s+month|earlier\s+today|previously|"
    r"gave\s+in\s+morning|given\s+yesterday|recommended\s+yesterday|"
    r"called\s+yesterday|shared\s+yesterday|recap|flashback"
    r")\b",
    re.IGNORECASE,
)

# Negation, avoidance, watchlist/wait, paper-trade, and educational commentary markers (P0-01)
NEGATED_OR_COMMENTARY_CALLOUT_PATTERN = re.compile(
    r"\b(?:"
    r"do\s+not\s+(?:buy|enter|add|trade|take|long)|"
    r"don'?t\s+(?:buy|enter|add|trade|take|long)|"
    r"not\s+a\s+(?:buy|call|trade|recommendation|tip|entry)|"
    r"no\s+(?:buy|entry|trade|fresh\s+entry|fresh\s+buy)|"
    r"never\s+buy|"
    r"avoid\b|"
    r"stay\s+away|"
    r"watch(?:\s+only|\s+list|\s+out)?\b|watchlist\b|watching\b|"
    r"keep\s+on\s+radar|on\s+radar|"
    r"wait(?:\s+for|\s+and\s+watch|\s+till|\s+until|\s+to\s+buy)?\b|"
    r"only\s+for\s+(?:learning|education|educational|study|paper\s+trade|tracking|watch|observation)|"
    r"for\s+(?:learning|educational?|study)\s+(?:purpose|purposes|only)|"
    r"paper\s+trade\s+only|"
    r"educational\s+(?:post|purpose|view|call|only|example)|"
    r"not\s+good\s+to\s+(?:buy|add|enter)|"
    r"not\s+gud\s+to\s+(?:buy|add|enter)"
    r")\b",
    re.IGNORECASE,
)

# Clock time expressions like "10:30", "09:15 am", "3:20pm", "14:05:00" (A04)
CLOCK_TIME_PATTERN = re.compile(
    r"\b(?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?(?:\s*(?:am|pm|ist|hrs?))?\b",
    re.IGNORECASE,
)

# Valid comma-grouped or plain numeric token (standard 1,234,567 or Indian 1,23,456 or ungrouped 1250.50)
_VALID_COMMAS_RE = re.compile(
    r"^(?:\d+|\d{1,3}(?:,\d{3})+|\d{1,2}(?:,\d{2})+,\d{3})(?:\.\d+)?$"
)

# Matches SL / Stop-Loss and Target / TGT / TP clauses (including `SL: @30`, `SL is @ 30`, `Target: @150`)
# so they can be masked BEFORE entry price extraction (`POLYCAB 8000 PE OCT SL: @30` -> no entry!).
_SL_CLAUSE_MASK_RE = re.compile(
    r"\b(?:sl|stop\s*-?\s*loss)\b\s*(?:(?:at|@|is|around|near|below|above|[:\-=])\s*)*(?:(?:rs\.?|inr|₹)\s*)?[-+]?[A-Za-z0-9,.\s/\-]+?(?=\b(?:target|targets|tgt|tp|buy|bought|add|added|entry|premium|price|cmp)\b|@|$|\n)",
    re.IGNORECASE,
)
_TARGET_CLAUSE_MASK_RE = re.compile(
    r"\b(?:targets?|tgt|tp)\b\s*(?:(?:at|@|is|around|near|above|[:\-=])\s*)*(?:(?:rs\.?|inr|₹)\s*)?[-+]?[A-Za-z0-9,.\s/\-+]+?(?=\b(?:sl|stop\s*-?\s*loss|buy|bought|add|added|entry|premium|price|cmp)\b|@|$|\n)",
    re.IGNORECASE,
)

# Option contract occurrence finder for A07 / BRAIN-V3-02 (including optional pre-strike month/day)
_OPTION_CONTRACT_CANDIDATE_RE = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)"
    r"(?:\s+(?:(?P<pre_day>\d{1,2})(?:st|nd|rd|th)?\s+)?(?P<pre_mon>JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY|JUN(?:E)?|JUL(?:Y)?|AUG(?:UST)?|SEP(?:T|TEMBER)?|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?))?"
    r"\s*(?P<strike>\d+(?:,\d+)*(?:\.\d+)?)\s*(?P<opt>CE|PE|CALL|PUT)\b",
    re.IGNORECASE,
)

# Shorthand multi-strike ('SBIN 800/820 CE', '800 / 820 PE') or multi-side ('800 CE/PE', 'CALL/PUT')
_SHORTHAND_MULTI_STRIKE_RE = re.compile(
    r"\b\d+(?:,\d+)*(?:\.\d+)?\s*(?:/|or|\band\b)\s*\d+(?:,\d+)*(?:\.\d+)?\s*(?:CE|PE|CALL|PUT)\b",
    re.IGNORECASE,
)
_SHORTHAND_MULTI_OPT_SIDE_RE = re.compile(
    r"\b(?:CE\s*/\s*PE|PE\s*/\s*CE|CALL\s*/\s*PUT|PUT\s*/\s*CALL)\b",
    re.IGNORECASE,
)

_NON_CONTRACT_WORDS = frozenset({
    "BUY", "BOUGHT", "ADD", "ADDED", "AT", "IN", "ON", "FOR", "AND", "OR", "WITH",
    "THE", "THIS", "OPTION", "OPTIONS", "STRIKE", "LOT", "LOTS", "ONLY", "FROM",
    "PREMIUM", "ENTRY", "AROUND", "ABOVE", "NEAR", "CMP", "FRESH", "CALL", "PUT",
    "CE", "PE", "SL", "TARGET", "TGT", "TP", "IDEA", "TRADE", "LONG", "SHORT",
    "DO", "NOT", "DONT", "AVOID", "WATCH", "WATCHING", "WATCHLIST", "WAIT",
    "LEARNING", "EDUCATION", "EDUCATIONAL", "STUDY", "PAPER", "NEVER", "NO",
    "JAN", "JANUARY", "FEB", "FEBRUARY", "MAR", "MARCH", "APR", "APRIL", "MAY",
    "JUN", "JUNE", "JUL", "JULY", "AUG", "AUGUST", "SEP", "SEPT", "SEPTEMBER",
    "OCT", "OCTOBER", "NOV", "NOVEMBER", "DEC", "DECEMBER",
})

# Explicit non-finite / invalid numeric keyword pattern after a price anchor (`at NaN`, `at Infinity`, `@ -Inf`, `at - NaN`)
_EXPLICIT_NONFINITE_PRICE_TOKEN_RE = re.compile(
    r"^(?:(?:rs\.?|inr|₹)\s*)?(?:[:=]\s*)?(?:[-+]\s*)*(?:nan|inf|infinity|null|none)\b",
    re.IGNORECASE,
)

# Signed or numeric token start after a price anchor, including space-separated signs (`at - 800`, `CMP - 800`, `entry + 800`) (P0-02)
_SIGNED_OR_NUMERIC_PRICE_START_RE = re.compile(
    r"^(?:(?:rs\.?|inr|₹)\s*)?(?:[:=]\s*)?(?:(?:[-+]\s*)+(?:\d|nan|inf|infinity)|\d|(?:nan|inf|infinity|null|none)\b)",
    re.IGNORECASE,
)

# Matches bare CMP/market keywords only when NOT followed by a numeric or signed numeric token (`@ CMP - 800` is NOT CMP-only)
_CMP_ONLY_TOKEN_RE = re.compile(
    r"^(?:cmp|market|mkt|now|current)\b(?!\s*(?:rs\.?|inr|₹)?\s*(?:[:=]\s*)?(?:(?:[-+]\s*)+(?:\d|nan|inf|infinity)|\d|(?:nan|inf|infinity|null|none)\b))",
    re.IGNORECASE,
)


def _space_pad_match(m: re.Match) -> str:
    return " " * (m.end() - m.start())


def normalize_signal_text(text: str) -> str:
    """
    Conservative text normalization (P2):
    - Replaces non-breaking spaces (`\u00a0`, `\u202f`) and zero-width spaces (`\u200b`..`\u200d`, `\ufeff`)
    - Normalizes `\r\n` and `\r` line endings to `\n`
    - Normalizes Unicode en-dash (`\u2013`), em-dash (`\u2014`), horizontal bar (`\u2015`),
      and Unicode minus (`\u2212`) to standard ASCII `-`
    - Preserves original signs, decimals, and punctuation.
    """
    if not text or not isinstance(text, str):
        return ""
    s = text.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]", " ", s)
    s = re.sub(r"[\u200b-\u200d\ufeff]", "", s)
    s = re.sub(r"[\u2012\u2013\u2014\u2015\u2212]", "-", s)
    return s


def strip_clock_times(text: str) -> str:
    """
    Replaces clock time tokens (e.g., 'at 10:30', '09:15 AM') with equal-length spaces so
    price extractors never mistake a timestamp hour/minute for an entry price (A04) while
    preserving exact character offsets in the normalized string.
    """
    if not text:
        return ""
    norm = normalize_signal_text(text)
    cleaned = re.sub(
        r"\b(?:at|@|around)\s+(?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?(?:\s*(?:am|pm|ist|hrs?))?\b",
        _space_pad_match,
        norm,
        flags=re.IGNORECASE,
    )
    return CLOCK_TIME_PATTERN.sub(_space_pad_match, cleaned)


def mask_sl_and_target_clauses(text: str) -> str:
    """
    Replaces SL/Stop-Loss and Target/TGT/TP clauses (including `SL @30`, `SL: @30`, `SL-30`, `Target: 500,1000`)
    with equal-length spaces so entry-price extractors and role checks never mistake a stop-loss or target
    for an entry price while preserving exact character offsets.
    """
    if not text:
        return ""
    s = strip_clock_times(text)
    s = _SL_CLAUSE_MASK_RE.sub(_space_pad_match, s)
    s = _TARGET_CLAUSE_MASK_RE.sub(_space_pad_match, s)
    return s


def parse_clean_number_token(raw_token: str) -> Optional[float]:
    """
    Parses a single positive numeric token supporting standard/Indian comma grouping (A02)
    while strictly rejecting negative numbers, malformed decimals ('12.3.4'), or bad commas ('12,34') (A03).
    """
    if raw_token is None or not isinstance(raw_token, str):
        return None
    tok = raw_token.strip()
    if not tok or tok.startswith("-") or tok.startswith("+"):
        return None
    if not _VALID_COMMAS_RE.match(tok):
        return None
    try:
        val = float(tok.replace(",", ""))
        if not math.isfinite(val):
            return None
        return val
    except ValueError:
        return None


def _validate_strict_positive_number(val: Any, field_name: str) -> Tuple[bool, float, str]:
    """
    BRAIN-V3-03: Validates a candidate numeric field before any text comparison or min/max operation.
    Strictly rejects `None`, booleans (`True`/`False`), non-numeric strings/objects, `NaN`, `±Infinity`, and `<= 0`.
    """
    if val is None or isinstance(val, bool):
        return False, 0.0, f"Invalid {field_name}: boolean or null value ({val!r}) is not allowed"
    if isinstance(val, str):
        parsed = parse_clean_number_token(val)
        if parsed is None or parsed <= 0.0:
            return False, 0.0, f"Invalid {field_name}: malformed or non-positive string ({val!r})"
        return True, parsed, "OK"
    if not isinstance(val, (int, float)):
        return False, 0.0, f"Invalid {field_name}: unsupported type {type(val).__name__}"
    try:
        fval = float(val)
    except (TypeError, ValueError, OverflowError):
        return False, 0.0, f"Invalid {field_name}: numeric conversion failed ({val!r})"
    if not math.isfinite(fval) or fval <= 0.0:
        return False, 0.0, f"Invalid {field_name}: non-finite or non-positive value ({val!r})"
    return True, fval, "OK"


def _parse_and_validate_price_token_stream(
    anchor_str: str,
    tail_str: str,
) -> Tuple[PriceStatus, float, float, str, str, str]:
    """
    Given an entry anchor (like `@`, `at`, `around`, `above`, `cmp`, `buy`, `premium`, `entry`)
    and the text immediately following it (`tail_str`), validates the complete price or range expression.
    """
    rest = tail_str.lstrip()
    if not rest:
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            anchor_str,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Incomplete explicit price expression after {anchor_str!r}",
        )

    # Strip optional currency prefix ('Rs.', 'INR', '₹') and optional ':' or '='
    curr_m = re.match(r"^(?:(?:rs\.?|inr|₹)\s*)?(?:[:=]\s*)?", rest, flags=re.IGNORECASE)
    after_curr = rest[curr_m.end():].lstrip() if curr_m else rest

    if not after_curr:
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            anchor_str,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Missing price value after {anchor_str!r}",
        )

    # Reject explicit NaN / Infinity / -Infinity tokens immediately (BRAIN-V3-01)
    if re.match(r"^(?:[-+]\s*)*(?:nan|inf|infinity|null|none)\b", after_curr, flags=re.IGNORECASE):
        bad_span = f"{anchor_str} {after_curr[:min(len(after_curr), 16)].strip()}".strip()
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            bad_span,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Non-finite or invalid explicit price token is not allowed: {bad_span!r}",
        )

    # 1. Reject any leading sign ('-', '+') with or without whitespace after the sign (P0-02):
    #    Covers '@-800', '@ -800', 'at -800', 'at - 800', 'CMP - 800', 'entry - 800', 'around - 800', 'at + 800', etc.
    if after_curr.startswith("-") or after_curr.startswith("+"):
        bad_span = f"{anchor_str} {rest[:min(len(rest), 16)].strip()}".strip()
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            bad_span,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Negative or signed explicit price is not allowed: {bad_span!r}",
        )

    # 2. Extract the first contiguous token (up to whitespace or range separator)
    first_chunk_m = re.match(r"^([^\s]+)(.*)$", after_curr, flags=re.DOTALL)
    if not first_chunk_m:
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            anchor_str,
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Malformed explicit price after {anchor_str!r}",
        )

    first_chunk = first_chunk_m.group(1)
    remainder = first_chunk_m.group(2)

    # Check if first_chunk itself contains a hyphenated range (e.g. '790-800', '800-abc', '800-')
    if "-" in first_chunk:
        parts = first_chunk.split("-", 1)
        p1_raw, p2_inline = parts[0], parts[1]
        v1 = parse_clean_number_token(p1_raw)
        if v1 is None or v1 <= 0:
            return (
                PriceStatus.INVALID,
                0.0,
                0.0,
                f"{anchor_str}{first_chunk}".strip(),
                DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                f"Malformed or non-positive explicit entry price: {first_chunk!r}",
            )
        if p2_inline:
            p2_clean = p2_inline.rstrip(".,;)")
            v2 = parse_clean_number_token(p2_clean)
            if v2 is None or v2 <= 0:
                return (
                    PriceStatus.INVALID,
                    0.0,
                    0.0,
                    f"{anchor_str}{first_chunk}".strip(),
                    DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    f"Malformed explicit price range upper bound in {first_chunk!r}",
                )
            span = f"{anchor_str} {p1_raw}-{p2_clean}".strip()
            return PriceStatus.VALID, min(v1, v2), max(v1, v2), span, DecisionReasonCode.OK.value, "OK"
        else:
            rem_stripped = remainder.lstrip()
            if not rem_stripped:
                return (
                    PriceStatus.INVALID,
                    0.0,
                    0.0,
                    f"{anchor_str}{first_chunk}".strip(),
                    DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    f"Incomplete price range after {first_chunk!r}",
                )
            next_tok_m = re.match(r"^([^\s,;)]+)", rem_stripped)
            next_tok = next_tok_m.group(1).rstrip(".") if next_tok_m else ""
            v2 = parse_clean_number_token(next_tok)
            if v2 is None or v2 <= 0:
                return (
                    PriceStatus.INVALID,
                    0.0,
                    0.0,
                    f"{anchor_str}{first_chunk} {next_tok}".strip(),
                    DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    f"Malformed explicit price range upper bound: {next_tok!r}",
                )
            span = f"{anchor_str} {p1_raw}-{next_tok}".strip()
            return PriceStatus.VALID, min(v1, v2), max(v1, v2), span, DecisionReasonCode.OK.value, "OK"

    # Otherwise first_chunk has no hyphen; validate v1
    p1_clean = first_chunk.rstrip(".,;)")
    v1 = parse_clean_number_token(p1_clean)
    if v1 is None or v1 <= 0:
        return (
            PriceStatus.INVALID,
            0.0,
            0.0,
            f"{anchor_str}{first_chunk}".strip(),
            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            f"Malformed or non-positive explicit entry price: {first_chunk!r}",
        )

    # Check if remainder starts with a range connector ('-', 'to')
    rem_lstrip = remainder.lstrip()
    range_conn_m = re.match(r"^(?:-|/\s*|\bto\b)\s*(.*)$", rem_lstrip, flags=re.IGNORECASE | re.DOTALL)
    if range_conn_m:
        after_conn = range_conn_m.group(1).lstrip()
        if not after_conn:
            return (
                PriceStatus.INVALID,
                0.0,
                0.0,
                f"{anchor_str} {p1_clean} {rem_lstrip}".strip(),
                DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                f"Incomplete explicit price range: '{anchor_str.strip()} {p1_clean} {rem_lstrip.strip()}'",
            )
        next_tok_m = re.match(r"^([^\s,;)]+)", after_conn)
        next_tok = next_tok_m.group(1).rstrip(".") if next_tok_m else ""
        v2 = parse_clean_number_token(next_tok)
        if v2 is None or v2 <= 0:
            return (
                PriceStatus.INVALID,
                0.0,
                0.0,
                f"{anchor_str} {p1_clean} to {next_tok}".strip(),
                DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                f"Malformed explicit price range upper bound: {next_tok!r}",
            )
        span = f"{anchor_str} {p1_clean}-{next_tok}".strip()
        return PriceStatus.VALID, min(v1, v2), max(v1, v2), span, DecisionReasonCode.OK.value, "OK"

    span = f"{anchor_str} {p1_clean}".strip()
    return PriceStatus.VALID, v1, v1, span, DecisionReasonCode.OK.value, "OK"


def extract_equity_price_bounds_detailed(text: str) -> PriceExtractionResult:
    """
    Extracts and validates explicit price or price range from an equity message with full
    `PriceStatus` (`ABSENT`, `VALID`, `INVALID`), `source_span`, `span_offsets`, and `reason_code`.
    """
    if not text or not isinstance(text, str):
        return PriceExtractionResult(
            status=PriceStatus.INVALID,
            reason_code=DecisionReasonCode.EMPTY_MESSAGE.value,
            reason="Empty text",
        )

    entry_text = mask_sl_and_target_clauses(text)

    # 1. Check any '@' anchor first
    for m in re.finditer(r"@\s*", entry_text):
        tail = entry_text[m.end():]
        if _CMP_ONLY_TOKEN_RE.match(tail.lstrip()):
            continue
        status, lo, hi, span, r_code, r_msg = _parse_and_validate_price_token_stream("@", tail)
        if status == PriceStatus.INVALID:
            return PriceExtractionResult(
                status=PriceStatus.INVALID,
                source_span=span,
                span_offsets=(m.start(), min(len(entry_text), m.start() + len(span))),
                reason_code=r_code,
                reason=r_msg,
            )

    # 2. Scan all explicit price anchors (`@`, `at`, `cmp`, `around`, `above`, `entry`, `price`)
    anchor_iter_re = re.compile(
        r"(?P<anchor>@|\b(?:at|cmp|around|above|entry|price)\b)",
        re.IGNORECASE,
    )

    extracted: List[Tuple[float, float, str, Tuple[int, int]]] = []
    for m in anchor_iter_re.finditer(entry_text):
        anchor_tok = m.group("anchor")
        tail = entry_text[m.end():]
        tail_l = tail.lstrip()
        if not tail_l:
            if anchor_tok == "@":
                return PriceExtractionResult(
                    status=PriceStatus.INVALID,
                    source_span="@",
                    span_offsets=(m.start(), m.end()),
                    reason_code=DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    reason="Incomplete '@' price marker at end of message",
                )
            continue

        if _CMP_ONLY_TOKEN_RE.match(tail_l):
            continue

        if anchor_tok != "@":
            # Catch signed/space-separated negative tokens ('at - 800', 'CMP - 800'), non-finite tokens, and numeric tokens (P0-02)
            if not (
                _SIGNED_OR_NUMERIC_PRICE_START_RE.match(tail_l)
                or _EXPLICIT_NONFINITE_PRICE_TOKEN_RE.match(tail_l)
            ):
                continue

        status, lo, hi, span, r_code, r_msg = _parse_and_validate_price_token_stream(anchor_tok, tail)
        span_end = min(len(entry_text), m.start() + len(span) + 2)
        if status == PriceStatus.INVALID:
            return PriceExtractionResult(
                status=PriceStatus.INVALID,
                source_span=span,
                span_offsets=(m.start(), span_end),
                reason_code=r_code,
                reason=r_msg,
            )
        extracted.append((lo, hi, span, (m.start(), span_end)))

    if not extracted:
        return PriceExtractionResult(
            status=PriceStatus.ABSENT,
            entry_min=0.0,
            entry_max=0.0,
            source_span="",
            span_offsets=None,
            reason_code=DecisionReasonCode.OK.value,
            reason="No explicit price stated (CMP)",
        )

    first_lo, first_hi, first_span, first_offs = extracted[0]
    for lo, hi, sp, _ in extracted[1:]:
        if abs(lo - first_lo) > 1e-6 or abs(hi - first_hi) > 1e-6:
            return PriceExtractionResult(
                status=PriceStatus.INVALID,
                source_span=f"{first_span} vs {sp}",
                span_offsets=first_offs,
                reason_code=DecisionReasonCode.CONFLICTING_ENTRY_PRICE.value,
                reason=f"Conflicting explicit entry prices stated: {(first_lo, first_hi)} vs {(lo, hi)}",
            )

    return PriceExtractionResult(
        status=PriceStatus.VALID,
        entry_min=first_lo,
        entry_max=first_hi,
        source_span=first_span,
        span_offsets=first_offs,
        reason_code=DecisionReasonCode.OK.value,
        reason="Explicit entry price validated",
    )


def extract_equity_price_bounds(text: str) -> Tuple[bool, bool, float, float, str]:
    """
    Backward-compatible 5-tuple wrapper around `extract_equity_price_bounds_detailed`.
    Returns `(is_valid, has_explicit_price, entry_min, entry_max, reason)`.
    """
    res = extract_equity_price_bounds_detailed(text)
    return res.is_valid, res.has_explicit_price, res.entry_min, res.entry_max, res.reason


_FNO_POSITIONAL_PREMIUM_RE = re.compile(
    r"[ \t]*[:=]?[ \t]*(?P<low>\d{1,10}(?:\.\d{1,6})?)"
    r"(?:[ \t]*(?:-|~|to)[ \t]*(?P<high>\d{1,10}(?:\.\d{1,6})?))?[ \t]*",
    re.IGNORECASE,
)


def _extract_fno_positional_premium(masked_sub: str, base_offset: int) -> Optional[PriceExtractionResult]:
    """Recognize a strict immediate post-contract price line, never a later unrelated number.

    SL/Target and clock clauses are already length-preservingly masked. The
    price occupies the entire first nonblank line; no stripping of signs or
    guessing ambiguous separators like comma, slash, 'and', or plain spaces.
    """
    leading = len(masked_sub) - len(masked_sub.lstrip(" \t\n"))
    remainder = masked_sub[leading:]
    if not remainder:
        return None

    line, _, later = remainder.partition("\n")
    # Only number-led entry clauses are eligible; month/expiry/ordinary prose
    # retain their existing handling. An optional ':' or '=' is harmless.
    if not re.match(r"^(?:[:=][ \t]*)?[-+]?\s*\d", line):
        return None

    def reject(message: str) -> PriceExtractionResult:
        return PriceExtractionResult(
            status=PriceStatus.INVALID,
            source_span=line.strip(),
            span_offsets=(base_offset + leading, base_offset + leading + len(line)),
            reason_code=DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
            reason=message,
        )

    # Do not allow unrelated unmasked words/numbers on later lines. The
    # harmless '+' suffix from 'Target 290+' may survive the target masker.
    if re.search(r"[A-Za-z0-9@]", later):
        return reject("Unanchored option premium followed by ambiguous extra text")
    match = _FNO_POSITIONAL_PREMIUM_RE.fullmatch(line)
    if match is None:
        return reject("Malformed or ambiguous unanchored option premium")
    lower = parse_clean_number_token(match.group("low"))
    upper = parse_clean_number_token(match.group("high")) if match.group("high") else lower
    if lower is None or upper is None or lower <= 0 or upper <= 0:
        return reject("Non-positive or invalid unanchored option premium")
    low, high = min(lower, upper), max(lower, upper)
    # A single misplaced digit could raise a limit price dramatically.
    if high > low * 1.5:
        return reject("Option premium range exceeds conservative 50% width limit")

    return PriceExtractionResult(
        status=PriceStatus.VALID,
        entry_min=low,
        entry_max=high,
        source_span=line.strip(),
        span_offsets=(base_offset + leading, base_offset + leading + len(line)),
        reason_code=DecisionReasonCode.OK.value,
        reason="Validated unanchored post-contract premium",
    )


def extract_fno_entry_bounds(text: str, after_pos: int = 0) -> PriceExtractionResult:
    """
    Extracts and validates the mandatory F&O option entry premium from `text` starting at `after_pos`
    (after the option contract header `SYM [EXPIRY] STRIKE CE/PE [EXPIRY]`), with SL and Target clauses masked out.
    """
    if not text or not isinstance(text, str):
        return PriceExtractionResult(
            status=PriceStatus.INVALID,
            reason_code=DecisionReasonCode.EMPTY_MESSAGE.value,
            reason="Empty option text",
        )

    base_offset = max(0, after_pos)
    masked_full = mask_sl_and_target_clauses(text)
    masked_sub = masked_full[base_offset:]

    anchor_re = re.compile(
        r"(?P<anchor>@|\b(?:at|buy|around|above|cmp\s*@?|entry|premium|price)\b)",
        re.IGNORECASE,
    )

    extracted: List[Tuple[float, float, str, Tuple[int, int]]] = []
    for m in anchor_re.finditer(masked_sub):
        anchor_tok = m.group("anchor")
        tail = masked_sub[m.end():]
        tail_l = tail.lstrip()
        abs_start = base_offset + m.start()
        if not tail_l:
            if anchor_tok.startswith("@"):
                return PriceExtractionResult(
                    status=PriceStatus.INVALID,
                    source_span=anchor_tok,
                    span_offsets=(abs_start, base_offset + m.end()),
                    reason_code=DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    reason="Incomplete '@' price marker in option signal",
                )
            continue

        if not anchor_tok.startswith("@"):
            if not (
                _SIGNED_OR_NUMERIC_PRICE_START_RE.match(tail_l)
                or _EXPLICIT_NONFINITE_PRICE_TOKEN_RE.match(tail_l)
            ):
                continue

        status, lo, hi, span, r_code, r_msg = _parse_and_validate_price_token_stream(anchor_tok, tail)
        abs_end = min(len(masked_full), abs_start + len(span) + 2)
        if status == PriceStatus.INVALID:
            return PriceExtractionResult(
                status=PriceStatus.INVALID,
                source_span=span,
                span_offsets=(abs_start, abs_end),
                reason_code=r_code,
                reason=r_msg,
            )
        extracted.append((lo, hi, span, (abs_start, abs_end)))

    if not extracted:
        # F&O-only fallback for an immediately adjacent unanchored premium.
        # Both regex and AI candidate grounding call this same validator.
        positional = _extract_fno_positional_premium(masked_sub, base_offset)
        if positional is not None:
            return positional
        return PriceExtractionResult(
            status=PriceStatus.ABSENT,
            entry_min=0.0,
            entry_max=0.0,
            source_span="",
            span_offsets=None,
            reason_code=DecisionReasonCode.NO_ENTRY_PREMIUM.value,
            reason="Missing mandatory entry premium for option signal (SL/Target is not an entry price)",
        )

    first_lo, first_hi, first_span, first_offs = extracted[0]
    for lo, hi, sp, _ in extracted[1:]:
        if abs(lo - first_lo) > 1e-6 or abs(hi - first_hi) > 1e-6:
            return PriceExtractionResult(
                status=PriceStatus.INVALID,
                source_span=f"{first_span} vs {sp}",
                span_offsets=first_offs,
                reason_code=DecisionReasonCode.CONFLICTING_ENTRY_PRICE.value,
                reason=f"Conflicting option entry premiums stated: {(first_lo, first_hi)} vs {(lo, hi)}",
            )

    return PriceExtractionResult(
        status=PriceStatus.VALID,
        entry_min=first_lo,
        entry_max=first_hi,
        source_span=first_span,
        span_offsets=first_offs,
        reason_code=DecisionReasonCode.OK.value,
        reason="Option entry premium validated",
    )


def validate_explicit_expiry_token(raw_month_token: Optional[str]) -> Tuple[bool, Optional[str], str]:
    """
    Validates an explicit expiry month token (A08).
    """
    if not raw_month_token:
        return True, None, "No explicit month stated"
    tok = str(raw_month_token).strip().upper()
    canonical = VALID_MONTH_MAP.get(tok)
    if canonical is None:
        return False, None, f"Invalid explicit expiry month token: {raw_month_token!r}"
    return True, canonical, "Valid explicit month"


def extract_explicit_option_expiry_from_text(
    text: str,
    after_opt_pos: int = 0,
    before_price_pos: Optional[int] = None,
    allowed_words: Optional[Set[str]] = None,
    pre_strike_seg: str = "",
) -> Tuple[bool, Optional[int], Optional[str], str, str]:
    """
    Inspects the option expiry segments (both any pre-strike segment between `symbol` and `strike`,
    e.g. `BUY SBIN NOV 800 CE @20`, and the post-contract segment between `CE`/`PE` and the entry price anchor)
    to extract any explicitly stated expiry day, month, or year, and detect:
    - Conflicting explicit expiry months (`POLYCAB 8000 PE OCT / NOV @100` or `SBIN NOV 800 CE DEC @20` -> `CONFLICTING_EXPIRY`)
    - Invalid explicit expiry month tokens (`POLYCAB 8000 PE OCTOBAD @100` -> `INVALID_EXPIRY_TOKEN`)
    - Explicit calendar years (`POLYCAB 8000 PE OCT 2027 @100` -> `UNSUPPORTED_EXPIRY_YEAR`).
    """
    norm = strip_clock_times(text or "")
    end_idx = before_price_pos if before_price_pos is not None else len(norm)
    post_seg = norm[max(0, after_opt_pos):max(0, end_idx)]

    if before_price_pos is None:
        post_seg = mask_sl_and_target_clauses(post_seg)
        price_cut = re.search(
            r"(?:@|\b(?:at|buy|around|above|cmp|entry|premium|price)\b\s*(?:rs\.?|inr|₹)?\s*[:=]?\s*(?:[-+]?\d|nan|inf|infinity))",
            post_seg,
            flags=re.IGNORECASE,
        )
        if price_cut:
            post_seg = post_seg[:price_cut.start()]

    seg = f"{pre_strike_seg} {post_seg}".strip() if pre_strike_seg else post_seg

    allow = allowed_words or {
        "BUY", "BOUGHT", "ADD", "ADDED", "AT", "AROUND", "ABOVE", "NEAR", "CMP", "IN", "FOR",
        "NOW", "TODAY", "FRESH", "ENTRY", "PRICE", "PREMIUM", "RS", "INR", "TO", "OR", "AND",
        "SL", "STOP", "LOSS", "TARGET", "TARGETS", "TGT", "TP", "WITH", "LOT", "LOTS",
        "INTRADAY", "POSITIONAL", "SWING", "HOLD", "HOLDING", "TILL", "MONTH", "END",
        "WEEK", "WEEKS", "LIMITED", "QTY", "SMALL", "LOW", "LIQUIDITY", "AVG", "AVERAGE",
        "EXPIRY", "SERIES", "CONTRACT", "CALL", "PUT", "CE", "PE", "OPTION", "OPTIONS", "STRIKE",
        "ST", "ND", "RD", "TH",
    }

    # 1. Check for explicit 4-digit year (e.g. 2025, 2027, 2028) or 2-digit year suffix like OCT27 / OCT'27
    year_m = re.search(
        r"\b(20\d{2})\b|(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)['\-]?\s*(\d{2})\b",
        seg,
        flags=re.IGNORECASE,
    )
    if year_m:
        stated_yr = year_m.group(1) or f"20{year_m.group(2)}"
        return (
            False,
            None,
            None,
            DecisionReasonCode.UNSUPPORTED_EXPIRY_YEAR.value,
            f"Explicit expiry year ({stated_yr}) cannot be safely represented without silent truncation; rejected",
        )

    # 2. Extract all alphabetic tokens in `seg` and validate month tokens
    months_found: List[str] = []
    for m in re.finditer(r"[A-Za-z]+", seg):
        tok_raw = m.group(0)
        tok_u = tok_raw.upper()
        if tok_u in VALID_MONTH_MAP:
            canon = VALID_MONTH_MAP[tok_u]
            if canon not in months_found:
                months_found.append(canon)
        elif tok_u not in allow:
            return (
                False,
                None,
                None,
                DecisionReasonCode.INVALID_EXPIRY_TOKEN.value,
                f"Invalid explicit expiry month token: {tok_raw!r}",
            )

    if len(months_found) > 1:
        return (
            False,
            None,
            None,
            DecisionReasonCode.CONFLICTING_EXPIRY.value,
            f"Conflicting explicit expiry months stated: {months_found}",
        )

    exp_mon = months_found[0] if months_found else None

    # 3. Extract optional expiry day (e.g., '28 OCT' or 'OCT 28' or '28th OCT')
    exp_day: Optional[int] = None
    day_matches = list(re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\b", seg, flags=re.IGNORECASE))
    if day_matches and exp_mon is not None:
        if len(day_matches) > 1:
            return (
                False,
                None,
                None,
                DecisionReasonCode.CONFLICTING_EXPIRY.value,
                f"Multiple conflicting expiry day numbers in expiry clause: {[d.group(1) for d in day_matches]}",
            )
        day_val = int(day_matches[0].group(1))
        if not (1 <= day_val <= 31):
            return (
                False,
                None,
                None,
                DecisionReasonCode.INVALID_EXPIRY_TOKEN.value,
                f"Invalid option expiry day: {day_val}",
            )
        exp_day = day_val

    span_str = f"{exp_day} {exp_mon}".strip() if (exp_day and exp_mon) else (exp_mon or "")
    return True, exp_day, exp_mon, DecisionReasonCode.OK.value, span_str


def is_historical_callout(text: str) -> Tuple[bool, str]:
    """
    Detects historical or retrospective commentary (e.g. 'Yesterday POLYCAB 8000 PE OCT @100') (A06).
    """
    if not text:
        return False, ""
    m = HISTORICAL_RETROSPECTIVE_PATTERN.search(normalize_signal_text(text))
    if m:
        return True, f"Historical/retrospective callout detected ({m.group(0)!r})"
    return False, ""


def is_negated_or_non_actionable_callout(text: str) -> Tuple[bool, str]:
    """
    P0-01: Detects negated, avoidance, watchlist/wait, or educational/non-entry callouts
    (e.g. 'DO NOT BUY ...', 'AVOID ...', 'WATCH ...', 'ONLY FOR LEARNING ...', 'NOT A BUY ...', 'WAIT FOR ...')
    before any F&O or Equity parser or validator can emit BUY_ENTRY.
    """
    if not text:
        return False, ""
    m = NEGATED_OR_COMMENTARY_CALLOUT_PATTERN.search(normalize_signal_text(text))
    if m:
        return True, f"Negated, avoidance, or non-actionable commentary detected ({m.group(0)!r})"
    return False, ""


def detect_multiple_option_contracts(text: str) -> Tuple[bool, List[str], str]:
    """
    Detects if a message references multiple or shorthand option contracts or conflicting underlyings:
    - 'POLYCAB 8000 PE OCT / RELIANCE 1500 CE OCT @100' (A07)
    - 'SBIN 800/820 CE @ 15' (shorthand multi-strike)
    - 'SBIN 800 CE/PE @ 15' or mixed CE and PE sides anywhere in message
    - 'BUY SBIN? RELIANCE 800 PE @100' (conflicting/multiple stock underlyings in an option message, BRAIN-V3-02)
    """
    if not text:
        return False, [], ""
    norm = normalize_signal_text(text)

    if _SHORTHAND_MULTI_STRIKE_RE.search(norm):
        m = _SHORTHAND_MULTI_STRIKE_RE.search(norm)
        return True, [m.group(0)], f"Ambiguous shorthand multi-strike option contract detected: {m.group(0)!r}"

    if _SHORTHAND_MULTI_OPT_SIDE_RE.search(norm):
        m = _SHORTHAND_MULTI_OPT_SIDE_RE.search(norm)
        return True, [m.group(0)], f"Ambiguous multi-side (CE/PE) option contract detected: {m.group(0)!r}"

    # Check if both CALL/CE and PUT/PE contract option sides appear in the same message
    has_ce = bool(re.search(r"\d(?:[\d,.]*)\s*(?:CE|CALL)\b|\bCE\b", norm, flags=re.IGNORECASE))
    has_pe = bool(re.search(r"\d(?:[\d,.]*)\s*(?:PE|PUT)\b|\bPE\b", norm, flags=re.IGNORECASE))
    if has_ce and has_pe:
        return True, ["CE", "PE"], "Ambiguous option message containing both CE/CALL and PE/PUT sides"

    contracts: List[str] = []
    contract_syms: Set[str] = set()
    seen: Set[Tuple[str, float, str]] = set()
    for m in _OPTION_CONTRACT_CANDIDATE_RE.finditer(norm):
        sym = m.group("sym").upper()
        if sym in _NON_CONTRACT_WORDS:
            continue
        strike_val = parse_clean_number_token(m.group("strike"))
        if strike_val is None or strike_val <= 0:
            continue
        opt_raw = m.group("opt").upper()
        opt_type = "CE" if opt_raw in ("CE", "CALL") else "PE"
        key = (sym, strike_val, opt_type)
        if key not in seen:
            seen.add(key)
            contract_syms.add(sym)
            contracts.append(f"{sym} {strike_val:g} {opt_type}")

    if len(contracts) > 1:
        return True, contracts, f"Multiple/mixed option contracts detected: {contracts}"

    # A stock name occurring in commentary does NOT constitute a second contract.
    # In particular, "in mind", "add slowly", "limited qty", and "#Positional"
    # must not be treated as additional F&O underlyings.  A second fully formed
    # contract is already rejected above; retain the legacy conflicting-header
    # protection only when another symbol is attached to the actual trade line.
    if len(contract_syms) == 1:
        primary_sym = next(iter(contract_syms))
        primary_match = next(
            m for m in _OPTION_CONTRACT_CANDIDATE_RE.finditer(norm)
            if m.group("sym").upper() == primary_sym
        )
        line_start = norm.rfind("\n", 0, primary_match.start()) + 1
        prefix = norm[line_start:primary_match.start()]
        conflict = re.search(
            r"(?:^|\s)(?:(?:buy|bought|add|added|sell)\s+#?(?P<action_sym>[A-Za-z][A-Za-z0-9&\-]{1,14})|#(?P<hash_sym>[A-Za-z][A-Za-z0-9&\-]{1,14}))\s*[?!,:-]?\s*$",
            prefix,
            flags=re.IGNORECASE,
        )
        if conflict:
            cand = (conflict.group("action_sym") or conflict.group("hash_sym")).upper()
            # Metadata hashtags are not trade symbols even when placed before
            # the contract header.  This exemption is F&O-only.
            metadata = {
                "POSITIONAL", "INTRADAY", "SWING", "HOLDING", "LEARNING",
                "DISCLAIMER", "BREAKOUT", "CHART", "NEWS", "VIEW",
                "WATCHLIST", "MOMENTUM", "LEVELS", "TARGET", "SL",
            }
            if (
                cand != primary_sym
                and cand not in _NON_CONTRACT_WORDS
                and cand not in metadata
                and (not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS)
            ):
                all_found = sorted((primary_sym, cand))
                return True, all_found, f"Conflicting underlyings in option contract header: {all_found}"

        # A separate explicit BUY line with another symbol and entry premium is
        # also actionable (unlike free-form commentary without a premium).
        for line in norm.splitlines():
            buy_m = re.match(
                r"^\s*(?:buy|bought|add|added)\s+#?(?P<sym>[A-Za-z][A-Za-z0-9&\-]{1,14})\s*(?:@|at|cmp\s*@?)\s*\d",
                line,
                flags=re.IGNORECASE,
            )
            if not buy_m:
                continue
            cand = buy_m.group("sym").upper()
            if cand != primary_sym and cand not in _NON_CONTRACT_WORDS:
                if not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS:
                    all_found = sorted((primary_sym, cand))
                    return True, all_found, f"Second actionable trade line in option message: {all_found}"

    return False, contracts, ""


def number_token_appears_in_text(
    val: float,
    text: str,
    require_entry_role: bool = False,
) -> bool:
    """
    Verifies that `val` appears as a COMPLETE numerical token in `text` (A09).
    """
    if val is None or isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or val <= 0:
        return False

    norm = normalize_signal_text(text or "")
    if require_entry_role:
        if re.search(r"\d\s*(?:CE|PE)\b|\b(?:CALL|PUT)\b", norm, flags=re.IGNORECASE):
            opt_hdr = _OPTION_CONTRACT_CANDIDATE_RE.search(norm)
            after_pos = opt_hdr.end() if opt_hdr else 0
            f_res = extract_fno_entry_bounds(norm, after_pos=after_pos)
            if f_res.status != PriceStatus.VALID:
                return False
            return abs(float(val) - f_res.entry_min) < 1e-6 or abs(float(val) - f_res.entry_max) < 1e-6
        else:
            e_res = extract_equity_price_bounds_detailed(norm)
            if e_res.status != PriceStatus.VALID:
                return False
            return abs(float(val) - e_res.entry_min) < 1e-6 or abs(float(val) - e_res.entry_max) < 1e-6

    search_text = strip_clock_times(norm)
    token_re = re.compile(r"(?<![A-Za-z0-9_.,:])\d[\d,.]*(?![A-Za-z0-9_.,:])")
    for m in token_re.finditer(search_text):
        raw_tok = m.group(0).rstrip(".,")
        parsed = parse_clean_number_token(raw_tok)
        if parsed is not None and abs(parsed - float(val)) < 1e-6:
            return True
        if "," in raw_tok:
            for sub_tok in raw_tok.split(","):
                sub_p = parse_clean_number_token(sub_tok.strip().rstrip("."))
                if sub_p is not None and abs(sub_p - float(val)) < 1e-6:
                    return True
    return False


def symbol_or_company_appears_in_text_with_offsets(
    sym: str,
    text: str,
    common_aliases: Optional[Dict[str, str]] = None,
) -> Tuple[bool, str, Optional[Tuple[int, int]]]:
    """
    Verifies that `sym` appears as a whole word/ticker token or resolved company name in `text` (A10),
    returning `(found, matched_source_span, (start_offset, end_offset))`.
    """
    if not sym or not text:
        return False, "", None
    clean_sym = sym.strip().upper()
    if not clean_sym:
        return False, "", None

    norm = normalize_signal_text(text)
    upper_text = norm.upper()

    # 1. Exact whole-token match (bounded by non-alphanumeric&-)
    ticker_pat = rf"(?<![A-Z0-9&\-])#?\s*{re.escape(clean_sym)}(?![A-Z0-9&\-])"
    m = re.search(ticker_pat, upper_text)
    if m:
        return True, norm[m.start():m.end()].strip(), (m.start(), m.end())

    # 2. Check common multi-word aliases if provided
    if common_aliases:
        for alias_phrase, canonical in common_aliases.items():
            if canonical.upper() == clean_sym:
                alias_pat = rf"\b{re.escape(alias_phrase.upper())}\b"
                am = re.search(alias_pat, upper_text)
                if am:
                    return True, norm[am.start():am.end()].strip(), (am.start(), am.end())

    # 3. Check company-name resolution via normalize_name & resolve_company_name
    if _NAME_INDEX:
        words = normalize_name(upper_text)
        for i in range(len(words)):
            for length in range(min(6, len(words) - i), 0, -1):
                sub_words = words[i : i + length]
                resolved = resolve_company_name(sub_words)
                if resolved == clean_sym:
                    span_str = " ".join(sub_words)
                    sm = re.search(re.escape(sub_words[0]), upper_text)
                    offs = (sm.start(), min(len(norm), sm.start() + len(span_str))) if sm else None
                    return True, span_str, offs

    return False, "", None


def symbol_or_company_appears_in_text(
    sym: str,
    text: str,
    common_aliases: Optional[Dict[str, str]] = None,
) -> Tuple[bool, str]:
    """Backward-compatible 2-tuple wrapper returning `(found, matched_source_span)`."""
    ok, span, _ = symbol_or_company_appears_in_text_with_offsets(sym, text, common_aliases=common_aliases)
    return ok, span


def validate_equity_candidate(
    symbol: str,
    raw_text: str,
    candidate_price: Optional[float] = None,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    common_aliases: Optional[Dict[str, str]] = None,
    index_keywords: Optional[Set[str]] = None,
    allow_adaptive_capture: bool = False,
) -> Tuple[Optional[EquitySignal], PriceStatus, Dict[str, Any], str, str]:
    """
    Unified Final Equity Candidate Validator enforced identically across `REGEX`, `ADAPTIVE_REGEX`, and `GEMINI`:
    - `BRAIN-V3-01`:
      * When `raw_text` genuinely contains no explicit entry price (`price_res.status == PriceStatus.ABSENT`),
        interprets the documented Gemini zero sentinel (`candidate_price == 0` or `0.0`, as well as `None`)
        as `PriceStatus.ABSENT` (`stated_price = 0.0`, `OrderType.LIMIT`).
      * When `raw_text` contains an invalid explicit price (`@0`, `@-800`, `@NaN`, `@Infinity`, `at NaN`, `@800-abc`),
        rejects with `PriceStatus.INVALID` (`INVALID_EXPLICIT_PRICE`).
      * When `raw_text` contains a valid explicit price (`@800`, `@790-800`), rejects `candidate_price == 0`
        or any candidate price that does not match the verified upper reference `price_res.entry_max`.
    """
    is_hist_eq, hist_eq_reason = is_historical_callout(raw_text or "")
    if is_hist_eq:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.HISTORICAL_CALLOUT.value, hist_eq_reason

    is_neg_eq, neg_eq_reason = is_negated_or_non_actionable_callout(raw_text or "")
    if is_neg_eq:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.ANALYSIS_COMMENTARY.value, neg_eq_reason

    clean_sym = (symbol or "").strip().upper()
    if common_aliases and clean_sym in common_aliases:
        clean_sym = common_aliases[clean_sym]

    idx_set = index_keywords or {
        "nifty", "banknifty", "sensex", "finnifty", "finifty",
        "bankex", "midcpnifty", "midcap", "niftyit", "niftybank",
    }
    if not clean_sym or clean_sym.lower() in idx_set:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.INDEX_REJECTED.value, f"Invalid or index symbol '{clean_sym}'"

    if VALID_EQUITY_SYMBOLS and clean_sym not in VALID_EQUITY_SYMBOLS:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.UNRESOLVED_SYMBOL.value, f"Symbol '{clean_sym}' not in VALID_EQUITY_SYMBOLS"

    sym_ok, sym_span, sym_offsets = symbol_or_company_appears_in_text_with_offsets(
        clean_sym, raw_text, common_aliases=common_aliases
    )
    if not sym_ok:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.UNRESOLVED_SYMBOL.value,
            f"Symbol '{clean_sym}' not evidenced as a complete symbol or company name in text",
        )

    # Validate candidate_price type & finiteness first (reject booleans, strings, NaN, ±Infinity, negative numbers)
    candidate_numeric: Optional[float] = None
    if candidate_price is not None:
        if isinstance(candidate_price, bool):
            return (
                None,
                PriceStatus.INVALID,
                {"symbol_span": sym_span},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                "Candidate price cannot be a boolean",
            )
        try:
            if isinstance(candidate_price, str):
                if candidate_price.strip() in ("0", "0.0", "0.00"):
                    candidate_numeric = 0.0
                else:
                    parsed_cp = parse_clean_number_token(candidate_price)
                    if parsed_cp is None:
                        raise ValueError("malformed candidate price string")
                    candidate_numeric = parsed_cp
            elif isinstance(candidate_price, (int, float)):
                candidate_numeric = float(candidate_price)
            else:
                raise TypeError("unsupported candidate price type")

            if not math.isfinite(candidate_numeric) or candidate_numeric < 0.0:
                raise ValueError("non-finite or negative candidate price")
        except (TypeError, ValueError, OverflowError):
            return (
                None,
                PriceStatus.INVALID,
                {"symbol_span": sym_span},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                f"Candidate price is invalid or non-finite: {candidate_price!r}",
            )

    price_res = extract_equity_price_bounds_detailed(raw_text)
    if price_res.status == PriceStatus.INVALID:
        return (
            None,
            PriceStatus.INVALID,
            {"symbol_span": sym_span, "price_span": price_res.source_span},
            price_res.reason_code,
            price_res.reason,
        )

    eff_price_status = price_res.status
    if price_res.status == PriceStatus.ABSENT:
        # BRAIN-V3-01: When the original message genuinely contains no entry price,
        # interpret `candidate_price == 0.0` (the documented Gemini zero sentinel) or `None` as PriceStatus.ABSENT.
        if candidate_numeric is None or candidate_numeric == 0.0:
            stated_price = 0.0
            eff_price_status = PriceStatus.ABSENT
        elif allow_adaptive_capture and candidate_numeric > 0.0:
            masked_entry_only = mask_sl_and_target_clauses(raw_text)
            if not number_token_appears_in_text(candidate_numeric, masked_entry_only, require_entry_role=False):
                return (
                    None,
                    PriceStatus.INVALID,
                    {"symbol_span": sym_span},
                    DecisionReasonCode.AI_GROUNDING_FAILED.value,
                    f"Adaptive captured price {candidate_numeric} not evidenced in entry clause",
                )
            stated_price = candidate_numeric
            eff_price_status = PriceStatus.VALID
        else:
            return (
                None,
                PriceStatus.INVALID,
                {"symbol_span": sym_span},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                f"Candidate supplied price {candidate_price} when no valid entry price exists in text",
            )
    else:
        # PriceStatus.VALID: an explicit entry price exists in raw_text (`price_res.entry_max` > 0).
        # A zero sentinel (0.0) or any price disagreeing with `price_res.entry_max` must be rejected!
        if candidate_numeric is not None:
            if candidate_numeric <= 0.0:
                return (
                    None,
                    PriceStatus.INVALID,
                    {"symbol_span": sym_span, "price_span": price_res.source_span},
                    DecisionReasonCode.AI_GROUNDING_FAILED.value,
                    f"Candidate returned zero sentinel ({candidate_price}) when explicit price {price_res.entry_max:g} exists in text",
                )
            if abs(candidate_numeric - price_res.entry_max) > 1e-6:
                return (
                    None,
                    PriceStatus.INVALID,
                    {"symbol_span": sym_span, "price_span": price_res.source_span},
                    DecisionReasonCode.AI_GROUNDING_FAILED.value,
                    f"Candidate entry price ({candidate_numeric:g}) does not match verified upper entry reference ({price_res.entry_max:g})",
                )
        stated_price = price_res.entry_max

    norm_text = normalize_signal_text(raw_text)
    act_m = re.search(
        r"\b(?:buy|biy|bought|boght|bougth|bot|baught|added|aded|add|asd|accumulate|accumulated|entry|long)\b",
        norm_text,
        flags=re.IGNORECASE,
    )
    action_span = norm_text[act_m.start():act_m.end()] if act_m else ""
    action_offsets = [act_m.start(), act_m.end()] if act_m else None

    evidence = {
        "symbol": clean_sym,
        "symbol_span": sym_span,
        "symbol_span_offsets": list(sym_offsets) if sym_offsets else None,
        "action_span": action_span,
        "action_span_offsets": action_offsets,
        "price_status": eff_price_status.value,
        "explicit_price": (eff_price_status == PriceStatus.VALID),
        "entry_min": price_res.entry_min if price_res.has_explicit_price else stated_price,
        "entry_max": price_res.entry_max if price_res.has_explicit_price else stated_price,
        "stated_price": stated_price,
        "price_span": price_res.source_span,
        "price_span_offsets": list(price_res.span_offsets) if price_res.span_offsets else None,
    }
    sig = EquitySignal(
        symbol=clean_sym,
        action=SignalAction.BUY_ENTRY,
        stated_price=stated_price,
        order_type=OrderType.LIMIT,
        raw_text=raw_text,
        channel_id=channel_id,
        message_id=message_id,
        message_timestamp=msg_timestamp,
    )
    reason_msg = (
        f"Successfully parsed equity buy signal (range {price_res.entry_min:g}-{price_res.entry_max:g} -> upper reference {price_res.entry_max:g})"
        if (price_res.has_explicit_price and price_res.entry_max > price_res.entry_min)
        else "Successfully parsed equity buy signal"
    )
    return sig, eff_price_status, evidence, DecisionReasonCode.OK.value, reason_msg


def validate_fno_candidate(
    symbol: str,
    strike: float,
    option_type: str,
    entry_min: float,
    entry_max: float,
    raw_text: str,
    expiry_month: Optional[str] = None,
    expiry_day: Optional[int] = None,
    stop_loss: Optional[float] = None,
    targets: Optional[List[float]] = None,
    caution: bool = False,
    hold_hint: Optional[str] = None,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    index_underlyings: Optional[Set[str]] = None,
) -> Tuple[Optional[FnOSignal], PriceStatus, Dict[str, Any], str, str]:
    """
    Unified Final F&O Option Candidate Validator enforced identically across `REGEX`, `ADAPTIVE_REGEX`, and `GEMINI`:
    1. `BRAIN-V3-03`: Strictly validates all candidate numeric inputs (`strike`, `entry_min`, `entry_max`,
       `expiry_day`, `stop_loss`, `targets`) BEFORE comparing against text-derived values.
       Rejects `NaN`, `±Infinity`, booleans, non-positive numbers, malformed types, and inverted bounds (`entry_min > entry_max`).
    2. `BRAIN-V3-02`: Grounds `symbol`, optional pre/post `expiry`, `strike`, and `option_type` (`CE`/`PE`) to one
       coherent atomic contract expression in `raw_text`, with verified character offsets.
       Never combines `symbol` from one phrase with `strike` from another (`BUY SBIN? RELIANCE 800 PE @100` -> rejected).
    """
    norm = normalize_signal_text(raw_text or "")
    if not norm:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.EMPTY_MESSAGE.value, "Empty option message"

    # --- Step 1 (BRAIN-V3-03): Validate all candidate numeric & enum fields FIRST ---
    raw_opt = (option_type or "").strip().upper() if isinstance(option_type, str) else ""
    if raw_opt not in ("CE", "PE", "CALL", "PUT"):
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Invalid option_type '{option_type}' (must be CE or PE)",
        )
    canon_opt = "CE" if raw_opt in ("CE", "CALL") else "PE"

    st_ok, strike_val, st_err = _validate_strict_positive_number(strike, "strike")
    if not st_ok:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.AI_GROUNDING_FAILED.value, st_err

    emin_ok, c_min, emin_err = _validate_strict_positive_number(entry_min, "entry_min")
    if not emin_ok:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.AI_GROUNDING_FAILED.value, emin_err

    emax_ok, c_max, emax_err = _validate_strict_positive_number(entry_max, "entry_max")
    if not emax_ok:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.AI_GROUNDING_FAILED.value, emax_err

    if c_min > c_max + 1e-9:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Conflicting candidate entry bounds: entry_min ({c_min:g}) > entry_max ({c_max:g})",
        )

    cand_day: Optional[int] = None
    if expiry_day is not None:
        if isinstance(expiry_day, bool) or not isinstance(expiry_day, (int, float)) or not math.isfinite(float(expiry_day)):
            return (
                None,
                PriceStatus.INVALID,
                {},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                f"Invalid candidate expiry_day: {expiry_day!r}",
            )
        if abs(float(expiry_day) - round(float(expiry_day))) > 1e-6:
            return (
                None,
                PriceStatus.INVALID,
                {},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                f"Non-integral candidate expiry_day: {expiry_day!r}",
            )
        cand_day = int(round(float(expiry_day)))
        if not (1 <= cand_day <= 31):
            return (
                None,
                PriceStatus.INVALID,
                {},
                DecisionReasonCode.INVALID_EXPIRY_TOKEN.value,
                f"Out-of-range candidate expiry_day: {cand_day}",
            )

    if stop_loss is not None:
        sl_ok, sl_num, sl_err = _validate_strict_positive_number(stop_loss, "stop_loss")
        if not sl_ok:
            return None, PriceStatus.INVALID, {}, DecisionReasonCode.AI_GROUNDING_FAILED.value, sl_err

    if targets is not None:
        if not isinstance(targets, (list, tuple)):
            return (
                None,
                PriceStatus.INVALID,
                {},
                DecisionReasonCode.AI_GROUNDING_FAILED.value,
                f"Invalid targets container type: {type(targets).__name__}",
            )
        for idx, t in enumerate(targets):
            t_ok, _, t_err = _validate_strict_positive_number(t, f"targets[{idx}]")
            if not t_ok:
                return None, PriceStatus.INVALID, {}, DecisionReasonCode.AI_GROUNDING_FAILED.value, t_err

    # --- Step 2: Deterministic message-level safety checks ---
    is_hist, hist_reason = is_historical_callout(norm)
    if is_hist:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.HISTORICAL_CALLOUT.value, hist_reason

    is_neg, neg_reason = is_negated_or_non_actionable_callout(norm)
    if is_neg:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.ANALYSIS_COMMENTARY.value, neg_reason

    clean_sym = (symbol or "").strip().upper() if isinstance(symbol, str) else ""
    idx_set = index_underlyings or {
        "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYBANK",
        "NIFTYIT", "NIFTYAUTO", "NIFTYPHARMA", "NIFTYFMCG", "NIFTYMETAL", "SENSEX50",
    }
    if not clean_sym or clean_sym in idx_set:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.INDEX_REJECTED.value,
            f"Index option rejected ({clean_sym}); single-stock options only",
        )
    if clean_sym in _NON_CONTRACT_WORDS:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.UNRESOLVED_SYMBOL.value,
            f"Invalid non-symbol keyword '{clean_sym}' cannot be an option underlying",
        )


    # Check genuine multiple contracts only after primary-index rejection, so
    # NIFTY/SENSEX report INDEX_REJECTED instead of a spurious ambiguity.
    is_multi, _, multi_reason = detect_multiple_option_contracts(norm)
    if is_multi:
        return None, PriceStatus.INVALID, {}, DecisionReasonCode.AMBIGUOUS_CONTRACT.value, multi_reason

    sym_ok, sym_span, sym_offsets = symbol_or_company_appears_in_text_with_offsets(clean_sym, norm)
    if not sym_ok or sym_offsets is None:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.UNRESOLVED_SYMBOL.value,
            f"Option underlying '{clean_sym}' not evidenced in text",
        )

    # --- Step 3 (BRAIN-V3-02): Atomic Contract Grounding (`symbol + [pre-expiry] + strike + CE/PE`) ---
    opt_side_pat = r"(?:CE|CALL)" if canon_opt == "CE" else r"(?:PE|PUT)"
    opp_side_pat = (
        r"\d(?:[\d,.]*)\s*(?:PE|PUT)\b|\bPE\b"
        if canon_opt == "CE"
        else r"\d(?:[\d,.]*)\s*(?:CE|CALL)\b|\bCE\b"
    )
    if re.search(opp_side_pat, norm, flags=re.IGNORECASE):
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Candidate option_type '{canon_opt}' contradicts or mixes with opposite option side in message text",
        )

    # Locate all occurrences of `clean_sym` (or its resolved alias/company phrase) in `norm`
    # and require `strike + opt_side` to be atomically bound to that underlying with ONLY optional
    # valid pre-expiry tokens (`[day] <month>`) or whitespace between them — NEVER another ticker or punctuation (`?`, `!`, `;`).
    cleaned_norm = strip_clock_times(norm)
    contract_hdr_re = re.compile(
        r"(?P<strike_tok>\d[\d,.]*)\s*(?P<opt_tok>" + opt_side_pat + r")\b",
        re.IGNORECASE,
    )
    matched_hdr = None
    matched_pre_expiry_seg = ""
    matched_sym_offsets = sym_offsets

    # Collect all candidate symbol spans for `clean_sym` in `cleaned_norm`
    sym_occurrences: List[Tuple[int, int, str]] = []
    for sm in re.finditer(rf"(?<![A-Za-z0-9&\-])#?\s*{re.escape(clean_sym)}(?![A-Za-z0-9&\-])", cleaned_norm, flags=re.IGNORECASE):
        sym_occurrences.append((sm.start(), sm.end(), norm[sm.start():sm.end()].strip()))
    if not sym_occurrences and sym_offsets is not None:
        sym_occurrences.append((sym_offsets[0], sym_offsets[1], sym_span))

    for hm in contract_hdr_re.finditer(cleaned_norm):
        st_parsed = parse_clean_number_token(hm.group("strike_tok").rstrip("."))
        if st_parsed is None or abs(st_parsed - strike_val) >= 1e-6:
            continue
        # Verify there is a preceding `clean_sym` occurrence atomically linked to this `hm`
        for s_start, s_end, s_txt in sym_occurrences:
            if s_end <= hm.start():
                between = cleaned_norm[s_end:hm.start()]
                # Disallow sentence breaks, question marks, slashes, or other stock symbols between symbol and strike
                if re.search(r"[?!;/\n]", between):
                    continue
                between_words = [w.upper() for w in re.findall(r"[A-Za-z]+", between)]
                # Only allow valid month tokens, ordinal suffixes, or contract filler words ('OPT', 'OPTION', 'STRIKE') between sym and strike
                allowed_between = set(VALID_MONTH_MAP.keys()) | {"OPT", "OPTION", "OPTIONS", "STRIKE", "ST", "ND", "RD", "TH"}
                if all(w in allowed_between for w in between_words):
                    matched_hdr = hm
                    matched_pre_expiry_seg = between.strip()
                    matched_sym_offsets = (s_start, s_end)
                    sym_span = s_txt
                    break
        if matched_hdr is not None:
            break

    if matched_hdr is None:
        return (
            None,
            PriceStatus.INVALID,
            {},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Atomic contract grounding failed: '{clean_sym} {strike_val:g} {canon_opt}' is not evidenced as a single coherent contract in text",
        )

    contract_start = matched_sym_offsets[0]
    contract_end = matched_hdr.end()
    contract_span = norm[contract_start:contract_end].strip()
    strike_offsets = (matched_hdr.start("strike_tok"), matched_hdr.end("strike_tok"))
    opt_offsets = (matched_hdr.start("opt_tok"), matched_hdr.end("opt_tok"))

    # --- Step 4: Validate explicit expiry (both pre-strike and post-strike) ---
    exp_ok, text_exp_day, text_exp_mon, exp_code, exp_info = extract_explicit_option_expiry_from_text(
        norm,
        after_opt_pos=matched_hdr.end(),
        pre_strike_seg=matched_pre_expiry_seg,
    )
    if not exp_ok:
        return None, PriceStatus.INVALID, {"contract_span": contract_span}, exp_code, exp_info

    cand_mon: Optional[str] = None
    if expiry_month:
        m_valid, m_canon, m_reason = validate_explicit_expiry_token(expiry_month)
        if not m_valid:
            return None, PriceStatus.INVALID, {}, DecisionReasonCode.INVALID_EXPIRY_TOKEN.value, m_reason
        cand_mon = m_canon

    if text_exp_mon is not None and cand_mon != text_exp_mon:
        return (
            None,
            PriceStatus.INVALID,
            {"contract_span": contract_span, "expiry_span": exp_info},
            DecisionReasonCode.OMITTED_EXPLICIT_EXPIRY.value,
            f"Candidate expiry_month ({cand_mon!r}) does not match explicitly stated expiry month ({text_exp_mon!r})",
        )
    if text_exp_mon is None and cand_mon is not None:
        return (
            None,
            PriceStatus.INVALID,
            {"contract_span": contract_span},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Candidate invented expiry_month ({cand_mon!r}) not stated in text",
        )

    if text_exp_day is not None and cand_day != text_exp_day:
        return (
            None,
            PriceStatus.INVALID,
            {"contract_span": contract_span, "expiry_span": exp_info},
            DecisionReasonCode.OMITTED_EXPLICIT_EXPIRY.value,
            f"Candidate expiry_day ({cand_day!r}) does not match explicitly stated expiry day ({text_exp_day!r})",
        )
    if text_exp_day is None and cand_day is not None:
        return (
            None,
            PriceStatus.INVALID,
            {"contract_span": contract_span},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Candidate invented expiry_day ({cand_day!r}) not stated in text",
        )

    # --- Step 5: Validate entry premium from the dedicated post-contract entry clause ---
    price_res = extract_fno_entry_bounds(norm, after_pos=matched_hdr.end())
    if price_res.status != PriceStatus.VALID:
        return (
            None,
            price_res.status,
            {"contract_span": contract_span, "price_span": price_res.source_span},
            price_res.reason_code,
            price_res.reason,
        )

    if abs(c_min - price_res.entry_min) > 1e-6 or abs(c_max - price_res.entry_max) > 1e-6:
        return (
            None,
            PriceStatus.INVALID,
            {"contract_span": contract_span, "price_span": price_res.source_span},
            DecisionReasonCode.AI_GROUNDING_FAILED.value,
            f"Candidate entry bounds ({c_min:g}, {c_max:g}) do not match verified entry premium ({price_res.entry_min:g}, {price_res.entry_max:g})",
        )

    # Sanitize optional informational SL / Targets
    clean_sl: Optional[float] = None
    if stop_loss is not None:
        sl_f = float(stop_loss)
        if 0 < sl_f < price_res.entry_min and number_token_appears_in_text(sl_f, norm, require_entry_role=False):
            clean_sl = sl_f

    clean_targets: List[float] = []
    if targets:
        for t in targets:
            tf = float(t)
            if tf > price_res.entry_max and number_token_appears_in_text(tf, norm, require_entry_role=False):
                clean_targets.append(tf)
    clean_targets = sorted(clean_targets)

    act_m = re.search(r"\b(?:buy|bought|add|added|entry|fresh|long)\b", norm, flags=re.IGNORECASE)
    action_span = norm[act_m.start():act_m.end()] if act_m else ""
    action_offsets = [act_m.start(), act_m.end()] if act_m else None

    evidence = {
        "symbol": clean_sym,
        "symbol_span": sym_span,
        "symbol_span_offsets": list(matched_sym_offsets),
        "action_span": action_span,
        "action_span_offsets": action_offsets,
        "strike": strike_val,
        "strike_span": norm[strike_offsets[0]:strike_offsets[1]],
        "strike_span_offsets": list(strike_offsets),
        "option_type": canon_opt,
        "option_side_span": norm[opt_offsets[0]:opt_offsets[1]],
        "option_side_offsets": list(opt_offsets),
        "contract_span": contract_span,
        "contract_span_offsets": [contract_start, contract_end],
        "expiry_month": text_exp_mon,
        "expiry_day": text_exp_day,
        "expiry_span": exp_info,
        "price_status": price_res.status.value,
        "entry_min": price_res.entry_min,
        "entry_max": price_res.entry_max,
        "price_span": price_res.source_span,
        "price_span_offsets": list(price_res.span_offsets) if price_res.span_offsets else None,
    }

    sig = FnOSignal(
        symbol=clean_sym,
        strike=strike_val,
        option_type=canon_opt,
        expiry_month=text_exp_mon,
        expiry_day=text_exp_day,
        entry_min=price_res.entry_min,
        entry_max=price_res.entry_max,
        stop_loss=clean_sl,
        targets=clean_targets,
        action=SignalAction.BUY_ENTRY,
        caution=bool(caution),
        hold_hint=hold_hint,
        raw_text=raw_text,
        channel_id=channel_id,
        message_id=message_id,
        message_timestamp=msg_timestamp,
    )
    return sig, PriceStatus.VALID, evidence, DecisionReasonCode.OK.value, "Successfully parsed F&O option buy signal"
