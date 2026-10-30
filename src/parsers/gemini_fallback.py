"""
Optional Gemini AI Fallback Parser (adopted from `darvas_script/services/gemini_service.py`
and `darvas_script/services/fno_parser.py`).

Implements REQUIREMENTS.md Section 3.3 (AI Fallback — Recheck Only) + V12-26 Hardening:
- Invoked ONLY when deterministic regex finds no match AND the message passes deterministic
  safety gatekeepers (age <= 60s, not an exit signal, not an index trade).
- Requires `VERTEX_PROJECT_ID` or `GEMINI_API_KEY` in environment; gracefully no-ops when unset.
- V12-26: Treats model output as untrusted extraction:
  * Enforces `GEMINI_TIMEOUT_SEC` on API calls and wall-clock elapsed time.
  * Rejects any `option_type` not explicitly in `{"CE", "CALL", "PE", "PUT"}` (never defaults unknown strings to `PE`).
  * Verifies source-level evidence: symbol/company token, strike, and entry prices must literally appear in the source message text.
  * Re-checks signal age (`max_age_sec`) after inference completes before returning a candidate.
"""

import logging
import math
import os
import re
import time
from typing import List, Optional
from pydantic import BaseModel, Field

from src.models import EquitySignal, FnOSignal, OrderType, SignalAction
from src.parsers.fno_parser import (
    INDEX_UNDERLYINGS,
    CAUTION_PATTERN,
    HOLD_PATTERN,
    looks_like_option,
    is_exit_message,
)
from src.parsers.equity_parser import (
    INDEX_KEYWORDS,
    filter_equity_message,
)
from src.instruments.resolver import VALID_EQUITY_SYMBOLS, normalize_name, resolve_company_name

logger = logging.getLogger(__name__)

GEMINI_TIMEOUT_SEC = 8.0
_FALLBACK_MODELS = ("gemini-2.5-flash", "gemini-2.0-flash")
_VALID_OPTION_TYPES = {"CE": "CE", "CALL": "CE", "PE": "PE", "PUT": "PE"}


class GeminiFnOCandidate(BaseModel):
    is_option_buy_entry: bool = Field(description="True only if this message is a fresh BUY call on a stock option")
    symbol: str = Field(description="Underlying stock symbol, e.g. POLYCAB, TATACHEM")
    strike: float
    option_type: str = Field(description="CE or PE")
    expiry_month: Optional[str] = Field(default=None, description="3-letter month like OCT if mentioned, else null")
    entry_min: float
    entry_max: float
    stop_loss: Optional[float] = Field(default=None)
    targets: List[float] = Field(default_factory=list)


class GeminiEquityCandidate(BaseModel):
    is_buy_entry: bool = Field(description="True only if this message is a fresh BUY recommendation for a single stock")
    stock_symbol: str = Field(description="Clean NSE/BSE stock ticker in uppercase without exchange prefix")
    entry_price: float = Field(default=0.0, description="Recommended entry price or 0 if CMP/Market")


_FNO_PROMPT = """You extract stock option BUY calls from Indian stock market Telegram messages.
Set is_option_buy_entry=false for anything that is not a fresh buy entry on a CE/PE option
(exits, profit booking updates, commentary, equity calls, or index options). Prices are option premiums.
If a single entry price is given, set entry_min = entry_max = that price.
Message:
"""

_EQUITY_PROMPT = """You are a deterministic financial data extractor for an automated NSE/BSE equity trading bot.
Determine if this Telegram message is an actionable BUY entry signal for a SINGLE Indian stock.
Set is_buy_entry=false for exits, profit booking, index trades (NIFTY/BANKNIFTY/SENSEX), options, or general market watchlists.
Message:
"""


def _number_appears_in_text(val: float, text: str) -> bool:
    if not math.isfinite(val) or val <= 0:
        return False
    candidates = {f"{val:g}", f"{val:.2f}", f"{val:.1f}"}
    if abs(val - round(val)) < 1e-6:
        candidates.add(str(int(round(val))))
    for token in candidates:
        pattern = rf"(?<![0-9.]){re.escape(token)}(?![0-9])"
        if re.search(pattern, text):
            return True
    return False


def _symbol_appears_in_text(sym: str, text: str) -> bool:
    upper_text = (text or "").upper()
    clean_sym = (sym or "").strip().upper()
    if not clean_sym:
        return False
    if clean_sym in upper_text:
        return True
    words = normalize_name(upper_text)
    for i in range(len(words)):
        for length in range(min(5, len(words) - i), 0, -1):
            resolved = resolve_company_name(words[i : i + length])
            if resolved == clean_sym:
                return True
    return False


def _get_genai_client():
    """Lazily initializes Google GenAI client if configured and installed."""
    project_id = os.environ.get("VERTEX_PROJECT_ID", "").strip()
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not project_id and not api_key:
        return None
    try:
        from google import genai
        http_opts = {"timeout": int(GEMINI_TIMEOUT_SEC * 1000)}
        if project_id:
            location = os.environ.get("GEMINI_LOCATION", "global").strip() or "global"
            return genai.Client(vertexai=True, project=project_id, location=location, http_options=http_opts)
        return genai.Client(api_key=api_key, http_options=http_opts)
    except Exception as e:
        logger.debug(f"[GEMINI FALLBACK] Client unavailable: {e}")
        return None


def parse_fno_gemini_sync(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Optional[FnOSignal]:
    """
    Synchronous Gemini fallback for non-standard option calls that pass `looks_like_option`
    and are not exits. Enforces source-text proof, strict CE/PE validation, timeout, and age recheck.
    """
    if not text or is_exit_message(text) or not looks_like_option(text):
        return None

    now_start = current_time if current_time is not None else time.time()
    msg_ts = msg_timestamp or now_start
    if (now_start - msg_ts) > max_age_sec:
        return None

    client = _get_genai_client()
    if not client:
        return None

    t0 = time.time()
    try:
        try:
            from google.genai import types
            gen_cfg = types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
                response_schema=GeminiFnOCandidate,
            )
        except Exception:
            gen_cfg = {
                "temperature": 0.0,
                "response_mime_type": "application/json",
                "response_schema": GeminiFnOCandidate,
            }
        for model_name in _FALLBACK_MODELS:
            if (time.time() - t0) > GEMINI_TIMEOUT_SEC:
                logger.warning("[GEMINI FNO] Timeout budget exceeded before model call")
                return None
            try:
                resp = client.models.generate_content(
                    model=model_name,
                    contents=_FNO_PROMPT + text,
                    config=gen_cfg,
                )
                elapsed = time.time() - t0
                if elapsed > GEMINI_TIMEOUT_SEC:
                    logger.warning(f"[GEMINI FNO] Inference exceeded timeout ({elapsed:.2f}s > {GEMINI_TIMEOUT_SEC}s)")
                    return None

                post_now = (current_time + elapsed) if current_time is not None else time.time()
                if (post_now - msg_ts) > max_age_sec:
                    logger.warning("[GEMINI FNO] Signal aged past max_age_sec during inference; rejecting")
                    return None

                g: Optional[GeminiFnOCandidate] = getattr(resp, "parsed", None)
                if not g or not g.is_option_buy_entry:
                    return None

                sym = g.symbol.strip().upper()
                if not sym or sym in INDEX_UNDERLYINGS:
                    return None

                raw_opt = (g.option_type or "").strip().upper()
                if raw_opt not in _VALID_OPTION_TYPES:
                    logger.warning(f"[GEMINI FNO] Rejected invalid option_type '{g.option_type}'")
                    return None
                opt_type = _VALID_OPTION_TYPES[raw_opt]

                e_min = min(float(g.entry_min), float(g.entry_max))
                e_max = max(float(g.entry_min), float(g.entry_max))
                if not math.isfinite(g.strike) or g.strike <= 0 or not math.isfinite(e_min) or e_min <= 0:
                    return None

                # V12-26: Require source-level proof that symbol, strike, and entry prices appear in text
                if not _symbol_appears_in_text(sym, text):
                    logger.warning(f"[GEMINI FNO] Symbol '{sym}' not evidenced in source text; rejecting")
                    return None
                if not _number_appears_in_text(float(g.strike), text):
                    logger.warning(f"[GEMINI FNO] Strike '{g.strike}' not evidenced in source text; rejecting")
                    return None
                if not _number_appears_in_text(e_min, text) or not _number_appears_in_text(e_max, text):
                    logger.warning(f"[GEMINI FNO] Entry prices ({e_min}, {e_max}) not evidenced in source text; rejecting")
                    return None

                sl_val = g.stop_loss if (g.stop_loss and 0 < g.stop_loss < e_min and _number_appears_in_text(float(g.stop_loss), text)) else None
                targets = sorted(
                    t for t in (g.targets or [])
                    if t and math.isfinite(float(t)) and float(t) > e_max and _number_appears_in_text(float(t), text)
                )
                hold_m = HOLD_PATTERN.search(text)

                return FnOSignal(
                    symbol=sym,
                    strike=float(g.strike),
                    option_type=opt_type,
                    expiry_month=g.expiry_month.strip().upper()[:3] if g.expiry_month else None,
                    entry_min=e_min,
                    entry_max=e_max,
                    stop_loss=sl_val,
                    targets=targets,
                    action=SignalAction.BUY_ENTRY,
                    caution=bool(CAUTION_PATTERN.search(text)),
                    hold_hint=hold_m.group(0).strip() if hold_m else None,
                    raw_text=text,
                    channel_id=channel_id,
                    message_id=message_id,
                    message_timestamp=msg_ts,
                )
            except Exception as e:
                logger.warning(f"[GEMINI FNO] {model_name} fallback error: {e}")
    except Exception:
        pass
    return None


def parse_equity_gemini_sync(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
) -> Optional[EquitySignal]:
    """
    Synchronous Gemini fallback for equity signals that pass `filter_equity_message`
    but miss regex ticker extraction. Enforces source-text proof, timeout, and age recheck.
    """
    now_start = current_time if current_time is not None else time.time()
    msg_ts = msg_timestamp or now_start
    passed, _, _ = filter_equity_message(
        text, msg_timestamp=msg_ts, max_age_sec=max_age_sec, current_time=now_start
    )
    if not passed:
        return None

    client = _get_genai_client()
    if not client:
        return None

    t0 = time.time()
    try:
        try:
            from google.genai import types
            eq_cfg = types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
                response_schema=GeminiEquityCandidate,
            )
        except Exception:
            eq_cfg = {
                "temperature": 0.0,
                "response_mime_type": "application/json",
                "response_schema": GeminiEquityCandidate,
            }
        for model_name in _FALLBACK_MODELS:
            if (time.time() - t0) > GEMINI_TIMEOUT_SEC:
                return None
            try:
                resp = client.models.generate_content(
                    model=model_name,
                    contents=_EQUITY_PROMPT + text,
                    config=eq_cfg,
                )
                elapsed = time.time() - t0
                if elapsed > GEMINI_TIMEOUT_SEC:
                    return None

                post_now = (current_time + elapsed) if current_time is not None else time.time()
                if (post_now - msg_ts) > max_age_sec:
                    return None

                g: Optional[GeminiEquityCandidate] = getattr(resp, "parsed", None)
                if not g or not g.is_buy_entry:
                    return None
                sym = g.stock_symbol.replace("NSE:", "").replace("BSE:", "").strip().upper()
                if not sym or sym.lower() in INDEX_KEYWORDS:
                    return None
                if VALID_EQUITY_SYMBOLS and sym not in VALID_EQUITY_SYMBOLS:
                    return None

                # V12-26: Require source-text proof of symbol and stated price (when > 0)
                if not _symbol_appears_in_text(sym, text):
                    return None
                stated_p = max(0.0, float(g.entry_price or 0.0))
                if stated_p > 0.0 and not _number_appears_in_text(stated_p, text):
                    return None

                return EquitySignal(
                    symbol=sym,
                    action=SignalAction.BUY_ENTRY,
                    stated_price=stated_p,
                    order_type=OrderType.LIMIT,
                    raw_text=text,
                    channel_id=channel_id,
                    message_id=message_id,
                    message_timestamp=msg_ts,
                )
            except Exception as e:
                logger.warning(f"[GEMINI EQUITY] {model_name} fallback error: {e}")
    except Exception:
        pass
    return None
