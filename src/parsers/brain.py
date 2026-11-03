"""
AuditBot BRAIN — Unified Synchronous Parsing & Validation Facade (Module 2 — v3.1).

Implements the exact architectural boundary and all v3.1 requirements:
- One synchronous parsing facade (`parse_signal_brain`) compatible with the existing `asyncio.to_thread` caller.
- Returns a typed `BrainDecision` containing:
  * `route`: `"OPTION"` | `"EQUITY"` | `"NONE"`
  * `action`: `SignalAction` (`BUY_ENTRY`, `EXIT_SIGNAL`, `BLOCKED`, `IGNORED`)
  * `reason_code`: structured machine-readable code (`INVALID_EXPLICIT_PRICE`, `AMBIGUOUS_CONTRACT`,
    `NO_ENTRY_PREMIUM`, `AI_TIMEOUT`, `ADAPTIVE_RULE_TIMEOUT`, `ADAPTIVE_WORKER_ERROR`,
    `UNRESOLVED_SYMBOL`, `PARSER_EXCEPTION`, etc.)
  * `reason`: human-readable validation / rejection / extraction explanation
  * `source`: `"REGEX"` | `"ADAPTIVE_REGEX"` | `"GEMINI"` | `"FILTER"` | `"ERROR"` | `"NONE"`
  * `parser_version`: `"BRAIN-V14.3.2"`
  * `rule_id`: matched deterministic or versioned adaptive rule identifier
  * `model_name`: Gemini model used when `source == "GEMINI"`
  * `price_status`: `PriceStatus` (`ABSENT`, `VALID`, `INVALID`)
  * `explicit_price`: boolean indicating whether a valid explicit price was stated
  * `evidence`: structured dictionary of verified tokens, source spans, and character offsets
  * `parse_latency_ms`: monotonic parsing duration in milliseconds
  * `equity_signal` / `fno_signal`: at most one native `EquitySignal` or `FnOSignal`

Key Safeguards Enforced:
1. Preserves `SignalAction.BLOCKED` on deterministic hard rejections and adaptive policy rejections.
2. Enforces a pre-handoff freshness gate (`(effective_now - msg_ts) <= max_age_sec`) before returning any
   executable `BUY_ENTRY` decision so slow adaptive/AI processing can never hand off a stale signal (`BRAIN-V3-07`).
3. Preserves worker failure reason codes (`ADAPTIVE_RULE_TIMEOUT`, `ADAPTIVE_WORKER_ERROR`, `BRAIN-V3-05`).
4. Uses the actual prior V14 parser implementation in `compare_corpus_replay()` and classifies differences into
   `expected_safety_correction`, `approved_behavior_preservation`, `unexpected_regression`, and `unverified`.
"""

from dataclasses import dataclass, field
import logging
import re
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from src.models import EquitySignal, FnOSignal, SignalAction, SignalType
from src.instruments.resolver import (
    VALID_EQUITY_SYMBOLS,
    _NAME_INDEX,
    normalize_name,
    resolve_company_name,
)
from src.parsers.equity_parser import (
    ACTION_PHRASE_PATTERN,
    ACTION_SYMBOL_PATTERN,
    ANALYSIS_KEYWORDS,
    BUY_INTENT_PATTERN,
    COMMON_STOCK_ALIASES,
    EXIT_PATTERN as EQ_EXIT_PATTERN,
    HASHTAG_PATTERN,
    HASHTAG_PHRASE_PATTERN,
    INDEX_KEYWORDS,
    META_TAGS,
    NOISE_WORDS,
    OPTION_HINT_PATTERN,
    PHRASE_STOP_WORDS,
    filter_equity_message_detailed,
    parse_equity_regex_detailed,
)
from src.parsers.fno_parser import (
    INDEX_UNDERLYINGS,
    looks_like_option,
    parse_fno_regex_detailed,
)
from src.parsers.gemini_fallback import (
    parse_equity_gemini_with_meta,
    parse_fno_gemini_with_meta,
)
from src.parsers.brain_validation import (
    PARSER_VERSION,
    DecisionReasonCode,
    PriceStatus,
)
from src.parsers.adaptive_rules import AdaptiveRuleManager, RegexEvalOutcome

logger = logging.getLogger(__name__)


@dataclass
class BrainDecision:
    route: str  # "OPTION" | "EQUITY" | "NONE"
    action: SignalAction
    reason: str
    reason_code: str = DecisionReasonCode.OK.value
    source: str = "NONE"  # "REGEX" | "ADAPTIVE_REGEX" | "GEMINI" | "FILTER" | "ERROR" | "NONE"
    parser_version: str = PARSER_VERSION
    rule_id: str = ""
    model_name: Optional[str] = None
    price_status: PriceStatus = PriceStatus.ABSENT
    explicit_price: bool = False
    evidence: Dict[str, Any] = field(default_factory=dict)
    parse_latency_ms: float = 0.0
    equity_signal: Optional[EquitySignal] = None
    fno_signal: Optional[FnOSignal] = None

    @property
    def signal(self) -> Optional[Any]:
        return self.fno_signal if self.fno_signal is not None else self.equity_signal

    @property
    def signal_type(self) -> Optional[SignalType]:
        if self.route == "OPTION":
            return SignalType.OPTION
        if self.route == "EQUITY":
            return SignalType.EQUITY
        return None


def parse_signal_brain(
    text: str,
    channel_id: int = 0,
    message_id: int = 0,
    msg_timestamp: float = 0.0,
    max_age_sec: float = 60.0,
    current_time: Optional[float] = None,
    journal_db: Optional[Any] = None,
    enable_ai_fallback: bool = True,
) -> BrainDecision:
    """
    Single synchronous BRAIN parsing entry point called by `TradingBotPipeline.process_raw_message`
    after source authorization, durable `claim_signal`, and initial message-age gating.
    Isolates any unexpected exception into a logged non-executable `BLOCKED` decision (`PARSER_EXCEPTION`).
    """
    t0_ns = time.perf_counter_ns()

    def _elapsed_ms() -> float:
        return round((time.perf_counter_ns() - t0_ns) / 1_000_000.0, 3)

    def _guard_freshness_before_handoff(candidate_dec: BrainDecision, base_now: float, m_ts: float) -> BrainDecision:
        """BRAIN-V3-07: Never hand off a BUY_ENTRY decision if elapsed parsing caused the signal to exceed max_age_sec."""
        elapsed_sec = (time.perf_counter_ns() - t0_ns) / 1_000_000_000.0
        post_now = (current_time + elapsed_sec) if current_time is not None else time.time()
        age_after = post_now - m_ts
        if age_after > max_age_sec:
            return BrainDecision(
                route=candidate_dec.route,
                action=SignalAction.BLOCKED,
                reason=f"Signal aged past max_age_sec ({age_after:.2f}s > {max_age_sec}s) before executor handoff",
                reason_code=DecisionReasonCode.MESSAGE_TOO_OLD.value,
                source=candidate_dec.source,
                rule_id=candidate_dec.rule_id,
                model_name=candidate_dec.model_name,
                price_status=candidate_dec.price_status,
                explicit_price=candidate_dec.explicit_price,
                evidence=candidate_dec.evidence,
                parse_latency_ms=_elapsed_ms(),
                equity_signal=None,
                fno_signal=None,
            )
        candidate_dec.parse_latency_ms = _elapsed_ms()
        return candidate_dec

    try:
        now = current_time if current_time is not None else time.time()
        msg_ts = msg_timestamp if (msg_timestamp and msg_timestamp > 0) else now

        if not text or not isinstance(text, str) or not text.strip():
            return BrainDecision(
                route="NONE",
                action=SignalAction.BLOCKED,
                reason="Empty message",
                reason_code=DecisionReasonCode.EMPTY_MESSAGE.value,
                source="FILTER",
                rule_id="EMPTY_CHECK",
                price_status=PriceStatus.INVALID,
                parse_latency_ms=_elapsed_ms(),
            )

        # =====================================================================
        # ROUTE 1: Stock Options (F&O) — Exclusive when `looks_like_option(text)`
        # =====================================================================
        if looks_like_option(text):
            fno_sig, fno_reason, fno_action, fno_code, fno_pstat, fno_ev = parse_fno_regex_detailed(
                text,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                max_age_sec=max_age_sec,
                current_time=now,
            )

            # 1a. Deterministic Exit Signal
            if fno_action == SignalAction.EXIT_SIGNAL:
                return BrainDecision(
                    route="OPTION",
                    action=SignalAction.EXIT_SIGNAL,
                    reason=fno_reason,
                    reason_code=fno_code,
                    source="FILTER",
                    rule_id="FNO_EXIT_GATE",
                    price_status=fno_pstat,
                    parse_latency_ms=_elapsed_ms(),
                )

            # 1b. Deterministic Hard Rejection
            if fno_action == SignalAction.BLOCKED:
                return BrainDecision(
                    route="OPTION",
                    action=SignalAction.BLOCKED,
                    reason=fno_reason,
                    reason_code=fno_code,
                    source="FILTER",
                    rule_id="FNO_SAFETY_GATE",
                    price_status=fno_pstat,
                    evidence=fno_ev,
                    parse_latency_ms=_elapsed_ms(),
                )

            # 1c. Deterministic Regex Match
            if fno_sig is not None:
                return _guard_freshness_before_handoff(
                    BrainDecision(
                        route="OPTION",
                        action=SignalAction.BUY_ENTRY,
                        reason=fno_reason,
                        reason_code=DecisionReasonCode.OK.value,
                        source="REGEX",
                        rule_id="FNO_REGEX_V14",
                        price_status=PriceStatus.VALID,
                        explicit_price=True,
                        evidence=fno_ev,
                        parse_latency_ms=_elapsed_ms(),
                        fno_signal=fno_sig,
                    ),
                    now,
                    msg_ts,
                )

            # 1d. Eligible F&O Extraction Miss (`fno_action == SignalAction.IGNORED`):
            # Try explicitly activated adaptive OPTION rules first (if journal_db provided)
            if journal_db is not None:
                rule_mgr = AdaptiveRuleManager(journal_db)
                adap_sig, adap_rule_id, adap_reason, adap_ev = rule_mgr.match_active_rules(
                    route="OPTION",
                    text=text,
                    channel_id=channel_id,
                    message_id=message_id,
                    msg_timestamp=msg_ts,
                    max_age_sec=max_age_sec,
                    current_time=now,
                )
                if adap_sig is not None:
                    return _guard_freshness_before_handoff(
                        BrainDecision(
                            route="OPTION",
                            action=SignalAction.BUY_ENTRY,
                            reason=adap_reason,
                            reason_code=DecisionReasonCode.OK.value,
                            source="ADAPTIVE_REGEX",
                            rule_id=adap_rule_id or "ADAPTIVE_OPTION",
                            price_status=PriceStatus.VALID,
                            explicit_price=True,
                            evidence=adap_ev,
                            parse_latency_ms=_elapsed_ms(),
                            fno_signal=adap_sig,
                        ),
                        now,
                        msg_ts,
                    )
                if adap_ev.get("policy_rejected"):
                    return BrainDecision(
                        route="OPTION",
                        action=SignalAction.BLOCKED,
                        reason=adap_reason,
                        reason_code=adap_ev.get("reason_code", DecisionReasonCode.AI_GROUNDING_FAILED.value),
                        source="ADAPTIVE_REGEX",
                        rule_id=adap_rule_id or "ADAPTIVE_OPTION_REJECTED",
                        price_status=PriceStatus.INVALID,
                        evidence=adap_ev,
                        parse_latency_ms=_elapsed_ms(),
                    )
                # P1-04: Fail closed immediately on adaptive worker timeout or error BEFORE Gemini fallback!
                w_out_opt = adap_ev.get("worker_outcome")
                if w_out_opt in (
                    RegexEvalOutcome.TIMEOUT.value,
                    RegexEvalOutcome.WORKER_ERROR.value,
                    RegexEvalOutcome.INVALID_PATTERN.value,
                    RegexEvalOutcome.EVALUATION_ERROR.value,
                ):
                    default_err_code = (
                        DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value
                        if w_out_opt == RegexEvalOutcome.TIMEOUT.value
                        else DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value
                    )
                    return BrainDecision(
                        route="OPTION",
                        action=SignalAction.BLOCKED,
                        reason=adap_reason or fno_reason,
                        reason_code=adap_ev.get("reason_code") or default_err_code,
                        source="ADAPTIVE_REGEX",
                        rule_id=adap_rule_id or "ADAPTIVE_OPTION_ERROR",
                        price_status=fno_pstat,
                        evidence=adap_ev,
                        parse_latency_ms=_elapsed_ms(),
                    )

            # 1e. Try synchronous Gemini F&O fallback (bounded monotonic deadline, unified validator)
            if enable_ai_fallback:
                elapsed_before_ai = (time.perf_counter_ns() - t0_ns) / 1_000_000_000.0
                ai_now = (current_time + elapsed_before_ai) if current_time is not None else None
                ai_fno_sig, ai_fno_reason, ai_model, ai_code, ai_ev = parse_fno_gemini_with_meta(
                    text,
                    channel_id=channel_id,
                    message_id=message_id,
                    msg_timestamp=msg_ts,
                    max_age_sec=max_age_sec,
                    current_time=ai_now,
                )
                if ai_fno_sig is not None:
                    return _guard_freshness_before_handoff(
                        BrainDecision(
                            route="OPTION",
                            action=SignalAction.BUY_ENTRY,
                            reason=ai_fno_reason,
                            reason_code=DecisionReasonCode.OK.value,
                            source="GEMINI",
                            rule_id="FNO_GEMINI_STRUCTURED",
                            model_name=ai_model,
                            price_status=PriceStatus.VALID,
                            explicit_price=True,
                            evidence=ai_ev,
                            parse_latency_ms=_elapsed_ms(),
                            fno_signal=ai_fno_sig,
                        ),
                        now,
                        msg_ts,
                    )
                if ai_code != DecisionReasonCode.AI_UNCONFIGURED.value:
                    fail_act = (
                        SignalAction.BLOCKED
                        if ai_code in (
                            DecisionReasonCode.AI_GROUNDING_FAILED.value,
                            DecisionReasonCode.OMITTED_EXPLICIT_EXPIRY.value,
                            DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                            DecisionReasonCode.MESSAGE_TOO_OLD.value,
                            DecisionReasonCode.AMBIGUOUS_CONTRACT.value,
                        )
                        else SignalAction.IGNORED
                    )
                    return BrainDecision(
                        route="OPTION",
                        action=fail_act,
                        reason=f"{fno_reason} (AI fallback: {ai_fno_reason})",
                        reason_code=ai_code,
                        source="GEMINI",
                        rule_id="FNO_GEMINI_REJECTED",
                        model_name=ai_model,
                        price_status=fno_pstat,
                        evidence=ai_ev,
                        parse_latency_ms=_elapsed_ms(),
                    )

            return BrainDecision(
                route="OPTION",
                action=SignalAction.IGNORED,
                reason=fno_reason,
                reason_code=fno_code,
                source="NONE",
                rule_id="FNO_UNMATCHED",
                price_status=fno_pstat,
                parse_latency_ms=_elapsed_ms(),
            )

        # =====================================================================
        # ROUTE 2: Cash Equity (`looks_like_option(text) == False`)
        # =====================================================================
        eq_passed, eq_filter_reason, eq_action, eq_filter_code = filter_equity_message_detailed(
            text,
            msg_timestamp=msg_ts,
            max_age_sec=max_age_sec,
            current_time=now,
        )

        # 2a. Deterministic Equity Exit Signal
        if not eq_passed and eq_action == SignalAction.EXIT_SIGNAL:
            return BrainDecision(
                route="EQUITY",
                action=SignalAction.EXIT_SIGNAL,
                reason=eq_filter_reason,
                reason_code=eq_filter_code,
                source="FILTER",
                rule_id="EQUITY_EXIT_GATE",
                price_status=PriceStatus.ABSENT,
                parse_latency_ms=_elapsed_ms(),
            )

        # 2b. Deterministic Hard Rejection (`BLOCKED`) or Casual Chatter (`IGNORED`)
        if not eq_passed:
            p_stat = (
                PriceStatus.INVALID
                if eq_filter_code in (
                    DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    DecisionReasonCode.CONFLICTING_ENTRY_PRICE.value,
                )
                else PriceStatus.ABSENT
            )
            return BrainDecision(
                route="EQUITY",
                action=eq_action,
                reason=eq_filter_reason,
                reason_code=eq_filter_code,
                source="FILTER",
                rule_id="EQUITY_SAFETY_GATE",
                price_status=p_stat,
                parse_latency_ms=_elapsed_ms(),
            )

        # 2c. Deterministic Equity Regex Extraction
        eq_sig, eq_reason, eq_reg_action, eq_reg_code, eq_pstat, eq_ev = parse_equity_regex_detailed(
            text,
            channel_id=channel_id,
            message_id=message_id,
            msg_timestamp=msg_ts,
            max_age_sec=max_age_sec,
            current_time=now,
        )
        if eq_sig is not None:
            return _guard_freshness_before_handoff(
                BrainDecision(
                    route="EQUITY",
                    action=SignalAction.BUY_ENTRY,
                    reason=eq_reason,
                    reason_code=DecisionReasonCode.OK.value,
                    source="REGEX",
                    rule_id="EQUITY_REGEX_V14",
                    price_status=eq_pstat,
                    explicit_price=(eq_pstat == PriceStatus.VALID),
                    evidence=eq_ev,
                    parse_latency_ms=_elapsed_ms(),
                    equity_signal=eq_sig,
                ),
                now,
                msg_ts,
            )

        if eq_reg_action == SignalAction.BLOCKED:
            return BrainDecision(
                route="EQUITY",
                action=SignalAction.BLOCKED,
                reason=eq_reason,
                reason_code=eq_reg_code,
                source="FILTER",
                rule_id="EQUITY_VALIDATOR_GATE",
                price_status=eq_pstat,
                evidence=eq_ev,
                parse_latency_ms=_elapsed_ms(),
            )

        # 2d. Eligible Equity Extraction Miss: Try active adaptive EQUITY rules first
        if journal_db is not None:
            rule_mgr = AdaptiveRuleManager(journal_db)
            adap_eq_sig, adap_rule_id, adap_reason, adap_ev = rule_mgr.match_active_rules(
                route="EQUITY",
                text=text,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                max_age_sec=max_age_sec,
                current_time=now,
            )
            if adap_eq_sig is not None:
                has_exp = adap_eq_sig.stated_price > 0.0
                return _guard_freshness_before_handoff(
                    BrainDecision(
                        route="EQUITY",
                        action=SignalAction.BUY_ENTRY,
                        reason=adap_reason,
                        reason_code=DecisionReasonCode.OK.value,
                        source="ADAPTIVE_REGEX",
                        rule_id=adap_rule_id or "ADAPTIVE_EQUITY",
                        price_status=PriceStatus.VALID if has_exp else PriceStatus.ABSENT,
                        explicit_price=has_exp,
                        evidence=adap_ev,
                        parse_latency_ms=_elapsed_ms(),
                        equity_signal=adap_eq_sig,
                    ),
                    now,
                    msg_ts,
                )
            if adap_ev.get("policy_rejected"):
                return BrainDecision(
                    route="EQUITY",
                    action=SignalAction.BLOCKED,
                    reason=adap_reason,
                    reason_code=adap_ev.get("reason_code", DecisionReasonCode.AI_GROUNDING_FAILED.value),
                    source="ADAPTIVE_REGEX",
                    rule_id=adap_rule_id or "ADAPTIVE_EQUITY_REJECTED",
                    price_status=PriceStatus.INVALID,
                    evidence=adap_ev,
                    parse_latency_ms=_elapsed_ms(),
                )
            # P1-04: Fail closed immediately on adaptive worker timeout or error BEFORE Gemini fallback!
            w_out_eq = adap_ev.get("worker_outcome")
            if w_out_eq in (
                RegexEvalOutcome.TIMEOUT.value,
                RegexEvalOutcome.WORKER_ERROR.value,
                RegexEvalOutcome.INVALID_PATTERN.value,
                RegexEvalOutcome.EVALUATION_ERROR.value,
            ):
                default_eq_err_code = (
                    DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value
                    if w_out_eq == RegexEvalOutcome.TIMEOUT.value
                    else DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value
                )
                return BrainDecision(
                    route="EQUITY",
                    action=SignalAction.BLOCKED,
                    reason=adap_reason or eq_reason,
                    reason_code=adap_ev.get("reason_code") or default_eq_err_code,
                    source="ADAPTIVE_REGEX",
                    rule_id=adap_rule_id or "ADAPTIVE_EQUITY_ERROR",
                    price_status=eq_pstat,
                    evidence=adap_ev,
                    parse_latency_ms=_elapsed_ms(),
                )

        # 2e. Try synchronous Gemini Equity fallback
        if enable_ai_fallback:
            elapsed_before_ai = (time.perf_counter_ns() - t0_ns) / 1_000_000_000.0
            ai_now = (current_time + elapsed_before_ai) if current_time is not None else None
            ai_eq_sig, ai_eq_reason, ai_model, ai_code, ai_ev = parse_equity_gemini_with_meta(
                text,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                max_age_sec=max_age_sec,
                current_time=ai_now,
            )
            if ai_eq_sig is not None:
                has_exp = ai_eq_sig.stated_price > 0.0
                return _guard_freshness_before_handoff(
                    BrainDecision(
                        route="EQUITY",
                        action=SignalAction.BUY_ENTRY,
                        reason=ai_eq_reason,
                        reason_code=DecisionReasonCode.OK.value,
                        source="GEMINI",
                        rule_id="EQUITY_GEMINI_STRUCTURED",
                        model_name=ai_model,
                        price_status=PriceStatus.VALID if has_exp else PriceStatus.ABSENT,
                        explicit_price=has_exp,
                        evidence=ai_ev,
                        parse_latency_ms=_elapsed_ms(),
                        equity_signal=ai_eq_sig,
                    ),
                    now,
                    msg_ts,
                )
            if ai_code != DecisionReasonCode.AI_UNCONFIGURED.value:
                fail_act = (
                    SignalAction.BLOCKED
                    if ai_code in (
                        DecisionReasonCode.AI_GROUNDING_FAILED.value,
                        DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                        DecisionReasonCode.INDEX_REJECTED.value,
                        DecisionReasonCode.MESSAGE_TOO_OLD.value,
                    )
                    else SignalAction.IGNORED
                )
                return BrainDecision(
                    route="EQUITY",
                    action=fail_act,
                    reason=f"{eq_reason} (AI fallback: {ai_eq_reason})",
                    reason_code=ai_code,
                    source="GEMINI",
                    rule_id="EQUITY_GEMINI_REJECTED",
                    model_name=ai_model,
                    price_status=eq_pstat,
                    evidence=ai_ev,
                    parse_latency_ms=_elapsed_ms(),
                )

        return BrainDecision(
            route="EQUITY",
            action=SignalAction.IGNORED,
            reason=eq_reason,
            reason_code=eq_reg_code,
            source="NONE",
            rule_id="EQUITY_UNMATCHED",
            price_status=eq_pstat,
            parse_latency_ms=_elapsed_ms(),
        )

    except Exception as exc:
        logger.error(f"[BRAIN PARSER EXCEPTION] Isolated unexpected parser error: {exc}", exc_info=True)
        return BrainDecision(
            route="NONE",
            action=SignalAction.BLOCKED,
            reason=f"Parser exception isolated (failing closed): {exc}",
            reason_code=DecisionReasonCode.PARSER_EXCEPTION.value,
            source="ERROR",
            rule_id="EXCEPTION_GUARD",
            price_status=PriceStatus.INVALID,
            parse_latency_ms=_elapsed_ms(),
        )


# =====================================================================
# Real V14 Frozen Baseline Parser Implementation (Part 11 Item #7)
# Executes the exact pre-BRAIN V14 parser logic from commit 579fd1f34b0a
# =====================================================================

_V14_MONTH_RE = "(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
_V14_NUM = r"\d+(?:\.\d+)?"
_V14_CORE_FNO_RE = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)\s*"
    r"(?P<strike>" + _V14_NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b"
    r"(?:\s+(?:(?P<day>\d{1,2})\s*)?(?P<mon>" + _V14_MONTH_RE + r")[A-Z]*)?"
    r"[^@\n]*?@\s*(?P<e1>" + _V14_NUM + r")(?:\s*(?:-|–|to)\s*(?P<e2>" + _V14_NUM + r"))?",
    re.IGNORECASE,
)
_V14_SECONDARY_FNO_RE = re.compile(
    r"(?<![A-Za-z0-9&\-])#?\s*(?P<sym>[A-Za-z][A-Za-z&\-]*)\s*"
    r"(?P<strike>" + _V14_NUM + r")\s*(?P<opt>CE|PE|CALL|PUT)\b"
    r"(?:\s+(?:(?P<day>\d{1,2})\s*)?(?P<mon>" + _V14_MONTH_RE + r")[A-Z]*)?"
    r"[^@\n]*?\b(?:at|buy|around|cmp\s*@?)\s*(?P<e1>" + _V14_NUM + r")(?:\s*(?:-|–|to)\s*(?P<e2>" + _V14_NUM + r"))?",
    re.IGNORECASE,
)
_V14_EQ_PRICE_RE = re.compile(
    r"(?:@|at|cmp|around|above)\s*(?:rs\.?)?\s*[:\-]?\s*([\d\.]+)",
    re.IGNORECASE,
)
_V14_NOISE_WORDS = frozenset({
    "THEM", "IT", "THIS", "THE", "SOME", "SOMETHING", "NOW", "TODAY", "MORE", "ALL",
    "BUY", "SELL", "BOUGHT", "ADDED", "ADD", "IN", "AT", "FOR",
    "BIY", "BOGHT", "BOUGTH", "BOT", "BAUGHT", "ASD", "ADED",
})
_V14_STOP_WORDS = frozenset({
    "AT", "CMP", "SL", "TARGET", "TGT", "ENTRY", "FOR", "NOW", "TODAY", "ABOVE", "BELOW",
    "AROUND", "NEAR", "RS", "WITH", "ON", "IN", "QTY", "LEVELS", "LEVEL", "ZONE", "BUY",
})


def _v14_phrase_tokens(phrase: str, skip_noise: bool) -> List[str]:
    tokens = phrase.split()
    if skip_noise:
        while tokens and tokens[0].upper() in _V14_NOISE_WORDS:
            tokens.pop(0)
    for i, tok in enumerate(tokens):
        if tok.upper().strip(".-") in _V14_STOP_WORDS:
            return tokens[:i]
    return tokens


def _legacy_baseline_decide(text: str, now: float = 1000.0, msg_ts: float = 995.0) -> Dict[str, Any]:
    """
    Executes the exact prior V14 deterministic parser logic (commit 579fd1f34b0a7360efe888cb7181224c333017fc)
    for real baseline replay comparison without invoking executors or brokers.
    """
    if not text or not isinstance(text, str):
        return {"route": "NONE", "action": "BLOCKED", "symbol": None, "price": None}

    if OPTION_HINT_PATTERN.search(text):
        if EQ_EXIT_PATTERN.search(text):
            return {"route": "OPTION", "action": "EXIT_SIGNAL", "symbol": None, "price": None}
        m = _V14_CORE_FNO_RE.search(text) or _V14_SECONDARY_FNO_RE.search(text)
        if not m:
            return {"route": "OPTION", "action": "IGNORED", "symbol": None, "price": None}
        raw_sym = m.group("sym").upper()
        sym = {"SENSEX": "SENSEX", "BANK NIFTY": "BANKNIFTY", "NIFTYBANK": "BANKNIFTY"}.get(raw_sym, raw_sym)
        if sym in INDEX_UNDERLYINGS:
            return {"route": "OPTION", "action": "BLOCKED", "symbol": sym, "price": None}
        e1 = float(m.group("e1"))
        e2 = float(m.group("e2")) if m.group("e2") else e1
        return {
            "route": "OPTION",
            "action": "BUY_ENTRY",
            "symbol": sym,
            "strike": float(m.group("strike")),
            "option_type": "CE" if m.group("opt").upper() in ("CE", "CALL") else "PE",
            "expiry_month": m.group("mon").upper()[:3] if m.group("mon") else None,
            "price": max(e1, e2),
        }

    # Exact V14 Equity filter + parser
    if len(text) > 300 or any(idx in text.lower() for idx in INDEX_KEYWORDS):
        return {"route": "EQUITY", "action": "BLOCKED", "symbol": None, "price": None}
    if EQ_EXIT_PATTERN.search(text):
        return {"route": "EQUITY", "action": "EXIT_SIGNAL", "symbol": None, "price": None}

    # V14 get_unique_stocks_in_message
    stocks = set()
    cleaned = text
    for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
        if re.search(r"\b" + re.escape(alias_phrase) + r"\b", cleaned, re.IGNORECASE):
            stocks.add(canonical)
            cleaned = re.sub(r"\b" + re.escape(alias_phrase) + r"\b", " ", cleaned, flags=re.IGNORECASE)
    if _NAME_INDEX:
        for pm in list(HASHTAG_PHRASE_PATTERN.finditer(cleaned)) + list(ACTION_PHRASE_PATTERN.finditer(cleaned)):
            p_tokens = _v14_phrase_tokens(pm.group(1), skip_noise=True)
            resolved = resolve_company_name(normalize_name(" ".join(p_tokens))) if p_tokens else None
            if resolved:
                stocks.add(resolved)
                phrase_str = " ".join(p_tokens)
                if phrase_str:
                    cleaned = re.sub(re.escape(phrase_str), " ", cleaned, flags=re.IGNORECASE)
    for h in HASHTAG_PATTERN.findall(cleaned):
        cand = h.upper()
        if cand not in META_TAGS and cand not in _V14_NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
            if not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS:
                stocks.add(cand)
    am_single = ACTION_SYMBOL_PATTERN.search(cleaned)
    if am_single:
        cand = am_single.group(1).upper()
        if cand not in META_TAGS and cand not in _V14_NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
            if not VALID_EQUITY_SYMBOLS or cand in VALID_EQUITY_SYMBOLS:
                stocks.add(cand)
    if len(stocks) > 1:
        return {"route": "EQUITY", "action": "BLOCKED", "symbol": None, "price": None}

    if any(kw in text.lower() for kw in ANALYSIS_KEYWORDS):
        return {"route": "EQUITY", "action": "BLOCKED", "symbol": None, "price": None}
    if not BUY_INTENT_PATTERN.search(text) and "@" not in text:
        return {"route": "EQUITY", "action": "BLOCKED", "symbol": None, "price": None}

    symbol: Optional[str] = None
    phrase: List[str] = []
    hashtags = HASHTAG_PATTERN.findall(text)
    candidate_tags = [h.upper() for h in hashtags if h.upper() not in META_TAGS and h.lower() not in INDEX_KEYWORDS]
    if candidate_tags:
        symbol = candidate_tags[0]
        phrase_match = next(
            (m for m in HASHTAG_PHRASE_PATTERN.finditer(text) if m.group(1).split()[0].upper().startswith(symbol)),
            None,
        )
        phrase = _v14_phrase_tokens(phrase_match.group(1), skip_noise=False) if phrase_match else []
        if len(phrase) >= 2 and _NAME_INDEX:
            resolved_by_name = resolve_company_name(normalize_name(" ".join(phrase)))
            if resolved_by_name:
                symbol = resolved_by_name
    if not symbol:
        phrase_match = ACTION_PHRASE_PATTERN.search(text)
        if phrase_match:
            phrase = _v14_phrase_tokens(phrase_match.group(1), skip_noise=True)
            if _NAME_INDEX and len(phrase) >= 2:
                symbol = resolve_company_name(normalize_name(" ".join(phrase)))
        if not symbol:
            for alias_phrase, canonical in sorted(COMMON_STOCK_ALIASES.items(), key=lambda x: -len(x[0])):
                pat = (
                    r"(?:\b(?:buy|biy|bought|boght|bougth|bot|baught|add|asd|added|aded)\s+"
                    r"(?:more\s+|in\s+)*(?:#\s*)?)" + re.escape(alias_phrase) + r"\b"
                )
                if re.search(pat, text, re.IGNORECASE):
                    symbol = canonical
                    break
        if not symbol:
            m = ACTION_SYMBOL_PATTERN.search(text)
            if m:
                cand = m.group(1).upper()
                if cand not in META_TAGS and cand not in _V14_NOISE_WORDS and cand.lower() not in INDEX_KEYWORDS:
                    symbol = cand
    if VALID_EQUITY_SYMBOLS and symbol not in VALID_EQUITY_SYMBOLS:
        symbol = resolve_company_name(normalize_name(" ".join(phrase))) if phrase else None
    if not symbol:
        return {"route": "EQUITY", "action": "IGNORED", "symbol": None, "price": None}
    if symbol in COMMON_STOCK_ALIASES:
        symbol = COMMON_STOCK_ALIASES[symbol]

    pm = _V14_EQ_PRICE_RE.search(text)
    price = 0.0
    if pm:
        raw_p = pm.group(1).strip(".")
        if raw_p:
            try:
                price = float(raw_p)
                if price < 0:
                    price = 0.0
            except ValueError:
                price = 0.0
    return {"route": "EQUITY", "action": "BUY_ENTRY", "symbol": symbol, "price": price}


# Explicit, machine-readable approved safety/normalization deltas relative to frozen V14 baseline (P1-06).
# Any changed decision NOT matching an entry in `approved_exceptions` is classified as `unverified`
# and recorded in `unexpected_regressions` (never automatically approved).
DEFAULT_APPROVED_REPLAY_EXCEPTIONS: Dict[str, Dict[str, Any]] = {
    "BUY SBIN @12.3.4": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Reject malformed multi-dot decimal that V14 truncated to 12.3",
    },
    "BUY SBIN @-800": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Reject explicit negative price that V14 parsed as +800.0",
    },
    "BUY SBIN at - 800": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-02: Reject space-separated negative price that V14 parsed as +800.0",
    },
    "BUY SBIN CMP - 800": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-02: Reject space-separated negative CMP price that V14 parsed as +800.0",
    },
    "BUY SBIN @0": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Reject explicit @0 price that V14 treated as 0.0 CMP",
    },
    "BUY SBIN @800-abc": {
        "diff_type": "newly_rejected",
        "expected_route": "EQUITY",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Reject malformed range upper bound that V14 truncated to 800.0",
    },
    "BUY SBIN @790-800": {
        "diff_type": "changed_price",
        "expected_route": "EQUITY",
        "expected_action": "BUY_ENTRY",
        "expected_reason_code": DecisionReasonCode.OK.value,
        "expected_symbol": "SBIN",
        "expected_price": 800.0,
        "rationale": "A01: Use upper bound (800.0) of equity entry range instead of lower bound (790.0)",
    },
    "BUY RELIANCE @1,250": {
        "diff_type": "changed_price",
        "expected_route": "EQUITY",
        "expected_action": "BUY_ENTRY",
        "expected_reason_code": DecisionReasonCode.OK.value,
        "expected_symbol": "RELIANCE",
        "expected_price": 1250.0,
        "rationale": "A02: Parse comma-grouped thousand price (1250.0) that V14 truncated at comma (1.0)",
    },
    "BUY POLYCAB 8000 PE OCT SL @30": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.NO_ENTRY_PREMIUM.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Mask SL clause before entry extraction so SL @30 is never mistaken for entry premium",
    },
    "BUY POLYCAB 8000 PE OCT SL: @30": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.NO_ENTRY_PREMIUM.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "Mask SL: @30 clause before entry extraction",
    },
    "Yesterday POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.HISTORICAL_CALLOUT.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "A06: Reject historical/retrospective option callouts",
    },
    "POLYCAB 8000 PE OCTOBAD @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.INVALID_EXPIRY_TOKEN.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "A08: Reject invalid month token OCTOBAD that V14 truncated to OCT",
    },
    "BUY SBIN NOV 800 CE @20": {
        "diff_type": "changed_price",
        "expected_route": "OPTION",
        "expected_action": "BUY_ENTRY",
        "expected_reason_code": DecisionReasonCode.OK.value,
        "expected_symbol": "SBIN",
        "expected_price": 20.0,
        "rationale": "BRAIN-V3-02: Parse pre-strike expiry NOV so symbol is SBIN rather than NOV",
    },
    "BUY SBIN? RELIANCE 800 PE @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.AMBIGUOUS_CONTRACT.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "BRAIN-V3-02: Reject multi-underlying option message",
    },
    "DO NOT BUY POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block negated F&O callout ('DO NOT BUY')",
    },
    "AVOID POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block avoidance F&O callout ('AVOID')",
    },
    "WATCH POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block watchlist F&O callout ('WATCH')",
    },
    "ONLY FOR LEARNING POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block educational F&O callout ('ONLY FOR LEARNING')",
    },
    "NOT A BUY POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block negated F&O callout ('NOT A BUY')",
    },
    "WAIT FOR POLYCAB 8000 PE OCT @100": {
        "diff_type": "newly_rejected",
        "expected_route": "OPTION",
        "expected_action": "BLOCKED",
        "expected_reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
        "expected_symbol": None,
        "expected_price": None,
        "rationale": "P0-01: Block wait/non-entry F&O callout ('WAIT FOR')",
    },
}


def _matches_approved_replay_exception(
    diff_type: str,
    patched_dict: Dict[str, Any],
    spec: Optional[Dict[str, Any]],
) -> bool:
    """
    P1-06: Returns True ONLY if `spec` is an explicit machine-readable exception entry and every
    expected field (`diff_type`, `expected_route`, `expected_action`, `expected_reason_code`,
    `expected_symbol`, `expected_price`) matches `patched_dict`.
    """
    if not spec or not isinstance(spec, dict):
        return False
    if spec.get("diff_type") and spec["diff_type"] != diff_type:
        return False
    if "expected_route" in spec and spec["expected_route"] != patched_dict.get("route"):
        return False
    if "expected_action" in spec and spec["expected_action"] != patched_dict.get("action"):
        return False
    if "expected_reason_code" in spec and spec["expected_reason_code"] != patched_dict.get("reason_code"):
        return False
    if "expected_symbol" in spec and spec["expected_symbol"] != patched_dict.get("symbol"):
        return False
    if "expected_price" in spec:
        exp_p = spec["expected_price"]
        act_p = patched_dict.get("price")
        if exp_p is None and act_p is not None:
            return False
        if exp_p is not None and (act_p is None or abs(float(exp_p) - float(act_p)) > 1e-6):
            return False
    return True


def compare_corpus_replay(
    messages: Iterable[str],
    journal_db: Optional[Any] = None,
    approved_exceptions: Optional[Dict[str, Dict[str, Any]]] = None,
    baseline_fixtures: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Frozen V14 Baseline Replay Comparison Tool (Part 11 Item #7 & P1-06):
    Compares frozen pre-BRAIN V14 parser decisions (from `baseline_fixtures` if supplied, else
    exact commit `579fd1f34b0a` parser logic in `_legacy_baseline_decide`) against `parse_signal_brain`
    across a message corpus without invoking any executor or broker.

    P1-06 Enforcement:
    - Enumerates every changed decision in `changed_decisions`.
    - Classifies a changed decision as `expected_safety_correction` ONLY when `msg` is present in
      `approved_exceptions` (defaults to `DEFAULT_APPROVED_REPLAY_EXCEPTIONS`) AND the patched output
      matches every expected field in that approval spec.
    - Any unapproved change in `newly_accepted`, `newly_rejected`, `changed_route`, or `changed_price`
      is NEVER auto-approved: it is classified as `unverified` and recorded in both `unverified`
      and `unexpected_regressions`.
    """
    eff_approved = DEFAULT_APPROVED_REPLAY_EXCEPTIONS if approved_exceptions is None else approved_exceptions

    newly_accepted: List[Dict[str, Any]] = []
    newly_rejected: List[Dict[str, Any]] = []
    changed_route: List[Dict[str, Any]] = []
    changed_price: List[Dict[str, Any]] = []
    unchanged: List[Dict[str, Any]] = []
    changed_decisions: List[Dict[str, Any]] = []

    expected_safety_corrections: List[Dict[str, Any]] = []
    approved_behavior_preservations: List[Dict[str, Any]] = []
    unexpected_regressions: List[Dict[str, Any]] = []
    unverified_items: List[Dict[str, Any]] = []

    for msg in messages:
        if baseline_fixtures is not None and msg in baseline_fixtures:
            base = dict(baseline_fixtures[msg])
        else:
            base = _legacy_baseline_decide(msg)

        patched = parse_signal_brain(
            msg,
            msg_timestamp=995.0,
            current_time=1000.0,
            journal_db=journal_db,
            enable_ai_fallback=False,
        )
        patched_price: Optional[float] = None
        patched_sym: Optional[str] = None
        if patched.fno_signal is not None:
            patched_price = patched.fno_signal.entry_max
            patched_sym = patched.fno_signal.symbol
        elif patched.equity_signal is not None:
            patched_price = patched.equity_signal.stated_price
            patched_sym = patched.equity_signal.symbol

        patched_dict: Dict[str, Any] = {
            "route": patched.route,
            "action": patched.action.value,
            "reason_code": patched.reason_code,
            "reason": patched.reason,
            "symbol": patched_sym,
            "price": patched_price,
            "price_status": patched.price_status.value,
        }

        entry: Dict[str, Any] = {
            "text": msg,
            "baseline": base,
            "patched": patched_dict,
        }

        base_exec = (base["action"] == "BUY_ENTRY")
        patch_exec = (patched.action == SignalAction.BUY_ENTRY)

        diff_type: Optional[str] = None
        if not base_exec and patch_exec:
            diff_type = "newly_accepted"
            newly_accepted.append(entry)
        elif base_exec and not patch_exec:
            diff_type = "newly_rejected"
            newly_rejected.append(entry)
        elif base["route"] != patched.route:
            diff_type = "changed_route"
            changed_route.append(entry)
        elif base_exec and patch_exec and (base["price"] != patched_price or base["symbol"] != patched_sym):
            diff_type = "changed_price"
            changed_price.append(entry)

        if diff_type is None:
            unchanged.append(entry)
            entry["diff_type"] = "unchanged"
            entry["classification"] = "approved_behavior_preservation"
            approved_behavior_preservations.append(entry)
        else:
            entry["diff_type"] = diff_type
            changed_decisions.append(entry)
            spec = eff_approved.get(msg)
            if spec is not None:
                if _matches_approved_replay_exception(diff_type, patched_dict, spec):
                    entry["classification"] = "expected_safety_correction"
                    entry["approved_rationale"] = spec.get("rationale", "")
                    expected_safety_corrections.append(entry)
                else:
                    entry["classification"] = "unexpected_regression"
                    entry["mismatch_against_approved_spec"] = spec
                    unexpected_regressions.append(entry)
            else:
                entry["classification"] = "unverified"
                unverified_items.append(entry)
                unexpected_regressions.append(entry)

    return {
        "total_messages": len(newly_accepted) + len(newly_rejected) + len(changed_route) + len(changed_price) + len(unchanged),
        "newly_accepted_count": len(newly_accepted),
        "newly_rejected_count": len(newly_rejected),
        "changed_route_count": len(changed_route),
        "changed_price_count": len(changed_price),
        "unchanged_count": len(unchanged),
        "changed_decision_count": len(changed_decisions),
        "newly_accepted": newly_accepted,
        "newly_rejected": newly_rejected,
        "changed_route": changed_route,
        "changed_price": changed_price,
        "changed_decisions": changed_decisions,
        "expected_safety_correction_count": len(expected_safety_corrections),
        "approved_behavior_preservation_count": len(approved_behavior_preservations),
        "unexpected_regression_count": len(unexpected_regressions),
        "unverified_count": len(unverified_items),
        "expected_safety_corrections": expected_safety_corrections,
        "approved_behavior_preservations": approved_behavior_preservations,
        "unexpected_regressions": unexpected_regressions,
        "unverified": unverified_items,
    }
