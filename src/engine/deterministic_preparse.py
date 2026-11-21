"""Optional speculative *deterministic* signal preprocessing.

Only the frozen deterministic REGEX BUY_ENTRY route can be reused. Adaptive
rules, AI fallback, journal writes, dedup, source checks, risk budgets, STOP,
and broker operations remain strictly inside TradingBotPipeline's original
single-authority lock. Candidate is bound to source/message/text/age policy.

Default mode is OFF. Preparing a hint performs no network or DB operations;
no AI fallback is permitted. It can help parallelize CPU-bound REGEX work
ahead of the serialized execution stage, but must be benchmarked before use.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import math
import time
from typing import Any, Optional

from src.models import SignalAction
from src.parsers.brain import parse_signal_brain


def _digest(text: str) -> str:
    return sha256(text.encode("utf-8",errors="strict")).hexdigest()


@dataclass(frozen=True)
class DeterministicPreparse:
    channel_id: int
    message_id: int
    msg_timestamp: float
    max_age_sec: float
    text_hash: str
    decision: Any
    prepared_ns: int

    def usable_for(self, *, text: str, channel_id: int, message_id: int,
                   msg_timestamp: float, max_age_sec: float,
                   injected_clock: Any = None) -> bool:
        if injected_clock is not None:
            return False
        if not isinstance(text, str) or not text.strip():
            return False
        if (self.channel_id != channel_id or self.message_id != message_id or
                self.msg_timestamp != msg_timestamp or
                self.max_age_sec != max_age_sec):
            return False
        if self.text_hash != _digest(text):
            return False
        if not (type(self.prepared_ns) is int and
                0 < self.prepared_ns <= time.perf_counter_ns()):
            return False
        decision = self.decision
        if (getattr(decision, "source", None) != "REGEX" or
                getattr(decision, "action", None) != SignalAction.BUY_ENTRY or
                getattr(decision, "reason_code", "") != "OK"):
            return False
        return (
            (getattr(decision, "route", None) == "EQUITY"
             and getattr(decision, "equity_signal", None) is not None)
            or
            (getattr(decision, "route", None) == "OPTION"
             and getattr(decision, "fno_signal", None) is not None)
        )


def prepare_deterministic_buy(
    *, text: str, channel_id: int, message_id: int,
    msg_timestamp: float, max_age_sec: float,
) -> Optional[DeterministicPreparse]:
    if not isinstance(text, str) or not text.strip():
        return None
    if (type(channel_id) is not int or type(message_id) is not int or
            type(max_age_sec) not in (float, int) or not math.isfinite(max_age_sec)
            or max_age_sec <= 0):
        return None
    if type(msg_timestamp) not in (float, int) or not math.isfinite(msg_timestamp) or msg_timestamp <= 0:
        return None
    # The parsing function itself isolates unexpected failures to a BLOCKED
    # decision; those are NOT reused, so canonical parsing handles the signal.
    decision = parse_signal_brain(
        text=text, channel_id=channel_id, message_id=message_id,
        msg_timestamp=msg_timestamp, max_age_sec=max_age_sec,
        current_time=time.time(), journal_db=None,
        enable_ai_fallback=False,
    )
    prepared = DeterministicPreparse(
        channel_id=channel_id, message_id=message_id,
        msg_timestamp=msg_timestamp, max_age_sec=max_age_sec,
        text_hash=_digest(text), decision=decision,
        prepared_ns=time.perf_counter_ns(),
    )
    return prepared if prepared.usable_for(
        text=text, channel_id=channel_id, message_id=message_id,
        msg_timestamp=msg_timestamp, max_age_sec=max_age_sec,
    ) else None
