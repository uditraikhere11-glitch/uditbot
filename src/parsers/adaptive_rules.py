"""
Audited Adaptive Regex Rule Lifecycle & Bounded Worker Pool for AuditBot BRAIN (Module 2 — v3.1).

Implements the controlled workflow required by the AuditBot BRAIN Architecture & v3.1 Review:
    DRAFT/PROPOSED -> EVALUATED -> APPROVED -> ACTIVE -> ROLLED_BACK / RESTORED_APPROVED

Closes BRAIN-V3-04, BRAIN-V3-05, BRAIN-V3-06, and Part 7 (Immutable Rule Lifecycle):
1. `BRAIN-V3-04` (No silent corpus truncation):
   - Enforces documented corpus limits (`MAX_POSITIVE_CORPUS_SIZE = 100`, `MAX_USER_NEGATIVE_CORPUS_SIZE = 200`).
   - Never silently truncates supplied positive or negative examples: if a corpus exceeds the limit,
     tracks `total_submitted`, `total_evaluated`, `total_failed`, and `total_skipped`, records a failure,
     sets `passed = False`, and blocks rule approval whenever `total_skipped > 0`.
   - Always executes all `MANDATORY_SAFETY_NEGATIVES` first so user-supplied samples can never displace them.
   - Requires expected structured output comparisons (`symbol`, `stated_price`, `strike`, `option_type`, `entry_min`, `entry_max`).
2. `BRAIN-V3-05` (Explicit worker failure classification):
   - Separates regex evaluation outcomes into `RegexEvalOutcome`:
     `MATCH`, `NO_MATCH`, `TIMEOUT`, `WORKER_ERROR`, `INVALID_PATTERN`, `EVALUATION_ERROR`.
   - Never treats `TIMEOUT`, `WORKER_ERROR`, `INVALID_PATTERN`, or `EVALUATION_ERROR` as an ordinary `NO_MATCH`
     during positive or negative corpus evaluation or runtime matching.
3. `BRAIN-V3-06` (Enforceable aggregate execution budget & exec-isolated worker pool):
   - Uses `subprocess.Popen([sys.executable, '-I', '-S', '-u', '-c', ...])` (exec-isolated standard-library
     worker processes, never `fork()` without `exec` in a multithreaded runtime).
   - Bounded warm worker pool (`MAX_IDLE_WORKERS = 4`, `MAX_CONCURRENT_WORKERS = 8` semaphore,
     `MAX_REQUESTS_PER_WORKER = 250`, `MAX_WORKER_AGE_SEC = 300.0`) with `select.select` hard timeout
     and immediate `SIGKILL` + reap on timeout or error.
   - Enforces both a configurable per-rule limit (`SUBPROCESS_TIMEOUT_SEC = 0.25`, 250ms) and an aggregate
     per-message adaptive budget (`AGGREGATE_ADAPTIVE_TIMEOUT_SEC = 0.50`, 500ms across all active rules).
"""

import atexit
from dataclasses import dataclass, field
from enum import Enum
import json
import math
import os
import queue
import re
import select
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from src.models import EquitySignal, FnOSignal, OrderType, SignalAction
from src.parsers.brain_validation import (
    DecisionReasonCode,
    PriceStatus,
    detect_multiple_option_contracts,
    extract_equity_price_bounds_detailed,
    is_historical_callout,
    is_negated_or_non_actionable_callout,
    mask_sl_and_target_clauses,
    normalize_signal_text,
    number_token_appears_in_text,
    parse_clean_number_token,
    strip_clock_times,
    validate_equity_candidate,
    validate_fno_candidate,
)
from src.parsers.equity_parser import (
    COMMON_STOCK_ALIASES,
    INDEX_KEYWORDS,
    META_TAGS,
    NOISE_WORDS,
    filter_equity_message_detailed,
)
from src.parsers.fno_parser import (
    INDEX_UNDERLYINGS,
    looks_like_option,
    is_exit_message,
)

MAX_PATTERN_LENGTH = 240
MAX_RULE_EVAL_MS = 50.0
SUBPROCESS_TIMEOUT_SEC = 0.25
AGGREGATE_ADAPTIVE_TIMEOUT_SEC = 0.50
MAX_POSITIVE_CORPUS_SIZE = 100
MAX_USER_NEGATIVE_CORPUS_SIZE = 200

# 1. Nested quantifiers like (a+)+, (.*)+, ([a-z]+)*
_REDOS_NESTED_QUANTIFIER_RE = re.compile(
    r"\([^()]*[+*][^()]*\)\s*[+*{]"
)

# 2. Quantified alternation groups like (?:a|aa)+, (foo|bar)+, (a|b)*
_REDOS_QUANTIFIED_ALTERNATION_RE = re.compile(
    r"\([^()]*\|[^()]*\)\s*[+*{]"
)

# 3. Backreferences (\1 .. \9) which make regex matching NP-complete
_REDOS_BACKREF_RE = re.compile(r"\\[1-9]")

# 4. Adjacent unbounded wildcard quantifiers (.*.*, .+.+)
_REDOS_ADJACENT_WILDCARD_RE = re.compile(r"\.[+*]\s*\.[+*]")

MANDATORY_SAFETY_NEGATIVES: Tuple[str, ...] = (
    "Exit #SBIN now",
    "Book profit in POLYCAB 8000 PE",
    "Buy #NIFTY @ 24500",
    "#BANKNIFTY 51000 PE OCT @ 300",
    "Yesterday POLYCAB 8000 PE OCT @100",
    "BUY SBIN @12.3.4",
    "BUY SBIN @-800",
    "BUY SBIN @800-abc",
    "BUY SBIN @800 to",
    "BUY SBIN @800 and RELIANCE @1400",
    "POLYCAB 8000 PE OCT / RELIANCE 1500 CE OCT @100",
    "POLYCAB 8000 PE OCT / NOV @100",
    "POLYCAB 8000 PE OCTOBAD @100",
    "POLYCAB 8000 PE OCT SL @30",
)


class RegexEvalOutcome(str, Enum):
    MATCH = "MATCH"
    NO_MATCH = "NO_MATCH"
    TIMEOUT = "TIMEOUT"
    WORKER_ERROR = "WORKER_ERROR"
    INVALID_PATTERN = "INVALID_PATTERN"
    EVALUATION_ERROR = "EVALUATION_ERROR"


class RuleLifecycleState:
    DRAFT = "DRAFT"
    PROPOSED = "PROPOSED"
    EVALUATED = "EVALUATED"
    APPROVED = "APPROVED"
    ACTIVE = "ACTIVE"
    ROLLED_BACK = "ROLLED_BACK"
    REJECTED = "REJECTED"

    VALID_STATES = frozenset({
        "DRAFT", "PROPOSED", "EVALUATED", "APPROVED", "ACTIVE", "ROLLED_BACK", "REJECTED",
    })


def validate_rule_pattern_safety(route: str, pattern: str) -> Tuple[bool, str]:
    """
    Validates that a candidate regex pattern is syntactically valid, within work bounds,
    free of nested quantifiers, quantified alternations (`(?:a|aa)+`), backreferences,
    and defines the required named capture groups.
    """
    clean_route = (route or "").strip().upper()
    if clean_route not in ("EQUITY", "OPTION"):
        return False, f"Invalid route {route!r}; must be 'EQUITY' or 'OPTION'"

    if not pattern or not isinstance(pattern, str):
        return False, "Pattern cannot be empty"
    if len(pattern) > MAX_PATTERN_LENGTH:
        return False, f"Pattern length ({len(pattern)}) exceeds max bound ({MAX_PATTERN_LENGTH})"

    if _REDOS_NESTED_QUANTIFIER_RE.search(pattern):
        return False, "Pattern rejected: nested quantifiers risk catastrophic backtracking (ReDoS)"

    if _REDOS_QUANTIFIED_ALTERNATION_RE.search(pattern):
        return False, "Pattern rejected: quantified alternation group (e.g. (?:a|aa)+) risks exponential backtracking (ReDoS)"

    if _REDOS_BACKREF_RE.search(pattern):
        return False, "Pattern rejected: backreferences are prohibited in adaptive rules"

    if _REDOS_ADJACENT_WILDCARD_RE.search(pattern):
        return False, "Pattern rejected: adjacent unbounded wildcards risk polynomial backtracking"

    try:
        compiled = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        return False, f"Invalid regular expression syntax: {e}"

    groups = set(compiled.groupindex.keys())
    if clean_route == "EQUITY":
        if "sym" not in groups:
            return False, "EQUITY rule pattern must define named group '(?P<sym>...)'"
    else:
        required = {"sym", "strike", "opt", "e1"}
        missing = required - groups
        if missing:
            return False, f"OPTION rule pattern missing required named groups: {sorted(missing)}"

    return True, "Pattern validated"


# Line-delimited JSON worker executed via `subprocess.Popen([sys.executable, '-I', '-S', '-u', '-c', ...])`.
# Uses execve isolation (never unsafe fork without exec in a multithreaded parent).
_PERSISTENT_REGEX_WORKER_CODE = r"""
import json, re, sys

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        pat = req.get("pattern")
        txt = req.get("text")
        if not isinstance(pat, str) or not isinstance(txt, str):
            sys.stdout.write(json.dumps({"status": "EVALUATION_ERROR", "groups": None}) + "\n")
            sys.stdout.flush()
            continue
        try:
            comp = re.compile(pat, re.IGNORECASE)
        except re.error:
            sys.stdout.write(json.dumps({"status": "INVALID_PATTERN", "groups": None}) + "\n")
            sys.stdout.flush()
            continue
        matched = comp.search(txt)
        out = {
            "status": "MATCH" if matched else "NO_MATCH",
            "groups": matched.groupdict() if matched else None,
        }
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()
    except Exception:
        sys.stdout.write(json.dumps({"status": "WORKER_ERROR", "groups": None}) + "\n")
        sys.stdout.flush()
"""


class _IsolatedRegexWorkerHandle:
    """Represents one exec-isolated stdlib Python regex worker process with bounded lifetime."""

    MAX_REQUESTS = 250
    MAX_AGE_SEC = 300.0

    def __init__(self) -> None:
        self.created_at = time.perf_counter()
        self.request_count = 0
        self.proc = subprocess.Popen(
            [sys.executable, "-I", "-S", "-u", "-c", _PERSISTENT_REGEX_WORKER_CODE],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
            close_fds=True,
            env={"PYTHONNOUSERSITE": "1"},
        )

    def is_healthy_and_within_lifetime(self) -> bool:
        if self.proc.poll() is not None:
            return False
        if self.request_count >= self.MAX_REQUESTS:
            return False
        if (time.perf_counter() - self.created_at) >= self.MAX_AGE_SEC:
            return False
        return True

    def terminate_and_reap(self) -> None:
        try:
            if self.proc.stdin:
                try:
                    self.proc.stdin.close()
                except Exception:
                    pass
            if self.proc.stdout:
                try:
                    self.proc.stdout.close()
                except Exception:
                    pass
            if self.proc.poll() is None:
                self.proc.kill()
            try:
                self.proc.wait(timeout=0.2)
            except (OSError, subprocess.TimeoutExpired):
                pass
        except Exception:
            pass


class _BoundedRegexWorkerPool:
    """
    Thread-safe bounded pool of exec-isolated regex workers (`BRAIN-V3-06`).
    - Prevents uncontrolled process accumulation via `_concurrency_sem` (`max_concurrent=8`).
    - Keeps up to `max_idle=4` warm workers for <0.5ms normal matching while preserving
      immediate `SIGKILL` termination on any per-rule or aggregate timeout.
    """

    def __init__(self, max_idle: int = 4, max_concurrent: int = 8) -> None:
        self._idle: "queue.Queue[_IsolatedRegexWorkerHandle]" = queue.Queue(maxsize=max_idle)
        self._sem = threading.BoundedSemaphore(max_concurrent)
        self._closed = False

    def execute(
        self,
        pattern: str,
        text: str,
        timeout_sec: float,
    ) -> Tuple[str, Optional[Dict[str, Optional[str]]], float]:
        t0 = time.perf_counter()
        deadline = t0 + float(timeout_sec)

        rem_for_sem = max(0.001, deadline - time.perf_counter())
        if not self._sem.acquire(timeout=rem_for_sem):
            return RegexEvalOutcome.TIMEOUT.value, None, (time.perf_counter() - t0) * 1000.0

        worker: Optional[_IsolatedRegexWorkerHandle] = None
        reusable = False
        try:
            while True:
                try:
                    cand = self._idle.get_nowait()
                except queue.Empty:
                    break
                if cand.is_healthy_and_within_lifetime() and not self._closed:
                    worker = cand
                    break
                cand.terminate_and_reap()

            if worker is None:
                if (deadline - time.perf_counter()) <= 0.001:
                    return RegexEvalOutcome.TIMEOUT.value, None, (time.perf_counter() - t0) * 1000.0
                worker = _IsolatedRegexWorkerHandle()

            if worker.proc.stdin is None or worker.proc.stdout is None:
                return RegexEvalOutcome.WORKER_ERROR.value, None, (time.perf_counter() - t0) * 1000.0

            payload = json.dumps({"pattern": pattern, "text": text}) + "\n"
            worker.proc.stdin.write(payload)
            worker.proc.stdin.flush()
            worker.request_count += 1

            rem = deadline - time.perf_counter()
            if rem <= 0.0:
                return RegexEvalOutcome.TIMEOUT.value, None, (time.perf_counter() - t0) * 1000.0

            rlist, _, _ = select.select([worker.proc.stdout], [], [], rem)
            if not rlist:
                # Worker timed out; kill immediately and do NOT return to pool
                return RegexEvalOutcome.TIMEOUT.value, None, (time.perf_counter() - t0) * 1000.0

            line = worker.proc.stdout.readline()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if elapsed_ms > (timeout_sec * 1000.0) + 15.0:
                return RegexEvalOutcome.TIMEOUT.value, None, elapsed_ms

            if not line:
                return RegexEvalOutcome.WORKER_ERROR.value, None, elapsed_ms

            resp = json.loads(line)
            status = resp.get("status")
            if status not in (
                RegexEvalOutcome.MATCH.value,
                RegexEvalOutcome.NO_MATCH.value,
                RegexEvalOutcome.INVALID_PATTERN.value,
                RegexEvalOutcome.WORKER_ERROR.value,
                RegexEvalOutcome.EVALUATION_ERROR.value,
            ):
                return RegexEvalOutcome.WORKER_ERROR.value, None, elapsed_ms

            groups = resp.get("groups")
            if groups is not None and not isinstance(groups, dict):
                return RegexEvalOutcome.WORKER_ERROR.value, None, elapsed_ms

            if status in (RegexEvalOutcome.MATCH.value, RegexEvalOutcome.NO_MATCH.value):
                reusable = True
            return status, groups, elapsed_ms

        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return RegexEvalOutcome.WORKER_ERROR.value, None, (time.perf_counter() - t0) * 1000.0
        finally:
            if worker is not None:
                if reusable and not self._closed and worker.is_healthy_and_within_lifetime():
                    try:
                        self._idle.put_nowait(worker)
                    except queue.Full:
                        worker.terminate_and_reap()
                else:
                    worker.terminate_and_reap()
            self._sem.release()

    def shutdown(self) -> None:
        self._closed = True
        while True:
            try:
                w = self._idle.get_nowait()
                w.terminate_and_reap()
            except queue.Empty:
                break


_GLOBAL_REGEX_POOL = _BoundedRegexWorkerPool(max_idle=4, max_concurrent=8)
atexit.register(_GLOBAL_REGEX_POOL.shutdown)


def shutdown_regex_worker_pool() -> None:
    """Explicitly terminates all warm idle regex workers."""
    _GLOBAL_REGEX_POOL.shutdown()


def safe_regex_search_bounded(
    pattern: str,
    text: str,
    timeout_sec: float = SUBPROCESS_TIMEOUT_SEC,
    use_subprocess: bool = True,
) -> Tuple[str, Optional[Dict[str, Optional[str]]], float]:
    """
    Matches untrusted adaptive patterns in an exec-isolated standard-library worker process
    with explicit outcome classification (`BRAIN-V3-05` & `BRAIN-V3-06`):
      - `MATCH`
      - `NO_MATCH`
      - `TIMEOUT`
      - `WORKER_ERROR`
      - `INVALID_PATTERN`
      - `EVALUATION_ERROR`
    """
    started = time.perf_counter()
    if not isinstance(pattern, str) or not pattern or len(pattern) > MAX_PATTERN_LENGTH:
        return RegexEvalOutcome.INVALID_PATTERN.value, None, (time.perf_counter() - started) * 1000.0
    if not isinstance(text, str) or len(text) > 300:
        return RegexEvalOutcome.EVALUATION_ERROR.value, None, (time.perf_counter() - started) * 1000.0
    if (
        isinstance(timeout_sec, bool)
        or not isinstance(timeout_sec, (float, int))
        or not math.isfinite(float(timeout_sec))
        or float(timeout_sec) <= 0.0
    ):
        return RegexEvalOutcome.EVALUATION_ERROR.value, None, (time.perf_counter() - started) * 1000.0

    try:
        re.compile(pattern, re.IGNORECASE)
    except re.error:
        return RegexEvalOutcome.INVALID_PATTERN.value, None, (time.perf_counter() - started) * 1000.0

    return _GLOBAL_REGEX_POOL.execute(pattern=pattern, text=text, timeout_sec=float(timeout_sec))


def _normalize_positive_sample(
    route: str,
    item: Union[str, Dict[str, Any]],
) -> Tuple[bool, str, Dict[str, Any], str]:
    """
    Normalizes a positive corpus entry into `(is_valid, text, expected_dict, error_reason)`.
    Requires expected structured output fields so rules cannot pass on mere non-null matches (`BRAIN-V3-04`).
    """
    clean_route = (route or "").strip().upper()
    if isinstance(item, dict):
        txt = str(item.get("text") or "").strip()
        exp = item.get("expected")
        if not txt or not isinstance(exp, dict) or not exp:
            return False, txt, {}, "Positive corpus dict must include non-empty 'text' and 'expected' dict"
        if clean_route == "EQUITY":
            if "symbol" not in exp or "stated_price" not in exp:
                return False, txt, {}, "EQUITY positive expected dict must specify 'symbol' and 'stated_price'"
        else:
            req_keys = {"symbol", "strike", "option_type", "entry_min", "entry_max"}
            if not req_keys.issubset(set(exp.keys())):
                return False, txt, {}, f"OPTION positive expected dict must specify {sorted(req_keys)}"
        return True, txt, exp, "OK"

    return False, str(item or ""), {}, "Positive corpus items must be dicts with 'text' and 'expected' structured output fields"


class AdaptiveRuleManager:
    """
    Manages the lifecycle of adaptive regex rules backed by additive tables (`parser_rule_candidates`,
    `parser_rule_versions`, `parser_rule_reviews`) in `JournalDB`.
    """

    def __init__(self, journal_db: Any):
        self.db = journal_db

    def propose_rule(
        self,
        rule_id: str,
        route: str,
        pattern: str,
        description: str = "",
        proposed_by: str = "operator",
        sample_positives: Optional[List[Union[str, Dict[str, Any]]]] = None,
        sample_negatives: Optional[List[str]] = None,
        initial_state: str = RuleLifecycleState.PROPOSED,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        clean_id = (rule_id or "").strip().upper()
        if not clean_id or not re.match(r"^[A-Z0-9_\-]{3,40}$", clean_id):
            return False, "Invalid rule_id (must be 3-40 chars of [A-Z0-9_-])", None

        clean_route = (route or "").strip().upper()
        ok, reason = validate_rule_pattern_safety(clean_route, pattern)
        if not ok:
            return False, reason, None

        init_st = initial_state.strip().upper() if isinstance(initial_state, str) else RuleLifecycleState.PROPOSED
        if init_st not in (RuleLifecycleState.DRAFT, RuleLifecycleState.PROPOSED):
            return False, f"Invalid initial proposal state {initial_state!r} (must be DRAFT or PROPOSED)", None

        pos_list = list(sample_positives or [])
        neg_list = list(sample_negatives or [])
        if len(pos_list) > MAX_POSITIVE_CORPUS_SIZE:
            return (
                False,
                f"Positive corpus size ({len(pos_list)}) exceeds max limit ({MAX_POSITIVE_CORPUS_SIZE})",
                None,
            )
        if len(neg_list) > MAX_USER_NEGATIVE_CORPUS_SIZE:
            return (
                False,
                f"User negative corpus size ({len(neg_list)}) exceeds max limit ({MAX_USER_NEGATIVE_CORPUS_SIZE})",
                None,
            )

        rec = self.db.create_or_update_parser_rule(
            rule_id=clean_id,
            route=clean_route,
            pattern=pattern,
            description=description,
            proposed_by=proposed_by,
            sample_positives=pos_list,
            sample_negatives=neg_list,
        )
        return True, f"Rule {clean_id} (v{rec['version']}) recorded in state {rec['state']}", rec

    def evaluate_rule(
        self,
        rule_id: str,
        positive_corpus: Optional[List[Union[str, Dict[str, Any]]]] = None,
        negative_corpus: Optional[List[str]] = None,
        reviewer: str = "evaluator",
        timeout_sec: float = SUBPROCESS_TIMEOUT_SEC,
        max_positive_corpus: int = MAX_POSITIVE_CORPUS_SIZE,
        max_negative_corpus: int = MAX_USER_NEGATIVE_CORPUS_SIZE,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Evaluates a candidate rule against a positive/negative corpus in an exec-isolated worker
        with mandatory structured output checks, mandatory safety negatives, explicit worker failure
        classification (`BRAIN-V3-05`), and zero silent truncation (`BRAIN-V3-04`).
        """
        rule = self.db.get_parser_rule(rule_id)
        if not rule:
            return False, f"Rule {rule_id!r} not found", {}

        route = rule["route"]
        pattern = rule["pattern"]
        ok, reason = validate_rule_pattern_safety(route, pattern)
        if not ok:
            return False, reason, {"outcome_counts": {RegexEvalOutcome.INVALID_PATTERN.value: 1}}

        raw_positives = list(positive_corpus if positive_corpus is not None else rule.get("sample_positives", []))
        user_negatives = list(negative_corpus if negative_corpus is not None else rule.get("sample_negatives", []))

        pos_submitted = len(raw_positives)
        mandatory_submitted = len(MANDATORY_SAFETY_NEGATIVES)
        user_neg_submitted = len(user_negatives)

        pos_to_eval = raw_positives[:max_positive_corpus]
        pos_skipped = max(0, pos_submitted - len(pos_to_eval))

        user_neg_to_eval = user_negatives[:max_negative_corpus]
        user_neg_skipped = max(0, user_neg_submitted - len(user_neg_to_eval))

        pos_evaluated = 0
        pos_matched = 0
        pos_failures: List[str] = []
        neg_false_positives: List[str] = []
        infrastructure_errors: List[str] = []
        outcome_counts: Dict[str, int] = {
            RegexEvalOutcome.MATCH.value: 0,
            RegexEvalOutcome.NO_MATCH.value: 0,
            RegexEvalOutcome.TIMEOUT.value: 0,
            RegexEvalOutcome.WORKER_ERROR.value: 0,
            RegexEvalOutcome.INVALID_PATTERN.value: 0,
            RegexEvalOutcome.EVALUATION_ERROR.value: 0,
        }
        max_elapsed_ms = 0.0

        if pos_skipped > 0:
            pos_failures.append(
                f"Positive corpus size ({pos_submitted}) exceeds max limit ({max_positive_corpus}); {pos_skipped} positive case(s) skipped (silent truncation prohibited)"
            )
        if user_neg_skipped > 0:
            neg_false_positives.append(
                f"User negative corpus size ({user_neg_submitted}) exceeds max limit ({max_negative_corpus}); {user_neg_skipped} negative case(s) skipped"
            )

        # 1. Evaluate Positive Corpus (verifying expected structured outputs!)
        for item in pos_to_eval:
            pos_evaluated += 1
            norm_ok, sample_txt, expected_dict, norm_err = _normalize_positive_sample(route, item)
            if not norm_ok:
                outcome_counts[RegexEvalOutcome.EVALUATION_ERROR.value] += 1
                infrastructure_errors.append(f"EVALUATION_ERROR on positive item {item!r}: {norm_err}")
                pos_failures.append(f"Invalid positive item {item!r}: {norm_err}")
                continue

            sig, match_reason, elapsed_ms, ev = self._apply_single_rule_with_evidence(
                route=route,
                pattern=pattern,
                text=sample_txt,
                now=1000.0,
                msg_ts=995.0,
                timeout_sec=timeout_sec,
                use_subprocess=True,
            )
            max_elapsed_ms = max(max_elapsed_ms, elapsed_ms)
            w_outcome = ev.get("worker_outcome", RegexEvalOutcome.MATCH.value if sig is not None else RegexEvalOutcome.NO_MATCH.value)
            outcome_counts[w_outcome] = outcome_counts.get(w_outcome, 0) + 1

            if w_outcome in (
                RegexEvalOutcome.TIMEOUT.value,
                RegexEvalOutcome.WORKER_ERROR.value,
                RegexEvalOutcome.INVALID_PATTERN.value,
                RegexEvalOutcome.EVALUATION_ERROR.value,
            ):
                infrastructure_errors.append(f"{w_outcome} on positive sample {sample_txt!r}: {match_reason}")
                pos_failures.append(f"{w_outcome} on positive sample {sample_txt!r}: {match_reason}")
                continue

            if sig is None:
                pos_failures.append(f"Did not match {sample_txt!r}: {match_reason}")
                continue

            mismatch = self._check_expected_output(route, sig, expected_dict)
            if mismatch:
                pos_failures.append(f"Structured output mismatch on {sample_txt!r}: {mismatch}")
            else:
                pos_matched += 1

        # 2. ALWAYS evaluate Mandatory Safety Negatives FIRST (independent of user negative count!)
        mandatory_evaluated = 0
        for safety_neg in MANDATORY_SAFETY_NEGATIVES:
            mandatory_evaluated += 1
            sig, match_reason, elapsed_ms, ev = self._apply_single_rule_with_evidence(
                route=route,
                pattern=pattern,
                text=safety_neg,
                now=1000.0,
                msg_ts=995.0,
                timeout_sec=timeout_sec,
                use_subprocess=True,
            )
            max_elapsed_ms = max(max_elapsed_ms, elapsed_ms)
            w_outcome = ev.get("worker_outcome", RegexEvalOutcome.MATCH.value if sig is not None else RegexEvalOutcome.NO_MATCH.value)
            outcome_counts[w_outcome] = outcome_counts.get(w_outcome, 0) + 1

            if w_outcome in (
                RegexEvalOutcome.TIMEOUT.value,
                RegexEvalOutcome.WORKER_ERROR.value,
                RegexEvalOutcome.INVALID_PATTERN.value,
                RegexEvalOutcome.EVALUATION_ERROR.value,
            ):
                err_msg = f"{w_outcome} on mandatory safety negative: {safety_neg!r} ({match_reason})"
                infrastructure_errors.append(err_msg)
                neg_false_positives.append(err_msg)
            elif sig is not None:
                neg_false_positives.append(f"Matched mandatory safety negative: {safety_neg!r}")

        # 3. Evaluate User Negatives
        user_neg_evaluated = 0
        for user_neg in user_neg_to_eval:
            user_neg_evaluated += 1
            sig, match_reason, elapsed_ms, ev = self._apply_single_rule_with_evidence(
                route=route,
                pattern=pattern,
                text=user_neg,
                now=1000.0,
                msg_ts=995.0,
                timeout_sec=timeout_sec,
                use_subprocess=True,
            )
            max_elapsed_ms = max(max_elapsed_ms, elapsed_ms)
            w_outcome = ev.get("worker_outcome", RegexEvalOutcome.MATCH.value if sig is not None else RegexEvalOutcome.NO_MATCH.value)
            outcome_counts[w_outcome] = outcome_counts.get(w_outcome, 0) + 1

            if w_outcome in (
                RegexEvalOutcome.TIMEOUT.value,
                RegexEvalOutcome.WORKER_ERROR.value,
                RegexEvalOutcome.INVALID_PATTERN.value,
                RegexEvalOutcome.EVALUATION_ERROR.value,
            ):
                err_msg = f"{w_outcome} on user negative: {user_neg!r} ({match_reason})"
                infrastructure_errors.append(err_msg)
                neg_false_positives.append(err_msg)
            elif sig is not None:
                neg_false_positives.append(f"Matched user negative: {user_neg!r}")

        total_submitted = pos_submitted + mandatory_submitted + user_neg_submitted
        total_evaluated = pos_evaluated + mandatory_evaluated + user_neg_evaluated
        total_skipped = pos_skipped + (mandatory_submitted - mandatory_evaluated) + user_neg_skipped
        total_failed = len(pos_failures) + len(neg_false_positives)

        passed = (
            pos_submitted > 0
            and total_skipped == 0
            and pos_skipped == 0
            and pos_matched == pos_submitted
            and len(pos_failures) == 0
            and len(neg_false_positives) == 0
            and len(infrastructure_errors) == 0
            and mandatory_evaluated == len(MANDATORY_SAFETY_NEGATIVES)
            and max_elapsed_ms <= (timeout_sec * 1000.0)
        )

        summary = {
            "passed": passed,
            "total_submitted": total_submitted,
            "total_evaluated": total_evaluated,
            "total_failed": total_failed,
            "total_skipped": total_skipped,
            "positive_submitted": pos_submitted,
            "positive_total": pos_submitted,
            "positive_evaluated": pos_evaluated,
            "positive_matched": pos_matched,
            "positive_failed": len(pos_failures),
            "positive_skipped": pos_skipped,
            "positive_failures": pos_failures,
            "mandatory_negatives_submitted": mandatory_submitted,
            "mandatory_negatives_checked": mandatory_evaluated,
            "user_negatives_submitted": user_neg_submitted,
            "user_negatives_checked": user_neg_evaluated,
            "user_negatives_skipped": user_neg_skipped,
            "negative_false_positives": neg_false_positives,
            "infrastructure_errors": infrastructure_errors,
            "infrastructure_error_count": len(infrastructure_errors),
            "outcome_counts": outcome_counts,
            "max_elapsed_ms": round(max_elapsed_ms, 3),
            "evaluated_at": time.time(),
        }

        new_state = RuleLifecycleState.EVALUATED if passed else RuleLifecycleState.PROPOSED
        self.db.update_parser_rule_state(
            rule_id=rule["rule_id"],
            new_state=new_state,
            actor=reviewer,
            notes="Corpus evaluation passed" if passed else "Corpus evaluation failed",
            eval_summary=summary,
        )
        return passed, ("Evaluation passed" if passed else "Evaluation failed"), summary

    @staticmethod
    def _check_expected_output(route: str, sig: Any, expected: Dict[str, Any]) -> Optional[str]:
        if route.upper() == "EQUITY":
            if "symbol" in expected and sig.symbol != str(expected["symbol"]).strip().upper():
                return f"symbol {sig.symbol!r} != expected {expected['symbol']!r}"
            if "stated_price" in expected and abs(float(sig.stated_price) - float(expected["stated_price"])) > 1e-6:
                return f"stated_price {sig.stated_price} != expected {expected['stated_price']}"
        else:
            if "symbol" in expected and sig.symbol != str(expected["symbol"]).strip().upper():
                return f"symbol {sig.symbol!r} != expected {expected['symbol']!r}"
            if "strike" in expected and abs(float(sig.strike) - float(expected["strike"])) > 1e-6:
                return f"strike {sig.strike} != expected {expected['strike']}"
            if "option_type" in expected and sig.option_type != str(expected["option_type"]).strip().upper():
                return f"option_type {sig.option_type!r} != expected {expected['option_type']!r}"
            if "entry_min" in expected and abs(float(sig.entry_min) - float(expected["entry_min"])) > 1e-6:
                return f"entry_min {sig.entry_min} != expected {expected['entry_min']}"
            if "entry_max" in expected and abs(float(sig.entry_max) - float(expected["entry_max"])) > 1e-6:
                return f"entry_max {sig.entry_max} != expected {expected['entry_max']}"
            if "expiry_month" in expected and sig.expiry_month != expected["expiry_month"]:
                return f"expiry_month {sig.expiry_month!r} != expected {expected['expiry_month']!r}"
            if "expiry_day" in expected and sig.expiry_day != expected["expiry_day"]:
                return f"expiry_day {sig.expiry_day!r} != expected {expected['expiry_day']!r}"
        return None

    def approve_rule(self, rule_id: str, reviewer: str, notes: str = "") -> Tuple[bool, str]:
        rule = self.db.get_parser_rule(rule_id)
        if not rule:
            return False, f"Rule {rule_id!r} not found"
        if rule["state"] not in (RuleLifecycleState.EVALUATED, RuleLifecycleState.APPROVED):
            return False, f"Rule {rule_id} must be in EVALUATED state before approval (current: {rule['state']})"
        eval_sum = rule.get("eval_summary") or {}
        if not eval_sum.get("passed"):
            return False, f"Rule {rule_id} has not passed bounded corpus evaluation"
        if int(eval_sum.get("total_skipped") or 0) > 0 or int(eval_sum.get("positive_skipped") or 0) > 0:
            return False, f"Rule {rule_id} approval blocked: evaluation skipped cases ({eval_sum.get('total_skipped')})"
        if int(eval_sum.get("infrastructure_error_count") or 0) > 0 or eval_sum.get("infrastructure_errors"):
            return False, f"Rule {rule_id} approval blocked: evaluation encountered worker/infrastructure errors"
        if int(eval_sum.get("mandatory_negatives_checked", len(MANDATORY_SAFETY_NEGATIVES))) < len(MANDATORY_SAFETY_NEGATIVES):
            return False, f"Rule {rule_id} approval blocked: mandatory safety negatives were not completely evaluated"

        self.db.update_parser_rule_state(
            rule_id=rule["rule_id"],
            new_state=RuleLifecycleState.APPROVED,
            actor=reviewer,
            notes=notes or "Explicitly approved by reviewer",
        )
        return True, f"Rule {rule_id} (v{rule['version']}) approved by {reviewer}"

    def activate_rule(self, rule_id: str, actor: str, notes: str = "") -> Tuple[bool, str]:
        rule = self.db.get_parser_rule(rule_id)
        if not rule:
            return False, f"Rule {rule_id!r} not found"
        if rule["state"] != RuleLifecycleState.APPROVED:
            return False, f"Rule {rule_id} must be in APPROVED state before activation (current: {rule['state']})"
        eval_sum = rule.get("eval_summary") or {}
        if not eval_sum.get("passed") or not rule.get("approved_by"):
            return False, f"Rule {rule_id} cannot be activated without a passing evaluation and reviewer approval"

        ok = self.db.update_parser_rule_state(
            rule_id=rule["rule_id"],
            new_state=RuleLifecycleState.ACTIVE,
            actor=actor,
            notes=notes or f"Activated v{rule['version']} (applies to future messages only; zero auto-replay)",
        )
        if not ok:
            return False, f"Database rejected activation for rule {rule_id}"
        return True, f"Rule {rule_id} (v{rule['version']}) is now ACTIVE"

    def deactivate_rule(self, rule_id: str, actor: str, reason: str = "") -> Tuple[bool, str]:
        """
        Deactivates a rule by transitioning its state to `ROLLED_BACK` (disabled).
        To restore an earlier approved version, use `restore_rule_version()`.
        """
        rule = self.db.get_parser_rule(rule_id)
        if not rule:
            return False, f"Rule {rule_id!r} not found"
        self.db.update_parser_rule_state(
            rule_id=rule["rule_id"],
            new_state=RuleLifecycleState.ROLLED_BACK,
            actor=actor,
            notes=reason or "Operator deactivated rule (ROLLED_BACK)",
        )
        return True, f"Rule {rule_id} (v{rule['version']}) deactivated to ROLLED_BACK"

    def rollback_rule(self, rule_id: str, actor: str, reason: str = "") -> Tuple[bool, str]:
        """Alias for `deactivate_rule` preserving CLI compatibility."""
        return self.deactivate_rule(rule_id, actor=actor, reason=reason)

    def restore_rule_version(
        self,
        rule_id: str,
        target_version: int,
        actor: str,
        activate: bool = False,
        notes: str = "",
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Restores to APPROVED only. Activation is a separate explicit action."""
        if activate:
            return False, "Restoration cannot activate: restore first, then explicitly activate", None
        return self.db.restore_parser_rule_version(
            rule_id=rule_id,
            target_version=target_version,
            actor=actor,
            activate=False,
            notes=notes,
        )

    def match_active_rules(
        self,
        route: str,
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        msg_timestamp: float = 0.0,
        max_age_sec: float = 60.0,
        current_time: Optional[float] = None,
        per_rule_timeout_sec: float = SUBPROCESS_TIMEOUT_SEC,
        aggregate_timeout_sec: float = AGGREGATE_ADAPTIVE_TIMEOUT_SEC,
    ) -> Tuple[Optional[Any], Optional[str], str, Dict[str, Any]]:
        """
        Evaluates only `ACTIVE` adaptive rules for `route` within both a per-rule timeout
        (`per_rule_timeout_sec`, default 250ms) AND an enforceable aggregate time budget
        (`aggregate_timeout_sec`, default 500ms across all active rules, `BRAIN-V3-06`).
        Routes any match through the unified final validator (`validate_equity_candidate` / `validate_fno_candidate`).
        Returns `(signal_or_None, matched_rule_id_or_None, reason, evidence_dict)`.
        """
        if not self.db or not hasattr(self.db, "list_parser_rules"):
            return None, None, "No adaptive rule store", {}
        active_rules = self.db.list_parser_rules(route=route, state=RuleLifecycleState.ACTIVE)
        if not active_rules:
            return None, None, "No active adaptive rules", {}

        now = current_time if current_time is not None else time.time()
        msg_ts = msg_timestamp or now
        remaining_age = max(0.0, float(max_age_sec) - (now - msg_ts))
        eff_agg_budget = min(float(aggregate_timeout_sec), remaining_age) if remaining_age > 0 else 0.0
        if eff_agg_budget <= 0.0:
            return None, None, "Message aged past max_age_sec before adaptive evaluation", {
                "worker_outcome": RegexEvalOutcome.TIMEOUT.value,
                "reason_code": DecisionReasonCode.MESSAGE_TOO_OLD.value,
            }

        agg_start = time.perf_counter()
        agg_deadline = agg_start + eff_agg_budget
        last_error_outcome: Optional[str] = None
        last_error_rule: Optional[str] = None
        last_policy_rejection: Optional[Tuple[str, str, Dict[str, Any]]] = None

        for r in active_rules:
            rem_agg = agg_deadline - time.perf_counter()
            if rem_agg <= 0.002:
                return (
                    None,
                    f"{r['rule_id']}@v{r['version']}",
                    f"Aggregate adaptive rule execution budget ({aggregate_timeout_sec * 1000.0:.0f}ms) exhausted",
                    {
                        "worker_outcome": RegexEvalOutcome.TIMEOUT.value,
                        "reason_code": DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value,
                        "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
                    },
                )

            rule_timeout = min(float(per_rule_timeout_sec), rem_agg)
            rule_tag = f"{r['rule_id']}@v{r['version']}"
            try:
                sig, reason, _, evidence = self._apply_single_rule_with_evidence(
                    route=route,
                    pattern=r["pattern"],
                    text=text,
                    channel_id=channel_id,
                    message_id=message_id,
                    now=now,
                    msg_ts=msg_ts,
                    max_age_sec=max_age_sec,
                    timeout_sec=rule_timeout,
                    use_subprocess=True,
                )
                if sig is not None:
                    return sig, rule_tag, reason, evidence

                w_out = evidence.get("worker_outcome")
                if w_out == RegexEvalOutcome.TIMEOUT.value:
                    return (
                        None,
                        rule_tag,
                        f"Adaptive rule execution failure ({w_out}) on {rule_tag}",
                        {
                            "worker_outcome": RegexEvalOutcome.TIMEOUT.value,
                            "reason_code": DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value,
                            "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
                        },
                    )
                if w_out in (
                    RegexEvalOutcome.WORKER_ERROR.value,
                    RegexEvalOutcome.INVALID_PATTERN.value,
                    RegexEvalOutcome.EVALUATION_ERROR.value,
                ):
                    return (
                        None,
                        rule_tag,
                        f"Adaptive rule execution failure ({w_out}) on {rule_tag}",
                        {
                            "worker_outcome": w_out,
                            "reason_code": DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value,
                            "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
                        },
                    )
                if evidence.get("policy_rejected"):
                    last_policy_rejection = (rule_tag, reason, evidence)
            except Exception as exc:
                return (
                    None,
                    rule_tag,
                    f"Adaptive rule execution exception on {rule_tag}: {exc}",
                    {
                        "worker_outcome": RegexEvalOutcome.WORKER_ERROR.value,
                        "reason_code": DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value,
                        "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
                    },
                )

        if last_policy_rejection is not None:
            r_tag, r_reason, r_ev = last_policy_rejection
            return None, r_tag, r_reason, r_ev

        if last_error_outcome is not None:
            r_code = (
                DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value
                if last_error_outcome == RegexEvalOutcome.TIMEOUT.value
                else DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value
            )
            return (
                None,
                last_error_rule,
                f"Adaptive rule execution failure ({last_error_outcome}) on {last_error_rule}",
                {
                    "worker_outcome": last_error_outcome,
                    "reason_code": r_code,
                    "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
                },
            )

        return None, None, "No active adaptive rule matched", {
            "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
            "aggregate_elapsed_ms": round((time.perf_counter() - agg_start) * 1000.0, 3),
        }

    def _apply_single_rule(
        self,
        route: str,
        compiled: Union[re.Pattern, str],
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        now: float = 0.0,
        msg_ts: float = 0.0,
        max_age_sec: float = 60.0,
    ) -> Tuple[Optional[Any], str]:
        pat_str = compiled.pattern if hasattr(compiled, "pattern") else str(compiled)
        sig, reason, _, _ = self._apply_single_rule_with_evidence(
            route=route,
            pattern=pat_str,
            text=text,
            channel_id=channel_id,
            message_id=message_id,
            now=now,
            msg_ts=msg_ts,
            max_age_sec=max_age_sec,
            timeout_sec=SUBPROCESS_TIMEOUT_SEC,
            use_subprocess=True,
        )
        return sig, reason

    def _apply_single_rule_isolated(
        self,
        route: str,
        pattern: str,
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        now: float = 0.0,
        msg_ts: float = 0.0,
        max_age_sec: float = 60.0,
        timeout_sec: float = SUBPROCESS_TIMEOUT_SEC,
        use_subprocess: bool = True,
    ) -> Tuple[Optional[Any], str, float]:
        sig, reason, elapsed_ms, _ = self._apply_single_rule_with_evidence(
            route=route,
            pattern=pattern,
            text=text,
            channel_id=channel_id,
            message_id=message_id,
            now=now,
            msg_ts=msg_ts,
            max_age_sec=max_age_sec,
            timeout_sec=timeout_sec,
            use_subprocess=use_subprocess,
        )
        return sig, reason, elapsed_ms

    def _apply_single_rule_with_evidence(
        self,
        route: str,
        pattern: str,
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        now: float = 0.0,
        msg_ts: float = 0.0,
        max_age_sec: float = 60.0,
        timeout_sec: float = SUBPROCESS_TIMEOUT_SEC,
        use_subprocess: bool = False,
    ) -> Tuple[Optional[Any], str, float, Dict[str, Any]]:
        if not text or not isinstance(text, str) or len(text) > 300:
            return None, "Invalid or oversized text", 0.0, {"worker_outcome": RegexEvalOutcome.EVALUATION_ERROR.value}

        clean_route = route.upper()
        # Execute bounded regex worker FIRST (or after deterministic preflight) and classify worker outcome
        cleaned_for_regex = strip_clock_times(normalize_signal_text(text))
        raw_status, gd, elapsed_ms = safe_regex_search_bounded(
            pattern=pattern,
            text=cleaned_for_regex,
            timeout_sec=timeout_sec,
            use_subprocess=use_subprocess,
        )
        # Normalize any legacy 'ERROR' return value (e.g. from test mocks) to WORKER_ERROR
        status = RegexEvalOutcome.WORKER_ERROR.value if raw_status == "ERROR" else raw_status

        if status == RegexEvalOutcome.TIMEOUT.value:
            return None, "REGEX_TIMEOUT", elapsed_ms, {
                "worker_outcome": RegexEvalOutcome.TIMEOUT.value,
                "reason_code": DecisionReasonCode.ADAPTIVE_RULE_TIMEOUT.value,
            }
        if status in (
            RegexEvalOutcome.WORKER_ERROR.value,
            RegexEvalOutcome.INVALID_PATTERN.value,
            RegexEvalOutcome.EVALUATION_ERROR.value,
        ):
            return None, f"REGEX_{status}", elapsed_ms, {
                "worker_outcome": status,
                "reason_code": DecisionReasonCode.ADAPTIVE_WORKER_ERROR.value,
            }
        if status != RegexEvalOutcome.MATCH.value or not gd:
            return None, "Pattern did not match", elapsed_ms, {
                "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
            }

        # Pattern matched (`status == MATCH`); now enforce route-specific deterministic filters and unified validator
        if clean_route == "EQUITY":
            passed, reason, f_act, f_code = filter_equity_message_detailed(
                text, msg_timestamp=msg_ts, max_age_sec=max_age_sec, current_time=now
            )
            if not passed:
                return None, reason, elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": (f_act == SignalAction.BLOCKED),
                    "reason_code": f_code,
                }

            sym = (gd.get("sym") or "").strip().upper()
            e1_raw = gd.get("e1")
            e2_raw = gd.get("e2")
            cand_price: Optional[float] = None
            if e1_raw:
                p1 = parse_clean_number_token(e1_raw.rstrip("."))
                if p1 is None or p1 <= 0:
                    return None, "Malformed e1 in adaptive rule", elapsed_ms, {
                        "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                        "policy_rejected": True,
                        "reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    }
                p2 = parse_clean_number_token(e2_raw.rstrip(".")) if e2_raw else p1
                if p2 is None or p2 <= 0:
                    return None, "Malformed e2 in adaptive rule", elapsed_ms, {
                        "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                        "policy_rejected": True,
                        "reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                    }
                cand_price = max(p1, p2)

            if cand_price is not None:
                masked_entry_only = mask_sl_and_target_clauses(text)
                if not number_token_appears_in_text(cand_price, masked_entry_only, require_entry_role=False):
                    return (
                        None,
                        f"Captured price {cand_price} is from an SL/Target/time clause, not an entry clause",
                        elapsed_ms,
                        {
                            "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                            "policy_rejected": True,
                            "reason_code": DecisionReasonCode.AI_GROUNDING_FAILED.value,
                        },
                    )

            sig, p_stat, evidence, v_code, v_reason = validate_equity_candidate(
                symbol=sym,
                raw_text=text,
                candidate_price=cand_price,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                common_aliases=COMMON_STOCK_ALIASES,
                index_keywords=set(INDEX_KEYWORDS),
                allow_adaptive_capture=True,
            )
            evidence["worker_outcome"] = RegexEvalOutcome.MATCH.value if sig is not None else RegexEvalOutcome.NO_MATCH.value
            if sig is None:
                evidence["policy_rejected"] = True
                evidence["reason_code"] = v_code
                return None, v_reason, elapsed_ms, evidence

            return sig, "Matched active adaptive equity rule", elapsed_ms, evidence

        else:
            norm = normalize_signal_text(text)
            if not looks_like_option(norm) or is_exit_message(norm):
                return None, "Not an eligible option entry", elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                }
            is_hist, hist_reason = is_historical_callout(norm)
            if is_hist:
                return None, hist_reason, elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.HISTORICAL_CALLOUT.value,
                }
            is_neg, neg_reason = is_negated_or_non_actionable_callout(norm)
            if is_neg:
                return None, neg_reason, elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.ANALYSIS_COMMENTARY.value,
                }
            is_multi, _, multi_reason = detect_multiple_option_contracts(norm)
            if is_multi:
                return None, multi_reason, elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.AMBIGUOUS_CONTRACT.value,
                }

            sym = (gd.get("sym") or "").strip().upper()
            strike = parse_clean_number_token((gd.get("strike") or "").rstrip("."))
            if strike is None or strike <= 0:
                return None, "Invalid option strike", elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                }

            opt_raw = (gd.get("opt") or "").strip().upper()
            e1 = parse_clean_number_token((gd.get("e1") or "").rstrip("."))
            if e1 is None or e1 <= 0:
                return None, "Invalid option entry price", elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                }
            e2_raw = gd.get("e2")
            e2 = parse_clean_number_token(e2_raw.rstrip(".")) if e2_raw else e1
            if e2 is None or e2 <= 0:
                return None, "Invalid option upper entry price", elapsed_ms, {
                    "worker_outcome": RegexEvalOutcome.NO_MATCH.value,
                    "policy_rejected": True,
                    "reason_code": DecisionReasonCode.INVALID_EXPLICIT_PRICE.value,
                }

            mon_raw = gd.get("mon_raw") or gd.get("mon")
            day_raw = gd.get("day")
            day_val = int(day_raw) if day_raw and str(day_raw).isdigit() else None

            sig, _, evidence, v_code, v_reason = validate_fno_candidate(
                symbol=sym,
                strike=strike,
                option_type=opt_raw,
                entry_min=min(e1, e2),
                entry_max=max(e1, e2),
                raw_text=text,
                expiry_month=mon_raw,
                expiry_day=day_val,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                index_underlyings=set(INDEX_UNDERLYINGS),
            )
            evidence["worker_outcome"] = RegexEvalOutcome.MATCH.value if sig is not None else RegexEvalOutcome.NO_MATCH.value
            if sig is None:
                evidence["policy_rejected"] = True
                evidence["reason_code"] = v_code
                return None, v_reason, elapsed_ms, evidence

            return sig, "Matched active adaptive option rule", elapsed_ms, evidence
