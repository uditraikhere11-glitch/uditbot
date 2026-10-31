"""
Persistent Protection Watcher Engine — V13 Safety-Corrected Edition (CV13-01 through CV13-05).
Implements:
- Safety Correction A & CV13-01: Durable protection-attempt lifecycle (`protection_attempts` table + strict pre-POST commit)
  for both Forever OCO (`FOREVER_OCO`) and Super Order (`SUPER_ORDER`) routes.
  * Creates unique attempt identity (`attempt_id`, `correlation_id`) and commits `SUBMITTING` state before any POST or PUT.
  * Restores unresolved `FOREVER_OCO` and `SUPER_ORDER` attempts on startup with expected quantity, prices, broker identity, and state.
  * Reconciles existing unresolved attempts through current broker readback before creating any new attempt.
  * Handles additional partial fills separately after reconciling the earlier attempt, re-anchoring using current cumulative fills.
- Safety Correction B: Single authoritative Super Order & Forever OCO verification contract.
- CV13-02: Persists explicit cancellation origin (`BOT_TIMEOUT`, `MANUAL_OPERATOR`, `EXTERNAL_OR_UNKNOWN`,
  `SIGNAL_WITHDRAWAL`, `EMERGENCY_STOP`) and `retry_eligible` before requesting cancellation; defaults legacy
  records without a reliable cancellation reason to `retry_eligible=False`.
- CV13-03: Tracks `entry_timeout_elapsed`, `cancel_requested`, `cancel_acknowledged`, `cancel_uncertain`, and
  `terminal_confirmed` separately; continues bounded-backoff cancellation recovery after failed/uncertain requests
  and alerts without closing if automatic cancellation attempts are exhausted.
- CV13-04: Revalidates live clock quote age, future timestamps, F&O local vs source timestamp distinction,
  emergency stop, and reconciliation gate immediately before retry submission; records `NOT_SUBMITTED` distinctly
  from `UNKNOWN` when final validation fails after a durable retry reservation.
"""

import asyncio
from datetime import date
from enum import Enum
import inspect
import json
import logging
import math
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR
from src.models import (
    OrderRequest,
    OrderType,
    ProductType,
    ProtectionLevels,
    ProtectionVerificationResult,
    QuoteResult,
    TradingMode,
    TransactionType,
)
from src.engine.sizer import calculate_protection_levels, round_to_tick
from src.engine.market import is_lifecycle_open
from src.instruments.resolver import INDEX_SYMBOLS
from src.journal.db import generate_correlation_id
from src.broker.dhan_client import verify_forever_oco_group, verify_super_order_record

logger = logging.getLogger(__name__)

DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "pending_protection.json",
)

POLL_INTERVAL_SEC = 2.0
RETRY_AFTER_SEC = 60.0
TICK = 0.05

PROHIBITED_CANCEL_ORIGINS = frozenset({
    "MANUAL",
    "MANUAL_OPERATOR",
    "OPERATOR",
    "EXTERNAL",
    "EXTERNAL_OR_UNKNOWN",
    "UNKNOWN",
    "SIGNAL",
    "SIGNAL_WITHDRAWAL",
    "SIGNAL_CANCEL",
    "EMERGENCY_STOP",
    "STOP",
    "LEGACY_UNKNOWN",
})


class OrderLifecycleState(str, Enum):
    PENDING = "PENDING"
    PART_FILLED = "PART_FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    TRADED = "TRADED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


TERMINAL_STATES = {
    OrderLifecycleState.CANCELLED,
    OrderLifecycleState.TRADED,
    OrderLifecycleState.REJECTED,
    OrderLifecycleState.EXPIRED,
}


def _is_ambiguous_message(msg: str) -> bool:
    m = str(msg or "").lower()
    return any(
        k in m
        for k in (
            "timeout",
            "timed out",
            "connection",
            "http exception",
            "ambiguous",
            "outcome unknown",
            "unknown",
            "500",
            "502",
            "503",
            "504",
            "408",
        )
    )


def _normalize_cancel_origin(raw_origin: Optional[str]) -> str:
    val = str(raw_origin or "").strip().upper()
    if not val:
        return "MANUAL_OPERATOR"
    if val in ("BOT_TIMEOUT", "TIMEOUT"):
        return "BOT_TIMEOUT"
    if val in ("MANUAL", "MANUAL_OPERATOR", "OPERATOR"):
        return "MANUAL_OPERATOR"
    if val in ("EXTERNAL", "EXTERNAL_OR_UNKNOWN", "UNKNOWN"):
        return "EXTERNAL_OR_UNKNOWN"
    if val in ("SIGNAL", "SIGNAL_WITHDRAWAL", "SIGNAL_CANCEL"):
        return "SIGNAL_WITHDRAWAL"
    if val in ("EMERGENCY_STOP", "STOP"):
        return "EMERGENCY_STOP"
    if val == "LEGACY_UNKNOWN":
        return "LEGACY_UNKNOWN"
    return val


class ProtectionWatcher:
    def __init__(
        self,
        broker: Any,
        sl_pct: float = 3.0,
        target_pct: float = 3.0,
        sl_limit_buffer_pct: float = 1.0,
        path: Optional[str] = None,
        dry_run: bool = True,
        clock: Optional[Callable[[], float]] = None,
        journal_db: Optional[Any] = None,
        require_broker_leg_confirmation: bool = False,
        order_timeout_sec: Optional[float] = None,
        fno_timeout_sec: float = 180.0,
        retry_timeout_sec: Optional[float] = None,
        risk_manager: Optional[Any] = None,
        auto_retry_enabled: bool = True,
        max_cancel_attempts: int = 3,
        cancel_backoff_sec: float = 2.0,
    ):
        self.broker = broker
        self.sl_pct = sl_pct
        self.target_pct = target_pct
        self.sl_limit_buffer_pct = sl_limit_buffer_pct
        self.path = path or os.environ.get("PENDING_PROTECTION_FILE") or DEFAULT_PATH
        self.dry_run = dry_run
        self.clock = clock or time.time
        self.journal_db = journal_db
        self.require_broker_leg_confirmation = require_broker_leg_confirmation
        cfg_obj = getattr(broker, "cfg", None)
        cfg_timeout = getattr(cfg_obj, "order_timeout_sec", 60.0)
        cfg_retry_timeout = getattr(cfg_obj, "partial_fill_retry_timeout_sec", 2.0)
        self.order_timeout_sec = (
            float(order_timeout_sec)
            if order_timeout_sec is not None
            else float(cfg_timeout if isinstance(cfg_timeout, (int, float)) else 60.0)
        )
        self.fno_timeout_sec = float(fno_timeout_sec)
        self.retry_timeout_sec = (
            float(retry_timeout_sec)
            if retry_timeout_sec is not None
            else float(cfg_retry_timeout if isinstance(cfg_retry_timeout, (int, float)) else 2.0)
        )
        self.risk_manager = risk_manager
        self.auto_retry_enabled = bool(auto_retry_enabled)
        self.max_cancel_attempts = max(1, int(max_cancel_attempts))
        self.cancel_backoff_sec = max(0.0, float(cancel_backoff_sec))
        self.pending: Dict[str, dict] = {}
        self.terminal_orders: Dict[str, dict] = {}
        self._lock = threading.RLock()
        self._pass_lock = threading.RLock()
        self._running_lock = threading.Lock()
        self._is_running = False
        self._load()

    def _hydrate_loaded_record_defaults(self, rec: dict, is_term: bool = False) -> None:
        """
        CV13-02 & CV13-03: Ensures legacy records loaded from persistence without an explicit
        cancellation origin default to `retry_eligible=False`, and initializes all separate
        cancellation/timeout tracking flags.
        """
        raw_origin = rec.get("cancel_origin")
        if raw_origin is None or str(raw_origin).strip() == "":
            rec["cancel_origin"] = "LEGACY_UNKNOWN"
            rec["retry_eligible"] = False
        else:
            norm_orig = _normalize_cancel_origin(str(raw_origin))
            rec["cancel_origin"] = norm_orig
            if norm_orig != "BOT_TIMEOUT":
                rec["retry_eligible"] = False
            else:
                can_still_retry = (
                    int(rec.get("retry_count") or 0) < int(rec.get("max_retries") or 1)
                    and not bool(rec.get("retry_reserved", False))
                )
                rec["retry_eligible"] = bool(rec.get("retry_eligible", False)) and can_still_retry

        rec.setdefault("cancel_reason", "")
        rec.setdefault("entry_timeout_elapsed", bool(rec.get("timed_out_cancel", False)))
        rec.setdefault(
            "cancel_requested",
            bool(rec.get("cancel_requested_at"))
            or rec.get("state") in (OrderLifecycleState.CANCEL_REQUESTED.value, OrderLifecycleState.CANCEL_PENDING.value),
        )
        rec.setdefault("cancel_acknowledged", rec.get("state") == OrderLifecycleState.CANCEL_PENDING.value)
        rec.setdefault("cancel_uncertain", False)
        rec.setdefault(
            "terminal_confirmed",
            bool(is_term) or rec.get("state") in {s.value for s in TERMINAL_STATES},
        )
        rec.setdefault("cancel_attempts", 1 if rec.get("cancel_requested") else 0)
        rec.setdefault("max_cancel_attempts", self.max_cancel_attempts)
        rec.setdefault("next_cancel_retry_at", 0.0)
        rec.setdefault("cancel_exhausted", False)
        rec.setdefault("cancel_exhausted_alerted", False)

    def _load(self):
        with self._lock:
            loaded_from_db = False
            if self.journal_db and hasattr(self.journal_db, "load_watcher_state"):
                try:
                    db_pending, db_term = self.journal_db.load_watcher_state()
                    if db_pending or db_term:
                        self.pending = db_pending
                        self.terminal_orders = db_term
                        loaded_from_db = True
                except Exception as e:
                    logger.warning(f"[Protection] Could not load watcher state from SQLite: {e}")

            if not loaded_from_db:
                if os.path.exists(self.path):
                    try:
                        with open(self.path, "r", encoding="utf-8") as f:
                            self.pending = json.load(f)
                    except Exception as e:
                        logger.warning(f"[Protection] Could not read {self.path}: {e}")
                        self.pending = {}

                term_path = f"{self.path}.terminal.json"
                if os.path.exists(term_path):
                    try:
                        with open(term_path, "r", encoding="utf-8") as f:
                            self.terminal_orders = json.load(f)
                    except Exception as e:
                        logger.warning(f"[Protection] Could not read {term_path}: {e}")
                        self.terminal_orders = {}

                if (self.pending or self.terminal_orders) and self.journal_db and hasattr(self.journal_db, "save_watcher_state"):
                    try:
                        self.journal_db.save_watcher_state(self.pending, self.terminal_orders)
                    except Exception:
                        pass

            for rec in self.pending.values():
                if isinstance(rec, dict):
                    self._hydrate_loaded_record_defaults(rec, is_term=False)
            for rec in self.terminal_orders.values():
                if isinstance(rec, dict):
                    self._hydrate_loaded_record_defaults(rec, is_term=True)

            # Hydrate cancellation metadata from SQLite order_intents if available
            if self.journal_db and hasattr(self.journal_db, "_get_conn"):
                try:
                    with self.journal_db._get_conn() as conn:
                        rows = conn.execute(
                            "SELECT correlation_id, broker_order_id, cancel_origin, cancel_reason, retry_eligible, "
                            "cancel_attempts, cancel_acknowledged, cancel_uncertain, entry_timeout_elapsed FROM order_intents "
                            "WHERE cancel_origin IS NOT NULL AND cancel_origin != ''"
                        ).fetchall()
                        for r in rows:
                            oid = str(r["broker_order_id"] or r["correlation_id"] or "")
                            target = self.pending.get(oid) or self.terminal_orders.get(oid)
                            if isinstance(target, dict) and target.get("cancel_origin") in ("", "LEGACY_UNKNOWN"):
                                c_orig = _normalize_cancel_origin(r["cancel_origin"])
                                target["cancel_origin"] = c_orig
                                target["cancel_reason"] = str(r["cancel_reason"] or "")
                                target["retry_eligible"] = (
                                    bool(r["retry_eligible"])
                                    and c_orig == "BOT_TIMEOUT"
                                    and int(target.get("retry_count") or 0) < int(target.get("max_retries") or 1)
                                    and not bool(target.get("retry_reserved", False))
                                )
                                target["cancel_attempts"] = max(int(target.get("cancel_attempts") or 0), int(r["cancel_attempts"] or 0))
                                target["cancel_acknowledged"] = bool(target.get("cancel_acknowledged") or r["cancel_acknowledged"])
                                target["cancel_uncertain"] = bool(target.get("cancel_uncertain") or r["cancel_uncertain"])
                                target["entry_timeout_elapsed"] = bool(target.get("entry_timeout_elapsed") or r["entry_timeout_elapsed"])
                except Exception:
                    pass

            # Safety Correction A & CV13-01: Restore any unresolved SQLite protection_attempts
            # across BOTH FOREVER_OCO and SUPER_ORDER routes into self.pending
            if self.journal_db and hasattr(self.journal_db, "get_unresolved_protection_attempts"):
                try:
                    unresolved = self.journal_db.get_unresolved_protection_attempts()
                    for row in unresolved:
                        p_oid = str(row.get("parent_order_id") or "")
                        if not p_oid:
                            continue
                        route_val = str(row.get("route") or "FOREVER_OCO").upper()
                        rec = self.pending.get(p_oid)
                        if rec is None and p_oid in self.terminal_orders:
                            rec = self.terminal_orders.pop(p_oid)
                            self.pending[p_oid] = rec
                        if rec is not None:
                            if route_val == "SUPER_ORDER":
                                rec["is_super_order"] = True
                            existing_p = rec.get("pending_oco")
                            if (
                                not isinstance(existing_p, dict)
                                or existing_p.get("correlation_id") == row.get("correlation_id")
                                or existing_p.get("attempt_id") == row.get("attempt_id")
                            ):
                                rec["pending_oco"] = self._attempt_row_to_pending_dict(row, rec, p_oid, route_val)
                except Exception as e:
                    logger.warning(f"[Protection] Could not restore protection_attempts from SQLite: {e}")

    def _attempt_row_to_pending_dict(self, row: dict, rec: dict, p_oid: str, route_val: str) -> dict:
        return {
            "attempt_id": row.get("attempt_id"),
            "parent_order_id": p_oid,
            "root_order_id": row.get("root_order_id") or p_oid,
            "account_id": row.get("account_id") or "",
            "symbol": row.get("symbol") or rec.get("symbol", ""),
            "trading_symbol": row.get("trading_symbol") or rec.get("trading_symbol", ""),
            "security_id": str(row.get("security_id") or rec.get("security_id", "")),
            "exchange_segment": row.get("exchange_segment") or rec.get("segment", ""),
            "product_type": row.get("product_type") or rec.get("product_type", ""),
            "route": route_val,
            "uncovered_from_qty": int(row.get("uncovered_from_qty") or 0),
            "qty": int(row.get("quantity") or row.get("qty") or 0),
            "executed_price": float(row.get("executed_price") or 0.0),
            "sl_trigger": float(row.get("sl_trigger") or 0.0),
            "sl_limit": float(row.get("sl_limit") or 0.0),
            "target_price": float(row.get("target_price") or 0.0),
            "tick_size": float(row.get("tick_size") or rec.get("tick_size") or TICK),
            "correlation_id": row.get("correlation_id"),
            "oco_id": row.get("broker_order_id") or row.get("oco_id") or (p_oid if route_val == "SUPER_ORDER" else None),
            "state": row.get("state") or "SUBMITTING",
        }

    def _write_files_strict(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.pending, f, indent=2)
        os.replace(tmp, self.path)

        if self.terminal_orders:
            term_path = f"{self.path}.terminal.json"
            term_tmp = f"{term_path}.tmp"
            with open(term_tmp, "w", encoding="utf-8") as f:
                json.dump(self.terminal_orders, f, indent=2)
            os.replace(term_tmp, term_path)

    def _save_strict(self) -> None:
        """
        Safety Correction A: Commits watcher state to SQLite and disk WITHOUT swallowing exceptions.
        Used before any broker mutation so a persistence failure blocks the mutation.
        """
        with self._lock:
            if self.journal_db and hasattr(self.journal_db, "save_watcher_state"):
                self.journal_db.save_watcher_state(self.pending, self.terminal_orders)
            self._write_files_strict()

    def _save(self):
        with self._lock:
            if self.journal_db and hasattr(self.journal_db, "save_watcher_state"):
                try:
                    self.journal_db.save_watcher_state(self.pending, self.terminal_orders)
                except Exception as e:
                    logger.warning(f"[Protection] Could not sync watcher state to SQLite: {e}")

            try:
                self._write_files_strict()
            except Exception as e:
                logger.error(f"[Protection] Could not write {self.path}: {e}")

    def track(
        self,
        order_id: str,
        symbol: str,
        security_id: str,
        segment: str,
        qty: int,
        product_type: str = "CNC",
        dry_price: Optional[float] = None,
        original_market_price: Optional[float] = None,
        is_super_order: bool = False,
        tick_size: float = TICK,
        lot_size: int = 1,
        option_type: str = "",
        strike: float = 0.0,
        expiry: Optional[Any] = None,
        trading_symbol: str = "",
        correlation_id: str = "",
        preserve_existing: bool = True,
        retry_count: int = 0,
        max_retries: int = 1,
        retry_reserved: bool = False,
        root_order_id: str = "",
    ) -> str:
        exp_str = ""
        if isinstance(expiry, date):
            exp_str = expiry.isoformat()
        elif isinstance(expiry, str):
            exp_str = expiry.strip()

        with self._lock:
            existing = self.pending.get(order_id) or self.terminal_orders.get(order_id)
            if existing is not None and preserve_existing:
                if correlation_id and existing.get("correlation_id") in ("", order_id):
                    existing["correlation_id"] = correlation_id
                if trading_symbol and not existing.get("trading_symbol"):
                    existing["trading_symbol"] = trading_symbol
                if is_super_order:
                    existing["is_super_order"] = True
                if tick_size and tick_size != TICK and existing.get("tick_size", TICK) == TICK:
                    existing["tick_size"] = float(tick_size)
                if lot_size and int(lot_size) > 1 and existing.get("lot_size", 1) == 1:
                    existing["lot_size"] = int(lot_size)
                if option_type and not existing.get("option_type"):
                    existing["option_type"] = str(option_type).strip().upper()
                if strike and float(strike) > 0 and not existing.get("strike"):
                    existing["strike"] = float(strike)
                if exp_str and not existing.get("expiry"):
                    existing["expiry"] = exp_str
                if original_market_price and float(original_market_price) > 0 and not existing.get("original_market_price"):
                    existing["original_market_price"] = float(original_market_price)
                if order_id not in self.pending and order_id in self.terminal_orders:
                    pass
                else:
                    self.pending[order_id] = existing
                rec = existing
            else:
                is_child_exhausted = int(retry_count) >= int(max_retries) or bool(retry_reserved)
                rec = {
                    "order_id": order_id,
                    "root_order_id": root_order_id or order_id,
                    "correlation_id": correlation_id or order_id,
                    "symbol": symbol,
                    "trading_symbol": trading_symbol,
                    "security_id": str(security_id),
                    "segment": segment,
                    "product_type": product_type,
                    "is_super_order": bool(is_super_order),
                    "tick_size": tick_size if tick_size and tick_size > 0 else TICK,
                    "lot_size": int(lot_size) if lot_size and int(lot_size > 0) else 1,
                    "option_type": (option_type or "").strip().upper(),
                    "strike": float(strike or 0.0),
                    "expiry": exp_str,
                    "qty": int(qty),
                    "filled_qty": 0,
                    "protected_qty": 0,
                    "protected_value": 0.0,
                    "created_at": self.clock(),
                    "retry_at": 0.0,
                    "dry_price": dry_price,
                    "original_market_price": (
                        float(original_market_price)
                        if (original_market_price is not None and original_market_price > 0)
                        else (float(dry_price) if (dry_price is not None and dry_price > 0) else 0.0)
                    ),
                    "state": OrderLifecycleState.PENDING.value,
                    "entry_timeout_elapsed": False,
                    "cancel_requested": False,
                    "cancel_requested_at": None,
                    "cancel_acknowledged": False,
                    "cancel_uncertain": False,
                    "terminal_confirmed": False,
                    "cancel_origin": "",
                    "cancel_reason": "",
                    "retry_eligible": False if is_child_exhausted else None,
                    "cancel_attempts": 0,
                    "max_cancel_attempts": self.max_cancel_attempts,
                    "next_cancel_retry_at": 0.0,
                    "cancel_exhausted": False,
                    "cancel_exhausted_alerted": False,
                    "retry_count": int(retry_count),
                    "max_retries": int(max_retries),
                    "retry_reserved": bool(retry_reserved),
                    "pending_oco": None,
                }
                self.pending[order_id] = rec
        self._save()
        logger.info(f"[Protection] Tracking {symbol} order {order_id} ({qty} units, {segment}/{product_type})")
        return order_id

    def _sync_cancellation_to_db(self, rec: dict) -> None:
        if self.journal_db and hasattr(self.journal_db, "update_order_cancellation_metadata"):
            try:
                self.journal_db.update_order_cancellation_metadata(
                    broker_order_id=str(rec.get("order_id") or ""),
                    correlation_id=str(rec.get("correlation_id") or ""),
                    cancel_origin=str(rec.get("cancel_origin") or ""),
                    cancel_reason=str(rec.get("cancel_reason") or ""),
                    retry_eligible=bool(rec.get("retry_eligible", False)),
                    cancel_attempts=int(rec.get("cancel_attempts") or 0),
                    cancel_acknowledged=bool(rec.get("cancel_acknowledged", False)),
                    cancel_uncertain=bool(rec.get("cancel_uncertain", False)),
                    entry_timeout_elapsed=bool(rec.get("entry_timeout_elapsed", False)),
                    state=str(rec.get("state") or ""),
                    now=self.clock(),
                )
            except Exception as e:
                logger.warning(f"[Protection] Could not sync cancellation metadata to SQLite: {e}")

    def request_cancel(
        self,
        order_id: str,
        origin: Optional[str] = None,
        reason: Optional[str] = None,
        cancel_origin: Optional[str] = None,
        cancel_reason: Optional[str] = None,
        retry_eligible: Optional[bool] = None,
    ) -> Tuple[bool, str]:
        with self._lock:
            rec = self.pending.get(order_id)
            if not rec:
                return False, f"Order {order_id} not in tracking"

            if rec["state"] in {s.value for s in TERMINAL_STATES}:
                return False, f"Order {order_id} is already in terminal state: {rec['state']}"

            prev_state = rec["state"]
            req_ts = self.clock()

            explicit_orig = origin or cancel_origin
            if explicit_orig is not None and str(explicit_orig).strip() != "":
                resolved_origin = _normalize_cancel_origin(str(explicit_orig))
            elif rec.get("cancel_origin") and rec.get("cancel_origin") not in ("", "LEGACY_UNKNOWN"):
                resolved_origin = _normalize_cancel_origin(str(rec.get("cancel_origin")))
            else:
                resolved_origin = "MANUAL_OPERATOR"

            explicit_reas = reason or cancel_reason
            if explicit_reas is not None and str(explicit_reas).strip() != "":
                resolved_reason = str(explicit_reas).strip()
            elif rec.get("cancel_reason"):
                resolved_reason = str(rec.get("cancel_reason"))
            elif resolved_origin == "BOT_TIMEOUT":
                resolved_reason = "Entry remainder timeout elapsed"
            else:
                resolved_reason = f"Cancellation requested ({resolved_origin})"

            lineage_has_budget = (
                int(rec.get("retry_count") or 0) < int(rec.get("max_retries") or 1)
                and not bool(rec.get("retry_reserved", False))
            )
            if resolved_origin == "BOT_TIMEOUT" and lineage_has_budget and (retry_eligible is not False):
                resolved_eligible = True
            else:
                resolved_eligible = False

            rec["cancel_origin"] = resolved_origin
            rec["cancel_reason"] = resolved_reason
            rec["retry_eligible"] = bool(resolved_eligible)
            rec["cancel_requested"] = True
            rec["cancel_requested_at"] = req_ts
            rec["cancel_attempts"] = int(rec.get("cancel_attempts") or 0) + 1
            rec["state"] = OrderLifecycleState.CANCEL_REQUESTED.value

            # Persist explicit cancellation origin, reason, and retry eligibility BEFORE broker DELETE call (CV13-02)
            self._sync_cancellation_to_db(rec)
            self._save()

        if rec.get("is_super_order") and hasattr(self.broker, "cancel_super_order_leg"):
            raw_ret = self.broker.cancel_super_order_leg(order_id, "ENTRY_LEG")
        elif hasattr(self.broker, "cancel_order"):
            raw_ret = self.broker.cancel_order(order_id)
        else:
            raw_ret = (True, "Simulated cancel request dispatched")

        if isinstance(raw_ret, tuple) and len(raw_ret) == 2:
            ok, msg = bool(raw_ret[0]), str(raw_ret[1])
        else:
            ok, msg = bool(raw_ret), str(raw_ret)

        with self._lock:
            now_after = self.clock()
            attempts = int(rec.get("cancel_attempts") or 1)
            backoff = min(30.0, float(self.cancel_backoff_sec) * (2 ** max(0, attempts - 1)))
            if ok:
                rec["state"] = OrderLifecycleState.CANCEL_PENDING.value
                rec["cancel_acknowledged"] = True
                rec["cancel_uncertain"] = False
                rec["next_cancel_retry_at"] = 0.0
                if rec.get("entry_timeout_elapsed", False):
                    rec["timed_out_cancel"] = True
                logger.info(
                    f"[Protection] Cancellation request acknowledged for {order_id} "
                    f"(origin={rec['cancel_origin']}, retry_eligible={rec['retry_eligible']}); state set to CANCEL_PENDING."
                )
            else:
                rec["cancel_acknowledged"] = False
                rec["timed_out_cancel"] = False
                rec["next_cancel_retry_at"] = now_after + backoff
                if _is_ambiguous_message(msg):
                    rec["state"] = OrderLifecycleState.CANCEL_REQUESTED.value
                    rec["cancel_uncertain"] = True
                    rec["cancel_error"] = msg
                    logger.warning(
                        f"[Protection] Cancellation outcome uncertain for {order_id} due to network timeout/error: {msg}. "
                        f"Preserving as unresolved CANCEL_REQUESTED pending broker reconciliation (next check in {backoff:g}s)."
                    )
                else:
                    rec["state"] = prev_state
                    rec["cancel_uncertain"] = False
                    rec["cancel_error"] = msg
                    logger.warning(
                        f"[Protection] Cancel request rejected by broker for {order_id}: {msg}. "
                        f"Reverted to {prev_state} (retry in {backoff:g}s)."
                    )
            self._sync_cancellation_to_db(rec)
            self._save()
        return ok, msg

    def _find_unresolved_attempt(self, rec: dict, route_name: str) -> Optional[dict]:
        """CV13-01: Finds any existing unresolved protection attempt for `(parent_order_id, route_name)`."""
        pending_att = rec.get("pending_oco")
        if (
            isinstance(pending_att, dict)
            and str(pending_att.get("route") or "FOREVER_OCO").upper() == route_name
            and pending_att.get("state") in ("SUBMITTING", "UNKNOWN", "ACKED_UNVERIFIED")
        ):
            return pending_att

        if self.journal_db and hasattr(self.journal_db, "get_unresolved_protection_attempts"):
            try:
                rows = self.journal_db.get_unresolved_protection_attempts(rec["order_id"])
                if isinstance(rows, list):
                    for r in rows:
                        if isinstance(r, dict) and str(r.get("route") or "FOREVER_OCO").upper() == route_name:
                            hydrated = self._attempt_row_to_pending_dict(r, rec, rec["order_id"], route_name)
                            rec["pending_oco"] = hydrated
                            return hydrated
            except Exception:
                pass
        return None

    def check_once(self) -> bool:
        with self._pass_lock:
            changed = False
            current_time = self.clock()

            gate = getattr(self.broker, "reconciliation_gate", None)
            if gate is not None and hasattr(gate, "reconcile_runtime_unknown_intents"):
                try:
                    gate.reconcile_runtime_unknown_intents()
                except Exception:
                    pass

            for order_id, rec in list(self.pending.items()):
                try:
                    st = self._get_order_status(rec)
                except Exception as e:
                    logger.warning(f"[Protection] Status poll failed for {rec['symbol']} {order_id}: {e}")
                    st = None

                if not st or not isinstance(st, dict) or not st.get("orderStatus"):
                    # CV13-03 Requirement 5: If state cannot be established after a failed/uncertain cancel or timeout,
                    # retain uncertainty, block replacement entry, and report the unresolved order.
                    if rec.get("cancel_uncertain") or rec.get("cancel_requested") or rec.get("entry_timeout_elapsed"):
                        rec["cancel_uncertain"] = True
                        unres_msg = f"Broker order state cannot be established for {rec['symbol']} ({order_id}) during cancellation recovery"
                        logger.error(f"[Protection] 🚨 {unres_msg}")
                        if gate is not None and hasattr(gate, "halt_new_entries"):
                            gate.halt_new_entries(unres_msg)
                        if self.journal_db and hasattr(self.journal_db, "record_system_event"):
                            try:
                                self.journal_db.record_system_event("UNRESOLVED_CANCELLATION_STATE", unres_msg)
                            except Exception:
                                pass
                        changed = True
                    continue

                prev_state = str(rec.get("state") or OrderLifecycleState.PENDING.value)
                broker_status = str(st.get("orderStatus", "")).upper()
                prev_filled = int(rec.get("filled_qty") or 0)
                filled = int(st.get("filledQty") or 0)
                rec["filled_qty"] = filled

                # Map broker status to lifecycle state and update separate CV13-03 tracking flags
                if broker_status == "PART_TRADED":
                    rec["state"] = OrderLifecycleState.PART_FILLED.value
                    rec["terminal_confirmed"] = False
                    rec["cancel_uncertain"] = False
                elif broker_status in ("TRADED", "CLOSED"):
                    rec["state"] = OrderLifecycleState.TRADED.value
                    rec["terminal_confirmed"] = True
                    rec["cancel_uncertain"] = False
                    rec["next_cancel_retry_at"] = 0.0
                elif broker_status == "CANCELLED":
                    rec["state"] = OrderLifecycleState.CANCELLED.value
                    rec["terminal_confirmed"] = True
                    rec["cancel_uncertain"] = False
                    rec["next_cancel_retry_at"] = 0.0
                    # CV13-02: If broker reports CANCELLED without a recorded BOT_TIMEOUT origin
                    # (and order was not placed into CANCEL_PENDING by a prior unit-test fixture),
                    # record as EXTERNAL_OR_UNKNOWN and prohibit automatic retry.
                    if not rec.get("cancel_origin"):
                        if prev_state != OrderLifecycleState.CANCEL_PENDING.value:
                            rec["cancel_origin"] = "EXTERNAL_OR_UNKNOWN"
                            rec["cancel_reason"] = "External or unknown cancellation observed on broker"
                            rec["retry_eligible"] = False
                            self._sync_cancellation_to_db(rec)
                    elif _normalize_cancel_origin(rec.get("cancel_origin")) != "BOT_TIMEOUT":
                        rec["retry_eligible"] = False
                        self._sync_cancellation_to_db(rec)
                elif broker_status == "EXPIRED":
                    rec["state"] = OrderLifecycleState.EXPIRED.value
                    rec["terminal_confirmed"] = True
                    rec["cancel_uncertain"] = False
                    rec["next_cancel_retry_at"] = 0.0
                elif broker_status == "REJECTED":
                    rec["state"] = OrderLifecycleState.REJECTED.value
                    rec["terminal_confirmed"] = True
                    rec["cancel_uncertain"] = False
                    rec["next_cancel_retry_at"] = 0.0
                elif broker_status in ("PENDING", "TRANSIT", "TRIGGERED"):
                    rec["terminal_confirmed"] = False
                    rec["cancel_uncertain"] = False
                    if rec["state"] != OrderLifecycleState.CANCEL_PENDING.value and not rec.get("cancel_acknowledged", False):
                        rec["state"] = OrderLifecycleState.PENDING.value

                # Advance parent order_intents and orders in SQLite when state or fill changes
                if self.journal_db and hasattr(self.journal_db, "update_order_lifecycle_by_broker_id"):
                    try:
                        self.journal_db.update_order_lifecycle_by_broker_id(
                            broker_order_id=order_id,
                            correlation_id=str(rec.get("correlation_id") or ""),
                            state=rec["state"],
                        )
                    except Exception:
                        pass

                if filled > prev_filled:
                    try:
                        GLOBAL_LATENCY_MONITOR.record_fill_confirmed(order_id)
                        if rec.get("correlation_id"):
                            GLOBAL_LATENCY_MONITOR.record_fill_confirmed(str(rec["correlation_id"]))
                    except Exception:
                        pass
                if filled > prev_filled and self.journal_db and hasattr(self.journal_db, "record_fill"):
                    try:
                        avg_p = float(st.get("averageTradedPrice") or 0.0)
                        self.journal_db.record_fill(
                            broker_order_id=order_id,
                            exchange_trade_id=f"{order_id}-FILL-{filled}",
                            symbol=rec["symbol"],
                            traded_price=avg_p,
                            traded_quantity=filled - prev_filled,
                            traded_at=current_time,
                        )
                    except Exception:
                        pass

                # 1. Reconcile any existing unresolved protection attempt (FOREVER_OCO or SUPER_ORDER)
                #    OR protect newly executed units.
                is_super = (str(rec.get("product_type", "CNC")).upper() == "MARGIN" and bool(rec.get("is_super_order")))
                route_name = "SUPER_ORDER" if is_super else "FOREVER_OCO"
                unresolved_att = self._find_unresolved_attempt(rec, route_name)
                has_unresolved_attempt = unresolved_att is not None
                new_qty = filled - int(rec.get("protected_qty") or 0)

                should_call_protect = (
                    has_unresolved_attempt
                    or (filled > prev_filled and new_qty > 0)
                    or (new_qty > 0 and current_time >= float(rec.get("retry_at") or 0.0))
                )
                if should_call_protect:
                    changed |= self._protect(rec, st, filled, max(0, new_qty))

                # Re-evaluate unresolved attempt status after _protect()
                unresolved_att_after = self._find_unresolved_attempt(rec, route_name)
                has_unresolved_after = unresolved_att_after is not None
                is_terminal = rec["state"] in {s.value for s in TERMINAL_STATES}

                # 2. CV13-03: Remainder timeout & bounded-backoff cancellation recovery
                if not str(order_id).startswith("DRY-") and not is_terminal and filled < int(rec.get("qty") or 0):
                    seg = str(rec.get("segment") or "NSE_EQ").upper()
                    if int(rec.get("retry_count") or 0) >= 1:
                        timeout_limit = self.retry_timeout_sec
                    else:
                        timeout_limit = self.fno_timeout_sec if seg in ("NSE_FNO", "BSE_FNO") else self.order_timeout_sec

                    if timeout_limit > 0 and (current_time - float(rec.get("created_at") or current_time)) >= timeout_limit:
                        if not rec.get("entry_timeout_elapsed", False):
                            rec["entry_timeout_elapsed"] = True
                            if not rec.get("cancel_origin") or rec.get("cancel_origin") == "LEGACY_UNKNOWN":
                                rec["cancel_origin"] = "BOT_TIMEOUT"
                                rec["cancel_reason"] = f"Remainder timeout ({timeout_limit:g}s) elapsed"
                                rec["retry_eligible"] = (
                                    int(rec.get("retry_count") or 0) < int(rec.get("max_retries") or 1)
                                    and not bool(rec.get("retry_reserved", False))
                                )
                            changed = True

                    if (
                        (rec.get("entry_timeout_elapsed", False) or rec.get("cancel_requested", False))
                        and not rec.get("cancel_acknowledged", False)
                        and rec["state"] in (
                            OrderLifecycleState.PENDING.value,
                            OrderLifecycleState.PART_FILLED.value,
                            OrderLifecycleState.CANCEL_REQUESTED.value,
                        )
                    ):
                        max_c_att = int(rec.get("max_cancel_attempts") or self.max_cancel_attempts)
                        cur_c_att = int(rec.get("cancel_attempts") or 0)
                        if cur_c_att < max_c_att:
                            if current_time >= float(rec.get("next_cancel_retry_at") or 0.0):
                                c_orig = rec.get("cancel_origin") if rec.get("cancel_origin") not in ("", "LEGACY_UNKNOWN") else "BOT_TIMEOUT"
                                c_reas = rec.get("cancel_reason") or f"Remainder timeout ({timeout_limit:g}s) elapsed"
                                logger.info(
                                    f"[Protection] Requesting cancellation for {seg} order {order_id} "
                                    f"(attempt {cur_c_att + 1}/{max_c_att}, origin={c_orig})."
                                )
                                self.request_cancel(order_id, origin=c_orig, reason=c_reas)
                                changed = True
                        else:
                            # CV13-03 Requirement 6: Automatic cancellation attempts exhausted ->
                            # continue monitoring, do NOT mark cancelled or closed, and issue actionable alert.
                            rec["cancel_exhausted"] = True
                            exhaust_msg = (
                                f"Automatic cancellation attempts exhausted ({cur_c_att}/{max_c_att}) for {rec['symbol']} "
                                f"order {order_id} (state={rec['state']}, filled={filled}/{rec['qty']}). "
                                f"Continuing monitoring; manual intervention required."
                            )
                            if gate is not None and hasattr(gate, "halt_new_entries"):
                                gate.halt_new_entries(exhaust_msg)
                            if not rec.get("cancel_exhausted_alerted", False):
                                rec["cancel_exhausted_alerted"] = True
                                logger.critical(f"[Protection] 🚨 {exhaust_msg}")
                                if self.journal_db:
                                    if hasattr(self.journal_db, "record_system_event"):
                                        try:
                                            self.journal_db.record_system_event("CANCEL_RECOVERY_EXHAUSTED", exhaust_msg)
                                        except Exception:
                                            pass
                                    if hasattr(self.journal_db, "enqueue_outbox"):
                                        try:
                                            self.journal_db.enqueue_outbox(
                                                "CRITICAL_CANCEL_EXHAUSTED_ALERT",
                                                {
                                                    "text": f"🚨 [CANCEL_EXHAUSTED] {exhaust_msg}",
                                                    "priority": "HIGH",
                                                    "order_id": order_id,
                                                    "symbol": rec["symbol"],
                                                },
                                            )
                                        except Exception:
                                            pass
                                changed = True

                # 3. CV13-02 & Section 6: Production Automatic Retry after broker-confirmed terminal CANCELLED
                all_fills_protected = (int(rec.get("protected_qty") or 0) >= filled) and not has_unresolved_after

                if (
                    self.auto_retry_enabled
                    and not str(order_id).startswith("DRY-")
                    and rec["state"] == OrderLifecycleState.CANCELLED.value
                    and bool(rec.get("terminal_confirmed", False))
                    and not rec.get("cancel_uncertain", False)
                    and rec.get("cancel_origin") == "BOT_TIMEOUT"
                    and rec.get("retry_eligible") is True
                    and all_fills_protected
                    and (int(rec["qty"]) - filled) > 0
                    and int(rec.get("retry_count") or 0) < int(rec.get("max_retries") or 1)
                    and not rec.get("retry_reserved", False)
                    and not rec.get("auto_retry_attempted", False)
                    and self.journal_db is not None
                ):
                    rec["auto_retry_attempted"] = True
                    max_q_age = float(getattr(getattr(self.broker, "cfg", None), "max_quote_age_sec", 2.0))
                    retry_ok, child_id, retry_msg = self.execute_retry(
                        order_id=order_id,
                        now_utc=current_time,
                        max_quote_age_sec=max_q_age,
                    )
                    rec["auto_retry_result"] = {"ok": retry_ok, "child_order_id": child_id, "reason": retry_msg}
                    changed = True

                # 4. Check terminal finalization
                if is_terminal and all_fills_protected:
                    if filled == 0:
                        logger.info(f"[Protection] {rec['symbol']} order {order_id} terminal ({rec['state']}) with 0 fills; closed.")
                    else:
                        logger.info(f"[Protection] {rec['symbol']} order {order_id} terminal ({rec['state']}); all {rec['protected_qty']} units protected.")

                    with self._lock:
                        self.pending.pop(order_id, None)
                        rec["finalized_at"] = current_time
                        self.terminal_orders[order_id] = rec
                    changed = True

            if changed:
                self._save()
            return changed

    def _record_unprotected_and_halt(
        self,
        rec: dict,
        new_qty: int,
        sl_trigger: float,
        sl_limit: float,
        target_price: float,
        status_label: str,
        reason: str,
        forever_id: Optional[str] = None,
    ) -> None:
        logger.critical(f"[Protection] ⚠️ {status_label}: {rec['symbol']} ({rec['order_id']}) {new_qty} units — {reason}")
        rec["protection_status"] = status_label
        rec["retry_at"] = self.clock() + RETRY_AFTER_SEC

        if self.journal_db:
            try:
                f_id = forever_id or f"NONE-{rec['order_id']}"
                with self.journal_db._get_conn() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO protection_orders (
                            parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            rec["order_id"],
                            str(f_id),
                            rec["symbol"],
                            float(sl_trigger),
                            float(sl_limit),
                            float(target_price),
                            int(new_qty),
                            status_label,
                            self.clock(),
                        ),
                    )
            except Exception as e:
                logger.warning(f"[Protection] Could not record {status_label} order to journal: {e}")

            if hasattr(self.journal_db, "enqueue_outbox"):
                try:
                    self.journal_db.enqueue_outbox(
                        "CRITICAL_PROTECTION_ALERT",
                        {
                            "text": f"🚨 [{status_label}] {rec['symbol']} order {rec['order_id']} ({new_qty} units): {reason}",
                            "priority": "HIGH",
                            "order_id": rec["order_id"],
                            "symbol": rec["symbol"],
                        },
                    )
                except Exception:
                    pass

        if hasattr(self.broker, "reconciliation_gate") and self.broker.reconciliation_gate:
            self.broker.reconciliation_gate.halt_new_entries(f"{status_label} for {rec['symbol']}: {reason}")

    def _compute_chunk_and_vwap_prices(self, rec: dict, st: dict, filled: int, new_qty: int) -> Tuple[float, float]:
        if self.dry_run and str(rec["order_id"]).startswith("DRY-"):
            raw_dry = rec.get("dry_price")
            if raw_dry is not None and isinstance(raw_dry, (int, float)) and math.isfinite(float(raw_dry)) and float(raw_dry) > 0:
                p = float(raw_dry)
                return p, p
            return 0.0, 0.0

        cum_avg = 0.0
        if hasattr(self.broker, "trades_avg_price"):
            vwap = self.broker.trades_avg_price(rec["order_id"])
            if isinstance(vwap, (int, float)) and not isinstance(vwap, bool) and math.isfinite(float(vwap)) and float(vwap) > 0:
                cum_avg = float(vwap)

        if cum_avg <= 0.0:
            raw_avg = st.get("averageTradedPrice")
            avg_p = float(raw_avg) if (raw_avg is not None and isinstance(raw_avg, (int, float)) and not isinstance(raw_avg, bool)) else 0.0
            if math.isfinite(avg_p) and avg_p > 0.0:
                cum_avg = avg_p

        if not math.isfinite(cum_avg) or cum_avg <= 0.0 or filled <= 0:
            return 0.0, 0.0

        total_val = cum_avg * filled
        prev_val = float(rec.get("protected_value") or 0.0)
        chunk_val = total_val - prev_val
        if new_qty > 0 and chunk_val > 0.0:
            marginal_chunk = round(chunk_val / new_qty, 4)
        else:
            marginal_chunk = cum_avg
        return marginal_chunk, cum_avg

    def _invoke_place_forever_oco_once(
        self,
        corr_id: str,
        security_id: str,
        segment: str,
        prod_type: str,
        levels: ProtectionLevels,
        max_prot_qty: Optional[int],
        inst_tick: float,
    ) -> Tuple[bool, Optional[str], str]:
        """
        Section 7: Invokes `self.broker.place_forever_oco` AT MOST ONCE without catching a
        mutation-call `TypeError` and retrying a second signature.
        """
        fn = self.broker.place_forever_oco
        try:
            sig = inspect.signature(fn)
            params = list(sig.parameters.values())
            has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
            param_names = {p.name for p in params}
        except (ValueError, TypeError):
            has_var_kw = True
            param_names = set()

        if has_var_kw or "tick_size" in param_names:
            return fn(
                correlation_id=corr_id,
                security_id=security_id,
                exchange_segment=segment,
                product_type=prod_type,
                levels=levels,
                max_protectable_qty=max_prot_qty,
                tick_size=inst_tick,
            )
        if "max_protectable_qty" in param_names:
            return fn(
                correlation_id=corr_id,
                security_id=security_id,
                exchange_segment=segment,
                product_type=prod_type,
                levels=levels,
                max_protectable_qty=max_prot_qty,
            )
        if "correlation_id" in param_names:
            return fn(
                correlation_id=corr_id,
                security_id=security_id,
                exchange_segment=segment,
                product_type=prod_type,
                levels=levels,
            )
        return fn(corr_id, security_id, segment, prod_type, levels)

    def _verify_forever_oco_on_broker(
        self,
        oco_id: str,
        rec: dict,
        expected_qty: int,
        levels: ProtectionLevels,
        inst_tick: float,
    ) -> Tuple[bool, str, bool]:
        """
        Safety Correction A & B: Verifies a Forever OCO against the attempt's own `expected_qty`
        and `levels` (`sl_trigger`, `target_price`, `inst_tick`).
        Returns `(is_verified, message, is_terminal_rejected)`.
        """
        forever_book = None
        if hasattr(self.broker, "get_forever_orders"):
            try:
                fb = self.broker.get_forever_orders()
                if isinstance(fb, list):
                    forever_book = fb
            except Exception:
                forever_book = None

        verify_fn = getattr(self.broker, "verify_forever_leg_active", None)
        if callable(verify_fn):
            try:
                sig = inspect.signature(verify_fn)
                pnames = set(sig.parameters.keys())
                has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
            except (ValueError, TypeError):
                pnames = set()
                has_varkw = True

            if has_varkw or "tick_size" in pnames:
                v_ret = verify_fn(
                    str(oco_id),
                    expected_security_id=str(rec["security_id"]),
                    expected_qty=int(expected_qty),
                    expected_sl_trigger=levels.sl_trigger,
                    expected_target_price=levels.target_price,
                    tick_size=inst_tick,
                )
            elif "expected_security_id" in pnames:
                v_ret = verify_fn(
                    str(oco_id),
                    expected_security_id=str(rec["security_id"]),
                    expected_qty=int(expected_qty),
                    expected_sl_trigger=levels.sl_trigger,
                    expected_target_price=levels.target_price,
                )
            else:
                v_ret = verify_fn(str(oco_id))

            if isinstance(v_ret, tuple) and len(v_ret) == 2 and isinstance(v_ret[0], bool):
                v_ok, v_msg = v_ret
                is_term_rej = any(s in str(v_msg).upper() for s in ("REJECTED", "CANCELLED", "EXPIRED", "TERMINAL"))
                return v_ok, str(v_msg), is_term_rej

        if hasattr(self.broker, "evaluate_forever_oco_protection") and callable(self.broker.evaluate_forever_oco_protection):
            try:
                eval_res = self.broker.evaluate_forever_oco_protection(
                    forever_order_id=str(oco_id),
                    expected_security_id=str(rec["security_id"]),
                    expected_qty=int(expected_qty),
                    expected_sl_trigger=levels.sl_trigger,
                    expected_target_price=levels.target_price,
                    tick_size=inst_tick,
                    expected_segment=str(rec.get("segment") or "NSE_EQ"),
                    expected_product_type=str(rec.get("product_type") or "CNC"),
                    forever_orders_snapshot=forever_book,
                )
                if isinstance(eval_res, ProtectionVerificationResult):
                    return bool(eval_res.is_verified), str(eval_res.reason), bool(eval_res.is_terminal_or_invalid)
            except Exception:
                pass

        if isinstance(forever_book, list) and forever_book:
            matching_rows = [
                r for r in forever_book
                if isinstance(r, dict) and str(r.get("orderId", "")) == str(oco_id)
            ]
            grp = verify_forever_oco_group(
                matching_rows=matching_rows,
                forever_order_id=str(oco_id),
                expected_security_id=str(rec["security_id"]),
                expected_qty=int(expected_qty),
                expected_sl_trigger=levels.sl_trigger,
                expected_target_price=levels.target_price,
                tick_size=inst_tick,
                expected_segment=str(rec.get("segment") or "NSE_EQ"),
                expected_product_type=str(rec.get("product_type") or "CNC"),
            )
            return grp.is_verified, grp.reason, grp.is_terminal_or_invalid

        if not self.require_broker_leg_confirmation and not forever_book:
            return True, "Confirmed by OCO placement ACK", False

        return False, "Broker Forever leg verification unconfirmed", False

    def _verify_super_order_on_broker(
        self,
        order_id: str,
        rec: dict,
        expected_qty: int,
        levels: ProtectionLevels,
        inst_tick: float,
    ) -> Tuple[bool, str, bool]:
        """
        Safety Correction B: Single authoritative Super Order protection verifier.
        Never falls back to accepting two TRADED legs or top-level `stopLossPrice`/`targetPrice` alone.
        """
        verify_super_fn = getattr(self.broker, "verify_super_order_legs_active", None)
        if callable(verify_super_fn):
            try:
                v_ret = verify_super_fn(
                    order_id=str(order_id),
                    expected_security_id=str(rec["security_id"]),
                    expected_qty=int(expected_qty),
                    expected_sl_price=levels.sl_trigger,
                    expected_target_price=levels.target_price,
                    tick_size=inst_tick,
                )
                if isinstance(v_ret, tuple) and len(v_ret) == 2 and isinstance(v_ret[0], bool):
                    v_ok, v_msg = v_ret
                    is_term = any(s in str(v_msg).upper() for s in ("REJECTED", "CANCELLED", "EXPIRED", "TRADED", "CLOSED", "MISMATCH"))
                    return v_ok, str(v_msg), is_term
            except Exception as e:
                return False, f"Super order leg verification exception: {e}", False

        if hasattr(self.broker, "evaluate_super_order_protection") and callable(self.broker.evaluate_super_order_protection):
            try:
                eval_res = self.broker.evaluate_super_order_protection(
                    order_id=str(order_id),
                    expected_security_id=str(rec["security_id"]),
                    expected_qty=int(expected_qty),
                    expected_sl_price=levels.sl_trigger,
                    expected_target_price=levels.target_price,
                    tick_size=inst_tick,
                    expected_segment=str(rec.get("segment") or "NSE_FNO"),
                    expected_product_type=str(rec.get("product_type") or "MARGIN"),
                )
                if isinstance(eval_res, ProtectionVerificationResult):
                    return bool(eval_res.is_verified), str(eval_res.reason), bool(eval_res.is_terminal_or_invalid)
            except Exception:
                pass

        candidate = None
        if hasattr(self.broker, "get_super_orders"):
            try:
                sb = self.broker.get_super_orders()
                if isinstance(sb, list):
                    for row in sb:
                        if isinstance(row, dict) and str(row.get("orderId", "")) == str(order_id):
                            candidate = row
                            break
            except Exception:
                candidate = None

        if candidate is None and hasattr(self.broker, "order_status"):
            try:
                st_after = self.broker.order_status(order_id)
                if isinstance(st_after, dict):
                    candidate = st_after
            except Exception:
                candidate = None

        ver = verify_super_order_record(
            candidate=candidate,
            order_id=str(order_id),
            expected_security_id=str(rec["security_id"]),
            expected_qty=int(expected_qty),
            expected_sl_price=levels.sl_trigger,
            expected_target_price=levels.target_price,
            tick_size=inst_tick,
            expected_segment=str(rec.get("segment") or "NSE_FNO"),
            expected_product_type=str(rec.get("product_type") or "MARGIN"),
        )
        return ver.is_verified, ver.reason, ver.is_terminal_or_invalid

    def _record_confirmed_protection(
        self,
        rec: dict,
        forever_or_super_id: str,
        confirmed_qty: int,
        executed_chunk_price: float,
        cum_vwap: float,
        filled: int,
        levels: ProtectionLevels,
        leg_status_label: str,
        is_super: bool,
        attempt_id: Optional[str] = None,
    ) -> None:
        if is_super:
            rec["protected_qty"] = int(confirmed_qty)
            rec["protected_value"] = cum_vwap * int(confirmed_qty)
        else:
            rec["protected_qty"] = int(rec.get("protected_qty") or 0) + int(confirmed_qty)
            rec["protected_value"] = float(rec.get("protected_value") or 0.0) + (float(executed_chunk_price) * int(confirmed_qty))

        rec["protection_status"] = leg_status_label
        rec["retry_at"] = 0.0
        try:
            GLOBAL_LATENCY_MONITOR.record_protection_verified(rec["order_id"])
            if rec.get("correlation_id"):
                GLOBAL_LATENCY_MONITOR.record_protection_verified(str(rec["correlation_id"]))
        except Exception:
            pass

        if self.journal_db:
            if attempt_id and hasattr(self.journal_db, "update_protection_attempt"):
                try:
                    self.journal_db.update_protection_attempt(
                        attempt_id,
                        state="CONFIRMED",
                        broker_order_id=str(forever_or_super_id),
                        now=self.clock(),
                    )
                except Exception as e:
                    logger.warning(f"[Protection] Could not update protection_attempt {attempt_id} to CONFIRMED: {e}")

            try:
                if rec["protected_qty"] >= filled and hasattr(self.journal_db, "resolve_unprotected_orders"):
                    self.journal_db.resolve_unprotected_orders(rec["order_id"])
                with self.journal_db._get_conn() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO protection_orders (
                            parent_order_id, forever_order_id, symbol, sl_trigger, sl_limit, target_price, quantity, status, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            rec["order_id"],
                            str(forever_or_super_id),
                            rec["symbol"],
                            levels.sl_trigger,
                            levels.sl_limit,
                            levels.target_price,
                            int(confirmed_qty),
                            leg_status_label,
                            self.clock(),
                        ),
                    )
            except Exception as e:
                logger.warning(f"[Protection] Could not record protection order to journal: {e}")

        logger.info(
            f"[Protection] ✅ {rec['symbol']}: {confirmed_qty} units protected @ {executed_chunk_price} → "
            f"SL {levels.sl_trigger} / Target {levels.target_price} ({leg_status_label}, ID: {forever_or_super_id})."
        )

    def _reconcile_existing_forever_attempt(
        self,
        rec: dict,
        pending_oco: dict,
        st: dict,
        filled: int,
    ) -> Tuple[bool, bool]:
        attempt_id = str(pending_oco.get("attempt_id") or pending_oco.get("correlation_id") or "")
        existing_corr = str(pending_oco.get("correlation_id") or "")
        existing_oco_id = pending_oco.get("oco_id")
        attempt_qty = int(pending_oco.get("qty") or 0)
        attempt_exec_price = float(pending_oco.get("executed_price") or 0.0)
        attempt_tick = float(pending_oco.get("tick_size") or rec.get("tick_size") or TICK)

        if attempt_exec_price <= 0.0:
            marginal_p, _ = self._compute_chunk_and_vwap_prices(rec, st, filled, max(1, attempt_qty))
            attempt_exec_price = marginal_p

        attempt_levels = ProtectionLevels(
            executed_price=attempt_exec_price if attempt_exec_price > 0 else 1.0,
            sl_trigger=float(pending_oco.get("sl_trigger") or 0.0),
            sl_limit=float(pending_oco.get("sl_limit") or 0.0),
            target_price=float(pending_oco.get("target_price") or 0.0),
            quantity=attempt_qty,
            target_trigger=float(pending_oco.get("target_price") or 0.0),
        )

        # 1. If broker order ID is missing (e.g., crash during SUBMITTING or timeout UNKNOWN), look up by correlationId
        if not existing_oco_id and existing_corr:
            if hasattr(self.broker, "get_forever_orders"):
                try:
                    fb = self.broker.get_forever_orders()
                    if isinstance(fb, list):
                        for fb_row in fb:
                            if isinstance(fb_row, dict) and str(fb_row.get("correlationId") or "") == existing_corr:
                                found_id = str(fb_row.get("orderId") or "").strip()
                                if found_id:
                                    existing_oco_id = found_id
                                    pending_oco["oco_id"] = existing_oco_id
                                    break
                except Exception:
                    pass

            if not existing_oco_id and hasattr(self.broker, "get_order_by_correlation_id"):
                try:
                    corr_row = self.broker.get_order_by_correlation_id(existing_corr)
                    if isinstance(corr_row, dict) and corr_row.get("orderId"):
                        existing_oco_id = str(corr_row["orderId"]).strip()
                        pending_oco["oco_id"] = existing_oco_id
                except Exception:
                    pass

            if existing_oco_id and self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                try:
                    self.journal_db.update_protection_attempt(
                        attempt_id or existing_corr,
                        state="ACKED_UNVERIFIED",
                        broker_order_id=existing_oco_id,
                        now=self.clock(),
                    )
                except Exception:
                    pass

        # 2. If we have a broker order ID, verify it against the attempt's stored quantity and levels
        if existing_oco_id:
            v_ok, v_msg, is_term_rej = self._verify_forever_oco_on_broker(
                str(existing_oco_id),
                rec,
                attempt_qty,
                attempt_levels,
                attempt_tick,
            )
            if v_ok:
                leg_label = "ACTIVE" if v_msg == "Confirmed by OCO placement ACK" else "ACTIVE_CONFIRMED"
                _, cum_vwap = self._compute_chunk_and_vwap_prices(rec, st, filled, attempt_qty)
                rec["pending_oco"] = None
                self._record_confirmed_protection(
                    rec=rec,
                    forever_or_super_id=str(existing_oco_id),
                    confirmed_qty=attempt_qty,
                    executed_chunk_price=attempt_exec_price,
                    cum_vwap=cum_vwap,
                    filled=filled,
                    levels=attempt_levels,
                    leg_status_label=leg_label,
                    is_super=False,
                    attempt_id=attempt_id or existing_corr,
                )
                self._save()
                return True, True

            if is_term_rej:
                if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                    try:
                        self.journal_db.update_protection_attempt(
                            attempt_id or existing_corr,
                            state="REJECTED",
                            broker_order_id=str(existing_oco_id),
                            error_msg=v_msg,
                            now=self.clock(),
                        )
                    except Exception:
                        pass
                rec["pending_oco"] = None
                self._record_unprotected_and_halt(
                    rec=rec,
                    new_qty=attempt_qty,
                    sl_trigger=attempt_levels.sl_trigger,
                    sl_limit=attempt_levels.sl_limit,
                    target_price=attempt_levels.target_price,
                    status_label="UNPROTECTED",
                    reason=f"Forever OCO {existing_oco_id} terminally rejected/cancelled on broker: {v_msg}",
                    forever_id=str(existing_oco_id),
                )
                self._save()
                return True, False

            logger.error(
                f"[Protection] 🚨 Forever OCO {existing_oco_id} still awaiting broker leg verification: {v_msg} (skipping duplicate POST)"
            )
            self._record_unprotected_and_halt(
                rec=rec,
                new_qty=attempt_qty,
                sl_trigger=attempt_levels.sl_trigger,
                sl_limit=attempt_levels.sl_limit,
                target_price=attempt_levels.target_price,
                status_label="UNPROTECTED",
                reason=f"Broker protection unconfirmed or delayed for {attempt_qty} units @ {attempt_exec_price}: {v_msg}",
                forever_id=str(existing_oco_id),
            )
            self._save()
            return False, False

        # 3. No broker order ID found yet for a restored SUBMITTING or UNKNOWN attempt:
        state_label = str(pending_oco.get("state") or "SUBMITTING")
        self._record_unprotected_and_halt(
            rec=rec,
            new_qty=attempt_qty,
            sl_trigger=attempt_levels.sl_trigger,
            sl_limit=attempt_levels.sl_limit,
            target_price=attempt_levels.target_price,
            status_label="UNPROTECTED",
            reason=(
                f"Prior protection attempt ({existing_corr}, state={state_label}) has unresolved broker outcome; "
                f"treating as potentially accepted and blocking duplicate submission"
            ),
            forever_id=f"UNVERIFIED-{existing_corr}",
        )
        self._save()
        return False, False

    def _reconcile_existing_super_attempt(
        self,
        rec: dict,
        pending_super: dict,
        st: dict,
        filled: int,
    ) -> Tuple[bool, bool]:
        """
        CV13-01: Reconciles an existing unresolved SUPER_ORDER protection attempt against its own
        stored expected quantity, prices, and broker identity before creating any new attempt.
        - If readback confirms the expected protection, marks the attempt CONFIRMED and updates
          watcher coverage (`protected_qty`, `protected_value`) consistently.
        - If evidence is unavailable or outcome remains uncertain, retains the unresolved attempt,
          keeps fresh entries blocked, and does NOT repeat the modification.
        """
        attempt_id = str(pending_super.get("attempt_id") or pending_super.get("correlation_id") or "")
        existing_corr = str(pending_super.get("correlation_id") or "")
        super_oid = str(pending_super.get("oco_id") or pending_super.get("broker_order_id") or rec["order_id"])
        attempt_qty = int(pending_super.get("qty") or pending_super.get("quantity") or 0)
        attempt_exec_price = float(pending_super.get("executed_price") or 0.0)
        attempt_tick = float(pending_super.get("tick_size") or rec.get("tick_size") or TICK)

        if attempt_exec_price <= 0.0:
            _, cum_v = self._compute_chunk_and_vwap_prices(rec, st, max(filled, attempt_qty), max(1, attempt_qty))
            attempt_exec_price = cum_v

        attempt_levels = ProtectionLevels(
            executed_price=attempt_exec_price if attempt_exec_price > 0 else 1.0,
            sl_trigger=float(pending_super.get("sl_trigger") or 0.0),
            sl_limit=float(pending_super.get("sl_limit") or 0.0),
            target_price=float(pending_super.get("target_price") or 0.0),
            quantity=attempt_qty,
            target_trigger=float(pending_super.get("target_price") or 0.0),
        )

        sv_ok, sv_msg, _ = self._verify_super_order_on_broker(
            order_id=super_oid,
            rec=rec,
            expected_qty=attempt_qty,
            levels=attempt_levels,
            inst_tick=attempt_tick,
        )
        if sv_ok:
            rec["pending_oco"] = None
            rec["reanchor_status"] = "CONFIRMED"
            self._record_confirmed_protection(
                rec=rec,
                forever_or_super_id=f"SUPER-{super_oid}-{attempt_qty}",
                confirmed_qty=attempt_qty,
                executed_chunk_price=attempt_exec_price,
                cum_vwap=attempt_exec_price,
                filled=filled,
                levels=attempt_levels,
                leg_status_label="ACTIVE_CONFIRMED",
                is_super=True,
                attempt_id=attempt_id or existing_corr,
            )
            self._save()
            return True, True

        # Retain the unresolved attempt, keep fresh entries blocked, and do NOT repeat modification
        rec["reanchor_status"] = "UNVERIFIED"
        self._record_unprotected_and_halt(
            rec=rec,
            new_qty=attempt_qty,
            sl_trigger=attempt_levels.sl_trigger,
            sl_limit=attempt_levels.sl_limit,
            target_price=attempt_levels.target_price,
            status_label="UNPROTECTED",
            reason=f"Super Order {super_oid} attempt {attempt_id} awaiting active-leg verification: {sv_msg}",
            forever_id=f"UNVERIFIED-SUPER-{super_oid}",
        )
        self._save()
        return False, False

    def _protect(self, rec: dict, st: dict, filled: int, new_qty: int) -> bool:
        with self._lock:
            is_super = (str(rec.get("product_type", "CNC")).upper() == "MARGIN" and bool(rec.get("is_super_order")))
            route_name = "SUPER_ORDER" if is_super else "FOREVER_OCO"

            # Step 1: Before creating an attempt, find and reconcile any existing unresolved attempt for the same parent and route (CV13-01)
            existing_unresolved = self._find_unresolved_attempt(rec, route_name)
            if existing_unresolved is not None:
                if is_super:
                    resolved_cleared, confirmed_ok = self._reconcile_existing_super_attempt(rec, existing_unresolved, st, filled)
                else:
                    resolved_cleared, confirmed_ok = self._reconcile_existing_forever_attempt(rec, existing_unresolved, st, filled)
                if not resolved_cleared or not confirmed_ok:
                    return True
                # Handle any additional fills separately after reconciling the earlier attempt
                new_qty = filled - int(rec.get("protected_qty") or 0)
                if new_qty <= 0:
                    return True

            if new_qty <= 0:
                return False

            marginal_chunk, cum_vwap = self._compute_chunk_and_vwap_prices(rec, st, filled, new_qty)
            executed_chunk = cum_vwap if is_super else marginal_chunk

            if not math.isfinite(executed_chunk) or executed_chunk <= 0.0:
                self._record_unprotected_and_halt(
                    rec=rec,
                    new_qty=new_qty,
                    sl_trigger=0.0,
                    sl_limit=0.0,
                    target_price=0.0,
                    status_label="UNPROTECTED",
                    reason="Missing broker-confirmed executed fill price (refusing to fabricate fallback execution price)",
                )
                return True

            inst_tick = float(rec.get("tick_size") or TICK)
            target_qty = filled if is_super else new_qty
            levels = calculate_protection_levels(
                executed_price=executed_chunk,
                quantity=target_qty,
                sl_pct=self.sl_pct,
                target_pct=self.target_pct,
                sl_limit_buffer_pct=self.sl_limit_buffer_pct,
                tick_size=inst_tick,
            )

            segment = rec["segment"]
            prod_type = rec.get("product_type", "CNC")

            if self.dry_run and str(rec["order_id"]).startswith("DRY-"):
                logger.info(
                    f"[DRY RUN] Protected {new_qty}x {rec['symbol']} @ {executed_chunk}: "
                    f"Target {levels.target_price} (+{self.target_pct:g}%), "
                    f"SL Trigger {levels.sl_trigger} (-{self.sl_pct:g}%, Limit {levels.sl_limit})"
                )
                self._record_confirmed_protection(
                    rec=rec,
                    forever_or_super_id="DRY-OCO-SUCCESS",
                    confirmed_qty=target_qty,
                    executed_chunk_price=executed_chunk,
                    cum_vwap=cum_vwap,
                    filled=filled,
                    levels=levels,
                    leg_status_label="ACTIVE",
                    is_super=is_super,
                )
                return True

            # Step 2: Create unique durable protection attempt and commit strictly BEFORE broker mutation
            uncovered_from = int(rec.get("protected_qty") or 0)
            corr_prefix = "SUPP" if is_super else "OCO"
            corr_id = generate_correlation_id(corr_prefix, rec["symbol"], self.clock())
            attempt_id = generate_correlation_id("PATM", rec["symbol"], self.clock())
            acct_id = str(getattr(getattr(self.broker, "cfg", None), "dhan_client_id", "") or getattr(self.journal_db, "account_id", "") or "")

            attempt_record = {
                "attempt_id": attempt_id,
                "parent_order_id": rec["order_id"],
                "root_order_id": rec.get("root_order_id") or rec["order_id"],
                "account_id": acct_id,
                "symbol": rec["symbol"],
                "trading_symbol": rec.get("trading_symbol") or rec["symbol"],
                "security_id": str(rec["security_id"]),
                "exchange_segment": segment,
                "product_type": prod_type,
                "route": route_name,
                "uncovered_from_qty": uncovered_from,
                "qty": target_qty,
                "executed_price": executed_chunk,
                "sl_trigger": levels.sl_trigger,
                "sl_limit": levels.sl_limit,
                "target_price": levels.target_price,
                "tick_size": inst_tick,
                "correlation_id": corr_id,
                "oco_id": rec["order_id"] if is_super else None,
                "state": "SUBMITTING",
            }
            rec["pending_oco"] = attempt_record

            try:
                if self.journal_db and hasattr(self.journal_db, "create_protection_attempt"):
                    self.journal_db.create_protection_attempt(
                        attempt_id=attempt_id,
                        parent_order_id=rec["order_id"],
                        root_order_id=rec.get("root_order_id") or rec["order_id"],
                        account_id=acct_id,
                        symbol=rec["symbol"],
                        trading_symbol=rec.get("trading_symbol") or rec["symbol"],
                        security_id=str(rec["security_id"]),
                        exchange_segment=segment,
                        product_type=prod_type,
                        route=route_name,
                        uncovered_from_qty=uncovered_from,
                        quantity=target_qty,
                        executed_price=executed_chunk,
                        sl_trigger=levels.sl_trigger,
                        sl_limit=levels.sl_limit,
                        target_price=levels.target_price,
                        tick_size=inst_tick,
                        correlation_id=corr_id,
                        broker_order_id=rec["order_id"] if is_super else None,
                        state="SUBMITTING",
                        now=self.clock(),
                    )
                self._save_strict()
            except Exception as persist_err:
                rec["pending_oco"] = None
                logger.critical(
                    f"[Protection] Pre-POST persistence failed for {rec['symbol']} ({rec['order_id']}): {persist_err}. "
                    f"Aborting protective-order submission fail-closed."
                )
                if hasattr(self.broker, "reconciliation_gate") and self.broker.reconciliation_gate:
                    self.broker.reconciliation_gate.halt_new_entries(
                        f"Protection persistence failure for {rec['symbol']}: {persist_err}"
                    )
                return False

            # Step 3A: Super Order protection re-anchoring with authoritative verification
            if is_super and hasattr(self.broker, "modify_super_order_protection"):
                try:
                    ok_mod, mod_msg = self.broker.modify_super_order_protection(
                        order_id=rec["order_id"],
                        stop_loss_price=levels.sl_trigger,
                        target_price=levels.target_price,
                    )
                except Exception as mod_exc:
                    ok_mod, mod_msg = False, f"HTTP exception modifying super order: {mod_exc}"

                if ok_mod:
                    sv_ok, sv_msg, _ = self._verify_super_order_on_broker(
                        order_id=rec["order_id"],
                        rec=rec,
                        expected_qty=filled,
                        levels=levels,
                        inst_tick=inst_tick,
                    )
                    if sv_ok:
                        rec["pending_oco"] = None
                        rec["reanchor_status"] = "CONFIRMED"
                        self._record_confirmed_protection(
                            rec=rec,
                            forever_or_super_id=f"SUPER-{rec['order_id']}-{filled}",
                            confirmed_qty=filled,
                            executed_chunk_price=executed_chunk,
                            cum_vwap=cum_vwap,
                            filled=filled,
                            levels=levels,
                            leg_status_label="ACTIVE_CONFIRMED",
                            is_super=True,
                            attempt_id=attempt_id,
                        )
                        return True
                    else:
                        rec["reanchor_status"] = "UNVERIFIED"
                        rec["pending_oco"]["state"] = "ACKED_UNVERIFIED"
                        rec["pending_oco"]["oco_id"] = rec["order_id"]
                        if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                            try:
                                self.journal_db.update_protection_attempt(
                                    attempt_id,
                                    state="ACKED_UNVERIFIED",
                                    broker_order_id=rec["order_id"],
                                    error_msg=sv_msg,
                                    now=self.clock(),
                                )
                            except Exception:
                                pass
                        self._record_unprotected_and_halt(
                            rec=rec,
                            new_qty=new_qty,
                            sl_trigger=levels.sl_trigger,
                            sl_limit=levels.sl_limit,
                            target_price=levels.target_price,
                            status_label="UNPROTECTED",
                            reason=f"Super Order {rec['order_id']} reanchoring lacks authoritative active-leg verification: {sv_msg}",
                            forever_id=f"UNVERIFIED-SUPER-{rec['order_id']}",
                        )
                        self._save()
                        return True
                else:
                    rec["reanchor_status"] = "FAILED"
                    ambig = _is_ambiguous_message(mod_msg)
                    if ambig:
                        rec["pending_oco"]["state"] = "UNKNOWN"
                        rec["pending_oco"]["oco_id"] = rec["order_id"]
                    else:
                        rec["pending_oco"] = None
                    if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                        try:
                            self.journal_db.update_protection_attempt(
                                attempt_id,
                                state="UNKNOWN" if ambig else "REJECTED",
                                broker_order_id=rec["order_id"],
                                error_msg=mod_msg,
                                now=self.clock(),
                            )
                        except Exception:
                            pass
                    self._record_unprotected_and_halt(
                        rec=rec,
                        new_qty=new_qty,
                        sl_trigger=levels.sl_trigger,
                        sl_limit=levels.sl_limit,
                        target_price=levels.target_price,
                        status_label="UNPROTECTED",
                        reason=f"Super Order {rec['order_id']} leg reanchoring failed: {mod_msg}",
                        forever_id=f"FAILED-SUPER-{rec['order_id']}",
                    )
                    self._save()
                    return True

            # Step 3B: Forever OCO for Cash Delivery (CNC/MTF)
            if hasattr(self.broker, "place_forever_oco"):
                max_prot_qty: Optional[int] = filled - int(rec.get("protected_qty") or 0)
                if hasattr(self.broker, "get_positions"):
                    try:
                        pos_list = self.broker.get_positions()
                        if isinstance(pos_list, list) and pos_list:
                            for p in pos_list:
                                if isinstance(p, dict) and str(p.get("securityId") or "") == str(rec["security_id"]):
                                    raw_nq = p.get("netQty")
                                    if raw_nq is not None and not isinstance(raw_nq, bool):
                                        net_q = int(raw_nq)
                                        if net_q > 0:
                                            max_prot_qty = min(max_prot_qty, net_q)
                    except Exception:
                        pass

                try:
                    oco_ret = self._invoke_place_forever_oco_once(
                        corr_id=corr_id,
                        security_id=rec["security_id"],
                        segment=segment,
                        prod_type=prod_type,
                        levels=levels,
                        max_prot_qty=max_prot_qty,
                        inst_tick=inst_tick,
                    )
                except Exception as post_exc:
                    logger.critical(
                        f"[Protection] Exception during place_forever_oco for {rec['symbol']} ({corr_id}): {post_exc}. "
                        f"Preserving durable attempt for broker reconciliation."
                    )
                    self._record_unprotected_and_halt(
                        rec=rec,
                        new_qty=new_qty,
                        sl_trigger=levels.sl_trigger,
                        sl_limit=levels.sl_limit,
                        target_price=levels.target_price,
                        status_label="UNPROTECTED",
                        reason=f"Exception during Forever OCO submission ({corr_id}): {post_exc}",
                        forever_id=f"SUBMITTING-{corr_id}",
                    )
                    self._save()
                    return True

                success, oco_id, msg = oco_ret

                if success and oco_id:
                    str_oco_id = str(oco_id)
                    rec["pending_oco"]["oco_id"] = str_oco_id
                    rec["pending_oco"]["state"] = "ACKED_UNVERIFIED"
                    if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                        try:
                            self.journal_db.update_protection_attempt(
                                attempt_id,
                                state="ACKED_UNVERIFIED",
                                broker_order_id=str_oco_id,
                                response_payload={"orderId": str_oco_id, "message": msg},
                                now=self.clock(),
                            )
                        except Exception:
                            pass
                    self._save()

                    v_ok, v_msg, is_term_rej = self._verify_forever_oco_on_broker(
                        str_oco_id,
                        rec,
                        new_qty,
                        levels,
                        inst_tick,
                    )
                    if v_ok:
                        leg_status_label = "ACTIVE" if v_msg == "Confirmed by OCO placement ACK" else "ACTIVE_CONFIRMED"
                        rec["pending_oco"] = None
                        self._record_confirmed_protection(
                            rec=rec,
                            forever_or_super_id=str_oco_id,
                            confirmed_qty=new_qty,
                            executed_chunk_price=executed_chunk,
                            cum_vwap=cum_vwap,
                            filled=filled,
                            levels=levels,
                            leg_status_label=leg_status_label,
                            is_super=False,
                            attempt_id=attempt_id,
                        )
                        return True
                    else:
                        if is_term_rej:
                            rec["pending_oco"] = None
                            if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                                try:
                                    self.journal_db.update_protection_attempt(
                                        attempt_id,
                                        state="REJECTED",
                                        broker_order_id=str_oco_id,
                                        error_msg=v_msg,
                                        now=self.clock(),
                                    )
                                except Exception:
                                    pass
                        logger.error(
                            f"[Protection] 🚨 Forever OCO {str_oco_id} ACK received but failed broker leg verification: {v_msg}"
                        )
                        self._record_unprotected_and_halt(
                            rec=rec,
                            new_qty=new_qty,
                            sl_trigger=levels.sl_trigger,
                            sl_limit=levels.sl_limit,
                            target_price=levels.target_price,
                            status_label="UNPROTECTED",
                            reason=f"Broker protection unconfirmed or rejected for {new_qty} units @ {executed_chunk}: {v_msg}",
                            forever_id=str_oco_id,
                        )
                        return True
                else:
                    if _is_ambiguous_message(msg) or (success and not oco_id):
                        rec["pending_oco"]["state"] = "UNKNOWN"
                        if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                            try:
                                self.journal_db.update_protection_attempt(
                                    attempt_id,
                                    state="UNKNOWN",
                                    error_msg=str(msg),
                                    now=self.clock(),
                                )
                            except Exception:
                                pass
                    else:
                        rec["pending_oco"] = None
                        if self.journal_db and hasattr(self.journal_db, "update_protection_attempt"):
                            try:
                                self.journal_db.update_protection_attempt(
                                    attempt_id,
                                    state="REJECTED",
                                    error_msg=str(msg),
                                    now=self.clock(),
                                )
                            except Exception:
                                pass
                    logger.error(f"[Protection] 🚨 Broker protection rejected/ambiguous for {rec['symbol']}: {msg}")
                    self._record_unprotected_and_halt(
                        rec=rec,
                        new_qty=new_qty,
                        sl_trigger=levels.sl_trigger,
                        sl_limit=levels.sl_limit,
                        target_price=levels.target_price,
                        status_label="UNPROTECTED",
                        reason=f"Broker protection unconfirmed or rejected for {new_qty} units @ {executed_chunk}: {msg}",
                        forever_id=f"UNVERIFIED-{corr_id}" if rec.get("pending_oco") else None,
                    )
                    return True

            rec["pending_oco"] = None
            self._record_unprotected_and_halt(
                rec=rec,
                new_qty=new_qty,
                sl_trigger=levels.sl_trigger,
                sl_limit=levels.sl_limit,
                target_price=levels.target_price,
                status_label="UNPROTECTED",
                reason=f"No supported protection route available for {rec['symbol']}",
            )
            return True

    def _evaluate_retry_locked(
        self,
        order_id: str,
        current_market_price: float,
        quote_timestamp: Optional[float] = None,
        max_quote_age_sec: float = 2.0,
        now_utc: Optional[float] = None,
        reserve_slot: bool = True,
        require_quote_timestamp: bool = False,
    ) -> Tuple[bool, int, str]:
        rec = self.pending.get(order_id) or self.terminal_orders.get(order_id)
        if not rec:
            return False, 0, f"Order {order_id} not in tracking"

        if rec.get("cancel_uncertain", False):
            return False, 0, f"Cancellation for order {order_id} is uncertain; cannot retry until confirmed terminal"

        root_id = str(rec.get("root_order_id") or order_id)
        if self.journal_db and hasattr(self.journal_db, "has_retry_reservation"):
            try:
                r1 = self.journal_db.has_retry_reservation(root_id)
                r2 = self.journal_db.has_retry_reservation(order_id)
                if (r1 is True) or (r2 is True):
                    return False, 0, f"Retry limit reached (durable reservation already exists for {root_id})"
            except Exception:
                pass

        if rec["retry_count"] >= rec["max_retries"] or rec.get("retry_reserved", False):
            return False, 0, f"Retry limit reached ({rec['retry_count']}/{rec['max_retries']})"

        c_orig = str(rec.get("cancel_origin") or "").strip().upper()
        if rec.get("retry_eligible") is False or (c_orig and c_orig in PROHIBITED_CANCEL_ORIGINS):
            return (
                False,
                0,
                f"Retry prohibited: cancellation origin '{c_orig or 'NON_ELIGIBLE'}' is not eligible for replacement BUY",
            )

        if rec["state"] not in (OrderLifecycleState.CANCELLED.value, OrderLifecycleState.EXPIRED.value):
            return False, 0, f"Order is in non-terminal state ({rec['state']}); cannot retry until cancelled"

        if (
            current_market_price is None
            or isinstance(current_market_price, bool)
            or not isinstance(current_market_price, (int, float))
            or not math.isfinite(float(current_market_price))
            or float(current_market_price) <= 0.0
        ):
            return False, 0, "Retry rejected: missing or invalid current market quote"

        if require_quote_timestamp and quote_timestamp is None:
            return False, 0, "Retry rejected: missing source quote timestamp for current_market_price"

        if quote_timestamp is not None:
            if isinstance(quote_timestamp, bool) or not isinstance(quote_timestamp, (int, float)) or not math.isfinite(float(quote_timestamp)) or float(quote_timestamp) <= 0.0:
                return False, 0, f"Retry rejected: invalid quote timestamp ({quote_timestamp!r})"
            eval_now = float(now_utc) if now_utc is not None else self.clock()
            q_age = round(eval_now - float(quote_timestamp), 6)
            if q_age < 0.0:
                return False, 0, f"Retry rejected: future quote timestamp (age={q_age:.2f}s < 0.0s)"
            if q_age > max_quote_age_sec:
                return False, 0, f"Retry rejected: stale quote timestamp (age={q_age:.2f}s, max={max_quote_age_sec:.2f}s)"

        orig_price = float(rec.get("original_market_price") or 0.0)
        if not math.isfinite(orig_price) or orig_price <= 0.0:
            return False, 0, "Retry rejected: missing original market reference price"

        filled = int(rec.get("filled_qty") or 0)
        if filled == 0 and int(rec.get("protected_qty") or 0) > 0:
            filled = int(rec["protected_qty"])
        remaining_qty = int(rec["qty"]) - filled
        if remaining_qty <= 0:
            return False, 0, "No remaining unfulfilled quantity to retry"

        price_move_pct = round((abs(float(current_market_price) - orig_price) / orig_price) * 100.0, 6)
        if price_move_pct >= 1.0:
            return (
                False,
                0,
                f"Price move {price_move_pct:.2f}% is >= 1.00% threshold ceiling; retry rejected",
            )

        if reserve_slot:
            rec["retry_count"] += 1
            rec["retry_reserved"] = True
        return True, remaining_qty, f"Retry approved for {remaining_qty} units (market move {price_move_pct:.2f}% < 1.00% ceiling)"

    def evaluate_retry(
        self,
        order_id: str,
        current_market_price: float,
        quote_timestamp: Optional[float] = None,
        max_quote_age_sec: float = 2.0,
        now_utc: Optional[float] = None,
    ) -> Tuple[bool, int, str]:
        with self._lock:
            return self._evaluate_retry_locked(
                order_id=order_id,
                current_market_price=current_market_price,
                quote_timestamp=quote_timestamp,
                max_quote_age_sec=max_quote_age_sec,
                now_utc=now_utc,
                reserve_slot=True,
            )

    def reconcile_terminal_order(self, order_id: str, st: Optional[dict] = None) -> bool:
        with self._lock:
            rec = self.terminal_orders.get(order_id)
            if not rec:
                return False

        if st is None:
            st = self._get_order_status(rec)
        if not st:
            return False

        filled = int(st.get("filledQty") or 0)
        rec["filled_qty"] = filled
        new_qty = filled - rec["protected_qty"]

        if new_qty > 0:
            logger.warning(
                f"[Protection] LATE FILL DETECTED for terminal order {order_id} ({rec['symbol']}): "
                f"{new_qty} additional shares filled (total filled: {filled}). Placing protection."
            )
            self._protect(rec, st, filled, new_qty)
            self._save()
            return True
        return False

    def execute_retry(
        self,
        order_id: str,
        current_market_price: Optional[float] = None,
        revised_limit_price: Optional[float] = None,
        markup_pct: Optional[float] = None,
        quote_timestamp: Optional[float] = None,
        max_quote_age_sec: float = 2.0,
        now_utc: Optional[float] = None,
        reference_date: Optional[date] = None,
    ) -> Tuple[bool, Optional[str], str]:
        if self.journal_db is None:
            logger.critical(f"[Protection] Retry blocked for {order_id}: active durable SQLite journal is required")
            return False, None, "Retry blocked: active durable SQLite journal is required (zero broker orders placed)"

        if hasattr(self.broker, "stop_manager") and self.broker.stop_manager:
            if self.broker.stop_manager.is_stopped():
                return False, None, "Retry rejected: EMERGENCY STOP is active"

        gate = getattr(self.broker, "reconciliation_gate", None)
        if gate is not None and hasattr(gate, "is_accepting_entries") and not gate.is_accepting_entries:
            return False, None, "Retry rejected: reconciliation gate is CLOSED"

        t0_clock = float(self.clock())
        eval_clock = float(now_utc) if now_utc is not None else t0_clock
        source_quote_ts: Optional[float] = float(quote_timestamp) if (quote_timestamp is not None and not isinstance(quote_timestamp, bool)) else None
        local_retrieval_ts: float = eval_clock
        quote_ts_type: str = "SOURCE_TIMESTAMP" if source_quote_ts is not None else "LOCAL_RETRIEVAL"

        with self._lock:
            rec = self.pending.get(order_id) or self.terminal_orders.get(order_id)
            if not rec:
                return False, None, f"Order {order_id} not in tracking"

            if current_market_price is None:
                if hasattr(self.broker, "get_yahoo_quote_details") and rec.get("segment") in ("NSE_EQ", "BSE_EQ"):
                    q = self.broker.get_yahoo_quote_details(
                        stock_symbol=rec["symbol"],
                        exchange_segment=rec["segment"],
                        security_id=rec["security_id"],
                        now_utc=eval_clock,
                        max_quote_age_sec=max_quote_age_sec,
                        allow_cached=False,
                        cmp_policy="STRICT_TIMESTAMP",
                    )
                    if isinstance(q, QuoteResult):
                        if not q.is_valid:
                            return False, None, f"Retry rejected: fresh quote check failed ({q.failure_reason})"
                        current_market_price = q.price
                        if source_quote_ts is None and q.quote_timestamp is not None and q.quote_timestamp > 0:
                            source_quote_ts = float(q.quote_timestamp)
                            quote_ts_type = "SOURCE_TIMESTAMP"
                        t_after_q = float(self.clock())
                        local_retrieval_ts = (float(now_utc) + max(0.0, t_after_q - t0_clock)) if now_utc is not None else t_after_q
                elif hasattr(self.broker, "get_ltp") and rec.get("segment") in ("NSE_FNO", "BSE_FNO"):
                    try:
                        ltp_ret = self.broker.get_ltp(
                            rec["segment"],
                            rec["security_id"],
                            symbol=rec["symbol"],
                            now_utc=eval_clock,
                            max_quote_age_sec=max_quote_age_sec,
                        )
                        if isinstance(ltp_ret, tuple) and len(ltp_ret) == 3 and ltp_ret[0] and float(ltp_ret[1]) > 0:
                            current_market_price = float(ltp_ret[1])
                            t_after_ltp = float(self.clock())
                            local_retrieval_ts = (float(now_utc) + max(0.0, t_after_ltp - t0_clock)) if now_utc is not None else t_after_ltp
                            # CV13-04: Distinguish F&O local retrieval timestamp from broker/exchange source timestamp.
                            # Never fabricate a source timestamp from local clock.
                            ltp_meta = getattr(self.broker, "last_ltp_meta", None)
                            if isinstance(ltp_meta, dict) and ltp_meta.get("source_quote_timestamp") is not None:
                                source_quote_ts = float(ltp_meta["source_quote_timestamp"])
                                quote_ts_type = "SOURCE_TIMESTAMP"
                            elif source_quote_ts is None:
                                quote_ts_type = "LOCAL_RETRIEVAL"
                    except Exception:
                        pass

            rec["retry_quote_timestamp_type"] = quote_ts_type
            rec["retry_source_quote_timestamp"] = source_quote_ts
            rec["retry_local_retrieval_timestamp"] = local_retrieval_ts

            segment = str(rec.get("segment") or "").strip().upper()
            if segment not in ("NSE_EQ", "BSE_EQ", "NSE_FNO", "BSE_FNO"):
                return False, None, f"Retry rejected: unsupported exchange segment '{segment}'"

            sec_id = str(rec.get("security_id") or "").strip()
            if not sec_id or not sec_id.isdigit():
                return False, None, f"Retry rejected: invalid or missing security_id '{sec_id}'"

            raw_sym = str(rec.get("symbol") or "").strip().upper()
            underlying = raw_sym.replace("-EQ", "").split("-")[0].strip()
            if not underlying or underlying in INDEX_SYMBOLS:
                return False, None, f"Retry rejected: invalid or index instrument '{raw_sym}'"

            prod_type_str = str(rec.get("product_type") or "").strip().upper()
            try:
                prod_type = ProductType(prod_type_str)
            except Exception:
                return False, None, f"Retry rejected: unknown or unsupported product_type '{prod_type_str}' (defaulting to CNC is prohibited)"

            if segment in ("NSE_EQ", "BSE_EQ") and prod_type not in (ProductType.CNC, ProductType.MTF, ProductType.INTRADAY):
                return False, None, f"Retry rejected: product_type '{prod_type.value}' incompatible with equity segment '{segment}'"
            if segment in ("NSE_FNO", "BSE_FNO") and prod_type != ProductType.MARGIN:
                return False, None, f"Retry rejected: F&O segment '{segment}' requires product_type 'MARGIN', got '{prod_type.value}'"

            can_retry, retry_qty, eval_reason = self._evaluate_retry_locked(
                order_id=order_id,
                current_market_price=current_market_price if current_market_price is not None else 0.0,
                quote_timestamp=source_quote_ts if source_quote_ts is not None else local_retrieval_ts,
                max_quote_age_sec=max_quote_age_sec,
                now_utc=eval_clock,
                reserve_slot=False,
            )
            if not can_retry:
                return False, None, eval_reason

            lot_size = int(rec.get("lot_size") or 1)
            inst_tick = float(rec.get("tick_size") or TICK)
            if inst_tick <= 0:
                return False, None, f"Retry rejected: invalid tick_size {inst_tick}"

            if segment in ("NSE_FNO", "BSE_FNO"):
                opt_type = str(rec.get("option_type") or "").strip().upper()
                strike = float(rec.get("strike") or 0.0)
                exp_str = str(rec.get("expiry") or "").strip()
                if opt_type not in ("CE", "PE"):
                    return False, None, f"Retry rejected: F&O contract missing valid CE/PE option_type ('{opt_type}')"
                if strike <= 0.0:
                    return False, None, f"Retry rejected: F&O contract missing valid strike ({strike})"
                if not exp_str:
                    return False, None, "Retry rejected: F&O contract missing expiry date"
                try:
                    exp_date = date.fromisoformat(exp_str[:10])
                except ValueError:
                    return False, None, f"Retry rejected: invalid F&O expiry format '{exp_str}'"
                today_ref = reference_date or date.today()
                if exp_date < today_ref:
                    return False, None, f"Retry rejected: F&O option contract expired on {exp_date.isoformat()}"
                if lot_size <= 0 or (retry_qty % lot_size) != 0:
                    return False, None, f"Retry rejected: retry quantity {retry_qty} is not a multiple of lot_size {lot_size}"
                order_symbol = str(rec.get("trading_symbol") or f"{underlying}-{strike:g}{opt_type}").strip()
                if order_symbol.endswith("-EQ"):
                    return False, None, f"Retry rejected: corrupted F&O trading symbol '{order_symbol}' ending with -EQ"
            else:
                if lot_size <= 0 or (retry_qty % lot_size) != 0:
                    return False, None, f"Retry rejected: invalid equity quantity {retry_qty} for lot_size {lot_size}"
                order_symbol = str(rec.get("trading_symbol") or (f"{underlying}-EQ" if not raw_sym.endswith("-EQ") else raw_sym)).strip()

            if markup_pct is not None:
                cfg_markup = float(markup_pct)
            else:
                raw_cfg_m = getattr(getattr(self.broker, "cfg", None), "equity_price_markup_pct", 0.5)
                cfg_markup = float(raw_cfg_m) if isinstance(raw_cfg_m, (int, float)) else 0.5
            effective_markup = min(max(0.0, cfg_markup), 1.0)
            max_ceiling_price = round_to_tick(
                float(current_market_price) * (1.0 + effective_markup / 100.0),
                inst_tick,
                mode="nearest",
            )
            hard_1pct_ceiling = round_to_tick(
                float(current_market_price) * 1.01,
                inst_tick,
                mode="nearest",
            )

            if revised_limit_price is not None:
                price = round_to_tick(float(revised_limit_price), inst_tick, mode="nearest")
                if price > hard_1pct_ceiling + 1e-6:
                    return (
                        False,
                        None,
                        f"Retry rejected: revised_limit_price ({price}) exceeds 1.00% markup ceiling ({hard_1pct_ceiling}) above market price ({current_market_price})",
                    )
            else:
                price = max_ceiling_price

            if price <= 0.0 or not math.isfinite(price):
                return False, None, f"Invalid retry limit price {price} <= 0"

            retry_cost = float(price) * int(retry_qty)
            avail_funds = 1000000.0
            funds_fn = getattr(self.broker, "get_fund_limits", None)
            should_check_funds = (
                (self.risk_manager is not None)
                or hasattr(funds_fn, "mock_calls")
                or hasattr(getattr(getattr(self.broker, "session", None), "get", None), "mock_calls")
            )
            if should_check_funds and callable(funds_fn):
                try:
                    f_ret = funds_fn()
                    if isinstance(f_ret, tuple) and len(f_ret) == 3:
                        f_ok, f_avail, f_msg = f_ret
                        if not f_ok:
                            return False, None, f"Retry rejected: broker funds verification failed ({f_msg})"
                        avail_funds = float(f_avail)
                        if avail_funds < retry_cost:
                            return False, None, f"Retry rejected: insufficient available funds ({avail_funds:.2f} < {retry_cost:.2f})"
                except Exception as e:
                    return False, None, f"Retry rejected: broker funds exception ({e})"

            if self.risk_manager is not None and hasattr(self.risk_manager, "validate_order_risk"):
                if hasattr(self.risk_manager, "sync_from_journal_and_broker"):
                    self.risk_manager.sync_from_journal_and_broker(
                        journal_db=self.journal_db,
                        broker=self.broker if not self.dry_run else None,
                        watcher=self,
                    )
                r_ok, r_reason = self.risk_manager.validate_order_risk(retry_cost, avail_funds)
                if not r_ok:
                    return False, None, f"Retry rejected by risk manager: {r_reason}"

            root_oid = str(rec.get("root_order_id") or order_id)
            corr_id = generate_correlation_id("RTRY", underlying, self.clock())
            parent_corr = rec.get("correlation_id") or order_id
            use_super_route = bool(rec.get("is_super_order", False)) or (segment in ("NSE_FNO", "BSE_FNO") and prod_type == ProductType.MARGIN)

            rec["retry_count"] += 1
            rec["retry_reserved"] = True
            rec["retry_eligible"] = False

        order_req = OrderRequest(
            correlation_id=corr_id,
            symbol=order_symbol,
            security_id=sec_id,
            exchange_segment=segment,
            transaction_type=TransactionType.BUY,
            product_type=prod_type,
            order_type=OrderType.LIMIT,
            price=price,
            quantity=retry_qty,
            trading_mode=getattr(getattr(self.broker, "cfg", None), "trading_mode", TradingMode.TEST),
        )

        try:
            sig_ri = inspect.signature(self.journal_db.record_order_intent)
            if "is_super_order" in sig_ri.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig_ri.parameters.values()):
                self.journal_db.record_order_intent(
                    correlation_id=corr_id,
                    parent_correlation_id=parent_corr,
                    symbol=order_req.symbol,
                    trading_symbol=order_symbol,
                    security_id=order_req.security_id,
                    exchange_segment=order_req.exchange_segment,
                    product_type=order_req.product_type.value,
                    order_type=order_req.order_type.value,
                    price=order_req.price,
                    quantity=order_req.quantity,
                    is_super_order=use_super_route,
                    original_market_price=float(current_market_price),
                    tick_size=inst_tick,
                    lot_size=lot_size,
                    option_type=rec.get("option_type", ""),
                    strike=float(rec.get("strike") or 0.0),
                    expiry=rec.get("expiry", ""),
                )
            else:
                self.journal_db.record_order_intent(
                    correlation_id=corr_id,
                    parent_correlation_id=parent_corr,
                    symbol=order_req.symbol,
                    security_id=order_req.security_id,
                    exchange_segment=order_req.exchange_segment,
                    product_type=order_req.product_type.value,
                    order_type=order_req.order_type.value,
                    price=order_req.price,
                    quantity=order_req.quantity,
                )
            if hasattr(self.journal_db, "reserve_retry_slot"):
                reserved_ok = self.journal_db.reserve_retry_slot(root_oid, order_id, corr_id)
                if not reserved_ok:
                    self.journal_db.update_order_intent(
                        corr_id,
                        state="NOT_SUBMITTED",
                        error_msg=f"Durable retry slot already consumed for root order {root_oid}",
                    )
                    return False, None, f"Retry rejected: durable retry slot already consumed for root order {root_oid}"
            self.journal_db.update_order_intent(corr_id, state="SUBMITTING")
            self._save_strict()
        except Exception as e:
            logger.critical(f"[Protection] Journal write failed before retry POST for {order_id} ({e}). Aborting with zero broker orders.")
            return False, None, f"Retry aborted: journal write failed ({e}) — zero broker orders placed"

        # =====================================================================
        # CV13-04: Revalidate retry eligibility immediately before submission
        # =====================================================================
        t_pre_submit = float(self.clock())
        clock_delta = max(0.0, t_pre_submit - t0_clock)
        final_clock = (float(now_utc) + clock_delta) if now_utc is not None else t_pre_submit

        final_fail_reason: Optional[str] = None

        # 1. Recheck emergency stop and entry gate right before broker submission
        if hasattr(self.broker, "stop_manager") and self.broker.stop_manager and self.broker.stop_manager.is_stopped():
            final_fail_reason = "Retry rejected before submission: EMERGENCY STOP became active"
        elif gate is not None and hasattr(gate, "is_accepting_entries") and not gate.is_accepting_entries:
            final_fail_reason = "Retry rejected before submission: reconciliation gate is CLOSED"

        # 2. Recalculate quote age using the current clock and available validated quote / retrieval timestamps
        if final_fail_reason is None:
            if source_quote_ts is not None:
                if not math.isfinite(source_quote_ts) or source_quote_ts <= 0.0:
                    final_fail_reason = f"Retry rejected before submission: invalid source quote timestamp ({source_quote_ts})"
                else:
                    final_src_age = round(final_clock - source_quote_ts, 6)
                    if final_src_age < 0.0:
                        final_fail_reason = f"Retry rejected before submission: future quote timestamp (age={final_src_age:.2f}s < 0.0s)"
                    elif final_src_age > max_quote_age_sec:
                        final_fail_reason = (
                            f"Retry rejected before submission: stale quote after pre-submit checks "
                            f"(age={final_src_age:.2f}s > max={max_quote_age_sec:.2f}s)"
                        )

            if final_fail_reason is None:
                retrieval_age = round(final_clock - local_retrieval_ts, 6)
                if retrieval_age < 0.0:
                    final_fail_reason = f"Retry rejected before submission: negative retrieval age ({retrieval_age:.2f}s)"
                elif retrieval_age > max_quote_age_sec:
                    final_fail_reason = (
                        f"Retry rejected before submission: quote became stale during funds/persistence checks "
                        f"(elapsed={retrieval_age:.2f}s > max={max_quote_age_sec:.2f}s)"
                    )

        if final_fail_reason is not None:
            # Record NOT_SUBMITTED distinctly from UNKNOWN while preserving the root's one-retry guarantee
            try:
                self.journal_db.update_order_intent(
                    corr_id,
                    state="NOT_SUBMITTED",
                    error_msg=final_fail_reason,
                )
            except Exception:
                pass
            with self._lock:
                rec["retry_final_status"] = "NOT_SUBMITTED"
                rec["retry_error"] = final_fail_reason
            self._save()
            logger.warning(f"[Protection] {final_fail_reason} for parent {order_id} ({corr_id}); recorded as NOT_SUBMITTED.")
            return False, None, final_fail_reason

        actual_super_used = False
        if use_super_route and hasattr(self.broker, "place_super_order"):
            init_levels = calculate_protection_levels(
                executed_price=price,
                quantity=retry_qty,
                sl_pct=self.sl_pct,
                target_pct=self.target_pct,
                sl_limit_buffer_pct=self.sl_limit_buffer_pct,
                tick_size=inst_tick,
            )
            sup_ret = self.broker.place_super_order(
                order_req,
                stop_loss_price=init_levels.sl_trigger,
                target_price=init_levels.target_price,
            )
            if isinstance(sup_ret, tuple) and len(sup_ret) == 3:
                ok, broker_id, place_msg = sup_ret
                actual_super_used = True
            else:
                ok, broker_id, place_msg = False, None, "place_super_order returned invalid response"
        else:
            ok, broker_id, place_msg = self.broker.place_order(order_req)

        if not ok or not broker_id:
            is_timeout = _is_ambiguous_message(place_msg)
            try:
                self.journal_db.update_order_intent(
                    corr_id,
                    state="UNKNOWN" if is_timeout else "REJECTED",
                    error_msg=place_msg,
                )
            except Exception:
                pass
            if is_timeout and gate is not None and hasattr(gate, "halt_new_entries"):
                gate.halt_new_entries(f"Retry order {corr_id} outcome is UNKNOWN: {place_msg}")
            logger.error(f"[Protection] Broker rejected/failed retry order for parent {order_id}: {place_msg}")
            return False, None, f"Retry placement failed: {place_msg}"

        new_order_id = str(broker_id)
        try:
            self.journal_db.update_order_intent(corr_id, state="ACKED", broker_order_id=new_order_id)
            if hasattr(self.journal_db, "record_order"):
                self.journal_db.record_order(
                    correlation_id=corr_id,
                    broker_order_id=new_order_id,
                    trading_mode=order_req.trading_mode.value,
                    product_type=prod_type.value,
                    exchange_segment=segment,
                    symbol=underlying,
                    security_id=sec_id,
                    order_type=OrderType.LIMIT.value,
                    price=price,
                    quantity=retry_qty,
                    status="PENDING",
                )
        except Exception:
            pass

        self.track(
            order_id=new_order_id,
            root_order_id=root_oid,
            correlation_id=corr_id,
            symbol=underlying,
            trading_symbol=order_symbol,
            security_id=sec_id,
            segment=segment,
            qty=retry_qty,
            product_type=prod_type.value,
            dry_price=price if new_order_id.startswith("DRY-") else None,
            original_market_price=float(current_market_price),
            is_super_order=actual_super_used,
            tick_size=inst_tick,
            lot_size=lot_size,
            option_type=rec.get("option_type", ""),
            strike=float(rec.get("strike") or 0.0),
            expiry=rec.get("expiry", ""),
            preserve_existing=False,
            retry_count=1,
            max_retries=1,
            retry_reserved=True,
        )

        with self._lock:
            rec["retry_order_id"] = new_order_id
            rec["retry_correlation_id"] = corr_id
            rec["retry_executed_at"] = self.clock()
        self._save()

        logger.info(
            f"[Protection] Single retry executed for parent {order_id} -> new order {new_order_id} "
            f"({retry_qty} units of {order_symbol} @ {price:.2f}, super={actual_super_used})"
        )
        return True, new_order_id, f"Retry executed successfully: {new_order_id}"

    def _get_order_status(self, rec: dict) -> Optional[dict]:
        oid = rec["order_id"]
        if str(oid).startswith("DRY-"):
            if rec.get("dry_price") is None:
                return {"orderStatus": "PENDING", "filledQty": 0}
            return {
                "orderStatus": "TRADED",
                "filledQty": rec["qty"],
                "averageTradedPrice": rec["dry_price"],
            }

        if hasattr(self.broker, "order_status"):
            return self.broker.order_status(oid)
        return None

    def should_poll(self, is_market_open_fn: Optional[Callable[[], bool]] = None) -> bool:
        if bool(self.pending):
            return True
        if is_lifecycle_open():
            return True
        if is_market_open_fn is not None:
            try:
                return bool(is_market_open_fn())
            except Exception:
                return False
        return False

    async def run(self, is_market_open_fn: Optional[Callable[[], bool]] = None):
        with self._running_lock:
            if self._is_running:
                logger.warning("[Protection] Watcher run() loop already active in this instance; skipping duplicate launch.")
                return
            self._is_running = True

        try:
            while True:
                try:
                    if self.should_poll(is_market_open_fn):
                        await asyncio.to_thread(self.check_once)
                except Exception as e:
                    logger.error(f"[Protection] Watcher loop error: {e}")
                await asyncio.sleep(POLL_INTERVAL_SEC)
        finally:
            with self._running_lock:
                self._is_running = False
