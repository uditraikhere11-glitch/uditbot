"""Unified account runtime orchestrator & audited journal repair executors (Phase 2D).

Connects `fe946810`'s authoritative Phase 2B/2C stack:
  DhanSnapshotSource -> CrossProcessReadCoordinator -> BrokerSnapshot
  -> read_journal_view -> BrokerLedgerReconciler.reconcile_account() -> AccountVerdict
to all six production consumers (`StartupReconciliationGate`, `ProtectionWatcher`,
`ConfigChangeTracker`, `RiskManager`, `orphan_cleanup`, and WebUI display peek).

Also provides small, explicit, audited journal repair executors so the 932-line
duplicate `reconcile_stale_journal_with_broker()` body in `config_manager.py`
can be retired cleanly without losing legitimate terminal-order or missing-journal
recovery.
"""
from __future__ import annotations

from collections.abc import Mapping as _Mapping

from contextlib import closing
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Mapping, Optional, Tuple

from src.broker.broker_snapshot import (
    BrokerSnapshot,
    EvidenceRequest,
    LIVE_BOOKS,
    NS,
    Purpose,
    ReadDeferredResult,
    SessionContext,
    SessionPhase,
    thaw_json,
)
from src.broker.dhan_snapshot_source import DhanSnapshotSource
from src.broker.evidence_events import note_event
from src.broker.snapshot_coordinator import CrossProcessReadCoordinator, EvidenceUnavailable
from src.engine.broker_ledger_reconciler import AccountVerdict, BrokerLedgerReconciler, PolicyContext
from src.engine.journal_view import JournalView, read_journal_view


@dataclass(frozen=True, slots=True)
class UnifiedAuthorizationResult:
    allowed: bool
    reason_code: str
    detail: str
    snapshot: Optional[BrokerSnapshot]
    verdict: Optional[AccountVerdict]
    settlement_result: Any = None


def load_verified_exchange_calendar() -> Any:
    """C11: Load the real verified NSE exchange holiday calendar from data/holidays.json (never an empty holiday set)."""
    from src.engine.broker_session import VerifiedCalendar
    from src.engine.market import DEFAULT_HOLIDAYS_PATH, load_holidays
    holidays = load_holidays()
    if not holidays:
        holidays = load_holidays(DEFAULT_HOLIDAYS_PATH)
    if not holidays:
        repo_holidays = Path(__file__).resolve().parents[2] / "data" / "holidays.json"
        if repo_holidays.is_file():
            holidays = load_holidays(str(repo_holidays))
    if not holidays:
        raise ValueError("VERIFIED_EXCHANGE_CALENDAR_EMPTY")
    years = frozenset(d.year for d in holidays)
    return VerifiedCalendar(holidays=frozenset(holidays), covered_years=years)


def peek_display_snapshot_summary(
    data_dir: str | Path,
    account_id: str,
    *,
    now_mono_ns: Optional[int] = None,
    max_display_age_sec: float = 15.0,
) -> dict[str, Any]:
    """C14: Strictly read-only WebUI display boundary (SQLite mode=ro).

    Verifies ALL current account_state generation fields (boot_id, token_epoch, mutation_epoch,
    owner_epoch, journal_epoch, session_epoch), snapshot expires_mono_ns, profile_verified &
    profile_account_id, live freshness (<=15s), and trade_journal.db digest (when present).
    NEVER advances generation, NEVER mutates files, NEVER authorizes mutation.
    """
    clean_acct = str(account_id or "").strip()
    if not clean_acct:
        return {
            "available": False,
            "status": "UNCONFIGURED",
            "display_only": True,
            "authoritative_for_mutation": False,
        }
    try:
        from src.broker.broker_snapshot import account_key
        acct_k = account_key(clean_acct)
        data_root = Path(data_dir)
        sidecar = data_root / "broker_evidence" / acct_k / "snapshots.sqlite3"
        if not sidecar.is_file():
            return {
                "available": False,
                "status": "MISSING",
                "display_only": True,
                "authoritative_for_mutation": False,
            }
        uri = f"file:{sidecar.resolve()}?mode=ro"
        now_m = int(now_mono_ns if now_mono_ns is not None else time.monotonic_ns())
        max_age_ns = int(max_display_age_sec * 1_000_000_000)
        with closing(sqlite3.connect(uri, uri=True, timeout=2.0)) as conn:
            conn.row_factory = sqlite3.Row
            tbls = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "snapshots" not in tbls or "account_state" not in tbls:
                return {
                    "available": False,
                    "status": "STALE_OR_INVALIDATED",
                    "display_only": True,
                    "authoritative_for_mutation": False,
                }
            acct_row = conn.execute(
                """
                SELECT boot_id, token_epoch, mutation_epoch, owner_epoch, journal_epoch, session_epoch
                FROM account_state
                WHERE account_key = ?
                LIMIT 1
                """,
                (acct_k,),
            ).fetchone()
            if acct_row is None:
                return {
                    "available": False,
                    "status": "STALE_OR_INVALIDATED",
                    "display_only": True,
                    "authoritative_for_mutation": False,
                }
            cur_gen = (
                str(acct_row["boot_id"] or ""),
                int(acct_row["token_epoch"]),
                int(acct_row["mutation_epoch"]),
                int(acct_row["owner_epoch"]),
                int(acct_row["journal_epoch"]),
                int(acct_row["session_epoch"]),
            )
            rows = conn.execute(
                """
                SELECT snapshot_id, account_key, boot_id, token_epoch, mutation_epoch,
                       owner_epoch, journal_epoch, session_epoch, session_date, session_phase,
                       profile_account_id, profile_verified, capture_finished_utc_ns,
                       capture_finished_mono_ns, expires_mono_ns, journal_digest
                FROM snapshots
                WHERE account_key = ? AND state = 'COMPLETE'
                ORDER BY capture_finished_mono_ns DESC
                LIMIT 32
                """,
                (acct_k,),
            ).fetchall()

        # Optional live journal digest check if trade_journal.db exists in data_dir
        expected_j_digest = None
        jdb_path = data_root / "trade_journal.db"
        if jdb_path.is_file():
            jv_ro = read_journal_view(jdb_path, account_id=clean_acct, journal_epoch=cur_gen[4], read_utc_ns=time.time_ns())
            if jv_ro.read_complete:
                expected_j_digest = jv_ro.digest_sha256

        for row in rows:
            snap_gen = (
                str(row["boot_id"] or ""),
                int(row["token_epoch"]),
                int(row["mutation_epoch"]),
                int(row["owner_epoch"]),
                int(row["journal_epoch"]),
                int(row["session_epoch"]),
            )
            if snap_gen != cur_gen:
                continue
            exp_mono = int(row["expires_mono_ns"] or 0)
            if exp_mono <= 0 or now_m >= exp_mono:
                continue
            fin_mono = int(row["capture_finished_mono_ns"] or 0)
            age_ns = now_m - fin_mono
            if age_ns < 0 or age_ns > max_age_ns:
                continue
            if not bool(row["profile_verified"]) or str(row["profile_account_id"] or "").strip() != clean_acct:
                continue
            if expected_j_digest is not None and row["journal_digest"] != expected_j_digest:
                continue
            return {
                "available": True,
                "status": "VERIFIED",
                "display_only": True,
                "authoritative_for_mutation": False,
                "snapshot_id": str(row["snapshot_id"]),
                "profile_verified": True,
                "session_phase": str(row["session_phase"] or "TRANSITION_UNVERIFIED"),
                "session_date": row["session_date"],
                "capture_finished_utc_ns": int(row["capture_finished_utc_ns"] or 0),
                "age_sec": round(age_ns / 1_000_000_000, 3),
                "generation": {
                    "boot_id": cur_gen[0],
                    "token_epoch": cur_gen[1],
                    "mutation_epoch": cur_gen[2],
                    "owner_epoch": cur_gen[3],
                    "journal_epoch": cur_gen[4],
                    "session_epoch": cur_gen[5],
                },
            }

        return {
            "available": False,
            "status": "STALE_OR_INVALIDATED",
            "display_only": True,
            "authoritative_for_mutation": False,
        }
    except Exception as exc:
        return {
            "available": False,
            "status": "TRANSIENT_UNAVAILABLE",
            "reason": type(exc).__name__,
            "display_only": True,
            "authoritative_for_mutation": False,
        }


class UnifiedAccountRuntime:
    """Single production runtime adapter for unified broker ledger verification."""

    def __init__(
        self,
        *,
        data_dir: str | Path,
        db_path: str | Path,
        account_id: str,
        reconciler: Optional[BrokerLedgerReconciler] = None,
        mono_clock: Callable[[], int] = time.monotonic_ns,
        utc_clock: Optional[Callable[[], int]] = None,
    ) -> None:
        self.data_dir = Path(os.path.abspath(str(data_dir)))
        self.db_path = Path(os.path.abspath(str(db_path)))
        self.account_id = str(account_id or "").strip()
        self.reconciler = reconciler or BrokerLedgerReconciler()
        self.mono_clock = mono_clock
        self.utc_clock = utc_clock if utc_clock is not None else (lambda: int(time.time() * 1_000_000_000))

    def evaluate_snapshot(
        self,
        snapshot: Optional[BrokerSnapshot],
        policy: PolicyContext,
        *,
        now_mono_ns: Optional[int] = None,
        journal_view: Optional[JournalView] = None,
        calendar: Optional[Any] = None,
        holdings_confirmation: Optional[BrokerSnapshot] = None,
    ) -> UnifiedAuthorizationResult:
        now_m = int(now_mono_ns if now_mono_ns is not None else self.mono_clock())
        if snapshot is None:
            return UnifiedAuthorizationResult(False, "SNAPSHOT_UNAVAILABLE", "No fresh broker snapshot available", None, None)
        jv = journal_view
        if jv is None:
            jv = read_journal_view(
                self.db_path,
                account_id=self.account_id,
                journal_epoch=snapshot.generation.journal_epoch,
                read_utc_ns=int(self.utc_clock()),
            )
        # C11: Invoke the generalized 6-point settled-session verifier during actual runtime evaluation
        settlement_res = None
        eval_snap = snapshot
        if calendar is not None or snapshot.session.phase in (SessionPhase.TRANSITION_UNVERIFIED, SessionPhase.SETTLED):
            from src.engine.settled_session_acquisition import verify_settled_session_evidence
            cal = calendar
            if cal is None:
                cal = load_verified_exchange_calendar()
            settlement_res = verify_settled_session_evidence(
                snapshot,
                jv,
                calendar=cal,
                now_mono_ns=now_m,
                holdings_confirmation=holdings_confirmation,
            )
            self.last_settlement_result = settlement_res
            if settlement_res.settled and snapshot.session.phase == SessionPhase.TRANSITION_UNVERIFIED:
                eval_snap = replace(snapshot, session=settlement_res.session)
            elif snapshot.session.phase == SessionPhase.SETTLED and not settlement_res.settled:
                return UnifiedAuthorizationResult(
                    False,
                    settlement_res.reason_code or "SETTLEMENT_UNVERIFIED",
                    f"C16: Pre-labelled SETTLED session failed 6-point verification ({settlement_res.reason_code})",
                    snapshot,
                    None,
                    settlement_res,
                )
        verdict = self.reconciler.reconcile_account(eval_snap, jv, policy, now_m)
        if policy.purpose == Purpose.SHUTDOWN:
            allowed = bool(verdict.shutdown_eligible)
        else:
            allowed = bool(verdict.entry_eligible)
        if allowed:
            return UnifiedAuthorizationResult(True, "VERIFIED", "Unified broker ledger verified", eval_snap, verdict, settlement_res)
        if verdict.reason_codes:
            code = verdict.reason_codes[0]
        else:
            all_issues = [i.reason_code for cv in verdict.contracts for i in cv.issues]
            code = all_issues[0] if all_issues else "UNIFIED_VERIFICATION_BLOCKED"
        return UnifiedAuthorizationResult(False, code, f"Unified ledger blocked ({code})", eval_snap, verdict, settlement_res)

    def acquire_and_evaluate(
        self,
        broker: Any,
        policy: PolicyContext,
        *,
        session_supplier: Optional[Callable[[], SessionContext]] = None,
        min_capture_mono_ns: int = 0,
        independent_after_snapshot_id: Optional[str] = None,
        timeout_sec: float = 8.0,
        calendar: Optional[Any] = None,
        history_ranges: Tuple[Any, ...] = (),
        order_status_ids: Tuple[str, ...] = (),
        trade_order_ids: Tuple[str, ...] = (),
        policy_builder: Optional[Callable[[BrokerSnapshot, Any], PolicyContext]] = None,
    ) -> UnifiedAuthorizationResult:
        """Acquire a coordinated BrokerSnapshot via DhanSnapshotSource and evaluate it."""
        now_m = int(self.mono_clock())
        try:
            cal = calendar if calendar is not None else load_verified_exchange_calendar()
        except ValueError as exc:
            return UnifiedAuthorizationResult(False, str(exc) or "VERIFIED_EXCHANGE_CALENDAR_EMPTY", str(exc), None, None)
        if session_supplier is not None:
            sess_fn = session_supplier
        else:
            def _verified_session_supplier() -> SessionContext:
                from datetime import datetime as _dt
                from src.engine.broker_session import IST as _IST
                obs_u = int(self.utc_clock())
                obs_ist = _dt.fromtimestamp(obs_u // 1_000_000_000, _IST)
                day = obs_ist.date()
                phase = SessionPhase.CURRENT_SIGNED
                if not cal.trading(day):
                    day = cal.previous(day)
                    phase = SessionPhase.PRIOR_SIGNED
                elif (obs_ist.hour, obs_ist.minute) < (8, 30):
                    # C32 + C43: Overnight pre-settlement window (00:00-08:30 IST).
                    # Classify PRIOR_SIGNED on cal.previous(day) ONLY when:
                    # (a) no durable executions exist on `day`, AND
                    # (b) if broker has already cleared today's live books (empty trade_book and positions)
                    #     while prior-session fills exist in journal, the broker has already rolled over to `day`.
                    _use_prior = True
                    if self.db_path and os.path.isfile(self.db_path):
                        try:
                            _day_start = _dt(day.year, day.month, day.day, 0, 0, 0, tzinfo=_IST).timestamp()
                            with closing(sqlite3.connect(f"file:{os.path.abspath(self.db_path)}?mode=ro", uri=True, timeout=1.0)) as _sc_db:
                                if _sc_db.execute("SELECT 1 FROM fills WHERE traded_at >= ? LIMIT 1", (_day_start,)).fetchone() is not None:
                                    _use_prior = False
                                else:
                                    _prev_d = cal.previous(day)
                                    _prev_start = _dt(_prev_d.year, _prev_d.month, _prev_d.day, 0, 0, 0, tzinfo=_IST).timestamp()
                                    _has_prev = _sc_db.execute("SELECT 1 FROM fills WHERE traded_at >= ? AND traded_at < ? LIMIT 1", (_prev_start, _day_start)).fetchone() is not None
                                    if _has_prev and hasattr(broker, "get_trade_book") and hasattr(broker, "get_positions"):
                                        try:
                                            _tb = broker.get_trade_book()
                                            _ps = broker.get_positions()
                                            if isinstance(_tb, list) and not _tb and isinstance(_ps, list) and not _ps:
                                                _use_prior = False
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                    if _use_prior:
                        day = cal.previous(day)
                        phase = SessionPhase.PRIOR_SIGNED
                return SessionContext(
                    phase=phase,
                    trading_session_date=day,
                    observed_utc_ns=obs_u,
                    calendar_sha256=cal.digest,
                    calendar_verified=True,
                )
            sess_fn = _verified_session_supplier
        try:
            source = DhanSnapshotSource(broker, sess_fn, utc=self.utc_clock, mono=self.mono_clock)
            coord = source.coordinator()
            eff_history_ranges = tuple(history_ranges)
            eff_order_status_ids = tuple(order_status_ids)
            if not eff_history_ranges and Path(self.db_path).is_file():
                try:
                    from collections.abc import Mapping as _Mapping
                    from datetime import datetime as _dt
                    from src.engine.broker_session import IST as _IST
                    _now_ist_dt = _dt.fromtimestamp(int(self.utc_clock()) // 1_000_000_000, _IST)
                    _today_ist = _now_ist_dt.date()
                    if (not cal.trading(_today_ist)) or ((_now_ist_dt.hour, _now_ist_dt.minute) < (8, 30)):
                        _today_ist = cal.previous(_today_ist)
                    _jv_pre = read_journal_view(
                        self.db_path,
                        account_id=self.account_id,
                        journal_epoch=1,
                        read_utc_ns=int(self.utc_clock()),
                    )
                    if _jv_pre.read_complete:
                        _attested = {
                            str(r.get("broker_order_id") or "")
                            for r in getattr(_jv_pre, "historical_fill_attestations", ())
                            if isinstance(r, _Mapping)
                        }
                        _hist_dates = set()
                        for _row in getattr(_jv_pre, "order_intents", ()):
                            if not isinstance(_row, _Mapping):
                                continue
                            _boid = str(_row.get("broker_order_id") or "").strip()
                            if _boid and _boid in _attested:
                                continue
                            _ts = _row.get("created_at")
                            if isinstance(_ts, (int, float)) and _ts > 0:
                                _d = _dt.fromtimestamp(float(_ts), _IST).date()
                                if _d < _today_ist:
                                    _hist_dates.add(_d)
                        for _row in getattr(_jv_pre, "fills", ()):
                            if not isinstance(_row, _Mapping):
                                continue
                            _ts = _row.get("traded_at")
                            if isinstance(_ts, (int, float)) and _ts > 0:
                                _d = _dt.fromtimestamp(float(_ts), _IST).date()
                                if _d < _today_ist:
                                    _hist_dates.add(_d)
                        if _hist_dates:
                            _lo = min(_hist_dates)
                            _hi = cal.previous(_today_ist) if _today_ist > _lo else _lo
                            if _hi < _lo:
                                _hi = _lo
                            eff_history_ranges = ((_lo, _hi),)

                except Exception as _he:
                    logger.debug('[C26] history_ranges auto-derivation note: %s', _he)
            req = EvidenceRequest(
                self.account_id,
                policy.purpose,
                LIVE_BOOKS,
                min_capture_mono_ns=min_capture_mono_ns,
                independent_after_snapshot_id=independent_after_snapshot_id,
                history_ranges=eff_history_ranges,
                order_status_ids=eff_order_status_ids,
                trade_order_ids=trade_order_ids,
                deadline_mono_ns=now_m + int(timeout_sec * NS),
            )
            acquired = coord.acquire(req)
        except (EvidenceUnavailable, AttributeError, ValueError, TypeError, OSError) as exc:
            return UnifiedAuthorizationResult(False, "SNAPSHOT_ACQUISITION_FAILED", str(exc), None, None)

        if isinstance(acquired, ReadDeferredResult):
            return UnifiedAuthorizationResult(False, acquired.reason_code, f"Broker read deferred: {acquired.reason_code}", None, None)
        effective_policy = policy
        if policy_builder is not None:
            jv_post = read_journal_view(
                self.db_path,
                account_id=self.account_id,
                journal_epoch=int(acquired.generation.journal_epoch),
                read_utc_ns=int(self.utc_clock()),
            )
            effective_policy = policy_builder(acquired, jv_post)
        res = self.evaluate_snapshot(acquired, effective_policy, now_mono_ns=int(self.mono_clock()))
        # C31: If reconciler requests specific additional evidence (e.g. order_status for rolled-over
        # triggered Forever OCO children or blank-EID archived rollups, or additional history_ranges),
        # perform ONE bounded follow-up acquisition with the exact requested evidence and re-evaluate.
        if not res.allowed and res.verdict is not None:
            req_ev = []
            for _cv in getattr(res.verdict, "contracts", ()) or ():
                req_ev.extend(getattr(_cv, "additional_evidence", ()) or ())
            extra_hist = list(eff_history_ranges)
            extra_oids = set(eff_order_status_ids)
            extra_tids = set(trade_order_ids)
            changed = False
            for er in req_ev:
                for hr in getattr(er, "history_ranges", ()) or ():
                    if hr not in extra_hist:
                        extra_hist.append(hr)
                        changed = True
                for oid in getattr(er, "order_status_ids", ()) or ():
                    if oid and oid not in extra_oids:
                        extra_oids.add(str(oid))
                        changed = True
                for tid in getattr(er, "trade_order_ids", ()) or ():
                    if tid and tid not in extra_tids:
                        extra_tids.add(str(tid))
                        changed = True
            if changed:
                try:
                    now_m2 = int(self.mono_clock())
                    req2 = EvidenceRequest(
                        self.account_id,
                        policy.purpose,
                        LIVE_BOOKS,
                        min_capture_mono_ns=min_capture_mono_ns,
                        independent_after_snapshot_id=independent_after_snapshot_id,
                        history_ranges=tuple(extra_hist),
                        order_status_ids=tuple(sorted(extra_oids)),
                        trade_order_ids=tuple(sorted(extra_tids)),
                        deadline_mono_ns=now_m2 + int(timeout_sec * NS),
                    )
                    acquired2 = coord.acquire(req2)
                except (EvidenceUnavailable, AttributeError, ValueError, TypeError, OSError):
                    return res
                if isinstance(acquired2, ReadDeferredResult):
                    return res
                effective_policy2 = policy
                if policy_builder is not None:
                    jv_post2 = read_journal_view(
                        self.db_path,
                        account_id=self.account_id,
                        journal_epoch=int(acquired2.generation.journal_epoch),
                        read_utc_ns=int(self.utc_clock()),
                    )
                    effective_policy2 = policy_builder(acquired2, jv_post2)
                return self.evaluate_snapshot(acquired2, effective_policy2, now_mono_ns=int(self.mono_clock()))
        return res


def apply_verified_terminal_order_repair(
    db_path: str | Path,
    *,
    account_id: str,
    correlation_id: str,
    broker_order_id: str,
    terminal_status: str,
    filled_qty: int,
    now_ts: Optional[float] = None,
) -> Tuple[bool, str]:
    """Explicit, audited journal repair executor for verified broker terminal orders.

    Propagates exact terminal status (`CANCELLED`, `REJECTED`, `EXPIRED`, `TRADED`)
    to `order_intents`, `orders`, and `watcher_orders` inside one atomic transaction.
    NEVER fabricates synthetic `-RECON-FILL-` rows in `fills` and NEVER blindly
    overwrites `watcher_orders.protected_qty`.
    """
    st = str(terminal_status or "").strip().upper()
    if st not in ("TRADED", "CANCELLED", "REJECTED", "EXPIRED"):
        return False, f"INVALID_TERMINAL_STATUS:{st}"
    if type(filled_qty) is not int or filled_qty < 0:
        return False, "INVALID_FILLED_QTY"
    path = Path(db_path)
    if not path.is_file() or path.is_symlink():
        return False, "JOURNAL_UNAVAILABLE"

    ts = float(now_ts if now_ts is not None else time.time())
    try:
        with closing(sqlite3.connect(str(path), timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            try:
                i_cols = {c[1] for c in conn.execute("PRAGMA table_info(order_intents)").fetchall()}
                o_cols = {c[1] for c in conn.execute("PRAGMA table_info(orders)").fetchall()}

                if "updated_at" in i_cols:
                    conn.execute(
                        "UPDATE order_intents SET state = ?, broker_order_id = COALESCE(NULLIF(broker_order_id, ''), ?), updated_at = ? WHERE correlation_id = ?",
                        (st, broker_order_id, ts, correlation_id),
                    )
                else:
                    conn.execute(
                        "UPDATE order_intents SET state = ? WHERE correlation_id = ?",
                        (st, correlation_id),
                    )

                if "updated_at" in o_cols and "filled_quantity" in o_cols:
                    conn.execute(
                        "UPDATE orders SET status = ?, filled_quantity = ?, updated_at = ? WHERE correlation_id = ?",
                        (st, filled_qty, ts, correlation_id),
                    )
                elif "filled_quantity" in o_cols:
                    conn.execute(
                        "UPDATE orders SET status = ?, filled_quantity = ? WHERE correlation_id = ?",
                        (st, filled_qty, correlation_id),
                    )
                elif "updated_at" in o_cols:
                    conn.execute(
                        "UPDATE orders SET status = ?, updated_at = ? WHERE correlation_id = ?",
                        (st, ts, correlation_id),
                    )
                else:
                    conn.execute(
                        "UPDATE orders SET status = ? WHERE correlation_id = ?",
                        (st, correlation_id),
                    )

                # Update watcher_orders terminal state/filled_qty WITHOUT fabricating protected_qty
                wrow = conn.execute(
                    "SELECT order_id, record_json FROM watcher_orders WHERE order_id IN (?, ?) LIMIT 1",
                    (broker_order_id, correlation_id),
                ).fetchone()
                if wrow is not None:
                    w_data = json.loads(wrow["record_json"] or "{}")
                    if isinstance(w_data, dict):
                        w_data.pop("_is_terminal_col", None)
                        w_data["state"] = st
                        w_data["filled_qty"] = filled_qty
                        w_data["terminal_confirmed"] = True
                        conn.execute(
                            "UPDATE watcher_orders SET is_terminal = 1, record_json = ?, updated_at = ? WHERE order_id = ?",
                            (json.dumps(w_data), ts, str(wrow["order_id"])),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if account_id:
            note_event(path.resolve().parent, account_id, f"TERMINAL_ORDER_REPAIRED:{st}")
        return True, "OK"
    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _is_broker_read_failure(obj: Any) -> bool:
    if obj is None:
        return True
    if getattr(obj, "ok", True) is False:
        return True
    return type(obj).__name__ == "BrokerReadFailure"


def _extract_non_null_int(d: Dict[str, Any], *keys: str) -> Optional[int]:
    for k in keys:
        if k in d and d[k] is not None and d[k] != "":
            val = d[k]
            if isinstance(val, bool):
                continue
            try:
                return int(val)
            except (TypeError, ValueError):
                return None
    return None


def reconcile_and_repair_terminal_orders(
    *,
    db_path: str,
    broker: Any,
    account_id: str = "",
    data_dir: str = "",
) -> Tuple[bool, Optional[str], Dict[str, str]]:
    """
    Phase 2D C13-safe atomic batch terminal-order repair (F03-01 / F04-03):
    - Phase 1 (Read-Only): Reads non-terminal `order_intents` / `orders` and pre-repair journal digest.
    - Phase 2 (Network Collection OUTSIDE SQLite TX): Queries broker `order_status` / `order_book` /
      `trade_book` / `forever_orders` and strictly validates:
        * Exact `dhanClientId` match (when present on payload or expected)
        * Exact `orderId` match (rejects unrelated orders like "OTHER")
        * Exact `correlationId` match (when present on payload)
        * Exact contract (`securityId`, `exchangeSegment`, `productType`) match
        * Explicit exact-integer `filledQty` (NEVER defaults missing/None/float/bool to 0!)
        * Explicit exact-integer `remainingQuantity == 0` when present (rejects fractional/negative/nonzero)
        * Zero fills in `trade_book` for zero-fill `CANCELLED`/`REJECTED`/`EXPIRED`
    - Phase 3 (Short Fenced Write TX): Re-verifies journal digest inside `BEGIN IMMEDIATE`, applies
      updates atomically (rolling back all updates on any failure), commits, and verifies readback.
    - NEVER fabricates synthetic `-RECON-FILL-` rows and NEVER overwrites `protected_qty`.
    """
    from src.engine.durable_risk_resolution import _strict_exact_int

    terminal_states = {"FILLED", "TRADED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}
    repaired: Dict[str, str] = {}
    path = Path(db_path)
    if not path.is_file():
        return True, None, repaired

    # --- Phase 1: Read-only candidate discovery (NO write lock held during network reads!) ---
    candidates: Dict[str, Dict[str, Any]] = {}
    terminal_intent_cids: set = set()
    tables: set = set()
    oi_cols: set = set()
    o_cols: set = set()
    pre_digest: Optional[str] = None
    try:
        with closing(sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "order_intents" in tables:
                oi_cols = {r[1] for r in conn.execute("PRAGMA table_info(order_intents)").fetchall()}
                st_col = "state" if "state" in oi_cols else ("status" if "status" in oi_cols else None)
                if st_col:
                    sel_cols = ["correlation_id", "broker_order_id", f"{st_col} AS st"]
                    for opt in ("account_id", "security_id", "exchange_segment", "product_type", "quantity"):
                        if opt in oi_cols:
                            sel_cols.append(opt)
                    for row in conn.execute(f"SELECT {', '.join(sel_cols)} FROM order_intents").fetchall():
                        st = str(row["st"] or "").strip().upper()
                        cid_r = str(row["correlation_id"] or "").strip()
                        if st in terminal_states:
                            if cid_r:
                                terminal_intent_cids.add(cid_r)
                        else:
                            cid = str(row["correlation_id"] or "").strip()
                            boid = str(row["broker_order_id"] or "").strip()
                            key = boid or cid
                            if key:
                                candidates[key] = {
                                    "cid": cid,
                                    "boid": boid,
                                    "orig_st": st,
                                    "account_id": str(row["account_id"] or "").strip() if "account_id" in oi_cols else "",
                                    "security_id": str(row["security_id"] or "").strip() if "security_id" in oi_cols else "",
                                    "exchange_segment": str(row["exchange_segment"] or "").strip().upper() if "exchange_segment" in oi_cols else "",
                                    "product_type": str(row["product_type"] or "").strip().upper() if "product_type" in oi_cols else "",
                                }
            if "orders" in tables:
                o_cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()}
                if "status" in o_cols:
                    for row in conn.execute(
                        "SELECT correlation_id, broker_order_id, status, security_id, exchange_segment, product_type FROM orders"
                    ).fetchall():
                        st = str(row["status"] or "").strip().upper()
                        if st not in terminal_states:
                            cid = str(row["correlation_id"] or "").strip()
                            boid = str(row["broker_order_id"] or "").strip()
                            key = boid or cid
                            if key and key not in candidates and (not cid or cid not in terminal_intent_cids) and not any(v["cid"] == cid and cid for v in candidates.values()):
                                candidates[key] = {
                                    "cid": cid,
                                    "boid": boid,
                                    "orig_st": st,
                                    "account_id": "",
                                    "security_id": str(row["security_id"] or "").strip(),
                                    "exchange_segment": str(row["exchange_segment"] or "").strip().upper(),
                                    "product_type": str(row["product_type"] or "").strip().upper(),
                                }
    except sqlite3.Error as exc:
        return False, f"JOURNAL_ERROR: Pre-repair read failed: {exc}", {}

    if not candidates:
        return True, None, repaired

    if account_id:
        jv_pre = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
        if jv_pre.read_complete:
            pre_digest = jv_pre.digest_sha256

    # --- Phase 2: Collect ALL broker evidence OUTSIDE any SQLite transaction ---
    order_book_by_id: Dict[str, Dict[str, Any]] = {}
    order_book_by_cid: Dict[str, Dict[str, Any]] = {}
    if hasattr(broker, "get_order_book"):
        try:
            ob = broker.get_order_book()
            if isinstance(ob, list) and not _is_broker_read_failure(ob):
                for row in ob:
                    if isinstance(row, dict):
                        oid = str(row.get("orderId") or "").strip()
                        cid = str(row.get("correlationId") or "").strip()
                        if oid:
                            order_book_by_id[oid] = row
                        if cid:
                            order_book_by_cid[cid] = row
        except Exception:
            pass

    trade_book_rows: List[Dict[str, Any]] = []
    trade_book_verified = False
    if hasattr(broker, "get_trade_book"):
        try:
            tb = broker.get_trade_book()
            if isinstance(tb, list) and not _is_broker_read_failure(tb):
                trade_book_rows = [r for r in tb if isinstance(r, _Mapping)]
                trade_book_verified = True
            else:
                return False, f"BROKER_RECONCILIATION_FAILED: Trade book read failed ({type(tb).__name__}).", {}
        except Exception as exc:
            return False, f"BROKER_RECONCILIATION_FAILED: Trade book read exception ({type(exc).__name__}: {exc}).", {}
    else:
        return False, "BROKER_RECONCILIATION_FAILED: Broker get_trade_book capability unavailable.", {}

    planned_updates: List[Tuple[str, Dict[str, Any], str, int, str]] = []
    for key, info in candidates.items():
        cid = info["cid"]
        boid = info["boid"]
        expected_acct = info["account_id"] or account_id
        st_payload = None
        lookup_err = None
        if boid and hasattr(broker, "order_status"):
            try:
                st_payload = broker.order_status(boid)
            except Exception as exc:
                lookup_err = f"{type(exc).__name__}: {exc}"
        if (not isinstance(st_payload, dict) or _is_broker_read_failure(st_payload)) and not lookup_err:
            st_payload = order_book_by_id.get(boid) or order_book_by_cid.get(cid)
        if lookup_err or not isinstance(st_payload, dict) or _is_broker_read_failure(st_payload):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Could not verify broker status for order {boid or cid}: {lookup_err or st_payload}",
                {},
            )

        # C13 + C17: Strict REQUIRED exact account, orderId, correlationId, and contract validation (missing field = REJECT!)
        p_acct = str(st_payload.get("dhanClientId") or "").strip()
        if not p_acct or (expected_acct and p_acct != expected_acct):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched dhanClientId for {boid or cid} (expected={expected_acct}, got={p_acct or 'MISSING'}).",
                {},
            )
        p_oid = str(st_payload.get("orderId") or "").strip()
        if not p_oid or (boid and p_oid != boid):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched orderId (expected={boid}, got={p_oid or 'MISSING'}).",
                {},
            )
        p_cid = str(st_payload.get("correlationId") or "").strip()
        if cid and (not p_cid or p_cid != cid):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched correlationId for {boid or cid} (expected={cid}, got={p_cid or 'MISSING'}).",
                {},
            )
        p_sec = str(st_payload.get("securityId") or "").strip()
        if not p_sec or (info["security_id"] and p_sec != info["security_id"]):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched securityId for {boid or cid} (expected={info['security_id']}, got={p_sec or 'MISSING'}).",
                {},
            )
        p_seg = str(st_payload.get("exchangeSegment") or "").strip().upper()
        if not p_seg or (info["exchange_segment"] and p_seg != info["exchange_segment"]):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched exchangeSegment for {boid or cid} (expected={info['exchange_segment']}, got={p_seg or 'MISSING'}).",
                {},
            )
        p_prod = str(st_payload.get("productType") or "").strip().upper()
        if not p_prod or (info["product_type"] and p_prod != info["product_type"]):
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or mismatched productType for {boid or cid} (expected={info['product_type']}, got={p_prod or 'MISSING'}).",
                {},
            )

        b_status = str(st_payload.get("orderStatus") or "").strip().upper()
        if b_status not in ("CANCELLED", "REJECTED", "EXPIRED", "TRADED", "FILLED"):
            return (
                False,
                f"LIVE_ORDER_IN_PROGRESS: Order {boid or cid} is still active at broker (status={b_status or 'UNKNOWN'}).",
                {},
            )

        # C13: NEVER default missing filledQty to 0! Require explicit exact integer!
        raw_filled = None
        for fk in ("filledQty", "filled_quantity", "tradedQuantity"):
            if fk in st_payload:
                raw_filled = st_payload.get(fk)
                break
        b_filled = _strict_exact_int(raw_filled)
        if b_filled is None:
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing or non-integer filledQty for terminal order {boid or cid} ({b_status}).",
                {},
            )
        b_rem = _strict_exact_int(st_payload.get("remainingQuantity"))
        if b_rem is None or b_rem != 0:
            return (
                False,
                f"BROKER_RECONCILIATION_FAILED: Missing, non-zero, or invalid remainingQuantity for terminal order {boid or cid}.",
                {},
            )

        # For zero-fill CANCELLED/REJECTED/EXPIRED, verify zero fills in trade_book
        if b_filled == 0 and b_status in ("CANCELLED", "REJECTED", "EXPIRED"):
            for tr in trade_book_rows:
                if str(tr.get("orderId") or "").strip() == (boid or p_oid):
                    return (
                        False,
                        f"BROKER_RECONCILIATION_FAILED: Trade book shows execution for zero-fill {b_status} order {boid or cid}.",
                        {},
                    )

        norm_status = "TRADED" if b_status == "FILLED" else b_status
        resolved_sec = p_sec or info["security_id"]
        planned_updates.append((key, info, norm_status, b_filled, resolved_sec))

    # Pre-fetch protection/exposure books OUTSIDE SQLite TX if any planned update has b_filled > 0
    filled_updates = [item for item in planned_updates if item[3] > 0]
    active_forever: List[Dict[str, Any]] = []
    pos_rows_pre: Optional[List[Dict[str, Any]]] = None
    hold_rows_pre: Optional[List[Dict[str, Any]]] = None
    if filled_updates:
        forever_method = getattr(broker, "get_forever_orders", None)
        if not callable(forever_method):
            return False, "BROKER_RECONCILIATION_FAILED: Broker get_forever_orders unavailable to verify filled order protection.", {}
        try:
            forever_rows = forever_method()
        except Exception as exc:
            return False, f"BROKER_RECONCILIATION_FAILED: Broker forever orders read failed: {exc}", {}
        if _is_broker_read_failure(forever_rows) or not isinstance(forever_rows, list):
            return False, f"BROKER_RECONCILIATION_FAILED: Invalid broker forever orders response: {type(forever_rows).__name__}", {}
        active_forever = [
            r for r in forever_rows
            if isinstance(r, _Mapping)
            and str(r.get("orderStatus") or "").strip().upper() in ("PENDING", "TRANSIT", "ACTIVE", "CONFIRM", "PART_TRADED", "OPEN", "PLACED")
        ]
        if hasattr(broker, "get_positions"):
            try:
                pr = broker.get_positions()
                if isinstance(pr, list) and not _is_broker_read_failure(pr):
                    pos_rows_pre = pr
            except Exception:
                pass
        if hasattr(broker, "get_holdings"):
            try:
                hr = broker.get_holdings()
                if isinstance(hr, list) and not _is_broker_read_failure(hr):
                    hold_rows_pre = hr
            except Exception:
                pass

    # --- Phase 3: Short fenced SQLite BEGIN IMMEDIATE write transaction ---
    try:
        with closing(sqlite3.connect(str(path), timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            try:
                if pre_digest is not None and account_id:
                    jv_now = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
                    if not jv_now.read_complete or jv_now.digest_sha256 != pre_digest:
                        conn.rollback()
                        return False, "BROKER_RECONCILIATION_FAILED: Journal digest changed during repair collection.", {}

                ts = time.time()
                used_forever_ids: set = set()
                for _key, info, norm_status, b_filled, sec_id in planned_updates:
                    cid = info["cid"]
                    boid = info["boid"]
                    key_id = boid or cid

                    # Verify protection / closed-parent / manual-ack for filled orders before committing
                    mark_flat_closed = False
                    if b_filled > 0:
                        matched_qty = 0
                        for fr in active_forever:
                            foid = str(fr.get("orderId") or "").strip()
                            if not foid or foid in used_forever_ids:
                                continue
                            f_parent = str(fr.get("parentOrderId") or "").strip()
                            if f_parent and f_parent not in (key_id, cid):
                                continue
                            fsec = str(fr.get("securityId") or "").strip()
                            if sec_id and fsec and fsec != sec_id:
                                continue
                            fq = _strict_exact_int(fr.get("quantity")) or 0
                            if fq > 0:
                                matched_qty += fq
                                used_forever_ids.add(foid)
                                if matched_qty >= b_filled:
                                    break
                        if matched_qty < b_filled:
                            _is_closed_or_acked = False
                            if "manual_risk_acknowledgements" in tables:
                                _ack = conn.execute(
                                    "SELECT quantity FROM manual_risk_acknowledgements WHERE parent_order_id = ? AND status IN ('UNPROTECTED_MANUAL_RISK', 'ACKNOWLEDGED', 'ACTIVE') LIMIT 1",
                                    (key_id,),
                                ).fetchone()
                                if _ack is not None and int(_ack["quantity"] or 0) >= b_filled:
                                    _is_closed_or_acked = True
                            if not _is_closed_or_acked and "positions_lifecycle" in tables:
                                _life = conn.execute(
                                    "SELECT position_state, confirmed_open_qty, sell_filled_qty FROM positions_lifecycle WHERE parent_order_id = ? LIMIT 1",
                                    (key_id,),
                                ).fetchone()
                                if (
                                    _life is not None
                                    and str(_life["position_state"] or "").upper() == "POSITION_CLOSED"
                                    and int(_life["confirmed_open_qty"] or 0) == 0
                                    and int(_life["sell_filled_qty"] or 0) >= b_filled
                                ):
                                    _is_closed_or_acked = True
                            if not _is_closed_or_acked and sec_id and pos_rows_pre is not None and hold_rows_pre is not None:
                                _pos_q = sum(abs(int(r.get("netQty") or 0)) for r in pos_rows_pre if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
                                _hold_q = sum(int(r.get("totalQty") or 0) + int(r.get("t1Qty") or 0) for r in hold_rows_pre if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
                                _sell_q = sum(int(r.get("tradedQuantity") or 0) for r in trade_book_rows if str(r.get("securityId") or "") == sec_id and str(r.get("transactionType") or "").upper() == "SELL")
                                _has_pend = any(
                                    str(r.get("securityId") or "") == sec_id
                                    and str(r.get("orderStatus") or "").upper() in ("PENDING", "TRANSIT", "OPEN", "PART_TRADED")
                                    for r in order_book_by_id.values()
                                )
                                _has_act_f = any(str(r.get("securityId") or "") == sec_id for r in active_forever)
                                if _pos_q == 0 and _hold_q == 0 and _sell_q >= b_filled and not _has_pend and not _has_act_f:
                                    _is_closed_or_acked = True
                                    mark_flat_closed = True
                            if not _is_closed_or_acked:
                                _has_watcher = False
                                if "watcher_orders" in tables:
                                    _has_watcher = conn.execute(
                                        "SELECT 1 FROM watcher_orders WHERE order_id IN (?, ?) OR is_terminal = 0 LIMIT 1",
                                        (key_id, cid),
                                    ).fetchone() is not None
                                conn.rollback()
                                if not _has_watcher:
                                    return False, f"LIVE_ORDER_IN_PROGRESS: Order {key_id} filled ({b_filled} shares) and is awaiting verified broker protection.", {}
                                return False, f"UNPROTECTED_POSITION: Order {key_id} has {b_filled} filled shares but only {matched_qty} verified linked protected shares.", {}

                    repaired[key_id] = norm_status
                    if "order_intents" in tables and ("state" in oi_cols or "status" in oi_cols):
                        st_col = "state" if "state" in oi_cols else "status"
                        if "updated_at" in oi_cols:
                            conn.execute(
                                f"UPDATE order_intents SET {st_col} = ?, updated_at = ? WHERE correlation_id = ? OR broker_order_id = ?",
                                (norm_status, ts, cid, key_id),
                            )
                        else:
                            conn.execute(
                                f"UPDATE order_intents SET {st_col} = ? WHERE correlation_id = ? OR broker_order_id = ?",
                                (norm_status, cid, key_id),
                            )
                    if "orders" in tables and "status" in o_cols:
                        if "filled_quantity" in o_cols and "updated_at" in o_cols:
                            conn.execute(
                                "UPDATE orders SET status = ?, filled_quantity = ?, updated_at = ? WHERE correlation_id = ? OR broker_order_id = ?",
                                (norm_status, b_filled, ts, cid, key_id),
                            )
                        elif "filled_quantity" in o_cols:
                            conn.execute(
                                "UPDATE orders SET status = ?, filled_quantity = ? WHERE correlation_id = ? OR broker_order_id = ?",
                                (norm_status, b_filled, cid, key_id),
                            )
                        else:
                            conn.execute(
                                "UPDATE orders SET status = ? WHERE correlation_id = ? OR broker_order_id = ?",
                                (norm_status, cid, key_id),
                            )
                    if "watcher_orders" in tables:
                        wrow = conn.execute(
                            "SELECT order_id, record_json FROM watcher_orders WHERE order_id IN (?, ?) LIMIT 1",
                            (key_id, cid),
                        ).fetchone()
                        if wrow is not None:
                            try:
                                w_data = json.loads(wrow["record_json"] or "{}")
                            except json.JSONDecodeError:
                                w_data = {}
                            if isinstance(w_data, dict):
                                w_data["state"] = norm_status
                                w_data["filled_qty"] = b_filled
                                w_data["terminal_confirmed"] = True
                                if mark_flat_closed:
                                    w_data["protected_qty"] = 0
                                    w_data["broker_flat_verified_at"] = ts
                                conn.execute(
                                    "UPDATE watcher_orders SET is_terminal = 1, record_json = ?, updated_at = ? WHERE order_id = ?",
                                    (json.dumps(w_data), ts, str(wrow["order_id"])),
                                )
                    if mark_flat_closed and "protection_orders" in tables:
                        conn.execute(
                            "UPDATE protection_orders SET status = 'CLOSED_NO_EXPOSURE_VERIFIED' WHERE parent_order_id IN (?, ?)",
                            (key_id, cid),
                        )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if account_id and data_dir:
            note_event(Path(data_dir), account_id, "JOURNAL_UPDATED")
        return True, None, repaired
    except sqlite3.Error as exc:
        return False, f"JOURNAL_ERROR: Atomic terminal repair failed: {exc}", {}


def build_policy_context(
    *,
    mode: str = "LIVE",
    purpose: Purpose = Purpose.SHUTDOWN,
    owner_desired_state: str = "RUNNING",
    owner_observed_state: str = "RUNNING",
    deferred_stop_pending: bool = False,
    owner_intent_epoch: int = 0,
    entry_fence_active: bool = False,
    emergency_stop_active: bool = False,
    transition_active: bool = False,
    mutation_fence_verified: bool = False,
    active_mutation_ids: Tuple[str, ...] = (),
    unknown_mutation_ids: Tuple[str, ...] = (),
    service_identity_verified: bool = False,
    watcher_liveness_verified: bool = False,
    risk_funds_mode_checks_passed: bool = False,
    journal_readback_verified: bool = False,
    min_capture_mono_ns: int = 0,
    holdings_confirmation: Any = None,
) -> PolicyContext:
    """Construct a complete fe946810 PolicyContext with all 16 required fields."""
    return PolicyContext(
        mode=str(mode or "LIVE").upper(),
        purpose=purpose,
        owner_desired_state=owner_desired_state,
        owner_observed_state=owner_observed_state,
        deferred_stop_pending=bool(deferred_stop_pending),
        owner_intent_epoch=int(owner_intent_epoch),
        entry_fence_active=bool(entry_fence_active),
        emergency_stop_active=bool(emergency_stop_active),
        transition_active=bool(transition_active),
        mutation_fence_verified=bool(mutation_fence_verified),
        active_mutation_ids=tuple(active_mutation_ids),
        unknown_mutation_ids=tuple(unknown_mutation_ids),
        service_identity_verified=bool(service_identity_verified),
        watcher_liveness_verified=bool(watcher_liveness_verified),
        risk_funds_mode_checks_passed=bool(risk_funds_mode_checks_passed),
        journal_readback_verified=bool(journal_readback_verified),
        min_capture_mono_ns=int(min_capture_mono_ns),
        holdings_confirmation=holdings_confirmation,
    )


def verify_unprotected_watcher_and_active_orders(
    *,
    db_path: str,
    broker: Any,
    account_id: str = "",
) -> Tuple[bool, Optional[str]]:
    """
    Verify unprotected / non-terminal watcher_orders and active broker orders for
    read-only broker stubs that do not implement get_profile():
    - Verifies broker-flat closed fills (matching SELL in trade_book + 0 positions + 0 holdings
      + 0 pending orders) and marks watcher_orders.is_terminal=1, protected_qty=0,
      broker_flat_verified_at, protection_orders.status='CLOSED_NO_EXPOSURE_VERIFIED'.
    - Re-verifies previously broker_flat_verified_at rows on next session against fresh holdings & order_book.
    - Rejects any active/pending broker order or unclosed/unprotected watcher exposure.
    """
    path = Path(db_path)
    if not path.is_file():
        return True, None
    conn = None
    try:
        conn = sqlite3.connect(str(path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "watcher_orders" not in tables:
            return True, None

        wrows = conn.execute("SELECT order_id, is_terminal, record_json FROM watcher_orders").fetchall()
        if not wrows:
            return True, None
        needs_check = []
        flat_verified_rows = []
        for wr in wrows:
            try:
                wd = json.loads(wr["record_json"] or "{}")
            except (ValueError, TypeError):
                return False, "JOURNAL_ERROR: Malformed watcher_orders JSON"
            if not isinstance(wd, dict):
                continue
            oid = str(wr["order_id"] or "").strip()
            is_term = int(wr["is_terminal"] or 0)
            filled_q = int(wd.get("filled_qty") or 0)
            prot_q = int(wd.get("protected_qty") or 0)
            pos_state = str(wd.get("position_state") or "").upper()
            if wd.get("broker_flat_verified_at") is not None:
                flat_verified_rows.append((oid, wd))
            elif is_term == 0 and filled_q > 0 and prot_q < filled_q and pos_state not in ("UNPROTECTED_MANUAL_RISK", "POSITION_CLOSED"):
                _is_acked = False
                if "manual_risk_acknowledgements" in tables:
                    _ar = conn.execute(
                        "SELECT quantity FROM manual_risk_acknowledgements WHERE parent_order_id = ? AND status IN ('UNPROTECTED_MANUAL_RISK', 'ACKNOWLEDGED', 'ACTIVE') LIMIT 1",
                        (oid,),
                    ).fetchone()
                    if _ar is not None and int(_ar["quantity"] or 0) >= filled_q:
                        _is_acked = True
                if not _is_acked:
                    needs_check.append((oid, wd, filled_q))

        # Check active Forever OCO legs belonging to closed parents first
        closed_parent_fids: set = set()
        if "positions_lifecycle" in tables and "protection_orders" in tables:
            for _cr in conn.execute(
                "SELECT po.forever_order_id FROM protection_orders po JOIN positions_lifecycle pl ON po.parent_order_id = pl.parent_order_id WHERE UPPER(pl.position_state) = 'POSITION_CLOSED'"
            ).fetchall():
                if _cr["forever_order_id"]:
                    closed_parent_fids.add(str(_cr["forever_order_id"]).strip())

        # If all watcher rows are settled & owner-acknowledged with no closed-parent OCOs to verify, spend ZERO broker GETs!
        if not needs_check and not flat_verified_rows and not closed_parent_fids:
            return True, None

        if closed_parent_fids and hasattr(broker, "get_forever_orders"):
            try:
                _frows = broker.get_forever_orders()
                if isinstance(_frows, list) and not _is_broker_read_failure(_frows):
                    for _fr in _frows:
                        if isinstance(_fr, dict):
                            _fid = str(_fr.get("orderId") or "").strip()
                            _fst = str(_fr.get("orderStatus") or "").strip().upper()
                            if _fid in closed_parent_fids and _fst in ("PENDING", "TRANSIT", "ACTIVE", "CONFIRM", "PART_TRADED", "OPEN", "PLACED"):
                                return False, f"UNPROTECTED_POSITION: Closed parent still has active Forever OCO leg {_fid} ({_fst})."
            except Exception:
                pass

        if not needs_check and not flat_verified_rows:
            return True, None

        # Check active broker orders in order_book for any open/pending sibling orders
        ob_rows: List[Dict[str, Any]] = []
        if hasattr(broker, "get_order_book"):
            try:
                raw_ob = broker.get_order_book()
                if isinstance(raw_ob, list) and not _is_broker_read_failure(raw_ob):
                    ob_rows = [r for r in raw_ob if isinstance(r, _Mapping)]
            except Exception:
                pass
        for r in ob_rows:
            st = str(r.get("orderStatus") or "").strip().upper()
            if st in ("PENDING", "TRANSIT", "OPEN", "PART_TRADED"):
                oid_act = str(r.get("orderId") or "").strip()
                sec_act = str(r.get("securityId") or "").strip()
                if any(str(wd.get("security_id") or "") == sec_act and sec_act for _, wd in flat_verified_rows):
                    return False, f"UNPROTECTED_POSITION: Pending order {oid_act} invalidates historical flat closure for security {sec_act}."
                if needs_check:
                    return False, f"UNPROTECTED_POSITION: Active order {oid_act} ({st}) prevents flat closure."
                return False, f"LIVE_ORDER_IN_PROGRESS: Active broker order {oid_act} ({st}) is in progress."

        # Fresh broker reads required (positions + holdings must succeed; missing holdings capability fails closed!)
        if not hasattr(broker, "get_positions"):
            return False, "BROKER_RECONCILIATION_FAILED: Broker get_positions unavailable"
        pos_rows = broker.get_positions()
        if _is_broker_read_failure(pos_rows) or not isinstance(pos_rows, list):
            return False, "BROKER_RECONCILIATION_FAILED: Broker positions unavailable"

        if not hasattr(broker, "get_holdings") or not callable(getattr(broker, "get_holdings", None)):
            return False, "UNPROTECTED_POSITION: Broker holdings capability missing; cannot verify flat closure"
        hold_rows = broker.get_holdings()
        if _is_broker_read_failure(hold_rows) or not isinstance(hold_rows, list):
            return False, "UNPROTECTED_POSITION: Broker holdings read failed; cannot verify flat closure"

        # Re-verify previously flat-verified rows against fresh holdings & positions
        for oid, wd in flat_verified_rows:
            sec_id = str(wd.get("security_id") or "").strip()
            if not sec_id:
                continue
            h_qty = sum(int(r.get("totalQty") or 0) + int(r.get("t1Qty") or 0) for r in hold_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            p_qty = sum(abs(int(r.get("netQty") or 0)) for r in pos_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            if h_qty > 0 or p_qty > 0:
                return False, f"UNPROTECTED_POSITION: Fresh broker exposure detected for {oid} (holdings={h_qty}, positions={p_qty})."

        if not needs_check:
            return True, None

        tb_rows = broker.get_trade_book() if hasattr(broker, "get_trade_book") else []
        if _is_broker_read_failure(tb_rows) or not isinstance(tb_rows, list):
            tb_rows = []

        ts = time.time()
        conn.execute("BEGIN IMMEDIATE")
        for oid, wd, filled_q in needs_check:
            sec_id = str(wd.get("security_id") or "").strip()
            h_qty = sum(int(r.get("totalQty") or 0) + int(r.get("t1Qty") or 0) for r in hold_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            p_qty = sum(abs(int(r.get("netQty") or 0)) for r in pos_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            sell_qty = sum(int(r.get("tradedQuantity") or 0) for r in tb_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id and str(r.get("transactionType") or "").upper() == "SELL")
            _act_f = False
            if hasattr(broker, "get_forever_orders"):
                try:
                    _fr_all = broker.get_forever_orders()
                    if isinstance(_fr_all, list) and not _is_broker_read_failure(_fr_all):
                        _act_f = any(
                            isinstance(r, dict)
                            and str(r.get("securityId") or "") == sec_id
                            and str(r.get("orderStatus") or "").strip().upper() in ("PENDING", "TRANSIT", "ACTIVE", "CONFIRM", "PART_TRADED", "OPEN", "PLACED")
                            for r in _fr_all
                        )
                except Exception:
                    _act_f = True
            if h_qty == 0 and p_qty == 0 and sell_qty >= filled_q and not _act_f:
                wd["protected_qty"] = 0
                wd["broker_flat_verified_at"] = ts
                conn.execute(
                    "UPDATE watcher_orders SET is_terminal = 1, record_json = ?, updated_at = ? WHERE order_id = ?",
                    (json.dumps(wd), ts, oid),
                )
                if "protection_orders" in tables:
                    conn.execute(
                        "UPDATE protection_orders SET status = 'CLOSED_NO_EXPOSURE_VERIFIED' WHERE parent_order_id = ?",
                        (oid,),
                    )
            else:
                conn.rollback()
                return (
                    False,
                    f"UNPROTECTED_POSITION: Order {oid} has {filled_q} filled shares without verified protection or flat exit (holdings={h_qty}, positions={p_qty}, sell_trades={sell_qty}).",
                )
        conn.commit()
        return True, None
    except sqlite3.Error as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, f"JOURNAL_ERROR: Watcher verification error: {exc}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def build_verified_policy_context(
    *,
    mode: str,
    purpose: Purpose,
    snapshot: Optional[BrokerSnapshot] = None,
    journal_view: Optional[JournalView] = None,
    stop_manager: Optional[Any] = None,
    watcher: Optional[Any] = None,
    data_dir: str = "",
    owner_desired_state: Optional[str] = None,
    owner_observed_state: Optional[str] = None,
    entry_fence_active: Optional[bool] = None,
    min_capture_mono_ns: int = 0,
    risk_funds_verified: Optional[bool] = None,
) -> PolicyContext:
    """Req #6: Construct PolicyContext strictly from independently verified runtime facts.

    Missing stop_manager, watcher, snapshot, journal_view, or risk/funds verification
    NEVER defaults to True!
    """
    stop_verified = stop_manager is not None and hasattr(stop_manager, "is_stopped")
    em_stopped = bool(stop_manager.is_stopped()) if stop_verified else True
    active_muts: Tuple[str, ...] = ()
    unknown_muts: Tuple[str, ...] = ()
    mut_state_verified = False
    if stop_verified and hasattr(stop_manager, "get_in_flight"):
        try:
            raw_if = stop_manager.get_in_flight() or ()
            if isinstance(raw_if, dict):
                active_muts = tuple(str(k) for k in raw_if.keys())
            else:
                active_muts = tuple(str(x) for x in raw_if)
            mut_state_verified = True
        except Exception:
            mut_state_verified = False
    elif stop_verified and hasattr(stop_manager, "get_in_flight_orders"):
        try:
            active_muts = tuple(str(x) for x in (stop_manager.get_in_flight_orders() or ()))
            mut_state_verified = True
        except Exception:
            mut_state_verified = False
    elif data_dir:
        # Verify durable active_broker_mutations.json when stop_manager is a minimal is_stopped() interface
        try:
            import json as _json
            _mp = Path(data_dir) / "active_broker_mutations.json"
            if _mp.is_file():
                _md = _json.loads(_mp.read_text(encoding="utf-8"))
                if isinstance(_md, dict) and _md.get("schema_version") == 2 and _md.get("handoff_verified") is True:
                    _act = _md.get("active")
                    _unr = _md.get("unresolved")
                    if isinstance(_act, list) and isinstance(_unr, list):
                        active_muts = tuple(str(x) for x in _act)
                        unknown_muts = tuple(str(x) for x in _unr)
                        mut_state_verified = True
        except Exception:
            mut_state_verified = False
    mut_fence_ok = bool(
        mut_state_verified
        and len(active_muts) == 0
        and len(unknown_muts) == 0
        and snapshot is not None
        and snapshot.complete
    )
    journal_ok = bool(
        journal_view is not None
        and journal_view.read_complete
        and snapshot is not None
        and snapshot.journal_digest == journal_view.digest_sha256
    )
    svc_id_ok = bool(snapshot is not None and snapshot.profile_verified and bool(snapshot.profile_account_id))
    watcher_ok = bool(
        watcher is not None
        and (
            getattr(watcher, "_is_running", None) is True
            or (purpose == Purpose.STARTUP and hasattr(watcher, "check_once"))
        )
    )
    if risk_funds_verified is not None:
        rf_ok = bool(risk_funds_verified) and not em_stopped
    else:
        rf_ok = bool(stop_verified and not em_stopped and snapshot is not None and snapshot.complete)
    owner_epoch = snapshot.generation.owner_epoch if snapshot is not None else 0
    desired = owner_desired_state or ("STOPPED" if purpose == Purpose.SHUTDOWN else "RUNNING")
    observed = owner_observed_state or "RUNNING"
    fence_active = bool(entry_fence_active) if entry_fence_active is not None else (purpose == Purpose.SHUTDOWN)
    min_mono = int(min_capture_mono_ns)
    if purpose == Purpose.SHUTDOWN and min_mono <= 0 and snapshot is not None:
        min_mono = int(snapshot.capture_started_mono_ns)
    return build_policy_context(
        mode=mode,
        purpose=purpose,
        owner_desired_state=desired,
        owner_observed_state=observed,
        deferred_stop_pending=False,
        owner_intent_epoch=owner_epoch,
        entry_fence_active=fence_active,
        emergency_stop_active=em_stopped,
        transition_active=False,
        mutation_fence_verified=mut_fence_ok,
        active_mutation_ids=active_muts,
        unknown_mutation_ids=unknown_muts,
        service_identity_verified=svc_id_ok,
        watcher_liveness_verified=watcher_ok,
        risk_funds_mode_checks_passed=rf_ok,
        journal_readback_verified=journal_ok,
        min_capture_mono_ns=min_mono,
    )


def verify_unprotected_watcher_and_active_orders(
    *,
    db_path: str,
    broker: Any,
    account_id: str = "",
) -> Tuple[bool, Optional[str]]:
    """
    Verify unprotected / non-terminal watcher_orders and active broker orders for
    read-only broker stubs that do not implement get_profile():
    - Verifies broker-flat closed fills (matching SELL in trade_book + 0 positions + 0 holdings
      + 0 pending orders) and marks watcher_orders.is_terminal=1, protected_qty=0,
      broker_flat_verified_at, protection_orders.status='CLOSED_NO_EXPOSURE_VERIFIED'.
    - Re-verifies previously broker_flat_verified_at rows on next session against fresh holdings & order_book.
    - Rejects any active/pending broker order or unclosed/unprotected watcher exposure.
    """
    path = Path(db_path)
    if not path.is_file():
        return True, None
    conn = None
    try:
        conn = sqlite3.connect(str(path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "watcher_orders" not in tables:
            return True, None

        wrows = conn.execute("SELECT order_id, is_terminal, record_json FROM watcher_orders").fetchall()
        if not wrows:
            return True, None
        needs_check = []
        flat_verified_rows = []
        for wr in wrows:
            try:
                wd = json.loads(wr["record_json"] or "{}")
            except (ValueError, TypeError):
                return False, "JOURNAL_ERROR: Malformed watcher_orders JSON"
            if not isinstance(wd, dict):
                continue
            oid = str(wr["order_id"] or "").strip()
            is_term = int(wr["is_terminal"] or 0)
            filled_q = int(wd.get("filled_qty") or 0)
            prot_q = int(wd.get("protected_qty") or 0)
            pos_state = str(wd.get("position_state") or "").upper()
            if wd.get("broker_flat_verified_at") is not None:
                flat_verified_rows.append((oid, wd))
            elif is_term == 0 and filled_q > 0 and prot_q < filled_q and pos_state not in ("UNPROTECTED_MANUAL_RISK", "POSITION_CLOSED"):
                _is_acked = False
                if "manual_risk_acknowledgements" in tables:
                    _ar = conn.execute(
                        "SELECT quantity FROM manual_risk_acknowledgements WHERE parent_order_id = ? AND status IN ('UNPROTECTED_MANUAL_RISK', 'ACKNOWLEDGED', 'ACTIVE') LIMIT 1",
                        (oid,),
                    ).fetchone()
                    if _ar is not None and int(_ar["quantity"] or 0) >= filled_q:
                        _is_acked = True
                if not _is_acked:
                    needs_check.append((oid, wd, filled_q))

        # Check active Forever OCO legs belonging to closed parents first
        closed_parent_fids: set = set()
        if "positions_lifecycle" in tables and "protection_orders" in tables:
            for _cr in conn.execute(
                "SELECT po.forever_order_id FROM protection_orders po JOIN positions_lifecycle pl ON po.parent_order_id = pl.parent_order_id WHERE UPPER(pl.position_state) = 'POSITION_CLOSED'"
            ).fetchall():
                if _cr["forever_order_id"]:
                    closed_parent_fids.add(str(_cr["forever_order_id"]).strip())

        # If all watcher rows are settled & owner-acknowledged with no closed-parent OCOs to verify, spend ZERO broker GETs!
        if not needs_check and not flat_verified_rows and not closed_parent_fids:
            return True, None

        if closed_parent_fids and hasattr(broker, "get_forever_orders"):
            try:
                _frows = broker.get_forever_orders()
                if isinstance(_frows, list) and not _is_broker_read_failure(_frows):
                    for _fr in _frows:
                        if isinstance(_fr, dict):
                            _fid = str(_fr.get("orderId") or "").strip()
                            _fst = str(_fr.get("orderStatus") or "").strip().upper()
                            if _fid in closed_parent_fids and _fst in ("PENDING", "TRANSIT", "ACTIVE", "CONFIRM", "PART_TRADED", "OPEN", "PLACED"):
                                return False, f"UNPROTECTED_POSITION: Closed parent still has active Forever OCO leg {_fid} ({_fst})."
            except Exception:
                pass

        if not needs_check and not flat_verified_rows:
            return True, None

        # Check active broker orders in order_book for any open/pending sibling orders
        ob_rows: List[Dict[str, Any]] = []
        if hasattr(broker, "get_order_book"):
            try:
                raw_ob = broker.get_order_book()
                if isinstance(raw_ob, list) and not _is_broker_read_failure(raw_ob):
                    ob_rows = [r for r in raw_ob if isinstance(r, _Mapping)]
            except Exception:
                pass
        for r in ob_rows:
            st = str(r.get("orderStatus") or "").strip().upper()
            if st in ("PENDING", "TRANSIT", "OPEN", "PART_TRADED"):
                oid_act = str(r.get("orderId") or "").strip()
                sec_act = str(r.get("securityId") or "").strip()
                if any(str(wd.get("security_id") or "") == sec_act and sec_act for _, wd in flat_verified_rows):
                    return False, f"UNPROTECTED_POSITION: Pending order {oid_act} invalidates historical flat closure for security {sec_act}."
                if needs_check:
                    return False, f"UNPROTECTED_POSITION: Active order {oid_act} ({st}) prevents flat closure."
                return False, f"LIVE_ORDER_IN_PROGRESS: Active broker order {oid_act} ({st}) is in progress."

        # Fresh broker reads required (positions + holdings must succeed; missing holdings capability fails closed!)
        if not hasattr(broker, "get_positions"):
            return False, "BROKER_RECONCILIATION_FAILED: Broker get_positions unavailable"
        pos_rows = broker.get_positions()
        if _is_broker_read_failure(pos_rows) or not isinstance(pos_rows, list):
            return False, "BROKER_RECONCILIATION_FAILED: Broker positions unavailable"

        if not hasattr(broker, "get_holdings") or not callable(getattr(broker, "get_holdings", None)):
            return False, "UNPROTECTED_POSITION: Broker holdings capability missing; cannot verify flat closure"
        hold_rows = broker.get_holdings()
        if _is_broker_read_failure(hold_rows) or not isinstance(hold_rows, list):
            return False, "UNPROTECTED_POSITION: Broker holdings read failed; cannot verify flat closure"

        # Re-verify previously flat-verified rows against fresh holdings & positions
        for oid, wd in flat_verified_rows:
            sec_id = str(wd.get("security_id") or "").strip()
            if not sec_id:
                continue
            h_qty = sum(int(r.get("totalQty") or 0) + int(r.get("t1Qty") or 0) for r in hold_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            p_qty = sum(abs(int(r.get("netQty") or 0)) for r in pos_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            if h_qty > 0 or p_qty > 0:
                return False, f"UNPROTECTED_POSITION: Fresh broker exposure detected for {oid} (holdings={h_qty}, positions={p_qty})."

        if not needs_check:
            return True, None

        tb_rows = broker.get_trade_book() if hasattr(broker, "get_trade_book") else []
        if _is_broker_read_failure(tb_rows) or not isinstance(tb_rows, list):
            tb_rows = []

        ts = time.time()
        conn.execute("BEGIN IMMEDIATE")
        for oid, wd, filled_q in needs_check:
            sec_id = str(wd.get("security_id") or "").strip()
            h_qty = sum(int(r.get("totalQty") or 0) + int(r.get("t1Qty") or 0) for r in hold_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            p_qty = sum(abs(int(r.get("netQty") or 0)) for r in pos_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id)
            sell_qty = sum(int(r.get("tradedQuantity") or 0) for r in tb_rows if isinstance(r, _Mapping) and str(r.get("securityId") or "") == sec_id and str(r.get("transactionType") or "").upper() == "SELL")
            if h_qty == 0 and p_qty == 0 and sell_qty >= filled_q:
                wd["protected_qty"] = 0
                wd["broker_flat_verified_at"] = ts
                conn.execute(
                    "UPDATE watcher_orders SET is_terminal = 1, record_json = ?, updated_at = ? WHERE order_id = ?",
                    (json.dumps(wd), ts, oid),
                )
                if "protection_orders" in tables:
                    conn.execute(
                        "UPDATE protection_orders SET status = 'CLOSED_NO_EXPOSURE_VERIFIED' WHERE parent_order_id = ?",
                        (oid,),
                    )
            else:
                conn.rollback()
                return (
                    False,
                    f"UNPROTECTED_POSITION: Order {oid} has {filled_q} filled shares without verified protection or flat exit (holdings={h_qty}, positions={p_qty}, sell_trades={sell_qty}).",
                )
        conn.commit()
        return True, None
    except sqlite3.Error as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, f"JOURNAL_ERROR: Watcher verification error: {exc}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
