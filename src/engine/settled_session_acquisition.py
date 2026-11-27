"""Generalized 6-point multi-book settled-session predicate (Phase 2D / C04-safe).

Built directly on `fe946810`'s verified `BrokerSnapshot`, `JournalView`,
`VerifiedCalendar`, and `classify_broker_session` contracts.

Fixes C04: NEVER certifies settlement from `history_coverage` metadata alone.
When a closed CNC sibling exists, requires:
1. Verified exchange calendar establishing current IST trading session > closed exit date.
2. No unresolved negative CNC carryover in `positions`.
3. `holdings` equals exact expected settled open quantity (with independent holdings/profile
   confirmation whenever any holdings row omits `dhanClientId`).
4. Current `trade_book` has no unexplained execution affecting relevant contracts.
5. Actual `historical_trades` execution payloads + `terminal_empty_page_verified` corroborate
   every closed sibling's actual verified BUY and SELL fills (blank-EID rollups accepted
   strictly as corroboration against existing durable fills, never fabricated).
6. `forever_orders`, `super_orders`, AND actual child `order_status` payloads prove zero
   remaining executable quantity on every closed sibling.

Also generalizes cleanly to flat accounts, ordinary protected holdings without a closed
sibling, acknowledged retained manual-risk lots, multiple securities, and mixed CNC/MTF.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Optional, Tuple

from src.broker.broker_snapshot import (
    BrokerSnapshot,
    EvidenceRequest,
    LIVE_BOOKS,
    NS,
    Purpose,
    SessionContext,
    SessionPhase,
    thaw_json,
)
from src.engine.broker_session import BrokerSessionState, IST, VerifiedCalendar, classify_broker_session
from src.engine.holding_quantity import aggregate_holdings
from src.engine.journal_view import JournalView
from src.engine.signed_delivery_live_evidence import ClosedHistoricalBuyMatcher, _decimal_price
from src.engine.unified_exit_reconciliation import _exchange_time, _integer

_DEAD_STATES = frozenset({"CANCELLED", "CANCELED", "REJECTED", "EXPIRED"})
_TERMINAL_FILLED = frozenset({"TRADED", "CLOSED", "FILLED", "COMPLETE"})
_ACTIVE_STATES = frozenset({"PENDING", "TRANSIT", "PART_TRADED", "ACTIVE", "CONFIRM", "OPEN", "TRIGGER_PENDING"})


@dataclass(frozen=True, slots=True)
class SettledSessionResult:
    settled: bool
    reason_code: str
    state: BrokerSessionState
    session: SessionContext


def _blocked(reason_code: str, observed_utc_ns: int) -> SettledSessionResult:
    return SettledSessionResult(
        settled=False,
        reason_code=reason_code,
        state=BrokerSessionState.AMBIGUOUS_BLOCKED,
        session=SessionContext(
            SessionPhase.TRANSITION_UNVERIFIED, None, observed_utc_ns, None, False
        ),
    )


def _date_ist(ts: float) -> date:
    return datetime.fromtimestamp(float(ts), IST).date()


def _verify_historical_executions_for_closed_parent(
    snapshot: BrokerSnapshot,
    account: str,
    security: str,
    segment: str,
    product: str,
    session_date: date,
    buy_fills: list[Mapping[str, Any]],
    sell_fills: list[Mapping[str, Any]],
) -> Tuple[bool, str]:
    """Verify actual historical_trades payloads corroborate every closed BUY & SELL fill (C04)."""
    if not snapshot.history_coverage or any(
        not cov.complete or not cov.terminal_empty_page_verified
        for cov in snapshot.history_coverage
    ):
        return False, "HISTORY_COVERAGE_INCOMPLETE"

    # Must have actual complete historical_trades book payloads, not metadata alone!
    complete_hist_books = [b for b in snapshot.historical_trades if b.complete and b.payload is not None]
    if not complete_hist_books:
        return False, "HISTORICAL_EXECUTION_PAYLOADS_MISSING"

    hist_rows = []
    for b in complete_hist_books:
        for row in thaw_json(b.payload):
            if isinstance(row, dict) and str(row.get("securityId") or "").strip() == security:
                hist_rows.append(row)

    if not hist_rows:
        return False, "HISTORICAL_EXECUTION_ROWS_ABSENT"

    # Build expected verified fill map & rollups for both BUY and SELL
    expected_trades: dict[tuple[str, str], tuple[str, int, float, float]] = {}
    for side, rows in (("BUY", buy_fills), ("SELL", sell_fills)):
        for f in rows:
            boid = str(f.get("broker_order_id") or "").strip()
            eid = str(f.get("exchange_trade_id") or "").strip()
            if not boid or not eid or f.get("is_verified_trade") != 1:
                return False, f"UNVERIFIED_DURABLE_{side}_FILL"
            expected_trades[(boid, eid)] = (
                side,
                int(f["traded_quantity"]),
                float(f["traded_price"]),
                float(f["traded_at"]),
            )

    # Group by (order_id, side) for corroboration-only blank-EID rollup verification
    grouped: dict[tuple[str, str], list[tuple[str, int, float, float]]] = {}
    for (oid, _eid), val in expected_trades.items():
        grouped.setdefault((oid, val[0]), []).append(val)

    rollups: dict[tuple[str, str], tuple[int, float, float, float]] = {}
    for gkey, items in grouped.items():
        tot_q = sum(x[1] for x in items)
        vwap = sum(x[1] * x[2] for x in items) / tot_q
        rollups[gkey] = (tot_q, vwap, min(x[3] for x in items), max(x[3] for x in items))

    matched_keys: set[tuple[str, str]] = set()
    used_rollup_orders: set[tuple[str, str]] = set()
    used_exact_orders: set[tuple[str, str]] = set()

    for row in hist_rows:
        oid = str(row.get("orderId") or "").strip()
        side = str(row.get("transactionType") or "").strip().upper()
        if (oid, side) not in rollups:
            continue
        if (
            str(row.get("dhanClientId") or "").strip() != account
            or str(row.get("exchangeSegment") or "").strip().upper() != segment
            or str(row.get("productType") or "").strip().upper() != product
        ):
            return False, f"HISTORICAL_{side}_CONTRACT_MISMATCH"
        if isinstance(row.get("exchangeTradeId"), bool):
            return False, "BOOLEAN_HISTORICAL_EID"
        eid = str(row.get("exchangeTradeId") or "").strip()
        try:
            day = _date_ist(_exchange_time(row.get("exchangeTime")))
            qty = _integer(row.get("tradedQuantity"), positive=True)
            price = float(_decimal_price(row.get("tradedPrice")))
        except (ValueError, TypeError):
            return False, f"MALFORMED_HISTORICAL_{side}_ROW"
        if day >= session_date:
            return False, f"HISTORICAL_{side}_DATE_NOT_PRIOR"

        if eid.upper() in ("", "0", "NONE", "NULL"):
            # Corroboration-only blank-EID rollup (must not mix with exact rows for same order)
            if (oid, side) in used_exact_orders or (oid, side) in used_rollup_orders:
                return False, f"AMBIGUOUS_HISTORICAL_{side}_ROLLUP"
            eoid = str(row.get("exchangeOrderId") or "").strip()
            if isinstance(row.get("exchangeOrderId"), bool) or eoid.upper() in ("", "0", "NONE", "NULL"):
                return False, f"HISTORICAL_{side}_ROLLUP_MISSING_EOID"
            tot_q, vwap, first_ts, last_ts = rollups[(oid, side)]
            if (
                qty != tot_q
                or abs(price - vwap) >= 1e-4
                or day != _date_ist(first_ts)
                or day != _date_ist(last_ts)
            ):
                return False, f"HISTORICAL_{side}_ROLLUP_CONFLICT"
            constituent = {k for k, v in expected_trades.items() if k[0] == oid and v[0] == side}
            if not constituent or (constituent & matched_keys):
                return False, f"DUPLICATE_HISTORICAL_{side}_ROLLUP"
            used_rollup_orders.add((oid, side))
            matched_keys.update(constituent)
        else:
            if (oid, side) in used_rollup_orders:
                return False, f"AMBIGUOUS_HISTORICAL_{side}_EXACT_AND_ROLLUP"
            exp = expected_trades.get((oid, eid))
            if (
                not exp
                or exp[0] != side
                or exp[1] != qty
                or abs(exp[2] - price) >= 1e-6
                or day != _date_ist(exp[3])
                or (oid, eid) in matched_keys
            ):
                return False, f"HISTORICAL_{side}_EXACT_CONFLICT"
            used_exact_orders.add((oid, side))
            matched_keys.add((oid, eid))

    if matched_keys != set(expected_trades.keys()):
        missing_sides = {expected_trades[k][0] for k in (set(expected_trades.keys()) - matched_keys)}
        return False, f"HISTORICAL_CORROBORATION_INCOMPLETE:{','.join(sorted(missing_sides))}"
    return True, "OK"


def _verify_closed_sibling_child_status(
    snapshot: BrokerSnapshot,
    account: str,
    security: str,
    segment: str,
    product: str,
    sell_order_ids: set[str],
    group_ids: set[str],
) -> Tuple[bool, str]:
    """Verify actual terminal child order_status and zero residual executable qty (C04)."""
    if not sell_order_ids:
        return False, "CLOSED_SIBLING_MISSING_SELL_ORDER_IDS"

    # Build index of order rows from order_book and explicit order_status map
    orders_by_id: dict[str, dict] = {}
    if snapshot.order_book.complete and snapshot.order_book.payload is not None:
        for r in thaw_json(snapshot.order_book.payload):
            if isinstance(r, dict) and str(r.get("orderId") or "").strip():
                orders_by_id[str(r["orderId"]).strip()] = r
    for oid, book in snapshot.order_status.items():
        if book.complete and isinstance(book.payload, Mapping):
            orders_by_id[str(oid).strip()] = thaw_json(book.payload)

    for soid in sorted(sell_order_ids):
        row = orders_by_id.get(soid)
        if row is None:
            return False, f"CLOSED_CHILD_ORDER_STATUS_MISSING:{soid}"
        if (
            str(row.get("dhanClientId") or "").strip() != account
            or str(row.get("securityId") or "").strip() != security
            or str(row.get("exchangeSegment") or "").strip().upper() != segment
            or str(row.get("productType") or "").strip().upper() != product
            or str(row.get("transactionType") or "").strip().upper() != "SELL"
        ):
            return False, f"CLOSED_CHILD_IDENTITY_MISMATCH:{soid}"
        st = str(row.get("orderStatus") or "").strip().upper()
        if st not in _TERMINAL_FILLED:
            return False, f"CLOSED_CHILD_NOT_TERMINAL:{soid}:{st}"
        rem = row.get("remainingQuantity")
        if isinstance(rem, bool) or rem is None or int(rem) != 0:
            return False, f"CLOSED_CHILD_RESIDUAL_NONZERO:{soid}"

    # Verify no active protective legs remain for the closed sibling's groups
    if snapshot.forever_orders.complete and snapshot.forever_orders.payload is not None:
        for row in thaw_json(snapshot.forever_orders.payload):
            if not isinstance(row, dict):
                continue
            gid = str(row.get("orderId") or "").strip()
            if gid in group_ids and str(row.get("orderStatus") or "").strip().upper() in _ACTIVE_STATES:
                return False, f"CLOSED_SIBLING_ACTIVE_FOREVER_LEG:{gid}"
    if snapshot.super_orders.complete and snapshot.super_orders.payload is not None:
        for row in thaw_json(snapshot.super_orders.payload):
            if not isinstance(row, dict):
                continue
            gid = str(row.get("orderId") or "").strip()
            if gid in group_ids and str(row.get("orderStatus") or "").strip().upper() in _ACTIVE_STATES:
                return False, f"CLOSED_SIBLING_ACTIVE_SUPER_ORDER:{gid}"

    return True, "OK"


def verify_settled_session_evidence(
    snapshot: Optional[BrokerSnapshot],
    journal: Optional[JournalView],
    *,
    calendar: Optional[VerifiedCalendar],
    now_mono_ns: int,
    holdings_confirmation: Optional[BrokerSnapshot] = None,
    require_history_for_flat: bool = True,
) -> SettledSessionResult:
    """Evaluate the generalized 6-point multi-book settled-session predicate."""
    obs_utc = snapshot.capture_finished_utc_ns if snapshot is not None else 0
    if snapshot is None:
        return _blocked("SNAPSHOT_MISSING", obs_utc)
    if not isinstance(calendar, VerifiedCalendar):
        return _blocked("CALENDAR_UNVERIFIED", obs_utc)
    if journal is None or not journal.read_complete or journal.account_id != snapshot.account_id:
        return _blocked("JOURNAL_VIEW_INCOMPLETE", obs_utc)
    if snapshot.journal_digest is not None and snapshot.journal_digest != journal.digest_sha256:
        return _blocked("JOURNAL_DIGEST_MISMATCH", obs_utc)

    # Validate freshness (<=10s live books), generation, profile identity, completeness
    req = EvidenceRequest(
        snapshot.account_id,
        Purpose.WATCHER,
        LIVE_BOOKS,
        history_ranges=tuple((h.from_date, h.to_date) for h in snapshot.history_coverage),
        order_status_ids=tuple(snapshot.order_status),
        trade_order_ids=tuple(snapshot.trades_for_order),
    )
    val_reasons = snapshot.validation_reasons(req, snapshot.generation, now_mono_ns)
    if val_reasons:
        return _blocked(val_reasons[0], obs_utc)

    account = snapshot.account_id
    current_ist = datetime.fromtimestamp(obs_utc // NS, IST)
    session_date = current_ist.date()
    try:
        if not calendar.trading(session_date):
            session_date = calendar.previous(session_date)
    except ValueError:
        return _blocked("CALENDAR_COVERAGE_UNAVAILABLE", obs_utc)

    # Independent holdings/profile confirmation when any holdings row omits dhanClientId
    hold_rows = thaw_json(snapshot.holdings.payload) if snapshot.holdings.payload is not None else []
    if any(isinstance(r, dict) and r.get("dhanClientId") != account for r in hold_rows):
        if (
            holdings_confirmation is None
            or holdings_confirmation.snapshot_id == snapshot.snapshot_id
            or holdings_confirmation.generation != snapshot.generation
            or not holdings_confirmation.profile_verified
            or holdings_confirmation.profile_account_id != account
            or not holdings_confirmation.holdings.complete
            or holdings_confirmation.holdings.payload_sha256 != snapshot.holdings.payload_sha256
        ):
            return _blocked("HOLDINGS_ACCOUNT_BINDING_REQUIRED", obs_utc)

    # Point 2: No unresolved prior-session negative CNC carryover in positions
    pos_rows = thaw_json(snapshot.positions.payload) if snapshot.positions.payload is not None else []
    for pos in pos_rows:
        if not isinstance(pos, dict):
            return _blocked("MALFORMED_POSITION_ROW", obs_utc)
        prod = str(pos.get("productType") or "").strip().upper()
        net_q = pos.get("netQty")
        if isinstance(net_q, bool) or net_q is None:
            return _blocked("MALFORMED_POSITION_QTY", obs_utc)
        if prod == "CNC" and int(net_q) < 0:
            return _blocked("NEGATIVE_CNC_CARRYOVER_ACTIVE", obs_utc)

    # Compute expected open holdings & closed siblings from verified JournalView
    fills_by_parent: dict[str, list[Mapping[str, Any]]] = {}
    for f in journal.fills:
        pid = str(f.get("parent_order_id") or f.get("broker_order_id") or "").strip()
        if pid:
            fills_by_parent.setdefault(pid, []).append(f)

    exits_by_parent: dict[str, list[Mapping[str, Any]]] = {}
    for ex in journal.exit_allocations:
        pid = str(ex.get("parent_order_id") or "").strip()
        if pid:
            exits_by_parent.setdefault(pid, []).append(ex)

    prot_groups_by_parent: dict[str, set[str]] = {}
    for po in journal.protection_orders:
        pid = str(po.get("parent_order_id") or "").strip()
        fid = str(po.get("forever_order_id") or "").strip()
        if pid and fid:
            prot_groups_by_parent.setdefault(pid, set()).add(fid)

    expected_open_by_sec: dict[str, int] = {}
    closed_siblings: list[tuple[str, str, str, str, list[Mapping[str, Any]], list[Mapping[str, Any]], set[str]]] = []

    for life in journal.positions_lifecycle:
        pid = str(life.get("parent_order_id") or "").strip()
        sec = str(life.get("security_id") or "").strip()
        seg = str(life.get("exchange_segment") or "").strip().upper()
        prod = str(life.get("product_type") or "").strip().upper()
        state = str(life.get("position_state") or "").strip().upper()
        if prod not in ("CNC", "MTF"):
            continue
        p_fills = fills_by_parent.get(pid, [])
        buy_f = [f for f in p_fills if str(f.get("transaction_type") or "").upper() == "BUY" and f.get("is_verified_trade") == 1]
        sell_f = [f for f in p_fills if str(f.get("transaction_type") or "").upper() == "SELL" and f.get("is_verified_trade") == 1]
        buy_qty = sum(int(f.get("traded_quantity") or 0) for f in buy_f)
        sell_qty = sum(int(ex.get("allocated_qty") or 0) for ex in exits_by_parent.get(pid, []))
        open_q = buy_qty - sell_qty
        if open_q < 0:
            return _blocked("OVER_ALLOCATED_EXIT_QTY", obs_utc)
        if state == "POSITION_CLOSED":
            if buy_qty <= 0 or sell_qty != buy_qty or not sell_f:
                return _blocked("UNVERIFIED_HISTORICAL_CLOSURE_NO_FILLS", obs_utc)
            # Point 1: session_date MUST be strictly later than closed sibling execution date
            max_sell_date = max(_date_ist(float(f["traded_at"])) for f in sell_f)
            if session_date <= max_sell_date:
                return _blocked("CLOSED_SIBLING_SAME_OR_FUTURE_SESSION", obs_utc)
            closed_siblings.append((pid, sec, seg, prod, buy_f, sell_f, prot_groups_by_parent.get(pid, set())))
        elif open_q > 0:
            expected_open_by_sec[sec] = expected_open_by_sec.get(sec, 0) + open_q

    # Also detect watcher_orders claiming POSITION_CLOSED with no lifecycle/fills
    life_pids = {str(l.get("parent_order_id") or "").strip() for l in journal.positions_lifecycle}
    for wrow in journal.watcher_orders:
        try:
            import json
            rec = json.loads(wrow.get("record_json") or "{}")
        except (ValueError, TypeError):
            return _blocked("WATCHER_RECORD_MALFORMED", obs_utc)
        wid = str(wrow.get("order_id") or "").strip()
        if str(rec.get("position_state") or "").upper() == "POSITION_CLOSED" and wid not in life_pids:
            return _blocked("UNVERIFIED_HISTORICAL_CLOSURE_NO_FILLS", obs_utc)

    # Point 3: Exact holdings totalQty match per security
    try:
        agg_hold = aggregate_holdings(hold_rows, account_id=account)
    except (ValueError, TypeError, KeyError):
        return _blocked("HOLDINGS_AGGREGATION_FAILED", obs_utc)
    nonzero_hold = {sec: qty for sec, qty in agg_hold.items() if qty > 0}
    if nonzero_hold != expected_open_by_sec:
        return _blocked("HOLDINGS_SETTLED_QTY_MISMATCH", obs_utc)

    # Point 4: Current trade_book must have no unexplained current-session executions
    trade_rows = thaw_json(snapshot.trade_book.payload) if snapshot.trade_book.payload is not None else []
    known_eids = {str(f.get("exchange_trade_id") or "").strip() for f in journal.fills if f.get("is_verified_trade") == 1}
    for tr in trade_rows:
        if not isinstance(tr, dict):
            return _blocked("MALFORMED_TRADE_BOOK_ROW", obs_utc)
        eid = str(tr.get("exchangeTradeId") or "").strip()
        if not eid or eid not in known_eids:
            return _blocked("UNEXPLAINED_CURRENT_SESSION_EXECUTION", obs_utc)

    # Point 5 & Point 6: For every closed sibling, verify ACTUAL historical BUY/SELL payloads AND child order_status!
    for pid, sec, seg, prod, buy_f, sell_f, gids in closed_siblings:
        hist_ok, hist_reason = _verify_historical_executions_for_closed_parent(
            snapshot, account, sec, seg, prod, session_date, buy_f, sell_f
        )
        if not hist_ok:
            return _blocked(hist_reason, obs_utc)
        sell_oids = {str(f.get("broker_order_id") or "").strip() for f in sell_f if str(f.get("broker_order_id") or "").strip()}
        child_ok, child_reason = _verify_closed_sibling_child_status(
            snapshot, account, sec, seg, prod, sell_oids, gids
        )
        if not child_ok:
            return _blocked(child_reason, obs_utc)

    # Generalized flat / ordinary open account: require verified historical coverage when requested
    if not closed_siblings and require_history_for_flat:
        if not snapshot.history_coverage or any(
            not cov.complete or not cov.terminal_empty_page_verified
            for cov in snapshot.history_coverage
        ):
            return _blocked("HISTORY_COVERAGE_INCOMPLETE", obs_utc)
        # Also verify no unmatched historical execution exists in complete historical books
        for b in snapshot.historical_trades:
            if b.complete and b.payload is not None:
                for row in thaw_json(b.payload):
                    if not isinstance(row, dict):
                        return _blocked("MALFORMED_HISTORICAL_ROW", obs_utc)
                    eid = str(row.get("exchangeTradeId") or "").strip()
                    if not eid or eid not in known_eids:
                        return _blocked("UNMATCHED_HISTORICAL_EXECUTION", obs_utc)

    # Final canonical classification via fe946810's classify_broker_session()
    state, ctx = classify_broker_session(
        observed_utc_ns=obs_utc,
        calendar=calendar,
        execution_dates=(),
        broker_session_date=session_date,
        books_complete=True,
        settlement_verified=True,
        signed_delivery_verified=False,
        product="CNC",
    )
    if state != BrokerSessionState.T1_SETTLED:
        return _blocked("SESSION_CLASSIFIER_REJECTED_SETTLED", obs_utc)
    return SettledSessionResult(True, "SETTLED_SIX_POINT_VERIFIED", state, ctx)
