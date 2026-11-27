"""Phase 2D: C02/C03/C09-safe durable orphan-leg and zero-fill cancellation resolution.

Strict invariants enforced:
- C09: Reject fractional (e.g. 0.9), float, boolean, negative, malformed, and absent
  quantities. Only exact integer zero (type(v) is int or pure ASCII digit string "0")
  is accepted.
- C02/C03: Require TWO genuinely independent fresh BrokerSnapshots
  (snap1.snapshot_id != snap2.snapshot_id) separated by >= min_separation_ns (1s default),
  with identical compatible Generation, actual transport provenance (transport_sent=True),
  exact account/security/segment/product/orderId/parent/group binding, explicit zero filled
  and zero remaining quantity, zero fills in trade_book, and zero conflicting observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from src.broker.broker_snapshot import (
    BrokerSnapshot,
    BookStatus,
    thaw_json,
)

NS = 1_000_000_000
MAX_LIVE_AGE_NS = 10 * NS
DEFAULT_MIN_SEPARATION_NS = 2 * NS
_ZERO_FILL_TERMINAL_STATES = frozenset({"CANCELLED", "REJECTED", "EXPIRED"})


@dataclass(frozen=True)
class DurableResolutionOutcome:
    resolved: bool
    reason_code: str
    resolved_ids: Tuple[str, ...] = ()
    remaining_ids: Tuple[str, ...] = ()
    snapshot_ids: Tuple[str, ...] = ()


def _strict_exact_int(value: Any) -> Optional[int]:
    """C09: Accept ONLY exact non-negative integers (reject float/fractional, bool, negative, None, malformed)."""
    if value is None or isinstance(value, bool):
        return None
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str):
        s = value.strip()
        if not s or not s.isdigit():
            return None
        return int(s)
    # Reject float (including 0.9, 0.0), Decimal, list, dict, etc.
    return None


def _strict_zero_fill_terminal_dead(row: Mapping[str, Any]) -> Tuple[bool, str]:
    """C09: Verify terminal CANCELLED/REJECTED/EXPIRED with exact integer filledQty==0 and remainingQuantity==0."""
    if not isinstance(row, Mapping):
        return False, "MALFORMED_ORDER_ROW"
    status = str(row.get("orderStatus") or "").strip().upper()
    if status not in _ZERO_FILL_TERMINAL_STATES:
        return False, f"NON_TERMINAL_OR_FILLED_STATUS:{status or 'MISSING'}"

    filled = _strict_exact_int(row.get("filledQty"))
    if filled is None:
        return False, "INVALID_OR_MISSING_FILLED_QTY"
    if filled != 0:
        return False, f"NONZERO_FILLED_QTY:{filled}"

    rem = _strict_exact_int(row.get("remainingQuantity"))
    if rem is None:
        return False, "INVALID_OR_MISSING_REMAINING_QTY"
    if rem != 0:
        return False, f"NONZERO_REMAINING_QTY:{rem}"

    return True, status


def _validate_two_independent_snapshots(
    snap1: Optional[BrokerSnapshot],
    snap2: Optional[BrokerSnapshot],
    *,
    expected_account_id: str,
    now_mono_ns: int,
    min_separation_ns: int = DEFAULT_MIN_SEPARATION_NS,
) -> Tuple[bool, str]:
    if snap1 is None or snap2 is None:
        return False, "MISSING_INDEPENDENT_SNAPSHOTS"
    if not isinstance(snap1, BrokerSnapshot) or not isinstance(snap2, BrokerSnapshot):
        return False, "INVALID_SNAPSHOT_TYPE"
    if not snap1.snapshot_id or not snap2.snapshot_id or snap1.snapshot_id == snap2.snapshot_id:
        return False, "IDENTICAL_OR_MISSING_SNAPSHOT_ID"
    if snap1.account_id != expected_account_id or snap2.account_id != expected_account_id:
        return False, "ACCOUNT_MISMATCH"
    if not snap1.complete or not snap2.complete:
        return False, "INCOMPLETE_SNAPSHOT"
    if not snap1.profile_verified or not snap2.profile_verified:
        return False, "PROFILE_UNVERIFIED"
    if snap1.generation != snap2.generation:
        return False, "GENERATION_MISMATCH_BETWEEN_SNAPSHOTS"

    # Enforce approved minimum separation interval between independent captures
    sep_ns = snap2.capture_started_mono_ns - snap1.capture_finished_mono_ns
    if sep_ns < min_separation_ns:
        return False, f"INSUFFICIENT_SNAPSHOT_SEPARATION:{sep_ns}"

    # Enforce 10-second live freshness ceiling on both snapshots
    for idx, snap in ((1, snap1), (2, snap2)):
        age_ns = now_mono_ns - snap.capture_finished_mono_ns
        if age_ns < 0 or age_ns > MAX_LIVE_AGE_NS:
            return False, f"STALE_SNAPSHOT_{idx}"
        for bname in ("order_book", "trade_book", "forever_orders"):
            book = getattr(snap, bname, None)
            if (
                book is None
                or not book.complete
                or book.status not in (BookStatus.OK_EMPTY, BookStatus.OK_RECORDS)
                or book.transport_sent is not True
            ):
                return False, f"UNVERIFIED_TRANSPORT_PROVENANCE_{bname.upper()}_{idx}"
    return True, "OK"


def _verify_order_dead_in_snapshot(
    snap: BrokerSnapshot,
    *,
    order_id: str,
    expected_account_id: str,
    expected_security_id: str,
    expected_segment: str,
    expected_product: str,
    expected_parent_id: str = "",
    expected_group_id: str = "",
    require_linkage: bool = False,
) -> Tuple[bool, str]:
    """Verify exact order identity, zero-fill dead status, zero trade_book fills, and zero conflicts."""
    # Check trade_book first: ANY fill referencing order_id immediately blocks
    tb_rows = thaw_json(snap.trade_book.payload) if snap.trade_book.payload is not None else []
    if isinstance(tb_rows, list):
        for tr in tb_rows:
            if not isinstance(tr, Mapping):
                continue
            if str(tr.get("orderId") or "").strip() == order_id:
                return False, f"TRADE_BOOK_FILL_OBSERVED:{order_id}"

    # Collect all observations of order_id across order_book, forever_orders, and order_status
    observations: List[Mapping[str, Any]] = []
    for book in (snap.order_book, snap.forever_orders):
        rows = thaw_json(book.payload) if book.payload is not None else []
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, Mapping) and str(r.get("orderId") or "").strip() == order_id:
                    observations.append(r)
    if order_id in snap.order_status:
        os_book = snap.order_status[order_id]
        if os_book.complete and os_book.transport_sent is True and os_book.payload is not None:
            raw = thaw_json(os_book.payload)
            if isinstance(raw, Mapping):
                observations.append(raw)
            elif isinstance(raw, list):
                for r in raw:
                    if isinstance(r, Mapping) and str(r.get("orderId") or "").strip() == order_id:
                        observations.append(r)

    if not observations:
        return False, "ORDER_NOT_FOUND_IN_SNAPSHOT"

    # EVERY observation must agree on exact identity, binding, and strict zero-fill terminal dead status
    observed_statuses = set()
    parent_linked = False
    group_linked = False
    for row in observations:
        if str(row.get("dhanClientId") or "").strip() != expected_account_id:
            return False, "ORDER_ACCOUNT_MISMATCH"
        if str(row.get("securityId") or "").strip() != expected_security_id:
            return False, "ORDER_SECURITY_MISMATCH"
        if str(row.get("exchangeSegment") or "").strip().upper() != expected_segment:
            return False, "ORDER_SEGMENT_MISMATCH"
        if str(row.get("productType") or "").strip().upper() != expected_product:
            return False, "ORDER_PRODUCT_MISMATCH"
        row_parent = str(row.get("parentOrderId") or "").strip()
        row_algo = str(row.get("algoId") or "").strip()
        if expected_parent_id:
            if row_parent:
                if row_parent != expected_parent_id:
                    return False, "ORDER_PARENT_BINDING_MISMATCH"
                parent_linked = True
        if expected_group_id:
            if row_algo:
                if row_algo != expected_group_id:
                    return False, "ORDER_GROUP_BINDING_MISMATCH"
                group_linked = True
            elif str(row.get("orderId") or "").strip() == expected_group_id:
                group_linked = True
        ok_dead, st_or_err = _strict_zero_fill_terminal_dead(row)
        if not ok_dead:
            return False, st_or_err
        observed_statuses.add(st_or_err)

    if len(observed_statuses) != 1:
        return False, f"CONFLICTING_ORDER_OBSERVATIONS:{sorted(observed_statuses)}"

    # C46 + C54: Unconditional sibling-leg safety AND parent/group linkage predicates.
    # Sibling-leg residual checks ALWAYS execute regardless of whether linkage was explicit on /v2/orders.
    if require_linkage:
        # Predicate I (Forever Orders): UNCONDITIONALLY check all rows in expected_group_id OR any
        # algoId/foreverOrderId observed on the target order rows for residual live/unverified siblings
        _observed_group_ids = set()
        if expected_group_id:
            _observed_group_ids.add(expected_group_id)
        for _obs_row in observations:
            for _gk in ("algoId", "foreverOrderId", "groupId"):
                _gv = str(_obs_row.get(_gk) or "").strip()
                if _gv:
                    _observed_group_ids.add(_gv)
        if _observed_group_ids:
            if not snap.forever_orders.complete:
                return False, "FOREVER_BOOK_INCOMPLETE_FOR_ORPHAN_PROOF"
            fo_rows = thaw_json(snap.forever_orders.payload) if snap.forever_orders.payload is not None else []
            if isinstance(fo_rows, list):
                matching_fo = [
                    r for r in fo_rows
                    if isinstance(r, Mapping)
                    and str(r.get("orderId") or "").strip() in _observed_group_ids
                ]
                for _fr in matching_fo:
                    if str(_fr.get("dhanClientId") or "").strip() != expected_account_id:
                        return False, "ORDER_ACCOUNT_MISMATCH"
                    if (
                        str(_fr.get("securityId") or "").strip() != expected_security_id
                        or str(_fr.get("exchangeSegment") or "").strip().upper() != expected_segment
                        or str(_fr.get("productType") or "").strip().upper() != expected_product
                    ):
                        return False, "ORDER_CONTRACT_MISMATCH"
                    _fp = str(_fr.get("parentOrderId") or "").strip()
                    if _fp and expected_parent_id and _fp != expected_parent_id:
                        return False, "ORDER_PARENT_BINDING_MISMATCH"
                    _fst = str(_fr.get("orderStatus") or "").strip().upper()
                    if _fst in ("CANCELLED", "CANCELED", "REJECTED", "EXPIRED"):
                        pass
                    elif _fst == "TRIGGERED":
                        # C54: Never equate TRIGGERED alone with zero residual executable quantity!
                        _rem_fr = _fr.get("remainingQuantity")
                        if type(_rem_fr) is not int or _rem_fr != 0:
                            return False, "ORPHAN_TRIGGERED_RESIDUAL_UNVERIFIED"
                        _trig_oid = str(_fr.get("triggeredOrderId") or "").strip()
                        if _trig_oid and _trig_oid != order_id:
                            return False, f"ORPHAN_RESIDUAL_EXECUTABLE_SIBLING:TRIGGERED:{_trig_oid}"
                    else:
                        return False, f"ORPHAN_RESIDUAL_EXECUTABLE_SIBLING:{_fst}"
                    if _fp and expected_parent_id and _fp == expected_parent_id:
                        parent_linked = True
                    if str(_fr.get("triggeredOrderId") or "").strip() == order_id:
                        group_linked = True

        # Predicate I (Super Orders): UNCONDITIONALLY check matching Super Order parent for live sibling legs
        if expected_parent_id:
            if not snap.super_orders.complete:
                return False, "SUPER_BOOK_INCOMPLETE_FOR_ORPHAN_PROOF"
            so_rows = thaw_json(snap.super_orders.payload) if snap.super_orders.payload is not None else []
            if isinstance(so_rows, list):
                for _sr in so_rows:
                    if (
                        isinstance(_sr, Mapping)
                        and str(_sr.get("orderId") or "").strip() == expected_parent_id
                        and str(_sr.get("dhanClientId") or "").strip() == expected_account_id
                        and str(_sr.get("securityId") or "").strip() == expected_security_id
                    ):
                        legs = _sr.get("legDetails") or []
                        if isinstance(legs, list):
                            found_target_leg = False
                            for _lg in legs:
                                if not isinstance(_lg, Mapping):
                                    continue
                                _lg_oid = str(_lg.get("orderId") or "").strip()
                                _lg_st = str(_lg.get("orderStatus") or "").strip().upper()
                                _lg_rem = _lg.get("remainingQuantity")
                                if _lg_oid == order_id:
                                    found_target_leg = True
                                elif str(_lg.get("legName") or "").strip().upper() in ("STOP_LOSS_LEG", "TARGET_LEG"):
                                    if _lg_st not in ("CANCELLED", "CANCELED", "REJECTED", "EXPIRED") or (type(_lg_rem) is int and _lg_rem != 0):
                                        return False, f"ORPHAN_RESIDUAL_EXECUTABLE_SIBLING:SUPER:{_lg_st}"
                            if found_target_leg:
                                parent_linked = True
                                if not expected_group_id or expected_group_id == expected_parent_id:
                                    group_linked = True

        if expected_parent_id and not parent_linked:
            return False, "ORPHAN_PARENT_LINKAGE_UNVERIFIED"
        if expected_group_id and not group_linked:
            return False, "ORPHAN_GROUP_LINKAGE_UNVERIFIED"

    return True, next(iter(observed_statuses))


def resolve_ordinary_zero_fill_cancellation(
    record: Dict[str, Any],
    snap1: Optional[BrokerSnapshot],
    snap2: Optional[BrokerSnapshot],
    *,
    expected_account_id: str,
    now_mono_ns: int,
    min_separation_ns: int = DEFAULT_MIN_SEPARATION_NS,
) -> DurableResolutionOutcome:
    """C03/C09-safe resolution of ordinary zero-fill cancellation via two independent snapshots."""
    ok_snaps, err_snaps = _validate_two_independent_snapshots(
        snap1, snap2, expected_account_id=expected_account_id, now_mono_ns=now_mono_ns, min_separation_ns=min_separation_ns
    )
    if not ok_snaps:
        return DurableResolutionOutcome(False, err_snaps)

    oid = str(record.get("order_id") or "").strip()
    sec = str(record.get("security_id") or "").strip()
    seg = str(record.get("segment") or record.get("exchange_segment") or "").strip().upper()
    prod = str(record.get("product_type") or "").strip().upper()
    parent_id = str(record.get("parent_order_id") or "").strip()
    group_id = str(record.get("forever_order_id") or record.get("algo_id") or "").strip()
    if not all((oid, sec, seg, prod)):
        return DurableResolutionOutcome(False, "MISSING_RECORD_IDENTITY")

    ok1, r1 = _verify_order_dead_in_snapshot(
        snap1, order_id=oid, expected_account_id=expected_account_id,
        expected_security_id=sec, expected_segment=seg, expected_product=prod,
        expected_parent_id=parent_id, expected_group_id=group_id,
    )
    if not ok1:
        return DurableResolutionOutcome(False, r1, remaining_ids=(oid,))

    ok2, r2 = _verify_order_dead_in_snapshot(
        snap2, order_id=oid, expected_account_id=expected_account_id,
        expected_security_id=sec, expected_segment=seg, expected_product=prod,
        expected_parent_id=parent_id, expected_group_id=group_id,
    )
    if not ok2:
        return DurableResolutionOutcome(False, r2, remaining_ids=(oid,))

    if r1 != r2:
        return DurableResolutionOutcome(False, f"CONFLICTING_SNAPSHOT_STATUS:{r1}:{r2}", remaining_ids=(oid,))

    return DurableResolutionOutcome(
        True,
        f"ZERO_FILL_TERMINAL_VERIFIED:{r1}",
        resolved_ids=(oid,),
        remaining_ids=(),
        snapshot_ids=(snap1.snapshot_id, snap2.snapshot_id),
    )


def resolve_durable_orphan_legs(
    record: Dict[str, Any],
    snap1: Optional[BrokerSnapshot],
    snap2: Optional[BrokerSnapshot],
    *,
    expected_account_id: str,
    now_mono_ns: int,
    min_separation_ns: int = DEFAULT_MIN_SEPARATION_NS,
) -> DurableResolutionOutcome:
    """C02/C09-safe resolution of tracked orphan protection legs via two independent snapshots."""
    ok_snaps, err_snaps = _validate_two_independent_snapshots(
        snap1, snap2, expected_account_id=expected_account_id, now_mono_ns=now_mono_ns, min_separation_ns=min_separation_ns
    )
    if not ok_snaps:
        return DurableResolutionOutcome(False, err_snaps)

    details = record.get("orphaned_leg_cancel_details")
    if not isinstance(details, dict) or not details:
        return DurableResolutionOutcome(False, "NO_TRACKED_ORPHAN_LEGS")

    sec = str(record.get("security_id") or "").strip()
    seg = str(record.get("segment") or record.get("exchange_segment") or "").strip().upper()
    prod = str(record.get("product_type") or "").strip().upper()
    parent_id = str(record.get("order_id") or record.get("parent_order_id") or "").strip()
    if not all((sec, seg, prod, parent_id)):
        return DurableResolutionOutcome(False, "MISSING_RECORD_IDENTITY")

    resolved_ids: List[str] = []
    remaining_ids: List[str] = []
    first_err = ""

    for leg_id_raw, leg_meta in details.items():
        leg_id = str(leg_id_raw or "").strip()
        if not leg_id:
            remaining_ids.append(str(leg_id_raw))
            first_err = first_err or "EMPTY_ORPHAN_LEG_ID"
            continue
        group_id = str((leg_meta or {}).get("group_id") or (leg_meta or {}).get("algo_id") or "").strip() if isinstance(leg_meta, Mapping) else ""
        ok1, r1 = _verify_order_dead_in_snapshot(
            snap1, order_id=leg_id, expected_account_id=expected_account_id,
            expected_security_id=sec, expected_segment=seg, expected_product=prod,
            expected_parent_id=parent_id, expected_group_id=group_id, require_linkage=True,
        )
        ok2, r2 = _verify_order_dead_in_snapshot(
            snap2, order_id=leg_id, expected_account_id=expected_account_id,
            expected_security_id=sec, expected_segment=seg, expected_product=prod,
            expected_parent_id=parent_id, expected_group_id=group_id, require_linkage=True,
        )
        if ok1 and ok2 and r1 == r2:
            resolved_ids.append(leg_id)
        else:
            remaining_ids.append(leg_id)
            first_err = first_err or (r1 if not ok1 else (r2 if not ok2 else f"CONFLICTING_SNAPSHOT_STATUS:{r1}:{r2}"))

    if remaining_ids:
        return DurableResolutionOutcome(
            False,
            first_err or "ORPHAN_LEG_UNRESOLVED",
            resolved_ids=tuple(resolved_ids),
            remaining_ids=tuple(remaining_ids),
            snapshot_ids=(snap1.snapshot_id, snap2.snapshot_id),
        )

    return DurableResolutionOutcome(
        True,
        "ORPHAN_LEGS_TERMINAL_VERIFIED",
        resolved_ids=tuple(resolved_ids),
        remaining_ids=(),
        snapshot_ids=(snap1.snapshot_id, snap2.snapshot_id),
    )
