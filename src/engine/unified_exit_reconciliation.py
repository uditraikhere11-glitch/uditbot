"""Fail-closed, journal-first allocation of broker-verified long BUY/SELL fills.

Canonical reconciliation pipeline:
  Authenticated broker evidence -> normalized executions -> durable idempotent
  fill journal -> fill-level chronological BUY/SELL allocation -> canonical
  accounting (`fills` + `exit_allocations`) -> lifecycle/watcher synchronization
  -> schema-aware protective-order verification -> independent broker readback
  -> reconciliation gate decision.

Remediates UREC-001..UREC-008 and P0-1..P0-4, P1-5..P1-6:
- P0-1: Durable idempotent crash/restart replay of committed `exit_allocations`
  (including partial watcher sync where one parent synced and another stayed stale).
- P0-2: Fill-level BUY/SELL execution chronology using actual verified execution
  timestamps (`exchangeTime` / `fills.traded_at`), never order submission time.
- P0-3: Explicit distinction between verified zero holding and absent holdings row;
  never defaults a missing holdings row to 0 when CNC `netQty < 0`.
- P0-4: Direct order-ID execution attribution for Forever OCO / Super Order `TRADED`
  legs; unrelated manual SELLs on the same security cannot corroborate OCO fills.
- P1-5: Schema-aware OCO classification separating `orderFlag` (`OCO`/`SINGLE`)
  from `orderType` (`LIMIT`/`MARKET`); requires confirmed terminal sibling legs.
- P1-6: Canonical per-parent accounting consistency across `get_position_accounting()`
  and `get_sell_fills_for_parent()`.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import math
import re
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from src.engine.holding_quantity import aggregate_holdings, observed_open_quantity

IST = ZoneInfo("Asia/Kolkata")
_INT = re.compile(r"\+?\d+\Z")
_CLOSED = frozenset(("CANCELLED", "CANCELED", "REJECTED", "EXPIRED"))
_FINISHED = _CLOSED | {"TRADED", "CLOSED"}
_MAX_HISTORICAL_SCAN_DAYS = 366


class ReconciliationEvidenceError(ValueError):
    """A missing, conflicting, or ambiguous broker fact must keep the gate BLOCKED."""


def _identity(row: dict, *, journal: bool = False) -> Tuple[str, str, str]:
    keys = (("security_id", "exchange_segment", "product_type") if journal else
            ("securityId", "exchangeSegment", "productType"))
    value = tuple(str(row.get(key) or "").strip().upper() for key in keys)
    if not all(value):
        raise ReconciliationEvidenceError("Incomplete security/segment/product identity")
    return value


def _integer(value: Any, *, positive: bool = False) -> int:
    if type(value) is int:
        result = value
    elif isinstance(value, str) and _INT.fullmatch(value.strip()):
        result = int(value.strip())
    else:
        raise ReconciliationEvidenceError("Missing or nonintegral executed quantity")
    if result < (1 if positive else 0):
        raise ReconciliationEvidenceError("Nonpositive or negative executed quantity")
    return result


def _price(value: Any) -> float:
    if isinstance(value, bool):
        raise ReconciliationEvidenceError("Invalid executed price")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ReconciliationEvidenceError("Missing executed price") from None
    if not math.isfinite(result) or result <= 0:
        raise ReconciliationEvidenceError("Invalid executed price")
    return result


def _exchange_time(value: Any) -> float:
    if not isinstance(value, str) or not value.strip():
        raise ReconciliationEvidenceError("Broker exchange execution time missing")
    try:
        d = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=IST)
        return d.timestamp()
    except (ValueError, OverflowError):
        raise ReconciliationEvidenceError("Broker exchange execution time malformed") from None


def _broker_trade(row: dict, account: str, contract: tuple, side: str) -> dict:
    if not isinstance(row, dict) or _identity(row) != contract:
        raise ReconciliationEvidenceError("Trade contract identity changed")
    if str(row.get("dhanClientId") or "").strip() != account:
        raise ReconciliationEvidenceError("Trade broker account unverified")
    if str(row.get("transactionType") or "").strip().upper() != side:
        raise ReconciliationEvidenceError("Trade BUY/SELL side mismatch")
    oid = str(row.get("orderId") or "").strip()
    eid = str(row.get("exchangeTradeId") or "").strip()
    if not oid or not eid or eid in ("0", "None", "null"):
        raise ReconciliationEvidenceError("Actual exchange trade/order ID missing")
    return {
        "oid": oid,
        "eid": eid,
        "qty": _integer(row.get("tradedQuantity"), positive=True),
        "price": _price(row.get("tradedPrice")),
        "when": _exchange_time(row.get("exchangeTime")),
    }


def _read(read_required: Callable, method: str, label: str) -> List[dict]:
    ok, rows, reason = read_required(method, label)
    if not ok or not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
        raise ReconciliationEvidenceError(f"Complete {label} unavailable: {reason}")
    return rows


def _load_committed_allocations_for_parent(conn: Any, parent_order_id: str) -> List[dict]:
    rows = conn.execute(
        """
        SELECT a.parent_order_id, a.sell_order_id AS broker_order_id,
               a.exchange_trade_id, a.allocated_qty AS traded_quantity,
               a.traded_price, a.traded_at, f.symbol, f.security_id,
               f.exchange_segment, f.product_type
        FROM exit_allocations a
        JOIN fills f ON f.exchange_trade_id = a.exchange_trade_id
        WHERE a.parent_order_id = ?
          AND f.is_verified_trade = 1
          AND UPPER(f.transaction_type) = 'SELL'
        ORDER BY a.traded_at ASC, a.id ASC
        """,
        (parent_order_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _query_historical_sells(
    broker: Any,
    account: str,
    contract: tuple,
    created_at: float,
    *,
    required_eids: Optional[Set[str]] = None,
    today_date: Optional[date] = None,
    parents: Set[str],
    known_sell_ids=(),
    allowed_closed_buys=None,
    closed_order_rollups=None,
) -> Dict[str, dict]:
    """Paginate bounded Dhan historical-trade windows from BUY date through today."""
    from src.broker.dhan_client import MAX_HISTORICAL_TRADE_WINDOW_DAYS

    get_history = getattr(broker, "get_historical_trades", None)
    if not callable(get_history):
        raise ReconciliationEvidenceError("Historical broker executions unavailable")

    first = datetime.fromtimestamp(created_at, IST).date()
    last = today_date if today_date is not None else datetime.now(IST).date()
    if first > last:
        raise ReconciliationEvidenceError("BUY order date lies in the future")
    if (last - first).days > _MAX_HISTORICAL_SCAN_DAYS:
        raise ReconciliationEvidenceError(
            f"Historical execution span ({(last - first).days}d) exceeds bounded {_MAX_HISTORICAL_SCAN_DAYS}d scan limit"
        )

    span = max(1, int(MAX_HISTORICAL_TRADE_WINDOW_DAYS))
    from src.engine.signed_delivery_live_evidence import ClosedHistoricalBuyMatcher
    matcher = ClosedHistoricalBuyMatcher(
        {k: ("BUY", *v) for k, v in (allowed_closed_buys or {}).items()},
        closed_order_rollups or {}, account=account, contract=contract, today=last,
    )
    verified: Dict[str, dict] = {}
    current = first
    while current <= last:
        end = min(current + timedelta(days=span - 1), last)
        rows = get_history(current.isoformat(), end.isoformat())
        if not isinstance(rows, list) or getattr(rows, "ok", True) is not True:
            raise ReconciliationEvidenceError("Historical trade window incomplete")
        for row in rows:
            if not isinstance(row, dict):
                raise ReconciliationEvidenceError("Malformed historical broker execution")
            row_sec = str(row.get("securityId") or "").strip().upper()
            if row_sec != contract[0]:
                continue
            eid = str(row.get("exchangeTradeId") or "").strip()
            if _identity(row) != contract:
                if (str(row.get("orderId") or ""), "BUY") in matcher.rollups:
                    raise ReconciliationEvidenceError("Closed historical BUY contract mismatch")
                continue
            if row.get("dhanClientId") != account:
                raise ReconciliationEvidenceError("Historical execution account differs")
            side = str(row.get("transactionType") or "").strip().upper()
            if side not in {"BUY", "SELL"}:
                raise ReconciliationEvidenceError("Historical execution side unverified")
            if side == "BUY" and str(row.get("orderId")) not in parents:
                try:
                    matched = matcher.match(row)
                except ValueError as exc:
                    raise ReconciliationEvidenceError(str(exc)) from exc
                if matched is not None:
                    continue
                if _exchange_time(row.get("exchangeTime")) >= created_at:
                    # A *fully closed, independently audited* sibling OCO
                    # BUY is not a competing candidate for the open 1+1 lots.
                    # Confirm the exact historical execution, never just its
                    # order ID; an unknown/unmatched BUY still blocks.
                    expected = (allowed_closed_buys or {}).get(
                        (str(row.get("orderId") or ""), eid)
                    )
                    if expected is None:
                        raise ReconciliationEvidenceError("Competing historical BUY requires allocation review")
                    try:
                        qty = _integer(row.get("tradedQuantity"), positive=True)
                        price = _price(row.get("tradedPrice"))
                        when = _exchange_time(row.get("exchangeTime"))
                        if (qty != expected[0] or abs(price - expected[1]) >= 1e-6
                                or datetime.fromtimestamp(when, IST).date()
                                != datetime.fromtimestamp(expected[2], IST).date()):
                            raise ReconciliationEvidenceError("Closed sibling historical BUY conflicts with journal")
                    except (TypeError, ValueError) as exc:
                        raise ReconciliationEvidenceError("Closed sibling historical BUY proof invalid") from exc
                    continue
            if required_eids is not None:
                if eid not in required_eids:
                    if side == "SELL" and _exchange_time(row.get("exchangeTime")) >= created_at and eid not in known_sell_ids:
                        raise ReconciliationEvidenceError("Additional historical SELL requires allocation review")
                    continue
                normalized = _broker_trade(row, account, contract, "SELL")
            else:
                if side != "SELL":
                    continue
                normalized = _broker_trade(row, account, contract, "SELL")
                if normalized["when"] < created_at:
                    continue
            if eid in verified and verified[eid] != normalized:
                raise ReconciliationEvidenceError("Contradictory historical execution")
            verified[eid] = normalized
        current = end + timedelta(days=1)

    if required_eids is not None and set(verified) != set(required_eids):
        raise ReconciliationEvidenceError("Not all archived SELL fills independently confirmed")
    return verified


def _classify_forever_group_is_oco(all_grp: List[dict]) -> bool:
    """P1-5: Schema-aware OCO classification separating `orderFlag` from `orderType`.

    Never lets `orderType='LIMIT'` mask `orderFlag='OCO'`. Fails closed if
    `orderFlag` and `orderType` contradict each other (e.g. SINGLE vs OCO).
    """
    flags = {str(r.get("orderFlag") or "").strip().upper() for r in all_grp if str(r.get("orderFlag") or "").strip()}
    types = {str(r.get("orderType") or "").strip().upper() for r in all_grp if str(r.get("orderType") or "").strip()}
    leg_names = {str(r.get("legName") or "").strip().upper() for r in all_grp if str(r.get("legName") or "").strip()}

    has_oco_flag = "OCO" in flags
    has_single_flag = "SINGLE" in flags
    has_oco_type = "OCO" in types
    has_single_type = "SINGLE" in types

    if (has_oco_flag and has_single_flag) or (has_oco_flag and has_single_type) or (has_single_flag and has_oco_type):
        raise ReconciliationEvidenceError("Conflicting Forever OCO/SINGLE classification in broker response")

    if not (has_single_flag or has_single_type or has_oco_flag or has_oco_type):
        raise ReconciliationEvidenceError("Unknown protective order classification")
    return bool(has_oco_flag or has_oco_type or len(all_grp) > 1 or "STOP_LOSS_LEG" in leg_names)


def _extract_leg_order_ids(row: dict) -> Set[str]:
    ids: Set[str] = set()
    for field in ("legOrderId", "childOrderId", "executedOrderId"):
        val = str(row.get(field) or "").strip()
        if val:
            ids.add(val)
    if not ids and row.get("orderId"):
        ids.add(str(row["orderId"]))
    return ids


def parse_verified_broker_trade(
    row: Any,
    *,
    expected_account_id: str,
    expected_security_id: str,
    expected_segment: str,
    expected_product_type: str,
    expected_side: Optional[str] = None,
) -> Dict[str, Any]:
    """C6: Single normalized execution-evidence parser shared by watcher and reconciler.

    Enforces exact account (when expected_account_id is set), contract (securityId,
    exchangeSegment, productType), non-empty orderId and exchangeTradeId, side in
    ('BUY', 'SELL'), strict positive integer tradedQuantity (rejecting float/bool),
    strict positive finite tradedPrice, and valid exchange timestamp when present.
    """
    if not isinstance(row, dict):
        raise ReconciliationEvidenceError("Malformed non-dict broker trade row")
    acct = str(row.get("dhanClientId") or row.get("clientId") or "").strip()
    exp_acct = str(expected_account_id or "").strip()
    if exp_acct and acct != exp_acct:
        raise ReconciliationEvidenceError("Broker trade account missing or mismatch")

    sec = str(row.get("securityId") or "").strip()
    seg = str(row.get("exchangeSegment") or "").strip().upper()
    prod = str(row.get("productType") or "").strip().upper()
    if not sec or not seg or not prod:
        raise ReconciliationEvidenceError("Broker trade missing contract identity")
    if (
        sec.upper() != str(expected_security_id or "").strip().upper()
        or seg != str(expected_segment or "").strip().upper()
        or prod != str(expected_product_type or "").strip().upper()
    ):
        raise ReconciliationEvidenceError("Broker trade contract mismatch")

    side = str(row.get("transactionType") or "").strip().upper()
    if side not in ("BUY", "SELL"):
        raise ReconciliationEvidenceError("Broker trade invalid transactionType")
    if expected_side and side != str(expected_side).strip().upper():
        raise ReconciliationEvidenceError("Broker trade side mismatch")

    oid = str(row.get("orderId") or "").strip()
    if not oid:
        raise ReconciliationEvidenceError("Broker trade missing orderId")

    etid = str(row.get("exchangeTradeId") or "").strip()
    if not etid or etid.upper() in ("0", "NONE", "NULL"):
        raise ReconciliationEvidenceError("Broker trade missing exchangeTradeId")

    qty = _integer(row.get("tradedQuantity"), positive=True)
    price = _price(row.get("tradedPrice"))

    when_ts: Optional[float] = None
    raw_time = next(
        (row.get(k) for k in ("exchangeTime", "updateTime", "createTime") if str(row.get(k) or "").strip()),
        None,
    )
    if raw_time is not None:
        when_ts = _exchange_time(raw_time)

    return {
        "oid": oid,
        "eid": etid,
        "side": side,
        "qty": qty,
        "price": price,
        "when": when_ts,
        "account": acct,
        "security_id": sec,
        "segment": seg,
        "product": prod,
    }


def validate_forever_oco_child_orders(
    *,
    forever_order_id: str,
    expected_leg_qty: int,
    key: tuple,
    account: str,
    orders: Optional[List[dict]],
    sell_fill_qty_by_oid: Dict[str, int],
) -> Set[str]:
    """Shared fail-closed validator for Dhan Forever OCO spawned child SELL orders.

    Used identically by `unified_exit_reconciliation._verify_protective_orders_safe`
    and `protection_watcher.ProtectionWatcher.supervise_open_positions`.

    Enforces:
    - Exact `algoId == forever_order_id`, `transactionType == 'SELL'`, contract `(sec, seg, prod)`,
      and `dhanClientId == account` (when present).
    - No non-terminal child orders for `forever_order_id`.
    - Every `TRADED` child order has positive `quantity`, positive `filledQty == quantity`,
      and `remainingQuantity == 0` (rejecting contradictory broker rows).
    - Every `TRADED` child order individually matches its own verified SELL fill quantity
      in `sell_fill_qty_by_oid` (rejecting extra uncorroborated `TRADED` child orders).
    - Non-`TRADED` terminal children (`CANCELLED`/`REJECTED`/`EXPIRED`) have zero filledQty
      and zero attributed SELL fills.
    - Total requested quantity, total `filledQty`, and total verified SELL fills across all
      `TRADED` children each equal `expected_leg_qty`.
    """
    gid = str(forever_order_id or "").strip()
    if not gid or not isinstance(orders, list):
        raise ReconciliationEvidenceError(
            f"Forever TRIGGERED protective leg ({gid}) lacks broker orderbook evidence"
        )
    leg_qty = _integer(expected_leg_qty, positive=True)

    seen_child_oids: Set[str] = set()
    traded_children: List[Tuple[str, int, int]] = []  # (child_oid, req_qty, filled_qty)

    for ob in orders:
        if not isinstance(ob, dict):
            continue
        if str(ob.get("algoId") or "").strip() != gid:
            continue
        child_oid = str(ob.get("orderId") or "").strip()
        if not child_oid:
            raise ReconciliationEvidenceError("Forever child order missing orderId")
        if child_oid in seen_child_oids:
            raise ReconciliationEvidenceError(f"Duplicate Forever child order {child_oid}")
        seen_child_oids.add(child_oid)

        if str(ob.get("securityId") or "").strip().upper() != key[0]:
            raise ReconciliationEvidenceError("Forever child order security mismatch")
        if not ob.get("exchangeSegment") or not ob.get("productType") or _identity(ob) != key:
            raise ReconciliationEvidenceError("Forever child order contract mismatch")
        ob_acct = str(ob.get("dhanClientId") or "").strip()
        if account and ob_acct != account:
            raise ReconciliationEvidenceError("Forever child order account missing or mismatch")
        if str(ob.get("transactionType") or "").strip().upper() != "SELL":
            raise ReconciliationEvidenceError("Forever child order side mismatch")

        ob_st = str(ob.get("orderStatus") or "").strip().upper()
        if ob_st not in _FINISHED:
            raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")

        if ob_st == "TRADED":
            ob_req = _integer(ob.get("quantity"), positive=True)
            ob_filled = _integer(ob.get("filledQty"), positive=True)
            ob_rem = _integer(ob.get("remainingQuantity"), positive=False)
            if ob_rem != 0:
                raise ReconciliationEvidenceError(
                    f"Forever child order {child_oid} reports remainingQuantity={ob_rem} despite TRADED status"
                )
            if ob_filled != ob_req:
                raise ReconciliationEvidenceError(
                    f"Forever child order {child_oid} filledQty ({ob_filled}) != requested quantity ({ob_req})"
                )
            child_fill_qty = int(sell_fill_qty_by_oid.get(child_oid, 0))
            if child_fill_qty != ob_filled:
                raise ReconciliationEvidenceError(
                    f"Forever child order {child_oid} TRADED filledQty ({ob_filled}) != verified SELL fills ({child_fill_qty})"
                )
            traded_children.append((child_oid, ob_req, ob_filled))
        else:
            raw_filled = ob.get("filledQty")
            if raw_filled is None and ob_st == "REJECTED":
                non_traded_filled = 0
            else:
                non_traded_filled = _integer(raw_filled, positive=False)
            if non_traded_filled != 0 or int(sell_fill_qty_by_oid.get(child_oid, 0)) != 0:
                raise ReconciliationEvidenceError(
                    f"Non-TRADED Forever child order {child_oid} ({ob_st}) contradicts observed execution"
                )

    if not traded_children:
        raise ReconciliationEvidenceError(
            f"Forever TRIGGERED protective leg ({gid}) has no TRADED algoId-linked child SELL order"
        )

    total_req = sum(req for _, req, _ in traded_children)
    total_filled = sum(filled for _, _, filled in traded_children)
    if total_req != leg_qty or total_filled != leg_qty:
        raise ReconciliationEvidenceError(
            f"Forever child order quantity mismatch for {gid}: leg_qty={leg_qty}, req={total_req}, filled={total_filled}"
        )
    return {cid for cid, _, _ in traded_children}


def _verify_protective_orders_safe(
    *,
    forever: List[dict],
    super_orders: List[dict],
    key: tuple,
    account: str,
    broker_evidence: Dict[str, dict],
    orders: Optional[List[dict]] = None,
) -> Set[str]:
    """UREC-005, P0-4 & P1-5: Schema-aware, order-ID-bound verification of protective orders.

    - `_CLOSED` (`CANCELLED`, `CANCELED`, `REJECTED`, `EXPIRED`) is terminal/safe.
    - `TRIGGERED` alone is NEVER proof of completed execution -> BLOCK.
    - Forever OCO (`orderFlag=='OCO'` even when `orderType=='LIMIT'`!) requires:
      1. Account match if `dhanClientId` is present.
      2. Distinct sibling leg(s) present and ALL sibling legs in `_CLOSED`.
      3. Direct order-ID corroboration: actual verified SELL fill(s) in `broker_evidence`
         whose `oid` matches the Forever order/leg ID (NEVER an unrelated manual SELL)
         and whose quantity matches the traded leg quantity.
    - Super Order `CLOSED`/`TRADED` requires terminal `legDetails` and direct order-ID
      corroboration against verified SELL fills.
    """
    forever_groups: Dict[str, List[dict]] = {}
    for row in forever:
        if str(row.get("securityId") or "").strip().upper() != key[0]:
            continue
        if not row.get("exchangeSegment") or not row.get("productType"):
            raise ReconciliationEvidenceError("Unattributed protective order on sold security")
        if _identity(row) != key:
            continue
        row_acct = str(row.get("dhanClientId") or "").strip()
        if row_acct and row_acct != account:
            raise ReconciliationEvidenceError("Protective order account mismatch")
        st = str(row.get("orderStatus") or "").upper()
        if st in _CLOSED:
            continue
        gid = str(row.get("orderId") or "").strip()
        if not gid:
            raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
        forever_groups.setdefault(gid, []).append(row)

    for gid, _grp in forever_groups.items():
        all_grp = [
            r for r in forever
            if str(r.get("orderId") or "").strip() == gid
            and str(r.get("securityId") or "").strip().upper() == key[0]
        ]
        traded_legs = [r for r in all_grp if str(r.get("orderStatus") or "").upper() == "TRADED"]
        triggered_legs = [r for r in all_grp if str(r.get("orderStatus") or "").upper() == "TRIGGERED"]
        non_closed_non_traded = [
            r for r in all_grp
            if str(r.get("orderStatus") or "").upper() not in (_CLOSED | {"TRADED"})
        ]
        is_oco = _classify_forever_group_is_oco(all_grp)

        # Live Dhan Forever OCO execution schema: /v2/forever/all keeps the triggered leg as
        # orderStatus="TRIGGERED" (often omitting the cancelled sibling leg), while /v2/orders
        # spawns a child SELL order linked via algoId == gid with orderStatus="TRADED".
        if non_closed_non_traded:
            non_closed_non_triggered = [
                r for r in all_grp
                if str(r.get("orderStatus") or "").upper() not in (_CLOSED | {"TRIGGERED"})
            ]
            if (
                not isinstance(orders, list)
                or non_closed_non_triggered
                or len(triggered_legs) != 1
                or traded_legs
            ):
                raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
            trig_leg = triggered_legs[0]
            if is_oco:
                trig_leg_name = str(trig_leg.get("legName") or "").strip().upper()
                if trig_leg_name not in ("TARGET_LEG", "STOP_LOSS_LEG") or {_identity(r) for r in all_grp} != {key}:
                    raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
                seen_trig_legs: Set[str] = set()
                for r in all_grp:
                    ln = str(r.get("legName") or "").strip().upper()
                    if ln:
                        if ln in seen_trig_legs:
                            raise ReconciliationEvidenceError("Duplicate or conflicting Forever OCO leg identity")
                        seen_trig_legs.add(ln)
            leg_qty = _integer(trig_leg.get("quantity"), positive=True)
            sell_fill_qty_by_oid: Dict[str, int] = {}
            for v in broker_evidence.values():
                if v.get("side") == "SELL" and v.get("oid"):
                    v_oid = str(v["oid"]).strip()
                    sell_fill_qty_by_oid[v_oid] = sell_fill_qty_by_oid.get(v_oid, 0) + int(v["qty"])
            validate_forever_oco_child_orders(
                forever_order_id=gid,
                expected_leg_qty=leg_qty,
                key=key,
                account=account,
                orders=orders,
                sell_fill_qty_by_oid=sell_fill_qty_by_oid,
            )
            continue

        if not traded_legs or len(traded_legs) > 1:
            raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")

        if is_oco:
            if (len(all_grp) != 2 or {_identity(r) for r in all_grp} != {key}
                    or {r.get("legName") for r in all_grp} != {"TARGET_LEG", "STOP_LOSS_LEG"}):
                raise ReconciliationEvidenceError(
                    "Forever OCO TRADED leg missing sibling-leg terminal confirmation"
                )
            seen_legs: Set[str] = set()
            for r in all_grp:
                ln = str(r.get("legName") or "").strip().upper()
                if ln:
                    if ln in seen_legs:
                        raise ReconciliationEvidenceError("Duplicate or conflicting Forever OCO leg identity")
                    seen_legs.add(ln)

        traded_leg = traded_legs[0]
        leg_qty = _integer(traded_leg.get("quantity"), positive=True)
        has_algo_children = isinstance(orders, list) and any(
            isinstance(ob, dict) and str(ob.get("algoId") or "").strip() == gid
            for ob in orders
        )
        if has_algo_children:
            sell_fill_qty_by_oid_t: Dict[str, int] = {}
            for v in broker_evidence.values():
                if v.get("side") == "SELL" and v.get("oid"):
                    v_oid = str(v["oid"]).strip()
                    sell_fill_qty_by_oid_t[v_oid] = sell_fill_qty_by_oid_t.get(v_oid, 0) + int(v["qty"])
            validate_forever_oco_child_orders(
                forever_order_id=gid,
                expected_leg_qty=leg_qty,
                key=key,
                account=account,
                orders=orders,
                sell_fill_qty_by_oid=sell_fill_qty_by_oid_t,
            )
            continue
        allowed_oids = _extract_leg_order_ids(traded_leg)
        matched_sell_qty = sum(
            v["qty"]
            for v in broker_evidence.values()
            if v.get("side") == "SELL" and v.get("oid") in allowed_oids
        )
        if matched_sell_qty != leg_qty:
            raise ReconciliationEvidenceError(
                f"Forever TRADED protective leg ({gid}) is not corroborated by verified SELL fills for its own order ID"
            )

    shared_super_exits = set()
    for row in super_orders:
        if str(row.get("securityId") or "").strip().upper() != key[0]:
            continue
        if not row.get("exchangeSegment") or not row.get("productType"):
            raise ReconciliationEvidenceError("Unattributed protective order on sold security")
        if _identity(row) != key:
            continue
        row_acct = str(row.get("dhanClientId") or "").strip()
        if row_acct and row_acct != account:
            raise ReconciliationEvidenceError("Super Order account mismatch")
        st = str(row.get("orderStatus") or "").upper()
        if st in _CLOSED:
            continue
        if st not in ("TRADED", "CLOSED"):
            raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
        legs = row.get("legDetails")
        if not isinstance(legs, list) or not legs:
            raise ReconciliationEvidenceError(
                "Super Order CLOSED/TRADED record missing legDetails confirmation"
            )
        exit_legs = [
            lg for lg in legs
            if isinstance(lg, dict) and str(lg.get("legName") or "").upper() in ("TARGET_LEG", "STOP_LOSS_LEG")
        ]
        if len(exit_legs) != 2 or {lg.get("legName") for lg in exit_legs} != {"TARGET_LEG", "STOP_LOSS_LEG"}:
            raise ReconciliationEvidenceError(
                "Super Order CLOSED/TRADED record missing protective leg details"
            )
        traded_exit_legs = []
        for lg in exit_legs:
            lg_st = str(lg.get("orderStatus") or "").upper()
            if lg_st == "TRADED":
                traded_exit_legs.append(lg)
            elif lg_st not in _CLOSED:
                raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
        if len(traded_exit_legs) != 1:
            raise ReconciliationEvidenceError("Outstanding or ambiguous protective order requires verification")
        super_oids = _extract_leg_order_ids(traded_exit_legs[0])
        parent_oid = str(row.get("orderId") or "").strip()
        if not super_oids and parent_oid:
            super_oids = {parent_oid}
        matched_super_sell = sum(
            v["qty"]
            for v in broker_evidence.values()
            if v.get("side") == "SELL" and v.get("oid") in super_oids
        )
        if matched_super_sell != _integer(traded_exit_legs[0].get("quantity"), positive=True):
            raise ReconciliationEvidenceError(
                "Super Order TRADED protective leg is not corroborated by verified SELL fills for its own order ID"
            )
        if parent_oid in super_oids:
            shared_super_exits.add(parent_oid)
    return shared_super_exits


def _signed_cnc_delivery_sell_reconciles(
    *,
    positions: List[dict],
    holdings: List[dict],
    key: tuple,
    account: str,
    projected: int,
    broker_evidence: Dict[str, dict],
    session_date=None,
) -> bool:
    """P0-3 & UREC-007: Reconcile signed day `netQty < 0` on CNC/MTF delivery sales.

    Explicitly distinguishes a missing holdings row from verified zero:
    if `key[0]` is ABSENT from `holdings`, returns False (NEVER certifies zero
    exposure from `netQty < 0` + omitted holdings row).
    Requires an explicit holdings row for `key[0]` AND exact signed day trade
    corroboration (`today_buys - today_sells == net_day`) AND exact holdings
    agreement (`settled + net_day == projected`).
    """
    if key[2] not in ("CNC", "MTF"):
        return False
    from src.engine.holding_quantity import _quantity
    from src.engine.signed_cnc_delivery import validate_signed_position_components
    matches = []
    matching_position = None
    for row in positions:
        if not isinstance(row, dict):
            return False
        if str(row.get("securityId") or "").strip().upper() != key[0]:
            continue
        if _identity(row) != key:
            try:
                if _quantity(row.get("netQty"), signed=True) != 0:
                    return False
            except ValueError:
                return False
            continue
        if account and row.get("dhanClientId") and str(row["dhanClientId"]).strip() != account:
            return False
        raw_net = row.get("netQty")
        if isinstance(raw_net, bool) or raw_net is None:
            return False
        try:
            if not re.fullmatch(r"-?[0-9]+", str(raw_net)):
                return False
            matches.append(int(raw_net))
            matching_position = row
        except (TypeError, ValueError):
            return False
    if len(matches) != 1 or matches[0] >= 0:
        return False
    net_day = matches[0]
    effective_date = session_date if session_date is not None else datetime.now(IST).date()
    if any(datetime.fromtimestamp(v["when"], IST).date() != effective_date
           for v in broker_evidence.values() if v.get("from_today")):
        return False
    today_buys = sum(v["qty"] for v in broker_evidence.values() if v.get("side") == "BUY" and v.get("from_today"))
    today_sells = sum(v["qty"] for v in broker_evidence.values() if v.get("side") == "SELL" and v.get("from_today"))
    if today_buys - today_sells != net_day:
        return False
    try:
        validate_signed_position_components(matching_position, net_day, today_buys, today_sells)
        holdings_map = aggregate_holdings(holdings, account_id=account)
    except Exception:
        return False
    # P0-3: An absent holdings row is NEVER confirmed zero when netQty < 0.
    if key[0] not in holdings_map:
        return False
    settled = holdings_map[key[0]]
    return settled >= -net_day and settled + net_day == projected


def verified_flat_delivery_day(broker_position, positions, holdings, trades, db, account, read_required, *, session_date=None):
    """A negative day balance may represent delivery stock sold from a prior day."""
    key = _identity(broker_position)
    if key[1] not in ("NSE_EQ", "BSE_EQ"):
        return False
    if key[2] not in ("CNC", "MTF") or not account or db.account_id != account:
        return False
    if key[0] not in aggregate_holdings(holdings, account_id=account):
        return False
    evidence = {}
    parents = set()
    for row in trades:
        if str(row.get("securityId")) != key[0]:
            continue
        side = row.get("transactionType")
        if side not in ("BUY", "SELL"):
            return False
        fill = _broker_trade(row, account, key, side)
        if datetime.fromtimestamp(fill["when"], IST).date() != (session_date or datetime.now(IST).date()) or fill["eid"] in evidence:
            return False
        evidence[fill["eid"]] = dict(fill, side=side, from_today=True)
        if side == "SELL":
            with db._get_conn() as conn:
                record = conn.execute("SELECT parent_order_id, traded_quantity, traded_price FROM fills "
                                      "WHERE exchange_trade_id=? AND broker_order_id=? AND transaction_type='SELL' "
                                      "AND is_verified_trade=1 AND security_id=? AND exchange_segment=? AND product_type=?",
                                      (fill["eid"], fill["oid"], *key)).fetchone()
                if not record or record[1] != fill["qty"] or record[2] != fill["price"]:
                    return False
                if record[0] == "MULTI_LOT_ALLOCATED":
                    parts = conn.execute("SELECT parent_order_id, allocated_qty FROM exit_allocations "
                                         "WHERE exchange_trade_id=? AND sell_order_id=?",
                                         (fill["eid"], fill["oid"])).fetchall()
                    if sum(part[1] for part in parts) != fill["qty"]:
                        return False
                    parents.update(part[0] for part in parts)
                else:
                    parents.add(record[0])
        else:
            record = db.get_fill_by_exchange_trade_id(fill["eid"])
            if (not record or not record["is_verified_trade"] or record["transaction_type"] != "BUY"
                    or record["broker_order_id"] != fill["oid"] or _identity(record, journal=True) != key
                    or record["traded_quantity"] != fill["qty"] or record["traded_price"] != fill["price"]):
                return False
            parents.add(fill["oid"])
    open_qty = 0
    for row in db.get_open_positions_lifecycle():
        if _identity(row, journal=True) == key:
            if row["account_id"] != account:
                return False
            open_qty += _integer(row["confirmed_open_qty"])
    for parent in parents:
        lot = db.get_position_lifecycle(parent)
        if (not lot or lot["account_id"] != account or _identity(lot, journal=True) != key
                or db.get_position_accounting(parent)["confirmed_open_qty"] != lot["confirmed_open_qty"]):
            return False
    if not parents:
        return False
    orders = _read(read_required, "get_order_book", "order book")
    if any(str(row.get("securityId")) == key[0] and row.get("orderStatus") not in _FINISHED for row in orders):
        return False
    _verify_protective_orders_safe(
        forever=_read(read_required, "get_forever_orders", "Forever orders"),
        super_orders=_read(read_required, "get_super_orders", "Super orders"),
        key=key, account=account, broker_evidence=evidence, orders=orders)
    return _signed_cnc_delivery_sell_reconciles(
        positions=positions, holdings=holdings, key=key, account=account, projected=open_qty, broker_evidence=evidence,
        session_date=session_date)


def _verify_fill_level_chronology(
    *,
    lot_details: List[dict],
    exits_for_contract: List[dict],
    broker_evidence: Dict[str, dict],
) -> None:
    """P0-2: Enforce fill-level BUY/SELL execution chronology using actual fill timestamps.

    Walks all verified BUY fills and SELL fills in chronological order (at equal
    timestamps, SELL is checked before BUY so no simultaneous/ambiguous SELL can
    consume an unexecuted BUY). Rejects any SELL that precedes sufficient verified
    BUY fill quantity — regardless of earlier order submission (`created_at`) time.
    """
    events: List[Tuple[float, int, int, str]] = []  # (exec_ts, order_rank [0=SELL, 1=BUY], qty, eid)

    for d in lot_details:
        for b in d["buys"]:
            b_eid = str(b.get("exchange_trade_id") or "").strip()
            b_qty = _integer(b.get("traded_quantity"), positive=True)
            if b_eid and b_eid in broker_evidence:
                fill = broker_evidence[b_eid]
                if (fill["side"] != "BUY" or fill["oid"] != d["oid"] or fill["qty"] != b_qty
                        or fill["price"] != _price(b["traded_price"])):
                    raise ReconciliationEvidenceError("Broker BUY differs from verified journal fill")
                b_ts = float(broker_evidence[b_eid]["when"])
            else:
                raw_ts = b.get("traded_at")
                if raw_ts is None or isinstance(raw_ts, bool):
                    raise ReconciliationEvidenceError("Verified BUY fill execution timestamp missing")
                try:
                    b_ts = float(raw_ts)
                except (TypeError, ValueError):
                    raise ReconciliationEvidenceError("Verified BUY fill execution timestamp malformed") from None
                if not math.isfinite(b_ts) or b_ts <= 0:
                    raise ReconciliationEvidenceError("Verified BUY fill execution timestamp invalid")
            events.append((b_ts, 1, b_qty, b_eid))

    for r in exits_for_contract:
        s_eid = str(r.get("exchange_trade_id") or "").strip()
        if s_eid not in broker_evidence:
            raise ReconciliationEvidenceError("Verified SELL fill execution timestamp missing")
        proof = broker_evidence[s_eid]
        s_ts = float(proof["when"])
        if not math.isfinite(s_ts) or s_ts <= 0:
            raise ReconciliationEvidenceError("Verified SELL fill execution timestamp invalid")
        events.append((s_ts, 0, int(proof["qty"]), s_eid))

    events.sort(key=lambda item: (item[0], item[1]))
    available_buy_qty = 0
    for _ts, rank, qty, _eid in events:
        if rank == 1:
            available_buy_qty += qty
        else:
            if available_buy_qty < qty:
                raise ReconciliationEvidenceError(
                    "SELL execution precedes or overlaps sufficient verified BUY fill quantity"
                )
            available_buy_qty -= qty


def _sync_parent_lot_to_watcher(
    *,
    db: Any,
    watcher: Any,
    d: dict,
    key: tuple,
    lot_proj: int,
    event_id: Optional[int],
) -> None:
    """Project canonical journal accounting to watcher and verify durable readback."""
    oid = d["oid"]
    buy_qty = d["buy_qty"]
    acct = db.get_position_accounting(oid)
    if (int(acct["buy_filled_qty"]) != buy_qty
            or int(acct["sell_filled_qty"]) != buy_qty - lot_proj
            or int(acct["confirmed_open_qty"]) != lot_proj):
        raise ReconciliationEvidenceError("Journal accounting readback disagrees after link")

    rec = watcher.reconcile_historical_order_fills(
        order_id=oid,
        correlation_id=str(d["intent"]["correlation_id"]),
        symbol=str(d["intent"]["symbol"]),
        security_id=key[0],
        segment=key[1],
        product_type=key[2],
        requested_qty=_integer(d["intent"]["quantity"], positive=True),
        entry_terminal_status=d.get("entry_terminal_status") or d["intent"].get("state"),
        filled_qty=buy_qty,
        avg_fill_price=float(acct["buy_vwap"]),
        verified_closed_qty=buy_qty - lot_proj,
        verified_sell_vwap=float(acct["sell_vwap"]),
        verified_realized_pnl=float(acct["realized_pnl"]),
        verified_protected_qty=0,
        is_amo=bool(d["intent"].get("is_amo")),
        protection_route=str(d["intent"].get("protection_route") or ""),
        original_market_price=float(d["intent"].get("original_market_price") or 0),
    )
    persisted = db.get_position_lifecycle(oid)
    if (not isinstance(rec, dict) or not isinstance(persisted, dict)
            or int(rec.get("confirmed_position_open_qty") or 0) != lot_proj
            or int(persisted.get("confirmed_open_qty") or 0) != lot_proj
            or (lot_proj == 0 and str(persisted.get("position_state")) != "POSITION_CLOSED")):
        raise ReconciliationEvidenceError("Watcher/lifecycle durable readback failed")
    if lot_proj == 0:
        with db._get_conn() as conn:
            conn.execute(
                "UPDATE protection_orders SET status='SUPERSEDED' "
                "WHERE parent_order_id=? AND status IN ('UNPROTECTED','UNVERIFIED')",
                (oid,),
            )
    if event_id is not None:
        with db._get_conn() as conn:
            conn.execute(
                "INSERT INTO system_events(event_type,description,payload,created_at) "
                "VALUES('UNIFIED_EXIT_WATCHER_SYNCED',?,?,?)",
                (str(event_id), oid, time.time()),
            )


def _check_watcher_safety(watcher: Any, lot_details: List[dict]) -> None:
    if not watcher or not hasattr(watcher, "reconcile_historical_order_fills"):
        raise ReconciliationEvidenceError("Watcher durable projection interface unavailable")
    for d in lot_details:
        oid = d["oid"]
        for collection in (getattr(watcher, "pending", None), getattr(watcher, "terminal_orders", None)):
            if isinstance(collection, dict):
                prior_rec = collection.get(oid) or {}
                local_pending = prior_rec.get("pending_oco")
                if local_pending and (
                    not isinstance(local_pending, dict)
                    or str(local_pending.get("state") or "").upper() not in _CLOSED
                ):
                    raise ReconciliationEvidenceError("Watcher has unresolved protection attempt")
                if any(prior_rec.get(flag) for flag in (
                    "cancel_uncertain", "persistence_uncertain", "orphaned_legs_detected"
                )):
                    raise ReconciliationEvidenceError("Watcher has unresolved cancellation or persistence uncertainty")


def reconcile_verified_exits(
    *,
    broker: Any,
    db: Any,
    watcher: Any,
    trade_book: List[dict],
    positions: List[dict],
    read_required: Callable,
    initial_holdings=None,
) -> Tuple[bool, str]:
    """Link and project verified SELL executions, returning (changed, message)."""
    if not hasattr(db, "_get_conn") or not isinstance(trade_book, list) or not isinstance(positions, list):
        raise ReconciliationEvidenceError("Durable journal or complete broker books unavailable")
    account = str(getattr(getattr(broker, "cfg", None), "dhan_client_id", "") or "").strip()
    today_ist = datetime.now(IST).date()

    with db._get_conn() as conn:
        exits = [dict(r) for r in conn.execute(
            "SELECT * FROM fills WHERE UPPER(transaction_type)='SELL' AND is_verified_trade=1 "
            "AND (parent_order_id IS NULL OR parent_order_id='' OR parent_order_id=broker_order_id) "
            "ORDER BY id"
        ).fetchall()]
        events = [dict(r) for r in conn.execute(
            "SELECT id, payload FROM system_events WHERE event_type='UNIFIED_EXIT_LINKED' "
            "AND id NOT IN (SELECT CAST(description AS INTEGER) FROM system_events "
            "WHERE event_type='UNIFIED_EXIT_WATCHER_SYNCED' AND description GLOB '[0-9]*')"
        ).fetchall()]
        pending_ids = [str(json.loads(event["payload"])["parent_order_id"]) for event in events]
        placeholders = ",".join("?" for _ in pending_ids) or "NULL"
        live = [dict(r) for r in conn.execute(
            "SELECT * FROM positions_lifecycle WHERE buy_filled_qty>0 AND "
            f"(position_state!='POSITION_CLOSED' OR parent_order_id IN ({placeholders}))", pending_ids
        ).fetchall()]
        intent_dates: Dict[str, float] = {}
        for row in live:
            p_oid = str(row["parent_order_id"])
            i_row = conn.execute(
                "SELECT created_at FROM order_intents WHERE broker_order_id=? AND account_id=? LIMIT 1",
                (p_oid, account),
            ).fetchone()
            if i_row is not None and i_row["created_at"] is not None:
                intent_dates[p_oid] = float(i_row["created_at"])

    keys = {_identity(r, journal=True) for r in live}
    committed_shared_fills = set()
    pending_sync: Dict[str, Tuple[int, tuple]] = {}
    for ev in events:
        try:
            payload = json.loads(ev["payload"])
            oid = str(payload["parent_order_id"])
            key = tuple(payload["contract"])
        except (ValueError, TypeError, KeyError):
            raise ReconciliationEvidenceError("Durable exit-recovery event is malformed") from None
        if payload.get("account") != account or key not in keys or oid in pending_sync:
            raise ReconciliationEvidenceError("Pending recovery identity is ambiguous")
        pending_sync[oid] = (int(ev["id"]), key)
        committed_shared_fills.update((oid, eid) for eid in payload.get("sell_exchange_trade_ids", []))

    groups: Dict[tuple, list] = {}
    for r in exits:
        key = _identity(r, journal=True)
        if key in keys and r["broker_order_id"] == r["parent_order_id"]:
            if (r["parent_order_id"], r["exchange_trade_id"]) in committed_shared_fills:
                continue
            lot = db.get_position_lifecycle(r["parent_order_id"])
            if lot and lot["position_state"] == "POSITION_CLOSED" and lot["confirmed_open_qty"] == 0:
                continue
        if key in keys:
            groups.setdefault(key, []).append(r)

    # UREC-002 (contract-scoped): Bounded historical SELL discovery per contract
    discovered_by_key: Dict[tuple, Dict[str, dict]] = {}
    cached_holdings: Optional[List[dict]] = initial_holdings
    pending_sync_keys = {k for _, k in pending_sync.values()}
    prior_day_keys: Dict[tuple, float] = {}
    for row in live:
        if row["position_state"] == "UNPROTECTED_MANUAL_RISK":
            continue  # Account-bound owner-lot history is verified by the gate.
        p_oid = str(row["parent_order_id"])
        created_ts = intent_dates.get(p_oid, 0.0)
        if created_ts > 0 and datetime.fromtimestamp(created_ts, IST).date() < today_ist:
            k = _identity(row, journal=True)
            if k in groups or k in pending_sync_keys:
                continue
            if k not in prior_day_keys or created_ts < prior_day_keys[k]:
                prior_day_keys[k] = created_ts

    if prior_day_keys and callable(getattr(broker, "get_historical_trades", None)):
        if cached_holdings is None:
            cached_holdings = _read(read_required, "get_holdings", "holdings")
        signed_holdings_cache = {}
        for k, earliest_ts in sorted(prior_day_keys.items()):
            lots_for_k = [r for r in live if _identity(r, journal=True) == k]
            expected_open = sum(int(r.get("confirmed_open_qty") or 0) for r in lots_for_k)
            obs_qty, _obs_src = observed_open_quantity(
                positions, cached_holdings,
                security_id=k[0], segment=k[1], product=k[2], account_id=account,
            )
            if obs_qty is not None and obs_qty >= expected_open:
                continue
            with db._get_conn() as conn:
                known_eids = {
                    str(x["exchange_trade_id"])
                    for x in conn.execute(
                        "SELECT exchange_trade_id FROM fills WHERE security_id=? AND exchange_trade_id IS NOT NULL",
                        (k[0],),
                    ).fetchall()
                }
            closed_buy_allowlist = {}
            closed_rollups = {}
            if _obs_src == "NEGATIVE_CNC_POSITION_REVIEW":
                # Never use an unrelated historical BUY as FIFO evidence. The
                # existing closed-OCO verifier binds exact BUY/SELL executions
                # to an immutable closed parent, original intent and child leg.
                from src.engine.signed_cnc_delivery import verify_signed_cnc_delivery_quantity
                from src.engine.signed_delivery_live_evidence import attest_signed_holdings
                try:
                    holdings_account = attest_signed_holdings(
                        broker, cached_holdings, account, signed_holdings_cache,
                    )
                    verify_signed_cnc_delivery_quantity(
                        db=db, positions=positions, holdings=cached_holdings,
                        trade_book=trade_book, account=account,
                        security=k[0], segment=k[1], product=k[2],
                        expected_open_qty=expected_open,
                        now=time.time(), holdings_profile_account=holdings_account,
                    )
                except (ValueError, TypeError, KeyError, AttributeError, OSError) as exc:
                    raise ReconciliationEvidenceError(
                        "Signed delivery holdings or closed sibling proof unavailable"
                    ) from exc
                closed = db.get_closed_forever_sibling_evidence(
                    account, k[0], k[1], k[2],
                    open_parent_ids=tuple(str(r["parent_order_id"]) for r in lots_for_k),
                    now=time.time(),
                )
                closed_rollups = closed.get("order_rollups", {})
                closed_buy_allowlist = {
                    key: (value[1], value[2], value[3])
                    for key, value in closed["trades"].items()
                    if value[0] == "BUY"
                }
            hist_found = _query_historical_sells(
                broker, account, k, earliest_ts,
                required_eids=None, today_date=today_ist,
                parents={r["parent_order_id"] for r in lots_for_k},
                allowed_closed_buys=closed_buy_allowlist,
                closed_order_rollups=closed_rollups,
            )
            new_sells = {eid: item for eid, item in hist_found.items() if eid not in known_eids}
            if new_sells:
                discovered_by_key[k] = new_sells

    if not groups and not pending_sync and not discovered_by_key:
        return False, "No unlinked bot-owned SELL executions"

    if not account or str(getattr(db, "account_id", "") or "").strip() != account:
        raise ReconciliationEvidenceError("Authenticated broker/journal account mismatch")

    order_book = _read(read_required, "get_order_book", "order_book")
    holdings = cached_holdings if cached_holdings is not None else _read(read_required, "get_holdings", "holdings")
    forever = _read(read_required, "get_forever_orders", "forever_orders")
    super_orders = _read(read_required, "get_super_orders", "super_orders")

    target_keys = sorted(set(groups) | pending_sync_keys | set(discovered_by_key))
    linked_messages: List[str] = []

    for key in target_keys:
        matching_lots = [row for row in live if _identity(row, journal=True) == key]
        if not matching_lots:
            continue

        # P0-1: Check if this contract is a pure crash-recovery replay of ALREADY COMMITTED
        # fills/exit_allocations (zero new unlinked candidates, zero new discovered SELLs).
        has_new_candidates = bool(groups.get(key) or discovered_by_key.get(key))
        is_pure_replay = (not has_new_candidates) and (key in pending_sync_keys)

        if is_pure_replay:
            # A prior commit authorizes repairing local copies only. A newly live
            # exit order must still prevent replay from silently clearing a halt.
            proof = {}
            for row in trade_book:
                if str(row.get("securityId")) == key[0] and row.get("transactionType") == "SELL":
                    fill = _broker_trade(row, account, key, "SELL")
                    proof[fill["eid"]] = dict(fill, side="SELL")
            _verify_protective_orders_safe(forever=forever, super_orders=super_orders,
                                          key=key, account=account, broker_evidence=proof,
                                          orders=order_book)
            if any(str(row.get("securityId")) == key[0] and row.get("orderStatus") not in _FINISHED
                   for row in order_book):
                raise ReconciliationEvidenceError("Outstanding broker order during exit recovery")
            replay_lots = []
            for parent in matching_lots:
                oid = str(parent["parent_order_id"])
                if oid not in pending_sync:
                    continue
                if str(parent.get("account_id") or "") != account:
                    raise ReconciliationEvidenceError("BUY lifecycle account identity mismatch")
                with db._get_conn() as conn:
                    intents = [dict(r) for r in conn.execute(
                        "SELECT * FROM order_intents WHERE broker_order_id=? AND account_id=?", (oid, account)
                    ).fetchall()]
                    buys = [dict(r) for r in conn.execute(
                        "SELECT * FROM fills WHERE broker_order_id=? AND UPPER(transaction_type)='BUY' "
                        "AND is_verified_trade=1 ORDER BY id", (oid,)
                    ).fetchall()]
                if len(intents) != 1 or _identity(intents[0], journal=True) != key:
                    raise ReconciliationEvidenceError("Unique original BUY intent not attested")
                buy_qty = sum(_integer(b["traded_quantity"], positive=True) for b in buys)
                if buy_qty != _integer(parent["buy_filled_qty"], positive=True):
                    raise ReconciliationEvidenceError("BUY lifecycle and exchange fill quantity differ")
                acct = db.get_position_accounting(oid)
                target_open = int(acct["confirmed_open_qty"])
                replay_lots.append({
                    "parent": parent,
                    "oid": oid,
                    "intent": intents[0],
                    "buys": buys,
                    "buy_qty": buy_qty,
                    "target_open": target_open,
                })
            _check_watcher_safety(watcher, replay_lots)
            synced_oids: List[str] = []
            for d in replay_lots:
                oid = d["oid"]
                ev_id = pending_sync[oid][0] if oid in pending_sync else None
                if ev_id is not None or d["target_open"] != int(d["parent"].get("confirmed_open_qty") or 0):
                    _sync_parent_lot_to_watcher(
                        db=db, watcher=watcher, d=d, key=key, lot_proj=d["target_open"], event_id=ev_id,
                    )
                    synced_oids.append(oid)
            if synced_oids:
                linked_messages.append(
                    f"Replayed committed SELL allocation(s) to watcher for BUY {', '.join(synced_oids)}"
                )
            continue

        # Fast-check multi-lot partial ambiguity before per-lot intent attestation
        if len(matching_lots) > 1:
            prelim_open = sum(int(row.get("confirmed_open_qty") or 0) for row in matching_lots)
            prelim_cands = list(groups.get(key, []))
            prelim_disc = discovered_by_key.get(key, {})
            prelim_committed = 0
            with db._get_conn() as conn:
                for row in matching_lots:
                    p_oid = str(row["parent_order_id"])
                    prelim_committed += sum(
                        _integer(a["traded_quantity"], positive=True)
                        for a in _load_committed_allocations_for_parent(conn, p_oid)
                    )
            prelim_sell = (
                sum(_integer(r["traded_quantity"], positive=True) for r in prelim_cands)
                + sum(int(item["qty"]) for item in prelim_disc.values())
                + prelim_committed
            )
            if prelim_sell < prelim_open:
                raise ReconciliationEvidenceError(
                    f"Ambiguous bot-owned BUY parents for {key}: partial SELL cannot be allocated without FIFO guessing"
                )

        # Validate every open BUY lot for this contract
        lot_details = []
        total_buy_qty = 0
        earliest_started = float("inf")
        all_already_linked: List[dict] = []
        seen_already_eids: Set[str] = set()
        parent_oids: Set[str] = set()

        for parent in matching_lots:
            oid = str(parent["parent_order_id"])
            parent_oids.add(oid)
            if str(parent.get("account_id") or "") != account:
                raise ReconciliationEvidenceError("BUY lifecycle account identity mismatch")
            with db._get_conn() as conn:
                intents = [dict(r) for r in conn.execute(
                    "SELECT * FROM order_intents WHERE broker_order_id=? AND account_id=?", (oid, account)
                ).fetchall()]
                buys = [dict(r) for r in conn.execute(
                    "SELECT * FROM fills WHERE broker_order_id=? AND UPPER(transaction_type)='BUY' "
                    "AND is_verified_trade=1 ORDER BY id", (oid,)
                ).fetchall()]
                direct_linked = [dict(r) for r in conn.execute(
                    "SELECT * FROM fills WHERE parent_order_id=? AND UPPER(transaction_type)='SELL' "
                    "AND is_verified_trade=1 ORDER BY id", (oid,)
                ).fetchall()]
                alloc_linked = _load_committed_allocations_for_parent(conn, oid)
            if len(intents) != 1 or _identity(intents[0], journal=True) != key:
                raise ReconciliationEvidenceError("Unique original BUY intent not attested")
            if not buys or any(_identity(b, journal=True) != key for b in buys):
                raise ReconciliationEvidenceError("Original verified BUY executions incomplete")
            buy_qty = sum(_integer(b["traded_quantity"], positive=True) for b in buys)
            if buy_qty != _integer(parent["buy_filled_qty"], positive=True):
                raise ReconciliationEvidenceError("BUY lifecycle and exchange fill quantity differ")
            if buy_qty > _integer(intents[0]["quantity"], positive=True):
                raise ReconciliationEvidenceError("Executed BUY exceeds submitted order quantity")
            started = float(intents[0].get("created_at") or 0)
            if started <= 0:
                raise ReconciliationEvidenceError("BUY intent creation time missing")
            earliest_started = min(earliest_started, started)
            total_buy_qty += buy_qty

            lot_already = []
            for r in direct_linked + alloc_linked:
                eid = str(r.get("exchange_trade_id") or "")
                lot_already.append(r)
                if eid and eid not in seen_already_eids:
                    seen_already_eids.add(eid)
                    all_already_linked.append(r)

            lot_details.append({
                "parent": parent,
                "oid": oid,
                "intent": intents[0],
                "buys": buys,
                "buy_qty": buy_qty,
                "started": started,
                "already_linked": lot_already,
            })

        # Combine unlinked local SELL fills + newly discovered historical SELLs
        candidates = list(groups.get(key, []))
        discovered = discovered_by_key.get(key, {})
        discovered_rows = [
            {
                "id": None,
                "broker_order_id": item["oid"],
                "exchange_trade_id": eid,
                "symbol": str(lot_details[0]["intent"]["symbol"]),
                "traded_price": item["price"],
                "traded_quantity": item["qty"],
                "traded_at": item["when"],
                "transaction_type": "SELL",
                "security_id": key[0],
                "exchange_segment": key[1],
                "product_type": key[2],
                "parent_order_id": item["oid"],
                "is_verified_trade": 1,
                "_discovered": True,
            }
            for eid, item in sorted(discovered.items())
        ]
        all_candidates = candidates + discovered_rows
        candidate_ids = {r["id"] for r in candidates}
        exits_for_contract = all_candidates + [r for r in all_already_linked if r["id"] not in candidate_ids]
        if not exits_for_contract:
            continue

        exit_eids: Set[str] = set()
        for r in exits_for_contract:
            eid = str(r.get("exchange_trade_id") or "")
            if not eid or eid in exit_eids or not str(r.get("broker_order_id") or ""):
                raise ReconciliationEvidenceError("Duplicate or invalid verified SELL fill")
            exit_eids.add(eid)

        total_sell_qty = sum(_integer(r["traded_quantity"], positive=True) for r in exits_for_contract)
        projected_total = total_buy_qty - total_sell_qty
        if projected_total < 0:
            raise ReconciliationEvidenceError("SELL execution quantity exceeds original BUY")

        if len(matching_lots) > 1 and projected_total != 0:
            raise ReconciliationEvidenceError(
                f"Ambiguous bot-owned BUY parents for {key}: partial SELL cannot be allocated without FIFO guessing"
            )

        # Prior closed lots may reappear in the same daily books on a later entry.
        closed_fills = {}
        closed_order_qty = {}
        matched_closed_fills = set()
        matched_closed_qty = {}
        closed_proof = {}
        with db._get_conn() as conn:
            closed = conn.execute(
                "SELECT parent_order_id, buy_filled_qty FROM positions_lifecycle WHERE account_id=? AND security_id=? "
                "AND exchange_segment=? AND product_type=? AND position_state='POSITION_CLOSED' "
                "AND confirmed_open_qty=0 AND buy_filled_qty=sell_filled_qty AND buy_filled_qty>0",
                (account, *key)).fetchall()
            for lot in closed:
                parent = lot[0]
                if parent in parent_oids:
                    continue
                buys = [dict(r) for r in conn.execute(
                    "SELECT * FROM fills WHERE broker_order_id=? AND transaction_type='BUY' AND is_verified_trade=1",
                    (parent,))]
                sells = db.get_sell_fills_for_parent(parent)
                if (sum(r["traded_quantity"] for r in buys) != lot["buy_filled_qty"]
                        or sum(r["traded_quantity"] for r in sells) != lot["buy_filled_qty"]):
                    continue
                for r in buys + sells:
                    if _identity(r, journal=True) != key:
                        raise ReconciliationEvidenceError("Closed execution contract mismatch")
                    identity = (r["broker_order_id"], r["transaction_type"])
                    eid = r["exchange_trade_id"]
                    qty, price = closed_fills.get((identity, eid), (0, r["traded_price"]))
                    closed_fills[identity, eid] = (qty + r["traded_quantity"], price)
                    closed_order_qty[identity] = closed_order_qty.get(identity, 0) + r["traded_quantity"]

        # Build today's broker trade evidence
        broker_evidence: Dict[str, dict] = {}
        for row in trade_book:
            if not isinstance(row, dict):
                raise ReconciliationEvidenceError("Malformed current trade book")
            if str(row.get("securityId") or "").strip().upper() != key[0]:
                continue
            if _identity(row) != key:
                raise ReconciliationEvidenceError("Cross-contract trade shares security ID")
            side = str(row.get("transactionType") or "").upper()
            if side not in ("BUY", "SELL"):
                raise ReconciliationEvidenceError("Unsupported traded side")
            normalized = _broker_trade(row, account, key, side)
            eid = normalized["eid"]
            closed_match = closed_fills.get(((normalized["oid"], side), eid))
            if closed_match == (normalized["qty"], normalized["price"]):
                identity = (normalized["oid"], side)
                if eid in broker_evidence:
                    raise ReconciliationEvidenceError("Conflicting duplicate exchange execution")
                closed_proof[eid] = dict(normalized, side=side)
                if eid not in matched_closed_fills:
                    matched_closed_qty[identity] = matched_closed_qty.get(identity, 0) + normalized["qty"]
                    matched_closed_fills.add(eid)
                continue
            if eid in closed_proof:
                raise ReconciliationEvidenceError("Conflicting duplicate exchange execution")
            if eid in broker_evidence and {k2: v2 for k2, v2 in broker_evidence[eid].items() if k2 not in ("side", "from_today", "from_history")} != normalized:
                raise ReconciliationEvidenceError("Conflicting duplicate exchange execution")
            broker_evidence[eid] = {**normalized, "side": side, "from_today": True, "from_history": False}
            if side == "BUY" and normalized["oid"] not in parent_oids:
                raise ReconciliationEvidenceError("Competing BUY execution requires allocation review")
            if side == "SELL" and eid not in exit_eids:
                raise ReconciliationEvidenceError("Another SELL execution needs allocation review")

        # Corroborate any SELL fills missing from today's tradebook via paginated history
        for eid, item in discovered.items():
            if eid not in broker_evidence:
                broker_evidence[eid] = {**item, "side": "SELL", "from_today": False, "from_history": True}
        missing = exit_eids - broker_evidence.keys()
        if missing:
            hist = _query_historical_sells(
                broker, account, key, earliest_started,
                required_eids=missing, today_date=today_ist, parents=parent_oids, known_sell_ids=exit_eids,
            )
            broker_evidence.update({
                eid: {**item, "side": "SELL", "from_today": False, "from_history": True}
                for eid, item in hist.items()
            })

        # P0-2: Enforce strict fill-level BUY/SELL execution chronology (not submission time!)
        _verify_fill_level_chronology(
            lot_details=lot_details,
            exits_for_contract=exits_for_contract,
            broker_evidence=broker_evidence,
        )

        # UREC-005, P0-4 & P1-5: Schema-aware, order-ID-bound protective order verification
        shared_super_exits = _verify_protective_orders_safe(
            forever=forever,
            super_orders=super_orders,
            key=key,
            account=account,
            broker_evidence={**closed_proof, **broker_evidence},
            orders=order_book,
        )

        shared_ids = parent_oids & {str(r["broker_order_id"]) for r in exits_for_contract}
        buy_eids = {r["exchange_trade_id"] for d in lot_details for r in d["buys"]}
        if buy_eids & exit_eids:
            raise ReconciliationEvidenceError("BUY and SELL share an exchange trade ID")
        if shared_ids and len(matching_lots) > 1:
            raise ReconciliationEvidenceError("Shared Super exit cannot be pooled across BUY parents")
        if shared_ids - shared_super_exits:
            raise ReconciliationEvidenceError("Shared BUY/SELL order ID lacks verified Super exit")

        for row in order_book:
            if str(row.get("securityId") or "").strip().upper() != key[0]:
                continue
            if not row.get("exchangeSegment") or not row.get("productType"):
                raise ReconciliationEvidenceError("Unattributed pending order on sold security")
            if _identity(row) != key:
                raise ReconciliationEvidenceError("Cross-contract order shares security ID")
            if str(row.get("orderStatus") or "").upper() not in _FINISHED:
                raise ReconciliationEvidenceError("Outstanding broker order on sold security")

        per_order: Dict[str, int] = {}
        per_order_eids: Dict[str, List[str]] = {}
        for r in exits_for_contract:
            eid = str(r["exchange_trade_id"])
            proof = broker_evidence[eid]
            if proof["side"] != "SELL" or proof["oid"] != str(r["broker_order_id"]):
                raise ReconciliationEvidenceError("SELL order/exchange-trade identity mismatch")
            if (proof["qty"] != _integer(r["traded_quantity"], positive=True)
                    or abs(proof["price"] - _price(r["traded_price"])) > 0.0001):
                raise ReconciliationEvidenceError("Recorded SELL quantity or price differs from broker")
            if proof["when"] < earliest_started:
                raise ReconciliationEvidenceError("SELL execution precedes original BUY intent")
            per_order[proof["oid"]] = per_order.get(proof["oid"], 0) + proof["qty"]
            per_order_eids.setdefault(proof["oid"], []).append(eid)

        ob_index = {}
        for ob in order_book:
            ob_id = str(ob.get("orderId") or "").strip()
            order_key = (ob_id, str(ob.get("transactionType") or "").upper())
            if ob_id and order_key in ob_index:
                raise ReconciliationEvidenceError("Duplicate broker orderbook order ID")
            if ob_id:
                ob_index[order_key] = ob

        evidence_oids = {v["oid"] for v in broker_evidence.values()}
        for ob in order_book:
            if str(ob.get("securityId") or "").strip().upper() != key[0]:
                continue
            ob_id = str(ob.get("orderId") or "").strip()
            ob_side = str(ob.get("transactionType") or "").strip().upper()
            ob_status = str(ob.get("orderStatus") or "").strip().upper()
            raw_filled = ob.get("filledQty")
            if raw_filled is None and ob_status == "REJECTED":
                if ob_id in evidence_oids or ob_id in per_order:
                    raise ReconciliationEvidenceError("Rejected order contradicts observed broker execution")
                filled = 0
            else:
                filled = _integer(raw_filled)
                if ob_status == "REJECTED" and filled > 0:
                    raise ReconciliationEvidenceError("Rejected order reports positive filled quantity")
            if (filled > 0 and matched_closed_qty.get((ob_id, ob_side)) == filled
                    and closed_order_qty.get((ob_id, ob_side)) == filled
                    and str(ob.get("dhanClientId") or account) == account
                    and ob_status in _FINISHED):
                continue
            if filled > 0:
                if ob_side == "BUY" and ob_id not in parent_oids:
                    raise ReconciliationEvidenceError("Competing broker BUY fill requires allocation review")
                if ob_side == "SELL" and ob_id not in per_order:
                    raise ReconciliationEvidenceError("Unattributed broker SELL fill requires allocation review")
                if ob_side not in ("BUY", "SELL"):
                    raise ReconciliationEvidenceError("Broker order with unknown executed side")
            if ob_side == "BUY" and ob_id in parent_oids:
                lot_match = next(d for d in lot_details if d["oid"] == ob_id)
                lot_match["entry_terminal_status"] = ob_status
                if filled != lot_match["buy_qty"]:
                    raise ReconciliationEvidenceError("Original broker BUY fill count disagrees with journal")

        # UREC-001: Validate today's SELL orders in order_book; do NOT require archived
        # prior-day historically verified SELL orders to appear in today's rolled-over orderbook.
        for sell_oid, q in per_order.items():
            row = ob_index.get((sell_oid, "SELL"))
            if row is not None:
                if str(row.get("transactionType") or "").upper() != "SELL" or _identity(row) != key:
                    raise ReconciliationEvidenceError("SELL orderbook identity mismatch")
                if str(row.get("dhanClientId") or account).strip() != account:
                    raise ReconciliationEvidenceError("SELL orderbook account mismatch")
                if _integer(row.get("filledQty")) != q:
                    raise ReconciliationEvidenceError("SELL orderbook partial fill count differs from tradebook")
            elif sell_oid not in shared_ids and any(str(r["broker_order_id"]) == sell_oid for r in all_candidates):
                order_proofs = [broker_evidence[eid] for eid in per_order_eids.get(sell_oid, [])]
                all_archived_prior_day = bool(order_proofs) and all(
                    p.get("from_history") and datetime.fromtimestamp(p["when"], IST).date() < today_ist
                    for p in order_proofs
                )
                if not all_archived_prior_day:
                    raise ReconciliationEvidenceError("Current SELL order absent from broker orderbook")

        # P0-3: Exact aggregate book evidence; never certify zero from missing holdings + negative netQty
        observed, source = observed_open_quantity(
            positions, holdings,
            security_id=key[0], segment=key[1], product=key[2], account_id=account,
        )
        if observed != projected_total:
            same_day_full_round_trip = (
                projected_total == 0
                and source == "SETTLED_HOLDING_NOT_REPORTED"
                and key[2] in ("CNC", "MTF")
                and all(r.get("side") in ("BUY", "SELL") for r in broker_evidence.values())
                and sum(v["qty"] for v in broker_evidence.values() if v["side"] == "BUY") == total_buy_qty
                and sum(v["qty"] for v in broker_evidence.values() if v["side"] == "SELL") == total_buy_qty
                and all(
                    datetime.fromtimestamp(v["when"], IST).date()
                    == datetime.fromtimestamp(earliest_started, IST).date()
                    for v in broker_evidence.values()
                )
            )
            signed_delivery_ok = (
                source == "NEGATIVE_CNC_POSITION_REVIEW"
                and key[1] in ("NSE_EQ", "BSE_EQ")
                and _signed_cnc_delivery_sell_reconciles(
                    positions=positions,
                    holdings=holdings,
                    key=key,
                    account=account,
                    projected=projected_total,
                    broker_evidence=broker_evidence,
                )
            )
            prior_day_full_exit = (
                projected_total == 0 and source == "SETTLED_HOLDING_NOT_REPORTED"
                and key[2] in ("CNC", "MTF")
                and not any(_identity(row) == key for row in positions if str(row.get("securityId")) == key[0])
                and all(broker_evidence[eid]["from_history"] and
                        datetime.fromtimestamp(broker_evidence[eid]["when"], IST).date() < today_ist
                        for eid in exit_eids)
            )
            if not (same_day_full_round_trip or signed_delivery_ok or prior_day_full_exit):
                raise ReconciliationEvidenceError(
                    f"Broker exposure does not match verified fills: expected={projected_total}, "
                    f"observed={observed}, status={source}"
                )

        _check_watcher_safety(watcher, lot_details)

        # Atomic persistence in SQLite
        is_multi_lot = len(lot_details) > 1
        primary_oid = lot_details[0]["oid"]
        event_ids: Dict[str, int] = {}

        if all_candidates:
            with db._get_conn() as conn:
                for r in discovered_rows:
                    eid = str(r["exchange_trade_id"])
                    proof = broker_evidence[eid]
                    target_parent = "MULTI_LOT_ALLOCATED" if is_multi_lot else primary_oid
                    conn.execute(
                        """
                        INSERT INTO fills (
                            broker_order_id, exchange_trade_id, symbol, traded_price,
                            traded_quantity, traded_at, transaction_type, security_id,
                            exchange_segment, product_type, parent_order_id, is_verified_trade
                        ) VALUES (?, ?, ?, ?, ?, ?, 'SELL', ?, ?, ?, ?, 1)
                        """,
                        (
                            proof["oid"], eid, str(r["symbol"]), proof["price"],
                            proof["qty"], proof["when"], key[0], key[1], key[2], target_parent,
                        ),
                    )
                for r in candidates:
                    prior = conn.execute(
                        "SELECT broker_order_id, parent_order_id, traded_quantity, traded_price, transaction_type "
                        "FROM fills WHERE id=? AND exchange_trade_id=? AND is_verified_trade=1",
                        (r["id"], r["exchange_trade_id"]),
                    ).fetchone()
                    if prior is None or str(prior["parent_order_id"] or "") not in ("", str(r["broker_order_id"])):
                        raise ReconciliationEvidenceError("SELL attribution changed concurrently")
                    proof = broker_evidence[str(r["exchange_trade_id"])]
                    target_parent = "MULTI_LOT_ALLOCATED" if is_multi_lot else primary_oid
                    changed = conn.execute(
                        "UPDATE fills SET parent_order_id=?, traded_at=? WHERE id=? AND "
                        "(parent_order_id IS NULL OR parent_order_id='' OR parent_order_id=broker_order_id)",
                        (target_parent, proof["when"], r["id"]),
                    ).rowcount
                    if changed != 1:
                        raise ReconciliationEvidenceError("Concurrent SELL fill attribution")

                if is_multi_lot:
                    sell_queue = [
                        {
                            "oid": broker_evidence[str(r["exchange_trade_id"])]["oid"],
                            "eid": str(r["exchange_trade_id"]),
                            "rem": _integer(r["traded_quantity"], positive=True),
                            "price": broker_evidence[str(r["exchange_trade_id"])]["price"],
                            "when": broker_evidence[str(r["exchange_trade_id"])]["when"],
                        }
                        for r in all_candidates
                    ]
                    sq_idx = 0
                    for d in lot_details:
                        need = d["buy_qty"] - sum(
                            _integer(x["traded_quantity"], positive=True) for x in d["already_linked"]
                        )
                        while need > 0:
                            while sq_idx < len(sell_queue) and sell_queue[sq_idx]["rem"] <= 0:
                                sq_idx += 1
                            if sq_idx >= len(sell_queue):
                                raise ReconciliationEvidenceError("Multi-lot allocation exhausted SELL fills")
                            take = min(need, sell_queue[sq_idx]["rem"])
                            sell_queue[sq_idx]["rem"] -= take
                            need -= take
                            conn.execute(
                                """
                                INSERT INTO exit_allocations(
                                    parent_order_id, sell_order_id, exchange_trade_id,
                                    allocated_qty, traded_price, traded_at
                                ) VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    d["oid"], sell_queue[sq_idx]["oid"], sell_queue[sq_idx]["eid"],
                                    take, sell_queue[sq_idx]["price"], sell_queue[sq_idx]["when"],
                                ),
                            )

                for d in lot_details:
                    lot_proj = 0 if is_multi_lot else projected_total
                    payload = json.dumps({
                        "account": account,
                        "parent_order_id": d["oid"],
                        "contract": list(key),
                        "sell_exchange_trade_ids": sorted(exit_eids),
                        "projected_open_qty": lot_proj,
                    }, sort_keys=True)
                    ev_id = conn.execute(
                        "INSERT INTO system_events(event_type,description,payload,created_at) "
                        "VALUES('UNIFIED_EXIT_LINKED',?,?,?)",
                        (d["oid"], payload, time.time()),
                    ).lastrowid
                    event_ids[d["oid"]] = int(ev_id)
        else:
            for d in lot_details:
                if d["oid"] in pending_sync:
                    event_ids[d["oid"]] = pending_sync[d["oid"]][0]

        for d in lot_details:
            lot_proj = 0 if is_multi_lot else projected_total
            _sync_parent_lot_to_watcher(
                db=db, watcher=watcher, d=d, key=key, lot_proj=lot_proj, event_id=event_ids.get(d["oid"]),
            )

        linked_oids = ", ".join(d["oid"] for d in lot_details)
        linked_messages.append(f"Verified SELL(s) linked to BUY {linked_oids}")

    if linked_messages:
        return True, "; ".join(linked_messages) + "; independent gate recheck required"
    return False, "No applicable exits"
