"""Live-schema evidence for the narrow T+0 closed-sibling recovery path.

No order operations, persistence, token generation, or gate overrides. The
caller provides a fresh gate/pass-local cache, never a long-lived cache.
"""
from __future__ import annotations

import json
from math import isfinite

from src.engine.holding_quantity import aggregate_holdings


_QUANTITY_FIELDS = (
    "totalQty", "dpQty", "t1Qty", "availableQty", "mtf_qty", "mtf_t1_qty",
    "collateralQty",
)
_DEAD = {"CANCELLED", "CANCELED", "REJECTED", "EXPIRED"}
_LEGS = {"TARGET_LEG", "STOP_LOSS_LEG"}


def _holdings_fingerprint(rows, account):
    """Compare ownership fields, not moving last-traded prices."""
    aggregate_holdings(rows, account_id=account)
    normalized = []
    for row in rows:
        if "dhanClientId" in row and str(row["dhanClientId"] or "").strip() != account:
            raise ValueError("Explicit holdings account missing or mismatched")
        sid = str(row.get("securityId") or "").strip()
        isin = str(row.get("isin") or "").strip().upper()
        if not sid or not isin:
            raise ValueError("Holdings security or ISIN missing")
        normalized.append([sid, isin, *[row.get(k) for k in _QUANTITY_FIELDS]])
    return json.dumps(sorted(normalized, key=lambda r: r[0]), allow_nan=False, separators=(",", ":"))


def attest_signed_holdings(broker, holdings, account, cache):
    """Return account attestation for this exact ownership snapshot only.

    A raw holdings row need not carry dhanClientId. When any row lacks it,
    require a second, account-proven snapshot using a *single pinned token*
    for profile and holdings. The ownership fields must match the original
    pass snapshot exactly. No synthetic dhanClientId is injected into a row.
    """
    if not account or str(getattr(getattr(broker, "cfg", None), "dhan_client_id", "") or "").strip() != account:
        raise ValueError("Signed holdings broker account unverified")
    fingerprint = _holdings_fingerprint(holdings, account)
    if all("dhanClientId" in row for row in holdings):
        return account  # Existing explicit row-account path remains strict.
    key = (id(broker), account, fingerprint)
    if cache.get(key) == account:
        return account
    read = getattr(broker, "get_account_bound_holdings", None)
    if not callable(read):
        raise ValueError("Account-bound holdings read unavailable")
    result = read()
    if not isinstance(result, tuple) or len(result) != 2 or result[0] != account:
        raise ValueError("Pinned Dhan profile does not verify holdings account")
    if _holdings_fingerprint(result[1], account) != fingerprint:
        raise ValueError("Holdings changed between original and account-bound read")
    cache[key] = account
    return account


def _positive_qty(value):
    if type(value) is int:
        number = value
    elif type(value) is str and value.isascii() and value.isdigit():
        number = int(value)
    else:
        raise ValueError("Malformed protective execution quantity")
    if number <= 0:
        raise ValueError("Nonpositive protective execution quantity")
    return number


def _text(row, field):
    return str(row.get(field) or "").strip()


def verify_closed_triggered_group(*, group, closed, forever, orders, day_trades,
                                  account, security, segment, product):
    """Read-only proof of a closed sibling's Dhan TRIGGERED OCO group.

    A triggered row is NOT terminal on its own. The journal must bind the
    exact OCO group and executed leg to one fully closed parent, the live
    tradebook must match its immutable SELL fills, and the shared broker
    validator must confirm every spawned child is terminal with zero residual
    executable quantity. An explicit sibling must be dead; an omitted sibling
    is not represented as explicitly cancelled (Dhan's triggered schema).
    """
    from src.engine.unified_exit_reconciliation import validate_forever_oco_child_orders

    proof = (closed.get("triggered_groups") or {}).get(group)
    if not isinstance(proof, dict) or not all(isinstance(book, list) and all(isinstance(r, dict) for r in book)
                                               for book in (forever, orders, day_trades)):
        raise ValueError("Closed triggered OCO lacks complete parent-bound books")
    expected_leg = proof.get("leg")
    expected_children = proof.get("children")
    expected_qty = _positive_qty(proof.get("quantity"))
    if (expected_leg not in _LEGS or not isinstance(expected_children, dict) or not expected_children
            or group in expected_children
            or sum(_positive_qty(q) for q in expected_children.values()) != expected_qty):
        raise ValueError("Closed triggered OCO parent allocation is ambiguous")
    rows = [r for r in forever if _text(r, "orderId") == group]
    seen_legs = set()
    triggered = []
    for row in rows:
        if (_text(row, "dhanClientId") != account or _text(row, "securityId") != security
                or _text(row, "exchangeSegment").upper() != segment
                or _text(row, "productType").upper() != product
                or _text(row, "transactionType").upper() != "SELL"):
            raise ValueError("Triggered OCO group identity mismatch")
        classifications = {_text(row, "orderFlag").upper(), _text(row, "orderType").upper()}
        if "OCO" not in classifications or "SINGLE" in classifications:
            raise ValueError("Triggered group not unambiguously Forever OCO")
        leg = _text(row, "legName").upper()
        state = _text(row, "orderStatus").upper()
        if leg not in _LEGS or leg in seen_legs:
            raise ValueError("Duplicate or unknown OCO leg")
        seen_legs.add(leg)
        if state == "TRIGGERED":
            if leg != expected_leg or _positive_qty(row.get("quantity")) != expected_qty:
                raise ValueError("Triggered leg differs from closed journal parent")
            triggered.append(row)
        elif state not in _DEAD:
            raise ValueError("Outstanding OCO sibling leg")
    if len(triggered) != 1:
        raise ValueError("Expected one triggered protective leg")

    live_children = {}
    seen_execs = set()
    for trade in day_trades:
        oid = _text(trade, "orderId")
        if oid not in expected_children:
            continue
        eid = _text(trade, "exchangeTradeId")
        qty = _positive_qty(trade.get("tradedQuantity"))
        journal = (closed.get("trades") or {}).get((oid, eid))
        try:
            if isinstance(trade.get("tradedPrice"), bool):
                raise ValueError("Boolean trade price")
            price = float(trade.get("tradedPrice"))
        except (TypeError, ValueError) as exc:
            raise ValueError("Malformed child execution price") from exc
        if (not eid or eid in seen_execs or not journal or journal[0] != "SELL"
                or journal[1] != qty or not isfinite(price) or price <= 0 or abs(journal[2] - price) >= 1e-6
                or _text(trade, "dhanClientId") != account or _text(trade, "securityId") != security
                or _text(trade, "exchangeSegment").upper() != segment
                or _text(trade, "productType").upper() != product
                or _text(trade, "transactionType").upper() != "SELL"):
            raise ValueError("Triggered child live SELL differs from journal")
        seen_execs.add(eid)
        live_children[oid] = live_children.get(oid, 0) + qty
    if live_children != expected_children:
        raise ValueError("Triggered group lacks all exact current child executions")
    confirmed = validate_forever_oco_child_orders(
        forever_order_id=group, expected_leg_qty=expected_qty,
        key=(security, segment, product), account=account, orders=orders,
        sell_fill_qty_by_oid=live_children,
    )
    if confirmed != set(expected_children):
        raise ValueError("Triggered group child identity differs from journal")
    return True


def _closed_watcher_context(snapshot, lifecycle, groups):
    """Strict context for the live verified_leg_map-only journal representation."""
    leg = lifecycle.get("triggered_leg")
    return (
        len(groups) == 1 and leg in _LEGS
        and snapshot.get("triggered_leg") == leg
        and snapshot.get("order_id") == lifecycle.get("parent_order_id")
        and snapshot.get("security_id") == lifecycle.get("security_id")
        and snapshot.get("segment") == lifecycle.get("exchange_segment")
        and snapshot.get("product_type") == lifecycle.get("product_type")
        and snapshot.get("protection_route") == "FOREVER_OCO"
        and snapshot.get("position_state") == "POSITION_CLOSED"
        and type(snapshot.get("confirmed_position_open_qty")) is int
        and snapshot["confirmed_position_open_qty"] == 0
        and type(snapshot.get("filled_qty")) is int
        and snapshot["filled_qty"] == lifecycle.get("buy_filled_qty")
        and type(snapshot.get("exit_filled_qty")) is int
        and snapshot["exit_filled_qty"] == lifecycle.get("sell_filled_qty")
        and snapshot.get("confirmed_protection_ids") == list(groups)
        and not any(snapshot.get(k) for k in (
            "manual_exit_detected", "orphaned_legs_detected",
            "orphaned_legs_manual_intervention_required", "uncommitted_broker_sell_trades",
            "persistence_uncertain", "reconciliation_required",
        ))
    )


def recover_closed_watcher_child_ids(snapshot, lifecycle, groups):
    """Accept only the exact persisted group + executed leg map, never a group as a child."""
    if not _closed_watcher_context(snapshot, lifecycle, groups):
        raise ValueError("Closed OCO watcher context does not prove child ownership")
    group = groups[0]
    leg = lifecycle["triggered_leg"]
    ids = snapshot.get("verified_leg_order_ids")
    mapping = snapshot.get("verified_leg_map")
    if (not isinstance(ids, list) or not ids
            or any(type(oid) is not str or not oid or oid != oid.strip() or oid == group for oid in ids)
            or len(ids) != len(set(ids)) or not isinstance(mapping, dict)
            or set(mapping) != set(ids) | {group}
            or any(value != leg for value in mapping.values())):
        raise ValueError("Closed OCO watcher leg map is missing, conflicting or ambiguous")
    return set(ids)


def closed_triggered_binding(snapshot, lifecycle, group_rows, sell_quantities):
    """Build a *candidate* group-to-child allowlist, never broker cancellation proof."""
    groups = [str(r["forever_order_id"]) for r in group_rows]
    if not _closed_watcher_context(snapshot, lifecycle, groups):
        return None
    if (group_rows[0]["status"] != "POSITION_CLOSED_LOCAL"
            or type(group_rows[0]["quantity"]) is not int
            or group_rows[0]["quantity"] != lifecycle["buy_filled_qty"]
            or groups[0] in sell_quantities or not sell_quantities
            or sum(sell_quantities.values()) != lifecycle["sell_filled_qty"]):
        return None
    leg = lifecycle["triggered_leg"]
    explicit = str(snapshot.get("tp_order_id" if leg == "TARGET_LEG" else "sl_order_id") or "").strip()
    if explicit:
        if set(sell_quantities) != {explicit}:
            return None
    else:
        try:
            if recover_closed_watcher_child_ids(snapshot, lifecycle, groups) != set(sell_quantities):
                return None
        except ValueError:
            return None
    return {"leg": leg, "quantity": lifecycle["sell_filled_qty"], "children": dict(sell_quantities)}
