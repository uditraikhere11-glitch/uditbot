"""Offline-only diagnostic for negative CNC/MTF Dhan day-position quantities.

Never changes the canonical reconciliation gate, order states, holdings, or
protection. CORROBORATED is only consistency evidence, NOT proof that a
specific bot-owned lot was closed or that new entries may be accepted.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Sequence

_NUM = re.compile(r"[+-]?\d+\Z")

@dataclass(frozen=True)
class SaleTriage:
    classification: str
    reason: str
    negative_net_qty: int | None
    bought_today: int
    sold_today: int
    unique_executions: int
    allow_entry_gate_open: bool = False
    allow_position_close: bool = False
    allow_protection_cancel: bool = False


def _qty(value: Any, *, signed=False) -> int:
    if type(value) is int:
        n = value
    elif isinstance(value, str) and _NUM.fullmatch(value.strip()):
        n = int(value.strip())
    else:
        raise ValueError("invalid integer quantity")
    if not signed and n < 0:
        raise ValueError("negative unsigned quantity")
    return n


def _identity(row: dict) -> tuple[str, str, str, str]:
    return (
        str(row.get("dhanClientId") or "").strip(),
        str(row.get("securityId") or "").strip(),
        str(row.get("exchangeSegment") or "").strip().upper(),
        str(row.get("productType") or "").strip().upper(),
    )


def _unknown(reason: str, net: int | None = None) -> SaleTriage:
    return SaleTriage("UNVERIFIED", reason, net, 0, 0, 0)


def triage_negative_delivery_position(
    position: dict, *, account_id: str, market_date: str,
    trades: Sequence[dict] | None, holdings: Sequence[dict] | None,
    books_complete: bool,
) -> SaleTriage:
    if not isinstance(position, dict):
        return _unknown("position is not a dictionary")
    try:
        net = _qty(position.get("netQty"), signed=True)
    except ValueError as exc:
        return _unknown(str(exc))
    if net >= 0:
        return SaleTriage("NOT_NEGATIVE", "no negative day position", net, 0, 0, 0)
    account, security, segment, product = _identity(position)
    if not account_id or not security or segment not in ("NSE_EQ", "BSE_EQ") or product not in ("CNC", "MTF"):
        return _unknown("not an identified equity CNC/MTF position", net)
    if account != str(account_id):
        return _unknown("position account missing or mismatched", net)
    try:
        day = datetime.strptime(market_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return _unknown("invalid exchange session date", net)
    if books_complete is not True or not isinstance(trades, (list, tuple)) or not isinstance(holdings, (list, tuple)):
        return _unknown("complete successful trade and holdings reads required", net)

    matched_holdings = []
    for holding in holdings:
        if not isinstance(holding, dict):
            return _unknown("malformed holdings row", net)
        if str(holding.get("securityId") or "").strip() != security:
            continue
        if str(holding.get("dhanClientId") or "").strip() != account_id:
            return _unknown("matching holdings account missing/mismatched", net)
        try:
            for col in ("totalQty", "dpQty", "t1Qty"):
                if col in holding and holding[col] is not None:
                    _qty(holding[col])
        except ValueError:
            return _unknown("malformed holdings quantity", net)
        matched_holdings.append(holding)
    if len(matched_holdings) > 1:
        return _unknown("ambiguous duplicate holdings", net)

    seen: dict[str, tuple[str, int, str]] = {}
    buys = sells = 0
    for row in trades:
        if not isinstance(row, dict):
            return _unknown("malformed trade row", net)
        if str(row.get("securityId") or "").strip() != security:
            continue
        account2, security2, segment2, product2 = _identity(row)
        if (account2, security2, segment2, product2) != (account_id, security, segment, product):
            return _unknown("trade contract/account identity disagreement", net)
        trade_id = str(row.get("exchangeTradeId") or "").strip()
        order_id = str(row.get("orderId") or "").strip()
        side = str(row.get("transactionType") or "").strip().upper()
        if not trade_id or not order_id or side not in ("BUY", "SELL"):
            return _unknown("missing execution ID, order ID or side", net)
        try:
            qty = _qty(row.get("tradedQuantity"))
            ts = datetime.strptime(str(row.get("exchangeTime") or ""), "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return _unknown("malformed exchange execution quantity or time", net)
        if ts.date() != day or qty <= 0:
            return _unknown("wrong execution session or nonpositive quantity", net)
        evidence = (side, qty, order_id)
        if trade_id in seen:
            if seen[trade_id] != evidence:
                return _unknown("conflicting duplicate exchange trade ID", net)
            continue
        seen[trade_id] = evidence
        if side == "BUY":
            buys += qty
        else:
            sells += qty

    if not seen or sells <= 0:
        return _unknown("no broker-confirmed same-day sales", net)
    if buys - sells != net:
        return SaleTriage("NET_MISMATCH", "unique exchange executions do not equal netQty", net,
                         buys, sells, len(seen))
    return SaleTriage(
        "CORROBORATED_DELIVERY_SELL_REQUIRES_ALLOCATION",
        "trade net matches; journal allocation, current holdings and protection still need independent verification",
        net, buys, sells, len(seen),
    )
