"""Fail-closed T+0 signed CNC/MTF delivery quantity proof.

Pure/read-only: no Dhan requests, orders, journal writes or gate changes. The
normal two-book quantity API remains deliberately unable to approve netQty<0.
A closed Forever-OCO sibling is only credited after durable parent, trade and
child-leg evidence; the active broker exit books remain separate admission gates.
"""
from __future__ import annotations

from datetime import datetime
from math import isfinite
import re
from typing import Any

from src.engine.holding_quantity import aggregate_holdings, observed_open_quantity

try:
    from zoneinfo import ZoneInfo
    _IST = ZoneInfo("Asia/Kolkata")
except ImportError:  # pragma: no cover - required in production Python
    raise


class SignedCNCDeliveryUnverified(ValueError):
    """Negative day quantity is not justified by exact bot-owned executions."""


def _positive_int(value: Any) -> int:
    if isinstance(value, bool) or isinstance(value, float) or not re.fullmatch(r"[+]?[0-9]+", str(value)):
        raise SignedCNCDeliveryUnverified("Malformed executed quantity")
    result = int(value)
    if result <= 0:
        raise SignedCNCDeliveryUnverified("Nonpositive executed quantity")
    return result


def _price(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        raise SignedCNCDeliveryUnverified("Malformed execution price")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SignedCNCDeliveryUnverified("Malformed execution price") from exc
    if not isfinite(result) or result <= 0:
        raise SignedCNCDeliveryUnverified("Nonpositive/nonfinite execution price")
    return result


def verify_signed_cnc_delivery_quantity(
    *, db: Any, positions: Any, holdings: Any, trade_book: Any,
    account: str, security: str, segment: str, product: str,
    expected_open_qty: int, now: float,
    holdings_profile_account: str | None = None,
) -> int:
    """Return remaining quantity only for fully proven account-bound T+0 sales.

    All same-security broker trades must have their exact immutable journal
    execution, including a single verified parent. Closed siblings must prove
    fully closed Forever-OCO BUY/SELL/child ownership via the journal's
    existing get_closed_forever_sibling_evidence() verifier. A currently open
    parent's SELL must match a durable named watcher exit leg. Incomplete
    books, unlinked/manual sales, missing ACK, bad allocations fail closed.
    """
    account, security = str(account or "").strip(), str(security or "").strip()
    segment, product = str(segment or "").strip().upper(), str(product or "").strip().upper()
    if (not account or not security or segment not in ("NSE_EQ", "BSE_EQ")
            or product not in ("CNC", "MTF")
            or not isinstance(positions, list) or not isinstance(holdings, list)
            or not isinstance(trade_book, list)
            or any(not isinstance(x, dict) for x in positions + holdings + trade_book)
            or not db or str(getattr(db, "account_id", "") or "").strip() != account
            or type(expected_open_qty) is not int or expected_open_qty < 0
            or type(now) not in (int, float) or not isfinite(now) or now <= 0):
        raise SignedCNCDeliveryUnverified("Signed delivery input/account evidence incomplete")
    observed, status = observed_open_quantity(
        positions, holdings, security_id=security, segment=segment,
        product=product, account_id=account,
    )
    if observed is not None or status != "NEGATIVE_CNC_POSITION_REVIEW":
        raise SignedCNCDeliveryUnverified("Not an unverified negative CNC delivery position")
    matching = []
    for position in positions:
        if str(position.get("securityId") or "").strip() != security:
            continue
        if str(position.get("dhanClientId") or "").strip() != account:
            raise SignedCNCDeliveryUnverified("Broker position account unverified")
        pos_contract = (str(position.get("exchangeSegment") or "").strip().upper(),
                        str(position.get("productType") or "").strip().upper())
        if pos_contract != (segment, product):
            if str(position.get("netQty") or "0").strip() not in ("0", "+0", "-0"):
                raise SignedCNCDeliveryUnverified("Other same-security position requires attribution")
            continue
        matching.append(position)
    if len(matching) != 1:
        raise SignedCNCDeliveryUnverified("Signed position contract ambiguous")
    raw_net = matching[0].get("netQty")
    if isinstance(raw_net, (bool, float)) or not re.fullmatch(r"-[1-9][0-9]*", str(raw_net)):
        raise SignedCNCDeliveryUnverified("Signed CNC netQty malformed")
    net_day = int(raw_net)
    # A prior-day delivered holding cannot be counted twice as a day position.
    for key in ("carryForwardBuyQty", "carryForwardSellQty"):
        if key in matching[0] and matching[0][key] is not None:
            if str(matching[0][key]) != "0":
                raise SignedCNCDeliveryUnverified("Nonzero carry-forward quantity overlaps holdings")
    if ("dayBuyQty" in matching[0] and "daySellQty" in matching[0]
            and matching[0]["dayBuyQty"] is not None and matching[0]["daySellQty"] is not None):
        raw_day_buy = matching[0]["dayBuyQty"]
        raw_day_sell = matching[0]["daySellQty"]
        if (isinstance(raw_day_buy, (bool, float))
                or isinstance(raw_day_sell, (bool, float))
                or not re.fullmatch(r"[+]?[0-9]+", str(raw_day_buy))
                or not re.fullmatch(r"[+]?[0-9]+", str(raw_day_sell))):
            raise SignedCNCDeliveryUnverified("Reported day quantities malformed")
        if int(raw_day_buy) - int(raw_day_sell) != net_day:
            raise SignedCNCDeliveryUnverified("Reported day BUY/SELL quantities conflict")
    rows = [r for r in holdings if str(r.get("securityId") or "").strip() == security]
    if len(rows) != 1 or not str(rows[0].get("isin") or "").strip():
        raise SignedCNCDeliveryUnverified("Settled holding identity/ISIN unavailable")
    if (holdings_profile_account is not None and holdings_profile_account != account
            or any("dhanClientId" in row and str(row["dhanClientId"] or "").strip() != account
                   for row in holdings)
            or (any("dhanClientId" not in row for row in holdings)
                and holdings_profile_account != account)):
        raise SignedCNCDeliveryUnverified("Settled holdings account provenance unavailable or mismatched")
    try:
        settled = aggregate_holdings(holdings, account_id=account)[security]
    except (ValueError, KeyError) as exc:
        raise SignedCNCDeliveryUnverified("Settled quantity unavailable") from exc
    if settled < -net_day or settled + net_day != expected_open_qty:
        raise SignedCNCDeliveryUnverified("Settled plus signed day quantity mismatches bot open lots")

    open_rows = [
        row for row in db.get_open_positions_lifecycle()
        if str(row.get("security_id") or "").strip() == security
        and str(row.get("exchange_segment") or "").strip().upper() == segment
        and str(row.get("product_type") or "").strip().upper() == product
    ]
    if (not open_rows
            or sum(_positive_int(r.get("confirmed_open_qty")) for r in open_rows) != expected_open_qty
            or any(str(r.get("account_id") or "").strip() != account for r in open_rows)):
        raise SignedCNCDeliveryUnverified("Open journal parent quantities/accounts disagree")
    open_ids = tuple(str(r.get("parent_order_id") or "").strip() for r in open_rows)
    if any(not p for p in open_ids) or len(open_ids) != len(set(open_ids)):
        raise SignedCNCDeliveryUnverified("Open parent identities ambiguous")
    for row in open_rows:
        if (str(row.get("position_state") or "").upper() in
                ("UNPROTECTED_MANUAL_RISK", "RECONCILIATION_REQUIRED")
                and int(row.get("protected_qty") or 0) == 0):
            parent = str(row["parent_order_id"])
            if not db.is_manual_risk_acknowledged(
                parent, parent, expected_account_id=account,
                expected_security_id=security,
                required_qty=int(row["confirmed_open_qty"]),
            ):
                raise SignedCNCDeliveryUnverified("Owner acknowledgement missing")

    # This strict journal routine verifies closed parent lifecycle, original
    # BUY intent/fills, OCO group, exact child leg IDs and full SELL allocations.
    try:
        closed = db.get_closed_forever_sibling_evidence(
            account, security, segment, product,
            open_parent_ids=open_ids, now=now,
        )
    except (AttributeError, ValueError, TypeError, KeyError) as exc:
        raise SignedCNCDeliveryUnverified("Closed sibling ownership unavailable") from exc
    closed_trades = closed.get("trades") or {}
    seen: set[str] = set()
    today_buy = today_sell = 0
    today = datetime.fromtimestamp(now, _IST).date()
    with db._get_conn() as conn:
        for trade in trade_book:
            sid = str(trade.get("securityId") or "").strip()
            if not sid:
                raise SignedCNCDeliveryUnverified("Broker trade security ID missing")
            if sid != security:
                continue
            if (str(trade.get("dhanClientId") or "").strip() != account
                    or str(trade.get("exchangeSegment") or "").strip().upper() != segment
                    or str(trade.get("productType") or "").strip().upper() != product):
                raise SignedCNCDeliveryUnverified("Other same-security contract/account trade")
            side = str(trade.get("transactionType") or "").strip().upper()
            order_id = str(trade.get("orderId") or "").strip()
            eid = str(trade.get("exchangeTradeId") or "").strip()
            if (side not in ("BUY", "SELL") or not order_id or not eid or eid in seen
                    or eid.upper() in ("0", "NONE", "NULL")
                    or "-FILL-" in eid.upper() or "-RECON-" in eid.upper()):
                raise SignedCNCDeliveryUnverified("Duplicate/missing exchange trade ID")
            seen.add(eid)
            qty = _positive_int(trade.get("tradedQuantity"))
            price = _price(trade.get("tradedPrice"))
            fill = db.get_fill_by_exchange_trade_id(eid)
            if (not isinstance(fill, dict) or fill.get("is_verified_trade") not in (1, True)
                    or str(fill.get("transaction_type") or "").strip().upper() != side
                    or str(fill.get("broker_order_id") or "").strip() != order_id
                    or str(fill.get("security_id") or "").strip() != security
                    or str(fill.get("exchange_segment") or "").strip().upper() != segment
                    or str(fill.get("product_type") or "").strip().upper() != product
                    or type(fill.get("traded_quantity")) is not int
                    or fill["traded_quantity"] != qty
                    or abs(_price(fill.get("traded_price")) - price) >= 1e-6
                    or not str(fill.get("parent_order_id") or "").strip()
                    or fill.get("parent_order_id") == "MULTI_LOT_ALLOCATED"):
                raise SignedCNCDeliveryUnverified("Broker execution lacks exact verified journal identity")
            # A trade-book row is not automatically a current-day execution
            # just because its journal insertion happened today. Verify the
            # broker exchangeTime independently of the stored fill timestamp.
            try:
                from src.engine.unified_exit_reconciliation import _exchange_time
                broker_at = _exchange_time(trade.get("exchangeTime"))
                journal_at = float(fill.get("traded_at") or 0)
                if (not isfinite(journal_at)
                        or datetime.fromtimestamp(broker_at, _IST).date() != today
                        or datetime.fromtimestamp(journal_at, _IST).date() != today):
                    raise SignedCNCDeliveryUnverified("Execution is not from this trading day")
            except (OverflowError, OSError, TypeError, ValueError) as exc:
                raise SignedCNCDeliveryUnverified("Broker/journal execution day invalid") from exc
            parent = str(fill["parent_order_id"])
            lot = db.get_position_lifecycle(parent)
            if (not isinstance(lot, dict)
                    or str(lot.get("account_id") or "").strip() != account
                    or str(lot.get("security_id") or "").strip() != security
                    or str(lot.get("exchange_segment") or "").strip().upper() != segment
                    or str(lot.get("product_type") or "").strip().upper() != product):
                raise SignedCNCDeliveryUnverified("Execution parent account/contract unverified")
            if side == "BUY":
                if order_id != parent or parent not in open_ids:
                    raise SignedCNCDeliveryUnverified("Unattributed current-day BUY")
                today_buy += qty
            else:
                closed_proof = closed_trades.get((order_id, eid))
                if str(lot.get("position_state") or "") == "POSITION_CLOSED":
                    if (not closed_proof or closed_proof[0] != "SELL"
                            or closed_proof[1] != qty or abs(closed_proof[2] - price) >= 1e-6):
                        raise SignedCNCDeliveryUnverified("Closed sibling SELL not proven by Forever OCO")
                else:
                    # A still-open parent can have a genuine partial exit, but
                    # the SELL must be its own named protective child order.
                    watcher_rows = conn.execute(
                        "SELECT record_json FROM watcher_orders WHERE order_id=?", (parent,)
                    ).fetchall()
                    if len(watcher_rows) != 1:
                        raise SignedCNCDeliveryUnverified("Open parent protective SELL leg unknown")
                    import json
                    record = json.loads(watcher_rows[0][0])
                    if order_id not in (str(record.get("tp_order_id") or ""),
                                        str(record.get("sl_order_id") or "")):
                        raise SignedCNCDeliveryUnverified("Manual/unattributed SELL order")
                # Never silently consume a portion of an allocated shared exit.
                if conn.execute(
                    "SELECT 1 FROM exit_allocations WHERE exchange_trade_id=? LIMIT 1", (eid,)
                ).fetchone():
                    raise SignedCNCDeliveryUnverified("Shared multi-lot SELL allocation requires review")
                today_sell += qty
    if not seen or today_sell <= 0 or today_buy - today_sell != net_day:
        raise SignedCNCDeliveryUnverified("Day BUY minus SELL does not match signed CNC netQty")
    return expected_open_qty
