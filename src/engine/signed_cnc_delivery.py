"""Fail-closed T+0 signed CNC/MTF delivery quantity proof.

Pure/read-only: no Dhan requests, orders, journal writes or gate changes. The
normal two-book quantity API remains deliberately unable to approve netQty<0.
A closed Forever-OCO sibling is only credited after durable parent, trade and
child-leg evidence; the active broker exit books remain separate admission gates.
"""
from __future__ import annotations

from datetime import datetime, time as clock_time, timedelta
from math import isfinite
import re
from typing import Any

from src.engine.holding_quantity import aggregate_holdings, observed_open_quantity, _quantity

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


def validate_signed_position_components(position, net_day, today_buy=None, today_sell=None,
                                        *, require_day_totals=False):
    """Validate reported components without treating a missing value as zero.

    Legacy callers may lack both day fields; a partial pair is never accepted.
    The closed-sibling recovery requires the complete pair.
    """
    for key in ("carryForwardBuyQty", "carryForwardSellQty"):
        if key in position and _quantity(position[key]) != 0:
            raise SignedCNCDeliveryUnverified("Carry-forward quantity overlaps holdings")
    for buy_key, sell_key, required in (
        ("dayBuyQty", "daySellQty", require_day_totals),
        ("buyQty", "sellQty", False),
    ):
        if required or buy_key in position or sell_key in position:
            buy, sell = _quantity(position.get(buy_key)), _quantity(position.get(sell_key))
            if buy - sell != net_day:
                raise SignedCNCDeliveryUnverified("Reported BUY/SELL quantities conflict")
            if today_buy is not None and (buy != today_buy or sell != today_sell):
                raise SignedCNCDeliveryUnverified("Reported BUY/SELL totals differ from verified tradebook")


def signed_delivery_session_date(*, trade_book, account, security, segment, product, now):
    """Select an execution date, never manufacture or extend a broker snapshot.

    Prior-session books are eligible only before 08:30 IST, for CNC, with
    complete calendar coverage and one exact preceding trading date. All
    quantity, execution and protection proofs remain mandatory downstream.
    """
    from src.engine.unified_exit_reconciliation import _exchange_time
    from src.engine.market import load_holidays_verified
    current = datetime.fromtimestamp(now, _IST)
    dates = set()
    if not isinstance(trade_book, list) or getattr(trade_book, "ok", True) is not True:
        raise SignedCNCDeliveryUnverified("Signed session tradebook unavailable")
    for row in trade_book:
        if not isinstance(row, dict) or not str(row.get("securityId") or "").strip():
            raise SignedCNCDeliveryUnverified("Signed session trade identity unavailable")
        if str(row["securityId"]).strip() != security:
            continue
        if (str(row.get("dhanClientId") or "").strip() != account
                or row.get("exchangeSegment") != segment or row.get("productType") != product):
            raise SignedCNCDeliveryUnverified("Signed session account or contract differs")
        try:
            at = _exchange_time(row.get("exchangeTime"))
            if at > now:
                raise ValueError("Future execution")
            dates.add(datetime.fromtimestamp(at, _IST).date())
        except (ValueError, TypeError, OverflowError, OSError) as exc:
            raise SignedCNCDeliveryUnverified("Signed session execution date unavailable") from exc
    if dates == {current.date()}:
        return current.date()
    if (product != "CNC" or current.time() >= clock_time(8, 30) or len(dates) != 1):
        raise SignedCNCDeliveryUnverified("Signed session date is stale or ambiguous")
    day = current.date() - timedelta(days=1)
    for _ in range(14):
        ok, holidays, _ = load_holidays_verified(required_year=day.year)
        if not ok:
            raise SignedCNCDeliveryUnverified("Previous trading session calendar unverified")
        if day.weekday() < 5 and day not in holidays:
            if dates == {day}:
                return day
            break
        day -= timedelta(days=1)
    raise SignedCNCDeliveryUnverified("Not the immediately preceding trading session")


def closed_sibling_live_rows_match(*, closed, trade_book, account, security, segment, product, sells_only=False):
    """Exact candidate allowlist match, including exchange execution day."""
    from src.engine.unified_exit_reconciliation import _exchange_time
    seen = set()
    try:
        if not isinstance(trade_book, list):
            return False
        for row in trade_book:
            if not isinstance(row, dict) or not str(row.get("securityId") or "").strip():
                return False
            if str(row["securityId"]).strip() != security:
                continue
            if sells_only and row.get("transactionType") == "BUY":
                continue
            key = (str(row.get("orderId") or "").strip(), str(row.get("exchangeTradeId") or "").strip())
            proof = closed["trades"].get(key)
            if (not proof or key in seen or row.get("dhanClientId") != account
                    or row.get("exchangeSegment") != segment or row.get("productType") != product
                    or row.get("transactionType") != proof[0]
                    or _positive_int(row.get("tradedQuantity")) != proof[1]
                    or abs(_price(row.get("tradedPrice")) - proof[2]) >= 1e-6
                    or datetime.fromtimestamp(_exchange_time(row.get("exchangeTime")), _IST).date()
                    != datetime.fromtimestamp(proof[3], _IST).date()):
                return False
            seen.add(key)
        return True
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError):
        return False


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
            try:
                sibling_net = _quantity(position.get("netQty"), signed=True)
            except ValueError as exc:
                raise SignedCNCDeliveryUnverified("Other same-security quantity unverified") from exc
            if sibling_net != 0:
                raise SignedCNCDeliveryUnverified("Other same-security position requires attribution")
            continue
        matching.append(position)
    if len(matching) != 1:
        raise SignedCNCDeliveryUnverified("Signed position contract ambiguous")
    raw_net = matching[0].get("netQty")
    if isinstance(raw_net, (bool, float)) or not re.fullmatch(r"-[1-9][0-9]*", str(raw_net)):
        raise SignedCNCDeliveryUnverified("Signed CNC netQty malformed")
    net_day = int(raw_net)
    try:
        validate_signed_position_components(matching[0], net_day, require_day_totals=True)
    except ValueError as exc:
        raise SignedCNCDeliveryUnverified("Reported day or carry-forward quantities invalid") from exc
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

    session_date = signed_delivery_session_date(
        trade_book=trade_book, account=account, security=security,
        segment=segment, product=product, now=now,
    )
    # This strict journal routine verifies closed parent lifecycle, original
    # BUY intent/fills, OCO group, exact child leg IDs and full SELL allocations.
    try:
        closed = db.get_closed_forever_sibling_evidence(
            account, security, segment, product,
            open_parent_ids=open_ids, now=now, session_date=session_date,
        )
    except (AttributeError, ValueError, TypeError, KeyError) as exc:
        raise SignedCNCDeliveryUnverified("Closed sibling ownership unavailable") from exc
    closed_trades = closed.get("trades") or {}
    seen: set[str] = set()
    today_buy = today_sell = 0
    today = session_date
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
    if session_date != datetime.fromtimestamp(now, _IST).date():
        # Overnight exception is closed-sibling-only, not a replay of a
        # previous day's partial exit or an unallocated/current BUY.
        if not closed_sibling_live_rows_match(
            closed=closed, trade_book=trade_book, account=account,
            security=security, segment=segment, product=product,
        ):
            raise SignedCNCDeliveryUnverified("Overnight execution is not a verified closed sibling")
    if not seen or today_sell <= 0 or today_buy - today_sell != net_day:
        raise SignedCNCDeliveryUnverified("Day BUY minus SELL does not match signed CNC netQty")
    try:
        validate_signed_position_components(matching[0], net_day, today_buy, today_sell,
                                            require_day_totals=True)
    except ValueError as exc:
        raise SignedCNCDeliveryUnverified("Reported day totals do not match verified executions") from exc
    return expected_open_qty
