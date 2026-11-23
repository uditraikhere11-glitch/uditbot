"""Broker-and-journal verification of a fully closed prior-day CNC lot.

Used only by service-control preflight, never to place/cancel trades or modify
SQLite. A successful local lifecycle label alone is insufficient.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import math
import sqlite3
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
FINISHED = {"TRADED", "CLOSED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED"}


def verified_service_closed_delivery_lot(
    *, db_path, parent_order_id, quantity, expected_security_id, account_id, broker,
    order_book, forever_book, super_book,
) -> bool:
    """Verify signed delivery accounting and persisted parent fill provenance.

    No writes and no broker mutations. Missing books, ambiguous attribution,
    active OCO siblings and unexplained trades fail closed.
    """
    try:
        return _verify(
            db_path, parent_order_id, quantity, expected_security_id, account_id,
            broker, order_book, forever_book, super_book,
        )
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, OverflowError):
        return False


def _verify(db_path, parent, required, expected_sec, account, broker, orders, forever, supers):
    if not (account and parent and expected_sec and broker and
            isinstance(orders, list) and isinstance(forever, list) and isinstance(supers, list)):
        return False
    if isinstance(required, bool) or int(required) <= 0:
        return False
    path = Path(db_path)
    if path.is_symlink() or not path.is_file():
        return False
    uri = path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=3) as con:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA query_only=ON")
        pos = con.execute(
            "SELECT * FROM positions_lifecycle WHERE parent_order_id=?", (parent,),
        ).fetchone()
        if pos is None or pos["account_id"] != account or pos["position_state"] != "POSITION_CLOSED":
            return False
        sec, seg, prod = (str(pos[k] or "").strip().upper()
                          for k in ("security_id", "exchange_segment", "product_type"))
        key = (sec, seg, prod)
        if sec != str(expected_sec).strip().upper() or seg not in ("NSE_EQ", "BSE_EQ") or prod != "CNC":
            return False
        if (int(pos["buy_filled_qty"]) != int(required)
                or int(pos["sell_filled_qty"]) != int(required)
                or int(pos["confirmed_open_qty"]) != 0
                or int(pos["manual_exit_detected"] or 0)
                or int(pos["orphaned_legs_detected"] or 0)):
            return False
        rows = con.execute(
            "SELECT * FROM fills WHERE parent_order_id=? OR broker_order_id=?",
            (parent, parent),
        ).fetchall()
        buys, sells = [], []
        eids = set()
        for f in rows:
            eid = str(f["exchange_trade_id"] or "").strip()
            oid = str(f["broker_order_id"] or "").strip()
            side = str(f["transaction_type"] or "").upper()
            if (not eid or eid == "0" or eid in eids or not oid
                    or int(f["is_verified_trade"] or 0) != 1
                    or (str(f["security_id"] or "").upper(),
                        str(f["exchange_segment"] or "").upper(),
                        str(f["product_type"] or "").upper()) != key):
                return False
            eids.add(eid)
            q, px, ts = f["traded_quantity"], f["traded_price"], f["traded_at"]
            if (isinstance(q, bool) or type(q) is not int or q <= 0
                    or not math.isfinite(float(px)) or float(px) <= 0
                    or not math.isfinite(float(ts)) or float(ts) <= 0):
                return False
            if side == "BUY" and oid == parent:
                buys.append(f)
            elif side == "SELL" and str(f["parent_order_id"] or "") == parent:
                sells.append(f)
            else:
                return False
        if (not buys or not sells or sum(r["traded_quantity"] for r in buys) != required
                or sum(r["traded_quantity"] for r in sells) != required):
            return False
        # Never treat a multi-lot allocation as a direct, single-lot exit.
        if con.execute(
            "SELECT 1 FROM exit_allocations WHERE parent_order_id=? OR exchange_trade_id IN "
            "(SELECT exchange_trade_id FROM fills WHERE parent_order_id=?) LIMIT 1",
            (parent, parent),
        ).fetchone():
            return False
        prot = con.execute(
            "SELECT forever_order_id, status FROM protection_orders WHERE parent_order_id=?",
            (parent,),
        ).fetchall()
        if len(prot) != 1 or str(prot[0]["status"] or "") != "POSITION_CLOSED_LOCAL":
            return False
        gid = str(prot[0]["forever_order_id"] or "").strip()
        if not gid:
            return False
        open_rows = con.execute(
            "SELECT account_id, confirmed_open_qty FROM positions_lifecycle "
            "WHERE security_id=? AND exchange_segment=? AND product_type=? "
            "AND position_state != 'POSITION_CLOSED'",
            key,
        ).fetchall()
        if any(row["account_id"] != account or int(row["confirmed_open_qty"]) < 0 for row in open_rows):
            return False
        expected_open = sum(int(row["confirmed_open_qty"]) for row in open_rows)

    def _book(name):
        result = getattr(broker, name)()
        if not isinstance(result, list) or getattr(result, "ok", True) is not True:
            raise ValueError("broker collection unreadable")
        return result
    positions, holdings, trades = (_book("get_positions"), _book("get_holdings"), _book("get_trade_book"))
    for book in (positions, holdings, trades, orders, forever, supers):
        if any(not isinstance(row, dict) for row in book):
            return False
    sec_orders = [x for x in orders if str(x.get("securityId") or "").upper() == sec]
    if any(str(x.get("orderStatus") or "").upper() not in FINISHED for x in sec_orders):
        return False
    sec_forever = [x for x in forever if str(x.get("securityId") or "").upper() == sec]
    if any(str(x.get("orderStatus") or "").upper() not in FINISHED | {"TRIGGERED"}
           for x in sec_forever):
        return False
    for row in sec_forever:
        if str(row.get("orderStatus") or "").upper() == "TRIGGERED":
            if (str(row.get("orderId") or "") != gid
                    or str(row.get("legName") or "") not in ("TARGET_LEG", "STOP_LOSS_LEG")
                    or str(row.get("dhanClientId") or "") != account
                    or (str(row.get("securityId") or "").upper(),
                        str(row.get("exchangeSegment") or "").upper(),
                        str(row.get("productType") or "").upper()) != key):
                return False
    if any(str(x.get("securityId") or "").upper() == sec and
           str(x.get("orderStatus") or "").upper() not in FINISHED for x in supers):
        return False

    matches = [r for r in positions if str(r.get("securityId") or "").upper() == sec]
    if len(matches) > 1:
        return False
    for r in matches:
        if (str(r.get("dhanClientId") or "") != account or
                (str(r.get("securityId") or "").upper(),
                 str(r.get("exchangeSegment") or "").upper(),
                 str(r.get("productType") or "").upper()) != key):
            return False
    net = 0 if not matches else matches[0].get("netQty")
    if isinstance(net, bool) or not str(net).lstrip("-").isdigit():
        return False
    net = int(net)
    same_holdings = [r for r in holdings if str(r.get("securityId") or "").upper() == sec]
    if len(same_holdings) > 1:
        return False
    if same_holdings:
        h = same_holdings[0]
        if h.get("dhanClientId") and str(h["dhanClientId"]) != account:
            return False
        total = h.get("totalQty")
        if isinstance(total, bool) or not str(total).isdigit():
            return False
        settled = int(total)
    else:
        settled = 0
    if settled + net != expected_open:
        return False

    today = datetime.now(IST).date()
    today_sell_eids = {str(f["exchange_trade_id"]): f for f in sells
                       if datetime.fromtimestamp(float(f["traded_at"]), IST).date() == today}
    sec_trades = [t for t in trades if str(t.get("securityId") or "").upper() == sec]
    if net < 0:
        if not same_holdings or int(same_holdings[0].get("availableQty") or 0) != 0:
            return False
        # Reuse the stricter shared trade parser and OCO child validator.
        from src.engine.unified_exit_reconciliation import (
            _broker_trade, validate_forever_oco_child_orders,
        )
        verified_qty_by_oid = {}
        signed_today = 0
        if not sec_trades or not today_sell_eids:
            return False
        for t in sec_trades:
            side = str(t.get("transactionType") or "").upper()
            if side not in ("BUY", "SELL"):
                return False
            proof = _broker_trade(t, account, key, side)
            if datetime.fromtimestamp(proof["when"], IST).date() != today:
                return False
            matching = next((f for f in rows if str(f["exchange_trade_id"]) == proof["eid"]), None)
            if (matching is None or matching["broker_order_id"] != proof["oid"]
                    or matching["traded_quantity"] != proof["qty"]
                    or float(matching["traded_price"]) != proof["price"]):
                return False
            signed_today += proof["qty"] if side == "BUY" else -proof["qty"]
            if side == "SELL":
                if proof["eid"] not in today_sell_eids:
                    return False
                verified_qty_by_oid[proof["oid"]] = verified_qty_by_oid.get(proof["oid"], 0) + proof["qty"]
        if signed_today != net or sum(verified_qty_by_oid.values()) != -net:
            return False
        linked_children = validate_forever_oco_child_orders(
            forever_order_id=gid, expected_leg_qty=sum(f["traded_quantity"] for f in sells),
            key=key, account=account, orders=orders,
            sell_fill_qty_by_oid=verified_qty_by_oid,
        )
        if set(verified_qty_by_oid) != linked_children:
            return False
        return True

    # Following-session case: no signed-negative inventory, no today's executions
    # on this contract; the canonical, verified historical SELL remains durable.
    return net >= 0 and not sec_trades and settled + net == expected_open
