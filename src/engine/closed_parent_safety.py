"""Read-only broker-authoritative proof of a closed CNC/MTF parent.

This is a service-control safety verifier, not a lifecycle repair action.
It reuses the strict signed-delivery reconciliation predicate for T+0 exits.
"""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import math
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from src.engine.unified_exit_reconciliation import (
    ReconciliationEvidenceError, verified_flat_delivery_day,
)
from src.journal.db import JournalDB

_FINISHED = {"TRADED", "CLOSED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED"}

class _ReadOnlyJournal(JournalDB):
    def __init__(self, db_path: str, account_id: str):
        self.db_path = db_path
        self.account_id = account_id

    @contextmanager
    def _get_conn(self):
        raw_path = Path(self.db_path)
        if raw_path.is_symlink():
            raise OSError("Refusing symlinked journal")
        path = raw_path.resolve()
        if not path.is_file():
            raise OSError("Missing journal")
        conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            yield conn
        finally:
            conn.close()

def verified_closed_delivery_parent(
    *, db_path: str, account: str, parent_id: str, required_qty: int,
    security_id: str, broker: Any, orders: list, forever: list,
    super_orders: list,
) -> bool:
    """Only approve a verified sale that leaves no broker exposure or exit risk."""
    if not broker or not account or not parent_id or not security_id:
        return False
    if type(required_qty) is not int or required_qty <= 0:
        return False
    if not all(isinstance(v, list) and all(isinstance(row, dict) for row in v)
               and getattr(v, "ok", True) for v in (orders, forever, super_orders)):
        return False
    try:
        db = _ReadOnlyJournal(db_path, account_id=account)
        life = db.get_position_lifecycle(parent_id)
        if not isinstance(life, dict):
            return False
        segment = str(life.get("exchange_segment") or "").upper()
        product = str(life.get("product_type") or "").upper()
        if segment not in ("NSE_EQ", "BSE_EQ") or product not in ("CNC", "MTF"):
            return False
        if (life.get("position_state") != "POSITION_CLOSED"
                or str(life.get("account_id") or "") != account
                or str(life.get("security_id") or "") != security_id):
            return False
        accounting = db.get_position_accounting(parent_id)
        if (accounting["buy_filled_qty"] != required_qty
                or accounting["sell_filled_qty"] != required_qty
                or accounting["confirmed_open_qty"] != 0
                or life["confirmed_open_qty"] != 0
                or life["buy_filled_qty"] != required_qty
                or life["sell_filled_qty"] != required_qty):
            return False
        sells = db.get_sell_fills_for_parent(parent_id)
        if not sells or sum(int(s.get("traded_quantity") or 0) for s in sells) != required_qty:
            return False
        for sell in sells:
            if (str(sell.get("parent_order_id") or "") != parent_id
                    or not sell.get("is_verified_trade")
                    or str(sell.get("transaction_type") or "").upper() != "SELL"
                    or str(sell.get("security_id") or "") != security_id
                    or str(sell.get("exchange_segment") or "").upper() != segment
                    or str(sell.get("product_type") or "").upper() != product
                    or not str(sell.get("exchange_trade_id") or "")
                    or not str(sell.get("broker_order_id") or "")
                    or not math.isfinite(float(sell.get("traded_price") or 0))
                    or float(sell.get("traded_price") or 0) <= 0):
                return False
        with db._get_conn() as conn:
            buys = conn.execute(
                "SELECT * FROM fills WHERE broker_order_id=? AND "
                "UPPER(transaction_type)='BUY' AND is_verified_trade=1", (parent_id,)
            ).fetchall()
            if (not buys or sum(int(b["traded_quantity"] or 0) for b in buys) != required_qty
                    or any(str(b["security_id"] or "") != security_id
                           or str(b["exchange_segment"] or "").upper() != segment
                           or str(b["product_type"] or "").upper() != product
                           or str(b["parent_order_id"] or "") != parent_id
                           for b in buys)):
                return False
            protected = conn.execute(
                "SELECT forever_order_id,status FROM protection_orders WHERE parent_order_id=?",
                (parent_id,),
            ).fetchall()
        if (len(protected) != 1
                or str(protected[0]["status"] or "").upper() != "POSITION_CLOSED_LOCAL"):
            return False
        group_id = str(protected[0]["forever_order_id"] or "")
        if not group_id:
            return False
        if any(str(p.get("security_id") or "") == security_id
               for p in db.get_open_positions_lifecycle()):
            return False

        positions = broker.get_positions()
        holdings = broker.get_holdings()
        trades = broker.get_trade_book()
        if not all(isinstance(v, list) and all(isinstance(r, dict) for r in v)
                   and getattr(v, "ok", True) for v in (positions, holdings, trades)):
            return False
        for rows in (positions, trades, orders, forever, super_orders):
            for row in rows:
                if str(row.get("securityId") or "") != security_id:
                    continue
                if str(row.get("dhanClientId") or "") not in ("", account):
                    return False
                if str(row.get("exchangeSegment") or "").upper() != segment:
                    return False
                if str(row.get("productType") or "").upper() != product:
                    return False
        for row in orders + super_orders:
            if (str(row.get("securityId") or "") == security_id
                    and str(row.get("orderStatus") or "").upper() not in _FINISHED):
                return False
        for row in forever:
            if str(row.get("securityId") or "") != security_id:
                continue
            if (str(row.get("orderId") or "") != group_id
                    or str(row.get("orderStatus") or "").upper() not in
                    ("TRIGGERED", "TRADED", "CLOSED", "CANCELLED", "CANCELED")):
                return False
        negative = [p for p in positions if str(p.get("securityId") or "") == security_id
                    and type(p.get("netQty")) is int and p["netQty"] < 0]
        if negative:
            if len(negative) != 1:
                return False
            books = {"get_order_book": orders,
                     "get_forever_orders": forever, "get_super_orders": super_orders}
            def read_required(method: str, _label: str):
                return True, books[method], "OK"
            return verified_flat_delivery_day(
                negative[0], positions, holdings, trades, db, account, read_required
            ) is True

        # A same-day SELL cannot be considered settled just because today's
        # broker trade book is unexpectedly empty.
        today_ist = datetime.now(ZoneInfo('Asia/Kolkata')).date()
        if any(datetime.fromtimestamp(float(sell.get('traded_at') or 0), ZoneInfo('Asia/Kolkata')).date() >= today_ist
               for sell in sells):
            return False
        # A later delivery session cannot use today's trade book to corroborate
        # the historical SELL. Require verified journal proof and fresh zero risk.
        if any(str(t.get("securityId") or "") == security_id for t in trades):
            return False
        if any(str(p.get("securityId") or "") == security_id and
               (type(p.get("netQty")) is not int or p["netQty"] != 0)
               for p in positions):
            return False
        for holding in holdings:
            if str(holding.get("securityId") or "") != security_id:
                continue
            if any(holding.get(field) is None or isinstance(holding[field], bool)
                   or float(holding[field]) != 0 for field in
                   ("totalQty", "availableQty")):
                return False
        return True
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError,
            ArithmeticError, ReconciliationEvidenceError):
        return False
