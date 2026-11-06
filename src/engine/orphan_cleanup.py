"""
Isolated, opt-in cleanup of BOT-OWNED orphaned Dhan Forever OCO protection.

No import by run.py or ProtectionWatcher; no background process is started by
importing this module. Read-only by default. Never infer orphanhood from a
single zero positions record: CNC exposure can live in /holdings.

CLI: python -m src.engine.orphan_cleanup       # preview only
     python -m src.engine.orphan_cleanup --execute  # explicit LIVE opt-in
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

from src.broker.dhan_client import BASE_URL, verify_forever_oco_group
from src.engine.reconciliation import validate_position_record
from src.models import TradingMode

logger = logging.getLogger(__name__)

ACTIVE_ORDER_STATES = frozenset({
    "PENDING", "OPEN", "TRANSIT", "CONFIRM", "ACTIVE", "PART_TRADED",
    "PART_FILLED", "TRIGGERED", "CANCEL_REQUESTED", "CANCEL_PENDING",
})
ACTIVE_JOURNAL_STATES = frozenset({
    "INTENT_CREATED", "SUBMITTING", "UNKNOWN", "ACKED", "PENDING",
    "TRANSIT", "TRIGGERED", "PART_FILLED", "PART_TRADED",
    "CANCEL_REQUESTED", "CANCEL_PENDING",
})
VERIFIED_JOURNAL_STATES = frozenset({"ACTIVE", "ACTIVE_CONFIRMED"})
_CLEAN_ID = re.compile(r"^[A-Za-z0-9_-]+$")
_NUMBER = re.compile(r"^\+?\d+$")


def _integer(value: Any) -> int:
    """Do not transform missing, malformed, negative or fractional qty to zero."""
    if isinstance(value, bool):
        raise ValueError("boolean quantity")
    if isinstance(value, int):
        qty = value
    elif isinstance(value, str) and _NUMBER.fullmatch(value.strip()):
        qty = int(value.strip())
    else:
        raise ValueError("malformed quantity")
    if qty < 0:
        raise ValueError("negative quantity")
    return qty


def _broker_list(value: Any, label: str) -> List[dict]:
    if getattr(value, "ok", True) is False:
        raise ValueError(f"{label}: {getattr(value, 'error', 'broker read failed')}")
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"{label}: expected a successful list of dicts")
    return list(value)


def _snapshot(broker: Any, account: str) -> Dict[str, List[dict]]:
    # All four reads must succeed. A missing holding or an API failure must
    # NEVER be converted to an empty account.
    positions = _broker_list(broker.get_positions(), "positions")
    forever = _broker_list(broker.get_forever_orders(), "forever")
    orders = _broker_list(broker.get_order_book(), "order_book")
    holdings_method = getattr(broker, "get_holdings", None)
    if callable(holdings_method):
        holdings = _broker_list(holdings_method(), "holdings")
    else:
        holdings = _broker_list(broker._read_collection("/holdings", "holdings"), "holdings")

    for pos in positions:
        good, _, msg = validate_position_record(pos, expected_account_id=account)
        if not good:
            raise ValueError(msg)
    for hold in holdings:
        if not str(hold.get("securityId") or "").strip() or not str(hold.get("tradingSymbol") or "").strip():
            raise ValueError("holdings: missing instrument identity")
        if hold.get("dhanClientId") not in (None, "", account):
            raise ValueError("holdings: account mismatch")
        for name in ("totalQty", "dpQty", "t1Qty", "availableQty", "collateralQty"):
            if name not in hold:
                raise ValueError(f"holdings: missing {name}")
            _integer(hold[name])
    for item in orders:
        state = str(item.get("orderStatus") or "").strip().upper()
        if not state:
            raise ValueError("order_book: missing orderStatus")
        if state in ACTIVE_ORDER_STATES and (
            not str(item.get("securityId") or "").strip()
            or not str(item.get("transactionType") or "").strip()
        ):
            raise ValueError("order_book: active order lacks identity or side")
    for item in forever:
        if not str(item.get("orderId") or "").strip() or not str(item.get("orderStatus") or "").strip():
            raise ValueError("forever: missing id/status")
    return {"positions": positions, "holdings": holdings, "orders": orders, "forever": forever}


def _journal_candidates(db_path: str) -> List[dict]:
    # SQLite URI mode=ro prevents accidental creation or updates to live journal.
    db_url = "file:" + quote(os.path.abspath(db_path)) + "?mode=ro"
    with sqlite3.connect(db_url, uri=True, timeout=5.0) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT a.*, p.status AS journal_protection_status
            FROM protection_attempts a
            JOIN protection_orders p
              ON p.parent_order_id = a.parent_order_id
             AND p.forever_order_id = a.broker_order_id
            WHERE a.route = 'FOREVER_OCO'
              AND a.state = 'CONFIRMED'
              AND p.status IN ('ACTIVE', 'ACTIVE_CONFIRMED')
            """
        ).fetchall()
        # Preserve unresolved entries. These can fill later even when the
        # initial snapshot shows no open position.
        unresolved = con.execute(
            "SELECT security_id, exchange_segment, product_type, state "
            "FROM order_intents WHERE state IN ({})".format(
                ",".join("?" for _ in ACTIVE_JOURNAL_STATES)
            ),
            tuple(sorted(ACTIVE_JOURNAL_STATES)),
        ).fetchall()
        unresolved_keys = {
            (str(row["security_id"]).strip(), str(row["exchange_segment"]).upper(),
             str(row["product_type"]).upper()) for row in unresolved
        }
        parents = {
            str(row["broker_order_id"]).strip()
            for row in con.execute("SELECT broker_order_id FROM order_intents WHERE broker_order_id IS NOT NULL")
        }
    candidates = [dict(r) for r in rows]
    return [r for r in candidates if (
        str(r.get("parent_order_id") or "") in parents
        and (str(r["security_id"]).strip(), str(r["exchange_segment"]).upper(),
             str(r["product_type"]).upper()) not in unresolved_keys
    )]


def _same_instrument(row: dict, candidate: dict) -> bool:
    sec = str(candidate["security_id"]).strip()
    sym = str(candidate.get("trading_symbol") or candidate.get("symbol") or "").strip().upper()
    return (str(row.get("securityId") or "").strip() == sec
            or bool(sym and str(row.get("tradingSymbol") or "").strip().upper() == sym))


def _eligible(snapshot: Dict[str, List[dict]], row: dict, account: str) -> Tuple[bool, str]:
    oid = str(row.get("broker_order_id") or "").strip()
    sec = str(row.get("security_id") or "").strip()
    seg = str(row.get("exchange_segment") or "").upper()
    prod = str(row.get("product_type") or "").upper()
    if not oid or not _CLEAN_ID.fullmatch(oid) or not sec or seg not in ("NSE_EQ", "BSE_EQ") or prod != "CNC":
        return False, "not a valid CNC Forever OCO identity"
    if not account or str(row.get("account_id") or "").strip() != account:
        return False, "missing/mismatched journal account"
    if not str(row.get("parent_order_id") or "").strip():
        return False, "missing parent order"

    for pos in snapshot["positions"]:
        if _same_instrument(pos, row) and _integer(pos["netQty"]) > 0:
            return False, "positive broker position"
    for hold in snapshot["holdings"]:
        if _same_instrument(hold, row):
            if any(_integer(hold[field]) > 0 for field in
                   ("totalQty", "dpQty", "t1Qty", "availableQty", "collateralQty")):
                return False, "shares still present in holdings/T1/collateral"

    for order in snapshot["orders"]:
        if _same_instrument(order, row) and str(order.get("orderStatus") or "").upper() in ACTIVE_ORDER_STATES:
            return False, "live broker order may change exposure"

    matched = [item for item in snapshot["forever"] if str(item.get("orderId") or "") == oid]
    if not matched:
        return False, "OCO absent from broker book"
    if any(str(item.get("dhanClientId") or "").strip() != account for item in matched):
        return False, "OCO broker account not verified"
    if any(str(item.get("transactionType") or "").upper() != "SELL" for item in matched):
        return False, "not SELL protection"
    verification = verify_forever_oco_group(
        matched, oid, expected_security_id=sec,
        expected_qty=_integer(row["quantity"]),
        expected_account_id=account,
        expected_segment=seg,
        expected_product_type=prod,
    )
    if not verification.is_verified:
        return False, "OCO verification: " + verification.reason
    return True, "bot-owned active OCO with zero verified exposure"


def _cancel_forever(broker: Any, order_id: str) -> Tuple[bool, str]:
    """Dedicated Forever endpoint. Never call cancel_order() for these IDs."""
    if not _CLEAN_ID.fullmatch(str(order_id or "")):
        return False, "invalid Forever ID"
    if broker.cfg.trading_mode != TradingMode.LIVE:
        return False, "TEST mode cannot send real cancellation"
    try:
        response = broker.session.delete(
            f"{BASE_URL}/forever/orders/{order_id}", headers=broker._headers(), timeout=5
        )
        if response.status_code not in (200, 202):
            return False, f"Forever cancel HTTP {response.status_code}"
        # An acknowledgement is not the same as final broker cancellation.
        return True, f"Forever cancel requested (HTTP {response.status_code}); verify broker book"
    except Exception as exc:
        return False, f"Forever cancel outcome UNKNOWN: {type(exc).__name__}: {exc}"


def clean_orphaned_forever_orders(
    broker: Any,
    db_path: str,
    *,
    execute: bool = False,
    sleep: Any = time.sleep,
    confirm_delay_sec: float = 2.0,
) -> Dict[str, Any]:
    """One bounded scan, no broad account cleanup, never modifies main bot data.

    In execute mode, require two successful independent broker snapshots and
    identical eligible OCO identity before submitting DELETE. On any failure
    return a BLOCKED report; do not treat missing data as zero.
    """
    result: Dict[str, Any] = {
        "mode": "EXECUTE" if execute else "PREVIEW", "eligible": [],
        "cancel_requested": [], "failed": [], "skipped": [], "error": "",
    }
    try:
        if execute and broker.cfg.trading_mode != TradingMode.LIVE:
            raise ValueError("LIVE account confirmation required; refusing TEST cancel")
        account = str(getattr(broker.cfg, "dhan_client_id", "") or "").strip()
        if not account:
            raise ValueError("missing configured Dhan account")
        first = _snapshot(broker, account)
        owned = _journal_candidates(db_path)
        # Any duplicate journal claim of the same Forever ID is ambiguous.
        unique: Dict[str, dict] = {}
        for row in owned:
            oid = str(row.get("broker_order_id") or "")
            if oid in unique:
                unique.pop(oid, None)
                result["skipped"].append({"id": oid, "reason": "ambiguous journal ownership"})
            elif not any(s["id"] == oid for s in result["skipped"]):
                unique[oid] = row

        initial = {}
        for oid, row in unique.items():
            good, reason = _eligible(first, row, account)
            if good:
                initial[oid] = row
                result["eligible"].append(oid)
            else:
                result["skipped"].append({"id": oid, "reason": reason})

        if not execute or not initial:
            return result

        if confirm_delay_sec < 1.0:
            raise ValueError("independent confirmation delay must be at least 1 second")
        sleep(confirm_delay_sec)
        second = _snapshot(broker, account)
        fresh_owned = {str(r["broker_order_id"]): r for r in _journal_candidates(db_path)}
        for oid, row in initial.items():
            if oid not in fresh_owned or fresh_owned[oid] != row:
                result["skipped"].append({"id": oid, "reason": "journal changed during confirmation"})
                continue
            good, reason = _eligible(second, row, account)
            if not good:
                result["skipped"].append({"id": oid, "reason": "second snapshot: " + reason})
                continue
            accepted, message = _cancel_forever(broker, oid)
            if accepted:
                logger.warning("[OCO CLEANUP] Forever cancel REQUESTED for %s (%s)", oid, row["symbol"])
                result["cancel_requested"].append(oid)
            else:
                logger.error("[OCO CLEANUP] Could not confirm cancel request for %s: %s", oid, message)
                result["failed"].append({"id": oid, "reason": message})
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("[OCO CLEANUP] Fail closed: %s", result["error"])
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Bot-owned Dhan Forever OCO cleanup; preview by default")
    parser.add_argument("--execute", action="store_true", help="Explicitly enable LIVE Forever OCO cancellation")
    args = parser.parse_args()
    from src.config import AppConfig
    from src.broker.dhan_client import DhanClient
    import json

    cfg = AppConfig.from_env()
    report = clean_orphaned_forever_orders(
        DhanClient(cfg), cfg.journal_db_path, execute=args.execute
    )
    print(json.dumps(report, indent=2))
    return 1 if report["error"] or report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
