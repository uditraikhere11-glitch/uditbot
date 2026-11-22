"""Safe omission of an obsolete daily BUY-order trade read.

This is only a read-admission policy, not permission to recover a lifecycle,
open the entry gate, or infer an exit. Full owner history and durable readback
are verified elsewhere in the serialized startup reconciliation gate.
"""
from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Any, Mapping

from src.engine.holding_quantity import observed_open_quantity
from src.engine.market import IST

_UNSAFE = (
    "sl_order_id", "tp_order_id", "exit_order_id", "confirmed_protection_ids",
    "verified_leg_order_ids", "verified_leg_map", "pending_oco",
    "orphaned_legs_detected", "orphaned_legs_manual_intervention_required",
    "orphaned_leg_cancel_states", "external_unlinked_sell_observed",
    "uncommitted_broker_sell_trades", "broker_exit_already_executed",
    "unverified_trade_detected", "persistence_uncertain", "cancel_uncertain",
    "cancel_requested", "retry_reserved", "entry_timeout_elapsed", "protected_qty",
)

def may_omit_prior_day_buy_trade_read(
    *,
    record: Mapping[str, Any],
    all_open_records: list[dict],
    leg_order_ids: set[str],
    journal_db: Any,
    gate_books: dict | None,
    current_trades: list[dict] | None,
    current_positions: list[dict] | None,
    current_holdings: list[dict] | None,
    account_id: str,
    now: float,
) -> bool:
    """Advisory policy: omit only an original settled BUY's daily trade GET.

    This predicate never approves a lifecycle transition or reopens the gate.
    Active SELL/SL/OCO reads, missing history, and unknown books remain strict.
    """
    try:
        oid = str(record.get("order_id") or "").strip()
        sec = str(record.get("security_id") or "").strip()
        seg = str(record.get("segment") or "").strip().upper()
        prod = str(record.get("product_type") or "").strip().upper()
        state = str(record.get("position_state") or "").strip().upper()
        account = str(account_id or "").strip()
        qty = int(record.get("filled_qty") or 0)
        closed = int(record.get("exit_filled_qty") or 0)
        if (not oid or not sec or not account or prod not in ("CNC", "MTF")
                or state not in ("UNPROTECTED_MANUAL_RISK", "RECONCILIATION_REQUIRED")
                or qty <= 0 or closed != 0 or leg_order_ids != {oid}
                or any(record.get(k) for k in _UNSAFE)):
            return False
        if not all(isinstance(book, list) for book in
                   (current_trades, current_positions, current_holdings)):
            return False
        holding_isins = {
            str(h.get("isin") or "").strip().upper()
            for h in current_holdings
            if isinstance(h, dict) and str(h.get("securityId") or "").strip() == sec
        } - {""}
        for trade in current_trades:
            if not isinstance(trade, dict) or not str(trade.get("securityId") or "").strip():
                return False
            if str(trade.get("securityId")) == sec:
                return False
            trade_isin = str(trade.get("isin") or "").strip().upper()
            if trade_isin and trade_isin in holding_isins:
                return False
            if (str(trade.get("exchangeSegment") or "").upper() in ("NSE_EQ", "BSE_EQ")
                    and str(trade.get("exchangeSegment") or "").upper() != seg
                    and not trade_isin):
                return False
        if not journal_db or not hasattr(journal_db, "owner_lot_basis"):
            return False
        if journal_db.is_manual_risk_acknowledged(
            oid, str(record.get("correlation_id") or oid),
            expected_account_id=account, expected_security_id=sec, required_qty=qty
        ) is not True:
            return False
        first_at, fills = journal_db.owner_lot_basis(account, sec, seg, prod, oid)
        today = datetime.fromtimestamp(float(now), IST).date()
        if (datetime.fromtimestamp(float(first_at), IST).date() >= today
                or not fills or sum(int(f["traded_quantity"]) for f in fills) != qty):
            return False
        # A transition OUT of RECONCILIATION_REQUIRED must never trust a
        # previously cached owner_history_day. Only this serialized gate pass
        # can furnish fresh, complete, account-bound historical BUY/SELL proof.
        proof_in_gate = (isinstance(gate_books, dict)
                         and (gate_books.get("verified") or {}).get(oid)
                         == (account, sec, qty))
        routine_proof_today = (
            state == "UNPROTECTED_MANUAL_RISK"
            and not record.get("reconciliation_required")
            and not record.get("unreadable_broker_response")
            and not record.get("manual_exit_detected")
            and record.get("owner_history_day") == today.isoformat()
        )
        if not (proof_in_gate or routine_proof_today):
            return False
        aggregate = 0
        for other in all_open_records:
            if (str(other.get("security_id") or "") == sec
                    and str(other.get("segment") or "").upper() == seg
                    and str(other.get("product_type") or "").upper() == prod):
                aggregate += max(0, int(other.get("filled_qty") or 0)
                                 - int(other.get("exit_filled_qty") or 0))
        owned, evidence = observed_open_quantity(
            current_positions, current_holdings, security_id=sec,
            segment=seg, product=prod, account_id=account,
        )
        return evidence == "VERIFIED_SETTLED_HOLDINGS" and owned == aggregate and aggregate >= qty
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError, OSError, sqlite3.Error):
        return False
