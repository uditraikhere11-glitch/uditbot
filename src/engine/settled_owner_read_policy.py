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


_PROTECTED_UNSAFE = (
    "pending_oco", "orphaned_legs_detected",
    "orphaned_legs_manual_intervention_required", "orphaned_leg_cancel_states",
    "external_unlinked_sell_observed", "uncommitted_broker_sell_trades",
    "broker_exit_already_executed", "unverified_trade_detected",
    "persistence_uncertain", "cancel_uncertain", "cancel_requested",
    "retry_reserved", "entry_timeout_elapsed", "manual_exit_detected",
    "ambiguous_super_closed", "triggered_leg", "reconciliation_required",
)


def durably_identified_prior_day_protected_buy(*, record, journal_db, account_id, now):
    """Recognize an obsolete parent BUY, not valid protection or gate proof.

    Only use this result to avoid retrying a prior-day BUY-only endpoint after
    incomplete broker history. A caller MUST block entry and still check all
    actual exit/protection legs whenever full attestation is missing.
    """
    try:
        oid = str(record.get("order_id") or "").strip()
        sec = str(record.get("security_id") or "").strip()
        seg = str(record.get("segment") or "").strip().upper()
        prod = str(record.get("product_type") or "").strip().upper()
        account = str(account_id or "").strip()
        qty = int(record.get("qty") or record.get("requested_qty") or record.get("filled_qty") or 0)
        filled = int(record.get("filled_qty") or 0)
        if (not oid or not sec or not account or prod not in ("CNC", "MTF")
                or str(record.get("state") or "").upper() != "TRADED"
                or str(record.get("position_state") or "").upper() != "PROTECTED"
                or str(record.get("protection_route") or "").upper() != "FOREVER_OCO"
                or qty <= 0 or filled != qty
                or int(record.get("exit_filled_qty") or 0) != 0
                or int(record.get("protected_qty") or 0) < qty
                or not journal_db or str(getattr(journal_db, "account_id", "") or "") != account):
            return False
        created, fills = journal_db.owner_lot_basis(account, sec, seg, prod, oid)
        return bool(
            datetime.fromtimestamp(float(created), IST).date()
            < datetime.fromtimestamp(float(now), IST).date()
            and fills and sum(int(f["traded_quantity"]) for f in fills) == qty
        )
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError,
            OSError, sqlite3.Error):
        return False


def may_omit_protected_prior_day_buy_trade_read(
    *, record, all_open_records, leg_order_ids, journal_db, gate_books,
    current_trades, current_positions, current_holdings, current_forever,
    account_id, now, session_proof=None, rejection=None,
):
    """Skip ONLY a historic, fully filled original BUY -- never OCO/exit legs.

    A serialized gate recheck requires a fresh historical execution attestation.
    A routine watch can reuse only this process's successful attestation for
    the current IST date, while all current broker exposure and OCO evidence
    must independently remain complete. This function cannot open the gate.
    """
    def deny(code):
        if rejection is not None:
            rejection["reason"] = code
        return False

    try:
        from src.broker.dhan_client import verify_forever_oco_group
        oid = str(record.get("order_id") or "").strip()
        sec = str(record.get("security_id") or "").strip()
        seg = str(record.get("segment") or "").strip().upper()
        prod = str(record.get("product_type") or "").strip().upper()
        account = str(account_id or "").strip()
        qty = int(record.get("qty") or record.get("requested_qty") or record.get("filled_qty") or 0)
        filled = int(record.get("filled_qty") or 0)
        if (not oid or not sec or not account or prod not in ("CNC", "MTF")
                or str(record.get("state") or "").upper() != "TRADED"
                or str(record.get("position_state") or "").upper() != "PROTECTED"
                or str(record.get("protection_route") or "").upper() != "FOREVER_OCO"
                or qty <= 0 or filled != qty
                or int(record.get("exit_filled_qty") or 0) != 0
                or int(record.get("protected_qty") or 0) < qty
                or oid not in leg_order_ids
                or any(record.get(k) for k in _PROTECTED_UNSAFE)):
            return deny("WATCHER_STATE_NOT_ELIGIBLE")
        if not journal_db or str(getattr(journal_db, "account_id", "") or "") != account:
            return deny("JOURNAL_ACCOUNT_MISMATCH")
        if not all(isinstance(book, list) for book in
                   (current_trades, current_positions, current_holdings, current_forever)):
            return deny("CURRENT_BROKER_BOOK_UNAVAILABLE")
        # Protect against a mistaken session identity or stale/partial book.
        for trade in current_trades:
            if not isinstance(trade, dict) or not str(trade.get("securityId") or "").strip():
                return deny("CURRENT_TRADE_IDENTITY_INCOMPLETE")
            if str(trade.get("securityId")) == sec:
                return deny("CURRENT_SAME_SECURITY_EXECUTION")
        first_at, fills = journal_db.owner_lot_basis(account, sec, seg, prod, oid)
        today = datetime.fromtimestamp(float(now), IST).date()
        if datetime.fromtimestamp(float(first_at), IST).date() >= today:
            return deny("ORIGINAL_BUY_NOT_PRIOR_DAY")
        if (not fills or sum(int(f["traded_quantity"]) for f in fills) != qty):
            return deny("VERIFIED_BUY_QUANTITY_MISMATCH")
        proof = (account, sec, qty)
        fresh = isinstance(gate_books, dict) and (
            (gate_books.get("protected_verified") or {}).get(oid) == proof
            and not (gate_books.get("errors") or {}).get(oid)
        )
        if gate_books is not None:
            if not fresh:
                return deny("FRESH_GATE_HISTORY_ATTESTATION_MISSING")
        elif session_proof != (today.isoformat(), *proof):
            return deny("PROCESS_SCOPED_HISTORY_ATTESTATION_MISSING")
        aggregate = sum(
            max(0, int(other.get("filled_qty") or 0)
                - int(other.get("exit_filled_qty") or 0))
            for other in all_open_records
            if (str(other.get("security_id") or "") == sec
                and str(other.get("segment") or "").upper() == seg
                and str(other.get("product_type") or "").upper() == prod)
        )
        observed, status = observed_open_quantity(
            current_positions, current_holdings, security_id=sec,
            segment=seg, product=prod, account_id=account,
        )
        if status != "VERIFIED_SETTLED_HOLDINGS" or observed != aggregate or aggregate < qty:
            return deny("SETTLED_HOLDINGS_NOT_VERIFIED")
        candidate_forever_ids = {str(x).strip() for x in (leg_order_ids or ()) if str(x).strip()}
        for rec_key in ("forever_order_id", "oco_order_id"):
            val = str(record.get(rec_key) or "").strip()
            if val and not val.startswith("SUPER-"):
                candidate_forever_ids.add(val)
        for cid_p in (record.get("confirmed_protection_ids") or ()):
            c_val = str(cid_p or "").strip()
            if c_val and not c_val.startswith("SUPER-"):
                candidate_forever_ids.add(c_val)
        if hasattr(journal_db, "get_protection_order_ids_for_parent"):
            for db_fid in journal_db.get_protection_order_ids_for_parent(oid):
                if db_fid and not db_fid.startswith("SUPER-"):
                    candidate_forever_ids.add(db_fid)
        group_ids = {
            str(r.get("orderId") or "").strip()
            for r in current_forever if isinstance(r, dict)
            and (str(r.get("orderId") or "").strip() in candidate_forever_ids
                 or (record.get("correlation_id") and
                     str(r.get("correlationId") or "").strip() == str(record["correlation_id"]).strip()))
        } - {""}
        if not group_ids:
            return deny("LINKED_FOREVER_OCO_GROUP_MISSING")
        for gid in group_ids:
            rows = [r for r in current_forever if isinstance(r, dict)
                    and str(r.get("orderId") or "") == gid]
            verified = verify_forever_oco_group(
                matching_rows=rows, forever_order_id=gid,
                expected_security_id=sec, expected_qty=qty,
                expected_account_id=account, expected_segment=seg,
                expected_product_type=prod,
            )
            if verified.is_verified and gid != oid:
                if rejection is not None:
                    rejection["reason"] = "VERIFIED"
                return True
        return deny("LINKED_FOREVER_OCO_GROUP_UNVERIFIED")
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError,
            OSError, sqlite3.Error) as exc:
        return deny("POLICY_ERROR_" + type(exc).__name__.upper())
