"""Strict, read-only broker-vs-journal evidence assessor for Phase 2.

No Dhan REST calls, writes, mutation authority, automatic order replay,
protection cancellation, STOP changes, or entry-gate release. The existing
StartupReconciliationGate remains the ONLY live reconciliation authority.

Evidence from an orderbook is not itself evidence of a fill. Actual fills
require unique exchangeTradeId records from the authenticated tradebook.
The assessor never claims a position is protected or free of other exposure.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence
import math
import re

_INT=re.compile(r"[0-9]+\Z")
_TERMINAL={"TRADED","REJECTED","CANCELLED","EXPIRED"}
_STATES=_TERMINAL|{"TRANSIT","PENDING","PART_TRADED","TRIGGERED","OPEN"}
_BAD_ID={"","0","none","null","nan"}


@dataclass(frozen=True)
class OrderRecoveryEvidence:
    classification: str
    reason_code: str
    broker_order_found: bool
    tradebook_executed_qty: int
    broker_reported_filled_qty: int | None
    expected_qty: int
    requires_protection_verification: bool
    requires_broker_review: bool
    may_resubmit: bool=False
    may_open_gate: bool=False
    may_claim_protection: bool=False


def _fail(code: str, expected: int=0, *, found: bool=False,
          executed: int=0, broker_filled: int | None=None) -> OrderRecoveryEvidence:
    return OrderRecoveryEvidence(
        "UNVERIFIED", code, found, executed, broker_filled, expected,
        executed>0, True,
    )


def _str(row: Mapping, key: str) -> str:
    return str(row.get(key) or "").strip()


def _qty(value: Any, *, positive=False) -> int:
    if type(value) is int:
        n=value
    elif isinstance(value,str) and _INT.fullmatch(value.strip()):
        n=int(value.strip())
    else:
        raise ValueError("integer quantity required")
    if n < (1 if positive else 0):
        raise ValueError("quantity out of range")
    return n


def _when_valid(value: Any) -> bool:
    if not isinstance(value,str) or not value.strip():
        return False
    try:
        d=datetime.fromisoformat(value.strip().replace("Z","+00:00"))
        return d.year>=2000
    except (ValueError, OverflowError):
        return False


def assess_order_recovery(
    intent: Mapping[str,Any],
    *,
    account_id: str,
    order_book: Sequence[Mapping[str,Any]] | None,
    trade_book: Sequence[Mapping[str,Any]] | None,
    books_complete: bool,
) -> OrderRecoveryEvidence:
    """Assess ONE durable BUY intent against complete authenticated broker books.

    Intended for a separately captured broker snapshot taken under a trusted
    broker identity. Missing/incomplete reads are never treated as empty books.
    No verification of protection legs or account-wide position netting occurs.
    """
    if not isinstance(intent,Mapping):
        return _fail("MALFORMED_JOURNAL_INTENT")
    try:
        quantity=_qty(intent.get("quantity"),positive=True)
    except ValueError:
        return _fail("INVALID_JOURNAL_QUANTITY")
    account=str(account_id or "").strip()
    corr=_str(intent,"correlation_id")
    journal_account=_str(intent,"account_id")
    sec=_str(intent,"security_id")
    segment=_str(intent,"exchange_segment").upper()
    product=_str(intent,"product_type").upper()
    broker_oid=_str(intent,"broker_order_id")
    if (not account or journal_account!=account or corr.lower() in _BAD_ID
            or not sec or not segment or not product):
        return _fail("JOURNAL_IDENTITY_UNVERIFIED",quantity)
    if books_complete is not True or not isinstance(order_book,(list,tuple)) or not isinstance(trade_book,(list,tuple)):
        return _fail("INCOMPLETE_BROKER_BOOKS",quantity)
    if any(not isinstance(x,Mapping) for x in order_book) or any(not isinstance(x,Mapping) for x in trade_book):
        return _fail("MALFORMED_BROKER_BOOK",quantity)

    # A known broker order ID must not silently fall back to a different
    # order with the same correlation ID.
    matching=[]
    for row in order_book:
        oid=_str(row,"orderId")
        cid=_str(row,"correlationId")
        if (broker_oid and oid==broker_oid) or cid==corr:
            matching.append(row)
    if len(matching)>1:
        return _fail("AMBIGUOUS_ORDERBOOK_OWNERSHIP",quantity)
    if not matching:
        # No row in current-day book is not proof the order was never
        # submitted; older entries may need historical broker lookup.
        return _fail("ORDER_NOT_IN_CURRENT_BOOK",quantity)
    row=matching[0]
    oid=_str(row,"orderId")
    cid=_str(row,"correlationId")
    if (oid.lower() in _BAD_ID or (broker_oid and oid!=broker_oid)
            or cid!=corr):
        return _fail("BROKER_ORDER_CORRELATION_CONFLICT",quantity,found=True)
    if (_str(row,"dhanClientId")!=account
            or _str(row,"securityId")!=sec
            or _str(row,"exchangeSegment").upper()!=segment
            or _str(row,"productType").upper()!=product
            or _str(row,"transactionType").upper()!="BUY"):
        return _fail("BROKER_ORDER_CONTRACT_CONFLICT",quantity,found=True)
    try:
        broker_qty=_qty(row.get("quantity"),positive=True)
        if broker_qty!=quantity:
            return _fail("BROKER_ORDER_QUANTITY_CONFLICT",quantity,found=True)
        state=_str(row,"orderStatus").upper()
        if state not in _STATES:
            return _fail("INVALID_BROKER_ORDER_STATE",quantity,found=True)
        # A rejected order may omit filledQty; this does not certify the
        # account's holdings or complete a protection lifecycle.
        fill_raw=row.get("filledQty")
        filled=0 if fill_raw is None and state=="REJECTED" else _qty(fill_raw)
        if filled>quantity:
            return _fail("BROKER_REPORTED_OVERFILL",quantity,found=True,broker_filled=filled)
    except ValueError:
        return _fail("MALFORMED_BROKER_QUANTITY",quantity,found=True)
    if state=="TRADED" and filled!=quantity:
        return _fail("TRADED_STATUS_INCOMPLETE",quantity,found=True,broker_filled=filled)
    if state=="PART_TRADED" and not 0<filled<quantity:
        return _fail("PARTIAL_STATUS_CONFLICT",quantity,found=True,broker_filled=filled)
    if state=="REJECTED" and filled:
        return _fail("REJECTED_WITH_EXECUTION",quantity,found=True,broker_filled=filled)

    unique={}
    for trade in trade_book:
        # A matching trade ID for a different order is an ambiguous identity;
        # duplicate executions cannot be grouped across broker orders.
        trade_oid=_str(trade,"orderId")
        trade_id=_str(trade,"exchangeTradeId")
        if trade_oid!=oid:
            continue
        if (trade_id.lower() in _BAD_ID or
                _str(trade,"dhanClientId")!=account or
                _str(trade,"securityId")!=sec or
                _str(trade,"exchangeSegment").upper()!=segment or
                _str(trade,"productType").upper()!=product or
                _str(trade,"transactionType").upper()!="BUY" or
                not _when_valid(trade.get("exchangeTime"))):
            return _fail("TRADE_IDENTITY_UNVERIFIED",quantity,found=True,broker_filled=filled)
        try:
            executed=_qty(trade.get("tradedQuantity"),positive=True)
            price=float(trade.get("tradedPrice"))
            if not math.isfinite(price) or price<=0:
                raise ValueError("invalid fill price")
        except (ValueError,TypeError,OverflowError):
            return _fail("MALFORMED_TRADE_EVIDENCE",quantity,found=True,broker_filled=filled)
        fingerprint=(executed,price)
        if trade_id in unique and unique[trade_id]!=fingerprint:
            return _fail("CONTRADICTORY_EXCHANGE_TRADE_ID",quantity,found=True,broker_filled=filled)
        unique[trade_id]=fingerprint
    executed=sum(x[0] for x in unique.values())
    if executed>quantity:
        return _fail("TRADEBOOK_OVERFILLED",quantity,found=True,executed=executed,broker_filled=filled)
    if executed!=filled:
        return _fail("BROKER_TRADEBOOK_FILL_DISAGREEMENT",quantity,found=True,executed=executed,broker_filled=filled)
    if state=="TRADED" and executed==quantity:
        return OrderRecoveryEvidence(
            "FILLED_AWAITING_PROTECTION","EXECUTION_VERIFIED_PROTECTION_NOT_PROVEN",
            True,executed,filled,quantity,True,True,
        )
    if executed:
        return OrderRecoveryEvidence(
            "PARTIAL_FILL_AWAITING_PROTECTION","PARTIAL_EXECUTION_REQUIRES_COVERAGE",
            True,executed,filled,quantity,True,True,
        )
    if state=="REJECTED":
        return OrderRecoveryEvidence(
            "REJECTED_WITH_NO_TRADE","BROKER_REJECTION_VERIFIED_NOT_RESUBMITTABLE",
            True,0,0,quantity,False,False,
        )
    if state in ("CANCELLED","EXPIRED"):
        return OrderRecoveryEvidence(
            "CLOSED_WITH_NO_TRADE","BROKER_TERMINAL_NO_TRADE",
            True,0,0,quantity,False,False,
        )
    return OrderRecoveryEvidence(
        "OPEN_UNFILLED","BROKER_ORDER_OUTSTANDING",
        True,0,0,quantity,False,True,
    )


def summarize_recovery_assessments(
    intents: Sequence[Mapping[str,Any]],
    *,
    account_id: str,
    order_book: Sequence[Mapping[str,Any]] | None,
    trade_book: Sequence[Mapping[str,Any]] | None,
    books_complete: bool,
) -> dict:
    """Anonymous recovery comparison; never displays client/order/symbol IDs."""
    if not isinstance(intents,(list,tuple)):
        raise TypeError("intent list required")
    classifications={}
    reasons={}
    protection_required=0
    unresolved=0
    for intent in intents:
        ev=assess_order_recovery(
            intent,account_id=account_id,order_book=order_book,
            trade_book=trade_book,books_complete=books_complete,
        )
        classifications[ev.classification]=classifications.get(ev.classification,0)+1
        reasons[ev.reason_code]=reasons.get(ev.reason_code,0)+1
        protection_required+=int(ev.requires_protection_verification)
        unresolved+=int(ev.requires_broker_review)
    return {
        "kind":"uditbot-phase2-recovery-assessment-v1",
        "intent_count":len(intents),
        "classifications":classifications,
        "reason_codes":reasons,
        "protection_review_count":protection_required,
        "unresolved_broker_review_count":unresolved,
        "may_open_new_entries":False,
        "may_resubmit_unknown_orders":False,
        "may_claim_verified_protection":False,
        "limitations":"Independent broker/journal diagnostic only; current-day book may omit historical fills; canonical gate unchanged.",
    }
