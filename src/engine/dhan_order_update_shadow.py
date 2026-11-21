"""Strict, offline normalization of DhanHQ v2 Live Order Update messages.

The broker WebSocket supplies cumulative TradedQty, not a unique exchange
trade identifier. Therefore every normalized message is a STATUS observation,
never a certified trade execution. It cannot open a gate, place a trade, close a
position, or verify OCO protection. Unknown identities fail closed.
Reference: https://dhanhq.co/docs/v2/order-update/
"""
from __future__ import annotations
from hashlib import sha256
import json
import re
from typing import Any
from src.engine.shadow_execution_events import BrokerEvent


class DhanUpdateRejected(ValueError):
    """Malformed, unsupported or untrusted Dhan order-update evidence."""


_SEGMENTS={
    ("NSE","E"):"NSE_EQ",
    ("BSE","E"):"BSE_EQ",
    ("NSE","D"):"NSE_FNO",
    ("BSE","D"):"BSE_FNO",
}
_PRODUCT_CODES={"C":"CNC","I":"INTRADAY","M":"MARGIN","F":"MTF"}
_STATUSES={"TRANSIT":"PENDING","PENDING":"PENDING","CONFIRMED":"OPEN",
           "OPEN":"OPEN","PART_TRADED":"PART_TRADED",
           "TRADED":"TRADED","CANCELLED":"CANCELLED",
           "REJECTED":"REJECTED","EXPIRED":"EXPIRED"}


def _integer(value: Any, label: str) -> int:
    if type(value) is int:
        result=value
    elif isinstance(value,str) and re.fullmatch(r"[0-9]+",value.strip()):
        result=int(value.strip())
    else:
        raise DhanUpdateRejected(f"{label} is missing or nonintegral")
    if result < 0:
        raise DhanUpdateRejected(f"{label} negative")
    return result


def normalize_dhan_order_update(raw: dict, *, expected_account_id: str) -> BrokerEvent:
    """Pure parser: **NOT** proof the order belongs to UditBot.

    Caller MUST independently match the broker order ID to a durable, bot-owned
    journal intent before any optional shadow projection. Raw updates cannot
    create executable order intents, fills or positions.
    """
    if not isinstance(raw,dict) or str(raw.get("Type") or "") != "order_alert":
        raise DhanUpdateRejected("not a Dhan order_alert event")
    data=raw.get("Data")
    if not isinstance(data,dict):
        raise DhanUpdateRejected("missing Data object")
    account=str(data.get("ClientId") or "").strip()
    if not expected_account_id or account!=str(expected_account_id).strip():
        raise DhanUpdateRejected("broker account identity mismatch")
    oid=str(data.get("OrderNo") or "").strip()
    sec=str(data.get("SecurityId") or "").strip()
    if not oid or not sec or not re.fullmatch(r"[0-9A-Za-z_-]+",oid):
        raise DhanUpdateRejected("missing/invalid order or security identity")
    exchange=str(data.get("Exchange") or "").strip().upper()
    segment=str(data.get("Segment") or "").strip().upper()
    mapped_segment=_SEGMENTS.get((exchange,segment))
    if not mapped_segment:
        raise DhanUpdateRejected("unsupported exchange/segment combination")
    code=str(data.get("Product") or "").strip().upper()
    product=_PRODUCT_CODES.get(code)
    named=str(data.get("ProductName") or "").strip().upper()
    if not product or (named and named!=product):
        raise DhanUpdateRejected("unsupported or contradictory product type")
    broker_status=str(data.get("Status") or "").strip().upper()
    status=_STATUSES.get(broker_status)
    if not status:
        raise DhanUpdateRejected("unsupported broker order status")
    requested=_integer(data.get("Quantity"),"Quantity")
    traded=_integer(data.get("TradedQty"),"TradedQty")
    if requested<=0 or traded>requested:
        raise DhanUpdateRejected("cumulative trade quantity exceeds ordered quantity")
    if data.get("RemainingQuantity") is not None:
        remaining=_integer(data["RemainingQuantity"],"RemainingQuantity")
        if remaining>requested or remaining+traded>requested:
            raise DhanUpdateRejected("contradictory remaining/cumulative quantity")
    # Event IDs identify identical status snapshots. TradedQty is a cumulative
    # STATUS observation; it is NOT an individual exchange trade.
    identity={
        "account":account,"oid":oid,"sec":sec,"segment":mapped_segment,
        "product":product,"status":status,"traded":traded,
        "updated":str(data.get("LastUpdatedTime") or "").strip(),
    }
    digest=sha256(json.dumps(identity,sort_keys=True,separators=(",",":")).encode()).hexdigest()[:32]
    return BrokerEvent(
        event_id="DHAN-ORDER-SNAPSHOT-"+digest,
        account_id=account,broker_order_id=oid,security_id=sec,
        exchange_segment=mapped_segment,product_type=product,
        kind="STATUS",status=status,cumulative_filled_qty=traded,
    )
