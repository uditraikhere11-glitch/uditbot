"""Read-only, account-scoped Dhan quantity interpretation for reconciliation.

Never turn a failed/partial broker read into a zero quantity. A successful
empty position book is not proof that settled DP holdings were sold.
No network access or broker mutations in this module.
"""
from __future__ import annotations
import re
from typing import Dict, List, Optional, Tuple

_INTEGER = re.compile(r"[+-]?\d+\Z")


class QuantityUnverified(ValueError):
    """Broker quantity cannot safely establish an unlinked manual reduction."""


def _quantity(value: object, *, signed: bool = False) -> int:
    if isinstance(value, bool):
        raise QuantityUnverified("Boolean quantity")
    if type(value) is int:
        qty = value
    elif isinstance(value, str) and _INTEGER.fullmatch(value.strip()):
        qty = int(value.strip())
    else:
        raise QuantityUnverified("Missing, nonintegral or malformed quantity")
    if not signed and qty < 0:
        raise QuantityUnverified("Negative settled quantity")
    return qty


def aggregate_holdings(
    rows: List[dict], *, account_id: str = "",
) -> Dict[str, int]:
    """Use the owned quantity, including unsettled shares when reported.

    Dhan may supply availableQty lower than totalQty because shares are blocked.
    A nonzero dpQty next to zero totalQty is contradictory and needs review;
    never use the smaller number as evidence of a sale.
    """
    if not isinstance(rows, list):
        raise QuantityUnverified("Holdings snapshot unavailable")
    totals: Dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("securityId") or "").strip():
            raise QuantityUnverified("Holdings row missing security identity")
        sid = str(row["securityId"]).strip()
        row_account = str(row.get("dhanClientId") or "").strip()
        if account_id and row_account and row_account != account_id:
            raise QuantityUnverified("Holdings account mismatch")
        total = row.get("totalQty")
        dp = row.get("dpQty")
        if total is not None:
            amount = _quantity(total)
            if amount < (0 if dp is None else _quantity(dp)) + _quantity(row.get("t1Qty", 0)):
                raise QuantityUnverified("totalQty smaller than DP/T1 components")
        elif dp is not None:
            amount = _quantity(dp) + _quantity(row.get("t1Qty", 0))
        else:
            raise QuantityUnverified("Missing owned quantity")
        if sid in totals:
            raise QuantityUnverified("Duplicate holdings security")
        totals[sid] = amount
    return totals


def observed_open_quantity(
    positions: Optional[List[dict]], holdings: Optional[List[dict]],
    *, security_id: str, segment: str, product: str, account_id: str,
) -> Tuple[Optional[int], str]:
    """Returns (quantity, evidence status); never a guessed quantity.

    Settled CNC/MTF uses the holdings snapshot. A zero intraday CNC netQty
    cannot override positive DP holdings. Contradictory live net positions,
    failed GETs, missing identities or inconsistent account data stay UNKNOWN.
    """
    if not isinstance(positions, list):
        return None, "POSITIONS_READ_UNAVAILABLE"
    matches = []
    for row in positions:
        if not isinstance(row, dict):
            return None, "POSITIONS_ROW_MALFORMED"
        if not str(row.get("securityId") or "").strip():
            return None, "POSITIONS_SECURITY_MISSING"
        if str(row.get("securityId") or "").strip() != str(security_id).strip():
            continue
        if not row.get("exchangeSegment") or not row.get("productType"):
            return None, "POSITION_CONTRACT_UNVERIFIED"
        if (str(row["exchangeSegment"]).strip().upper() != str(segment).strip().upper()
                or str(row["productType"]).strip().upper() != str(product).strip().upper()):
            continue
        if account_id and row.get("dhanClientId") and str(row["dhanClientId"]).strip() != account_id:
            return None, "POSITION_ACCOUNT_MISMATCH"
        try:
            matches.append(_quantity(row.get("netQty"), signed=True))
        except QuantityUnverified:
            return None, "POSITIONS_QUANTITY_MALFORMED"
    if len(matches) > 1:
        return None, "DUPLICATE_POSITION_CONTRACT"
    net = matches[0] if matches else 0
    if str(product).upper() not in ("CNC", "MTF"):
        if net < 0:
            return None, "NEGATIVE_NET_POSITION_REVIEW"
        return net, "VERIFIED_POSITIONS"
    if not isinstance(holdings, list):
        return None, "SETTLED_HOLDINGS_READ_UNAVAILABLE"
    try:
        settled = aggregate_holdings(holdings, account_id=account_id).get(str(security_id).strip())
    except QuantityUnverified as exc:
        return None, "SETTLED_HOLDINGS_UNVERIFIED: " + str(exc)
    if net < 0:
        # A negative intraday position can indicate a same-day manual SELL,
        # even while old DP quantities remain in the holdings book.
        return None, "NEGATIVE_CNC_POSITION_REVIEW"
    if net > 0 and settled in (None, 0):
        # Dhan documents /positions as the *current-day* open position book;
        # /holdings contains stock from prior trading sessions. A filled CNC
        # BUY placed today may correctly appear only in /positions. This is
        # verified current-day exposure, NOT evidence of settled legacy stock.
        return net, "VERIFIED_POSITIONS"
    if settled is None:
        return None, "SETTLED_HOLDING_NOT_REPORTED"
    if net > 0 and settled > 0:
        # When both books show nonzero quantities, don't guess whether
        # broker totals overlap or represent distinct lots.
        return None, "CNC_POSITIONS_HOLDINGS_CONTRADICT"
    return settled, "VERIFIED_SETTLED_HOLDINGS"
