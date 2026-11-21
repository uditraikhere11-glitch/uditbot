"""Pure, fail-closed preflight for a complete Dhan trade-book snapshot.

Before startup recovery writes even the first fill, validate *every* row and
detect repeated exchange execution IDs with contradictory broker identities.
A malformed row late in a book must not cause partial canonical fill writes.

This is a preflight, NOT confirmation that REST returned a complete book:
the caller must first use its authenticated required-collection read.
"""
from __future__ import annotations

import math
import re
from typing import Any, Sequence, Tuple


class TradebookPreflightError(ValueError):
    """Malformed or contradictory execution evidence."""


def _execution_id(value: Any) -> str:
    if isinstance(value, bool):
        raise TradebookPreflightError("invalid exchange execution ID")
    eid = str(value).strip() if value is not None else ""
    if (
        not eid or eid.lower() in ("none", "null", "na", "nan", "inf", "infinity")
        or re.fullmatch(r"[+-]?0+(?:\.0+)?", eid)
        or any(marker in eid for marker in ("-TRD-", "-FILL-", "-RECON-"))
    ):
        raise TradebookPreflightError("synthetic, missing or zero exchange execution ID")
    return eid


def preflight_tradebook(rows: Sequence[Any], *, expected_account_id: str) -> int:
    """Return number of unique executions, or raise before journal mutations.

    Identical duplicate broker rows can be repeated by a transport; their
    unique execution ID remains single. An execution ID reused for a different
    account, order, contract, side, quantity, price or time is a hard conflict.
    """
    if not isinstance(rows, list) or not isinstance(expected_account_id, str):
        raise TradebookPreflightError("invalid authenticated broker book")
    seen: dict[str, Tuple[Any, ...]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise TradebookPreflightError("non-object trade-book row")
        acct = str(row.get("dhanClientId") or "").strip()
        if expected_account_id and acct and acct != expected_account_id:
            raise TradebookPreflightError("trade-book account mismatch")
        oid = str(row.get("orderId") or "").strip()
        if not oid:
            raise TradebookPreflightError("missing broker order ID")
        eid = _execution_id(row.get("exchangeTradeId"))
        side = str(row.get("transactionType") or "").strip().upper()
        if side not in ("BUY", "SELL"):
            raise TradebookPreflightError("invalid executed trade side")
        raw_qty = row.get("tradedQuantity")
        if isinstance(raw_qty, bool) or not re.fullmatch(r"[+]?[0-9]+", str(raw_qty)):
            raise TradebookPreflightError("invalid trade quantity")
        qty = int(raw_qty)
        price = row.get("tradedPrice")
        if isinstance(price, bool):
            raise TradebookPreflightError("invalid traded price")
        try:
            px = float(price or 0.0)
        except (ValueError, TypeError, OverflowError) as exc:
            raise TradebookPreflightError("invalid traded price") from exc
        if qty <= 0 or not math.isfinite(px) or px <= 0:
            raise TradebookPreflightError("nonpositive execution quantity/price")
        security = str(row.get("securityId") or "")
        segment = str(row.get("exchangeSegment") or "").upper()
        product = str(row.get("productType") or "").upper()
        when = str(row.get("exchangeTime") or "").strip()
        # Preserve existing incomplete-field permissiveness. The canonical
        # record_fill still verifies trade identity and quantity independently.
        fingerprint = (acct, oid, security, segment, product,
                       side, qty, px, when)
        previous = seen.setdefault(eid, fingerprint)
        if previous != fingerprint:
            raise TradebookPreflightError(
                "same exchange execution ID carries contradictory evidence"
            )
    return len(seen)
