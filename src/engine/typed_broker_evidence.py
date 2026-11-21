"""Typed interpretation of Dhan external order ID lookup responses.

This module never sends requests, retries, places orders, or authorizes orders.
A failed or 404 lookup is NOT proof that an earlier submission never happened.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExternalOrderEvidence:
    kind: str
    reason: str
    broker_order_id: str | None = None
    may_resubmit: bool = False
    may_mark_filled: bool = False


def classify_external_order_lookup(
    *, correlation_id: str, http_status: int | None = None,
    payload: Any = None, transport_error: BaseException | None = None,
    expected_account_id: str = "", expected_security_id: str = "",
) -> ExternalOrderEvidence:
    if not isinstance(correlation_id, str) or not correlation_id.strip():
        return ExternalOrderEvidence("INVALID_LOOKUP", "missing correlation ID")
    if transport_error is not None:
        return ExternalOrderEvidence("TRANSPORT_UNKNOWN", type(transport_error).__name__)
    if type(http_status) is not int:
        return ExternalOrderEvidence("TRANSPORT_UNKNOWN", "missing or malformed HTTP status")
    if http_status in (401, 403):
        return ExternalOrderEvidence("AUTH_ERROR", "broker authentication failed")
    if http_status == 429:
        return ExternalOrderEvidence("RATE_LIMITED", "broker request throttled")
    if http_status == 404:
        return ExternalOrderEvidence("NOT_FOUND_UNVERIFIED", "404 may reflect eventual consistency; not safe to resubmit")
    if http_status in (408, 500, 502, 503, 504):
        return ExternalOrderEvidence("TRANSPORT_UNKNOWN", "broker request outcome unavailable")
    if http_status != 200:
        return ExternalOrderEvidence("HTTP_UNVERIFIED", f"unexpected response HTTP {http_status}")
    if isinstance(payload, list):
        if len(payload) != 1:
            return ExternalOrderEvidence("MALFORMED", "broker lookup returned zero or multiple records")
        payload = payload[0]
    if not isinstance(payload, dict):
        return ExternalOrderEvidence("MALFORMED", "broker response is not an order object")
    oid = payload.get("orderId")
    if type(oid) not in (str, int) or str(oid).strip().lower() in ("", "0", "none", "null"):
        return ExternalOrderEvidence("MALFORMED", "broker order ID missing")
    reported_cid = payload.get("correlationId")
    if not isinstance(reported_cid, str) or reported_cid.strip() != correlation_id.strip():
        return ExternalOrderEvidence("IDENTITY_CONFLICT", "correlation ID absent or mismatched")
    if expected_account_id and str(payload.get("dhanClientId") or "").strip() != expected_account_id:
        return ExternalOrderEvidence("IDENTITY_CONFLICT", "broker account mismatch")
    if expected_security_id and str(payload.get("securityId") or "").strip() != expected_security_id:
        return ExternalOrderEvidence("IDENTITY_CONFLICT", "security ID mismatch")
    return ExternalOrderEvidence("FOUND_UNVERIFIED_STATUS", "order identity found; status/fills require authoritative verification", str(oid).strip())
