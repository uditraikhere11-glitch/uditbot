"""
DhanHQ API v2 Client Interface + V13 Audit-Hardened Execution-Safety Edition.
Handles:
- Yahoo Finance Primary Price Tunnel (`get_yahoo_quote_details`, `fetch_yahoo_ltp`) with strict
  2.0-second UTC source timestamp validation (`regularMarketTime`) measured against post-HTTP wall clock
  (`V12-15`), mandatory venue/exchange/symbol/delay metadata (`exchangeDataDelayedBy == 0`), BSE `.BO`
  delayed-quote rejection for LIVE, and F&O underlying substitution blocking.
  IMPORTANT LIMITATION: Yahoo Finance's published market-data terms state that its information is
  not intended for trading; this integration is an owner-selected price tunnel and NOT an approved
  exchange-grade live execution feed.
- Shared Dhan Access Token Coordinator integration (`fresh_token`, `force_login`, `read_token`) with
  automatic mtime-based shared token adoption (`V12-20`) and zero recursive order resubmission (`Section E`).
- Structural HTTP 5xx / 408 / network timeout classification as `UNKNOWN` (`uncertain_outcome=True`),
  preserving in-flight correlation IDs (`V12-12`: full 30-char correlation ID on Super Orders; `V12-13`).
- Structured broker collection reads (`BrokerReadList` vs `BrokerReadFailure`) distinguishing `OK_EMPTY`,
  `OK_RECORDS`, `FAILED`, `MALFORMED`, and `UNAVAILABLE` (`Safety Correction C`).
- Authoritative Forever OCO and Super Order verification contract (`evaluate_forever_oco_protection`,
  `verify_forever_leg_active`, `evaluate_super_order_protection`, `verify_super_order_legs_active`)
  with zero weak fallback overrides (`Safety Correction B`).
"""

import base64
import hashlib
import hmac
import json
import logging
import math
import os
import re
import struct
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import requests
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.models import (
    BrokerReadFailure,
    BrokerReadList,
    BrokerReadStatus,
    OrderRequest,
    ProtectionLevels,
    ProtectionVerificationResult,
    ProtectionVerificationStatus,
    QuoteResult,
    TradingMode,
    TransactionType,
)
from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR
from src.broker.dhan_token import (
    force_login,
    fresh_token,
    is_token_error as _shared_is_token_error,
    read_token,
    write_token_atomic,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dhan.co/v2"
AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"

VALID_NSE_EXCHANGES = frozenset({"NSI", "NSE", "NATIONAL STOCK EXCHANGE OF INDIA"})
VALID_BSE_EXCHANGES = frozenset({"BSE", "BOM", "BOMBAY STOCK EXCHANGE"})
ACTIVE_BROKER_ORDER_STATES = frozenset({"PENDING", "OPEN", "TRANSIT", "CONFIRM", "ACTIVE"})
TERMINAL_OR_INACTIVE_ORDER_STATES = frozenset({"REJECTED", "CANCELLED", "EXPIRED", "TRADED", "CLOSED", "TRIGGERED"})


def compute_totp(secret: str, digits: int = 6, period: int = 30) -> str:
    """
    Computes standard RFC 6238 TOTP code using purely Python standard library.
    Zero external dependencies required.
    """
    clean_secret = secret.strip().replace(" ", "").upper()
    pad = len(clean_secret) % 8
    if pad:
        clean_secret += "=" * (8 - pad)
    key = base64.b32decode(clean_secret, casefold=True)
    counter = int(time.time() // period)
    msg = struct.pack(">Q", counter)
    h = hmac.new(key, msg, hashlib.sha1).digest()
    offset = h[-1] & 0x0f
    chunk = h[offset : offset + 4]
    val = struct.unpack(">I", chunk)[0] & 0x7fffffff
    return str(val % (10 ** digits)).zfill(digits)


def verify_forever_oco_group(
    matching_rows: List[Dict[str, Any]],
    forever_order_id: str,
    expected_security_id: Optional[str] = None,
    expected_qty: Optional[int] = None,
    expected_sl_trigger: Optional[float] = None,
    expected_target_price: Optional[float] = None,
    tick_size: float = 0.05,
    expected_account_id: Optional[str] = None,
    expected_segment: Optional[str] = None,
    expected_product_type: Optional[str] = None,
) -> ProtectionVerificationResult:
    """
    Authoritative Forever OCO verification contract shared by `DhanClient`, `ProtectionWatcher`,
    and `StartupReconciliationGate`.
    - Deduplicates two-leg OCO pairs under `forever_order_id` so a 5-share OCO pair counts as 5, never 10.
    - Rejects TRADED, CLOSED, TRIGGERED, CANCELLED, REJECTED, EXPIRED, or missing statuses.
    - Validates account, securityId, exchangeSegment, productType, SELL side, quantities, and prices.
    """
    oid = str(forever_order_id or "").strip()
    if not oid:
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            reason="Missing forever_order_id",
        )

    if expected_segment:
        exp_seg_up = str(expected_segment).strip().upper()
        if exp_seg_up not in ("NSE_EQ", "BSE_EQ"):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever OCO cannot cover non-equity segment '{exp_seg_up}'",
            )

    if expected_product_type:
        exp_prod_up = str(expected_product_type).strip().upper()
        if exp_prod_up not in ("CNC", "MTF"):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever OCO cannot cover productType '{exp_prod_up}' (requires CNC or MTF)",
            )

    if not matching_rows:
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.UNVERIFIED,
            is_verified=False,
            is_terminal_or_invalid=False,
            group_id=oid,
            reason=f"Forever order {oid} absent from broker forever book",
        )

    tol = max(float(tick_size or 0.05), 0.05) + 1e-4
    has_sl_leg = False
    has_target_leg = False
    sl_qty = 0
    target_qty = 0
    observed_sec = ""
    observed_seg = ""
    observed_prod = ""
    observed_acct = ""
    observed_sl = 0.0
    observed_target = 0.0

    for row in matching_rows:
        if not isinstance(row, dict):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} contains malformed non-dict record",
            )

        acct = str(row.get("dhanClientId") or "").strip()
        if acct:
            observed_acct = acct
            if expected_account_id and acct != str(expected_account_id).strip():
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} account mismatch ({acct} != {expected_account_id})",
                )

        st = str(row.get("orderStatus") or "").strip().upper()
        if not st:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} missing orderStatus",
            )
        if st in TERMINAL_OR_INACTIVE_ORDER_STATES:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever order {oid} is in terminal or inactive state: {st}",
            )
        if st not in ACTIVE_BROKER_ORDER_STATES:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} is not in an active resting state: '{st}'",
            )

        sec = str(row.get("securityId") or "").strip()
        if not sec:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} missing securityId",
            )
        observed_sec = sec
        if expected_security_id is not None and sec != str(expected_security_id).strip():
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                security_id=sec,
                reason=f"Forever order {oid} securityId mismatch ('{sec}' != '{expected_security_id}')",
            )

        row_seg = str(row.get("exchangeSegment") or "").strip().upper()
        if row_seg:
            observed_seg = row_seg
            if row_seg not in ("NSE_EQ", "BSE_EQ"):
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} has unsupported exchangeSegment '{row_seg}'",
                )
            if expected_segment and row_seg != str(expected_segment).strip().upper():
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} exchangeSegment mismatch ('{row_seg}' != '{expected_segment}')",
                )

        row_prod = str(row.get("productType") or "").strip().upper()
        if row_prod:
            observed_prod = row_prod
            if row_prod not in ("CNC", "MTF"):
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} has unsupported productType '{row_prod}'",
                )
            if expected_product_type and row_prod != str(expected_product_type).strip().upper():
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} productType mismatch ('{row_prod}' != '{expected_product_type}')",
                )

        tx_type = str(row.get("transactionType") or "").strip().upper()
        if tx_type != "SELL":
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever order {oid} invalid or missing transactionType '{tx_type}' (expected SELL)",
            )

        raw_qty = row.get("remainingQuantity") if row.get("remainingQuantity") is not None else row.get("quantity")
        if raw_qty is None or isinstance(raw_qty, bool):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} missing quantity evidence",
            )
        try:
            traded_q = int(row.get("tradedQuantity") or row.get("filledQty") or 0)
            row_qty = int(row["remainingQuantity"]) if row.get("remainingQuantity") is not None else (int(raw_qty) - traded_q)
        except (ValueError, TypeError):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                reason=f"Forever order {oid} non-integer quantity '{raw_qty}'",
            )

        if row_qty <= 0:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever order {oid} has non-positive remaining quantity ({row_qty})",
            )

        if expected_qty is not None and row_qty != int(expected_qty):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                reason=f"Forever order {oid} quantity mismatch ({row_qty} != {expected_qty})",
            )

        leg_name = str(row.get("legName") or "").strip().upper()
        if leg_name == "STOP_LOSS_LEG":
            trig = row.get("triggerPrice") if row.get("triggerPrice") is not None else row.get("price")
            if trig is None or float(trig) <= 0:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.UNVERIFIED,
                    is_verified=False,
                    group_id=oid,
                    reason=f"Forever order {oid} STOP_LOSS_LEG missing triggerPrice",
                )
            if expected_sl_trigger is not None and abs(float(trig) - float(expected_sl_trigger)) > tol:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} STOP_LOSS_LEG price mismatch ({trig} != {expected_sl_trigger})",
                )
            has_sl_leg = True
            sl_qty = row_qty
            observed_sl = float(trig)
        elif leg_name == "TARGET_LEG":
            tgt = row.get("price") if row.get("price") is not None else row.get("triggerPrice")
            if tgt is None or float(tgt) <= 0:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.UNVERIFIED,
                    is_verified=False,
                    group_id=oid,
                    reason=f"Forever order {oid} TARGET_LEG missing price",
                )
            if expected_target_price is not None and abs(float(tgt) - float(expected_target_price)) > tol:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    reason=f"Forever order {oid} TARGET_LEG price mismatch ({tgt} != {expected_target_price})",
                )
            has_target_leg = True
            target_qty = row_qty
            observed_target = float(tgt)
        else:
            nested_legs = row.get("legDetails")
            if isinstance(nested_legs, list) and len(nested_legs) >= 2:
                for lg in nested_legs:
                    if not isinstance(lg, dict):
                        continue
                    lg_st = str(lg.get("orderStatus") or "").strip().upper()
                    if not lg_st or lg_st not in ACTIVE_BROKER_ORDER_STATES:
                        return ProtectionVerificationResult(
                            status=(
                                ProtectionVerificationStatus.TERMINAL_OR_INVALID
                                if lg_st in TERMINAL_OR_INACTIVE_ORDER_STATES
                                else ProtectionVerificationStatus.UNVERIFIED
                            ),
                            is_verified=False,
                            is_terminal_or_invalid=(lg_st in TERMINAL_OR_INACTIVE_ORDER_STATES),
                            group_id=oid,
                            reason=f"Forever OCO {oid} nested leg not active: '{lg_st or 'MISSING'}'",
                        )
                    ln = str(lg.get("legName") or "").strip().upper()
                    lg_q_raw = lg.get("remainingQuantity") if lg.get("remainingQuantity") is not None else lg.get("quantity")
                    lg_q = int(lg_q_raw) if lg_q_raw is not None else row_qty
                    if ln == "STOP_LOSS_LEG":
                        has_sl_leg = True
                        sl_qty = lg_q
                    elif ln == "TARGET_LEG":
                        has_target_leg = True
                        target_qty = lg_q
            else:
                q1_raw = row.get("quantity1")
                q1 = int(q1_raw) if q1_raw is not None else row_qty
                if expected_qty is not None and q1 != int(expected_qty):
                    return ProtectionVerificationResult(
                        status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                        is_verified=False,
                        is_terminal_or_invalid=True,
                        group_id=oid,
                        reason=f"Forever order {oid} leg1 quantity mismatch ({q1} != {expected_qty})",
                    )
                raw_sl = row.get("triggerPrice")
                raw_tgt = row.get("price1") if row.get("price1") is not None else row.get("triggerPrice1")
                if raw_sl is not None and float(raw_sl) > 0:
                    if expected_sl_trigger is not None and abs(float(raw_sl) - float(expected_sl_trigger)) > tol:
                        return ProtectionVerificationResult(
                            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                            is_verified=False,
                            is_terminal_or_invalid=True,
                            group_id=oid,
                            reason=f"Forever order {oid} SL trigger mismatch ({raw_sl} != {expected_sl_trigger})",
                        )
                    has_sl_leg = True
                    sl_qty = row_qty
                    observed_sl = float(raw_sl)
                if raw_tgt is not None and float(raw_tgt) > 0:
                    if expected_target_price is not None and abs(float(raw_tgt) - float(expected_target_price)) > tol:
                        return ProtectionVerificationResult(
                            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                            is_verified=False,
                            is_terminal_or_invalid=True,
                            group_id=oid,
                            reason=f"Forever order {oid} target price mismatch ({raw_tgt} != {expected_target_price})",
                        )
                    has_target_leg = True
                    target_qty = q1
                    observed_target = float(raw_tgt)

    if not (has_sl_leg and has_target_leg):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            reason=f"Forever order {oid} missing complete two-leg OCO coverage (has_sl={has_sl_leg}, has_target={has_target_leg})",
        )

    paired_qty = min(sl_qty, target_qty)
    if paired_qty <= 0 or (expected_qty is not None and paired_qty < int(expected_qty)):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            reason=f"Forever order {oid} paired coverage ({paired_qty}) insufficient for expected ({expected_qty})",
        )

    return ProtectionVerificationResult(
        status=ProtectionVerificationStatus.VERIFIED_ACTIVE,
        is_verified=True,
        is_terminal_or_invalid=False,
        protected_quantity=paired_qty,
        group_id=oid,
        security_id=observed_sec,
        exchange_segment=observed_seg or (str(expected_segment or "").strip().upper()),
        product_type=observed_prod or (str(expected_product_type or "").strip().upper()),
        account_id=observed_acct or (str(expected_account_id or "").strip()),
        sl_price=observed_sl,
        target_price=observed_target,
        reason=f"Forever order {oid} confirmed active with both SL and Target legs ({paired_qty} units)",
    )


def verify_super_order_record(
    candidate: Optional[Dict[str, Any]],
    order_id: str,
    expected_security_id: Optional[str] = None,
    expected_qty: Optional[int] = None,
    expected_sl_price: Optional[float] = None,
    expected_target_price: Optional[float] = None,
    tick_size: float = 0.05,
    expected_account_id: Optional[str] = None,
    expected_segment: Optional[str] = None,
    expected_product_type: Optional[str] = None,
) -> ProtectionVerificationResult:
    """
    Safety Correction B: Single authoritative Super Order verification contract shared across
    `DhanClient`, `ProtectionWatcher`, and `StartupReconciliationGate`.
    - Requires documented active `legDetails` containing both `STOP_LOSS_LEG` and `TARGET_LEG`.
    - Never accepts top-level `stopLossPrice` / `targetPrice` without active leg evidence.
    - Rejects `TRADED`, `CLOSED`, `TRIGGERED`, `CANCELLED`, `REJECTED`, `EXPIRED`, or missing `orderStatus`.
    - Requires positive remaining quantity evidence and tick-aligned prices on both protective legs.
    """
    oid = str(order_id or "").strip()
    if not oid:
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            reason="Missing super order_id",
        )

    if not isinstance(candidate, dict):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.UNVERIFIED,
            is_verified=False,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} readback returned None or non-dict",
        )

    cand_oid = str(candidate.get("orderId") or "").strip()
    if not cand_oid or cand_oid != oid:
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=cand_oid,
            reason=f"Super order parent orderId mismatch ('{cand_oid}' != '{oid}')",
        )

    acct = str(candidate.get("dhanClientId") or "").strip()
    if acct and expected_account_id and acct != str(expected_account_id).strip():
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} account mismatch ('{acct}' != '{expected_account_id}')",
        )

    sec = str(candidate.get("securityId") or "").strip()
    if expected_security_id is not None:
        if not sec:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} missing securityId evidence",
            )
        if sec != str(expected_security_id).strip():
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                parent_order_id=oid,
                security_id=sec,
                reason=f"Super order {oid} securityId mismatch ('{sec}' != '{expected_security_id}')",
            )

    cand_seg = str(candidate.get("exchangeSegment") or "").strip().upper()
    if cand_seg and expected_segment and cand_seg != str(expected_segment).strip().upper():
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} exchangeSegment mismatch ('{cand_seg}' != '{expected_segment}')",
        )

    cand_prod = str(candidate.get("productType") or "").strip().upper()
    if cand_prod and expected_product_type and cand_prod != str(expected_product_type).strip().upper():
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} productType mismatch ('{cand_prod}' != '{expected_product_type}')",
        )

    parent_st = str(candidate.get("orderStatus") or "").strip().upper()
    if parent_st in ("REJECTED", "CANCELLED", "EXPIRED"):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} parent is in terminal failed state: {parent_st}",
        )

    leg_details = candidate.get("legDetails")
    if not isinstance(leg_details, list) or len(leg_details) < 2:
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.UNVERIFIED,
            is_verified=False,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} lacks active legDetails evidence (top-level prices alone do not prove resting coverage)",
        )

    tol = max(float(tick_size or 0.05), 0.05) + 1e-4
    has_target = False
    has_sl = False
    sl_rem_qty = 0
    target_rem_qty = 0
    observed_sl_price = 0.0
    observed_target_price = 0.0

    for leg in leg_details:
        if not isinstance(leg, dict):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} contains malformed non-dict leg in legDetails",
            )

        leg_name = str(leg.get("legName") or "").strip().upper()
        if leg_name not in ("STOP_LOSS_LEG", "TARGET_LEG"):
            continue

        leg_tx = str(leg.get("transactionType") or "").strip().upper()
        if leg_tx and leg_tx != "SELL":
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} has invalid transactionType '{leg_tx}' (expected SELL)",
            )

        leg_st = str(leg.get("orderStatus") or "").strip().upper()
        if not leg_st:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} is missing orderStatus",
            )
        if leg_st in TERMINAL_OR_INACTIVE_ORDER_STATES:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} is not active resting coverage ({leg_st})",
            )
        if leg_st not in ACTIVE_BROKER_ORDER_STATES:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} has unverified status '{leg_st}'",
            )

        raw_rem_qty = leg.get("remainingQuantity") if leg.get("remainingQuantity") is not None else leg.get("quantity")
        if raw_rem_qty is None or isinstance(raw_rem_qty, bool):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} is missing required quantity evidence",
            )
        try:
            traded_q = int(leg.get("tradedQuantity") or leg.get("filledQty") or 0)
            leg_qty = int(leg["remainingQuantity"]) if leg.get("remainingQuantity") is not None else (int(raw_rem_qty) - traded_q)
        except (ValueError, TypeError):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} has invalid quantity '{raw_rem_qty}'",
            )

        if leg_qty <= 0:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} has non-positive remaining quantity ({leg_qty})",
            )

        if expected_qty is not None and leg_qty < int(expected_qty):
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                group_id=oid,
                parent_order_id=oid,
                reason=f"Super order {oid} leg {leg_name} remaining quantity ({leg_qty}) < expected ({expected_qty})",
            )

        if leg_name == "TARGET_LEG":
            leg_price = (
                leg.get("price")
                if leg.get("price") is not None
                else (leg.get("targetPrice") if leg.get("targetPrice") is not None else leg.get("triggerPrice"))
            )
            if leg_price is None or float(leg_price) <= 0:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.UNVERIFIED,
                    is_verified=False,
                    group_id=oid,
                    parent_order_id=oid,
                    reason=f"Super order {oid} TARGET_LEG missing price evidence",
                )
            if expected_target_price is not None and abs(float(leg_price) - float(expected_target_price)) > tol:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    parent_order_id=oid,
                    reason=f"Super order {oid} TARGET_LEG price mismatch ({leg_price} != {expected_target_price})",
                )
            has_target = True
            target_rem_qty = leg_qty
            observed_target_price = float(leg_price)

        elif leg_name == "STOP_LOSS_LEG":
            leg_price = (
                leg.get("price")
                if leg.get("price") is not None
                else (leg.get("stopLossPrice") if leg.get("stopLossPrice") is not None else leg.get("triggerPrice"))
            )
            if leg_price is None or float(leg_price) <= 0:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.UNVERIFIED,
                    is_verified=False,
                    group_id=oid,
                    parent_order_id=oid,
                    reason=f"Super order {oid} STOP_LOSS_LEG missing price evidence",
                )
            if expected_sl_price is not None and abs(float(leg_price) - float(expected_sl_price)) > tol:
                return ProtectionVerificationResult(
                    status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                    is_verified=False,
                    is_terminal_or_invalid=True,
                    group_id=oid,
                    parent_order_id=oid,
                    reason=f"Super order {oid} STOP_LOSS_LEG price mismatch ({leg_price} != {expected_sl_price})",
                )
            has_sl = True
            sl_rem_qty = leg_qty
            observed_sl_price = float(leg_price)

    if not (has_target and has_sl):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} readback lacks complete active pair (has_sl={has_sl}, has_target={has_target})",
        )

    paired_qty = min(sl_rem_qty, target_rem_qty)
    if paired_qty <= 0 or (expected_qty is not None and paired_qty < int(expected_qty)):
        return ProtectionVerificationResult(
            status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
            is_verified=False,
            is_terminal_or_invalid=True,
            group_id=oid,
            parent_order_id=oid,
            reason=f"Super order {oid} active leg coverage ({paired_qty}) < expected ({expected_qty})",
        )

    return ProtectionVerificationResult(
        status=ProtectionVerificationStatus.VERIFIED_ACTIVE,
        is_verified=True,
        is_terminal_or_invalid=False,
        protected_quantity=paired_qty,
        group_id=oid,
        parent_order_id=oid,
        security_id=sec,
        exchange_segment=cand_seg or (str(expected_segment or "").strip().upper()),
        product_type=cand_prod or (str(expected_product_type or "").strip().upper()),
        account_id=acct or (str(expected_account_id or "").strip()),
        sl_price=observed_sl_price,
        target_price=observed_target_price,
        reason=f"Super order {oid} legs confirmed active via broker readback ({paired_qty} units)",
    )


class DhanClient:
    def __init__(
        self,
        config: AppConfig,
        stop_manager: Optional[EmergencyStopManager] = None,
        reconciliation_gate: Optional[Any] = None,
    ):
        self.cfg = config
        self.session = requests.Session()
        self.stop_manager = stop_manager or EmergencyStopManager(
            state_file=os.path.join(config.data_dir, "emergency_stop.json")
        )
        self.reconciliation_gate = reconciliation_gate
        self._token_mtime: float = 0.0
        self.token = self._load_token()
        self._quote_cache: Dict[Tuple[str, str], QuoteResult] = {}
        self.last_quote_result: Optional[QuoteResult] = None
        self.last_ltp_meta: Optional[Dict[str, Any]] = None
        self.last_order_http_latency_ms: Optional[float] = None
        self.last_order_broker_status: str = ""
        self.last_funds_is_fallback: bool = False

    def _is_isolated_test_default_token_path(self, token_path: str) -> bool:
        import sys
        if ("unittest" in sys.modules or "pytest" in sys.modules) and os.environ.get("AUDITBOT_ALLOW_HOME_TOKEN_IN_TESTS") != "1":
            default_home_tok = os.path.abspath(os.path.expanduser("~/.dhan_token"))
            if os.path.abspath(token_path) == default_home_tok:
                return True
        return False

    def _perform_totp_http_login(self) -> Tuple[bool, str, str]:
        """Executes raw TOTP HTTP call against Dhan auth endpoint and validates account identity."""
        if not self.cfg.dhan_client_id or not self.cfg.dhan_pin or not self.cfg.dhan_totp_secret:
            return False, "", "Missing DHAN_CLIENT_ID, DHAN_PIN, or DHAN_TOTP_SECRET in config"
        try:
            totp_code = compute_totp(self.cfg.dhan_totp_secret)
            params = {
                "dhanClientId": self.cfg.dhan_client_id,
                "pin": self.cfg.dhan_pin,
                "totp": totp_code,
            }
            resp = self.session.post(AUTH_URL, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, dict):
                    return False, "", f"Malformed TOTP response from Dhan: {resp.text}"
                resp_client_id = str(data.get("dhanClientId", "")).strip()
                if resp_client_id and self.cfg.dhan_client_id and resp_client_id != str(self.cfg.dhan_client_id).strip():
                    return False, "", f"Dhan account mismatch in TOTP response: expected '{self.cfg.dhan_client_id}', got '{resp_client_id}'"
                new_token = str(data.get("accessToken", "")).strip()
                if new_token:
                    return True, new_token, "Token generated successfully"
                return False, "", f"Token missing in Dhan response: {resp.text}"
            return False, "", f"Dhan auth rejected TOTP ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, "", f"Exception generating token via TOTP: {e}"

    def generate_token_with_totp(
        self,
        force: bool = True,
        rejected_token: Optional[str] = None,
    ) -> Tuple[bool, str, str]:
        """
        V12-20: Generates or adopts a fresh 24-hour access token through the shared `fcntl.flock`
        coordinator (`force_login` when `force=True`, or `fresh_token` when `force=False`).
        """
        if not self.cfg.dhan_client_id or not self.cfg.dhan_pin or not self.cfg.dhan_totp_secret:
            return False, "", "Missing DHAN_CLIENT_ID, DHAN_PIN, or DHAN_TOTP_SECRET in config"

        token_path = os.path.expanduser(self.cfg.dhan_token_file)
        last_err = ["Token generation failed"]

        def _login_cb() -> Optional[str]:
            ok, tok, msg = self._perform_totp_http_login()
            if ok and tok:
                last_err[0] = msg
                return tok
            last_err[0] = msg
            return None

        try:
            if force:
                new_token = force_login(_login_cb, path=token_path)
            else:
                rej = rejected_token if rejected_token is not None else self.token
                new_token = fresh_token(rej, _login_cb, path=token_path)

            if new_token:
                self.token = new_token
                try:
                    if os.path.exists(token_path):
                        self._token_mtime = os.path.getmtime(token_path)
                except Exception:
                    pass
                logger.info(f"✅ Active Dhan access token coordinated via {token_path}")
                return True, new_token, "Token generated successfully"
            return False, "", last_err[0]
        except Exception as e:
            return False, "", f"Exception generating token via TOTP: {e}"

    def _sync_shared_token_from_disk(self) -> None:
        """V12-20 & CV13-06: Adopts newer token written by scheduled refresh or another worker."""
        token_path = os.path.expanduser(self.cfg.dhan_token_file)
        if not token_path or self._is_isolated_test_default_token_path(token_path) or not os.path.exists(token_path):
            return
        try:
            mtime = os.path.getmtime(token_path)
            if mtime > self._token_mtime:
                disk_tok = read_token(token_path)
                if disk_tok:
                    self.token = disk_tok
                    self._token_mtime = mtime
        except Exception:
            pass

    def _load_token(self) -> str:
        """
        CV13-06: Single authoritative access-token source (`~/.dhan_token`).
        Prefers the shared token file when present (unless isolated in a unit test),
        falling back to explicit in-memory config token or coordinated TOTP generation.
        """
        token_path = os.path.expanduser(self.cfg.dhan_token_file)
        is_isolated_default = self._is_isolated_test_default_token_path(token_path)

        if not is_isolated_default and token_path and os.path.exists(token_path):
            tok = read_token(token_path)
            if tok:
                try:
                    self._token_mtime = os.path.getmtime(token_path)
                except Exception:
                    pass
                return tok

        if self.cfg.dhan_access_token:
            return self.cfg.dhan_access_token.strip()

        if not is_isolated_default and self.cfg.dhan_client_id and self.cfg.dhan_pin and self.cfg.dhan_totp_secret:
            ok, tok, _ = self.generate_token_with_totp(force=False, rejected_token="")
            if ok and tok:
                return tok

        return ""

    def _headers(self) -> Dict[str, str]:
        self._sync_shared_token_from_disk()
        return {
            "access-token": self.token,
            "client-id": self.cfg.dhan_client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def is_token_error(self, status_code: int, response_text: str) -> bool:
        """Detects Dhan expired or invalid token errors."""
        return _shared_is_token_error(status_code, response_text)

    def verify_token_authenticated(self) -> Tuple[bool, float, str]:
        """
        P1 TOTP Recovery Guard: Verifies against Dhan /v2/fundlimit that the active token
        is genuinely authenticated at the broker, never masking errors with TEST-mode fallback.
        """
        if not self.token:
            return False, 0.0, "Missing access token"
        self.last_funds_is_fallback = False
        ok_f, avail, msg_f = self.get_fund_limits(retry=False)
        if not ok_f or getattr(self, "last_funds_is_fallback", False):
            return False, 0.0, msg_f
        return True, avail, msg_f

    def get_fund_limits(self, retry: bool = True) -> Tuple[bool, float, str]:
        """
        Retrieves available ledger balance from Dhan v2 /fundlimit endpoint.
        Returns: (success, available_balance, message)
        """
        self.last_funds_is_fallback = False
        if self.cfg.trading_mode == TradingMode.TEST:
            if not self.token:
                self.last_funds_is_fallback = True
                return True, 100000.0, "Test mode default balance (token missing)"
            url = f"{BASE_URL}/fundlimit"
            try:
                resp = self.session.get(url, headers=self._headers(), timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        avail = float(data.get("availabelBalance", data.get("availableBalance", data.get("sodLimit", 0.0))))
                        self.last_funds_is_fallback = False
                        return True, avail, "Funds retrieved successfully from broker (HTTP 200)"
                    self.last_funds_is_fallback = True
                    return True, 100000.0, "Test mode fallback balance (Malformed response from Dhan)"
                elif self.is_token_error(resp.status_code, resp.text):
                    self.last_funds_is_fallback = True
                    return True, 100000.0, f"Test mode fallback balance (Dhan auth error {resp.status_code})"
                self.last_funds_is_fallback = True
                return True, 100000.0, f"Test mode fallback balance (Dhan returned {resp.status_code})"
            except Exception as e:
                self.last_funds_is_fallback = True
                return True, 100000.0, f"Test mode fallback balance (Dhan query exception: {e})"

        url = f"{BASE_URL}/fundlimit"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, dict):
                    return False, 0.0, f"Malformed Dhan funds response (expected dict, got {type(data).__name__})"
                avail = float(data.get("availabelBalance", data.get("availableBalance", data.get("sodLimit", 0.0))))
                return True, avail, "Funds retrieved successfully"
            elif self.is_token_error(resp.status_code, resp.text) and retry:
                if self.cfg.dhan_pin and self.cfg.dhan_totp_secret:
                    logger.warning("Dhan token rejected on /fundlimit. Attempting coordinated refresh...")
                    ok, _, _ = self.generate_token_with_totp(force=False, rejected_token=self.token)
                    if ok:
                        return self.get_fund_limits(retry=False)
                return False, 0.0, f"Dhan auth error ({resp.status_code}): {resp.text}"
            else:
                return False, 0.0, f"Dhan funds API error ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, 0.0, f"HTTP exception fetching funds: {e}"

    # =========================================================================
    # SECTION A & V12-15: Yahoo Finance Primary Price Source with Strict 2.0s Freshness Gate
    # =========================================================================

    def get_yahoo_quote_details(
        self,
        stock_symbol: str,
        exchange_segment: str = "NSE_EQ",
        security_id: str = "",
        now_utc: Optional[float] = None,
        max_quote_age_sec: Optional[float] = None,
        allow_cached: bool = False,
        cmp_policy: Optional[str] = None,
    ) -> QuoteResult:
        seg = (exchange_segment or "").strip().upper()
        clean_sym = (stock_symbol or "").replace("-EQ", "").strip().upper()
        max_age = float(max_quote_age_sec) if max_quote_age_sec is not None else float(getattr(self.cfg, "max_quote_age_sec", 2.0))
        current_utc = float(now_utc) if now_utc is not None else time.time()
        effective_policy = str(cmp_policy or getattr(self.cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP") or "STRICT_TIMESTAMP").upper().strip()
        is_darvas_policy = (effective_policy == "DARVAS_HTTP_SUCCESS" and seg == "NSE_EQ")

        if seg in ("NSE_FNO", "BSE_FNO") or seg not in ("NSE_EQ", "BSE_EQ"):
            res = QuoteResult(
                is_valid=False,
                source="YAHOO_FINANCE",
                ticker=clean_sym,
                venue="UNKNOWN",
                security_id=str(security_id or ""),
                cmp_policy=effective_policy,
                failure_reason=(
                    f"Unsupported segment '{seg}' for Yahoo Finance equity price source: "
                    f"cannot substitute underlying equity quote for F&O option contract premium"
                ),
            )
            self.last_quote_result = res
            return res

        if not clean_sym:
            res = QuoteResult(
                is_valid=False,
                source="YAHOO_FINANCE",
                venue="UNKNOWN",
                security_id=str(security_id or ""),
                cmp_policy=effective_policy,
                failure_reason="Missing stock symbol for Yahoo Finance quote",
            )
            self.last_quote_result = res
            return res

        cache_key = (seg, clean_sym)
        if allow_cached and cache_key in self._quote_cache:
            cached = self._quote_cache[cache_key]
            recomputed_age = round(current_utc - cached.quote_timestamp, 6) if (cached.quote_timestamp is not None and cached.quote_timestamp > 0) else None
            if cached.is_valid and (recomputed_age is not None and 0.0 <= recomputed_age <= max_age) and (is_darvas_policy or cached.freshness_verified) and (not security_id or cached.security_id == str(security_id)):
                updated = cached.model_copy(update={"age_sec": recomputed_age}) if hasattr(cached, "model_copy") else cached.copy(update={"age_sec": recomputed_age})
                self.last_quote_result = updated
                return updated
            else:
                self._quote_cache.pop(cache_key, None)

        suffix = ".BO" if seg == "BSE_EQ" else ".NS"
        expected_ticker = f"{clean_sym}{suffix}"
        url = YAHOO_CHART_URL.format(ticker=expected_ticker)
        headers = {"User-Agent": "Mozilla/5.0"}

        t4_start_ns = time.perf_counter_ns()
        try:
            t_req_start = time.time()
            resp = self.session.get(url, headers=headers, timeout=5)
            t4_end_ns = time.perf_counter_ns()
            http_latency_ms = round(max(0.0, (t4_end_ns - t4_start_ns) / 1_000_000.0), 3)
            http_elapsed = max(0.0, time.time() - t_req_start)
            eval_utc = (float(now_utc) + (http_elapsed if http_elapsed >= 0.05 else 0.0)) if now_utc is not None else time.time()

            if resp.status_code != 200:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=seg,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Yahoo Finance returned HTTP {resp.status_code} for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            t5_start_ns = time.perf_counter_ns()
            data = resp.json()
            results = (data.get("chart") or {}).get("result") or []
            if not results or not isinstance(results[0], dict):
                t5_end_ns = time.perf_counter_ns()
                parse_latency_ms = round(max(0.0, (t5_end_ns - t5_start_ns) / 1_000_000.0), 3)
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=seg,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Empty chart result in Yahoo Finance response for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            meta = results[0].get("meta") or {}
            raw_price = meta.get("regularMarketPrice")
            raw_ts = meta.get("regularMarketTime")
            meta_sym = str(meta.get("symbol") or "").strip().upper()
            meta_exch = str(meta.get("exchangeName") or meta.get("fullExchangeName") or "").strip().upper()
            meta_curr = str(meta.get("currency") or "").strip().upper()
            market_state = str(meta.get("marketState") or "").strip().upper()
            observed_venue = meta_exch if meta_exch else "UNKNOWN"
            observed_currency = meta_curr if meta_curr else "UNKNOWN"
            observed_market_state = market_state if market_state else "UNKNOWN"
            meta_sec_id = str(meta.get("securityId") or "").strip()
            raw_delay = meta.get("exchangeDataDelayedBy")
            t5_end_ns = time.perf_counter_ns()
            parse_latency_ms = round(max(0.0, (t5_end_ns - t5_start_ns) / 1_000_000.0), 3)

            t6_start_ns = time.perf_counter_ns()
            if raw_price is None or isinstance(raw_price, bool) or not isinstance(raw_price, (int, float)):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Missing or non-numeric regularMarketPrice for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            price = float(raw_price)
            if not math.isfinite(price) or price <= 0.0:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=0.0,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Invalid non-positive or non-finite price ({raw_price}) for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            if not meta_sym or meta_sym != expected_ticker:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=meta_sym or expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Instrument identity mismatch: requested '{expected_ticker}', response had '{meta_sym}'",
                )
                self.last_quote_result = res
                return res

            if meta_sec_id and security_id and meta_sec_id != str(security_id):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=meta_sec_id,
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Security ID mismatch: expected '{security_id}', response had '{meta_sec_id}'",
                )
                self.last_quote_result = res
                return res

            if seg == "NSE_EQ" and (not meta_exch or meta_exch not in VALID_NSE_EXCHANGES):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Venue mismatch or missing exchangeName for NSE_EQ: Yahoo returned exchange '{observed_venue}'",
                )
                self.last_quote_result = res
                return res

            if seg == "BSE_EQ" and (not meta_exch or meta_exch not in VALID_BSE_EXCHANGES):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Venue mismatch or missing exchangeName for BSE_EQ: Yahoo returned exchange '{observed_venue}'",
                )
                self.last_quote_result = res
                return res

            if meta_curr != "INR":
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Unsupported or missing quote currency '{observed_currency}' (expected INR)",
                )
                self.last_quote_result = res
                return res

            if is_darvas_policy:
                # CV13-07: DARVAS_HTTP_SUCCESS policy for NSE_EQ
                from src.engine.market import is_market_open
                m_open = False
                try:
                    eval_dt = datetime.datetime.fromtimestamp(eval_utc, tz=IST) if eval_utc else None
                    m_open = is_market_open(now=eval_dt)
                except Exception:
                    pass

                delay_metadata_present = (raw_delay is not None and not isinstance(raw_delay, bool) and isinstance(raw_delay, (int, float)))
                delay_seconds = float(raw_delay) if delay_metadata_present else None
                is_delayed = bool(delay_seconds is not None and delay_seconds > 0.0)

                quote_ts: Optional[float] = None
                age_sec: Optional[float] = None
                quote_age_status = "QUOTE_AGE_UNKNOWN"
                freshness_verified = False
                quote_freshness_status = "ACCEPTED_UNVERIFIED_TIMESTAMP"
                if raw_ts is not None and not isinstance(raw_ts, bool) and isinstance(raw_ts, (int, float)):
                    cand_ts = float(raw_ts)
                    if math.isfinite(cand_ts) and cand_ts > 0.0:
                        quote_ts = cand_ts
                        calc_age = round(eval_utc - quote_ts, 6)
                        if calc_age >= 0.0:
                            age_sec = calc_age
                            quote_age_status = f"{age_sec:.2f}s"
                            if (
                                observed_market_state in ("REGULAR", "OPEN")
                                and delay_metadata_present
                                and not is_delayed
                                and age_sec <= max_age
                            ):
                                freshness_verified = True
                                quote_freshness_status = "FRESHNESS_VERIFIED"
                            elif age_sec > max_age:
                                freshness_verified = False
                                quote_freshness_status = f"ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S ({age_sec:.2f}s > {max_age:.2f}s)"
                            else:
                                freshness_verified = False
                                quote_freshness_status = f"ACCEPTED_BY_DARVAS_POLICY (state={observed_market_state}, delay_verified={delay_metadata_present and not is_delayed})"
                        else:
                            quote_age_status = "FUTURE_TIMESTAMP_INVALID"
                            quote_freshness_status = "ACCEPTED_UNVERIFIED_TIMESTAMP"

                is_current_session = m_open and observed_market_state not in ("CLOSED", "POST", "PRE", "POSTPOST", "PREPRE")
                ref_label = "LIVE_REFERENCE" if is_current_session else "HISTORICAL_REFERENCE"
                auth_latency_ms = round(max(0.0, (time.perf_counter_ns() - t6_start_ns) / 1_000_000.0), 3)
                total_cmp_ms = round(http_latency_ms + parse_latency_ms + auth_latency_ms, 3)

                GLOBAL_LATENCY_MONITOR.record_yahoo_request(
                    True,
                    http_latency_ms,
                    age_sec if quote_age_status not in ("QUOTE_AGE_UNKNOWN", "FUTURE_TIMESTAMP_INVALID") else None,
                    symbol=clean_sym,
                )
                res = QuoteResult(
                    is_valid=True,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    security_id=str(security_id or ""),
                    currency=observed_currency,
                    market_state=observed_market_state,
                    quote_timestamp=quote_ts,
                    age_sec=age_sec,
                    is_delayed=is_delayed,
                    delay_seconds=delay_seconds,
                    delay_metadata_present=delay_metadata_present,
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    auth_latency_ms=auth_latency_ms,
                    total_cmp_latency_ms=total_cmp_ms,
                    quote_age_status=quote_age_status,
                    freshness_verified=freshness_verified,
                    quote_freshness_status=quote_freshness_status,
                    cmp_policy="DARVAS_HTTP_SUCCESS",
                    reference_label=ref_label,
                    failure_reason="",
                )
                self._quote_cache[cache_key] = res
                self.last_quote_result = res
                return res

            # STRICT_TIMESTAMP Policy
            if market_state not in ("REGULAR", "OPEN"):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Non-regular or closed market session state: '{market_state}'",
                )
                self.last_quote_result = res
                return res

            delay_metadata_present = (raw_delay is not None and not isinstance(raw_delay, bool) and isinstance(raw_delay, (int, float)))
            delay_seconds = float(raw_delay) if delay_metadata_present else (900.0 if seg == "BSE_EQ" else 0.0)
            is_delayed = (delay_seconds > 0.0) or (seg == "BSE_EQ")
            if is_delayed:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    is_delayed=True,
                    delay_seconds=delay_seconds,
                    delay_metadata_present=delay_metadata_present,
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=(
                        f"Delayed quote rejected for live execution ({expected_ticker}, venue={seg}, "
                        f"delay={delay_seconds:g}s — BSE .BO / delayed feeds are prohibited for automated orders)"
                    ),
                )
                self.last_quote_result = res
                return res

            if raw_ts is None or isinstance(raw_ts, bool) or not isinstance(raw_ts, (int, float)):
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    quote_age_status="QUOTE_AGE_UNKNOWN",
                    cmp_policy=effective_policy,
                    failure_reason=f"Missing or invalid source quote timestamp (regularMarketTime) for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            quote_ts = float(raw_ts)
            if not math.isfinite(quote_ts) or quote_ts <= 0.0:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    quote_age_status="QUOTE_AGE_UNKNOWN",
                    cmp_policy=effective_policy,
                    failure_reason=f"Non-positive or non-finite regularMarketTime ({raw_ts}) for {expected_ticker}",
                )
                self.last_quote_result = res
                return res

            age_sec = round(eval_utc - quote_ts, 6)
            if age_sec < 0.0:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    quote_timestamp=quote_ts,
                    age_sec=age_sec,
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Future source quote timestamp rejected (age={age_sec:.3f}s < 0.0s)",
                )
                self.last_quote_result = res
                return res

            if age_sec > max_age:
                GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, age_sec)
                res = QuoteResult(
                    is_valid=False,
                    price=price,
                    source="YAHOO_FINANCE",
                    ticker=expected_ticker,
                    venue=observed_venue,
                    currency=observed_currency,
                    market_state=observed_market_state,
                    security_id=str(security_id or ""),
                    quote_timestamp=quote_ts,
                    age_sec=age_sec,
                    quote_age_status=f"{age_sec:.2f}s",
                    http_latency_ms=http_latency_ms,
                    parse_latency_ms=parse_latency_ms,
                    cmp_policy=effective_policy,
                    failure_reason=f"Stale source quote rejected: age {age_sec:.2f}s exceeds {max_age:.2f}s maximum",
                )
                self.last_quote_result = res
                return res

            auth_latency_ms = round(max(0.0, (time.perf_counter_ns() - t6_start_ns) / 1_000_000.0), 3)
            total_cmp_ms = round(http_latency_ms + parse_latency_ms + auth_latency_ms, 3)
            GLOBAL_LATENCY_MONITOR.record_yahoo_request(True, http_latency_ms, age_sec, symbol=clean_sym)
            res = QuoteResult(
                is_valid=True,
                price=price,
                source="YAHOO_FINANCE",
                ticker=expected_ticker,
                venue=observed_venue,
                security_id=str(security_id or ""),
                currency=observed_currency,
                market_state=observed_market_state,
                quote_timestamp=quote_ts,
                age_sec=age_sec,
                is_delayed=False,
                delay_seconds=float(raw_delay) if delay_metadata_present else None,
                delay_metadata_present=delay_metadata_present,
                http_latency_ms=http_latency_ms,
                parse_latency_ms=parse_latency_ms,
                auth_latency_ms=auth_latency_ms,
                total_cmp_latency_ms=total_cmp_ms,
                quote_age_status=f"{age_sec:.2f}s",
                freshness_verified=True,
                quote_freshness_status="FRESHNESS_VERIFIED",
                cmp_policy=effective_policy,
                reference_label="LIVE_REFERENCE",
                failure_reason="",
            )
            self._quote_cache[cache_key] = res
            self.last_quote_result = res
            return res

        except Exception as e:
            http_latency_ms = round(max(0.0, (time.perf_counter_ns() - t4_start_ns) / 1_000_000.0), 3)
            GLOBAL_LATENCY_MONITOR.record_yahoo_request(False, http_latency_ms, None)
            res = QuoteResult(
                is_valid=False,
                source="YAHOO_FINANCE",
                ticker=expected_ticker,
                venue="UNKNOWN",
                security_id=str(security_id or ""),
                http_latency_ms=http_latency_ms,
                cmp_policy=effective_policy,
                failure_reason=f"Yahoo Finance exception for {expected_ticker}: {e}",
            )
            self.last_quote_result = res
            return res

    def _fetch_ltp(
        self,
        stock_symbol: str,
        exchange_segment: str = "NSE_EQ",
        now_utc: Optional[float] = None,
        max_quote_age_sec: Optional[float] = None,
        cmp_policy: Optional[str] = None,
    ) -> Optional[float]:
        ok, price, _ = self.fetch_yahoo_ltp(
            stock_symbol,
            exchange_segment=exchange_segment,
            now_utc=now_utc,
            max_quote_age_sec=max_quote_age_sec,
            cmp_policy=cmp_policy,
        )
        return price if ok else None

    def fetch_yahoo_ltp(
        self,
        stock_symbol: str,
        exchange_segment: str = "NSE_EQ",
        security_id: str = "",
        now_utc: Optional[float] = None,
        max_quote_age_sec: Optional[float] = None,
        cmp_policy: Optional[str] = None,
    ) -> Tuple[bool, float, str]:
        q = self.get_yahoo_quote_details(
            stock_symbol=stock_symbol,
            exchange_segment=exchange_segment,
            security_id=security_id,
            now_utc=now_utc,
            max_quote_age_sec=max_quote_age_sec,
            allow_cached=False,
            cmp_policy=cmp_policy,
        )
        if q.is_valid:
            age_label = q.quote_age_status or (f"{q.age_sec:.2f}s" if q.age_sec is not None else "QUOTE_AGE_UNKNOWN")
            return True, q.price, f"LTP retrieved via Yahoo Finance ({q.ticker}, age={age_label}, policy={q.cmp_policy})"
        return False, 0.0, q.failure_reason

    def get_ltp(
        self,
        exchange_segment: str,
        security_id: str,
        retry: bool = True,
        symbol: Optional[str] = None,
        now_utc: Optional[float] = None,
        max_quote_age_sec: Optional[float] = None,
        cmp_policy: Optional[str] = None,
    ) -> Tuple[bool, float, str]:
        seg = (exchange_segment or "").strip().upper()

        if seg in ("NSE_FNO", "BSE_FNO") and symbol and not self.token:
            q = self.get_yahoo_quote_details(
                stock_symbol=symbol,
                exchange_segment=seg,
                security_id=security_id,
                now_utc=now_utc,
                max_quote_age_sec=max_quote_age_sec,
                cmp_policy=cmp_policy,
            )
            return False, 0.0, q.failure_reason

        effective_policy = str(cmp_policy or getattr(self.cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP") or "STRICT_TIMESTAMP").upper().strip()

        if symbol and seg in ("NSE_EQ", "BSE_EQ"):
            yf_ok, yf_ltp, yf_msg = self.fetch_yahoo_ltp(
                stock_symbol=symbol,
                exchange_segment=seg,
                security_id=security_id,
                now_utc=now_utc,
                max_quote_age_sec=max_quote_age_sec,
                cmp_policy=effective_policy,
            )
            if yf_ok and yf_ltp > 0:
                return True, yf_ltp, yf_msg
            # Never silently substitute an unverified Dhan marketfeed price after a Yahoo failure
            return False, 0.0, yf_msg

        if seg in ("NSE_EQ", "BSE_EQ") and effective_policy == "DARVAS_HTTP_SUCCESS":
            return (
                False,
                0.0,
                "DARVAS_HTTP_SUCCESS specifies Yahoo Finance as the executable CMP source; unverified Dhan marketfeed substitution is prohibited",
            )

        if not self.token:
            return False, 0.0, "No valid fresh quote from Yahoo Finance and no Dhan access token configured"

        url = f"{BASE_URL}/marketfeed/ltp"
        sec_key = int(security_id) if str(security_id).isdigit() else str(security_id)
        payload = {exchange_segment: [sec_key]}
        try:
            t_req_start = time.time()
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            http_elapsed = max(0.0, time.time() - t_req_start)
            eval_utc = (float(now_utc) + (http_elapsed if http_elapsed >= 0.05 else 0.0)) if now_utc is not None else time.time()
            max_age = float(max_quote_age_sec) if max_quote_age_sec is not None else float(getattr(self.cfg, "max_quote_age_sec", 2.0))
            if resp.status_code == 200:
                data = resp.json()
                seg_data = data.get("data", {}).get(exchange_segment, {})
                sec_data = seg_data.get(str(security_id), {})
                ltp = float(sec_data.get("last_price", 0.0))
                if ltp > 0 and math.isfinite(ltp):
                    raw_src_ts = (
                        sec_data.get("last_trade_time")
                        if sec_data.get("last_trade_time") is not None
                        else (
                            sec_data.get("quoteTimestamp")
                            if sec_data.get("quoteTimestamp") is not None
                            else sec_data.get("timestamp")
                        )
                    )
                    src_ts: Optional[float] = None
                    if raw_src_ts is not None and not isinstance(raw_src_ts, bool) and isinstance(raw_src_ts, (int, float)):
                        cand_ts = float(raw_src_ts)
                        if math.isfinite(cand_ts) and cand_ts > 0.0:
                            src_ts = cand_ts
                            src_age = round(eval_utc - src_ts, 6)
                            if src_age < 0.0:
                                return False, 0.0, f"Future Dhan LTP source timestamp rejected (age={src_age:.2f}s < 0.0s)"
                            if src_age > max_age:
                                return False, 0.0, f"Stale Dhan LTP source timestamp rejected (age={src_age:.2f}s > {max_age:.2f}s)"
                    if seg in ("NSE_EQ", "BSE_EQ") and src_ts is None:
                        return False, 0.0, "Unverified Dhan marketfeed LTP rejected: missing source quote timestamp (last_trade_time)"
                    self.last_ltp_meta = {
                        "price": ltp,
                        "exchange_segment": exchange_segment,
                        "security_id": str(security_id),
                        "source_quote_timestamp": src_ts,
                        "local_retrieved_at": eval_utc,
                        "timestamp_type": "SOURCE_TIMESTAMP" if src_ts is not None else "LOCAL_RETRIEVAL",
                    }
                    return True, ltp, "LTP retrieved successfully from Dhan"
                return False, 0.0, f"LTP is 0 or missing in Dhan response: {resp.text}"
            elif self.is_token_error(resp.status_code, resp.text) and retry:
                if "806" not in resp.text and self.cfg.dhan_pin and self.cfg.dhan_totp_secret:
                    ok, _, _ = self.generate_token_with_totp(force=False, rejected_token=self.token)
                    if ok:
                        return self.get_ltp(
                            exchange_segment, security_id, retry=False, symbol=symbol,
                            now_utc=now_utc, max_quote_age_sec=max_quote_age_sec,
                        )
                return False, 0.0, f"Dhan auth error ({resp.status_code}): {resp.text}"
            return False, 0.0, f"Dhan LTP API error ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, 0.0, f"HTTP exception fetching LTP: {e}"

    def get_order_by_correlation_id(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """
        Queries Dhan external correlation endpoint: GET /v2/orders/external/{correlation-id}.
        Provides durable order status lookup after crash or lost network ACK.
        """
        if self.cfg.trading_mode == TradingMode.TEST:
            return None

        url = f"{BASE_URL}/orders/external/{correlation_id}"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data[0] if data and isinstance(data[0], dict) else None
                return data if isinstance(data, dict) and data.get("orderId") else None
            return None
        except Exception as e:
            logger.error(f"HTTP exception fetching order by correlation {correlation_id}: {e}")
            return None

    # =========================================================================
    # SAFETY CORRECTION C.A: Structured Broker Collection Reads
    # =========================================================================

    def _read_collection(self, path_suffix: str, label: str) -> Union[BrokerReadList, BrokerReadFailure]:
        """
        Safety Correction C.A: Distinguishes:
        1. Successful response with zero records -> `BrokerReadList([], status="OK_EMPTY")`
        2. Successful response with records -> `BrokerReadList(records, status="OK_RECORDS")`
        3. Failed, malformed, or unavailable response -> `BrokerReadFailure(status=..., error=...)` (never `[]`)
        """
        if self.cfg.trading_mode == TradingMode.TEST:
            return BrokerReadList([], status=BrokerReadStatus.OK_EMPTY.value, status_code=200)

        url = f"{BASE_URL}{path_suffix}"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            status_code = int(getattr(resp, "status_code", 0) or 0)
            if status_code == 200:
                try:
                    data = resp.json()
                except Exception as e:
                    err = f"Malformed JSON in {label} response: {e}"
                    logger.error(err)
                    return BrokerReadFailure(status=BrokerReadStatus.MALFORMED.value, error=err, status_code=status_code)

                if not isinstance(data, list) or any(not isinstance(item, dict) for item in data):
                    err = f"Malformed {label} payload: expected list of dicts, got {type(data).__name__}"
                    logger.error(err)
                    return BrokerReadFailure(status=BrokerReadStatus.MALFORMED.value, error=err, status_code=status_code)

                return BrokerReadList(
                    data,
                    status=BrokerReadStatus.OK_RECORDS.value if len(data) > 0 else BrokerReadStatus.OK_EMPTY.value,
                    status_code=200,
                )

            err_text = str(getattr(resp, "text", "") or "")
            err = f"Dhan {label} read failed (HTTP {status_code}): {err_text}"
            logger.error(err)
            return BrokerReadFailure(status=BrokerReadStatus.FAILED.value, error=err, status_code=status_code)
        except Exception as e:
            err = f"HTTP exception fetching {label}: {e}"
            logger.error(err)
            return BrokerReadFailure(status=BrokerReadStatus.UNAVAILABLE.value, error=err, status_code=0)

    def get_order_book(self) -> Union[BrokerReadList, BrokerReadFailure]:
        """Retrieves today's complete order book: GET /v2/orders."""
        return self._read_collection("/orders", "order_book")

    def get_super_orders(self) -> Union[BrokerReadList, BrokerReadFailure]:
        """V12-05: Retrieves all Super Orders with nested legDetails: GET /v2/super/orders."""
        return self._read_collection("/super/orders", "super_orders")

    def get_trade_book(self) -> Union[BrokerReadList, BrokerReadFailure]:
        """Retrieves today's trade executions: GET /v2/trades."""
        return self._read_collection("/trades", "trade_book")

    def get_positions(self) -> Union[BrokerReadList, BrokerReadFailure]:
        """Retrieves net positions: GET /v2/positions."""
        return self._read_collection("/positions", "positions")

    def get_forever_orders(self) -> Union[BrokerReadList, BrokerReadFailure]:
        """Retrieves all Forever OCO orders: GET /v2/forever/all."""
        return self._read_collection("/forever/all", "forever_orders")

    # =========================================================================
    # SAFETY CORRECTION B & C: Authoritative Broker Protection Verification
    # =========================================================================

    def evaluate_forever_oco_protection(
        self,
        forever_order_id: str,
        expected_security_id: Optional[str] = None,
        expected_qty: Optional[int] = None,
        expected_sl_trigger: Optional[float] = None,
        expected_target_price: Optional[float] = None,
        tick_size: float = 0.05,
        expected_segment: Optional[str] = None,
        expected_product_type: Optional[str] = None,
        forever_orders_snapshot: Optional[Any] = None,
    ) -> ProtectionVerificationResult:
        if not forever_order_id:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                reason="Missing forever_order_id",
            )

        orders = forever_orders_snapshot if forever_orders_snapshot is not None else self.get_forever_orders()
        if isinstance(orders, BrokerReadFailure) or getattr(orders, "ok", True) is False or not isinstance(orders, list):
            err_msg = getattr(orders, "error", "Forever order book read failed or malformed")
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=str(forever_order_id),
                reason=f"Forever order {forever_order_id} unverified: {err_msg}",
            )

        if not orders:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.UNVERIFIED,
                is_verified=False,
                group_id=str(forever_order_id),
                reason=f"Forever order {forever_order_id} not found in broker forever book (empty book)",
            )

        matching_rows = [
            r for r in orders
            if isinstance(r, dict) and str(r.get("orderId", "")) == str(forever_order_id)
        ]
        return verify_forever_oco_group(
            matching_rows=matching_rows,
            forever_order_id=str(forever_order_id),
            expected_security_id=expected_security_id,
            expected_qty=expected_qty,
            expected_sl_trigger=expected_sl_trigger,
            expected_target_price=expected_target_price,
            tick_size=tick_size,
            expected_account_id=self.cfg.dhan_client_id,
            expected_segment=expected_segment,
            expected_product_type=expected_product_type,
        )

    def verify_forever_leg_active(
        self,
        forever_order_id: str,
        expected_security_id: Optional[str] = None,
        expected_qty: Optional[int] = None,
        expected_sl_trigger: Optional[float] = None,
        expected_target_price: Optional[float] = None,
        tick_size: float = 0.05,
        expected_segment: Optional[str] = None,
        expected_product_type: Optional[str] = None,
    ) -> Tuple[bool, str]:
        res = self.evaluate_forever_oco_protection(
            forever_order_id=forever_order_id,
            expected_security_id=expected_security_id,
            expected_qty=expected_qty,
            expected_sl_trigger=expected_sl_trigger,
            expected_target_price=expected_target_price,
            tick_size=tick_size,
            expected_segment=expected_segment,
            expected_product_type=expected_product_type,
        )
        return res.is_verified, res.reason

    def evaluate_super_order_protection(
        self,
        order_id: str,
        expected_security_id: Optional[str] = None,
        expected_qty: Optional[int] = None,
        expected_sl_price: Optional[float] = None,
        expected_target_price: Optional[float] = None,
        tick_size: float = 0.05,
        expected_segment: Optional[str] = None,
        expected_product_type: Optional[str] = None,
        super_orders_snapshot: Optional[Any] = None,
    ) -> ProtectionVerificationResult:
        if not order_id:
            return ProtectionVerificationResult(
                status=ProtectionVerificationStatus.TERMINAL_OR_INVALID,
                is_verified=False,
                is_terminal_or_invalid=True,
                reason="Missing super order_id",
            )

        candidate: Optional[Dict[str, Any]] = None
        super_book = super_orders_snapshot if super_orders_snapshot is not None else self.get_super_orders()
        if isinstance(super_book, list):
            for row in super_book:
                if isinstance(row, dict) and str(row.get("orderId", "")) == str(order_id):
                    candidate = row
                    break

        if candidate is None:
            st = self.order_status(order_id)
            if isinstance(st, dict):
                candidate = st

        return verify_super_order_record(
            candidate=candidate,
            order_id=str(order_id),
            expected_security_id=expected_security_id,
            expected_qty=expected_qty,
            expected_sl_price=expected_sl_price,
            expected_target_price=expected_target_price,
            tick_size=tick_size,
            expected_account_id=self.cfg.dhan_client_id,
            expected_segment=expected_segment,
            expected_product_type=expected_product_type,
        )

    def verify_super_order_legs_active(
        self,
        order_id: str,
        expected_security_id: Optional[str] = None,
        expected_qty: Optional[int] = None,
        expected_sl_price: Optional[float] = None,
        expected_target_price: Optional[float] = None,
        tick_size: float = 0.05,
        expected_segment: Optional[str] = None,
        expected_product_type: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        Safety Correction B: Verifies that a Super Order has both `STOP_LOSS_LEG` and `TARGET_LEG`
        actively resting with required quantities, prices, and identity.
        Never accepts top-level `stopLossPrice`/`targetPrice` alone or `TRADED` legs.
        """
        res = self.evaluate_super_order_protection(
            order_id=order_id,
            expected_security_id=expected_security_id,
            expected_qty=expected_qty,
            expected_sl_price=expected_sl_price,
            expected_target_price=expected_target_price,
            tick_size=tick_size,
            expected_segment=expected_segment,
            expected_product_type=expected_product_type,
        )
        return res.is_verified, res.reason

    def _reconcile_uncertain_submission(self, corr_id: str, expected_security_id: str) -> Tuple[bool, Optional[str], str]:
        found = self.get_order_by_correlation_id(corr_id)
        if not found:
            ob = self.get_order_book()
            if isinstance(ob, list):
                for row in ob:
                    if isinstance(row, dict) and str(row.get("correlationId") or "") == corr_id:
                        found = row
                        break

        if isinstance(found, dict) and found.get("orderId"):
            sec = str(found.get("securityId") or "").strip()
            if sec and expected_security_id and sec != str(expected_security_id):
                return False, None, f"Reconciled correlation {corr_id} has mismatched securityId ({sec} != {expected_security_id})"
            st = str(found.get("orderStatus") or "PENDING").upper()
            if st == "REJECTED":
                return False, None, f"Reconciled order {found['orderId']} was REJECTED at Dhan"
            return True, str(found["orderId"]), f"Order {found['orderId']} confirmed via correlation reconciliation ({st})"

        return False, None, f"Ambiguous/uncertain response for correlation {corr_id}: outcome UNKNOWN (automatic resubmission prohibited)"

    def place_order(self, order_req: OrderRequest, retry: bool = True) -> Tuple[bool, Optional[str], str]:
        if order_req.transaction_type != TransactionType.BUY:
            return False, None, f"MUTATION BLOCKED: Direct {order_req.transaction_type} orders are forbidden on BUY-ONLY entry path"

        corr_id = order_req.correlation_id[:30]
        if hasattr(self.stop_manager, "try_begin_mutation"):
            allowed, st = self.stop_manager.try_begin_mutation(corr_id)
            if not allowed:
                reason_msg = (
                    f"MUTATION BLOCKED: Durable EMERGENCY STOP active "
                    f"(triggered by {st.get('stopped_by')}: {st.get('reason')})"
                )
                logger.critical(f"[STOPPED] {reason_msg}")
                return False, None, reason_msg
        elif self.stop_manager.is_stopped():
            st = self.stop_manager.get_state()
            reason_msg = (
                f"MUTATION BLOCKED: Durable EMERGENCY STOP active "
                f"(triggered by {st.get('stopped_by')}: {st.get('reason')})"
            )
            logger.critical(f"[STOPPED] {reason_msg}")
            return False, None, reason_msg
        else:
            self.stop_manager.register_in_flight(corr_id)

        if self.reconciliation_gate and not self.reconciliation_gate.is_accepting_entries:
            self.stop_manager.deregister_in_flight(corr_id)
            reason_msg = "MUTATION BLOCKED: Startup reconciliation gate is CLOSED (broker state unverified)"
            logger.critical(f"[GATE_CLOSED] {reason_msg}")
            return False, None, reason_msg

        if self.cfg.trading_mode == TradingMode.TEST:
            self.stop_manager.deregister_in_flight(corr_id)
            return False, None, "ORDER BLOCKED: System is in TEST mode (Zero live orders allowed)"

        url = f"{BASE_URL}/orders"
        payload = {
            "dhanClientId": self.cfg.dhan_client_id,
            "correlationId": corr_id,
            "transactionType": "BUY",
            "exchangeSegment": order_req.exchange_segment,
            "productType": order_req.product_type.value,
            "orderType": "LIMIT",
            "validity": "DAY",
            "securityId": str(order_req.security_id),
            "quantity": int(order_req.quantity),
            "price": float(order_req.price),
            "triggerPrice": 0.0,
            "disclosedQuantity": 0,
            "afterMarketOrder": False,
        }

        uncertain_outcome = False
        self.last_order_http_latency_ms = None
        t_order_start_ns = time.perf_counter_ns()
        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            self.last_order_http_latency_ms = round(max(0.0, (time.perf_counter_ns() - t_order_start_ns) / 1_000_000.0), 3)
            if resp.status_code in (200, 201):
                data = resp.json() if resp.text else {}
                order_id = str(data.get("orderId", "")).strip()
                if not order_id:
                    uncertain_outcome = True
                    rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                    if rec_ok and rec_oid:
                        uncertain_outcome = False
                    return rec_ok, rec_oid, rec_msg
                raw_ord_st = str(data.get("orderStatus", "")).upper().strip()
                self.last_order_broker_status = raw_ord_st or "TRANSIT"
                if raw_ord_st == "REJECTED":
                    return False, None, f"Dhan order rejected by RMS: {data}"
                return True, order_id, f"Order acknowledged by Dhan (status={self.last_order_broker_status})"
            elif self.is_token_error(resp.status_code, resp.text):
                uncertain_outcome = True
                if self.cfg.dhan_pin and self.cfg.dhan_totp_secret:
                    logger.warning(f"Dhan auth error on /orders ({corr_id}). Refreshing token for read-reconciliation only (no resubmission)...")
                    self.generate_token_with_totp(force=False, rejected_token=self.token)
                rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                if rec_ok and rec_oid:
                    uncertain_outcome = False
                    return True, rec_oid, rec_msg
                return False, None, f"Dhan auth error ({resp.status_code}) on {corr_id}; {rec_msg}"
            elif resp.status_code >= 500 or resp.status_code == 408:
                uncertain_outcome = True
                rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                if rec_ok and rec_oid:
                    uncertain_outcome = False
                    return True, rec_oid, rec_msg
                return False, None, f"Dhan HTTP {resp.status_code} server error on {corr_id} (outcome UNKNOWN): {resp.text}"
            else:
                return False, None, f"Dhan order placement rejected ({resp.status_code}): {resp.text}"
        except Exception as e:
            uncertain_outcome = True
            return False, None, f"HTTP exception placing order: {e}"
        finally:
            if hasattr(self.stop_manager, "finalize_mutation"):
                self.stop_manager.finalize_mutation(corr_id, keep_persisted_if_uncertain=uncertain_outcome)
            elif not uncertain_outcome:
                self.stop_manager.deregister_in_flight(corr_id)

    # =========================================================================
    # SECTION E & V12-12: Dhan Super Order Placement & Protection Modification
    # =========================================================================

    def place_super_order(
        self,
        order_req: OrderRequest,
        stop_loss_price: float,
        target_price: float,
        trailing_jump: float = 0.0,
        retry: bool = False,
    ) -> Tuple[bool, Optional[str], str]:
        if order_req.transaction_type != TransactionType.BUY:
            return False, None, f"MUTATION BLOCKED: Direct {order_req.transaction_type} super orders are forbidden"

        corr_id = order_req.correlation_id[:30]
        if hasattr(self.stop_manager, "try_begin_mutation"):
            allowed, st = self.stop_manager.try_begin_mutation(corr_id)
            if not allowed:
                reason_msg = (
                    f"MUTATION BLOCKED: Durable EMERGENCY STOP active "
                    f"(triggered by {st.get('stopped_by')}: {st.get('reason')})"
                )
                logger.critical(f"[STOPPED] {reason_msg}")
                return False, None, reason_msg
        elif self.stop_manager.is_stopped():
            st = self.stop_manager.get_state()
            reason_msg = (
                f"MUTATION BLOCKED: Durable EMERGENCY STOP active "
                f"(triggered by {st.get('stopped_by')}: {st.get('reason')})"
            )
            logger.critical(f"[STOPPED] {reason_msg}")
            return False, None, reason_msg
        else:
            self.stop_manager.register_in_flight(corr_id)

        if self.reconciliation_gate and not self.reconciliation_gate.is_accepting_entries:
            self.stop_manager.deregister_in_flight(corr_id)
            reason_msg = "MUTATION BLOCKED: Startup reconciliation gate is CLOSED (broker state unverified)"
            logger.critical(f"[GATE_CLOSED] {reason_msg}")
            return False, None, reason_msg

        if self.cfg.trading_mode == TradingMode.TEST:
            self.stop_manager.deregister_in_flight(corr_id)
            return False, None, "SUPER ORDER BLOCKED: System is in TEST mode (Zero live orders allowed)"

        if stop_loss_price <= 0 or target_price <= 0 or target_price <= stop_loss_price:
            self.stop_manager.deregister_in_flight(corr_id)
            return False, None, f"Invalid Super Order protection levels: SL={stop_loss_price}, Target={target_price}"

        url = f"{BASE_URL}/super/orders"
        payload = {
            "dhanClientId": self.cfg.dhan_client_id,
            "correlationId": corr_id,
            "transactionType": "BUY",
            "exchangeSegment": order_req.exchange_segment,
            "productType": order_req.product_type.value,
            "orderType": "LIMIT",
            "securityId": str(order_req.security_id),
            "quantity": int(order_req.quantity),
            "price": float(order_req.price),
            "targetPrice": float(target_price),
            "stopLossPrice": float(stop_loss_price),
            "trailingJump": float(trailing_jump),
        }

        uncertain_outcome = False
        self.last_order_http_latency_ms = None
        t_order_start_ns = time.perf_counter_ns()
        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            self.last_order_http_latency_ms = round(max(0.0, (time.perf_counter_ns() - t_order_start_ns) / 1_000_000.0), 3)
            if resp.status_code in (200, 201):
                try:
                    data = resp.json() if resp.text else {}
                except Exception:
                    data = {}
                order_id = str(data.get("orderId", "")).strip()
                if not order_id:
                    uncertain_outcome = True
                    rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                    if rec_ok and rec_oid:
                        uncertain_outcome = False
                    return rec_ok, rec_oid, rec_msg
                raw_ord_st = str(data.get("orderStatus", "")).upper().strip()
                self.last_order_broker_status = raw_ord_st or "TRANSIT"
                if raw_ord_st == "REJECTED":
                    return False, None, f"Dhan super order rejected by RMS: {data}"
                return True, order_id, f"Super order acknowledged by Dhan (status={self.last_order_broker_status})"
            elif self.is_token_error(resp.status_code, resp.text):
                uncertain_outcome = True
                if self.cfg.dhan_pin and self.cfg.dhan_totp_secret:
                    logger.warning(
                        f"Dhan token error ({resp.status_code}) on /super/orders ({corr_id}). "
                        f"Refreshing credentials for read-only correlation reconciliation (zero resubmission)..."
                    )
                    self.generate_token_with_totp(force=False, rejected_token=self.token)
                rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                if rec_ok and rec_oid:
                    uncertain_outcome = False
                    return True, rec_oid, rec_msg
                return False, None, f"Dhan super order auth/ambiguous error ({resp.status_code}) for {corr_id}: {rec_msg}"
            elif resp.status_code >= 500 or resp.status_code == 408:
                uncertain_outcome = True
                rec_ok, rec_oid, rec_msg = self._reconcile_uncertain_submission(corr_id, str(order_req.security_id))
                if rec_ok and rec_oid:
                    uncertain_outcome = False
                    return True, rec_oid, rec_msg
                return False, None, f"Dhan HTTP {resp.status_code} server error on super order {corr_id} (outcome UNKNOWN): {resp.text}"
            return False, None, f"Dhan super order rejected ({resp.status_code}): {resp.text}"
        except Exception as e:
            uncertain_outcome = True
            return False, None, f"HTTP exception placing super order: {e}"
        finally:
            if hasattr(self.stop_manager, "finalize_mutation"):
                self.stop_manager.finalize_mutation(corr_id, keep_persisted_if_uncertain=uncertain_outcome)
            elif not uncertain_outcome:
                self.stop_manager.deregister_in_flight(corr_id)

    def modify_super_order_protection(
        self,
        order_id: str,
        stop_loss_price: float,
        target_price: float,
        trailing_jump: float = 0.0,
    ) -> Tuple[bool, str]:
        if self.cfg.trading_mode == TradingMode.TEST:
            return False, "SUPER ORDER MODIFY BLOCKED: System is in TEST mode"

        legs = [
            {
                "dhanClientId": self.cfg.dhan_client_id,
                "orderId": str(order_id),
                "legName": "TARGET_LEG",
                "targetPrice": float(target_price),
            },
            {
                "dhanClientId": self.cfg.dhan_client_id,
                "orderId": str(order_id),
                "legName": "STOP_LOSS_LEG",
                "stopLossPrice": float(stop_loss_price),
                "trailingJump": float(trailing_jump),
            },
        ]
        url = f"{BASE_URL}/super/orders/{order_id}"
        try:
            for body in legs:
                resp = self.session.put(url, headers=self._headers(), json=body, timeout=5)
                if resp.status_code not in (200, 202):
                    return False, f"Failed to modify {body['legName']} ({resp.status_code}): {resp.text}"
                data = resp.json() if resp.text else {}
                if str(data.get("orderStatus", "")).upper() in ("REJECTED", "CANCELLED", "EXPIRED"):
                    return False, f"Dhan rejected {body['legName']} modification: {data}"
            logger.info(f"Dhan super order {order_id}: SL -> {stop_loss_price}, target -> {target_price}")
            return True, f"Super order {order_id} protection legs updated to SL={stop_loss_price}, Target={target_price}"
        except Exception as e:
            return False, f"HTTP exception modifying super order {order_id}: {e}"

    def cancel_super_order_leg(self, order_id: str, leg_name: str = "ENTRY_LEG") -> Tuple[bool, str]:
        if not order_id or not isinstance(order_id, str) or not order_id.strip():
            return False, "Super order cancellation rejected: missing order_id"
        order_id_clean = order_id.strip()
        if not re.match(r"^[a-zA-Z0-9_-]+$", order_id_clean):
            return False, f"Super order cancellation rejected: invalid characters in '{order_id}'"

        if self.cfg.trading_mode == TradingMode.TEST:
            return True, f"TEST mode: simulated super order {leg_name} cancel accepted (HTTP 202)"

        url = f"{BASE_URL}/super/orders/{order_id_clean}/{leg_name}"
        try:
            resp = self.session.delete(url, headers=self._headers(), timeout=5)
            if resp.status_code in (200, 202):
                return True, f"Super order {leg_name} cancellation accepted (HTTP {resp.status_code}) for {order_id_clean}"
            return False, f"Failed to cancel super order {leg_name} ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, f"HTTP exception cancelling super order {leg_name}: {e}"

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        if not order_id or not isinstance(order_id, str) or not order_id.strip():
            return False, "Order cancellation rejected: missing or empty order_id"

        order_id_clean = order_id.strip()
        if not re.match(r"^[a-zA-Z0-9_-]+$", order_id_clean):
            return False, f"Order cancellation rejected: invalid characters in order_id '{order_id}'"

        if self.cfg.trading_mode == TradingMode.TEST:
            return True, "TEST mode: simulated cancel request accepted (HTTP 202)"

        url = f"{BASE_URL}/orders/{order_id_clean}"
        try:
            resp = self.session.delete(url, headers=self._headers(), timeout=5)
            if resp.status_code == 202:
                return True, f"Order cancellation request accepted by broker (HTTP 202) for {order_id_clean}"
            elif resp.status_code == 200:
                return True, f"Order cancellation request acknowledged by broker (HTTP 200) for {order_id_clean}"
            elif resp.status_code == 401:
                return False, f"Dhan auth error (401): {resp.text}"
            elif resp.status_code == 429:
                return False, f"Dhan rate limit exceeded (429): {resp.text}"
            return False, f"Failed to cancel order ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, f"HTTP exception cancelling order: {e}"

    def place_forever_oco(
        self,
        correlation_id: str,
        security_id: str,
        exchange_segment: str,
        product_type: str,
        levels: ProtectionLevels,
        max_protectable_qty: Optional[int] = None,
        tick_size: float = 0.05,
    ) -> Tuple[bool, Optional[str], str]:
        if self.cfg.trading_mode == TradingMode.TEST:
            return False, None, "FOREVER OCO BLOCKED: System is in TEST mode"

        prod_upper = product_type.upper()
        if prod_upper not in ("CNC", "MTF"):
            err = f"Forever OCO unsupported for product '{product_type}'. Dhan v2 Forever orders require CNC or MTF."
            logger.critical(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        qty = int(levels.quantity)
        if qty <= 0:
            err = f"Forever OCO rejected: invalid quantity {qty} <= 0"
            logger.error(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        if max_protectable_qty is not None and qty > max_protectable_qty:
            err = (
                f"Forever OCO rejected: protective quantity ({qty}) exceeds confirmed net open position "
                f"({max_protectable_qty}) — oversell blocked"
            )
            logger.critical(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        sl_limit = float(levels.sl_limit)
        sl_trigger = float(levels.sl_trigger)
        target_price = float(levels.target_price)
        target_trigger = float(getattr(levels, "target_trigger", None) or levels.target_price)

        if sl_limit <= 0 or sl_trigger <= 0 or target_price <= 0 or target_trigger <= 0:
            err = "Forever OCO rejected: all price and trigger fields must be > 0"
            logger.error(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        if sl_limit > sl_trigger:
            err = f"Forever OCO rejected: SELL stop-loss limit price ({sl_limit}) must be <= trigger price ({sl_trigger})"
            logger.error(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        if target_price <= sl_trigger:
            err = f"Forever OCO rejected: target price ({target_price}) must be > stop-loss trigger ({sl_trigger})"
            logger.error(f"[PROTECTION_REJECTED] {err}")
            return False, None, err

        active_tick = tick_size if tick_size and tick_size > 0 else 0.05

        def _is_tick_aligned(p: float, tick: float = active_tick) -> bool:
            steps = round(p / tick, 4)
            return abs(steps - round(steps)) < 1e-4

        for name, p in [
            ("sl_limit", sl_limit),
            ("sl_trigger", sl_trigger),
            ("target_price", target_price),
            ("target_trigger", target_trigger),
        ]:
            if not _is_tick_aligned(p):
                err = f"Forever OCO rejected: {name} ({p}) is not aligned to tick grid ({active_tick})"
                logger.error(f"[PROTECTION_REJECTED] {err}")
                return False, None, err

        url = f"{BASE_URL}/forever/orders"
        payload = {
            "dhanClientId": self.cfg.dhan_client_id,
            "correlationId": correlation_id[:30],
            "orderFlag": "OCO",
            "transactionType": "SELL",
            "exchangeSegment": exchange_segment,
            "productType": prod_upper,
            "orderType": "LIMIT",
            "validity": "DAY",
            "securityId": str(security_id),
            "quantity": qty,
            "disclosedQuantity": 0,
            "price": sl_limit,
            "triggerPrice": sl_trigger,
            "price1": target_price,
            "triggerPrice1": target_trigger,
            "quantity1": qty,
        }

        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            if resp.status_code in (200, 201):
                data = resp.json()
                order_status = str(data.get("orderStatus", "PENDING")).upper()
                if order_status == "REJECTED":
                    return False, None, f"Forever OCO rejected by broker RMS: {data}"
                forever_id = str(data.get("orderId", "")).strip()
                if not forever_id:
                    return False, None, f"Ambiguous Forever OCO response missing orderId for {correlation_id}: outcome UNKNOWN"
                return True, forever_id, "Forever OCO placed successfully on Dhan"
            elif resp.status_code >= 500 or resp.status_code == 408:
                return False, None, f"Dhan HTTP {resp.status_code} server error on Forever OCO {correlation_id} (outcome UNKNOWN): {resp.text}"
            return False, None, f"Forever OCO rejected ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, None, f"HTTP exception placing Forever OCO: {e}"

    def order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.cfg.trading_mode == TradingMode.TEST:
            return {"orderStatus": "TRADED", "filledQty": 0, "averageTradedPrice": 0.0}

        url = f"{BASE_URL}/orders/{order_id}"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, dict) else None
            logger.warning(f"Failed to fetch Dhan order status for {order_id} ({resp.status_code}): {resp.text}")
            return None
        except Exception as e:
            logger.error(f"HTTP exception fetching order status for {order_id}: {e}")
            return None

    def trades_avg_price(self, order_id: str) -> Optional[float]:
        if self.cfg.trading_mode == TradingMode.TEST:
            return None

        url = f"{BASE_URL}/trades/{order_id}"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                trades = resp.json()
                if isinstance(trades, list) and trades:
                    total_val = sum(float(t.get("tradedPrice", 0.0)) * int(t.get("tradedQuantity", 0)) for t in trades if isinstance(t, dict))
                    total_qty = sum(int(t.get("tradedQuantity", 0)) for t in trades if isinstance(t, dict))
                    if total_qty > 0:
                        return round(total_val / total_qty, 4)
            return None
        except Exception as e:
            logger.error(f"HTTP exception fetching trades for {order_id}: {e}")
            return None
