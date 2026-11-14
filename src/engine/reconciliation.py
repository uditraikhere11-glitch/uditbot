"""
Startup & Runtime Reconciliation Gate Engine — V13 Safety-Corrected Edition (CV13-01..05).
Implements STEP 15 (15-A through 15-F) + Safety Corrections A, B, C & CV13-05:
- Holds ACCEPTING_NEW_ENTRIES=False until broker state is fully verified.
- Safety Correction C.A: Requires `get_trade_book()`, `get_positions()`, and (when needed)
  `get_order_book()`, `get_forever_orders()`, and `get_super_orders()` to return valid structured
  list responses (`OK_EMPTY` / `OK_RECORDS`), blocking startup on `FAILED`, `MALFORMED`, or `UNAVAILABLE` reads.
- CV13-05: Strictly validates every position record before evaluating coverage:
  * `netQty` must be present and non-null, accepting documented valid integer representations (`int` or integer `str`)
    while rejecting booleans, non-finite values, fractional values, negative values, and malformed numeric strings.
  * Requires explicit `securityId`, `exchangeSegment`, and `productType` identity on every position record
    (never inferring missing exposure quantities as zero or guessing missing segment/product identity).
- Safety Correction C.B: Computes open-position coverage strictly from current, authoritatively validated,
  deduplicated broker protection groups (`verify_forever_oco_group` / `verify_super_order_record`).
  * Never credits historical or local `protected_qty` without active broker evidence.
  * Never double-counts local coverage + broker coverage for the same protection.
  * Counts a paired two-leg Forever OCO or Super Order once by its paired quantity (e.g. 5 shares = 5, not 10).
  * Rejects coverage from mismatched account, securityId, exchangeSegment, or productType.
- Safety Correction C.C: Preserves controlled recovery (blocks fresh entries, reports exact instrument,
  exposure, confirmed coverage, and unresolved evidence, and never auto-flattens unknown positions).
"""

from enum import Enum
import json
import logging
import math
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from datetime import datetime
import threading
import zoneinfo
from src.models import BrokerReadFailure, ProtectionRoute, infer_record_protection_route
from src.broker.dhan_client import (
    DhanClient,
    MAX_HISTORICAL_TRADE_WINDOW_DAYS,
    verify_forever_oco_group,
    verify_super_order_record,
)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

logger = logging.getLogger(__name__)

VALID_POSITION_SEGMENTS = frozenset({"NSE_EQ", "BSE_EQ", "NSE_FNO", "BSE_FNO"})
VALID_POSITION_PRODUCTS = frozenset({"CNC", "MTF", "MARGIN", "INTRADAY"})
_INT_STR_RE = re.compile(r"^[+-]?\d+$")


class ReconciliationState(str, Enum):
    INITIALIZING = "INITIALIZING"
    RECONCILING = "RECONCILING"
    HEALTHY = "HEALTHY"
    BLOCKED = "BLOCKED"


def validate_position_record(pos: Any, expected_account_id: str = "") -> Tuple[bool, int, str]:
    """
    CV13-05: Validates a single broker position record before evaluating coverage.
    - Rejects non-dict records.
    - Requires `securityId`, `exchangeSegment`, and `productType` identity.
    - Requires `netQty` to be present and non-null; accepts valid integer representations (`int` or integer `str`)
      and rejects booleans, non-finite values, fractional values, negative quantities, and malformed numeric strings.
    """
    if not isinstance(pos, dict):
        return False, 0, "Malformed non-dict entry in broker positions response"

    raw_sec = pos.get("securityId")
    if raw_sec is None or isinstance(raw_sec, bool):
        return False, 0, "Malformed broker position record: missing or invalid 'securityId'"
    sec_id = str(raw_sec).strip()
    if not sec_id or sec_id.lower() in ("none", "null"):
        return False, 0, "Malformed broker position record: empty 'securityId'"

    raw_seg = pos.get("exchangeSegment")
    if raw_seg is None or isinstance(raw_seg, bool):
        return False, 0, f"Malformed broker position record for securityId={sec_id}: missing 'exchangeSegment'"
    seg = str(raw_seg).strip().upper()
    if seg not in VALID_POSITION_SEGMENTS:
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: invalid or missing 'exchangeSegment' ({raw_seg!r})",
        )

    raw_prod = pos.get("productType")
    if raw_prod is None or isinstance(raw_prod, bool):
        return False, 0, f"Malformed broker position record for securityId={sec_id}: missing 'productType'"
    prod = str(raw_prod).strip().upper()
    if prod not in VALID_POSITION_PRODUCTS:
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: invalid or missing 'productType' ({raw_prod!r})",
        )

    if "dhanClientId" in pos and pos.get("dhanClientId") is not None:
        if isinstance(pos.get("dhanClientId"), bool):
            return False, 0, f"Malformed broker position record for securityId={sec_id}: boolean 'dhanClientId'"
        pos_acct = str(pos.get("dhanClientId") or "").strip()
        if pos_acct and expected_account_id and pos_acct != expected_account_id:
            return (
                False,
                0,
                f"Open position account mismatch for securityId={sec_id} ({pos_acct} != {expected_account_id})",
            )

    if "netQty" not in pos or pos["netQty"] is None:
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: missing or null 'netQty'",
        )

    raw_qty = pos["netQty"]
    if isinstance(raw_qty, bool):
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: boolean 'netQty' ({raw_qty!r}) is invalid",
        )
    if isinstance(raw_qty, float):
        if not math.isfinite(raw_qty):
            return (
                False,
                0,
                f"Malformed broker position record for securityId={sec_id}: non-finite 'netQty' ({raw_qty!r})",
            )
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: fractional/float 'netQty' ({raw_qty!r})",
        )
    if isinstance(raw_qty, int):
        net_qty = raw_qty
    elif isinstance(raw_qty, str):
        s_qty = raw_qty.strip()
        if not s_qty or not _INT_STR_RE.match(s_qty):
            return (
                False,
                0,
                f"Malformed broker position record for securityId={sec_id}: invalid numeric string 'netQty' ({raw_qty!r})",
            )
        net_qty = int(s_qty)
    else:
        return (
            False,
            0,
            f"Malformed broker position record for securityId={sec_id}: unsupported 'netQty' type {type(raw_qty).__name__}",
        )

    if net_qty < 0:
        return (
            False,
            net_qty,
            f"Unsupported negative 'netQty' ({net_qty}) for securityId={sec_id}",
        )

    return True, net_qty, "OK"


class StartupReconciliationGate:
    def __init__(self, broker: Any, journal_db: Any, watcher: Any, stop_manager: Any):
        self.broker = broker
        self.db = journal_db
        self.watcher = watcher
        self.stop_manager = stop_manager
        self.state = ReconciliationState.INITIALIZING
        self._accepting_entries: bool = False
        self.last_reconciled_at: float = 0.0
        self.last_error: str = ""
        self.unresolved_count: int = 0
        self._reconcile_lock = threading.Lock()
        self._entry_state_lock = threading.Lock()
        self._halt_generation = 0
        self.last_notified_gate_state: str = ""
        self.last_historical_reconciled_ids: List[str] = []
        # Do not run 31-day, multi-page broker history reads on the two-second
        # ProtectionWatcher thread. Only one bounded background recheck may run.
        self._historical_background_lock = threading.Lock()
        self._historical_background_retry_after = 0.0

    @property
    def is_accepting_entries(self) -> bool:
        """Trading gate: returns True ONLY if broker state has been verified healthy and STOP is inactive."""
        return self._accepting_entries and not self.stop_manager.is_stopped()

    def halt_new_entries(self, reason: str) -> None:
        """
        11-F / 15-E: Immediately closes the entry gate when unprotected fill exposure,
        an UNKNOWN order outcome, or a runtime broker reconciliation failure is detected.
        """
        with self._entry_state_lock:
            self._halt_generation += 1
            self._accepting_entries = False
            self.state = ReconciliationState.BLOCKED
            self.last_error = reason
        logger.critical(f"🛑 [RECONCILIATION GATE HALTED] Blocking all new entries: {reason}")
        if hasattr(self.db, "record_system_event"):
            try:
                self.db.record_system_event("RECONCILIATION_GATE_HALTED", reason)
            except Exception:
                pass

    def is_healthy_for_resume(self) -> bool:
        """04-E: Checks whether reconciliation state has zero unresolved intents or protection before clearing STOP."""
        if hasattr(self.db, "get_non_terminal_intents"):
            if len(self.db.get_non_terminal_intents()) > 0:
                return False
        if hasattr(self.db, "get_unprotected_orders"):
            if len(self.db.get_unprotected_orders()) > 0:
                return False
        if hasattr(self.db, "get_unresolved_protection_attempts"):
            if len(self.db.get_unresolved_protection_attempts()) > 0:
                return False
        return True

    def _read_required_broker_collection(
        self,
        method_name: str,
        label: str,
    ) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        Safety Correction C.A: Calls `getattr(self.broker, method_name)()` and distinguishes:
        - Successful response with zero records (`[]` / `OK_EMPTY`)
        - Successful response with records (`[dict, ...]` / `OK_RECORDS`)
        - Failed, malformed, or unavailable response (`BrokerReadFailure`, `None`, non-list, or non-dict items)
        """
        fn = getattr(self.broker, method_name, None)
        if not callable(fn):
            return False, [], f"Required broker read method '{method_name}' is unavailable"
        try:
            res = fn()
        except Exception as e:
            return False, [], f"Broker {label} query raised exception: {e}"

        if isinstance(res, BrokerReadFailure) or getattr(res, "ok", True) is False:
            status_tag = getattr(res, "status", "FAILED")
            err_detail = getattr(res, "error", "Broker read failed")
            return False, [], f"Broker {label} read failed ({status_tag}): {err_detail}"

        if not isinstance(res, list) or any(not isinstance(item, dict) for item in res):
            return (
                False,
                [],
                f"Broker {label} read returned malformed payload (expected list of dicts, got {type(res).__name__})",
            )

        return True, list(res), "OK"

    def _track_or_merge_intent_in_watcher(self, intent: Dict[str, Any], resolved_broker_id: str, st: Dict[str, Any]) -> None:
        if not self.watcher or not hasattr(self.watcher, "track"):
            return

        seg = str(intent.get("exchange_segment") or "NSE_EQ")
        prod = str(intent.get("product_type") or "CNC")
        resolved_route = infer_record_protection_route(intent)
        is_super = (resolved_route == ProtectionRoute.SUPER_ORDER.value)

        try:
            self.watcher.track(
                order_id=resolved_broker_id,
                correlation_id=intent.get("correlation_id") or resolved_broker_id,
                symbol=intent["symbol"],
                trading_symbol=intent.get("trading_symbol") or intent["symbol"],
                security_id=str(intent["security_id"]),
                segment=seg,
                qty=int(intent["quantity"]),
                product_type=prod,
                original_market_price=float(intent.get("original_market_price") or intent.get("price") or 0.0),
                is_super_order=is_super,
                is_amo=bool(int(intent.get("is_amo") or 0)),
                protection_route=resolved_route,
                tick_size=float(intent.get("tick_size") or 0.05),
                lot_size=int(intent.get("lot_size") or 1),
                option_type=str(intent.get("option_type") or ""),
                strike=float(intent.get("strike") or 0.0),
                expiry=intent.get("expiry") or "",
                preserve_existing=True,
            )
        except TypeError:
            self.watcher.track(
                order_id=resolved_broker_id,
                symbol=intent["symbol"],
                security_id=intent["security_id"],
                segment=seg,
                qty=intent["quantity"],
                product_type=prod,
            )

    def _validate_broker_order_matches_intent(
        self,
        st: Dict[str, Any],
        intent: Optional[Dict[str, Any]],
        corr_id: str,
        broker_oid: Optional[str],
    ) -> Tuple[bool, str]:
        if not isinstance(st, dict) or not st.get("orderId"):
            return False, "Missing orderId in broker order payload"

        got_oid = str(st.get("orderId") or "").strip()
        exp_oid = str(broker_oid or "").strip()
        if exp_oid and got_oid != exp_oid:
            return False, f"Broker orderId mismatch ({got_oid} != {exp_oid})"

        expected_acct = str(
            (intent or {}).get("account_id")
            or getattr(getattr(self.broker, "cfg", None), "dhan_client_id", "")
            or getattr(self.db, "account_id", "")
            or ""
        ).strip()
        got_acct = str(st.get("dhanClientId") or "").strip()
        if got_acct and expected_acct and got_acct != expected_acct:
            return False, f"Broker order account mismatch ({got_acct} != {expected_acct})"

        if isinstance(intent, dict):
            exp_sec = str(intent.get("security_id") or "").strip()
            got_sec = str(st.get("securityId") or "").strip()
            if exp_sec and got_sec and got_sec != exp_sec:
                return False, f"Broker order securityId mismatch ({got_sec} != {exp_sec})"

            exp_seg = str(intent.get("exchange_segment") or "").strip().upper()
            got_seg = str(st.get("exchangeSegment") or "").strip().upper()
            if exp_seg and got_seg and got_seg != exp_seg:
                return False, f"Broker order exchangeSegment mismatch ({got_seg} != {exp_seg})"

            exp_prod = str(intent.get("product_type") or "").strip().upper()
            got_prod = str(st.get("productType") or "").strip().upper()
            if exp_prod and got_prod and got_prod != exp_prod:
                return False, f"Broker order productType mismatch ({got_prod} != {exp_prod})"

            got_side = str(st.get("transactionType") or "").strip().upper()
            if got_side and got_side != "BUY":
                return False, f"Broker order transactionType mismatch ({got_side} != BUY)"

            if st.get("quantity") is not None and intent.get("quantity") is not None:
                try:
                    if int(st["quantity"]) != int(intent["quantity"]):
                        return False, f"Broker order quantity mismatch ({st['quantity']} != {intent['quantity']})"
                except (ValueError, TypeError):
                    return False, f"Broker order invalid quantity ({st.get('quantity')!r})"

        return True, "OK"

    def _lookup_intent_at_broker(
        self,
        corr_id: str,
        broker_oid: Optional[str],
        order_book: List[Dict[str, Any]],
        intent: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_order_by_correlation_id"):
            try:
                res = self.broker.get_order_by_correlation_id(corr_id)
                if isinstance(res, dict) and res.get("orderId"):
                    candidates.append(res)
            except Exception:
                pass

        if order_book:
            for ob_row in order_book:
                if isinstance(ob_row, dict) and (
                    ob_row.get("correlationId") == corr_id
                    or (broker_oid and str(ob_row.get("orderId")) == str(broker_oid))
                ):
                    candidates.append(ob_row)
                    break

        if hasattr(self.broker, "order_status"):
            target_id = broker_oid or corr_id
            try:
                res = self.broker.order_status(target_id)
                if isinstance(res, dict) and res.get("orderId"):
                    candidates.append(res)
            except Exception:
                pass

        for cand in candidates:
            ok_match, reason = self._validate_broker_order_matches_intent(cand, intent, corr_id, broker_oid)
            if ok_match:
                return cand
            logger.error(f"[RECONCILIATION] Rejected broker order candidate for {corr_id}: {reason}")
        return None

    def _compute_verified_oco_for_order(
        self,
        *,
        security_id: str,
        exchange_segment: str,
        product_type: str,
        expected_account_id: str,
        forever_orders: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[bool, int, str]:
        if forever_orders is None:
            if not hasattr(self.broker, "get_forever_orders"):
                return True, 0, "No get_forever_orders method"
            ok_fo, fo_list, fo_msg = self._read_required_broker_collection("get_forever_orders", "forever_orders")
            if not ok_fo:
                return False, 0, fo_msg
            forever_orders = fo_list

        groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in forever_orders:
            if not isinstance(row, dict):
                return False, 0, "Malformed non-dict record in forever_orders"
            gid = str(row.get("orderId") or "").strip()
            if not gid:
                return False, 0, "Malformed Forever OCO record missing orderId"
            groups.setdefault(gid, []).append(row)

        covered = 0
        matched_gids: List[str] = []
        for gid, grp_rows in groups.items():
            row_sec = str(grp_rows[0].get("securityId") or "").strip()
            if row_sec != str(security_id).strip():
                continue
            ver = verify_forever_oco_group(
                matching_rows=grp_rows,
                forever_order_id=gid,
                expected_security_id=str(security_id).strip(),
                expected_account_id=expected_account_id or None,
                expected_segment=str(exchange_segment).strip().upper(),
                expected_product_type=str(product_type).strip().upper(),
            )
            if ver.is_verified and ver.protected_quantity > 0:
                covered += int(ver.protected_quantity)
                matched_gids.append(gid)
        self._last_matched_oco_ids = matched_gids
        return True, covered, "OK"


    def _preflight_zero_id_historical_evidence(
        self,
        eligible: List[Dict[str, Any]],
        trades_by_oid: Dict[str, List[Dict[str, Any]]],
        full_history: List[Dict[str, Any]],
        expected_account_id: str,
        now_ts: float,
    ) -> Optional[str]:
        """R01: All zero-ID candidates pass before any journal fill is written.

        Exact broker profile + one authenticated settled holdings snapshot are
        required. Aggregate all bot-owned shares per security, never separately
        credit the same holdings quantity to two one-share orders.
        """
        missing_id_intents = []
        required_by_security: Dict[str, int] = {}
        isins_by_security: Dict[str, str] = {}
        seen_exchange_orders: Dict[str, str] = {}
        today_ist = datetime.fromtimestamp(now_ts, tz=IST).date()

        for row in full_history:
            if not row.get("_broker_attested_no_execution_id"):
                continue
            eoid = str(row.get("exchangeOrderId") or "").strip()
            oid = str(row.get("orderId") or "").strip()
            if not eoid or eoid in ("0", "None", "null"):
                return "Historical zero-ID record has no exchange order ID"
            if eoid in seen_exchange_orders and seen_exchange_orders[eoid] != oid:
                return "Exchange order ID reused across different historical broker orders"
            seen_exchange_orders[eoid] = oid

        for intent in eligible:
            oid = str(intent.get("broker_order_id") or "").strip()
            candidate_rows = trades_by_oid.get(oid, [])
            if not any(row.get("_broker_attested_no_execution_id") for row in candidate_rows):
                continue
            if len(candidate_rows) != 1:
                return f"Order {oid}: multiple/mixed historical records with absent exchange trade IDs"
            row = candidate_rows[0]
            seg = str(intent.get("exchange_segment") or "").upper()
            prod = str(intent.get("product_type") or "").upper()
            if seg not in ("NSE_EQ", "BSE_EQ") or prod != "CNC":
                return f"Order {oid}: zero-ID fallback restricted to CNC cash equity"
            qty = int(intent.get("quantity") or 0)
            if qty <= 0 or int(row.get("tradedQuantity") or 0) != qty:
                return f"Order {oid}: missing-ID record is not a complete single fill"

            raw_time = str(row.get("exchangeTime") or "")
            try:
                exchange_dt = datetime.fromisoformat(
                    raw_time.replace("Z", "+00:00").replace(" ", "T")
                )
                if exchange_dt.tzinfo is None:
                    exchange_dt = exchange_dt.replace(tzinfo=IST)
                exchange_ts = exchange_dt.timestamp()
                trade_date = datetime.fromtimestamp(exchange_ts, tz=IST).date()
                order_ts = float(intent.get("created_at") or 0)
                intent_date = datetime.fromtimestamp(order_ts, tz=IST).date()
            except (ValueError, TypeError, OverflowError):
                return f"Order {oid}: invalid historical exchange time"
            if (
                not order_ts
                or trade_date != intent_date
                or trade_date > today_ist
                or exchange_ts + 60 < order_ts
            ):
                return f"Order {oid}: exchange time does not match submitted order date"
            if str(row.get("dhanClientId") or "") != expected_account_id:
                return f"Order {oid}: historical account identity mismatch"
            if str(row.get("securityId") or "") != str(intent.get("security_id") or ""):
                return f"Order {oid}: historical security identity mismatch"
            sec = str(intent["security_id"])
            row_isin = str(row.get("isin") or "").strip().upper()
            if not row_isin:
                return f"Order {oid}: missing ISIN required for corroborating broker holdings"
            if sec in isins_by_security and row_isin != isins_by_security[sec]:
                return f"Order {oid}: conflicting ISIN for the same security ID"
            isins_by_security[sec] = row_isin
            required_by_security[sec] = required_by_security.get(sec, 0) + qty
            missing_id_intents.append(intent)

        if not missing_id_intents:
            return None

        profile_fn = getattr(self.broker, "get_authenticated_profile_id", None)
        if not callable(profile_fn):
            return "Broker profile identity verification unavailable for zero-ID historical proof"
        try:
            profile_acct = str(
                profile_fn(bypass_test_mode=True, allow_refresh=False) or ""
            ).strip()
        except Exception as exc:
            return f"Broker profile identity verification failed: {type(exc).__name__}"
        if profile_acct != expected_account_id:
            return "Broker /profile account missing or differs from historical account"

        ok_hold, holdings, hold_err = self._read_required_broker_collection(
            "get_holdings", "holdings"
        )
        if not ok_hold:
            return f"Missing-ID historical evidence cannot verify holdings: {hold_err}"
        holdings_by_security: Dict[str, int] = {}
        for holding in holdings:
            sec = str(holding.get("securityId") or "").strip()
            if sec not in required_by_security:
                continue
            if holding.get("dhanClientId") is not None and (
                str(holding["dhanClientId"]).strip() != expected_account_id
            ):
                return f"Broker holdings account mismatch for security {sec}"
            if str(holding.get("isin") or "").strip().upper() != isins_by_security[sec]:
                return f"Broker holdings ISIN mismatch/missing for security {sec}"
            raw_dp_qty = holding.get("dpQty")
            if isinstance(raw_dp_qty, bool) or not str(raw_dp_qty).strip().isdigit():
                return f"Invalid settled dpQty in broker holdings for security {sec}"
            holdings_by_security[sec] = (
                holdings_by_security.get(sec, 0) + int(raw_dp_qty)
            )

        # Include previously reconciled bot-owned lots on the same security.
        # This prevents the same two DP shares from satisfying four bot orders.
        if self.db and hasattr(self.db, "get_open_positions_lifecycle"):
            try:
                previous_lots = self.db.get_open_positions_lifecycle()
            except Exception:
                return "Cannot inspect existing bot-owned settled position ledger"
            if not isinstance(previous_lots, list):
                return "Malformed bot-owned position ledger"
            candidate_oids = {
                str(r.get("broker_order_id") or "") for r in missing_id_intents
            }
            for row in previous_lots:
                sec = str(row.get("security_id") or "").strip()
                if (
                    sec in required_by_security
                    and str(row.get("parent_order_id") or "") not in candidate_oids
                    and str(row.get("product_type") or "").upper() == "CNC"
                ):
                    required_by_security[sec] += max(
                        0, int(row.get("confirmed_open_qty") or 0)
                    )

        for sec, expected_qty in required_by_security.items():
            if holdings_by_security.get(sec, 0) < expected_qty:
                return (
                    f"Insufficient verified settled broker holdings for security {sec}: "
                    f"DP={holdings_by_security.get(sec, 0)}, bot-owned required={expected_qty}"
                )
        return None

    def _reconcile_unresolved_intents_via_historical_trades(
        self,
        unresolved_intent_rows: List[Dict[str, Any]],
        *,
        now_ts: Optional[float] = None,
    ) -> Tuple[List[str], List[str], Optional[str]]:
        """
        H01-H15: Bounded historical trade fallback for intents absent from today's `/v2/orders`.
        Returns `(resolved_full_corr_ids, remaining_unresolved_reasons, fatal_error_or_none)`.
        """
        if not unresolved_intent_rows:
            return [], [], None

        hist_fn = getattr(self.broker, "get_historical_trades", None)
        if not callable(hist_fn):
            return [], [f"{r['correlation_id']} (unverified at broker)" for r in unresolved_intent_rows], None

        eval_now = float(now_ts if now_ts is not None else time.time())
        today_ist = datetime.fromtimestamp(eval_now, tz=IST).date()
        expected_acct = str(
            getattr(getattr(self.broker, "cfg", None), "dhan_client_id", "")
            or getattr(self.db, "account_id", "")
            or ""
        ).strip()

        eligible: List[Dict[str, Any]] = []
        eligible_dates = []
        remaining_reasons: List[str] = []

        for intent in unresolved_intent_rows:
            cid = str(intent.get("correlation_id") or "")
            boid = str(intent.get("broker_order_id") or "").strip()
            if not boid:
                remaining_reasons.append(
                    f"Intent {cid} (state={intent.get('state')}) missing broker_order_id; cannot verify via historical trades (manual review required)"
                )
                continue
            raw_created = float(intent.get("created_at") or eval_now)
            created_ist_date = datetime.fromtimestamp(raw_created, tz=IST).date()
            age_days = (today_ist - created_ist_date).days
            if age_days > MAX_HISTORICAL_TRADE_WINDOW_DAYS:
                remaining_reasons.append(
                    f"Intent {cid} (orderId={boid}, date={created_ist_date}) exceeds {MAX_HISTORICAL_TRADE_WINDOW_DAYS}-day historical trade retention window; manual review required"
                )
                continue
            eligible.append(intent)
            eligible_dates.append(min(created_ist_date, today_ist))

        if not eligible:
            return [], remaining_reasons, None

        from_date_str = min(eligible_dates).strftime("%Y-%m-%d")
        to_date_str = today_ist.strftime("%Y-%m-%d")

        try:
            hist_res = hist_fn(from_date_str, to_date_str, bypass_test_mode=True)
        except TypeError:
            try:
                hist_res = hist_fn(from_date_str, to_date_str)
            except Exception as exc:
                return [], remaining_reasons, f"Historical trade query raised exception ({from_date_str}..{to_date_str}): {exc}"
        except Exception as exc:
            return [], remaining_reasons, f"Historical trade query raised exception ({from_date_str}..{to_date_str}): {exc}"

        if isinstance(hist_res, BrokerReadFailure) or getattr(hist_res, "ok", True) is False:
            err_detail = getattr(hist_res, "error", "Historical trade read failed")
            return [], remaining_reasons, f"Historical trade reconciliation failed ({from_date_str}..{to_date_str}): {err_detail}"

        if not isinstance(hist_res, list):
            # Unconfigured MagicMock in legacy unit tests that do not mock get_historical_trades
            for intent in eligible:
                remaining_reasons.append(f"Intent {intent['correlation_id']} (state: {intent.get('state')}) unverified at broker")
            return [], remaining_reasons, None

        trades_by_oid: Dict[str, List[Dict[str, Any]]] = {}
        for idx, raw_tr in enumerate(hist_res):
            if not isinstance(raw_tr, dict):
                return [], remaining_reasons, f"Malformed non-dict historical trade record #{idx}"
            tr_acct = str(raw_tr.get("dhanClientId") or "").strip()
            if expected_acct and tr_acct != expected_acct:
                return (
                    [],
                    remaining_reasons,
                    f"Historical trade account mismatch on record #{idx}: expected {expected_acct!r}, got {tr_acct!r}",
                )
            oid_key = str(raw_tr.get("orderId") or "").strip()
            if oid_key:
                trades_by_oid.setdefault(oid_key, []).append(raw_tr)

        preflight_error = self._preflight_zero_id_historical_evidence(
            eligible, trades_by_oid, hist_res, expected_acct, eval_now,
        )
        if preflight_error:
            return [], remaining_reasons, (
                "Historical missing-execution-ID verification blocked: " + preflight_error
            )

        resolved_full_cids: List[str] = []
        for intent in eligible:
            cid = str(intent["correlation_id"])
            boid = str(intent["broker_order_id"]).strip()
            sym = str(intent.get("symbol") or "")
            sec_id = str(intent.get("security_id") or "").strip()
            seg = str(intent.get("exchange_segment") or "NSE_EQ").strip().upper()
            prod = str(intent.get("product_type") or "CNC").strip().upper()
            req_qty = int(intent.get("quantity") or 0)
            intent_acct = str(intent.get("account_id") or expected_acct).strip()

            cand_trades = trades_by_oid.get(boid, [])
            if not cand_trades:
                # H03 & H15: Never infer fill from holdings aggregate alone or assume cancelled!
                remaining_reasons.append(
                    f"Intent {cid} (orderId={boid}) absent from current order book and historical trade book ({from_date_str}..{to_date_str})"
                )
                continue

            validated_for_order: List[Dict[str, Any]] = []
            validation_err: Optional[str] = None
            for tr in cand_trades:
                ok_v, norm_tr, v_err = DhanClient.validate_historical_trade_record(
                    tr,
                    expected_account_id=intent_acct,
                    expected_order_id=boid,
                    expected_security_id=sec_id,
                    expected_exchange_segment=seg,
                    expected_product_type=prod,
                    expected_transaction_type="BUY",
                    allow_missing_exchange_trade_id=True,
                )
                if not ok_v or norm_tr is None:
                    validation_err = v_err
                    break
                validated_for_order.append(norm_tr)

            if validation_err or not validated_for_order:
                remaining_reasons.append(
                    f"Intent {cid} (orderId={boid}) rejected invalid historical trade proof: {validation_err or 'no valid trades'}"
                )
                continue

            try:
                if validated_for_order[0].get("_broker_attested_no_execution_id"):
                    # Preflight already rejected multi-row/mixed-ID fills,
                    # incomplete pagination, account and settled-share mismatches.
                    tx_res = self.db.apply_attested_historical_order_fill(
                        correlation_id=cid,
                        broker_order_id=boid,
                        expected_account_id=intent_acct,
                        symbol=sym,
                        security_id=sec_id,
                        exchange_segment=seg,
                        product_type=prod,
                        requested_quantity=req_qty,
                        attested_trade=validated_for_order[0],
                        now=eval_now,
                    )
                else:
                    tx_res = self.db.apply_verified_historical_fills(
                        correlation_id=cid,
                        broker_order_id=boid,
                        expected_account_id=intent_acct,
                        symbol=sym,
                        security_id=sec_id,
                        exchange_segment=seg,
                        product_type=prod,
                        requested_quantity=req_qty,
                        verified_trades=validated_for_order,
                        now=eval_now,
                    )
            except Exception as db_exc:
                return (
                    [],
                    remaining_reasons,
                    f"Failed to persist historical fills for {cid} (orderId={boid}): {db_exc}",
                )

            filled_q = int(tx_res["filled_quantity"])
            vwap_p = float(tx_res["vwap"])
            resolved_st = str(tx_res["resolved_state"])

            ok_fo, ver_prot_q, _ = self._compute_verified_oco_for_order(
                security_id=sec_id,
                exchange_segment=seg,
                product_type=prod,
                expected_account_id=intent_acct,
            )
            if not ok_fo:
                ver_prot_q = 0

            is_man_ack = (
                hasattr(self.db, "is_manual_risk_acknowledged")
                and self.db.is_manual_risk_acknowledged(
                    boid,
                    cid,
                    expected_account_id=intent_acct,
                    expected_security_id=sec_id,
                    required_qty=filled_q,
                )
            )
            acct_info = self.db.get_position_accounting(boid) if hasattr(self.db, "get_position_accounting") else {}
            closed_q = int(acct_info.get("sell_filled_qty") or 0)

            if self.watcher and hasattr(self.watcher, "reconcile_historical_order_fills"):
                self.watcher.reconcile_historical_order_fills(
                    order_id=boid,
                    correlation_id=cid,
                    symbol=sym,
                    security_id=sec_id,
                    segment=seg,
                    product_type=prod,
                    requested_qty=req_qty,
                    filled_qty=filled_q,
                    avg_fill_price=vwap_p,
                    verified_protected_qty=min(filled_q, ver_prot_q),
                    verified_closed_qty=closed_q,
                    confirmed_protection_ids=list(getattr(self, "_last_matched_oco_ids", []) or []),
                    manual_risk_acknowledged=is_man_ack,
                    is_amo=bool(int(intent.get("is_amo") or 0)),
                    protection_route=str(intent.get("protection_route") or ""),
                    original_market_price=float(intent.get("original_market_price") or intent.get("price") or vwap_p),
                    now=eval_now,
                )

            if hasattr(self.stop_manager, "deregister_in_flight"):
                try:
                    self.stop_manager.deregister_in_flight(cid)
                except Exception:
                    pass

            if resolved_st == "TRADED" and filled_q == req_qty:
                resolved_full_cids.append(cid)
                self.last_historical_reconciled_ids.append(boid)
                logger.info(
                    "[RECONCILIATION] Historical recovery %s orderId=%s -> TRADED "
                    "(%s/%s at %.4f; evidence=%s)",
                    cid, boid, filled_q, req_qty, vwap_p,
                    tx_res.get("evidence_source", "EXCHANGE_TRADE_ID"),
                )
            else:
                # H07: Partial historical fill preserves unresolved remainder and blocks startup
                remaining_reasons.append(
                    f"Intent {cid} (orderId={boid}) partially filled ({filled_q}/{req_qty}) in historical trades; remainder ({req_qty - filled_q}) unverified"
                )

        return resolved_full_cids, remaining_reasons, None

    def reconcile_runtime_unknown_intents(self) -> int:
        if not self.db:
            return 0
        try:
            if hasattr(self.db, "get_non_terminal_intents"):
                unknowns = [
                    r for r in self.db.get_non_terminal_intents()
                    if str(r.get("state") or "").upper() in ("UNKNOWN", "ACKED", "PENDING")
                ]
            elif hasattr(self.db, "get_unknown_intents"):
                unknowns = self.db.get_unknown_intents()
            else:
                return 0
        except Exception:
            return 0
        if not unknowns:
            return 0

        order_book: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_order_book"):
            ok_ob, ob_list, _ = self._read_required_broker_collection("get_order_book", "order_book")
            if ok_ob:
                order_book = ob_list

        resolved_count = 0
        still_unresolved: List[Dict[str, Any]] = []
        for intent in unknowns:
            corr_id = intent["correlation_id"]
            broker_oid = intent.get("broker_order_id")
            st = self._lookup_intent_at_broker(corr_id, broker_oid, order_book, intent=intent)
            if isinstance(st, dict) and st.get("orderId"):
                order_status = str(st.get("orderStatus", "PENDING")).upper()
                resolved_broker_id = str(st.get("orderId"))
                filled_qty = int(st.get("filledQty") or 0)
                self.db.update_order_intent(corr_id, state=order_status, broker_order_id=resolved_broker_id)
                if hasattr(self.stop_manager, "deregister_in_flight"):
                    self.stop_manager.deregister_in_flight(corr_id)
                if order_status not in ("CANCELLED", "REJECTED", "EXPIRED") or filled_qty > 0:
                    self._track_or_merge_intent_in_watcher(intent, resolved_broker_id, st)
                resolved_count += 1
            elif str(intent.get("state") or "").upper() != "UNKNOWN" and broker_oid:
                still_unresolved.append(intent)

        if still_unresolved:
            # History pagination can take many seconds; never run it inside
            # ProtectionWatcher.check_once(). Schedule a serialized, bounded
            # full gate recheck instead. It remains fail-closed during recovery.
            self._schedule_historical_background_recheck()

        return resolved_count

    def _schedule_historical_background_recheck(self) -> None:
        if self.is_accepting_entries or not self.db:
            return
        now = time.monotonic()
        if now < self._historical_background_retry_after:
            return
        if not self._historical_background_lock.acquire(blocking=False):
            return
        # Cool down even after a failed API read; avoids per-poll API storms.
        self._historical_background_retry_after = now + 600.0

        def _worker() -> None:
            try:
                ok, detail = self.recheck_and_recover_if_verified()
                if not ok:
                    logger.warning("[RECONCILIATION] Background historical recovery remains blocked: %s", detail)
            except Exception as exc:
                logger.exception("[RECONCILIATION] Background historical recheck failed: %s", exc)
            finally:
                self._historical_background_lock.release()

        threading.Thread(
            target=_worker,
            name="dhan-history-reconciliation",
            daemon=True,
        ).start()

    def _verify_open_positions_protected(self, positions: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Safety Correction C.B, C.C & CV13-05:
        Validates every position record strictly before evaluating coverage, then verifies that every
        open broker position (`netQty > 0`) is covered by current, authoritatively validated,
        deduplicated broker protection records (`forever_orders` or `super_orders`).
        """
        if not isinstance(positions, list):
            return False, "Broker positions payload is not a valid list"
        if not positions:
            return True, "No open positions"

        expected_acct = str(
            getattr(getattr(self.broker, "cfg", None), "dhan_client_id", "")
            or getattr(self.db, "account_id", "")
            or ""
        ).strip()

        open_positions: List[Tuple[Dict[str, Any], int]] = []
        for pos in positions:
            valid_rec, net_qty, val_msg = validate_position_record(pos, expected_account_id=expected_acct)
            if not valid_rec:
                return False, val_msg
            if net_qty > 0:
                open_positions.append((pos, net_qty))

        if not open_positions:
            return True, "No open long positions"

        # When open exposure exists, required protection reads must succeed
        forever_orders: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_forever_orders"):
            ok_fo, fo_list, fo_msg = self._read_required_broker_collection("get_forever_orders", "forever_orders")
            if not ok_fo:
                return False, fo_msg
            forever_orders = fo_list

        super_orders: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_super_orders"):
            ok_so, so_list, so_msg = self._read_required_broker_collection("get_super_orders", "super_orders")
            if not ok_so:
                return False, so_msg
            super_orders = so_list

        # Group Forever OCO rows by stable orderId so two-leg pairs are validated and counted once
        forever_groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in forever_orders:
            gid = str(row.get("orderId") or "").strip()
            if not gid:
                return False, "Malformed Forever OCO record missing orderId"
            forever_groups.setdefault(gid, []).append(row)

        # Group Super Orders by stable orderId
        super_groups: Dict[str, Dict[str, Any]] = {}
        for row in super_orders:
            gid = str(row.get("orderId") or "").strip()
            if not gid:
                return False, "Malformed Super Order record missing orderId"
            super_groups[gid] = row

        used_forever_ids: Set[str] = set()
        used_super_ids: Set[str] = set()

        # Index local watcher records by security_id ONLY for metadata/reconciliation (never for coverage credit!)
        pending_map = getattr(self.watcher, "pending", None)
        local_pending_by_sec: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(pending_map, dict):
            for rec in pending_map.values():
                if isinstance(rec, dict):
                    s = str(rec.get("security_id") or "").strip()
                    if s:
                        local_pending_by_sec.setdefault(s, []).append(rec)

        for pos, net_qty in open_positions:
            sec_id = str(pos["securityId"]).strip()
            pos_seg = str(pos["exchangeSegment"]).strip().upper()
            pos_prod = str(pos["productType"]).strip().upper()
            pos_sym = str(pos.get("tradingSymbol") or pos.get("symbol") or sec_id).strip()

            local_recs = local_pending_by_sec.get(sec_id, [])

            covered_qty = 0
            unresolved_evidence: List[str] = []

            # 1. Validate Forever OCO groups for this securityId, segment, and productType
            for gid, grp_rows in forever_groups.items():
                if gid in used_forever_ids:
                    continue
                row_sec = str(grp_rows[0].get("securityId") or "").strip()
                if row_sec != sec_id:
                    continue

                ver = verify_forever_oco_group(
                    matching_rows=grp_rows,
                    forever_order_id=gid,
                    expected_security_id=sec_id,
                    expected_account_id=expected_acct or None,
                    expected_segment=pos_seg,
                    expected_product_type=pos_prod,
                )
                if ver.is_verified and ver.protected_quantity > 0:
                    used_forever_ids.add(gid)
                    covered_qty += ver.protected_quantity
                else:
                    unresolved_evidence.append(f"Forever OCO {gid}: {ver.reason}")

            # 2. Validate Super Order groups for this securityId, segment, and productType
            for gid, so_row in super_groups.items():
                if gid in used_super_ids:
                    continue
                row_sec = str(so_row.get("securityId") or "").strip()
                if row_sec != sec_id:
                    continue

                ver = verify_super_order_record(
                    candidate=so_row,
                    order_id=gid,
                    expected_security_id=sec_id,
                    expected_account_id=expected_acct or None,
                    expected_segment=pos_seg,
                    expected_product_type=pos_prod,
                )
                if ver.is_verified and ver.protected_quantity > 0:
                    used_super_ids.add(gid)
                    covered_qty += ver.protected_quantity
                else:
                    unresolved_evidence.append(f"Super Order {gid}: {ver.reason}")

            # Sync down any stale local protected_qty if broker coverage is lower than local claim
            for rec in local_recs:
                if int(rec.get("protected_qty") or 0) > covered_qty:
                    rec["protected_qty"] = covered_qty

            if covered_qty < net_qty:
                ev_str = f", unresolved_evidence=[{'; '.join(unresolved_evidence)}]" if unresolved_evidence else ""
                return (
                    False,
                    f"Unprotected open broker position detected for {pos_sym} "
                    f"(securityId={sec_id}, segment={pos_seg}, product={pos_prod}, "
                    f"netQty={net_qty}, verified_protected_qty={covered_qty}{ev_str})",
                )

        return True, "All open broker positions have verified protective coverage"

    def _verify_bot_owned_holdings_protected(
        self,
        intraday_positions: List[Dict[str, Any]],
    ) -> Tuple[bool, str]:
        """
        H08 & H16: Verifies that bot-owned settled CNC/MTF holdings (from historically reconciled
        fills in `positions_lifecycle` where `confirmed_open_qty > 0` and not already accounted for
        in intraday `positions`) have either:
        1. Active broker-verified Forever OCO coverage (`verify_forever_oco_group`), OR
        2. An explicitly audited, narrowly scoped owner manual-risk acknowledgement (`UNPROTECTED_MANUAL_RISK`).
        Unrelated manual holdings in the user's Dhan account are never treated as bot-owned.
        """
        if not self.db or not hasattr(self.db, "get_open_positions_lifecycle"):
            return False, "Cannot verify settled holdings: positions_lifecycle storage unavailable"

        try:
            open_lifecycle_rows = self.db.get_open_positions_lifecycle()
            if not isinstance(open_lifecycle_rows, list):
                return False, "Cannot verify settled holdings: malformed positions_lifecycle response"
        except Exception as exc:
            return False, f"Cannot verify settled holdings: positions_lifecycle query failed: {exc}"

        # Filter to CNC/MTF bot-owned open positions that were historically reconciled (or settled into holdings)
        intraday_sec_ids = {
            str(p.get("securityId") or "").strip()
            for p in (intraday_positions or [])
            if isinstance(p, dict) and int(p.get("netQty") or 0) > 0
        }
        bot_settled_candidates = [
            row for row in open_lifecycle_rows
            if str(row.get("product_type") or "").strip().upper() in ("CNC", "MTF")
            and int(row.get("confirmed_open_qty") or 0) > 0
            and str(row.get("security_id") or "").strip() not in intraday_sec_ids
            and (
                str(row.get("parent_order_id") or "") in self.last_historical_reconciled_ids
                or str(row.get("position_state") or "").upper() in ("UNPROTECTED", "UNPROTECTED_MANUAL_RISK")
            )
        ]
        if not bot_settled_candidates:
            return True, "No settled bot-owned holdings requiring separate verification"

        expected_acct = str(
            getattr(getattr(self.broker, "cfg", None), "dhan_client_id", "")
            or getattr(self.db, "account_id", "")
            or ""
        ).strip()

        # Check live holdings if get_holdings is available on broker
        holdings_by_sec: Dict[str, int] = {}
        has_holdings_api = False
        if hasattr(self.broker, "get_holdings"):
            try:
                h_res = self.broker.get_holdings()
                if isinstance(h_res, BrokerReadFailure) or getattr(h_res, "ok", True) is False:
                    err_h = getattr(h_res, "error", "Broker holdings read failed")
                    return False, f"Broker holdings read failed while verifying settled bot positions: {err_h}"
                if isinstance(h_res, list):
                    has_holdings_api = True
                    for h in h_res:
                        if isinstance(h, dict):
                            s_id = str(h.get("securityId") or "").strip()
                            t_qty = int(h.get("totalQty") if h.get("totalQty") is not None else (h.get("dpQty") or 0))
                            if s_id:
                                holdings_by_sec[s_id] = holdings_by_sec.get(s_id, 0) + max(0, t_qty)
            except Exception as exc:
                return False, f"Broker holdings query failed while verifying settled bot positions: {exc}"

        if not has_holdings_api:
            return False, "Cannot verify settled holdings: broker holdings API unavailable or malformed"
        required_by_security: Dict[str, int] = {}
        for row in bot_settled_candidates:
            sec = str(row.get("security_id") or "").strip()
            required_by_security[sec] = required_by_security.get(sec, 0) + int(row.get("confirmed_open_qty") or 0)
        for sec, required_qty in required_by_security.items():
            if holdings_by_sec.get(sec, 0) < required_qty:
                return False, (
                    f"Bot-owned settled holding quantity cannot be verified for securityId={sec}: "
                    f"sumOfBotOpenQty={required_qty}, brokerHoldingsQty={holdings_by_sec.get(sec, 0)}"
                )

        forever_orders: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_forever_orders"):
            ok_fo, fo_list, fo_msg = self._read_required_broker_collection("get_forever_orders", "forever_orders")
            if not ok_fo:
                return False, fo_msg
            forever_orders = fo_list

        forever_groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in forever_orders:
            gid = str(row.get("orderId") or "").strip()
            if not gid:
                return False, "Malformed Forever OCO record missing orderId"
            forever_groups.setdefault(gid, []).append(row)

        remaining_forever_qty: Dict[str, int] = {}
        invalid_forever_ids: Set[str] = set()
        for row in bot_settled_candidates:
            p_oid = str(row.get("parent_order_id") or "").strip()
            sym = str(row.get("symbol") or "").strip()
            sec_id = str(row.get("security_id") or "").strip()
            seg = str(row.get("exchange_segment") or "NSE_EQ").strip().upper()
            prod = str(row.get("product_type") or "CNC").strip().upper()
            open_q = int(row.get("confirmed_open_qty") or 0)

            covered_q = 0
            matched_gids: List[str] = []
            for gid, grp_rows in forever_groups.items():
                if gid in invalid_forever_ids:
                    continue
                if str(grp_rows[0].get("securityId") or "").strip() != sec_id:
                    continue
                if gid not in remaining_forever_qty:
                    ver = verify_forever_oco_group(
                        matching_rows=grp_rows,
                        forever_order_id=gid,
                        expected_security_id=sec_id,
                        expected_account_id=expected_acct or None,
                        expected_segment=seg,
                        expected_product_type=prod,
                    )
                    if ver.is_verified and ver.protected_quantity > 0:
                        remaining_forever_qty[gid] = int(ver.protected_quantity)
                    else:
                        invalid_forever_ids.add(gid)
                        continue
                rem_q = remaining_forever_qty.get(gid, 0)
                if rem_q > 0 and covered_q < open_q:
                    alloc_q = min(open_q - covered_q, rem_q)
                    covered_q += alloc_q
                    remaining_forever_qty[gid] = rem_q - alloc_q
                    matched_gids.append(gid)

            if covered_q >= open_q:
                if self.watcher and hasattr(self.watcher, "reconcile_historical_order_fills"):
                    existing_w = (getattr(self.watcher, "pending", None) or {}).get(p_oid) or {}
                    w_cid = str(existing_w.get("correlation_id") or p_oid)
                    self.watcher.reconcile_historical_order_fills(
                        order_id=p_oid,
                        correlation_id=w_cid,
                        symbol=sym,
                        security_id=sec_id,
                        segment=seg,
                        product_type=prod,
                        requested_qty=int(row.get("entry_qty") or open_q),
                        filled_qty=int(row.get("buy_filled_qty") or open_q),
                        avg_fill_price=float(row.get("buy_vwap") or 0.0),
                        verified_protected_qty=min(open_q, covered_q),
                        verified_closed_qty=int(row.get("sell_filled_qty") or 0),
                    )
                continue

            is_man_ack = (
                hasattr(self.db, "is_manual_risk_acknowledged")
                and self.db.is_manual_risk_acknowledged(
                    p_oid,
                    p_oid,
                    expected_account_id=expected_acct,
                    expected_security_id=sec_id,
                    required_qty=open_q,
                )
            )
            if is_man_ack:
                logger.warning(
                    f"[RECONCILIATION] Settled bot holding for {sym} (orderId={p_oid}, qty={open_q}) "
                    f"is in audited UNPROTECTED_MANUAL_RISK state (owner acknowledged)."
                )
                continue

            holding_q_note = f", brokerHoldingsQty={holdings_by_sec.get(sec_id, 0)}" if has_holdings_api else ""
            return (
                False,
                f"Unprotected bot-owned settled holding detected for {sym} "
                f"(orderId={p_oid}, securityId={sec_id}, segment={seg}, product={prod}, "
                f"openQty={open_q}, verified_protected_qty={covered_q}{holding_q_note})",
            )

        return True, "All bot-owned settled holdings verified"

    def _restore_committed_historical_watcher_records(self) -> None:
        """Repair a crash AFTER verified SQLite commit but BEFORE watcher persistence.

        Use only durable, transactionally committed historical proof events. A
        missing/stale watcher is restored with ZERO assumed protection; active
        broker legs are revalidated separately before the gate can open.
        """
        if not self.db or not hasattr(self.db, "_get_conn"):
            return
        with self.db._get_conn() as conn:
            ev_rows = conn.execute(
                "SELECT payload FROM system_events WHERE event_type = 'HISTORICAL_FILL_RECONCILED'"
            ).fetchall()

        for ev in ev_rows:
            proof = json.loads(ev["payload"])
            oid = str(proof.get("broker_order_id") or "").strip()
            cid = str(proof.get("correlation_id") or "").strip()
            if not oid or not cid:
                raise ValueError("Incomplete committed historical fill proof event")
            with self.db._get_conn() as conn:
                intent = conn.execute(
                    "SELECT * FROM order_intents WHERE correlation_id=? AND broker_order_id=? LIMIT 1",
                    (cid, oid),
                ).fetchone()
                if intent is None:
                    raise ValueError(f"Historical proof missing corresponding intent ({cid})")
            current_acct = str(getattr(self.db, "account_id", "") or "").strip()
            intent_acct = str(intent["account_id"] or "").strip()
            if current_acct and intent_acct and current_acct != intent_acct:
                raise ValueError(f"Historical proof account mismatch on {oid}")
            accounting = self.db.get_position_accounting(oid)
            verified_qty = int(accounting.get("buy_filled_qty") or 0)
            expected_qty = int(intent["quantity"] or 0)
            if verified_qty <= 0 or verified_qty > expected_qty:
                raise ValueError(f"Historical proof fill quantity invalid on {oid}")
            existing = (getattr(self.watcher, "pending", {}) or {}).get(oid) or (
                getattr(self.watcher, "terminal_orders", {}) or {}
            ).get(oid) if self.watcher else None
            position = self.db.get_position_lifecycle(oid)
            if (existing and position
                    and int(existing.get("filled_qty") or 0) == verified_qty
                    and int(position.get("buy_filled_qty") or 0) == verified_qty):
                continue
            if not self.watcher or not hasattr(self.watcher, "reconcile_historical_order_fills"):
                raise ValueError(f"Cannot restore historical watcher for {oid}: watcher unavailable")
            acked = (hasattr(self.db, "is_manual_risk_acknowledged") and
                     self.db.is_manual_risk_acknowledged(
                         oid, cid, expected_account_id=current_acct,
                         expected_security_id=str(intent["security_id"]),
                         required_qty=max(0, verified_qty - int(accounting.get("sell_filled_qty") or 0)),
                     ))
            self.watcher.reconcile_historical_order_fills(
                order_id=oid,
                correlation_id=cid,
                symbol=str(intent["symbol"]),
                security_id=str(intent["security_id"]),
                segment=str(intent["exchange_segment"]),
                product_type=str(intent["product_type"]),
                requested_qty=expected_qty,
                filled_qty=verified_qty,
                avg_fill_price=float(accounting.get("buy_vwap") or 0.0),
                verified_protected_qty=0,
                verified_closed_qty=int(accounting.get("sell_filled_qty") or 0),
                manual_risk_acknowledged=bool(acked),
                is_amo=bool(int(intent["is_amo"] or 0)),
                protection_route=str(intent["protection_route"] or ""),
                original_market_price=float(intent["original_market_price"] or intent["price"] or 0.0),
            )
            logger.warning(
                "[RECONCILIATION] Rebuilt watcher state from committed verified historical fills: %s",
                oid,
            )

    def run_startup_reconciliation(self) -> Tuple[bool, str]:
        """
        Executes fail-closed startup reconciliation before listener accepts live trades.
        """
        with self._reconcile_lock:
            logger.info("🔄 [RECONCILIATION] Starting broker-authoritative startup reconciliation...")
            with self._entry_state_lock:
                starting_halt_generation = self._halt_generation
                self.state = ReconciliationState.RECONCILING
                self._accepting_entries = False
            self.last_historical_reconciled_ids = []

            # 0. Repair any interrupted SQLite fill/order state before querying broker (H09)
            if self.db and hasattr(self.db, "repair_interrupted_fill_states"):
                try:
                    self.db.repair_interrupted_fill_states()
                except Exception as rep_exc:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Failed to repair interrupted SQLite fill state: {rep_exc}"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error

            # H09 post-commit crash window: fill journal may have committed just
            # before the process died, leaving watcher/lifecycle unpersisted.
            try:
                self._restore_committed_historical_watcher_records()
            except Exception as restore_exc:
                self.state = ReconciliationState.BLOCKED
                self.last_error = f"Historical watcher restoration failed: {restore_exc}"
                logger.critical("[RECONCILIATION] %s", self.last_error)
                return False, self.last_error

            # 1. Check emergency stop first (H13: allow historical read-only recovery first if intents pending,
            #    but NEVER open the entry gate when Emergency STOP is active!)
            stop_active_at_start = bool(self.stop_manager.is_stopped())

            # 2. Verify broker connection and fund limits
            try:
                ok, avail, msg = self.broker.get_fund_limits()
                if not ok:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Broker funds check failed: {msg}"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error
            except Exception as e:
                self.state = ReconciliationState.BLOCKED
                self.last_error = f"Broker connection exception: {e}"
                logger.critical(f"[RECONCILIATION] {self.last_error}")
                return False, self.last_error

            # 3. Query broker trade book and positions (Safety Correction C.A: fail closed on read error/malformed)
            if hasattr(self.broker, "get_trade_book"):
                ok_tb, trade_book, tb_msg = self._read_required_broker_collection("get_trade_book", "trade_book")
                if not ok_tb:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = tb_msg
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error

                for trade in trade_book:
                    if trade.get("orderId") and trade.get("exchangeTradeId") and hasattr(self.db, "record_fill"):
                        try:
                            self.db.record_fill(
                                broker_order_id=str(trade["orderId"]),
                                exchange_trade_id=str(trade["exchangeTradeId"]),
                                symbol=str(trade.get("tradingSymbol") or trade.get("customSymbol") or ""),
                                traded_price=float(trade.get("tradedPrice") or 0.0),
                                traded_quantity=int(trade.get("tradedQuantity") or 0),
                                transaction_type=str(trade.get("transactionType") or "BUY").upper(),
                                security_id=str(trade.get("securityId") or ""),
                                exchange_segment=str(trade.get("exchangeSegment") or ""),
                                product_type=str(trade.get("productType") or ""),
                                is_verified_trade=True,
                            )
                        except Exception as fill_exc:
                            # P1: Never silently swallow fill persistence failures!
                            self.state = ReconciliationState.BLOCKED
                            self.last_error = f"Failed to persist broker trade fill for order {trade.get('orderId')}: {fill_exc}"
                            logger.critical(f"[RECONCILIATION] {self.last_error}")
                            if hasattr(self.db, "record_system_event"):
                                try:
                                    self.db.record_system_event("FILL_PERSISTENCE_ERROR", self.last_error)
                                except Exception:
                                    pass
                            return False, self.last_error

            positions: List[Dict[str, Any]] = []
            if hasattr(self.broker, "get_positions"):
                ok_pos, pos_list, pos_msg = self._read_required_broker_collection("get_positions", "positions")
                if not ok_pos:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = pos_msg
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error
                positions = pos_list

            # 4. Reconcile all non-terminal intents from journal (current-day book first, then bounded historical fallback)
            try:
                if hasattr(self.db, "get_non_terminal_intents"):
                    pending_intents = self.db.get_non_terminal_intents()
                else:
                    pending_intents = self.db.get_unknown_intents()

                unresolved_intents: List[str] = []
                needs_historical_fallback: List[Dict[str, Any]] = []
                if pending_intents:
                    logger.warning(f"[RECONCILIATION] Found {len(pending_intents)} pending intent(s) to reconcile.")
                    order_book: List[Dict[str, Any]] = []
                    if hasattr(self.broker, "get_order_book"):
                        ok_ob, ob_list, _ = self._read_required_broker_collection("get_order_book", "order_book")
                        if ok_ob:
                            order_book = ob_list

                    for intent in pending_intents:
                        corr_id = intent["correlation_id"]
                        broker_oid = intent.get("broker_order_id")
                        st = self._lookup_intent_at_broker(corr_id, broker_oid, order_book, intent=intent)

                        if isinstance(st, dict) and st.get("orderId"):
                            order_status = str(st.get("orderStatus", "PENDING")).upper()
                            resolved_broker_id = str(st.get("orderId"))
                            filled_qty = int(st.get("filledQty") or 0)
                            self.db.update_order_intent(corr_id, state=order_status, broker_order_id=resolved_broker_id)
                            logger.info(f"[RECONCILIATION] Resolved intent {corr_id} -> {order_status} (Broker ID: {resolved_broker_id})")

                            existing_in_watcher = (
                                hasattr(self.watcher, "pending")
                                and isinstance(self.watcher.pending, dict)
                                and resolved_broker_id in self.watcher.pending
                            )
                            if order_status not in ("CANCELLED", "REJECTED", "EXPIRED") or filled_qty > 0 or existing_in_watcher:
                                self._track_or_merge_intent_in_watcher(intent, resolved_broker_id, st)
                        else:
                            needs_historical_fallback.append(intent)

                    if needs_historical_fallback:
                        _, hist_unres_reasons, hist_fatal_err = self._reconcile_unresolved_intents_via_historical_trades(
                            needs_historical_fallback
                        )
                        if hist_fatal_err:
                            self.state = ReconciliationState.BLOCKED
                            self.last_error = hist_fatal_err
                            self.unresolved_count = len(needs_historical_fallback)
                            logger.critical(f"[RECONCILIATION] {self.last_error}")
                            return False, self.last_error
                        for reason_str in hist_unres_reasons:
                            unresolved_intents.append(reason_str)
                            logger.warning(f"[RECONCILIATION] {reason_str}")

                    self.unresolved_count = len(unresolved_intents)
                    if unresolved_intents:
                        self.state = ReconciliationState.BLOCKED
                        self.last_error = (
                            f"Unresolved order intent(s) present: {len(unresolved_intents)} unverified order(s) require broker reconciliation "
                            f"({'; '.join(unresolved_intents)})"
                        )
                        logger.critical(f"[RECONCILIATION] {self.last_error}")
                        return False, self.last_error
                else:
                    self.unresolved_count = 0
            except Exception as e:
                self.state = ReconciliationState.BLOCKED
                self.last_error = f"Journal intent reconciliation exception: {e}"
                logger.critical(f"[RECONCILIATION] {self.last_error}")
                return False, self.last_error

            # H13: If Emergency STOP is active, read-only historical fill recovery above was permitted,
            #      but the new-entry gate MUST remain BLOCKED!
            if stop_active_at_start or self.stop_manager.is_stopped():
                self.state = ReconciliationState.BLOCKED
                self.last_error = "Durable EMERGENCY STOP is active"
                logger.warning(f"[RECONCILIATION] {self.last_error}. Remaining in fail-closed state.")
                return False, self.last_error

            # 5. Check existing pending protection orders in watcher
            if self.watcher and hasattr(self.watcher, "check_once"):
                try:
                    self.watcher.check_once()
                except Exception as e:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Watcher reconciliation pass failed: {e}"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error

            # 6a. Verify all open intraday broker positions are covered by active broker protection (Safety Correction C.B & CV13-05)
            pos_ok, pos_msg = self._verify_open_positions_protected(positions)
            if not pos_ok:
                self.state = ReconciliationState.BLOCKED
                self.last_error = pos_msg
                logger.critical(f"[RECONCILIATION] {self.last_error}")
                return False, self.last_error

            # 6b. Verify all bot-owned settled CNC/MTF holdings have active Forever OCO protection or audited manual-risk acknowledgement (H08 & H16)
            hld_ok, hld_msg = self._verify_bot_owned_holdings_protected(positions)
            if not hld_ok:
                self.state = ReconciliationState.BLOCKED
                self.last_error = hld_msg
                logger.critical(f"[RECONCILIATION] {self.last_error}")
                return False, self.last_error

            # 7. Verify no UNPROTECTED fills or unresolved protection attempts remain in journal
            if hasattr(self.db, "get_unprotected_orders"):
                unprot = self.db.get_unprotected_orders()
                if unprot:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Unprotected filled position(s) present: {len(unprot)} order(s) lack active broker SL/Target"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error

            if hasattr(self.db, "get_unresolved_protection_attempts"):
                unres_att = self.db.get_unresolved_protection_attempts()
                if unres_att:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Unresolved protection attempt(s) present: {len(unres_att)} attempt(s) awaiting broker verification"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error

            # 8. Invariant check passed -> open the gate
            with self._entry_state_lock:
                if self._halt_generation != starting_halt_generation or self.stop_manager.is_stopped():
                    self._accepting_entries = False
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = self.last_error or "Safety halt occurred during reconciliation"
                    return False, self.last_error
                self._accepting_entries = True
                self.state = ReconciliationState.HEALTHY
                self.last_reconciled_at = time.time()
                self.last_error = ""
            logger.info("✅ [RECONCILIATION] Invariant checks passed! Gate opened: ACCEPTING_NEW_ENTRIES=True.")
            return True, "Reconciliation successful, system healthy and accepting new trades."

    def recheck_and_recover_if_verified(self) -> Tuple[bool, str]:
        """
        Section 3.7: Bounded, serialized runtime recheck so the reconciliation gate can recover
        and emit a deduplicated transition alert once broker status and journal persistence are verified,
        without requiring repeated manual service restarts.
        """
        prev_state = self.state
        ok, msg = self.run_startup_reconciliation()
        new_state = self.state
        if prev_state != new_state and self.db and hasattr(self.db, "record_system_event"):
            ev_type = "RECONCILIATION_GATE_RECOVERED" if ok else "RECONCILIATION_GATE_BLOCKED"
            try:
                self.db.record_system_event(ev_type, f"Reconciliation gate transitioned {prev_state.value} -> {new_state.value}: {msg}")
            except Exception:
                pass
        return ok, msg
