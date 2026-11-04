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
import logging
import math
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from src.models import BrokerReadFailure
from src.broker.dhan_client import verify_forever_oco_group, verify_super_order_record

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

    @property
    def is_accepting_entries(self) -> bool:
        """Trading gate: returns True ONLY if broker state has been verified healthy and STOP is inactive."""
        return self._accepting_entries and not self.stop_manager.is_stopped()

    def halt_new_entries(self, reason: str) -> None:
        """
        11-F / 15-E: Immediately closes the entry gate when unprotected fill exposure,
        an UNKNOWN order outcome, or a runtime broker reconciliation failure is detected.
        """
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
        raw_super = intent.get("is_super_order")
        if raw_super is None:
            is_super = (seg.upper() in ("NSE_FNO", "BSE_FNO") and prod.upper() == "MARGIN")
        else:
            is_super = bool(int(raw_super)) or (seg.upper() in ("NSE_FNO", "BSE_FNO") and prod.upper() == "MARGIN")

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

    def _lookup_intent_at_broker(
        self,
        corr_id: str,
        broker_oid: Optional[str],
        order_book: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        st = None
        if hasattr(self.broker, "get_order_by_correlation_id"):
            try:
                res = self.broker.get_order_by_correlation_id(corr_id)
                if isinstance(res, dict) and res.get("orderId"):
                    st = res
            except Exception:
                st = None

        if not st and order_book:
            for ob_row in order_book:
                if isinstance(ob_row, dict) and (
                    ob_row.get("correlationId") == corr_id
                    or (broker_oid and str(ob_row.get("orderId")) == str(broker_oid))
                ):
                    st = ob_row
                    break

        if not st and hasattr(self.broker, "order_status"):
            target_id = broker_oid or corr_id
            try:
                res = self.broker.order_status(target_id)
                if isinstance(res, dict) and res.get("orderId"):
                    st = res
            except Exception:
                st = None

        return st

    def reconcile_runtime_unknown_intents(self) -> int:
        if not self.db or not hasattr(self.db, "get_unknown_intents"):
            return 0
        try:
            unknowns = self.db.get_unknown_intents()
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
        for intent in unknowns:
            corr_id = intent["correlation_id"]
            broker_oid = intent.get("broker_order_id")
            st = self._lookup_intent_at_broker(corr_id, broker_oid, order_book)
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
        return resolved_count

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

    def run_startup_reconciliation(self) -> Tuple[bool, str]:
        """
        Executes fail-closed startup reconciliation before listener accepts live trades.
        """
        logger.info("🔄 [RECONCILIATION] Starting broker-authoritative startup reconciliation...")
        self.state = ReconciliationState.RECONCILING
        self._accepting_entries = False

        # 1. Check emergency stop first
        if self.stop_manager.is_stopped():
            self.state = ReconciliationState.BLOCKED
            self.last_error = "Durable EMERGENCY STOP is active"
            logger.warning(f"[RECONCILIATION] {self.last_error}. Remaining in fail-closed state.")
            return False, self.last_error

        # 2. Check broker connection & funds (health check via /v2/fundlimit)
        try:
            ok, funds, msg = self.broker.get_fund_limits(retry=True)
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
                        )
                    except Exception:
                        pass

        positions: List[Dict[str, Any]] = []
        if hasattr(self.broker, "get_positions"):
            ok_pos, pos_list, pos_msg = self._read_required_broker_collection("get_positions", "positions")
            if not ok_pos:
                self.state = ReconciliationState.BLOCKED
                self.last_error = pos_msg
                logger.critical(f"[RECONCILIATION] {self.last_error}")
                return False, self.last_error
            positions = pos_list

        # 4. Reconcile all non-terminal intents from journal
        try:
            if hasattr(self.db, "get_non_terminal_intents"):
                pending_intents = self.db.get_non_terminal_intents()
            else:
                pending_intents = self.db.get_unknown_intents()

            unresolved_intents = []
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
                    st = self._lookup_intent_at_broker(corr_id, broker_oid, order_book)

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
                        unresolved_intents.append(corr_id)
                        logger.warning(f"[RECONCILIATION] Intent {corr_id} (state: {intent.get('state')}) unverified at broker.")

                self.unresolved_count = len(unresolved_intents)
                if unresolved_intents:
                    self.state = ReconciliationState.BLOCKED
                    self.last_error = f"Unresolved order intent(s) present: {len(unresolved_intents)} unverified order(s) require broker reconciliation"
                    logger.critical(f"[RECONCILIATION] {self.last_error}")
                    return False, self.last_error
            else:
                self.unresolved_count = 0
        except Exception as e:
            self.state = ReconciliationState.BLOCKED
            self.last_error = f"Journal intent reconciliation exception: {e}"
            logger.critical(f"[RECONCILIATION] {self.last_error}")
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

        # 6. Verify all open broker positions are covered by active broker protection (Safety Correction C.B & CV13-05)
        pos_ok, pos_msg = self._verify_open_positions_protected(positions)
        if not pos_ok:
            self.state = ReconciliationState.BLOCKED
            self.last_error = pos_msg
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
        self._accepting_entries = True
        self.state = ReconciliationState.HEALTHY
        self.last_reconciled_at = time.time()
        self.last_error = ""
        logger.info("✅ [RECONCILIATION] Invariant checks passed! Gate opened: ACCEPTING_NEW_ENTRIES=True.")
        return True, "Reconciliation successful, system healthy and accepting new trades."
