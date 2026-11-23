"""Strict read-only attribution of mixed same-day CNC and older settled bot lots.

Never infer owned shares by adding two Dhan GET quantities without an exact
account-, contract-, parent- and exchange-trade-bound journal/broker match.
This module does not place orders, change owner acknowledgements, or open gates.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from datetime import datetime
import json
import math
from typing import Any, Mapping

from src.engine.holding_quantity import aggregate_holdings, observed_open_quantity
from src.engine.market import IST


class MixedCNCUnverified(ValueError):
    """A split between settled and current-session shares remains unproven."""


@dataclass(frozen=True)
class MixedCNCProof:
    account: str
    security: str
    segment: str
    product: str
    settled_qty: int
    intraday_qty: int
    settled_parent_ids: tuple[str, ...]
    intraday_parent_ids: tuple[str, ...]
    intraday_trade_keys: tuple[tuple[str, str], ...]

    @property
    def total_qty(self) -> int:
        return self.settled_qty + self.intraday_qty

    @property
    def parent_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.settled_parent_ids + self.intraday_parent_ids))

    def to_audit_dict(self) -> dict[str, Any]:
        return {
            "account": self.account, "security": self.security,
            "segment": self.segment, "product": self.product,
            "settled_qty": self.settled_qty, "intraday_qty": self.intraday_qty,
            "settled_parent_ids": list(self.settled_parent_ids),
            "intraday_parent_ids": list(self.intraday_parent_ids),
            "intraday_trade_keys": [list(k) for k in self.intraday_trade_keys],
        }


_INT = re.compile(r"[+-]?[0-9]{1,18}\Z")


def _strict_int(value: Any, name: str, *, allow_negative: bool = False) -> int:
    if type(value) is int:
        qty = value
    elif isinstance(value, str) and _INT.fullmatch(value.strip()):
        qty = int(value.strip())
    else:
        raise MixedCNCUnverified("Malformed position " + name)
    if not allow_negative and qty < 0:
        raise MixedCNCUnverified("Negative position " + name)
    return qty


def verify_mixed_cnc_split(
    *, db: Any, positions: Any, holdings: Any, trade_book: Any,
    account: str, security: str, segment: str, product: str,
    parent_quantities: Mapping[str, int], now: float,
    forever: Any = None,
) -> MixedCNCProof:
    """Prove *only* net BUY today + previously settled bot-owned CNC exposure.

    The caller must separately verify full prior execution history, owner ACKs,
    journal persistence, and live protective legs before authorising entries.
    This local predicate merely makes both broker quantity surfaces attributable.
    A single unlinked trade/SELL, disputed identity or uncertain fill fails closed.
    """
    account, security = str(account or "").strip(), str(security or "").strip()
    segment, product = str(segment or "").strip().upper(), str(product or "").strip().upper()
    if not account or not security or not segment or product != "CNC":
        raise MixedCNCUnverified("Mixed delivery requires account-bound CNC identity")
    if not isinstance(parent_quantities, dict) or len(parent_quantities) < 2:
        raise MixedCNCUnverified("Mixed delivery requires individually identified bot parents")
    if not isinstance(trade_book, list) or not all(isinstance(t, dict) for t in trade_book):
        raise MixedCNCUnverified("Current broker trade book unavailable")
    if not math.isfinite(float(now)):
        raise MixedCNCUnverified("Invalid trading-day timestamp")
    date_today = datetime.fromtimestamp(now, IST).date()
    observed, provenance = observed_open_quantity(
        positions, holdings, security_id=security, segment=segment,
        product=product, account_id=account,
    )
    if observed is not None or provenance != "CNC_POSITIONS_HOLDINGS_CONTRADICT":
        raise MixedCNCUnverified("Broker quantity is not a verified mixed CNC scenario: " + provenance)

    # All other rows on the same security must have zero exposure; a BSE or
    # MTF sibling cannot be silently hidden behind the requested NSE CNC row.
    pos_matches = []
    for row in positions:
        if str(row.get("securityId") or "").strip() != security:
            continue
        if str(row.get("exchangeSegment") or "").strip().upper() == segment and str(
            row.get("productType") or "").strip().upper() == product:
            pos_matches.append(row)
        elif str(row.get("netQty") or "0").strip() not in ("0", "+0", "-0"):
            raise MixedCNCUnverified("Other same-security broker position requires attribution")
    if len(pos_matches) != 1:
        raise MixedCNCUnverified("Mixed CNC position contract is ambiguous")
    pos_row = pos_matches[0]
    raw_net = pos_row.get("netQty")
    if isinstance(raw_net, (bool, float)) or raw_net is None:
        raise MixedCNCUnverified("Malformed position netQty")
    current_net = _strict_int(raw_net, "netQty", allow_negative=True)
    for cf_key in ("carryForwardBuyQty", "carryForwardSellQty"):
        if cf_key in pos_row and pos_row[cf_key] is not None:
            raw_cf = pos_row[cf_key]
            if isinstance(raw_cf, (bool, float)):
                raise MixedCNCUnverified(f"Malformed position {cf_key}")
            if _strict_int(raw_cf, cf_key) != 0:
                raise MixedCNCUnverified("Carry-forward position quantity overlaps settled holdings")
    if (
        "dayBuyQty" in pos_row and pos_row["dayBuyQty"] is not None
        and "daySellQty" in pos_row and pos_row["daySellQty"] is not None
    ):
        raw_db, raw_ds = pos_row["dayBuyQty"], pos_row["daySellQty"]
        if isinstance(raw_db, (bool, float)) or isinstance(raw_ds, (bool, float)):
            raise MixedCNCUnverified("Malformed position dayBuyQty/daySellQty")
        if _strict_int(raw_db, "dayBuyQty") - _strict_int(raw_ds, "daySellQty") != current_net:
            raise MixedCNCUnverified("Position day buy/sell quantities disagree with netQty")
    settled_qty = aggregate_holdings(holdings, account_id=account).get(security)
    holding_rows = [h for h in holdings if str(h.get("securityId") or "").strip() == security]
    if len(holding_rows) != 1 or not holding_rows[0].get("isin"):
        raise MixedCNCUnverified("Settled holding ISIN or identity unavailable")
    isin = str(holding_rows[0]["isin"]).strip().upper()
    if current_net <= 0 or settled_qty is None or settled_qty <= 0:
        raise MixedCNCUnverified("Both independent quantity books must contain positive shares")

    settled_ids: list[str] = []
    current_ids: list[str] = []
    expected_today: dict[tuple[str, str], tuple[int, float]] = {}
    old_qty = new_qty = 0
    filled_trade_ids: set[object] = set()
    used_protection_groups: set[str] = set()
    if not hasattr(db, "owner_lot_basis") or not hasattr(db, "get_fill_by_exchange_trade_id"):
        raise MixedCNCUnverified("Verified journal fill evidence unavailable")

    for raw_oid, raw_qty in parent_quantities.items():
        oid = str(raw_oid or "").strip()
        if not oid or type(raw_qty) is not int or raw_qty <= 0:
            raise MixedCNCUnverified("Invalid parent or open quantity")
        created, fills = db.owner_lot_basis(account, security, segment, product, oid)
        if not fills or sum(int(f["traded_quantity"]) for f in fills) != raw_qty:
            raise MixedCNCUnverified("Original parent BUY quantity does not match")
        if not math.isfinite(float(created)) or float(created) > now:
            raise MixedCNCUnverified("Original parent chronology invalid")
        fill_dates = set()
        for f in fills:
            etid = str(f.get("exchange_trade_id") or "").strip()
            f_qty = int(f["traded_quantity"])
            f_price = float(f["traded_price"])
            if f_qty <= 0 or not math.isfinite(f_price) or f_price <= 0:
                raise MixedCNCUnverified("Original BUY basis has invalid quantity or price")
            if not etid or etid == "0":
                # The existing owner-lot history verifier permits pre-existing
                # NULL-ID BUYs only with exact broker-attested original evidence.
                # This case is valid ONLY for a *previous-session* settled lot.
                # Full historical BUY/SELL inspection is still mandatory before
                # the gate can accept the split; this preliminary check does
                # not promote the attestation to current-day broker evidence.
                try:
                    original = json.loads(f.get("evidence_json") or "null")
                except (TypeError, ValueError):
                    original = None
                if (not isinstance(original, dict)
                        or str(original.get("dhanClientId") or "").strip() != account
                        or str(original.get("orderId") or "").strip() != oid
                        or str(original.get("securityId") or "").strip() != security
                        or str(original.get("isin") or "").strip().upper() != isin
                        or str(original.get("exchangeSegment") or "").strip().upper() != segment
                        or str(original.get("productType") or "").strip().upper() != product
                        or str(original.get("transactionType") or "").strip().upper() != "BUY"
                        or str(original.get("exchangeTradeId") or "").strip() not in ("", "0")
                        or type(original.get("tradedQuantity")) not in (int, str)
                        or str(original["tradedQuantity"]) != str(f_qty)
                        or abs(float(original.get("tradedPrice") or 0.0) - f_price) > 1e-6
                        or datetime.fromtimestamp(float(created), IST).date() >= date_today):
                    raise MixedCNCUnverified("Legacy original BUY lacks verified historical attestation")
                # Multiple NULL-ID fills for one parent are ambiguous under
                # verify_owner_history's (parent, None) identity contract.
                legacy_key = (oid, "LEGACY_ATTESTED_NULL_ID")
                if legacy_key in filled_trade_ids:
                    raise MixedCNCUnverified("Duplicate legacy original BUY identity")
                filled_trade_ids.add(legacy_key)
                fill_dates.add(datetime.fromtimestamp(float(created), IST).date())
                continue
            if etid in filled_trade_ids:
                raise MixedCNCUnverified("Distinct verified exchange trade IDs required")
            filled_trade_ids.add(etid)
            journal_fill = db.get_fill_by_exchange_trade_id(etid)
            if not isinstance(journal_fill, dict):
                raise MixedCNCUnverified("Exchange trade ID has no durable journal row")
            if (str(journal_fill.get("broker_order_id") or "").strip() != oid
                    or str(journal_fill.get("parent_order_id") or "").strip() != oid
                    or str(journal_fill.get("security_id") or "").strip() != security
                    or str(journal_fill.get("exchange_segment") or "").strip().upper() != segment
                    or str(journal_fill.get("product_type") or "").strip().upper() != product
                    or str(journal_fill.get("transaction_type") or "").strip().upper() != "BUY"
                    or journal_fill.get("is_verified_trade") not in (1, True)
                    or int(journal_fill.get("traded_quantity") or 0) != f_qty
                    or abs(float(journal_fill.get("traded_price") or 0.0) - f_price) > 1e-6):
                raise MixedCNCUnverified("Durable exchange BUY fill differs from parent basis")
            traded_at = journal_fill.get("traded_at")
            if type(traded_at) not in (float, int) or not math.isfinite(traded_at):
                raise MixedCNCUnverified("Exchange fill time unavailable")
            traded_date = datetime.fromtimestamp(traded_at, IST).date()
            if traded_date > date_today:
                raise MixedCNCUnverified("Future-dated broker fill")
            fill_dates.add(traded_date)
            if traded_date == date_today:
                expected_today[(oid, etid)] = (f_qty, f_price)
        if len(fill_dates) != 1:
            raise MixedCNCUnverified("Parent spanning settlement periods requires allocation review")
        if date_today in fill_dates:
            current_ids.append(oid)
            new_qty += raw_qty
        else:
            settled_ids.append(oid)
            old_qty += raw_qty
            ack = getattr(db, "is_manual_risk_acknowledged", None)
            ack_ok = callable(ack) and ack(
                oid, oid, expected_account_id=account,
                expected_security_id=security, required_qty=raw_qty,
            ) is True
            protected_ok = False
            if not ack_ok and callable(getattr(db, "get_position_lifecycle", None)):
                life = db.get_position_lifecycle(oid)
                get_ids = getattr(db, "get_protection_order_ids_for_parent", None)
                if (isinstance(life, dict) and callable(get_ids)
                        and isinstance(forever, list)
                        and all(isinstance(r, dict) for r in forever)):
                    ids = get_ids(oid)
                    if not isinstance(ids, list) or not ids or any(
                            not isinstance(gid, str) or not gid.strip() for gid in ids):
                        raise MixedCNCUnverified("Prior-day protection group identities unavailable")
                    if (str(life.get("account_id") or "").strip() == account
                            and str(life.get("security_id") or "").strip() == security
                            and str(life.get("exchange_segment") or "").strip().upper() == segment
                            and str(life.get("product_type") or "").strip().upper() == product
                            and str(life.get("protection_route") or "").strip().upper() == "FOREVER_OCO"
                            and str(life.get("position_state") or "").strip().upper() == "PROTECTED"
                            and not life.get("manual_exit_detected")
                            and not life.get("orphaned_legs_detected")
                            and int(life.get("confirmed_open_qty") or 0) == raw_qty
                            and int(life.get("protected_qty") or 0) >= raw_qty
                            and int(life.get("sell_filled_qty") or 0) == 0):
                        covered = 0
                        local_groups: set[str] = set()
                        for gid in ids:
                            if gid in used_protection_groups or gid in local_groups:
                                raise MixedCNCUnverified("Forever protection group double-assigned")
                            matching = [r for r in forever if str(r.get("orderId") or "") == gid]
                            from src.broker.dhan_client import verify_forever_oco_group
                            if any(str(r.get("dhanClientId") or "").strip() != account
                                   or str(r.get("exchangeSegment") or "").strip().upper() != segment
                                   or str(r.get("productType") or "").strip().upper() != product
                                   for r in matching):
                                raise MixedCNCUnverified("Forever OCO account/contract identity unavailable")
                            proof = verify_forever_oco_group(
                                matching_rows=matching, forever_order_id=gid,
                                expected_account_id=account, expected_security_id=security,
                                expected_segment=segment, expected_product_type=product,
                            )
                            if not proof.is_verified:
                                raise MixedCNCUnverified("Prior-day Forever OCO not broker-active: " + proof.reason)
                            covered += proof.protected_quantity
                            local_groups.add(gid)
                        if covered != raw_qty:
                            raise MixedCNCUnverified("Prior-day Forever OCO coverage not exactly allocated")
                        used_protection_groups.update(local_groups)
                        protected_ok = True
            if not (ack_ok or protected_ok):
                raise MixedCNCUnverified("Prior-day parent lacks exact owner ACK or active protection")
            review = getattr(db, "owner_lot_review_reason", None)
            if callable(review) and review(account, oid):
                raise MixedCNCUnverified("Prior-day parent has durable movement conflict")
    if not settled_ids or not current_ids or settled_qty != old_qty or current_net != new_qty:
        raise MixedCNCUnverified("Independent settled/current-day quantities do not match bot BUY parents")

    seen_today: set[tuple[str, str]] = set()
    for t in trade_book:
        sid = str(t.get("securityId") or "").strip()
        tis = str(t.get("isin") or "").strip().upper()
        if sid != security and tis != isin:
            if not sid or (not tis and str(t.get("exchangeSegment") or "").strip().upper() != segment):
                raise MixedCNCUnverified("Unidentifiable cross-exchange trade")
            continue
        oid = str(t.get("orderId") or "").strip()
        tid = str(t.get("exchangeTradeId") or "").strip()
        key = (oid, tid)
        if (str(t.get("dhanClientId") or "").strip() != account
                or str(t.get("exchangeSegment") or "").strip().upper() != segment
                or str(t.get("productType") or "").strip().upper() != product
                or str(t.get("transactionType") or "").strip().upper() != "BUY"
                or (tis and tis != isin) or not tid or tid == "0"
                or key in seen_today or key not in expected_today):
            raise MixedCNCUnverified("Unattributed BUY/SELL or identity conflict in current trade book")
        raw_q = t.get("tradedQuantity")
        raw_p = t.get("tradedPrice")
        if isinstance(raw_q, (bool, float)) or raw_q is None or isinstance(raw_p, bool) or raw_p is None:
            raise MixedCNCUnverified("Malformed exchange trade fill")
        try:
            qty = int(raw_q)
            price = float(raw_p)
        except (TypeError, ValueError):
            raise MixedCNCUnverified("Malformed exchange trade fill") from None
        exp_qty, exp_price = expected_today[key]
        if qty != exp_qty or not math.isfinite(price) or abs(price - exp_price) > 1e-6:
            raise MixedCNCUnverified("Current broker fill differs from durable BUY")
        seen_today.add(key)
    if seen_today != expected_today.keys():
        raise MixedCNCUnverified("Not every current-day BUY was independently confirmed by Dhan")

    return MixedCNCProof(
        account=account, security=security, segment=segment, product=product,
        settled_qty=old_qty, intraday_qty=new_qty,
        settled_parent_ids=tuple(sorted(settled_ids)),
        intraday_parent_ids=tuple(sorted(set(current_ids))),
        intraday_trade_keys=tuple(sorted(expected_today)),
    )
