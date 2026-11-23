"""Read-only evidence for acknowledged settled lots. No order or lifecycle writes."""
from contextvars import ContextVar
from datetime import datetime, timedelta
import json

from src.broker.dhan_client import DhanClient, MAX_HISTORICAL_TRADE_WINDOW_DAYS
from src.engine.market import IST
from src.engine.holding_quantity import observed_open_quantity

# Scoped to the thread and lifetime of one independent full gate pass.
GATE_BOOKS = ContextVar("owner_lot_gate_books", default=None)


class ExecutionConflict(ValueError):
    """An authenticated, executed transaction conflicts with the original lot basis."""
    event = "OWNER_LOT_EXECUTION_CONFLICT"


class DematReview(ValueError):
    """Two broker surfaces show a shortfall that executed trades cannot explain."""
    event = "OWNER_LOT_DEMAT_REVIEW"


def verify_owner_history(broker, db, account, security, segment, product, parents,
                         holding, day_trades, now, evidence):
    """Match every original execution; a SELL followed by a BUY is still a conflict."""
    isin = str(holding.get("isin") or "").strip().upper()
    if not isin:
        raise ValueError("Holdings ISIN unavailable")
    expected = {}
    starts = []
    for oid, qty in parents.items():
        created, fills = db.owner_lot_basis(account, security, segment, product, oid)
        starts.append(datetime.fromtimestamp(created, IST).date())
        if sum(f["traded_quantity"] for f in fills) != qty:
            raise ValueError("Original BUY quantity differs from open lot")
        for f in fills:
            trade_id = f["exchange_trade_id"]
            attested = None if trade_id else json.loads(f["evidence_json"])
            if attested and (attested.get("dhanClientId") != account or attested.get("orderId") != oid
                             or attested.get("securityId") != security or attested.get("isin") != isin):
                raise ValueError("Original attestation account or security differs")
            key = (oid, str(trade_id) if trade_id else None)
            if key in expected:
                raise ValueError("Ambiguous original BUY identity")
            expected[key] = (f["traded_quantity"], f["traded_price"], attested)

    first, last = min(starts), datetime.fromtimestamp(now, IST).date()
    if first > last:
        raise ValueError("Original order date in future")
    evidence.update(from_date=first.isoformat(), to_date=last.isoformat(), windows=[], executions=[])
    seen = set()

    def inspect(rows, historical):
        for row in rows:
            sid = str(row.get("securityId") or "")
            row_isin = str(row.get("isin") or "").strip().upper()
            if not sid or not row.get("exchangeSegment"):
                raise ValueError("Trade identity incomplete")
            if sid != security and row_isin != isin:
                if not row_isin and row["exchangeSegment"] in ("BSE_EQ", "NSE_EQ") and row["exchangeSegment"] != segment:
                    raise ValueError("Cross-exchange equity trade missing ISIN")
                continue
            oid = str(row.get("orderId") or "")
            side = row.get("transactionType")
            trade_id = str(row.get("exchangeTradeId") or "").strip()
            key = (oid, trade_id if trade_id and trade_id != "0" else None)
            fields = ("dhanClientId", "orderId", "exchangeOrderId", "exchangeTradeId",
                      "exchangeTime", "securityId", "isin", "exchangeSegment", "productType",
                      "transactionType", "tradedQuantity", "tradedPrice")
            evidence["executions"].append({k: row.get(k) for k in fields})
            if row.get("dhanClientId") != account or (row_isin and row_isin != isin):
                raise ValueError("Trade account or ISIN differs")
            valid, normalized, _ = DhanClient.validate_historical_trade_record(
                row, expected_account_id=account, expected_transaction_type=None,
                allow_missing_exchange_trade_id=historical)
            if not valid or normalized is None:
                raise ValueError("Execution identity or quantity incomplete")
            if side == "SELL" and normalized.get("exchangeTradeId"):
                raise ExecutionConflict("Confirmed SELL requires attribution for owner-managed holding")
            if side != "BUY" or key not in expected:
                raise ValueError("Unexplained execution for owner-managed holding")
            qty, price, attested = expected[key]
            if (row.get("exchangeSegment") != segment or row.get("productType") != product
                    or int(row["tradedQuantity"]) != qty or float(row["tradedPrice"]) != price):
                raise ValueError("Execution differs from verified BUY fill")
            if attested and any(row.get(k) != attested[k] for k in ("exchangeOrderId", "exchangeTime", "isin")):
                raise ValueError("Zero-ID execution differs from durable broker attestation")
            if historical:
                if key in seen:
                    raise ValueError("Duplicate historical execution")
                seen.add(key)

    inspect(day_trades, False)
    start = first
    while start <= last:
        end = min(start + timedelta(days=MAX_HISTORICAL_TRADE_WINDOW_DAYS), last)
        rows = broker.get_historical_trades(start.isoformat(), end.isoformat())
        if not isinstance(rows, list) or getattr(rows, "ok", True) is not True:
            code = getattr(rows, "status_code", 0)
            reason = str(getattr(rows, "error", "") or "")
            if "LOCAL_DHAN_GET_DEFERRAL" in reason:
                raise ValueError(f"Historical pagination incomplete (LOCAL_GOVERNOR_DEFERRAL; HTTP {code})")
            if code == 429:
                raise ValueError("Historical pagination incomplete (BROKER_HTTP_429)")
            raise ValueError("Historical pagination incomplete (HTTP %s)" % code)
        evidence["windows"].append([start.isoformat(), end.isoformat(), len(rows)])
        inspect(rows, True)
        start = end + timedelta(days=1)
    if seen != expected.keys():
        raise ValueError("Complete history does not contain every original BUY")


def corroborated_external_sells(trades, account, security, segment, product, owned_ids):
    """Count distinct broker executions outside tracked orders before orphan cancellation."""
    executions = {}
    if not account:
        return 0
    for row in trades or []:
        if row.get("transactionType") != "SELL" or str(row.get("securityId")) != security:
            continue
        ok, trade, _ = DhanClient.validate_historical_trade_record(
            row, expected_account_id=account, expected_security_id=security,
            expected_exchange_segment=segment, expected_product_type=product,
            expected_transaction_type="SELL")
        if not ok or trade["orderId"] in owned_ids:
            continue
        execution = (trade["orderId"], trade["tradedQuantity"])
        key = trade["exchangeTradeId"]
        if key in executions and executions[key] != execution:
            return 0
        executions[key] = execution
    return sum(qty for _, qty in executions.values())


def prepare_owner_lots(broker, db, batch, positions, day_trades, now, holdings=None, forever=None):
    """Run only in the serialized gate worker, before it takes the watcher pass lock."""
    rows = db.get_open_positions_lifecycle() if hasattr(db, "get_open_positions_lifecycle") else []
    if not isinstance(rows, list):
        return  # The gate's required lifecycle check rejects invalid storage.
    account = str(getattr(broker.cfg, "dhan_client_id", "") or "").strip() if rows else ""
    groups = {}
    for row in rows:
        product = row.get("product_type")
        if product not in ("CNC", "MTF"):
            continue
        oid = str(row["parent_order_id"])
        sec = str(row["security_id"])
        qty = int(row["confirmed_open_qty"])
        parents, acknowledged = groups.setdefault((sec, row["exchange_segment"], product), ({}, []))
        parents[oid] = qty
        if (hasattr(db, "is_manual_risk_acknowledged") and
                db.is_manual_risk_acknowledged(oid, oid, expected_account_id=account,
                                               expected_security_id=sec, required_qty=qty) is True):
            acknowledged.append(oid)
    if not any(acked for _, acked in groups.values()):
        return
    if not batch.get("account"):
        batch["account"] = broker.get_authenticated_profile_id(allow_refresh=False)
    if not account or batch["account"] != account:
        for _, acked in groups.values():
            batch["errors"].update(dict.fromkeys(acked, "Dhan profile differs from owner-lot account"))
        db.record_system_event("OWNER_LOT_UNRESOLVED", "Broker account identity unverified",
                               json.dumps({"expected_account": account, "parents": list(batch["errors"])}))
        return
    if holdings is None:
        holdings = broker.get_holdings()
    orders = broker.get_order_book()
    if forever is None:
        forever = broker.get_forever_orders()
    for (security, segment, product), (parents, acked) in groups.items():
        if not acked:
            continue
        quantity, source = observed_open_quantity(positions, holdings, security_id=security,
            segment=segment, product=product, account_id=account)
        evidence = dict(account=account, security=security, segment=segment, product=product,
                        parents=parents, acknowledged=acked, observed_at=now, quantity=quantity,
                        quantity_source=source, profile_matches=True)
        try:
            for oid in acked:
                review = db.owner_lot_review_reason(account, oid)
                if review:
                    raise ValueError(f"{review}: independently reviewed movement evidence required")
            if not isinstance(orders, list):
                raise ValueError("Current order book unavailable")
            if source != "VERIFIED_SETTLED_HOLDINGS":
                raise ValueError("Settled holding unavailable: " + source)
            holding = next(r for r in holdings if str(r.get("securityId")) == security)
            evidence["holding"] = {k: holding.get(k) for k in ("isin", "totalQty", "dpQty", "t1Qty", "availableQty")}
            evidence["positions"] = [{k: r.get(k) for k in ("securityId", "exchangeSegment", "productType",
                                      "netQty", "buyQty", "sellQty")} for r in positions
                                     if str(r.get("securityId")) == security]
            if not isinstance(forever, list):
                raise ValueError("Forever protection book unavailable")
            related_oco = [{k: r.get(k) for k in ("orderId", "orderStatus", "transactionType")}
                           for r in forever if str(r.get("securityId")) == security]
            evidence["forever_orders"] = related_oco
            if len(acked) == len(parents) and any(str(r["orderStatus"]).upper() not in
                    ("CANCELLED", "CANCELED", "REJECTED", "EXPIRED") for r in related_oco):
                raise ValueError("Forever order for owner-managed shares requires attribution")
            relevant_orders = [{k: r.get(k) for k in ("orderId", "transactionType", "orderStatus", "filledQty")}
                               for r in orders if str(r.get("securityId")) == security
                               or (holding.get("isin") and r.get("isin") == holding["isin"])]
            evidence["orders"] = relevant_orders
            verify_owner_history(broker, db, account, security, segment, product, parents,
                                 holding, day_trades, now, evidence)
            for position in evidence["positions"]:
                if not position["exchangeSegment"] or not position["productType"]:
                    raise ValueError("Position identity incomplete")
                if position["productType"] == product and position["sellQty"] not in (None, 0, "0"):
                    raise ValueError("Position reports selling activity requiring attribution")
            if any(not r.get("securityId") for r in orders):
                raise ValueError("Current order security identity missing")
            for order in relevant_orders:
                status = str(order["orderStatus"] or "").upper()
                if status in ("REJECTED", "CANCELLED", "CANCELED", "EXPIRED") and str(order["filledQty"]) == "0":
                    continue
                if status == "TRADED" and order["orderId"] in parents and order["transactionType"] == "BUY":
                    continue
                raise ValueError("Current order requires execution or pending-quantity review")
            if quantity < sum(parents.values()):
                # eDIS shares a broker with holdings. Agreement warrants demat
                # statement review, but establishes neither a transfer nor a SELL.
                corroborating_qty = broker.get_edis_owned_quantity(str(holding.get("isin") or ""))
                evidence["edis_total"] = corroborating_qty if type(corroborating_qty) is int else None
                if evidence["edis_total"] == quantity:
                    raise DematReview("Holdings and eDIS report a shortfall unexplained by trades; review demat statement")
            if quantity != sum(parents.values()):
                raise ValueError("Quantity differs from original lots; demat movement unresolved")
        except (ExecutionConflict, DematReview) as exc:
            decision = str(exc)
            for oid in acked:
                db.record_system_event(exc.event, f"{account}/{oid}", json.dumps(evidence))
                batch["errors"][oid] = decision
        except (ValueError, TypeError, KeyError) as exc:
            decision = str(exc) if type(exc) is ValueError else "Malformed owner lot evidence"
            batch["errors"].update(dict.fromkeys(acked, decision))
        else:
            decision = "Verified original purchases and settled ownership"
            batch["verified"].update({oid: (account, security, parents[oid]) for oid in acked})
        db.record_system_event("OWNER_LOT_EVIDENCE", decision, json.dumps(evidence))
    if isinstance(orders, list):
        batch["get_order_book"] = orders
    if isinstance(holdings, list):
        batch["get_holdings"] = holdings
    if isinstance(forever, list):
        batch["get_forever_orders"] = forever


def _protected_history_rejection_code(exc):
    """Small, non-sensitive reason vocabulary; never log broker payloads/tokens."""
    message = str(exc)
    if "Historical pagination incomplete" in message:
        return "HISTORICAL_PAGE_UNAVAILABLE"
    if "Complete history does not contain every original BUY" in message:
        return "ORIGINAL_BUY_MISSING_FROM_HISTORY"
    if "Original BUY quantity differs" in message:
        return "ORIGINAL_BUY_QUANTITY_MISMATCH"
    if "Original account-bound order date unavailable" in message:
        return "ORIGINAL_ORDER_DATE_UNAVAILABLE"
    if "Verified original BUY fills unavailable" in message:
        return "VERIFIED_BUY_FILLS_UNAVAILABLE"
    if "settled quantity mismatch" in message:
        return "SETTLED_HOLDINGS_QUANTITY_MISMATCH"
    if "holding missing" in message or "Holdings ISIN unavailable" in message:
        return "HOLDING_IDENTITY_INCOMPLETE"
    if "Duplicate historical execution" in message:
        return "DUPLICATE_HISTORICAL_EXECUTION"
    if "Unexplained execution" in message:
        return "UNATTRIBUTED_EXECUTION"
    if "Trade account or ISIN differs" in message:
        return "ACCOUNT_OR_ISIN_MISMATCH"
    if "Trade identity incomplete" in message or "Execution identity" in message:
        return "HISTORICAL_TRADE_IDENTITY_INCOMPLETE"
    return "HISTORICAL_ATTESTATION_REJECTED"


def prepare_protected_prior_day_buy_history(broker, db, batch, positions, day_trades, now):
    """Admit NO old-order exemption unless full prior-day protected BUY history is fresh.

    This runs within GATE_BOOKS, once per serialized whole-account gate pass.
    A missing/contradictory result is a failed proof, never a broker SELL or
    permission to relax protection / coverage. No lifecycle writes occur.
    """
    rows = db.get_open_positions_lifecycle()
    if not isinstance(rows, list):
        raise ValueError("Protected BUY history: lifecycle journal unavailable")
    today = datetime.fromtimestamp(now, IST).date()
    targets = []
    for row in rows:
        if (str(row.get("position_state") or "").upper() != "PROTECTED"
                or str(row.get("protection_route") or "").upper() != "FOREVER_OCO"
                or str(row.get("product_type") or "").upper() not in ("CNC", "MTF")):
            continue
        try:
            qty = int(row.get("confirmed_open_qty") or 0)
            if (qty <= 0 or int(row.get("entry_qty") or 0) != qty
                    or int(row.get("buy_filled_qty") or 0) != qty
                    or int(row.get("protected_qty") or 0) < qty
                    or int(row.get("sell_filled_qty") or 0) != 0
                    or row.get("manual_exit_detected") or row.get("orphaned_legs_detected")):
                continue
            first_at, _ = db.owner_lot_basis(
                db.account_id, str(row["security_id"]),
                str(row["exchange_segment"]), str(row["product_type"]),
                str(row["parent_order_id"]),
            )
            if datetime.fromtimestamp(float(first_at), IST).date() >= today:
                continue
            targets.append(row)
        except (ValueError, TypeError, KeyError):
            # The parent is not proven old; watcher keeps strict normal semantics.
            batch.setdefault("protected_skip_reason", {})[str(row.get("parent_order_id") or "")] = (
                "ORIGINAL_BUY_IDENTITY_OR_DATE_UNAVAILABLE"
            )
            continue
    if not targets:
        return
    account = str(getattr(getattr(broker, "cfg", None), "dhan_client_id", "") or "").strip()
    if not account or account != str(db.account_id or "").strip():
        raise ValueError("Protected BUY history: account mismatch")
    profile = batch.get("account")
    if profile is None:
        profile = broker.get_authenticated_profile_id(allow_refresh=False)
        batch["account"] = profile
    if profile != account:
        raise ValueError("Protected BUY history: broker profile mismatch")
    holdings = batch.get("get_holdings")
    if holdings is None:
        holdings = broker.get_holdings()
    orders = batch.get("get_order_book")
    if orders is None:
        orders = broker.get_order_book()
    forever = batch.get("get_forever_orders")
    if forever is None:
        forever = broker.get_forever_orders()
    if not all(isinstance(book, list) for book in (holdings, orders, forever)):
        raise ValueError("Protected BUY history: required broker book unavailable")
    # A missing order identity must never be interpreted as a legitimate empty book.
    if any(not isinstance(o, dict) or not str(o.get("orderId") or "").strip() for o in orders):
        raise ValueError("Protected BUY history: malformed current order book")

    for row in targets:
        oid = str(row["parent_order_id"])
        sec = str(row["security_id"])
        seg = str(row["exchange_segment"])
        prod = str(row["product_type"])
        qty = int(row["confirmed_open_qty"])
        try:
            # Every journal-owned parent of this contract must be represented
            # in the historical check, so a sibling cannot conceal an exit.
            siblings = {
                str(r["parent_order_id"]): int(r["confirmed_open_qty"])
                for r in rows
                if (str(r.get("security_id")) == sec
                    and str(r.get("exchange_segment")) == seg
                    and str(r.get("product_type")) == prod
                    and int(r.get("confirmed_open_qty") or 0) > 0)
            }
            observed, source = observed_open_quantity(
                positions, holdings, security_id=sec, segment=seg,
                product=prod, account_id=account,
            )
            if source != "VERIFIED_SETTLED_HOLDINGS" or observed != sum(siblings.values()):
                raise ValueError("Protected BUY history: settled quantity mismatch")
            holding = next((h for h in holdings if str(h.get("securityId") or "") == sec), None)
            if not isinstance(holding, dict):
                raise ValueError("Protected BUY history: holding missing")
            evidence = {}
            verify_owner_history(broker, db, account, sec, seg, prod, siblings,
                                 holding, day_trades, now, evidence)
            batch.setdefault("protected_verified", {})[oid] = (account, sec, qty)
            batch.setdefault("protected_skip_reason", {}).pop(oid, None)
        except ExecutionConflict as exc:
            # A confirmed historical SELL is positive conflicting evidence;
            # the gate must block even if the old BUY endpoint is readable.
            batch.setdefault("errors", {})[oid] = (
                "Protected original BUY history contains a SELL: " + str(exc)
            )
            batch.setdefault("protected_skip_reason", {})[oid] = "CONFIRMED_SELL_CONFLICT"
        except (ValueError, TypeError, KeyError, IndexError) as exc:
            # A rejected attestation is never broker evidence. The watcher
            # receives a stable reason code, not the raw response or token.
            # It must remain fail-closed and continue live protective-leg reads.
            batch.setdefault("protected_skip_reason", {})[oid] = (
                _protected_history_rejection_code(exc)
            )
            continue
    batch["get_holdings"] = holdings
    batch["get_forever_orders"] = forever
    batch["get_order_book"] = orders
