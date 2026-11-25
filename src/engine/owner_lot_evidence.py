"""Read-only evidence for acknowledged settled lots. No order or lifecycle writes."""
from contextvars import ContextVar
from datetime import datetime, timedelta
import json

from src.broker.dhan_client import DhanClient, MAX_HISTORICAL_TRADE_WINDOW_DAYS
from src.engine.market import IST
from src.engine.holding_quantity import observed_open_quantity, _quantity
from src.engine.mixed_cnc_evidence import verify_mixed_cnc_split

# Scoped to the thread and lifetime of one independent full gate pass.
GATE_BOOKS = ContextVar("owner_lot_gate_books", default=None)


class ExecutionConflict(ValueError):
    """An authenticated, executed transaction conflicts with the original lot basis."""
    event = "OWNER_LOT_EXECUTION_CONFLICT"


class DematReview(ValueError):
    """Two broker surfaces show a shortfall that executed trades cannot explain."""
    event = "OWNER_LOT_DEMAT_REVIEW"


def verify_owner_history(broker, db, account, security, segment, product, parents,
                         holding, day_trades, now, evidence, *, current_day_live_keys=(),
                         verified_closed_sibling_trades=None, authorized_closed_sell_ids=(),
                         closed_order_rollups=None, session_date=None):
    """Match every original execution; a SELL followed by a BUY is still a conflict."""
    isin = str(holding.get("isin") or "").strip().upper()
    if not isin:
        raise ValueError("Holdings ISIN unavailable")
    expected = {}
    starts = []
    for oid, qty in parents.items():
        if authorized_closed_sell_ids:
            created, fills = db.owner_lot_basis(
                account, security, segment, product, oid,
                authorized_closed_sell_ids=authorized_closed_sell_ids,
            )
        else:
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
    allowed_closed = verified_closed_sibling_trades or {}
    from src.engine.signed_delivery_live_evidence import ClosedHistoricalBuyMatcher
    matcher = ClosedHistoricalBuyMatcher(
        allowed_closed, closed_order_rollups or {},
        account=account, contract=(security, segment, product), today=last,
    )
    seen_closed_history = set()
    seen_closed_today = set()
    live_date = last if session_date is None else session_date

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
            if historical and side == "BUY":
                matched = matcher.match(row)
                if matched is not None:
                    if matched & seen_closed_history:
                        raise ValueError("Duplicate closed sibling historical execution")
                    seen_closed_history.update(matched)
                    continue
            if key in allowed_closed:
                expected_side, expected_qty, expected_price, expected_at = allowed_closed[key]
                if (side != expected_side
                        or row.get("exchangeSegment") != segment
                        or row.get("productType") != product
                        or int(row["tradedQuantity"]) != expected_qty
                        or abs(float(row["tradedPrice"]) - expected_price) > 1e-6):
                    raise ExecutionConflict("Closed sibling execution differs from journal allocation")
                if historical:
                    if key in seen_closed_history:
                        raise ValueError("Duplicate closed sibling historical execution")
                    seen_closed_history.add(key)
                elif datetime.fromtimestamp(expected_at, IST).date() == live_date:
                    from src.engine.unified_exit_reconciliation import _exchange_time
                    if datetime.fromtimestamp(_exchange_time(row.get("exchangeTime")), IST).date() != live_date:
                        raise ExecutionConflict("Closed sibling live execution date differs")
                    if key in seen_closed_today:
                        raise ExecutionConflict("Duplicate closed sibling live execution")
                    seen_closed_today.add(key)
                continue
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
            err = str(getattr(rows, "error", "") or "")
            source = ("LOCAL_GOVERNOR_NO_GET"
                      if "LOCAL_DHAN_GET_DEFERRAL" in err or ("local" in err.lower() and "defer" in err.lower())
                      else "BROKER_OR_UNKNOWN")
            raise ValueError("Historical pagination incomplete (HTTP %s; source=%s)" %
                             (getattr(rows, "status_code", 0), source))
        evidence["windows"].append([start.isoformat(), end.isoformat(), len(rows)])
        inspect(rows, True)
        start = end + timedelta(days=1)
    # The historical endpoint may not publish today's settled executions
    # immediately. In the *strict mixed CNC* path only, the new-day fills
    # have already been verified against account-bound live tradebook AND
    # durable exchange IDs. Never replace yesterday's historical evidence.
    live_keys = set(current_day_live_keys)
    if not live_keys.issubset(expected.keys()) or (seen | live_keys) != expected.keys():
        raise ValueError("Complete history does not contain every original BUY")
    current_closed = {k for k, row in allowed_closed.items()
                      if datetime.fromtimestamp(row[3], IST).date() == live_date}
    if (seen_closed_history | (seen_closed_today & current_closed)) != set(allowed_closed):
        raise ValueError("Closed sibling complete broker execution history unavailable")


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
            # A current-session CNC BUY can coexist with older DP shares.
            # Only an exact, account-bound BUY-fill split may replace the
            # generic fail-closed contradiction, and historical execution
            # verification must still succeed below before admission.
            split_proof = None
            session_date = None
            if source == "NEGATIVE_CNC_POSITION_REVIEW":
                # Before rejecting T+0 settled stock, prove a bot-owned
                # closed sibling's SELL and the exact remaining parent total.
                # This cannot be inferred from holdings + netQty alone.
                from src.engine.signed_cnc_delivery import verify_signed_cnc_delivery_quantity, signed_delivery_session_date
                session_date = signed_delivery_session_date(
                    trade_book=day_trades, account=account, security=security,
                    segment=segment, product=product, now=now,
                )
                from src.engine.signed_delivery_live_evidence import attest_signed_holdings
                holdings_account = attest_signed_holdings(
                    broker, holdings, account, batch.setdefault("signed_holdings_provenance", {}),
                )
                quantity = verify_signed_cnc_delivery_quantity(
                    db=db, positions=positions, holdings=holdings,
                    trade_book=day_trades, account=account,
                    security=security, segment=segment, product=product,
                    expected_open_qty=sum(parents.values()), now=now,
                    holdings_profile_account=holdings_account,
                )
                source = "VERIFIED_SIGNED_CNC_DELIVERY"
                evidence.update(quantity=quantity, quantity_source=source)
            if source == "CNC_POSITIONS_HOLDINGS_CONTRADICT" and product == "CNC":
                split_proof = verify_mixed_cnc_split(
                    db=db, positions=positions, holdings=holdings,
                    trade_book=day_trades, account=account, security=security,
                    segment=segment, product=product,
                    parent_quantities=parents, now=now, forever=forever,
                )
                quantity = split_proof.total_qty
                source = "VERIFIED_MIXED_CNC_DELIVERY"
                evidence.update(
                    quantity=quantity, quantity_source=source,
                    mixed_split=split_proof.to_audit_dict(),
                )
            if source not in ("VERIFIED_SETTLED_HOLDINGS", "VERIFIED_MIXED_CNC_DELIVERY", "VERIFIED_SIGNED_CNC_DELIVERY"):
                raise ValueError("Settled holding unavailable: " + source)
            holding = next(r for r in holdings if str(r.get("securityId")) == security)
            evidence["holding"] = {k: holding.get(k) for k in ("isin", "totalQty", "dpQty", "t1Qty", "availableQty")}
            evidence["positions"] = [{k: r.get(k) for k in ("securityId", "exchangeSegment", "productType",
                                      "netQty", "buyQty", "sellQty")} for r in positions
                                     if str(r.get("securityId")) == security]
            closed = {"trades": {}, "order_fills": {}, "forever_ids": (),
                      "sell_ids": (), "today_sell_qty": 0}
            if product == "CNC" and callable(getattr(db, "get_closed_forever_sibling_evidence", None)):
                closed = db.get_closed_forever_sibling_evidence(
                    account, security, segment, product,
                    open_parent_ids=tuple(sorted(parents)), now=now, session_date=session_date,
                )
                evidence["verified_closed_sibling_orders"] = list(closed["order_fills"])
            # Rollover is not an arithmetic waiver. All surviving lots must
            # be acknowledged, all closed sibling SELLs must be from earlier
            # trading days, and current books must show no same-security
            # executions or exposure. Full history is independently checked below.
            rollover = (
                source == "VERIFIED_SETTLED_HOLDINGS" and product == "CNC"
                and len(acked) == len(parents) and quantity == sum(parents.values())
                and bool(closed.get("sell_ids")) and closed.get("today_sell_qty") == 0
                and isinstance(day_trades, list) and getattr(day_trades, "ok", True) is True
                and isinstance(positions, list) and getattr(positions, "ok", True) is True
                and all(isinstance(t, dict) and t.get("securityId")
                        and str(t["securityId"]) != security
                        and (not t.get("isin") or t["isin"] != holding.get("isin"))
                        for t in day_trades)
            )
            if rollover:
                for position in positions:
                    if str(position.get("securityId") or "") != security:
                        continue
                    if (str(position.get("dhanClientId") or "") != account
                            or _quantity(position.get("netQty"), signed=True) != 0):
                        raise ValueError("Rollover position account or quantity unverified")
                    from src.engine.signed_cnc_delivery import validate_signed_position_components
                    validate_signed_position_components(position, 0, 0, 0)
                from src.engine.signed_delivery_live_evidence import attest_signed_holdings
                attest_signed_holdings(
                    broker, holdings, account, batch.setdefault("signed_holdings_provenance", {}),
                )
            if not isinstance(forever, list):
                raise ValueError("Forever protection book unavailable")
            related_oco = [{k: r.get(k) for k in ("orderId", "orderStatus", "transactionType")}
                           for r in forever if str(r.get("securityId")) == security]
            evidence["forever_orders"] = related_oco
            if len(acked) == len(parents):
                terminal = ("CANCELLED", "CANCELED", "REJECTED", "EXPIRED",
                            "TRADED", "FILLED", "COMPLETE", "CLOSED")
                for r in related_oco:
                    status = str(r["orderStatus"] or "").upper()
                    group = str(r.get("orderId") or "")
                    if status == "TRIGGERED" and rollover:
                        # Validate after the full historical execution scan.
                        # A persistent TRIGGERED group cannot be treated as dead.
                        continue
                    if status == "TRIGGERED" and source == "VERIFIED_SIGNED_CNC_DELIVERY":
                        # Dhan may omit the cancelled sibling from the triggered group.
                        # Require exact closed parent, leg, live SELL and terminal child
                        # corroboration; do NOT add TRIGGERED to generic terminal states.
                        from src.engine.signed_delivery_live_evidence import verify_closed_triggered_group
                        verify_closed_triggered_group(
                            group=group, closed=closed, forever=forever, orders=orders,
                            day_trades=day_trades, account=account, security=security,
                            segment=segment, product=product,
                        )
                        continue
                    if status not in (terminal if group in closed["forever_ids"] else terminal[:4]):
                        raise ValueError("Forever order for owner-managed shares requires attribution")
            relevant_orders = [{k: r.get(k) for k in ("orderId", "transactionType", "orderStatus", "filledQty")}
                               for r in orders if str(r.get("securityId")) == security
                               or (holding.get("isin") and r.get("isin") == holding["isin"])]
            evidence["orders"] = relevant_orders
            verify_owner_history(broker, db, account, security, segment, product, parents,
                                 holding, day_trades, now, evidence,
                                 current_day_live_keys=(split_proof.intraday_trade_keys
                                                        if split_proof is not None else ()),
                                 verified_closed_sibling_trades=closed["trades"],
                                 authorized_closed_sell_ids=closed["sell_ids"],
                                 closed_order_rollups=closed.get("order_rollups", {}),
                                 session_date=session_date)
            if rollover:
                from src.engine.unified_exit_reconciliation import _exchange_time
                from src.engine.signed_delivery_live_evidence import verify_closed_triggered_group
                previous_sells = []
                seen_sell_keys = set()
                today = datetime.fromtimestamp(now, IST).date()
                for trade in evidence["executions"]:
                    key = (str(trade.get("orderId") or ""), str(trade.get("exchangeTradeId") or ""))
                    expected = closed["trades"].get(key)
                    if not expected or expected[0] != "SELL":
                        continue
                    at = datetime.fromtimestamp(_exchange_time(trade.get("exchangeTime")), IST).date()
                    if (key in seen_sell_keys or at >= today
                            or at != datetime.fromtimestamp(expected[3], IST).date()):
                        raise ValueError("Rollover historical SELL execution day or identity differs")
                    seen_sell_keys.add(key)
                    previous_sells.append(trade)
                if seen_sell_keys != {k for k, v in closed["trades"].items() if v[0] == "SELL"}:
                    raise ValueError("Rollover historical SELL evidence incomplete")
                # A previous-day child may disappear from today's order book.
                # Require an authoritative read by exact ID, never infer terminal
                # status from absence. Include every current order in validation.
                for group in {str(r["orderId"]) for r in related_oco
                              if str(r["orderStatus"] or "").upper() == "TRIGGERED"}:
                    binding = closed.get("triggered_groups", {}).get(group)
                    if not binding:
                        raise ValueError("Rollover triggered group lacks closed parent binding")
                    child_orders = list(orders)
                    for child in binding["children"]:
                        if any(str(r.get("orderId") or "") == child for r in child_orders):
                            continue
                        lookup = getattr(broker, "order_status", None)
                        row = lookup(child) if callable(lookup) else None
                        if not isinstance(row, dict) or str(row.get("orderId") or "") != child:
                            raise ValueError("Rollover closed child order status unavailable")
                        child_orders.append(row)
                    verify_closed_triggered_group(
                        group=group, closed=closed, forever=forever, orders=child_orders,
                        day_trades=previous_sells, account=account, security=security,
                        segment=segment, product=product,
                    )
            for position in evidence["positions"]:
                if not position["exchangeSegment"] or not position["productType"]:
                    raise ValueError("Position identity incomplete")
                if position["productType"] == product and position["sellQty"] not in (None, 0, "0"):
                    try:
                        sold = int(position["sellQty"])
                    except (TypeError, ValueError):
                        raise ValueError("Malformed broker selling quantity") from None
                    if (type(position["sellQty"]) is bool or sold <= 0
                            or sold != closed["today_sell_qty"]):
                        raise ValueError("Position reports selling activity requiring attribution")
            if any(not r.get("securityId") for r in orders):
                raise ValueError("Current order security identity missing")
            for order in relevant_orders:
                status = str(order["orderStatus"] or "").upper()
                if status in ("REJECTED", "CANCELLED", "CANCELED", "EXPIRED") and str(order["filledQty"]) == "0":
                    continue
                if status == "TRADED" and order["orderId"] in parents and order["transactionType"] == "BUY":
                    continue
                oinfo = closed["order_fills"].get(str(order["orderId"]))
                if (oinfo is not None
                        and status in ("TRADED", "COMPLETE", "FILLED", "CLOSED")
                        and str(order["transactionType"] or "").upper() == oinfo[0]
                        and type(order["filledQty"]) in (int, str)
                        and str(order["filledQty"]).isdigit()
                        and int(order["filledQty"]) == oinfo[1]):
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
            if rollover:
                from src.engine.signed_delivery_live_evidence import _holdings_fingerprint
                batch.setdefault("settled_closed_sibling_verified", {}).update({
                    oid: (account, security, segment, product, parents[oid],
                          _holdings_fingerprint(holdings, account))
                    for oid in acked
                })
            if session_date is not None:
                # Preserve the actual proof books even when sharing is not
                # authorized. The watcher compares them to its own reads.
                batch["get_positions"] = positions
                batch["get_trade_book"] = day_trades
                batch.setdefault("signed_owner_verified", {})[(security, segment, product)] = (
                    account, tuple(sorted(parents.items())), session_date,
                )
            if split_proof is not None:
                # The full historical BUY/SELL attestation above has now
                # succeeded; watcher may consume this fresh pass-specific
                # proof, never a generic addition of broker quantities.
                batch.setdefault("mixed_verified", {})[(security, segment, product)] = split_proof
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
