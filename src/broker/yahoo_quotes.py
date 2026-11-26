"""Extracted Yahoo execution-quote policy (behavior-preserving migration).

The DhanClient method retains its signature and delegates here. Session, cache,
thread-local metadata and configuration remain owned by the original client.
"""
import math
import time
from datetime import datetime
from typing import Optional

from src.broker.yahoo_observer import YAHOO_HTTP_TIMEOUT, YAHOO_HTTP_HEADERS, parse_yahoo_response
from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR
from src.models import QuoteResult

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
VALID_NSE_EXCHANGES = frozenset({"NSI", "NSE", "NATIONAL STOCK EXCHANGE OF INDIA"})
VALID_BSE_EXCHANGES = frozenset({"BSE", "BOM", "BOMBAY STOCK EXCHANGE"})


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
    headers = YAHOO_HTTP_HEADERS

    t4_start_ns = time.perf_counter_ns()
    try:
        t_req_start = time.time()
        resp = self.session.get(url, headers=headers, timeout=YAHOO_HTTP_TIMEOUT)
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
        data = parse_yahoo_response(resp)
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
