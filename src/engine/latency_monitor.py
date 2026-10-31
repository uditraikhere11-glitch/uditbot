"""
CV13-07: End-to-End Monotonic Latency & Quote-Age Performance Monitor with Durable SQLite Correlation.

Implements Section 5, Section 6 & Section 7 requirements + P1 Durable Telemetry Correlation:
- Uses high-resolution monotonic clock (`time.perf_counter_ns()`) for all elapsed-duration measurements.
- Uses IST wall-clock timestamps separately for operator-readable event times and cross-restart L11/L12 correlation.
- Correlates Telegram ingestion, Yahoo retrieval, Dhan acknowledgement, actual fill, and protection verification
  using durable identifiers (`trace_id`, `correlation_id`, and `broker_order_id`) persisted in SQLite (`JournalDB`).
- Captures all 12 independent pipeline stages (L1 through L12):
  L1: Telegram message received by AuditBot
  L2: Signal parsing and validation
  L3: Instrument resolution
  L4: Yahoo HTTP request and response
  L5: Yahoo JSON parsing and CMP extraction
  L6: CMP/reference-price authorization
  L7: Limit-price and quantity calculation
  L8: Risk and broker preflight checks
  L9: Dhan order HTTP request and response
  L10: Broker order-ID acknowledgement
  L11: Fill confirmation, if and when received
  L12: Broker-confirmed protection establishment
- Strictly separates Yahoo HTTP retrieval latency (ms) from reported source quote age (seconds).
- Preserves missing Yahoo `regularMarketTime` as `QUOTE_AGE_UNKNOWN` (`reported_quote_age_sec = None`)
  and explicitly distinguishes accepted pricing (`DARVAS_HTTP_SUCCESS`) from verified quote freshness.
"""

from dataclasses import dataclass, field
import math
import statistics
import threading
import time
from typing import Any, Dict, List, Optional


def _ns_to_ms(delta_ns: Optional[int]) -> Optional[float]:
    if delta_ns is None or not isinstance(delta_ns, (int, float)):
        return None
    if delta_ns < 0:
        return None
    return round(float(delta_ns) / 1_000_000.0, 3)


@dataclass
class SignalLatencyTrace:
    """Tracks monotonic timestamps (ns), wall-clock checkpoints, and stage durations (ms) for a signal."""
    trace_id: str
    symbol: str = ""
    channel_id: int = 0
    message_id: int = 0
    correlation_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    ist_received_time: float = field(default_factory=time.time)
    telegram_msg_timestamp: Optional[float] = None

    # Monotonic stage checkpoints (perf_counter_ns)
    l1_received_ns: int = field(default_factory=time.perf_counter_ns)
    l2_parsed_ns: Optional[int] = None
    l3_resolved_ns: Optional[int] = None
    l4_yahoo_req_start_ns: Optional[int] = None
    l4_yahoo_req_end_ns: Optional[int] = None
    l5_yahoo_parsed_ns: Optional[int] = None
    l6_cmp_auth_ns: Optional[int] = None
    l7_sized_ns: Optional[int] = None
    l8_preflight_ns: Optional[int] = None
    l9_dhan_req_start_ns: Optional[int] = None
    l9_dhan_req_end_ns: Optional[int] = None
    l10_broker_ack_ns: Optional[int] = None
    l11_fill_confirmed_ns: Optional[int] = None
    l12_protection_verified_ns: Optional[int] = None
    completed_ns: Optional[int] = None

    # Wall-clock checkpoints for cross-restart L10 -> L11 -> L12 correlation
    ack_wall_time: Optional[float] = None
    fill_wall_time: Optional[float] = None
    protection_wall_time: Optional[float] = None

    # Explicit stage durations (ms)
    l2_parse_ms: Optional[float] = None
    l3_resolve_ms: Optional[float] = None
    l4_yahoo_http_ms: Optional[float] = None
    l5_yahoo_parse_ms: Optional[float] = None
    l6_cmp_auth_ms: Optional[float] = None
    l7_sizing_ms: Optional[float] = None
    l8_risk_preflight_ms: Optional[float] = None
    l9_dhan_http_ms: Optional[float] = None
    l10_ack_ms: Optional[float] = None
    persisted_ack_to_fill_ms: Optional[float] = None
    persisted_fill_to_protection_ms: Optional[float] = None

    # Quote age & freshness metadata (kept strictly distinct from HTTP latency)
    reported_quote_age_sec: Optional[float] = None
    quote_timestamp: Optional[float] = None
    quote_age_status: str = "QUOTE_AGE_UNKNOWN"
    freshness_verified: bool = False
    quote_freshness_status: str = "UNVERIFIED"
    cmp_policy: str = "STRICT_TIMESTAMP"
    price_source: str = "Stated Price"
    yahoo_cmp: Optional[float] = None

    # Mark stages
    def mark_l2_parsed(self, now_ns: Optional[int] = None) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l2_parsed_ns = t
        self.l2_parse_ms = _ns_to_ms(t - self.l1_received_ns)

    def mark_l3_resolved(self, now_ns: Optional[int] = None) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l3_resolved_ns = t
        prev = self.l2_parsed_ns or self.l1_received_ns
        self.l3_resolve_ms = _ns_to_ms(t - prev)

    def record_yahoo_stages(
        self,
        http_ms: Optional[float],
        parse_ms: Optional[float],
        auth_ms: Optional[float],
        quote_age_sec: Optional[float],
        quote_age_status: str,
        cmp_policy: str,
        yahoo_cmp: Optional[float] = None,
        quote_timestamp: Optional[float] = None,
        freshness_verified: bool = False,
        quote_freshness_status: str = "",
        now_ns: Optional[int] = None,
    ) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        if http_ms is not None and http_ms >= 0.0:
            self.l4_yahoo_http_ms = round(float(http_ms), 3)
        if parse_ms is not None and parse_ms >= 0.0:
            self.l5_yahoo_parse_ms = round(float(parse_ms), 3)
        if auth_ms is not None and auth_ms >= 0.0:
            self.l6_cmp_auth_ms = round(float(auth_ms), 3)
        self.l6_cmp_auth_ns = t
        if quote_timestamp is not None and math.isfinite(quote_timestamp) and quote_timestamp > 0.0:
            self.quote_timestamp = float(quote_timestamp)
        else:
            self.quote_timestamp = None

        if quote_age_sec is not None and math.isfinite(quote_age_sec) and quote_age_sec >= 0.0:
            self.reported_quote_age_sec = round(float(quote_age_sec), 3)
            self.quote_age_status = f"{self.reported_quote_age_sec:.2f}s"
        else:
            self.reported_quote_age_sec = None
            self.quote_age_status = quote_age_status or "QUOTE_AGE_UNKNOWN"

        self.freshness_verified = bool(freshness_verified)
        if quote_freshness_status:
            self.quote_freshness_status = str(quote_freshness_status)
        elif self.freshness_verified:
            self.quote_freshness_status = "FRESHNESS_VERIFIED"
        elif self.reported_quote_age_sec is None:
            self.quote_freshness_status = "ACCEPTED_UNVERIFIED_TIMESTAMP"
        elif self.reported_quote_age_sec > 2.0:
            self.quote_freshness_status = f"ACCEPTED_BY_DARVAS_POLICY_EXCEEDS_2S ({self.reported_quote_age_sec:.2f}s > 2.00s)"
        else:
            self.quote_freshness_status = "UNVERIFIED"

        self.cmp_policy = cmp_policy
        self.price_source = "Yahoo Finance"
        if yahoo_cmp is not None and yahoo_cmp > 0:
            self.yahoo_cmp = float(yahoo_cmp)

    def mark_l6_stated_price(self, stated_price: float, now_ns: Optional[int] = None) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l6_cmp_auth_ns = t
        prev = self.l3_resolved_ns or self.l2_parsed_ns or self.l1_received_ns
        self.l6_cmp_auth_ms = _ns_to_ms(t - prev)
        self.price_source = "Stated Signal Price"
        self.quote_freshness_status = "STATED_PRICE"

    def mark_l7_sized(self, now_ns: Optional[int] = None) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l7_sized_ns = t
        prev = self.l6_cmp_auth_ns or self.l3_resolved_ns or self.l1_received_ns
        self.l7_sizing_ms = _ns_to_ms(t - prev)

    def mark_l8_preflight(self, now_ns: Optional[int] = None) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l8_preflight_ns = t
        prev = self.l7_sized_ns or self.l6_cmp_auth_ns or self.l1_received_ns
        self.l8_risk_preflight_ms = _ns_to_ms(t - prev)

    def mark_l9_dhan_start(self, now_ns: Optional[int] = None) -> None:
        self.l9_dhan_req_start_ns = now_ns if now_ns is not None else time.perf_counter_ns()

    def mark_l10_dhan_ack(
        self,
        dhan_http_ms: Optional[float] = None,
        now_ns: Optional[int] = None,
        wall_time: Optional[float] = None,
    ) -> None:
        t = now_ns if now_ns is not None else time.perf_counter_ns()
        self.l9_dhan_req_end_ns = t
        self.l10_broker_ack_ns = t
        self.ack_wall_time = float(wall_time) if wall_time is not None else time.time()
        if dhan_http_ms is not None and dhan_http_ms >= 0.0:
            self.l9_dhan_http_ms = round(float(dhan_http_ms), 3)
        elif self.l9_dhan_req_start_ns is not None:
            self.l9_dhan_http_ms = _ns_to_ms(t - self.l9_dhan_req_start_ns)
        self.l10_ack_ms = self.l9_dhan_http_ms

    def mark_l11_fill_confirmed(
        self,
        now_ns: Optional[int] = None,
        wall_time: Optional[float] = None,
    ) -> None:
        if now_ns is not None:
            self.l11_fill_confirmed_ns = now_ns
        elif wall_time is None and self.l10_broker_ack_ns is not None:
            self.l11_fill_confirmed_ns = time.perf_counter_ns()
        self.fill_wall_time = float(wall_time) if wall_time is not None else time.time()

    def mark_l12_protection_verified(
        self,
        now_ns: Optional[int] = None,
        wall_time: Optional[float] = None,
    ) -> None:
        if now_ns is not None:
            self.l12_protection_verified_ns = now_ns
        elif wall_time is None and self.l11_fill_confirmed_ns is not None:
            self.l12_protection_verified_ns = time.perf_counter_ns()
        self.protection_wall_time = float(wall_time) if wall_time is not None else time.time()

    def mark_completed(self, now_ns: Optional[int] = None) -> None:
        self.completed_ns = now_ns if now_ns is not None else time.perf_counter_ns()

    # Computed end-to-end intervals
    @property
    def telegram_to_cmp_ms(self) -> Optional[float]:
        if self.l6_cmp_auth_ns is None:
            return None
        return _ns_to_ms(self.l6_cmp_auth_ns - self.l1_received_ns)

    @property
    def cmp_to_submission_ms(self) -> Optional[float]:
        if self.l6_cmp_auth_ns is None or self.l9_dhan_req_start_ns is None:
            return None
        return _ns_to_ms(self.l9_dhan_req_start_ns - self.l6_cmp_auth_ns)

    @property
    def telegram_to_submission_ms(self) -> Optional[float]:
        if self.l9_dhan_req_start_ns is None:
            return None
        return _ns_to_ms(self.l9_dhan_req_start_ns - self.l1_received_ns)

    @property
    def dhan_request_to_ack_ms(self) -> Optional[float]:
        if self.l9_dhan_http_ms is not None:
            return self.l9_dhan_http_ms
        if self.l9_dhan_req_start_ns is not None and self.l10_broker_ack_ns is not None:
            return _ns_to_ms(self.l10_broker_ack_ns - self.l9_dhan_req_start_ns)
        return None

    @property
    def telegram_to_ack_ms(self) -> Optional[float]:
        if self.l10_broker_ack_ns is None:
            return None
        return _ns_to_ms(self.l10_broker_ack_ns - self.l1_received_ns)

    @property
    def ack_to_fill_ms(self) -> Optional[float]:
        if self.l10_broker_ack_ns is not None and self.l11_fill_confirmed_ns is not None:
            return _ns_to_ms(self.l11_fill_confirmed_ns - self.l10_broker_ack_ns)
        if self.persisted_ack_to_fill_ms is not None:
            return self.persisted_ack_to_fill_ms
        if self.ack_wall_time is not None and self.fill_wall_time is not None and self.fill_wall_time >= self.ack_wall_time:
            return round((self.fill_wall_time - self.ack_wall_time) * 1000.0, 3)
        return None

    @property
    def fill_to_protection_ms(self) -> Optional[float]:
        if self.l11_fill_confirmed_ns is not None and self.l12_protection_verified_ns is not None:
            return _ns_to_ms(self.l12_protection_verified_ns - self.l11_fill_confirmed_ns)
        if self.persisted_fill_to_protection_ms is not None:
            return self.persisted_fill_to_protection_ms
        if self.fill_wall_time is not None and self.protection_wall_time is not None and self.protection_wall_time >= self.fill_wall_time:
            return round((self.protection_wall_time - self.fill_wall_time) * 1000.0, 3)
        return None

    @property
    def total_processing_ms(self) -> Optional[float]:
        end_ns = (
            self.completed_ns
            or self.l10_broker_ack_ns
            or self.l8_preflight_ns
            or self.l7_sized_ns
            or self.l6_cmp_auth_ns
            or self.l3_resolved_ns
            or self.l2_parsed_ns
        )
        if end_ns is None:
            return None
        return _ns_to_ms(end_ns - self.l1_received_ns)

    @property
    def external_telegram_transit_sec(self) -> Optional[float]:
        """
        True Telegram message-to-bot transit time when a trustworthy wall-clock message
        timestamp is provided. Never mixed with internal bot processing latency.
        """
        if self.telegram_msg_timestamp is None or self.telegram_msg_timestamp <= 0:
            return None
        diff = self.ist_received_time - self.telegram_msg_timestamp
        if not math.isfinite(diff) or diff < 0.0:
            return None
        return round(diff, 3)

    def to_breakdown_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
            "broker_order_id": self.broker_order_id,
            "symbol": self.symbol,
            "channel_id": self.channel_id,
            "message_id": self.message_id,
            "ist_received_time": self.ist_received_time,
            "telegram_msg_timestamp": self.telegram_msg_timestamp,
            "price_source": self.price_source,
            "yahoo_cmp": self.yahoo_cmp,
            "yahoo_http_latency_ms": self.l4_yahoo_http_ms,
            "yahoo_parse_ms": self.l5_yahoo_parse_ms,
            "cmp_auth_ms": self.l6_cmp_auth_ms,
            "quote_timestamp": self.quote_timestamp,
            "reported_quote_age_sec": self.reported_quote_age_sec,
            "quote_age_status": self.quote_age_status,
            "freshness_verified": self.freshness_verified,
            "quote_freshness_status": self.quote_freshness_status,
            "cmp_policy": self.cmp_policy,
            "l2_parse_ms": self.l2_parse_ms,
            "l3_resolve_ms": self.l3_resolve_ms,
            "l7_sizing_ms": self.l7_sizing_ms,
            "l8_risk_preflight_ms": self.l8_risk_preflight_ms,
            "telegram_to_cmp_ms": self.telegram_to_cmp_ms,
            "cmp_to_submission_ms": self.cmp_to_submission_ms,
            "telegram_to_submission_ms": self.telegram_to_submission_ms,
            "dhan_request_to_ack_ms": self.dhan_request_to_ack_ms,
            "telegram_to_ack_ms": self.telegram_to_ack_ms,
            "ack_wall_time": self.ack_wall_time,
            "fill_wall_time": self.fill_wall_time,
            "protection_wall_time": self.protection_wall_time,
            "ack_to_fill_ms": self.ack_to_fill_ms,
            "fill_to_protection_ms": self.fill_to_protection_ms,
            "total_processing_ms": self.total_processing_ms,
            "external_telegram_transit_sec": self.external_telegram_transit_sec,
        }


class LatencyMonitor:
    """
    Thread-safe performance monitor collecting Yahoo HTTP latency, quote-age distributions,
    stage intervals, and rejection counters, backed by durable SQLite persistence so metrics
    and order-correlated L1–L12 histories survive bot restarts.
    """

    def __init__(self, max_samples: int = 500, db: Optional[Any] = None):
        self._lock = threading.Lock()
        self.max_samples = max_samples
        self._db: Optional[Any] = None
        self._hydrated_db_path: Optional[str] = None
        self.yahoo_http_latencies_ms: List[float] = []
        self.telegram_to_cmp_latencies_ms: List[float] = []
        self.telegram_to_ack_latencies_ms: List[float] = []
        self.ack_to_fill_latencies_ms: List[float] = []
        self.fill_to_protection_latencies_ms: List[float] = []
        self.quote_ages_sec: List[float] = []
        self.quote_age_unknown_count: int = 0
        self.yahoo_success_count: int = 0
        self.yahoo_failure_count: int = 0
        self.rejection_reasons: Dict[str, int] = {}
        self.traces_by_order_id: Dict[str, SignalLatencyTrace] = {}
        self.recent_traces: List[SignalLatencyTrace] = []
        if db is not None:
            self.bind_db(db)

    def bind_db(self, db: Any, force_reload: bool = False) -> None:
        """
        Binds a durable JournalDB instance and hydrates historical telemetry and correlated traces
        so metrics and order-correlated L11/L12 updates survive bot restarts.
        """
        if db is None:
            return
        db_path = getattr(db, "db_path", None)
        with self._lock:
            self._db = db
            if not force_reload and db_path and self._hydrated_db_path == db_path:
                return
            self._hydrated_db_path = db_path
            if hasattr(db, "load_latency_telemetry_state"):
                try:
                    state = db.load_latency_telemetry_state(max_samples=self.max_samples)
                    if isinstance(state, dict):
                        self.yahoo_success_count = int(state.get("yahoo_success_count", 0))
                        self.yahoo_failure_count = int(state.get("yahoo_failure_count", 0))
                        self.quote_age_unknown_count = int(state.get("quote_age_unknown_count", 0))
                        self.yahoo_http_latencies_ms = list(state.get("yahoo_http_latencies_ms", []))
                        self.quote_ages_sec = list(state.get("quote_ages_sec", []))
                        self.telegram_to_cmp_latencies_ms = list(state.get("telegram_to_cmp_latencies_ms", []))
                        self.telegram_to_ack_latencies_ms = list(state.get("telegram_to_ack_latencies_ms", []))
                        self.ack_to_fill_latencies_ms = list(state.get("ack_to_fill_latencies_ms", []))
                        self.fill_to_protection_latencies_ms = list(state.get("fill_to_protection_latencies_ms", []))
                        self.rejection_reasons = dict(state.get("rejection_reasons", {}))
                        self.traces_by_order_id.clear()
                        self.recent_traces.clear()
                        for row in state.get("trace_rows", []):
                            tr = SignalLatencyTrace(
                                trace_id=str(row.get("trace_id") or ""),
                                symbol=str(row.get("symbol") or ""),
                                channel_id=int(row.get("channel_id") or 0),
                                message_id=int(row.get("message_id") or 0),
                                correlation_id=row.get("correlation_id"),
                                broker_order_id=row.get("broker_order_id"),
                                ist_received_time=float(row.get("ist_received_time") or 0.0),
                                telegram_msg_timestamp=row.get("telegram_msg_timestamp"),
                                ack_wall_time=row.get("ack_wall_time"),
                                fill_wall_time=row.get("fill_wall_time"),
                                protection_wall_time=row.get("protection_wall_time"),
                                l2_parse_ms=row.get("l2_parse_ms"),
                                l3_resolve_ms=row.get("l3_resolve_ms"),
                                l4_yahoo_http_ms=row.get("yahoo_http_ms"),
                                l5_yahoo_parse_ms=row.get("yahoo_parse_ms"),
                                l6_cmp_auth_ms=row.get("cmp_auth_ms"),
                                l7_sizing_ms=row.get("l7_sizing_ms"),
                                l8_risk_preflight_ms=row.get("l8_risk_preflight_ms"),
                                l9_dhan_http_ms=row.get("dhan_request_to_ack_ms"),
                                l10_ack_ms=row.get("dhan_request_to_ack_ms"),
                                persisted_ack_to_fill_ms=row.get("ack_to_fill_ms"),
                                persisted_fill_to_protection_ms=row.get("fill_to_protection_ms"),
                                reported_quote_age_sec=row.get("reported_quote_age_sec"),
                                quote_timestamp=row.get("quote_timestamp"),
                                quote_age_status=str(row.get("quote_age_status") or "QUOTE_AGE_UNKNOWN"),
                                freshness_verified=bool(row.get("freshness_verified", False)),
                                quote_freshness_status=str(row.get("quote_freshness_status") or "UNVERIFIED"),
                                cmp_policy=str(row.get("cmp_policy") or "STRICT_TIMESTAMP"),
                                price_source=str(row.get("price_source") or "Yahoo Finance"),
                                yahoo_cmp=row.get("yahoo_cmp"),
                            )
                            self.recent_traces.append(tr)
                            if tr.trace_id:
                                self.traces_by_order_id[tr.trace_id] = tr
                            if tr.correlation_id:
                                self.traces_by_order_id[str(tr.correlation_id)] = tr
                            if tr.broker_order_id:
                                self.traces_by_order_id[str(tr.broker_order_id)] = tr
                except Exception:
                    pass

    def record_yahoo_request(
        self,
        success: bool,
        http_latency_ms: Optional[float],
        quote_age_sec: Optional[float] = None,
        symbol: str = "",
    ) -> None:
        with self._lock:
            if success:
                self.yahoo_success_count += 1
            else:
                self.yahoo_failure_count += 1

            clean_lat: Optional[float] = None
            if http_latency_ms is not None and math.isfinite(http_latency_ms) and http_latency_ms >= 0.0:
                clean_lat = float(http_latency_ms)
                self.yahoo_http_latencies_ms.append(clean_lat)
                if len(self.yahoo_http_latencies_ms) > self.max_samples:
                    self.yahoo_http_latencies_ms.pop(0)

            clean_age: Optional[float] = None
            if quote_age_sec is not None and math.isfinite(quote_age_sec) and quote_age_sec >= 0.0:
                clean_age = float(quote_age_sec)
                self.quote_ages_sec.append(clean_age)
                if len(self.quote_ages_sec) > self.max_samples:
                    self.quote_ages_sec.pop(0)
            elif success:
                self.quote_age_unknown_count += 1

            db = self._db
        if db is not None and hasattr(db, "record_yahoo_telemetry"):
            try:
                db.record_yahoo_telemetry(
                    success=success,
                    http_latency_ms=clean_lat,
                    quote_age_sec=clean_age,
                    symbol=symbol,
                )
            except Exception:
                pass

    def record_trace(
        self,
        trace: SignalLatencyTrace,
        order_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        with self._lock:
            if order_id:
                trace.broker_order_id = str(order_id)
            if correlation_id:
                trace.correlation_id = str(correlation_id)

            t_cmp = trace.telegram_to_cmp_ms
            if t_cmp is not None and t_cmp >= 0.0:
                self.telegram_to_cmp_latencies_ms.append(t_cmp)
                if len(self.telegram_to_cmp_latencies_ms) > self.max_samples:
                    self.telegram_to_cmp_latencies_ms.pop(0)

            t_ack = trace.telegram_to_ack_ms
            if t_ack is not None and t_ack >= 0.0:
                self.telegram_to_ack_latencies_ms.append(t_ack)
                if len(self.telegram_to_ack_latencies_ms) > self.max_samples:
                    self.telegram_to_ack_latencies_ms.pop(0)

            if trace.trace_id:
                self.traces_by_order_id[str(trace.trace_id)] = trace
            if order_id:
                self.traces_by_order_id[str(order_id)] = trace
            if correlation_id:
                self.traces_by_order_id[str(correlation_id)] = trace

            if trace not in self.recent_traces:
                self.recent_traces.append(trace)
                if len(self.recent_traces) > self.max_samples:
                    self.recent_traces.pop(0)

            db = self._db
            bd = trace.to_breakdown_dict()
        if db is not None and hasattr(db, "upsert_latency_trace"):
            try:
                db.upsert_latency_trace(bd)
            except Exception:
                pass

    def record_fill_confirmed(
        self,
        order_id: str,
        now_ns: Optional[int] = None,
        wall_time: Optional[float] = None,
    ) -> Optional[float]:
        fill_wt = float(wall_time) if wall_time is not None else time.time()
        val: Optional[float] = None
        with self._lock:
            tr = self.traces_by_order_id.get(str(order_id))
            if tr is not None:
                if tr.l11_fill_confirmed_ns is None and tr.persisted_ack_to_fill_ms is None:
                    tr.mark_l11_fill_confirmed(now_ns=now_ns, wall_time=fill_wt)
                    val = tr.ack_to_fill_ms
                    if val is not None and val >= 0.0:
                        self.ack_to_fill_latencies_ms.append(val)
                else:
                    val = tr.ack_to_fill_ms
            db = self._db

        if db is not None and hasattr(db, "update_latency_fill_confirmed"):
            try:
                db_val = db.update_latency_fill_confirmed(
                    identifier=str(order_id),
                    fill_wall_time=fill_wt,
                    monotonic_ack_to_fill_ms=val,
                )
                if val is None and db_val is not None and db_val >= 0.0:
                    val = db_val
                    with self._lock:
                        self.ack_to_fill_latencies_ms.append(val)
                        if tr is not None:
                            tr.persisted_ack_to_fill_ms = val
                            tr.fill_wall_time = fill_wt
            except Exception:
                pass
        return val

    def record_protection_verified(
        self,
        order_id: str,
        now_ns: Optional[int] = None,
        wall_time: Optional[float] = None,
    ) -> Optional[float]:
        prot_wt = float(wall_time) if wall_time is not None else time.time()
        val: Optional[float] = None
        with self._lock:
            tr = self.traces_by_order_id.get(str(order_id))
            if tr is not None:
                if tr.l12_protection_verified_ns is None and tr.persisted_fill_to_protection_ms is None:
                    tr.mark_l12_protection_verified(now_ns=now_ns, wall_time=prot_wt)
                    val = tr.fill_to_protection_ms
                    if val is not None and val >= 0.0:
                        self.fill_to_protection_latencies_ms.append(val)
                else:
                    val = tr.fill_to_protection_ms
            db = self._db

        if db is not None and hasattr(db, "update_latency_protection_verified"):
            try:
                db_val = db.update_latency_protection_verified(
                    identifier=str(order_id),
                    protection_wall_time=prot_wt,
                    monotonic_fill_to_prot_ms=val,
                )
                if val is None and db_val is not None and db_val >= 0.0:
                    val = db_val
                    with self._lock:
                        self.fill_to_protection_latencies_ms.append(val)
                        if tr is not None:
                            tr.persisted_fill_to_protection_ms = val
                            tr.protection_wall_time = prot_wt
            except Exception:
                pass
        return val

    def get_correlated_trace(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Retrieves correlated L1–L12 trace by broker_order_id, correlation_id, or trace_id (in-memory or SQLite)."""
        with self._lock:
            tr = self.traces_by_order_id.get(str(identifier))
            if tr is not None:
                return tr.to_breakdown_dict()
            db = self._db
        if db is not None and hasattr(db, "get_latency_trace"):
            try:
                return db.get_latency_trace(str(identifier))
            except Exception:
                pass
        return None

    def record_rejection(self, reason: str) -> None:
        if not reason:
            return
        clean = str(reason).split(":")[0].strip()[:80]
        with self._lock:
            self.rejection_reasons[clean] = self.rejection_reasons.get(clean, 0) + 1
            db = self._db
        if db is not None and hasattr(db, "increment_latency_rejection_reason"):
            try:
                db.increment_latency_rejection_reason(clean)
            except Exception:
                pass

    @staticmethod
    def _percentile(sorted_vals: List[float], pct: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = (len(sorted_vals) - 1) * (pct / 100.0)
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return round(sorted_vals[lo], 2)
        frac = idx - lo
        return round(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac, 2)

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            yf = sorted(self.yahoo_http_latencies_ms)
            total_yf = self.yahoo_success_count + self.yahoo_failure_count
            yf_rate = round((self.yahoo_success_count / total_yf) * 100.0, 1) if total_yf > 0 else 0.0

            q_under_2 = sum(1 for a in self.quote_ages_sec if a <= 2.0)
            q_2_to_5 = sum(1 for a in self.quote_ages_sec if 2.0 < a <= 5.0)
            q_5_to_15 = sum(1 for a in self.quote_ages_sec if 5.0 < a <= 15.0)
            q_over_15 = sum(1 for a in self.quote_ages_sec if a > 15.0)

            return {
                "yahoo_requests_total": total_yf,
                "yahoo_success_count": self.yahoo_success_count,
                "yahoo_failure_count": self.yahoo_failure_count,
                "yahoo_success_rate_pct": yf_rate,
                "avg_yahoo_http_ms": round(statistics.mean(yf), 2) if yf else 0.0,
                "median_yahoo_http_ms": round(statistics.median(yf), 2) if yf else 0.0,
                "p95_yahoo_http_ms": self._percentile(yf, 95.0) if yf else 0.0,
                "max_yahoo_http_ms": round(max(yf), 2) if yf else 0.0,
                "avg_telegram_to_cmp_ms": round(statistics.mean(self.telegram_to_cmp_latencies_ms), 2) if self.telegram_to_cmp_latencies_ms else 0.0,
                "avg_telegram_to_ack_ms": round(statistics.mean(self.telegram_to_ack_latencies_ms), 2) if self.telegram_to_ack_latencies_ms else 0.0,
                "avg_ack_to_fill_ms": round(statistics.mean(self.ack_to_fill_latencies_ms), 2) if self.ack_to_fill_latencies_ms else 0.0,
                "avg_fill_to_protection_ms": round(statistics.mean(self.fill_to_protection_latencies_ms), 2) if self.fill_to_protection_latencies_ms else 0.0,
                "quote_age_distribution": {
                    "lte_2s": q_under_2,
                    "2s_to_5s": q_2_to_5,
                    "5s_to_15s": q_5_to_15,
                    "gt_15s": q_over_15,
                    "unknown": self.quote_age_unknown_count,
                },
                "rejection_reasons": dict(self.rejection_reasons),
            }

    def format_dashboard_section(self) -> str:
        s = self.get_summary()
        q_dist = s["quote_age_distribution"]
        lines = [
            "-------------------------------------------------------",
            "        LATENCY & CMP PERFORMANCE TELEMETRY            ",
            "-------------------------------------------------------",
            f"[*] Yahoo HTTP Requests : Total={s['yahoo_requests_total']} | Success={s['yahoo_success_count']} ({s['yahoo_success_rate_pct']}%) | Failed={s['yahoo_failure_count']}",
            f"[*] Yahoo HTTP Latency  : Avg={s['avg_yahoo_http_ms']:.1f}ms | Median={s['median_yahoo_http_ms']:.1f}ms | P95={s['p95_yahoo_http_ms']:.1f}ms | Max={s['max_yahoo_http_ms']:.1f}ms",
            f"[*] Quote Age Buckets   : <=2s: {q_dist['lte_2s']} | 2-5s: {q_dist['2s_to_5s']} | 5-15s: {q_dist['5s_to_15s']} | >15s: {q_dist['gt_15s']} | Unknown: {q_dist['unknown']}",
            f"[*] Stage Intervals     : TG->CMP Avg={s['avg_telegram_to_cmp_ms']:.1f}ms | TG->Broker Ack Avg={s['avg_telegram_to_ack_ms']:.1f}ms | Ack->Fill Avg={s['avg_ack_to_fill_ms']:.1f}ms | Fill->Prot Avg={s['avg_fill_to_protection_ms']:.1f}ms",
        ]
        if s["rejection_reasons"]:
            top_rej = ", ".join(f"{k}({v})" for k, v in list(s["rejection_reasons"].items())[:4])
            lines.append(f"[*] Rejection Summary   : {top_rej}")
        return "\n".join(lines)


GLOBAL_LATENCY_MONITOR = LatencyMonitor()
