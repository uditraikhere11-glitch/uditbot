"""
Yahoo Finance Darvas Observational Price Retriever.

Implements observational price retrieval using Darvas's working approach:
https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}.NS?interval=1d&range=1d

Explicit architectural separation:
1. Observational retrieval:
   - Fetches reported price, quote timestamp, measured age, currency, exchange, marketState, and delay metadata.
   - Missing marketState is reported as 'UNKNOWN', not falsely classified as closed.
   - Missing exchangeDataDelayedBy is NEVER interpreted as zero delay.
   - Explicitly marked executable=False.
2. An observational quote must NEVER silently become an executable quote.
"""

from dataclasses import dataclass
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"


@dataclass
class YahooObservation:
    ticker: str
    symbol: str
    exchange_segment: str
    reported_price: Optional[float] = None
    quote_timestamp: Optional[float] = None
    measured_age_sec: Optional[float] = None
    exchange: str = "UNKNOWN"
    currency: str = "UNKNOWN"
    market_state: str = "UNKNOWN"
    delayed_by_sec: Optional[int] = None
    delay_status: str = "UNKNOWN"
    is_success: bool = False
    status_code: Optional[int] = None
    error: Optional[str] = None
    source_name: str = "YAHOO_FINANCE_DARVAS_OBSERVER"
    executable: bool = False  # Hard contract: NEVER executable

    def format_diagnostic(self) -> str:
        lines = [
            f"[*] Observer Source     : {self.source_name} (executable={self.executable})",
            f"[*] Ticker / Venue      : {self.ticker} ({self.exchange_segment})",
        ]
        if self.is_success and self.reported_price is not None:
            lines.append(f"[*] Reported Price      : {self.currency} {self.reported_price:,.2f}")
            age_str = f"{self.measured_age_sec:.2f}s" if self.measured_age_sec is not None else "UNKNOWN"
            lines.append(f"[*] Quote Timestamp     : {self.quote_timestamp} (Age: {age_str})")
            lines.append(f"[*] Market State        : {self.market_state} (Exchange: {self.exchange})")
            delay_str = f"{self.delayed_by_sec}s delay" if self.delayed_by_sec is not None else "UNKNOWN (Missing metadata - cannot assume real-time)"
            lines.append(f"[*] Feed Delay Metadata : {delay_str}")
            lines.append("[*] Execution Status    : OBSERVATION ONLY (Not authorized for order execution)")
        else:
            lines.append(f"[!] Observation Failed  : {self.error or 'Unknown error'}")
            if self.status_code:
                lines.append(f"[!] HTTP Status Code    : {self.status_code}")
        return "\n".join(lines)


class YahooObserver:
    """Darvas-compatible observational quote fetcher for diagnostics."""

    def __init__(self, session=None):
        if session is None:
            import requests
            self.session = requests.Session()
        else:
            self.session = session

    def observe(
        self,
        symbol: str,
        exchange_segment: str = "NSE_EQ",
        now_utc: Optional[float] = None,
        timeout: float = 5.0,
    ) -> YahooObservation:
        clean_sym = (symbol or "").replace("-EQ", "").strip().upper()
        seg = (exchange_segment or "NSE_EQ").strip().upper()
        suffix = ".BO" if seg == "BSE_EQ" else ".NS"
        ticker = f"{clean_sym}{suffix}" if clean_sym else "UNKNOWN"

        obs = YahooObservation(
            ticker=ticker,
            symbol=clean_sym,
            exchange_segment=seg,
            executable=False,
        )

        if not clean_sym:
            obs.error = "Missing symbol for Yahoo Finance observation"
            return obs

        url = YAHOO_CHART_URL.format(ticker=ticker)
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            t0 = time.time()
            resp = self.session.get(url, headers=headers, timeout=timeout)
            elapsed = time.time() - t0
            obs.status_code = resp.status_code

            if resp.status_code != 200:
                obs.error = f"HTTP {resp.status_code} returned by Yahoo Finance"
                return obs

            data = resp.json()
            results = (data.get("chart") or {}).get("result") or []
            if not results or not isinstance(results[0], dict):
                obs.error = "Empty chart result in Yahoo Finance response"
                return obs

            meta = results[0].get("meta") or {}
            raw_price = meta.get("regularMarketPrice")
            raw_ts = meta.get("regularMarketTime")

            if raw_price is not None:
                try:
                    obs.reported_price = float(raw_price)
                except (ValueError, TypeError):
                    obs.reported_price = None

            if raw_ts is not None:
                try:
                    obs.quote_timestamp = float(raw_ts)
                    eval_now = now_utc if now_utc is not None else time.time()
                    obs.measured_age_sec = max(0.0, eval_now - obs.quote_timestamp)
                except (ValueError, TypeError):
                    obs.quote_timestamp = None
                    obs.measured_age_sec = None

            obs.exchange = str(meta.get("exchangeName") or meta.get("fullExchangeName") or "UNKNOWN").strip()
            obs.currency = str(meta.get("currency") or "UNKNOWN").strip().upper()

            # Critical: Missing marketState must be reported as UNKNOWN, never falsely closed
            raw_market_state = meta.get("marketState")
            if raw_market_state is not None and str(raw_market_state).strip():
                obs.market_state = str(raw_market_state).strip().upper()
            else:
                obs.market_state = "UNKNOWN"

            # Critical: Missing exchangeDataDelayedBy must NEVER be interpreted as zero delay
            raw_delay = meta.get("exchangeDataDelayedBy")
            if raw_delay is not None:
                try:
                    obs.delayed_by_sec = int(raw_delay)
                    obs.delay_status = "REALTIME" if obs.delayed_by_sec == 0 else "DELAYED"
                except (ValueError, TypeError):
                    obs.delayed_by_sec = None
                    obs.delay_status = "UNKNOWN"
            else:
                obs.delayed_by_sec = None
                obs.delay_status = "UNKNOWN"

            obs.is_success = (obs.reported_price is not None)
            if not obs.is_success:
                obs.error = "No regularMarketPrice in metadata"

            return obs
        except Exception as e:
            obs.error = f"Exception fetching Yahoo quote: {e}"
            return obs
