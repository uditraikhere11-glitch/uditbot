"""
Instrument Resolver using official Dhan Scrip Master.
Implements STEP 13 (13-A through 13-E) + Darvas Script Enhancements + V13 Audit Hardening:
- Authoritative Dhan master normalization for listed OPTSTK and Cash Equities.
- V12-01 Fix: Builds staged catalogues off to the side and atomically swaps on `load_dhan_master`
  and deduplicates `load_option` by `(symbol, strike, option_type, expiry, exchange_segment, security_id)`
  so reloading the same master file never creates phantom duplicate option contracts.
- V12-25 Fix: Validates cache age/TTL (`max_cache_age_sec`), `.meta.json` SHA-256 integrity, and
  requires at least 1 valid parsed instrument (`(eq_count + opt_count) > 0`); builds `_name_index`
  once per batch rather than on every equity row.
- Dynamic company-name resolution (`normalize_name`, `build_name_index`, `resolve_company_name`).
- Strict pre-broker index rejection (OPTIDX and underlying index symbols).
"""

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import os
import csv
import io
import logging
import re
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
import requests

logger = logging.getLogger(__name__)

DHAN_DETAILED_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
DHAN_COMPACT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

INDEX_SYMBOLS = frozenset({
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYBANK",
    "NIFTYIT", "NIFTYAUTO", "NIFTYPHARMA", "NIFTYFMCG", "NIFTYMETAL", "SENSEX50"
})

# --- Darvas Script Company-Name Resolution Globals & Helpers ---
VALID_EQUITY_SYMBOLS: Set[str] = set()
_NAME_INDEX: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {}

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


def normalize_name(text: str) -> Tuple[str, ...]:
    """'Manipal Payment & Identity' -> ('MANIPAL', 'PAYMENT', 'AND', 'IDENTITY')"""
    return tuple(_NON_ALNUM.sub(" ", (text or "").upper().replace("&", " AND ")).split())


def build_name_index(rows: Iterable[Tuple[str, str, str]]) -> Dict[str, List[Tuple[Tuple[str, ...], str]]]:
    """
    rows: iterable of (exchange, ticker, company_name).
    NSE ticker wins when a company is listed on both NSE and BSE.
    Skips debt/bond rows listed as EQUITY (e.g. 'HEGICL-7.1%-09112031-NCD').
    """
    by_name: Dict[Tuple[str, ...], str] = {}
    for exchange, ticker, name in rows:
        words = normalize_name(name or "")
        if not words or not ticker or ticker[0].isdigit() or "%" in (name or "") or "NCD" in words:
            continue
        if words not in by_name or str(exchange).upper().startswith("NSE"):
            by_name[words] = ticker.strip().upper()
    index: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {}
    for words, ticker in by_name.items():
        index.setdefault(words[0], []).append((words, ticker))
    return index


def resolve_company_name(
    phrase_words: Iterable[str],
    name_index: Optional[Dict[str, List[Tuple[Tuple[str, ...], str]]]] = None,
) -> Optional[str]:
    """
    Map the words after 'bought'/'buy' to a ticker by company name,
    e.g. ('MANIPAL', 'PAYMENT') -> 'MPIMANIPAL'.
    """
    words = tuple(phrase_words)
    if not words:
        return None
    idx = name_index if name_index is not None else _NAME_INDEX
    candidates = idx.get(words[0], [])
    for n in range(len(words), 0, -1):
        prefix = words[:n]
        tickers = {t for name, t in candidates if name[:n] == prefix}
        if len(tickers) > 1:
            return None  # ambiguous: don't guess
        if len(tickers) == 1:
            ticker = tickers.pop()
            meaningful = [w for w in prefix if len(w) > 1 and w != "AND"]
            if len(meaningful) >= 2 or (n == 1 and len(prefix[0]) > 1 and ticker.startswith(prefix[0])):
                return ticker
            return None
    return None


@dataclass(frozen=True)
class EquityInstrument:
    symbol: str
    security_id: str
    exchange_segment: str
    tick_size: float = 0.05
    lot_size: int = 1
    company_name: str = ""


@dataclass(frozen=True)
class OptionContract:
    symbol: str
    security_id: str
    exchange_segment: str
    strike: float
    option_type: str
    expiry: date
    lot_size: int
    tick_size: float = 0.05
    trading_symbol: str = ""


class InstrumentResolver:
    def __init__(self, allow_next_month_expiry_fallback: bool = True):
        self._equities: Dict[str, EquityInstrument] = {}
        self._options: Dict[Tuple[str, float, str, date], OptionContract] = {}
        self._options_list: List[OptionContract] = []
        self._options_key_to_idx: Dict[Tuple[str, float, str, date, str, str], int] = {}
        self._company_rows: List[Tuple[str, str, str]] = []
        self._name_index: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {}
        self.last_loaded_at: float = 0.0
        self.last_loaded_path: str = ""
        self.allow_next_month_expiry_fallback: bool = allow_next_month_expiry_fallback

    def load_equity(
        self,
        symbol: str,
        security_id: str,
        exchange_segment: str = "NSE_EQ",
        tick_size: float = 0.05,
        company_name: str = "",
        rebuild_index: bool = True,
    ):
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return
        exch = "NSE" if "NSE" in exchange_segment.upper() else "BSE"
        if sym not in self._equities or exch == "NSE":
            self._equities[sym] = EquityInstrument(
                symbol=sym,
                security_id=str(security_id),
                exchange_segment=exchange_segment,
                tick_size=tick_size,
                lot_size=1,
                company_name=company_name,
            )
        if company_name:
            VALID_EQUITY_SYMBOLS.add(sym)
            self._company_rows.append((exch, sym, company_name))
            if rebuild_index:
                self._name_index = build_name_index(self._company_rows)
                _NAME_INDEX.clear()
                _NAME_INDEX.update(self._name_index)

    def load_company_names(self, rows: Iterable[Tuple[str, str, str]]):
        """
        Explicitly populates the company-name index and VALID_EQUITY_SYMBOLS from
        an iterable of (exchange, ticker, company_name) tuples.
        """
        row_list = list(rows)
        self._company_rows.extend(row_list)
        self._name_index = build_name_index(self._company_rows)
        _NAME_INDEX.clear()
        _NAME_INDEX.update(self._name_index)
        for exch, ticker, cname in row_list:
            clean_t = ticker.strip().upper()
            if clean_t and not clean_t[0].isdigit() and clean_t not in INDEX_SYMBOLS:
                VALID_EQUITY_SYMBOLS.add(clean_t)

    def resolve_company_name(self, phrase_words: Iterable[str]) -> Optional[str]:
        """Resolves a multi-word company name phrase to its canonical NSE/BSE ticker."""
        return resolve_company_name(phrase_words, self._name_index or _NAME_INDEX)

    def load_option(
        self,
        symbol: str,
        security_id: str,
        strike: float,
        option_type: str,
        expiry: date,
        lot_size: int,
        exchange_segment: str = "NSE_FNO",
        tick_size: float = 0.05,
        trading_symbol: str = "",
    ):
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return
        opt_type = option_type.strip().upper()
        sec_str = str(security_id).strip()
        seg_upper = exchange_segment.strip().upper()
        contract = OptionContract(
            symbol=sym,
            security_id=sec_str,
            exchange_segment=exchange_segment,
            strike=float(strike),
            option_type=opt_type,
            expiry=expiry,
            lot_size=int(lot_size),
            tick_size=float(tick_size),
            trading_symbol=trading_symbol,
        )
        self._options[(sym, float(strike), opt_type, expiry)] = contract
        # V12-01: Deduplicate identical (symbol, strike, option_type, expiry, segment, security_id) entries
        # O(1) indexed lookup prevents N^2 bottleneck when loading entire 100k scrip catalog.
        dedup_key = (sym, round(float(strike), 4), opt_type, expiry, seg_upper, sec_str)
        if dedup_key in self._options_key_to_idx:
            idx = self._options_key_to_idx[dedup_key]
            self._options_list[idx] = contract
        else:
            self._options_key_to_idx[dedup_key] = len(self._options_list)
            self._options_list.append(contract)

    # --- Atomic Staged Refresh (13-D) ---

    def atomic_stage_download(self, dest_path: str, url: str) -> bool:
        """
        13-D: Downloads catalog to temporary file, validates size, header integrity, and data rows,
        computes SHA256 hash, and atomically swaps via os.replace BEFORE updating metadata.
        Preserves previous verified catalog if download or validation fails.
        """
        tmp_path = f"{dest_path}.{os.getpid()}.tmp"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200 or len(resp.content) < 10000:
                logger.warning(f"Download from {url} failed or too small ({len(resp.content)} bytes)")
                return False

            with open(tmp_path, "wb") as f:
                f.write(resp.content)
                f.flush()
                os.fsync(f.fileno())

            # Validate header structure and at least 1 valid row before swapping
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f)
                header = next(reader, [])
                first_row = next(reader, None)
                if not any("SEM" in col or "SECURITY" in col or "INSTRUMENT" in col for col in header) or not first_row:
                    logger.error("Downloaded scrip master failed header/row validation")
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    return False

            hasher = hashlib.sha256()
            hasher.update(resp.content)
            sha256_hash = hasher.hexdigest()

            # Swap catalog first, then write metadata atomically
            os.replace(tmp_path, dest_path)

            meta_path = f"{dest_path}.meta.json"
            meta_tmp = f"{meta_path}.{os.getpid()}.tmp"
            meta = {
                "downloaded_at": datetime.now().isoformat(),
                "downloaded_ts": time.time(),
                "sha256": sha256_hash,
                "byte_size": len(resp.content),
                "source_url": url,
            }
            with open(meta_tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(meta_tmp, meta_path)

            logger.info(f"✅ Atomically updated Dhan scrip master: {dest_path} (SHA256: {sha256_hash[:12]}...)")
            return True
        except Exception as e:
            logger.error(f"Error during atomic master download: {e}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            return False

    def _is_cache_fresh(self, cache_path: str, max_cache_age_sec: float = 86400.0) -> bool:
        """V12-25: Validates cache age and optional .meta.json SHA-256 integrity."""
        if not cache_path or not os.path.exists(cache_path) or os.path.getsize(cache_path) <= 10000:
            return False
        meta_path = f"{cache_path}.meta.json"
        now = time.time()
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as mf:
                    meta = json.load(mf)
                dl_ts = float(meta.get("downloaded_ts") or 0.0)
                if not dl_ts and meta.get("downloaded_at"):
                    dl_ts = datetime.fromisoformat(meta["downloaded_at"]).timestamp()
                if dl_ts > 0 and (now - dl_ts) > max_cache_age_sec:
                    return False
                expected_sha = str(meta.get("sha256") or "").strip()
                if expected_sha:
                    with open(cache_path, "rb") as cf:
                        actual_sha = hashlib.sha256(cf.read()).hexdigest()
                    if actual_sha != expected_sha:
                        logger.warning(f"Scrip master cache SHA-256 mismatch for {cache_path}")
                        return False
                return True
            except Exception:
                pass
        try:
            mtime = os.path.getmtime(cache_path)
            return (now - mtime) <= max_cache_age_sec
        except Exception:
            return True

    def load_dhan_master(
        self,
        cache_path: Optional[str] = None,
        max_cache_age_sec: float = 86400.0,
        allow_network_download: bool = True,
    ) -> bool:
        """
        V12-01 & V12-25:
        - Checks cache freshness; refreshes if stale when `allow_network_download=True`.
        - Parses into staged structures and atomically swaps `self._equities`, `self._options`,
          `self._options_list`, `self._company_rows`, and `self._name_index` so reloading the
          same master never duplicates option or equity rows.
        - Rejects empty/invalid parses (`(eq_count + opt_count) == 0`) without clobbering existing data.
        """
        raw_text = ""
        cache_exists = bool(cache_path and os.path.exists(cache_path) and os.path.getsize(cache_path) > 10000)
        cache_fresh = self._is_cache_fresh(cache_path, max_cache_age_sec) if cache_exists else False

        if cache_exists and cache_fresh:
            try:
                with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                    raw_text = f.read()
                logger.info(f"Loaded fresh scrip master from local cache: {cache_path}")
            except Exception:
                raw_text = ""

        if not raw_text and cache_path and allow_network_download:
            for url in [DHAN_DETAILED_MASTER_URL, DHAN_COMPACT_MASTER_URL]:
                if self.atomic_stage_download(cache_path, url):
                    with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                        raw_text = f.read()
                    break

        if not raw_text and cache_exists:
            try:
                with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                    raw_text = f.read()
                logger.warning(f"Using stale scrip master fallback from local cache: {cache_path}")
            except Exception:
                raw_text = ""

        if not raw_text:
            print("Warning: Could not download or read Dhan scrip master.")
            return False

        staged_resolver = InstrumentResolver(allow_next_month_expiry_fallback=self.allow_next_month_expiry_fallback)
        eq_count = 0
        opt_count = 0
        names: List[Tuple[str, str, str]] = []
        try:
            reader = csv.DictReader(io.StringIO(raw_text))
            for row in reader:
                inst = (row.get("INSTRUMENT") or row.get("SEM_INSTRUMENT_NAME") or "").strip().upper()
                exch = (row.get("EXCH_ID") or row.get("SEM_EXM_EXCH_ID") or "").strip().upper()
                raw_sym = (row.get("TRADING_SYMBOL") or row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
                underlying_sym = (row.get("UNDERLYING_SYMBOL") or "").strip().upper()
                custom_name = (
                    row.get("SEM_CUSTOM_SYMBOL")
                    or row.get("DISPLAY_NAME")
                    or row.get("SM_SYMBOL_NAME")
                    or ""
                ).strip()
                sec_id = (row.get("SECURITY_ID") or row.get("SEM_SMST_SECURITY_ID") or "").strip()

                if inst in ("EQUITY", "EQ"):
                    sym = (underlying_sym or raw_sym).replace("-EQ", "").strip().upper()
                else:
                    sym = (underlying_sym or custom_name.upper() or raw_sym.replace("-EQ", "")).strip()

                if not sym or not sec_id or sym in INDEX_SYMBOLS or inst in ("OPTIDX", "FUTIDX", "INDEX"):
                    continue

                try:
                    raw_tick = float(row.get("TICK_SIZE") or row.get("SEM_TICK_SIZE") or "0.05")
                    tick = raw_tick / 100.0 if raw_tick >= 1.0 else raw_tick
                except ValueError:
                    tick = 0.05

                if inst in ("EQUITY", "EQ") and exch in ("NSE", "BSE"):
                    staged_resolver.load_equity(
                        sym,
                        security_id=sec_id,
                        exchange_segment=f"{exch}_EQ",
                        tick_size=tick,
                        company_name=custom_name,
                        rebuild_index=False,
                    )
                    if custom_name:
                        names.append((exch, sym, custom_name))
                    eq_count += 1
                elif inst in ("OPTSTK", "OPT") and exch in ("NSE", "BSE"):
                    exp_str = (row.get("SM_EXPIRY_DATE") or row.get("SEM_EXPIRY_DATE") or "").strip()
                    if exp_str:
                        try:
                            exp_date = datetime.strptime(exp_str[:10], "%Y-%m-%d").date()
                            strike = float(row.get("STRIKE_PRICE") or row.get("SEM_STRIKE_PRICE") or "0")
                            opt_type = (row.get("OPTION_TYPE") or row.get("SEM_OPTION_TYPE") or "CE").strip().upper()
                            lot = int(float(row.get("LOT_SIZE") or row.get("SEM_LOT_UNITS") or "1"))
                            staged_resolver.load_option(
                                symbol=sym,
                                security_id=sec_id,
                                strike=strike,
                                option_type=opt_type,
                                expiry=exp_date,
                                lot_size=lot,
                                exchange_segment=f"{exch}_FNO",
                                tick_size=tick,
                                trading_symbol=raw_sym,
                            )
                            opt_count += 1
                        except Exception:
                            pass

            # V12-25: Require at least 1 valid instrument row
            if (eq_count + opt_count) == 0:
                logger.error("Scrip master parsed 0 useful instruments; preserving existing catalog.")
                return False

            # V12-01 & V12-25: Atomically replace catalogues and build company-name index once
            self._equities = staged_resolver._equities
            self._options = staged_resolver._options
            self._options_list = staged_resolver._options_list
            self._options_key_to_idx = staged_resolver._options_key_to_idx
            self._company_rows = names
            self._name_index = build_name_index(names) if names else {}
            if names:
                _NAME_INDEX.clear()
                _NAME_INDEX.update(self._name_index)

            self.last_loaded_at = time.time()
            self.last_loaded_path = cache_path or ""
            print(
                f"✅ Loaded {len(self._equities)} equities ({len(self._name_index)} company name roots) "
                f"and {len(self._options_list)} option contracts from Dhan scrip master."
            )
            return True
        except Exception as e:
            print(f"Error parsing scrip master CSV: {e}")
            return False

    def resolve_equity(self, symbol: str) -> Tuple[Optional[EquityInstrument], str]:
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return None, f"Index trade rejected: {sym}"
        if sym in self._equities:
            return self._equities[sym], "Found"

        resolved_ticker = self.resolve_company_name(normalize_name(sym))
        if resolved_ticker and resolved_ticker in self._equities:
            return self._equities[resolved_ticker], f"Resolved company name '{symbol}' -> {resolved_ticker}"

        return None, f"Equity instrument not found in scrip master: {sym}"

    def resolve_option(
        self,
        symbol: str,
        strike: float,
        option_type: str,
        expiry_month: Optional[str] = None,
        reference_date: Optional[date] = None,
        exact_expiry: Optional[date] = None,
        expiry_day: Optional[int] = None,
        allow_next_month_fallback: Optional[bool] = None,
    ) -> Tuple[Optional[OptionContract], str]:
        """
        13-A / 13-B / INSTR-01: Resolves single-stock listed option contract.
        - If exact_expiry (or expiry_day) provided: matches exact listed date.
        - If expiry_month provided: selects the listed monthly contract; rejects if competing duplicate
          contracts exist on the exact same monthly expiry date (`INSTR-01`).
        - If expiry_month is omitted and current month has expired: falls back to nearest upcoming expiry
          when `allow_next_month_expiry_fallback` is enabled.
        """
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return None, f"Index options rejected: {sym}"

        opt_type = option_type.strip().upper()
        ref = reference_date or date.today()
        use_fallback = self.allow_next_month_expiry_fallback if allow_next_month_fallback is None else allow_next_month_fallback

        if expiry_day and not exact_expiry:
            mon_clean = expiry_month.strip()[:3].upper() if expiry_month else None
            target_month = MONTH_MAP.get(mon_clean, ref.month) if mon_clean else ref.month
            target_year = ref.year if target_month >= ref.month else (ref.year + 1 if expiry_month else ref.year)
            try:
                exact_expiry = date(target_year, target_month, int(expiry_day))
            except ValueError:
                return None, f"Invalid calendar expiry day/month: day={expiry_day}, month={expiry_month}"

        # 1. Exact expiry date lookup
        if exact_expiry:
            if exact_expiry < ref:
                return None, f"Contract expired on {exact_expiry.isoformat()}"
            matches = [
                c for c in self._options_list
                if c.symbol == sym
                and abs(c.strike - strike) < 0.01
                and c.option_type == opt_type
                and c.expiry == exact_expiry
            ]
            if len(matches) == 1:
                return matches[0], "Found exact match"
            if len(matches) > 1:
                nse_matches = [c for c in matches if "NSE" in c.exchange_segment.upper()]
                if len(nse_matches) == 1:
                    return nse_matches[0], "Found exact NSE match"
                return None, f"Ambiguous competing contracts for {sym} {strike:g}{opt_type} on {exact_expiry.isoformat()}"
            return None, f"No contract for {sym} {strike:g}{opt_type} exp={exact_expiry.isoformat()}"

        # 2. Month-based resolution
        if expiry_month:
            mon_clean = expiry_month.strip()[:3].upper()
            target_month = MONTH_MAP.get(mon_clean)
            if not target_month:
                return None, f"Invalid expiry month string: {expiry_month}"
            target_year = ref.year if target_month >= ref.month else ref.year + 1
        else:
            target_month = ref.month
            target_year = ref.year

        candidates = [
            c for c in self._options_list
            if c.symbol == sym
            and abs(c.strike - strike) < 0.01
            and c.option_type == opt_type
            and c.expiry.year == target_year
            and c.expiry.month == target_month
            and c.expiry >= ref
        ]

        if candidates:
            candidates.sort(key=lambda c: c.expiry)
            furthest_date = candidates[-1].expiry
            same_date = [c for c in candidates if c.expiry == furthest_date]
            if len(same_date) > 1:
                nse_same = [c for c in same_date if "NSE" in c.exchange_segment.upper()]
                if len(nse_same) == 1:
                    return nse_same[0], f"Found listed monthly contract ({nse_same[0].expiry.isoformat()})"
                return None, f"Ambiguous competing monthly contracts for {sym} {strike:g}{opt_type} on {furthest_date.isoformat()}"
            return candidates[-1], f"Found listed monthly contract ({candidates[-1].expiry.isoformat()})"

        # 3. Darvas Fallback when expiry_month was omitted and current month has no remaining expiries
        if not expiry_month and use_fallback:
            upcoming = [
                c for c in self._options_list
                if c.symbol == sym
                and abs(c.strike - strike) < 0.01
                and c.option_type == opt_type
                and c.expiry >= ref
            ]
            if upcoming:
                upcoming.sort(key=lambda c: c.expiry)
                return upcoming[0], f"Found nearest upcoming contract ({upcoming[0].expiry.isoformat()})"

        return None, f"No option contract found for {sym} {strike:g}{opt_type} in {target_year}-{target_month:02d}"
