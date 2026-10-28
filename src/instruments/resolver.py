"""
Instrument Resolver using official Dhan Scrip Master.
Resolves NSE/BSE cash equities and NFO single-stock options to official Dhan Security IDs,
lot sizes, tick sizes, and exchange segments. Rejects index contracts.
Supports both compact and detailed Dhan Scrip Master CSV formats.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import os
import csv
import io
import logging
import requests

logger = logging.getLogger(__name__)

# Detailed scrip master contains all derivatives (OPTSTK, FUTSTK, etc.) as well as cash equities
DHAN_DETAILED_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
DHAN_COMPACT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

INDEX_SYMBOLS = frozenset({
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYBANK"
})

@dataclass(frozen=True)
class EquityInstrument:
    symbol: str
    security_id: str
    exchange_segment: str
    tick_size: float = 0.05
    lot_size: int = 1

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
    def __init__(self):
        self._equities: Dict[str, EquityInstrument] = {}
        self._options: Dict[Tuple[str, float, str, int, int], OptionContract] = {}
        self._options_list: List[OptionContract] = []

    def load_equity(self, symbol: str, security_id: str, exchange_segment: str = "NSE_EQ", tick_size: float = 0.05):
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return
        self._equities[sym] = EquityInstrument(
            symbol=sym,
            security_id=security_id,
            exchange_segment=exchange_segment,
            tick_size=tick_size,
            lot_size=1,
        )

    def load_option(self, symbol: str, security_id: str, strike: float, option_type: str, expiry: date, lot_size: int, exchange_segment: str = "NSE_FNO", tick_size: float = 0.05, trading_symbol: str = ""):
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return
        opt_type = option_type.strip().upper()
        contract = OptionContract(
            symbol=sym,
            security_id=security_id,
            exchange_segment=exchange_segment,
            strike=strike,
            option_type=opt_type,
            expiry=expiry,
            lot_size=lot_size,
            tick_size=tick_size,
            trading_symbol=trading_symbol,
        )
        self._options[(sym, strike, opt_type, expiry.year, expiry.month)] = contract
        self._options_list.append(contract)

    def load_dhan_master(self, cache_path: Optional[str] = None) -> bool:
        raw_text = ""
        # Check local cache
        if cache_path and os.path.exists(cache_path) and os.path.getsize(cache_path) > 10000:
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    raw_text = f.read()
                logger.info(f"Loaded scrip master from local cache: {cache_path}")
            except Exception:
                pass

        if not raw_text:
            # Download detailed master first (contains both options and equities)
            for url in [DHAN_DETAILED_MASTER_URL, DHAN_COMPACT_MASTER_URL]:
                try:
                    print(f"📥 Downloading Dhan scrip master catalog ({url.split('/')[-1]})...")
                    resp = requests.get(url, timeout=30)
                    if resp.status_code == 200 and len(resp.text) > 10000:
                        raw_text = resp.text
                        if cache_path:
                            try:
                                os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)
                                with open(cache_path, "w", encoding="utf-8") as f:
                                    f.write(raw_text)
                            except Exception:
                                pass
                        break
                except Exception as e:
                    logger.warning(f"Failed downloading from {url}: {e}")

        if not raw_text:
            print("Warning: Could not download or read Dhan scrip master.")
            return False

        eq_count = 0
        opt_count = 0
        try:
            reader = csv.DictReader(io.StringIO(raw_text))
            for row in reader:
                # Flexible column extraction supporting both detailed and compact Dhan formats
                inst = (row.get("INSTRUMENT") or row.get("SEM_INSTRUMENT_NAME") or "").strip().upper()
                exch = (row.get("EXCH_ID") or row.get("SEM_EXM_EXCH_ID") or "").strip().upper()
                raw_sym = (row.get("TRADING_SYMBOL") or row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
                custom_sym = (row.get("UNDERLYING_SYMBOL") or row.get("SEM_CUSTOM_SYMBOL") or "").strip().upper()
                sym = custom_sym or raw_sym.replace("-EQ", "").strip()
                sec_id = (row.get("SECURITY_ID") or row.get("SEM_SMST_SECURITY_ID") or "").strip()

                if not sym or sym in INDEX_SYMBOLS:
                    continue

                # Tick size (Dhan detailed CSV uses paise, e.g. 5 paise = 0.05 rupees)
                try:
                    raw_tick = float(row.get("TICK_SIZE") or row.get("SEM_TICK_SIZE") or "0.05")
                    tick = raw_tick / 100.0 if raw_tick >= 1.0 else raw_tick
                except ValueError:
                    tick = 0.05

                # Cash Equities
                if inst in ("EQUITY", "EQ") and exch in ("NSE", "BSE"):
                    self.load_equity(sym, security_id=sec_id, exchange_segment=f"{exch}_EQ", tick_size=tick)
                    eq_count += 1
                # Stock Options
                elif inst in ("OPTSTK", "OPT") and exch in ("NSE", "BSE"):
                    exp_str = (row.get("SM_EXPIRY_DATE") or row.get("SEM_EXPIRY_DATE") or "").strip()
                    if exp_str:
                        try:
                            exp_date = datetime.strptime(exp_str[:10], "%Y-%m-%d").date()
                            strike = float(row.get("STRIKE_PRICE") or row.get("SEM_STRIKE_PRICE") or "0")
                            opt_type = (row.get("OPTION_TYPE") or row.get("SEM_OPTION_TYPE") or "CE").strip().upper()
                            lot = int(float(row.get("LOT_SIZE") or row.get("SEM_LOT_UNITS") or "1"))
                            self.load_option(
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

            print(f"✅ Loaded {eq_count} equities and {opt_count} option contracts from Dhan scrip master.")
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
        return None, f"Equity instrument not found in scrip master: {sym}"

    def resolve_option(self, symbol: str, strike: float, option_type: str, expiry_month: Optional[str] = None, reference_date: Optional[date] = None) -> Tuple[Optional[OptionContract], str]:
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return None, f"Index options rejected: {sym}"

        opt_type = option_type.strip().upper()
        ref = reference_date or date.today()

        if expiry_month:
            mon_clean = expiry_month.strip()[:3].upper()
            target_month = MONTH_MAP.get(mon_clean)
            if not target_month:
                return None, f"Invalid expiry month string: {expiry_month}"
            target_year = ref.year if target_month >= ref.month else ref.year + 1
        else:
            target_month = ref.month
            target_year = ref.year

        key = (sym, strike, opt_type, target_year, target_month)
        if key in self._options:
            contract = self._options[key]
            if contract.expiry < ref:
                return None, f"Contract expired on {contract.expiry.isoformat()}"
            return contract, "Found exact match"

        candidates = [
            c for c in self._options_list
            if c.symbol == sym
            and abs(c.strike - strike) < 0.01
            and c.option_type == opt_type
            and c.expiry.year == target_year
            and c.expiry.month == target_month
            and c.expiry >= ref
        ]

        if len(candidates) >= 1:
            candidates.sort(key=lambda c: c.expiry)
            return candidates[0], "Found match"
        return None, f"No option contract found for {sym} {strike:g}{opt_type} exp={target_year}-{target_month:02d}"