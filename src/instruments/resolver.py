"""
Instrument Resolver using official Dhan Scrip Master.
Resolves NSE/BSE cash equities and NFO single-stock options to official Dhan Security IDs,
lot sizes, tick sizes, and exchange segments. Rejects index contracts.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

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
    exchange_segment: str  # NSE_EQ | BSE_EQ
    tick_size: float = 0.05
    lot_size: int = 1


@dataclass(frozen=True)
class OptionContract:
    symbol: str
    security_id: str
    exchange_segment: str  # NSE_FNO | BSE_FNO
    strike: float
    option_type: str  # CE | PE
    expiry: date
    lot_size: int
    tick_size: float = 0.05
    trading_symbol: str = ""


class InstrumentResolver:
    """
    In-memory registry of validated instruments derived from Dhan scrip master.
    """

    def __init__(self):
        # Maps: symbol -> EquityInstrument
        self._equities: Dict[str, EquityInstrument] = {}
        # Maps: (symbol, strike, option_type, expiry_year, expiry_month) -> OptionContract
        self._options: Dict[Tuple[str, float, str, int, int], OptionContract] = {}
        # List of all option contracts for monthly lookup
        self._options_list: List[OptionContract] = []

    def load_equity(self, symbol: str, security_id: str, exchange_segment: str = "NSE_EQ", tick_size: float = 0.05):
        """Registers a cash equity instrument."""
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return  # Never register index instruments
        self._equities[sym] = EquityInstrument(
            symbol=sym,
            security_id=security_id,
            exchange_segment=exchange_segment,
            tick_size=tick_size,
            lot_size=1,
        )

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
        """Registers a stock option instrument."""
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return  # Never register index options
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
        key = (sym, strike, opt_type, expiry.year, expiry.month)
        self._options[key] = contract
        self._options_list.append(contract)

    def resolve_equity(self, symbol: str) -> Tuple[Optional[EquityInstrument], str]:
        """Resolves an equity ticker to Dhan instrument specifications."""
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return None, f"Index trade rejected: {sym}"
        if sym in self._equities:
            return self._equities[sym], "Found"
        return None, f"Equity instrument not found in scrip master: {sym}"

    def resolve_option(
        self,
        symbol: str,
        strike: float,
        option_type: str,
        expiry_month: Optional[str] = None,
        reference_date: Optional[date] = None,
    ) -> Tuple[Optional[OptionContract], str]:
        """
        Resolves a stock option to an exact contract.
        If expiry_month is omitted, defaults to current calendar month.
        """
        sym = symbol.strip().upper()
        if sym in INDEX_SYMBOLS:
            return None, f"Index options rejected: {sym}"

        opt_type = option_type.strip().upper()
        ref = reference_date or date.today()

        # Determine target month and year
        if expiry_month:
            mon_clean = expiry_month.strip()[:3].upper()
            target_month = MONTH_MAP.get(mon_clean)
            if not target_month:
                return None, f"Invalid expiry month string: {expiry_month}"
            # Year heuristic: if target_month < ref.month, assume next year
            target_year = ref.year if target_month >= ref.month else ref.year + 1
        else:
            # Current calendar month default
            target_month = ref.month
            target_year = ref.year

        key = (sym, strike, opt_type, target_year, target_month)
        if key in self._options:
            contract = self._options[key]
            # Ensure contract expiry has not already passed
            if contract.expiry < ref:
                return None, f"Contract expired on {contract.expiry.isoformat()}"
            return contract, "Found exact match"

        # Search candidates in that month
        candidates = [
            c for c in self._options_list
            if c.symbol == sym
            and abs(c.strike - strike) < 0.01
            and c.option_type == opt_type
            and c.expiry.year == target_year
            and c.expiry.month == target_month
            and c.expiry >= ref
        ]

        if len(candidates) == 1:
            return candidates[0], "Found match"
        elif len(candidates) > 1:
            # Sort by nearest future expiry
            candidates.sort(key=lambda c: c.expiry)
            return candidates[0], "Multiple expiries found; selected nearest"
        else:
            return None, (
                f"No option contract found for {sym} {strike:g}{opt_type} "
                f"exp={target_year}-{target_month:02d}"
            )
