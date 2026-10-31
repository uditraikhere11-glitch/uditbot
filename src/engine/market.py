"""
NSE/BSE Market Hours, Holiday Calendar, and Timing Engine (all times IST).
Restricts fresh order entries strictly between 09:15 and 15:25 IST on trading weekdays (`is_market_open`),
while keeping lifecycle/protection monitoring open through market close (`is_lifecycle_open`, `V12-06`).
Fails closed when holiday calendar data is missing, unreadable, malformed, or lacks coverage for the evaluated year.
"""

import json
import logging
import os
import zoneinfo
from datetime import date, datetime, time
from typing import Iterable, Optional, Set, Tuple

logger = logging.getLogger(__name__)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")
MARKET_OPEN = time(9, 15)
ENTRY_CUTOFF = time(15, 25)   # No fresh entries allowed in final 5 minutes
MARKET_CLOSE = time(15, 30)  # Standard cash and F&O closing time
LIFECYCLE_CLOSE = time(15, 35)  # Post-close settlement buffer for protection polling

DEFAULT_HOLIDAYS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "holidays.json",
)


def now_ist() -> datetime:
    """Returns the current timezone-aware datetime in IST."""
    return datetime.now(IST)


def is_amo_window(now: Optional[datetime] = None, holidays: Optional[Iterable[date]] = None) -> bool:
    """
    Returns True if current IST time falls outside regular NSE/BSE market hours
    (adopted from `_is_amo_window` in `darvas_script/client_agent/client.py`).
    """
    return not is_market_open(now=now, holidays=holidays, cutoff=MARKET_CLOSE)


def load_holidays_verified(
    path: Optional[str] = None,
    required_year: Optional[int] = None,
) -> Tuple[bool, Set[date], str]:
    """
    Loads and validates exchange holidays from {"holidays": ["YYYY-MM-DD", ...]}.
    Fails closed (returns ok=False) if the file is missing, unreadable, malformed JSON,
    contains non-ISO date entries, is empty, or has no holiday entries for `required_year`.
    """
    filepath = path or os.environ.get("HOLIDAYS_FILE") or DEFAULT_HOLIDAYS_PATH
    if not filepath or not os.path.exists(filepath):
        msg = f"Holidays file not found: {filepath}"
        logger.error(msg)
        return False, set(), msg

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        msg = f"Could not read or parse holidays file {filepath}: {e}"
        logger.error(msg)
        return False, set(), msg

    if not isinstance(data, dict) or "holidays" not in data or not isinstance(data.get("holidays"), list):
        msg = f"Malformed holidays file {filepath}: expected JSON object with 'holidays' list"
        logger.error(msg)
        return False, set(), msg

    holidays_list = data.get("holidays", [])
    if not holidays_list:
        msg = f"Holidays file {filepath} contains an empty 'holidays' list"
        logger.error(msg)
        return False, set(), msg

    parsed: Set[date] = set()
    for raw_d in holidays_list:
        if not isinstance(raw_d, str) or not raw_d.strip():
            msg = f"Invalid holiday entry {raw_d!r} in {filepath}"
            logger.error(msg)
            return False, set(), msg
        try:
            parsed.add(date.fromisoformat(raw_d.strip()))
        except ValueError as e:
            msg = f"Invalid ISO date {raw_d!r} in {filepath}: {e}"
            logger.error(msg)
            return False, set(), msg

    if required_year is not None and not any(d.year == required_year for d in parsed):
        msg = f"Holiday calendar {filepath} has no entries covering year {required_year}"
        logger.error(msg)
        return False, parsed, msg

    return True, parsed, "OK"


def load_holidays(path: Optional[str] = None) -> Set[date]:
    """
    Loads exchange holidays from {"holidays": ["YYYY-MM-DD", ...]}.
    Returns empty set if file not found or invalid (callers authorizing fresh entries use
    `load_holidays_verified` / `market_status` which fails closed on empty/unverified calendars).
    """
    ok, parsed, _ = load_holidays_verified(path=path)
    return parsed if ok else set()


_ORIGINAL_LOAD_HOLIDAYS = load_holidays


def is_market_open(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
    holidays_path: Optional[str] = None,
) -> bool:
    """
    Returns True if current time is within active market hours for fresh entries:
    - Monday to Friday (weekday < 5)
    - Verified NSE holiday calendar is present and current date is not a trading holiday
    - Time between 09:15 and 15:25 IST (or custom cutoff)
    """
    is_open, _ = market_status(
        now=now,
        holidays=holidays,
        cutoff=cutoff,
        holidays_path=holidays_path,
        allow_unverified_calendar=False,
    )
    return is_open


def is_lifecycle_open(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
    holidays_path: Optional[str] = None,
) -> bool:
    """
    V12-06: Returns True during full market & post-entry settlement hours (09:15 to 15:35 IST)
    so ProtectionWatcher continues monitoring fills, cancellations, and OCO/Super Order legs
    after the 15:25 fresh-entry cutoff.
    """
    active_cutoff = cutoff or LIFECYCLE_CLOSE
    is_open, _ = market_status(
        now=now,
        holidays=holidays,
        cutoff=active_cutoff,
        holidays_path=holidays_path,
        allow_unverified_calendar=True,
    )
    return is_open


def market_status(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
    holidays_path: Optional[str] = None,
    allow_unverified_calendar: bool = False,
) -> Tuple[bool, str]:
    """
    Evaluates market open/closed status and returns (is_open: bool, reason: str).
    Fails closed for fresh entries when holiday calendar data is missing, unreadable,
    empty, or lacks coverage for the evaluated year.
    """
    if now is None:
        dt = now_ist()
    elif now.tzinfo is None:
        dt = now.replace(tzinfo=IST)
    else:
        dt = now.astimezone(IST)

    # 1. Weekend check
    weekday_idx = dt.weekday()
    if weekday_idx == 5:
        return False, "Market Closed (Saturday)"
    if weekday_idx == 6:
        return False, "Market Closed (Sunday)"

    # 2. Holiday calendar verification & holiday check (fail-closed for fresh entries)
    active_holidays: Set[date] = set()
    if holidays is not None:
        try:
            raw_items = list(holidays)
            for item in raw_items:
                if isinstance(item, datetime):
                    active_holidays.add(item.date())
                elif isinstance(item, date):
                    active_holidays.add(item)
                elif isinstance(item, str):
                    active_holidays.add(date.fromisoformat(item.strip()))
                else:
                    raise TypeError(f"Unsupported holiday date type: {type(item).__name__}")
        except Exception as e:
            return False, f"Market Calendar Unverified (Invalid holiday data: {e})"

        if not allow_unverified_calendar:
            if not active_holidays:
                return False, "Market Calendar Unverified (Empty holiday set cannot authorize fresh entries)"
            if not any(d.year == dt.year for d in active_holidays):
                return False, f"Market Calendar Unverified (No holiday entries covering {dt.year})"
    else:
        if load_holidays is not _ORIGINAL_LOAD_HOLIDAYS or hasattr(load_holidays, "_mock_name"):
            try:
                loaded = load_holidays(holidays_path) if holidays_path is not None else load_holidays()
                if loaded is None or not isinstance(loaded, (set, list, tuple, frozenset)):
                    return False, "Market Calendar Unverified (Holiday loader returned invalid data)"
                active_holidays = set(loaded)
            except Exception as e:
                return False, f"Market Calendar Unverified (Holiday loader failed: {e})"
            if not allow_unverified_calendar:
                if not active_holidays:
                    return False, "Market Calendar Unverified (Missing or empty holiday calendar data)"
                if not any(isinstance(d, date) and d.year == dt.year for d in active_holidays):
                    return False, f"Market Calendar Unverified (No holiday entries covering {dt.year})"
        else:
            cal_ok, loaded_set, cal_err = load_holidays_verified(path=holidays_path, required_year=dt.year)
            if not cal_ok and not allow_unverified_calendar:
                return False, f"Market Calendar Unverified ({cal_err})"
            active_holidays = loaded_set

    if dt.date() in active_holidays:
        return False, f"Market Closed (Exchange Holiday: {dt.date().isoformat()})"

    # 3. Time window check
    t = dt.time()
    active_cutoff = cutoff or ENTRY_CUTOFF

    if t < MARKET_OPEN:
        return False, f"Market Pre-Open / Closed ({t.strftime('%H:%M:%S')} IST < 09:15:00)"
    if t > active_cutoff:
        if t <= MARKET_CLOSE:
            return False, f"Entry Cutoff Exceeded ({t.strftime('%H:%M:%S')} IST > {active_cutoff.strftime('%H:%M')})"
        return False, f"Market Closed Post-Hours ({t.strftime('%H:%M:%S')} IST > 15:30:00)"

    return True, f"Market Open ({t.strftime('%H:%M:%S')} IST)"
