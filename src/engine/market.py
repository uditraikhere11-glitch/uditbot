"""
NSE/BSE Market Hours, Holiday Calendar, and Timing Engine (all times IST).
Restricts fresh order entries strictly between 09:15 and 15:25 IST on trading weekdays (`is_market_open`),
while keeping lifecycle/protection monitoring open through market close (`is_lifecycle_open`, `V12-06`).
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


def load_holidays(path: Optional[str] = None) -> Set[date]:
    """
    Loads exchange holidays from {"holidays": ["YYYY-MM-DD", ...]}.
    Returns empty set if file not found or invalid.
    """
    filepath = path or os.environ.get("HOLIDAYS_FILE") or DEFAULT_HOLIDAYS_PATH
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            holidays_list = data.get("holidays", [])
            return {date.fromisoformat(d) for d in holidays_list if d}
    except FileNotFoundError:
        logger.warning(f"Holidays file {filepath} not found; treating all non-weekends as open.")
        return set()
    except Exception as e:
        logger.error(f"Could not read holidays file {filepath}: {e}")
        return set()


def is_market_open(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
) -> bool:
    """
    Returns True if current time is within active market hours for fresh entries:
    - Monday to Friday (weekday < 5)
    - Not an NSE trading holiday
    - Time between 09:15 and 15:25 IST (or custom cutoff)
    """
    is_open, _ = market_status(now=now, holidays=holidays, cutoff=cutoff)
    return is_open


def is_lifecycle_open(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
) -> bool:
    """
    V12-06: Returns True during full market & post-entry settlement hours (09:15 to 15:35 IST)
    so ProtectionWatcher continues monitoring fills, cancellations, and OCO/Super Order legs
    after the 15:25 fresh-entry cutoff.
    """
    active_cutoff = cutoff or LIFECYCLE_CLOSE
    is_open, _ = market_status(now=now, holidays=holidays, cutoff=active_cutoff)
    return is_open


def market_status(
    now: Optional[datetime] = None,
    holidays: Optional[Iterable[date]] = None,
    cutoff: Optional[time] = None,
) -> Tuple[bool, str]:
    """
    Evaluates market open/closed status and returns (is_open: bool, reason: str).
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

    # 2. Holiday check
    active_holidays = set(holidays) if holidays is not None else load_holidays()
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
