"""Reporting-only NSE/BSE holiday context for health and recovery Telegram cards.

Never authorizes trading, changes any gate or treats missing calendar data as
an open exchange session. The existing market engine remains authoritative.
"""

from datetime import date, datetime, time, timedelta
from typing import Iterable, Tuple
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
SESSION_CLOSE = time(15, 35)


def format_market_calendar_lines(
    *, today: date, next_session_date: date,
    calendar_verified: bool, holidays_set: Iterable[date],
) -> Tuple[str, str]:
    """Separate calendar facts from the authentication/overall health verdict."""
    holidays = set(holidays_set or ())
    if today.weekday() >= 5:
        today_status = "🚫 CLOSED (Weekend)"
    elif not calendar_verified:
        today_status = "⚠️ UNVERIFIED (exchange holiday calendar unavailable)"
    elif today in holidays:
        today_status = "🚫 CLOSED (Exchange holiday)"
    else:
        today_status = "✅ SCHEDULED TRADING DAY"
    tomorrow = today + timedelta(days=1)
    day_label = tomorrow.strftime("%a %d %b %Y")
    if tomorrow.weekday() >= 5:
        tomorrow_status = "🚫 CLOSED (Weekend)"
    elif not calendar_verified:
        tomorrow_status = "⚠️ UNVERIFIED (exchange holiday calendar unavailable)"
    elif tomorrow in holidays:
        tomorrow_status = "🚫 CLOSED (Exchange holiday)"
    else:
        tomorrow_status = "✅ SCHEDULED TRADING DAY"

    if calendar_verified:
        next_status = f"{next_session_date:%a %d %b %Y}, 09:15 IST"
    else:
        next_status = "⚠️ UNVERIFIED (exchange calendar unavailable)"

    return (
        f"📅 Market today ({today:%a %d %b %Y}): {today_status}",
        f"📅 Market tomorrow ({day_label}): {tomorrow_status}",
        f"📆 Next scheduled session: {next_status}",
    )


def verified_market_calendar_lines(now_ts: float) -> Tuple[str, str]:
    """Read-only calendar summary for runtime recovery notices.

    This deliberately does not infer trading readiness, holidays from weekdays,
    or a next-session date from an incomplete exchange calendar.
    """
    from src.engine.market import load_holidays_verified

    now = datetime.fromtimestamp(float(now_ts), IST)
    today = now.date()
    next_session = today + (timedelta(days=1) if now.time() >= SESSION_CLOSE else timedelta())
    holiday_dates = set()
    years = set()
    ok = False
    try:
        loaded_ok, loaded, _ = load_holidays_verified(required_year=now.year)
        if loaded_ok and loaded:
            holiday_dates.update(loaded)
            years.add(now.year)
            ok = True
    except Exception:
        ok = False

    for _ in range(370):
        if not ok:
            break
        if next_session.weekday() >= 5:
            next_session += timedelta(days=1)
            continue
        if next_session.year not in years:
            try:
                year_ok, year_holidays, _ = load_holidays_verified(
                    required_year=next_session.year,
                )
                if not year_ok or not year_holidays:
                    ok = False
                    break
                holiday_dates.update(year_holidays)
                years.add(next_session.year)
            except Exception:
                ok = False
                break
        if next_session not in holiday_dates:
            break
        next_session += timedelta(days=1)
    else:
        ok = False

    return format_market_calendar_lines(
        today=today, next_session_date=next_session,
        calendar_verified=ok, holidays_set=holiday_dates,
    )
