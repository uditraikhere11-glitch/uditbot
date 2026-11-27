"""Pure exchange-session classification, separate from acquisition and order policy.

A calendar and a broker-observed book/session marker are inputs, not a host-date
inference. An empty book alone never proves that EOD rollover completed.
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from zoneinfo import ZoneInfo
from src.broker.broker_snapshot import SessionContext, SessionPhase, payload_digest, NS

IST = ZoneInfo('Asia/Kolkata')


class BrokerSessionState(str, Enum):
    T0_SIGNED = 'T0_SIGNED'
    PRE_EOD_PRIOR_SESSION = 'PRE_EOD_PRIOR_SESSION'
    T1_SETTLED = 'T1_SETTLED'
    AMBIGUOUS_BLOCKED = 'AMBIGUOUS_BLOCKED'


@dataclass(frozen=True, slots=True)
class VerifiedCalendar:
    holidays: frozenset[date]
    covered_years: frozenset[int]

    def __post_init__(self):
        object.__setattr__(self, 'holidays', frozenset(self.holidays))
        object.__setattr__(self, 'covered_years', frozenset(self.covered_years))
        if (any(type(d) is not date or d.year not in self.covered_years for d in self.holidays)
                or any(type(y) is not int or not 2000 <= y <= 2100 for y in self.covered_years)):
            raise ValueError('Invalid verified calendar')

    @property
    def digest(self):
        return payload_digest({'holidays': sorted(d.isoformat() for d in self.holidays),
                               'covered_years': sorted(self.covered_years)})

    def trading(self, day):
        if day.year not in self.covered_years:
            raise ValueError('Calendar year unavailable')
        return day.weekday() < 5 and day not in self.holidays

    def previous(self, day):
        for n in range(1, 15):
            candidate = day - timedelta(days=n)
            if self.trading(candidate):
                return candidate
        raise ValueError('Previous trading date unavailable')


def classify_broker_session(*, observed_utc_ns: int, calendar: VerifiedCalendar,
                            execution_dates, broker_session_date: date | None,
                            books_complete: bool, settlement_verified: bool,
                            signed_delivery_verified: bool, product: str):
    """Return explicit state plus locked Phase 2B SessionContext.

    `settlement_verified` and `signed_delivery_verified` are independent
    evidence facts, not simply the local time or absence of positions/trades.
    Unknown or contradictory evidence blocks. This function performs no I/O.
    """
    blocked = (BrokerSessionState.AMBIGUOUS_BLOCKED,
               SessionContext(SessionPhase.TRANSITION_UNVERIFIED, None,
                              observed_utc_ns, None, False))
    if (books_complete is not True or not isinstance(calendar, VerifiedCalendar)
            or type(broker_session_date) is not date
            or type(settlement_verified) is not bool or type(signed_delivery_verified) is not bool):
        return blocked
    try:
        current = datetime.fromtimestamp(observed_utc_ns // NS, IST)
        day = current.date()
        dates = frozenset(execution_dates)
        if (any(type(d) is not date or d > day for d in dates)
                or len(dates) > 1 or not calendar.trading(broker_session_date)
                or (dates and dates != {broker_session_date})):
            return blocked
        if settlement_verified and signed_delivery_verified:
            return blocked
        if broker_session_date == day and calendar.trading(day):
            if signed_delivery_verified and dates == {day}:
                phase, state = SessionPhase.CURRENT_SIGNED, BrokerSessionState.T0_SIGNED
            elif settlement_verified and not dates:
                phase, state = SessionPhase.SETTLED, BrokerSessionState.T1_SETTLED
            else:
                return blocked
        elif (product == 'CNC' and signed_delivery_verified and not settlement_verified
              and current.time() < time(8, 30) and dates == {broker_session_date}
              and broker_session_date == calendar.previous(day)):
            phase, state = SessionPhase.PRIOR_SIGNED, BrokerSessionState.PRE_EOD_PRIOR_SESSION
        else:
            return blocked
    except (ValueError, TypeError, OverflowError, OSError):
        return blocked
    return state, SessionContext(phase, broker_session_date, observed_utc_ns, calendar.digest, True)
