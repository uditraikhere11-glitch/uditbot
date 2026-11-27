"""Deterministic clock infrastructure for Phase 3 Golden Replay.

Covers time.time(), time.time_ns(), time.monotonic(), time.monotonic_ns(),
UTC/IST datetimes, exchange calendar dates, midnight rollover, weekend/holiday
transitions, wall-clock regression, monotonic anomaly, and simulated reboot.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

from src.broker.broker_snapshot import NS, SessionContext, SessionPhase
from src.engine.broker_session import VerifiedCalendar, classify_broker_session

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc


@dataclass
class ClockSnapshot:
    utc_ns: int
    mono_ns: int
    boot_id: str
    ist_iso: str
    exchange_date: str


class DeterministicClock:
    """Single coherent controlled clock with nanosecond-exact conversions."""

    def __init__(
        self,
        *,
        initial_utc: datetime | None = None,
        initial_mono_ns: int = 10_000 * NS,
        boot_id: str = "golden-boot-001",
        holidays: Iterable[date] = (),
        covered_years: Iterable[int] = (2025, 2026, 2027),
    ) -> None:
        if initial_utc is None:
            initial_utc = datetime(2026, 11, 25, 10, 0, 0, tzinfo=IST).astimezone(UTC)
        elif initial_utc.tzinfo is None:
            initial_utc = initial_utc.replace(tzinfo=UTC)
        else:
            initial_utc = initial_utc.astimezone(UTC)
        self._utc_ns: int = int(round(initial_utc.timestamp() * NS))
        self._mono_ns: int = int(initial_mono_ns)
        self._boot_id: str = str(boot_id)
        self._calendar = VerifiedCalendar(
            holidays=frozenset(holidays),
            covered_years=frozenset(covered_years),
        )
        self._calendar_sha256 = self._compute_calendar_sha()

    def _compute_calendar_sha(self) -> str:
        import hashlib
        raw = ",".join(sorted(d.isoformat() for d in self._calendar.holidays))
        yrs = ",".join(str(y) for y in sorted(self._calendar.covered_years))
        return hashlib.sha256(f"{yrs}|{raw}".encode("utf-8")).hexdigest()

    @property
    def boot_id(self) -> str:
        return self._boot_id

    @property
    def calendar(self) -> VerifiedCalendar:
        return self._calendar

    @property
    def calendar_sha256(self) -> str:
        return self._calendar_sha256

    def time_ns(self) -> int:
        return self._utc_ns

    def time(self) -> float:
        return self._utc_ns / NS

    def monotonic_ns(self) -> int:
        return self._mono_ns

    def monotonic(self) -> float:
        return self._mono_ns / NS

    def utc_datetime(self) -> datetime:
        return datetime.fromtimestamp(self._utc_ns / NS, tz=UTC)

    def ist_datetime(self) -> datetime:
        return datetime.fromtimestamp(self._utc_ns / NS, tz=IST)

    def exchange_date(self) -> date:
        return self.ist_datetime().date()

    def is_trading_day(self, d: date | None = None) -> bool:
        target = d or self.exchange_date()
        return target.weekday() < 5 and target not in self._calendar.holidays

    def advance(self, seconds: float) -> ClockSnapshot:
        delta_ns = int(round(float(seconds) * NS))
        if delta_ns < 0:
            raise ValueError("Use regress_wall_clock() for negative wall-clock steps")
        self._utc_ns += delta_ns
        self._mono_ns += delta_ns
        return self.snapshot()

    def advance_to_ist(self, target_ist: datetime) -> ClockSnapshot:
        if target_ist.tzinfo is None:
            target_ist = target_ist.replace(tzinfo=IST)
        target_ns = int(round(target_ist.astimezone(UTC).timestamp() * NS))
        delta_ns = target_ns - self._utc_ns
        if delta_ns < 0:
            raise ValueError("target_ist is earlier than current clock; use regress_wall_clock()")
        self._utc_ns = target_ns
        self._mono_ns += delta_ns
        return self.snapshot()

    def regress_wall_clock(self, seconds: float) -> ClockSnapshot:
        """Simulate NTP/wall-clock backward jump while monotonic clock continues."""
        delta_ns = int(round(abs(float(seconds)) * NS))
        self._utc_ns -= delta_ns
        return self.snapshot()

    def inject_monotonic_anomaly(self, delta_seconds: float) -> ClockSnapshot:
        """Simulate monotonic jump or regression (e.g., broken host clock / VM resume)."""
        self._mono_ns += int(round(float(delta_seconds) * NS))
        return self.snapshot()

    def simulate_reboot(self, *, new_boot_id: str, downtime_seconds: float = 15.0) -> ClockSnapshot:
        """Simulate VM reboot: wall clock advances, monotonic resets, boot_id changes."""
        self._utc_ns += int(round(float(downtime_seconds) * NS))
        self._mono_ns = 100 * NS
        self._boot_id = str(new_boot_id)
        return self.snapshot()

    def session_context(
        self,
        *,
        phase: SessionPhase | None = None,
        trading_session_date: date | None = None,
        calendar_verified: bool = True,
        execution_dates: Iterable[date] = (),
        broker_session_date: date | None = None,
        books_complete: bool = True,
        settlement_verified: bool = False,
        signed_delivery_verified: bool = False,
        product: str = "CNC",
    ) -> SessionContext:
        if phase is not None:
            return SessionContext(
                phase=phase,
                trading_session_date=trading_session_date or self.exchange_date(),
                observed_utc_ns=self._utc_ns,
                calendar_sha256=self._calendar_sha256 if calendar_verified else None,
                calendar_verified=calendar_verified,
            )
        _state, ctx = classify_broker_session(
            observed_utc_ns=self._utc_ns,
            calendar=self._calendar,
            execution_dates=tuple(execution_dates),
            broker_session_date=broker_session_date,
            books_complete=books_complete,
            settlement_verified=settlement_verified,
            signed_delivery_verified=signed_delivery_verified,
            product=product,
        )
        return ctx

    def snapshot(self) -> ClockSnapshot:
        return ClockSnapshot(
            utc_ns=self._utc_ns,
            mono_ns=self._mono_ns,
            boot_id=self._boot_id,
            ist_iso=self.ist_datetime().isoformat(),
            exchange_date=self.exchange_date().isoformat(),
        )
