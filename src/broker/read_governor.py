"""Durable, account-scoped admission control for Dhan GETs.

Coordinates processes using the same state directory; it cannot observe other
VMs, manual clients, or broker-side counters. Never caches successful evidence
and never sleeps in a watcher. Local denial is explicitly not a broker response.
"""
from contextlib import closing
from datetime import timezone
from email.utils import parsedate_to_datetime
import hashlib
import math
from pathlib import Path
import sqlite3
import time


class ReadDeferred(RuntimeError):
    def __init__(self, seconds, reason):
        self.seconds = max(1, math.ceil(seconds))
        super().__init__(f'Dhan GET deferred locally for {self.seconds}s: {reason}')


class ReadGovernor:
    # Conservative local GET budgets, not a claim about Dhan endpoint categories.
    LIMITS = ((60, 200), (3600, 800), (86400, 5500))

    def __init__(self, directory, account_id, clock=time.time):
        self.directory = Path(directory)
        self.account_id = str(account_id if account_id is not None else '').strip()
        key = hashlib.sha256(self.account_id.encode('utf-8')).hexdigest()[:24]
        self.path = self.directory / f'dhan-reads-{key}.sqlite3'
        self.clock = clock

    def _connect(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        conn = sqlite3.connect(self.path, timeout=0.05)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=FULL')
            conn.execute('CREATE TABLE IF NOT EXISTS reads (at REAL NOT NULL)')
            conn.execute('CREATE INDEX IF NOT EXISTS reads_at ON reads(at)')
            conn.execute('CREATE TABLE IF NOT EXISTS cooldown (id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
            conn.commit()
            return conn
        except Exception:
            conn.close()
            raise

    def _run_with_busy_retry(self, fn):
        # At most two 50ms lock waits; never stall supervision for seconds.
        for attempt in range(2):
            try:
                with closing(self._connect()) as conn, conn:
                    return fn(conn)
            except sqlite3.OperationalError as exc:
                if ('locked' in str(exc).lower() or 'busy' in str(exc).lower()) and attempt == 0:
                    time.sleep(0.01)
                    continue
                raise

    def admit(self, *, historical=False, priority='normal'):
        now = float(self.clock())

        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until FROM cooldown WHERE id=1').fetchone()
            if row and row[0] is not None and float(row[0]) > now:
                raise ReadDeferred(float(row[0]) - now, 'shared broker rate-limit cooldown')
            conn.execute('DELETE FROM reads WHERE at <= ?', (now - 86400,))
            prio = str(priority or 'normal').strip().lower()
            for window, limit in self.LIMITS:
                # Keep a reserve for supervision/identity/funds instead of history,
                # and keep a top 10% reserve against low-priority background polling.
                if historical and limit > 1:
                    ceiling = max(1, int(limit * 0.8))
                elif prio == 'low' and limit > 1:
                    ceiling = max(1, int(limit * 0.9))
                else:
                    ceiling = limit
                count, oldest = conn.execute(
                    'SELECT COUNT(*), MIN(at) FROM reads WHERE at > ?',
                    (now - window,),
                ).fetchone()
                if count >= ceiling:
                    wait_sec = (float(oldest) + window - now) if oldest is not None else float(window)
                    raise ReadDeferred(wait_sec, 'local GET budget exhausted')
            conn.execute('INSERT INTO reads(at) VALUES (?)', (now,))

        return self._run_with_busy_retry(_op)

    def throttled(self, retry_after=None):
        now = float(self.clock())
        delay = None
        if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
            delay = float(retry_after)
        elif isinstance(retry_after, str):
            clean_ra = retry_after.strip()
            try:
                delay = float(clean_ra)
            except ValueError:
                try:
                    dt = parsedate_to_datetime(clean_ra)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    delay = dt.timestamp() - now
                except (ValueError, TypeError, OverflowError):
                    pass
        if delay is not None and (not math.isfinite(delay) or delay <= 0):
            delay = None

        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until, strikes FROM cooldown WHERE id=1').fetchone()
            # No header: increase quiet period for repeat throttle responses.
            prev_strikes = int(row[1]) if (row and row[1] is not None) else 0
            prev_until = float(row[0]) if (row and row[0] is not None) else 0.0
            strikes = min(prev_strikes + 1, 7)
            eff_delay = max(60 * 2 ** (strikes - 1), delay or 0)
            until = max(prev_until, now + eff_delay)
            conn.execute('INSERT OR REPLACE INTO cooldown VALUES (1, ?, ?)', (until, strikes))
            return until - now

        return self._run_with_busy_retry(_op)

    def record_success(self):
        """Resets accumulated strike backoff once cooldown has expired and a live GET succeeds."""
        now = float(self.clock())

        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until, strikes FROM cooldown WHERE id=1').fetchone()
            if row and row[1] and (row[0] is None or float(row[0]) <= now):
                conn.execute('UPDATE cooldown SET strikes=0 WHERE id=1')

        try:
            self._run_with_busy_retry(_op)
        except (OSError, sqlite3.Error):
            pass
