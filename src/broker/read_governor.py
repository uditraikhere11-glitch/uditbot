"""Durable, account-scoped admission control for Dhan GETs.

Coordinates processes using the same state directory; it cannot observe other
VMs, manual clients, or broker-side counters. Never caches successful evidence
and never sleeps in a watcher. Local denial is explicitly not a broker response.
"""
import hashlib
import math
import sqlite3
import time
from contextlib import closing
from email.utils import parsedate_to_datetime
from pathlib import Path


class ReadDeferred(RuntimeError):
    def __init__(self, seconds, reason):
        self.seconds = max(1, math.ceil(seconds))
        super().__init__(f'Dhan GET deferred locally for {self.seconds}s: {reason}')


class ReadGovernor:
    # Conservative local GET budgets, not a claim about Dhan endpoint categories.
    LIMITS = ((60, 200), (3600, 800), (86400, 5500))

    def __init__(self, directory, account_id, clock=time.time):
        self.directory = Path(directory)
        key = hashlib.sha256(str(account_id).encode()).hexdigest()[:24]
        self.path = self.directory / f'dhan-reads-{key}.sqlite3'
        self.clock = clock

    def _connect(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        conn = sqlite3.connect(self.path, timeout=0.2)
        conn.execute('CREATE TABLE IF NOT EXISTS reads (at REAL NOT NULL)')
        conn.execute('CREATE INDEX IF NOT EXISTS reads_at ON reads(at)')
        conn.execute('CREATE TABLE IF NOT EXISTS cooldown (id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
        conn.commit()
        return conn

    def admit(self, *, historical=False):
        now = self.clock()
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until FROM cooldown WHERE id=1').fetchone()
            if row and row[0] > now:
                raise ReadDeferred(row[0] - now, 'shared broker rate-limit cooldown')
            conn.execute('DELETE FROM reads WHERE at <= ?', (now - 86400,))
            for window, limit in self.LIMITS:
                # Keep a reserve for supervision/identity/funds instead of history.
                ceiling = max(1, int(limit * .8)) if historical and window > 1 else limit
                count, oldest = conn.execute('SELECT COUNT(*), MIN(at) FROM reads WHERE at > ?', (now - window,)).fetchone()
                if count >= ceiling:
                    raise ReadDeferred(oldest + window - now, 'local GET budget exhausted')
            conn.execute('INSERT INTO reads(at) VALUES (?)', (now,))

    def throttled(self, retry_after=None):
        now = self.clock()
        delay = None
        if isinstance(retry_after, str):
            try:
                delay = float(retry_after)
            except ValueError:
                try:
                    delay = parsedate_to_datetime(retry_after).timestamp() - now
                except (ValueError, TypeError, OverflowError):
                    pass
        if delay is not None and (not math.isfinite(delay) or delay <= 0):
            delay = None
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until, strikes FROM cooldown WHERE id=1').fetchone()
            # No header: increase quiet period for repeat throttle responses.
            strikes = min((row[1] if row else 0) + 1, 7)
            delay = max(60 * 2 ** (strikes - 1), delay or 0)
            until = max(row[0] if row else 0, now + delay)
            conn.execute('INSERT OR REPLACE INTO cooldown VALUES (1, ?, ?)', (until, strikes))
            return until - now
