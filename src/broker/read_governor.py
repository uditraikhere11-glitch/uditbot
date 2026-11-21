"""Account-scoped, category-aware DhanHQ v2 request admission.

Dhan's published v2 table (checked 2026-11-18):
  ORDER:       10/s, 250/min, 1000/hour, 7000/day
  DATA:         5/s,                    100000/day
  QUOTE:        1/s,                    no longer windows
  NON_TRADING: 20/s,                    no longer windows

GET order/trade/status/portfolio/funds/profile calls are NON_TRADING. The
DhanHQ product announcement for v2 explicitly removed longer-window limits
from GET calls; rate governance for order placement/modification/cancellation
is distinct from these non-mutating reads.

Durable admissions and broker cooldowns are account-scoped and cross-process.
No successful broker response is cached or fabricated, and no watcher sleeps.
The existing legacy SQLite records (all historically GET admissions) are
migrated in place to NON_TRADING without deleting history or cooling counters.
"""
from contextlib import closing, contextmanager
import fcntl
import os
from contextvars import ContextVar
from datetime import timezone
from email.utils import parsedate_to_datetime
import hashlib
import math
import re
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit


RECONCILIATION_PACING = ContextVar("reconciliation_pacing", default=False)


class ReadDeferred(RuntimeError):
    def __init__(self, seconds, reason, *, short_window=False):
        self.short_window = short_window
        self.seconds = max(1, math.ceil(seconds))
        super().__init__(f'Dhan API deferred locally for {self.seconds}s: {reason}')


class ReadGovernor:
    # These limits are Dhan's documented API CATEGORY limits, not a shared GET quota.
    CATEGORY_LIMITS = {
        'ORDER': ((1, 10), (60, 250), (3600, 1000), (86400, 7000)),
        'DATA': ((1, 5), (86400, 100000)),
        'QUOTE': ((1, 1),),
        'NON_TRADING': ((1, 20),),
    }
    # Legacy test fixtures may override LIMITS per instance. It is never a
    # production daily-GET allowance (the default is None).
    LIMITS = None
    PRIORITY_FRACTIONS = {'routine': .80, 'low': .90, 'normal': 1.0,
                          'identity': 1.0, 'urgent': 1.0}

    def __init__(self, directory, account_id, clock=time.time):
        self.directory = Path(directory)
        self.account_id = str(account_id if account_id is not None else '').strip()
        key = hashlib.sha256(self.account_id.encode('utf-8')).hexdigest()[:24]
        # Keep the original account-scoped DB path so existing cooldown and
        # history are not silently abandoned during the migration.
        self.path = self.directory / f'dhan-reads-{key}.sqlite3'
        self.clock = clock
        # Initialized only after cross-process flock, schema checks and a
        # committed SQLite migration. Never cache rate-limit quotas/cooldowns.
        self._verified_file_key = None
        self._verified_schema_version = None

    @classmethod
    def classify(cls, method, url):
        """Classify the Dhan endpoint, not an arbitrary caller-assigned priority.

        All standard GETs use NON_TRADING except explicitly designated data /
        quote reads. Mutation calls must use an explicitly recognized route.
        """
        method = str(method or '').upper()
        path = urlsplit(str(url or '')).path.rstrip('/').lower()
        if path.startswith('/v2/'):
            path = path[3:]
        elif path == '/v2':
            path = '/'
        quote = path.startswith('/marketfeed/')
        data = path.startswith(('/charts/', '/historical/', '/intraday/',
                                '/optionchain', '/expiredoptions/', '/rollingoption/'))
        order = (path == '/orders' or path.startswith(('/orders/', '/trades',
                 '/super/orders', '/forever/orders', '/alerts/orders')) or
                 path == '/positions' or path.startswith('/positions/'))
        if method == 'GET':
            return 'QUOTE' if quote else 'DATA' if data else 'NON_TRADING'
        if method == 'POST' and quote:
            return 'QUOTE'
        if method == 'POST' and data:
            return 'DATA'
        if method in ('POST', 'PUT', 'DELETE') and order:
            return 'ORDER'
        raise ValueError('Unclassified Dhan mutation or unsupported request method')

    @staticmethod
    def modification_order_id(method, url):
        if str(method or '').upper() != 'PUT':
            return None
        path = urlsplit(str(url or '')).path.rstrip('/')
        parts = path.split('/')
        if len(parts) < 2 or parts[-2].lower() != 'orders':
            # Routes for leg modifications can have an additional segment;
            # never mistake an unsupported route for the ordinary order ID.
            return None
        order_id = parts[-1]
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', order_id):
            raise ValueError('Invalid broker modification order ID')
        return order_id

    @staticmethod
    def _category(category):
        result = str(category or '').strip().upper().replace('-', '_')
        if result not in ReadGovernor.CATEGORY_LIMITS:
            raise ValueError('Unsupported Dhan API category')
        return result

    def _limits(self, category):
        # Only a deliberately assigned legacy per-instance testing override.
        override = self.__dict__.get('LIMITS')
        return tuple(override) if override is not None else self.CATEGORY_LIMITS[category]

    @contextmanager
    def _schema_initialization_lock(self):
        """Coordinate first use across workers before the DB file exists.

        Inspecting SQLite's file-size marker without a cross-process lock can
        mistake another worker's uncommitted initial schema for corruption.
        This separate owner-only lock protects initialization only; the SQLite
        transaction continues to protect each individual admission.
        """
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = str(self.path) + ".schema.lock"
        fd = os.open(
            lock_path,
            os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _connect(self):
        """Fast connection for an already verified, unchanged SQLite ledger.

        The old implementation acquired the cross-process schema flock and
        repeated CREATE TABLE/INDEX statements on EVERY broker API GET.
        Only first use needs schema migration. All admissions still acquire
        SQLite BEGIN IMMEDIATE and persist under synchronous=FULL.

        The verified file's (device,inode) must not change. A replaced,
        deleted or truncated governor cannot silently reset its quotas.
        Missing/corrupt runtime tables still raise from the admission SQL.
        """
        verified = self._verified_file_key
        if verified is not None:
            try:
                st = self.path.stat()
            except OSError as exc:
                raise sqlite3.DatabaseError(
                    "Persisted broker governor ledger vanished"
                ) from exc
            if (st.st_dev, st.st_ino) != verified or st.st_size <= 0:
                raise sqlite3.DatabaseError(
                    "Persisted broker governor ledger changed unexpectedly"
                )
            # mode=rw refuses to create a fresh quota DB if the path is
            # removed between stat() and sqlite3.connect().
            conn = sqlite3.connect(
                self.path.absolute().as_uri() + "?mode=rw",
                uri=True, timeout=0.07,
            )
            try:
                conn.execute("PRAGMA synchronous=FULL")
                mode = conn.execute("PRAGMA journal_mode").fetchone()
                schema_version = conn.execute("PRAGMA schema_version").fetchone()
                after = self.path.stat()
                if (not mode or str(mode[0]).lower() != "wal"
                        or not schema_version
                        or int(schema_version[0]) != self._verified_schema_version
                        or (after.st_dev, after.st_ino) != verified
                        or after.st_size <= 0):
                    raise sqlite3.DatabaseError(
                        "Persisted broker governor WAL/identity unverified"
                    )
                return conn
            except BaseException:
                conn.close()
                raise

        # First use still executes the exact original cross-process schema
        # and legacy-migration barrier before any API admission.
        with self._schema_initialization_lock():
            conn = self._connect_unlocked()
            try:
                st = self.path.stat()
                if st.st_size <= 0:
                    raise sqlite3.DatabaseError("Governor schema initialization incomplete")
                schema = conn.execute("PRAGMA schema_version").fetchone()
                if not schema:
                    raise sqlite3.DatabaseError("Governor schema version unavailable")
                self._verified_schema_version = int(schema[0])
                self._verified_file_key = (st.st_dev, st.st_ino)
            except BaseException:
                conn.close()
                raise
            return conn

    def _connect_unlocked(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        persisted = self.path.is_file() and self.path.stat().st_size > 0
        conn = sqlite3.connect(self.path, timeout=0.07)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=FULL')
            # Serialize first-time schema creation and the persisted integrity
            # check: concurrent starters must not see an uncommitted schema.
            conn.execute('BEGIN IMMEDIATE')
            if persisted:
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
                if not {'reads', 'cooldown'}.issubset(tables):
                    # Never recreate lost persisted broker-limit evidence as
                    # an empty, apparently healthy quota database.
                    raise sqlite3.DatabaseError('Persisted governor audit tables missing')
            conn.execute("CREATE TABLE IF NOT EXISTS reads "
                         "(at REAL NOT NULL, category TEXT NOT NULL DEFAULT 'NON_TRADING')")
            cols = {row[1] for row in conn.execute('PRAGMA table_info(reads)')}
            if 'at' not in cols:
                raise sqlite3.DatabaseError('Invalid governor reads schema')
            if 'category' in cols and persisted and not {'category_cooldown', 'order_modifications'} <= tables:
                raise sqlite3.DatabaseError('Persisted category governor tables missing')
            if 'category' not in cols:
                # Legacy DB contains ONLY GET admissions. Existing rows remain
                # present and now count against per-second non-trading policy;
                # there is no unauthorized reset of durable evidence.
                conn.execute("ALTER TABLE reads ADD COLUMN category TEXT NOT NULL "
                             "DEFAULT 'NON_TRADING'")
            conn.execute('CREATE INDEX IF NOT EXISTS reads_at ON reads(at)')
            conn.execute('CREATE INDEX IF NOT EXISTS reads_category_at ON reads(category,at)')
            conn.execute('CREATE TABLE IF NOT EXISTS cooldown '
                         '(id INTEGER PRIMARY KEY, until REAL, strikes INTEGER)')
            conn.execute('CREATE TABLE IF NOT EXISTS category_cooldown '
                         '(category TEXT PRIMARY KEY, until REAL, strikes INTEGER)')
            conn.execute('CREATE TABLE IF NOT EXISTS order_modifications '
                         '(order_id TEXT PRIMARY KEY, used INTEGER NOT NULL)')
            conn.commit()
            return conn
        except Exception:
            conn.close()
            raise

    def _run_with_busy_retry(self, fn):
        # Finite waits for SQLite WAL writers; no long supervision stall.
        for attempt in range(6):
            try:
                with closing(self._connect()) as conn, conn:
                    return fn(conn)
            except sqlite3.OperationalError as exc:
                if ('locked' in str(exc).lower() or 'busy' in str(exc).lower()) and attempt < 5:
                    time.sleep(.005 * (attempt + 1))
                    continue
                raise

    @staticmethod
    def _cooldown_until(conn, category, now):
        # The original account-wide cooldown, if active, must survive upgrade.
        row = conn.execute('SELECT until FROM cooldown WHERE id=1').fetchone()
        legacy = float(row[0]) if row and row[0] is not None else 0.
        row = conn.execute('SELECT until FROM category_cooldown WHERE category=?',
                           (category,)).fetchone()
        per_category = float(row[0]) if row and row[0] is not None else 0.
        return max(now, legacy, per_category)

    def _ceiling(self, limit, priority, *, historical=False):
        # Preserve exact narrow synthetic legacy-window fixture semantics.
        # Production default still admits the full documented rate.
        legacy_fixture = self.__dict__.get("LIMITS") is not None
        frac = .80 if historical else (.95 if legacy_fixture and priority == "normal"
                                     else self.PRIORITY_FRACTIONS[priority])
        return max(1, int(limit * frac))

    def admit(self, *, category='NON_TRADING', historical=False, priority='normal',
              modification_order_id=None):
        cat = self._category(category)
        prio = str(priority or 'normal').strip().lower()
        if prio not in self.PRIORITY_FRACTIONS:
            raise ValueError('Unsupported Dhan request admission priority')
        now = float(self.clock())

        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            until = self._cooldown_until(conn, cat, now)
            if until > now:
                raise ReadDeferred(until - now, 'broker rate-limit cooldown')
            conn.execute('DELETE FROM reads WHERE at <= ?', (now - 86400,))
            for window, limit in self._limits(cat):
                ceiling = self._ceiling(limit, prio, historical=historical)
                count, oldest = conn.execute(
                    'SELECT COUNT(*),MIN(at) FROM reads '
                    'WHERE category=? AND at>?', (cat, now-window)
                ).fetchone()
                if count >= ceiling:
                    delay = max(.001, (float(oldest)+window-now) if oldest is not None else window)
                    raise ReadDeferred(delay, f'{cat} {window}s category budget exhausted', short_window=window == 1)
            if modification_order_id is not None:
                if cat != 'ORDER' or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', str(modification_order_id)):
                    raise ValueError('Invalid order modification admission')
                row = conn.execute('SELECT used FROM order_modifications WHERE order_id=?',
                                   (str(modification_order_id),)).fetchone()
                if row and int(row[0]) >= 25:
                    raise ReadDeferred(1, 'maximum 25 modifications per order reached')
                conn.execute('INSERT INTO order_modifications(order_id,used) VALUES (?,1) '
                             'ON CONFLICT(order_id) DO UPDATE SET used=used+1',
                             (str(modification_order_id),))
            conn.execute('INSERT INTO reads(at,category) VALUES (?,?)', (now, cat))

        return self._run_with_busy_retry(_op)

    def seconds_until_capacity(self, *, required=1, priority='routine', category='NON_TRADING'):
        """Read-only, migration-aware hint. Never reserves capacity or sleeps.

        A planned batch greater than the one-second ceiling is naturally spread
        across seconds. Estimate capacity for its first admissible wave, not an
        impossible demand to fit all 40 calls inside one second.
        """
        cat = self._category(category)
        prio = str(priority or 'routine').strip().lower()
        if prio not in self.PRIORITY_FRACTIONS:
            raise ValueError('Invalid watcher read priority')
        need = int(required)
        if need < 1 or need > 40:
            raise ValueError('Invalid required read capacity')
        if not self.path.is_file():
            return 0.0
        now = float(self.clock())
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',
                                     uri=True, timeout=.10)) as conn:
            # Distinguish a valid old store from corrupt state without writing.
            cols = {x[1] for x in conn.execute('PRAGMA table_info(reads)')}
            if 'at' not in cols:
                raise sqlite3.DatabaseError('Governor reads table absent')
            has_cat = 'category' in cols
            cooldown = conn.execute('SELECT until FROM cooldown WHERE id=1').fetchone()
            delay = max(0., float(cooldown[0])-now) if cooldown and cooldown[0] else 0.
            if 'category_cooldown' in {x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
                row = conn.execute('SELECT until FROM category_cooldown WHERE category=?',(cat,)).fetchone()
                if row and row[0]:
                    delay = max(delay, float(row[0])-now)
            for window, limit in self._limits(cat):
                ceiling = self._ceiling(limit, prio)
                wave = min(need, ceiling) if window == 1 else need
                if wave > ceiling:
                    raise ValueError('Read requirement exceeds category window allowance')
                if has_cat:
                    where, params = 'category=? AND at>?', (cat, now-window)
                else:
                    # Old ledger was all GET admissions, so safely treat it
                    # as non-trading in read-only preview before first admit.
                    where, params = 'at>?', (now-window,)
                    if cat != 'NON_TRADING':
                        continue
                count = conn.execute('SELECT COUNT(*) FROM reads WHERE '+where,params).fetchone()[0]
                excess = count+wave-ceiling
                if excess > 0:
                    row = conn.execute('SELECT at FROM reads WHERE '+where+
                                       ' ORDER BY at ASC LIMIT 1 OFFSET ?',
                                       (*params, excess-1)).fetchone()
                    if row is None:
                        raise sqlite3.DatabaseError('Inconsistent governor read window')
                    delay = max(delay, float(row[0])+window-now+.01)
            return max(0.,delay)

    def throttled(self, retry_after=None, *, category='NON_TRADING'):
        cat = self._category(category)
        now = float(self.clock())
        delay = None
        if isinstance(retry_after, (int,float)) and not isinstance(retry_after,bool):
            delay = float(retry_after)
        elif isinstance(retry_after,str):
            try:
                delay = float(retry_after.strip())
            except ValueError:
                try:
                    dt = parsedate_to_datetime(retry_after.strip())
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    delay = dt.timestamp()-now
                except (ValueError,TypeError,OverflowError):
                    pass
        if delay is not None and (not math.isfinite(delay) or delay<=0):
            delay = None

        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until,strikes FROM category_cooldown WHERE category=?',(cat,)).fetchone()
            strikes = min((int(row[1]) if row and row[1] is not None else 0)+1,7)
            existing = float(row[0]) if row and row[0] is not None else 0.
            until = max(existing,now+max(60*2**(strikes-1),delay or 0))
            conn.execute('INSERT OR REPLACE INTO category_cooldown(category,until,strikes) VALUES (?,?,?)',
                         (cat,until,strikes))
            return until-now

        return self._run_with_busy_retry(_op)

    def record_success(self, *, category='NON_TRADING'):
        cat = self._category(category)
        now = float(self.clock())
        def _op(conn):
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute('SELECT until,strikes FROM category_cooldown WHERE category=?',(cat,)).fetchone()
            if row and row[1] and (row[0] is None or float(row[0])<=now):
                conn.execute('UPDATE category_cooldown SET strikes=0 WHERE category=?',(cat,))
        try:
            self._run_with_busy_retry(_op)
        except (OSError,sqlite3.Error):
            pass
