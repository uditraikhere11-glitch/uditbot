"""
Durable SQLite Audit Journal and State Persistence.
Maintains tamper-evident audit history of all signals, validations, orders, and fills.
Implements STEP 08, STEP 14, STEP 16, STEP 24 + V13 Safety Corrections (including CV13-01..05):
- WAL mode, synchronous NORMAL/FULL, foreign keys, and busy_timeout.
- Atomic unique signal claims (`INSERT ... ON CONFLICT`) safe under high multi-thread/multi-process concurrency (`JOURNAL-01`).
- Complete route, option contract, and cancellation/timeout metadata persisted in `order_intents` (`is_super_order`,
  `original_market_price`, `tick_size`, `lot_size`, `option_type`, `strike`, `expiry`, `trading_symbol`,
  `cancel_origin`, `cancel_reason`, `retry_eligible`, `cancel_attempts`, `cancel_acknowledged`, `cancel_uncertain`,
  `entry_timeout_elapsed`) (`V12-02`, `CV13-02`, `CV13-03`).
- Durable atomic retry slot reservation table (`retry_reservations`) keyed by `root_order_id` and `parent_order_id` (`V12-11`).
- Durable protection-attempt lifecycle (`protection_attempts` table + partial unique index on active uncovered slices)
  committed strictly before any protective-order POST or Super Order modification (`Safety Correction A`, `CV13-01`).
- Canonical non-terminal state query including raw broker states (`PENDING`, `PART_TRADED`, `TRANSIT`, `TRIGGERED`) (`V12-19`),
  while keeping pre-submit failed retry reservations (`NOT_SUBMITTED`) distinct from `UNKNOWN` (`CV13-04`).
- Automatic resolution/superseding of `UNPROTECTED` incidents once protection succeeds (`V12-19`).
- Live portfolio risk aggregation (`get_live_portfolio_risk_state`) for `RiskManager` (`V12-08`).
"""

from contextlib import contextmanager
from datetime import date
import datetime
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
import zoneinfo
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")
_CORR_SEQ_LOCK = threading.Lock()
_CORR_SEQ = 0

NON_TERMINAL_INTENT_STATES = (
    "INTENT_CREATED",
    "SUBMITTING",
    "UNKNOWN",
    "ACKED",
    "PENDING",
    "TRANSIT",
    "TRIGGERED",
    "PART_FILLED",
    "PART_TRADED",
    "CANCEL_REQUESTED",
    "CANCEL_PENDING",
)

UNRESOLVED_PROTECTION_ATTEMPT_STATES = (
    "SUBMITTING",
    "UNKNOWN",
    "ACKED_UNVERIFIED",
)


def generate_correlation_id(prefix: str = "ORD", symbol: str = "", now: Optional[float] = None) -> str:
    """
    08-C / H-07 / V12-12: Generates a collision-free, broker-compliant correlation ID strictly <= 30 characters.
    Used identically across regular orders, Super Orders, Forever OCO orders, and SQLite intents.
    """
    global _CORR_SEQ
    with _CORR_SEQ_LOCK:
        _CORR_SEQ = (_CORR_SEQ + 1) % 10000
        seq = _CORR_SEQ
    ts = int((now if now is not None else time.time()) * 1000) % 10000000
    clean_prefix = "".join(c for c in (prefix or "ORD").upper() if c.isalnum())[:4] or "ORD"
    clean_sym = "".join(c for c in (symbol or "SYM").upper() if c.isalnum())[:8] or "SYM"
    rand_hex = uuid.uuid4().hex[:5]
    corr = f"{clean_prefix}-{clean_sym}-{ts}-{seq:04d}{rand_hex}"
    return corr[:30]


class JournalDB:
    def __init__(self, db_path: str, account_id: str = ""):
        self.db_path = db_path
        self.account_id = str(account_id or os.environ.get("DHAN_CLIENT_ID", "")).strip()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 10000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER,
                message_id INTEGER,
                signal_type TEXT,
                symbol TEXT,
                action TEXT,
                raw_text TEXT,
                received_at REAL,
                status TEXT,
                reason TEXT
            );

            CREATE TABLE IF NOT EXISTS signal_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT DEFAULT '',
                strategy_id TEXT DEFAULT 'DEFAULT',
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                symbol TEXT,
                first_seen_at REAL NOT NULL,
                raw_text TEXT NOT NULL,
                status TEXT DEFAULT 'CLAIMED',
                UNIQUE(channel_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS signal_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                claim_id INTEGER NOT NULL,
                revision_num INTEGER NOT NULL,
                old_text TEXT,
                new_text TEXT NOT NULL,
                revised_at REAL NOT NULL,
                FOREIGN KEY (claim_id) REFERENCES signal_claims(id)
            );

            CREATE TABLE IF NOT EXISTS order_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                correlation_id TEXT UNIQUE NOT NULL,
                parent_correlation_id TEXT,
                account_id TEXT DEFAULT '',
                symbol TEXT NOT NULL,
                trading_symbol TEXT DEFAULT '',
                security_id TEXT NOT NULL,
                exchange_segment TEXT NOT NULL,
                product_type TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                is_super_order INTEGER DEFAULT 0,
                original_market_price REAL DEFAULT 0.0,
                tick_size REAL DEFAULT 0.05,
                lot_size INTEGER DEFAULT 1,
                option_type TEXT DEFAULT '',
                strike REAL DEFAULT 0.0,
                expiry TEXT DEFAULT '',
                cancel_origin TEXT DEFAULT '',
                cancel_reason TEXT DEFAULT '',
                retry_eligible INTEGER DEFAULT 0,
                cancel_attempts INTEGER DEFAULT 0,
                cancel_acknowledged INTEGER DEFAULT 0,
                cancel_uncertain INTEGER DEFAULT 0,
                entry_timeout_elapsed INTEGER DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'INTENT_CREATED',
                broker_order_id TEXT,
                error_msg TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS retry_reservations (
                root_order_id TEXT PRIMARY KEY,
                parent_order_id TEXT UNIQUE NOT NULL,
                child_correlation_id TEXT NOT NULL,
                reserved_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                symbol TEXT,
                limit_price REAL,
                units INTEGER,
                total_quantity INTEGER,
                total_cost REAL,
                is_passed INTEGER,
                rejection_reason TEXT,
                created_at REAL,
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                correlation_id TEXT UNIQUE,
                broker_order_id TEXT,
                trading_mode TEXT,
                product_type TEXT,
                exchange_segment TEXT,
                symbol TEXT,
                security_id TEXT,
                order_type TEXT,
                price REAL,
                quantity INTEGER,
                status TEXT,
                created_at REAL,
                updated_at REAL
            );

            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker_order_id TEXT,
                exchange_trade_id TEXT UNIQUE,
                symbol TEXT,
                traded_price REAL,
                traded_quantity INTEGER,
                traded_at REAL
            );

            CREATE TABLE IF NOT EXISTS protection_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_order_id TEXT,
                forever_order_id TEXT UNIQUE,
                symbol TEXT,
                sl_trigger REAL,
                sl_limit REAL,
                target_price REAL,
                quantity INTEGER,
                status TEXT,
                created_at REAL
            );

            CREATE TABLE IF NOT EXISTS protection_attempts (
                attempt_id TEXT PRIMARY KEY,
                parent_order_id TEXT NOT NULL,
                root_order_id TEXT NOT NULL,
                account_id TEXT DEFAULT '',
                symbol TEXT NOT NULL,
                trading_symbol TEXT DEFAULT '',
                security_id TEXT NOT NULL,
                exchange_segment TEXT NOT NULL,
                product_type TEXT NOT NULL,
                route TEXT NOT NULL,
                uncovered_from_qty INTEGER NOT NULL DEFAULT 0,
                quantity INTEGER NOT NULL,
                executed_price REAL NOT NULL,
                sl_trigger REAL NOT NULL,
                sl_limit REAL NOT NULL,
                target_price REAL NOT NULL,
                tick_size REAL NOT NULL DEFAULT 0.05,
                correlation_id TEXT UNIQUE NOT NULL,
                state TEXT NOT NULL DEFAULT 'SUBMITTING',
                broker_order_id TEXT,
                response_json TEXT,
                error_msg TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_protection_attempt
            ON protection_attempts(parent_order_id, uncovered_from_qty)
            WHERE state IN ('SUBMITTING', 'UNKNOWN', 'ACKED_UNVERIFIED', 'CONFIRMED');

            CREATE TABLE IF NOT EXISTS watcher_orders (
                order_id TEXT PRIMARY KEY,
                is_terminal INTEGER DEFAULT 0,
                record_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT DEFAULT 'PENDING',
                retry_count INTEGER DEFAULT 0,
                next_retry_at REAL DEFAULT 0.0,
                last_error TEXT,
                created_at REAL NOT NULL,
                processed_at REAL
            );

            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                description TEXT,
                payload TEXT,
                created_at REAL
            );

            CREATE TABLE IF NOT EXISTS signal_latency_telemetry (
                trace_id TEXT PRIMARY KEY,
                correlation_id TEXT,
                broker_order_id TEXT,
                symbol TEXT,
                channel_id INTEGER DEFAULT 0,
                message_id INTEGER DEFAULT 0,
                ist_received_time REAL,
                telegram_msg_timestamp REAL,
                cmp_policy TEXT,
                price_source TEXT,
                yahoo_cmp REAL,
                yahoo_http_ms REAL,
                yahoo_parse_ms REAL,
                cmp_auth_ms REAL,
                quote_timestamp REAL,
                reported_quote_age_sec REAL,
                quote_age_status TEXT,
                freshness_verified INTEGER DEFAULT 0,
                quote_freshness_status TEXT,
                l2_parse_ms REAL,
                l3_resolve_ms REAL,
                l7_sizing_ms REAL,
                l8_risk_preflight_ms REAL,
                telegram_to_cmp_ms REAL,
                cmp_to_submission_ms REAL,
                telegram_to_submission_ms REAL,
                dhan_request_to_ack_ms REAL,
                telegram_to_ack_ms REAL,
                ack_wall_time REAL,
                fill_wall_time REAL,
                protection_wall_time REAL,
                ack_to_fill_ms REAL,
                fill_to_protection_ms REAL,
                total_processing_ms REAL,
                updated_at REAL
            );

            CREATE INDEX IF NOT EXISTS idx_lat_corr_id ON signal_latency_telemetry(correlation_id);
            CREATE INDEX IF NOT EXISTS idx_lat_broker_oid ON signal_latency_telemetry(broker_order_id);

            CREATE TABLE IF NOT EXISTS yahoo_request_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT DEFAULT '',
                success INTEGER NOT NULL,
                http_latency_ms REAL,
                quote_age_sec REAL,
                quote_age_unknown INTEGER DEFAULT 0,
                recorded_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS latency_rejection_counters (
                reason TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            );
            """)
            # Idempotent additive schema migrations for existing databases
            for alter_sql in [
                "ALTER TABLE signal_claims ADD COLUMN account_id TEXT DEFAULT ''",
                "ALTER TABLE signal_claims ADD COLUMN strategy_id TEXT DEFAULT 'DEFAULT'",
                "ALTER TABLE order_intents ADD COLUMN parent_correlation_id TEXT",
                "ALTER TABLE order_intents ADD COLUMN account_id TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN trading_symbol TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN is_super_order INTEGER DEFAULT 0",
                "ALTER TABLE order_intents ADD COLUMN original_market_price REAL DEFAULT 0.0",
                "ALTER TABLE order_intents ADD COLUMN tick_size REAL DEFAULT 0.05",
                "ALTER TABLE order_intents ADD COLUMN lot_size INTEGER DEFAULT 1",
                "ALTER TABLE order_intents ADD COLUMN option_type TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN strike REAL DEFAULT 0.0",
                "ALTER TABLE order_intents ADD COLUMN expiry TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN cancel_origin TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN cancel_reason TEXT DEFAULT ''",
                "ALTER TABLE order_intents ADD COLUMN retry_eligible INTEGER DEFAULT 0",
                "ALTER TABLE order_intents ADD COLUMN cancel_attempts INTEGER DEFAULT 0",
                "ALTER TABLE order_intents ADD COLUMN cancel_acknowledged INTEGER DEFAULT 0",
                "ALTER TABLE order_intents ADD COLUMN cancel_uncertain INTEGER DEFAULT 0",
                "ALTER TABLE order_intents ADD COLUMN entry_timeout_elapsed INTEGER DEFAULT 0",
                "ALTER TABLE outbox ADD COLUMN retry_count INTEGER DEFAULT 0",
                "ALTER TABLE outbox ADD COLUMN next_retry_at REAL DEFAULT 0.0",
                "ALTER TABLE outbox ADD COLUMN last_error TEXT",
            ]:
                try:
                    conn.execute(alter_sql)
                except sqlite3.OperationalError:
                    pass

            # Migration: heal legacy fills table with broken foreign key referencing non-unique orders column
            try:
                fks = conn.execute("PRAGMA foreign_key_list(fills)").fetchall()
                if any(row[2] == "orders" for row in fks):
                    conn.execute("PRAGMA foreign_keys = OFF;")
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS fills_v13_clean (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            broker_order_id TEXT,
                            exchange_trade_id TEXT UNIQUE,
                            symbol TEXT,
                            traded_price REAL,
                            traded_quantity INTEGER,
                            traded_at REAL
                        );
                    """)
                    conn.execute("INSERT OR IGNORE INTO fills_v13_clean SELECT id, broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at FROM fills;")
                    conn.execute("DROP TABLE fills;")
                    conn.execute("ALTER TABLE fills_v13_clean RENAME TO fills;")
                    conn.execute("PRAGMA foreign_keys = ON;")
            except Exception:
                pass

    # --- Durable Protection-Attempt Lifecycle (Safety Correction A & CV13-01) ---

    def create_protection_attempt(
        self,
        attempt_id: str,
        parent_order_id: str,
        root_order_id: str,
        account_id: str,
        symbol: str,
        security_id: str,
        exchange_segment: str,
        product_type: str,
        route: str,
        uncovered_from_qty: int,
        quantity: int,
        executed_price: float,
        sl_trigger: float,
        sl_limit: float,
        target_price: float,
        tick_size: float,
        correlation_id: str,
        trading_symbol: str = "",
        broker_order_id: Optional[str] = None,
        state: str = "SUBMITTING",
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Safety Correction A & CV13-01: Persists and commits a protection attempt in `SUBMITTING` state BEFORE
        any protective-order POST or Super Order modification. Raises on SQLite failure or duplicate active slice
        so the caller never submits an unpersisted or competing order.
        """
        ts = float(now if now is not None else time.time())
        acct = str(account_id or self.account_id).strip()
        corr = str(correlation_id)[:30]
        att_id = str(attempt_id)[:40]
        b_oid = str(broker_order_id) if broker_order_id else None
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO protection_attempts (
                    attempt_id, parent_order_id, root_order_id, account_id, symbol, trading_symbol,
                    security_id, exchange_segment, product_type, route, uncovered_from_qty, quantity,
                    executed_price, sl_trigger, sl_limit, target_price, tick_size, correlation_id,
                    state, broker_order_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    att_id,
                    str(parent_order_id),
                    str(root_order_id or parent_order_id),
                    acct,
                    str(symbol),
                    str(trading_symbol or symbol),
                    str(security_id),
                    str(exchange_segment),
                    str(product_type),
                    str(route),
                    int(uncovered_from_qty),
                    int(quantity),
                    float(executed_price),
                    float(sl_trigger),
                    float(sl_limit),
                    float(target_price),
                    float(tick_size or 0.05),
                    corr,
                    str(state),
                    b_oid,
                    ts,
                    ts,
                ),
            )
        return {
            "attempt_id": att_id,
            "parent_order_id": str(parent_order_id),
            "root_order_id": str(root_order_id or parent_order_id),
            "account_id": acct,
            "symbol": str(symbol),
            "trading_symbol": str(trading_symbol or symbol),
            "security_id": str(security_id),
            "exchange_segment": str(exchange_segment),
            "product_type": str(product_type),
            "route": str(route),
            "uncovered_from_qty": int(uncovered_from_qty),
            "qty": int(quantity),
            "executed_price": float(executed_price),
            "sl_trigger": float(sl_trigger),
            "sl_limit": float(sl_limit),
            "target_price": float(target_price),
            "tick_size": float(tick_size or 0.05),
            "correlation_id": corr,
            "oco_id": b_oid,
            "state": str(state),
        }

    def update_protection_attempt(
        self,
        attempt_or_corr_id: str,
        state: str,
        broker_order_id: Optional[str] = None,
        response_payload: Optional[Any] = None,
        error_msg: Optional[str] = None,
        now: Optional[float] = None,
    ) -> None:
        ts = float(now if now is not None else time.time())
        resp_str = None
        if response_payload is not None:
            try:
                resp_str = json.dumps(response_payload) if not isinstance(response_payload, str) else response_payload
            except Exception:
                resp_str = str(response_payload)
        key = str(attempt_or_corr_id)
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE protection_attempts
                SET state = ?,
                    broker_order_id = COALESCE(?, broker_order_id),
                    response_json = COALESCE(?, response_json),
                    error_msg = COALESCE(?, error_msg),
                    updated_at = ?
                WHERE attempt_id = ? OR correlation_id = ?
                """,
                (str(state), broker_order_id, resp_str, error_msg, ts, key, key[:30]),
            )

    def supersede_protection_attempts(self, parent_order_id: str, now: Optional[float] = None) -> int:
        ts = float(now if now is not None else time.time())
        placeholders = ",".join("?" for _ in UNRESOLVED_PROTECTION_ATTEMPT_STATES)
        with self._get_conn() as conn:
            cur = conn.execute(
                f"UPDATE protection_attempts SET state = 'SUPERSEDED', updated_at = ? WHERE parent_order_id = ? AND state IN ({placeholders})",
                (ts, str(parent_order_id), *UNRESOLVED_PROTECTION_ATTEMPT_STATES),
            )
            return cur.rowcount

    def get_unresolved_protection_attempts(self, parent_order_id: Optional[str] = None) -> List[Dict[str, Any]]:
        placeholders = ",".join("?" for _ in UNRESOLVED_PROTECTION_ATTEMPT_STATES)
        with self._get_conn() as conn:
            if parent_order_id is not None:
                cur = conn.execute(
                    f"SELECT * FROM protection_attempts WHERE parent_order_id = ? AND state IN ({placeholders}) ORDER BY created_at ASC",
                    (str(parent_order_id), *UNRESOLVED_PROTECTION_ATTEMPT_STATES),
                )
            else:
                cur = conn.execute(
                    f"SELECT * FROM protection_attempts WHERE state IN ({placeholders}) ORDER BY created_at ASC",
                    UNRESOLVED_PROTECTION_ATTEMPT_STATES,
                )
            return [dict(row) for row in cur.fetchall()]

    def get_protection_attempts(self, parent_order_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            if parent_order_id is not None:
                cur = conn.execute(
                    "SELECT * FROM protection_attempts WHERE parent_order_id = ? ORDER BY created_at ASC",
                    (str(parent_order_id),),
                )
            else:
                cur = conn.execute("SELECT * FROM protection_attempts ORDER BY created_at ASC")
            return [dict(row) for row in cur.fetchall()]

    # --- Signal Claims & Revisions (08-B / DEC-06 / JOURNAL-01) ---

    def claim_signal(
        self,
        channel_id: int,
        message_id: int,
        raw_text: str,
        symbol: str = "",
        account_id: str = "",
        strategy_id: str = "DEFAULT",
    ) -> Tuple[bool, str, int]:
        """
        08-B / JOURNAL-01: Durable atomic signal claim safe across concurrent threads and processes.
        Uses atomic INSERT OR IGNORE + immediate row inspection inside a transaction so concurrent
        callers never raise an uncaught `sqlite3.IntegrityError`.
        """
        now = time.time()
        acct = str(account_id or self.account_id).strip()
        with self._get_conn() as conn:
            cur_ins = conn.execute(
                """
                INSERT OR IGNORE INTO signal_claims (
                    account_id, strategy_id, channel_id, message_id, symbol, first_seen_at, raw_text, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'CLAIMED')
                """,
                (acct, strategy_id, channel_id, message_id, symbol, now, raw_text),
            )
            if cur_ins.rowcount == 1:
                return True, "CLAIM_ACCEPTED", cur_ins.lastrowid

            cur = conn.execute(
                "SELECT id, raw_text FROM signal_claims WHERE channel_id = ? AND message_id = ?",
                (channel_id, message_id),
            )
            existing = cur.fetchone()
            if existing is None:
                return False, "DUPLICATE_MESSAGE_CLAIM", 0

            claim_id, old_text = existing["id"], existing["raw_text"]
            if old_text.strip() == raw_text.strip():
                return False, "DUPLICATE_MESSAGE_CLAIM", claim_id

            # Edited message -> record revision without creating a second trade intent
            cur_rev = conn.execute(
                "SELECT COUNT(*) FROM signal_revisions WHERE claim_id = ?",
                (claim_id,),
            )
            rev_count = cur_rev.fetchone()[0] + 1
            conn.execute(
                """
                INSERT INTO signal_revisions (claim_id, revision_num, old_text, new_text, revised_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (claim_id, rev_count, old_text, raw_text, now),
            )
            conn.execute(
                "UPDATE signal_claims SET raw_text = ? WHERE id = ?",
                (raw_text, claim_id),
            )
            logger.info(f"[Journal] Message revision {rev_count} recorded for channel {channel_id} msg {message_id}. No new entry triggered.")
            return False, "MESSAGE_EDITED_EVENT", claim_id

    # --- Pre-Submit Order Intents (08-C / 08-D / 08-E / V12-02) ---

    def record_order_intent(
        self,
        correlation_id: str,
        symbol: str,
        security_id: str,
        exchange_segment: str,
        product_type: str,
        order_type: str,
        price: float,
        quantity: int,
        parent_correlation_id: Optional[str] = None,
        account_id: str = "",
        is_super_order: Optional[bool] = None,
        original_market_price: float = 0.0,
        tick_size: float = 0.05,
        lot_size: int = 1,
        option_type: str = "",
        strike: float = 0.0,
        expiry: Any = "",
        trading_symbol: str = "",
    ) -> int:
        """Persists order intent + full contract/route metadata before network POST (<=30-char correlation ID)."""
        now = time.time()
        acct = str(account_id or self.account_id).strip()
        exp_str = expiry.isoformat() if isinstance(expiry, date) else str(expiry or "").strip()
        if is_super_order is None:
            is_super = 1 if (exchange_segment.upper() in ("NSE_FNO", "BSE_FNO") and product_type.upper() == "MARGIN") else 0
        else:
            is_super = 1 if is_super_order else 0

        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO order_intents (
                    correlation_id, parent_correlation_id, account_id, symbol, trading_symbol,
                    security_id, exchange_segment, product_type, order_type, price, quantity,
                    is_super_order, original_market_price, tick_size, lot_size, option_type, strike, expiry,
                    state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'INTENT_CREATED', ?, ?)
                """,
                (
                    correlation_id[:30],
                    parent_correlation_id[:30] if parent_correlation_id else None,
                    acct,
                    symbol,
                    trading_symbol or symbol,
                    str(security_id),
                    exchange_segment,
                    product_type,
                    order_type,
                    float(price),
                    int(quantity),
                    is_super,
                    float(original_market_price or price),
                    float(tick_size or 0.05),
                    int(lot_size or 1),
                    str(option_type or "").strip().upper(),
                    float(strike or 0.0),
                    exp_str,
                    now,
                    now,
                ),
            )
            return cur.lastrowid

    def reserve_retry_slot(
        self,
        root_order_id: str,
        parent_order_id: str,
        child_correlation_id: str,
    ) -> bool:
        """
        V12-11: Atomically reserves the single retry slot for a root/parent order in SQLite.
        Prevents duplicate retries across threads, child chains, and restarts.
        """
        now = time.time()
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO retry_reservations (
                    root_order_id, parent_order_id, child_correlation_id, reserved_at
                ) VALUES (?, ?, ?, ?)
                """,
                (str(root_order_id), str(parent_order_id), str(child_correlation_id)[:30], now),
            )
            return cur.rowcount == 1

    def has_retry_reservation(self, root_or_parent_id: str) -> bool:
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT 1 FROM retry_reservations WHERE root_order_id = ? OR parent_order_id = ?",
                (str(root_or_parent_id), str(root_or_parent_id)),
            )
            return cur.fetchone() is not None

    def update_order_intent(
        self,
        correlation_id: str,
        state: str,
        broker_order_id: Optional[str] = None,
        error_msg: Optional[str] = None,
    ) -> None:
        """Updates intent state to SUBMITTING, ACKED, REJECTED, NOT_SUBMITTED, UNKNOWN, PART_FILLED, TRADED, or CANCELLED."""
        now = time.time()
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE order_intents
                SET state = ?, broker_order_id = COALESCE(?, broker_order_id),
                    error_msg = COALESCE(?, error_msg), updated_at = ?
                WHERE correlation_id = ?
                """,
                (state, broker_order_id, error_msg, now, correlation_id[:30]),
            )

    def update_order_cancellation_metadata(
        self,
        broker_order_id: str,
        correlation_id: str = "",
        cancel_origin: str = "",
        cancel_reason: str = "",
        retry_eligible: bool = False,
        cancel_attempts: int = 0,
        cancel_acknowledged: bool = False,
        cancel_uncertain: bool = False,
        entry_timeout_elapsed: bool = False,
        state: Optional[str] = None,
        now: Optional[float] = None,
    ) -> None:
        """CV13-02 & CV13-03: Persists cancellation origin, reason, retry eligibility, and recovery state in SQLite."""
        ts = float(now if now is not None else time.time())
        corr = str(correlation_id or broker_order_id)[:30]
        b_oid = str(broker_order_id)
        with self._get_conn() as conn:
            if state is not None:
                conn.execute(
                    """
                    UPDATE order_intents
                    SET cancel_origin = ?,
                        cancel_reason = ?,
                        retry_eligible = ?,
                        cancel_attempts = ?,
                        cancel_acknowledged = ?,
                        cancel_uncertain = ?,
                        entry_timeout_elapsed = ?,
                        state = ?,
                        updated_at = ?
                    WHERE correlation_id = ? OR broker_order_id = ?
                    """,
                    (
                        str(cancel_origin),
                        str(cancel_reason),
                        1 if retry_eligible else 0,
                        int(cancel_attempts),
                        1 if cancel_acknowledged else 0,
                        1 if cancel_uncertain else 0,
                        1 if entry_timeout_elapsed else 0,
                        str(state),
                        ts,
                        corr,
                        b_oid,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE order_intents
                    SET cancel_origin = ?,
                        cancel_reason = ?,
                        retry_eligible = ?,
                        cancel_attempts = ?,
                        cancel_acknowledged = ?,
                        cancel_uncertain = ?,
                        entry_timeout_elapsed = ?,
                        updated_at = ?
                    WHERE correlation_id = ? OR broker_order_id = ?
                    """,
                    (
                        str(cancel_origin),
                        str(cancel_reason),
                        1 if retry_eligible else 0,
                        int(cancel_attempts),
                        1 if cancel_acknowledged else 0,
                        1 if cancel_uncertain else 0,
                        1 if entry_timeout_elapsed else 0,
                        ts,
                        corr,
                        b_oid,
                    ),
                )

    def update_order_lifecycle_by_broker_id(
        self,
        broker_order_id: str,
        correlation_id: str = "",
        state: str = "PENDING",
    ) -> None:
        """V12-19: Advances both `order_intents` and `orders` tables when watcher observes fills or terminal status."""
        now = time.time()
        with self._get_conn() as conn:
            if correlation_id:
                conn.execute(
                    "UPDATE order_intents SET state = ?, broker_order_id = COALESCE(broker_order_id, ?), updated_at = ? WHERE correlation_id = ? OR broker_order_id = ?",
                    (state, str(broker_order_id), now, correlation_id[:30], str(broker_order_id)),
                )
                conn.execute(
                    "UPDATE orders SET status = ?, broker_order_id = COALESCE(broker_order_id, ?), updated_at = ? WHERE correlation_id = ? OR broker_order_id = ?",
                    (state, str(broker_order_id), now, correlation_id[:30], str(broker_order_id)),
                )
            else:
                conn.execute(
                    "UPDATE order_intents SET state = ?, updated_at = ? WHERE broker_order_id = ? OR correlation_id = ?",
                    (state, now, str(broker_order_id), str(broker_order_id)[:30]),
                )
                conn.execute(
                    "UPDATE orders SET status = ?, updated_at = ? WHERE broker_order_id = ? OR correlation_id = ?",
                    (state, now, str(broker_order_id), str(broker_order_id)[:30]),
                )

    def resolve_unprotected_orders(self, parent_order_id: str, resolved_status: str = "SUPERSEDED") -> int:
        """
        V12-19: Marks prior UNPROTECTED / UNVERIFIED / SUBMITTING records for `parent_order_id`
        as SUPERSEDED once active protection is confirmed for that parent order.
        """
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE protection_orders
                SET status = ?
                WHERE parent_order_id = ? AND status IN ('UNPROTECTED', 'UNVERIFIED', 'SUBMITTING')
                """,
                (resolved_status, str(parent_order_id)),
            )
            return cur.rowcount

    def get_unknown_intents(self) -> List[Dict[str, Any]]:
        """Retrieves any intents in UNKNOWN state for safe reconciliation."""
        with self._get_conn() as conn:
            cur = conn.execute("SELECT * FROM order_intents WHERE state = 'UNKNOWN'")
            return [dict(row) for row in cur.fetchall()]

    def get_non_terminal_intents(self) -> List[Dict[str, Any]]:
        """
        15-C / V12-19: Retrieves all non-terminal intents across canonical and raw broker states
        (`INTENT_CREATED`, `SUBMITTING`, `UNKNOWN`, `ACKED`, `PENDING`, `TRANSIT`, `TRIGGERED`,
        `PART_FILLED`, `PART_TRADED`, `CANCEL_REQUESTED`, `CANCEL_PENDING`).
        """
        placeholders = ",".join("?" for _ in NON_TERMINAL_INTENT_STATES)
        with self._get_conn() as conn:
            cur = conn.execute(
                f"SELECT * FROM order_intents WHERE state IN ({placeholders}) ORDER BY created_at ASC",
                NON_TERMINAL_INTENT_STATES,
            )
            return [dict(row) for row in cur.fetchall()]

    def get_unprotected_orders(self) -> List[Dict[str, Any]]:
        """11-F / 15-D: Returns any protection records currently in UNPROTECTED or UNVERIFIED state."""
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT * FROM protection_orders WHERE status IN ('UNPROTECTED', 'UNVERIFIED')"
            )
            return [dict(row) for row in cur.fetchall()]

    # --- Authoritative SQLite Watcher Persistence (08-F / Step 14) ---

    def save_watcher_state(self, pending: Dict[str, dict], terminal: Dict[str, dict]) -> None:
        now = time.time()
        with self._get_conn() as conn:
            conn.execute("DELETE FROM watcher_orders")
            for oid, rec in pending.items():
                conn.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?, 0, ?, ?)",
                    (str(oid), json.dumps(rec), now),
                )
            for oid, rec in terminal.items():
                conn.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?, 1, ?, ?)",
                    (str(oid), json.dumps(rec), now),
                )

    def load_watcher_state(self) -> Tuple[Dict[str, dict], Dict[str, dict]]:
        pending: Dict[str, dict] = {}
        terminal: Dict[str, dict] = {}
        with self._get_conn() as conn:
            cur = conn.execute("SELECT order_id, is_terminal, record_json FROM watcher_orders")
            for row in cur.fetchall():
                try:
                    data = json.loads(row["record_json"])
                    if row["is_terminal"]:
                        terminal[row["order_id"]] = data
                    else:
                        pending[row["order_id"]] = data
                except Exception:
                    pass
        return pending, terminal

    # --- Outbox for Durable Notifications (Step 16) ---

    def enqueue_outbox(self, event_type: str, payload: Dict[str, Any]) -> int:
        now = time.time()
        with self._get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO outbox (event_type, payload, status, retry_count, next_retry_at, created_at) VALUES (?, ?, 'PENDING', 0, 0.0, ?)",
                (event_type, json.dumps(payload), now),
            )
            return cur.lastrowid

    def fetch_pending_outbox(self, limit: int = 10, now: Optional[float] = None) -> List[Dict[str, Any]]:
        current_ts = now if now is not None else time.time()
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                SELECT * FROM outbox
                WHERE status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY id ASC LIMIT ?
                """,
                (current_ts, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    def mark_outbox_processed(self, outbox_id: int) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE outbox SET status = 'PROCESSED', processed_at = ? WHERE id = ?",
                (time.time(), outbox_id),
            )

    def mark_outbox_retry(self, outbox_id: int, error_msg: str = "", max_retries: int = 5, now: Optional[float] = None) -> str:
        """16-C: Increments retry count with exponential backoff or marks DEAD_LETTER if max_retries exceeded."""
        current_ts = now if now is not None else time.time()
        with self._get_conn() as conn:
            cur = conn.execute("SELECT retry_count FROM outbox WHERE id = ?", (outbox_id,))
            row = cur.fetchone()
            if not row:
                return "MISSING"
            retries = int(row["retry_count"] or 0) + 1
            if retries >= max_retries:
                conn.execute(
                    "UPDATE outbox SET status = 'DEAD_LETTER', retry_count = ?, last_error = ? WHERE id = ?",
                    (retries, error_msg[:500], outbox_id),
                )
                return "DEAD_LETTER"
            backoff = min(300.0, float(2 ** retries))
            conn.execute(
                "UPDATE outbox SET retry_count = ?, next_retry_at = ?, last_error = ? WHERE id = ?",
                (retries, current_ts + backoff, error_msg[:500], outbox_id),
            )
            return "RETRY_SCHEDULED"

    # --- Existing Signal & Order Methods (Backward Compatible) ---

    def record_signal(
        self,
        channel_id: int,
        message_id: int,
        signal_type: str,
        symbol: str,
        action: str,
        raw_text: str,
        status: str,
        reason: str,
    ) -> int:
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO signals (channel_id, message_id, signal_type, symbol, action, raw_text, received_at, status, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (channel_id, message_id, signal_type, symbol, action, raw_text, time.time(), status, reason)
            )
            return cur.lastrowid

    def record_validation(
        self,
        signal_id: int,
        symbol: str,
        limit_price: float,
        units: int,
        total_quantity: int,
        total_cost: float,
        is_passed: bool,
        rejection_reason: str,
    ) -> int:
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO validations (signal_id, symbol, limit_price, units, total_quantity, total_cost, is_passed, rejection_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (signal_id, symbol, limit_price, units, total_quantity, total_cost, 1 if is_passed else 0, rejection_reason, time.time())
            )
            return cur.lastrowid

    def record_order(
        self,
        correlation_id: str,
        trading_mode: str,
        product_type: str,
        exchange_segment: str,
        symbol: str,
        security_id: str,
        order_type: str,
        price: float,
        quantity: int,
        status: str = "TEST_VALIDATED",
        broker_order_id: Optional[str] = None,
    ) -> int:
        now = time.time()
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT OR REPLACE INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (correlation_id[:30], broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, status, now, now)
            )
            return cur.lastrowid

    def record_fill(
        self,
        broker_order_id: str,
        exchange_trade_id: str,
        symbol: str,
        traded_price: float,
        traded_quantity: int,
        traded_at: Optional[float] = None,
    ) -> bool:
        """14-B / V12-19: Records unique exchange trade fill idempotently."""
        ts = traded_at if traded_at is not None else time.time()
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(broker_order_id), str(exchange_trade_id), symbol, float(traded_price), int(traded_quantity), ts),
            )
            return cur.rowcount == 1

    def record_system_event(self, event_type: str, description: str, payload: str = "") -> int:
        with self._get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?, ?, ?, ?)",
                (event_type, description, payload, time.time())
            )
            return cur.lastrowid

    def get_today_orders_count(self, start_of_day: Optional[float] = None) -> int:
        if start_of_day is None:
            now_ist = datetime.datetime.now(IST)
            start_of_day = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND trading_mode = 'LIVE' AND status NOT IN ('REJECTED', 'CANCELLED', 'EXPIRED', 'NOT_SUBMITTED')",
                (start_of_day,)
            )
            return cur.fetchone()[0]

    def get_live_portfolio_risk_state(self, start_of_day: Optional[float] = None) -> Dict[str, Any]:
        """V12-08: Aggregates today's live order count, active open positions, and committed exposure."""
        if start_of_day is None:
            now_ist = datetime.datetime.now(IST)
            start_of_day = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        with self._get_conn() as conn:
            daily_trades = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND trading_mode = 'LIVE' AND status NOT IN ('REJECTED', 'CANCELLED', 'EXPIRED', 'NOT_SUBMITTED')",
                (start_of_day,),
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT security_id, price, quantity FROM orders WHERE created_at >= ? AND trading_mode = 'LIVE' AND status IN ('PENDING', 'ACKED', 'PART_FILLED', 'PART_TRADED', 'TRADED')",
                (start_of_day,),
            ).fetchall()
            open_sec_ids = set()
            exposure = 0.0
            for r in rows:
                open_sec_ids.add(str(r["security_id"]))
                exposure += float(r["price"] or 0.0) * int(r["quantity"] or 0)
            return {
                "daily_trades": int(daily_trades),
                "open_positions": len(open_sec_ids),
                "current_exposure": float(exposure),
            }

    def get_recent_signals(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            cur = conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    def get_dashboard_summary(self, start_of_day: Optional[float] = None) -> Dict[str, Any]:
        """
        24-B & 24-C: Calculates read-only metrics aggregated by IST day boundary and canonical states.
        Distinguishes local protection records from broker-confirmed active protection and reports
        all non-terminal intents.
        """
        if start_of_day is not None:
            day_boundary = start_of_day
        else:
            now_ist = datetime.datetime.now(IST)
            day_boundary = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        placeholders = ",".join("?" for _ in NON_TERMINAL_INTENT_STATES)
        with self._get_conn() as conn:
            c_sig_all = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            c_sig_day = conn.execute("SELECT COUNT(*) FROM signals WHERE received_at >= ?", (day_boundary,)).fetchone()[0]
            c_ord_live = conn.execute("SELECT COUNT(*) FROM orders WHERE trading_mode = 'LIVE'").fetchone()[0]
            c_ord_test = conn.execute("SELECT COUNT(*) FROM orders WHERE trading_mode = 'TEST'").fetchone()[0]
            c_ord_eq = conn.execute("SELECT COUNT(*) FROM orders WHERE exchange_segment IN ('NSE_EQ', 'BSE_EQ')").fetchone()[0]
            c_ord_fno = conn.execute("SELECT COUNT(*) FROM orders WHERE exchange_segment IN ('NSE_FNO', 'BSE_FNO')").fetchone()[0]
            c_prot_active = conn.execute("SELECT COUNT(*) FROM protection_orders WHERE status IN ('ACTIVE', 'ACTIVE_CONFIRMED')").fetchone()[0]
            c_prot_confirmed = conn.execute("SELECT COUNT(*) FROM protection_orders WHERE status = 'ACTIVE_CONFIRMED'").fetchone()[0]
            c_prot_unprotected = conn.execute("SELECT COUNT(*) FROM protection_orders WHERE status IN ('UNPROTECTED', 'UNVERIFIED')").fetchone()[0]
            c_unknown = conn.execute("SELECT COUNT(*) FROM order_intents WHERE state = 'UNKNOWN'").fetchone()[0]
            c_non_terminal = conn.execute(
                f"SELECT COUNT(*) FROM order_intents WHERE state IN ({placeholders})",
                NON_TERMINAL_INTENT_STATES,
            ).fetchone()[0]
            att_placeholders = ",".join("?" for _ in UNRESOLVED_PROTECTION_ATTEMPT_STATES)
            c_unres_att = conn.execute(
                f"SELECT COUNT(*) FROM protection_attempts WHERE state IN ({att_placeholders})",
                UNRESOLVED_PROTECTION_ATTEMPT_STATES,
            ).fetchone()[0]

            return {
                "signals_all": c_sig_all,
                "signals_today": c_sig_day,
                "orders_live": c_ord_live,
                "orders_test": c_ord_test,
                "orders_eq": c_ord_eq,
                "orders_fno": c_ord_fno,
                "protection_active": c_prot_active,
                "protection_confirmed": c_prot_confirmed,
                "protection_unprotected": c_prot_unprotected,
                "unresolved_protection_attempts": c_unres_att,
                "unknown_intents": c_unknown,
                "non_terminal_intents": c_non_terminal,
            }


    # --- Durable Latency & Quote-Age Telemetry Persistence (CV13-07 P1) ---

    def record_yahoo_telemetry(
        self,
        success: bool,
        http_latency_ms: Optional[float],
        quote_age_sec: Optional[float],
        symbol: str = "",
    ) -> None:
        age_unknown = 1 if (success and quote_age_sec is None) else 0
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO yahoo_request_telemetry (symbol, success, http_latency_ms, quote_age_sec, quote_age_unknown, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(symbol or ""),
                    1 if success else 0,
                    float(http_latency_ms) if http_latency_ms is not None else None,
                    float(quote_age_sec) if quote_age_sec is not None else None,
                    age_unknown,
                    time.time(),
                ),
            )

    def increment_latency_rejection_reason(self, reason: str) -> None:
        if not reason:
            return
        now = time.time()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO latency_rejection_counters (reason, count, updated_at)
                VALUES (?, 1, ?)
                ON CONFLICT(reason) DO UPDATE SET
                    count = count + 1,
                    updated_at = excluded.updated_at
                """,
                (str(reason)[:80], now),
            )

    def upsert_latency_trace(self, bd: Dict[str, Any]) -> None:
        trace_id = str(bd.get("trace_id") or "")
        if not trace_id:
            return
        now = time.time()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO signal_latency_telemetry (
                    trace_id, correlation_id, broker_order_id, symbol, channel_id, message_id,
                    ist_received_time, telegram_msg_timestamp, cmp_policy, price_source, yahoo_cmp,
                    yahoo_http_ms, yahoo_parse_ms, cmp_auth_ms, quote_timestamp, reported_quote_age_sec,
                    quote_age_status, freshness_verified, quote_freshness_status,
                    l2_parse_ms, l3_resolve_ms, l7_sizing_ms, l8_risk_preflight_ms,
                    telegram_to_cmp_ms, cmp_to_submission_ms, telegram_to_submission_ms,
                    dhan_request_to_ack_ms, telegram_to_ack_ms,
                    ack_wall_time, fill_wall_time, protection_wall_time,
                    ack_to_fill_ms, fill_to_protection_ms, total_processing_ms, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?
                )
                ON CONFLICT(trace_id) DO UPDATE SET
                    correlation_id = COALESCE(excluded.correlation_id, signal_latency_telemetry.correlation_id),
                    broker_order_id = COALESCE(excluded.broker_order_id, signal_latency_telemetry.broker_order_id),
                    ack_wall_time = COALESCE(excluded.ack_wall_time, signal_latency_telemetry.ack_wall_time),
                    fill_wall_time = COALESCE(excluded.fill_wall_time, signal_latency_telemetry.fill_wall_time),
                    protection_wall_time = COALESCE(excluded.protection_wall_time, signal_latency_telemetry.protection_wall_time),
                    ack_to_fill_ms = COALESCE(excluded.ack_to_fill_ms, signal_latency_telemetry.ack_to_fill_ms),
                    fill_to_protection_ms = COALESCE(excluded.fill_to_protection_ms, signal_latency_telemetry.fill_to_protection_ms),
                    dhan_request_to_ack_ms = COALESCE(excluded.dhan_request_to_ack_ms, signal_latency_telemetry.dhan_request_to_ack_ms),
                    telegram_to_ack_ms = COALESCE(excluded.telegram_to_ack_ms, signal_latency_telemetry.telegram_to_ack_ms),
                    total_processing_ms = COALESCE(excluded.total_processing_ms, signal_latency_telemetry.total_processing_ms),
                    updated_at = excluded.updated_at
                """,
                (
                    trace_id,
                    bd.get("correlation_id"),
                    bd.get("broker_order_id"),
                    bd.get("symbol"),
                    int(bd.get("channel_id") or 0),
                    int(bd.get("message_id") or 0),
                    bd.get("ist_received_time"),
                    bd.get("telegram_msg_timestamp"),
                    bd.get("cmp_policy"),
                    bd.get("price_source"),
                    bd.get("yahoo_cmp"),
                    bd.get("yahoo_http_latency_ms"),
                    bd.get("yahoo_parse_ms"),
                    bd.get("cmp_auth_ms"),
                    bd.get("quote_timestamp"),
                    bd.get("reported_quote_age_sec"),
                    bd.get("quote_age_status"),
                    1 if bd.get("freshness_verified") else 0,
                    bd.get("quote_freshness_status"),
                    bd.get("l2_parse_ms"),
                    bd.get("l3_resolve_ms"),
                    bd.get("l7_sizing_ms"),
                    bd.get("l8_risk_preflight_ms"),
                    bd.get("telegram_to_cmp_ms"),
                    bd.get("cmp_to_submission_ms"),
                    bd.get("telegram_to_submission_ms"),
                    bd.get("dhan_request_to_ack_ms"),
                    bd.get("telegram_to_ack_ms"),
                    bd.get("ack_wall_time"),
                    bd.get("fill_wall_time"),
                    bd.get("protection_wall_time"),
                    bd.get("ack_to_fill_ms"),
                    bd.get("fill_to_protection_ms"),
                    bd.get("total_processing_ms"),
                    now,
                ),
            )

    def update_latency_fill_confirmed(
        self,
        identifier: str,
        fill_wall_time: float,
        monotonic_ack_to_fill_ms: Optional[float] = None,
    ) -> Optional[float]:
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT trace_id, ack_wall_time, ack_to_fill_ms
                FROM signal_latency_telemetry
                WHERE broker_order_id = ? OR correlation_id = ? OR trace_id = ?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            if not row:
                return monotonic_ack_to_fill_ms
            if row["ack_to_fill_ms"] is not None:
                return float(row["ack_to_fill_ms"])
            calc_ms = monotonic_ack_to_fill_ms
            if calc_ms is None and row["ack_wall_time"] is not None:
                delta = float(fill_wall_time) - float(row["ack_wall_time"])
                if delta >= 0.0:
                    calc_ms = round(delta * 1000.0, 3)
            conn.execute(
                """
                UPDATE signal_latency_telemetry
                SET fill_wall_time = COALESCE(fill_wall_time, ?),
                    ack_to_fill_ms = COALESCE(ack_to_fill_ms, ?),
                    updated_at = ?
                WHERE trace_id = ?
                """,
                (float(fill_wall_time), calc_ms, time.time(), row["trace_id"]),
            )
            return calc_ms

    def update_latency_protection_verified(
        self,
        identifier: str,
        protection_wall_time: float,
        monotonic_fill_to_prot_ms: Optional[float] = None,
    ) -> Optional[float]:
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT trace_id, fill_wall_time, ack_wall_time, fill_to_protection_ms
                FROM signal_latency_telemetry
                WHERE broker_order_id = ? OR correlation_id = ? OR trace_id = ?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            if not row:
                return monotonic_fill_to_prot_ms
            if row["fill_to_protection_ms"] is not None:
                return float(row["fill_to_protection_ms"])
            calc_ms = monotonic_fill_to_prot_ms
            base_wt = row["fill_wall_time"] if row["fill_wall_time"] is not None else row["ack_wall_time"]
            if calc_ms is None and base_wt is not None:
                delta = float(protection_wall_time) - float(base_wt)
                if delta >= 0.0:
                    calc_ms = round(delta * 1000.0, 3)
            conn.execute(
                """
                UPDATE signal_latency_telemetry
                SET protection_wall_time = COALESCE(protection_wall_time, ?),
                    fill_to_protection_ms = COALESCE(fill_to_protection_ms, ?),
                    updated_at = ?
                WHERE trace_id = ?
                """,
                (float(protection_wall_time), calc_ms, time.time(), row["trace_id"]),
            )
            return calc_ms

    def get_latency_trace(self, identifier: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM signal_latency_telemetry
                WHERE broker_order_id = ? OR correlation_id = ? OR trace_id = ?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            return dict(row) if row else None

    def load_latency_telemetry_state(self, max_samples: int = 500) -> Dict[str, Any]:
        with self._get_conn() as conn:
            yf_succ = conn.execute("SELECT COUNT(*) FROM yahoo_request_telemetry WHERE success = 1").fetchone()[0]
            yf_fail = conn.execute("SELECT COUNT(*) FROM yahoo_request_telemetry WHERE success = 0").fetchone()[0]
            q_unk = conn.execute("SELECT COUNT(*) FROM yahoo_request_telemetry WHERE quote_age_unknown = 1").fetchone()[0]

            yf_rows = conn.execute(
                "SELECT http_latency_ms, quote_age_sec FROM yahoo_request_telemetry ORDER BY id DESC LIMIT ?",
                (max_samples,),
            ).fetchall()
            yf_lats = [float(r["http_latency_ms"]) for r in reversed(yf_rows) if r["http_latency_ms"] is not None]
            q_ages = [float(r["quote_age_sec"]) for r in reversed(yf_rows) if r["quote_age_sec"] is not None]

            tr_rows = conn.execute(
                "SELECT * FROM signal_latency_telemetry ORDER BY ist_received_time DESC LIMIT ?",
                (max_samples,),
            ).fetchall()
            trace_dicts = [dict(r) for r in reversed(tr_rows)]

            tg_cmp = [float(r["telegram_to_cmp_ms"]) for r in trace_dicts if r.get("telegram_to_cmp_ms") is not None]
            tg_ack = [float(r["telegram_to_ack_ms"]) for r in trace_dicts if r.get("telegram_to_ack_ms") is not None]
            ack_fill = [float(r["ack_to_fill_ms"]) for r in trace_dicts if r.get("ack_to_fill_ms") is not None]
            fill_prot = [float(r["fill_to_protection_ms"]) for r in trace_dicts if r.get("fill_to_protection_ms") is not None]

            rej_rows = conn.execute("SELECT reason, count FROM latency_rejection_counters").fetchall()
            rejs = {str(r["reason"]): int(r["count"]) for r in rej_rows}

            return {
                "yahoo_success_count": int(yf_succ),
                "yahoo_failure_count": int(yf_fail),
                "quote_age_unknown_count": int(q_unk),
                "yahoo_http_latencies_ms": yf_lats,
                "quote_ages_sec": q_ages,
                "telegram_to_cmp_latencies_ms": tg_cmp,
                "telegram_to_ack_latencies_ms": tg_ack,
                "ack_to_fill_latencies_ms": ack_fill,
                "fill_to_protection_latencies_ms": fill_prot,
                "rejection_reasons": rejs,
                "trace_rows": trace_dicts,
            }
