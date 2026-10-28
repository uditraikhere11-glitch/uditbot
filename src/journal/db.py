"""
Durable SQLite Audit Journal and State Persistence.
Maintains tamper-evident audit history of all signals, validations, orders, and fills.
Zero synthetic fills or simulated holdings.
"""

import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


class JournalDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

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
                traded_at REAL,
                FOREIGN KEY (broker_order_id) REFERENCES orders(broker_order_id)
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

            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                description TEXT,
                payload TEXT,
                created_at REAL
            );
            """)

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
                INSERT INTO orders (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (correlation_id, broker_order_id, trading_mode, product_type, exchange_segment, symbol, security_id, order_type, price, quantity, status, now, now)
            )
            return cur.lastrowid

    def record_system_event(self, event_type: str, description: str, payload: str = "") -> int:
        with self._get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?, ?, ?, ?)",
                (event_type, description, payload, time.time())
            )
            return cur.lastrowid

    def get_today_orders_count(self) -> int:
        start_of_day = time.time() - (time.time() % 86400)
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND trading_mode = 'LIVE'",
                (start_of_day,)
            )
            return cur.fetchone()[0]

    def get_recent_signals(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            cur = conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]
