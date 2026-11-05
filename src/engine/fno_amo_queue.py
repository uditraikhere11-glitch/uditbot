"""Durable, fail-closed local F&O AMO staging.

Dhan's documented Super Order placement schema has no AMO flag. This queue
stores an eligible *signal* after hours and permits one protected Super Order
submission only after an independently verified market-open check. It never
places a naked regular F&O AMO on /orders.

Every dequeue is committed as DISPATCHING before calling broker code. A crash
cannot silently re-submit an ambiguous order: DISPATCHING requires manual review.
"""
from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from typing import Any, Dict, Optional, Tuple

DEFAULT_MAX_AGE_SEC = 18 * 3600
MAX_PENDING = 20

class FnoAmoQueue:
    def __init__(self, data_dir: str, account_id: str = ""):
        self.data_dir = os.path.realpath(data_dir)
        self.account_id = str(account_id or "")
        os.makedirs(self.data_dir, mode=0o700, exist_ok=True)
        self.path = os.path.join(self.data_dir, "fno_amo_queue.sqlite3")
        if os.path.lexists(self.path) and (
            os.path.islink(self.path) or not stat.S_ISREG(os.stat(self.path).st_mode)
        ):
            raise RuntimeError("F&O AMO queue must be a regular non-symlink SQLite file")
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(fd)
        with self._conn() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS fno_amo (
                item_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                created_at REAL NOT NULL, expires_at REAL NOT NULL,
                source TEXT NOT NULL, symbol TEXT NOT NULL,
                contract_id TEXT NOT NULL, expiry TEXT NOT NULL,
                signal_json TEXT NOT NULL, state TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                processed_at REAL
            )""")

    def _conn(self):
        con = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=5000")
        return con

    def enqueue(self, signal: Any, source: str, contract_id: str,
                expiry: str, now: float, max_age_sec: float = DEFAULT_MAX_AGE_SEC) -> Tuple[bool,str]:
        """Queue only a genuine attributed signal and resolved, nonempty contract."""
        if not (int(signal.channel_id) and int(signal.message_id)
                and contract_id and expiry and self.account_id
                and 0 < max_age_sec <= DEFAULT_MAX_AGE_SEC):
            return False, "F&O AMO requires a verified source, contract, account, and bounded lifetime"
        item_id = f"{int(signal.channel_id)}:{int(signal.message_id)}"
        raw = json.dumps(signal.model_dump(mode="json"), separators=(",", ":"), ensure_ascii=True)
        con = self._conn()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute("SELECT state FROM fno_amo WHERE item_id=?", (item_id,)).fetchone()
            if existing:
                con.execute("COMMIT")
                return False, "Duplicate F&O AMO source message already recorded"
            count = con.execute("SELECT COUNT(*) FROM fno_amo WHERE state='QUEUED' AND account_id=?",
                                (self.account_id,)).fetchone()[0]
            if count >= MAX_PENDING:
                con.execute("COMMIT")
                return False, "F&O AMO queue has reached the safety cap"
            competing = con.execute(
                "SELECT item_id FROM fno_amo WHERE contract_id=? AND account_id=? "
                "AND state IN ('QUEUED','DISPATCHING','UNCERTAIN') LIMIT 1",
                (str(contract_id), self.account_id)
            ).fetchone()
            if competing:
                con.execute("COMMIT")
                return False, "An F&O AMO for this contract is already queued or unresolved"
            con.execute(
                "INSERT INTO fno_amo (item_id,account_id,created_at,expires_at,source,symbol,"
                "contract_id,expiry,signal_json,state) VALUES (?,?,?,?,?,?,?,?,?,'QUEUED')",
                (item_id, self.account_id, float(now), float(now+max_age_sec),
                 str(source)[:120], str(signal.symbol)[:40], str(contract_id), str(expiry), raw)
            )
            con.execute("COMMIT")
            return True, item_id
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def claim_one(self, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Atomically reserve one eligible signal before any broker POST."""
        now = float(now if now is not None else time.time())
        con = self._conn()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE fno_amo SET state='EXPIRED',reason='Queue lifetime expired',processed_at=? "
                "WHERE state='QUEUED' AND expires_at<=?", (now, now)
            )
            row = con.execute(
                "SELECT * FROM fno_amo WHERE state='QUEUED' AND account_id=? "
                "AND expires_at>? AND created_at<=? ORDER BY created_at ASC LIMIT 1",
                (self.account_id, now, now)
            ).fetchone()
            if not row:
                con.execute("COMMIT")
                return None
            con.execute("UPDATE fno_amo SET state='DISPATCHING',processed_at=?,"
                        "reason='Reserved for one broker attempt; no automatic retry' "
                        "WHERE item_id=? AND state='QUEUED'", (now, row["item_id"]))
            con.execute("COMMIT")
            return dict(row)
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def finish(self, item_id: str, state: str, reason: str) -> None:
        if state not in ("SUBMITTED", "BLOCKED", "UNCERTAIN"):
            raise ValueError("Invalid F&O AMO terminal state")
        con = self._conn()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT state FROM fno_amo WHERE item_id=?", (item_id,)).fetchone()
            if not row or row["state"] != "DISPATCHING":
                raise RuntimeError("F&O AMO state changed; refusing completion")
            con.execute(
                "UPDATE fno_amo SET state=?,reason=?,processed_at=? WHERE item_id=?",
                (state, str(reason)[:300], time.time(), item_id)
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def cancel_queued(self, item_id: str) -> bool:
        """Operator/safety cancellation allowed ONLY before claim; no broker cancellation."""
        con = self._conn()
        try:
            con.execute("BEGIN IMMEDIATE")
            n = con.execute("UPDATE fno_amo SET state='CANCELLED',reason='Cancelled before dispatch',"
                            "processed_at=? WHERE item_id=? AND state='QUEUED' AND account_id=?",
                            (time.time(), str(item_id), self.account_id)).rowcount
            con.execute("COMMIT")
            return n == 1
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def snapshot(self, limit: int = 50) -> list[Dict[str, Any]]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT item_id,account_id,source,symbol,created_at,expires_at,expiry,state,reason "
                "FROM fno_amo WHERE account_id=? ORDER BY created_at DESC LIMIT ?",
                (self.account_id, min(50, max(1, int(limit))))
            ).fetchall()
            return [dict(r) for r in rows]
