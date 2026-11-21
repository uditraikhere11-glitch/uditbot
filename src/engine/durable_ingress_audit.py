"""Standalone durable ingress evidence, NOT an order-replay engine.

Opt-in component for experiments. Not imported by production run.py, and
does not access Dhan, Telegram, risk, STOP, or the canonical trading journal.

Commits a SHA-256 source-message fingerprint before a candidate execution.
An unreturned signal persists across crash and requires broker/journal review;
it is NEVER submitted again automatically. A completed callback proves only
that the local pipeline returned, not that an order is accepted or protected.
"""
from __future__ import annotations

from hashlib import sha256
import math
import os
from pathlib import Path
import sqlite3
import stat
import time


class IngressAuditConflict(ValueError):
    """Message identity or durable account changed; manual review required."""


class DurableIngressAudit:
    SUFFIX = ".ingress-audit.sqlite3"

    def __init__(self, filename: str, *, account_id: str):
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("explicit account_id required")
        path = Path(filename).expanduser().absolute()
        if not path.name.endswith(self.SUFFIX):
            raise ValueError("separate ingress-audit database suffix required")
        if not path.parent.is_dir() or path.is_symlink():
            raise ValueError("existing safe directory and non-symlink file required")
        created_new = False
        try:
            fd=os.open(path, os.O_CREAT|os.O_EXCL|os.O_RDWR, 0o600)
        except FileExistsError:
            if path.is_symlink() or not path.is_file():
                raise ValueError("invalid existing ingress audit file")
            mode=stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise PermissionError("ingress audit file must not be group/world accessible")
            if path.stat().st_size <= 0:
                raise IngressAuditConflict("existing empty ledger cannot reset history")
        else:
            os.close(fd)
            created_new = True
        self.path=path
        self.account_id=account_id.strip()
        self.conn=sqlite3.connect(str(path),timeout=3.0,isolation_level=None)
        try:
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("BEGIN IMMEDIATE")
            if created_new:
                self.conn.execute("CREATE TABLE ingress_meta "
                                  "(name TEXT PRIMARY KEY, value TEXT NOT NULL)")
                self.conn.execute("""
                    CREATE TABLE ingress_messages (
                        channel_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        raw_hash TEXT NOT NULL,
                        message_timestamp REAL NOT NULL,
                        state TEXT NOT NULL CHECK (state IN ('RECEIVED','PIPELINE_RETURNED')),
                        first_seen REAL NOT NULL,
                        last_update REAL NOT NULL,
                        PRIMARY KEY (channel_id, message_id)
                    )
                """)
                self.conn.execute("INSERT INTO ingress_meta VALUES (?, ?)",
                                  ("account_id", self.account_id))
            else:
                tables = {r[0] for r in self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if not {"ingress_meta", "ingress_messages"} <= tables:
                    raise IngressAuditConflict("existing ledger schema incomplete; manual review required")
                binding = self.conn.execute(
                    "SELECT value FROM ingress_meta WHERE name='account_id'"
                ).fetchone()
                if binding is None or binding[0] != self.account_id:
                    raise IngressAuditConflict("existing ledger account binding missing or mismatched")
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.close()
            raise

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self,*args):
        self.close()

    @staticmethod
    def _identity(channel_id, message_id, text, message_timestamp):
        if (type(channel_id) is not int or channel_id == 0
                or type(message_id) is not int or message_id <= 0
                or not isinstance(text, str) or not text.strip()
                or isinstance(message_timestamp, bool)
                or not isinstance(message_timestamp, (int, float))
                or not math.isfinite(float(message_timestamp))
                or message_timestamp <= 0):
            raise ValueError("invalid Telegram source identity or timestamp")
        return (channel_id, message_id, sha256(text.encode("utf-8")).hexdigest(),
                float(message_timestamp))

    def admit(self, *, channel_id: int, message_id: int, text: str,
              message_timestamp: float, before_commit=None) -> str:
        """Persist before submitting, never mutate an earlier signal identity.

        Return ADMITTED or DUPLICATE_PENDING/DUPLICATE_RETURNED.
        A duplicate must NOT be sent to the execution pipeline again.
        """
        chan, mid, digest, ts=self._identity(
            channel_id,message_id,text,message_timestamp
        )
        now=time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row=self.conn.execute(
                "SELECT raw_hash,message_timestamp,state FROM ingress_messages "
                "WHERE channel_id=? AND message_id=?",
                (chan,mid),
            ).fetchone()
            if row is not None:
                if row[0] != digest or float(row[1]) != ts:
                    raise IngressAuditConflict("existing signal identity edited or changed")
                result=("DUPLICATE_RETURNED" if row[2]=="PIPELINE_RETURNED"
                        else "DUPLICATE_PENDING")
                self.conn.execute("COMMIT")
                return result
            self.conn.execute(
                "INSERT INTO ingress_messages "
                "(channel_id,message_id,raw_hash,message_timestamp,state,"
                "first_seen,last_update) VALUES(?,?,?,?,?,?,?)",
                (chan,mid,digest,ts,"RECEIVED",now,now),
            )
            if before_commit is not None:
                before_commit()  # offline crash-test injection, never broker mutation
            self.conn.execute("COMMIT")
            return "ADMITTED"
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def mark_pipeline_returned(self, *, channel_id: int, message_id: int,
                               text: str, message_timestamp: float) -> str:
        """Marks callback return ONLY; never marks broker ACK/fills/protection."""
        chan,mid,digest,ts=self._identity(
            channel_id,message_id,text,message_timestamp
        )
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row=self.conn.execute(
                "SELECT raw_hash,message_timestamp,state FROM ingress_messages "
                "WHERE channel_id=? AND message_id=?",
                (chan,mid),
            ).fetchone()
            if row is None or row[0]!=digest or float(row[1])!=ts:
                raise IngressAuditConflict("cannot complete unknown/changed signal")
            if row[2] == "PIPELINE_RETURNED":
                self.conn.execute("COMMIT")
                return "ALREADY_RETURNED"
            self.conn.execute(
                "UPDATE ingress_messages SET state='PIPELINE_RETURNED', "
                "last_update=? WHERE channel_id=? AND message_id=? AND state='RECEIVED'",
                (time.time(),chan,mid),
            )
            self.conn.execute("COMMIT")
            return "RETURNED"
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def summary(self) -> dict:
        rows=self.conn.execute(
            "SELECT state,COUNT(*) FROM ingress_messages GROUP BY state"
        ).fetchall()
        by_state=dict(rows)
        pending=int(by_state.get("RECEIVED",0))
        returned=int(by_state.get("PIPELINE_RETURNED",0))
        return {
            "kind":"uditbot-ingress-audit-v1",
            "unreturned_signals":pending,
            "pipeline_returned_signals":returned,
            "requires_manual_reconciliation":pending>0,
            "automatic_replay_permitted":False,
            "broker_fill_or_protection_certified":False,
            "production_approved":False,
        }
