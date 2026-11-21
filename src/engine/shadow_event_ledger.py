"""Crash-consistent *shadow-only* execution evidence ledger.

This is an independent, explicitly created SQLite file ending in
'.shadow.sqlite3', never UditBot's journal. It records normalized status and
trade observations for differential offline replay. It cannot place orders,
alter canonical fills, change protective orders or open trading gates.
"""
from __future__ import annotations
from dataclasses import asdict, fields
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
from typing import Callable, Optional, Tuple

from src.engine.shadow_execution_events import (
    BrokerEvent, EvidenceConflict, OrderProjection,
)


class ShadowLedgerConflict(ValueError):
    """A known event was changed or shadow data is ambiguous."""


class ShadowEventLedger:
    SUFFIX=".shadow.sqlite3"

    def __init__(self, path: str, *, account_id: str):
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("account_id required")
        candidate=Path(path).expanduser().absolute()
        if not candidate.name.endswith(self.SUFFIX):
            raise ValueError("shadow path must end in '.shadow.sqlite3'")
        if not candidate.parent.is_dir():
            raise FileNotFoundError("existing shadow directory required")
        if candidate.is_symlink():
            raise ValueError("symlink shadow ledgers are prohibited")
        # A new shadow ledger is created explicitly with 0600 permissions.
        # No production journal path is opened for write by this module.
        try:
            fd=os.open(candidate, os.O_CREAT|os.O_EXCL|os.O_RDWR, 0o600)
        except FileExistsError:
            if candidate.is_symlink() or not candidate.is_file():
                raise ValueError("invalid existing shadow journal")
            if candidate.stat().st_mode & 0o077:
                raise PermissionError("existing shadow journal permissions must be 0600")
        else:
            os.close(fd)
        self.path=candidate
        self.account_id=account_id
        self._conn=sqlite3.connect(str(candidate), isolation_level=None, timeout=3.0)
        self._conn.row_factory=sqlite3.Row
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA journal_mode=DELETE")
        self._conn.execute("CREATE TABLE IF NOT EXISTS shadow_metadata "
                           "(name TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS shadow_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                order_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                serialized TEXT NOT NULL,
                UNIQUE(account_id, order_id, event_id)
            )
        """)
        try:
            with self._conn:
                row=self._conn.execute(
                    "SELECT value FROM shadow_metadata WHERE name='account_id'"
                ).fetchone()
                if row is None:
                    self._conn.execute("INSERT INTO shadow_metadata VALUES (?,?)",
                                       ("account_id",account_id))
                elif row["value"]!=account_id:
                    raise ShadowLedgerConflict("shadow ledger account ID mismatch")
        except BaseException:
            self._conn.close()
            raise

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self,*args):
        self.close()

    def append(self, event: BrokerEvent, *,
               before_commit: Optional[Callable[[],None]]=None) -> str:
        """Durably commit before projection; conflicts never overwrite evidence."""
        if not isinstance(event,BrokerEvent) or event.account_id!=self.account_id:
            raise ShadowLedgerConflict("event account is unverified")
        if not event.event_id or not event.broker_order_id:
            raise ShadowLedgerConflict("missing event identity")
        serialized=json.dumps(asdict(event),sort_keys=True,separators=(",",":"))
        fingerprint=sha256(serialized.encode("utf-8")).hexdigest()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            previous=self._conn.execute(
                "SELECT fingerprint FROM shadow_events "
                "WHERE account_id=? AND order_id=? AND event_id=?",
                (self.account_id,event.broker_order_id,event.event_id),
            ).fetchone()
            if previous is not None:
                if previous["fingerprint"]!=fingerprint:
                    raise ShadowLedgerConflict("contradictory event with same identity")
                self._conn.execute("COMMIT")
                return "DUPLICATE"
            self._conn.execute(
                "INSERT INTO shadow_events(account_id,order_id,event_id,fingerprint,serialized) "
                "VALUES (?,?,?,?,?)",
                (self.account_id,event.broker_order_id,event.event_id,fingerprint,serialized),
            )
            if before_commit is not None:
                before_commit()
            self._conn.execute("COMMIT")
            return "PERSISTED"
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def count(self) -> int:
        return int(self._conn.execute(
            "SELECT COUNT(*) FROM shadow_events WHERE account_id=?",
            (self.account_id,),
        ).fetchone()[0])

    def replay_order(self, *, broker_order_id: str, security_id: str,
                     exchange_segment: str, product_type: str,
                     requested_qty: int, max_events: int = 10000) -> Tuple[OrderProjection,int]:
        """Replay only journal-attested order identity; no write or broker call."""
        if type(max_events) is not int or max_events<1:
            raise ValueError("invalid replay bound")
        projection=OrderProjection(
            self.account_id,broker_order_id,security_id,
            exchange_segment,product_type,requested_qty,
        )
        count=self._conn.execute(
            "SELECT COUNT(*) FROM shadow_events WHERE account_id=? AND order_id=?",
            (self.account_id,broker_order_id),
        ).fetchone()[0]
        if count>max_events:
            raise ShadowLedgerConflict("replay capacity exceeded")
        columns={f.name for f in fields(BrokerEvent)}
        for row in self._conn.execute(
            "SELECT event_id,fingerprint,serialized FROM shadow_events "
            "WHERE account_id=? AND order_id=? ORDER BY seq",
            (self.account_id,broker_order_id),
        ):
            serialized=row["serialized"]
            if sha256(serialized.encode("utf-8")).hexdigest()!=row["fingerprint"]:
                raise ShadowLedgerConflict("stored shadow fingerprint mismatch")
            data=json.loads(serialized)
            if not isinstance(data,dict) or set(data)!=columns:
                raise ShadowLedgerConflict("stored shadow event schema mismatch")
            event=BrokerEvent(**data)
            if event.event_id!=row["event_id"]:
                raise ShadowLedgerConflict("stored shadow event identity mismatch")
            try:
                projection.apply(event)
            except EvidenceConflict as exc:
                raise ShadowLedgerConflict("replayed broker evidence is contradictory") from exc
        return projection,count
