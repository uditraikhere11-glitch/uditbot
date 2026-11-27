"""Production-schema SQLite WAL journal seeding and diff utilities for Phase 3."""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from src.journal.db import JournalDB


def seed_journal_from_spec(
    db_path: Path | str,
    *,
    account_id: str,
    spec: Mapping[str, Any],
) -> JournalDB:
    """Create a real production-schema SQLite WAL JournalDB and populate rows from spec."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = JournalDB(str(path), account_id=str(account_id))
    with db._get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        for intent in spec.get("order_intents", []):
            cols = list(intent.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO order_intents ({','.join(cols)}) VALUES ({placeholders})",
                tuple(intent[c] for c in cols),
            )
        for order in spec.get("orders", []):
            cols = list(order.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO orders ({','.join(cols)}) VALUES ({placeholders})",
                tuple(order[c] for c in cols),
            )
        for fill in spec.get("fills", []):
            cols = list(fill.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO fills ({','.join(cols)}) VALUES ({placeholders})",
                tuple(fill[c] for c in cols),
            )
        for alloc in spec.get("exit_allocations", []):
            cols = list(alloc.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO exit_allocations ({','.join(cols)}) VALUES ({placeholders})",
                tuple(alloc[c] for c in cols),
            )
        for lc in spec.get("positions_lifecycle", []):
            cols = list(lc.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO positions_lifecycle ({','.join(cols)}) VALUES ({placeholders})",
                tuple(lc[c] for c in cols),
            )
        for po in spec.get("protection_orders", []):
            cols = list(po.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO protection_orders ({','.join(cols)}) VALUES ({placeholders})",
                tuple(po[c] for c in cols),
            )
        for pa in spec.get("protection_attempts", []):
            cols = list(pa.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO protection_attempts ({','.join(cols)}) VALUES ({placeholders})",
                tuple(pa[c] for c in cols),
            )
        for ack in spec.get("manual_risk_acknowledgements", []):
            cols = list(ack.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO manual_risk_acknowledgements ({','.join(cols)}) VALUES ({placeholders})",
                tuple(ack[c] for c in cols),
            )
        for att in spec.get("broker_attested_historical_fills", []):
            cols = list(att.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO broker_attested_historical_fills ({','.join(cols)}) VALUES ({placeholders})",
                tuple(att[c] for c in cols),
            )
        for ev in spec.get("system_events", []):
            cols = list(ev.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO system_events ({','.join(cols)}) VALUES ({placeholders})",
                tuple(ev[c] for c in cols),
            )
        for ob in spec.get("outbox", []):
            cols = list(ob.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO outbox ({','.join(cols)}) VALUES ({placeholders})",
                tuple(ob[c] for c in cols),
            )
        for wo in spec.get("watcher_orders", []):
            cols = list(wo.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO watcher_orders ({','.join(cols)}) VALUES ({placeholders})",
                tuple(wo[c] for c in cols),
            )
        for rr in spec.get("retry_reservations", []):
            cols = list(rr.keys())
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT OR REPLACE INTO retry_reservations ({','.join(cols)}) VALUES ({placeholders})",
                tuple(rr[c] for c in cols),
            )
    return db


def snapshot_journal_tables(db_path: Path | str) -> dict[str, list[dict[str, Any]]]:
    """Read-only snapshot of key journal tables for exact diff assertions."""
    tables = (
        "order_intents",
        "fills",
        "exit_allocations",
        "positions_lifecycle",
        "protection_orders",
        "protection_attempts",
        "manual_risk_acknowledgements",
        "broker_attested_historical_fills",
        "system_events",
        "outbox",
        "watcher_orders",
        "retry_reservations",
    )
    out: dict[str, list[dict[str, Any]]] = {}
    with closing(sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        existing = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for t in tables:
            if t in existing:
                out[t] = [dict(r) for r in conn.execute(f"SELECT * FROM {t}").fetchall()]
            else:
                out[t] = []
    return out


def journal_state_digest(db_path: Path | str) -> str:
    snap = snapshot_journal_tables(db_path)
    raw = json.dumps(snap, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def diff_journal_snapshots(
    before: Mapping[str, list[dict[str, Any]]],
    after: Mapping[str, list[dict[str, Any]]],
) -> dict[str, dict[str, int]]:
    diffs: dict[str, dict[str, int]] = {}
    for table in sorted(set(before.keys()) | set(after.keys())):
        b_rows = before.get(table, [])
        a_rows = after.get(table, [])
        if b_rows != a_rows:
            diffs[table] = {"before_count": len(b_rows), "after_count": len(a_rows)}
    return diffs
