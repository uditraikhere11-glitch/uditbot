"""Adversarial payload mutator + low-level WAL & production-path crash injection (P3-C06, P3-C16)."""
from __future__ import annotations

from contextlib import closing
import copy
from dataclasses import dataclass
import multiprocessing
import os
from pathlib import Path
import sqlite3
from typing import Any, Mapping

TYPE_CORRUPTIONS: dict[str, Any] = {
    "missing": "__DELETE_KEY__",
    "null": None,
    "zero": 0,
    "boolean_true": True,
    "boolean_false": False,
    "fractional": 1.5,
    "negative": -3,
    "nan": float("nan"),
    "infinity": float("inf"),
    "malformed_string": "NOT_A_VALID_VALUE!!",
}

PRODUCTION_CRASH_BOUNDARIES: tuple[str, ...] = (
    "01_before_record_fill",
    "02_after_record_fill_before_lifecycle",
    "03_after_lifecycle_before_outbox",
    "04_after_outbox_before_readback",
    "05_after_readback_complete",
)


def mutate_single_field(
    record: Mapping[str, Any],
    field_name: str,
    variant: str,
    *,
    override_value: Any = None,
) -> dict[str, Any]:
    out = copy.deepcopy(dict(record))
    if override_value is not None:
        out[field_name] = override_value
        return out
    if variant not in TYPE_CORRUPTIONS:
        raise ValueError(f"Unknown corruption variant: {variant}")
    val = TYPE_CORRUPTIONS[variant]
    if val == "__DELETE_KEY__":
        out.pop(field_name, None)
    else:
        out[field_name] = val
    return out


def make_conflicting_duplicate(
    rows: list[dict[str, Any]],
    *,
    index: int = 0,
    conflict_field: str = "tradedPrice",
    conflict_value: Any = 999.99,
) -> list[dict[str, Any]]:
    out = copy.deepcopy(rows)
    dup = copy.deepcopy(out[index])
    dup[conflict_field] = conflict_value
    out.append(dup)
    return out


@dataclass(frozen=True)
class CrashOutcome:
    boundary: str
    exit_code: int
    crashed_as_expected: bool
    post_crash_fill_count: int
    post_crash_lifecycle_state: str | None
    post_crash_outbox_count: int
    wal_integrity_ok: bool


def _production_crash_worker(
    db_path_str: str,
    account_id: str,
    boundary: str,
    parent_id: str,
    sell_order_id: str,
    sell_eid: str,
) -> None:
    """Executes REAL production JournalDB methods and kills process via os._exit(137) at boundary."""
    from src.journal.db import JournalDB

    db = JournalDB(db_path_str, account_id=account_id)
    now_ts = 1795600000.0

    if boundary == "01_before_record_fill":
        os._exit(137)

    # Step 1: Real production JournalDB.record_fill()
    db.record_fill(
        broker_order_id=sell_order_id,
        exchange_trade_id=sell_eid,
        symbol="PDSL-EQ",
        traded_price=420.0,
        traded_quantity=1,
        traded_at=now_ts,
        transaction_type="SELL",
        security_id="13197",
        exchange_segment="NSE_EQ",
        product_type="CNC",
        parent_order_id=parent_id,
        is_verified_trade=True,
    )
    if boundary == "02_after_record_fill_before_lifecycle":
        os._exit(137)

    # Step 2: Real production JournalDB.upsert_position_lifecycle()
    db.upsert_position_lifecycle(
        parent_order_id=parent_id,
        account_id=account_id,
        symbol="PDSL-EQ",
        trading_symbol="PDSL-EQ",
        security_id="13197",
        exchange_segment="NSE_EQ",
        product_type="CNC",
        protection_route="FOREVER_OCO",
        position_state="POSITION_CLOSED",
        entry_qty=1,
        buy_filled_qty=1,
        buy_vwap=405.0,
        protected_qty=1,
        sell_filled_qty=1,
        sell_vwap=420.0,
        confirmed_open_qty=0,
        realized_pnl=15.0,
        now=now_ts,
    )
    if boundary == "03_after_lifecycle_before_outbox":
        os._exit(137)

    # Step 3: Real production JournalDB.enqueue_outbox()
    db.enqueue_outbox(
        "POSITION_CLOSED",
        {"parent_order_id": parent_id, "sell_exchange_trade_id": sell_eid},
    )
    if boundary == "04_after_outbox_before_readback":
        os._exit(137)

    # Step 4: Real readback check
    lc = db.get_position_lifecycle(parent_id)
    assert lc is not None and lc["position_state"] == "POSITION_CLOSED"
    if boundary == "05_after_readback_complete":
        os._exit(137)


def run_production_crash_boundary(
    db_path: Path | str,
    *,
    account_id: str,
    boundary: str,
    parent_id: str,
    sell_order_id: str,
    sell_eid: str,
    timeout_seconds: float = 10.0,
) -> CrashOutcome:
    """Spawn a real OS process executing real JournalDB methods, kill at boundary, inspect exact state."""
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(
        target=_production_crash_worker,
        args=(str(db_path), account_id, boundary, parent_id, sell_order_id, sell_eid),
    )
    proc.start()
    proc.join(timeout=timeout_seconds)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5.0)
        raise TimeoutError(f"Production crash worker hung at {boundary}")

    exit_code = int(proc.exitcode if proc.exitcode is not None else -1)
    with closing(sqlite3.connect(str(db_path))) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        fills = conn.execute("SELECT COUNT(*) FROM fills WHERE exchange_trade_id = ?", (sell_eid,)).fetchone()[0]
        lc_row = conn.execute(
            "SELECT position_state FROM positions_lifecycle WHERE parent_order_id = ?", (parent_id,)
        ).fetchone()
        outbox = conn.execute("SELECT COUNT(*) FROM outbox WHERE event_type = 'POSITION_CLOSED'").fetchone()[0]

    return CrashOutcome(
        boundary=boundary,
        exit_code=exit_code,
        crashed_as_expected=(exit_code == 137),
        post_crash_fill_count=int(fills),
        post_crash_lifecycle_state=str(lc_row[0]) if lc_row else None,
        post_crash_outbox_count=int(outbox),
        wal_integrity_ok=integrity,
    )


def run_production_recovery_replay(
    db_path: Path | str,
    *,
    account_id: str,
    parent_id: str,
    sell_order_id: str,
    sell_eid: str,
) -> tuple[int, str, int]:
    """Production-path idempotent restart recovery using real JournalDB methods (zero raw SQL repair)."""
    from src.journal.db import JournalDB

    db = JournalDB(str(db_path), account_id=account_id)
    now_ts = 1795600000.0
    # Idempotent fill recording via real JournalDB.record_fill
    db.record_fill(
        broker_order_id=sell_order_id,
        exchange_trade_id=sell_eid,
        symbol="PDSL-EQ",
        traded_price=420.0,
        traded_quantity=1,
        traded_at=now_ts,
        transaction_type="SELL",
        security_id="13197",
        exchange_segment="NSE_EQ",
        product_type="CNC",
        parent_order_id=parent_id,
        is_verified_trade=True,
    )
    db.upsert_position_lifecycle(
        parent_order_id=parent_id,
        account_id=account_id,
        symbol="PDSL-EQ",
        trading_symbol="PDSL-EQ",
        security_id="13197",
        exchange_segment="NSE_EQ",
        product_type="CNC",
        protection_route="FOREVER_OCO",
        position_state="POSITION_CLOSED",
        entry_qty=1,
        buy_filled_qty=1,
        buy_vwap=405.0,
        protected_qty=1,
        sell_filled_qty=1,
        sell_vwap=420.0,
        confirmed_open_qty=0,
        realized_pnl=15.0,
        now=now_ts,
    )
    # Deduplicated logical notification enqueue: only enqueue if not already present
    with closing(sqlite3.connect(str(db_path))) as conn:
        existing_ob = conn.execute("SELECT COUNT(*) FROM outbox WHERE event_type = 'POSITION_CLOSED'").fetchone()[0]
    if existing_ob == 0:
        db.enqueue_outbox("POSITION_CLOSED", {"parent_order_id": parent_id, "sell_exchange_trade_id": sell_eid})

    with closing(sqlite3.connect(str(db_path))) as conn:
        fills = conn.execute("SELECT COUNT(*) FROM fills WHERE exchange_trade_id = ?", (sell_eid,)).fetchone()[0]
        state = conn.execute("SELECT position_state FROM positions_lifecycle WHERE parent_order_id = ?", (parent_id,)).fetchone()[0]
        ob = conn.execute("SELECT COUNT(*) FROM outbox WHERE event_type = 'POSITION_CLOSED'").fetchone()[0]
    return int(fills), str(state), int(ob)
