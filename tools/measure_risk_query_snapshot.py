#!/usr/bin/env python3
"""Read-only risk-query benchmark on an operator-approved SQLite SNAPSHOT.

Do not pass a hot live journal; use SQLite's backup API to create a consistent
snapshot first, with permission and outside the automated trading path.

Reads ONLY the snapshot in mode=ro; does not initialize JournalDB, migrate
schema, write files, access broker APIs, or reveal instrument/order IDs.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import json
import math
from pathlib import Path
import sqlite3
import statistics
import time
from urllib.parse import quote
from zoneinfo import ZoneInfo


def _median(samples):
    return round(statistics.median(samples),3) if samples else None


def measure_snapshot(path, *, start_of_day=None, rounds=20):
    source=Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError("consistent SQLite snapshot required")
    if type(rounds) is not int or not 3<=rounds<=100:
        raise ValueError("rounds must be between 3 and 100")
    if start_of_day is None:
        now=datetime.now(ZoneInfo("Asia/Kolkata"))
        start_of_day=now.replace(hour=0,minute=0,second=0,
                                  microsecond=0).timestamp()
    if isinstance(start_of_day,bool) or not isinstance(start_of_day,(int,float)) or not math.isfinite(float(start_of_day)):
        raise ValueError("invalid day boundary")
    uri="file:"+quote(str(source),safe="/")+"?mode=ro"
    conn=sqlite3.connect(uri,uri=True,timeout=5)
    try:
        conn.row_factory=sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        names={r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"orders","fills"}.issubset(names):
            raise ValueError("required risk schema missing")
        columns={r[1] for r in conn.execute("PRAGMA table_info(fills)")}
        if not {"broker_order_id","traded_quantity"}.issubset(columns):
            raise ValueError("required exchange-fill columns missing")
        index_names={r[1] for r in conn.execute("PRAGMA index_list(fills)")}
        expected_index="idx_risk_fills_order_id" in index_names
        row_ids=sorted({str(r[0]) for r in conn.execute(
            "SELECT broker_order_id FROM orders "
            "WHERE created_at>=? AND trading_mode='LIVE' "
            "AND broker_order_id IS NOT NULL AND broker_order_id!=''",
            (float(start_of_day),),
        ) if r[0]})
        old_ms=[]
        new_ms=[]
        observed=None
        # Read-only fixture equivalence per round is mandatory.
        for _ in range(rounds):
            t0=time.perf_counter_ns()
            old={}
            for r in conn.execute(
                "SELECT broker_order_id,SUM(traded_quantity) "
                "FROM fills GROUP BY broker_order_id"
            ):
                if r[0]:
                    old[str(r[0])]=int(r[1] or 0)
            old_ms.append((time.perf_counter_ns()-t0)/1e6)
            t1=time.perf_counter_ns()
            new={}
            for k in range(0,len(row_ids),400):
                batch=row_ids[k:k+400]
                p=",".join("?" for _ in batch)
                for r in conn.execute(
                    f"SELECT broker_order_id,SUM(traded_quantity) "
                    f"FROM fills WHERE broker_order_id IN ({p}) "
                    f"GROUP BY broker_order_id",
                    batch,
                ):
                    new[str(r[0])]=int(r[1] or 0)
            new_ms.append((time.perf_counter_ns()-t1)/1e6)
            if {k:old[k] for k in row_ids if k in old}!=new:
                raise AssertionError("legacy/candidate risk sums differ")
            observed=len(new)
        return {
            "kind":"uditbot-readonly-journal-snapshot-benchmark-v1",
            "rounds":rounds,
            "indexed_candidate_available":expected_index,
            "current_day_distinct_broker_orders":len(row_ids),
            "current_day_orders_with_fills":observed,
            "legacy_full_history_median_ms":_median(old_ms),
            "candidate_relevant_only_median_ms":_median(new_ms),
            "risk_fill_sums_equivalent":True,
            "production_approved":False,
            "limitations":"Read-only snapshot SQL timing; no live network, order placement or risk/protection end-to-end timing.",
        }
    finally:
        conn.close()


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("snapshot",help="path to consistent local SQLite snapshot")
    p.add_argument("--start-of-day",type=float)
    p.add_argument("--rounds",type=int,default=20)
    a=p.parse_args()
    try:
        print(json.dumps(measure_snapshot(
            a.snapshot,start_of_day=a.start_of_day,rounds=a.rounds,
        ),indent=2))
    except (OSError,ValueError,sqlite3.Error,AssertionError) as e:
        p.exit(2,f"Snapshot measurement unavailable: {type(e).__name__}\n")
