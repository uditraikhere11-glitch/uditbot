#!/usr/bin/env python3
"""Benchmark the actual UditBot journal-risk hot path (synthetic data only).

Compares the pre-optimization full-history fill-grouping SQL oracle with
JournalDB.get_live_portfolio_risk_state on a temporary REAL JournalDB schema.

Never reads production data, broker credentials or a Dhan endpoint. Output is
a laboratory database benchmark, not actual order latency or live VM speed.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import statistics
import tempfile
import time

from src.journal.db import JournalDB
from tests.test_execution_modernization_journal_risk import _legacy_risk, START


def measure(*, historical_fills: int = 30000, rounds: int = 10):
    if not (1000 <= historical_fills <= 150000) or not (3 <= rounds <= 100):
        raise ValueError("invalid benchmark bounds")
    with tempfile.TemporaryDirectory() as directory:
        journal=JournalDB(str(Path(directory)/"risk.sqlite3"),account_id="SYNTHETIC")
        with journal._get_conn() as conn:
            conn.execute(
                "INSERT INTO orders(correlation_id,broker_order_id,trading_mode,"
                "security_id,price,quantity,filled_quantity,status,created_at) "
                "VALUES('TODAY','TODAY','LIVE','SYNTHETIC_SEC',100,2,0,'PENDING',?)",
                (START+3,),
            )
            conn.executemany(
                "INSERT INTO fills (broker_order_id,exchange_trade_id,"
                "traded_quantity) VALUES(?,?,?)",
                [(f"HIST-{i//2}",f"TRADE-{i}",(i%3)+1)
                 for i in range(historical_fills)],
            )
            conn.execute(
                "INSERT INTO fills(broker_order_id,exchange_trade_id,traded_quantity)"
                " VALUES('TODAY','TODAY-TRADE',1)"
            )

        baseline_ms=[]
        optimized_ms=[]
        expected=None
        for _ in range(rounds):
            t0=time.perf_counter_ns()
            with journal._get_conn() as conn:
                expected=_legacy_risk(conn)
            baseline_ms.append((time.perf_counter_ns()-t0)/1e6)

            t1=time.perf_counter_ns()
            current=journal.get_live_portfolio_risk_state(START)
            optimized_ms.append((time.perf_counter_ns()-t1)/1e6)
            if expected!=current:
                raise AssertionError("indexed risk method disagreed with original SQL")
        before=statistics.median(baseline_ms)
        after=statistics.median(optimized_ms)
        return {
            "kind":"synthetic-risk-journal-sql-benchmark-v1",
            "historical_exchange_fills":historical_fills,
            "today_live_orders":1,
            "rounds":rounds,
            "legacy_full_history_median_ms":round(before,3),
            "indexed_relevant_only_median_ms":round(after,3),
            "median_speedup_x":round(before/after,2) if after>0 else None,
            "risk_state_equivalent":True,
            "orders_submitted":0,
            "production_latency_improvement_proven":False,
            "limitations":"Synthetic SQLite workload. Live Dhan REST latency, journal sizes and actual VM hardware not measured.",
        }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--historical-fills",type=int,default=30000)
    p.add_argument("--rounds",type=int,default=10)
    args=p.parse_args()
    print(json.dumps(measure(historical_fills=args.historical_fills,
                             rounds=args.rounds),indent=2))


if __name__=="__main__":
    main()
