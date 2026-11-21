#!/usr/bin/env python3
"""Offline Dhan rate-governor connection-overhead comparison.

Uses real ReadGovernor with a fake clock and a temporary SQLite ledger.
Baseline repeats full schema verification (original code path) on every GET
admission; optimized keeps the verified schema identity and still writes
durable admissions under BEGIN IMMEDIATE and synchronous=FULL.

No Dhan or Telegram endpoints, no credentials, and NO live rate-limit claims.
"""
from __future__ import annotations
import argparse
import json
import sqlite3
import statistics
import tempfile
import time

from src.broker.read_governor import ReadGovernor


def benchmark(count=150):
    if type(count) is not int or not 30 <= count <= 1000:
        raise ValueError("benchmark count outside 30..1000")
    with tempfile.TemporaryDirectory() as root:
        clock=[400000.0]
        gov=ReadGovernor(root,"SYNTHETIC_ACCOUNT",clock=lambda:clock[0])
        baseline=[]
        fast=[]
        gov.admit(category="NON_TRADING")
        for _ in range(count):
            clock[0]+=1.05
            gov._verified_file_key=None  # measured original schema path
            start=time.perf_counter_ns()
            gov.admit(category="NON_TRADING")
            baseline.append((time.perf_counter_ns()-start)/1e6)
        for _ in range(count):
            clock[0]+=1.05
            start=time.perf_counter_ns()
            gov.admit(category="NON_TRADING")
            fast.append((time.perf_counter_ns()-start)/1e6)
        with sqlite3.connect(gov.path) as conn:
            admitted=conn.execute("SELECT COUNT(*) FROM reads").fetchone()[0]
        if admitted != count*2+1:
            raise AssertionError("admission ledger mutated/lost entries")
        old=statistics.median(baseline)
        new=statistics.median(fast)
        return {
            "kind":"synthetic-dhan-governor-sqlite-admission-v1",
            "admissions_per_mode":count,
            "old_full_schema_median_ms":round(old,3),
            "optimized_verified_schema_median_ms":round(new,3),
            "median_speedup_x":round(old/new,2) if new>0 else None,
            "persisted_admissions_verified":admitted,
            "broker_requests_made":0,
            "production_latency_improvement_proven":False,
            "limitations":"Local synthetic SQLite admission timing only; excludes real broker RTT, multi-process contention and VM hardware.",
        }


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count",type=int,default=150)
    args=p.parse_args()
    print(json.dumps(benchmark(args.count),indent=2))
