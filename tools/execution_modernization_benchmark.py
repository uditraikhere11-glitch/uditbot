#!/usr/bin/env python3
"""Reproducible OFFLINE parser/serial-commit contention benchmark.

Measures real UditBot deterministic parser with a synthetic fixed delay for
the serialized *commit* section. This is NOT live order execution, real
broker latency, or measured production performance; no Dhan/Telegram access.

Baseline: parse + model commit held under one lock.
Candidate: deterministic parse outside lock, results committed in input order.
If parsing is too cheap, thread overhead may outweigh gains. Report either
outcome rather than manufacturing a target speedup.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import statistics
import threading
import time

from src.engine.deterministic_preparse import prepare_deterministic_buy
from src.parsers.brain import parse_signal_brain


def percentile(values, pct):
    v=sorted(values)
    if not v:
        return None
    i=(len(v)-1)*pct/100.0
    a=int(math.floor(i))
    b=int(math.ceil(i))
    return round(v[a]+(v[b]-v[a])*(i-a),3)


def trial(*,count:int,workers:int,commit_ms:float):
    arrival=time.time()
    tasks=[(i,arrival) for i in range(count)]
    lock=threading.Lock()

    def parse_baseline(item):
        index,stamp=item
        result=parse_signal_brain(
            text="Buy #SBIN @805",channel_id=123,message_id=index+1,
            msg_timestamp=stamp,current_time=stamp,
            max_age_sec=120,enable_ai_fallback=False,journal_db=None,
        )
        return result

    def pretend_serial_commit():
        # Models an I/O-bound serialized commitment without touching a broker.
        time.sleep(commit_ms/1000)

    t0=time.perf_counter_ns()
    base_times=[]
    for item in tasks:
        a=time.perf_counter_ns()
        with lock:
            _=parse_baseline(item)
            pretend_serial_commit()
        base_times.append((time.perf_counter_ns()-a)/1_000_000)
    base_wall=(time.perf_counter_ns()-t0)/1_000_000

    def pre(item):
        index,stamp=item
        return prepare_deterministic_buy(
            text="Buy #SBIN @805",channel_id=123,message_id=index+1,
            msg_timestamp=stamp,max_age_sec=120,
        )

    t0=time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        hints=list(pool.map(pre,tasks))  # results returned in original order
    prepared=sum(x is not None for x in hints)
    prep_wall=(time.perf_counter_ns()-t0)/1_000_000

    t1=time.perf_counter_ns()
    for i,item in enumerate(tasks):
        if hints[i] is None:
            _=parse_baseline(item)
        pretend_serial_commit()
    commit_wall=(time.perf_counter_ns()-t1)/1_000_000
    candidate_wall=(time.perf_counter_ns()-t0)/1_000_000
    return {
        "kind":"uditbot-offline-contention-benchmark-v1",
        "count":count,"workers":workers,"synthetic_commit_delay_ms":commit_ms,
        "candidate_regex_hints_usable":prepared,
        "baseline_total_wall_ms":round(base_wall,3),
        "candidate_total_wall_ms":round(candidate_wall,3),
        "candidate_preparation_wall_ms":round(prep_wall,3),
        "candidate_serial_commit_wall_ms":round(commit_wall,3),
        "baseline_sample_processing_p50_ms":percentile(base_times,50),
        "baseline_sample_processing_p95_ms":percentile(base_times,95),
        "relative_wall_time_change_pct":
            round((candidate_wall/base_wall-1)*100,2) if base_wall>0 else None,
        "production_latency_improvement_proven":False,
        "limitations":"Synthetic commit wait, fixed read-only parser fixture, no market/broker/network load; cannot approve real trading.",
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count",type=int,default=120)
    p.add_argument("--workers",type=int,default=8)
    p.add_argument("--commit-delay-ms",type=float,default=2.0)
    args=p.parse_args()
    if not 10<=args.count<=2000 or not 1<=args.workers<=64:
        p.error("count/workers out of range")
    if not math.isfinite(args.commit_delay_ms) or not 0<=args.commit_delay_ms<=50:
        p.error("commit delay out of range")
    print(json.dumps(trial(count=args.count,workers=args.workers,
                           commit_ms=args.commit_delay_ms),indent=2))


if __name__=="__main__":
    main()
