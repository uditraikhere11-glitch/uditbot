#!/usr/bin/env python3
"""Offline-only batch preflight benchmark for Phase 2 reconciliation.

Real preflight function, synthetic Dhan tradebook rows. This is startup
validation overhead only, not Dhan REST response time or execution latency.
"""
from __future__ import annotations
import argparse
import json
import statistics
import time
from src.engine.broker_tradebook_preflight import preflight_tradebook


def bench(count=1000, rounds=10):
    if not 10 <= count <= 20000 or not 3 <= rounds <= 100:
        raise ValueError("invalid benchmark bounds")
    book = [{
        "dhanClientId":"SYNTHETIC", "orderId":f"ORDER-{i//3}",
        "exchangeTradeId":f"EXEC-{i}", "securityId":f"SEC-{i%25}",
        "exchangeSegment":"NSE_EQ", "productType":"CNC",
        "transactionType":"BUY" if i%2==0 else "SELL",
        "tradedQuantity":1+i%3, "tradedPrice":100.0 + i%9,
        "exchangeTime":"2026-11-21 09:31:00",
    } for i in range(count)]
    samples=[]
    for _ in range(rounds):
        t0=time.perf_counter_ns()
        n=preflight_tradebook(book,expected_account_id="SYNTHETIC")
        samples.append((time.perf_counter_ns()-t0)/1e6)
        if n!=count:
            raise AssertionError("preflight silently dropped an execution")
    return {
        "kind":"phase2-tradebook-batch-preflight-offline-v1",
        "synthetic_executions":count, "rounds":rounds,
        "median_preflight_ms":round(statistics.median(samples),3),
        "min_preflight_ms":round(min(samples),3),
        "max_preflight_ms":round(max(samples),3),
        "canonical_fill_writes_during_preflight":0,
        "broker_requests":0,"production_recovery_verified":False,
        "limitations":"Local synthetic validation cost only; no real account/book completeness or VM load.",
    }


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count",type=int,default=1000)
    p.add_argument("--rounds",type=int,default=10)
    a=p.parse_args()
    print(json.dumps(bench(a.count,a.rounds),indent=2))
