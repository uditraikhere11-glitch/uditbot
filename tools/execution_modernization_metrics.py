#!/usr/bin/env python3
"""Read-only, anonymized UditBot latency report and before/after comparison.

Example:
  python3 tools/execution_modernization_metrics.py collect journal.sqlite > before.json
  python3 tools/execution_modernization_metrics.py compare before.json after.json

Use a consistent database snapshot and comparable market sessions. No broker
calls, no credentials, no order/account IDs in output, and no writes.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sqlite3
from urllib.parse import quote


METRICS=(
    "total_processing_ms", "l2_parse_ms", "l3_resolve_ms",
    "yahoo_http_ms", "l7_sizing_ms", "l8_risk_preflight_ms",
    "telegram_to_cmp_ms", "telegram_to_submission_ms",
    "dhan_request_to_ack_ms", "telegram_to_ack_ms",
    "ack_to_fill_ms", "fill_to_protection_ms",
)


def percentile(values, percentage):
    if not values:
        return None
    i=(len(values)-1)*percentage/100
    lo,hi=math.floor(i),math.ceil(i)
    return round(values[lo]+(values[hi]-values[lo])*(i-lo),3)


def describe(values):
    ordered=sorted(float(x) for x in values if type(x) in (int,float)
                   and math.isfinite(float(x)) and x>=0)
    return {"n":len(ordered),"p50":percentile(ordered,50),
            "p95":percentile(ordered,95),"p99":percentile(ordered,99)}


def collect(path, limit=1000):
    src=Path(path).expanduser().resolve()
    if not src.is_file():
        raise FileNotFoundError("latency journal does not exist")
    if type(limit) is not int or not 1<=limit<=100000:
        raise ValueError("limit outside 1..100000")
    uri="file:"+quote(str(src),safe="/")+"?mode=ro"
    conn=sqlite3.connect(uri,uri=True,timeout=3)
    try:
        conn.execute("PRAGMA query_only=ON")
        tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "signal_latency_telemetry" not in tables:
            raise ValueError("signal_latency_telemetry unavailable")
        cols={r[1] for r in conn.execute("PRAGMA table_info(signal_latency_telemetry)")}
        if "ist_received_time" not in cols:
            raise ValueError("signal latency timestamp absent")
        fields=[f for f in METRICS if f in cols]
        if not fields:
            raise ValueError("no supported latency columns")
        records=conn.execute(
            "SELECT ist_received_time,"+",".join(fields)+
            " FROM signal_latency_telemetry ORDER BY ist_received_time DESC LIMIT ?",
            (limit,),
        ).fetchall()
        timestamps=[r[0] for r in records if type(r[0]) in (int,float)
                    and math.isfinite(r[0]) and r[0]>0]
        return {
            "kind":"uditbot-latency-observation-v1",
            "sample_rows":len(records),
            "time_range_utc":{
                "start":datetime.fromtimestamp(min(timestamps),timezone.utc).isoformat() if timestamps else None,
                "end":datetime.fromtimestamp(max(timestamps),timezone.utc).isoformat() if timestamps else None,
            },
            "metrics_ms":{f:describe([r[i+1] for r in records]) for i,f in enumerate(fields)},
            "limitations":"Recorded journal rows only; historical latency excludes ingress queue wait. Compare like-for-like sessions.",
        }
    finally:
        conn.close()


def compare(baseline,candidate):
    result={"kind":"uditbot-latency-difference-v1",
            "baseline_rows":baseline.get("sample_rows",0),
            "candidate_rows":candidate.get("sample_rows",0),
            "metrics":{},"production_approved":False}
    for field in sorted(set(baseline.get("metrics_ms",{})) & set(candidate.get("metrics_ms",{}))):
        a,b=baseline["metrics_ms"][field],candidate["metrics_ms"][field]
        r={"n_before":a.get("n",0),"n_after":b.get("n",0),
           "minimum_sample_gate":min(a.get("n",0),b.get("n",0))>=30}
        for key in ("p50","p95","p99"):
            x,y=a.get(key),b.get(key)
            r[key]={"before_ms":x,"after_ms":y,
                    "change_pct":round((y-x)/x*100,2) if type(x) in (int,float)
                                and type(y) in (int,float) and x>0 else None}
        result["metrics"][field]=r
    result["limitations"]="Descriptive change only; workloads and broker/market conditions must be matched. Safety gates must be tested separately."
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest="cmd",required=True)
    c=sub.add_parser("collect")
    c.add_argument("db")
    c.add_argument("--limit",type=int,default=1000)
    d=sub.add_parser("compare")
    d.add_argument("baseline")
    d.add_argument("candidate")
    args=p.parse_args()
    try:
        if args.cmd=="collect":
            result=collect(args.db,args.limit)
        else:
            with open(args.baseline,encoding="utf-8") as f:
                baseline=json.load(f)
            with open(args.candidate,encoding="utf-8") as f:
                candidate=json.load(f)
            result=compare(baseline,candidate)
        print(json.dumps(result,indent=2))
    except (OSError,sqlite3.Error,ValueError,KeyError) as exc:
        p.exit(2,f"Measurement unavailable: {type(exc).__name__}: {exc}\n")


if __name__=="__main__":
    main()
