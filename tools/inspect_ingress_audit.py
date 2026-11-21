#!/usr/bin/env python3
"""Read-only report of unresolved inbound signal identity evidence.

No broker calls, no SQLite migration, no mutation or automatic resubmission.
Report deliberately omits channel IDs, message IDs, raw text and account IDs.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sqlite3
import time


def inspect(path):
    p=Path(path).expanduser().resolve()
    if not p.is_file() or not p.name.endswith(".ingress-audit.sqlite3"):
        raise FileNotFoundError("existing ingress-audit SQLite ledger required")
    connection=sqlite3.connect(p.as_uri()+"?mode=ro",uri=True,timeout=3.0)
    try:
        connection.execute("PRAGMA query_only=ON")
        rows=connection.execute(
            "SELECT state,COUNT(*) FROM ingress_messages GROUP BY state"
        ).fetchall()
        summary=dict(rows)
        oldest=connection.execute(
            "SELECT MIN(first_seen) FROM ingress_messages WHERE state='RECEIVED'"
        ).fetchone()[0]
        return {
            "kind":"uditbot-readonly-ingress-audit-report-v1",
            "unreturned_signals":int(summary.get("RECEIVED",0)),
            "pipeline_returned_signals":int(summary.get("PIPELINE_RETURNED",0)),
            "oldest_unreturned_age_seconds":
                round(max(0.0,time.time()-float(oldest)),1)
                if oldest is not None else None,
            "manual_reconciliation_required":bool(summary.get("RECEIVED")),
            "automatic_signal_replay_allowed":False,
            "broker_fills_verified":False,
            "production_approved":False,
        }
    finally:
        connection.close()


if __name__=="__main__":
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("audit_db",help="Existing separate ingress-audit database")
    a=ap.parse_args()
    try:
        print(json.dumps(inspect(a.audit_db),indent=2))
    except (OSError,ValueError,sqlite3.Error) as error:
        ap.exit(2,f"Read-only ingress audit unavailable: {type(error).__name__}\n")
