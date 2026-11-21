#!/usr/bin/env python3
"""Offline read-only comparison: UditBot pending intents vs full broker books.

Never contacts Dhan/Telegram, never writes a trading database, never issues
or retries an order, never opens a gate, and never certifies protection.
Inputs must be operator-approved consistent copies, not a hot live journal.

Example (use only approved PRIVATE local snapshots):
 AUDITBOT_PHASE2_ACCOUNT_ID=... python3 -m tools.phase2_recovery_compare \
   --journal /private/journal-snapshot.sqlite3 \
   --books /private/broker-books.json
Output is aggregate counts and reason codes only, no account/order/security IDs.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import quote

from src.engine.recovery_evidence import summarize_recovery_assessments
from src.journal.db import NON_TERMINAL_INTENT_STATES


def read_only_comparison(
    journal_path: str,
    books_path: str,
    *,
    account_id: str,
    max_intents: int=5000,
) -> dict:
    if not isinstance(account_id,str) or not account_id.strip():
        raise ValueError("expected account ID required")
    if type(max_intents) is not int or not 1 <= max_intents <= 100000:
        raise ValueError("max_intents out of range")
    src=Path(journal_path).expanduser().resolve(strict=True)
    broker=Path(books_path).expanduser().resolve(strict=True)
    if src == broker:
        raise ValueError("broker snapshot must be separate from journal")
    if not src.is_file() or not broker.is_file():
        raise ValueError("existing broker/journal snapshots required")
    if broker.stat().st_size>20*1024*1024:
        raise ValueError("broker snapshot exceeds 20MB")
    if os.name == "posix" and stat.S_IMODE(broker.stat().st_mode)&0o077:
        raise PermissionError("broker snapshot must be private 0600")
    with broker.open("r",encoding="utf-8") as f:
        payload=json.load(f)
    if not isinstance(payload,dict):
        raise ValueError("broker snapshot must be an object")
    if payload.get("account_id")!=account_id.strip():
        raise ValueError("snapshot does not match requested account")
    books_complete=payload.get("books_complete") is True
    orders=payload.get("order_book")
    trades=payload.get("trade_book")
    uri="file:"+quote(str(src),safe="/")+"?mode=ro"
    conn=sqlite3.connect(uri,uri=True,timeout=4.0)
    try:
        conn.row_factory=sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        columns={r[1] for r in conn.execute("PRAGMA table_info(order_intents)")}
        required={
            "correlation_id","broker_order_id","account_id","security_id",
            "exchange_segment","product_type","quantity","state",
        }
        if not required.issubset(columns):
            raise ValueError("original journal intent schema unavailable")
        states=tuple(sorted(NON_TERMINAL_INTENT_STATES))
        query=",".join("?" for _ in states)
        count=conn.execute(
            f"SELECT COUNT(*) FROM order_intents WHERE state IN ({query})", states,
        ).fetchone()[0]
        if count>max_intents:
            raise ValueError("pending intent count exceeds bounded audit limit")
        intents=[dict(r) for r in conn.execute(
            f"SELECT correlation_id,broker_order_id,account_id,security_id,"
            f"exchange_segment,product_type,quantity,state FROM order_intents "
            f"WHERE state IN ({query}) ORDER BY created_at,correlation_id",states,
        )]
        if any(str(r["account_id"] or "").strip()!=account_id for r in intents):
            raise ValueError("journal has cross-account pending order intent")
    finally:
        conn.close()
    outcome=summarize_recovery_assessments(
        intents,account_id=account_id,order_book=orders,
        trade_book=trades,books_complete=books_complete,
    )
    return {
        "kind":"uditbot-phase2-journal-versus-broker-comparison-v1",
        "baseline_nonterminal_journal_intents":count,
        "strict_evidence_classifications":outcome["classifications"],
        "strict_evidence_reason_codes":outcome["reason_codes"],
        "partial_or_full_fills_needing_protection_review":outcome["protection_review_count"],
        "intents_needing_broker_review":outcome["unresolved_broker_review_count"],
        "all_broker_books_claimed_complete":books_complete,
        "safe_for_automatic_replay":False,
        "safe_to_open_entry_gate":False,
        "all_positions_protected":False,
        "production_approved":False,
        "limitations":"Operator-supplied books may be stale; this is a comparison, not independent authentication or canonical gate clearance.",
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal",required=True)
    p.add_argument("--books",required=True)
    p.add_argument("--max-intents",type=int,default=5000)
    args=p.parse_args()
    try:
        report=read_only_comparison(
            args.journal,args.books,
            account_id=os.environ.get("AUDITBOT_PHASE2_ACCOUNT_ID",""),
            max_intents=args.max_intents,
        )
        print(json.dumps(report,indent=2,sort_keys=True))
    except (OSError,ValueError,TypeError,sqlite3.Error,json.JSONDecodeError) as exc:
        p.exit(2,f"Phase 2 comparison unavailable: {type(exc).__name__}\n")


if __name__=="__main__":
    main()
