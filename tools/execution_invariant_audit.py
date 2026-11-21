#!/usr/bin/env python3
"""Read-only UditBot SQLite consistency audit; no broker calls or mutations.

This is an anomaly detector, NOT proof that actual Dhan account exposure is
safe, that all fills were observed, or that protection is active. Run against
an operator-approved consistent journal snapshot for a strict forensic audit.
Returns anonymous counts only, not credentials or order identifiers.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sqlite3
from urllib.parse import quote


class SchemaUnavailable(ValueError):
    pass


def _require(conn, table, names):
    columns={row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if not columns or not set(names).issubset(columns):
        raise SchemaUnavailable(f"required journal schema unavailable: {table}")


def audit_journal(path):
    source=Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError("journal not found")
    uri="file:"+quote(str(source),safe="/")+"?mode=ro"
    conn=sqlite3.connect(uri,uri=True,timeout=3)
    try:
        conn.execute("PRAGMA query_only=ON")
        _require(conn,"positions_lifecycle",("parent_order_id","buy_filled_qty",
                                              "sell_filled_qty","confirmed_open_qty",
                                              "position_state"))
        _require(conn,"fills",("exchange_trade_id","traded_quantity","is_verified_trade"))
        _require(conn,"exit_allocations",("exchange_trade_id","allocated_qty"))
        _require(conn,"order_intents",("correlation_id","state"))
        _require(conn,"protection_attempts",("state",))

        rows=conn.execute("SELECT buy_filled_qty,sell_filled_qty,confirmed_open_qty,"
                          "position_state FROM positions_lifecycle").fetchall()
        bad_arithmetic=0
        negative_quantity=0
        manual_risk=0
        for buys,sells,opened,state in rows:
            try:
                if any(type(v) is not int for v in (buys,sells,opened)):
                    bad_arithmetic+=1
                    continue
                if min(buys,sells,opened)<0 or sells>buys:
                    negative_quantity+=1
                    continue
                if opened!=buys-sells:
                    bad_arithmetic+=1
                if state=="UNPROTECTED_MANUAL_RISK":
                    manual_risk+=1
            except (TypeError,ValueError):
                bad_arithmetic+=1

        overalloc=0
        unknown_allocations=0
        allocations=conn.execute(
            "SELECT a.exchange_trade_id,SUM(a.allocated_qty),"
            "MAX(f.traded_quantity),MAX(f.is_verified_trade),COUNT(f.exchange_trade_id) "
            "FROM exit_allocations a "
            "LEFT JOIN fills f ON f.exchange_trade_id=a.exchange_trade_id "
            "GROUP BY a.exchange_trade_id"
        ).fetchall()
        for eid,allocated,traded,verified,matches in allocations:
            if type(allocated) is not int or allocated<=0:
                overalloc+=1
            elif matches==0 or type(traded) is not int or verified!=1:
                unknown_allocations+=1
            elif allocated>traded:
                overalloc+=1

        repeated_fills=conn.execute(
            "SELECT COUNT(*) FROM (SELECT exchange_trade_id FROM fills "
            "WHERE exchange_trade_id IS NOT NULL AND exchange_trade_id!='' "
            "GROUP BY exchange_trade_id HAVING COUNT(*)>1)"
        ).fetchone()[0]
        unresolved_intents=conn.execute(
            "SELECT COUNT(*) FROM order_intents WHERE state IN "
            "('INTENT_CREATED','SUBMITTING','UNKNOWN','CANCEL_PENDING',"
            "'CANCEL_REQUESTED','ACKED_UNVERIFIED')"
        ).fetchone()[0]
        unresolved_protection=conn.execute(
            "SELECT COUNT(*) FROM protection_attempts WHERE state IN "
            "('SUBMITTING','UNKNOWN','ACKED_UNVERIFIED')"
        ).fetchone()[0]
        hard=bad_arithmetic+negative_quantity+overalloc+repeated_fills
        return {
            "kind":"uditbot-readonly-invariant-audit-v1",
            "position_rows":len(rows),
            "position_arithmetic_mismatch":bad_arithmetic,
            "position_quantity_invalid":negative_quantity,
            "exit_overallocated":overalloc,
            "duplicated_exchange_trade_ids":repeated_fills,
            "hard_inconsistency_count":hard,
            "unknown_exit_allocation_count":unknown_allocations,
            "unresolved_intent_count":unresolved_intents,
            "unresolved_protection_attempt_count":unresolved_protection,
            "acknowledged_manual_risk_lot_count":manual_risk,
            "production_approved":False,
            "limitations":"Local journal checks only. Broker books, account holdings, active OCO/Super protection and missing fills must be verified separately.",
        }
    finally:
        conn.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("db")
    args=p.parse_args()
    try:
        print(json.dumps(audit_journal(args.db),indent=2))
    except (OSError,ValueError,sqlite3.Error) as exc:
        p.exit(2,f"Invariant audit unavailable: {type(exc).__name__}: {exc}\n")


if __name__=="__main__":
    main()
