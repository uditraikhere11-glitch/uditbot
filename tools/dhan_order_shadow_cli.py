"""Explicit operator-run, read-only Dhan order stream observer.

This CLI never runs under UditBot's production runner. Requires an explicit
network-opt-in switch, independent shadow database, and current access token
in environment (never in process args). It does not call REST or mutate any
live orders. Only use after an independently approved operational review.

Optional dependency: python -m pip install -r requirements-order-shadow.txt
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os

from src.engine.dhan_order_observer import (
    DhanShadowWebSocket, JournalOwnership, ShadowOrderObserver,
)
from src.engine.shadow_event_ledger import ShadowEventLedger


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal",required=True,help="Existing trading journal, READ ONLY")
    parser.add_argument("--shadow-db",required=True,
                        help="Separate private path ending .shadow.sqlite3")
    parser.add_argument("--max-messages",type=int,default=1000)
    parser.add_argument("--explicit-readonly-network-opt-in",action="store_true")
    args=parser.parse_args()
    if not args.explicit_readonly_network_opt_in:
        parser.error("network opt-in is required; no connection attempted")
    account=os.environ.get("DHAN_CLIENT_ID","").strip()
    token=os.environ.get("DHAN_ACCESS_TOKEN","").strip()
    if not account or not token:
        parser.error("set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN securely in environment")
    if os.path.realpath(args.journal)==os.path.realpath(args.shadow_db):
        parser.error("shadow journal must differ from trading journal")
    try:
        owner=JournalOwnership(args.journal)
        with ShadowEventLedger(args.shadow_db,account_id=account) as ledger:
            observer=ShadowOrderObserver(account,owner,shadow_ledger=ledger)
            report=asyncio.run(DhanShadowWebSocket(observer).run_once(
                access_token=token,network_opt_in=True,max_messages=args.max_messages,
            ))
            print(json.dumps(report,indent=2))
    except Exception as exc:
        # Prevent secrets, original broker payloads, and account IDs in output.
        parser.exit(2,f"Read-only shadow observer unavailable: {type(exc).__name__}\n")


if __name__=="__main__":
    main()
