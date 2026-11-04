#!/usr/bin/env python3
"""GET-only DhanHQ V2 order read probe. Does NOT place/modify/cancel orders.

Examples:
  python3 auditbot_dhan_readonly_order_probe.py
  python3 auditbot_dhan_readonly_order_probe.py --order-id 112xxxxxxxxx

Needs the existing ~/.dhan_token (or DHAN_TOKEN_FILE path). Never prints the token.
Read-only observations are not broker execution/protection certification.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.request

BASE = 'https://api.dhan.co/v2'


def get_json(path, token):
    assert path.startswith('/') and '?' not in path and '..' not in path
    request = urllib.request.Request(
        BASE + path,
        headers={'access-token': token, 'Accept': 'application/json'},
        method='GET',
    )
    try:
        with urllib.request.urlopen(request, timeout=6) as response:
            return json.loads(response.read(2_000_000).decode('utf-8')), None
    except urllib.error.HTTPError as exc:
        return None, f'HTTP {exc.code} (response details suppressed)'
    except (urllib.error.URLError, TimeoutError, ValueError, UnicodeDecodeError) as exc:
        return None, f'{type(exc).__name__} (details suppressed)'


def number(v):
    if v is None:
        return '-'
    try:
        return f'{float(v):,.2f}'
    except (ValueError, TypeError):
        return '-'


def display_order(row):
    if not isinstance(row, dict):
        return
    oid = str(row.get('orderId') or '')
    short = ('…' + oid[-4:]) if oid else 'unknown'
    print(f"Order {short} | {str(row.get('tradingSymbol') or 'unknown')[:40]} | "
          f"{row.get('transactionType', '?')} {row.get('orderStatus', '?')} | "
          f"Filled {row.get('filledQty', '?')}/{row.get('quantity', '?')} | "
          f"Average ₹{number(row.get('averageTradedPrice'))} | "
          f"Limit ₹{number(row.get('price'))}")
    if row.get('omsErrorDescription'):
        print('  Broker has an error description; inspect privately in Dhan, not public logs.')


def main():
    p = argparse.ArgumentParser(description='Read-only Dhan order/trade status probe')
    p.add_argument('--order-id', help='Exact Dhan order id to inspect. Never paste the token.')
    p.add_argument('--max-items', type=int, default=5, help='Show up to 1-20 recent orders')
    args = p.parse_args()
    if args.order_id and not re.fullmatch(r'[0-9]{4,30}', args.order_id):
        p.error('--order-id must be a numeric Dhan order id (4 to 30 digits)')
    if not 1 <= args.max_items <= 20:
        p.error('--max-items must be 1-20')

    path = Path(os.environ.get('DHAN_TOKEN_FILE') or '~/.dhan_token').expanduser()
    try:
        token = path.read_text(encoding='utf-8').strip()
    except OSError:
        print('Dhan token file unavailable. No API calls made.', file=sys.stderr)
        return 2
    if not token:
        print('Dhan token file is empty. No API calls made.', file=sys.stderr)
        return 2

    if args.order_id:
        order, err = get_json('/orders/' + args.order_id, token)
        print('GET /v2/orders/{order-id}:', 'OK' if err is None else err)
        if isinstance(order, dict) and order.get('orderId'):
            display_order(order)
        trades, err2 = get_json('/trades/' + args.order_id, token)
        print('GET /v2/trades/{order-id}:', 'OK' if err2 is None else err2)
        rows = trades if isinstance(trades, list) else ([trades] if isinstance(trades, dict) and trades.get('tradedQuantity') is not None else [])
        qty = 0
        value = 0.0
        for t in rows:
            if (not isinstance(t, dict)) or str(t.get('orderId', '')) != args.order_id:
                continue
            try:
                q = int(t.get('tradedQuantity', 0))
                price = float(t.get('tradedPrice', 0))
                if q > 0 and price > 0:
                    qty += q
                    value += q * price
            except (TypeError, ValueError):
                pass
        print(f'Verified matching trades: quantity={qty}; VWAP=₹{number(value / qty) if qty else "-"}')
        return 0 if err is None and err2 is None else 1

    orders, err = get_json('/orders', token)
    print('GET /v2/orders:', 'OK' if err is None else err)
    if not isinstance(orders, list):
        print('Cannot display order list; broker response is not a list.')
        return 1
    print(f'Order count for the current trading day: {len(orders)}')
    for row in orders[-args.max_items:]:
        display_order(row)
    print('This script only checks read access; it does NOT verify that SL/target legs remain active.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
