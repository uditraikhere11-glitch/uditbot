"""Standalone SQLite/execution-book regressions; no network or broker mutations."""
import contextlib
from datetime import datetime, timedelta
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# A real UditBot checkout supplies these imports. The tiny fallback only
# supports standalone testing of this changed-files bundle.
try:
    from src.engine.unified_exit_reconciliation import reconcile_verified_exits, ReconciliationEvidenceError
except ModuleNotFoundError as e:
    if e.name not in ("src.engine.holding_quantity", "src.broker", "src.broker.dhan_client"):
        raise
    import types
    m = types.ModuleType("src.engine.holding_quantity")
    def observed_open_quantity(positions, holdings, *, security_id, segment, product, account_id):
        target = [x for x in positions if str(x.get('securityId')) == security_id]
        if target:
            net = int(target[0]['netQty'])
            if net > 0:
                return net, 'VERIFIED_POSITIONS'
            if net < 0:
                return None, 'NEGATIVE_CNC_POSITION_REVIEW'
        matching = [x for x in holdings if str(x.get('securityId')) == security_id]
        if matching:
            return int(matching[0]['totalQty']), 'VERIFIED_SETTLED_HOLDINGS'
        if product not in ('CNC','MTF'):
            return 0, 'VERIFIED_POSITIONS'
        return None, 'SETTLED_HOLDING_NOT_REPORTED'
    def aggregate_holdings(rows, *, account_id=""):
        totals = {}
        for row in rows:
            sid = str(row.get("securityId") or "").strip()
            if sid:
                totals[sid] = int(row.get("totalQty") if row.get("totalQty") is not None else row.get("dpQty", 0))
        return totals
    m.aggregate_holdings = aggregate_holdings
    m.observed_open_quantity = observed_open_quantity
    sys.modules['src.engine.holding_quantity'] = m
    m2 = types.ModuleType('src.broker')
    m2.__path__ = []
    sys.modules['src.broker'] = m2
    m3 = types.ModuleType('src.broker.dhan_client')
    m3.MAX_HISTORICAL_TRADE_WINDOW_DAYS = 30
    sys.modules['src.broker.dhan_client'] = m3
    from src.engine.unified_exit_reconciliation import reconcile_verified_exits, ReconciliationEvidenceError

IST = ZoneInfo('Asia/Kolkata')
DAY = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)
B_OID, S_OID = 'BUY-111', '331261119150608'
ACCOUNT = 'TEST_ACCOUNT'
CONTRACT = ('5264', 'NSE_EQ', 'CNC')


def trade(oid, eid, side, qty, ts, price=417.75, account=ACCOUNT):
    return dict(orderId=oid, exchangeTradeId=eid, transactionType=side, tradedQuantity=qty,
                tradedPrice=price, exchangeTime=ts.strftime('%Y-%m-%d %H:%M:%S'),
                dhanClientId=account, securityId='5264', exchangeSegment='NSE_EQ',
                productType='CNC')


class DB:
    account_id = ACCOUNT
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'db.sqlite')
        with self._get_conn() as c:
            c.executescript('''
            CREATE TABLE fills(id INTEGER PRIMARY KEY, broker_order_id TEXT, exchange_trade_id TEXT UNIQUE,
                symbol TEXT, traded_price REAL, traded_quantity INT, traded_at REAL,
                transaction_type TEXT, security_id TEXT, exchange_segment TEXT, product_type TEXT,
                parent_order_id TEXT, is_verified_trade INTEGER);
            CREATE TABLE positions_lifecycle(parent_order_id TEXT PRIMARY KEY, account_id TEXT,
                symbol TEXT, security_id TEXT, exchange_segment TEXT, product_type TEXT,
                buy_filled_qty INT, sell_filled_qty INT, confirmed_open_qty INT, position_state TEXT);
            CREATE TABLE order_intents(broker_order_id TEXT, account_id TEXT, correlation_id TEXT,
                symbol TEXT, security_id TEXT, exchange_segment TEXT, product_type TEXT,
                quantity INT, created_at REAL, is_amo INT, protection_route TEXT, original_market_price REAL);
            CREATE TABLE system_events(id INTEGER PRIMARY KEY, event_type TEXT, description TEXT,
                payload TEXT, created_at REAL);
            CREATE TABLE exit_allocations(id INTEGER PRIMARY KEY, parent_order_id TEXT, sell_order_id TEXT,
                exchange_trade_id TEXT, allocated_qty INT, traded_price REAL, traded_at REAL,
                UNIQUE(parent_order_id,exchange_trade_id));
            CREATE TABLE protection_orders(parent_order_id TEXT, status TEXT);
            ''')
            c.execute('INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (B_OID,ACCOUNT,'corr-111','PDSL',*CONTRACT,3,DAY.timestamp(),0,'FOREVER_OCO',416.0))
            c.execute('INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)',
                (B_OID,ACCOUNT,'PDSL',*CONTRACT,3,0,3,'RECONCILIATION_REQUIRED'))
            c.execute('INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)',
                (B_OID,'B-1','PDSL',416.0,3,DAY.timestamp()+40,'BUY',*CONTRACT,B_OID,1))
            c.execute('INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)',
                (S_OID,'S-1','PDSL',417.75,1,DAY.timestamp()+2000,'SELL',*CONTRACT,S_OID,1))
    @contextlib.contextmanager
    def _get_conn(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        try:
            with c:
                yield c
        finally:
            c.close()
    def get_position_accounting(self, oid):
        with self._get_conn() as c:
            buys = c.execute("SELECT * FROM fills WHERE parent_order_id=? AND transaction_type='BUY'",(oid,)).fetchall()
            sells = c.execute("SELECT * FROM fills WHERE parent_order_id=? AND transaction_type='SELL'",(oid,)).fetchall()
            try:
                allocs = c.execute(
                    "SELECT a.exchange_trade_id, a.allocated_qty, a.traded_price FROM exit_allocations a "
                    "JOIN fills f ON f.exchange_trade_id=a.exchange_trade_id "
                    "WHERE a.parent_order_id=? AND f.is_verified_trade=1 AND UPPER(f.transaction_type)='SELL'",
                    (oid,),
                ).fetchall()
            except sqlite3.OperationalError:
                allocs = []
        b = sum(x['traded_quantity'] for x in buys)
        seen = {str(x['exchange_trade_id']) for x in sells}
        s = sum(x['traded_quantity'] for x in sells)
        s_val = sum(x['traded_price']*x['traded_quantity'] for x in sells)
        for a in allocs:
            if str(a['exchange_trade_id']) not in seen:
                seen.add(str(a['exchange_trade_id']))
                s += a['allocated_qty']
                s_val += a['traded_price']*a['allocated_qty']
        s_vwap = (s_val / s) if s else 0.0
        return dict(buy_filled_qty=b,sell_filled_qty=s,buy_vwap=416.0,
                    sell_vwap=s_vwap,
                    realized_pnl=round((s_vwap-416.0)*min(b,s),2) if s else 0.0,
                    confirmed_open_qty=max(0,b-s))
    def get_position_lifecycle(self, oid):
        with self._get_conn() as c:
            r=c.execute('SELECT * FROM positions_lifecycle WHERE parent_order_id=?',(oid,)).fetchone()
            return dict(r) if r else None
    def close(self):
        self.tmp.cleanup()


class Watcher:
    def __init__(self, db): self.db=db; self.calls=[]
    def reconcile_historical_order_fills(self, **kwargs):
        self.calls.append(kwargs)
        opened=kwargs['filled_qty']-kwargs['verified_closed_qty']
        state='POSITION_CLOSED' if opened==0 else 'UNPROTECTED'
        with self.db._get_conn() as c:
            c.execute('UPDATE positions_lifecycle SET sell_filled_qty=?,confirmed_open_qty=?,position_state=? WHERE parent_order_id=?',
                      (kwargs['verified_closed_qty'],opened,state,kwargs['order_id']))
        return dict(confirmed_position_open_qty=opened,position_state=state)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.db=DB(); self.w=Watcher(self.db)
        self.broker=Mock()
        self.broker.cfg.dhan_client_id=ACCOUNT
        self.books=dict(order_book=[
            dict(orderId=B_OID, securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',
                 orderStatus='TRADED',transactionType='BUY',filledQty=3,dhanClientId=ACCOUNT),
            dict(orderId=S_OID, securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',
                 orderStatus='TRADED',transactionType='SELL',filledQty=1,dhanClientId=ACCOUNT)],
            holdings=[],forever_orders=[],super_orders=[])
        self.bu=trade(B_OID,'B-1','BUY',3,DAY+timedelta(seconds=40),416)
        self.se=trade(S_OID,'S-1','SELL',1,DAY+timedelta(minutes=10,seconds=46))
        self.trades=[self.bu,self.se]
        self.positions=[dict(securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',netQty=2)]
    def tearDown(self): self.db.close()
    def read(self, method,label): return True, self.books[label], 'OK'
    def run_it(self):
        return reconcile_verified_exits(broker=self.broker,db=self.db,watcher=self.w,
                                        trade_book=self.trades,positions=self.positions,read_required=self.read)
    def full_exit(self):
        with self.db._get_conn() as c:
            c.execute('UPDATE fills SET traded_quantity=3 WHERE exchange_trade_id="S-1"')
        self.se['tradedQuantity']=3
        self.books['order_book'][1]['filledQty']=3
        self.positions=[dict(securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',netQty=0)]

    def test_partial_sell_is_durably_linked_without_declaring_closed(self):
        changed,_=self.run_it(); self.assertTrue(changed)
        self.assertEqual(self.db.get_position_accounting(B_OID)['confirmed_open_qty'],2)
        self.assertEqual(self.db.get_position_lifecycle(B_OID)['position_state'],'UNPROTECTED')
    def test_full_manual_exit_closes_real_buy_lifecycle(self):
        self.full_exit(); changed,_=self.run_it(); self.assertTrue(changed)
        self.assertEqual(self.db.get_position_lifecycle(B_OID)['position_state'],'POSITION_CLOSED')
        self.assertEqual(len(self.w.calls),1)
        self.assertEqual(self.w.calls[0]['verified_sell_vwap'],417.75)
        self.assertEqual(self.w.calls[0]['verified_realized_pnl'],5.25)
    def test_duplicate_restart_is_idempotent(self):
        self.full_exit(); self.run_it(); result=self.run_it()
        self.assertFalse(result[0])
        self.assertEqual(len(self.w.calls),1)
        with self.db._get_conn() as c:
            count=c.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        self.assertEqual(count,2)
    def test_missing_holdings_row_same_day_full_round_trip(self):
        self.full_exit(); self.positions=[]
        self.assertTrue(self.run_it()[0])
    def test_over_sell_rejected(self):
        with self.db._get_conn() as c:
            c.execute('UPDATE fills SET traded_quantity=4 WHERE exchange_trade_id="S-1"')
        self.se['tradedQuantity']=4
        self.books['order_book'][1]['filledQty']=4
        with self.assertRaisesRegex(ReconciliationEvidenceError,'exceeds'):
            self.run_it()
    def test_competing_parent_blocks(self):
        with self.db._get_conn() as c:
            c.execute('INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)',
                      ('BUY-222',ACCOUNT,'PDSL',*CONTRACT,1,0,1,'UNPROTECTED'))
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Ambiguous'):
            self.run_it()
    def test_wrong_account_blocks_without_link(self):
        self.se['dhanClientId']='OTHER'
        with self.assertRaises(ReconciliationEvidenceError): self.run_it()
        self.assertEqual(self.db.get_position_accounting(B_OID)['sell_filled_qty'],0)
    def test_forever_active_blocks_without_link(self):
        self.books['forever_orders']=[dict(securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',orderStatus='ACTIVE')]
        with self.assertRaisesRegex(ReconciliationEvidenceError,'protective'):
            self.run_it()
        self.assertEqual(self.db.get_position_accounting(B_OID)['sell_filled_qty'],0)
    def test_pending_order_blocks_without_link(self):
        self.books['order_book'][1]['orderStatus']='PENDING'
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Outstanding broker order'):
            self.run_it()
    def test_unreported_holdings_with_partial_and_no_position_blocks(self):
        self.positions=[]
        with self.assertRaisesRegex(ReconciliationEvidenceError,'exposure'):
            self.run_it()
    def test_mismatching_orderbook_filled_count_blocks(self):
        self.books['order_book'][1]['filledQty']=2
        with self.assertRaisesRegex(ReconciliationEvidenceError,'partial fill'):
            self.run_it()
    def test_broker_read_failure_blocks(self):
        def fail(m, l): return False,None,'429'
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Complete order_book'):
            reconcile_verified_exits(broker=self.broker,db=self.db,watcher=self.w,
                    trade_book=self.trades,positions=self.positions,read_required=fail)
    def test_broker_trade_conflict_blocks(self):
        self.se['tradedPrice']=402.0
        with self.assertRaisesRegex(ReconciliationEvidenceError,'price differs'):
            self.run_it()
    def test_sell_before_buy_blocks(self):
        self.se['exchangeTime']=(DAY-timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        with self.assertRaisesRegex(ReconciliationEvidenceError,'precedes'):
            self.run_it()
    def test_crash_after_atomic_link_before_watcher_sync_retries(self):
        class Fail(Watcher):
            def reconcile_historical_order_fills(self,**kw):
                raise RuntimeError('power loss')
        self.w=Fail(self.db)
        with self.assertRaisesRegex(RuntimeError,'power loss'): self.run_it()
        self.assertEqual(self.db.get_position_accounting(B_OID)['sell_filled_qty'],1)
        self.w=Watcher(self.db)
        changed,_=self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_lifecycle(B_OID)['confirmed_open_qty'],2)
    def test_older_verified_linked_sell_does_not_duplicate(self):
        self.full_exit(); self.run_it()
        self.assertFalse(self.run_it()[0])
    def test_long_intraday_full_buy_sell_net_zero(self):
        self.full_exit()
        with self.db._get_conn() as c:
            c.execute("UPDATE order_intents SET product_type='INTRADAY'")
            c.execute("UPDATE positions_lifecycle SET product_type='INTRADAY'")
            c.execute("UPDATE fills SET product_type='INTRADAY'")
        for entry in self.trades + self.books['order_book'] + self.positions:
            entry['productType'] = 'INTRADAY'
        self.assertTrue(self.run_it()[0])
        self.assertEqual(self.db.get_position_lifecycle(B_OID)['position_state'],'POSITION_CLOSED')
    def test_unrelated_contract_sell_is_not_attributed(self):
        with self.db._get_conn() as c:
            c.execute('UPDATE fills SET security_id="9999" WHERE exchange_trade_id="S-1"')
        self.assertFalse(self.run_it()[0])
        self.assertEqual(self.db.get_position_accounting(B_OID)['sell_filled_qty'],0)
    def test_traded_sell_not_in_journal_blocks(self):
        self.books['order_book'].append(dict(orderId='OTHER-SELL',securityId='5264',
                exchangeSegment='NSE_EQ',productType='CNC',transactionType='SELL',
                orderStatus='TRADED',filledQty=1))
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Unattributed broker SELL'):
            self.run_it()
    def test_extra_executed_buy_blocks(self):
        self.books['order_book'].append(dict(orderId='OTHER-BUY',securityId='5264',
                exchangeSegment='NSE_EQ',productType='CNC',transactionType='BUY',
                orderStatus='TRADED',filledQty=1))
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Competing broker BUY'):
            self.run_it()
    def test_duplicate_broker_order_id_fails_closed(self):
        self.books['order_book'].append(dict(self.books['order_book'][1]))
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Duplicate broker orderbook'):
            self.run_it()
    def test_cross_contract_trade_same_security_blocks(self):
        self.trades.append(dict(trade('FOREIGN','F-2','BUY',1,DAY+timedelta(minutes=5)),productType='INTRADAY'))
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Cross-contract trade'):
            self.run_it()
    def test_open_super_order_blocks(self):
        self.books['super_orders']=[dict(securityId='5264',exchangeSegment='NSE_EQ',productType='CNC',orderStatus='ACTIVE')]
        with self.assertRaisesRegex(ReconciliationEvidenceError,'protective'):
            self.run_it()
    def test_future_sell_after_original_buy_is_required(self):
        self.se['exchangeTime']=(DAY+timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')
        with self.assertRaisesRegex(ReconciliationEvidenceError,'precedes or overlaps'):
            self.run_it()
    def test_no_unlinked_sell_does_not_query_broker(self):
        with self.db._get_conn() as c:
            c.execute('DELETE FROM fills WHERE transaction_type="SELL"')
        def should_not_read(*args): raise AssertionError('unnecessary GET')
        result=reconcile_verified_exits(broker=self.broker,db=self.db,watcher=self.w,
                            trade_book=self.trades,positions=self.positions,read_required=should_not_read)
        self.assertFalse(result[0])
    def test_sell_order_cancelled_with_confirmed_partial_fill_can_link(self):
        self.books['order_book'][1]['orderStatus']='CANCELLED'
        self.assertTrue(self.run_it()[0])
    def test_ambiguous_watcher_protection_blocks(self):
        self.w.pending={B_OID:{'pending_oco':{'state':'UNKNOWN'}}}
        with self.assertRaisesRegex(ReconciliationEvidenceError,'Watcher has unresolved protection'):
            self.run_it()
    def test_rejected_local_protection_does_not_block_verified_manual_sell(self):
        self.w.pending={B_OID:{'pending_oco':{'state':'REJECTED'}}}
        self.full_exit()
        self.assertTrue(self.run_it()[0])
    def test_second_partial_sell_requires_correct_aggregate_book_evidence(self):
        self.run_it()
        with self.db._get_conn() as c:
            c.execute('INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)',
                      ('SELL-2','S-2','PDSL',418.0,2,DAY.timestamp()+3000,'SELL',*CONTRACT,'SELL-2',1))
        second=trade('SELL-2','S-2','SELL',2,DAY+timedelta(minutes=20),418.0)
        self.trades.append(second)
        self.books['order_book'].append(dict(orderId='SELL-2',securityId='5264',exchangeSegment='NSE_EQ',
                                             productType='CNC',orderStatus='TRADED',transactionType='SELL',filledQty=2))
        self.positions[0]['netQty']=0
        # Must NOT incorrectly match only the newest exit and ignore prior one.
        self.assertTrue(self.run_it()[0])
        self.assertEqual(self.db.get_position_lifecycle(B_OID)['confirmed_open_qty'],0)


    # --- UREC-001 through UREC-008 Independent Audit Regression Tests ---

    def _setup_historical_books(self, buy_days_ago):
        self.trades = []
        self.books["order_book"] = []
        self.books["holdings"] = [dict(securityId="5264", totalQty=2)]
        self.positions = []
        earlier = DAY - timedelta(days=buy_days_ago)
        self.se["exchangeTime"] = (DAY - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        with self.db._get_conn() as c:
            c.execute("UPDATE order_intents SET created_at=?", (earlier.timestamp(),))
            c.execute("UPDATE fills SET traded_at=? WHERE transaction_type='BUY'", ((earlier + timedelta(seconds=40)).timestamp(),))
        self.broker.get_historical_trades.return_value = [self.se]

    def test_urec001_verified_previous_day_sell_does_not_require_today_orderbook(self):
        self._setup_historical_books(2)
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 2)

    def test_urec002_older_manual_sell_absent_from_journal_discovered_via_history(self):
        self._setup_historical_books(2)
        with self.db._get_conn() as c:
            c.execute("DELETE FROM fills WHERE transaction_type='SELL'")
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertTrue(self.broker.get_historical_trades.called)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 2)

    def test_urec003_buy_older_than_31_days_paginates_bounded_history_windows(self):
        self._setup_historical_books(45)
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertGreaterEqual(self.broker.get_historical_trades.call_count, 2)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 2)

    def test_urec004_rejected_zero_fill_order_may_omit_filled_qty(self):
        self.full_exit()
        self.books["order_book"].append({
            "orderId": "REJECTED-99", "securityId": "5264",
            "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "transactionType": "SELL", "orderStatus": "REJECTED",
            "dhanClientId": ACCOUNT,
        })
        changed, _ = self.run_it()
        self.assertTrue(changed)

    def test_urec004_cancelled_order_missing_filled_qty_still_blocks(self):
        self.full_exit()
        self.books["order_book"].append({
            "orderId": "CANCELLED-99", "securityId": "5264",
            "exchangeSegment": "NSE_EQ", "productType": "CNC",
            "transactionType": "SELL", "orderStatus": "CANCELLED",
            "dhanClientId": ACCOUNT,
        })
        with self.assertRaisesRegex(ReconciliationEvidenceError, "Missing or nonintegral"):
            self.run_it()

    def test_same_day_sale_cannot_erase_reported_holdings(self):
        self.full_exit()
        self.books["holdings"] = [dict(securityId="5264", totalQty=4)]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "exposure"):
            self.run_it()

    def test_urec005_forever_oco_traded_leg_with_cancelled_sibling_and_verified_fill_succeeds(self):
        self.full_exit()
        self.books["forever_orders"] = [
            dict(orderId=S_OID, orderFlag="OCO", orderType="LIMIT", legName="TARGET_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="TRADED", quantity=3),
            dict(orderId=S_OID, orderFlag="OCO", orderType="LIMIT", legName="STOP_LOSS_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="CANCELLED", quantity=3),
        ]
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_lifecycle(B_OID)["position_state"], "POSITION_CLOSED")

    def test_urec005_forever_oco_traded_with_active_sibling_or_triggered_only_blocks(self):
        self.full_exit()
        self.books["forever_orders"] = [
            dict(orderId="OCO-1", orderType="OCO", legName="TARGET_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="TRADED", quantity=3),
            dict(orderId="OCO-1", orderType="OCO", legName="STOP_LOSS_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="ACTIVE", quantity=3),
        ]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "protective"):
            self.run_it()
        self.books["forever_orders"] = [
            dict(orderId="OCO-1", orderType="OCO", legName="TARGET_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="TRIGGERED", quantity=3),
            dict(orderId="OCO-1", orderType="OCO", legName="STOP_LOSS_LEG", securityId="5264",
                 exchangeSegment="NSE_EQ", productType="CNC", orderStatus="CANCELLED", quantity=3),
        ]
        with self.assertRaisesRegex(ReconciliationEvidenceError, "protective"):
            self.run_it()

    def test_urec007_signed_negative_cnc_day_position_on_delivery_sale_reconciles(self):
        # Prior-day settled BUY (3 shares in DP holdings), today manual SELL of 1 share ->
        # Dhan /positions reports netQty=-1, /holdings reports pre-debit totalQty=3 (3 + (-1) = 2).
        earlier = DAY - timedelta(days=2)
        with self.db._get_conn() as c:
            c.execute("UPDATE order_intents SET created_at=?", (earlier.timestamp(),))
        self.trades = [self.se]
        self.books["order_book"] = [self.books["order_book"][1]]
        self.positions = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=-1, dhanClientId=ACCOUNT)]
        self.books["holdings"] = [dict(securityId="5264", totalQty=3, dhanClientId=ACCOUNT)]
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 2)

    def test_urec008_multi_lot_full_liquidation_closes_both_parents_deterministically(self):
        with self.db._get_conn() as c:
            c.execute("INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-222", ACCOUNT, "corr-222", "PDSL", *CONTRACT, 2, DAY.timestamp(), 0, "FOREVER_OCO", 416.0))
            c.execute("INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-222", ACCOUNT, "PDSL", *CONTRACT, 2, 0, 2, "UNPROTECTED"))
            c.execute("INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-222", "B-2", "PDSL", 416.0, 2, DAY.timestamp() + 45, "BUY", *CONTRACT, "BUY-222", 1))
            c.execute("UPDATE fills SET traded_quantity=5 WHERE exchange_trade_id='S-1'")
        self.se["tradedQuantity"] = 5
        self.trades.append(trade("BUY-222", "B-2", "BUY", 2, DAY + timedelta(seconds=45), 416.0))
        self.books["order_book"].append(
            dict(orderId="BUY-222", securityId="5264", exchangeSegment="NSE_EQ", productType="CNC",
                 orderStatus="TRADED", transactionType="BUY", filledQty=2, dhanClientId=ACCOUNT)
        )
        self.books["order_book"][1]["filledQty"] = 5
        self.positions = [dict(securityId="5264", exchangeSegment="NSE_EQ", productType="CNC", netQty=0)]
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_lifecycle(B_OID)["position_state"], "POSITION_CLOSED")
        self.assertEqual(self.db.get_position_lifecycle("BUY-222")["position_state"], "POSITION_CLOSED")
        with self.db._get_conn() as c:
            alloc_sum = c.execute("SELECT SUM(allocated_qty) FROM exit_allocations WHERE exchange_trade_id='S-1'").fetchone()[0]
        self.assertEqual(alloc_sum, 5)
        self.assertEqual(self.db.get_position_accounting(B_OID)["sell_filled_qty"], 3)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 0)
        self.assertEqual(self.db.get_position_accounting("BUY-222")["sell_filled_qty"], 2)
        self.assertEqual(self.db.get_position_accounting("BUY-222")["confirmed_open_qty"], 0)

    def test_contract_scoped_historical_discovery_not_suppressed_by_unrelated_same_day_sell(self):
        # Contract A (5264) has an unlinked same-day SELL in `fills`.
        # Unrelated Contract B (9999) has a prior-day BUY + offline historical SELL absent from local `fills`.
        earlier = DAY - timedelta(days=3)
        with self.db._get_conn() as c:
            c.execute("INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", ACCOUNT, "corr-B1", "OTHER", "9999", "NSE_EQ", "CNC", 2, earlier.timestamp(), 0, "FOREVER_OCO", 100.0))
            c.execute("INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", ACCOUNT, "OTHER", "9999", "NSE_EQ", "CNC", 2, 0, 2, "UNPROTECTED"))
            c.execute("INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", "B-B1", "OTHER", 100.0, 2, earlier.timestamp() + 10, "BUY", "9999", "NSE_EQ", "CNC", "BUY-B1", 1))
        self.books["holdings"] = [dict(securityId="9999", totalQty=1)]
        hist_b = dict(
            orderId="SELL-B1", exchangeTradeId="S-B1", transactionType="SELL", tradedQuantity=1,
            tradedPrice=105.0, exchangeTime=(DAY - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            dhanClientId=ACCOUNT, securityId="9999", exchangeSegment="NSE_EQ", productType="CNC",
        )
        self.broker.get_historical_trades.return_value = [hist_b]
        changed, _ = self.run_it()
        self.assertTrue(changed)
        self.assertEqual(self.db.get_position_accounting(B_OID)["confirmed_open_qty"], 2)
        self.assertEqual(self.db.get_position_accounting("BUY-B1")["confirmed_open_qty"], 1)

    def test_contract_scoped_historical_discovery_dhan_read_failure_blocks_cleanly(self):
        earlier = DAY - timedelta(days=3)
        with self.db._get_conn() as c:
            c.execute("INSERT INTO order_intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", ACCOUNT, "corr-B1", "OTHER", "9999", "NSE_EQ", "CNC", 2, earlier.timestamp(), 0, "FOREVER_OCO", 100.0))
            c.execute("INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", ACCOUNT, "OTHER", "9999", "NSE_EQ", "CNC", 2, 0, 2, "UNPROTECTED"))
            c.execute("INSERT INTO fills VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("BUY-B1", "B-B1", "OTHER", 100.0, 2, earlier.timestamp() + 10, "BUY", "9999", "NSE_EQ", "CNC", "BUY-B1", 1))
        self.books["holdings"] = [dict(securityId="9999", totalQty=1)]
        self.broker.get_historical_trades.return_value = None  # simulated broker read failure
        with self.assertRaisesRegex(ReconciliationEvidenceError, "Historical trade window incomplete"):
            self.run_it()
        self.assertEqual(self.db.get_position_accounting(B_OID)["sell_filled_qty"], 0)

if __name__=='__main__': unittest.main()
