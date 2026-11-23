"""Offline characterization of post-OCO exits and production LIVE watcher admission.

Runs against the actual source function AST and a disposable SQLite broker journal.
No Dhan, Telegram, order mutation or production DB access.
"""
import ast
from contextlib import nullcontext
import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import types
import unittest
from unittest.mock import Mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")
NOW = datetime.datetime(2026, 11, 23, 16, 0, tzinfo=IST).timestamp()
OLD = datetime.datetime(2026, 11, 2, 11, 0, tzinfo=IST).timestamp()


def extract(path, functions, globals_):
    tree = ast.parse((ROOT / path).read_text())
    bodies = tree.body
    if path.endswith('/db.py') or path.endswith('/reconciliation.py'):
        bodies = next(n.body for n in tree.body if isinstance(n, ast.ClassDef) and n.name == ('JournalDB' if path.endswith('/db.py') else 'StartupReconciliationGate'))
    selected = [n for n in bodies if isinstance(n, ast.FunctionDef) and n.name in functions]
    assert len(selected) == len(functions), (path, functions, [n.name for n in selected])
    code = compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])), str(ROOT/path), 'exec')
    exec(code, globals_)
    return {name: globals_[name] for name in functions}


class FakeVerifier:
    @staticmethod
    def validate_historical_trade_record(row, **kwargs):
        acct = kwargs.get('expected_account_id')
        if row.get('dhanClientId') != acct or not row.get('exchangeTradeId'):
            return False, None, 'BAD_IDENTITY'
        if row.get('transactionType') not in ('BUY', 'SELL'):
            return False, None, 'BAD_SIDE'
        if (not isinstance(row.get('tradedQuantity'), int)
                or row['tradedQuantity'] <= 0 or row.get('tradedPrice', 0) <= 0):
            return False, None, 'BAD_QTY'
        return True, row, 'OK'


class ClosedExitFixture(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript('''
            CREATE TABLE positions_lifecycle (parent_order_id TEXT, account_id TEXT, security_id TEXT,
                exchange_segment TEXT, product_type TEXT, position_state TEXT, protection_route TEXT,
                confirmed_open_qty INTEGER, buy_filled_qty INTEGER, sell_filled_qty INTEGER,
                entry_qty INTEGER, manual_exit_detected INTEGER, orphaned_legs_detected INTEGER);
            CREATE TABLE order_intents (broker_order_id TEXT, account_id TEXT, security_id TEXT,
                exchange_segment TEXT, product_type TEXT, created_at REAL);
            CREATE TABLE fills (broker_order_id TEXT, parent_order_id TEXT, exchange_trade_id TEXT,
                security_id TEXT, exchange_segment TEXT, product_type TEXT, transaction_type TEXT,
                is_verified_trade INTEGER, traded_quantity INTEGER, traded_price REAL, traded_at REAL);
            CREATE TABLE exit_allocations (parent_order_id TEXT, exchange_trade_id TEXT, allocated_qty INTEGER);
            CREATE TABLE protection_orders (parent_order_id TEXT, forever_order_id TEXT);
            CREATE TABLE watcher_orders (order_id TEXT, record_json TEXT);
            CREATE TABLE broker_attested_historical_fills (broker_order_id TEXT, account_id TEXT, evidence_json TEXT);
        ''')
        self.db = types.SimpleNamespace(account_id='SYNTHETIC',_get_conn=lambda: nullcontext(self.conn),
            get_open_positions_lifecycle=lambda: [dict(parent_order_id=o,security_id='11915',
                exchange_segment='NSE_EQ',product_type='CNC',confirmed_open_qty=1) for o in ('OLD1','OLD2')],
            is_manual_risk_acknowledged=lambda oid,cid,**k:oid in ('OLD1','OLD2') and cid==oid and k['required_qty']==1,
            owner_lot_review_reason=lambda *a:None,
            record_system_event=Mock())
        meths=extract('src/journal/db.py',('get_closed_forever_sibling_evidence','owner_lot_basis'),
              dict(Tuple=tuple,Dict=dict,Any=object,json=json,datetime=datetime,IST=IST))
        for k,v in meths.items():setattr(self.db,k,types.MethodType(v,self.db))
        self.history=[]
        for oid,tid,qty,at,side,price in (
            ('OLD1','O1',1,OLD,'BUY',15.1), ('OLD2','O2',1,OLD+86400,'BUY',15.2),
            ('NEW28','B16',16,NOW-120,'BUY',21.1),('NEW28','B12',12,NOW-120,'BUY',21.1),
            ('EXIT28','S28',28,NOW-60,'SELL',22.2)):
            self.conn.execute('''INSERT INTO fills VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (oid, 'NEW28' if oid=='EXIT28' else oid,tid,'11915','NSE_EQ','CNC',side,1,qty,price,at))
            self.history.append(dict(dhanClientId='SYNTHETIC',securityId='11915',isin='TESTISIN',
                exchangeSegment='NSE_EQ',productType='CNC',orderId=oid,exchangeTradeId=tid,
                transactionType=side,tradedQuantity=qty,tradedPrice=price))
        for oid,at in [('OLD1',OLD),('OLD2',OLD+86400),('NEW28',NOW-150)]:
            self.conn.execute('INSERT INTO order_intents VALUES (?,?,?,?,?,?)',
                (oid,'SYNTHETIC','11915','NSE_EQ','CNC',at))
        self.conn.execute('INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('NEW28','SYNTHETIC','11915','NSE_EQ','CNC','POSITION_CLOSED','FOREVER_OCO',0,28,28,28,0,0))
        self.conn.execute('INSERT INTO protection_orders VALUES (?,?)',('NEW28','FOREVER28'))
        self.conn.execute('INSERT INTO watcher_orders VALUES (?,?)',('NEW28',json.dumps({
            'security_id':'11915','tp_order_id':'EXIT28'})))
        self.positions=[dict(securityId='11915',exchangeSegment='NSE_EQ',productType='CNC',
                             netQty=0,sellQty=28)]
        self.holdings=[dict(securityId='11915',isin='TESTISIN',totalQty=2,dpQty=2)]
        self.orders=[dict(orderId='OLD1',securityId='11915',transactionType='BUY',
                          orderStatus='TRADED',filledQty=1),
                     dict(orderId='OLD2',securityId='11915',transactionType='BUY',
                          orderStatus='TRADED',filledQty=1),
                     dict(orderId='NEW28',securityId='11915',transactionType='BUY',
                          orderStatus='TRADED',filledQty=28),
                     dict(orderId='EXIT28',securityId='11915',transactionType='SELL',
                          orderStatus='TRADED',filledQty=28)]
        self.forever=[dict(orderId='FOREVER28',securityId='11915',orderStatus='TRADED')]
        self.broker=types.SimpleNamespace(cfg=types.SimpleNamespace(dhan_client_id='SYNTHETIC'),
            get_authenticated_profile_id=lambda **k:'SYNTHETIC',
            get_order_book=lambda: self.orders,
            get_holdings=lambda: self.holdings,
            get_forever_orders=lambda: self.forever,
            get_historical_trades=lambda *a:self.history,
            get_edis_owned_quantity=lambda *a:None)
        # Actual source logic; broker trade schema is validated by a synthetic strict stub.
        from src.engine.holding_quantity import observed_open_quantity
        proof=extract('src/engine/owner_lot_evidence.py',
            ('verify_owner_history','prepare_owner_lots'),dict(
             datetime=datetime.datetime,timedelta=datetime.timedelta,json=json,IST=IST,
             DhanClient=FakeVerifier,MAX_HISTORICAL_TRADE_WINDOW_DAYS=30,
             observed_open_quantity=observed_open_quantity,
             verify_mixed_cnc_split=lambda **k:None,
             ExecutionConflict=type('ExecutionConflict',(ValueError,),{'event':'OWNER_LOT_EXECUTION_CONFLICT'}),
             DematReview=type('DematReview',(ValueError,),{'event':'OWNER_LOT_DEMAT_REVIEW'})))
        self.verify=proof['verify_owner_history']
        # prepare_owner_lots resolves verify_owner_history via its own module's globals.
        self.prepare=proof['prepare_owner_lots']

    def result(self):
        batch=dict(account='SYNTHETIC',verified={},errors={},share=False)
        self.prepare(self.broker,self.db,batch,self.positions,self.history[-3:],NOW,self.holdings,self.forever)
        return batch

    def test_full_verified_oco_exit_preserves_both_owner_lots(self):
        r=self.result();self.assertFalse(r['errors'],r['errors'])
        self.assertEqual(set(r['verified']),{'OLD1','OLD2'})
        self.assertEqual(sum(q for _,_,q in r['verified'].values()),2)

    def test_unknown_external_sell_blocks(self):
        self.history.append(dict(self.history[-1],orderId='EXTERNAL',exchangeTradeId='X1',tradedQuantity=1))
        self.assertTrue(self.result()['errors'])

    def test_wrong_child_leg_does_not_get_exemption(self):
        self.conn.execute('UPDATE watcher_orders SET record_json=?',
            (json.dumps({'security_id':'11915','tp_order_id':'WRONG'}),))
        self.assertTrue(self.result()['errors'])

    def test_missing_journal_sell_cannot_be_exempt(self):
        self.conn.execute("DELETE FROM fills WHERE transaction_type='SELL'")
        self.assertTrue(self.result()['errors'])

    def test_partial_oco_close_blocks(self):
        self.conn.execute('UPDATE positions_lifecycle SET sell_filled_qty=27')
        self.assertTrue(self.result()['errors'])

    def test_unverified_journal_sell_blocks(self):
        self.conn.execute("UPDATE fills SET is_verified_trade=0 WHERE transaction_type='SELL'")
        self.assertTrue(self.result()['errors'])

    def test_forever_still_active_blocks(self):
        self.forever[0]['orderStatus']='PENDING'
        self.assertTrue(self.result()['errors'])

    def test_historical_read_http429_blocks(self):
        self.broker.get_historical_trades=lambda *args: None
        self.assertTrue(self.result()['errors'])

    def test_broker_sell_quantity_mismatch_blocks(self):
        self.positions[0]['sellQty']=29
        self.assertTrue(self.result()['errors'])

    def test_cross_account_sell_blocks(self):
        self.history[-1]['dhanClientId']='OTHER'
        self.assertTrue(self.result()['errors'])

    def test_restart_next_day_old_lots_still_verified(self):
        global NOW
        old_now=NOW
        try:
            NOW=datetime.datetime(2026,11,24,11,tzinfo=IST).timestamp()
            self.positions[0]['sellQty']=0
            # An inactive order must not be re-credited as active protection.
            self.assertFalse(self.result()['errors'])
        finally:
            NOW=old_now


class WatcherAdmissionFence(unittest.TestCase):
    def setUp(self):
        props=extract('src/engine/reconciliation.py',('is_accepting_entries',),
                      dict(time=time,os=__import__('os')))
        self.prop=props['is_accepting_entries']
        self.watcher=types.SimpleNamespace(_is_running=True,_supervision_read_complete=True,
            _supervision_success_at=time.time()-2,_heartbeat_monotonic=time.monotonic()-2)
        self.gate=types.SimpleNamespace(_accepting_entries=True,
            _require_live_watcher_liveness=True,watcher=self.watcher,
            stop_manager=types.SimpleNamespace(is_stopped=lambda:False),
            broker=types.SimpleNamespace(cfg=types.SimpleNamespace(data_dir='/synthetic')),
            db=types.SimpleNamespace())
        import sys
        import types as t
        self.original=sys.modules.get('src.engine.owner_service_admission')
        sys.modules['src.engine.owner_service_admission']=t.SimpleNamespace(owner_entries_permitted=lambda *_:True)
        self.addCleanup(lambda:self._restore(sys))

    def _restore(self,sys):
        if self.original is None:sys.modules.pop('src.engine.owner_service_admission',None)
        else:sys.modules['src.engine.owner_service_admission']=self.original

    def allows(self):return self.prop.fget(self.gate)

    def test_fresh_live_watcher_admits(self):self.assertTrue(self.allows())
    def test_dead_watcher_denies(self):
        self.watcher._is_running=False;self.assertFalse(self.allows())
    def test_watcher_heartbeat_stale_denies(self):
        self.watcher._heartbeat_monotonic=time.monotonic()-31;self.assertFalse(self.allows())
    def test_completed_broker_read_stale_denies(self):
        self.watcher._supervision_success_at=time.time()-301;self.assertFalse(self.allows())
    def test_broker_read_unverified_denies(self):
        self.watcher._supervision_read_complete=False;self.assertFalse(self.allows())
    def test_owner_stop_still_denies(self):
        self.gate.stop_manager.is_stopped=lambda:True;self.assertFalse(self.allows())
    def test_gate_closed_still_denies(self):
        self.gate._accepting_entries=False;self.assertFalse(self.allows())
    def test_watcher_without_heartbeat_denies(self):
        del self.watcher._heartbeat_monotonic;self.assertFalse(self.allows())


if __name__=='__main__':unittest.main()
