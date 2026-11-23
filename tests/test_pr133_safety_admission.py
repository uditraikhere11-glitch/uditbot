"""PR133 corrective admission and closed-parent evidence safety tests; no external network."""
import os
import ast
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

from src.journal.db import JournalDB
from tests import test_pr132_mixed_delivery_and_status as _fixture_bootstrap

ACCOUNT = 'SYNTHETIC'
SEC = '11915'


class AdmissionFence(unittest.TestCase):
    def setUp(self):
        self.ctx = tempfile.TemporaryDirectory()
        self.dir = self.ctx.name
        self.w = NS(_is_running=True, _heartbeat_monotonic=time.monotonic(),
                    _supervision_read_complete=True, _supervision_success_at=time.time())
        self.broker = NS(cfg=NS(data_dir=self.dir))
        self.stop = NS(is_stopped=lambda: False)
        source = Path(__file__).resolve().parents[1] / 'src/engine/reconciliation.py'
        tree = ast.parse(source.read_text())
        klass = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'StartupReconciliationGate')
        method = next(n for n in klass.body if isinstance(n, ast.FunctionDef) and n.name == 'is_accepting_entries')
        surrogate = ast.ClassDef(name='IsolatedGate', bases=[], keywords=[], body=[method], decorator_list=[])
        code = ast.fix_missing_locations(ast.Module(body=[surrogate], type_ignores=[]))
        ns = {'time': time, 'os': os}
        exec(compile(code, str(source), 'exec'), ns)
        self.g = ns['IsolatedGate']()
        self.g.broker, self.g.db, self.g.watcher, self.g.stop_manager = self.broker, NS(), self.w, self.stop
        self.g._accepting_entries = True
        self.g._require_live_watcher_liveness = True
    def tearDown(self): self.ctx.cleanup()

    def verdict(self):
        with patch('src.engine.owner_service_admission.owner_entries_permitted', return_value=True):
            return self.g.is_accepting_entries

    def test_verified_watcher_can_pass_liveness(self): self.assertTrue(self.verdict())
    def test_dead_watcher_blocks(self):
        self.w._is_running = False
        self.assertFalse(self.verdict())
    def test_missing_heartbeat_blocks(self):
        self.w._heartbeat_monotonic = None
        self.assertFalse(self.verdict())
    def test_stale_heartbeat_blocks(self):
        self.w._heartbeat_monotonic = time.monotonic() - 31
        self.assertFalse(self.verdict())
    def test_stale_supervision_blocks(self):
        self.w._supervision_success_at = time.time() - 301
        self.assertFalse(self.verdict())
    def test_missing_supervision_blocks(self):
        self.w._supervision_read_complete = False
        self.assertFalse(self.verdict())
    def test_future_or_malformed_proof_blocks(self):
        for val in (True, 'now', time.time() + 60, None):
            with self.subTest(value=val):
                self.w._supervision_success_at = val
                self.assertFalse(self.verdict())
    def test_old_gate_without_service_liveness_is_opt_in(self):
        self.g._require_live_watcher_liveness = False
        self.w._is_running = False
        self.assertTrue(self.verdict())


class ClosedForeverLedger(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = JournalDB(os.path.join(self.tmp.name, 'journal.sqlite'), account_id=ACCOUNT)
        self.now = 1795440000.0  # synthetic epoch only
        with self.db._get_conn() as conn:
            conn.execute('''INSERT INTO order_intents
                (correlation_id,account_id,symbol,security_id,exchange_segment,product_type,
                 order_type,price,quantity,broker_order_id,created_at,updated_at)
                VALUES ('corr-28',?,?,?,?,?,'LIMIT',21.0,28,'NEW28',?,?)''',
                (ACCOUNT,'YESBANK',SEC,'NSE_EQ','CNC',self.now-7200,self.now-7200))
            conn.execute('''INSERT INTO positions_lifecycle
                (parent_order_id,account_id,symbol,security_id,exchange_segment,product_type,
                 protection_route,position_state,entry_qty,buy_filled_qty,sell_filled_qty,
                 confirmed_open_qty,updated_at) VALUES
                ('NEW28',?,?,?,'NSE_EQ','CNC','FOREVER_OCO','POSITION_CLOSED',28,28,28,0,?)''',
                (ACCOUNT,'YESBANK',SEC,self.now))
            conn.execute('''INSERT INTO protection_orders
                (parent_order_id,forever_order_id,symbol,quantity,status,created_at)
                VALUES ('NEW28','FO28','YESBANK',28,'TRADED',?)''', (self.now-7200,))
            conn.execute('''INSERT INTO watcher_orders (order_id,is_terminal,record_json,updated_at)
                VALUES ('NEW28',1,'{"security_id":"11915","tp_order_id":"TP28","sl_order_id":"SL28"}',?)''', (self.now,))
            for order,eid,side,qty,price in [('NEW28','B16','BUY',16,21.1),('NEW28','B12','BUY',12,21.1),('TP28','S28','SELL',28,21.9)]:
                conn.execute('''INSERT INTO fills (broker_order_id,exchange_trade_id,symbol,
                    traded_price,traded_quantity,traded_at,parent_order_id,security_id,exchange_segment,
                    product_type,transaction_type,is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)''',
                    (order,eid,'YESBANK',price,qty,self.now-1800,'NEW28',SEC,'NSE_EQ','CNC',side))
    def tearDown(self): self.tmp.cleanup()
    def proof(self):
        return self.db.get_closed_forever_sibling_evidence(
            ACCOUNT,SEC,'NSE_EQ','CNC',open_parent_ids=('OLD1','OLD2'),now=self.now)
    def test_closed_child_fill_attested(self):
        p=self.proof()
        self.assertEqual(p['sell_qty'],28)
        self.assertEqual(len(p['trades']),3)
        self.assertEqual(p['forever_ids'],('FO28',))
        self.assertEqual(p['sell_ids'],('S28',))
        self.assertEqual(p['order_fills']['TP28'],('SELL',28))
    def test_unknown_child_sell_does_not_pass(self):
        with self.db._get_conn() as conn: conn.execute("UPDATE fills SET broker_order_id='EXTERNAL' WHERE exchange_trade_id='S28'")
        with self.assertRaises(ValueError): self.proof()
    def test_partial_child_sell_does_not_pass(self):
        with self.db._get_conn() as conn: conn.execute("UPDATE fills SET traded_quantity=27 WHERE exchange_trade_id='S28'")
        with self.assertRaises(ValueError): self.proof()
    def test_missing_group_does_not_pass(self):
        with self.db._get_conn() as conn: conn.execute("DELETE FROM protection_orders")
        with self.assertRaises(ValueError): self.proof()
    def test_no_closed_parent_returns_all_fields(self):
        with self.db._get_conn() as conn: conn.execute("UPDATE positions_lifecycle SET position_state='PROTECTED'")
        p=self.proof()
        self.assertEqual(p['today_sell_qty'],0)
    def test_existing_open_sibling_conflicts_does_not_pass(self):
        with self.assertRaises(ValueError):
            self.db.get_closed_forever_sibling_evidence(ACCOUNT,SEC,'NSE_EQ','CNC',open_parent_ids=('NEW28',),now=self.now)

if __name__=='__main__': unittest.main()

class EndToEndClosedOCO(unittest.TestCase):
    """Exercise actual owner-lot functions and real SQLite under a synthetic broker."""
    def setUp(self):
        self.t = ClosedForeverLedger()
        self.t.setUp()
        self.db = self.t.db
        self.now = self.t.now
        with self.db._get_conn() as conn:
            for n,stamp,price in [('OLD1',self.now-86400*20,15.1),('OLD2',self.now-86400*19,15.2)]:
                conn.execute('''INSERT INTO order_intents
                    (correlation_id,account_id,symbol,security_id,exchange_segment,product_type,
                     order_type,price,quantity,broker_order_id,created_at,updated_at)
                     VALUES (?,?,?,?,?,?,'LIMIT',?,1,?,?,?)''',
                     ('corr-'+n,ACCOUNT,'YESBANK',SEC,'NSE_EQ','CNC',price,n,stamp,stamp))
                conn.execute('''INSERT INTO positions_lifecycle
                    (parent_order_id,account_id,symbol,security_id,exchange_segment,product_type,
                     protection_route,position_state,entry_qty,buy_filled_qty,sell_filled_qty,
                     confirmed_open_qty,updated_at) VALUES
                     (?,?,?,?,'NSE_EQ','CNC','MANUAL','UNPROTECTED_MANUAL_RISK',1,1,0,1,?)''',
                     (n,ACCOUNT,'YESBANK',SEC,stamp))
                conn.execute('''INSERT INTO fills (broker_order_id,exchange_trade_id,symbol,
                     traded_price,traded_quantity,traded_at,parent_order_id,security_id,exchange_segment,
                     product_type,transaction_type,is_verified_trade) VALUES (?,?,'YESBANK',?,1,?,?,?,'NSE_EQ','CNC','BUY',1)''',
                     (n,'E'+n,price,stamp,n,SEC))
                conn.execute('''INSERT INTO manual_risk_acknowledgements
                     (parent_order_id,correlation_id,account_id,symbol,security_id,quantity,
                      acknowledged_by,reason,status,acknowledged_at)
                     VALUES (?,?,?,'YESBANK',?,1,'test','owner', 'UNPROTECTED_MANUAL_RISK',?)''',
                     (n,n,ACCOUNT,SEC,stamp))
        self.history=[]
        with self.db._get_conn() as conn:
            for row in conn.execute('SELECT broker_order_id,exchange_trade_id,transaction_type,traded_quantity,traded_price,traded_at FROM fills ORDER BY traded_at').fetchall():
                self.history.append(dict(dhanClientId=ACCOUNT,orderId=row['broker_order_id'],
                    securityId=SEC,isin='INE528G01035',exchangeSegment='NSE_EQ',productType='CNC',
                    transactionType=row['transaction_type'],exchangeTradeId=row['exchange_trade_id'],
                    tradedQuantity=row['traded_quantity'],tradedPrice=row['traded_price'],
                    exchangeOrderId='ex-'+row['broker_order_id'],exchangeTime='2026-11-23 14:00:00'))
        self.broker = NS(cfg=NS(dhan_client_id=ACCOUNT),
            get_authenticated_profile_id=lambda **kw: ACCOUNT,
            get_order_book=lambda: [dict(securityId=SEC,orderId=oid,
                orderStatus='TRADED',transactionType='BUY',filledQty=qty) for oid,qty in [('OLD1',1),('OLD2',1),('NEW28',28)]] +
                [dict(securityId=SEC,orderId='TP28',orderStatus='TRADED',transactionType='SELL',filledQty=28)],
            get_forever_orders=lambda: [dict(securityId=SEC,orderId='FO28',orderStatus='TRADED',transactionType='SELL')],
            get_historical_trades=lambda start,end:list(self.history),
            get_edis_owned_quantity=lambda *a: None)
        self.positions = [dict(securityId=SEC,exchangeSegment='NSE_EQ',productType='CNC',
                               netQty=0,sellQty=28,dayBuyQty=28,daySellQty=28)]
        self.holdings = [dict(securityId=SEC,isin='INE528G01035',dhanClientId=ACCOUNT,
                              totalQty=2,dpQty=2,t1Qty=0)]
    def tearDown(self): self.t.tearDown()

    @staticmethod
    def funcs():
        import json as jsonmod
        from datetime import datetime,timedelta
        from zoneinfo import ZoneInfo
        from src.engine.holding_quantity import observed_open_quantity
        from src.engine.mixed_cnc_evidence import verify_mixed_cnc_split
        src=Path(__file__).resolve().parents[1] / 'src/engine/owner_lot_evidence.py'
        tree=ast.parse(src.read_text())
        fns=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('verify_owner_history','prepare_owner_lots')]
        class ExecutionConflict(ValueError): event='OWNER_LOT_EXECUTION_CONFLICT'
        class DematReview(ValueError): event='OWNER_LOT_DEMAT_REVIEW'
        class BrokerValidator:
            @staticmethod
            def validate_historical_trade_record(row,**kw):
                if row.get('dhanClientId') != kw.get('expected_account_id') or not row.get('exchangeTradeId'):
                    return False,None,'INVALID'
                return True,row,''
        ns=dict(json=jsonmod,datetime=datetime,timedelta=timedelta,IST=ZoneInfo('Asia/Kolkata'),
                MAX_HISTORICAL_TRADE_WINDOW_DAYS=90,DhanClient=BrokerValidator,
                ExecutionConflict=ExecutionConflict,DematReview=DematReview,
                observed_open_quantity=observed_open_quantity,verify_mixed_cnc_split=verify_mixed_cnc_split)
        exec(compile(ast.fix_missing_locations(ast.Module(body=fns,type_ignores=[])),str(src),'exec'),ns)
        return ns
    def run_gate(self):
        fns=self.funcs()
        batch=dict(account=ACCOUNT,verified={},errors={})
        fns['prepare_owner_lots'](self.broker,self.db,batch,self.positions,
              [x for x in self.history if x['transactionType']=='SELL' or x['orderId']=='NEW28'],
              self.now,self.holdings,self.broker.get_forever_orders())
        return batch
    def test_full_target_exit_keeps_old_shares_verified(self):
        result=self.run_gate()
        self.assertEqual(result['errors'],{})
        self.assertEqual(set(result['verified']),{'OLD1','OLD2'})
    def test_external_sell_during_day_blocks(self):
        self.history.append(dict(self.history[-1],orderId='EXTERNAL',exchangeTradeId='EVIL',transactionType='SELL',tradedQuantity=1))
        result=self.run_gate()
        self.assertEqual(set(result['errors']),{'OLD1','OLD2'})
    def test_ambiguous_closed_child_fill_blocks(self):
        with self.db._get_conn() as conn: conn.execute("UPDATE fills SET traded_quantity=27 WHERE exchange_trade_id='S28'")
        result=self.run_gate()
        self.assertEqual(set(result['errors']),{'OLD1','OLD2'})

class PriorDayProtectedWithNewBuy(unittest.TestCase):
    def setUp(self):
        self.db = _fixture_bootstrap.FakeDB()
        self.fx = _fixture_bootstrap.BrokerFixture(self.db)
        self.db.is_manual_risk_acknowledged = lambda oid,cid,**kw: oid == 'OLD2'
        self.db.get_position_lifecycle = lambda oid: dict(
            account_id=ACCOUNT,security_id=SEC,exchange_segment='NSE_EQ',product_type='CNC',
            protection_route='FOREVER_OCO',position_state='PROTECTED',
            manual_exit_detected=False,orphaned_legs_detected=False,
            confirmed_open_qty=1,protected_qty=1,sell_filled_qty=0) if oid=='OLD1' else None
        self.db.get_protection_order_ids_for_parent = lambda oid: ['FO_OLD1'] if oid=='OLD1' else []
        self.fx.forever = [dict(orderId='FO_OLD1',dhanClientId=ACCOUNT,securityId=SEC,
            exchangeSegment='NSE_EQ',productType='CNC',transactionType='SELL',
            orderStatus='PENDING',quantity=1)]

    def call(self):
        from src.engine.mixed_cnc_evidence import verify_mixed_cnc_split
        import sys,types
        def check(matching_rows, **kw):
            rows=matching_rows
            valid = len(rows)==1 and rows[0].get('orderStatus')=='PENDING' and rows[0].get('quantity')==1
            return NS(is_verified=valid, protected_quantity=1 if valid else 0,
                      reason='broker active order absent or invalid')
        with patch.dict(sys.modules,{'src.broker.dhan_client': NS(verify_forever_oco_group=check)}):
            return verify_mixed_cnc_split(db=self.db,positions=self.fx.positions,
                holdings=self.fx.holdings,trade_book=self.fx.trades,account=ACCOUNT,
                security=SEC,segment='NSE_EQ',product='CNC',
                parent_quantities={r['parent_order_id']:r['confirmed_open_qty'] for r in self.db.rows},
                now=_fixture_bootstrap.stamp(23,15,10),forever=self.fx.forever)
    def test_prior_day_protected_mixed_today_passes_only_with_broker_proof(self):
        self.assertEqual(self.call().total_qty,30)
    def test_prior_day_protection_group_missing_blocks(self):
        self.fx.forever=[]
        with self.assertRaises(Exception): self.call()
    def test_prior_day_protection_terminal_blocks(self):
        self.fx.forever[0]['orderStatus']='TRADED'
        with self.assertRaises(Exception): self.call()
    def test_duplicate_protection_ids_blocks(self):
        self.db.get_protection_order_ids_for_parent=lambda oid:['FO_OLD1','FO_OLD1'] if oid=='OLD1' else []
        with self.assertRaises(Exception): self.call()
    def test_missing_protection_identity_blocks(self):
        self.fx.forever[0]['dhanClientId']='OTHER'
        with self.assertRaises(Exception): self.call()
    def test_lifecycle_only_but_no_live_order_blocks(self):
        self.fx.forever=[]
        with self.assertRaises(Exception): self.call()
