"""Phase 2C isolated ledger, historical corroboration and fail-closed regressions."""
from dataclasses import replace
from datetime import datetime, date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import sqlite3
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo
from src.broker.broker_snapshot import (NS, LIVE_BOOKS, BookEvidence, BookStatus, BrokerSnapshot,
    ContractKey, Generation, HistoryCoverage, Purpose, SessionContext, SessionPhase, account_key, payload_digest)
from src.broker.dhan_snapshot_source import _journal_digest
from src.engine.broker_ledger_reconciler import BrokerLedgerReconciler, PolicyContext, VerdictStatus
from src.engine.broker_session import classify_broker_session, BrokerSessionState, VerifiedCalendar
from src.engine.journal_view import read_journal_view
from src.engine.signed_delivery_live_evidence import (ClosedHistoricalBuyMatcher, closed_order_rollups,
    verify_closed_triggered_group)
from src.engine.watcher_risk_classification import TRANSIENT_FIELDS,DURABLE_RISK_FIELDS,BUSINESS_FACT_FIELDS,legacy_risks
from src.journal.db import JournalDB

A='9000000001'; C=ContractKey(A,'11915','NSE_EQ','CNC'); IST=ZoneInfo('Asia/Kolkata')
DAY=date(2026,11,26); WALL=int(datetime(2026,11,26,11,tzinfo=IST).timestamp()); MONO=100*NS
GEN=Generation(account_key(A),'offline-boot',1,2,3,4,5)
IDENT=dict(dhanClientId=A,securityId=C.security_id,exchangeSegment=C.exchange_segment,productType=C.product_type)
POLICY=PolicyContext('LIVE',Purpose.ENTRY,'RUNNING','RUNNING',False,GEN.owner_epoch,False,False,False,
    True,(),(),True,True,True,True)


def book(key,payload):
    return BookEvidence(key,'/v2/'+key,{},BookStatus.OK_RECORDS if payload else BookStatus.OK_EMPTY,200,None,None,True,
                        WALL*NS, WALL*NS, MONO, MONO,A,GEN.token_epoch,payload,payload_digest(payload),True)


class LedgerShadowTests(unittest.TestCase):
    def setUp(self):
        temp=TemporaryDirectory(); self.addCleanup(temp.cleanup);self.path=Path(temp.name)/'offline.db';self.db=JournalDB(str(self.path),account_id=A)
    def view(self): return read_journal_view(self.path,account_id=A,journal_epoch=GEN.journal_epoch,read_utc_ns=WALL*NS)
    def snapshot(self,view,**books):
        fields={k:book(k,books.get(k,[])) for k in LIVE_BOOKS}
        return BrokerSnapshot('shadow-snapshot',GEN,A,A,True,SessionContext(SessionPhase.SETTLED,DAY,WALL*NS,'a'*64,True),
            WALL*NS,WALL*NS,MONO,MONO,**fields,historical_trades=(),order_status={},trades_for_order={},profile=book('profile',{'dhanClientId':A}),
            funds=None,edis=None,history_coverage=(),complete=True,invalid_reasons=(),producer_pid=1,producer_starttime=2,
            journal_digest=view.digest_sha256,profile_valid_until_mono_ns=MONO+30*NS)
    def verdict(self,view,snapshot,policy=POLICY,clock=MONO):
        with patch('sqlite3.connect',side_effect=AssertionError('No SQL inside verifier')), patch('requests.Session.request',side_effect=AssertionError('No network')):
            return BrokerLedgerReconciler().reconcile_security(snapshot,view,C,policy,clock)
    def test_read_only_view_exact_digest_and_no_writes(self):
        v=self.view();self.assertTrue(v.read_complete);self.assertEqual(v.digest_sha256,_journal_digest(self.path))
        with self.db._get_conn() as c: before=c.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        verdict=self.verdict(v,self.snapshot(v)); self.assertTrue(verdict.entry_eligible,verdict.reason_codes)
        self.assertEqual(verdict.ledger_open_qty,0);self.assertFalse(verdict.shutdown_eligible)
        with self.db._get_conn() as c: self.assertEqual(c.execute('SELECT COUNT(*) FROM fills').fetchone()[0],before)
    def test_missing_journal_never_empty_success(self):
        v=read_journal_view(self.path.with_name('absent'),account_id=A,journal_epoch=4,read_utc_ns=WALL*NS)
        self.assertFalse(v.read_complete);self.assertFalse(self.path.with_name('absent').exists());res=self.verdict(v,self.snapshot(v));self.assertFalse(res.entry_eligible);self.assertIsNone(res.broker_open_qty)
    def test_failed_books_or_identity_never_authorize(self):
        v=self.view();s=self.snapshot(v);bad=replace(s.positions,status=BookStatus.HTTP_FAILURE,complete=False,payload=None,payload_sha256=None,http_status=500)
        for snap in (replace(s,complete=False,positions=bad),replace(s,complete=False,profile_verified=False,profile_account_id=None)):
            res=self.verdict(v,snap);self.assertFalse(res.entry_eligible);self.assertIsNone(res.broker_open_qty)
    def test_stale_and_changed_journal_block(self):
        v=self.view();s=self.snapshot(v);self.assertFalse(self.verdict(v,s,clock=MONO+11*NS).entry_eligible)
        with self.db._get_conn() as conn: conn.execute("INSERT INTO system_events(event_type,description) VALUES ('OFFLINE','new event')")
        res=self.verdict(self.view(),s);self.assertFalse(res.entry_eligible);self.assertIn('JOURNAL_READBACK_UNVERIFIED',res.reason_codes)
    def test_nonzero_broker_exposure_without_journal_blocks(self):
        v=self.view();s=self.snapshot(v,positions=[dict(IDENT,netQty=1)]);res=self.verdict(v,s)
        self.assertEqual(res.broker_open_qty,1);self.assertFalse(res.entry_eligible);self.assertFalse(res.ledger_verified)
    def test_mutation_owner_and_shutdown_fences(self):
        v=self.view();s=self.snapshot(v)
        for policy in (replace(POLICY,unknown_mutation_ids=('unknown',)),replace(POLICY,owner_intent_epoch=2),replace(POLICY,emergency_stop_active=True)):
            self.assertFalse(self.verdict(v,s,policy).entry_eligible)
        stop=replace(POLICY,purpose=Purpose.SHUTDOWN,owner_desired_state='STOPPED',entry_fence_active=True)
        self.assertFalse(self.verdict(v,s,stop).shutdown_eligible)
        self.assertTrue(self.verdict(v,s,replace(stop,min_capture_mono_ns=MONO)).shutdown_eligible)
    def test_durable_risk_persists_despite_failed_read(self):
        with self.db._get_conn() as conn:
            conn.execute('INSERT INTO watcher_orders VALUES (?,?,?,?)',('P',0,json.dumps(dict(security_id='11915',segment='NSE_EQ',product_type='CNC',owner_readback_failed=True,unreadable_broker_response=True)),WALL))
        v=self.view();s=replace(self.snapshot(v),complete=False);res=self.verdict(v,s)
        self.assertEqual(res.status,VerdictStatus.DURABLE_UNRESOLVED);self.assertIn('LEGACY_UNCLASSIFIED_RISK:owner_readback_failed',res.reason_codes)
        self.assertNotIn('LEGACY_UNCLASSIFIED_RISK:unreadable_broker_response',res.reason_codes)
    def test_empty_account_and_unknown_broker_contract(self):
        v=self.view();s=self.snapshot(v);verifier=BrokerLedgerReconciler()
        self.assertTrue(verifier.reconcile_account(s,v,POLICY,MONO).entry_eligible)
        other=dict(IDENT,securityId='999',netQty=2)
        result=verifier.reconcile_account(self.snapshot(v,positions=[other]),v,POLICY,MONO)
        self.assertFalse(result.entry_eligible)
        self.assertIn('BROKER_LEDGER_QUANTITY_MISMATCH',result.reason_codes)
        unknown=verifier.reconcile_account(self.snapshot(v,positions=[{'netQty':2}]),v,POLICY,MONO)
        self.assertFalse(unknown.entry_eligible)
        self.assertIn('ACCOUNT_CONTRACT_UNVERIFIED',unknown.reason_codes)
    def test_damaged_empty_schema_is_incomplete(self):
        with self.db._get_conn() as conn:
            conn.execute('DROP TABLE orders')
            conn.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
        self.assertFalse(self.view().read_complete)
    def test_filled_order_without_parent_blocks(self):
        self.db.record_order('CORR','LIVE','CNC','NSE_EQ','FAKE','11915','LIMIT',100,1,status='TRADED',broker_order_id='O')
        with self.db._get_conn() as conn:
            conn.execute("UPDATE orders SET filled_quantity=1 WHERE broker_order_id='O'")
        v=self.view();res=self.verdict(v,self.snapshot(v))
        self.assertFalse(res.entry_eligible);self.assertIn('JOURNAL_ORDER_WITHOUT_LIFECYCLE',res.reason_codes)
    def test_retained_acknowledged_two_lots_still_physically_unprotected(self):
        day=DAY-timedelta(days=1); at=WALL-86400; history=[]
        for parent in ('RETAINED-A','RETAINED-B'):
            self.db.record_order_intent(correlation_id=parent,account_id=A,symbol='FAKE',security_id='11915',
                exchange_segment='NSE_EQ',product_type='CNC',order_type='LIMIT',price=100,quantity=1,protection_route='FOREVER_OCO')
            self.db.update_order_intent(parent,'TRADED',broker_order_id=parent)
            with self.db._get_conn() as conn:
                conn.execute('UPDATE order_intents SET created_at=?, updated_at=? WHERE correlation_id=?',(at,at,parent))
            self.db.record_fill(parent,'EXEC-'+parent,'FAKE',100,1,traded_at=at,transaction_type='BUY',security_id='11915',
                exchange_segment='NSE_EQ',product_type='CNC',parent_order_id=parent,is_verified_trade=True)
            self.db.upsert_position_lifecycle(parent_order_id=parent,account_id=A,symbol='FAKE',security_id='11915',
                exchange_segment='NSE_EQ',product_type='CNC',protection_route='FOREVER_OCO',position_state='UNPROTECTED_MANUAL_RISK',
                entry_qty=1,buy_filled_qty=1,buy_vwap=100,protected_qty=0,confirmed_open_qty=1)
            self.db.acknowledge_manual_risk_holding(parent_order_id=parent,correlation_id=parent,account_id=A,symbol='FAKE',security_id='11915',
                quantity=1,acknowledged_by='offline owner',reason='retain')
            history.append(dict(IDENT,orderId=parent,exchangeTradeId='EXEC-'+parent,exchangeOrderId='EXCHANGE-'+parent,
                transactionType='BUY',tradedQuantity=1,tradedPrice=100,exchangeTime=datetime.fromtimestamp(at,IST).isoformat()))
        v=self.view();s=self.snapshot(v,holdings=[dict(securityId='11915',dhanClientId=A,isin='OFFLINE-ISIN',totalQty=2,dpQty=2,t1Qty=0)])
        pages=[]
        for i,payload in enumerate((history,[])):
            key=f'history:{day}:{day}:{i}'
            pages.append(replace(book(key,payload),endpoint=f'/v2/trades/{day}/{day}/{i}',request_params={'from_date':str(day),'to_date':str(day),'page':i}))
        s=replace(s,historical_trades=tuple(pages),history_coverage=(HistoryCoverage(day,day,tuple(p.book_key for p in pages),True,True,MONO,True),))
        result=self.verdict(v,s)
        self.assertTrue(result.entry_eligible,result.reason_codes)
        self.assertEqual((result.broker_open_qty,result.ledger_open_qty,result.verified_protected_qty,result.manual_ack_qty,result.unprotected_qty),(2,2,0,2,2))
        self.assertEqual([p.open_qty for p in result.parents],[1,1])
        self.assertTrue(BrokerLedgerReconciler().reconcile_account(s,v,POLICY,MONO).entry_eligible)
        with self.db._get_conn() as conn:
            conn.execute("DELETE FROM manual_risk_acknowledgements WHERE parent_order_id='RETAINED-B'")
        v=self.view();result=self.verdict(v,replace(s,journal_digest=v.digest_sha256))
        self.assertFalse(result.entry_eligible);self.assertEqual(result.unprotected_qty,2);self.assertEqual(result.manual_ack_qty,1)

    def test_protected_fno_shadow_and_missing_leg_block(self):
        parent='FNO-PARENT'; contract=ContractKey(A,'54321','NSE_FNO','INTRADAY')
        ident=dict(dhanClientId=A,securityId='54321',exchangeSegment='NSE_FNO',productType='INTRADAY')
        self.db.record_order_intent(correlation_id=parent,account_id=A,symbol='FAKE-FNO',security_id='54321',
            exchange_segment='NSE_FNO',product_type='INTRADAY',order_type='LIMIT',price=100,quantity=25,protection_route='SUPER_ORDER')
        self.db.update_order_intent(parent,'TRADED',broker_order_id=parent)
        with self.db._get_conn() as conn:
            conn.execute('UPDATE order_intents SET created_at=?, updated_at=? WHERE correlation_id=?',(WALL-60,WALL-60,parent))
        self.db.record_fill(parent,'EXEC-FNO-1','FAKE-FNO',100,25,traded_at=WALL-60,transaction_type='BUY',security_id='54321',
            exchange_segment='NSE_FNO',product_type='INTRADAY',parent_order_id=parent,is_verified_trade=True)
        self.db.upsert_position_lifecycle(parent_order_id=parent,account_id=A,symbol='FAKE-FNO',security_id='54321',
            exchange_segment='NSE_FNO',product_type='INTRADAY',protection_route='SUPER_ORDER',position_state='PROTECTED',
            entry_qty=25,buy_filled_qty=25,buy_vwap=100,protected_qty=25,confirmed_open_qty=25)
        with self.db._get_conn() as conn:
            conn.execute('INSERT INTO protection_orders(parent_order_id,forever_order_id,symbol,sl_trigger,sl_limit,target_price,quantity,status,created_at,protection_route) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (parent,parent,'FAKE-FNO',90,89,110,25,'ACTIVE',WALL-60,'SUPER_ORDER'))
        trade=dict(ident,orderId=parent,exchangeTradeId='EXEC-FNO-1',exchangeOrderId='EXCHANGE-FNO-1',
            transactionType='BUY',tradedQuantity=25,tradedPrice=100,exchangeTime=datetime.fromtimestamp(WALL-60,IST).isoformat())
        legs=[dict(legName=leg,orderStatus='PENDING',transactionType='SELL',remainingQuantity=25,price=price)
              for leg,price in [('STOP_LOSS_LEG',90),('TARGET_LEG',110)]]
        super_row=dict(ident,orderId=parent,orderStatus='TRADED',legDetails=legs)
        v=self.view();s=self.snapshot(v,positions=[dict(ident,netQty=25)],trade_book=[trade],super_orders=[super_row])
        verifier=BrokerLedgerReconciler()
        res=verifier.reconcile_security(s,v,contract,POLICY,MONO)
        self.assertTrue(res.entry_eligible,res.reason_codes)
        self.assertEqual((res.ledger_open_qty,res.verified_protected_qty,res.unprotected_qty),(25,25,0))
        self.assertTrue(verifier.reconcile_account(s,v,POLICY,MONO).entry_eligible)
        missing=replace(s,super_orders=book('super_orders',[dict(super_row,legDetails=legs[:1])]))
        res=verifier.reconcile_security(missing,v,contract,POLICY,MONO)
        self.assertFalse(res.entry_eligible);self.assertIn('SUPER_PROTECTION_UNVERIFIED',res.reason_codes)
        # Closing the same parent requires exact terminal protective legs and child fill.
        self.db.record_fill('FNO-TARGET','EXEC-FNO-2','FAKE-FNO',110,25,traded_at=WALL-30,
            transaction_type='SELL',security_id='54321',exchange_segment='NSE_FNO',product_type='INTRADAY',
            parent_order_id=parent,is_verified_trade=True)
        with self.db._get_conn() as conn:
            conn.execute("UPDATE positions_lifecycle SET position_state='POSITION_CLOSED',sell_filled_qty=25,protected_qty=0,confirmed_open_qty=0,triggered_leg='TARGET_LEG' WHERE parent_order_id=?",(parent,))
        sell=dict(ident,orderId='FNO-TARGET',exchangeTradeId='EXEC-FNO-2',exchangeOrderId='EXCHANGE-FNO-2',
            transactionType='SELL',tradedQuantity=25,tradedPrice=110,exchangeTime=datetime.fromtimestamp(WALL-30,IST).isoformat())
        child=dict(ident,orderId='FNO-TARGET',orderStatus='TRADED',transactionType='SELL',quantity=25,filledQty=25,remainingQuantity=0)
        terminal_legs=[dict(legName='TARGET_LEG',orderId='FNO-TARGET',orderStatus='TRADED',transactionType='SELL',quantity=25,remainingQuantity=0),
                       dict(legName='STOP_LOSS_LEG',orderId='FNO-STOP',orderStatus='CANCELLED',transactionType='SELL',quantity=25,remainingQuantity=0)]
        terminal=dict(super_row,orderStatus='CLOSED',legDetails=terminal_legs)
        v=self.view();s=self.snapshot(v,positions=[dict(ident,netQty=0)],trade_book=[trade,sell],order_book=[child],super_orders=[terminal])
        result=verifier.reconcile_security(s,v,contract,POLICY,MONO)
        self.assertTrue(result.entry_eligible,result.reason_codes)
        self.assertTrue(result.parents[0].closure_verified)
        for wrong in ([terminal_legs[0]], [dict(terminal_legs[0],orderId='OTHER'),terminal_legs[1]],
                      [terminal_legs[0],dict(terminal_legs[1],orderStatus='PENDING')]):
            bad=replace(s,super_orders=book('super_orders',[dict(terminal,legDetails=wrong)]))
            result=verifier.reconcile_security(bad,v,contract,POLICY,MONO)
            self.assertFalse(result.entry_eligible);self.assertIn('CLOSED_SUPER_LEG_UNVERIFIED',result.reason_codes)

    def _seed_parent(self, parent, quantity, at, *, state='PROTECTED', sell=0):
        self.db.record_order_intent(correlation_id=parent,account_id=A,symbol='FAKE',security_id=C.security_id,
            exchange_segment=C.exchange_segment,product_type=C.product_type,order_type='LIMIT',price=100,quantity=quantity,protection_route='FOREVER_OCO')
        self.db.update_order_intent(parent,'TRADED',broker_order_id=parent)
        with self.db._get_conn() as conn:
            conn.execute('UPDATE order_intents SET created_at=?, updated_at=? WHERE correlation_id=?',(at,at,parent))
        self.db.record_fill(parent,'EXEC-'+parent,'FAKE',100,quantity,traded_at=at,transaction_type='BUY',security_id=C.security_id,
            exchange_segment=C.exchange_segment,product_type=C.product_type,parent_order_id=parent,is_verified_trade=True)
        self.db.upsert_position_lifecycle(parent_order_id=parent,account_id=A,symbol='FAKE',security_id=C.security_id,
            exchange_segment=C.exchange_segment,product_type=C.product_type,protection_route='FOREVER_OCO',position_state=state,
            entry_qty=quantity,buy_filled_qty=quantity,buy_vwap=100,sell_filled_qty=sell,protected_qty=quantity if state=='PROTECTED' else 0,
            confirmed_open_qty=quantity-sell)
        if state=='UNPROTECTED_MANUAL_RISK':
            self.db.acknowledge_manual_risk_holding(parent_order_id=parent,correlation_id=parent,account_id=A,symbol='FAKE',security_id=C.security_id,
                quantity=quantity,acknowledged_by='offline owner',reason='retain')
        return dict(IDENT,orderId=parent,exchangeTradeId='EXEC-'+parent,exchangeOrderId='EXCHANGE-'+parent,transactionType='BUY',
                    tradedQuantity=quantity,tradedPrice=100,exchangeTime=datetime.fromtimestamp(at,IST).isoformat(),isin='OFFLINE-ISIN')

    def _archive(self,s,rows,day):
        pages=[]
        for i,payload in enumerate((rows,[])):
            key=f'history:{day}:{day}:{i}'
            pages.append(replace(book(key,payload),endpoint=f'/v2/trades/{day}/{day}/{i}',request_params={'from_date':str(day),'to_date':str(day),'page':i}))
        return replace(s,historical_trades=tuple(pages),history_coverage=(HistoryCoverage(day,day,tuple(p.book_key for p in pages),True,True,MONO,True),))

    def _mixed(self):
        old=[self._seed_parent(p,1,WALL-86400,state='UNPROTECTED_MANUAL_RISK') for p in ('OLD-A','OLD-B')]
        new=self._seed_parent('NEW',28,WALL-60)
        with self.db._get_conn() as conn:
            conn.execute('INSERT INTO protection_orders(parent_order_id,forever_order_id,symbol,sl_trigger,sl_limit,target_price,quantity,status,created_at,protection_route) VALUES (?,?,?,?,?,?,?,?,?,?)',
                ('NEW','GROUP','FAKE',90,89,110,28,'ACTIVE_CONFIRMED',WALL-60,'FOREVER_OCO'))
        v=self.view()
        pos=dict(IDENT,netQty=28,dayBuyQty=28,daySellQty=0,carryForwardBuyQty=0,carryForwardSellQty=0)
        hold=dict(securityId=C.security_id,dhanClientId=A,isin='OFFLINE-ISIN',totalQty=2,dpQty=2,t1Qty=0)
        forever=dict(IDENT,orderId='GROUP',transactionType='SELL',orderStatus='PENDING',quantity=28,quantity1=28,triggerPrice=90,price1=110)
        s=self.snapshot(v,positions=[pos],holdings=[hold],trade_book=[new],forever_orders=[forever])
        s=replace(s,session=replace(s.session,phase=SessionPhase.CURRENT_SIGNED))
        return v,self._archive(s,old,DAY-timedelta(days=1)),pos,hold,new,forever

    def test_mixed_current_signed_matches_legacy_quantity_proof(self):
        from src.engine.mixed_cnc_evidence import verify_mixed_cnc_split, MixedCNCUnverified
        v,s,pos,hold,new,forever=self._mixed()
        legacy=verify_mixed_cnc_split(db=self.db,positions=[pos],holdings=[hold],trade_book=[new],account=A,security=C.security_id,
            segment=C.exchange_segment,product=C.product_type,parent_quantities={'OLD-A':1,'OLD-B':1,'NEW':28},now=WALL,forever=[forever])
        res=self.verdict(v,s)
        self.assertTrue(res.entry_eligible,res.reason_codes)
        self.assertEqual((res.broker_open_qty,res.ledger_open_qty,res.verified_protected_qty,res.manual_ack_qty,res.unprotected_qty),(legacy.total_qty,30,28,2,2))
        for bad in (dict(pos,dayBuyQty=29,daySellQty=1),dict(pos,carryForwardBuyQty=1),dict(pos,netQty=27)):
            with self.subTest(bad=bad):
                self.assertFalse(self.verdict(v,replace(s,positions=book('positions',[bad]))).entry_eligible)
        self.assertFalse(self.verdict(v,replace(s,positions=book('positions',[]))).entry_eligible)
        self.assertFalse(self.verdict(v,replace(s,session=replace(s.session,phase=SessionPhase.SETTLED))).entry_eligible)
        self.assertFalse(self.verdict(v,replace(s,forever_orders=book('forever_orders',[]))).entry_eligible)

    def test_unlabelled_holdings_requires_distinct_bound_capture(self):
        v,s,pos,hold,new,forever=self._mixed(); hold.pop('dhanClientId')
        s=replace(s,holdings=book('holdings',[hold]))
        self.assertFalse(self.verdict(v,s).entry_eligible)
        def independent(payload):
            proof=self.snapshot(v,holdings=payload)
            def advance(b):
                return replace(b,request_started_mono_ns=MONO+NS,received_mono_ns=MONO+NS,
                               request_started_utc_ns=(WALL+1)*NS,received_utc_ns=(WALL+1)*NS)
            return replace(proof,snapshot_id='independent',session=replace(s.session,observed_utc_ns=(WALL+1)*NS),
                capture_started_mono_ns=MONO+NS,capture_finished_mono_ns=MONO+NS,
                capture_started_utc_ns=(WALL+1)*NS,capture_finished_utc_ns=(WALL+1)*NS,
                **{k:advance(getattr(proof,k)) for k in LIVE_BOOKS|{'profile'}})
        good=independent([dict(hold,dhanClientId=A,lastPrice=999)])
        self.assertTrue(self.verdict(v,s,replace(POLICY,holdings_confirmation=good),MONO+NS).entry_eligible)
        for bad in (s, independent([dict(hold,totalQty=3)]), replace(good,journal_digest='f'*64)):
            self.assertFalse(self.verdict(v,s,replace(POLICY,holdings_confirmation=bad),MONO+NS).entry_eligible)
        # Explicitly blank/mismatched account rows are rejected at snapshot construction.
        with self.assertRaises(ValueError): book('holdings',[dict(hold,dhanClientId='')])

    def _closed_rollup(self, *, prior_signed=False):
        # Two retained prior-day lots plus one closed same-contract sibling.
        old=[self._seed_parent(p,1,WALL-2*86400,state='UNPROTECTED_MANUAL_RISK') for p in ('OLD-A','OLD-B')]
        old_buy=self._seed_parent('CLOSED',3,WALL-2*86400,state='POSITION_CLOSED',sell=3)
        # Split BUY and SELL execution sets are durable and separately bound.
        sell_at=WALL-86400 if prior_signed else WALL-60
        with self.db._get_conn() as conn:
            conn.execute("UPDATE fills SET traded_quantity=1 WHERE broker_order_id='CLOSED'")
            conn.execute("UPDATE positions_lifecycle SET triggered_leg='TARGET_LEG' WHERE parent_order_id='CLOSED'")
            conn.execute('INSERT INTO protection_orders(parent_order_id,forever_order_id,symbol,sl_trigger,sl_limit,target_price,quantity,status,created_at,protection_route) VALUES (?,?,?,?,?,?,?,?,?,?)',
                ('CLOSED','GROUP','FAKE',90,89,110,3,'POSITION_CLOSED_LOCAL',WALL-2*86400,'FOREVER_OCO'))
            record=dict(order_id='CLOSED',security_id=C.security_id,segment=C.exchange_segment,product_type=C.product_type,
                        protection_route='FOREVER_OCO',position_state='POSITION_CLOSED',confirmed_position_open_qty=0,
                        filled_qty=3,exit_filled_qty=3,triggered_leg='TARGET_LEG',confirmed_protection_ids=['GROUP'],
                        verified_leg_order_ids=['CHILD'],verified_leg_map={'GROUP':'TARGET_LEG','CHILD':'TARGET_LEG'})
            conn.execute('INSERT INTO watcher_orders VALUES (?,?,?,?)',('CLOSED',1,json.dumps(record),WALL))
        self.db.record_fill('CLOSED','EXEC-CLOSED-SECOND','FAKE',100,2,traded_at=WALL-2*86400,transaction_type='BUY',security_id=C.security_id,
            exchange_segment=C.exchange_segment,product_type=C.product_type,parent_order_id='CLOSED',is_verified_trade=True)
        sells=[]
        for eid,qty in [('SELL-A',1),('SELL-B',2)]:
            self.db.record_fill('CHILD',eid,'FAKE',110,qty,traded_at=sell_at,transaction_type='SELL',security_id=C.security_id,
                exchange_segment=C.exchange_segment,product_type=C.product_type,parent_order_id='CLOSED',is_verified_trade=True)
            sells.append(dict(IDENT,orderId='CHILD',exchangeTradeId=eid,exchangeOrderId='EXCHANGE-CHILD',transactionType='SELL',
                tradedQuantity=qty,tradedPrice=110,exchangeTime=datetime.fromtimestamp(sell_at,IST).isoformat(),isin='OFFLINE-ISIN'))
        # Independently confirmed closed BUY identity; archive summary is only corroboration.
        buy_status=dict(IDENT,orderId='CLOSED',exchangeOrderId='EXCHANGE-CLOSED',transactionType='BUY',orderStatus='TRADED',quantity=3,filledQty=3,remainingQuantity=0)
        child=dict(IDENT,orderId='CHILD',algoId='GROUP',exchangeOrderId='EXCHANGE-CHILD',transactionType='SELL',orderStatus='TRADED',quantity=3,filledQty=3,remainingQuantity=0)
        group=dict(IDENT,orderId='GROUP',orderType='OCO',legName='TARGET_LEG',orderStatus='TRIGGERED',transactionType='SELL',quantity=3)
        hold=dict(securityId=C.security_id,dhanClientId=A,isin='OFFLINE-ISIN',totalQty=5,dpQty=5,t1Qty=0)
        pos=dict(IDENT,netQty=-3,dayBuyQty=0,daySellQty=3,carryForwardBuyQty=0,carryForwardSellQty=0)
        session_day=DAY-timedelta(days=1) if prior_signed else DAY
        v=self.view();s=self.snapshot(v,positions=[pos],holdings=[hold],trade_book=sells,forever_orders=[group],order_book=[child])
        if prior_signed:
            # At 00:08 IST, the previous session remains independently signed.
            # Keep observation in that exact session's permitted window.
            wall=int(datetime(2026,11,26,0,8,tzinfo=IST).timestamp())
        else: wall=WALL
        s=replace(s,session=replace(s.session,phase=SessionPhase.PRIOR_SIGNED if prior_signed else SessionPhase.CURRENT_SIGNED,
                                    trading_session_date=session_day,observed_utc_ns=wall*NS))
        old.append(dict(old_buy,exchangeTradeId='',tradedQuantity=3))
        lo=DAY-timedelta(days=2);hi=session_day-timedelta(days=1);pages=[]
        for i,payload in enumerate((old,[])):
            key=f'history:{lo}:{hi}:{i}'
            pages.append(replace(book(key,payload),endpoint=f'/v2/trades/{lo}/{hi}/{i}',request_params={'from_date':str(lo),'to_date':str(hi),'page':i}))
        def status(r):
            oid=r['orderId'];return replace(book('order_status:'+oid,r),endpoint='/v2/orders/'+oid,request_params={'order_id':oid})
        s=replace(s,historical_trades=tuple(pages),history_coverage=(HistoryCoverage(lo,hi,tuple(p.book_key for p in pages),True,True,MONO,True),),order_status={'CLOSED':status(buy_status)})
        if prior_signed:
            def shift(b):return replace(b,request_started_utc_ns=wall*NS,received_utc_ns=wall*NS)
            s=replace(s,capture_started_utc_ns=wall*NS,capture_finished_utc_ns=wall*NS,
                historical_trades=tuple(shift(b) for b in s.historical_trades),order_status={k:shift(b) for k,b in s.order_status.items()},
                **{k:shift(getattr(s,k)) for k in LIVE_BOOKS|{'profile'}})
        return v,s,group,child

    def test_signed_closed_sibling_requires_exact_persisted_leg_binding(self):
        v,s,group,child=self._closed_rollup()
        res=self.verdict(v,s)
        self.assertTrue(res.entry_eligible,res.reason_codes)
        self.assertEqual((res.broker_open_qty,res.ledger_open_qty,res.manual_ack_qty,res.closed_parent_quantity),(2,2,2,3))
        self.assertEqual(len(res.historical_matches),1)
        self.assertEqual(res.historical_matches[0].matched_exchange_trade_ids,('EXEC-CLOSED','EXEC-CLOSED-SECOND'))
        self.assertEqual(res.historical_matches[0].total_qty,3)
        for wrong in (dict(group,legName='STOP_LOSS_LEG'),dict(group,quantity=2),dict(group,orderType='SINGLE')):
            self.assertFalse(self.verdict(v,replace(s,forever_orders=book('forever_orders',[wrong]))).entry_eligible)
        for wrong in (dict(child,algoId='WRONG'),dict(child,remainingQuantity=1),dict(child,quantity=4)):
            self.assertFalse(self.verdict(v,replace(s,order_book=book('order_book',[wrong]))).entry_eligible)
        with self.db._get_conn() as conn:
            rec=json.loads(conn.execute("SELECT record_json FROM watcher_orders WHERE order_id='CLOSED'").fetchone()[0]);rec['verified_leg_map']['CHILD']='STOP_LOSS_LEG'
            conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='CLOSED'",(json.dumps(rec),))
        v2=self.view();res=self.verdict(v2,replace(s,journal_digest=v2.digest_sha256))
        self.assertFalse(res.entry_eligible);self.assertIn('CLOSED_TRIGGERED_CHILD_UNVERIFIED',res.reason_codes)

    def test_prior_signed_closed_sibling_matches_current_arithmetic(self):
        v,s,_,_=self._closed_rollup(prior_signed=True)
        res=self.verdict(v,s)
        self.assertTrue(res.entry_eligible,res.reason_codes)
        self.assertEqual((res.broker_open_qty,res.ledger_open_qty,res.manual_ack_qty),(2,2,2))

    def test_durable_issue_lifecycle_requires_exact_individual_resolution(self):
        from src.engine.durable_ledger_issues import OPENED,RESOLVED,read_durable_issues
        from src.broker.broker_snapshot import thaw_json
        v,s,_,_=self._closed_rollup(); verifier=BrokerLedgerReconciler()
        with self.db._get_conn() as conn:
            rec=json.loads(conn.execute("SELECT record_json FROM watcher_orders WHERE order_id='CLOSED'").fetchone()[0])
            rec.update(owner_readback_failed=True,last_persistence_error='write uncertain',unreadable_broker_response=True)
            conn.execute("UPDATE watcher_orders SET record_json=?,updated_at=? WHERE order_id='CLOSED'",(json.dumps(rec),WALL-30))
        v=self.view();s=replace(s,journal_digest=v.digest_sha256)
        proposals=verifier.plan_durable_issue_openings(s,v,C,POLICY,MONO)
        # Only durable fields are imported; transient read status is recomputed.
        self.assertEqual({p[2]['reason_code'] for p in proposals},{'LEGACY_UNCLASSIFIED_RISK:owner_readback_failed','LEGACY_UNCLASSIFIED_RISK:last_persistence_error'})
        self.assertEqual({p[2]['first_seen_utc_ns'] for p in proposals},{(WALL-30)*NS})
        with self.db._get_conn() as conn:
            for typ,ident,payload in proposals:
                self.assertEqual(typ,OPENED)
                conn.execute('INSERT INTO system_events(event_type,description,payload,created_at) VALUES (?,?,?,?)',(typ,ident,json.dumps(thaw_json(payload)),WALL))
            # A legacy writer may drop flags, but cannot erase the durable event.
            rec.pop('owner_readback_failed');rec.pop('last_persistence_error');rec.pop('unreadable_broker_response')
            conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='CLOSED'",(json.dumps(rec),))
        v=self.view();s=replace(s,snapshot_id='new-proof',journal_digest=v.digest_sha256)
        current=self.verdict(v,s);self.assertFalse(current.entry_eligible)
        state=read_durable_issues(v);self.assertFalse(state.malformed);self.assertEqual(len(state.unresolved),2)
        first=state.unresolved[0]
        self.assertEqual(first.first_seen_utc_ns,(WALL-30)*NS)
        with self.assertRaises(ValueError):verifier.plan_durable_issue_resolution(replace(s,complete=False),v,C,POLICY,MONO,first.issue_id)
        with self.assertRaises(ValueError):verifier.plan_durable_issue_resolution(replace(s,snapshot_id=first.opened_snapshot_id),v,C,POLICY,MONO,first.issue_id)
        for i in range(2):
            v=self.view();s=replace(s,journal_digest=v.digest_sha256,snapshot_id=f'proof-{i}')
            target=read_durable_issues(v).unresolved[0]
            typ,ident,payload=verifier.plan_durable_issue_resolution(s,v,C,POLICY,MONO,target.issue_id)
            self.assertEqual(typ,RESOLVED);self.assertEqual(payload['opening_event_id'],target.event_id)
            with self.db._get_conn() as conn:
                conn.execute('INSERT INTO system_events(event_type,description,payload,created_at) VALUES (?,?,?,?)',(typ,ident,json.dumps(thaw_json(payload)),WALL+1))
            v=self.view();s=replace(s,journal_digest=v.digest_sha256)
            self.assertEqual(len(read_durable_issues(v).unresolved),1-i)
            self.assertEqual(self.verdict(v,s).entry_eligible,i==1)
        # Previously resolved event cannot erase a new occurrence of the same field.
        with self.db._get_conn() as conn:
            rec['owner_readback_failed']=True
            conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='CLOSED'",(json.dumps(rec),))
        v=self.view();s=replace(s,journal_digest=v.digest_sha256)
        self.assertFalse(self.verdict(v,s).entry_eligible)
        self.assertEqual(len(verifier.plan_durable_issue_openings(s,v,C,POLICY,MONO)),1)

    def test_unsupported_durable_mutation_issue_cannot_be_cleared_by_healthy_books(self):
        from src.engine.durable_ledger_issues import read_durable_issues
        from src.broker.broker_snapshot import thaw_json
        v,s,_,_=self._closed_rollup();verifier=BrokerLedgerReconciler()
        with self.db._get_conn() as conn:
            rec=json.loads(conn.execute("SELECT record_json FROM watcher_orders WHERE order_id='CLOSED'").fetchone()[0]);rec['cancel_uncertain']=True
            conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='CLOSED'",(json.dumps(rec),))
        v=self.view();s=replace(s,journal_digest=v.digest_sha256)
        proposal=verifier.plan_durable_issue_openings(s,v,C,POLICY,MONO)[0]
        with self.db._get_conn() as conn:
            conn.execute('INSERT INTO system_events(event_type,description,payload,created_at) VALUES (?,?,?,?)',(proposal[0],proposal[1],json.dumps(thaw_json(proposal[2])),WALL))
            rec.pop('cancel_uncertain');conn.execute("UPDATE watcher_orders SET record_json=? WHERE order_id='CLOSED'",(json.dumps(rec),))
        v=self.view();s=replace(s,snapshot_id='new-proof',journal_digest=v.digest_sha256)
        self.assertFalse(self.verdict(v,s).entry_eligible)
        with self.assertRaises(ValueError):verifier.plan_durable_issue_resolution(s,v,C,POLICY,MONO,proposal[1])
        self.assertEqual(len(read_durable_issues(v).unresolved),1)

    def test_synthetic_fill_cannot_be_promoted_by_explicit_verified_flag(self):
        for idx,eid in enumerate(('O-RECON-FILL-1','O-TRD-1','O-FILL-1','o-recon-fill-1','0','NONE','')):
            self.db.record_fill('O'+str(idx),eid,'FAKE',100,1,is_verified_trade=True)
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM fills').fetchone()[0],7)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM fills WHERE is_verified_trade=1').fetchone()[0],0)

    def test_archive_missing_evidence_never_labels_ledger_verified(self):
        v,s,*_=self._mixed()
        res=self.verdict(v,replace(s,historical_trades=(),history_coverage=()))
        self.assertFalse(res.ledger_verified)
        self.assertFalse(res.open_quantity_verified)
        self.assertFalse(res.entry_eligible)
        self.assertIn('REQUIRED_ARCHIVE_COVERAGE_MISSING',res.reason_codes)
        self.assertEqual(res.watcher_actions[0].kind,'RECHECK')
        self.assertEqual(res.watcher_actions[0].precondition_journal_digest,v.digest_sha256)

    def test_conflicting_isin_and_impossible_session_block(self):
        v,s,pos,hold,new,forever=self._mixed()
        res=self.verdict(v,replace(s,holdings=book('holdings',[dict(hold,isin='WRONG-ISIN')])))
        self.assertFalse(res.entry_eligible);self.assertIn('EXECUTION_ISIN_CONFLICT',res.reason_codes)
        bad=replace(s,session=replace(s.session,phase=SessionPhase.PRIOR_SIGNED,trading_session_date=DAY))
        self.assertFalse(self.verdict(v,bad).entry_eligible)
        self.assertIn('BROKER_SESSION_CHRONOLOGY_UNVERIFIED',self.verdict(v,bad).reason_codes)

    def _risk_fixture(self):
        from src.engine.durable_ledger_issues import read_durable_issues
        from src.broker.broker_snapshot import thaw_json
        v,s,*_=self._mixed()
        rec=dict(order_id='NEW',security_id=C.security_id,segment=C.exchange_segment,product_type=C.product_type,
                 owner_readback_failed=True,unreadable_broker_response=True)
        with self.db._get_conn() as conn:
            conn.execute('INSERT INTO watcher_orders VALUES (?,?,?,?)',('NEW',0,json.dumps(rec),WALL-60))
        v=self.view();s=replace(s,journal_digest=v.digest_sha256)
        verifier=BrokerLedgerReconciler()
        openings=verifier.plan_durable_issue_openings(s,v,C,POLICY,MONO)
        self.assertEqual(len(openings),1)
        typ,ident,payload=openings[0]
        self.assertEqual(payload['reason_code'],'LEGACY_UNCLASSIFIED_RISK:owner_readback_failed')
        self.assertEqual(payload['first_seen_utc_ns'],(WALL-60)*NS)
        # Tests simulate only the immutable event append; production persistence
        # and revalidation of proposals remain in the Phase 2D boundary.
        self.db.record_system_event(typ,ident,json.dumps(thaw_json(payload)))
        v=self.view();self.assertFalse(read_durable_issues(v).malformed)
        self.assertEqual(len(read_durable_issues(v).unresolved),1)
        with self.db._get_conn() as conn:
            rec['owner_readback_failed']=False
            conn.execute('UPDATE watcher_orders SET record_json=? WHERE order_id=?',(json.dumps(rec),'NEW'))
        v=self.view();s=replace(s,snapshot_id='fresh-risk-proof',journal_digest=v.digest_sha256)
        return v,s,ident

    def test_durable_occurrence_survives_raw_flag_clear_until_exact_readback(self):
        from src.engine.durable_ledger_issues import read_durable_issues
        from src.broker.broker_snapshot import thaw_json
        v,s,ident=self._risk_fixture();verifier=BrokerLedgerReconciler()
        res=self.verdict(v,s);self.assertFalse(res.entry_eligible)
        issue=next(i for i in res.issues if i.issue_id==ident)
        self.assertEqual(issue.first_seen_utc_ns,(WALL-60)*NS)
        parent=next(p for p in res.parents if p.parent_order_id=='NEW')
        self.assertIn(ident,parent.unresolved_risk_ids)
        with self.assertRaises(ValueError):
            verifier.plan_durable_issue_resolution(replace(s,complete=False),v,C,POLICY,MONO,ident)
        typ,description,proof=verifier.plan_durable_issue_resolution(s,v,C,POLICY,MONO,ident)
        self.assertEqual(description,ident);self.assertEqual(proof['opening_event_id'],read_durable_issues(v).unresolved[0].event_id)
        self.assertFalse(self.verdict(v,s).entry_eligible) # proposal is not committed proof
        self.db.record_system_event(typ,description,json.dumps(thaw_json(proof)))
        v=self.view();s=replace(s,snapshot_id='readback-after-commit',journal_digest=v.digest_sha256)
        self.assertFalse(read_durable_issues(v).unresolved)
        self.assertTrue(self.verdict(v,s).entry_eligible)
        with self.assertRaises(ValueError): verifier.plan_durable_issue_resolution(s,v,C,POLICY,MONO,ident)

    def test_malformed_resolution_cannot_erase_occurrence(self):
        from src.engine.durable_ledger_issues import read_durable_issues
        from src.broker.broker_snapshot import thaw_json
        v,s,ident=self._risk_fixture();verifier=BrokerLedgerReconciler()
        typ,description,proof=verifier.plan_durable_issue_resolution(s,v,C,POLICY,MONO,ident)
        bad=thaw_json(proof);bad['opening_sha256']='0'*64
        self.db.record_system_event(typ,description,json.dumps(bad))
        v=self.view();s=replace(s,journal_digest=v.digest_sha256)
        state=read_durable_issues(v)
        self.assertTrue(state.malformed);self.assertEqual(len(state.unresolved),1)
        res=self.verdict(v,s);self.assertFalse(res.entry_eligible)
        self.assertIn('DURABLE_ISSUE_EVENT_MALFORMED',res.reason_codes)



class SessionAndFlags(unittest.TestCase):
    def test_exact_inventory(self):
        self.assertEqual((len(TRANSIENT_FIELDS),len(DURABLE_RISK_FIELDS),len(BUSINESS_FACT_FIELDS)),(20,37,57))
        self.assertFalse(TRANSIENT_FIELDS&DURABLE_RISK_FIELDS)
        self.assertIn('last_persistence_error',DURABLE_RISK_FIELDS);self.assertIn('owner_readback_failed',DURABLE_RISK_FIELDS)
    def test_prior_session_cutoff_and_no_clock_only_settlement(self):
        cal=VerifiedCalendar(frozenset(),frozenset({2026}));prev=DAY-timedelta(days=1)
        midnight=int(datetime(2026,11,26,0,8,tzinfo=IST).timestamp())*NS
        kw=dict(calendar=cal,execution_dates=[prev],broker_session_date=prev,books_complete=True,settlement_verified=False,signed_delivery_verified=True,product='CNC')
        self.assertEqual(classify_broker_session(observed_utc_ns=midnight,**kw)[0],BrokerSessionState.PRE_EOD_PRIOR_SESSION)
        self.assertEqual(classify_broker_session(observed_utc_ns=WALL*NS,**kw)[0],BrokerSessionState.AMBIGUOUS_BLOCKED)
        kw.update(execution_dates=[],broker_session_date=DAY,signed_delivery_verified=False)
        self.assertEqual(classify_broker_session(observed_utc_ns=WALL*NS,**kw)[0],BrokerSessionState.AMBIGUOUS_BLOCKED)
        kw['settlement_verified']=True
        self.assertEqual(classify_broker_session(observed_utc_ns=WALL*NS,**kw)[0],BrokerSessionState.T1_SETTLED)
    def test_malformed_durable_is_not_transient(self):
        self.assertIn('LEGACY_UNCLASSIFIED_RISK:reconciliation_required',legacy_risks({'reconciliation_required':True}))
        self.assertIn('LEGACY_UNCLASSIFIED_RISK:pending_oco',legacy_risks({'pending_oco':{'state':'UNKNOWN'}}))
        self.assertIn('LEGACY_UNCLASSIFIED_RISK:owner_readback_failed',legacy_risks({'owner_readback_failed':None}))


class HistoricalSellRollup(unittest.TestCase):
    def setup_match(self):
        at=WALL-86400;fills={('CHILD','REAL-EID-A'):('SELL',1,100.0,at),('CHILD','REAL-EID-B'):('SELL',2,101.0,at)}
        matcher=ClosedHistoricalBuyMatcher(fills,closed_order_rollups(fills),account=A,contract=('11915','NSE_EQ','CNC'),today=DAY,now=WALL)
        row=dict(IDENT,orderId='CHILD',exchangeOrderId='EXCHANGE-ORDER',exchangeTradeId='',transactionType='SELL',tradedQuantity=3,tradedPrice='100.666667',exchangeTime=datetime.fromtimestamp(at,IST).isoformat())
        return fills,matcher,row
    def test_blank_sell_corrob_only_and_independent_child_identity(self):
        fills,m,row=self.setup_match(); self.assertEqual(m.match(row),set(fills));self.assertEqual(row['exchangeTradeId'],'')
        closed={'trades':fills,'triggered_groups':{'GROUP':{'leg':'TARGET_LEG','quantity':3,'children':{'CHILD':3}}}}
        forever=[dict(IDENT,orderId='GROUP',orderType='OCO',legName='TARGET_LEG',orderStatus='TRIGGERED',transactionType='SELL',quantity=3)]
        child=dict(IDENT,orderId='CHILD',algoId='GROUP',exchangeOrderId='EXCHANGE-ORDER',transactionType='SELL',orderStatus='TRADED',quantity=3,filledQty=3,remainingQuantity=0)
        args=dict(group='GROUP',closed=closed,forever=forever,day_trades=[],account=A,security='11915',segment='NSE_EQ',product='CNC',historical_sell_matches=m.sell_matches)
        self.assertTrue(verify_closed_triggered_group(orders=[child],**args))
        for ch in (dict(child,exchangeOrderId='WRONG'),dict(child,remainingQuantity=1),dict(child,algoId='OTHER')):
            with self.assertRaises(ValueError): verify_closed_triggered_group(orders=[ch],**args)
        with self.assertRaises(ValueError): verify_closed_triggered_group(orders=[],**args)
        with self.assertRaises(ValueError): m.match(row)
    def test_wrong_price_qty_date_and_bool_fail_closed(self):
        for updates in ({'tradedQuantity':2},{'tradedPrice':'100.67'},{'exchangeTime':'2026-11-24 11:00:00'},{'exchangeTradeId':False},{'exchangeOrderId':''}):
            _,m,row=self.setup_match()
            with self.subTest(updates=updates),self.assertRaises(ValueError):m.match(dict(row,**updates))
    def test_exact_plus_rollup_cannot_overlap(self):
        _,m,row=self.setup_match();m.match(dict(row,exchangeTradeId='REAL-EID-A',tradedQuantity=1,tradedPrice=100))
        with self.assertRaises(ValueError):m.match(row)
