"""Predeployment closed-loop gate simulation for three owner-managed/covered YESBANK lots.

Offline AST extraction uses the exact candidate reconciliation method with synthetic
account/holdings/OCO evidence and no live Dhan calls, database writes, or VM state.
"""
from __future__ import annotations

import ast
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest


class ExistingYesbankSafetyPreflight(unittest.TestCase):
    ACCOUNT = 'SYNTH-ACCOUNT'
    SECURITY = '11915'
    FIRST = '221261102657708'
    SECOND = '23126110439808'
    THIRD = '221261123606908'

    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / 'src/engine/reconciliation.py'
        module = ast.parse(source.read_text())
        gate = next(n for n in module.body if isinstance(n, ast.ClassDef)
                    and n.name == 'StartupReconciliationGate')
        method = next(n for n in gate.body if isinstance(n, ast.FunctionDef)
                      and n.name == '_verify_bot_owned_holdings_protected')
        class Books:
            def get(self):
                return None
        namespace = dict(
            Tuple=tuple, List=list, Dict=dict, Any=object,
            GATE_BOOKS=Books(), logger=logging.getLogger(__name__),
            BrokerReadFailure=type('BrokerReadFailure', (Exception,), {}),
            verify_forever_oco_group=lambda **kw: SimpleNamespace(is_verified=True, protected_quantity=28)
        )
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), 'exec'), namespace)
        cls.verify = staticmethod(namespace['_verify_bot_owned_holdings_protected'])

    def _build(self, states=('RECONCILIATION_REQUIRED', 'RECONCILIATION_REQUIRED', 'PROTECTED'),
               ack_ids=None, cover=True, holdings_qty=30, active=True):
        if ack_ids is None:
            ack_ids = {self.FIRST, self.SECOND}
        open_rows=[]
        for oid, qty, state in zip((self.FIRST,self.SECOND,self.THIRD), (1,1,28), states):
            open_rows.append(dict(parent_order_id=oid, account_id=self.ACCOUNT,
                symbol='YESBANK-EQ', security_id=self.SECURITY,
                exchange_segment='NSE_EQ', product_type='CNC', confirmed_open_qty=qty,
                position_state=state, entry_qty=qty, buy_filled_qty=qty, sell_filled_qty=0,
                protected_qty=28 if oid==self.THIRD else 0,
                manual_exit_detected=0, orphaned_legs_detected=0))
        db=SimpleNamespace(account_id=self.ACCOUNT,
            get_open_positions_lifecycle=lambda:open_rows,
            is_manual_risk_acknowledged=lambda oid,*a,**kw:oid in ack_ids,
            get_protection_order_ids_for_parent=lambda oid:['OCO-28'] if oid==self.THIRD and cover else [])
        forevers=[dict(orderId='OCO-28',securityId=self.SECURITY)] if active else []
        broker=SimpleNamespace(cfg=SimpleNamespace(dhan_client_id=self.ACCOUNT),
            get_holdings=lambda:[dict(securityId=self.SECURITY, dhanClientId=self.ACCOUNT,
                            totalQty=holdings_qty,dpQty=holdings_qty,t1Qty=0)],
            get_forever_orders=lambda:forevers)
        return SimpleNamespace(db=db, broker=broker, watcher=None,
            _in_startup_reconciliation_pass=False,
            _read_required_broker_collection=lambda name,label:(True,forevers,'OK'))

    def test_disposable_journal_preserves_ack_and_blocks_until_durable_recovery(self):
        from src.journal.db import JournalDB, write_protection_order
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            journal = JournalDB(str(Path(td) / 'synthetic.db'), account_id=self.ACCOUNT)
            initial = {self.FIRST: 'RECONCILIATION_REQUIRED',
                       self.SECOND: 'RECONCILIATION_REQUIRED', self.THIRD: 'PROTECTED'}
            for oid, qty in ((self.FIRST, 1), (self.SECOND, 1), (self.THIRD, 28)):
                journal.upsert_position_lifecycle(
                    parent_order_id=oid, symbol='YESBANK-EQ', security_id=self.SECURITY,
                    exchange_segment='NSE_EQ', product_type='CNC', protection_route='FOREVER_OCO',
                    position_state=initial[oid], entry_qty=qty, buy_filled_qty=qty,
                    confirmed_open_qty=qty, protected_qty=qty if qty == 28 else 0,
                    account_id=self.ACCOUNT)
            for oid in (self.FIRST, self.SECOND):
                journal.acknowledge_manual_risk_holding(
                    parent_order_id=oid, correlation_id='CORR-' + oid,
                    account_id=self.ACCOUNT, symbol='YESBANK-EQ', security_id=self.SECURITY,
                    quantity=1, acknowledged_by='synthetic owner', reason='retain synthetic share')
            with journal._get_conn() as conn:
                write_protection_order(conn, parent_id=self.THIRD, protection_id='OCO-28',
                    symbol='YESBANK-EQ', sl_trigger=10.0, sl_limit=9.9,
                    target_price=12.0, quantity=28, status='ACTIVE_CONFIRMED', created_at=1.0)
            self.assertEqual(
                [r['position_state'] for r in journal.get_open_positions_lifecycle() if r['parent_order_id'] in (self.FIRST,self.SECOND)],
                ['RECONCILIATION_REQUIRED','RECONCILIATION_REQUIRED'])
            self.assertTrue(journal.is_manual_risk_acknowledged(self.FIRST,self.FIRST,
                expected_account_id=self.ACCOUNT,expected_security_id=self.SECURITY,required_qty=1))
            self.assertTrue(journal.is_manual_risk_acknowledged(self.SECOND,self.SECOND,
                expected_account_id=self.ACCOUNT,expected_security_id=self.SECURITY,required_qty=1))
            x = self._build()
            x.db = journal
            self.assertFalse(self.verify(x, [], ())[0])
            # Simulate the *outcome* of an independently verified, durable watcher
            # recovery. Never write this to the real journal or bypass evidence.
            for oid in (self.FIRST, self.SECOND):
                journal.upsert_position_lifecycle(
                    parent_order_id=oid, symbol='YESBANK-EQ', security_id=self.SECURITY,
                    exchange_segment='NSE_EQ', product_type='CNC', protection_route='FOREVER_OCO',
                    position_state='UNPROTECTED_MANUAL_RISK', entry_qty=1, buy_filled_qty=1,
                    confirmed_open_qty=1, protected_qty=0, account_id=self.ACCOUNT)
            self.assertTrue(self.verify(x, [], ())[0])
            self.assertEqual(journal.get_protection_order_ids_for_parent(self.FIRST), [])
            self.assertEqual(journal.get_protection_order_ids_for_parent(self.SECOND), [])
            self.assertEqual(journal.get_protection_order_ids_for_parent(self.THIRD), ['OCO-28'])

    def test_legacy_reconciliation_required_blocks_even_when_all_owner_acks_match(self):
        x=self._build()
        ok,reason=self.verify(x, [], ())
        self.assertFalse(ok)
        self.assertIn('Settled bot-owned holding remains unresolved',reason)
        self.assertIn(self.FIRST,reason)

    def test_both_owner_lots_must_be_durably_recovered_for_positive_gate(self):
        x=self._build(states=('UNPROTECTED_MANUAL_RISK','UNPROTECTED_MANUAL_RISK','PROTECTED'))
        ok,reason=self.verify(x, [], ())
        self.assertTrue(ok,reason)

    def test_owner_ack_cannot_cover_missing_second_parent(self):
        x=self._build(states=('UNPROTECTED_MANUAL_RISK','UNPROTECTED_MANUAL_RISK','PROTECTED'),
                       ack_ids={self.FIRST})
        ok,reason=self.verify(x, [], ())
        self.assertFalse(ok)
        self.assertIn(self.SECOND,reason)

    def test_no_parent_forever_binding_does_not_allow_28_share_credit(self):
        x=self._build(states=('UNPROTECTED_MANUAL_RISK','UNPROTECTED_MANUAL_RISK','PROTECTED'),cover=False)
        ok,reason=self.verify(x, [], ())
        self.assertFalse(ok)
        self.assertIn(self.THIRD,reason)

    def test_insufficient_holdings_blocks_even_when_local_acks_and_binding_exist(self):
        x=self._build(states=('UNPROTECTED_MANUAL_RISK','UNPROTECTED_MANUAL_RISK','PROTECTED'),holdings_qty=29)
        ok,reason=self.verify(x, [], ())
        self.assertFalse(ok)
        self.assertIn('quantity',reason.lower())

if __name__=='__main__':
    unittest.main()
