"""Isolated safety proofs that need no live Dhan/Telegram or repository setup."""
import ast
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
import importlib.util
import logging
import sys
import types
import typing
import unittest

ROOT = Path(__file__).resolve().parents[1]


def extracted(path, class_name=None, methods=(), functions=()):
    tree = ast.parse((ROOT / path).read_text())
    body = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in functions]
    if class_name:
        clazz = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
        selected = [n for n in clazz.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in methods]
        body.append(ast.ClassDef(name=class_name, bases=[], keywords=[], body=selected, decorator_list=[]))
    mod = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    return compile(mod, str(ROOT / path), 'exec')


class NotSubmittedTests(TestCase):
    def setUp(self):
        self.code = extracted('src/main.py', functions=('_is_transport_uncertainty',))
        ns = {}
        exec(self.code, ns)
        self.unknown = ns['_is_transport_uncertainty']

    def test_local_failure_not_unknown_even_when_error_contains_connection_or_503(self):
        for message in ('MUTATION NOT_SUBMITTED (config fence): unknown process',
                        'MUTATION NOT_SUBMITTED (local admission): HTTP 503 connection failed',
                        'MUTATION NOT SUBMITTED (config fence): timeout'):
            self.assertFalse(self.unknown(message))

    def test_real_broker_uncertain_response_stays_unknown(self):
        for msg in ('Dhan HTTP 503 server error (outcome UNKNOWN)', 'Connection timed out'):
            self.assertTrue(self.unknown(msg))


class ReconcileTests(TestCase):
    @classmethod
    def setUpClass(cls):
        ns = {'Tuple': typing.Tuple, 'Dict': typing.Dict, 'List': typing.List,
              'Optional': typing.Optional, 'Any': typing.Any,
              'logger': logging.getLogger('test')}
        exec(extracted('src/engine/reconciliation.py', 'Gate', ()) if False else
             extracted('src/engine/reconciliation.py', 'StartupReconciliationGate',
                       ('_lookup_intent_at_broker', '_validate_broker_order_matches_intent')), ns)
        cls.Klass = ns['StartupReconciliationGate']

    def setUp(self):
        self.gate = self.Klass()
        self.gate.broker = SimpleNamespace(
            get_order_by_correlation_id=Mock(), order_status=Mock(),
            cfg=SimpleNamespace(dhan_client_id='A'),
        )
        self.gate.db = SimpleNamespace(account_id='A')
        self.row = {'orderId':'111', 'correlationId':'ABC', 'orderStatus':'PENDING', 'securityId':'123'}

    def test_duplicate_correlation_never_falls_through_to_point_lookup(self):
        self.assertIsNone(self.gate._lookup_intent_at_broker('ABC', None,
            [self.row, dict(self.row, orderId='222')]))
        self.gate.broker.order_status.assert_not_called()
        self.gate.broker.get_order_by_correlation_id.assert_not_called()

    def test_bad_single_matching_row_never_falls_through(self):
        self.assertIsNone(self.gate._lookup_intent_at_broker('ABC', None,
            [dict(self.row, securityId='wrong')], intent={'security_id':'123'}))
        self.gate.broker.order_status.assert_not_called()

    def test_rejected_without_filled_qty_is_accepted(self):
        valid, reason = self.gate._validate_broker_order_matches_intent(
            dict(self.row, orderStatus='REJECTED'), {'quantity':5}, 'ABC', '111')
        self.assertTrue(valid, reason)

    def test_cancelled_without_fill_remains_unknown_if_partial_unverified(self):
        row = dict(self.row, orderStatus='CANCELLED')
        for intent in (None, {'quantity':5}, {'quantity':5,'filled_qty':2}):
            valid,_ = self.gate._validate_broker_order_matches_intent(row,intent,'ABC','111')
            self.assertFalse(valid)
        valid,why = self.gate._validate_broker_order_matches_intent(row, {'quantity':5,'filled_qty':0}, 'ABC','111')
        self.assertTrue(valid,why)

    def test_executed_order_needs_explicit_filled_qty(self):
        for status in ('TRADED', 'PART_TRADED'):
            valid,_ = self.gate._validate_broker_order_matches_intent(
                dict(self.row, orderStatus=status), {'quantity':5}, 'ABC','111')
            self.assertFalse(valid)


class OutboxTests(TestCase):
    def test_floodwait_stops_whole_batch(self):
        p=ROOT/'src/notifier/outbox_worker.py'
        spec=importlib.util.spec_from_file_location('outbox_pr93_isolated',p)
        mod=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        class DB:
            def __init__(self): self.events=[{'id':i,'payload':{'text':'signal'},'event_type':'LOG'} for i in range(1,11)]; self.deferred=[]
            def claim_pending_outbox(self,limit,now): return self.events[:1] if self.events else []
            def defer_outbox(self,oid,until): self.events.pop(0); self.deferred.append((oid,until))
        db=DB(); called=[]
        def dispatch(text): called.append(text); raise mod.TelegramRetryAfter(90)
        result=mod.OutboxWorker(db,dispatch).process_pending_events(limit=10,now=100)
        self.assertEqual(result,0)
        self.assertEqual(len(called),1)
        self.assertEqual(db.deferred,[(1,190)])


class PollTests(TestCase):
    def test_stale_reconciliation_does_not_loop_every_10_seconds_when_closed(self):
        p=ROOT/'src/engine/watch_poll_policy.py';spec=importlib.util.spec_from_file_location('policy_pr93_test',p)
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        rec={'order_id':'221261102657708','position_state':'RECONCILIATION_REQUIRED',
             'reconciliation_required':True,'unreadable_broker_response':True, 'filled_qty':1}
        self.assertEqual(mod.watcher_cadence_seconds(pending={},open_records=[rec],now=0,market_open=False),300)
        self.assertEqual(mod.watcher_cadence_seconds(pending={},open_records=[rec],now=0,market_open=True),10)


class BrokerMutationTests(TestCase):
    def test_fenced_mutation_does_not_charge_modification_quota(self):
        # The V2 gate rejects before entering the mutation body.
        class BrokerHandoffBlocked(RuntimeError): pass
        class BrokerMutationNotSubmitted(BrokerHandoffBlocked): pass
        @contextmanager
        def blocked(*args): raise BrokerHandoffBlocked('config fence')
        class ReadDeferred(RuntimeError): pass
        class ReadGovernor:
            @staticmethod
            def classify(method,url): return 'ORDER'
            @staticmethod
            def modification_order_id(method,url): return 'X'
        sys.modules['src'] = sys.modules.get('src',types.ModuleType('src'))
        sys.modules['src.broker'] = sys.modules.get('src.broker',types.ModuleType('src.broker'))
        sys.modules['src.engine'] = sys.modules.get('src.engine',types.ModuleType('src.engine'))
        gmod=types.ModuleType('src.broker.read_governor');gmod.ReadGovernor=ReadGovernor;gmod.ReadDeferred=ReadDeferred
        hmod=types.ModuleType('src.engine.broker_handoff');hmod.broker_mutation=blocked
        hmod.BrokerHandoffBlocked=BrokerHandoffBlocked;hmod.BrokerMutationNotSubmitted=BrokerMutationNotSubmitted
        with unittest.mock.patch.dict(sys.modules,{'src.broker.read_governor':gmod,'src.engine.broker_handoff':hmod}):
            ns={'sqlite3':__import__('sqlite3'),'os':__import__('os'),
                'BrokerMutationNotSubmitted':BrokerMutationNotSubmitted}
            exec(extracted('src/broker/dhan_client.py','DhanClient',('_broker_mutating_http',)),ns)
            b=ns['DhanClient']()
            gov=Mock();b._get_active_read_governor=lambda:gov
            b.cfg=SimpleNamespace(data_dir='/tmp/a',dhan_client_id='A')
            b.session=SimpleNamespace(put=Mock())
            with unittest.mock.patch.dict(__import__('os').environ,{'UDITBOT_TRACKER_V2_ENABLE':'1'}):
                with self.assertRaises(BrokerMutationNotSubmitted):
                    b._broker_mutating_http('PUT','https://api.dhan.co/v2/orders/X')
            gov.admit.assert_not_called()
            b.session.put.assert_not_called()

    def test_local_sqlite_refusal_never_reaches_http_or_unknown(self):
        class BrokerHandoffBlocked(RuntimeError): pass
        class BrokerMutationNotSubmitted(BrokerHandoffBlocked): pass
        class ReadDeferred(RuntimeError): pass
        class ReadGovernor:
            @staticmethod
            def classify(method,url): return 'ORDER'
            @staticmethod
            def modification_order_id(method,url): return 'X'
        @contextmanager
        def allowed(*args):
            finished=[]
            yield lambda success: finished.append(success)
            assert finished==[True], finished
        gmod=types.ModuleType('src.broker.read_governor');gmod.ReadGovernor=ReadGovernor;gmod.ReadDeferred=ReadDeferred
        hmod=types.ModuleType('src.engine.broker_handoff');hmod.broker_mutation=allowed
        hmod.BrokerHandoffBlocked=BrokerHandoffBlocked;hmod.BrokerMutationNotSubmitted=BrokerMutationNotSubmitted
        with unittest.mock.patch.dict(sys.modules,{'src.broker.read_governor':gmod,'src.engine.broker_handoff':hmod}):
            ns={'sqlite3':__import__('sqlite3'),'os':__import__('os'),'BrokerMutationNotSubmitted':BrokerMutationNotSubmitted}
            exec(extracted('src/broker/dhan_client.py','DhanClient',('_broker_mutating_http',)),ns)
            b=ns['DhanClient']();b.cfg=SimpleNamespace(data_dir='/tmp/a',dhan_client_id='A')
            b.session=SimpleNamespace(put=Mock())
            b._read_deferral_response=lambda e:SimpleNamespace(status_code=503,headers={'X-AuditBot-Local-Deferral':'1'},text=str(e))
            gov=Mock();gov.admit.side_effect=__import__('sqlite3').DatabaseError('corrupt');b._get_active_read_governor=lambda:gov
            with unittest.mock.patch.dict(__import__('os').environ,{'UDITBOT_TRACKER_V2_ENABLE':'1'}):
                response=b._broker_mutating_http('PUT','https://api.dhan.co/v2/orders/X')
            self.assertEqual(response.headers['X-AuditBot-Local-Deferral'],'1')
            b.session.put.assert_not_called()


if __name__=='__main__': unittest.main()
