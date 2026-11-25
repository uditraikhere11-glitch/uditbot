"""IA-001: fail-closed verified SELL conflicts (offline disposable SQLite only)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from src.journal.db import JournalDB


class VerifiedSellConflictRegression(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = str(Path(folder.name) / 'test-only.db')
        self.db = JournalDB(self.path)
        self.base = dict(broker_order_id='SELL-A', exchange_trade_id='X-1',
                         symbol='FAKE', traded_price=100.0, traded_quantity=1,
                         transaction_type='SELL', security_id='FAKE-1',
                         exchange_segment='NSE_EQ', product_type='CNC',
                         is_verified_trade=True)

    def _stored(self):
        with JournalDB(self.path)._get_conn() as conn:
            return [dict(r) for r in conn.execute(
                'SELECT broker_order_id, parent_order_id, exchange_trade_id, '
                'traded_price, traded_quantity, transaction_type FROM fills'
            ).fetchall()]

    def test_conflicting_order_does_not_delete_placeholders(self):
        self.db.record_fill(broker_order_id='SELL-B', exchange_trade_id='SELL-B-FILL-1',
                            symbol='FAKE', traded_price=90, traded_quantity=1,
                            transaction_type='SELL', is_verified_trade=False)
        self.assertTrue(self.db.record_fill(**self.base))
        with self.assertRaisesRegex(ValueError, 'conflicts'):
            self.db.record_fill(**dict(self.base,broker_order_id='SELL-B'))
        rows = self._stored()
        self.assertEqual(len(rows),2)
        self.assertEqual({r['exchange_trade_id'] for r in rows}, {'X-1','SELL-B-FILL-1'})

    def test_conflicting_quantity_price_side_and_contract_fail_closed(self):
        self.assertTrue(self.db.record_fill(**self.base))
        cases = ({'traded_quantity':2},{'traded_price':101.0},
                 {'transaction_type':'BUY'},{'security_id':'OTHER'},
                 {'exchange_segment':'BSE_EQ'},{'product_type':'MTF'})
        for change in cases:
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError,'conflicts'):
                    self.db.record_fill(**dict(self.base,**change))
                self.assertEqual(self._stored()[0]['traded_quantity'],1)

    def test_parent_relink_allowed_exactly_once_then_conflict_blocks(self):
        self.assertTrue(self.db.record_fill(**self.base))
        self.assertTrue(self.db.record_fill(**dict(self.base,parent_order_id='PARENT-A')))
        self.assertFalse(self.db.record_fill(**dict(self.base,parent_order_id='PARENT-A')))
        with self.assertRaisesRegex(ValueError,'conflicts'):
            self.db.record_fill(**dict(self.base,parent_order_id='PARENT-B'))
        self.assertEqual(self._stored()[0]['parent_order_id'],'PARENT-A')

    def test_identical_sell_replay_idempotent_after_reopen(self):
        self.assertTrue(self.db.record_fill(**self.base))
        self.assertFalse(JournalDB(self.path).record_fill(**self.base))
        self.assertEqual(len(self._stored()),1)

    def test_concurrent_conflict_claims_one_winner_no_loss(self):
        with ThreadPoolExecutor(max_workers=3) as pool:
            def claim(k):
                try:
                    return JournalDB(self.path).record_fill(**dict(self.base,broker_order_id=k))
                except ValueError:
                    return 'CONFLICT'
            results=list(pool.map(claim,['SELL-A','SELL-B','SELL-C']))
        self.assertEqual(results.count(True),1,results)
        self.assertEqual(results.count('CONFLICT'),2,results)
        self.assertEqual(len(self._stored()),1)


if __name__=='__main__':
    unittest.main()

# A separate test of the exact current startup-reconciliation method, extracted
# through Python AST for independent review environments that do not contain
# the whole UditBot repository. Full CI must also run production integration.
class StartupGateFillConflictTest(unittest.TestCase):
    def _method(self):
        import ast
        import logging
        import math
        import re
        from types import SimpleNamespace
        from pathlib import Path
        module = ast.parse((Path(__file__).resolve().parents[1] / 'src/engine/reconciliation.py').read_text())
        cls = next(x for x in module.body if isinstance(x, ast.ClassDef) and x.name == 'StartupReconciliationGate')
        fn = next(x for x in cls.body if isinstance(x, ast.FunctionDef) and x.name == '_run_startup_reconciliation_inner')
        env = {'ReconciliationState':SimpleNamespace(BLOCKED='BLOCKED'), 'logger':logging.getLogger(__name__),
               'math':math, 're':re}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'startup_reconciliation_source','exec'),env)
        return env['_run_startup_reconciliation_inner']

    def test_startup_blocks_conflicting_sell_before_any_entry_admission(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            db=JournalDB(str(Path(folder)/'db.sqlite'))
            db.record_fill(broker_order_id='SELL-A',exchange_trade_id='X-1',symbol='FAKE',
                           traded_price=100,traded_quantity=1,transaction_type='SELL',
                           security_id='FAKE-1',exchange_segment='NSE_EQ',product_type='CNC',
                           is_verified_trade=True)
            row=dict(orderId='SELL-B', exchangeTradeId='X-1',transactionType='SELL',
                     tradedQuantity=1,tradedPrice=100,tradingSymbol='FAKE',securityId='FAKE-1',
                     exchangeSegment='NSE_EQ',productType='CNC')
            class FakeGate:
                def __init__(self):
                    self.db=db
                    self.broker=SimpleNamespace(cfg=SimpleNamespace(trading_mode='TEST'),
                              get_fund_limits=lambda: (True,10000,'synthetic'),
                              get_trade_book=lambda: [row])
                    self.stop_manager=SimpleNamespace(is_stopped=lambda:False)
                    self.state='RECONCILING'
                    self.last_error=''
                    self._accepting_entries=False
                    self._restore_committed_historical_watcher_records=lambda:None
                    self._read_required_broker_collection=lambda method,label:(True,[row],'OK')
            gate=FakeGate()
            ok,message=self._method()(gate,0)
            self.assertFalse(ok)
            self.assertEqual(gate.state,'BLOCKED')
            self.assertFalse(gate._accepting_entries)
            self.assertIn('Failed to persist broker trade fill',message)
            with JournalDB(str(Path(folder)/'db.sqlite'))._get_conn() as con:
                kept=con.execute('SELECT broker_order_id FROM fills WHERE exchange_trade_id=?',('X-1',)).fetchone()
                self.assertEqual(kept[0],'SELL-A')

    def test_startup_detects_false_return_with_conflicting_row(self):
        # Fault injection: a future journal implementation might return False
        # rather than raise. The independent startup postcondition must still
        # keep the gate closed, not credit a silently discarded broker trade.
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            real=JournalDB(str(Path(folder)/'db.sqlite'))
            real.record_fill(broker_order_id='SELL-A',exchange_trade_id='X-2',symbol='FAKE',
                             traded_price=100,traded_quantity=1,transaction_type='SELL',
                             security_id='FAKE-1',exchange_segment='NSE_EQ',product_type='CNC',
                             is_verified_trade=True)
            broker_row=dict(orderId='SELL-B',exchangeTradeId='X-2',transactionType='SELL',
                            tradedQuantity=1,tradedPrice=100,tradingSymbol='FAKE',securityId='FAKE-1',
                            exchangeSegment='NSE_EQ',productType='CNC')
            fake_db=SimpleNamespace(
                repair_interrupted_fill_states=real.repair_interrupted_fill_states,
                record_fill=lambda **kwargs:False,
                get_fill_by_exchange_trade_id=real.get_fill_by_exchange_trade_id,
                record_system_event=lambda *args:None,
            )
            gate=SimpleNamespace(
                db=fake_db,
                broker=SimpleNamespace(cfg=SimpleNamespace(trading_mode='TEST'),
                    get_fund_limits=lambda:(True,10000,'synthetic'),
                    get_trade_book=lambda:[broker_row]),
                stop_manager=SimpleNamespace(is_stopped=lambda:False),
                state='RECONCILING',last_error='',_accepting_entries=False,
                _restore_committed_historical_watcher_records=lambda:None,
                _read_required_broker_collection=lambda method,label:(True,[broker_row],'OK'),
            )
            ok,message=self._method()(gate,0)
            self.assertFalse(ok)
            self.assertEqual(gate.state,'BLOCKED')
            self.assertIn('Replayed exchange fill conflicts',message)

    def test_startup_allows_identical_false_replay_to_continue_full_checks(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            real=JournalDB(str(Path(folder)/'db.sqlite'))
            values=dict(broker_order_id='SELL-A',exchange_trade_id='X-3',symbol='FAKE',
                        traded_price=100,traded_quantity=1,transaction_type='SELL',
                        security_id='FAKE-1',exchange_segment='NSE_EQ',product_type='CNC',
                        is_verified_trade=True)
            real.record_fill(**values)
            row=dict(orderId='SELL-A',exchangeTradeId='X-3',transactionType='SELL',
                     tradedQuantity=1,tradedPrice=100,tradingSymbol='FAKE',securityId='FAKE-1',
                     exchangeSegment='NSE_EQ',productType='CNC')
            broker=SimpleNamespace(cfg=SimpleNamespace(trading_mode='TEST'),
                get_fund_limits=lambda:(True,10000,'synthetic'),get_trade_book=lambda:[row],
                get_positions=lambda:[])
            def read(method,label):
                if method=='get_trade_book': return (True,[row],'OK')
                return (False,[], 'synthetic stop after broker-trade persistence')
            gate=SimpleNamespace(
                db=real,broker=broker,stop_manager=SimpleNamespace(is_stopped=lambda:False),
                state='RECONCILING',last_error='',_accepting_entries=False,
                _restore_committed_historical_watcher_records=lambda:None,
                _read_required_broker_collection=read,
            )
            ok,message=self._method()(gate,0)
            self.assertFalse(ok)
            self.assertEqual(gate.state,'BLOCKED')
            self.assertIn('synthetic stop after broker-trade persistence',message)
            self.assertNotIn('Failed to persist broker trade fill',message)
