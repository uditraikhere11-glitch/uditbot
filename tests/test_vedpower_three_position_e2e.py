"""Offline three-position VEDPOWER + YESBANK regression against real WAL journal/gate/watcher.

No live Dhan or Telegram connections. All broker returns are synthetic,
but source reconciliation, history paging calls, SQLite, and watcher are real.
"""
import sqlite3
from contextlib import closing
import unittest
from unittest.mock import patch

from src.models import BrokerReadFailure, BrokerReadStatus
from src.engine.reconciliation import ReconciliationState
from tests.test_yesbank_nov22_e2e import SyntheticNov22Fixture, ORDERS, NOW
from tests import test_historical_zero_trade_id_r01 as r01

VED = "321261120279408"
SEC = "3033"
CID = "EQ-VEDPOWER-01"
ISIN = "INE000A01001"  # synthetic for an offline-only account
OCO = "OCO-VED-001"


def oco_rows(status="PENDING", qty=1):
    return r01.as_read_list([
        dict(orderId=OCO, correlationId=CID, dhanClientId=r01.ACCOUNT,
             securityId=SEC, exchangeSegment="NSE_EQ", productType="CNC",
             transactionType="SELL", orderStatus=status, legName=leg,
             quantity=qty, triggerPrice=90, price=price)
        for leg,price in (("STOP_LOSS_LEG",89), ("TARGET_LEG",110))
    ])


class VEDPOWERProtectedThreeHoldings(SyntheticNov22Fixture):
    def setUp(self):
        super().setUp()
        self.ved_trade = r01.trade(VED, "EX-VED-01", "2026-11-20", "10:15:01", 100.0)
        self.ved_trade.update(securityId=SEC, isin=ISIN)
        created=r01.timestamp("2026-11-20","10:15:00")
        with self.db._get_conn() as conn:
            conn.execute(
                "INSERT INTO order_intents (correlation_id,account_id,symbol,security_id,"
                "exchange_segment,product_type,order_type,price,quantity,state,"
                "broker_order_id,created_at,updated_at) "
                "VALUES (?,?,?,?,'NSE_EQ','CNC','LIMIT',100,1,'ACKED',?,?,?)",
                (CID,r01.ACCOUNT,"VEDPOWER",SEC,VED,created,created),
            )
            conn.execute(
                "INSERT INTO orders (correlation_id,broker_order_id,trading_mode,"
                "product_type,exchange_segment,symbol,security_id,order_type,price,quantity,"
                "status,created_at,updated_at) VALUES (?,?,'LIVE','CNC','NSE_EQ',?,?,"
                "'LIMIT',100,1,'ACKED',?,?)",
                (CID,VED,"VEDPOWER",SEC,created,created),
            )
        self.db.apply_attested_historical_order_fill(
            correlation_id=CID, broker_order_id=VED, expected_account_id=r01.ACCOUNT,
            symbol="VEDPOWER", security_id=SEC, exchange_segment="NSE_EQ",
            product_type="CNC", requested_quantity=1,
            attested_trade=self.fx._normalized(self.ved_trade), now=NOW,
        )
        self.db.upsert_position_lifecycle(
            parent_order_id=VED, symbol="VEDPOWER", security_id=SEC,
            exchange_segment="NSE_EQ", product_type="CNC",
            protection_route="FOREVER_OCO", position_state="PROTECTED",
            entry_qty=1, buy_filled_qty=1, buy_vwap=100., protected_qty=1,
            confirmed_open_qty=1, account_id=r01.ACCOUNT, now=NOW,
        )
        self.broker.get_historical_trades.return_value = r01.as_read_list([
            self.fx._normalized(self.fx.first), self.fx._normalized(self.fx.second),
            self.fx._normalized(self.ved_trade),
        ])
        self.broker.get_holdings.return_value=r01.as_read_list([
            dict(securityId="11915",isin="INE528G01035",totalQty=2,dpQty=2),
            dict(securityId=SEC,isin=ISIN,totalQty=1,dpQty=1),
        ])
        self.broker.get_forever_orders.return_value=oco_rows()
        self.broker.get_trades_for_order.side_effect=lambda oid: (
            BrokerReadFailure(BrokerReadStatus.UNAVAILABLE.value,
                              "old BUY GET unavailable",429) if oid==VED else r01.as_read_list([])
        )
        self.restart()
        self.watcher.reconcile_historical_order_fills(
            order_id=VED, correlation_id=CID, symbol="VEDPOWER",security_id=SEC,
            segment="NSE_EQ",product_type="CNC",requested_qty=1,filled_qty=1,
            avg_fill_price=100.,verified_protected_qty=1,
            confirmed_protection_ids=[OCO],entry_terminal_status="TRADED",
            protection_route="FOREVER_OCO",now=r01.timestamp("2026-11-20","10:15:02"),
        )
        row=self.db.get_position_lifecycle(VED)
        self.assertEqual((row["position_state"],row["protected_qty"]), ("PROTECTED",1))
        self.broker.get_trades_for_order.reset_mock()
        self.broker.get_historical_trades.reset_mock()

    def test_all_three_old_parent_buys_skipped_but_active_oco_read_is_mandatory(self):
        ok,msg=self.gate_once()
        self.assertTrue(ok,msg)
        self.assertTrue(self.gate.is_accepting_entries)
        self.assertEqual(self.db.get_position_lifecycle(VED)["position_state"],"PROTECTED")
        for oid in ORDERS:
            self.assertEqual(self.db.get_position_lifecycle(oid)["position_state"],
                             "UNPROTECTED_MANUAL_RISK")
        attempted=[str(c.args[0]) for c in self.broker.get_trades_for_order.call_args_list]
        self.assertNotIn(VED,attempted)
        for oid in ORDERS:self.assertNotIn(oid,attempted)
        self.assertIn(OCO,attempted)
        self.assertGreaterEqual(self.broker.get_historical_trades.call_count,2)
        self.assertEqual(self.watcher._supervision_read_complete,True)

    def _persist_actual_victim_watcher_flag(self):
        """Matches the 2026-11-22 production screenshot, not a clean fixture."""
        import json
        with self.db._get_conn() as conn:
            row=conn.execute(
                "SELECT record_json FROM watcher_orders WHERE order_id=?", (VED,)
            ).fetchone()
            self.assertIsNotNone(row)
            rec=json.loads(row[0])
            self.assertEqual(rec["position_state"], "PROTECTED")
            self.assertEqual(rec["state"], "TRADED")
            self.assertEqual(rec["protected_qty"], 1)
            self.assertEqual(rec["filled_qty"], 1)
            self.assertEqual(rec["reconciliation_required"], False)
            # The production watcher has one persisted active OCO leg ID.
            # The synthetic attested-fill helper seeds confirmed protection,
            # but has not yet populated this watcher-specific list.
            rec["verified_leg_order_ids"] = [OCO]
            rec["unreadable_broker_response"] = True
            conn.execute(
                "UPDATE watcher_orders SET record_json=? WHERE order_id=?",
                (json.dumps(rec), VED),
            )
        self.restart()
        self.assertTrue(self.watcher.terminal_orders[VED]["unreadable_broker_response"])

    def test_real_persisted_vedpower_unreadable_flag_is_not_a_permanent_recovery_blocker(self):
        self._persist_actual_victim_watcher_flag()
        ok,reason=self.gate_once()
        self.assertTrue(ok,reason)
        self.assertTrue(self.gate.is_accepting_entries)
        attempted=[c.args[0] for c in self.broker.get_trades_for_order.call_args_list]
        self.assertNotIn(VED,attempted)
        self.assertIn(OCO,attempted)
        self.assert_ack_and_no_sell()

    def test_missing_historical_original_buy_blocks_without_retrying_old_buy_or_OCO(self):
        self._persist_actual_victim_watcher_flag()
        self.broker.get_historical_trades.return_value = r01.as_read_list([
            self.fx._normalized(self.fx.first), self.fx._normalized(self.fx.second)
        ])
        # A Dhan 429 on the obsolete original lookup would place *all* GET
        # requests into a 60-second category cooldown. The corrected watcher
        # must not send this lookup and still MUST query the active OCO SELL.
        attempts=[]
        def old_lookup_fails(oid):
            attempts.append(oid)
            if oid == VED:
                self.gov.throttled(60)
                return BrokerReadFailure(BrokerReadStatus.UNAVAILABLE.value,
                                         "synthetic old daily BUY endpoint 429",429)
            return r01.as_read_list([])
        self.broker.get_trades_for_order.side_effect=old_lookup_fails
        with self.assertLogs("src.engine.protection_watcher",level="WARNING") as captured:
            ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertNotIn(VED,attempts)
        self.assertIn(OCO,attempts)
        self.assertTrue(any("ORIGINAL_BUY_MISSING_FROM_HISTORY" in line
                            for line in captured.output),captured.output)
        with closing(self.gov._connect()) as db:
            remaining=db.execute(
                "SELECT until FROM category_cooldown WHERE category='NON_TRADING'"
            ).fetchone()
            self.assertIsNone(remaining)
        self.assert_ack_and_no_sell()
        # Restoring *fresh* complete history, rather than clearing flags or
        # modifying production SQLite, allows an independent green pass.
        self.broker.get_historical_trades.return_value=r01.as_read_list([
            self.fx._normalized(self.fx.first),self.fx._normalized(self.fx.second),
            self.fx._normalized(self.ved_trade),
        ])
        self.broker.get_trades_for_order.reset_mock()
        ok,reason=self.gate_once()
        self.assertTrue(ok,reason)
        self.assertTrue(self.gate.is_accepting_entries)
        self.assertNotIn(VED,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])
        self.assert_ack_and_no_sell()

    def test_missing_live_oco_still_blocks_and_does_not_hit_old_buy(self):
        self._persist_actual_victim_watcher_flag()
        self.broker.get_forever_orders.return_value=r01.as_read_list([])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertNotIn(VED,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])
        self.assert_ack_and_no_sell()

    def test_current_day_or_unverified_buy_still_requires_original_trade_endpoint(self):
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id=?",
                         (self.now-120,VED))
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertIn(VED,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])

    def test_oco_disappears_fails_closed(self):
        self.broker.get_forever_orders.return_value=r01.as_read_list([])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_oco_cancelled_fails_closed(self):
        self.broker.get_forever_orders.return_value=oco_rows("CANCELLED")
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_shortfall_without_sell_fails_closed(self):
        self.broker.get_holdings.return_value=r01.as_read_list([
            dict(securityId="11915",isin="INE528G01035",totalQty=2,dpQty=2),
            dict(securityId=SEC,isin=ISIN,totalQty=0,dpQty=0),
        ])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_protected_old_buy_history_missing_fails_closed(self):
        self.broker.get_historical_trades.return_value=r01.as_read_list([
            self.fx._normalized(self.fx.first), self.fx._normalized(self.fx.second)
        ])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_sell_followed_buy_same_holdings_qty_fails_closed(self):
        hist=list(self.broker.get_historical_trades.return_value)
        sell=dict(hist[-1],orderId="EXTERNAL-SELL",exchangeTradeId="EXSELL-1",
                  transactionType="SELL",exchangeTime="2026-11-21T12:00:00")
        rebuy=dict(hist[-1],orderId="EXTERNAL-REBUY",exchangeTradeId="EXBUY-1",
                   transactionType="BUY",exchangeTime="2026-11-21T12:01:00")
        self.broker.get_historical_trades.return_value=r01.as_read_list([*hist,sell,rebuy])
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

    def test_active_oco_leg_trade_read_unavailable_fails_closed(self):
        self.broker.get_trades_for_order.side_effect=lambda oid: (
            BrokerReadFailure(BrokerReadStatus.UNAVAILABLE.value,
                              "Synthetic OCO exit GET denied",429) if oid==OCO
            else r01.as_read_list([])
        )
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertIn(OCO,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])

    def test_journal_write_failure_blocks(self):
        with patch.object(self.db,"upsert_position_lifecycle",
                          side_effect=sqlite3.OperationalError("synthetic write denied")):
            ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)

if __name__=="__main__":unittest.main()

# Additional scenarios are kept as subclasses so unittest discovers them.
class VEDPOWERRecoveryAcrossDH904(VEDPOWERProtectedThreeHoldings):
    def test_broker_deferral_then_real_serialized_worker_restores_three_lots(self):
        ok,reason=self.gate_once()
        self.assertTrue(ok,reason)
        blocked=BrokerReadFailure(BrokerReadStatus.UNAVAILABLE.value,
                                  "synthetic DH-904",429)
        self.broker.get_positions.return_value=blocked
        self.broker.get_holdings.return_value=blocked
        self.watcher.check_once()
        self.gate.halt_new_entries("Synthetic broker unavailable")
        self.assertFalse(self.gate.is_accepting_entries)
        self.gov.throttled(60)
        self.recovery_worker()
        self.assertFalse(self.gate.is_accepting_entries)
        self.now+=61
        self.broker.get_positions.return_value=r01.as_read_list([])
        self.broker.get_holdings.return_value=r01.as_read_list([
            dict(securityId="11915",isin="INE528G01035",totalQty=2,dpQty=2),
            dict(securityId=SEC,isin=ISIN,totalQty=1,dpQty=1),
        ])
        self.gate._historical_background_retry_after=0
        self.broker.get_trades_for_order.reset_mock()
        self.recovery_worker()
        self.assertEqual(self.gate.state,ReconciliationState.HEALTHY,self.gate.last_error)
        self.assertTrue(self.gate.is_accepting_entries)
        self.assertNotIn(VED,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])
        self.assertEqual(self.db.get_position_lifecycle(VED)["position_state"],"PROTECTED")
        self.assert_ack_and_no_sell()

    def test_current_day_buy_cannot_be_exempt(self):
        with self.db._get_conn() as conn:
            conn.execute("UPDATE order_intents SET created_at=? WHERE broker_order_id=?",
                         (self.now-120,VED))
        ok,_=self.gate_once()
        self.assertFalse(ok)
        self.assertFalse(self.gate.is_accepting_entries)
        self.assertIn(VED,[c.args[0] for c in self.broker.get_trades_for_order.call_args_list])

    def test_after_success_routine_uses_process_scoped_proof_and_reads_live_exit(self):
        ok,reason=self.gate_once()
        self.assertTrue(ok,reason)
        self.broker.get_trades_for_order.reset_mock()
        self.watcher.check_once()
        attempted=[c.args[0] for c in self.broker.get_trades_for_order.call_args_list]
        self.assertNotIn(VED,attempted)
        self.assertIn(OCO,attempted)
        self.restart()
        self.assertFalse(hasattr(self.watcher,"_protected_buy_history_session"))
        self.broker.get_trades_for_order.reset_mock()
        self.watcher.check_once()
        # A clean legacy protected record without prior unreadable evidence
        # retains its original fallback; an already-unreadable parent instead
        # uses the fail-closed, no-repeat path tested above.
        attempted=[c.args[0] for c in self.broker.get_trades_for_order.call_args_list]
        self.assertIn(VED,attempted)
        self.assertIn(OCO,attempted)
