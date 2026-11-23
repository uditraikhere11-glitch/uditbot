"""PR #130: offline owner-intent, closed CNC and service-control safety tests."""
import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.engine.owner_stop_status import owner_intent_blocks_new_entries
from src.engine.closed_parent_safety import verified_closed_delivery_parent
from src.engine.config_manager import ConfigChangeTracker
from src.journal.db import JournalDB


class OwnerIntentAdmissionTests(unittest.TestCase):
    def test_pending_stop_blocks_even_if_reconciliation_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            self.assertFalse(owner_intent_blocks_new_entries(tmp))
            (p / "service_control_state.json").write_text(json.dumps({
                "desired_service_state": "STOPPED", "deferred_stop_pending": True,
                "observed_state": "RUNNING",
            }))
            self.assertTrue(owner_intent_blocks_new_entries(tmp))
            (p / "service_control_state.json").write_text(json.dumps({
                "desired_service_state": "RUNNING", "deferred_stop_pending": False,
                "observed_state": "RUNNING",
            }))
            self.assertFalse(owner_intent_blocks_new_entries(tmp))
            (p / "mode_transition_gate.json").write_text('{"active": true}')
            self.assertTrue(owner_intent_blocks_new_entries(tmp))
            (p / "mode_transition_gate.json").write_text('{"active": false}')
            self.assertFalse(owner_intent_blocks_new_entries(tmp))

    def test_corrupted_owner_or_transition_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p / "service_control_state.json").write_text("{")
            self.assertTrue(owner_intent_blocks_new_entries(tmp))
            (p / "service_control_state.json").write_text(
                '{"desired_service_state":"RUNNING","deferred_stop_pending":false}')
            (p / "mode_transition_gate.json").write_text('{"active": "false"}')
            self.assertTrue(owner_intent_blocks_new_entries(tmp))

    def test_runtime_gate_refuses_pending_owner_stop(self):
        from src.engine.reconciliation import StartupReconciliationGate
        with tempfile.TemporaryDirectory() as tmp:
            gate = StartupReconciliationGate(
                broker=SimpleNamespace(cfg=SimpleNamespace(data_dir=tmp)),
                journal_db=None, watcher=None,
                stop_manager=SimpleNamespace(is_stopped=lambda: False),
            )
            gate._accepting_entries = True
            self.assertTrue(gate.is_accepting_entries)
            (Path(tmp) / "service_control_state.json").write_text(json.dumps({
                "desired_service_state": "STOPPED",
                "observed_state": "RUNNING",
                "deferred_stop_pending": True,
            }))
            self.assertFalse(gate.is_accepting_entries)
            self.assertTrue(gate._accepting_entries)
            # Still allows protective supervision; only fresh entry admission changes.


class CancelDeferredStopTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.tracker = ConfigChangeTracker(data_dir=str(self.root))
        self.tracker.record_service_control_intent(
            desired_service_state="STOPPED", last_action="OWNER_STOP",
            reason="Deferred for broker obligations", requested_mode="LIVE",
            observed_state="RUNNING", deferred_stop_pending=True,
        )
        self.tracker.set_transition_gate(
            True, change_id="OWNER-STOP-PENDING",
            target_mode="STOPPED", reason="Stop waiting",
        )
        self.calls = []

    def fake_status(self, action, **kwargs):
        self.calls.append(action)
        return {
            "service_state": "RUNNING", "runtime_verified": True,
            "saved_mode": "LIVE", "running_mode": "LIVE",
            "systemd_properties": {"MainPID": 10001},
            "owner_intent": self.tracker.get_service_control_record(),
            "transition_gate": self.tracker.inspect_transition_gate_status(read_only=True),
        }

    def test_cancel_under_lock_does_not_restart_or_touch_journal(self):
        with patch.object(self.tracker, "control_service", side_effect=self.fake_status):
            result = self.tracker.cancel_deferred_owner_stop()
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.tracker.get_service_control_record()["last_action"], "OWNER_CANCEL_STOP")
        self.assertFalse(self.tracker.get_service_control_record()["deferred_stop_pending"])
        self.assertFalse(self.tracker.inspect_transition_gate_status()["active"])
        self.assertTrue(all(x == "status" for x in self.calls))
        self.assertFalse(owner_intent_blocks_new_entries(str(self.root)))

    def test_changed_pid_invalidates_owner_cancellation(self):
        count = [0]
        def status(action, **kwargs):
            count[0] += 1
            result = self.fake_status(action, **kwargs)
            if count[0] >= 2:
                result["systemd_properties"] = {"MainPID": 10002}
            return result
        with patch.object(self.tracker, "control_service", side_effect=status):
            result = self.tracker.cancel_deferred_owner_stop()
        self.assertFalse(result["ok"])
        self.assertEqual(result["blocked_reason"], "POSTCONDITION_UNVERIFIED")

    def test_refuses_unverified_pid_or_config(self):
        def unverified(action, **kwargs):
            return {**self.fake_status(action, **kwargs), "runtime_verified": False}
        with patch.object(self.tracker, "control_service", side_effect=unverified):
            result = self.tracker.cancel_deferred_owner_stop()
        self.assertFalse(result["ok"])
        self.assertEqual(self.tracker.get_service_control_record()["desired_service_state"], "STOPPED")
        self.assertTrue(self.tracker.inspect_transition_gate_status()["active"])

    def test_refuses_unrelated_active_transition(self):
        self.tracker.set_transition_gate(
            True, change_id="LIVE-TO-TEST", target_mode="TEST", reason="Not a stop",
        )
        with patch.object(self.tracker, "control_service", side_effect=self.fake_status):
            result = self.tracker.cancel_deferred_owner_stop()
        self.assertFalse(result["ok"])
        self.assertTrue(self.tracker.get_service_control_record()["deferred_stop_pending"])

    def test_rejected_config_transaction_preserves_owner_stop(self):
        (self.root / "deferred_config.env").write_text("TRADING_MODE=TEST\n")
        with patch.object(self.tracker, "control_service", side_effect=self.fake_status):
            result = self.tracker.cancel_deferred_owner_stop()
        self.assertFalse(result["ok"])
        self.assertEqual(result["blocked_reason"], "CONFIG_OPERATION_PENDING")
        self.assertTrue(self.tracker.get_service_control_record()["deferred_stop_pending"])

    def test_partial_gate_write_remains_blocked_and_can_retry(self):
        from src.engine.config_manager import GateOperationResult, TransitionGateStatus
        original = self.tracker.set_transition_gate
        with patch.object(self.tracker, "control_service", side_effect=self.fake_status):
            with patch.object(self.tracker, "set_transition_gate", return_value=GateOperationResult(
                    ok=False, status=TransitionGateStatus.FILE_WRITE_FAILURE.value,
                    reason="simulated", data={})):
                failed = self.tracker.cancel_deferred_owner_stop()
            self.assertFalse(failed["ok"])
            self.assertTrue(owner_intent_blocks_new_entries(str(self.root)))
            self.tracker.set_transition_gate = original
            retry = self.tracker.cancel_deferred_owner_stop()
        self.assertTrue(retry["ok"], retry)
        self.assertFalse(owner_intent_blocks_new_entries(str(self.root)))


class ClosedDeliveryParentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dbfile = str(Path(self.tmp.name) / "journal.db")
        JournalDB(self.dbfile, account_id="FAKE")
        with sqlite3.connect(self.dbfile) as con:
            con.execute("""INSERT INTO positions_lifecycle (
                parent_order_id, account_id, symbol, security_id, exchange_segment,
                product_type, protection_route, position_state, entry_qty,
                buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                "BUY1", "FAKE", "VEDPOWER", "750518", "NSE_EQ", "CNC", "FOREVER_OCO",
                "POSITION_CLOSED", 1, 1, 1, 0, time.time(),
            ))
            for oid,eid,side,price in (
                ("BUY1","603051757","BUY",34.69),
                ("SELL1","600110497","SELL",35.06),
            ):
                con.execute("""INSERT INTO fills (
                    broker_order_id, exchange_trade_id, symbol, traded_price,
                    traded_quantity, traded_at, transaction_type, security_id,
                    exchange_segment, product_type, parent_order_id, is_verified_trade)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",(
                    oid,eid,"VEDPOWER",price,1,time.time(),side,"750518",
                    "NSE_EQ","CNC","BUY1",1,
                ))
            con.execute("""INSERT INTO protection_orders (
                parent_order_id, forever_order_id, symbol,sl_trigger,sl_limit,
                target_price,quantity,status,created_at)
                VALUES (?,?,?,?,?,?,?,?,?)""",(
                "BUY1","FOREVER1","VEDPOWER",34.34,33.99,35.04,1,
                "POSITION_CLOSED_LOCAL",time.time(),
            ))
        self.broker = MagicMock()
        self.broker.get_positions.return_value = [
            {"securityId":"750518","exchangeSegment":"NSE_EQ",
             "productType":"CNC","dhanClientId":"FAKE","netQty":-1},
        ]
        self.broker.get_holdings.return_value = [
            {"securityId":"750518","totalQty":1,"availableQty":0},
        ]
        self.broker.get_trade_book.return_value = [
            {"securityId":"750518","exchangeSegment":"NSE_EQ",
             "productType":"CNC","dhanClientId":"FAKE",
             "transactionType":"SELL","exchangeTradeId":"600110497"},
        ]
        self.forever = [{"securityId":"750518","exchangeSegment":"NSE_EQ",
                         "productType":"CNC","dhanClientId":"FAKE",
                         "orderId":"FOREVER1","orderStatus":"TRIGGERED"}]
        self.kw = dict(
            db_path=self.dbfile, account="FAKE", parent_id="BUY1",
            required_qty=1, security_id="750518",
            broker=self.broker, orders=[], forever=self.forever, super_orders=[],
        )

    def test_t0_uses_exact_signed_delivery_predicate(self):
        with patch("src.engine.closed_parent_safety.verified_flat_delivery_day",
                   return_value=True) as verify:
            self.assertTrue(verified_closed_delivery_parent(**self.kw))
            verify.assert_called_once()

    def test_t1_flat_accepts_only_current_zero_exposure(self):
        self.broker.get_positions.return_value = []
        self.broker.get_holdings.return_value = []
        self.broker.get_trade_book.return_value = []
        # A same-day empty broker tradebook is not T+1 proof.
        self.assertFalse(verified_closed_delivery_parent(**self.kw))
        with sqlite3.connect(self.dbfile) as con:
            con.execute("UPDATE fills SET traded_at=? WHERE transaction_type='SELL'", (time.time() - 86400,))
        self.assertTrue(verified_closed_delivery_parent(**self.kw))
        self.broker.get_holdings.return_value = [
            {"securityId":"750518","totalQty":1,"availableQty":1}
        ]
        self.assertFalse(verified_closed_delivery_parent(**self.kw))

    def test_ambiguous_or_active_protection_blocks(self):
        self.assertFalse(verified_closed_delivery_parent(
            **{**self.kw, "forever":[dict(self.forever[0],orderStatus="ACTIVE")]}
        ))
        self.assertFalse(verified_closed_delivery_parent(
            **{**self.kw, "security_id":"OTHER"}
        ))
        with sqlite3.connect(self.dbfile) as con:
            con.execute("UPDATE positions_lifecycle SET confirmed_open_qty=1 WHERE parent_order_id='BUY1'")
        self.assertFalse(verified_closed_delivery_parent(**self.kw))

    def test_no_broker_call_for_still_open_parent(self):
        with sqlite3.connect(self.dbfile) as con:
            con.execute("UPDATE positions_lifecycle SET position_state='PROTECTED' WHERE parent_order_id='BUY1'")
        self.assertFalse(verified_closed_delivery_parent(**self.kw))
        self.broker.get_positions.assert_not_called()




class CoordinatorPacingTests(unittest.TestCase):
    def test_deferred_stop_does_not_repeat_broker_preflight_every_half_second(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = ConfigChangeTracker(data_dir=tmp)
            with patch.object(tracker, "_has_active_or_deferred_work", return_value=True), \
                 patch.object(tracker, "get_service_control_record", return_value={
                     "desired_service_state": "STOPPED", "deferred_stop_pending": True,
                     "stop_ready_to_apply": False,
                 }), \
                 patch.object(tracker, "run_control_coordinator_tick") as tick:
                tracker.start_coordinator_daemon(
                    env_path=".env", poll_interval_sec=0.01, stop_when_idle=False,
                )
                time.sleep(0.12)
                tracker.stop_coordinator_daemon()
                self.assertEqual(tick.call_count, 1)


    def test_persistent_ready_flag_does_not_burst_repeated_broker_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = ConfigChangeTracker(data_dir=tmp)
            with patch.object(tracker, "_has_active_or_deferred_work", return_value=True), \
                 patch.object(tracker, "get_service_control_record", return_value={
                     "desired_service_state": "STOPPED",
                     "deferred_stop_pending": True,
                     "stop_ready_to_apply": True,
                 }), \
                 patch.object(tracker, "run_control_coordinator_tick") as tick:
                tracker.start_coordinator_daemon(
                    env_path=".env", poll_interval_sec=0.01, stop_when_idle=False,
                )
                time.sleep(0.12)
                tracker.stop_coordinator_daemon()
                self.assertEqual(tick.call_count, 1)


class CancellationApiTests(unittest.TestCase):
    def test_csrf_and_explicit_intent_required(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from src.control_center_backend import register_control_center_routes
        app = FastAPI()
        register_control_center_routes(app, "known-csrf")
        client = TestClient(app)
        self.assertEqual(client.post("/control/cancel-pending-stop", json={
            "csrf_token": "wrong", "confirm_keep_running": True,
        }).status_code, 403)
        self.assertEqual(client.post("/control/cancel-pending-stop", json={
            "csrf_token": "known-csrf", "confirm_keep_running": False,
        }).status_code, 422)
        with tempfile.TemporaryDirectory() as tmp:
            with patch("src.control_center_backend._config_and_paths", return_value=(
                     SimpleNamespace(data_dir=tmp), os.path.join(tmp, ".env"), tmp)), \
                 patch.object(ConfigChangeTracker, "cancel_deferred_owner_stop",
                              return_value={"ok": True, "cancelled": True}) as action:
                response = client.post("/control/cancel-pending-stop", json={
                    "csrf_token": "known-csrf", "confirm_keep_running": True,
                })
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["cancelled"], True)
                action.assert_called_once()


class ThreePositionServiceControlTests(unittest.TestCase):
    """Exact parent/child IDs and retained owner lots; fake broker only."""

    def test_vedpower_closed_yesbank_acks_preserved_during_service_preflight(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from src.config import AppConfig, TradingMode
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "data"
            data.mkdir()
            env = root / ".env"
            env.write_text("TRADING_MODE=LIVE\nDHAN_CLIENT_ID=FAKE\n")
            stop = data / "emergency_stop.json"
            stop.write_text(json.dumps({"is_stopped": False, "in_flight_orders": []}))
            path = str(data / "trade_journal.db")
            JournalDB(path, account_id="FAKE")
            parents = [
                ("221261102657708", "YESBANK-EQ", "11915", 15.52, "UNPROTECTED_MANUAL_RISK"),
                ("23126110439808", "YESBANK-EQ", "11915", 15.51, "UNPROTECTED_MANUAL_RISK"),
                ("321261120279408", "VEDPOWER-EQ", "750518", 34.69, "POSITION_CLOSED"),
            ]
            with sqlite3.connect(path) as con:
                for oid, symbol, security, price, state in parents:
                    now = time.time()
                    con.execute("""INSERT INTO order_intents (
                        correlation_id,account_id,symbol,security_id,exchange_segment,product_type,
                        order_type,price,quantity,state,broker_order_id,created_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (oid,"FAKE",symbol,security,"NSE_EQ","CNC","LIMIT",
                         price,1,"TRADED",oid,now-86400,now))
                    con.execute("""INSERT INTO orders (
                        correlation_id,broker_order_id,trading_mode,product_type,exchange_segment,
                        symbol,security_id,order_type,price,quantity,filled_quantity,status,
                        created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (oid,oid,"LIVE","CNC","NSE_EQ",symbol,security,"LIMIT",price,1,
                         1,"TRADED",now-86400,now))
                    watcher = dict(order_id=oid,correlation_id=oid,state="TRADED",symbol=symbol,
                        security_id=security,segment="NSE_EQ",product_type="CNC",
                        filled_qty=1,protected_qty=0,position_state=state,
                        exit_filled_qty=1 if state=="POSITION_CLOSED" else 0)
                    con.execute("INSERT INTO watcher_orders VALUES (?,?,?,?)",
                                (oid,1,json.dumps(watcher),now))
                    con.execute("""INSERT INTO positions_lifecycle (
                        parent_order_id,account_id,symbol,security_id,exchange_segment,
                        product_type,protection_route,position_state,entry_qty,buy_filled_qty,
                        sell_filled_qty,confirmed_open_qty,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (oid,"FAKE",symbol,security,"NSE_EQ","CNC",
                         "FOREVER_OCO" if security=="750518" else "MANUAL",
                         state,1,1,1 if security=="750518" else 0,
                         0 if security=="750518" else 1,now))
                    con.execute("""INSERT INTO fills (
                        broker_order_id,exchange_trade_id,symbol,traded_price,traded_quantity,
                        traded_at,transaction_type,security_id,exchange_segment,product_type,
                        parent_order_id,is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (oid,oid+"-BUY",symbol,price,1,now-86400,"BUY",
                         security,"NSE_EQ","CNC",oid,1))
                    if security=="11915":
                        con.execute("""INSERT INTO manual_risk_acknowledgements (
                            parent_order_id,correlation_id,account_id,symbol,security_id,quantity,
                            acknowledged_by,reason,status,acknowledged_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?)""",
                            (oid,oid,"FAKE",symbol,security,1,"owner","retain",
                             "UNPROTECTED_MANUAL_RISK",now-3600))
                con.execute("""INSERT INTO fills (
                    broker_order_id,exchange_trade_id,symbol,traded_price,traded_quantity,
                    traded_at,transaction_type,security_id,exchange_segment,product_type,
                    parent_order_id,is_verified_trade) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    ("32126112357408","600110497","VEDPOWER-EQ",35.06,1,time.time(),
                     "SELL","750518","NSE_EQ","CNC","321261120279408",1))
                con.execute("""INSERT INTO protection_orders (
                    parent_order_id,forever_order_id,symbol,sl_trigger,sl_limit,target_price,
                    quantity,status,created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                    ("321261120279408","23132611201560","VEDPOWER-EQ",
                     34.34,33.99,35.04,1,"POSITION_CLOSED_LOCAL",time.time()))
                before = con.execute("""SELECT parent_order_id,quantity,status
                    FROM manual_risk_acknowledgements ORDER BY parent_order_id""").fetchall()

            exchange_time=datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
            child=dict(orderId="32126112357408",algoId="23132611201560",
                dhanClientId="FAKE",exchangeSegment="NSE_EQ",productType="CNC",
                securityId="750518",transactionType="SELL",orderStatus="TRADED",
                quantity=1,filledQty=1,remainingQuantity=0)
            forever=dict(orderId="23132611201560",orderStatus="TRIGGERED",
                legName="TARGET_LEG",orderType="OCO",quantity=1,price=35.04,
                transactionType="SELL",dhanClientId="FAKE",exchangeSegment="NSE_EQ",
                productType="CNC",securityId="750518")
            trade=dict(orderId="32126112357408",exchangeTradeId="600110497",
                dhanClientId="FAKE",securityId="750518",exchangeSegment="NSE_EQ",
                productType="CNC",transactionType="SELL",tradedQuantity=1,
                tradedPrice=35.06,exchangeTime=exchange_time)
            broker=MagicMock()
            broker.cfg=AppConfig(trading_mode=TradingMode.LIVE,dhan_client_id="FAKE",
                                 data_dir=str(data))
            broker.get_order_book.return_value=[child]
            broker.get_forever_orders.return_value=[forever]
            broker.get_super_orders.return_value=[]
            broker.get_positions.return_value=[dict(dhanClientId="FAKE",securityId="750518",
                exchangeSegment="NSE_EQ",productType="CNC",netQty=-1)]
            broker.get_holdings.return_value=[dict(securityId="750518",
                totalQty=1,availableQty=0,dpQty=0,t1Qty=0)]
            broker.get_trade_book.return_value=[trade]
            broker.order_status.return_value=None
            tracker=ConfigChangeTracker(data_dir=str(data))
            outcome=tracker.reconcile_stale_journal_with_broker(
                db_path=path,broker=broker,env_path=str(env),is_live_mode=True,
                is_service_running=True,stop_state_path=str(stop))
            self.assertTrue(outcome[0], outcome)
            with sqlite3.connect(path) as con:
                after=con.execute("""SELECT parent_order_id,quantity,status
                    FROM manual_risk_acknowledgements ORDER BY parent_order_id""").fetchall()
                self.assertEqual(before,after)
                remaining=con.execute("""SELECT confirmed_open_qty FROM positions_lifecycle
                    WHERE parent_order_id='321261120279408'""").fetchone()
                self.assertEqual(remaining[0],0)
            broker.place_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
