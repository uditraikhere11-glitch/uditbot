"""PR94: PR93 production-safety regressions; synthetic brokerage/Telegram only."""
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
import requests

from src.broker.dhan_client import DhanClient, _is_local_mutation_deferral, BASE_URL
from src.broker.read_governor import ReadGovernor, ReadDeferred
from src.engine.broker_handoff import BrokerMutationNotSubmitted, guarded_request
from src.engine.reconciliation import StartupReconciliationGate
from src.engine.protection_watcher import _is_ambiguous_message
from src.journal.db import JournalDB
from src.main import _is_transport_uncertainty
from src.models import TradingMode, ProtectionLevels
from src.notifier.outbox_worker import OutboxWorker, TelegramRetryAfter
from src.notifier.telegram_notifier import TelegramNotifier


def local_deferral(code=503):
    r = requests.Response()
    r.status_code = code
    r._content = b"Active broker mutation state unknown: local configuration fence"
    r.headers["X-AuditBot-Local-Deferral"] = "1"
    return r


def light_client():
    c = object.__new__(DhanClient)
    c.cfg = NS(trading_mode=TradingMode.LIVE, dhan_client_id="FAKE", data_dir="/unused")
    c._headers = Mock(return_value={})
    c.session = NS(post=Mock(), put=Mock(), delete=Mock())
    c._halt_for_read_deferral = Mock()
    return c


class NotSubmittedTests(unittest.TestCase):
    def test_explicit_not_submitted_cannot_become_unknown(self):
        for msg in (
            "MUTATION NOT_SUBMITTED (config fence): unknown state 503",
            "MUTATION NOT_SUBMITTED (local admission): timeout in SQLite",
            "MUTATION NOT_SUBMITTED (local admission): connection failed",
        ):
            self.assertFalse(_is_transport_uncertainty(msg))
            self.assertFalse(_is_ambiguous_message(msg))
        self.assertTrue(_is_transport_uncertainty("Dhan HTTP 503 server error outcome UNKNOWN"))
        self.assertTrue(_is_ambiguous_message("Broker HTTP exception after transmission"))

    def test_local_oco_deferral_never_returns_remote_unknown(self):
        c = light_client()
        c._broker_mutating_http = Mock(return_value=local_deferral())
        levels = ProtectionLevels(
            executed_price=100.0, sl_trigger=97.0, sl_limit=96.0,
            target_price=103.0, target_trigger=103.0, quantity=1,
        )
        ok, oid, msg = c.place_forever_oco(
            correlation_id="CORR", security_id="11915", exchange_segment="NSE_EQ",
            product_type="CNC", levels=levels, max_protectable_qty=1,
        )
        self.assertFalse(ok)
        self.assertIsNone(oid)
        self.assertIn("MUTATION NOT_SUBMITTED", msg)
        self.assertNotIn("outcome UNKNOWN", msg)
        c.session.post.assert_not_called()

    def test_local_super_modification_returns_deferred(self):
        c = light_client()
        c._broker_mutating_http = Mock(return_value=local_deferral())
        ok, msg, info = c.modify_super_order_leg("ORDER123","STOP_LOSS_LEG", 98.0)
        self.assertFalse(ok)
        self.assertEqual("DEFERRED", info["state"])
        self.assertIn("MUTATION NOT_SUBMITTED", msg)
        c.session.put.assert_not_called()

    def test_all_cancels_are_local_not_submitted(self):
        c = light_client()
        c._broker_mutating_http = Mock(return_value=local_deferral())
        for fn in (
            lambda: c.cancel_order("ORDER123"),
            lambda: c.cancel_super_order_leg("ORDER123","ENTRY_LEG"),
            lambda: c.cancel_forever_order("ORDER123"),
        ):
            ok,msg=fn()
            self.assertFalse(ok)
            self.assertIn("MUTATION NOT_SUBMITTED",msg)
        c.session.delete.assert_not_called()

    def test_fence_denial_cannot_burn_modification_admission(self):
        c = light_client()
        governor = Mock()
        c._get_active_read_governor = Mock(return_value=governor)
        with patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "1"}):
            with patch("src.engine.broker_handoff.guarded_request",
                       side_effect=BrokerMutationNotSubmitted("ledger missing")):
                with self.assertRaises(BrokerMutationNotSubmitted):
                    c._broker_mutating_http("PUT", BASE_URL + "/orders/ORD123",timeout=5)
        governor.admit.assert_not_called()
        c.session.put.assert_not_called()

    def test_admission_failure_settles_v2_ledger_without_http(self):
        c = light_client()
        c._get_active_read_governor = Mock(return_value=NS(
            admit=Mock(side_effect=ReadDeferred(4,"synthetic rate limit")),
            record_success=Mock(), throttled=Mock(),
        ))
        finished = []

        @contextmanager
        def synthetic_fence(*args):
            def finish(known):
                finished.append(known)
            yield finish

        with patch("src.engine.broker_handoff.broker_mutation", synthetic_fence):
            with patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "1"}):
                result = c._broker_mutating_http("POST", BASE_URL + "/orders",timeout=5)
        self.assertEqual(429, result.status_code)
        self.assertTrue(_is_local_mutation_deferral(result))
        self.assertEqual([True], finished)
        c.session.post.assert_not_called()


class BrokerCorrelationTests(unittest.TestCase):
    def test_duplicate_corr_id_never_falls_back_to_broker_oid(self):
        gate = object.__new__(StartupReconciliationGate)
        broker = NS(
            get_order_by_correlation_id=Mock(return_value=None),
            order_status=Mock(return_value={"orderId":"O1","orderStatus":"TRADED"}),
            cfg=NS(dhan_client_id="FAKE"),
        )
        gate.broker = broker
        gate.db = NS(account_id="FAKE")
        records = [
            {"correlationId":"DUP","orderId":"O1","securityId":"11915"},
            {"correlationId":"DUP","orderId":"O2","securityId":"11915"},
        ]
        self.assertIsNone(gate._lookup_intent_at_broker(
            "DUP","O1",records,{"account_id":"FAKE","security_id":"11915"}))
        broker.order_status.assert_not_called()

    def test_rejected_order_without_filled_qty_keeps_existing_contract(self):
        gate = object.__new__(StartupReconciliationGate)
        gate.broker = NS(cfg=NS(dhan_client_id="FAKE"))
        gate.db = NS(account_id="FAKE")
        ok, _ = gate._validate_broker_order_matches_intent(
            {"orderId":"111","orderStatus":"REJECTED"},
            {"account_id":"FAKE","security_id":"11915"},
            "CORR","111",
        )
        self.assertTrue(ok)  # PR93 main never required filledQty for REJECTED.


class FloodWaitTests(unittest.TestCase):
    def test_claimed_batch_defers_all_without_spamming_or_dead_letter(self):
        with tempfile.TemporaryDirectory() as root:
            db = JournalDB(str(Path(root)/"test.db"),account_id="FAKE")
            for i in range(3):
                db.enqueue_outbox("LOG", {"text":f"item {i}"})
            dispatch = Mock(side_effect=[TelegramRetryAfter(120),True,True,True])
            worker = OutboxWorker(db,dispatch)
            self.assertEqual(0,worker.process_pending_events(limit=10,now=100.0))
            self.assertEqual(1,dispatch.call_count)
            self.assertEqual(0,worker.process_pending_events(limit=10,now=150.0))
            rows = db.fetch_pending_outbox(limit=10,now=221.0)
            self.assertEqual(3,len(rows))
            self.assertTrue(all(float(r["next_retry_at"]) >= 220.0 for r in rows))
            self.assertTrue(all(int(r["retry_count"]) == 0 for r in rows))
            self.assertEqual(3,worker.process_pending_events(limit=10,now=221.0))
            self.assertEqual(4,dispatch.call_count)
            self.assertEqual([],db.fetch_pending_outbox(limit=10,now=222.0))

    def test_bot_api_flood_wait_does_not_send_second_request(self):
        notifier = TelegramNotifier(NS(tg_bot_token="FAKE",log_group_id="-1"))
        resp = NS(status_code=429,json=lambda: {"parameters":{"retry_after":37}},
                  headers={},text="rate limited")
        with patch("src.notifier.telegram_notifier.requests.post",
                   return_value=resp) as send:
            with self.assertRaises(TelegramRetryAfter) as ex:
                notifier.send_log_message("some test text")
        self.assertEqual(37,ex.exception.seconds)
        send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
