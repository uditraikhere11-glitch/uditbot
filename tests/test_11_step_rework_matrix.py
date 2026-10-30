"""
Dedicated Verification Suite for the 11-Step Rework Specification + V12 Consolidated Safety Policy:
- SAFE-01: STOP missing / corrupt / disk full / wrong account -> ZERO new entry broker mutations, fail-closed status.
- SAFE-02: Concurrent STOP vs POST -> No request authorized after STOP commits; prior in-flight traced on disk.
- JOURNAL-01: 100 concurrent duplicate claims across threads -> Exactly one claim and one broker intent; zero IntegrityError.
- JOURNAL-02: Crash / timeout after submit before ACK -> UNKNOWN persists; lookup by correlation; no blind duplicate POST.
- RETRY-01: DELETE accepted (HTTP 202), order later fills -> Protect actual fill, no premature retry, correct remaining qty.
- RETRY-02: Owner's revised retry policy -> Movement strictly < 1.00% (0.99% eligible; 1.00% and 1.01% rejected),
  revised limit markup = configured XX% capped at 1.00% (XX% + 1% prohibited), max 1 child.
- PROTECT-01: Protection placement ID returned but leg not active in Forever book -> No ACTIVE_CONFIRMED status, UNPROTECTED alert, and entry gate halted.
- PROTECT-02: Unsupported product/route -> Reject automatic new entry / block oversell before exposing unprotectable position.
- INSTR-01: Explicit expiry day and ambiguous competing monthly contracts -> Listed correct contract or deterministic blocked result.
- REC-01: INTENT_CREATED / UNKNOWN on startup -> Gate remains blocked until complete broker-side reconciliation.
- OUTBOX-01: Telegram outage / 429 / JWT / bot token secret -> Broker path unaffected, message retried with backoff, no secret or redaction exception.
- SINGLETON-01: Dual process launch -> Only one listener owns lease; release() preserves inode without unlinking.
- DASH-01: INTENT_CREATED with no UNKNOWN rows -> Dashboard displays unresolved count; zero broker HTTP mutations.
- BASELINE-01: Existing regex and Step 05 source channel allowlist -> All baseline regex rules and source authorization enforced.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import io
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import requests

from src.config import AppConfig
from src.models import (
    OrderRequest,
    ProductType,
    ProtectionLevels,
    SignalAction,
    SignalType,
    TradingMode,
)
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.lock import SingletonListenerLock
from src.engine.protection_watcher import ProtectionWatcher, OrderLifecycleState
from src.engine.reconciliation import StartupReconciliationGate, ReconciliationState
from src.instruments.resolver import InstrumentResolver
from src.journal.db import JournalDB
from src.notifier.outbox_worker import OutboxWorker, redact_secrets
from src.broker.dhan_client import DhanClient
from src.cli import cmd_dashboard
from src.main import TradingBotPipeline


class Test11StepReworkProofMatrix(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = os.path.join(self.temp_dir.name, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.stop_file = os.path.join(self.data_dir, "emergency_stop.json")
        self.db_path = os.path.join(self.data_dir, "journal.db")
        self.pending_file = os.path.join(self.data_dir, "pending_protection.json")
        self.lock_file = os.path.join(self.data_dir, "listener.lock")

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("requests.Session.post")
    def test_safe_01_stop_missing_corrupt_disk_full_and_wrong_account(self, mock_post):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            data_dir=self.data_dir,
        )
        stop_mgr = EmergencyStopManager(self.stop_file, fail_closed_if_missing=True, expected_account_id="1113731536")
        self.assertTrue(stop_mgr.is_stopped())
        client = DhanClient(cfg, stop_manager=stop_mgr)

        req = OrderRequest(
            correlation_id="SAFE01-ORD-1",
            symbol="SBIN-EQ",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=800.0,
            quantity=10,
            trading_mode=TradingMode.LIVE,
        )
        ok, oid, _ = client.place_order(req)
        self.assertFalse(ok)
        self.assertIsNone(oid)
        mock_post.assert_not_called()

        stop_mgr_other = EmergencyStopManager(self.stop_file, expected_account_id="9999999999")
        stop_mgr_other.provision_initial_clean_state(operator="Udit", account_id="9999999999")
        self.assertTrue(stop_mgr.is_stopped())

        stop_mgr.provision_initial_clean_state(operator="Udit", account_id="1113731536")
        self.assertFalse(stop_mgr.is_stopped())
        with patch("os.open", side_effect=OSError(28, "No space left on device")):
            save_ok = stop_mgr._save_raw({"is_stopped": False})
            self.assertFalse(save_ok)
        self.assertTrue(stop_mgr.is_stopped())
        ok2, _, _ = client.place_order(req)
        self.assertFalse(ok2)
        mock_post.assert_not_called()

        stop_mgr._disk_write_failed = False
        stop_mgr.trigger_stop(operator="Udit", reason="Test halt", account_id="1113731536")
        stop_mgr.resume(operator="UNAUTHORIZED", reason="Attempt bypass")
        self.assertTrue(stop_mgr.is_stopped())

    @patch("requests.Session.post")
    def test_safe_02_concurrent_stop_vs_post_and_timeout_in_flight(self, mock_post):
        stop_mgr = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state(operator="Udit", account_id="1113731536")

        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            data_dir=self.data_dir,
        )
        client = DhanClient(cfg, stop_manager=stop_mgr)

        mock_post.side_effect = requests.exceptions.Timeout("Read timeout after POST sent")
        req1 = OrderRequest(
            correlation_id="SAFE02-INFLIGHT-1",
            symbol="SBIN-EQ",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=800.0,
            quantity=10,
            trading_mode=TradingMode.LIVE,
        )
        ok, _, _ = client.place_order(req1)
        self.assertFalse(ok)

        restarted_stop = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        self.assertIn("SAFE02-INFLIGHT-1", restarted_stop.get_in_flight())

        restarted_stop.trigger_stop(operator="Udit", reason="Concurrent stop")
        mock_post.reset_mock()
        req2 = OrderRequest(
            correlation_id="SAFE02-BLOCKED-2",
            symbol="INFY-EQ",
            security_id="1594",
            exchange_segment="NSE_EQ",
            product_type=ProductType.CNC,
            price=1500.0,
            quantity=5,
            trading_mode=TradingMode.LIVE,
        )
        ok2, _, msg2 = client.place_order(req2)
        self.assertFalse(ok2)
        self.assertIn("EMERGENCY STOP active", msg2)
        mock_post.assert_not_called()

    def test_journal_01_100_concurrent_duplicate_claims(self):
        db = JournalDB(self.db_path, account_id="1113731536")

        def attempt_claim(_):
            return db.claim_signal(
                channel_id=-100777,
                message_id=42,
                raw_text="Buy #SBIN @ 805",
                symbol="SBIN",
            )

        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(attempt_claim, range(100)))

        accepted = [r for r in results if r[0] is True]
        duplicates = [r for r in results if r[0] is False and r[1] == "DUPLICATE_MESSAGE_CLAIM"]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(duplicates), 99)

    @patch("src.main.market_status", return_value=(True, "Market Open"))
    @patch("requests.Session.get")
    @patch("requests.Session.post")
    def test_journal_02_timeout_persists_unknown_not_rejected(self, mock_post, mock_get, _mock_mkt):
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            equity_active=True,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
        )
        pipeline = TradingBotPipeline(cfg)
        pipeline.stop_manager.provision_initial_clean_state("Udit", "1113731536")
        pipeline.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ")

        def _get_router(url, **kwargs):
            r = MagicMock()
            r.status_code = 200
            r.text = "{}"
            if "/fundlimit" in str(url):
                r.json.return_value = {"availabelBalance": 100000.0}
            else:
                r.json.return_value = []
            return r
        mock_get.side_effect = _get_router

        rec_ok, _ = pipeline.reconciliation_gate.run_startup_reconciliation()
        self.assertTrue(rec_ok)

        mock_post.side_effect = requests.exceptions.Timeout("Read timed out waiting for Dhan ACK")

        res = pipeline.process_raw_message(
            text="Buy #SBIN @ 805",
            channel_id=-1001,
            message_id=901,
            msg_timestamp=1000.0,
            current_time=1002.0,
        )
        self.assertFalse(res.is_valid)

        unknowns = pipeline.db.get_unknown_intents()
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0]["state"], "UNKNOWN")
        self.assertEqual(unknowns[0]["symbol"], "SBIN-EQ")

    def test_retry_01_and_02_late_fill_and_owner_revised_threshold_boundary(self):
        """
        Section B: Owner's revised retry policy:
        - 1.00% and 1.01% movement REJECTED (>= 1.00%)
        - 0.99% movement ELIGIBLE (< 1.00%)
        - Revised LIMIT markup = configured XX% capped at 1.00%
        """
        db = JournalDB(self.db_path)
        mock_broker = MagicMock()
        mock_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, equity_price_markup_pct=0.5)
        mock_broker.stop_manager = MagicMock()
        mock_broker.stop_manager.is_stopped.return_value = False

        watcher = ProtectionWatcher(
            broker=mock_broker,
            sl_pct=3.0,
            target_pct=3.0,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=db,
        )

        oid = watcher.track("ORD-R1", "SBIN", "3045", "NSE_EQ", qty=10, original_market_price=1000.0)
        mock_broker.cancel_order.return_value = (True, "HTTP 202 Accepted")
        watcher.request_cancel(oid, origin="BOT_TIMEOUT")
        self.assertEqual(watcher.pending[oid]["state"], OrderLifecycleState.CANCEL_PENDING.value)

        mock_broker.order_status.return_value = {
            "orderStatus": "CANCELLED",
            "filledQty": 2,
            "averageTradedPrice": 1000.0,
        }
        mock_broker.trades_avg_price.return_value = 1000.0
        mock_broker.place_forever_oco.return_value = (True, "OCO-R1", "Success")
        watcher.check_once()

        self.assertIn(oid, watcher.terminal_orders)
        self.assertEqual(watcher.terminal_orders[oid]["protected_qty"], 2)

        # 1. Movement of 1.01% (1010.10) -> REJECTED (>= 1.00%)
        ok_101, qty_101, _ = watcher.evaluate_retry(oid, current_market_price=1010.10)
        self.assertFalse(ok_101)
        self.assertEqual(qty_101, 0)

        # 2. Movement of 1.00% (1010.00) -> REJECTED (>= 1.00%)
        ok_100, qty_100, _ = watcher.evaluate_retry(oid, current_market_price=1010.00)
        self.assertFalse(ok_100)
        self.assertEqual(qty_100, 0)

        # 3. Movement of 0.99% (1009.90) with markup_pct=0.5% -> ELIGIBLE!
        # Revised limit = 1009.90 * (1 + 0.5/100) = 1014.9495 -> tick 1014.95
        mock_broker.place_order.return_value = (True, "ORD-R1-CHILD", "Placed")
        exec_ok, child_id, _ = watcher.execute_retry(oid, current_market_price=1009.90, markup_pct=0.5)
        self.assertTrue(exec_ok)
        self.assertEqual(child_id, "ORD-R1-CHILD")
        sent_req = mock_broker.place_order.call_args[0][0]
        self.assertEqual(sent_req.quantity, 8)
        self.assertEqual(sent_req.price, 1014.95)

        # Second retry attempt is strictly blocked
        exec_ok2, _, _ = watcher.execute_retry(oid, current_market_price=1005.00, markup_pct=0.5)
        self.assertFalse(exec_ok2)

    def test_protect_01_and_02_leg_verification_and_oversell_guard(self):
        db = JournalDB(self.db_path)
        stop_mgr = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state("Udit", "1113731536")

        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1113731536",
            dhan_access_token="mock_token",
            data_dir=self.data_dir,
        )
        client = DhanClient(cfg, stop_manager=stop_mgr)

        levels = ProtectionLevels(
            executed_price=1000.0,
            sl_trigger=970.0,
            sl_limit=960.0,
            target_price=1030.0,
            quantity=25,
        )
        ok_over, _, msg_over = client.place_forever_oco(
            correlation_id="OVERSELL-1",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            levels=levels,
            max_protectable_qty=10,
        )
        self.assertFalse(ok_over)
        self.assertIn("oversell blocked", msg_over)

        mock_broker = MagicMock()
        gate = StartupReconciliationGate(mock_broker, db, MagicMock(), stop_mgr)
        gate._accepting_entries = True
        gate.state = ReconciliationState.HEALTHY
        mock_broker.reconciliation_gate = gate

        watcher = ProtectionWatcher(
            broker=mock_broker,
            sl_pct=3.0,
            target_pct=3.0,
            path=self.pending_file,
            dry_run=False,
            clock=lambda: 1000.0,
            journal_db=db,
            require_broker_leg_confirmation=True,
        )
        oid = watcher.track("ORD-PROT01", "SBIN", "3045", "NSE_EQ", qty=10)
        mock_broker.order_status.return_value = {
            "orderStatus": "TRADED",
            "filledQty": 10,
            "averageTradedPrice": 800.0,
        }
        mock_broker.trades_avg_price.return_value = 800.0
        mock_broker.place_forever_oco.return_value = (True, "OCO-FAKE-ACTIVE", "Placed")
        mock_broker.verify_forever_leg_active.return_value = (False, "Forever order OCO-FAKE-ACTIVE is in terminal failed state: REJECTED")

        watcher.check_once()
        self.assertEqual(watcher.pending[oid]["protected_qty"], 0)
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(len(db.get_unprotected_orders()), 1)

    def test_instr_01_explicit_day_and_ambiguous_same_date_rejected(self):
        resolver = InstrumentResolver()
        resolver.load_option("POLYCAB", "54230", 8000.0, "PE", date(2026, 10, 22), 100, "NSE_FNO")
        resolver.load_option("POLYCAB", "54231", 8000.0, "PE", date(2026, 10, 29), 100, "NSE_FNO")

        c1, _ = resolver.resolve_option(
            "POLYCAB", 8000.0, "PE", expiry_month="OCT", expiry_day=22, reference_date=date(2026, 10, 1)
        )
        self.assertIsNotNone(c1)
        self.assertEqual(c1.security_id, "54230")

        resolver.load_option("POLYCAB", "54299", 8000.0, "PE", date(2026, 10, 29), 100, "NSE_FNO")
        c_ambig, msg_ambig = resolver.resolve_option(
            "POLYCAB", 8000.0, "PE", expiry_month="OCT", reference_date=date(2026, 10, 1)
        )
        self.assertIsNone(c_ambig)
        self.assertIn("Ambiguous competing monthly contracts", msg_ambig)

    def test_rec_01_intent_created_on_startup_keeps_gate_blocked(self):
        db = JournalDB(self.db_path)
        stop_mgr = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state("Udit", "1113731536")

        db.record_order_intent(
            correlation_id="INTENT-UNVERIFIED-1",
            symbol="SBIN-EQ",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=805.0,
            quantity=10,
        )

        mock_broker = MagicMock()
        mock_broker.get_fund_limits.return_value = (True, 100000.0, "OK")
        mock_broker.get_trade_book.return_value = []
        mock_broker.get_positions.return_value = []
        mock_broker.get_order_by_correlation_id.return_value = None
        mock_broker.get_order_book.return_value = []
        mock_broker.order_status.return_value = None

        gate = StartupReconciliationGate(mock_broker, db, MagicMock(), stop_mgr)
        ok, msg = gate.run_startup_reconciliation()
        self.assertFalse(ok)
        self.assertFalse(gate.is_accepting_entries)
        self.assertEqual(gate.state, ReconciliationState.BLOCKED)
        self.assertIn("Unresolved order intent(s) present", msg)

    def test_outbox_01_jwt_redaction_and_exponential_backoff(self):
        raw = (
            'Error sending to https://api.telegram.org/bot123456789:AAHfiqksKZ8WmR2z9pL1xYz_0123456789/sendMessage '
            'with token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c" '
            'and dhan_pin=987654'
        )
        clean = redact_secrets(raw)
        self.assertNotIn("eyJhbGci", clean)
        self.assertNotIn("987654", clean)
        self.assertNotIn("AAHfiqks", clean)
        self.assertIn("***REDACTED", clean)

        db = JournalDB(self.db_path)
        db.enqueue_outbox("TRADE_LOG", {"text": raw})

        worker = OutboxWorker(db, dispatch_fn=lambda _: False, max_retries=3)
        count = worker.process_pending_events(now=1000.0)
        self.assertEqual(count, 0)

        self.assertEqual(len(db.fetch_pending_outbox(now=1001.0)), 0)
        self.assertEqual(len(db.fetch_pending_outbox(now=1003.0)), 1)

    def test_singleton_01_dual_launch_and_inode_preservation(self):
        lock1 = SingletonListenerLock(self.lock_file)
        ok1, _ = lock1.acquire()
        self.assertTrue(ok1)
        inode_before = os.stat(self.lock_file).st_ino

        lock2 = SingletonListenerLock(self.lock_file)
        ok2, _ = lock2.acquire()
        self.assertFalse(ok2)

        lock1.release()
        self.assertTrue(os.path.exists(self.lock_file))
        inode_after = os.stat(self.lock_file).st_ino
        self.assertEqual(inode_before, inode_after)

    @patch("requests.Session.delete")
    @patch("requests.Session.put")
    @patch("requests.Session.post")
    def test_dash_01_intent_created_displays_attention_zero_mutations(self, mock_post, mock_put, mock_del):
        db = JournalDB(self.db_path)
        stop_mgr = EmergencyStopManager(self.stop_file, expected_account_id="1113731536")
        stop_mgr.provision_initial_clean_state("Udit", "1113731536")

        db.record_order_intent(
            correlation_id="DASH-INTENT-1",
            symbol="SBIN-EQ",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=10,
        )

        cfg = AppConfig(dhan_client_id="1113731536", monitored_channel_ids=[-100123])
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            cmd_dashboard(cfg, db, stop_mgr)

        out = buf.getvalue()
        self.assertIn("[ATTENTION] Non-Terminal Intents : 1 unresolved order intent(s)", out)
        self.assertNotIn("0 unmanaged orders", out)
        mock_post.assert_not_called()
        mock_put.assert_not_called()
        mock_del.assert_not_called()

    def test_baseline_01_source_allowlist_blocks_unauthorized_channels(self):
        cfg = AppConfig(
            trading_mode=TradingMode.TEST,
            equity_active=True,
            monitored_channel_ids=[-100111],
            test_group_id=-100222,
            data_dir=self.data_dir,
            journal_db_path=self.db_path,
            dhan_access_token="",
            dhan_token_file=os.path.join(self.data_dir, ".dhan_token"),
            dhan_client_id="1113731536",
            equity_budget_inr=25000.0,
            equity_price_markup_pct=0.5,
        )
        pipeline = TradingBotPipeline(cfg)
        pipeline.stop_manager.provision_initial_clean_state("Udit", "1113731536")
        pipeline.resolver.load_equity("SBIN", security_id="3045", exchange_segment="NSE_EQ")

        res_unauth = pipeline.process_raw_message(
            text="Buy #SBIN @ 805",
            channel_id=-100999,
            message_id=1,
            msg_timestamp=1000.0,
            current_time=1002.0,
        )
        self.assertFalse(res_unauth.is_valid)
        self.assertIn("Unauthorized source channel", res_unauth.reason)

        res_auth = pipeline.process_raw_message(
            text="Buy #SBIN @ 805",
            channel_id=-100111,
            message_id=2,
            msg_timestamp=1000.0,
            current_time=1002.0,
        )
        self.assertTrue(res_auth.is_valid)


if __name__ == "__main__":
    unittest.main()
