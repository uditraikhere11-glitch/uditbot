"""Offline recovery notice replay with isolated durable journal and token files."""
import datetime
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from src.broker.dhan_token import write_token_atomic
from src.engine.dhan_token_recovery_notice import (
    EVENT, RuntimeTokenRecoveryNotice, fingerprint, update_scheduled_recovery,
)
from src.journal.db import JournalDB


class TokenRecoveryNoticeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.cfg = NS(data_dir=str(self.root), dhan_token_file=str(self.root / "token"),
                      dhan_client_id="SYNTHETIC1536", instance_name="OfflineBot",
                      trading_mode="LIVE", log_group_id=123, scrip_master_max_age_sec=86400)
        self.now = datetime.datetime(2026, 11, 25, 17, 2,
                                     tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
        self.expiry = self.now + 86400
        self.profile = dict(identity_status="VERIFIED", identity_verified=True,
                            profile_client_id=self.cfg.dhan_client_id, token_expired=False,
                            token_expiration_ts=self.expiry,
                            token_expiration_str="2026-11-26 17:02:00 IST")
        self.broker = NS(
            verify_profile_and_token_expiry=Mock(return_value=self.profile),
            _governed_get=Mock(return_value=NS(status_code=200, json=lambda: {
                "dhanClientId": self.cfg.dhan_client_id, "availableBalance": 949.20})),
            stop_manager=NS(is_stopped=lambda: False),
        )
        self.gate = NS(is_accepting_entries=True)
        self.db = JournalDB(str(self.root / "test.db"), account_id=self.cfg.dhan_client_id)
        self.worker = RuntimeTokenRecoveryNotice(self.cfg, self.broker, self.db, self.gate)
        self.marker = self.root / "dhan_token_recovery_pending.json"
        cal = patch("src.engine.market.load_holidays_verified",
                    return_value=(True, {datetime.date(2026, 1, 26)}, "offline"))
        master = patch("src.instruments.resolver.InstrumentResolver.load_dhan_master",
                       return_value=True)
        cal.start()
        master.start()
        self.addCleanup(cal.stop)
        self.addCleanup(master.stop)

    def scheduled(self, renewed=False, ready=False):
        update_scheduled_recovery(
            self.cfg, renewed=renewed, ready=ready, token_problem=not ready,
            prior_fingerprint=fingerprint("old"), prior_expiration="2026-11-25 17:00 IST",
            current_token="fresh" if renewed else "old", now=self.now-120)

    def cards(self):
        with self.db._get_conn() as conn:
            return [json.loads(r[0])["text"] for r in conn.execute(
                "SELECT payload FROM outbox WHERE event_type='ORDER_STATUS_UPDATE'")]

    def recover(self):
        self.scheduled()
        write_token_atomic("fresh", self.cfg.dhan_token_file)
        return self.worker.poll(now=self.now)

    def test_failed_scheduled_then_runtime_verified_recovery_and_restart(self):
        self.assertTrue(self.recover())
        self.assertEqual(len(self.cards()), 1)
        card = self.cards()[0]
        for text in ("AUDITBOT — DHAN TOKEN RECOVERY VERIFIED", "AUTO-RECOVERED",
                     "***1536", "2026-11-26 17:02:00 IST", "949.20", "✅ READY",
                     "RECOVERED — NO ACTION REQUIRED"):
            self.assertIn(text, card)
        self.assertFalse(json.loads(self.marker.read_text())["pending"])
        self.assertEqual(json.loads(self.marker.read_text())["last_notified_token_fingerprint"],
                         fingerprint("fresh"))
        self.broker.verify_profile_and_token_expiry.assert_called_once_with(
            allow_refresh=False, candidate_token="fresh", now_ts=self.now)
        self.assertEqual(self.broker._governed_get.call_args.kwargs["headers"]["access-token"], "fresh")
        self.assertFalse(self.worker.poll(now=self.now+31))
        restarted = RuntimeTokenRecoveryNotice(self.cfg, self.broker, self.db, self.gate)
        self.assertFalse(restarted.poll(now=self.now+61))
        self.assertEqual(len(self.cards()), 1)

    def test_clean_scheduled_renewal_suppresses_recovery(self):
        self.scheduled()
        self.scheduled(renewed=True, ready=True)
        write_token_atomic("fresh", self.cfg.dhan_token_file)
        self.worker.observe_auth_recovery("old", "fresh")
        self.assertFalse(self.worker.poll(now=self.now))
        self.assertFalse(json.loads(self.marker.read_text())["pending"])
        self.assertEqual(self.cards(), [])

    def test_runtime_auth_recovery_without_scheduled_failure(self):
        write_token_atomic("fresh", self.cfg.dhan_token_file)
        self.worker.observe_auth_recovery("old", "fresh")
        self.assertTrue(self.worker.poll(now=self.now))
        self.assertEqual(len(self.cards()), 1)

    def assert_blocked(self):
        self.assertFalse(self.recover())
        self.assertTrue(json.loads(self.marker.read_text())["pending"])
        self.assertEqual(self.cards(), [])

    def test_short_expiry(self):
        self.profile.update(token_expiration_ts=self.now+60,
                            token_expiration_str="2026-11-25 17:03 IST")
        self.assert_blocked()

    def test_expiry_after_open_before_close(self):
        self.profile.update(token_expiration_ts=self.now+18*3600,
                            token_expiration_str="2026-11-26 11:02 IST")
        self.assert_blocked()

    def test_wrong_account(self):
        self.profile["profile_client_id"] = "OTHER"
        self.assert_blocked()

    def test_funds_unavailable(self):
        self.broker._governed_get.return_value.status_code = 401
        self.assert_blocked()

    def test_funds_missing_balance(self):
        self.broker._governed_get.return_value.json = lambda: {}
        self.assert_blocked()

    def test_owner_stop(self):
        self.broker.stop_manager.is_stopped = lambda: True
        self.assert_blocked()

    def test_reconciliation_blocked(self):
        self.gate.is_accepting_entries = False
        self.assert_blocked()

    def test_same_rejected_token_not_accepted(self):
        self.scheduled()
        write_token_atomic("old", self.cfg.dhan_token_file)
        self.assertFalse(self.worker.poll(now=self.now))
        self.assertEqual(self.cards(), [])

    def test_queue_failure_retains_pending(self):
        self.scheduled()
        write_token_atomic("fresh", self.cfg.dhan_token_file)
        with patch.object(self.db, "enqueue_order_notification_once", side_effect=OSError):
            self.assertFalse(self.worker.poll(now=self.now))
        self.assertTrue(json.loads(self.marker.read_text())["pending"])
        self.assertTrue(self.worker.poll(now=self.now+31))
        self.assertEqual(len(self.cards()), 1)

    def test_token_rotated_during_probe(self):
        def rotate(**kwargs):
            write_token_atomic("different", self.cfg.dhan_token_file)
            return self.profile
        self.broker.verify_profile_and_token_expiry.side_effect = rotate
        self.assert_blocked()
