"""Offline integration: scheduled red card -> runtime verified recovery outbox."""
import datetime
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from src.broker.dhan_token import write_token_atomic
from src.cli import cmd_daily_health_check
from src.engine.token_recovery_notification import (
    MARKER, TokenRecoveryConfirmation, begin_scheduled_check, fingerprint, scheduled_recovery_state,
)
from src.journal.db import JournalDB


class TokenRecoveryNoticeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cfg = NS(data_dir=str(self.root), instance_name="Synthetic UditBot",
                      dhan_client_id="SYNTHETIC1536", dhan_token_file=str(self.root / "token"),
                      log_group_id=-100123, trading_mode="LIVE", scrip_master_max_age_sec=86400)
        self.ist = ZoneInfo("Asia/Kolkata")
        self.now = datetime.datetime(2026, 11, 25, 17, 2, 31, tzinfo=self.ist).timestamp()
        self.old = "synthetic-old-token"
        self.new = "synthetic-new-token"
        self.exp = datetime.datetime(2026, 11, 26, 17, 2, 31, tzinfo=self.ist).timestamp()
        self.db = JournalDB(str(self.root / "journal.db"), account_id=self.cfg.dhan_client_id)
        self.stop = NS(get_state=lambda: {"is_stopped": False})
        self.gate = NS(is_accepting_entries=True)
        self.master = Mock(return_value=True)
        self.broker = Mock()
        self.broker.verify_profile_and_token_expiry.return_value = self.profile()
        self.broker._governed_get.return_value = NS(
            status_code=200, json=lambda: {"dhanClientId": self.cfg.dhan_client_id,
                                         "availabelBalance": 949.20})
        self.calendar = patch("src.engine.market.load_holidays_verified",
                              return_value=(True, {datetime.date(2026, 12, 25)}, "synthetic"))
        self.calendar.start()
        self.addCleanup(self.calendar.stop)
        write_token_atomic(self.old, self.cfg.dhan_token_file)
        self.worker = self.new_worker()

    def profile(self, expiry=None, account=None):
        exp = self.exp if expiry is None else expiry
        return dict(identity_verified=True, identity_status="VERIFIED",
                    profile_client_id=self.cfg.dhan_client_id if account is None else account,
                    token_expired=False, token_expiration_ts=exp,
                    token_expiration_str=datetime.datetime.fromtimestamp(exp, self.ist).strftime(
                        "%Y-%m-%d %H:%M:%S IST"))

    def new_worker(self):
        return TokenRecoveryConfirmation(self.cfg, self.broker, self.db,
                                         self.stop, self.gate, self.master)

    def state(self):
        return json.loads((self.root / MARKER).read_text())

    def cards(self):
        with self.db._get_conn() as conn:
            return [json.loads(r[0])["text"] for r in conn.execute(
                "SELECT payload FROM outbox WHERE event_type='ORDER_STATUS_UPDATE'")]

    def pending(self):
        scheduled_recovery_state(
            self.cfg, renewed=False, ready=False, token_problem=True,
            prior_token_fingerprint=fingerprint(self.old),
            active_token_fingerprint=fingerprint(self.old),
            prior_token_expiration_ist="2026-11-25 18:00:00 IST", now=self.now - 120)
        write_token_atomic(self.new, self.cfg.dhan_token_file)

    def poll(self, worker=None, mono=100):
        return (worker or self.worker).poll(now=self.now, monotonic=mono)

    def test_scheduled_failure_then_runtime_recovery_one_durable_card(self):
        # Exercise the real scheduled command, with no real TOTP or HTTP.
        old_expiry = self.now + 3600
        daily_client = Mock()
        daily_client.token = self.old
        daily_client.last_funds_is_fallback = False
        daily_client.get_fund_limits.return_value = (True, 949.20, "verified")
        daily_client.verify_profile_and_token_expiry.return_value = self.profile(old_expiry)
        with patch("src.cli.cmd_refresh_token", return_value=False), \
             patch("src.broker.dhan_client.DhanClient", return_value=daily_client) as factory, \
             patch("src.instruments.resolver.InstrumentResolver") as resolver, \
             patch("src.cli.dispatch_morning_health_report_once", return_value="QUEUED") as dispatch, \
             patch("src.cli.time.time", return_value=self.now - 120):
            factory.extract_token_expiration.return_value = (old_expiry, "2026-11-25 18:02:31 IST")
            resolver.return_value.load_dhan_master.return_value = True
            self.assertEqual(cmd_daily_health_check(self.cfg, self.db, self.stop), 1)
            self.assertIn("NOT CONFIRMED", dispatch.call_args.args[2])
        self.assertTrue(self.state()["pending"])
        self.assertFalse(self.poll())
        self.broker.verify_profile_and_token_expiry.assert_not_called()
        # Exercise real runtime coordination/publish and callback (only
        # the verified HTTP login response and subsequent GETs are mocked).
        from src.broker.dhan_client import DhanClient
        runtime = object.__new__(DhanClient)
        runtime.cfg = NS(dhan_client_id=self.cfg.dhan_client_id,
                         dhan_pin="synthetic-pin", dhan_totp_secret="synthetic-secret",
                         dhan_token_file=self.cfg.dhan_token_file)
        runtime.token = self.old
        runtime._perform_totp_http_login = Mock(return_value=(True, self.new, "verified"))
        runtime._token_recovery_confirmation = self.worker.auth_recovered
        ok, token, _ = runtime.generate_token_with_totp(force=False, rejected_token=self.old)
        self.assertTrue(ok)
        self.assertEqual(token, self.new)
        self.assertEqual(Path(self.cfg.dhan_token_file).read_text(), self.new)
        runtime._perform_totp_http_login.assert_called_once()
        self.assertTrue(self.poll())
        self.assertFalse(self.state()["pending"])
        self.assertEqual(self.state()["last_notified_token_fingerprint"], fingerprint(self.new))
        cards = self.cards()
        self.assertEqual(len(cards), 1)
        for part in ("AUDITBOT — DHAN TOKEN RECOVERY VERIFIED", "AUTO-RECOVERED",
                     "***1536", "2026-11-26 17:02:31 IST", "₹949.20",
                     "✅ READY", "RECOVERED — NO ACTION REQUIRED"):
            self.assertIn(part, cards[0])
        self.assertNotIn(self.cfg.dhan_client_id, cards[0])
        self.broker.verify_profile_and_token_expiry.assert_called_once_with(
            allow_refresh=False, candidate_token=self.new, now_ts=self.now)
        self.assertEqual(self.broker._governed_get.call_args.kwargs["headers"]["access-token"], self.new)
        self.assertFalse(self.poll(mono=200))
        restarted = self.new_worker()
        restarted.auth_recovered(self.old, self.new)
        self.assertFalse(self.poll(restarted, mono=300))
        self.assertEqual(len(self.cards()), 1)
        self.broker.generate_token_with_totp.assert_not_called()

    def test_clean_scheduled_report_clears_without_extra_card(self):
        self.pending()
        scheduled_recovery_state(
            self.cfg, renewed=True, ready=True, token_problem=False,
            prior_token_fingerprint=fingerprint(self.old),
            active_token_fingerprint=fingerprint(self.new),
            prior_token_expiration_ist="", now=self.now)
        self.worker.auth_recovered(self.old, self.new)
        self.assertFalse(self.poll())
        self.assertFalse(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_scheduled_lease_prevents_duplicate_during_green_check(self):
        self.pending()
        lease = begin_scheduled_check(self.cfg, now=self.now - 10)
        self.worker.auth_recovered(self.old, self.new)
        self.assertFalse(self.poll())
        self.broker.verify_profile_and_token_expiry.assert_not_called()
        scheduled_recovery_state(
            self.cfg, renewed=True, ready=True, token_problem=False,
            prior_token_fingerprint=fingerprint(self.old),
            active_token_fingerprint=fingerprint(self.new),
            prior_token_expiration_ist="", now=self.now, check_id=lease)
        self.assertFalse(self.poll(mono=200))
        self.assertEqual(self.cards(), [])

    def test_expired_scheduled_lease_does_not_strand_recovery(self):
        self.pending()
        begin_scheduled_check(self.cfg, now=self.now - 301)
        self.assertTrue(self.poll())
        self.assertEqual(len(self.cards()), 1)

    def test_runtime_auth_failure_recovery_without_daily_marker(self):
        write_token_atomic(self.new, self.cfg.dhan_token_file)
        self.worker.auth_recovered(self.old, self.new)
        self.assertTrue(self.poll())
        self.assertEqual(len(self.cards()), 1)

    def test_expiration_before_next_open_keeps_pending(self):
        self.pending()
        self.broker.verify_profile_and_token_expiry.return_value = self.profile(self.now + 3600)
        self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_expiration_during_next_session_keeps_pending(self):
        self.pending()
        self.broker.verify_profile_and_token_expiry.return_value = self.profile(
            datetime.datetime(2026, 11, 26, 12, 0, tzinfo=self.ist).timestamp())
        self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_wrong_identity_keeps_pending(self):
        self.pending()
        self.broker.verify_profile_and_token_expiry.return_value = self.profile(account="OTHER")
        self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])
        self.broker._governed_get.assert_not_called()

    def test_funds_failure_keeps_pending_and_can_retry(self):
        self.pending()
        good = self.broker._governed_get.return_value
        self.broker._governed_get.return_value = NS(status_code=401)
        self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])
        self.broker._governed_get.return_value = good
        self.assertFalse(self.poll(mono=101))
        self.assertTrue(self.poll(mono=161))
        self.assertEqual(len(self.cards()), 1)

    def test_funds_missing_or_wrong_account_blocks(self):
        self.pending()
        for index, funds in enumerate(({}, {"availableBalance": True},
                                       {"availableBalance": 900, "dhanClientId": "OTHER"},
                                       {"availableBalance": float("nan")})):
            with self.subTest(funds=funds):
                self.broker._governed_get.return_value = NS(status_code=200, json=lambda: funds)
                self.assertFalse(self.poll(mono=100 + index * 61))
                self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_owner_stop_or_gate_block_keeps_pending(self):
        self.pending()
        self.gate.is_accepting_entries = False
        self.assertFalse(self.poll())
        self.gate.is_accepting_entries = True
        self.stop.get_state = lambda: {"is_stopped": True}
        self.assertFalse(self.poll(mono=161))
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_unverified_calendar_blocks(self):
        self.pending()
        with patch("src.engine.market.load_holidays_verified", return_value=(False, set(), "")):
            self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_token_replaced_during_verification_blocks(self):
        self.pending()
        def rotated(*args, **kwargs):
            write_token_atomic("different-token", self.cfg.dhan_token_file)
            return NS(status_code=200, json=lambda: {"availableBalance": 949.20})
        self.broker._governed_get.side_effect = rotated
        self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(self.cards(), [])

    def test_crash_after_outbox_commit_retries_without_duplicate_claim(self):
        self.pending()
        from src.engine import token_recovery_notification as mod
        real_save = mod._save
        def fail_clear(cfg, state):
            if state.get("pending") is False:
                raise OSError("synthetic crash")
            real_save(cfg, state)
        with patch.object(mod, "_save", side_effect=fail_clear):
            self.assertFalse(self.poll())
        self.assertTrue(self.state()["pending"])
        self.assertEqual(len(self.cards()), 1)
        self.assertTrue(self.poll(self.new_worker(), mono=200))
        self.assertFalse(self.state()["pending"])
        self.assertEqual(len(self.cards()), 1)


if __name__ == "__main__":
    unittest.main()
