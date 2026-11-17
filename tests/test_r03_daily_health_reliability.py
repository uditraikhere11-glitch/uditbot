"""Offline R03/R17 regression: no real Dhan traffic, TOTP, tokens or Telegram messages.

Tests the actual CLI and broker-governor integration rather than only policy
helpers. Synthetic credentials and temporary SQLite state are used throughout.
"""
import hashlib
import io
import sqlite3
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import requests

from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor, ReadDeferred
from src.cli import cmd_refresh_token, cmd_daily_health_check, format_morning_health_report
from src.models import TradingMode


ACCOUNT = "9000000001"


def identity_proof(token):
    return {
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "verified_at": time.time(),
        "account_id": ACCOUNT,
        "profile": {
            "identity_status": "VERIFIED",
            "identity_verified": True,
            "identity_mismatch": False,
            "profile_client_id": ACCOUNT,
            "token_expiration_str": "18/11/2026 17:00",
            "token_expiration_ts": 1794992400.0,
            "token_expired": False,
        }
    }


class R03DailyEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = NS(
            dhan_client_id=ACCOUNT,
            dhan_pin="SYNTH_PIN",
            dhan_totp_secret="SYNTH_SECRET",
            dhan_token_file=str(Path(self.tmp.name) / "token"),
            data_dir=self.tmp.name,
            scrip_master_max_age_sec=86400,
            trading_mode=TradingMode.LIVE,
            instance_name="SYNTH-AUDITBOT",
        )
        self.stop_mgr = NS(get_state=lambda: {"is_stopped": False})
        self.db = NS(
            get_non_terminal_intents=Mock(return_value=[]),
            get_unprotected_orders=Mock(return_value=[]),
            record_system_event=Mock(),
        )

    def test_candidate_profile_proof_is_bound_to_exact_new_token(self):
        client = DhanClient.__new__(DhanClient)
        client.cfg = self.cfg
        client.session = Mock()
        client.session.post.return_value = NS(
            status_code=200,
            json=lambda: {"accessToken": "SYNTH_NEW_TOKEN", "dhanClientId": ACCOUNT},
        )
        client.verify_profile_and_token_expiry = Mock(return_value=identity_proof("SYNTH_NEW_TOKEN")["profile"])
        with patch("src.broker.dhan_client.compute_totp", return_value="000000"):
            ok, token, _ = client._perform_totp_http_login()
        self.assertTrue(ok)
        self.assertEqual(token, "SYNTH_NEW_TOKEN")
        self.assertEqual(client._last_candidate_profile_verification["token_sha256"],
                         hashlib.sha256(token.encode()).hexdigest())
        client.verify_profile_and_token_expiry.assert_called_once_with(
            allow_refresh=False, candidate_token=token,
        )

    def test_failed_candidate_profile_never_creates_attestation(self):
        client = DhanClient.__new__(DhanClient)
        client.cfg = self.cfg
        client.session = Mock()
        client.session.post.return_value = NS(
            status_code=200,
            json=lambda: {"accessToken": "BAD_CANDIDATE", "dhanClientId": ACCOUNT},
        )
        client.verify_profile_and_token_expiry = Mock(return_value={
            "identity_status": "UNVERIFIED", "identity_verified": False,
        })
        with patch("src.broker.dhan_client.compute_totp", return_value="000000"):
            ok, token, _ = client._perform_totp_http_login()
        self.assertFalse(ok)
        self.assertEqual(token, "")
        self.assertIsNone(client._last_candidate_profile_verification)

    def test_refresh_returns_proof_without_persisting_token_in_sink(self):
        tok = "SYNTH_NEW_TOKEN"
        client = Mock()
        client.generate_token_with_totp.return_value = (True, tok, "OK")
        client.token = tok
        client._last_candidate_profile_verification = identity_proof(tok)
        client.verify_token_authenticated.return_value = (True, 1068.97, "Funds retrieved successfully")
        client.last_funds_is_fallback = False
        evidence = {}
        with patch("src.broker.dhan_client.DhanClient", return_value=client):
            with redirect_stdout(io.StringIO()):
                self.assertTrue(cmd_refresh_token(self.cfg, self.stop_mgr, verification_sink=evidence))
        self.assertTrue(evidence["token_generated"])
        self.assertTrue(evidence["funds_ok"])
        self.assertEqual(evidence["profile"]["profile_client_id"], ACCOUNT)
        self.assertNotIn("token", evidence)
        self.assertNotIn(tok, repr(evidence))
        client.verify_token_authenticated.assert_called_once()

    def _run_daily(self, *, refresh_success, sink_data, token, profile=None, funds=None):
        c = Mock()
        c.token = token
        c.get_fund_limits.return_value = funds or (True, 1068.97, "Funds retrieved successfully")
        c.last_funds_is_fallback = False
        c.verify_profile_and_token_expiry.return_value = profile or {
            "identity_status": "UNVERIFIED", "identity_verified": False,
            "profile_client_id": "", "token_expired": False,
        }
        def refresh(*args, **kwargs):
            kwargs["verification_sink"].update(sink_data)
            return refresh_success
        captured = {}
        def format_report(_cfg, **kwargs):
            captured.update(kwargs)
            if kwargs["token_verified"] and kwargs["funds_verified"]:
                return "Next trading session readiness: ✅ VERIFIED CHECKS\\nOverall result: ✅ SUCCESS"
            return "Next trading session readiness: ⚠️ REVIEW REQUIRED\\nOverall result: ❌ ACTION REQUIRED"
        with patch("src.cli.cmd_refresh_token", side_effect=refresh), \\
             patch("src.broker.dhan_client.DhanClient", return_value=c), \\
             patch("src.instruments.resolver.InstrumentResolver.load_dhan_master", return_value=True), \\
             patch("src.cli.format_morning_health_report", side_effect=format_report), \\
             patch("src.cli.dispatch_morning_health_report_once", return_value="DELIVERED"):
            out = io.StringIO()
            with redirect_stdout(out):
                rc = cmd_daily_health_check(self.cfg, self.db, self.stop_mgr)
        return rc, out.getvalue(), c, captured

    def test_refresh_success_reuses_profile_and_funds_without_duplicate_gets(self):
        token = "SYNTH_FRESH"
        att = identity_proof(token)
        sink = {
            "token_generated": True, "token_sha256": att["token_sha256"],
            "profile": att["profile"], "funds_attempted": True,
            "funds_ok": True, "funds_balance": 1068.97,
            "funds_message": "Funds retrieved successfully", "funds_fallback": False,
        }
        rc, out, client, report = self._run_daily(
            refresh_success=True, sink_data=sink, token=token,
        )
        self.assertEqual(rc, 0)
        client.get_fund_limits.assert_not_called()
        client.verify_profile_and_token_expiry.assert_not_called()
        self.assertTrue(report["token_verified"])
        self.assertTrue(report["token_renewed"])
        self.assertIn("token_refresh=OK", out)
        self.assertIn("account_identity=VERIFIED", out)

    def test_stale_proof_must_not_authorize_rotated_token(self):
        att = identity_proof("OLDER")
        sink = {
            "token_generated": True, "token_sha256": att["token_sha256"],
            "profile": att["profile"], "funds_attempted": True,
            "funds_ok": True, "funds_balance": 1068.97,
        }
        rc, out, client, report = self._run_daily(
            refresh_success=True, sink_data=sink, token="NEWER",
        )
        self.assertEqual(rc, 1)
        client.get_fund_limits.assert_called_once()
        client.verify_profile_and_token_expiry.assert_called_once()
        self.assertFalse(report["token_verified"])
        self.assertIn("account_identity=UNVERIFIED", out)

    def test_post_refresh_funds_failure_retains_profile_not_readiness(self):
        token = "SYNTH_FRESH"
        att = identity_proof(token)
        sink = {
            "token_generated": True, "token_sha256": att["token_sha256"],
            "profile": att["profile"], "funds_attempted": True,
            "funds_ok": False, "funds_balance": 0.0,
            "funds_message": "Dhan locally deferred", "funds_fallback": False,
        }
        rc, out, client, report = self._run_daily(
            refresh_success=False, sink_data=sink, token=token,
        )
        self.assertEqual(rc, 1)
        self.assertTrue(report["token_verified"])
        self.assertFalse(report["funds_verified"])
        self.assertTrue(report["token_renewed"])
        client.verify_profile_and_token_expiry.assert_not_called()
        client.get_fund_limits.assert_not_called()
        self.assertIn("token_refresh=OK", out)
        self.assertIn("AUTH_CHECK=FAIL", out)

    def test_original_token_identity_can_be_verified_when_refresh_failed(self):
        profile = identity_proof("ORIGINAL")["profile"]
        rc, out, client, report = self._run_daily(
            refresh_success=False, sink_data={}, token="ORIGINAL", profile=profile,
        )
        self.assertEqual(rc, 1)
        self.assertFalse(report["token_verified"])
        self.assertFalse(report["token_renewed"])
        self.assertIn("token_refresh=FAIL", out)
        self.assertIn("account_identity=VERIFIED", out)

    def test_format_displays_identity_independently_without_green_readiness(self):
        with patch("src.engine.market.load_holidays_verified", return_value=(False, None, "not loaded")):
            card = format_morning_health_report(
                self.cfg, now_ts=time.time(), token_verified=False,
                token_renewed=True, funds_verified=False, fallback_funds=False,
                available_balance=0.0, master_loaded=False, is_stopped=True,
                account_identity_status="VERIFIED", profile_client_id=ACCOUNT,
            )
        self.assertIn("Token renewal: ✅ SUCCESS", card)
        self.assertIn("Dhan account identity: ✅ VERIFIED", card)
        self.assertIn("Overall result: ❌ ACTION REQUIRED", card)


class R03GovernorReserveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 1800000000.0
        self.g = ReadGovernor(self.tmp.name, ACCOUNT, clock=lambda: self.now)
        self.g.LIMITS = ((3600, 800),)

    def test_760_routine_reads_cannot_consume_40_identity_slots(self):
        self.g.admit(priority="normal")
        with sqlite3.connect(self.g.path) as db:
            db.executemany("INSERT INTO reads(at) VALUES (?)", [(self.now,)] * 759)
        with self.assertRaises(ReadDeferred):
            self.g.admit(priority="normal")
        # Realistic 17:00 identity/funds reads are admitted within the hard cap.
        self.g.admit(priority="identity")
        self.g.admit(priority="identity")
        with sqlite3.connect(self.g.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reads").fetchone()[0], 762)

    def test_identity_cannot_bypass_full_budget_or_broker_cooldown(self):
        self.g.admit(priority="identity")
        with sqlite3.connect(self.g.path) as db:
            db.executemany("INSERT INTO reads(at) VALUES (?)", [(self.now,)] * 799)
        with self.assertRaises(ReadDeferred):
            self.g.admit(priority="identity")
        self.now += 3601
        self.g.throttled(90)
        with self.assertRaises(ReadDeferred):
            self.g.admit(priority="identity")

    def test_local_profile_429_is_distinguished_from_broker_429(self):
        def fake_client(local):
            c = DhanClient.__new__(DhanClient)
            c.cfg = NS(trading_mode=TradingMode.LIVE, dhan_client_id=ACCOUNT)
            response = requests.Response()
            response.status_code = 429
            response._content = b"rate limited"
            if local:
                response.headers["X-AuditBot-Local-Deferral"] = "1"
            c._governed_get = Mock(return_value=response)
            c._get_with_auth_recovery = Mock(return_value=response)
            return c
        for local in (True, False):
            c = fake_client(local)
            self.assertIsNone(c.get_authenticated_profile(candidate_token="SYNTH"))
            self.assertEqual(c._last_profile_http_status, 429)
            self.assertEqual(c._last_profile_local_deferral, local)


if __name__ == "__main__":
    unittest.main()
