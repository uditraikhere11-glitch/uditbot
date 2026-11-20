"""Step 2 offline safety tests; all systemd/network operations are mocked."""
from __future__ import annotations
import contextlib
import os
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.control_center_backend import (
    _single_key_change, build_amo_orders, build_control_status,
    register_control_center_routes,
)


class ChangeTextTests(unittest.TestCase):
    def test_patches_single_setting_preserves_secret_bytes(self):
        old = 'TRADING_MODE=LIVE\nDHAN_PIN=PRIVATE\nEQUITY_AMO_ENABLED=false # comment\nFOO=bar\n'
        new = _single_key_change(old, True)
        self.assertEqual(new, old.replace('EQUITY_AMO_ENABLED=false', 'EQUITY_AMO_ENABLED=true'))
        self.assertIn('DHAN_PIN=PRIVATE', new)

    def test_missing_key_added_without_rewriting_other_keys(self):
        old = 'TRADING_MODE=LIVE\nTG_API_HASH=a=b#c'
        new = _single_key_change(old, True)
        self.assertEqual(new, old + '\nEQUITY_AMO_ENABLED=true\n')

    def test_duplicate_key_fails_closed(self):
        with self.assertRaises(ValueError):
            _single_key_change('EQUITY_AMO_ENABLED=true\nequity_amo_enabled=false\n', False)

    def test_crlf_preserved(self):
        self.assertEqual(_single_key_change('A=1\r\nEQUITY_AMO_ENABLED=false\r\n', True),
                         'A=1\r\nEQUITY_AMO_ENABLED=true\r\n')

    def test_no_change_preserves_exact_content(self):
        text = 'EQUITY_AMO_ENABLED=true\nDATA_DIR=data\n'
        self.assertEqual(_single_key_change(text, True), text)


class AmoJournalTests(unittest.TestCase):
    def test_missing_db_is_unverified_not_empty_broker(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = build_amo_orders(tmp)
            self.assertFalse(result['verified'])
            self.assertIn('unavailable', result['summary'])

    def test_ack_is_not_traded_and_no_order_ids_exposed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'trade_journal.db')
            con = sqlite3.connect(db)
            try:
                con.executescript('''
                CREATE TABLE order_intents(correlation_id TEXT, symbol TEXT, state TEXT,
                  is_amo INTEGER, created_at REAL);
                CREATE TABLE orders(correlation_id TEXT, status TEXT, filled_quantity INTEGER,
                  quantity INTEGER, broker_order_id TEXT);
                INSERT INTO order_intents VALUES('private-corr','TRIDENT-EQ','ACKED',1,123);
                INSERT INTO orders VALUES('private-corr','TRANSIT',0,1,'SECRET-ORDER-ID');
                INSERT INTO order_intents VALUES('normal-corr','ABC-EQ','ACKED',0,124);
                ''')
                con.commit()
            finally:
                con.close()
            result = build_amo_orders(tmp)
            self.assertEqual(len(result['orders']), 1)
            self.assertIn('PENDING', result['orders'][0]['status'])
            self.assertFalse(result['verified'])
            self.assertNotIn('SECRET-ORDER-ID', str(result))
            self.assertNotIn('TRADED', str(result))

    def test_journal_fill_is_reported_not_automatically_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'trade_journal.db')
            con = sqlite3.connect(db)
            try:
                con.executescript('''
                CREATE TABLE order_intents(correlation_id TEXT, symbol TEXT, state TEXT,
                  is_amo INTEGER, created_at REAL);
                CREATE TABLE orders(correlation_id TEXT, status TEXT, filled_quantity INTEGER,quantity INTEGER);
                INSERT INTO order_intents VALUES('c','ABC-EQ','ACKED',1,123);
                INSERT INTO orders VALUES('c','TRADED',2,2);
                ''')
                con.commit()
            finally:
                con.close()
            result = build_amo_orders(tmp)
            self.assertIn('broker unverified', result['orders'][0]['status'])


class ControlApiTests(unittest.TestCase):
    """Actual field-scoped Control Center contract, with synthetic service state."""

    def setUp(self):
        from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
        from src.engine.config_patch import revision
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        original_cwd = os.getcwd()
        os.chdir(self.tmp.name)
        self.addCleanup(os.chdir, original_cwd)
        self.env = os.path.join(self.tmp.name, ".env")
        self.data_dir = os.path.join(self.tmp.name, "data")
        self.original = (
            "TRADING_MODE=TEST\nDHAN_CLIENT_ID=FAKE_ACCOUNT\nDHAN_PIN=unmodified\n"
            "EQUITY_AMO_ENABLED=false\nFNO_AMO_ENABLED=false\n"
            f"DATA_DIR={self.data_dir}\n"
        )
        Path(self.env).write_text(self.original, encoding="utf-8")
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.revision = revision
        self.state_patch = patch.object(
            ConfigChangeTracker, "resolve_service_operating_state",
            return_value=(ServiceOperatingState.STOPPED_BY_OWNER, "owner stopped"),
        )
        self.broker_patch = patch.object(
            ConfigChangeTracker, "get_readonly_broker",
            side_effect=AssertionError("Stopped AMO toggle must never contact broker"),
        )
        self.state_patch.start()
        self.broker_patch.start()
        self.addCleanup(self.state_patch.stop)
        self.addCleanup(self.broker_patch.stop)
        # Save & Apply inspects systemd independently of the legacy status reader.
        # Reject any restart attempt in this owner-stopped fixture.
        def stopped_service(command, **kwargs):
            self.assertEqual(command, ["systemctl", "show", "trading_bot.service", "-p", "ActiveState", "--value"])
            from subprocess import CompletedProcess
            return CompletedProcess(command, 0, "inactive\n", "")
        service = patch("src.engine.simple_save_apply.subprocess")
        service.start().run.side_effect = stopped_service
        self.addCleanup(service.stop)
        self.app = FastAPI()
        register_control_center_routes(self.app, "test-csrf")
        self.client = TestClient(self.app)

    def submit(self, endpoint="/control/amo/equity", enabled=True, request_id="OFFLINE-AMO-IDEMPOTENT-001", **extra):
        body = {"enabled": enabled, "csrf_token": "test-csrf",
                "expected_revision": self.revision(Path(self.env).read_text(encoding="utf-8")),
                "request_id": request_id}
        body.update(extra)
        return self.client.post(endpoint, json=body)

    def test_forged_csrf_no_mutations(self):
        r = self.client.post("/control/amo/equity", json={"enabled": True, "csrf_token": "forged"})
        self.assertEqual(r.status_code, 403)
        self.assertEqual(Path(self.env).read_text(), self.original)
        self.assertIsNone(self.tracker.get_change_record())

    def test_non_boolean_rejected(self):
        for bad in ("true", 1, None, {}, []):
            with self.subTest(value=bad):
                r = self.submit(enabled=bad)
                self.assertEqual(r.status_code, 422)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_malformed_payload_rejected(self):
        for bad in ({"enabled": True}, {"enabled": True, "csrf_token": "test-csrf", "extra": "x"}):
            self.assertEqual(self.client.post("/control/amo/equity", json=bad).status_code, 400)
        self.assertEqual(self.client.post("/control/amo/equity", json={
            "enabled": True, "csrf_token": "test-csrf",
        }).status_code, 409)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_equity_amo_is_queued_via_existing_coordinator(self):
        # Owner-stopped transitions now save for next controlled start rather
        # than queuing broker-gated restarts (F-10).
        r = self.submit()
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["state"], "SAVED_FOR_NEXT_START")
        self.assertFalse(r.json()["runtime_verified"])
        self.assertIn("EQUITY_AMO_ENABLED=true", Path(self.env).read_text())
        self.assertIn("DHAN_PIN=unmodified", Path(self.env).read_text())
        self.assertFalse(Path(self.tracker.pending_op_file).exists())

    def test_preexisting_pending_change_prevents_overwrite(self):
        Path(self.tracker.pending_op_file).write_text("{}")
        r = self.submit()
        self.assertEqual(r.status_code, 409)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_owner_stop_pending_prevents_toggle(self):
        with patch.object(self.tracker.__class__, "get_service_control_record",
                          return_value={"deferred_stop_pending": True}):
            self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_unknown_service_state_disables_changes(self):
        from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.UNKNOWN, "synthetic outage")):
            self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_running_version_mismatch_blocks_toggle(self):
        from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
        with patch.object(ConfigChangeTracker, "resolve_service_operating_state",
                          return_value=(ServiceOperatingState.RUNNING, "synthetic running")):
            with patch.object(ConfigChangeTracker, "get_active_running_version",
                              return_value={"pid": 12345, "version_hash": "unknown"}):
                # The policy must never claim a running setting is verified
                # solely because a configuration change was queued.
                r = self.submit()
                self.assertIn(r.status_code, (202, 409))
                if r.status_code == 202:
                    self.assertFalse(r.json().get("runtime_verified", False))
                    self.assertEqual(Path(self.env).read_text(), self.original)

    def test_status_is_readonly_and_no_secrets(self):
        with patch("src.control_center_backend.build_control_status", return_value={
            "service_state": "STOPPED_BY_OWNER", "equity_amo_can_edit": True,
            "fno_amo_can_edit": True, "fno_amo_supported": True,
            "fno_amo_saved": False, "fno_amo_active": None,
        }):
            r = self.client.get("/control/status")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["cache-control"], "no-store")
        self.assertTrue(r.json()["fno_amo_supported"])
        self.assertNotIn("unmodified", r.text)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_same_setting_does_not_create_restart(self):
        r = self.submit(enabled=False)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIn(r.json()["state"], ("SAVED_FOR_NEXT_START", "UNCHANGED"))
        self.assertEqual(Path(self.env).read_text(), self.original)
        self.assertFalse(Path(self.tracker.pending_op_file).exists())

    def test_fno_amo_uses_same_safe_coordinator_with_separate_key(self):
        r = self.submit("/control/amo/fno", True)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["state"], "SAVED_FOR_NEXT_START")
        self.assertIn("FNO_AMO_ENABLED=true", Path(self.env).read_text())
        again = self.submit("/control/amo/fno", True)
        self.assertEqual(again.status_code, 200)
        self.assertEqual(Path(self.env).read_text().count("FNO_AMO_ENABLED=true"), 1)

    def test_fno_amo_csrf_and_boolean_fail_closed(self):
        for csrf in ("forged", ""):
            self.assertEqual(self.submit("/control/amo/fno", True, csrf_token=csrf).status_code, 403)
        self.assertEqual(self.submit("/control/amo/fno", "yes").status_code, 422)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_fno_local_cancel_requires_valid_csrf(self):
        r = self.client.post("/control/amo/fno/cancel",
                             json={"item_id": "-1:2", "csrf_token": "forged"})
        self.assertEqual(r.status_code, 403)

    def test_health_endpoint_is_read_only_and_bounded(self):
        with patch("src.control_center_backend.build_control_health",
                   return_value={"summary": "test", "checks": []}):
            r = self.client.get("/control/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["cache-control"], "no-store")
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_stale_revision_rejected_without_overwrite(self):
        self.assertEqual(self.submit(expected_revision="0" * 64).status_code, 409)
        self.assertEqual(Path(self.env).read_text(), self.original)

    def test_identical_idempotent_retry_returns_same_change(self):
        expected = self.revision(self.original)
        body = {"enabled": True, "csrf_token": "test-csrf",
                "expected_revision": expected, "request_id": "RETRY-SAME-REQUEST-0001"}
        first = self.client.post("/control/amo/fno", json=body)
        second = self.client.post("/control/amo/fno", json=body)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["change_id"], second.json()["change_id"])
        changed_body = dict(body, enabled=False)
        self.assertEqual(self.client.post("/control/amo/fno", json=changed_body).status_code, 409)


if __name__=='__main__':
    unittest.main()