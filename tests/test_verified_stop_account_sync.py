"""
V14 verified Dhan-account binding migration for durable Emergency STOP.

Hermetic tests only: no broker network, systemd, or live order mutations.
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.engine.emergency_stop import EmergencyStopManager
from src.broker.dhan_client import DhanClient
from src.models import TradingMode


OLD_ID = "1113731536"
NEW_ID = "1105508885"


class TestVerifiedEmergencyStopAccountSync(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "emergency_stop.json"
        self.manager = EmergencyStopManager(
            state_file=str(self.path), expected_account_id=NEW_ID
        )

    def state(self, *, is_stopped=False, in_flight=None, account_id=OLD_ID, **extra):
        raw = {
            "is_stopped": is_stopped,
            "stopped_by": "Owner" if is_stopped else "",
            "reason": "Owner stopped trading" if is_stopped else "Normal",
            "account_id": account_id,
            "in_flight_orders": list(in_flight or []),
            "state_version": 2,
        }
        raw.update(extra)
        self.path.write_text(json.dumps(raw))
        os.chmod(self.path, 0o600)
        return raw

    @staticmethod
    def broker(profile_id=NEW_ID):
        b = MagicMock()
        b.get_authenticated_profile_id.return_value = profile_id
        return b

    def test_01_verified_account_migration_forces_stop_and_preserves_audit(self):
        self.state(is_stopped=False)
        broker = self.broker()
        ok, msg = self.manager.sync_account_from_authenticated_broker(broker)
        self.assertTrue(ok, msg)
        s = json.loads(self.path.read_text())
        self.assertEqual(s["account_id"], NEW_ID)
        self.assertEqual(s["previous_account_id"], OLD_ID)
        self.assertTrue(s["is_stopped"])
        self.assertEqual(s["stopped_by"], "SYSTEM_ACCOUNT_SWITCH")
        self.assertIn("owner-authorized resume", s["reason"])
        self.assertTrue(self.manager.is_stopped())
        broker.get_authenticated_profile_id.assert_called_once_with()

    def test_02_wrong_profile_never_rebinds(self):
        self.state()
        before = self.path.read_bytes()
        ok, _ = self.manager.sync_account_from_authenticated_broker(
            self.broker("1111111111")
        )
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(self.manager.is_stopped())

    def test_03_missing_profile_never_rebinds(self):
        self.state()
        before = self.path.read_bytes()
        ok, _ = self.manager.sync_account_from_authenticated_broker(self.broker(None))
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)

    def test_04_pending_watcher_does_not_migrate(self):
        self.state()
        before = self.path.read_bytes()
        broker = self.broker()
        ok, _ = self.manager.sync_account_from_authenticated_broker(
            broker, pending_watcher_count=1
        )
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)
        broker.get_authenticated_profile_id.assert_not_called()

    def test_05_in_flight_mutation_blocks_rebind(self):
        self.state(in_flight=["TRADE-123"])
        before = self.path.read_bytes()
        broker = self.broker()
        ok, _ = self.manager.sync_account_from_authenticated_broker(broker)
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)
        broker.get_authenticated_profile_id.assert_not_called()

    def test_06_existing_manual_stop_never_resumed(self):
        self.state(is_stopped=True)
        ok, msg = self.manager.sync_account_from_authenticated_broker(self.broker())
        self.assertTrue(ok, msg)
        s = json.loads(self.path.read_text())
        self.assertTrue(s["is_stopped"])
        self.assertEqual(s["prior_stop_reason"], "Owner stopped trading")
        self.assertTrue(self.manager.is_stopped())

    def test_07_existing_correct_binding_is_idempotent(self):
        self.state(account_id=NEW_ID, is_stopped=False)
        before = self.path.read_bytes()
        broker = self.broker()
        ok, msg = self.manager.sync_account_from_authenticated_broker(broker)
        self.assertTrue(ok, msg)
        self.assertEqual(self.path.read_bytes(), before)
        broker.get_authenticated_profile_id.assert_called_once_with()

    def test_08_missing_file_remains_fail_closed(self):
        ok, _ = self.manager.sync_account_from_authenticated_broker(self.broker())
        self.assertFalse(ok)
        self.assertFalse(self.path.exists())
        self.assertTrue(self.manager.is_stopped())

    def test_09_corrupt_file_cannot_be_replaced(self):
        self.path.write_text("{broken")
        before = self.path.read_bytes()
        ok, _ = self.manager.sync_account_from_authenticated_broker(self.broker())
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(self.manager.is_stopped())

    def test_10_profile_exception_fails_closed(self):
        self.state()
        before = self.path.read_bytes()
        broker = MagicMock()
        broker.get_authenticated_profile_id.side_effect = TimeoutError("offline")
        ok, _ = self.manager.sync_account_from_authenticated_broker(broker)
        self.assertFalse(ok)
        self.assertEqual(self.path.read_bytes(), before)

    def test_11_recheck_concurrent_change_fails_closed(self):
        self.state()
        broker = self.broker()
        def concurrent_update():
            self.state(account_id="9999999999")
            return NEW_ID
        broker.get_authenticated_profile_id.side_effect = concurrent_update
        ok, _ = self.manager.sync_account_from_authenticated_broker(broker)
        self.assertFalse(ok)
        self.assertEqual(json.loads(self.path.read_text())["account_id"], "9999999999")

    def test_12_profile_read_live_uses_authenticated_broker_response(self):
        client = DhanClient.__new__(DhanClient)
        client.cfg = SimpleNamespace(trading_mode=TradingMode.LIVE)
        client._headers = MagicMock(return_value={"access-token": "FAKE"})
        client.session = MagicMock()
        client.session.get.return_value = SimpleNamespace(
            status_code=200, json=lambda: {"dhanClientId": NEW_ID}
        )
        self.assertEqual(client.get_authenticated_profile_id(), NEW_ID)
        client.session.get.assert_called_once()
        self.assertTrue(client.session.get.call_args.args[0].endswith("/profile"))

    def test_13_profile_read_rejects_malformed_identity(self):
        client = DhanClient.__new__(DhanClient)
        client.cfg = SimpleNamespace(trading_mode=TradingMode.LIVE)
        client._headers = MagicMock(return_value={"access-token": "FAKE"})
        client.session = MagicMock()
        client.session.get.return_value = SimpleNamespace(
            status_code=200, json=lambda: {"dhanClientId": ""}
        )
        self.assertIsNone(client.get_authenticated_profile_id())

    def test_14_test_mode_never_reads_broker_profile(self):
        client = DhanClient.__new__(DhanClient)
        client.cfg = SimpleNamespace(trading_mode=TradingMode.TEST)
        client.session = MagicMock()
        self.assertIsNone(client.get_authenticated_profile_id())
        client.session.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
