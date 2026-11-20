"""Synthetic service startup and old-host evidence with tracker V2 disabled."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.config import AppConfig
from src.control_center_backend import build_control_status
from src.engine.broker_handoff import BrokerMutationNotSubmitted
from src.engine.config_patch import ConfigurationConflict
from src.engine.simple_save_apply import _retire_previous_transaction
from src.main import TradingBotPipeline


class SettingsHostMigrationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.data = Path(temp.name)
        self.folder = self.data / "config_restart_v2"
        self.folder.mkdir(mode=0o700)
        self.env = self.data / ".env"
        self.env.write_text(f"DATA_DIR={self.data}\nTRADING_MODE=TEST\nDHAN_CLIENT_ID=TEST_ACCOUNT\n")
        flag = patch.dict(os.environ, {"UDITBOT_TRACKER_V2_ENABLE": "0"})
        flag.start()
        self.addCleanup(flag.stop)

    def test_service_registers_and_obeys_fence_without_v2_activation(self):
        cfg = AppConfig.from_env(env={"DATA_DIR": str(self.data), "DHAN_CLIENT_ID": "TEST_ACCOUNT",
            "TRADING_MODE": "TEST", "JOURNAL_DB_PATH": str(self.data / "journal.db"),
            "DHAN_TOKEN_FILE": str(self.data / "dummy-token")}, env_file="")
        with patch("src.main.TelegramNotifier"), patch("src.main.InstrumentResolver.load_dhan_master"):
            pipeline = TradingBotPipeline(cfg, service_process=True)
        ledger = json.loads((self.data / "active_broker_mutations.json").read_text())
        self.assertEqual(ledger["producer_pid"], os.getpid())
        self.assertEqual(ledger["account_id"], "TEST_ACCOUNT")
        ack = json.loads((self.data / "active_config_version.json").read_text())
        self.assertRegex(ack["credential_fingerprint"], r"^[0-9a-f]{64}$")
        (self.folder / "apply_barrier.json").write_text('{"active":true}')
        with patch.object(pipeline.broker.session, "post") as send:
            with self.assertRaises(BrokerMutationNotSubmitted):
                pipeline.broker._broker_mutating_http("POST", "https://invalid.test/v2/orders")
            send.assert_not_called()

    def test_control_status_reports_restored_fence_without_web_flag(self):
        (self.folder / "apply_barrier.json").write_text('{"active":true}')
        with patch("src.engine.config_manager.ConfigChangeTracker.control_service",
                   return_value={"service_state":"RUNNING", "runtime_verified":True}):
            status = build_control_status(str(self.env))
        self.assertTrue(status["tracker_v2_apply_blocked"])
        self.assertFalse(status["runtime_verified"])
        self.assertFalse(status["equity_amo_can_edit"])

    def test_disappearing_journal_does_not_hide_remaining_fence(self):
        journal = self.folder / "current_transaction.json"
        journal.write_text("{}")
        @contextmanager
        def after_other_writer(folder):
            journal.unlink()
            (folder / "apply_barrier.json").write_text('{"active":true}')
            yield
        with patch("src.engine.simple_save_apply.prior_settings_lock", after_other_writer):
            with self.assertRaisesRegex(ConfigurationConflict, "fence is unresolved"):
                _retire_previous_transaction(self.data, self.env, {}, "0" * 64)
