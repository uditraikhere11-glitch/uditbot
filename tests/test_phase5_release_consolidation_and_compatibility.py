"""Phase 5 release consolidation, deployment/config compatibility, and end-to-end integration tests."""
from __future__ import annotations

import importlib
import os
from pathlib import Path
import subprocess
import time
import unittest
from unittest.mock import patch

from src.config import AppConfig, TradingMode
from src.control_center_backend import _BROKER_CACHE, _cached_broker_status, _build_shared_runtime_status_summary
from src.engine.config_manager import ConfigChangeTracker
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.engine.risk_manager import RiskManager
from src.notifier.runtime_status import format_runtime_status, summarize_runtime_status
from tests._phase2d_live_harness import LivePhase2DHarness

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestPhase5ReleaseConsolidationAndCompatibility(unittest.TestCase):
    def setUp(self):
        _BROKER_CACHE.clear()

    def tearDown(self):
        _BROKER_CACHE.clear()

    def test_all_core_runtime_and_webui_modules_import_cleanly(self):
        """Phase 5: All production runtime, broker, WebUI, and notifier modules import without error."""
        modules = (
            "src.models",
            "src.config",
            "src.broker.dhan_client",
            "src.broker.snapshot_coordinator",
            "src.broker.dhan_snapshot_source",
            "src.engine.unified_account_runtime",
            "src.engine.broker_ledger_reconciler",
            "src.engine.protection_watcher",
            "src.engine.reconciliation",
            "src.engine.risk_manager",
            "src.engine.config_manager",
            "src.engine.durable_issue_writer",
            "src.engine.durable_risk_resolution",
            "src.control_center_backend",
            "src.notifier.runtime_status",
            "src.main",
        )
        for mod in modules:
            with self.subTest(module=mod):
                self.assertIsNotNone(importlib.import_module(mod))

    def test_legacy_dangerous_and_backup_files_remain_absent(self):
        """Phase 5: Legacy bypass/patch/backup scripts remain removed."""
        forbidden = (
            "bypass_amo_validator.py",
            "bypass_reconciliation.py",
            "dhan_client.py",
            "nuke_groww.py",
            "patch_brain.py",
            "patch_safe_brain.py",
            "rollback_brain.py",
            "setup_app.py",
            "src/broker/dhan_client.py.bak",
            "today_bot_logs.txt",
        )
        for rel in forbidden:
            self.assertFalse((REPO_ROOT / rel).exists(), f"Forbidden legacy file reappeared: {rel}")

    def test_deployment_scripts_and_service_files_compatible(self):
        """Phase 5: Existing shell deployment scripts pass bash -n syntax check and required unit files exist."""
        shell_scripts = (
            "bot",
            "deploy/audit_vm_state.sh",
            "deploy/backup_runtime.sh",
            "deploy/install_vm_services.sh",
            "deploy/migrate_existing_webui_to_control_center.sh",
            "deploy/restore_runtime.sh",
            "deploy/setup_vm.sh",
            "deploy/upgrade_webui_control_center.sh",
        )
        for rel in shell_scripts:
            script = REPO_ROOT / rel
            self.assertTrue(script.is_file(), f"Missing deployment script: {rel}")
            proc = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, f"bash -n failed for {rel}: {proc.stderr}")

        for unit in (
            "deploy/dhan-token-refresh.service",
            "deploy/dhan-token-refresh.timer",
            "docs/UNIFIED_BROKER_LEDGER_RELEASE_RUNBOOK.md",
            ".env.example",
        ):
            self.assertTrue((REPO_ROOT / unit).is_file(), f"Missing release/deploy artifact: {unit}")

    def test_config_defaults_and_backward_compatibility_preserved(self):
        """Phase 5: AppConfig defaults remain intact and backward compatible."""
        cfg = AppConfig()
        self.assertIsInstance(cfg.trading_mode, TradingMode)
        self.assertNotEqual(cfg.trading_mode, TradingMode.LIVE)
        self.assertGreater(cfg.max_open_positions, 0)
        self.assertGreater(cfg.max_daily_trades, 0)
        self.assertTrue(bool(cfg.data_dir))

    def test_end_to_end_integrated_runtime_lifecycle_parity(self):
        """Phase 5: StartupGate -> RiskManager -> ConfigShutdownGate -> WebUI zero-HTTP status -> Telegram card work together."""
        acct = "1113731536"
        h = LivePhase2DHarness(account_id=acct)
        self.addCleanup(h.close)
        os.chmod(h.cfg.dhan_token_file, 0o600)

        watcher = ProtectionWatcher(h.client, path=str(Path(h.cfg.data_dir) / "watcher.json"), journal_db=h.db)
        class _Stop:
            def is_stopped(self): return False
            def get_in_flight(self): return []

        # 1. Startup reconciliation via real HTTP transport harness
        gate = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        ok_s, msg_s = gate.run_unified_startup_verification(data_dir=str(h.cfg.data_dir))
        self.assertTrue(ok_s, msg_s)
        self.assertEqual(gate.state, ReconciliationState.HEALTHY)
        self.assertTrue(gate.is_accepting_entries)

        # 2. RiskManager consumes the same coordinated snapshot
        rm = RiskManager(h.cfg)
        ok_rm, msg_rm = rm.sync_from_journal_and_broker(
            journal_db=h.db, broker=h.client, broker_snapshot=gate._unified_snapshot
        )
        self.assertTrue(ok_rm, msg_rm)

        # 3. ConfigChangeTracker shutdown gate succeeds cleanly
        tracker = ConfigChangeTracker(data_dir=str(h.cfg.data_dir))
        res_gate = tracker.evaluate_unified_shutdown_gate(action="stop", db_path=h.db_path, broker=h.client)
        self.assertTrue(res_gate["allowed"], res_gate)

        # 4. WebUI broker status reuses fresh unified snapshot with 0 HTTP calls
        with patch("src.broker.dhan_client.DhanClient", side_effect=AssertionError("No broker HTTP allowed")):
            ui_broker = _cached_broker_status(h.cfg, refresh=False)
            self.assertEqual(ui_broker["status"], "VERIFIED", ui_broker)
            self.assertEqual(ui_broker.get("source"), "unified_snapshot")

        # 5. Telegram & WebUI runtime status projections match
        op_snap = {
            "reconciliation": gate.state.value,
            "entries_open": gate.is_accepting_entries,
            "watcher_running": True,
            "watcher_supervision_verified": True,
            "created_at": time.time(),
        }
        uni_snap = {"available": True, "status": "VERIFIED", "session_phase": "SETTLED"}
        self.assertEqual(
            summarize_runtime_status(op_snap, unified_snapshot=uni_snap),
            _build_shared_runtime_status_summary({"operational_snapshot": op_snap}, uni_snap),
        )
        tg_card = format_runtime_status({**op_snap, "unified_broker_snapshot": uni_snap}, instance="UditBot", mode="LIVE")
        self.assertIn("New Entries Gate: OPEN", tg_card)
        self.assertIn("Unified Snapshot: VERIFIED (SETTLED)", tg_card)


if __name__ == "__main__":
    unittest.main()
