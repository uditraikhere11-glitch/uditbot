"""Offline tests for fast serialized reconciliation recovery on rotated Dhan token adoption."""
from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.broker.dhan_client import DhanClient
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.reconciliation import ReconciliationState, StartupReconciliationGate
from src.engine.token_rotation_recovery import install_token_rotation_fast_recovery
from src.journal.db import JournalDB


class TokenRotationFastRecoveryTests(unittest.TestCase):
    def _make_setup(self, td: str):
        token_path = Path(td) / "dhan_token"
        token_path.write_text("OLD-EXPIRED-TOKEN\n", encoding="utf-8")
        cfg = AppConfig(
            dhan_client_id="1113731536",
            dhan_access_token="OLD-EXPIRED-TOKEN",
            dhan_token_file=str(token_path),
            data_dir=td,
        )
        client = DhanClient(cfg, auto_generate_token=False)
        db = JournalDB(str(Path(td) / "journal.db"), account_id="1113731536")
        stop_mgr = EmergencyStopManager(state_file=str(Path(td) / "emergency_stop.json"))
        stop_mgr.provision_initial_clean_state()
        gate = StartupReconciliationGate(broker=client, watcher=None, stop_manager=stop_mgr, journal_db=db)
        client.reconciliation_gate = gate
        self.assertTrue(install_token_rotation_fast_recovery(client, gate))
        # Idempotent re-install
        self.assertTrue(install_token_rotation_fast_recovery(client, gate))
        return token_path, client, gate, stop_mgr

    def test_blocked_gate_clears_600s_backoff_and_rechecks_on_token_rotation(self):
        with tempfile.TemporaryDirectory() as td:
            token_path, client, gate, _ = self._make_setup(td)
            gate.halt_new_entries("Broker funds check failed: Dhan auth error (401)")
            gate._historical_background_retry_after = time.monotonic() + 600.0

            recheck_called = threading.Event()

            def _fake_recheck():
                recheck_called.set()
                return True, "Recovered"

            with patch.object(gate, "recheck_and_recover_if_verified", side_effect=_fake_recheck):
                tmp_tok = Path(td) / "dhan_token.tmp"
                tmp_tok.write_text("NEW-VALID-TOKEN-2026\n", encoding="utf-8")
                os.replace(tmp_tok, token_path)

                client._sync_shared_token_from_disk()
                self.assertEqual(client.token, "NEW-VALID-TOKEN-2026")
                self.assertTrue(recheck_called.wait(timeout=3.0), "Expected immediate background recheck")

    def test_healthy_gate_is_noop_on_token_rotation(self):
        with tempfile.TemporaryDirectory() as td:
            token_path, client, gate, _ = self._make_setup(td)
            gate.state = ReconciliationState.HEALTHY
            gate._accepting_entries = True

            with patch.object(gate, "_schedule_historical_background_recheck") as sched:
                tmp_tok = Path(td) / "dhan_token.tmp"
                tmp_tok.write_text("NEW-VALID-TOKEN-2026\n", encoding="utf-8")
                os.replace(tmp_tok, token_path)
                client._sync_shared_token_from_disk()
                sched.assert_not_called()

    def test_emergency_stop_blocks_token_rotation_recheck(self):
        with tempfile.TemporaryDirectory() as td:
            token_path, client, gate, stop_mgr = self._make_setup(td)
            stop_mgr.trigger_stop("Manual emergency stop test", "Operator")
            gate.halt_new_entries("Dhan auth error (401)")
            gate._historical_background_retry_after = time.monotonic() + 600.0

            with patch.object(gate, "_schedule_historical_background_recheck") as sched:
                tmp_tok = Path(td) / "dhan_token.tmp"
                tmp_tok.write_text("NEW-VALID-TOKEN-2026\n", encoding="utf-8")
                os.replace(tmp_tok, token_path)
                client._sync_shared_token_from_disk()
                sched.assert_not_called()
                self.assertFalse(gate.is_accepting_entries)

    def test_rapid_token_rewrites_rate_limited_to_once_per_30s(self):
        with tempfile.TemporaryDirectory() as td:
            token_path, client, gate, _ = self._make_setup(td)
            gate.halt_new_entries("Dhan auth error (401)")

            with patch.object(gate, "_schedule_historical_background_recheck") as sched:
                for idx in (1, 2):
                    tmp_tok = Path(td) / f"dhan_token_{idx}.tmp"
                    tmp_tok.write_text(f"NEW-TOKEN-{idx}\n", encoding="utf-8")
                    os.replace(tmp_tok, token_path)
                    client._sync_shared_token_from_disk()
                self.assertEqual(sched.call_count, 1)


if __name__ == "__main__":
    unittest.main()
