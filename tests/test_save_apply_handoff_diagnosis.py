"""Offline regressions for AmanBot's stale broker handoff Save & Apply error.

Every case uses synthetic runtime evidence and never sends Dhan requests,
modifies actual .env, or triggers systemd operations.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.engine.config_patch import ConfigurationConflict
from src.engine.runtime_evidence import SafetyDecision
from src.engine import simple_save_apply as apply_module


class RunningHandoffDiagnosisTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = Path(self.tmp.name) / ".env"
        self.env.write_text("TRADING_MODE=TEST\nDHAN_CLIENT_ID=ACCOUNT\n")
        self.ack = {
            "pid": 1234,
            "process_starttime": 9001,
            "credential_fingerprint": "abc" * 21 + "a",
        }
        self.cfg = SimpleNamespace(
            data_dir=str(Path(self.tmp.name) / "data"),
            dhan_client_id="ACCOUNT",
            trading_mode=SimpleNamespace(value="TEST"),
        )
        self.ledger = {"account_id": "ACCOUNT"}
        self.gate = SafetyDecision(
            allowed=True,
            code="NO_ACTIVE_BROKER_OPERATION",
            message="clear",
            evidence={"producer_pid": 1234, "producer_starttime": 9001},
        )

    def proof(self, *, gate=None, ledger=None, fp=None, ack=None):
        tracker = Mock()
        tracker.is_active_version_confirmed.return_value = (True, self.ack if ack is None else ack, "Verified")
        patches = (
            patch.object(apply_module.AppConfig, "from_env", return_value=self.cfg),
            patch.object(apply_module, "ConfigChangeTracker", return_value=tracker),
            patch.object(apply_module, "inspect_systemd_service_properties",
                         return_value={"ActiveState": "active", "MainPID": 1234}),
            patch.object(apply_module, "compute_config_version", return_value="hash"),
            patch.object(apply_module, "evaluate_active_broker_operation_gate",
                         return_value=self.gate if gate is None else gate),
            patch.object(apply_module, "read_private_json",
                         return_value=self.ledger if ledger is None else ledger),
            patch("src.engine.tracker_credentials.fingerprint",
                  return_value=self.ack["credential_fingerprint"] if fp is None else fp),
        )
        from contextlib import ExitStack
        with ExitStack() as stack:
            for item in patches:
                stack.enter_context(item)
            return apply_module._positive_running_restart_proof(
                self.env, {"DHAN_CLIENT_ID": "ACCOUNT", "TRADING_MODE": "TEST"}
            )

    def fail(self, reason, **kw):
        with self.assertRaisesRegex(ConfigurationConflict, reason):
            self.proof(**kw)

    def test_valid_evidence_allows_proof_without_mutation(self):
        result = self.proof()
        self.assertEqual((result["pid"], result["starttime"]), (1234, 9001))

    def test_missing_producer_is_not_treated_as_empty_ledger(self):
        self.fail(
            "NO_RUNTIME_HANDOFF",
            gate=SafetyDecision(False, "NO_RUNTIME_HANDOFF", "producer missing", {}),
        )

    def test_unresolved_broker_mutation_blocks_without_cutover_advice(self):
        self.fail(
            "UNRESOLVED_BROKER_MUTATION",
            gate=SafetyDecision(False, "UNRESOLVED_BROKER_MUTATION", "unknown order", {}),
        )

    def test_active_mutation_blocks_without_restart(self):
        self.fail(
            "BROKER_MUTATION_ACTIVE",
            gate=SafetyDecision(False, "BROKER_MUTATION_ACTIVE", "active", {}),
        )

    def test_missing_fingerprint_blocks_even_when_ledger_clean(self):
        ack = dict(self.ack, credential_fingerprint="")
        self.fail("CURRENT_CREDENTIAL_ACK_MISSING", ack=ack)

    def test_mismatched_fingerprint_blocks_even_when_ledger_clean(self):
        self.fail("CURRENT_CREDENTIAL_MISMATCH", fp="f" * 64)

    def test_account_mismatch_blocks_even_when_ledger_clean(self):
        self.fail("BROKER_HANDOFF_ACCOUNT_MISMATCH", ledger={"account_id": "DIFFERENT"})

    def test_stale_producer_identity_blocks_even_when_ledger_clean(self):
        gate = SafetyDecision(True, "NO_ACTIVE_BROKER_OPERATION", "clear",
                              {"producer_pid": 9999, "producer_starttime": 9001})
        self.fail("BROKER_HANDOFF_PID_MISMATCH", gate=gate)


if __name__ == "__main__":
    unittest.main()
