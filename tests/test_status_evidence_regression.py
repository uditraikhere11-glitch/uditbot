"""Offline checks for the evidence consumed by Telegram and Control Center."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.cli import format_startup_online_card
from src import control_center_backend as backend
from src.control_center_ui import _HTML
from src.engine.operational_snapshot import publish_operational_snapshot, read_verified_snapshot
from src.engine.runtime_evidence import proc_starttime


class StatusEvidenceTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.cfg = SimpleNamespace(data_dir=folder.name, dhan_client_id="synthetic",
                                   equity_amo_enabled=False, fno_amo_enabled=False)
        self.gate = SimpleNamespace(state=SimpleNamespace(value="HEALTHY"),
                                    is_accepting_entries=True)
        self.pipeline = SimpleNamespace(cfg=self.cfg, reconciliation_gate=self.gate,
                                        watcher=SimpleNamespace(_is_running=True))

    def test_in_progress_state_survives_snapshot_roundtrip(self):
        self.gate.state.value = "RECONCILING"
        self.gate.is_accepting_entries = False
        publish_operational_snapshot(self.pipeline)
        evidence = read_verified_snapshot(str(self.root), expected_pid=os.getpid(),
                                          expected_starttime=proc_starttime(os.getpid()),
                                          expected_account_id="synthetic")
        self.assertEqual(evidence["reconciliation"], "RECONCILING")
        self.assertIs(evidence["entries_open"], False)

    def test_restart_barrier_overrides_open_heartbeat_until_cleared(self):
        tracker = Mock()
        tracker.has_unresolved_pending_operation.return_value = False
        tracker.deferred_env_file = str(self.root / "deferred")
        tracker.get_change_record.return_value = None
        tracker.get_service_control_record.return_value = {}
        tracker.inspect_transition_gate_status.return_value = {"ok": True, "active": False}
        tracker.get_active_running_version.return_value = {
            "pid": os.getpid(), "process_starttime": proc_starttime(os.getpid())
        }
        raw = {"runtime_verified": True, "service_state": "RUNNING",
               "owner_intent": {"desired_service_state": "RUNNING", "observed_state": "RUNNING",
                                "deferred_stop_pending": False}}
        barrier = self.root / "config_restart_v2" / "apply_barrier.json"
        barrier.parent.mkdir()
        publish_operational_snapshot(self.pipeline)
        for active, expected in ((True, "BLOCKED"), (False, "OPEN")):
            barrier.write_text(json.dumps({"active": active}))
            barrier.chmod(0o600)
            from src.notifier.runtime_status import format_runtime_status
            observed = publish_operational_snapshot(self.pipeline)
            self.assertEqual(observed["entries_open"], not active)
            card = format_runtime_status(observed, instance="offline", mode="TEST")
            self.assertIn("New Entries Gate: " + ("BLOCKED" if active else "OPEN"), card)
            result = backend._build_control_status(tracker, raw, self.cfg, str(self.root / ".env"),
                                                   str(self.root))
            self.assertEqual(result["new_entries_gate"], expected)
            self.assertEqual(result["reconciliation"], "HEALTHY")

    def test_supervision_expiry_bounds_card_lifetime(self):
        from time import time
        self.pipeline.watcher._supervision_success_at = time() - 299
        self.pipeline.watcher._supervision_read_complete = True
        publish_operational_snapshot(self.pipeline)
        tracker = Mock()
        tracker.get_active_running_version.return_value = {
            "pid": os.getpid(), "process_starttime": proc_starttime(os.getpid())}
        status = backend._operational_evidence(tracker, {"runtime_verified": True,
            "service_state": "RUNNING"}, self.cfg, str(self.root))
        self.assertEqual(status["protection_supervision"], "VERIFIED")
        self.assertLessEqual(status["runtime_evidence_ttl"], 1)

    def test_healthy_but_owner_blocked_card_preserves_both_facts_and_time(self):
        self.gate.is_accepting_entries = False
        evidence = publish_operational_snapshot(self.pipeline)
        with patch("src.notifier.message_designer.render", return_value=None):
            card = format_startup_online_card(
                self.cfg, allowed_chats_count=1, reconciliation_ok=evidence["reconciliation"] == "HEALTHY",
                gate_open=evidence["entries_open"], watcher_verified=evidence["watcher_supervision_verified"],
                snapshot_at=0,
            )
        self.assertIn("Reconciliation: Healthy", card)
        self.assertIn("New Entries Gate: Blocked", card)
        self.assertIn("Startup snapshot: 05:30:00", card)
        self.assertIn("Protection Watcher: Unverified", card)

    @unittest.skipUnless(shutil.which("node"), "Browser logic check needs Node.js")
    def test_failed_refresh_and_older_success_cannot_retain_green_cards(self):
        refresh = _HTML.split("async function refreshStatus()", 1)[1].split("async function serviceAction", 1)[0]
        render = _HTML.split("function renderOperationalStatus(s)", 1)[1].split("async function refreshBrokerEvidence", 1)[0]
        script = """
const assert=require('node:assert/strict');
const elements = new Map();
function $(id) {
  if (!elements.has(id)) elements.set(id, {classList:{toggle(){},remove(){},add(){}},setAttribute(){},
    closest(){return this},querySelector(){return null}});
  return elements.get(id);
}
let current={}, ownerIntentSafe=true, statusRequest=0, evidenceExpiry;
const setText=(id,value)=>$(id).textContent=value;
const safeStatus=x=>x;
let resolveOld;
const requestJSON=()=>statusRequest===1 ? new Promise(r=>resolveOld=r) : Promise.reject(Error('offline'));
""" + "async function refreshStatus()" + refresh + "function renderOperationalStatus(s)" + render + """
(async()=>{
  renderOperationalStatus({reconciliation:'HEALTHY', protection_watcher:'ACTIVE', new_entries_gate:'OPEN'});
  const old=refreshStatus();
  await refreshStatus();
  resolveOld({service_state:'RUNNING',reconciliation:'HEALTHY',new_entries_gate:'OPEN'});
  await old;
  for(const id of ['v17p-gate','v17p-reconciliation','v17p-protection'])
    assert.equal($(id).textContent,'UNVERIFIED');
  assert.equal(current,null);
})().catch(err=>{console.error(err);process.exitCode=1;});
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
