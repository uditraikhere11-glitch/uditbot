"""Phase 4 targeted tests: runtime reliability, zero-HTTP WebUI snapshot reuse, and Telegram/WebUI status parity."""
from __future__ import annotations

import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.config import AppConfig, TradingMode
from src.control_center_backend import _BROKER_CACHE, _cached_broker_status, _build_shared_runtime_status_summary
from src.engine.watch_poll_policy import (
    AFTER_HOURS_SECONDS,
    MANUAL_RISK_OPEN_SECONDS,
    acknowledged_settled_manual_risk_only,
    watcher_cadence_seconds,
)
from src.notifier.runtime_status import (
    RuntimeStatusTransitions,
    format_runtime_status,
    summarize_runtime_status,
)
from tests._phase2d_live_harness import persist_synthetic_snapshot_to_sidecar
from tests.test_phase2d_c01_c05_and_settlement import _make_snapshot


class TestPhase4RuntimeEfficiencyAndParity(unittest.TestCase):
    def setUp(self):
        _BROKER_CACHE.clear()

    def tearDown(self):
        _BROKER_CACHE.clear()

    def test_webui_broker_status_reuses_fresh_unified_snapshot_with_zero_http(self):
        """Phase 4: _cached_broker_status(refresh=False) reuses fresh read-only unified snapshot with 0 broker HTTP calls."""
        from src.engine.protection_watcher import ProtectionWatcher
        from src.engine.reconciliation import StartupReconciliationGate
        from tests._phase2d_live_harness import LivePhase2DHarness

        acct = "1113731536"
        h = LivePhase2DHarness(account_id=acct)
        self.addCleanup(h.close)
        os.chmod(h.cfg.dhan_token_file, 0o600)

        # Without snapshot -> UNVERIFIED, 0 HTTP calls
        with patch("src.broker.dhan_client.DhanClient", side_effect=AssertionError("No broker client allowed")):
            r0 = _cached_broker_status(h.cfg, refresh=False)
            self.assertEqual(r0["status"], "UNVERIFIED")

        # Run real StartupReconciliationGate.run_unified_startup_verification to publish real sidecar snapshot
        watcher = ProtectionWatcher(h.client, path=str(Path(h.cfg.data_dir) / "watcher.json"), journal_db=h.db)
        class _Stop:
            def is_stopped(self): return False
            def get_in_flight(self): return []
        gate = StartupReconciliationGate(h.client, h.db, watcher, _Stop())
        ok_u, msg_u = gate.run_unified_startup_verification(data_dir=str(h.cfg.data_dir))
        self.assertTrue(ok_u, msg_u)

        # Now _cached_broker_status(refresh=False) MUST return VERIFIED with 0 broker HTTP calls!
        with patch("src.broker.dhan_client.DhanClient", side_effect=AssertionError("No broker client allowed")):
            r1 = _cached_broker_status(h.cfg, refresh=False)
            self.assertEqual(r1["status"], "VERIFIED", r1)
            self.assertEqual(r1.get("source"), "unified_snapshot")
            self.assertTrue(bool(r1.get("snapshot_id")))

    def test_telegram_and_webui_share_canonical_runtime_status_projection(self):
        """Phase 4: Telegram format_runtime_status and WebUI runtime_status_summary derive from identical projection."""
        op_snap = {
            "reconciliation": "HEALTHY",
            "entries_open": True,
            "watcher_running": True,
            "watcher_supervision_verified": True,
            "created_at": time.time(),
        }
        uni_snap = {
            "available": True,
            "status": "FRESH",
            "session_phase": "SETTLED",
            "profile_verified": True,
            "display_only": True,
            "authoritative_for_mutation": False,
        }
        tg_summary = summarize_runtime_status(op_snap, unified_snapshot=uni_snap)
        webui_summary = _build_shared_runtime_status_summary({"operational_snapshot": op_snap}, uni_snap)
        self.assertEqual(tg_summary, webui_summary)
        self.assertEqual(tg_summary["status_badge"], "HEALTHY")
        self.assertTrue(tg_summary["entries_open"])
        self.assertTrue(tg_summary["supervision_verified"])
        self.assertFalse(tg_summary["authoritative_for_mutation"])

        card = format_runtime_status({**op_snap, "unified_broker_snapshot": uni_snap}, instance="UditBot", mode="LIVE")
        self.assertIn("Unified Snapshot: FRESH (SETTLED)", card)
        self.assertIn("New Entries Gate: OPEN", card)

        # Blocked state parity
        blocked_op = {**op_snap, "reconciliation": "HALTED", "entries_open": False}
        blocked_summary = summarize_runtime_status(blocked_op, unified_snapshot=uni_snap)
        self.assertEqual(blocked_summary["status_badge"], "BLOCKED")
        self.assertFalse(blocked_summary["entries_open"])

        # Jitter suppression preserved
        tr = RuntimeStatusTransitions()
        self.assertIsNone(tr.observe(op_snap, instance="UditBot", mode="LIVE"))
        self.assertIsNone(tr.observe(blocked_op, instance="UditBot", mode="LIVE"))
        emitted = tr.observe(blocked_op, instance="UditBot", mode="LIVE")
        self.assertIsNotNone(emitted)
        self.assertIn("New Entries Gate: BLOCKED", emitted)

    def test_watch_poll_policy_bounded_cadence_for_acknowledged_manual_risk(self):
        """Phase 4: Settled acknowledged manual-risk holdings stay on bounded 120s/300s cadence (no hot polling)."""
        rec = {
            "order_id": "221261102657708",
            "product_type": "CNC",
            "filled_qty": 1,
            "position_state": "UNPROTECTED_MANUAL_RISK",
            "_schedule_ack_verified": True,
        }
        self.assertTrue(acknowledged_settled_manual_risk_only({}, [rec]))
        self.assertEqual(
            watcher_cadence_seconds(pending={}, open_records=[rec], now=time.time(), market_open=True),
            MANUAL_RISK_OPEN_SECONDS,
        )
        self.assertEqual(
            watcher_cadence_seconds(pending={}, open_records=[rec], now=time.time(), market_open=False),
            AFTER_HOURS_SECONDS,
        )


if __name__ == "__main__":
    unittest.main()
