"""Phase 3 Portable Golden Replay Engineering Framework for UditBot.

Executes real production classes (DhanClient, ReadGovernor, DhanSnapshotSource,
CrossProcessReadCoordinator, BrokerSnapshot, JournalView, BrokerLedgerReconciler,
StartupReconciliationGate, ProtectionWatcher, ConfigChangeTracker, RiskManager,
orphan_cleanup, control_center_backend) behind deterministic offline transport
and clock boundaries.
"""
from __future__ import annotations

__all__ = [
    "clocks",
    "transport",
    "journal_seed",
    "fault_injection",
    "process_harness",
    "assertions",
    "runner",
]
