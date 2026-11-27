# UditBot — Unified Broker Ledger Release Runbook (Phases 2D–5)

## Overview
This runbook documents the consolidated UditBot release integrating:
- **Phase 2D (Accepted Architecture, C01–C68):** Unified `BrokerSnapshot` coordinator, `UnifiedAccountRuntime`, `StartupReconciliationGate` 10-second live verdict & transport interlock, semantic wire-bound `AuthorizedBrokerOperation`, `ProtectionWatcher` durable persistence readback & paired-snapshot resolution, `ConfigChangeTracker` unified shutdown gate, and Pydantic v2 compatibility.
- **Phase 3 (Golden Replay & Master Integration):** Complete G01–G12 Golden Replay suite with deterministic `multiprocessing.Event` cross-process synchronization, public `DhanClient` submission/cancellation verification, G02 external SELL recovery, and production restart recovery coverage.
- **Phase 4 (Runtime Efficiency & Status Parity):** Zero-HTTP WebUI broker status reuse from fresh read-only unified snapshots (`snapshots.sqlite3`, `mode=ro`), shared Telegram/WebUI `summarize_runtime_status()` projection, and bounded R02 watch-poll cadence.
- **Phase 5 (Release Consolidation & Deployment Compatibility):** End-to-end component verification, `.env` and `AppConfig` default preservation, systemd/deployment script syntax & entrypoint validation, and legacy file absence verification.

## Configuration & Deployment Compatibility
- All existing `.env` keys, risk/sizing defaults, Owner STOP, and Emergency STOP behaviors remain unchanged.
- Existing systemd units and helper scripts under `deploy/` (`install_vm_services.sh`, `dhan-token-refresh.service`, `dhan-token-refresh.timer`, `backup_runtime.sh`, `restore_runtime.sh`, `audit_vm_state.sh`, `migrate_existing_webui_to_control_center.sh`) remain compatible with `run.py`, `bot`, and `src.control_center_entry`.
- Retained YESBANK-EQ lots (`221261102657708`, `23126110439808`) are never auto-liquidated or mutated by startup reconciliation or watcher recovery.

## GitHub CI Verification
Before merging Draft PR #145 (`refactor/unified-broker-ledger` -> `main`):
1. Push the verified phase commits to `refactor/unified-broker-ledger` (no force push; never push directly to `main`).
2. Confirm `.github/workflows/unified-broker-ledger-offline.yml` and required PR checks pass at the exact pushed head under Python 3.12 with `requirements-test.txt` (Pydantic v2).
3. Obtain independent audit sign-off before merge or VM deployment.

## Rollback Procedure
If any issue is observed after a future controlled deployment:
1. Issue Owner STOP via WebUI or `./bot stop` and verify zero in-flight broker mutations.
2. Back up `data/` (`./deploy/backup_runtime.sh`).
3. Roll back code to frozen `main` (`0e7241de6880663508ce804ae84e33a476d1e6a2`) without deleting `data/trade_journal.db`.
4. Verify `./deploy/audit_vm_state.sh` before resuming service.
