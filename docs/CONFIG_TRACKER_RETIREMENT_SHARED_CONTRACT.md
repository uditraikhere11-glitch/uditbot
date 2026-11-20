# UditBot + AmanBot — unified configuration tracker retirement / broker-fence contract

**Status: REVIEW ONLY — NOT DEPLOYABLE.** Source of truth: `uditraikhere11-glitch/uditbot` `main` at `34f72ca89564fc2c9f2ba7db1a7b4f0e534801a0` (20 November 2026). One shared source version will later be rolled out to two *independent* VMs. Do not copy runtime files or secrets between VMs.

## Release boundary

**Retire only:** legacy/V2 configuration submissions, staging/replay, V2 activation installer, duplicate configuration change trackers, their obsolete routes/pollers/UI. New UI configuration updates must have exactly one entrypoint: `src/engine/simple_save_apply.py::apply_settings`.

**Preserve:** WebUI login/CSRF, existing Setup and Control Center screens, Owner START/STOP, deferred Owner STOP, emergency STOP, mode-transition protection, Dhan rate governor, order/exit/reconciliation code, risk controls, account binding, protective order watcher, existing process-version/credential verification, broker mutation ledger and locks. Do not rename the existing `tracker_v2_broker.lock`, `active_broker_mutations.json`, or `config_restart_v2/apply_barrier.json` during a mixed-version cutover. These are safety protocol paths, not disposable historical settings.

## VM evidence kept outside the shared repository

UditBot and AmanBot must each pass a fresh, private service/ledger/barrier/Owner STOP inspection before deployment. UditBot was checked on 20 November 2026 and both trading and WebUI processes still had the V2 flag enabled. Historical configuration evidence was present but not an active restart transaction. Operator-specific PIDs, change records, credentials, and VM state are deliberately excluded from shared source control.

## Implementation constraints

1. **Always-on ORDER admission:** `src/broker/dhan_client.py::_broker_mutating_http` must use account-/PID-/starttime-bound `broker_mutation` before every actual ORDER POST/PUT/DELETE, independent of `UDITBOT_TRACKER_V2_ENABLE`. Preserve governor ordering, return values, HTTP payloads, explicit no-send handling and `UNKNOWN` for ambiguous network outcomes. A diagnostic client lacking a producer identity must fail closed, not send a request.
2. **Standalone orphan cleanup:** `src/engine/orphan_cleanup.py::_cancel_forever` currently calls `broker.session.delete` directly. Route it through the fenced, rate-governed Dhan client path. Because the opt-in CLI runs in a distinct process, it must not impersonate the live producer. If no safe independent mutator identity is available, `--execute` must reject before transport, with a clear diagnostic. Preserve preview behavior and two-snapshot confirmation.
3. **Startup producer registration:** all genuine service entrypoints must register the account and broker handoff without the V2 feature flag. Never register arbitrary tooling / WebUI / CLI process as a trading producer. Fail closed on account mismatch, stale producer, active or unresolved broker mutations and corrupt ledger.
4. **Single config writer:** authenticated Save & Apply must validate the expected revision, apply an atomic compare-and-swap, and restart a RUNNING service at most once with evidence. A STOPPED service remains stopped. A no-op must not fabricate runtime verification. Existing owner STOP takes precedence.
5. **Legacy/V2 settings writer retirement:** prevent historic deferred configuration replay and old V2 submission, including imports from alternate routes, background coordinator, Watcher, CLI and tests. Preserve read-only histories, outstanding transition gates and owner-control operations. Do not clear or rewrite past records to make the upgrade pass.
6. **Owner STOP isolation:** extract deferred STOP readiness and completion from settings replay. Do not delete coordinator until this path works after process replacement, with known and unknown outstanding protection obligations.
7. **One-shot historical restart recovery:** independently verify a prior `RESTART_ISSUED`/`ACK_UNCERTAIN` from original journal, systemd PID+starttime, runtime hash, account and credential fingerprint and durable broker barrier. Never retry restart; do not release an unrelated barrier. Handle crashes before release, after release before journal, and after journal before UI readback idempotently.
8. **Backups and rollback:** retain full nested V2 journal/barrier evidence and metadata through new backups. Verify GnuPG round trip, permissions and traversal/corruption rejection in disposable environment. Do not delete VM data or remove systemd drop-ins as part of a GitHub source-only merge.

## Required adversarial acceptance suites

- A. Submission path mapping (10): allowed Setup; duplicate request; old route; undocumented API; V2 flag 0/1; alternate process; malformed POST; CSRF failure; stale revision; no-op.
- B. ORDER transport (10): POST, PUT, DELETE, Forever OCO, super order, standard cancel, governor reject, timeout, HTTP ambiguous status, unexpected exception.
- C. Broker ledger (10): missing, symlink, corrupt, wrong account, old PID, PID reuse, active mutation, UNKNOWN, barrier active, ledger write failure.
- D. Orphan cleanup (10): preview, 2 successful reads, stale holdings, T1 quantity, unresolved entry, wrong account, concurrent mutation, missing producer registration, governor deferral, cancel response mismatch.
- E. RUNNING Save & Apply (10): unchanged, accepted, failed restart, late ACK, wrong PID, wrong starttime, wrong mode, wrong hash, credential mismatch, unrelated broker barrier.
- F. STOPPED Save & Apply (10): owner STOPPED, unit inactive, missing PID, stale owner record, no-op, safe save, invalid mode, concurrent save, conflicting .env, no unexpected systemctl call.
- G. Owner STOP (10): owner STOP during save, delayed stop with open protected order, resolved order, watcher failure, restart, stale file, corrupt file, multi-worker race, explicit START, disallowed auto-start.
- H. Historical settings (10): QUEUED, IN_PROGRESS, APPLYING, WAITING_FOR_ORDER, READY_TO_APPLY, VERIFYING, malformed journal, missing transaction, absent deferred file, no replay after upgrade.
- I. Restart recovery (10): ACK_UNCERTAIN with flag off, before barrier release, after release crash, after journal crash, after UI record crash, old PID, PID reuse, wrong credentials, wrong account, unrelated change ID.
- J. Systemd cutover (10): mixed versions, service flags from EnvironmentFile, unrelated drop-in, locked service, env start failure, WebUI auth, backup non-overwrite, expected SHA, rollback, health proofs.
- K. Backup/restore (10): nested records, empty folder, missing archive member, GPG success/failure, symlink, traversal, partial write rollback, permissions, TEST-only restore, no dual-running brokers.
- L. Non-config regressions (10): BUY/SELL reconciliation, held shares, T1, Forever OCO placement, Forever OCO cancellation, AMO, rate governor, emergency STOP, signal parsing, protection watcher.

**120 numbered scenarios are a specification, not proof that 120 tests were implemented or passed.** Acceptance requires executable cases with real test output for each applicable scenario, plus the repository's existing regression suite.

## GitHub workflow and VM rollout

- Keep changes on a dedicated branch based on the exact approved `main` SHA; no direct edits to `main`.
- Produce changed-files-only ZIP (complete replacements), exact unified patch and deletion manifest; independent caller audit against fresh GitHub source; full tests including the 120 scenario matrix; no live Dhan/Telegram API calls.
- PR creation or merge only after the owner's explicit approval. Merge is separate from deploying.
- When approved, deploy to AmanBot **first** with its own broker-safe window and rollback evidence; independently verify before requesting a UditBot production rollout.
- Neither rollout may copy `.env`, `.dhan_token`, SQLite, data directory, Owner STOP state, Telegram session, Dhan account binding, or any runtime journal between machines.
- Actual systemd V2 drop-in cleanup is a separate host-specific action after compatible, running code is verified; GitHub deletion of an installer does not remove an installed override.

## Audit utility

`python3 tools/audit_tracker_retirement.py --mode baseline` produces a read-only JSON inventory and exits 0. `--mode release` fails if structural retirement checks remain open. This structural utility **cannot establish runtime correctness**; the broker, process, crash and backup tests above remain mandatory.