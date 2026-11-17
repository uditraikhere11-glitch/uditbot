# UditBot tracker V2 — disabled-by-default review candidate

**Baseline:** `bd594af2e80b92f7ff0349971ba2f2f34150ab4d` (GitHub PR #86).

## Scope

This is a **new isolated implementation**, not a replacement for the existing
running coordinator. It introduces these additive review modules:

- `src/engine/tracker_safety_preflight.py`: independent read-only evidence validator.
- `src/engine/tracker_v2_safety.py`: strict JSON readers, process/barrier/ACK
  evidence checking, active broker-mutation handoff and legacy migration preview.
- `src/engine/tracker_v2_candidate.py`: off-by-default single-save coordinator with
  optimistic CAS, private atomic backup, exclusive lock, idempotency key, at-most-once
  restart intent, explicit ACK uncertainty and late ACK recovery.
- `tests/test_tracker_safety_preflight.py`, `tests/test_tracker_v2_review.py`:
  offline positive and negative cases.

The candidate coordinator itself never imports a live broker or starts services.
An **additional opt-in broker mutation interlock** is now present in
`src/engine/broker_handoff.py`, the seven actual Dhan order POST/PUT/DELETE
send sites, and the real trading pipeline registration. It is deliberately
inactive unless `UDITBOT_TRACKER_V2_ENABLE=1` is explicitly configured; the
current WebUI still does not activate or consume the coordinator. Direct Dhan
price-feed and authentication POSTs are excluded from broker mutation guards.

**Off by default:** `CandidateCoordinator(data_dir, env_path)` rejects `submit`
and `recover_late_ack`; unit tests instantiate with `feature_enabled=True` and
inject **fake** operation-gate, systemd restart and runtime ACK callbacks.
There is **no enabled WebUI save/restart integration** in this submission.
The new broker mutation guard is off by default and must not be enabled on
LIVE without full coordinator handoff and recovery validation. It must **not** be enabled on a LIVE VM until the independent
review and separate integration/VM staging tests pass.

## Why this scope

The requested previous candidate archive and independent audit were absent from
accessible materials. Full GitHub source cannot be locally cloned in this
sandbox because outbound DNS/network is disabled. We confirmed the latest
branch and key blob hashes through GitHub read-only connector, then created
strictly additive new paths to avoid overwriting PR #84, #85 and #86 fixes.
The existing `src/engine/webui_update.py` still owns live saves; nothing here
replaces its behavior.

## Required work before an owner-approved integration PR

1. Reproduce full repository checkout at pinned latest HEAD; refresh SHA if
   `main` moves. Verify all new paths are absent from repo and apply patch.
2. Integrate the coordinator behind an **environment switch off by default**
   into existing authenticated routes and the shared transaction lock. Preserve
   FastAPI's exact form/CSRF/password handling and existing AMO controls.
3. Implement the trading-process participation in the entry fence (not just
   a WebUI-side file), PID starttime, credential fingerprint, and verifiable
   broker-operation handoff. Until then running restarts must fail closed.
4. Confirm the expected hash is derived from canonical `compute_config_version`
   of the newly effective settings; callback assertions alone are insufficient.
5. Resolve the exact owner-stop and legacy transaction migration schemas,
   manual-risk/protection watcher evidence and START/STOP transitions.
6. Exercise actual service restarts only in controlled TEST staging with fake
   Dhan/Telegram and a disposable `systemd` unit.
7. Run the complete GitHub repository's PR83–PR86 regression CI with true
   network isolation. Never claim the isolated 63 tests are the full suite.
8. Return a full GitHub tree inventory and verified `git apply --check` against
   latest upstream. This package includes a critical-file subset, not a complete
   repository inventory.

## Safety limitations

- No automatic existing-trade SL/target/OCO/Super Order changes.
- No automatic Dhan orders or order cancellations.
- The in-memory fake injected into unit tests is **not** a runtime handoff.
- A process restart whose outcome is uncertain is not reissued, and the file
  is not rolled back as if the process reverted.
- A late ACK may only release a matching entry fence after exact identity,
  configuration hash, mode, credential fingerprint and Owner Stop checks.
- Any mismatch or ambiguous state requires operator review; no TTL expiry.

**No GitHub branch or PR created, no VM updated, no service restarted.**
