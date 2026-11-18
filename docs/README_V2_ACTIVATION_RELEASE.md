# UditBot — V2 configuration tracker activation / audit package

**Development baseline:** `uditraikhere11-glitch/uditbot` main at exact commit `11d2c4c3c48264a03e498761b1e5158c8eb6706a` (PR89/PR90 merged). **Scope:** opt-in deployment tooling, dual-service save routing, and accurate Control Center owner-intent health. This package is **NOT** an instruction to change the running LIVE VM before safe broker/owner checks.

## What changed (and what did not)

| Component | Change |
|---|---|
| `deploy/tracker_v2_preflight.py` | Read-only offline systemd, private backup, service intent, V2 transaction, broker mutation and runtime evidence checks. Does not invoke Dhan/Telegram. |
| `deploy/tracker_v2_activation.sh` | Explicit two-service systemd activation; refuses to stop/start/restart LIVE trading service; restarts WebUI only when trading is positively stopped by owner. Provides strict initial rollback, inspection and post-start verification. |
| `src/engine/tracker_v2_dual_service.py` | Pure read-only gate preventing legacy WebUI saves over a V2 process/transaction and rejecting V2 saves when the running producer cannot be verified. |
| `src/engine/tracker_v2_health.py` | Pure owner intent/service mismatch classification. Stale Owner STOP must show WARN, not all green. |
| `src/engine/webui_update.py` | **Exact narrow source integration**: call dual-service route guard before the existing V2/legacy dispatcher. Existing authentication/CSRF and actual `submit_v2_webui_patch` remain untouched. |
| `src/control_center_backend.py` | **Exact narrow source integration**: add read-only `Owner service intent` health check. Do not report broker auth/profile as PASS without a live check. |
| `tests/test_tracker_v2_*.py` | Isolated regression tests for backup checks, malformed records, mismatched flags, owner-control discrepancies, and pinned source injection. |

**Not modified:** trading BRAIN, strategy/parsing, risk sizing, Dhan HTTP payloads, read governor, reconciliation, broker ledgers, V2 candidate core, credential fingerprint algorithm, existing Telegram templates, systemd unit source files, `.env`, LIVE order and journal data.

### Scope of GitHub branch / changed-files ZIP

The full source of two *existing* files is about 53 KB. To make this archive small, `patches/build_source_replacements.py` verifies the exact original **Git blob SHA** for each input file, rejects drift, and emits complete, ready-to-review replacement files into a new output folder. **It never overwrites the repository.** New source files, scripts and tests are complete within this ZIP. No old-baseline source is substituted.

## Safety / operator conditions

- Owner STOP intent was independently reconciled by the operator on 18 Nov 2026: desired `RUNNING`, observed `RUNNING`, deferred STOP `False`, runtime VERIFIED, PID unchanged. The transition gate is `VALID_INACTIVE` with `active=False`; its historical reason string does not require clearing. The activation preflight nevertheless re-checks current records and refuses unsafe drift. **Do not overwrite `service_control_state.json` manually.**
- Broker identity/auth remain `UNKNOWN` in the read-only dashboard. Local GET budget was exhausted (~49,971 seconds at the last snapshot). **No proof of broker health, flat positions or OCO safety exists from these health checks.** The activation tool does not bypass rate limiter/reconciliation.
- The activation is deliberately **stopped-service only**. If the broker-safe Control Center STOP cannot complete, production activation is BLOCKED. Broker GET 429/cooldown is not evidence that the account is flat. Do not force `systemctl stop` to get around the gate.
- Before staging, produce a fresh private runtime backup **after** the trading service is stopped; confirm that the archived `.env` exactly matches the stopped runtime and that the archived SQLite journal passes `quick_check` and matches critical row counts, copy it **off the VM**, and store it securely. An unencrypted backup contains credentials: restrict its filesystem permissions and do not upload to GitHub/Telegram.
- PR91 is open for GET governor and reconciliation; its changes are separate. Re-audit this package against a new main if PR91 merges before this work.
- The script must run from the clean Git checkout at the **exact approved release commit**; do not run it from an untracked ZIP-extracted directory.

## Production sequence (only after approval and independent broker verification)

1. Merge approved source changes and confirm exact `git rev-parse HEAD`. Do not activate simply because CI is green.
2. In Control Center request **Stop Bot**. Wait for `systemctl is-active trading_bot.service` to report `inactive` **and** owner intent to record `STOPPED_BY_OWNER`. If STOP is deferred, stop here. Do not force the stop or clear old safety records.
3. Create a new private backup after stop. Validate the archived SQLite snapshot and exact `.env` against the stopped runtime, then make an off-VM copy. Determine full backup path (`/home/.../uditbot-backups/...tar.gz`). The standalone `backup_runtime.sh` in main uses GPG; the owner may instead use an independently verified plain archive, but safeguard credentials.
4. Run, on the uditbot VM, from the exact clean merged source:

   ```bash
   cd ~/uditbot
   HEAD_SHA=$(git rev-parse HEAD)
   BACKUP=/absolute/path/to/verified/private/runtime-backup.tar.gz
   bash deploy/tracker_v2_activation.sh --preflight "$HEAD_SHA" "$BACKUP"
   bash deploy/tracker_v2_activation.sh --apply-stopped "$HEAD_SHA" "$BACKUP" I_HAVE_OFF_VM_BACKUP_AND_AUTHORIZE_STOPPED_V2
   ```

5. Script installs root-owned, non-secret `/etc/uditbot-tracker-v2/activation.env` referenced by **both** unit drop-ins. It checks *effective systemd* configuration, restarts **only `auditbot-web.service`**, verifies its running V2 flag and unauthenticated redirect to login, and leaves trading_bot.service **stopped**.
6. Independently verify the account/token, Dhan order book, positions/holdings and protective OCO state (broker read-only, respecting GET limiter), and that emergency STOP/owner intent permit reactivation. If any item is unknown, DO NOT START LIVE.
7. Use the authenticated Control Center Start Bot with explicit LIVE confirmation. No CLI force-start. The existing `control_service('start')` records RUNNING owner intent after positive runtime ACK.
8. Verify both processes and durable handoff without sending any broker request:

   ```bash
   cd ~/uditbot
   bash deploy/tracker_v2_activation.sh --verify-running "$(git rev-parse HEAD)"
   ```

9. On **TEST staging only**, perform no-change save (no restart); then one harmless genuine config change and confirm exactly one restart, new PID/starttime, canonical hash, HMAC fingerprint, exact saved `.env` SHA, `VERIFIED` durable V2 transaction and the existing Telegram log-group confirmation. Exercise failed broker GET, pending order, Owner STOP and late ACK.
10. LIVE save can be authorized separately only after staging and reconciliation/governor findings are resolved. **Successful activation is not proof that any new LIVE order is safe to place.**

## Rollback / recovery

- Before the **first** V2 trading producer has started (no V2 journal, barrier or ledger): after confirming bot remains stopped, run:

  ```bash
  bash deploy/tracker_v2_activation.sh --rollback-stopped "$(git rev-parse HEAD)" I_AUTHORIZE_STOPPED_V2_ROLLBACK
  ```

  This removes only this package's marked unit drop-ins / flag file, reloads systemd and restarts WebUI. It does not touch the bot, order journal, owner intent or credentials.
- **After a V2 trading producer, a saved transaction, or an apply barrier exists, automatic rollback is FORBIDDEN.** Preserve evidence; manually reconcile the exact PID/hash/account and broker mutation record before any downgrade. Do not delete `active_broker_mutations.json`, `config_restart_v2`, `pending_*`, or any safety flag to make the UI green.
- Any uncertain restart ACK must be handled through the existing authenticated recheck (which never issues a second restart). Never retry the same configuration request with a new request ID just to bypass an unresolved transaction.
- If WebUI startup fails during initial stopped activation, the script may remove **its own** flags only when it positively confirms the trading service remains stopped and no V2 transaction/barrier/ledger exists. Otherwise it preserves state and requires operator recovery.

## Testing and remaining limits

Local offline candidate development tests run with no Dhan, Telegram or LIVE VM. The backup preflight now rejects fake/non-SQLite archives, stale `.env` snapshots, mismatched critical journal counts and duplicate archive entries. Its temporary SQLite validation copy is private and deleted; it never modifies the trading journal. The earlier VM ran 57/57 PR89/90 tracker tests (9,640 deterministic scenarios), but those were before this activation integration. A full merged-repository test run and disposable systemd+login/CSRF+single-restart acceptance must be run on the final branch before a production change. See `docs/PRODUCTION_VERIFICATION_REPORT.md` for the fresh verification decision (64 local tests passed under three umasks; LIVE remains BLOCKED). The source replacement script deliberately refuses a newer or altered baseline; regenerate and re-audit if GitHub moves.

**Known production blockers:** the LIVE service is currently running and intentionally must not be stopped except through its broker-safe Control Center operation; Dhan authentication, account binding, order/holding protection and GET governor recovery remain unverified; no fresh off-VM backup is confirmed. Owner intent was reconciled but must be rechecked at deploy time. These are **not** bypassed by this patch.

## Final audit report

Read `docs/PRODUCTION_VERIFICATION_REPORT.md`. This code package is an offline candidate only; **production V2 release is blocked** until the listed real-world gates are verified.
