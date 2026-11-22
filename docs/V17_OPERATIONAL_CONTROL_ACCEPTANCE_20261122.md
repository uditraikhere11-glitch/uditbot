# UditBot V17 — Runtime Control, Health & Reports correction

Integration baseline: GitHub `main` at `1c2b13743435654c31f9063c4ac1dceca9d57956` (2026-11-22). Operational candidate rebased from the original offline 96db0c6 patch while preserving PR #121 Setup changes.

## Design and functionality

- AuditBot is the product/engine name. The account-specific header and drawer use the configured instance label (e.g., UditBot, AmanBot); the drawer says "Powered by AuditBot · Trading System". The distracting blue logo square becomes a restrained monochrome brand mark.
- Trading Controls no longer duplicates Health Check. Health stays an independent workspace view.
- PR #121 six native Setup categories, the single 54-field Save & Apply form, masked Dhan header, and removal of duplicate Setup status/AMO controls are preserved verbatim.
- LIVE/TEST controls call `POST /control/mode` with CSRF, a revision fingerprint, explicit confirmation and the **existing** native `apply_settings` Save & Apply coordinator. Owner STOP, transition gates, active requests and runtime verification may disable changes. Setup form and all 54 unique names are untouched.
- Restart uses the **existing** `/setup/service-control` authenticated, owner-confirmed restart API. Disabled until the running service, mode and owner intent are verified; no independent service restart mechanism is added.
- The Running indicator pulses only with verified systemd mode evidence and respects reduced-motion preferences.
- The Dashboard's reconciliation, protection-watcher, entry-gate and Telegram-client statuses are derived from a 10-second producer snapshot written by the trading process. The read-only WebUI validates private-file permissions, age <=35s, process PID/starttime, configured Dhan account fingerprint and internal consistency; any stale or unreadable record is UNVERIFIED.
- Dhan profile verification uses a bounded read-only broker GET, never TOTP minting, broker order submission or implicit refresh. Token expiry is displayed only if backed by a verified broker/JWT expiry result; token-file presence is not proof of validity.
- Health distinguishes a connected Telegram client from message-delivery verification. The pre-existing Owner STOP/service-state conflict remains a WARN and blocks mode/restart or falsely OPEN status.
- Reports & Logs has its own read-only SQLite journal view (counts, recent orders and allowlisted safety events). It intentionally omits secrets/raw messages, raw arbitrary service logs and unverified broker executions.

## Production rollout caveats

- Development and tests were isolated. The read-only snapshot producer is new code in `src/main.py`; **it will NOT run in the existing trading process without a separately authorized and safety-reviewed trading-service restart**. A WebUI-only restart is safe for presentation rollout but Dashboard runtime items will stay UNVERIFIED until the new producer is live.
- `runtime_operational_snapshot.json` is created as a 0600 private file in the configured data directory. If the WebUI process runs under a different user and cannot read it, status must remain UNVERIFIED; do not weaken file permissions to make it green.
- Owner STOP conflict shown in the live screenshot must be independently investigated before any trading mode change, trading service restart, or new entries.
- Broker profile GET requires a configured Dhan token file, valid network access and independently matching account. A failed profile GET must not trigger new token generation.
- No production data or live broker action was used. The ZIP is **for review and independent acceptance, not an authorization to deploy**.

## Test criteria

- Full repository suite with pinned Telethon dependency, non-root and isolated fixtures.
- Browser safety checks on 390×844 and 1440×900, including failure/Owner STOP behavior.
- Native 54-field Setup remains unchanged; LIVE/TEST and Restart are wired through existing authenticated coordinators.
- Independent diff and patch replay against the exact GitHub baseline.

## Integrated offline acceptance (22 November 2026)

- Full isolated suite re-run as non-root in six shards: **1,271 tests and 1,919 subtests passed**, zero failures.
- Dedicated operational browser acceptance: both 390×844 and 1440×900, eight scenarios each, pass without external calls.
- Dedicated Setup browser acceptance: both sizes, all six native categories and all 54 unique fields, pass without state mutation.
- The initial root-run exception in a deployment-script test was an execution-context issue, not a code regression; all tests passed when rerun as the non-root bot owner.
- Final merge/restart remain subject to review and independent production operational acceptance.
