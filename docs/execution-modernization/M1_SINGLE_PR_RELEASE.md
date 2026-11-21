# M1 — Single-PR engineering milestone and one-step *code* rollback

**Track:** https://github.com/uditraikhere11-glitch/uditbot/issues/115
**Sole integration PR:** https://github.com/uditraikhere11-glitch/uditbot/pull/113
**Retired unmerged PR:** https://github.com/uditraikhere11-glitch/uditbot/pull/114
**Status:** DEVELOPMENT; no merge or production deployment is authorized.

## Why one PR

All previously staged risk-query optimization and event/ingress/recovery
work must be reviewed as **one integrated candidate**. PR #114 was closed
without merge only AFTER its production files and benchmark sources were
SHA-verified identical to the integrated branch.

The owner requests a release that can be backed out without reverting
85 individual development commits. **Use GitHub's SQUASH AND MERGE**, which
writes **one** new commit to `main`. Do NOT use merge-commit or rebase
merge for this milestone.

## Limits of "one-go" rollback

A single `git revert <squash-SHA>` reverses the **source code** of M1 in a
single new commit (provided there are no intervening conflicting changes).
It does **not**:
- Undo previously accepted Dhan orders, trades, cancellations or fills;
- Erase/create/repair real SQLite journal records;
- Reset the durable rate-limit or Emergency STOP ledger;
- Undo SQLite schema changes automatically;
- Restart the live VM or verify its broker reconciliation state.

M1 adds only additive indexes to the existing journal on initialization.
Indexes can remain after rollback; do not delete the SQLite database or run
destructive migrations. Shadow WebSocket data lives in a separate, opt-in
`.shadow.sqlite3` file and must not be used as trading authority.

## Release procedure AFTER explicit owner approval

1. Pin and record `main` base SHA, review commit SHA, successful full CI
   and reviewed Dhan/SQLite safety evidence.
2. Confirm a consistent, private SQLite backup using the SQLite backup
   API, active STOP state, unresolved protection attempts, account identity,
   pending orders and bot-owned holdings (including CNC/MTF).
3. Review the full PR diff for zero hidden broker mutations, order retries,
   protection cancellation, journal resets or config changes.
4. **Squash and merge PR #113**, record the returned single squash SHA.
5. Separately authorize a supervised, maintenance-window VM deployment.
   A merge does **not** trigger an automatic trading-service restart.
6. Verify startup reconciliation against Dhan authoritative books and
   read-only latency baselines. No `HEALTHY` without verified protection.
7. If a regression is discovered, invoke the STOP/entry-gate procedure
   first (without cancelling unknown live orders), then open a single
   rollback PR that reverts the recorded squash SHA:
   `git revert --no-commit <squash-SHA>`; inspect `git diff --cached`,
   make one rollback commit and submit it for approved merge. No reset,
   force push or journal overwrite.
8. Supervised VM rollback is a separate explicit operation. Revalidate
   token/account, durable intents, open exposure, broker-side SL/target
   and protection status before resuming any entry.

## Real prioritized technical work

| Area | Status |
|---|---|
| Indexed daily risk query | Real code optimized, offline benchmark 11.8–17.5x for that component |
| Dhan remote 429 mutation ambiguity | Hardened for regular/Super/OCO mutation paths; test + CI gate required |
| Deterministic FIFO ingress and preparse | Opt-in OFF by default, perf not demonstrated |
| Dhan order-update observer | Read-only, explicit opt-in, reconnect/silence handling; no trading authority |
| Shadow event ledger | Durably replayable, separate SQLite file |
| Signed CNC/MTF day-sale evidence | Existing gate retained; synthetic property and real-stack tests required |
| Crash/restart across live Dhan books | Not proven without supervised actual broker evidence |
| Real VM before/after p95 | Not measured; no production speed claim |
| Owner approval for merge/deploy | Pending |

## Hard acceptance gates

- Full current-head CI green, including 365-day synthetic runs, multi-lot
  signed sale, duplicate order correlation and remote HTTP-429 ambiguity.
- Zero observed order resubmission after timeout/429/unknown ACK.
- Zero false-flat, false-HEALTHY, or automatic OCO cancellation based on
  shadow WebSocket order-status alone.
- Single broker mutation owner, unchanged risk caps, STOP, user settings
  and account identity.
- Measured current VM p50/p95/p99 before/after with matched traffic; do
  not enable opt-in experiments unless they beat current performance and
  pass safety tests.
- Independent review and explicitly approved cutover/rollback.

**Do not merge or deploy while any line in the milestone checklist is open.**
