# Manual-risk supervision: 120 seconds and one closed-session snapshot

Baseline: main be1ee04707e0b86a378124fea2c5f113703fe1ef (includes PR #82).
Incorporates and supersedes the polling candidate in PR #81 at ed7b4e28.
Do not independently merge the older PR #81 after this implementation.

## Behavior

Both PR #82 exclusions are removed. Manual-risk positions remain open and
supervised. The reduced schedule requires a current local JournalDB owner
acknowledgement bound to the configured Dhan account, security and open quantity.
Positions with recorded exit legs or unresolved obligations do not qualify.

Eligible holdings are checked every 120 seconds during the existing lifecycle
market window (including its post-close settlement buffer). Outside that window,
one completed, readable supervision pass runs per process/session. A restart
requires fresh evidence. Routine checks then stop until reopening. Holidays and
session boundaries use the existing market calendar, not a new schedule.

The 2-second heartbeat only inspects local state. New orders, changed safety
state, acknowledgement removal, and reopening wake a broker pass. Orders arriving
during a pass are not swallowed. Uncertain protection/cancellation is urgent,
including records already moved into terminal_orders. Pending/new orders retain
the bounded fast window from PR #81. Other holdings retain periodic supervision.

Read deferrals do not consume the one-shot allowance. Unsuccessful one-shot
passes retry after 60 seconds subject to durable governor admission. Completed
means required broker books were readable, not merely that check_once returned
False (which also means no state change). Uncertainty keeps the entry gate closed.

A manual-risk holding whose broker quantity is missing or reduced without linked
SELL evidence enters RECONCILIATION_REQUIRED. It is never fabricated as CLOSED,
PROTECTED, or a SELL fill. Existing journal acknowledgement is not deleted.

## Remaining outside-hours trade-off

No polling design can observe a new external Dhan manual trade after the last
snapshot without another broker read or an external event. This policy deliberately
accepts that interval until reopening. The watcher logs this limitation when the
one-shot completes. Local state changes and uncertainty interrupt the pause.
request_supervision_check() supplies an in-process hook for an operator/broker
integration; this PR does not wire a new WebUI button or broker websocket, and
must not be described as detecting unseen external events immediately.

The completed snapshot is supervision evidence, not permission to clear STOP,
resume a blocked entry gate, or change risk settings. No governor reset, token
refresh change, production DB migration or VM deployment is part of this PR.

## Review and deployment gates

Run focused scheduler/governor tests, all offline regression tests and compileall.
Review against latest main; require Gemini changed-files review and independent
audit before owner-approved merge/deployment. Preserve each VM's account/data,
back up before deployment and verify journal/reconciliation health after restart.

This supersedes the dangerous monitoring skip, not the unrelated WebUI config
apply issue. No changes to parsers, risk sizing, order placement or WebUI are made.
