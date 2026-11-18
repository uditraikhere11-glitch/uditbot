# Simple Save & Apply (replacement for configuration tracker UI)

The Setup page's **Save & Apply** button now makes one synchronous request to
\`POST /setup/save-apply\`. Its response is a short success/error message, not a
six-step tracker, progress bar, background worker, pending-change ID, or V2
configuration transaction.

- Validate all submitted fields using the existing canonical field schema,
  require CSRF and a current page revision, and preserve unrelated .env keys.
- Replace .env atomically with owner-only 0600 permissions and a short-lived
  file lock (no persistent transaction or V2 journal).
- If trading_bot.service is RUNNING and values changed, issue ONE systemd
  restart; if stopped, save for next owner-controlled start without starting
  LIVE trading.
- No save-time Dhan GET or broker mutation. Trading startup remains subject to
  the existing independent reconciliation, emergency STOP and broker guards.
- The page does not resurrect unfinished legacy/V2 configuration operations.
  Existing safety data is preserved, not automatically erased; these records
  need a separate audited migration if they currently hold an entry fence.
- The old asynchronous endpoints remain in code temporarily for external
  compatibility but are no longer called by the Setup page. Remove only after
  their callers are audited; deleting the trading-core ConfigChangeTracker
  would also destroy Owner STOP/control interfaces.

**Important:** Restarting an already running LIVE service interrupts the watcher.
Do not deploy this candidate until its safety implications and production
acceptance tests have been reviewed. This PR is intentionally unmerged.
