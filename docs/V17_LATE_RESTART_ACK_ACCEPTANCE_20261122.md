# V17 — Late runtime acknowledgement during owner Restart

## Evidence and root-cause hypothesis

UditBot live read-only diagnostics on 2026-11-22 showed:

- `trading_bot.service` active/running, new PID `1756899` and strict runtime verification `True`.
- Owner desired state `RUNNING`, last action `OWNER_RESTART`, observed state **`STARTING`**, with no deferred STOP.
- Runtime acknowledgement `acknowledged_at - requested_timestamp = 7.87` seconds.
- The service coordinator allowed only six seconds for strict post-restart runtime verification, **starting after** its synchronous `systemctl` invocation. The screenshot timing supports, but does not conclusively establish, a timeout in the post-systemctl polling window; exact in-process timing logs were not available.

The LIVE restart itself had already happened; no further live restart was performed in developing this fix.

## Targeted code correction

1. Extend the production runtime-acknowledgement polling window from 6 to **20 seconds**, while retaining the existing 0.25-second poll cadence and all fresh PID/starttime, config hash, trading mode, minimum acknowledgment timestamp, and systemd checks inside `is_active_version_confirmed()`.
2. Use a monotonic clock to compute the wait budget (wall-clock corrections no longer advance or rewind the timer).
3. Preserve the existing isolated-test short timeout, fail-closed response, and Owner STOP and order-protection checks. Do **not** automatically change the owner's durable STARTING/STOPPED record based merely on an active systemd unit.

The code only improves time available for *strict verification*; it does not infer health, bypass an Owner STOP, place orders, modify reconciliation/position management, or write a runtime status snapshot.

## Tests

`tests/test_v17_restart_ack_window.py` tests:

- a valid acknowledged version at 7.87 seconds is accepted despite the former six-second cutoff;
- no valid acknowledgment still fails closed after a bounded wait, retaining its precise error;
- an invalid PID/hash identity never becomes success simply because time elapsed;
- isolated unit tests preserve the fast custom polling interval.

Existing focused V14 service-control, mode-switch and tracker-health tests must remain green. Full GitHub CI must pass on the exact PR head before merge.

## Deployment constraints

**Code merge is not approval to deploy or restart.** The current VM still has the durable `OWNER_RESTART / observed_state=STARTING` record. Installing this code alone will **not** modify that record. A one-time explicit, backed-up record reconciliation is possible only after independent strict runtime and owner-intent checks, no pending STOP, no active mode-transition gate, and broker/open-order protection review. Do not invoke `systemctl restart` merely to test the change.

A subsequent owner-authorized, broker-safety-gated Restart is required to activate the already merged PR #123 `run.py` snapshot producer. Verify fresh snapshot age, systemd MainPID, account fingerprint, owner-intent PASS, watcher and reconciliation conditions, and independent Health checks afterward.

## Risks and limits

- Longer frontend Restart requests may remain pending for up to 20 seconds of verification after `systemctl` returns. HTTP clients/proxies must tolerate that duration.
- An acknowledgement arriving after 20 seconds continues to fail closed, and operator review is required; this patch intentionally does not auto-rewrite an ambiguous owner-control record.
- The observed 7.87-second request-to-ack interval is not identical to the coordinator's post-systemctl poll duration; the change mitigates the measured short-window failure mode but needs live validation after separately authorized deployment.
