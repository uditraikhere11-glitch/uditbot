# Restart V2 runtime safety cases — proposed, unactivated behavior

| Event | Candidate behavior | Proof boundary |
|---|---|---|
| No field changes | Return `NO_CHANGE`, no restart, no invented runtime ACK | No-op tests |
| Trading service stopped | Save private new `.env`, journal `SAVED_FOR_NEXT_START`; do not start service | Stopped-service tests |
| Pre-existing legacy queue or Owner Stop | Block before write; preserve original operation and entry-gate files | Pending/Owner Stop tests |
| Running order placement/mutation | Positive, versioned handoff and matching producer PID/starttime mandatory | Fake gate only; real integration OPEN |
| Crash before `.env` replacement | `PREPARED` and backup survive; read-only classifier demands recovery | Journal classifier; crash injection OPEN |
| Crash after `.env` replacement | Recognize target revision, never infer runtime applied | Transaction classifier |
| Restart call initiated, response lost | Persist `RESTART_ISSUED` before invocation, then `ACK_UNCERTAIN`; no second call | Restart failure tests |
| Late valid ACK | Validate original expected hash/mode/fingerprint, PID/starttime, fencing and Owner Stop; no second restart | Late ACK test |
| Wrong ACK identity/hash/mode/fingerprint | Stay uncertain with entry fence | Negative ACK tests |
| External `.env` writer | Preserve external file, require manual reconciliation | CAS/recovery tests |
| Broker protection still running | Active mutation evidence blocks restart | Fake mutation tests |
| Owner Stop arrives during restart | Never release fence and never supersede stop | Race simulation |
| Barrier owner runs beyond threshold | No TTL release; live owner retains barrier | Separate-process test |
| Owner dies | Dead owner requires explicit recovery, never auto release | Separate-process test |

**Security prerequisites:** real trade-dispatch entry fence must be enforced by
running engine, not merely WebUI. `.env` CAS only protects against cooperating
writers: an arbitrary external writer racing between the final revision read
and atomic replace remains a residual risk; integration must unify writes
under the same system-wide lock and detect unexpected file changes.
