# Combined release review

Baseline d59feee9b47f83967c483ab9ad930a68b8a49b74. Candidate 8fa6d5693d1822d1913469e1e32a28750871253b. The combined branch carries the account-binding correction with additional protection against lost evidence tables. Its tests reproduce the package's failure modes and extend them to abrupt child-process exits.

| Risk | Expected evidence | Release block |
|---|---|---|
| Lost SQLite binding or evidence | Same or different account cannot reopen damaged ledgers. Original bytes survive. | Any silent recreation |
| Mid-sync crash, single or multiple lots | Existing unified exit integration tests restart from the synthetic journal and verify allocations and gate state. | Extra or missing allocation |
| Lost ACK after POST, HTTP 504 or remote 429 | Same correlation survives restart. A repeated call sends zero HTTP mutations. STOP state remains independently controlled. | Duplicate send or inferred execution |
| Signed CNC/MTF and partial fills | Explicit holdings and consistent broker fills required. Missing holdings keeps the gate closed. | False-flat decision |
| OCO or Super exit | Terminal sibling state and exact SELL execution must agree. | Protection inferred from an ambiguous leg |
| Notification delivery | Pipeline enqueues locally. Outbox failure returns false. Telegram delivery runs in the worker. | Transport on pipeline thread |
| Performance | SQL and ingress figures measured offline. Broker ACK and protection require matched, anonymized session samples. | Claim of live benefit from fixture timings |

Review the focused and full CI logs at the candidate commit and all benchmark JSON before approving the existing draft PR. Benchmark results depend on runner load. A local SQLite enqueue can still wait on journal contention.

## Rollback

After a single squash merge, run `git revert <squash-commit-sha>` on a rollback branch and submit that change as one reviewed PR. Preserve the canonical journal, optional evidence files, STOP state and outstanding protection. A code revert cannot reverse broker executions. Require read-only reconciliation of outstanding unknown orders and protection before resuming entries under the older code.
