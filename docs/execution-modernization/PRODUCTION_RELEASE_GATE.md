# UditBot modernization: performance acceptance and controlled release

Prepared 2026-11-21. Candidate: dev/execution-modernization-20261121.
**No live VM was accessed. No production Dhan requests, service restarts, merges,
or deployments were performed during this development.**

## Production hot-path changes that ACTUALLY execute (not shadow-only)

1. `src/journal/db.py`: the daily LIVE portfolio-risk state no longer groups
   every historical fill in the database on every incoming order. It retrieves
   only fills belonging to the current day's LIVE order IDs, using bounded
   parameterized query batches. Exact previous trade-count, position/exposure
   rules and safety cutoffs are preserved. Any failed fills read now raises so
   LIVE risk synchronization must fail closed, not silently report zero fills.
2. Add `idx_risk_orders_mode_day`, `idx_risk_fills_order_id`,
   `idx_intents_state_created` idempotently on journal initialization.
   They are additive and do not remove history, reallocate trades or change
   existing entry/protection authorization.
3. The proposed `src/broker/read_governor.py` fast path was tested and
   **rejected**. The synthetic median admission changed from 1.169 ms to
   1.152 ms (1.01x), insufficient to justify touching a safety-critical
   cross-process rate limiter. The original governor is unchanged from main.

The observer, shadow event ledger, opt-in FIFO dispatcher and deterministic
preparse remain separate enhancements. The latter two are OFF by default and
must not be counted in production performance claims.

## Verified offline evidence and its limitations

The risk SQL benchmark used a temporary REAL JournalDB schema with 30,000
synthetic historical fills and 1 current LIVE order, 10 repeated rounds:
- Original legacy query median: **22.384 ms**
- Indexed query median: **1.281 ms**
- **17.48x lower risk-query time**, with identical outputs and zero orders sent.

See GitHub Actions run 44881444093. This measures the database component
only. It does NOT establish 17.48x shorter Telegram-to-Dhan ACK, and the gain
depends on real journal size, current-day activity and VM disk conditions.
The governor experiment was independently benchmarked and reverted.
The full-current-head CI must be checked on the latest development commit
before proposing a release.

## Commands for supervised *read-only* VM evidence collection

Do not interrupt trading, modify .env, or copy database files with plain cp
while they are under WAL writes. Use a separately approved consistent SQLite
snapshot via the backup API. Example from the checked-out code directory:

```bash
# Set SOURCE ONLY after identifying the actual journal DB path locally.
# This example intentionally does not guess UditBot's file location.
read -r -p "Absolute existing UditBot journal DB path: " SOURCE
test -f "$SOURCE" || { echo "STOP: no such journal"; exit 1; }
mkdir -m 700 -p "$HOME/uditbot-readonly-bench"
export SOURCE
python3 - <<'PY'
import os, sqlite3
from pathlib import Path
src=Path(os.environ["SOURCE"]).resolve(strict=True)
dst=Path.home()/"uditbot-readonly-bench"/"journal-snapshot.sqlite3"
if src == dst or dst.exists():
    raise SystemExit("STOP: target exists or equals source (never overwrite)")
source=sqlite3.connect(src.as_uri()+"?mode=ro",uri=True,timeout=8)
try:
    target=sqlite3.connect(dst,timeout=8)
    try:
        source.backup(target)
    finally:
        target.close()
finally:
    source.close()
os.chmod(dst,0o600)
print("Consistent read-only journal copy prepared:",dst)
PY

python3 -m tools.measure_risk_query_snapshot \
  "$HOME/uditbot-readonly-bench/journal-snapshot.sqlite3" \
  --rounds 30
```

Output is anonymized aggregate timings and index availability only.
A pre-upgrade snapshot usually has no new fill index; record it as BASELINE.
The candidate requires a separately verified post-migration indexed snapshot.
Do not upload the snapshot, account identifiers, broker credentials, or full
journals to public GitHub or attach to a public PR.

## Production release gates (ALL must pass)

1. Commit-frozen head; all new tests + full repository offline suite are green.
2. Independent source review verifies no impact on reconciliation, protection,
   token auth, single owner, STOP, config and idempotent mutations.
3. Existing SQLite journal backed up consistently; confirm indexes and
   unmodified trading state before/after migration.
4. Dhan category-governor existing behavior remains unchanged; its
   cross-process quotas, cooldowns, mutation limits and busy-retry tests pass.
5. Current VM telemetry: matched p50/p95/p99 ingress, quote, broker HTTP,
   ACK, fill-to-protection, watcher API admission and journal lock waits.
6. No false HEALTHY, no duplicate order, no unverified OCO/Super protection,
   no journal evidence loss and no new broker rate-limits.
7. Measurable LIVE workflow value: objective remains >=20% improvement at p95
   where achievable without compromising safety. If only local SQLite latency
   improves while order end-to-end remains unchanged, report that clearly.
8. Owner authorizes PR merge and a separate supervised release. No automatic
   VM update. Preserve the rollback checkout and original databases.

**Rollback:** A source rollback can restore the pre-modernization Python
logic. Added SQLite indexes are additive and can remain in place; do NOT
delete/overwrite the live journal or attempt destructive schema rollbacks.
Do not disable the durable governor ledger to 'fix' failed admission.

**Recommendation:** Classify this as a reviewed production CANDIDATE only
after the latest CI passes. A live release requires VM-specific and
broker-authoritative evidence. Offline tests alone cannot certify that.
