# Production candidate: indexed risk-state journal queries

## What changed

UditBot's canonical `JournalDB.get_live_portfolio_risk_state()` used to
execute `SELECT broker_order_id, SUM(traded_quantity) FROM fills GROUP BY
broker_order_id` for *every* new-order portfolio risk check. This grouped
the entire retained history, even when no historical order belonged to today.

The candidate now:
1. Retrieves today's LIVE broker order IDs (same semantics as before).
2. Groups fills by those IDs only, using parameterized batches of 400.
3. Adds idempotent indexes on `fills(broker_order_id)`,
   `orders(trading_mode,created_at)`, and
   `order_intents(state,created_at)` during existing journal initialization.
4. Converts a previously swallowed fills-table SQLite read error into an
   exception so the existing LIVE RiskManager fails closed.
5. Uses `EXISTS` with a broker-order-ID equality to count accepted partially
   filled canceled orders without scanning every historical fill ID.

**Unaffected:** Dhan GET governor, broker order placement, STOP, risk limits,
source parser, configuration WebUI, protection, reconciliation, Telegram,
holdings, fills, exit allocations, journal contents and other tables.

## Quantified offline results

A temporary JournalDB with **30,000 synthetic historical fills** and **one
current LIVE order** showed these medians across 10 repetitions in CI:

| Run | Old history scan | Indexed relevant-only | Local speedup |
|---|---:|---:|---:|
| 44881444093 | 22.384 ms | 1.281 ms | 17.48x |
| 44881951964 | 14.080 ms | 1.191 ms | 11.82x |

These are **risk SQL component** results, not end-to-end order execution.
Small real journals may benefit little; network-bound Dhan calls are
unchanged. Do not promise a total latency gain without VM evidence.

## Safety proof required

- Current candidate CI (both new tests and full legacy tests) green.
- Exact parity against original SQL across PENDING, TRADED, CANCELLED,
  REJECTED, PART_TRADED, EXPIRED, TEST, OLD and NOT_SUBMITTED orders.
- Verified fill identity still counted, no synthetic fill creation, no
  allocations or live state changes.
- 425 current orders require multiple IN batches, verified all counted.
- NULL and duplicate broker order IDs give original equivalent results.
- Journal failure raises instead of silently treating missing fills as zero.
- Database schema migrations are additive and idempotent.
- The independently run `tools/measure_risk_query_snapshot.py` can
  compare timings on a consistent **read-only** VM snapshot.

## How to measure on a separate, consistent journal snapshot

Do not use plain `cp` against a database being written in SQLite WAL mode.
Use SQLite's backup API with an authorized maintenance procedure to create
a snapshot, then run:

```bash
python3 -m tools.measure_risk_query_snapshot \
  /absolute/path/to/approved/journal-snapshot.sqlite3 --rounds 30
```

The script performs no writes or schema migration; it prints only aggregated
timings, whether the new index is installed, and a result-equivalence check.
It never prints order IDs, account IDs, trade details or credentials.

## Production cutover conditions

1. Owner approval of this isolated PR and a tested rollback commit.
2. Operator confirms real VM journal snapshot and database backup.
3. Maintenance-window restart (index creation can take time on a large DB).
4. Verify index presence, unchanged journal row counts, and original
   reconciliation/protection status after the service starts.
5. Review ingress-to-ACK p50/p95/p99 and broker governor alerts before
   declaring any whole-system improvement.

Rollback can restore the original Python code; the additive indexes may
remain without changing trading decisions. NEVER reset the journal or
delete broker evidence for a rollback.

**No GitHub merge, VM update, Dhan call, SQLite write on a real account, or
production restart is part of creating this release candidate.**
