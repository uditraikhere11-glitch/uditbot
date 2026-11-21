# UditBot Execution Modernization — Development Branch

**Source baseline:** main d59feee9b47f83967c483ab9ad930a68b8a49b74 (2026-11-20)
**Branch:** dev/execution-modernization-20261121
**Status:** UNDER REVIEW / OFFLINE ONLY. **NOT production approved.**
**Scope:** low-risk instrumentation, typed read-only broker evidence, and
non-authoritative order-event replay ahead of any critical execution refactor.

## Design references

- NautilusTrader: typed execution events and reconciliation reports.
- QuantConnect LEAN: boundary between brokerage, orders and transactions.
- Hummingbot: exchange trade ID deduplication, lost-order recovery.
- DhanHQ V2: external correlation lookup and live order-update WebSocket.

We do not vendor or run these projects in the Dhan execution path.

## Changes implemented in this branch

1. `run.py`, `src/main.py`, `src/engine/latency_monitor.py`:
   capture Telegram callback monotonic time and measure worker scheduling and
   global pipeline lock wait. Record bounded p50/p95/p99 in memory and log
   anonymized summaries every 50 samples. **No changes to order execution,
   parsing decisions, sizing, STOP, risk state, SQLite schema or protection.**
2. `src/engine/typed_broker_evidence.py`:
   parse found, malformed, 404, failed-auth, 429, 5xx and timeout results into
   explicit types. No result authorizes resubmission or asserts a fill.
3. `src/broker/dhan_client.py`:
   add opt-in `diagnose_external_order_lookup()`. It performs a GET if called
   in LIVE, never POST/PUT/DELETE. Existing critical
   `get_order_by_correlation_id()` behavior is unchanged.
4. `src/engine/delivery_sell_triage.py`:
   check signed CNC/MTF same-day positions against account-bound unique trade
   evidence. It NEVER opens the gate, writes the journal, closes a lot or
   cancels protection.
5. `src/engine/shadow_execution_events.py`:
   pure in-memory replay of ACK, order STATUS, exchange TRADE and UNKNOWN;
   deduplicate executions, detect identity/terminal contradictions and flag
   uncertain evidence.
6. `src/engine/dhan_order_update_shadow.py`:
   normalize official Dhan Live Order Update payloads into **STATUS snapshots
   only**. The documented WebSocket supplies cumulative TradedQty but not an
   exchange trade ID. Thus notifications are not certified individual fills.
   There is **no WebSocket connection, credential loading, or runtime task** in
   this branch. The normalizer cannot authorize trading or reconcile exposure.
7. `tools/execution_modernization_metrics.py`:
   read-only/anonymized latency collector and descriptive before/after report.
8. `tests/test_execution_modernization_*.py` and
   `.github/workflows/execution-modernization-offline.yml`:
   tests and full offline regression gate.

## Findings that prevent an unsafe 'quick fix'

The current `src/engine/reconciliation.py` already checks
`verified_flat_delivery_day()` before feeding negative CNC/MTF positions to
`validate_position_record()`. It still rejects negative exposure lacking
complete broker/journal/holding/protection evidence. Deleting this rejection,
taking `abs(netQty)`, or treating `netQty<0` as zero would risk a false
HEALTHY result. The diagnostic here does NOT replace that mature pathway.

The current `src/main.py` deliberately holds `_pipeline_lock` across
`process_raw_message()`. We do NOT move parsing/HTTP calls outside the lock
until risk-reservation concurrency and duplicate signal tests demonstrate
equivalent behavior. Synthetic throughput tests do NOT prove a live speedup.

## Quantitative release approval requirements

For baseline and candidate, compare the SAME broker-data fixtures, same CPU,
equivalent configuration, market hours and signal arrival distributions.
Collect at least 30 relevant samples per metric (prefer 500+ for p99).

- P0: zero duplicate broker mutations after ACK loss, timeout or retries.
- P0: zero false-HEALTHY reconciliation decisions and zero unauthorized STOP bypass.
- P0: zero false protection verification and no lost journal commitments.
- P0: every mandatory current and added offline regression test must pass.
- P1: measure callback-to-processing, pipeline lock wait, parse, quote,
      broker HTTP, broker ACK, fill-to-protection p50/p95/p99 separately.
- P1: target >=20% reduction in matched p95 processing latency when candidate
      concurrency work is introduced, without a p99 or fill-to-protection
      regression. This is a target, NOT a measured result.
- P1: stream disconnect, repeated updates, missing trade IDs, out-of-order
      statuses and REST disagreement all stay safe and resolvable.
- P2: rate-governor limits, fsync, journal durability and configuration fences
      remain enforced.

**A faster but less safe candidate is a failed release.**

## Offline CI

The development workflow installs requirements-test, then activates the
existing tests/ci_offline perimeter and runs:

```
python -B -m compileall -q run.py src tests
python -W error::ResourceWarning -m unittest discover -s tests -t . -p 'test_execution_modernization_*.py' -v
python -W error::ResourceWarning -m unittest discover -s tests -t . -p 'test_*.py'
```

Check `Actions > Execution modernization - isolated CI` for the CURRENT
branch HEAD, not an earlier successful run. Only a green current-HEAD result
counts. Do not mistake a canceled or pending run for passed coverage.

## Read-only latency baseline from a consistent journal snapshot

```bash
python3 tools/execution_modernization_metrics.py collect /path/to/journal.sqlite --limit 1000 > baseline.json
# After an expressly approved, separate candidate deployment and matched session:
python3 tools/execution_modernization_metrics.py collect /path/to/after.sqlite --limit 1000 > candidate.json
python3 tools/execution_modernization_metrics.py compare baseline.json candidate.json
```

The collector does not print account/order IDs. Its reports are descriptive
and never set `production_approved=true`. The new queue counters log
`INGRESS_LATENCY ...` every 50 measured callbacks (only from the trading
process); before that there may be no aggregated ingress sample.

## Remaining integration work — NOT falsely claimed complete

1. Harden and verify the existing negative CNC/MTF `verified_flat_delivery_day`
   and unified-exit reconciliation scenarios with full Dhan + SQLite fixtures.
2. Establish actual baseline p50/p95/p99 from user-approved journal snapshots.
3. Prove a two-phase concurrent parse / serialized risk-commit design with
   exact ordering, stale-signal rejection and identical safe broker behavior.
4. Introduce authenticated Dhan WebSocket as a read-only **opt-in observer**,
   securely bind only bot-owned orders, and reconcile stream gaps via REST.
5. Run crash-injection, repeated ACK, partial-fill, disk failure, journal
   migration and full 365-day replay in independent offline environments.
6. Require separate owner authorization before a PR merge or any VM deployment.

**This branch is developed for independent review. No live migration is
implied or authorized by this document.**
