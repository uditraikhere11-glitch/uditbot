# UditBot modernization — consolidated engineering review

**Prepared:** 2026-11-21 (Asia/Kolkata)
**Base:** `d59feee9b47f83967c483ab9ad930a68b8a49b74` (`main`)
**Candidate:** `dev/execution-modernization-20261121`
**Status:** UNDER REVIEW. Not a production-approved deployment.

## Why this is not a NautilusTrader rewrite

UditBot has live broker-specific safeguards (Dhan requests, singleton listener,
broker mutation fencing, STOP, SQLite intent/fill journal, protections and
reconciliation). Replacing these with an independent generic engine would risk
identity mismatches and duplicate submissions. These additions borrow event
identity, isolation and observability patterns without introducing a second
live execution authority.

## Implemented candidate changes

| Workstream | Code | Runtime effect | Safety boundary |
|---|---|---|---|
| Callback/lock latency | run.py, src/main.py, latency_monitor.py | Bound p50/p95/p99 ingress and lock wait, anonymized summaries | No change to order permission |
| Trace identity | src/main.py | UUID signal trace ID avoids same-millisecond collisions | No sizing/order change |
| Telegram metadata | run.py, telegram_event_metadata.py | Reuse cached chat, preserve old fallback | Source allowlist still required |
| Broker lookup evidence | typed_broker_evidence.py, DhanClient | Opt-in GET-only diagnosis of external correlation outcomes | 404/timeouts never authorize resubmission |
| Delivery-sale triage | delivery_sell_triage.py | New evidence-only negative CNC/MTF classification | Never changes the active gate or lot state |
| Signed-sale regression | delivery_equivalence tests | Tests existing verified_flat_delivery_day helper | Existing gate code retained |
| Shadow event projection | shadow_execution_events.py | Typed ACK/STATUS/TRADE/UNKNOWN simulation | No order authority |
| Live order update normalizer | dhan_order_update_shadow.py | Dhan cumulative traded quantity treated as status only | Cannot synthesize exchange trade IDs |
| Bounded Dhan shadow observer | dhan_order_observer.py | Match only bot-owned orders; detect disconnect and contradictory evidence | Never places/cancels orders; cannot change entry gate |
| Separate shadow event ledger | shadow_event_ledger.py | Crash-consistent isolated .shadow.sqlite3 replay, 0600 | Never writes to UditBot journal |
| Explicit socket CLI | tools/dhan_order_shadow_cli.py | Manual opt-in only; no automatic startup | Requires current env token; zero mutation APIs |
| Deterministic preparse | deterministic_preparse.py, src/main.py, run.py | Optional REGEX BUY prework outside execution lock | Off by default; full BRAIN fallback for all other cases |
| Bounded single-worker FIFO | ordered_ingress.py, run.py | Optional ordered submission without unbounded queued executor work | Off by default; no concurrent risk/broker mutations |
| Read-only KPI tools | execution_modernization_metrics.py, execution_invariant_audit.py | Aggregate latency/journal anomaly reports | No production writes or secrets in output |
| Synthetic before/after experiment | execution_modernization_benchmark.py | Real parser + synthetic serialized I/O comparison | Not a live latency claim |
| Offline CI | execution-modernization-offline.yml | Full unit suite and synthetic benchmark | No live broker/Telegram calls |

## Configuration defaults

Both experimental runtime paths are **OFF by default**:
- `AUDITBOT_ORDERED_INGRESS=0` (set 1 only for controlled approved evaluation)
- `AUDITBOT_ORDERED_INGRESS_MAX_PENDING=64` (when FIFO mode enabled)
- `AUDITBOT_DETERMINISTIC_PREPARSE=0` (set 1 only for controlled approved evaluation)

FIFO and speculative preparse are NOT simultaneously activated; if both flags
are set, FIFO takes precedence and preparse is disabled to avoid completion
order changes. The active `TradingBotPipeline._pipeline_lock` still protects
all journal claims, source checks, risk reservations and broker operations.

Dhan WebSocket is *not imported or launched by run.py*. There are no live
socket sessions as part of this development branch. For an authorized
standalone observational evaluation only, an operator can separately install
`requirements-order-shadow.txt` and invoke the CLI with
`--explicit-readonly-network-opt-in`. Do not use this procedure without
change control and an independent operational safety review.

## Mandatory acceptance criteria

1. Full offline suite + all new modernizing tests pass at the CURRENT HEAD.
2. Zero observed duplicate broker submission after lost ACK, 429, timeout,
   worker cancellation or restart.
3. Zero synthetic false-HEALTHY/false-protection or unauthorized STOP override.
4. No changes to existing reconciliation/protection/STOP/amount controls.
5. Shadow order update disconnect/reconnect cannot release a trading gate;
   account-wide manual orders cannot appear as bot-owned orders.
6. No unbounded event store, queue, or unsafe cancellation during shutdown.
7. Matched workload and environment, candidate vs baseline: preferably
   >=20% lower p95 callback-to-ACK, with no p99 or fill-to-protection
   regression. This is a TARGET, not an achieved claim.
8. Keep baseline journal, logs and broker positions; never erase evidence
   or cancel existing protections to force a green dashboard.
9. Explicit owner approval before merge, deployment or any systemd restart.

## Quantitative evidence to collect

- Run `tools/execution_modernization_metrics.py collect` on **approved
  read-only snapshots** of the same UditBot journal before and after.
- Run `tools/execution_invariant_audit.py` on consistent journal snapshots.
- Compare p50/p95/p99 for quote/parse/ack, fill-to-protection, and
  `INGRESS_LATENCY` logs (50-event aggregates). FIFO mode additionally
  logs `ORDERED_INGRESS_SUMMARY` on orderly shutdown.
- Offline benchmark `tools/execution_modernization_benchmark.py` measures
  an ACTUAL deterministic parser, but uses a SYNTHETIC serialized commit
  delay. It will NEVER certify production speedup.
- `tests/test_execution_modernization_365day_synthetic.py` contains 1,095
  *synthetic* daily evidence checks, not historical Dhan market replay.
- Never copy access tokens, raw account IDs or complete trading journals
  into a public CI artifact. Existing CI uses isolated synthetic fixtures.

## Work not established in the real VM

- No side-by-side actual Dhan or Telegram measured before/after latency
  (VM not accessed).
- No live authenticated stream session; only local mocked stream tests.
- No replacement of the canonical negative-quantity startup gate or the
  proven `verified_flat_delivery_day` logic.
- No new live-privileged order event consumer.
- No proof of historical 365-day broker replay under actual day-by-day books.
- No automated post-release rollout, rollback validation or real order
  protection response-time measurement.

These boundaries are deliberate. Production-ready modernization requires
site-specific credentials, journal access, supervised shadow operation, and
broker-authoritative verification—none can be simulated into existence.

**Release recommendation:** independently review this branch, run the full
current-head CI, compare read-only baseline metrics, then approve any
trading-critical stage separately. Do NOT merge or deploy automatically.
