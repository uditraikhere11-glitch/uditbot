# M1 Phase 2 — broker reconciliation and recovery comparison

**Baseline:** production `main` at `d59feee9b47f83967c483ab9ad930a68b8a49b74`
**Development candidate:** `dev/execution-modernization-20261121`
**Delivery:** single draft PR #113, to be squashed into ONE main commit only after owner approval.
**No production VM, Dhan account, systemd service or live journal was changed.**

## What is genuinely corrected relative to original UditBot

| Safety case | Baseline `main` | Candidate Phase 2 | Evidence |
|---|---|---|---|
| Bad trade late in broker tradebook | Earlier valid rows can be persisted before the later invalid row is discovered | Preflight checks all rows and contradictory exchange IDs before first startup fill write | `broker_tradebook_preflight.py`, `test_execution_modernization_tradebook_gate.py` |
| Same exchangeTradeId appears with conflicting BUY/SELL or contract/quantity/account | Failure depends on per-row journal duplicate handling, potentially after earlier writes | Deterministically blocks whole batch before writes | `test_execution_modernization_tradebook_preflight.py` |
| Lost ACK / absent correlation lookup | Existing broker recovery gate remains the trading authority | New typed external-ID diagnosis never treats 404, 5xx or timeout as resubmission permission | `typed_broker_evidence.py`, `test_execution_modernization_broker_evidence.py` |
| Dhan order stream cumulative fill update | No isolated shadow event projector | Cumulative status is not a unique exchange trade; no direct fill certification | `shadow_execution_events.py`, `dhan_order_update_shadow.py` |
| Foreign-account or mismatched BUY/SELL order update | Journal/broker reconciler is authoritative | Read-only observer requires journal owner order, account, contract, side and original quantity | `dhan_order_observer.py`, `test_execution_modernization_order_ownership.py` |
| Crash before shadow-event persistence | No separate shadow ledger | Separate atomic 0600 SQLite ledger; invalid projector events rejected before commit; valid events replayable | `shadow_event_ledger.py`, `test_execution_modernization_shadow_integration.py` |
| Telegram callback canceled while ingress queue is full | No optional bounded durable ingress experiment | Experimental FIFO preserves admission ordering and callback cancellation; switch OFF by default | `ordered_ingress.py`, `test_execution_modernization_ordered_ingress.py` |
| Crash after inbound signal but before callback completion | Canonical journal intent recovery already exists | Experimental separate ingress evidence survives restart, reports unresolved status, **never automatically replays orders**; OFF by default | `durable_ingress_audit.py`, `test_execution_modernization_audited_dispatch.py` |
| Negative CNC/MTF netQty and earlier holdings | Existing fail-closed `verified_flat_delivery_day()` should be preserved | Added account-bound evidence diagnostics and adversarial tests, no broad relaxation of the production negative-net gate | `delivery_sell_triage.py`, `test_execution_modernization_delivery_equivalence.py` |
| OCO/Super protective status after exit | Existing protective broker-order verification predicate | Adversarial tests reject incomplete/triggered/partial/mismatched OCO evidence; no unverified protection asserted | `test_execution_modernization_protection_recovery.py` |

## What remains UNPROVEN

- Actual Dhan broker-authoritative restart recovery against a real, consistently captured orderbook, tradebook, positions, holdings and protection snapshot.
- A full year of authentic historical account events; the 365-session matrix uses synthetic inputs and must not be represented as actual historical replay.
- End-to-end production p50, p95 and p99 Telegram-to-Dhan ACK and fill-to-protection improvements.
- Correct authorization and processing of deliberately unresolved ingress audit entries after broker verification. An automatic clear or auto-retry is intentionally unavailable.
- Independent review of the much larger consolidated M1 diff and supervised single-commit rollback rehearsal.

## Acceptance scoring — NOT self-certifying

| Gate | Required | Measurement |
|---|---|---|
| Batch tradebook preflight | Every malformed/conflicting execution blocks before first fill write | Hermetic startup-gate integration |
| Unknown Dhan submission | Zero automatic duplicate broker mutations | Network-isolated adversarial tests + broker audit |
| Negative delivery sales | No false flat / false HEALTHY state | Current-day and archival complete account evidence |
| Protection | Zero claimed active OCO/Super protection without corroboration | Actual orders and exchange trade IDs |
| Ingress | No silent in-memory loss under orderly cancellation; no auto-replay after hard crash | Saturated FIFO and durable-ledger tests |
| Risk query | Same account exposure decision with lower history-scanning cost | Exact legacy SQL oracle parity + synthetic timings |
| Full test suite | 100% green at the exact final branch head | GitHub Actions |
| LIVE deployment | Independently reviewed, broker/VM verified, owner authorized | NOT achieved |

## Phase 2 decision

**Candidate developed for independent review, NOT production-release-approved.**
Leave PR #113 as DRAFT; do not merge/restart or place live Dhan orders.
The baseline comparison is functional, not a claim of improved trading profitability.
One squash commit can be reverted, but a source revert cannot undo broker fills
or erase database mutations; a production rollback must preserve those records.
