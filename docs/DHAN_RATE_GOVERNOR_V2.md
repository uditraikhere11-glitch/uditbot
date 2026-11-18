# DhanHQ v2 category-aware governor — audit reference (18 Nov 2026)

Primary official DhanHQ v2 source: https://dhanhq.co/docs/v2/ — Rate Limit table.
Supplementary official support: https://dhan.co/support/platforms/dhanhq-api/what-are-the-api-rate-limits-for-dhan/
Product announcement about GET availability: https://madefortrade.in/t/now-live-updated-dhanhq-v2-trading-data-apis-available/32237

| Category | per second | per minute | per hour | per day |
|---|---:|---:|---:|---:|
| ORDER (mutations) | 10 | 250 | 1000 | 7000 |
| DATA | 5 | no published limit | no published limit | 100000 |
| QUOTE | 1 | unlimited | unlimited | unlimited |
| NON_TRADING (GET order statuses, trades, holdings, positions, funds, profile) | 20 | unlimited | unlimited | unlimited |

DhanHQ release communications say GET requests no longer use longer-window Order API allowances. The order-book GET therefore *must not* be charged against one arbitrary shared 5,500/day bucket. An endpoint's HTTP verb is part of category classification: POST /orders is ORDER, GET /orders is NON_TRADING; POST /marketfeed/ltp is QUOTE.

Order modifications are conservatively capped at 25 admitted modifications per order ID as documented in the official DhanHQ v2 introduction. Reservations count before the request to prevent duplicative uncertain modifications.

## Safety and migration

- All category admissions are enforced in durable account-scoped SQLite with atomic `BEGIN IMMEDIATE` and bounded lock retries; no silent success if the store is corrupt or locked.
- One-time `ALTER TABLE` adds `category` to the existing `reads` table. The historical table contains GET requests only, so all historical rows are preserved and classified NON_TRADING. **No counters or cooldowns are deleted to force an order or a stop.**
- Existing `cooldown` is retained, and any active legacy account-wide broker cooldown remains authoritative until expiration. New confirmed broker 429 cooldowns are category-scoped.
- A governor admission is not a successful broker read. Missing, malformed, 401/403/429 or ambiguous broker data still blocks trading and safe stop through existing reconciliation rules.
- Protection and cancellation mutations use ORDER limits, with urgent scheduling priority for protective PUT/DELETE; no order request body, replay or retry policy changes.
- Market Quote POST uses the QUOTE (1/s) category. TOTP authentication POST is not a Dhan v2 trading/data API category and retains its separate auth coordinator.
- The `LIMITS` compatibility override exists only for pre-existing synthetic tests; live policy defaults are `CATEGORY_LIMITS`. No fake universal daily GET ceiling remains.
- Per-second pacing is fail-fast (nonblocking), not auto-sleep. Watcher preview checks first admissible burst when required batch spans seconds. Actual individual GETs still pass atomic admission.
- Dhan documents category limits, but does not explicitly guarantee an identical quota across independently connected VMs/apps. This governor coordinates only processes sharing this account's SQLite file; genuine Dhan 429 retains precedence.

## Release gates

- New category limits, GET vs POST classification, 5,225 legacy GET migration, authentic 429 isolation, per-order modification cap, corrupt ledger and concurrency tests.
- Existing R01/R02, Dhan auth, AMO, OCO, reconciliation, broker handoff, V2 tracker and full combined repo regressions.
- CI green on exact final PR commit and independent diff audit before merge.
- **Merge is code-only. LIVE VM remains unchanged.** Deploying requires stopped-state recovery and off-VM backups according to PR92. Never force service stop to sidestep broker uncertainty.
