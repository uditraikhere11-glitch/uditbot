# AuditBot V11 — 11-Step Rework Specification + Darvas Script Integration Report

## 1. Non-Negotiable Rules Compliance
1. **Single Canonical Pipeline Preserved**: `src/main.py` (`TradingBotPipeline`) and `run.py` remain the sole canonical pipeline. No multi-service WebSocket/Notion/Sheets/Kite bloat was imported.
2. **Source-Only Archive Hygiene**: The packaged ZIP contains **zero** mutable runtime files (`data/trade_journal.db`, `data/pending_protection.json`, `data/emergency_stop.json`, or `.session` files are strictly excluded; only `data/holidays.json` is included).
3. **Offline Hermetic Testing**: All 154 unit and fault-injection tests run 100% offline with mocked HTTP transports (`requests.Session.post/put/delete/get`).

---

## 2. 11-Step Rework Specification — PR / Verification Matrix

| Step | Files Changed | Original Defect IDs Closed | Offline Tests Run / Pass | Key Hardening & Invariants Enforced | Status |
|---|---|---|---|---|---|
| **STEP 04 (P0)** Durable Emergency STOP | `src/engine/emergency_stop.py`, `src/broker/dhan_client.py`, `src/cli.py` | Missing/corrupt/wrong-account STOP file; race between `STOP` & `POST`; disk-full write failure; unauthorized `RESUME`; oversell guard | `test_safe_01`, `test_safe_02`, `TestEmergencyStop` (9/9 pass) | Fails closed on missing, corrupt, disk-full (`ENOSPC`), or wrong-account state file; atomic cross-process `try_begin_mutation` under `fcntl.flock` persists in-flight IDs to disk before POST; blocks generic `SELL` entries and oversell Forever OCO legs (`qty > max_protectable_qty`). | **Offline Verified** |
| **STEP 05 & 06 (P0)** Source Auth & Web UI Security | `src/main.py`, `run.py`, `src/cli.py` | `chats=None` subscribing to all chats on empty allowlist; `0.0.0.0` web UI bind | `test_baseline_01_source_allowlist_blocks_unauthorized_channels` | Rejects empty `MONITORED_CHANNEL_IDS` at listener startup; enforces channel allowlist in `process_raw_message` before claiming/parsing; binds setup UI strictly to `127.0.0.1`. | **Offline Verified** |
| **STEP 07 & 08 (P0)** Journal, Idempotency & Intent Durability | `src/journal/db.py`, `src/main.py` | Same-second correlation ID collisions; `IntegrityError` under concurrency; timeout marked `REJECTED`; equity LIVE path missing intent state update | `test_journal_01_100_concurrent_duplicate_claims`, `test_journal_02_timeout_persists_unknown_not_rejected`, `TestJournalDB` | Atomic `INSERT OR IGNORE` claims handle 100 concurrent threads with 0 `IntegrityError`; `generate_correlation_id()` combines monotonic sequence + millisecond timestamp + UUID suffix (`<=30` chars); pre-POST `SUBMITTING` transition and network timeout $\rightarrow$ `UNKNOWN` (never `REJECTED`). | **Offline Verified** |
| **STEP 10 (P0)** Safe Cancel, Late Fill & One Retry | `src/engine/protection_watcher.py` | Cancel timeout uncertainty; `qty - protected_qty` remainder error; `0.99%/1.00%/1.01%` threshold & `(XX% + 1%)` revised limit formula | `test_retry_01_and_02_late_fill_and_threshold_boundary`, `TestSafeCancelAndProtection` | Tracks `CANCEL_REQUESTED` with `cancel_uncertain=True` on timeout; computes remainder from confirmed `filled_qty`; enforces `>1.0%` move (`0.99%` & `1.00%` rejected, `1.01%` approved), 1-child max linked via `parent_correlation_id`, and `(markup_pct + 1.0)%` tick-aligned retry limit. | **Offline Verified** |
| **STEP 11 & 14 (P0/P1)** Product-Specific Broker Protection & Unified Watcher | `src/broker/dhan_client.py`, `src/engine/protection_watcher.py` | Forever OCO unsupported for `MARGIN`; unverified Forever leg ID; independent JSON authority; hardcoded `0.05` tick | `test_protect_01_and_02_leg_verification_and_oversell_guard`, `test_dhan_super_order_place_and_reanchor_on_fill`, `TestStep14And15` | SQLite `watcher_orders` table is authoritative (with one-time JSON migration); `CNC`/`MTF` use Forever OCO (`/v2/forever/orders`) with `verify_forever_leg_active()`; `MARGIN` options use Dhan Super Orders (`/v2/super/orders`) and re-anchor legs on fill; unverified/rejected protection marks `UNPROTECTED` and halts `StartupReconciliationGate`. | **Offline Verified** |
| **STEP 13 (P1)** Exact Contract Resolution & Atomic Refresh | `src/instruments/resolver.py`, `src/main.py` | `expiry_day` forwarding; competing same-date monthly contracts; partial CSV swap | `test_instr_01_explicit_day_and_ambiguous_same_date_rejected`, `TestInstrumentResolver` | Forwards `expiry_day` end-to-end; rejects competing duplicate same-date monthly contracts (`INSTR-01`); validates CSV header + data rows before atomic `os.replace`; adds Darvas nearest-expiry fallback when `expiry_month` is omitted after monthly expiry. | **Offline Verified** |
| **STEP 15 (P0)** Broker-Authoritative Startup Reconciliation | `src/engine/reconciliation.py`, `src/main.py`, `run.py` | Unverified `INTENT_CREATED` auto-cancelled; `order_status` called with correlation ID; missing `halt_new_entries` | `test_rec_01_intent_created_on_startup_keeps_gate_blocked`, `TestStep14And15` | Queries `/v2/orders/external/{correlation-id}` and orderbook before `order_status`; keeps gate `BLOCKED` if any `INTENT_CREATED`, `SUBMITTING`, `UNKNOWN`, `ACKED`, `PART_FILLED`, `CANCEL_PENDING`, or `UNPROTECTED` record remains unverified at broker. | **Offline Verified** |
| **STEP 16 (P1)** Durable Asynchronous Notifications | `src/notifier/outbox_worker.py`, `src/journal/db.py`, `run.py` | Regex group reference error on JWT redaction; synchronous Telegram send in `run.py`; missing retry backoff | `test_outbox_01_jwt_redaction_and_exponential_backoff`, `TestLogFormatter` | Callable regex replacement redacts JWTs, PINs, TOTP secrets, API hashes, and `bot<id>:<token>` URLs with zero group-reference errors; exponential backoff (`2^retry`) and `DEAD_LETTER` escalation; `run.py` dispatches strictly via durable outbox. | **Offline Verified** |
| **STEP 20 (P2)** Additive Regex + Darvas Company Names | `src/parsers/equity_parser.py`, `src/parsers/fno_parser.py`, `src/instruments/resolver.py` | F&O exit hijacking equity exits; missing dynamic company-name resolution | `TestStep20RegexAliases`, `TestDarvasMergedFeatures` | Adds `looks_like_option` (~1µs gatekeeper) so equity exits route cleanly to `EQUITY`; integrates `normalize_name`, `build_name_index`, and `resolve_company_name` from `darvas_script` while preserving all baseline regex rules and index rejection. | **Offline Verified** |
| **STEP 23 & 24 (P1)** Singleton Lease & Truthful SSH Dashboard | `src/engine/lock.py`, `src/cli.py`, `src/journal/db.py`, `src/main.py` | Lock file unlinked on release; UTC day bucket; `INTENT_CREATED` hidden when `UNKNOWN=0`; local OCO called broker-confirmed | `test_singleton_01_dual_launch_and_inode_preservation`, `test_dash_01_intent_created_displays_attention_zero_mutations`, `TestStep23And24` | `SingletonListenerLock.release()` closes fd without unlinking path (preserving inode); `get_dashboard_summary()` uses IST (`Asia/Kolkata`) midnight, reports all non-terminal intents and `UNPROTECTED` positions, distinguishes local vs broker-confirmed protection, and performs zero HTTP mutations. | **Offline Verified** |

---

## 3. Darvas Script Enhancements Included
- **`looks_like_option` & `is_exit_message`** (`src/parsers/fno_parser.py` & `src/main.py`): Prevents F&O exit checks from hijacking Cash Equity exit signals.
- **`resolve_company_name`, `build_name_index`, `normalize_name`** (`src/instruments/resolver.py` & `src/parsers/equity_parser.py`): Resolves multi-word company names from Dhan's `SEM_CUSTOM_SYMBOL` column (e.g. `"Bought MANIPAL PAYMENT at 450"` $\rightarrow$ `MPIMANIPAL`) while rejecting ambiguous single-word prefixes (`"Bought MANIPAL"`).
- **Yahoo Finance Live LTP (`fetch_yahoo_ltp` / `_fetch_ltp`)** (`src/broker/dhan_client.py`): Zero-cost real-time NSE (`.NS`) and BSE (`.BO`) LTP fallback when Dhan `/v2/marketfeed/ltp` returns error `806` (Data API not subscribed).
- **Dhan Super Orders (`/v2/super/orders`)** (`src/broker/dhan_client.py` & `src/engine/protection_watcher.py`): Attaches SL + Target legs to `MARGIN` option entries and re-anchors them (`PUT /v2/super/orders/{id}`) to $\pm X\%$ of the actual executed fill price.
- **Shared Token Lock (`src/broker/dhan_token.py`)**: Cross-process `fcntl.flock` token file coordinator preventing `DH-906 Invalid Token` collisions.

---

## AuditBot V12 — Consolidated Execution-Safety Correction (2026-10-29)

Built directly on `auditbot_complete_source_hardened_V11_11step_darvas.zip` to close all execution-safety gaps across Sections A–I while keeping `src/main.py` as the single canonical pipeline and preserving `src/setup_app.py`, `src/setup_wizard.py`, and `tests/test_setup.py` byte-for-byte unchanged.

### Key Safety Corrections Implemented
1. **A. Yahoo Finance Primary Price Source (`src/broker/dhan_client.py`, `src/models.py`)**:
   - Added structured `QuoteResult` (`price`, `source`, `ticker`, `venue`, `exchange_segment`, `security_id`, `currency`, `market_state`, `quote_timestamp`, `age_sec`, `is_delayed`, `delay_minutes`, `is_fresh`, `failure_reason`).
   - Validates `regularMarketTime` against current UTC server time (`0.0 <= age_sec <= 2.0` seconds inclusive; rejects future timestamps, missing/zero timestamps, HTTP-only freshness, expired cached entries, delayed `.BO` quotes, wrong exchange/currency/market state, and NSE F&O underlying equity price substitution).
   - **Licensing & Feed Limitation**: Yahoo Finance's published terms state its data is informational and not intended for live trading execution. Automated entries fail closed whenever the quote cannot be independently verified as fresh (`<= 2.0s` source UTC timestamp), undelayed (`exchangeDataDelayedBy == 0`), and venue-matched.
2. **B. Owner's Revised Retry Policy (`src/engine/protection_watcher.py`)**:
   - Absolute price movement from `original_market_price` must be **strictly `< 1.00%`** (`0.99%` eligible; `1.00%` and `1.01%` rejected).
   - Revised BUY LIMIT markup is configured `XX%` **capped at `1.00%`** (previous `XX% + 1%` formula removed).
   - Enforces max 1 retry after broker-confirmed terminal parent cancellation (`CANCELLED`), within the 5.0s deadline, for confirmed unfilled quantity (`qty - confirmed_filled_qty`).
3. **C. Journal-First Order Execution (`src/engine/protection_watcher.py`)**:
   - `execute_retry()` requires an active durable `JournalDB` (`self.journal_db`).
   - Atomically reserves the single retry slot (`retry_reservation_state = "RESERVED"`), writes the child intent with `parent_order_id` (`INTENT_CREATED`), and transitions to `SUBMITTING` (`may-have-sent`) **before** any broker `POST`.
   - If the journal is missing, locked, or fails, `execute_retry()` aborts with **zero** broker orders. On broker timeout, preserves `UNKNOWN` and never resubmits.
4. **D. Exact Stock/Option Identity Preservation (`src/engine/protection_watcher.py`, `src/main.py`)**:
   - Removed indiscriminate `f"{symbol}-EQ"` construction.
   - Reconstructs retry orders from exact verified original metadata (`exchange_segment`, `security_id`, `symbol`/`trading_symbol`, `product_type`, `option_type`, `strike`, `expiry`, `lot_size`, `tick_size`).
   - Rejects unknown product types (never defaults silently to `CNC`), expired F&O contracts, index symbols, and lot-size mismatches before broker submission.
5. **E. Dhan Super Order Authentication Safety (`src/broker/dhan_client.py`)**:
   - Removed recursive order resubmission after HTTP 401/token refresh in `place_super_order()` and `place_order()`.
   - On auth failure or ambiguous response, preserves the original `correlation_id`, refreshes credentials solely for read-only reconciliation (`get_order_by_correlation_id` and `get_orders`), and never submits a duplicate order while the first outcome is uncertain.
6. **F. Never Fabricate Executed Prices (`src/engine/protection_watcher.py`)**:
   - Removed `original_market_price or 100.0` and `dry_price or 100.0` fallbacks in `_protect()`.
   - If broker-confirmed executed fill price (`averageTradedPrice` / `trades_avg_price`) is unavailable or `<= 0`, records `UNPROTECTED`, halts new entries via `reconciliation_gate.halt_new_entries()`, and sends a high-priority operator alert without inventing a protective price.
7. **G. Broker-Confirmed Protection (`src/broker/dhan_client.py`, `src/engine/protection_watcher.py`)**:
   - `verify_forever_leg_active()` verifies `dhanClientId`, `securityId`, `SELL` transaction type, quantity, and active OCO legs (`CONFIRM`/`ACTIVE`/`PENDING`/`TRANSIT`/`OPEN`).
   - If OCO ACK has no active legs in `get_forever_orders()` or Super Order leg re-anchoring (`modify_super_order_protection`) fails, records `UNPROTECTED` / `reanchor_status="FAILED"`, halts new entries, and never claims 3% fill-based protection is active.
8. **H. Web Setup Parity**:
   - `src/setup_app.py`, `src/setup_wizard.py`, and `tests/test_setup.py` are byte-for-byte unchanged; `src/cli.py` preserves `host: str = "127.0.0.1"` localhost default bind.

### Rollback Instructions
To revert from V12 back to V11 on an isolated source checkout without touching runtime state (`data/trade_journal.db`, `data/pending_protection.json`, `data/emergency_stop.json`, `.env`, `.session`):
```bash
git apply -R auditbot_v11_to_v12.diff
python3 -m unittest discover -s tests -p "test_*.py" -v
```

---

## AuditBot V13 — Remediation of All 26 Independent Audit Findings (2026-10-30)

Built directly on `auditbot_complete_source_hardened_V12.zip` (SHA-256 `08283eeec99351bb326cb7d1878dd53ea50faaf455ca21bb3d2c733e3ed59338`) to close all 26 prioritized findings (`V12-01` through `V12-26`) and convert all 15 Appendix B defect reproductions into permanent offline safety regression tests (`tests/test_v13_independent_audit_remediations.py`).

### Summary of Remediations (V12-01 to V12-26)
- **V12-01 (HIGH)**: `InstrumentResolver.load_dhan_master` parses into staged structures and atomically swaps catalogues, and `load_option` deduplicates by `(symbol, strike, option_type, expiry, exchange_segment, security_id)`; `run.py` skips redundant reloads.
- **V12-02 (CRITICAL)**: `ProtectionWatcher.track(..., preserve_existing=True)` and `StartupReconciliationGate` merge broker observations without resetting `protected_qty`, `retry_count`, `is_super_order`, or option metadata; `order_intents` durably persists full contract/route metadata.
- **V12-03 (CRITICAL)**: `ProtectionWatcher._protect()` persists `pending_oco` (`SUBMITTING` -> `ACKED_UNVERIFIED` / `UNKNOWN`) and reconciles the existing `oco_id`/`correlation_id` on subsequent passes without submitting duplicate OCO orders; passes `max_protectable_qty`.
- **V12-04 (CRITICAL)**: `DhanClient.verify_forever_leg_active()` requires active resting status (`PENDING`/`OPEN`/`TRANSIT`/`CONFIRM`/`ACTIVE`, rejecting `TRADED` or missing status), mandatory `securityId`, `SELL` side, quantities, and both SL and Target legs.
- **V12-05 (CRITICAL)**: Added `DhanClient.get_super_orders()` (`GET /v2/super/orders`) and `verify_super_order_legs_active()`; `_protect()` never defaults `reanchor_verified = True` when readback is `None` or missing active legs.
- **V12-06 (CRITICAL)**: Added `is_lifecycle_open()` (09:15–15:35 IST) in `src/engine/market.py` and `ProtectionWatcher.should_poll()` so fill/protection monitoring continues past the 15:25 fresh-entry cutoff and while any order is pending.
- **V12-07 (CRITICAL)**: `StartupReconciliationGate.run_startup_reconciliation()` queries `get_positions()` and `get_trade_book()`, recovers partial fills on terminal orders, and keeps the gate closed if any net open broker position (`netQty > 0`) lacks verified protection.
- **V12-08 (HIGH)**: `RiskManager.sync_from_journal_and_broker()` and `record_order_committed()` update daily trade count, open positions, exposure, and realized P&L across sequential pipeline executions.
- **V12-09 (HIGH)**: `ProtectionWatcher.check_once()` enforces `order_timeout_sec` (default 60s) on resting equity orders and `180s` on resting F&O orders.
- **V12-10 (HIGH)**: `ProtectionWatcher.execute_retry()` routes Super Order retries through `broker.place_super_order()` with calculated SL/Target legs.
- **V12-11 (HIGH)**: `execute_retry()` enforces the 1.00% markup ceiling even when `revised_limit_price` is passed directly, persists atomic unique retry reservations in SQLite (`retry_reservations`), and passes exhausted retry budget (`retry_count=1, max_retries=1`) to child orders.
- **V12-12 (HIGH)**: `place_super_order()` preserves the full `<= 30`-character `correlation_id` (matching `JournalDB` and Dhan Super Order v2 spec).
- **V12-13 (HIGH)**: HTTP 5xx (`500`/`502`/`503`/`504`) and `408` responses set `uncertain_outcome = True`, preserve `in_flight_orders`, classify as `UNKNOWN` in `_is_transport_uncertainty()`, halt `reconciliation_gate`, and are continuously reconciled by `reconcile_runtime_unknown_intents()`.
- **V12-14 (HIGH)**: `run.py`, `src/main.py`, `ProtectionWatcher.run()`, and `OutboxWorker.run()` offload synchronous HTTP/SQLite calls via `asyncio.to_thread` with serialized lock ownership.
- **V12-15 (HIGH)**: `get_yahoo_quote_details()` includes HTTP elapsed time in UTC age validation, requires explicit `symbol`, `exchangeName`, `currency`, and `marketState` metadata, and `_handle_equity_signal` re-validates quote/signal age immediately before broker `POST`.
- **V12-16 (HIGH)**: `ProtectionWatcher._protect()` forwards exact instrument `tick_size` to `place_forever_oco()` and `verify_forever_leg_active()`.
- **V12-17 (HIGH)**: `ProtectionWatcher._compute_chunk_and_vwap_prices()` computes marginal chunk execution price `(cum_vwap * filled - protected_value) / new_qty` for incremental Forever OCO chunks while re-anchoring Super Orders to cumulative VWAP.
- **V12-18 (HIGH)**: Added `tg_bot_token` (`TG_BOT_TOKEN`) to `AppConfig`; `TelegramNotifier.send_log_message()` returns `False` when disabled so `OutboxWorker` never marks undelivered alerts `PROCESSED`; `run.py` configures `INFO` logging and provides a Telethon user-session delivery fallback.
- **V12-19 (HIGH)**: `ProtectionWatcher` calls `record_fill()`, advances `order_intents`/`orders` on fills, and calls `resolve_unprotected_orders()` to mark prior `UNPROTECTED` incidents `SUPERSEDED` once protection is confirmed; `get_non_terminal_intents()` includes `PENDING`/`PART_TRADED`/`TRANSIT`/`TRIGGERED`.
- **V12-20 (HIGH)**: `DhanClient` coordinates token refreshes through `dhan_token.fresh_token()` / `force_login()` under `fcntl.flock`, uses PID/thread-unique atomic `.tmp` files, and auto-adopts newer shared tokens via mtime check in `_headers()`.
- **V12-21, V12-22, V12-23, V12-24 (HIGH/MEDIUM)**: `setup_app.py` binds `127.0.0.1` by default, masks saved secrets on `GET /`, HTML-escapes values, preserves custom `.env` keys on save, and applies explicit `0600` chmod; `bot` resolves symlinks and `cd`s to `APP_ROOT`; `src/cli.py` implements `daily_health_check` and `deploy/setup_vm.sh` enables `dhan-token-refresh.timer`.
- **V12-25, V12-26 (MEDIUM/HIGH)**: `InstrumentResolver` validates cache age/SHA-256, rejects 0-row parses, and builds `_name_index` once per batch; `gemini_fallback.py` requires verbatim source-text evidence of symbol, strike, and prices, validates `option_type` strictly in `{"CE","CALL","PE","PUT"}`, enforces `GEMINI_TIMEOUT_SEC`, and re-checks message age after inference.
