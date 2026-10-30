# Auditbot V13 — Confirmed Corrections CV13-01 through CV13-05 Implementation Matrix

## 1. Baseline & Scope Verification
- **Reviewed Baseline Specification**: `auditbot_complete_source_hardened_V13_corrected.zip` (prior Turn-7 snapshot `SHA-256: 5c3a38b2b9c301e4046c4c4fa2edde9303e4ceae98f959c309c3d38bff4f45be`; Turn-8 working tree `SHA-256: 7a989980604225441107f8ed43a7d9a61fe3c39fcd2d5811ffaf80e040f3b607`).
- **Priority Classification**:
  - `CV13-01`, `CV13-02`, `CV13-03`: **High** priority
  - `CV13-04`, `CV13-05`: **Medium** priority
- **Preserved Behavior**:
  - Existing architecture, SQLite WAL journal, Dhan v2 integration, and Telegram workflow.
  - Durable protection attempts and partial unique index `idx_unique_active_protection_attempt` on `(parent_order_id, uncovered_from_qty)`.
  - Strict broker leg verification (`verify_forever_oco_group`, `verify_super_order_record`).
  - Marginal chunk fill pricing for Forever OCO and cumulative VWAP re-anchoring for Super Orders.
  - Startup coverage deduplication and structured `BrokerReadList` / `BrokerReadFailure` distinction.
  - Single retry per root-order lineage (`retry_reservations`).
  - Current instrument identity, tick-size, lot-size, and calendar-accurate expiry validation.
  - Approved entry prices, `< 1.00%` movement ceiling, configured `XX%` markup capped at `1.00%` (never `XX% + 1%`), SL/target rules, timeouts, parser rules, and emergency stop behavior.

## 2. CV13-01 through CV13-05 Implementation Matrix

| ID | Priority | Status | Files Modified | Implementation & Evidence Summary |
|---|---|---|---|---|
| **CV13-01** | **High** | **Fixed and tested** | `src/engine/protection_watcher.py`, `src/journal/db.py`, `tests/test_cv13_01_to_05_corrections.py` | `_load()` restores unresolved `SUPER_ORDER` attempts (including quantity, prices, `oco_id=parent_order_id`, and state) into `self.pending[oid]["pending_oco"]`. Before creating any attempt, `_protect()` calls `_find_unresolved_attempt(rec, route_name)` and `_reconcile_existing_super_attempt()`. If broker readback confirms the stored attempt's quantity and levels, the attempt is marked `CONFIRMED` and watcher coverage (`protected_qty`, `protected_value`) is updated consistently before evaluating any subsequent partial fill slice (`uncovered_from_qty=protected_qty`) at current cumulative VWAP. If readback is unverified or modification timed out (`UNKNOWN`), the unresolved attempt is retained without repeating `modify_super_order_protection` and fresh entries stay blocked. Verified by `test_cv13_01_a`..`d`. |
| **CV13-02** | **High** | **Fixed and tested** | `src/engine/protection_watcher.py`, `src/journal/db.py`, `tests/test_cv13_01_to_05_corrections.py` | `request_cancel()` normalizes and persists `cancel_origin` (`BOT_TIMEOUT`, `MANUAL_OPERATOR`, `EXTERNAL_OR_UNKNOWN`, `SIGNAL_WITHDRAWAL`, `EMERGENCY_STOP`), `cancel_reason`, and `retry_eligible` into SQLite (`order_intents` and `watcher_orders`) and disk before invoking broker DELETE. Only `BOT_TIMEOUT` with available root-lineage retry budget sets `retry_eligible=True`. Unsolicited broker `CANCELLED` observations record `EXTERNAL_OR_UNKNOWN` (`retry_eligible=False`), and legacy records loaded without `cancel_origin` default to `LEGACY_UNKNOWN` (`retry_eligible=False`). Verified by `test_cv13_02_a`..`b`. |
| **CV13-03** | **High** | **Fixed and tested** | `src/engine/protection_watcher.py`, `src/journal/db.py`, `tests/test_cv13_01_to_05_corrections.py` | Tracks `entry_timeout_elapsed`, `cancel_requested`, `cancel_acknowledged`, `cancel_uncertain`, and `terminal_confirmed` as separate fields on each tracked order. After a failed (e.g. HTTP 429) or uncertain (timeout) cancellation, `check_once()` polls current broker order status first, reconciles and protects any intervening fills, stops cancellation attempts if terminal, retries cancellation after bounded exponential backoff (`cancel_backoff_sec * 2^(attempts-1)`) if still pending/partially filled, retains uncertainty and halts the gate if broker status cannot be established, and alerts while keeping the order monitored in `self.pending` if `max_cancel_attempts` is exhausted. Verified by `test_cv13_03_a`..`b`. |
| **CV13-04** | **Medium** | **Fixed and tested** | `src/engine/protection_watcher.py`, `src/broker/dhan_client.py`, `src/journal/db.py`, `tests/test_cv13_01_to_05_corrections.py` | After quote retrieval, funds/risk checks, and pre-submit persistence (`record_order_intent`, `reserve_retry_slot`, `_save_strict`), `execute_retry()` reads the current live clock immediately before broker submission, recalculates quote age against both the validated source quote timestamp and local retrieval timestamp, rejects stale or future-dated quotes, and rechecks `stop_manager.is_stopped()` and `reconciliation_gate.is_accepting_entries`. For F&O, `DhanClient.get_ltp()` and `execute_retry()` track `retry_local_retrieval_timestamp` (`LOCAL_RETRIEVAL`) distinctly from `retry_source_quote_timestamp` (`SOURCE_TIMESTAMP`) without fabricating a source timestamp. If final pre-submit validation fails after a durable reservation, `order_intents.state` is updated to `NOT_SUBMITTED` (distinct from `UNKNOWN`) while preserving the root's one-retry guarantee. Verified by `test_cv13_04_a`..`c`. |
| **CV13-05** | **Medium** | **Fixed and tested** | `src/engine/reconciliation.py`, `tests/test_cv13_01_to_05_corrections.py` | `validate_position_record()` validates every position record in `StartupReconciliationGate._verify_open_positions_protected()` before evaluating coverage: requires non-empty `securityId`, valid `exchangeSegment` (`NSE_EQ`, `BSE_EQ`, `NSE_FNO`, `BSE_FNO`), valid `productType` (`CNC`, `MTF`, `MARGIN`, `INTRADAY`), and explicit non-null `netQty` (`int` or integer `str` `>= 0`), rejecting missing/null, boolean, non-finite, fractional/float, negative, and malformed string quantities. Verified by `test_cv13_05_a`..`b`. |

## 3. Migration Notes
- **Additive SQLite Schema Migrations (`src/journal/db.py`)**:
  On startup, `JournalDB._init_db()` runs idempotent `ALTER TABLE order_intents ADD COLUMN ...` statements for existing databases:
  - `cancel_origin TEXT DEFAULT ''`
  - `cancel_reason TEXT DEFAULT ''`
  - `retry_eligible INTEGER DEFAULT 0`
  - `cancel_attempts INTEGER DEFAULT 0`
  - `cancel_acknowledged INTEGER DEFAULT 0`
  - `cancel_uncertain INTEGER DEFAULT 0`
  - `entry_timeout_elapsed INTEGER DEFAULT 0`
- **Legacy Watcher State Hydration (`src/engine/protection_watcher.py`)**:
  `_hydrate_loaded_record_defaults()` automatically populates missing cancellation/timeout fields on existing `watcher_orders` / `pending_protection.json` records, defaulting any legacy record without an explicit `cancel_origin` to `cancel_origin="LEGACY_UNKNOWN"` and `retry_eligible=False` while continuing normal fill reconciliation and protection.

## 4. Existing Web Setup Exposure Note (Out of Scope by Instruction)
- Per owner instruction, `src/setup_app.py`, `src/setup_wizard.py`, and `tests/test_setup.py` are preserved byte-for-byte unchanged (`SHA-256` verified).
- **Operational Security Note**: `src/setup_app.py` binds to `127.0.0.1` by default, masks secrets (`DHAN_PIN`, `DHAN_TOTP_SECRET`, `DHAN_ACCESS_TOKEN`, `TG_API_HASH`) on `GET /`, and HTML-escapes rendered values, but does not implement an application-layer authentication or CSRF token mechanism on `POST /`. Operators must keep `SETUP_HOST=127.0.0.1` (never `0.0.0.0` on an untrusted network) and access the setup UI only over a local loopback session or authenticated SSH port-forward.

## 5. Remaining Live Deployment & Broker Validation Requirements
- Offline tests verify deterministic state transitions with mocked broker transports and outbound HTTP blocked.
- Before unattended `TRADING_MODE=LIVE` operation on the production VM:
  1. Run `./bot check-all` during NSE regular hours (`09:15–15:30 IST`) to confirm live Dhan TOTP token generation, `/v2/fundlimit` connectivity, fresh `data/scrip_master.csv`, and `< 2.0s` Yahoo Finance `.NS` quote latency.
  2. Note that Yahoo Finance's published market-data terms state its data is informational and not intended for trading; monitor quote age rejections in `data/trade_journal.db`.
  3. Never overwrite the live `data/trade_journal.db`, `data/emergency_stop.json`, `data/pending_protection.json`, `.env`, or `.session` files when deploying source updates.

## 6. Test Commands & Actual Results
```bash
python3 -m unittest tests/test_cv13_01_to_05_corrections.py -v
# Ran 13 tests in 0.194s — OK

python3 -c '
import unittest
from unittest.mock import patch
import requests
def block_http(self, method, url, *args, **kwargs):
    raise RuntimeError(f"UNMOCKED_HTTP_BLOCKED: {method} {url}")
with patch.object(requests.sessions.Session, "request", block_http):
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_*.py")
    res = unittest.TextTestRunner(verbosity=1).run(suite)
    assert res.wasSuccessful()
'
# Ran 233 tests in 1.226s — OK (failures=0, errors=0)
```
