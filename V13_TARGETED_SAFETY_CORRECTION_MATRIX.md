# Auditbot V13 Targeted Safety Correction Matrix, Changelog & Migration Guide

## 1. Baseline Verification & Discrepancy Report

| Archive / Baseline | SHA-256 | Relationship |
|---|---|---|
| Initial V13 Release Archive | `f2949211ce1bb7fa86ce93fba72d679e36c9dd3d2f4a8475ab0c43a72fc3b899` | Reviewed 194-test V13 archive prior to owner's Web Setup revert directive |
| Post-Owner-Directive V13 Archive | `087228bf9557032b1018cffeec214f63ff7caecf046f4522ecaadfa6a9380498` | Identical to `f2949211ce...` across all 62 runtime/engine/broker files; differs only in restoring `src/setup_app.py` (`ac2e108f...`), `src/setup_wizard.py` (`72223eb1...`), and `tests/test_setup.py` (`256981a2...`) per explicit owner mandate |

### Documented Configuration / Spec Discrepancies (Preserved Without Inventing Trading Rules)
1. **Web Setup Binding & Unmasked Form Fields (`src/setup_app.py`)**: Per explicit owner instruction, `src/setup_app.py`, `src/setup_wizard.py`, and `tests/test_setup.py` are locked byte-for-byte to their original versions (`0.0.0.0:8080` binding and unmasked pre-filled setup form values on `GET /`), while `.env` file permissions (`0o600`) in `src/cli.py` and `src/broker/dhan_token.py` remain enforced.
2. **Retry Timeout Configuration (`PARTIAL_FILL_RETRY_TIMEOUT_SEC`)**: `AppConfig.partial_fill_retry_timeout_sec` defaults to `2.0` seconds in `src/config.py` (configurable via `PARTIAL_FILL_RETRY_TIMEOUT_SEC`), whereas earlier narrative documentation referenced a 5-second retry timeout. `ProtectionWatcher` uses the configured `cfg.partial_fill_retry_timeout_sec` (`self.retry_timeout_sec`) without inventing a new rule.

---

## 2. Concise Change Log

1. **`src/models.py`**:
   - Added `ProtectionVerificationStatus` (`VERIFIED_ACTIVE`, `UNVERIFIED`, `TERMINAL_OR_INVALID`) and `ProtectionVerificationResult`.
   - Added `BrokerReadStatus` (`OK_EMPTY`, `OK_RECORDS`, `FAILED`, `MALFORMED`, `UNAVAILABLE`), `BrokerReadList`, and `BrokerReadFailure` (which never compares equal to `[]` and has `ok=False`).
2. **`src/journal/db.py`**:
   - Added `protection_attempts` table and partial unique index `idx_unique_active_protection_attempt` on `(parent_order_id, uncovered_from_qty)` where `state IN ('SUBMITTING', 'UNKNOWN', 'ACKED_UNVERIFIED', 'CONFIRMED')`.
   - Added `create_protection_attempt()`, `update_protection_attempt()`, `get_unresolved_protection_attempts()`, and `get_protection_attempts()`.
3. **`src/broker/dhan_client.py`**:
   - Updated `get_positions()`, `get_trade_book()`, `get_forever_orders()`, `get_super_orders()`, and `get_order_book()` via `_read_collection()` to return `BrokerReadList` (`OK_EMPTY` / `OK_RECORDS`) on valid HTTP 200 list-of-dict responses and `BrokerReadFailure` (`FAILED`, `MALFORMED`, `UNAVAILABLE`) on HTTP errors, exceptions, non-list JSON, or non-dict elements.
   - Implemented shared authoritative verifiers `verify_forever_oco_group()` / `evaluate_forever_oco_protection()` and `verify_super_order_record()` / `evaluate_super_order_protection()`.
   - Removed the top-level `stopLossPrice`/`targetPrice` fallback from `verify_super_order_legs_active()` and enforced mandatory active `legDetails` status, remaining quantity, and price checks.
4. **`src/engine/protection_watcher.py`**:
   - Added `_pass_lock` and `_save_strict()` so pre-POST protection attempts are durably committed to SQLite and disk before any broker mutation; persistence failures abort before `place_forever_oco` or `modify_super_order_protection` is called.
   - Restored unresolved SQLite `protection_attempts` during `_load()` and treated `SUBMITTING` and `UNKNOWN` attempts as potentially accepted (never falling through to a second submission on an empty lookup).
   - Updated `_reconcile_existing_forever_attempt()` to verify existing attempts strictly against their own stored `qty`, `sl_trigger`, `sl_limit`, `target_price`, and `tick_size` before protecting any newly uncovered remainder at its marginal execution price.
   - Removed the weak watcher fallback in `_protect()` that previously accepted two `TRADED` legs or top-level prices when `verify_super_order_legs_active()` returned `False`.
   - Removed mutation-call `TypeError` retry loops (`_invoke_place_forever_oco_once()` inspects signature prior to invocation and calls at most once).
   - Wired automatic retry into `check_once()` after confirmed terminal `CANCELLED` status and full protection of any partial fills.
5. **`src/engine/reconciliation.py`**:
   - Added `_read_required_broker_collection()` so failed or malformed `trade_book`, `positions`, `forever_orders`, or `super_orders` reads keep the startup gate `BLOCKED`.
   - Rebuilt `_verify_open_positions_protected()` to compute coverage strictly from current, authoritatively validated, deduplicated broker protection groups (`verify_forever_oco_group` and `verify_super_order_record`), eliminating local `protected_qty` credit, local+broker double counting, and two-leg OCO double counting.
6. **`src/main.py`**:
   - Passed `risk_manager=self.risk_manager` and `retry_timeout_sec=config.partial_fill_retry_timeout_sec` to `ProtectionWatcher`.
   - Made `TradingBotPipeline.__init__` hermetic when `scrip_master.csv` is absent on disk.

---

## 3. Correction Matrix

| Area / Defect ID | Confirmed Defect / Incomplete Feature | Implementation | Verification Tests (`tests/test_v13_targeted_safety_corrections.py`) |
|---|---|---|---|
| **Safety Correction A.1** (Confirmed Defect) | Pre-POST `pending_oco` kept only in memory; `_save()` swallowed persistence errors | `JournalDB.create_protection_attempt()` + `ProtectionWatcher._save_strict()` before any broker POST; aborts fail-closed on exception | `test_A1_persistence_failure_prevents_broker_post` |
| **Safety Correction A.2** (Confirmed Defect) | Restored `SUBMITTING` attempt with `oco_id=None` fell through to a second `place_forever_oco` submission | `_load()` restores `protection_attempts`; `_reconcile_existing_forever_attempt()` treats `SUBMITTING` and `UNKNOWN` as potentially accepted and blocks duplicate POST | `test_A2_restart_after_durable_reservation_before_submission_does_not_blindly_resubmit`, `test_A3_restart_after_broker_acceptance_before_saving_response_reconciles_without_duplicate`, `test_A4_http_uncertainty_preserves_original_attempt_and_identity` |
| **Safety Correction A.3** (Confirmed Defect) | Existing attempt verified against cumulative parent `new_qty` (`10`) instead of stored attempt `qty` (`5`) and prices | `_reconcile_existing_forever_attempt()` verifies against stored attempt `qty`/`levels`, confirms first 5 shares, then `_protect()` submits Attempt B only for remaining 5 shares at marginal price | `test_A5_delayed_verification_with_5_to_10_cumulative_fill_confirms_first_5_and_protects_remainder_5`, `test_A7_healthy_normal_submission_completes_successfully` |
| **Safety Correction A.4** (Confirmed Defect) | Concurrent watcher checks could submit competing OCO attempts for the same uncovered fill | `ProtectionWatcher._pass_lock` + SQLite partial unique index `idx_unique_active_protection_attempt` on `(parent_order_id, uncovered_from_qty)` | `test_A6_concurrent_checks_cannot_submit_duplicate_protection` |
| **Safety Correction B** (Confirmed Defect) | Watcher fallback overrode failed `verify_super_order_legs_active()` and accepted two `TRADED` legs or top-level prices without active leg evidence | Unified `verify_super_order_record()` in `src/broker/dhan_client.py`; removed watcher fallback in `src/engine/protection_watcher.py` and top-level price fallback in `dhan_client.py` | `test_B1_two_traded_legs_never_produce_confirmed_protection`, `test_B2_missing_leg_status_or_missing_quantity_evidence_does_not_confirm_coverage`, `test_B3_stop_loss_only_or_target_only_response_fails_verification`, `test_B4_wrong_instrument_wrong_parent_insufficient_qty_and_incorrect_prices_fail_verification`, `test_B5_matching_top_level_prices_with_no_active_leg_evidence_remain_unverified`, `test_B6_valid_documented_active_pair_passes`, `test_B7_rejected_strict_result_cannot_be_overridden_by_watcher` |
| **Safety Correction C.A** (Confirmed Defect) | `get_positions()`, `get_trade_book()`, `get_forever_orders()`, `get_super_orders()` returned `[]` on HTTP 5xx, exceptions, or malformed JSON | `DhanClient._read_collection()` returns `BrokerReadList` (`OK_EMPTY`/`OK_RECORDS`) vs `BrokerReadFailure` (`FAILED`/`MALFORMED`/`UNAVAILABLE`); `StartupReconciliationGate` blocks on non-OK reads | `test_C5_failed_positions_trade_book_or_required_protection_reads_keep_startup_blocked`, `test_C6_malformed_responses_keep_startup_blocked`, `test_C7_genuinely_empty_account_with_successful_reads_can_start` |
| **Safety Correction C.B & C.C** (Confirmed Defect) | Startup credited local `protected_qty`, double-counted local + broker coverage, double-counted two OCO legs (5 -> 10), and ignored product/segment mismatches | `StartupReconciliationGate._verify_open_positions_protected()` groups by `orderId`, validates via `verify_forever_oco_group()` / `verify_super_order_record()`, counts each group once, and never credits local coverage | `test_C1_historical_local_coverage_of_10_with_no_active_broker_protection_blocks_startup`, `test_C2_local_coverage_of_5_plus_same_broker_coverage_of_5_cannot_cover_position_of_10`, `test_C3_five_share_oco_pair_counts_as_five`, `test_C4_coverage_for_wrong_product_or_segment_is_rejected`, `test_C8_correctly_protected_account_can_start` |
| **Section 6: Automatic Retry** (Incomplete Feature -> Wired) | `execute_retry()` had no production caller in `ProtectionWatcher.check_once()` | Wired `execute_retry()` into `ProtectionWatcher.check_once()` once order reaches broker-confirmed terminal `CANCELLED` and all partial fills are confirmed protected | `test_R1_production_watcher_timeout_cancel_and_confirmed_cancelled_triggers_single_retry`, `test_R2_production_watcher_super_order_timeout_cancel_retries_via_place_super_order` |

---

## 4. Migration & Rollback Instructions

### Schema Migration (Automatic & Additive)
- On startup, `JournalDB._init_db()` automatically executes `CREATE TABLE IF NOT EXISTS protection_attempts (...)` and `CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_protection_attempt ...`.
- Existing tables (`signals`, `signal_claims`, `signal_revisions`, `order_intents`, `retry_reservations`, `orders`, `fills`, `protection_orders`, `watcher_orders`, `outbox`, `system_events`) and existing account data are preserved without modification or deletion.
- Existing unresolved records in `pending_protection.json` / `watcher_orders` without an `attempt_id` are conservatively reconciled in `_reconcile_existing_forever_attempt()` using their stored `correlation_id` / `oco_id` and `qty`.

### Rollback Implications
- Because `protection_attempts` is purely additive, rolling back to V13 code does not break SQLite table reads on existing tables.
- However, before rolling back, verify that `SELECT COUNT(*) FROM protection_attempts WHERE state IN ('SUBMITTING', 'UNKNOWN', 'ACKED_UNVERIFIED');` returns `0` so no in-flight protection attempt is stranded across a downgrade.
- Never overwrite live `data/trade_journal.db`, `data/pending_protection.json`, `data/emergency_stop.json`, `.env`, or `*.session` files when deploying or rolling back.

---

## 5. Exact Offline Verification Commands

```bash
# 1. Run the new 24-test targeted safety correction suite
python3 -m unittest tests/test_v13_targeted_safety_corrections.py -v

# 2. Run the full 218-test suite under the strict outbound-HTTP-blocking harness
python3 - <<'EOF'
import unittest, requests
from unittest.mock import patch

def _blocked_request(self, method, url, *args, **kwargs):
    raise RuntimeError(f"HERMETIC VIOLATION: Unmocked outbound HTTP {method} {url}")

with patch.object(requests.sessions.Session, "request", _blocked_request):
    suite = unittest.defaultTestLoader.discover("tests")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    assert result.wasSuccessful(), f"Failed: {len(result.failures)} failures, {len(result.errors)} errors"
EOF

# 3. Compile all Python modules and check shell script syntax
python3 -m py_compile run.py src/*.py src/**/*.py tests/*.py
bash -n bot deploy/setup_vm.sh
```

---

## 6. Checks Requiring Live Deployment Environment
1. **Live Dhan v2 Payload Conformance**: Confirm that live `GET /v2/forever/all` and `GET /v2/super/orders` responses in the production Dhan account populate `orderId`, `securityId`, `legDetails`, `orderStatus`, and `remainingQuantity`/`quantity` identically to documented v2 schemas during market hours.
2. **Yahoo Finance Live Latency & Clock Sync**: Verify NTP clock synchronization (`chrony`/`systemd-timesyncd`) on the production VM so live `regularMarketTime` deltas remain within `[0.0s, 2.0s]` during NSE regular hours (`09:15–15:25 IST`).
3. **Systemd Service & File Permissions**: Verify `.env`, `~/.dhan_token`, and `data/emergency_stop.json` ownership and `0600` permissions under the target VM service user.
