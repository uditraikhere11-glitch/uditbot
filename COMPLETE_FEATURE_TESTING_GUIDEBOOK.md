# Auditbot V13 — Complete Feature Testing Guidebook

## Overview & Safety Preconditions
Auditbot V13 is a single-account Telegram-to-Dhan automated trading system for NSE/BSE Cash Equities (`CNC`/`MTF`) and listed Stock Options (`MARGIN` via Dhan Super Orders). Before running any test, verify:
- Keep `TRADING_MODE=TEST` in `.env` during all offline, parser, sizing, and dry-run pipeline tests.
- Never copy test databases (`trade_journal.db`), test protection files (`pending_protection.json`), or test emergency-stop files (`emergency_stop.json`) over a live VM's `data/` directory.

---

## 1. Master Test Suite Execution (All 220 Automated Unit & Regression Tests)

### 1.1 Run the Complete Hermetic Offline Suite (Blocks All Outbound Network Calls)
```bash
python3 - <<'EOF'
import unittest, requests
from unittest.mock import patch

def _deny_http(self, method, url, *args, **kwargs):
    raise RuntimeError(f"HERMETIC VIOLATION: Unmocked outbound HTTP {method} {url}")

with patch.object(requests.sessions.Session, "request", _deny_http):
    suite = unittest.defaultTestLoader.discover("tests")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"Total: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
    assert result.wasSuccessful()
EOF
```
- **Expected Outcome**: `Ran 220 tests ... OK (Failures: 0, Errors: 0)`.

### 1.2 Bytecode & Shell Script Syntax Verification
```bash
python3 -m py_compile run.py src/*.py src/**/*.py tests/*.py
bash -n bot deploy/setup_vm.sh
```
- **Expected Outcome**: Exit code `0` with zero syntax errors.

---

## 2. Feature-by-Feature Test Matrix & Commands

| Feature Group | Test Module | Exact Command | What It Verifies |
|---|---|---|---|
| **1. Cash Equity Parser & Company-Name Resolution** | `tests/test_equity_parser.py`, `tests/test_darvas_merged_features.py`, `tests/test_step20_regex_aliases.py` | `python3 -m unittest tests/test_equity_parser.py tests/test_step20_regex_aliases.py -v` | Hashtag buys (`Buy #SBIN @ 805`), CMP buys (`Bought #RELIANCE`), `around`/`above` aliases, Darvas company-name resolution (`Bought Manipal Payment at 450` -> `MPIMANIPAL`), multi-stock rejection, exit signal classification (`Exit #SBIN`), index rejection (`NIFTY`/`BANKNIFTY`), and 60s message age gate. |
| **2. Stock Options (F&O) Parser & `looks_like_option` Gate** | `tests/test_fno_parser.py`, `tests/test_darvas_merged_features.py` | `python3 -m unittest tests/test_fno_parser.py -v` | Option range & single-price parsing (`#POLYCAB 8000 PE OCT @90-105`), `CALL`/`PUT` to `CE`/`PE` mapping, index option blocking, F&O exit routing (`Book profit in POLYCAB 8000 PE`), and separation from equity exits. |
| **3. Gemini Structured Fallback Parser (Hardened)** | `tests/test_v13_independent_audit_remediations.py` | `python3 -m unittest tests.test_v13_independent_audit_remediations.TestV13IndependentAuditRemediations.test_22_v12_26_gemini_fallback_requires_source_evidence_and_valid_option_type -v` | Verifies Gemini fallback requires source-text evidence for strike/price and rejects hallucinated strikes or invalid `option_type` (`STRADDLE` -> `None`, never coerced to `PE`). |
| **4. Scrip Master & Instrument Resolver** | `tests/test_resolver.py`, `tests/test_v13_independent_audit_remediations.py` | `python3 -m unittest tests/test_resolver.py -v` | Atomic staged download + SHA-256 `.meta.json` validation, idempotent reload without duplicating `_options_list`, exact expiry day precedence, monthly expiry selection, nearest-upcoming expiry fallback, and debt/NCD exclusion. |
| **5. Yahoo Finance Primary Live LTP Tunnel (`<= 2.0s` Gate)** | `tests/test_v12_execution_safety.py`, `tests/test_darvas_merged_features.py` | `python3 -m unittest tests/test_v12_execution_safety.py -k "yahoo or bse or fno_using_underlying" -v` | Validates `regularMarketPrice` + `regularMarketTime` against post-HTTP UTC wall clock (`<= 2.0s` eligible, `2.1s` rejected), rejects negative/future timestamps, rejects BSE `.BO` delayed quotes in LIVE mode, validates `NSI`/`NSE` venue and `INR` currency, and blocks substituting underlying equity LTP for F&O option premiums. |
| **6. Position Sizing & Tick Rounding** | `tests/test_sizer.py` | `python3 -m unittest tests/test_sizer.py -v` | Equity `BUDGET` & `FIXED_QTY` sizing, Equity `+0.5%` markup, F&O `+4.0%` ceiling above `entry_max`, F&O `BUDGET`/`FIXED_LOTS` with `FNO_MAX_LOTS` cap, multi-target tranche splitting, and `SL`/`Target` tick-grid alignment. |
| **7. Cross-Channel Deduplication & Atomic Signal Claims** | `tests/test_duplicate_filter.py`, `tests/test_journal.py`, `tests/test_11_step_rework_matrix.py` | `python3 -m unittest tests/test_duplicate_filter.py tests/test_journal.py -v` | 20-second cross-channel per-symbol deduplication, SQLite `INSERT OR IGNORE` atomic `(channel_id, message_id)` claims across 100 concurrent threads, and Telegram message edit tracking (`signal_revisions`) without triggering a duplicate order. |
| **8. Risk Manager & Durable Emergency Stop** | `tests/test_risk_manager.py`, `tests/test_emergency_stop.py` | `python3 -m unittest tests/test_risk_manager.py tests/test_emergency_stop.py -v` | Fail-closed missing/corrupt/wrong-account `emergency_stop.json`, live portfolio risk sync (`MAX_DAILY_TRADES`, `MAX_OPEN_POSITIONS`, `MAX_TOTAL_EXPOSURE_INR`, `MAX_DAILY_LOSS_INR`, `MAX_DRAWDOWN_PCT`), and risk-reducing cancellation/protection exemptions during STOP. |
| **9. Shared Dhan TOTP Token Coordinator** | `tests/test_dhan_auth.py`, `tests/test_v13_independent_audit_remediations.py` | `python3 -m unittest tests/test_dhan_auth.py -v` | Pure-stdlib RFC 6238 TOTP generation, `fcntl.flock` cross-process token coordination (`fresh_token` / `force_login`), `0600` file permissions, and mtime-based token adoption without recursive order resubmission. |
| **10. Safety Correction A: Durable Protection-Attempt Lifecycle** | `tests/test_v13_targeted_safety_corrections.py` | `python3 -m unittest tests/test_v13_targeted_safety_corrections.py -k "test_A or test_D1" -v` | Pre-POST SQLite commit in `protection_attempts`, fail-closed abort if persistence fails, crash recovery of `SUBMITTING`/`UNKNOWN` attempts without duplicate OCO/Super submission, `5 -> 10` incremental fill verification against stored attempt qty/prices, and concurrency protection. |
| **11. Safety Correction B: Authoritative Super & OCO Verification** | `tests/test_v13_targeted_safety_corrections.py`, `tests/test_hardened_reproductions.py` | `python3 -m unittest tests/test_v13_targeted_safety_corrections.py -k "test_B" -v` | Rejects two `TRADED` legs, missing leg status/quantity, single leg, wrong parent/securityId/qty/price, and top-level `stopLossPrice`/`targetPrice` without active `legDetails`; prevents watcher fallback from overriding strict verification failure. |
| **12. Safety Correction C: Broker-Evidenced Startup Reconciliation** | `tests/test_v13_targeted_safety_corrections.py`, `tests/test_step14_step15.py` | `python3 -m unittest tests/test_v13_targeted_safety_corrections.py -k "test_C" -v` | Distinguishes `OK_EMPTY` (`[]`), `OK_RECORDS`, and `FAILED`/`MALFORMED`/`UNAVAILABLE` broker reads; blocks startup on read failure, malformed JSON, historical local coverage without active broker protection, local+broker double counting, or wrong product/segment; counts a 5-share two-leg OCO pair once as 5. |
| **13. Section 6: Production Automatic Retry & Order Timeouts** | `tests/test_v13_targeted_safety_corrections.py`, `tests/test_safe_cancel_and_protection.py` | `python3 -m unittest tests/test_v13_targeted_safety_corrections.py -k "test_R or test_D2" -v` | 60s Equity and 180s F&O entry remainder timeouts, `CANCEL_REQUESTED -> CANCEL_PENDING -> CANCELLED` state machine, automatic single retry wired into `ProtectionWatcher.check_once()` after confirmed terminal `CANCELLED` and partial-fill protection, `< 1.00%` movement gate, `<= 1.00%` markup cap, Super Order retry routing (`place_super_order`), and 1-retry lineage cap. |
| **14. Durable Outbox Notifier & Secret Redaction** | `tests/test_notifier.py`, `tests/test_11_step_rework_matrix.py` | `python3 -m unittest tests/test_notifier.py -v` | Trade/exit card formatting, SQLite outbox queue with exponential backoff (`DEAD_LETTER` after max retries), truthful `False` return when `TG_BOT_TOKEN` is unconfigured, and JWT/PIN/bot-token secret redaction. |
| **15. Operator CLI (`bot`), Daily Health Check & Web Setup UI** | `tests/test_setup.py`, `tests/test_step23_step24.py` | `python3 -m unittest tests/test_setup.py tests/test_step23_step24.py -v` | `./bot status`, `./bot dashboard`, `./bot check-dhan`, `./bot daily_health_check`, `./bot emergency-stop`, `./bot resume`, singleton `listener.lock` enforcement, and byte-for-byte original Web Setup UI (`src/setup_app.py`). |

---

## 3. Interactive CLI & End-to-End Dry-Run Verification Walkthrough

### 3.1 Verify Operator CLI Commands in `TEST` Mode
Run each command from the repository root (or via a symlinked `/usr/local/bin/bot`):
```bash
# 1. Provision clean initial emergency stop state for your account ID in TEST mode
python3 -c '
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
import os
cfg = AppConfig.from_env()
os.makedirs(cfg.data_dir, exist_ok=True)
 EmergencyStopManager(os.path.join(cfg.data_dir, "emergency_stop.json"), expected_account_id=cfg.dhan_client_id).provision_initial_clean_state("Udit", cfg.dhan_client_id)
'

# 2. Check status and read-only SSH dashboard (zero network calls)
./bot status
./bot dashboard
./bot mode
./bot journal 5

# 3. Test Emergency Stop activation and Resume
./bot emergency-stop "Routine drill test"
./bot status
./bot resume "Drill completed"
./bot status
```

### 3.2 Run an End-to-End Pipeline Simulation in `TEST` Mode (Zero Broker Orders)
```bash
python3 - <<'EOF'
import os, tempfile
from datetime import date
from src.config import AppConfig
from src.models import TradingMode
from src.main import TradingBotPipeline

with tempfile.TemporaryDirectory() as tmp:
    cfg = AppConfig(
        trading_mode=TradingMode.TEST,
        equity_active=True,
        fno_active=True,
        dhan_client_id="1113731536",
        data_dir=tmp,
        journal_db_path=os.path.join(tmp, "journal.db"),
    )
    pipe = TradingBotPipeline(cfg)
    pipe.stop_manager.provision_initial_clean_state("Udit", "1113731536")
    pipe.resolver.load_equity("MPIMANIPAL", "99112", "NSE_EQ", 0.05, "Manipal Payment and Identity Solutions")
    pipe.resolver.load_option("POLYCAB", "54231", 8000.0, "PE", date(2099, 10, 29), 100, "NSE_FNO", 0.05, "POLYCAB-29OCT99-8000-PE")

    # 1. Test Company-Name Equity Signal
    r_eq = pipe.process_raw_message("Bought Manipal Payment at 450", channel_id=-1001, message_id=1, msg_timestamp=1000.0, current_time=1002.0)
    print("Equity Result:", r_eq.is_valid, r_eq.symbol, r_eq.limit_price, r_eq.quantity_or_lots, r_eq.reason)
    assert r_eq.is_valid and r_eq.symbol == "MPIMANIPAL"

    # 2. Test Stock Option Signal
    r_opt = pipe.process_raw_message("#POLYCAB 8000 PE OCT @90-105", channel_id=-1001, message_id=2, msg_timestamp=1005.0, current_time=1007.0)
    print("Option Result:", r_opt.is_valid, r_opt.symbol, r_opt.limit_price, r_opt.quantity_or_lots, r_opt.reason)
    assert r_opt.is_valid and r_opt.symbol == "POLYCAB"

    # 3. Test Index Rejection
    r_idx = pipe.process_raw_message("Buy #NIFTY 25000 CE @ 100", channel_id=-1001, message_id=3, msg_timestamp=1010.0, current_time=1011.0)
    print("Index Result:", r_idx.is_valid, r_idx.action.value, r_idx.reason)
    assert not r_idx.is_valid
EOF
```

### 3.3 Pre-Live Deployment Checklist (In Staging / Production VM)
1. **Verify Dhan Credentials & 08:00 IST Health Check**:
   - Configure `.env` (`chmod 600 .env`) with `TRADING_MODE=TEST` first.
   - Run `./bot daily_health_check` and `./bot check-dhan` to verify TOTP token generation (`~/.dhan_token` with `0600` permissions), `/v2/fundlimit` balance readout, Yahoo Finance live quote freshness (`<= 2.0s`), and `data/scrip_master.csv` download.
2. **Verify Web Setup UI**:
   - Run `./bot web 8080` (or `python3 -m src.setup_app`) and open `http://<vm-ip>:8080/` to inspect or update `.env` parameters, generate a Dhan token, or authenticate the Telegram session.
3. **Verify Startup Reconciliation Gate**:
   - Start `./bot start` (or `python3 run.py`) in `TEST` mode first and confirm logs print:
     `✅ [RECONCILIATION] Invariant checks passed! Gate opened: ACCEPTING_NEW_ENTRIES=True.`
   - Only switch to `TRADING_MODE=LIVE` (`./bot live`) after confirming `bot dashboard` reports `0` unprotected positions, `0` unresolved protection attempts, and `0` unknown intents.
