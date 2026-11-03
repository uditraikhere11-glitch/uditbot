"""
SSH Terminal Operator Dashboard CLI ('bot' command).
Provides lightweight, secure operational controls over SSH without web dependencies.
Implements STEP 24 (24-A through 24-E) + V13 Audit Hardening:
- Read-only SSH dashboard sourced strictly from durable SQLite journal and STOP state (`DASH-01`).
- Never performs any outbound broker HTTP mutations (`POST`, `PUT`, `DELETE`) from the dashboard.
- V12-20 Fix: Routes token refresh through the shared `fcntl.flock` token coordinator.
- V12-22 Fix: Passes `account_id=cfg.dhan_client_id` and `expected_account_id=cfg.dhan_client_id` to
  `JournalDB` and `EmergencyStopManager`, and resolves `.env` via `resolve_env_path`.
- V12-24 Fix: Implements `daily_health_check` command invoked by `dhan-token-refresh.service` with
  non-zero exit codes on failure.
"""

import sys
import os
import json
import time
import datetime
import zoneinfo
from src.config import AppConfig, resolve_env_path
from src.journal.db import JournalDB
from src.engine.emergency_stop import EmergencyStopManager


def _mask_account_id(acct: str) -> str:
    if not acct:
        return "Not Configured"
    acct_str = str(acct).strip()
    if len(acct_str) <= 4:
        return acct_str
    return f"***{acct_str[-4:]}"


def print_banner():
    print("=======================================================")
    print("      UDIT'S TELEGRAM-TO-DHAN BOT (SINGLE-ACCOUNT)     ")
    print("=======================================================")


def cmd_status(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager):
    print_banner()
    stop_state = stop_mgr.get_state()
    is_stopped = stop_state.get("is_stopped", False)
    stop_str = (
        f"🛑 STOPPED (by {stop_state.get('stopped_by')}: {stop_state.get('reason')})"
        if is_stopped else "🟢 NORMAL (Trading Allowed)"
    )

    print(f"[*] Emergency STOP Status  : {stop_str}")
    print(f"[*] Operating Master Switch : {cfg.trading_mode.value}")
    print(f"[*] Equity Trading Active  : {'YES' if cfg.equity_active else 'NO'}")
    print(f"[*] Stock Options Active   : {'YES' if cfg.fno_active else 'NO'}")
    print(f"[*] Dhan Client ID         : {_mask_account_id(cfg.dhan_client_id)}")
    print(f"[*] Token File Location    : {cfg.dhan_token_file}")
    token_exists = os.path.exists(os.path.expanduser(cfg.dhan_token_file)) and os.path.getsize(os.path.expanduser(cfg.dhan_token_file)) > 0
    print(f"[*] Token File Present     : {'YES' if token_exists else 'NO'}")
    print(f"[*] Monitored Channels     : {len(cfg.monitored_channel_ids)} configured")
    print(f"[*] Log Channel ID         : {cfg.log_group_id or 'Not Configured'}")
    print("-------------------------------------------------------")
    print(f"[*] Equity Sizing Mode     : {cfg.equity_sizing_mode} (Budget={cfg.equity_budget_inr:g} INR)")
    print(f"[*] Equity CMP Policy      : {getattr(cfg, 'equity_cmp_execution_policy', 'STRICT_TIMESTAMP')}")
    print(f"[*] FnO Sizing Mode        : {cfg.fno_sizing_mode} (Budget={cfg.fno_capital_per_trade:g} INR, Max Lots={cfg.fno_max_lots})")
    print(f"[*] Protection (Post-Fill) : SL={cfg.sl_pct}% | Target={cfg.target_pct}%")
    print(f"[*] Risk Limits            : Max Order={cfg.max_order_amount_inr:g} | Min Funds={cfg.min_trade_funds_inr:g}")
    print(f"[*] Timing Thresholds      : Quote Staleness={cfg.max_quote_age_sec}s | Retry Timeout={cfg.partial_fill_retry_timeout_sec}s")
    print(f"[*] SQLite Journal Path    : {cfg.journal_db_path}")
    print(f"[*] Emergency State File   : {stop_mgr.state_file}")
    if is_stopped:
        in_flight = stop_state.get("in_flight_orders", [])
        if in_flight:
            print(f"[*] In-Flight Orders       : {in_flight}")
    print("=======================================================")


def cmd_dashboard(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager):
    """
    STEP 24: Read-only live operational dashboard based strictly on journal state.
    Zero broker network mutations or unverified claims of active protection.
    """
    print("=======================================================")
    print("           AUDIT BOT READ-ONLY SSH DASHBOARD           ")
    print("=======================================================")
    stop_state = stop_mgr.get_state()
    is_stopped = stop_state.get("is_stopped", False)
    stop_str = (
        f"🛑 STOPPED (by {stop_state.get('stopped_by')}: {stop_state.get('reason')})"
        if is_stopped else "🟢 NORMAL (Trading Allowed)"
    )
    allowlist_count = len(cfg.monitored_channel_ids)
    allowlist_status = f"{allowlist_count} channel(s)" if allowlist_count > 0 else "⚠️ EMPTY (Blocked in LIVE)"

    print(f"[*] Operating Mode      : {cfg.trading_mode.value}")
    print(f"[*] Emergency STOP      : {stop_str}")
    print(f"[*] Dhan Account ID     : {_mask_account_id(cfg.dhan_client_id)}")
    print(f"[*] Source Allowlist    : {allowlist_status}")
    print(f"[*] Active Segments     : Equity={'YES' if cfg.equity_active else 'NO'} | F&O={'YES' if cfg.fno_active else 'NO'}")
    print(f"[*] Equity CMP Policy   : {getattr(cfg, 'equity_cmp_execution_policy', 'STRICT_TIMESTAMP')}")
    print("-------------------------------------------------------")
    ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    now_ist = datetime.datetime.now(ist)
    today_start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    metrics = db.get_dashboard_summary(start_of_day=today_start_ist)
    print(f"[*] Signals Received    : Today (IST)={metrics['signals_today']} | Total All-Time={metrics['signals_all']}")
    print(f"[*] Orders (By Mode)    : LIVE={metrics['orders_live']} | TEST={metrics['orders_test']}")
    print(f"[*] Orders (By Segment) : Cash Equity={metrics['orders_eq']} | Stock Options={metrics['orders_fno']}")

    prot_confirmed = metrics.get("protection_confirmed", 0)
    prot_local = metrics.get("protection_active", 0)
    prot_unprot = metrics.get("protection_unprotected", 0)
    if prot_unprot > 0:
        print(f"🚨 [ALERT] Unprotected  : {prot_unprot} filled position(s) UNPROTECTED!")
    if prot_local > 0 and prot_confirmed < prot_local:
        print(
            f"[*] Protective Records  : {prot_local} local OCO legs recorded "
            f"({prot_confirmed} broker-confirmed, {prot_local - prot_confirmed} UNVERIFIED)"
        )
    else:
        print(f"[*] Protective Records  : {prot_local} local OCO legs recorded ({prot_confirmed} broker-confirmed)")

    non_term = metrics.get("non_terminal_intents", metrics["unknown_intents"])
    if non_term > 0:
        print(
            f"⚠️ [ATTENTION] Non-Terminal Intents : {non_term} unresolved order intent(s) "
            f"(UNKNOWN={metrics['unknown_intents']}) require broker reconciliation!"
        )
    else:
        print("[*] In-Flight / Unknown : 0 unmanaged orders (Clean state)")
    try:
        from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR
        GLOBAL_LATENCY_MONITOR.bind_db(db, force_reload=True)
        print(GLOBAL_LATENCY_MONITOR.format_dashboard_section())
    except Exception:
        pass
    print("=======================================================")


def cmd_journal(db: JournalDB, limit: int = 10):
    print(f"\n--- Last {limit} Signals Recorded in SQLite Audit Journal ---")
    signals = db.get_recent_signals(limit=limit)
    if not signals:
        print("No signals recorded yet.")
        return
    for s in signals:
        print(
            f"[{s['id']}] {s['signal_type']} | {s['symbol']} | Action: {s['action']} | "
            f"Status: {s['status']} | Reason: {s['reason']}"
        )


def cmd_emergency_stop(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager, reason: str = ""):
    op_name = getattr(cfg, "operator_name", os.environ.get("OPERATOR_NAME", "Udit"))
    stop_reason = reason or "Operator triggered emergency stop via CLI"
    stop_mgr.trigger_stop(
        operator=op_name,
        reason=stop_reason,
        account_id=cfg.dhan_client_id,
    )
    db.record_system_event("EMERGENCY_STOP", f"Operator {op_name} triggered emergency stop: {stop_reason}")
    print(">>> EMERGENCY STOP ACTIVATED <<<")
    print(f"Operator: {op_name}")
    print(f"Reason: {stop_reason}")
    print(f"Account: {cfg.dhan_client_id or 'All'}")
    print("All NEW risk-increasing broker order submissions are now BLOCKED.")
    print("Risk-reducing cancellations and open-position protection legs remain authorized.")


def cmd_resume(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager, reason: str = ""):
    op_name = getattr(cfg, "operator_name", os.environ.get("OPERATOR_NAME", "Udit"))
    resume_reason = reason or "Operator resumed trading via CLI"
    metrics = db.get_dashboard_summary()
    if metrics.get("protection_unprotected", 0) > 0 or metrics.get("unresolved_protection_attempts", 0) > 0:
        print("❌ RESUME BLOCKED: Unprotected filled positions exist in journal. Reconcile before resuming.")
        return
    stop_mgr.resume(operator=op_name, reason=resume_reason)
    db.record_system_event("RESUME_TRADING", f"Operator {op_name} resumed trading: {resume_reason}")
    print(">>> TRADING OPERATIONS RESUMED <<<")
    print(f"Authorized by: {op_name}")
    print(f"Reason: {resume_reason}")


def cmd_refresh_token(cfg: AppConfig, stop_mgr: EmergencyStopManager) -> bool:
    """Generates a fresh 24-hour Dhan access token using Client ID, PIN, and TOTP secret via shared lock."""
    from src.broker.dhan_client import DhanClient

    print("=======================================================")
    print("         DHAN AUTOMATED TOTP TOKEN REFRESH            ")
    print("=======================================================")
    print(f"[*] Dhan Client ID    : {cfg.dhan_client_id or 'Not Configured'}")
    print(f"[*] Dhan PIN          : {'Configured' if cfg.dhan_pin else 'MISSING'}")
    print(f"[*] Dhan TOTP Secret  : {'Configured' if cfg.dhan_totp_secret else 'MISSING'}")

    if not cfg.dhan_client_id or not cfg.dhan_pin or not cfg.dhan_totp_secret:
        print("\n❌ Missing credentials in .env.")
        print("Please ensure DHAN_CLIENT_ID, DHAN_PIN, and DHAN_TOTP_SECRET are set.")
        return False

    from src.broker.dhan_token import read_token, write_token_atomic
    token_path = os.path.expanduser(cfg.dhan_token_file)
    prev_token = read_token(token_path) if os.path.exists(token_path) else ""

    client = DhanClient(cfg, stop_manager=stop_mgr)
    ok, token, msg = client.generate_token_with_totp(force=True)
    if ok and token:
        ok_f, bal, msg_f = client.verify_token_authenticated()
        if not ok_f:
            print(f"\n❌ Post-refresh broker verification failed: {msg_f}")
            if prev_token and prev_token != token:
                write_token_atomic(prev_token, token_path)
                client.token = prev_token
                try:
                    client._token_mtime = os.path.getmtime(token_path)
                except Exception:
                    pass
                ok_prev, bal_prev, msg_prev = client.verify_token_authenticated()
                if ok_prev:
                    print(
                        f"[*] RECOVERY_VERIFIED: Restored previous token and confirmed broker authentication "
                        f"(Balance: INR {bal_prev:,.2f})."
                    )
                else:
                    print(
                        f"[!] RECOVERY_UNVERIFIED: Previous token is no longer valid after new TOTP attempt "
                        f"({msg_prev}). Broker authentication remains UNVERIFIED!"
                    )
            else:
                print("[!] No distinct previous token available for recovery; broker authentication remains UNVERIFIED.")
            print("=======================================================")
            return False

        print("\n✅ Successfully generated fresh Dhan access token via TOTP!")
        print(f"[*] Saved to token file: {cfg.dhan_token_file} (Single authoritative token source)")
        print(f"[*] Available Balance : INR {bal:,.2f}")
        print("=======================================================")
        return True
    else:
        print(f"\n❌ Failed to generate token: {msg}")
        print("=======================================================")
        return False


def cmd_daily_health_check(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager) -> int:
    """
    V12-24: Executes the scheduled 08:00 IST daily token refresh and health probe invoked by
    `dhan-token-refresh.service`. Returns exit code 0 on success, 1 on failure.
    """
    from src.broker.dhan_client import DhanClient
    from src.instruments.resolver import InstrumentResolver

    print("=======================================================")
    print("       AUDIT BOT 08:00 IST DAILY HEALTH CHECK          ")
    print("=======================================================")

    tok_ok = cmd_refresh_token(cfg, stop_mgr)
    client = DhanClient(cfg, stop_manager=stop_mgr)
    ok_funds, avail_bal, funds_msg = client.get_fund_limits(retry=False)

    scrip_path = os.path.join(cfg.data_dir, "scrip_master.csv")
    resolver = InstrumentResolver()
    master_ok = resolver.load_dhan_master(
        scrip_path,
        max_cache_age_sec=cfg.scrip_master_max_age_sec,
        allow_network_download=True,
    )

    overall_ok = bool(tok_ok and ok_funds)
    summary = (
        f"token_refresh={'OK' if tok_ok else 'FAIL'}, "
        f"funds={'OK' if ok_funds else 'FAIL'} ({avail_bal:,.2f} INR: {funds_msg}), "
        f"scrip_master={'OK' if master_ok else 'WARN'}"
    )
    db.record_system_event("DAILY_HEALTH_CHECK", summary)
    if overall_ok:
        print(f"✅ Daily health check passed: {summary}")
        return 0
    print(f"❌ Daily health check failed: {summary}")
    return 1


def cmd_check_dhan(cfg: AppConfig, stop_mgr: EmergencyStopManager):
    """Diagnoses Dhan connection, authentication, market feed (LTP), and execution readiness."""
    from src.broker.dhan_client import DhanClient
    from src.engine.market import market_status

    print("=======================================================")
    print("        DHAN API CONNECTION & EXECUTION DIAGNOSTIC      ")
    print("=======================================================")
    print(f"[*] Client ID           : {cfg.dhan_client_id or 'NOT CONFIGURED'}")
    print(f"[*] Token File Location : {cfg.dhan_token_file}")

    client = DhanClient(cfg, stop_manager=stop_mgr)
    token_len = len(client.token)
    if token_len == 0:
        print("[!] Access Token        : MISSING (No token in .env or token file)")
    else:
        print(f"[*] Access Token        : PRESENT ({token_len} characters loaded securely)")

    print("\n[1] Testing Dhan API Authentication & Funds (/fundlimit)...")
    ok_funds = False
    is_real_auth = False
    try:
        ok_funds, avail_balance, funds_msg = client.get_fund_limits()
        is_fallback = getattr(client, "last_funds_is_fallback", False) or ("fallback" in funds_msg.lower() or "test mode default" in funds_msg.lower())
        if ok_funds and not is_fallback:
            is_real_auth = True
            print("    [OK] AUTHENTICATION SUCCESSFUL (BROKER CONFIRMED)")
            print(f"    Available Ledger Balance: INR {avail_balance:,.2f}")
        elif is_fallback:
            print("    [WARN] UNVERIFIED BROKER AUTHENTICATION (TEST-Mode Fallback):")
            print(f"           Fallback Balance : INR {avail_balance:,.2f}")
            print(f"           Detail           : {funds_msg}")
        elif "auth" in funds_msg.lower() or "token" in funds_msg.lower():
            print(f"    [FAIL] AUTHENTICATION FAILED: {funds_msg}")
        elif "malformed" in funds_msg.lower():
            print(f"    [FAIL] MALFORMED BROKER RESPONSE: {funds_msg}")
        else:
            print(f"    [FAIL] HTTP / BROKER FUNDS ERROR: {funds_msg}")
    except Exception as e:
        print(f"    [FAIL] EXCEPTION CALLING FUNDS API: {e}")

    print("\n[2] Testing Market Data Feed (/marketfeed/ltp + Yahoo Finance fallback)...")
    test_symbol = "RELIANCE"
    test_sec_id = "2885"
    test_seg = "NSE_EQ"
    try:
        ok_ltp, ltp_val, ltp_msg = client.get_ltp(test_seg, test_sec_id, symbol=test_symbol)
        if ok_ltp and ltp_val > 0:
            print("    [OK] MARKETFEED CONNECTED")
            print(f"    Live Quote for {test_symbol} (Sec ID {test_sec_id}): INR {ltp_val:,.2f}")
        elif "806" in ltp_msg or "data apis not subscribed" in ltp_msg.lower():
            print(f"    [NOTE] Data API is not subscribed on Dhan ({ltp_msg}).")
            print("           Yahoo Finance live LTP fallback is active for Cash Equity signals without a stated price.")
        else:
            print(f"    [FAIL] MARKETFEED FAILED: {ltp_msg}")
    except Exception as e:
        print(f"    [FAIL] EXCEPTION CALLING LTP API: {e}")

    # [2.1] Darvas Yahoo Finance Observational Price Diagnostic (Safe, Non-executable diagnostic)
    print("\n[2.1] Yahoo Finance Observational Diagnostic (Darvas Observer)...")
    try:
        from src.broker.yahoo_observer import YahooObserver
        observer = YahooObserver()
        obs = observer.observe(test_symbol, exchange_segment=test_seg)
        for line in obs.format_diagnostic().splitlines():
            print(f"    {line}")
    except Exception as e:
        print(f"    [SKIPPED] Yahoo observer diagnostic unavailable: {e}")

    print("\n[3] Order Execution Readiness Matrix:")
    mode_str = cfg.trading_mode.value
    is_live = (mode_str == "LIVE")
    print(f"    [*] Master Mode         : {mode_str} {'(LIVE BROKER ORDERS ENABLED)' if is_live else '(ZERO LIVE ORDERS - TEST ONLY)'}")

    stop_state = stop_mgr.get_state()
    is_stopped = stop_state.get("is_stopped", False)
    if is_stopped:
        print(f"    [STOPPED] EMERGENCY STOP ACTIVE : BLOCKED by {stop_state.get('stopped_by')} ({stop_state.get('reason')})")
    else:
        print("    [OPEN] EMERGENCY STOP        : INACTIVE (Trading gate open)")

    mkt_open, mkt_reason = market_status()
    mkt_status = "OPEN" if mkt_open else "CLOSED"
    print(f"    [*] NSE Market Session   : {mkt_status} ({mkt_reason})")
    print(f"    [*] Post-Fill Protection : SL={cfg.sl_pct}% | Target={cfg.target_pct}% (Forever OCO / Super Order)")
    print(f"    [*] Risk Sizing Limits   : Equity Budget=INR {cfg.equity_budget_inr:g} | FnO Budget=INR {cfg.fno_capital_per_trade:g}")

    print("=======================================================")
    if is_real_auth and is_live and not is_stopped:
        print(">>> STATUS: READ-ONLY BROKER CONNECTION SUCCESSFUL <<<")
        print("    (Live order readiness requires verified startup reconciliation and active protection)")
    elif not is_live:
        print(">>> STATUS: TEST MODE ACTIVE (Orders will be simulated/blocked) <<<")
    elif not is_real_auth:
        print(">>> STATUS: BLOCKED (Dhan token/auth failure - run 'bot login' or update token) <<<")
        print(">>> STATUS: BLOCKED (Emergency stop active - run 'bot resume') <<<")
    else:
        print(">>> STATUS: PARTIALLY CONFIGURED (Review warnings above) <<<")
    print("=======================================================")


def cmd_test_signal(cfg: AppConfig, raw_signal: str) -> int:
    """
    Safe SSH diagnostic signal evaluator: forces TRADING_MODE=TEST in memory so zero live
    broker orders can ever be submitted, and displays CMP, sizing, and L1-L12 latency telemetry.
    """
    from src.models import TradingMode
    from src.main import TradingBotPipeline

    if not raw_signal or not raw_signal.strip():
        print("Usage: ./bot test-signal \"Buy #IDEA\"")
        return 2

    test_cfg = cfg.model_copy(update={"trading_mode": TradingMode.TEST}) if hasattr(cfg, "model_copy") else cfg.copy(update={"trading_mode": TradingMode.TEST})
    pipeline = TradingBotPipeline(test_cfg)
    ch_id = test_cfg.monitored_channel_ids[0] if test_cfg.monitored_channel_ids else (test_cfg.test_group_id or -1001)
    now = time.time()
    print(f"[*] Processing SSH TEST-mode signal: {raw_signal!r} (CMP Policy={test_cfg.equity_cmp_execution_policy})")
    res = pipeline.process_raw_message(
        raw_signal,
        channel_id=ch_id,
        message_id=int(now * 1000) % 100000000,
        msg_timestamp=now,
        current_time=now,
        source_name="SSH_TEST_SIGNAL",
    )
    print(f"[*] Valid            : {res.is_valid}")
    print(f"[*] Action           : {res.action}")
    print(f"[*] Reason           : {res.reason}")
    if res.symbol:
        print(f"[*] Symbol           : {res.symbol} (SecID={res.security_id})")
    if res.cmp_policy:
        print(f"[*] CMP Policy       : {res.cmp_policy} ({res.reference_label})")
    if res.quote_age_status:
        print(f"[*] Quote Age        : {res.quote_age_status} | Freshness: {res.quote_freshness_status}")
    if res.limit_price is not None:
        print(f"[*] Limit Price      : INR {res.limit_price:.2f} | Qty/Lots: {res.quantity_or_lots}")
    if res.latency_breakdown:
        lb = res.latency_breakdown
        print(f"[*] Latency Breakdown: Yahoo HTTP={lb.get('yahoo_http_latency_ms')}ms | TG->CMP={lb.get('telegram_to_cmp_ms')}ms | Total={lb.get('total_processing_ms')}ms")
    return 0 if res.is_valid else 1


def cmd_setup():
    from src.setup_wizard import run_setup_wizard
    run_setup_wizard(resolve_env_path(".env"))


def cmd_web(port: int = 8080, host: str = "127.0.0.1"):
    """STEP 06: Binds setup web UI strictly to localhost (127.0.0.1) by default."""
    print(f"Starting Audit Bot Setup Web Interface on http://{host}:{port}...")
    import uvicorn
    from src.setup_app import app
    uvicorn.run(app, host=host, port=port)


def main():
    cfg = AppConfig.from_env()
    db = JournalDB(cfg.journal_db_path, account_id=cfg.dhan_client_id)
    stop_mgr = EmergencyStopManager(
        os.path.join(cfg.data_dir, "emergency_stop.json"),
        expected_account_id=cfg.dhan_client_id,
    )

    args = sys.argv[1:]
    if not args or args[0] in ("status", "--status"):
        cmd_status(cfg, db, stop_mgr)
    elif args[0] in ("dashboard", "--dashboard", "dash"):
        cmd_dashboard(cfg, db, stop_mgr)
    elif args[0] in ("mode", "--mode"):
        print(f"Current Master Switch: {cfg.trading_mode.value}")
    elif args[0] in ("check-dhan", "test-dhan", "dhan"):
        cmd_check_dhan(cfg, stop_mgr)
    elif args[0] in ("login", "refresh-token", "token", "totp"):
        ok = cmd_refresh_token(cfg, stop_mgr)
        if not ok:
            sys.exit(1)
    elif args[0] in ("daily_health_check", "daily-health-check", "health-check"):
        code = cmd_daily_health_check(cfg, db, stop_mgr)
        sys.exit(code)
    elif args[0] in ("journal", "logs"):
        limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        cmd_journal(db, limit)
    elif args[0] in ("test-signal", "test_signal"):
        sig_text = " ".join(args[1:]).strip()
        sys.exit(cmd_test_signal(cfg, sig_text))
    elif args[0] in ("emergency-stop", "stop"):
        reason = " ".join(args[1:]) if len(args) > 1 else ""
        cmd_emergency_stop(cfg, db, stop_mgr, reason)
    elif args[0] in ("resume", "start"):
        reason = " ".join(args[1:]) if len(args) > 1 else ""
        cmd_resume(cfg, db, stop_mgr, reason)
    elif args[0] in ("setup", "config", "wizard"):
        cmd_setup()
    elif args[0] in ("web", "ui"):
        port = int(args[1]) if len(args) > 1 and args[1].isdigit() else 8080
        cmd_web(port)
    elif args[0] in ("help", "--help", "-h"):
        print(
            "Usage: bot [status | dashboard | mode | check-dhan | login | daily_health_check | "
            "journal <n> | emergency-stop [reason] | resume [reason] | setup | web]"
        )
    else:
        print(f"Unknown command: {args[0]}. Run 'bot help' for usage.")
        sys.exit(2)


if __name__ == "__main__":
    main()
