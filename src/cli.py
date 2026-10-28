"""
SSH Terminal Operator Dashboard CLI ('bot' command).
Provides lightweight, secure operational controls over SSH without web dependencies.
Includes interactive setup wizard and setup web server triggers.
"""

import sys
import os
import json
from src.config import AppConfig
from src.journal.db import JournalDB


def print_banner():
    print("=======================================================")
    print("      UDIT'S TELEGRAM-TO-DHAN BOT (SINGLE-ACCOUNT)     ")
    print("=======================================================")


def cmd_status(cfg: AppConfig, db: JournalDB):
    print_banner()
    print(f"[*] Operating Master Switch : {cfg.trading_mode.value}")
    print(f"[*] Equity Trading Active  : {'YES' if cfg.equity_active else 'NO'}")
    print(f"[*] Stock Options Active   : {'YES' if cfg.fno_active else 'NO'}")
    print(f"[*] Dhan Client ID         : {cfg.dhan_client_id or 'Not Configured'}")
    print(f"[*] Token File Location    : {cfg.dhan_token_file}")
    token_exists = os.path.exists(cfg.dhan_token_file) and os.path.getsize(cfg.dhan_token_file) > 0
    print(f"[*] Token File Present     : {'YES' if token_exists else 'NO'}")
    print(f"[*] Monitored Channels     : {len(cfg.monitored_channel_ids)} configured")
    print(f"[*] Log Channel ID         : {cfg.log_group_id or 'Not Configured'}")
    print("-------------------------------------------------------")
    print(f"[*] Equity Sizing Mode     : {cfg.equity_sizing_mode} (Budget={cfg.equity_budget_inr:g} INR)")
    print(f"[*] FnO Sizing Mode        : {cfg.fno_sizing_mode} (Budget={cfg.fno_capital_per_trade:g} INR, Max Lots={cfg.fno_max_lots})")
    print(f"[*] Protection (Post-Fill) : SL={cfg.sl_pct}% | Target={cfg.target_pct}%")
    print(f"[*] Risk Limits            : Max Order={cfg.max_order_amount_inr:g} | Min Funds={cfg.min_trade_funds_inr:g}")
    print(f"[*] Timing Thresholds      : Quote Staleness={cfg.max_quote_age_sec}s | Retry Timeout={cfg.partial_fill_retry_timeout_sec}s")
    print(f"[*] SQLite Journal Path    : {cfg.journal_db_path}")
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


def cmd_emergency_stop(db: JournalDB):
    db.record_system_event("EMERGENCY_STOP", "Operator triggered emergency stop via CLI")
    print(">>> EMERGENCY STOP ACTIVATED <<<")
    print("All new trade entries are BLOCKED.")
    print("Pending entry orders flagged for cancellation.")


def cmd_resume(db: JournalDB):
    db.record_system_event("RESUME_TRADING", "Operator resumed trading via CLI")
    print(">>> TRADING OPERATIONS RESUMED <<<")


def cmd_setup():
    from src.setup_wizard import run_setup_wizard
    run_setup_wizard(".env")


def cmd_web(port: int = 8080):
    print(f"Starting Sapphire Bot Setup Web Interface on http://127.0.0.1:{port}...")
    import uvicorn
    from src.setup_app import app
    uvicorn.run(app, host="0.0.0.0", port=port)


def main():
    cfg = AppConfig.from_env()
    db = JournalDB(cfg.journal_db_path)

    args = sys.argv[1:]
    if not args or args[0] in ("status", "--status"):
        cmd_status(cfg, db)
    elif args[0] in ("mode", "--mode"):
        print(f"Current Master Switch: {cfg.trading_mode.value}")
    elif args[0] in ("journal", "logs"):
        limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
        cmd_journal(db, limit)
    elif args[0] in ("emergency-stop", "stop"):
        cmd_emergency_stop(db)
    elif args[0] in ("resume", "start"):
        cmd_resume(db)
    elif args[0] in ("setup", "config", "wizard"):
        cmd_setup()
    elif args[0] in ("web", "ui"):
        port = int(args[1]) if len(args) > 1 and args[1].isdigit() else 8080
        cmd_web(port)
    elif args[0] in ("help", "--help", "-h"):
        print("Usage: bot [status | mode | journal <n> | emergency-stop | resume | setup | web]")
    else:
        print(f"Unknown command: {args[0]}. Run 'bot help' for usage.")


if __name__ == "__main__":
    main()
