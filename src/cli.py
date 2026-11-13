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
    print(f"[*] Equity Protection    : SL={cfg.equity_sl_pct}% | Target={cfg.equity_target_pct}%")
    print(f"[*] FnO Protection       : SL={cfg.fno_sl_pct}% | Target={cfg.fno_target_pct}%")
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
    print(f"Account: {_mask_account_id(cfg.dhan_client_id) if cfg.dhan_client_id else 'All'}")
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
    print(f"[*] Dhan Client ID    : {_mask_account_id(cfg.dhan_client_id)}")
    print(f"[*] Dhan PIN          : {'Configured' if cfg.dhan_pin else 'MISSING'}")
    print(f"[*] Dhan TOTP Secret  : {'Configured' if cfg.dhan_totp_secret else 'MISSING'}")

    if not cfg.dhan_client_id or not cfg.dhan_pin or not cfg.dhan_totp_secret:
        print("\n❌ Missing credentials in .env.")
        print("Please ensure DHAN_CLIENT_ID, DHAN_PIN, and DHAN_TOTP_SECRET are set.")
        return False

    client = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)
    ok, token, msg = client.generate_token_with_totp(force=True)
    if ok and token:
        # Bind the authenticated candidate returned by the coordinator, even
        # when a test double or alternative implementation does not mutate it.
        client.token = token
        # Allow a newly issued access token to become usable before reporting
        # failure. Never generate additional TOTP tokens or restore a stale one.
        # Only retry broker authentication errors, not incorrect credentials,
        # account-binding failures, rate limits or network failures.
        ok_f, bal, msg_f = False, 0.0, "Not verified"
        for delay in (0, 2, 3, 4):
            if delay:
                time.sleep(delay)
            ok_f, bal, msg_f = client.verify_token_authenticated()
            if ok_f:
                break
            auth_error = str(msg_f or "").lower()
            if not any(marker in auth_error for marker in (
                "dh-901", "auth error", "401", "invalid authentication",
            )):
                break
        if not ok_f:
            print(f"\n❌ Post-refresh broker verification failed: {msg_f}")
            print("[!] The newly generated token is retained for later verification; no unsafe rollback or extra login was attempted.")
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


def format_morning_health_report(
    cfg: AppConfig,
    *,
    now_ts: float,
    token_verified: bool,
    funds_verified: bool,
    fallback_funds: bool,
    available_balance: float,
    master_loaded: bool,
    is_stopped: bool,
    account_identity_status: str = "",
    identity_status: str = "",
    profile_client_id: str = "",
    token_expiration_str: str = "",
    token_expiration_ts: float = 0.0,
    token_expired: bool = False,
    holiday_calendar_verified=None,
) -> str:
    """
    Builds the scheduled 05:00 PM IST after-market Dhan health report (`AUDITBOT — 05:00 PM DHAN HEALTH CHECK`).
    - C3 (P0): Strictly distinguishes current authentication (`Dhan authentication: ✅ VERIFIED`) from
      next-session readiness (`Next trading session readiness: ✅ VERIFIED CHECKS` vs `⚠️ REVIEW REQUIRED`).
    - Requires a verified token expiration extending through the next trading session lifecycle close
      (`15:35 IST`) with a verified NSE holiday calendar before declaring `✅ VERIFIED CHECKS` or `✅ SUCCESS`.
    - Reports only measured statuses; never exposes raw tokens, PINs, TOTP secrets, or unmasked account IDs.
    """
    import html

    ist_tz = zoneinfo.ZoneInfo("Asia/Kolkata")
    ist = datetime.datetime.fromtimestamp(now_ts, tz=ist_tz)
    name = html.escape(str(getattr(cfg, "instance_name", "AuditBot") or "AuditBot"))
    raw_acct = str(profile_client_id or getattr(cfg, "dhan_client_id", "") or "")
    account = html.escape(_mask_account_id(raw_acct))
    configured_mode = getattr(cfg, "trading_mode", "UNKNOWN")
    mode = html.escape(str(getattr(configured_mode, "value", configured_mode)))

    raw_id_status = str(account_identity_status or identity_status or "").strip().upper()
    if not raw_id_status:
        raw_id_status = "VERIFIED" if token_verified else "UNVERIFIED"
    identity_ok = (raw_id_status == "VERIFIED")

    clean_exp = str(token_expiration_str or "").strip()
    parsed_exp_ts = float(token_expiration_ts) if (token_expiration_ts and float(token_expiration_ts) > 0) else None
    if parsed_exp_ts is None and clean_exp and clean_exp.upper() != "UNVERIFIED":
        raw_for_parse = clean_exp[:-4].strip() if clean_exp.upper().endswith(" IST") else clean_exp
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d %b %Y, %H:%M:%S",
            "%d %b %Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                naive_dt = datetime.datetime.strptime(raw_for_parse, fmt)
                parsed_exp_ts = naive_dt.replace(tzinfo=ist_tz).timestamp()
                break
            except ValueError:
                continue
        if parsed_exp_ts is None:
            try:
                dt_iso = datetime.datetime.fromisoformat(raw_for_parse)
                if dt_iso.tzinfo is None:
                    dt_iso = dt_iso.replace(tzinfo=ist_tz)
                else:
                    dt_iso = dt_iso.astimezone(ist_tz)
                parsed_exp_ts = dt_iso.timestamp()
            except ValueError:
                parsed_exp_ts = None

    effective_expired = bool(token_expired or (parsed_exp_ts is not None and parsed_exp_ts <= float(now_ts)))
    auth_ok = bool(token_verified and identity_ok and not effective_expired)

    # The calendar is authoritative only after its loader succeeds. A missing,
    # throwing, or stale file MUST NOT make next-session readiness green.
    # The optional False override can force the report unverified, but True
    # must never override a failed authoritative calendar check.
    from src.engine.market import load_holidays_verified
    holidays_set = set()
    cal_ok = False
    verified_years = set()
    try:
        loaded_ok, loaded_holidays, _ = load_holidays_verified(
            required_year=ist.year
        )
        if loaded_ok and loaded_holidays:
            holidays_set = set(loaded_holidays)
            verified_years.add(ist.year)
            cal_ok = True
    except (OSError, ValueError, TypeError, RuntimeError):
        cal_ok = False
    except Exception:
        # Defensive fail closed for unexpected loader failures as well.
        cal_ok = False
    if holiday_calendar_verified is False:
        cal_ok = False

    cur_date = ist.date()
    session_close_time = datetime.time(15, 35, 0)
    session_open_time = datetime.time(9, 15, 0)
    # The next session can fall in a different calendar year. Require that
    # year's verified holiday data before declaring readiness (Dec/Jan case).
    next_session_date = cur_date
    if ist.time() >= session_close_time:
        next_session_date += datetime.timedelta(days=1)
    for _ in range(370):
        if next_session_date.weekday() >= 5:
            next_session_date += datetime.timedelta(days=1)
            continue
        if next_session_date.year not in verified_years and cal_ok:
            try:
                year_ok, year_holidays, _ = load_holidays_verified(
                    required_year=next_session_date.year
                )
                if year_ok and year_holidays:
                    holidays_set.update(year_holidays)
                    verified_years.add(next_session_date.year)
                else:
                    cal_ok = False
            except Exception:
                cal_ok = False
        if not cal_ok:
            # We may display an indicative weekday only. It is NOT a verified
            # exchange session and cannot authorize a green status.
            break
        if next_session_date not in holidays_set:
            break
        next_session_date += datetime.timedelta(days=1)
    else:
        cal_ok = False

    next_open_dt = datetime.datetime.combine(next_session_date, session_open_time, tzinfo=ist_tz)
    next_close_dt = datetime.datetime.combine(next_session_date, session_close_time, tzinfo=ist_tz)
    next_open_ts = next_open_dt.timestamp()
    next_close_ts = next_close_dt.timestamp()
    next_open_str = next_open_dt.strftime("%a %d %b %H:%M IST")
    next_close_str = next_close_dt.strftime("%a %d %b %H:%M IST")

    next_session_expiry_ok = False
    if effective_expired:
        expiry_line = f"❌ EXPIRED (<code>{html.escape(clean_exp or 'expired')}</code>)"
    elif not auth_ok or not clean_exp or clean_exp.upper() == "UNVERIFIED" or parsed_exp_ts is None:
        expiry_line = "⚠️ UNVERIFIED"
    elif not cal_ok:
        expiry_line = f"⚠️ <code>{html.escape(clean_exp)}</code> (Holiday calendar UNVERIFIED)"
    elif parsed_exp_ts < next_open_ts:
        expiry_line = (
            f"⚠️ <code>{html.escape(clean_exp)}</code> "
            f"(Expires before next session open {next_open_str}; scheduled 17:00 IST refresh required)"
        )
    elif parsed_exp_ts < next_close_ts:
        expiry_line = (
            f"⚠️ <code>{html.escape(clean_exp)}</code> "
            f"(Expires during next session before {next_close_str})"
        )
    else:
        expiry_line = f"✅ <code>{html.escape(clean_exp)}</code> (Covers next session through {next_close_str})"
        next_session_expiry_ok = True

    ready = bool(
        auth_ok
        and cal_ok
        and next_session_expiry_ok
        and funds_verified
        and not fallback_funds
        and master_loaded
        and not is_stopped
    )
    token_line = "✅ VERIFIED" if auth_ok else "❌ FAILED"
    if raw_id_status == "VERIFIED" and auth_ok:
        identity_line = f"✅ VERIFIED (<code>{account}</code>)"
    elif raw_id_status.startswith("MISMATCH"):
        identity_line = f"❌ MISMATCH (<code>{account}</code>)"
    elif effective_expired or raw_id_status == "EXPIRED":
        identity_line = f"❌ EXPIRED (<code>{account}</code>)"
    else:
        identity_line = f"❌ UNVERIFIED (<code>{account}</code>)"

    if funds_verified and not fallback_funds:
        funds_line = f"✅ VERIFIED — ₹{available_balance:,.2f}"
    elif fallback_funds:
        funds_line = "⚠️ TEST FALLBACK — NOT VERIFIED WITH DHAN"
    else:
        funds_line = "❌ UNVERIFIED"

    master_line = "✅ OK" if master_loaded else "⚠️ WARNING (UNAVAILABLE / STALE)"
    stop_line = "🔴 ACTIVE — NEW ENTRIES BLOCKED" if is_stopped else "✅ INACTIVE"
    readiness_line = "✅ VERIFIED CHECKS" if ready else "⚠️ REVIEW REQUIRED"
    overall_line = "✅ SUCCESS" if ready else "❌ ACTION REQUIRED"
    return "\n".join((
        "🕔 <b>AUDITBOT — 05:00 PM DHAN HEALTH CHECK</b>",
        f"🤖 Instance: <b>{name}</b> | 🕒 {ist:%d %b %Y, %H:%M:%S} IST",
        f"🔑 Dhan authentication: {token_line}",
        f"💳 Dhan account identity: {identity_line}",
        f"⏳ Token expiration: {expiry_line}",
        f"💰 Broker funds: {funds_line}",
        f"📚 Scrip master: {master_line}",
        f"⚙️ Trading mode: <b>{mode}</b>",
        f"🛡️ Emergency STOP: {stop_line}",
        f"🚦 Next trading session readiness: {readiness_line}",
        f"📊 <b>Overall result: {overall_line}</b>",
    ))


format_after_market_health_report = format_morning_health_report


def dispatch_morning_health_report_once(
    cfg: AppConfig, db: JournalDB, report: str, *, now_ts: float
) -> str:
    """Send once per account/IST date to the configured Telegram log group, plus one recovery notice.

    C4 (P1): Preserves at most one routine initial daily report (`DAILY_HEALTH_1700_REPORT`),
    records `DAILY_HEALTH_1700_FAILED` when the initial report indicates failure/review-required,
    and allows one separately deduplicated recovery notification (`DAILY_HEALTH_1700_RECOVERY`)
    if a subsequent run on the same IST date transitions to healthy/ready (`SUCCESS` / `RECOVERED`).
    Never labels a queued notification as delivered.
    """
    if not getattr(cfg, "log_group_id", 0):
        return "NOT_CONFIGURED (LOG_GROUP_ID is missing)"
    from src.broker.dhan_token import locked_token_file
    from src.notifier.telegram_notifier import TelegramNotifier

    ist_date = datetime.datetime.fromtimestamp(
        now_ts, tz=zoneinfo.ZoneInfo("Asia/Kolkata")
    ).strftime("%Y-%m-%d")
    identity = f"DAILY_HEALTH:{ist_date}:{cfg.dhan_client_id}:{getattr(cfg, 'instance_name', 'AuditBot')}"

    text_up = str(report or "").upper()
    is_ready_report = (
        "OVERALL RESULT: ✅ SUCCESS" in text_up
        or "✅ CHECKS PASSED" in text_up
    ) and not any(marker in text_up for marker in (
        "OVERALL RESULT: ❌", "NEXT TRADING SESSION READINESS: ⚠️",
        "OVERALL RESULT: ⚠️",
    ))
    # An ambiguous or malformed report is never silently treated as READY.
    is_fail_report = not is_ready_report

    # Separate lock from the Dhan token lock; never hold the token lock across
    # Telegram network I/O. The DB claim is unique across service restarts.
    lock_path = os.path.join(cfg.data_dir, ".morning_health_report")
    try:
        with locked_token_file(lock_path):
            with db._get_conn() as conn:
                has_initial = bool(
                    conn.execute(
                        "SELECT 1 FROM order_notification_events WHERE order_id=? AND event_key=?",
                        (identity, "DAILY_HEALTH_1700_REPORT"),
                    ).fetchone()
                )
                has_failed = bool(
                    conn.execute(
                        "SELECT 1 FROM order_notification_events WHERE order_id=? AND event_key=?",
                        (identity, "DAILY_HEALTH_1700_FAILED"),
                    ).fetchone()
                )
                has_recovery = bool(
                    conn.execute(
                        "SELECT 1 FROM order_notification_events WHERE order_id=? AND event_key=?",
                        (identity, "DAILY_HEALTH_1700_RECOVERY"),
                    ).fetchone()
                )
                has_deterioration = bool(
                    conn.execute(
                        "SELECT 1 FROM order_notification_events WHERE order_id=? AND event_key=?",
                        (identity, "DAILY_HEALTH_1700_DETERIORATION"),
                    ).fetchone()
                )

            if not has_initial:
                event_key = "DAILY_HEALTH_1700_REPORT"
                state_key = (
                    "DAILY_HEALTH_1700_FAILED"
                    if is_fail_report
                    else ("DAILY_HEALTH_1700_READY" if is_ready_report else "")
                )
            elif has_failed and is_ready_report and not has_recovery:
                event_key = "DAILY_HEALTH_1700_RECOVERY"
                state_key = "DAILY_HEALTH_1700_READY"
            elif is_fail_report and not has_deterioration and (
                not has_failed or has_recovery
            ):
                # A green 17:00 report must never hide a new 17:10 failure.
                # One deterioration and one recovery per IST date bound flips.
                event_key = "DAILY_HEALTH_1700_DETERIORATION"
                state_key = "DAILY_HEALTH_1700_FAILED"
            else:
                return "ALREADY SENT OR QUEUED"

            notifier = TelegramNotifier(cfg)
            if notifier.enabled and notifier.send_log_message(report):
                with db._get_conn() as conn:
                    conn.execute(
                        "INSERT OR IGNORE INTO order_notification_events (order_id, event_key, created_at) VALUES (?, ?, ?)",
                        (identity, event_key, float(now_ts)),
                    )
                    if state_key:
                        conn.execute(
                            "INSERT OR IGNORE INTO order_notification_events (order_id, event_key, created_at) VALUES (?, ?, ?)",
                            (identity, state_key, float(now_ts)),
                        )
                return "DELIVERED VIA TELEGRAM BOT API"

            # Existing outbox worker delivers using Telegram Bot API or, if the
            # bot token is absent, its Telegram user session (when running).
            queued = db.enqueue_order_notification_once(identity, event_key, report)
            if queued and state_key:
                try:
                    with db._get_conn() as conn:
                        conn.execute(
                            "INSERT OR IGNORE INTO order_notification_events (order_id, event_key, created_at) VALUES (?, ?, ?)",
                            (identity, state_key, float(now_ts)),
                        )
                except Exception:
                    pass
            return "QUEUED FOR TELEGRAM RETRY" if queued else "ALREADY SENT OR QUEUED"
    except Exception as exc:
        # Notification failure must never change token validation or trading mode.
        return f"NOTIFICATION ERROR ({type(exc).__name__})"


dispatch_after_market_health_report_once = dispatch_morning_health_report_once


def cmd_daily_health_check(cfg: AppConfig, db: JournalDB, stop_mgr: EmergencyStopManager) -> int:
    """
    V12-24 & C7 (P1): Executes the scheduled 17:00 IST (05:00 PM IST) daily token refresh and
    after-market readiness check invoked by `dhan-token-refresh.service`.

    Exit-Code Contract:
    - Separates `AUTH_CHECK` (`PASS` / `FAIL`: TOTP refresh, `/v2/profile` account identity,
      non-expired token, and verified real `/v2/fundlimit` balance) from
      `NEXT_SESSION_READINESS` (`VERIFIED_CHECKS` / `REVIEW_REQUIRED`: `AUTH_CHECK == PASS`
      plus verified token expiration covering the next trading session close at 15:35 IST
      with a verified holiday calendar, fresh scrip master, and inactive Emergency STOP).
    - Returns exit code `0` ONLY when both `AUTH_CHECK == PASS` and
      `NEXT_SESSION_READINESS == VERIFIED_CHECKS`. Returns `1` whenever any mandatory
      authentication or next-session readiness check fails or requires operator review.
    """
    from src.broker.dhan_client import DhanClient
    from src.instruments.resolver import InstrumentResolver

    print("=======================================================")
    print("     AUDIT BOT 05:00 PM IST DHAN HEALTH CHECK          ")
    print("=======================================================")

    tok_ok = cmd_refresh_token(cfg, stop_mgr)
    # The scheduled refresh already attempted the single intended TOTP login.
    # Do not implicitly generate another token while gathering the report.
    client = DhanClient(cfg, stop_manager=stop_mgr, auto_generate_token=False)
    ok_funds, avail_bal, funds_msg = client.get_fund_limits(retry=False)

    report_now = time.time()
    identity_status = "UNVERIFIED"
    identity_ok = False
    profile_client_id = ""
    token_expiration_str = ""
    token_expiration_ts = 0.0
    token_expired = False
    verify_fn = getattr(client, "verify_profile_and_token_expiry", None)
    if callable(verify_fn):
        try:
            prof_info = verify_fn(allow_refresh=False, now_ts=report_now)
            if isinstance(prof_info, dict):
                raw_id_st = str(prof_info.get("identity_status") or "").strip().upper()
                profile_client_id = str(prof_info.get("profile_client_id") or "").strip()
                if raw_id_st == "MISMATCH" or prof_info.get("identity_mismatch"):
                    identity_status = "MISMATCH"
                    identity_ok = False
                elif raw_id_st == "VERIFIED" and bool(prof_info.get("identity_verified", False)) and tok_ok:
                    identity_status = "VERIFIED"
                    identity_ok = True
                elif raw_id_st == "EXPIRED" or prof_info.get("token_expired"):
                    identity_status = "EXPIRED"
                    identity_ok = False
                else:
                    identity_status = "UNVERIFIED"
                    identity_ok = False
                token_expiration_str = str(prof_info.get("token_expiration_str") or "").strip()
                raw_exp_ts = prof_info.get("token_expiration_ts")
                token_expiration_ts = float(raw_exp_ts) if (raw_exp_ts is not None and float(raw_exp_ts) > 0) else 0.0
                token_expired = bool(prof_info.get("token_expired", False))
        except Exception:
            identity_status = "UNVERIFIED"
            identity_ok = False

    scrip_path = os.path.join(cfg.data_dir, "scrip_master.csv")
    resolver = InstrumentResolver()
    master_ok = resolver.load_dhan_master(
        scrip_path,
        max_cache_age_sec=cfg.scrip_master_max_age_sec,
        allow_network_download=True,
    )

    fallback_funds = bool(
        getattr(client, "last_funds_is_fallback", False)
        or "fallback" in str(funds_msg).lower()
        or "test mode default" in str(funds_msg).lower()
    )
    funds_verified = bool(ok_funds and not fallback_funds)
    stop_state = (
        stop_mgr.get_state()
        if callable(getattr(stop_mgr, "get_state", None))
        else {"is_stopped": True}
    )
    is_stopped = bool(stop_state.get("is_stopped", True))
    auth_verified = bool(tok_ok and identity_ok and not token_expired)
    report = format_morning_health_report(
        cfg,
        now_ts=report_now,
        token_verified=auth_verified,
        funds_verified=funds_verified,
        fallback_funds=fallback_funds,
        available_balance=avail_bal,
        master_loaded=master_ok,
        is_stopped=is_stopped,
        account_identity_status=identity_status,
        profile_client_id=profile_client_id,
        token_expiration_str=token_expiration_str,
        token_expiration_ts=token_expiration_ts,
        token_expired=token_expired,
    )
    notify_status = dispatch_morning_health_report_once(
        cfg, db, report, now_ts=report_now
    )
    auth_check_ok = bool(auth_verified and funds_verified)
    next_session_ready = (
        "Next trading session readiness: ✅ VERIFIED CHECKS" in report
        and "Overall result: ✅ SUCCESS" in report
    )
    auth_check_label = "PASS" if auth_check_ok else "FAIL"
    readiness_label = "VERIFIED_CHECKS" if next_session_ready else "REVIEW_REQUIRED"
    overall_ok = bool(auth_check_ok and next_session_ready)
    summary = (
        f"AUTH_CHECK={auth_check_label}, "
        f"NEXT_SESSION_READINESS={readiness_label}, "
        f"token_refresh={'OK' if auth_verified else 'FAIL'}, "
        f"account_identity={identity_status}, "
        f"token_expiration={token_expiration_str or 'UNVERIFIED'}, "
        f"funds={'OK' if funds_verified else 'UNVERIFIED'} ({avail_bal:,.2f} INR: {funds_msg}), "
        f"scrip_master={'OK' if master_ok else 'WARN'}, "
        f"emergency_stop={'ACTIVE' if is_stopped else 'INACTIVE'}, "
        f"telegram_report={notify_status}"
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
    print(f"[*] Client ID           : {_mask_account_id(cfg.dhan_client_id) if cfg.dhan_client_id else 'NOT CONFIGURED'}")
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
    print(f"    [*] Equity Post-Fill     : SL={cfg.equity_sl_pct}% | Target={cfg.equity_target_pct}% (Forever OCO)")
    print(f"    [*] FnO Post-Fill        : SL={cfg.fno_sl_pct}% | Target={cfg.fno_target_pct}% (Forever OCO / Super Order)")
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


def cmd_rules(db: JournalDB, sub_args: list) -> int:
    from src.parsers.adaptive_rules import AdaptiveRuleManager
    mgr = AdaptiveRuleManager(db)
    sub = (sub_args[0].lower() if sub_args else "list")
    if sub in ("list", "ls"):
        rules = db.list_parser_rules()
        print("=======================================================")
        print("          AUDIT BOT ADAPTIVE PARSER RULES              ")
        print("=======================================================")
        if not rules:
            print("  No adaptive parser rules registered.")
        for r in rules:
            print(f"  [{r['state']}] {r['rule_id']} (v{r['version']}, route={r['route']}) — {r['pattern']}")
        print("=======================================================")
        return 0
    elif sub == "versions" and len(sub_args) >= 2:
        rule_id = sub_args[1]
        vers = db.get_parser_rule_versions(rule_id)
        for v in vers:
            print(f"  {v['rule_id']} v{v['version']} [{v['state']}] approved_by={v['approved_by']!r} pattern={v['pattern']}")
        return 0
    elif sub == "approve" and len(sub_args) >= 2:
        rule_id = sub_args[1]
        reviewer = sub_args[2] if len(sub_args) >= 3 else "Udit"
        notes = " ".join(sub_args[3:]) if len(sub_args) >= 4 else ""
        ok, msg = mgr.approve_rule(rule_id, reviewer=reviewer, notes=notes)
        print(f"{'✅' if ok else '❌'} {msg}")
        return 0 if ok else 1
    elif sub == "activate" and len(sub_args) >= 2:
        rule_id = sub_args[1]
        actor = sub_args[2] if len(sub_args) >= 3 else "Udit"
        ok, msg = mgr.activate_rule(rule_id, actor=actor)
        print(f"{'✅' if ok else '❌'} {msg}")
        return 0 if ok else 1
    elif sub in ("deactivate", "rollback") and len(sub_args) >= 2:
        rule_id = sub_args[1]
        actor = sub_args[2] if len(sub_args) >= 3 else "Udit"
        reason = " ".join(sub_args[3:]) if len(sub_args) >= 4 else "Operator CLI deactivation"
        ok, msg = mgr.deactivate_rule(rule_id, actor=actor, reason=reason)
        print(f"{'✅' if ok else '❌'} {msg}")
        return 0 if ok else 1
    elif sub == "restore" and len(sub_args) >= 3 and sub_args[2].isdigit():
        rule_id = sub_args[1]
        ver_num = int(sub_args[2])
        actor = sub_args[3] if len(sub_args) >= 4 else "Udit"
        ok, msg, _ = mgr.restore_rule_version(rule_id, target_version=ver_num, actor=actor, activate=False)
        print(f"{'✅' if ok else '❌'} {msg}")
        if ok:
            print("Rule restored in APPROVED state only; run `bot rules activate <ID> <actor>` separately to enable it.")
        return 0 if ok else 1
    else:
        print("Usage: bot rules [list | versions <ID> | approve <ID> [reviewer] | activate <ID> [actor] | deactivate <ID> [actor] [reason] | restore <ID> <version> [actor]]")
        return 2


def cmd_service_control(cfg: AppConfig, action: str, args: list) -> int:
    """B10 & B12: Owner-authorized service start/stop/restart/status command with safety and control-state tracking."""
    from src.engine.config_manager import ConfigChangeTracker
    tracker = ConfigChangeTracker(data_dir=cfg.data_dir)
    confirm_live = ("--yes" in args or "-y" in args)
    clean_args = [a for a in args if a not in ("--yes", "-y")]
    reason = " ".join(clean_args).strip() or f"Operator CLI {action}"

    if action in ("start", "restart") and cfg.trading_mode.value == "LIVE" and not confirm_live:
        ans = input(f"You are about to {action} AuditBot in LIVE mode. Real Dhan orders may be placed. Continue? (yes/no): ").strip().lower()
        if ans not in ("yes", "y"):
            print(f"Aborted LIVE service {action}.")
            return 1
        confirm_live = True

    env_p = resolve_env_path(".env")
    ro_broker = tracker.get_readonly_broker(env_path=env_p) if action != "status" else None
    res = tracker.control_service(
        action=action,
        reason=reason,
        confirm_live=confirm_live,
        env_path=env_p,
        stop_state_path=os.path.join(cfg.data_dir, "emergency_stop.json"),
        db_path=cfg.journal_db_path,
        broker=ro_broker,
    )
    msg = res.get("message") or res.get("error") or f"Service {action} completed"
    if res.get("ok"):
        print(f"✅ {msg} ({res.get('service_state_label', res.get('service_state'))})")
        return 0
    if res.get("deferred"):
        print(f"⏳ {msg}")
        return 0
    print(f"❌ {msg}")
    return 1


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
    elif args[0] in ("service-stop", "bot-stop"):
        sys.exit(cmd_service_control(cfg, "stop", args[1:]))
    elif args[0] in ("service-start", "bot-start"):
        sys.exit(cmd_service_control(cfg, "start", args[1:]))
    elif args[0] in ("service-restart", "bot-restart"):
        sys.exit(cmd_service_control(cfg, "restart", args[1:]))
    elif args[0] in ("service-status", "bot-status"):
        sys.exit(cmd_service_control(cfg, "status", args[1:]))
    elif args[0] in ("emergency-stop", "stop"):
        reason = " ".join(args[1:]) if len(args) > 1 else ""
        cmd_emergency_stop(cfg, db, stop_mgr, reason)
    elif args[0] in ("resume", "start"):
        reason = " ".join(args[1:]) if len(args) > 1 else ""
        cmd_resume(cfg, db, stop_mgr, reason)
    elif args[0] in ("setup", "config", "wizard"):
        cmd_setup()
    elif args[0] in ("rules", "rule"):
        sys.exit(cmd_rules(db, args[1:]))
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
