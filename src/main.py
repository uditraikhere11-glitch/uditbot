"""
Main Application Pipeline Orchestrator — V13 Audit-Hardened Execution-Safety Edition.
Processes raw Telegram messages through deterministic validation, deduplication,
sizing, risk limits, market hours validation, and broker execution (strictly disabled in TEST mode).
Enforces:
- Section A & V12-15: Yahoo Finance primary price tunnel with <=2.0s UTC source quote timestamp validation
  measured against live post-HTTP time, and re-validated immediately before broker order submission.
- V12-01 & V12-25: Hermetic offline scrip master loading (`allow_network_download=os.path.exists(scrip_cache)`).
- V12-02 & Section D: Persists complete option/route metadata in `order_intents` and `ProtectionWatcher.track()`.
- V12-06 & V12-14: Uses `is_lifecycle_open` for watcher monitoring and offloads synchronous handler I/O via `asyncio.to_thread`.
- V12-08: Synchronizes `RiskManager` portfolio state (`sync_from_journal_and_broker`) before validation and
  records committed orders (`record_order_committed`) immediately upon acceptance.
- V12-13 & Section E: Classifies HTTP 500/502/503/504/408 and network timeouts as `UNKNOWN`, halts
  `reconciliation_gate` on `UNKNOWN` outcomes, and never resubmits ambiguous orders.
- Safety Corrections A, B, C & Section 6: Connects `RiskManager` and configured `partial_fill_retry_timeout_sec`
  into `ProtectionWatcher` for production retry lifecycle and authoritative broker leg confirmation.
"""

from datetime import date, datetime
import logging
import os
import threading
import time
from typing import Optional
from src.config import AppConfig
from src.models import (
    TradingMode, ProductType, OrderType, TransactionType,
    SignalAction, SignalType, OrderRequest, ValidationResult
)
from src.parsers.equity_parser import parse_equity_regex, filter_equity_message
from src.parsers.fno_parser import parse_fno_regex, looks_like_option
from src.parsers.gemini_fallback import parse_fno_gemini_sync, parse_equity_gemini_sync
from src.parsers.duplicate_filter import DuplicateFilter
from src.instruments.resolver import InstrumentResolver
from src.engine.sizer import (
    size_equity, size_option, calculate_equity_limit_price,
    calculate_option_limit_price, calculate_protection_levels
)
from src.engine.market import is_market_open, is_lifecycle_open, market_status
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.risk_manager import RiskManager
from src.journal.db import JournalDB, generate_correlation_id
from src.broker.dhan_client import DhanClient
from src.notifier.formatter import format_trade_log, format_exit_log
from src.notifier.telegram_notifier import TelegramNotifier
from src.engine.reconciliation import StartupReconciliationGate

logger = logging.getLogger("trading_bot")


def _is_transport_uncertainty(msg: str) -> bool:
    """
    V12-13: Detects HTTP 5xx (500/502/503/504), 408, timeouts, auth ambiguity, or network
    exceptions whose broker outcome is UNKNOWN.
    """
    m = (msg or "").lower()
    return any(
        k in m
        for k in (
            "timeout",
            "timed out",
            "connection",
            "http exception",
            "ambiguous",
            "outcome unknown",
            "unknown",
            "server error",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
            "(500)",
            "(502)",
            "(503)",
            "(504)",
            "504",
            "503",
            "502",
            "500",
            "408",
        )
    )


class TradingBotPipeline:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self._pipeline_lock = threading.Lock()
        self.db = JournalDB(config.journal_db_path, account_id=config.dhan_client_id)
        self.dedup = DuplicateFilter(window_sec=config.dedup_window_sec)
        self.resolver = InstrumentResolver(
            allow_next_month_expiry_fallback=getattr(config, "allow_next_month_expiry_fallback", True)
        )
        scrip_cache = os.path.join(config.data_dir, "scrip_master.csv")
        self.resolver.load_dhan_master(
            scrip_cache,
            max_cache_age_sec=getattr(config, "scrip_master_max_age_sec", 86400.0),
            allow_network_download=os.path.exists(scrip_cache),
        )
        self.stop_manager = EmergencyStopManager(
            state_file=os.path.join(config.data_dir, "emergency_stop.json"),
            expected_account_id=config.dhan_client_id,
        )
        self.risk_manager = RiskManager(config, stop_manager=self.stop_manager)
        self.broker = DhanClient(config, stop_manager=self.stop_manager)
        self.notifier = TelegramNotifier(config)
        self.watcher = ProtectionWatcher(
            broker=self.broker,
            sl_pct=config.sl_pct,
            target_pct=config.target_pct,
            sl_limit_buffer_pct=config.sl_limit_buffer_pct,
            path=os.path.join(config.data_dir, "pending_protection.json"),
            dry_run=(config.trading_mode == TradingMode.TEST),
            journal_db=self.db,
            order_timeout_sec=config.order_timeout_sec,
            retry_timeout_sec=getattr(config, "partial_fill_retry_timeout_sec", 2.0),
            risk_manager=self.risk_manager,
        )
        self.reconciliation_gate = StartupReconciliationGate(
            broker=self.broker,
            journal_db=self.db,
            watcher=self.watcher,
            stop_manager=self.stop_manager,
        )
        self.broker.reconciliation_gate = self.reconciliation_gate

    def reload_config(self, env_file: str = ".env") -> AppConfig:
        """V12-23: Reloads configuration from .env into the active pipeline."""
        with self._pipeline_lock:
            self.cfg = AppConfig.from_env(env_file=env_file)
            self.risk_manager.cfg = self.cfg
            self.broker.cfg = self.cfg
            self.notifier = TelegramNotifier(self.cfg)
            self.watcher.sl_pct = self.cfg.sl_pct
            self.watcher.target_pct = self.cfg.target_pct
            self.watcher.sl_limit_buffer_pct = self.cfg.sl_limit_buffer_pct
            self.watcher.order_timeout_sec = self.cfg.order_timeout_sec
            self.watcher.retry_timeout_sec = getattr(self.cfg, "partial_fill_retry_timeout_sec", 2.0)
            return self.cfg

    def dispatch_notification(self, event_type: str, card: str, priority: str = "NORMAL") -> bool:
        """Dispatches notification via durable SQLite outbox to decouple Telegram latency from broker execution."""
        payload = {
            "text": card,
            "event_type": event_type,
            "priority": priority,
            "timestamp": time.time(),
        }
        if hasattr(self, "db") and self.db:
            try:
                self.db.enqueue_outbox(event_type, payload)
            except Exception as e:
                logger.error(f"[Outbox] Could not enqueue notification: {e}")
            if getattr(self.cfg, "trading_mode", None) == TradingMode.TEST:
                if hasattr(self, "notifier") and self.notifier and self.notifier.enabled:
                    self.notifier.send_log_message(card)
            return True
        elif hasattr(self, "notifier") and self.notifier:
            return self.notifier.send_log_message(card)
        return False

    def process_raw_message(
        self,
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        msg_timestamp: float = 0.0,
        current_time: Optional[float] = None,
        source_name: str = "Test group",
    ) -> ValidationResult:
        with self._pipeline_lock:
            injected_clock = current_time
            now = injected_clock if injected_clock is not None else time.time()
            msg_ts = msg_timestamp or now

            allowed_channels = set(self.cfg.monitored_channel_ids or [])
            # In TEST mode, test_group_id is permitted for dry-run testing.
            # In LIVE mode, test_group_id is strictly ignored to prevent test signals from executing live orders.
            if self.cfg.trading_mode != TradingMode.LIVE and self.cfg.test_group_id:
                allowed_channels.add(self.cfg.test_group_id)

            if getattr(self.cfg, "enforce_source_allowlist", False) and not allowed_channels:
                reason = "Source allowlist is empty (MONITORED_CHANNEL_IDS not configured)"
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

            if self.cfg.trading_mode == TradingMode.LIVE and self.cfg.test_group_id and channel_id == self.cfg.test_group_id:
                reason = f"Ignored test group signal in LIVE mode ({channel_id} is TEST_GROUP_ID; LIVE mode restricted strictly to monitored channels)"
                self.db.record_signal(channel_id, message_id, "UNKNOWN", "", "BLOCKED", text, "IGNORED_TEST_GROUP", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

            if allowed_channels and channel_id != 0 and channel_id not in allowed_channels:
                reason = f"Unauthorized source channel ({channel_id} not in allowlist)"
                self.db.record_signal(channel_id, message_id, "UNKNOWN", "", "BLOCKED", text, "REJECTED_SOURCE", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

            if channel_id and message_id:
                try:
                    is_new_claim, claim_status, claim_id = self.db.claim_signal(
                        channel_id=channel_id,
                        message_id=message_id,
                        raw_text=text,
                    )
                except Exception as e:
                    reason = f"Journal DB claim error (failing closed): {e}"
                    logger.critical(f"[JOURNAL FAIL-CLOSED] {reason}")
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

                if not is_new_claim:
                    if claim_status == "MESSAGE_EDITED_EVENT":
                        return ValidationResult(
                            is_valid=False,
                            action=SignalAction.IGNORED,
                            reason="Message edited; revision logged without duplicate trade.",
                        )
                    elif claim_status == "DUPLICATE_MESSAGE_CLAIM":
                        return ValidationResult(
                            is_valid=False,
                            action=SignalAction.BLOCKED,
                            reason="Duplicate source message already claimed.",
                        )
            is_live = (self.cfg.trading_mode == TradingMode.LIVE)

            if (now - msg_ts) > self.cfg.max_message_age_sec:
                age = now - msg_ts
                reason = f"Message too old ({age:.1f}s > {self.cfg.max_message_age_sec}s)"
                self.db.record_signal(channel_id, message_id, "UNKNOWN", "", "BLOCKED", text, "REJECTED", reason)

                log_card = format_trade_log(
                    symbol="UNKNOWN",
                    source_name=source_name,
                    original_message=text,
                    segment="UNKNOWN",
                    status=f"Rejected: {reason}",
                    dhan_order_details="None (Blocked by message age filter)",
                    heads_up=reason,
                    is_live=is_live,
                    is_processed=False,
                    timestamp=now,
                )
                self.dispatch_notification("TRADE_LOG", log_card)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

            if looks_like_option(text):
                fno_sig, fno_reason, fno_action = parse_fno_regex(
                    text, channel_id=channel_id, message_id=message_id,
                    msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec,
                    current_time=now,
                )

                if fno_action == SignalAction.EXIT_SIGNAL:
                    self.db.record_signal(channel_id, message_id, "OPTION", "", "EXIT_SIGNAL", text, "LOGGED_NO_ACTION", fno_reason)
                    exit_card = format_exit_log(
                        symbol="OPTION",
                        source_name=source_name,
                        original_message=text,
                        segment="FNO",
                        reason=fno_reason,
                        is_live=is_live,
                        timestamp=now,
                    )
                    self.dispatch_notification("TRADE_LOG", exit_card)
                    return ValidationResult(is_valid=False, action=SignalAction.EXIT_SIGNAL, reason=fno_reason, signal_type=SignalType.OPTION, log_card=exit_card)

                if fno_action == SignalAction.BLOCKED:
                    self.db.record_signal(channel_id, message_id, "OPTION", "", "BLOCKED", text, "REJECTED", fno_reason)
                    rej_card = format_trade_log(
                        symbol="OPTION",
                        source_name=source_name,
                        original_message=text,
                        segment="FNO",
                        status=f"Rejected: {fno_reason}",
                        dhan_order_details="None (Blocked by parser validation)",
                        heads_up=fno_reason,
                        is_live=is_live,
                        is_processed=False,
                        timestamp=now,
                    )
                    self.dispatch_notification("TRADE_LOG", rej_card)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=fno_reason, signal_type=SignalType.OPTION, log_card=rej_card)

                if fno_sig is None and fno_action == SignalAction.IGNORED:
                    fno_sig = parse_fno_gemini_sync(
                        text, channel_id=channel_id, message_id=message_id,
                        msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec,
                        current_time=injected_clock,
                    )

                if fno_sig is not None:
                    return self._handle_fno_signal(fno_sig, source_name=source_name, now=now, injected_clock=injected_clock)

            eq_passed, eq_filter_reason, eq_action = filter_equity_message(
                text, msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec, current_time=now
            )
            if not eq_passed and eq_action == SignalAction.EXIT_SIGNAL:
                self.db.record_signal(channel_id, message_id, "EQUITY", "", "EXIT_SIGNAL", text, "LOGGED_NO_ACTION", eq_filter_reason)
                exit_card = format_exit_log(
                    symbol="EQUITY",
                    source_name=source_name,
                    original_message=text,
                    segment="EQUITY",
                    reason=eq_filter_reason,
                    is_live=is_live,
                    timestamp=now,
                )
                self.dispatch_notification("TRADE_LOG", exit_card)
                return ValidationResult(
                    is_valid=False,
                    action=SignalAction.EXIT_SIGNAL,
                    reason=eq_filter_reason,
                    signal_type=SignalType.EQUITY,
                    log_card=exit_card,
                )

            eq_sig, eq_reason = parse_equity_regex(
                text, channel_id=channel_id, message_id=message_id,
                msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec,
                current_time=now,
            )

            if eq_sig is None and eq_passed:
                eq_sig = parse_equity_gemini_sync(
                    text, channel_id=channel_id, message_id=message_id,
                    msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec,
                    current_time=injected_clock,
                )

            if eq_sig is not None:
                return self._handle_equity_signal(eq_sig, source_name=source_name, now=now, injected_clock=injected_clock)

            self.db.record_signal(channel_id, message_id, "NONE", "", "IGNORED", text, "FILTERED", eq_reason)
            return ValidationResult(
                is_valid=False,
                action=SignalAction.IGNORED,
                reason=eq_reason,
            )

    def _handle_fno_signal(self, sig, source_name: str, now: float, injected_clock: Optional[float] = None) -> ValidationResult:
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)
        heads_up_note = None

        if not self.cfg.fno_active:
            reason = "Stock Options processing is currently INACTIVE in configuration"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "DUPLICATE", dup_reason)
            rej_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="Rejected - Duplicate Signal",
                dhan_order_details="None (Duplicate signal blocked)",
                heads_up=dup_reason,
                is_live=is_live,
                is_processed=False,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=dup_reason, signal_type=SignalType.OPTION, symbol=sig.symbol, log_card=rej_card)

        ref_date: Optional[date] = None
        if injected_clock is not None:
            if injected_clock >= 946684800.0:
                ref_date = datetime.fromtimestamp(injected_clock).date()
            elif getattr(self.resolver, "_options_list", None):
                ref_date = min((c.expiry for c in self.resolver._options_list), default=None)

        contract, res_reason = self.resolver.resolve_option(
            symbol=sig.symbol,
            strike=sig.strike,
            option_type=sig.option_type,
            expiry_month=sig.expiry_month,
            expiry_day=getattr(sig, "expiry_day", None),
            reference_date=ref_date,
        )

        if not contract:
            heads_up_note = "Instrument unverified"
            res_reason = f"Instrument unverified: {res_reason}"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "UNRESOLVED", res_reason)
            rej_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="Rejected - Unresolved contract",
                dhan_order_details="None (Unverified instrument)",
                heads_up=heads_up_note,
                is_live=is_live,
                is_processed=False,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=res_reason, signal_type=SignalType.OPTION, symbol=sig.symbol, log_card=rej_card)

        limit_price = calculate_option_limit_price(sig.entry_max, contract.tick_size)

        sizing = size_option(
            sizing_mode=self.cfg.fno_sizing_mode,
            capital_per_trade=self.cfg.fno_capital_per_trade,
            fixed_lots=self.cfg.fno_fixed_lots,
            max_lots=self.cfg.fno_max_lots,
            lot_size=contract.lot_size,
            limit_price=limit_price,
            targets=sig.targets,
        )
        if not sizing.is_valid:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        ok_funds, avail_funds, funds_msg = self.broker.get_fund_limits()
        if not ok_funds:
            reason = f"Broker funds verification failed: {funds_msg}"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        if hasattr(self.risk_manager, "sync_from_journal_and_broker"):
            self.risk_manager.sync_from_journal_and_broker(
                journal_db=self.db,
                broker=self.broker if is_live else None,
                watcher=self.watcher,
            )

        effective_funds = avail_funds if (is_live or avail_funds >= sizing.total_cost) else getattr(self.cfg, "test_mode_paper_funds_inr", 100000.0)
        risk_passed, risk_reason = self.risk_manager.validate_order_risk(sizing.total_cost, effective_funds)
        if not risk_passed:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", risk_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=risk_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        if is_live and not hasattr(self.broker, "place_super_order"):
            reason = "Entry blocked: MARGIN options require supported Super Order protection route (Forever OCO unsupported for MARGIN)"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_UNPROTECTABLE", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        sig_id = self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BUY_ENTRY", sig.raw_text, "ACCEPTED", "Passed all validations")
        self.db.record_validation(sig_id, sig.symbol, limit_price, sizing.units, sizing.total_quantity, sizing.total_cost, True, "Approved")

        correlation_id = generate_correlation_id("OPT", sig.symbol, now)
        opt_trading_sym = contract.trading_symbol or f"{sig.symbol}-{sig.strike:g}{sig.option_type}"
        order_req = OrderRequest(
            correlation_id=correlation_id,
            symbol=opt_trading_sym,
            security_id=contract.security_id,
            exchange_segment=contract.exchange_segment,
            transaction_type=TransactionType.BUY,
            product_type=ProductType.MARGIN,
            order_type=OrderType.LIMIT,
            price=limit_price,
            quantity=sizing.total_quantity,
            trading_mode=self.cfg.trading_mode,
        )

        tranches_info = ""
        if sizing.tranches and len(sizing.tranches) > 1:
            tranches_str = ", ".join(f"{l}L @ Tgt {t:g}" for l, t in sizing.tranches)
            tranches_info = f" [Tranches: {tranches_str}]"

        try:
            self.db.record_order_intent(
                correlation_id=correlation_id,
                symbol=order_req.symbol,
                trading_symbol=opt_trading_sym,
                security_id=order_req.security_id,
                exchange_segment=order_req.exchange_segment,
                product_type=order_req.product_type.value,
                order_type=order_req.order_type.value,
                price=order_req.price,
                quantity=order_req.quantity,
                is_super_order=True,
                original_market_price=sig.entry_max,
                tick_size=contract.tick_size,
                lot_size=contract.lot_size,
                option_type=contract.option_type,
                strike=contract.strike,
                expiry=contract.expiry,
            )
        except Exception as e:
            reason = f"Journal DB error recording order intent (blocking entry): {e}"
            logger.critical(f"[JOURNAL FAIL-CLOSED] {reason}")
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        if self.cfg.trading_mode == TradingMode.TEST:
            self.db.update_order_intent(correlation_id, state="TEST_VALIDATED")
            self.db.record_order(
                correlation_id=correlation_id,
                trading_mode="TEST",
                product_type=ProductType.MARGIN.value,
                exchange_segment=contract.exchange_segment,
                symbol=sig.symbol,
                security_id=contract.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status="TEST_VALIDATED",
            )

            self.watcher.track(
                order_id=correlation_id,
                correlation_id=correlation_id,
                symbol=sig.symbol,
                trading_symbol=opt_trading_sym,
                security_id=contract.security_id,
                segment=contract.exchange_segment,
                qty=sizing.total_quantity,
                product_type=ProductType.MARGIN.value,
                dry_price=limit_price,
                original_market_price=sig.entry_max,
                is_super_order=True,
                tick_size=contract.tick_size,
                lot_size=contract.lot_size,
                option_type=contract.option_type,
                strike=contract.strike,
                expiry=contract.expiry,
            )

            log_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="TEST Mode: Validated & Sized (No broker order sent)",
                dhan_order_details=f"[TEST Mode / Qty: {sizing.total_quantity} ({sizing.units} lots){tranches_info} / Limit: {limit_price:.2f}]",
                heads_up=heads_up_note or "TEST Mode active - zero broker orders placed",
                is_live=False,
                is_processed=True,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", log_card)

            return ValidationResult(
                is_valid=True,
                action=SignalAction.BUY_ENTRY,
                reason="TEST Mode: Signal validated, lot sized, ceiling calculated. Zero live orders placed.",
                signal_type=SignalType.OPTION,
                symbol=sig.symbol,
                security_id=contract.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
                log_card=log_card,
            )
        else:
            m_open, m_msg = market_status()
            if not m_open:
                reason = f"LIVE Order Blocked: {m_msg}"
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_MARKET_CLOSED", reason)
                rej_card = format_trade_log(
                    symbol=sig.symbol,
                    source_name=source_name,
                    original_message=sig.raw_text,
                    segment="FNO",
                    status=f"Rejected: {reason}",
                    dhan_order_details=f"None ({m_msg})",
                    heads_up=reason,
                    is_live=True,
                    is_processed=False,
                    timestamp=now,
                )
                self.dispatch_notification("TRADE_LOG", rej_card)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol, log_card=rej_card)

            self.db.update_order_intent(correlation_id, state="SUBMITTING")

            init_levels = calculate_protection_levels(
                executed_price=limit_price,
                quantity=sizing.total_quantity,
                sl_pct=self.cfg.sl_pct,
                target_pct=self.cfg.target_pct,
                sl_limit_buffer_pct=self.cfg.sl_limit_buffer_pct,
                tick_size=contract.tick_size,
            )
            used_super = False
            if hasattr(self.broker, "place_super_order"):
                ok, broker_id, msg = self.broker.place_super_order(
                    order_req,
                    stop_loss_price=init_levels.sl_trigger,
                    target_price=init_levels.target_price,
                )
                used_super = True
            else:
                ok, broker_id, msg = self.broker.place_order(order_req)

            if ok:
                intent_state = "ACKED"
                status = "PENDING"
                if hasattr(self.risk_manager, "record_order_committed"):
                    self.risk_manager.record_order_committed(sizing.total_cost)
            elif _is_transport_uncertainty(msg):
                intent_state = "UNKNOWN"
                status = "UNKNOWN"
                self.reconciliation_gate.halt_new_entries(f"Order {correlation_id} outcome is UNKNOWN: {msg}")
            else:
                intent_state = "REJECTED"
                status = "REJECTED"

            self.db.update_order_intent(
                correlation_id=correlation_id,
                state=intent_state,
                broker_order_id=broker_id,
                error_msg=None if ok else msg,
            )
            order_details = f"[Order ID: {broker_id} / Status: {status}{tranches_info}]" if broker_id else f"[{status}: {msg}]"
            self.db.record_order(
                correlation_id=correlation_id,
                broker_order_id=broker_id,
                trading_mode="LIVE",
                product_type=ProductType.MARGIN.value,
                exchange_segment=contract.exchange_segment,
                symbol=sig.symbol,
                security_id=contract.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status=status,
            )

            if ok and broker_id:
                self.watcher.track(
                    order_id=broker_id,
                    correlation_id=correlation_id,
                    symbol=sig.symbol,
                    trading_symbol=opt_trading_sym,
                    security_id=contract.security_id,
                    segment=contract.exchange_segment,
                    qty=sizing.total_quantity,
                    product_type=ProductType.MARGIN.value,
                    original_market_price=sig.entry_max,
                    is_super_order=used_super,
                    tick_size=contract.tick_size,
                    lot_size=contract.lot_size,
                    option_type=contract.option_type,
                    strike=contract.strike,
                    expiry=contract.expiry,
                )

            log_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="Message sent to Dhan" if ok else f"Order Failed ({status}): {msg}",
                dhan_order_details=order_details,
                heads_up=heads_up_note,
                is_live=True,
                is_processed=ok,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", log_card)

            return ValidationResult(
                is_valid=ok,
                action=SignalAction.BUY_ENTRY,
                reason=f"LIVE Order placed on Dhan: {broker_id}" if ok else f"Dhan order placement failed: {msg}",
                signal_type=SignalType.OPTION,
                symbol=sig.symbol,
                security_id=contract.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
                log_card=log_card,
            )

    def _handle_equity_signal(self, sig, source_name: str, now: float, injected_clock: Optional[float] = None) -> ValidationResult:
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)
        heads_up_note = None

        if not self.cfg.equity_active:
            reason = "Equity Cash processing is currently INACTIVE in configuration"
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "DUPLICATE", dup_reason)
            rej_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status="Rejected - Duplicate Signal",
                dhan_order_details="None (Duplicate signal blocked)",
                heads_up=dup_reason,
                is_live=is_live,
                is_processed=False,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=dup_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol, log_card=rej_card)

        inst, res_reason = self.resolver.resolve_equity(sig.symbol)
        if not inst:
            heads_up_note = "Instrument unverified"
            res_reason = f"Instrument unverified: {res_reason}"
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "UNRESOLVED", res_reason)
            rej_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status="Rejected - Unresolved ticker",
                dhan_order_details="None (Unverified instrument)",
                heads_up=heads_up_note,
                is_live=is_live,
                is_processed=False,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=res_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol, log_card=rej_card)

        ref_price = sig.stated_price
        used_live_quote = False
        if ref_price <= 0.0:
            used_live_quote = True
            ok, ltp, ltp_msg = self.broker.get_ltp(
                inst.exchange_segment,
                inst.security_id,
                symbol=inst.symbol,
                now_utc=injected_clock,
                max_quote_age_sec=self.cfg.max_quote_age_sec,
            )
            if ok and ltp > 0:
                ref_price = ltp
            elif self.cfg.trading_mode == TradingMode.TEST and "no valid fresh quote" in ltp_msg.lower():
                ref_price = 1000.0
            else:
                reason = f"Could not establish fresh quote (<= {self.cfg.max_quote_age_sec}s): {ltp_msg}"
                self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "LTP_ERROR", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        limit_price = calculate_equity_limit_price(ref_price, self.cfg.equity_price_markup_pct, inst.tick_size)

        sizing = size_equity(
            sizing_mode=self.cfg.equity_sizing_mode,
            budget_inr=self.cfg.equity_budget_inr,
            fixed_qty=self.cfg.equity_fixed_qty,
            limit_price=limit_price,
        )
        if not sizing.is_valid:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        ok_funds, avail_funds, funds_msg = self.broker.get_fund_limits()
        if not ok_funds:
            reason = f"Broker funds verification failed: {funds_msg}"
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        if hasattr(self.risk_manager, "sync_from_journal_and_broker"):
            self.risk_manager.sync_from_journal_and_broker(
                journal_db=self.db,
                broker=self.broker if is_live else None,
                watcher=self.watcher,
            )

        effective_funds = avail_funds if (is_live or avail_funds >= sizing.total_cost) else getattr(self.cfg, "test_mode_paper_funds_inr", 100000.0)
        risk_passed, risk_reason = self.risk_manager.validate_order_risk(sizing.total_cost, effective_funds)
        if not risk_passed:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", risk_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=risk_reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        sig_id = self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BUY_ENTRY", sig.raw_text, "ACCEPTED", "Passed all validations")
        self.db.record_validation(sig_id, inst.symbol, limit_price, sizing.units, sizing.total_quantity, sizing.total_cost, True, "Approved")

        correlation_id = generate_correlation_id("EQ", inst.symbol, now)
        eq_trading_sym = f"{inst.symbol}-EQ"
        order_req = OrderRequest(
            correlation_id=correlation_id,
            symbol=eq_trading_sym,
            security_id=inst.security_id,
            exchange_segment=inst.exchange_segment,
            transaction_type=TransactionType.BUY,
            product_type=self.cfg.equity_product_type,
            order_type=OrderType.LIMIT,
            price=limit_price,
            quantity=sizing.total_quantity,
            trading_mode=self.cfg.trading_mode,
        )

        try:
            self.db.record_order_intent(
                correlation_id=correlation_id,
                symbol=order_req.symbol,
                trading_symbol=eq_trading_sym,
                security_id=order_req.security_id,
                exchange_segment=order_req.exchange_segment,
                product_type=order_req.product_type.value,
                order_type=order_req.order_type.value,
                price=order_req.price,
                quantity=order_req.quantity,
                is_super_order=False,
                original_market_price=ref_price,
                tick_size=inst.tick_size,
                lot_size=inst.lot_size,
            )
        except Exception as e:
            reason = f"Journal DB error recording order intent (blocking entry): {e}"
            logger.critical(f"[JOURNAL FAIL-CLOSED] {reason}")
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        if self.cfg.trading_mode == TradingMode.TEST:
            self.db.update_order_intent(correlation_id, state="TEST_VALIDATED")
            self.db.record_order(
                correlation_id=correlation_id,
                trading_mode="TEST",
                product_type=self.cfg.equity_product_type.value,
                exchange_segment=inst.exchange_segment,
                symbol=inst.symbol,
                security_id=inst.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status="TEST_VALIDATED",
            )

            self.watcher.track(
                order_id=correlation_id,
                correlation_id=correlation_id,
                symbol=inst.symbol,
                trading_symbol=eq_trading_sym,
                security_id=inst.security_id,
                segment=inst.exchange_segment,
                qty=sizing.total_quantity,
                product_type=self.cfg.equity_product_type.value,
                dry_price=limit_price,
                original_market_price=ref_price,
                tick_size=inst.tick_size,
                lot_size=inst.lot_size,
            )

            log_card = format_trade_log(
                symbol=inst.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status="TEST Mode: Validated & Sized (No broker order sent)",
                dhan_order_details=f"[TEST Mode / Qty: {sizing.total_quantity} shares / Limit: {limit_price:.2f}]",
                heads_up=heads_up_note or "TEST Mode active - zero broker orders placed",
                is_live=False,
                is_processed=True,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", log_card)

            return ValidationResult(
                is_valid=True,
                action=SignalAction.BUY_ENTRY,
                reason="TEST Mode: Signal validated, share count sized, limit price computed. Zero live orders placed.",
                signal_type=SignalType.EQUITY,
                symbol=inst.symbol,
                security_id=inst.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
                log_card=log_card,
            )
        else:
            m_open, m_msg = market_status()
            if not m_open:
                reason = f"LIVE Order Blocked: {m_msg}"
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_MARKET_CLOSED", reason)
                rej_card = format_trade_log(
                    symbol=inst.symbol,
                    source_name=source_name,
                    original_message=sig.raw_text,
                    segment="EQUITY",
                    status=f"Rejected: {reason}",
                    dhan_order_details=f"None ({m_msg})",
                    heads_up=reason,
                    is_live=True,
                    is_processed=False,
                    timestamp=now,
                )
                self.dispatch_notification("TRADE_LOG", rej_card)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol, log_card=rej_card)

            pre_submit_now = injected_clock if injected_clock is not None else time.time()
            if (pre_submit_now - (sig.message_timestamp or now)) > self.cfg.max_message_age_sec:
                reason = f"LIVE Order Blocked: signal aged past {self.cfg.max_message_age_sec}s before submission"
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            if used_live_quote and getattr(self.broker, "last_quote_result", None) is not None:
                last_q = self.broker.last_quote_result
                if last_q.quote_timestamp > 0 and (pre_submit_now - last_q.quote_timestamp) > self.cfg.max_quote_age_sec:
                    reason = (
                        f"LIVE Order Blocked: quote expired before submission "
                        f"(age={pre_submit_now - last_q.quote_timestamp:.2f}s > {self.cfg.max_quote_age_sec:.2f}s)"
                    )
                    self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            self.db.update_order_intent(correlation_id, state="SUBMITTING")

            ok, broker_id, msg = self.broker.place_order(order_req)

            if ok:
                intent_state = "ACKED"
                status = "PENDING"
                if hasattr(self.risk_manager, "record_order_committed"):
                    self.risk_manager.record_order_committed(sizing.total_cost)
            elif _is_transport_uncertainty(msg):
                intent_state = "UNKNOWN"
                status = "UNKNOWN"
                self.reconciliation_gate.halt_new_entries(f"Order {correlation_id} outcome is UNKNOWN: {msg}")
            else:
                intent_state = "REJECTED"
                status = "REJECTED"

            self.db.update_order_intent(
                correlation_id=correlation_id,
                state=intent_state,
                broker_order_id=broker_id,
                error_msg=None if ok else msg,
            )
            order_details = f"[Order ID: {broker_id} / Status: {status}]" if broker_id else f"[{status}: {msg}]"
            self.db.record_order(
                correlation_id=correlation_id,
                broker_order_id=broker_id,
                trading_mode="LIVE",
                product_type=self.cfg.equity_product_type.value,
                exchange_segment=inst.exchange_segment,
                symbol=inst.symbol,
                security_id=inst.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status=status,
            )

            if ok and broker_id:
                self.watcher.track(
                    order_id=broker_id,
                    correlation_id=correlation_id,
                    symbol=inst.symbol,
                    trading_symbol=eq_trading_sym,
                    security_id=inst.security_id,
                    segment=inst.exchange_segment,
                    qty=sizing.total_quantity,
                    product_type=self.cfg.equity_product_type.value,
                    original_market_price=ref_price,
                    tick_size=inst.tick_size,
                    lot_size=inst.lot_size,
                )

            log_card = format_trade_log(
                symbol=inst.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status="Message sent to Dhan" if ok else f"Order Failed ({status}): {msg}",
                dhan_order_details=order_details,
                heads_up=heads_up_note,
                is_live=True,
                is_processed=ok,
                timestamp=now,
            )
            self.dispatch_notification("TRADE_LOG", log_card)

            return ValidationResult(
                is_valid=ok,
                action=SignalAction.BUY_ENTRY,
                reason=f"LIVE Order placed on Dhan: {broker_id}" if ok else f"Dhan order placement failed: {msg}",
                signal_type=SignalType.EQUITY,
                symbol=inst.symbol,
                security_id=inst.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
                log_card=log_card,
            )


async def start_telegram_listener(pipeline: TradingBotPipeline):
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    from src.engine.lock import SingletonListenerLock
    from src.notifier.outbox_worker import OutboxWorker
    import asyncio

    cfg = pipeline.cfg
    allowed_chats = list(cfg.monitored_channel_ids or [])
    if cfg.trading_mode != TradingMode.LIVE and cfg.test_group_id and cfg.test_group_id not in allowed_chats:
        allowed_chats.append(cfg.test_group_id)

    if not allowed_chats:
        raise RuntimeError("MONITORED_CHANNEL_IDS is empty; refusing to subscribe to all Telegram chats.")

    lock = SingletonListenerLock(os.path.join(cfg.data_dir, "listener.lock"))
    acquired, lock_msg = lock.acquire()
    if not acquired:
        raise RuntimeError(f"Cannot start listener: {lock_msg}")

    outbox_worker = None
    try:
        rec_ok, rec_msg = await asyncio.to_thread(pipeline.reconciliation_gate.run_startup_reconciliation)
        logger.info(f"Startup Reconciliation: ok={rec_ok}, msg={rec_msg}")

        session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else os.path.join(cfg.data_dir, "tg_userbot.session")
        client = TelegramClient(session, cfg.tg_api_id, cfg.tg_api_hash)

        await client.start()
        me = await client.get_me()
        logger.info(f"Connected to Telegram as: {me.first_name} (@{me.username or 'No username'})")
        logger.info(f"Operating Mode: {cfg.trading_mode.value}")
        logger.info(f"Monitored Channels: {allowed_chats}")

        asyncio.create_task(pipeline.watcher.run(is_lifecycle_open))
        outbox_worker = OutboxWorker(
            journal_db=pipeline.db,
            dispatch_fn=pipeline.notifier.send_log_message,
        )
        asyncio.create_task(outbox_worker.run())

        @client.on(events.NewMessage(chats=allowed_chats))
        async def on_new_message(event):
            if event.chat_id not in allowed_chats:
                return
            if getattr(event.message, "edit_date", None):
                return

            text = event.message.message or ""
            if not text.strip():
                return

            chat = await event.get_chat()
            source_name = getattr(chat, "title", str(event.chat_id))
            msg_ts = event.message.date.timestamp() if hasattr(event.message, "date") and event.message.date else time.time()

            logger.info(f"Incoming message from '{source_name}' ({event.chat_id}): {text[:60]}...")
            result = await asyncio.to_thread(
                pipeline.process_raw_message,
                text=text,
                channel_id=event.chat_id,
                message_id=event.id,
                msg_timestamp=msg_ts,
                source_name=source_name,
            )
            logger.info(f"Evaluation outcome: is_valid={result.is_valid}, action={result.action.value}, reason={result.reason}")

        @client.on(events.MessageEdited(chats=allowed_chats))
        async def on_edited_message(event):
            if event.chat_id not in allowed_chats:
                return
            text = event.message.message or ""
            if not text.strip():
                return
            await asyncio.to_thread(
                pipeline.db.claim_signal,
                channel_id=event.chat_id,
                message_id=event.id,
                raw_text=text,
            )

        print(f"\n🚀 Bot is actively listening for trade signals in {cfg.trading_mode.value} mode...")
        print(f"📡 Monitored channels: {allowed_chats}")
        print(f"📊 Logging destination: {cfg.log_group_id}\n")

        await client.run_until_disconnected()
    finally:
        if outbox_worker:
            outbox_worker.stop()
        lock.release()


def run():
    import sys
    import asyncio
    cfg = AppConfig.from_env()

    if len(sys.argv) > 1 and sys.argv[1] in ("--login", "login"):
        from src.tg_login import interactive_login
        asyncio.run(interactive_login(cfg))
        return

    pipeline = TradingBotPipeline(cfg)
    asyncio.run(start_telegram_listener(pipeline))


if __name__ == "__main__":
    run()
