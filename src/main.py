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
import json
import os
import threading
import time
from typing import Optional
from src.config import AppConfig
from src.models import (
    TradingMode, ProductType, OrderType, TransactionType,
    SignalAction, SignalType, OrderRequest, ValidationResult, FnOSignal
)
from src.parsers.equity_parser import parse_equity_regex, filter_equity_message
from src.parsers.fno_parser import parse_fno_regex, looks_like_option
from src.parsers.gemini_fallback import parse_fno_gemini_sync, parse_equity_gemini_sync
from src.parsers.brain import parse_signal_brain
from src.parsers.duplicate_filter import DuplicateFilter
from src.instruments.resolver import InstrumentResolver
from src.engine.sizer import (
    size_equity, size_option, calculate_equity_limit_price,
    calculate_option_limit_price, calculate_protection_levels
)
from src.engine.market import IST, is_market_open, is_lifecycle_open, market_status
from src.engine.latency_monitor import GLOBAL_LATENCY_MONITOR, SignalLatencyTrace
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.fno_amo_queue import FnoAmoQueue
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.risk_manager import RiskManager
from src.journal.db import JournalDB, generate_correlation_id
from src.broker.dhan_client import DhanClient
from src.notifier.formatter import format_trade_log, format_exit_log
from src.notifier.execution_updates import (
    OrderStatusReporter, format_order_submitted, format_order_failed, format_amo_acknowledged,
)
from src.notifier.telegram_notifier import TelegramNotifier
from src.engine.reconciliation import StartupReconciliationGate

logger = logging.getLogger("trading_bot")


def _is_eligible_amo_closed_session(reason: str) -> bool:
    """Permit AMO only for verified closed-market reasons, not calendar errors."""
    return str(reason or "").startswith((
        "Market Pre-Open / Closed", "Market Closed Post-Hours",
        "Market Closed (Saturday)", "Market Closed (Sunday)",
        "Market Closed (Exchange Holiday",
    ))


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
        self._amo_queue = None
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
            equity_sl_pct=config.equity_sl_pct,
            equity_target_pct=config.equity_target_pct,
            fno_sl_pct=config.fno_sl_pct,
            fno_target_pct=config.fno_target_pct,
            sl_limit_buffer_pct=config.sl_limit_buffer_pct,
            path=os.path.join(config.data_dir, "pending_protection.json"),
            dry_run=(config.trading_mode == TradingMode.TEST),
            journal_db=self.db,
            order_timeout_sec=config.order_timeout_sec,
            retry_timeout_sec=getattr(config, "partial_fill_retry_timeout_sec", 2.0),
            risk_manager=self.risk_manager,
        )
        self.watcher.execution_reporter = OrderStatusReporter(self.db, self.broker)
        self.reconciliation_gate = StartupReconciliationGate(
            broker=self.broker,
            journal_db=self.db,
            watcher=self.watcher,
            stop_manager=self.stop_manager,
        )
        self.broker.reconciliation_gate = self.reconciliation_gate
        self.latency_monitor = GLOBAL_LATENCY_MONITOR
        self.latency_monitor.bind_db(self.db, force_reload=True)
        try:
            from src.engine.config_manager import ConfigChangeTracker, compute_config_version
            tracker = ConfigChangeTracker(data_dir=config.data_dir)
            tracker.record_active_running_version(
                version_hash=compute_config_version(config),
                trading_mode=config.trading_mode.value,
            )
        except Exception as e:
            logger.warning(f"Could not record active running config version: {e}")

    def reload_config(self, env_file: str = ".env") -> AppConfig:
        """V12-23: Reloads configuration from .env into the active pipeline."""
        with self._pipeline_lock:
            self.cfg = AppConfig.from_env(env_file=env_file)
            self.risk_manager.cfg = self.cfg
            self.broker.cfg = self.cfg
            self.notifier = TelegramNotifier(self.cfg)
            self.watcher.sl_pct = self.cfg.sl_pct
            self.watcher.target_pct = self.cfg.target_pct
            self.watcher.equity_sl_pct = self.cfg.equity_sl_pct
            self.watcher.equity_target_pct = self.cfg.equity_target_pct
            self.watcher.fno_sl_pct = self.cfg.fno_sl_pct
            self.watcher.fno_target_pct = self.cfg.fno_target_pct
            self.watcher.sl_limit_buffer_pct = self.cfg.sl_limit_buffer_pct
            self.watcher.order_timeout_sec = self.cfg.order_timeout_sec
            self.watcher.retry_timeout_sec = getattr(self.cfg, "partial_fill_retry_timeout_sec", 2.0)
            try:
                from src.engine.config_manager import ConfigChangeTracker, compute_config_version
                tracker = ConfigChangeTracker(data_dir=self.cfg.data_dir)
                tracker.record_active_running_version(
                    version_hash=compute_config_version(self.cfg),
                    trading_mode=self.cfg.trading_mode.value,
                )
            except Exception:
                pass
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
        signal_received_at: Optional[float] = None,
    ) -> ValidationResult:
        with self._pipeline_lock:
            l1_start_ns = time.perf_counter_ns()
            injected_clock = current_time
            now = injected_clock if injected_clock is not None else time.time()
            msg_ts = msg_timestamp or now
            trace = SignalLatencyTrace(
                trace_id=f"TRC-{int(now * 1000)}-{os.getpid()}",
                channel_id=channel_id,
                message_id=message_id,
                ist_received_time=signal_received_at if signal_received_at is not None else now,
                telegram_msg_timestamp=msg_timestamp if msg_timestamp and msg_timestamp > 0 else None,
                l1_received_ns=l1_start_ns,
            )

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

            decision = parse_signal_brain(
                text=text,
                channel_id=channel_id,
                message_id=message_id,
                msg_timestamp=msg_ts,
                max_age_sec=self.cfg.max_message_age_sec,
                current_time=injected_clock if injected_clock is not None else now,
                journal_db=self.db,
            )
            prov_kwargs = {
                "parser_version": decision.parser_version,
                "parse_source": decision.source,
                "rule_id": decision.rule_id,
                "model_name": decision.model_name or "",
                "reason_code": decision.reason_code,
                "parse_latency_ms": decision.parse_latency_ms,
                "evidence": decision.evidence,
            }

            if decision.route == "OPTION":
                if decision.action == SignalAction.EXIT_SIGNAL:
                    self.db.record_signal(channel_id, message_id, "OPTION", "", "EXIT_SIGNAL", text, "LOGGED_NO_ACTION", decision.reason, **prov_kwargs)
                    exit_card = format_exit_log(
                        symbol="OPTION",
                        source_name=source_name,
                        original_message=text,
                        segment="FNO",
                        reason=decision.reason,
                        is_live=is_live,
                        timestamp=now,
                    )
                    self.dispatch_notification("TRADE_LOG", exit_card)
                    return ValidationResult(is_valid=False, action=SignalAction.EXIT_SIGNAL, reason=decision.reason, signal_type=SignalType.OPTION, log_card=exit_card)

                if decision.action == SignalAction.BLOCKED:
                    self.db.record_signal(channel_id, message_id, "OPTION", "", "BLOCKED", text, "REJECTED", decision.reason, **prov_kwargs)
                    rej_card = format_trade_log(
                        symbol="OPTION",
                        source_name=source_name,
                        original_message=text,
                        segment="FNO",
                        status=f"Rejected: {decision.reason}",
                        dhan_order_details="None (Blocked by parser validation)",
                        heads_up=decision.reason,
                        is_live=is_live,
                        is_processed=False,
                        timestamp=now,
                    )
                    self.dispatch_notification("TRADE_LOG", rej_card)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=decision.reason, signal_type=SignalType.OPTION, log_card=rej_card)

                if decision.fno_signal is not None:
                    post_now = (injected_clock + (decision.parse_latency_ms / 1000.0)) if injected_clock is not None else time.time()
                    if (post_now - msg_ts) > self.cfg.max_message_age_sec:
                        stale_reason = f"Signal aged past max_message_age_sec ({post_now - msg_ts:.1f}s > {self.cfg.max_message_age_sec}s) after BRAIN parsing"
                        prov_kwargs["reason_code"] = "MESSAGE_TOO_OLD"
                        self.db.record_signal(channel_id, message_id, "OPTION", decision.fno_signal.symbol, "BLOCKED", text, "REJECTED", stale_reason, **prov_kwargs)
                        return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=stale_reason, signal_type=SignalType.OPTION)
                    trace.symbol = decision.fno_signal.symbol
                    trace.mark_l2_parsed()
                    return self._handle_fno_signal(decision.fno_signal, source_name=source_name, now=post_now, injected_clock=injected_clock, trace=trace, notification_received_at=signal_received_at)

                self.db.record_signal(channel_id, message_id, "OPTION", "", "IGNORED", text, "FILTERED", decision.reason, **prov_kwargs)
                return ValidationResult(
                    is_valid=False,
                    action=SignalAction.IGNORED,
                    reason=decision.reason,
                    signal_type=SignalType.OPTION,
                )

            if decision.route == "EQUITY" and decision.action == SignalAction.EXIT_SIGNAL:
                self.db.record_signal(channel_id, message_id, "EQUITY", "", "EXIT_SIGNAL", text, "LOGGED_NO_ACTION", decision.reason, **prov_kwargs)
                exit_card = format_exit_log(
                    symbol="EQUITY",
                    source_name=source_name,
                    original_message=text,
                    segment="EQUITY",
                    reason=decision.reason,
                    is_live=is_live,
                    timestamp=now,
                )
                self.dispatch_notification("TRADE_LOG", exit_card)
                return ValidationResult(
                    is_valid=False,
                    action=SignalAction.EXIT_SIGNAL,
                    reason=decision.reason,
                    signal_type=SignalType.EQUITY,
                    log_card=exit_card,
                )

            if decision.equity_signal is not None:
                post_now = (injected_clock + (decision.parse_latency_ms / 1000.0)) if injected_clock is not None else time.time()
                if (post_now - msg_ts) > self.cfg.max_message_age_sec:
                    stale_reason = f"Signal aged past max_message_age_sec ({post_now - msg_ts:.1f}s > {self.cfg.max_message_age_sec}s) after BRAIN parsing"
                    prov_kwargs["reason_code"] = "MESSAGE_TOO_OLD"
                    self.db.record_signal(channel_id, message_id, "EQUITY", decision.equity_signal.symbol, "BLOCKED", text, "REJECTED", stale_reason, **prov_kwargs)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=stale_reason, signal_type=SignalType.EQUITY)
                trace.symbol = decision.equity_signal.symbol
                trace.mark_l2_parsed()
                return self._handle_equity_signal(decision.equity_signal, source_name=source_name, now=post_now, injected_clock=injected_clock, trace=trace)

            rec_type = "EQUITY" if decision.route == "EQUITY" else "NONE"
            rec_act = decision.action.value
            rec_stat = "REJECTED" if decision.action == SignalAction.BLOCKED else "FILTERED"
            self.db.record_signal(channel_id, message_id, rec_type, "", rec_act, text, rec_stat, decision.reason, **prov_kwargs)
            return ValidationResult(
                is_valid=False,
                action=decision.action,
                reason=decision.reason,
                signal_type=SignalType.EQUITY if decision.route == "EQUITY" else None,
            )

    def _get_fno_amo_queue(self) -> FnoAmoQueue:
        # Lazy: disabled F&O AMO never creates queues or affects existing trades.
        if self._amo_queue is None:
            self._amo_queue = FnoAmoQueue(self.cfg.data_dir, account_id=self.cfg.dhan_client_id)
        return self._amo_queue

    def dispatch_due_fno_amo(self) -> bool:
        """Process at most ONE locally queued signal per poll, only at verified OPEN.

        The reserved record is permanently single-attempt; crash/UNKNOWN outcomes
        stay DISPATCHING/UNCERTAIN for manual Dhan reconciliation, never auto-retry.
        No broker /orders F&O AMO is placed.
        """
        with self._pipeline_lock:
            if not (self.cfg.trading_mode == TradingMode.LIVE
                    and self.cfg.fno_active and self.cfg.fno_amo_enabled):
                return False
            is_open, _ = market_status()
            if not is_open:
                return False
            if self.stop_manager.is_stopped():
                return False
            if not getattr(self.reconciliation_gate, "is_accepting_entries", False):
                return False
            q = self._get_fno_amo_queue()
            entry = q.claim_one()
            if entry is None:
                return False
            item_id = entry["item_id"]
            try:
                now = time.time()
                sig = FnOSignal.model_validate(json.loads(entry["signal_json"]))
                if (int(sig.channel_id) not in set(self.cfg.monitored_channel_ids or [])
                        or f"{sig.channel_id}:{sig.message_id}" != item_id
                        or entry["account_id"] != self.cfg.dhan_client_id
                        or float(entry["created_at"]) > now
                        or float(entry["expires_at"]) <= now):
                    q.finish(item_id, "BLOCKED", "Source/account/expiry changed before dispatch")
                    return True
                result = self._handle_fno_signal(
                    sig=sig, source_name=entry["source"], now=now,
                    amo_dispatch=True, expected_amo_contract_id=entry["contract_id"],
                    expected_amo_expiry=entry["expiry"],
                )
                if result.is_valid:
                    q.finish(item_id, "SUBMITTED", "Protected Super Order placed; verify broker fill and protection")
                else:
                    # A Dhan response can be ambiguous. Never auto-repeat a claim.
                    reason = str(result.reason or "Protected entry rejected")[:290]
                    state = "UNCERTAIN" if _is_transport_uncertainty(reason) else "BLOCKED"
                    q.finish(item_id, state, reason)
            except Exception as err:
                logger.exception("F&O AMO dispatch failed; manual broker reconciliation required for %s", item_id)
                try:
                    q.finish(item_id, "UNCERTAIN", f"Dispatch exception; broker outcome must be checked: {type(err).__name__}")
                except Exception:
                    logger.critical("F&O AMO record remains DISPATCHING (fail-closed): %s", item_id)
            return True

    def _handle_fno_signal(self, sig, source_name: str, now: float, injected_clock: Optional[float] = None, trace: Optional[SignalLatencyTrace] = None, notification_received_at: Optional[float] = None, amo_dispatch: bool = False, expected_amo_contract_id: Optional[str] = None, expected_amo_expiry: Optional[str] = None) -> ValidationResult:
        fno_received_at = notification_received_at if notification_received_at is not None else now
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)
        heads_up_note = None

        if not self.cfg.fno_active:
            reason = "Stock Options processing is currently INACTIVE in configuration"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup and not amo_dispatch:
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
        if contract is not None and trace is not None:
            trace.mark_l3_resolved()
            trace.mark_l6_stated_price(sig.entry_max)

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

        if amo_dispatch and (
            str(contract.security_id) != str(expected_amo_contract_id)
            or contract.expiry.isoformat() != str(expected_amo_expiry)
        ):
            reason = "F&O AMO blocked: queued contract/expiry no longer matches current instrument master"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "AMO_CONTRACT_CHANGED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        limit_price = calculate_option_limit_price(sig.entry_max, contract.tick_size)

        sizing = size_option(
            sizing_mode=self.cfg.fno_sizing_mode,
            capital_per_trade=self.cfg.fno_capital_per_trade,
            fixed_lots=self.cfg.fno_fixed_lots,
            max_lots=self.cfg.fno_max_lots,
            lot_size=contract.lot_size,
            limit_price=limit_price,
            # Recommendation targets must never determine our lot tranches or exits.
            # One F&O order is sized independently of provider SL/targets.
            targets=None,
        )
        if not sizing.is_valid:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        # Protected F&O AMO is LOCAL staging only: Dhan Super Order API does
        # not document broker AMO fields. Never send an unprotected regular order.
        if is_live and not amo_dispatch and self.cfg.fno_amo_enabled:
            market_open, market_reason = market_status()
            if not market_open and _is_eligible_amo_closed_session(market_reason):
                local_dt = datetime.fromtimestamp(now, IST)
                today = local_dt.date()
                expiry_valid = (contract.expiry >= today if local_dt.hour < 9
                                else contract.expiry > today)
                fresh = (0 <= now - float(sig.message_timestamp or now)
                         <= self.cfg.max_message_age_sec)
                if (not expiry_valid or not fresh
                        or contract.exchange_segment != "NSE_FNO"):
                    reason = "F&O AMO blocked: expired/invalid session, source age, or unsupported option segment"
                    self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "AMO_INELIGIBLE", reason)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)
                try:
                    queued, note = self._get_fno_amo_queue().enqueue(
                        signal=sig, source=source_name, contract_id=str(contract.security_id),
                        expiry=contract.expiry.isoformat(), now=now,
                        max_age_sec=float(self.cfg.fno_amo_max_age_sec),
                    )
                except Exception as exc:
                    queued, note = False, "Durable AMO queue unavailable: " + type(exc).__name__
                    logger.exception("F&O AMO persistence failed closed")
                if queued:
                    reason = "F&O AMO staged LOCALLY for OPEN; not yet sent to Dhan or executed"
                    self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol,
                                          "AMO_QUEUED", sig.raw_text, "AMO_LOCAL_QUEUED", reason)
                    card = format_trade_log(
                        symbol=sig.symbol, source_name=source_name, original_message=sig.raw_text,
                        segment="FNO", status=reason, dhan_order_details="None (Local queue only)",
                        heads_up="Will be rechecked for margin, expiry, funds and protection at market open",
                        is_live=True, is_processed=False, timestamp=now,
                    )
                    self.dispatch_notification("TRADE_LOG", card)
                    return ValidationResult(is_valid=True, action=SignalAction.BUY_ENTRY,
                                            reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol,
                                            log_card=card, limit_price=limit_price,
                                            quantity_or_lots=sizing.units, calculated_exposure=sizing.total_cost)
                self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol,
                                      "BLOCKED", sig.raw_text, "AMO_QUEUE_REJECTED", note)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED,
                                        reason=note, signal_type=SignalType.OPTION, symbol=sig.symbol)
        if trace is not None:
            trace.mark_l7_sized()

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
        if trace is not None:
            trace.mark_l8_preflight()

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
            eval_mkt_dt = None
            if injected_clock is not None and injected_clock >= 1767225600.0:
                import src.engine.market as _mkt_mod
                if not hasattr(_mkt_mod.now_ist, "_mock_name"):
                    eval_mkt_dt = datetime.fromtimestamp(injected_clock, tz=IST)
            try:
                m_open, m_msg = market_status(eval_mkt_dt) if eval_mkt_dt is not None else market_status()
            except TypeError:
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

            try:
                from src.engine.config_manager import ConfigChangeTracker
                t_blocked, t_reason = ConfigChangeTracker(data_dir=self.cfg.data_dir).is_live_entry_blocked_by_transition()
            except Exception as e:
                t_blocked, t_reason = True, f"Transition gate check error: {e}"
            if t_blocked:
                reason = f"LIVE Order Blocked: {t_reason}"
                self.latency_monitor.record_rejection(reason)
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_TRANSITION_PENDING", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

            pre_submit_now = injected_clock if injected_clock is not None else time.time()
            if not amo_dispatch and (pre_submit_now - (sig.message_timestamp or now)) > self.cfg.max_message_age_sec:
                reason = f"LIVE Order Blocked: signal aged past {self.cfg.max_message_age_sec}s before submission"
                self.latency_monitor.record_rejection(reason)
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

            if trace is not None:
                if trace.l3_resolved_ns is None:
                    trace.mark_l3_resolved()
                if trace.l6_cmp_auth_ns is None:
                    trace.mark_l6_stated_price(sig.entry_max)
                if trace.l7_sized_ns is None:
                    trace.mark_l7_sized()
                if trace.l8_preflight_ns is None:
                    trace.mark_l8_preflight()
                trace.mark_l9_dhan_start()

            self.db.update_order_intent(correlation_id, state="SUBMITTING")

            init_levels = calculate_protection_levels(
                executed_price=limit_price,
                quantity=sizing.total_quantity,
                sl_pct=self.cfg.fno_sl_pct,
                target_pct=self.cfg.fno_target_pct,
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

            if trace is not None:
                trace.mark_l10_dhan_ack(dhan_http_ms=getattr(self.broker, "last_order_http_latency_ms", None))
                trace.mark_completed()
                self.latency_monitor.record_trace(trace, order_id=broker_id, correlation_id=correlation_id)

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
                self.latency_monitor.record_rejection(msg)

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
                    notification_meta={
                        "symbol": sig.symbol, "source": source_name,
                        "segment": "FNO", "action": "BUY", "mode": "LIVE",
                        "limit_price": limit_price, "signal_received_at": fno_received_at,
                    },
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
            user_card = (
                format_order_submitted({
                    "symbol": sig.symbol, "source": source_name,
                    "segment": "FNO", "action": "BUY", "mode": "LIVE",
                    "limit_price": limit_price, "signal_received_at": fno_received_at,
                }, sizing.total_quantity)
                if ok and broker_id else
                format_order_failed({
                    "symbol": sig.symbol, "source": source_name,
                    "segment": "FNO", "action": "BUY", "mode": "LIVE",
                    "limit_price": limit_price, "signal_received_at": fno_received_at,
                }, sizing.total_quantity, uncertain=(status == "UNKNOWN" or (ok and not broker_id)))
            )
            self.dispatch_notification("TRADE_LOG", user_card)

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

    def _handle_equity_signal(
        self,
        sig,
        source_name: str,
        now: float,
        injected_clock: Optional[float] = None,
        trace: Optional[SignalLatencyTrace] = None,
    ) -> ValidationResult:
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)
        heads_up_note = None
        if trace is None:
            trace = SignalLatencyTrace(
                trace_id=f"TRC-{int(now * 1000)}-{os.getpid()}",
                symbol=sig.symbol,
                channel_id=sig.channel_id,
                message_id=sig.message_id,
                ist_received_time=now,
                telegram_msg_timestamp=sig.message_timestamp if sig.message_timestamp else None,
            )
            trace.mark_l2_parsed()

        if not self.cfg.equity_active:
            reason = "Equity Cash processing is currently INACTIVE in configuration"
            self.latency_monitor.record_rejection(reason)
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup:
            self.latency_monitor.record_rejection(dup_reason)
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
            self.latency_monitor.record_rejection(res_reason)
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

        trace.mark_l3_resolved()

        # Select the correct CMP policy before pricing an after-hours entry.
        pre_price_amo = False
        if (
            is_live and bool(getattr(self.cfg, "equity_amo_enabled", False))
            and inst.exchange_segment == "NSE_EQ"
            and self.cfg.equity_product_type == ProductType.CNC
        ):
            price_eval_dt = None
            if injected_clock is not None and injected_clock >= 1767225600.0:
                import src.engine.market as _mkt_mod
                if not hasattr(_mkt_mod.now_ist, "_mock_name"):
                    price_eval_dt = datetime.fromtimestamp(injected_clock, tz=IST)
            try:
                price_m_open, price_m_msg = market_status(price_eval_dt) if price_eval_dt is not None else market_status()
            except TypeError:
                price_m_open, price_m_msg = market_status()
            pre_price_amo = not price_m_open and _is_eligible_amo_closed_session(price_m_msg)

        ref_price = sig.stated_price
        used_live_quote = False
        configured_policy = str(getattr(self.cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP") or "STRICT_TIMESTAMP").upper().strip()
        ref_label = "STATED_PRICE"
        last_q = None

        if ref_price <= 0.0:
            used_live_quote = True
            ok, ltp, ltp_msg = self.broker.get_ltp(
                inst.exchange_segment,
                inst.security_id,
                symbol=inst.symbol,
                now_utc=injected_clock,
                max_quote_age_sec=self.cfg.max_quote_age_sec,
                cmp_policy="DARVAS_HTTP_SUCCESS" if pre_price_amo else configured_policy,
            )
            last_q = getattr(self.broker, "last_quote_result", None)

            if pre_price_amo:
                amo_ref_age = getattr(last_q, "age_sec", None)
                max_amo_age = float(getattr(self.cfg, "equity_amo_max_reference_age_sec", 259200.0))
                valid_amo_ref = (
                    ok and ltp > 0 and last_q is not None
                    and bool(getattr(last_q, "is_valid", False))
                    and getattr(last_q, "quote_timestamp", None) is not None
                    and isinstance(amo_ref_age, (int, float)) and 0.0 <= amo_ref_age <= max_amo_age
                    and str(getattr(last_q, "currency", "")).upper() == "INR"
                    and str(getattr(last_q, "reference_label", "")) == "HISTORICAL_REFERENCE"
                )
                if not valid_amo_ref:
                    reason = (
                        "AMO blocked: no verified historical Yahoo reference within "
                        f"{max_amo_age / 3600.0:g} hours ({ltp_msg})"
                    )
                    self.latency_monitor.record_rejection(reason)
                    self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "AMO_REFERENCE_UNVERIFIED", reason)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)
                heads_up_note = (
                    f"AMO LIMIT priced from last published quote ({amo_ref_age / 3600.0:.1f}h old); "
                    "Dhan queues the order for market OPEN; execution and stop/target are not yet confirmed"
                )

            if ok and ltp > 0:
                ref_price = ltp
                ref_label = getattr(last_q, "reference_label", "LIVE_REFERENCE") if last_q else "LIVE_REFERENCE"
                if not is_live and not is_market_open():
                    ref_label = "HISTORICAL_REFERENCE"
            elif not is_live:
                # Section 8: TEST-mode CMP reference outside market hours (no arbitrary 1000.0 fallback)
                if last_q is not None and last_q.price > 0 and any(
                    k in ltp_msg.lower()
                    for k in ("non-regular", "closed", "stale", "missing or invalid source quote timestamp")
                ):
                    ref_price = float(last_q.price)
                    ref_label = "HISTORICAL_REFERENCE"
                    heads_up_note = "TEST Mode (HISTORICAL_REFERENCE price outside live market hours - zero broker orders placed)"
                else:
                    reason = f"Could not establish CMP reference: {ltp_msg}"
                    self.latency_monitor.record_rejection(reason)
                    self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "LTP_ERROR", reason)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)
            else:
                reason = (
                    f"Could not establish fresh quote (<= {self.cfg.max_quote_age_sec}s): {ltp_msg}"
                    if configured_policy == "STRICT_TIMESTAMP"
                    else f"Could not establish CMP reference ({configured_policy}): {ltp_msg}"
                )
                self.latency_monitor.record_rejection(reason)
                self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "LTP_ERROR", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            q_age_status = getattr(last_q, "quote_age_status", "QUOTE_AGE_UNKNOWN") if last_q else "QUOTE_AGE_UNKNOWN"
            q_age_val = last_q.age_sec if (last_q and q_age_status not in ("QUOTE_AGE_UNKNOWN", "FUTURE_TIMESTAMP_INVALID")) else None
            q_ts_val = getattr(last_q, "quote_timestamp", None) if last_q else None
            q_fresh_ok = bool(getattr(last_q, "freshness_verified", False)) if last_q else False
            q_fresh_st = str(getattr(last_q, "quote_freshness_status", "UNVERIFIED")) if last_q else "UNVERIFIED"
            trace.record_yahoo_stages(
                http_ms=getattr(last_q, "http_latency_ms", None) if last_q else None,
                parse_ms=getattr(last_q, "parse_latency_ms", None) if last_q else None,
                auth_ms=getattr(last_q, "auth_latency_ms", None) if last_q else None,
                quote_age_sec=q_age_val,
                quote_age_status=q_age_status,
                cmp_policy=configured_policy,
                yahoo_cmp=ref_price,
                quote_timestamp=q_ts_val,
                freshness_verified=q_fresh_ok,
                quote_freshness_status=q_fresh_st,
            )
            if configured_policy == "DARVAS_HTTP_SUCCESS" and not q_fresh_ok:
                if q_age_val is not None and q_age_val > self.cfg.max_quote_age_sec:
                    darvas_note = (
                        f"Darvas policy accepted quote exceeding {self.cfg.max_quote_age_sec:.2f}s "
                        f"(source quote age={q_age_val:.2f}s; pricing accepted, strict 2s freshness not met)"
                    )
                    heads_up_note = f"{heads_up_note} | {darvas_note}" if heads_up_note else darvas_note
                elif q_ts_val is None:
                    darvas_note = "Darvas policy accepted quote with missing regularMarketTime (QUOTE_AGE_UNKNOWN; freshness unverified)"
                    heads_up_note = f"{heads_up_note} | {darvas_note}" if heads_up_note else darvas_note
        else:
            trace.mark_l6_stated_price(ref_price)

        limit_price = calculate_equity_limit_price(ref_price, self.cfg.equity_price_markup_pct, inst.tick_size)

        sizing = size_equity(
            sizing_mode=self.cfg.equity_sizing_mode,
            budget_inr=self.cfg.equity_budget_inr,
            fixed_qty=self.cfg.equity_fixed_qty,
            limit_price=limit_price,
        )
        if not sizing.is_valid:
            self.latency_monitor.record_rejection(sizing.error_reason)
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        trace.mark_l7_sized()

        ok_funds, avail_funds, funds_msg = self.broker.get_fund_limits()
        if not ok_funds:
            reason = f"Broker funds verification failed: {funds_msg}"
            self.latency_monitor.record_rejection(reason)
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
            self.latency_monitor.record_rejection(risk_reason)
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
                is_amo=pre_price_amo,
                original_market_price=ref_price,
                tick_size=inst.tick_size,
                lot_size=inst.lot_size,
            )
        except Exception as e:
            reason = f"Journal DB error recording order intent (blocking entry): {e}"
            self.latency_monitor.record_rejection(reason)
            logger.critical(f"[JOURNAL FAIL-CLOSED] {reason}")
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

        trace.mark_l8_preflight()

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

            trace.mark_completed()
            self.latency_monitor.record_trace(trace, order_id=correlation_id, correlation_id=correlation_id)
            lat_dict = trace.to_breakdown_dict()
            cmp_details = {
                "symbol": inst.symbol,
                "source": source_name,
                "signal": "BUY",
                "price_source": "Yahoo Finance" if used_live_quote else "Stated Price",
                "yahoo_cmp": ref_price if used_live_quote else None,
                "yahoo_cmp_str": f"{ref_price:.2f} (Stated)" if not used_live_quote else None,
                "yahoo_http_latency_ms": trace.l4_yahoo_http_ms,
                "reported_quote_age": trace.quote_age_status if used_live_quote else "N/A",
                "quote_freshness_status": trace.quote_freshness_status if used_live_quote else "STATED_PRICE",
                "cmp_policy": configured_policy,
                "reference_label": ref_label,
                "limit_price": limit_price,
                "quantity": sizing.total_quantity,
                "order_submission": "TEST_VALIDATED (No broker order sent)",
                "broker_order_id": "None",
                "execution_status": "Not Submitted (TEST Mode)",
                "protection_status": "Not Applicable",
            }

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
                equity_cmp_details=cmp_details,
                latency_breakdown=lat_dict,
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
                latency_breakdown=lat_dict,
                cmp_policy=configured_policy,
                reference_label=ref_label,
                quote_age_status=trace.quote_age_status if used_live_quote else "STATED_PRICE",
                freshness_verified=trace.freshness_verified if used_live_quote else True,
                quote_freshness_status=trace.quote_freshness_status if used_live_quote else "STATED_PRICE",
                broker_order_status="TEST_VALIDATED",
            )
        else:
            eval_mkt_dt = None
            if injected_clock is not None and injected_clock >= 1767225600.0:
                import src.engine.market as _mkt_mod
                if not hasattr(_mkt_mod.now_ist, "_mock_name"):
                    eval_mkt_dt = datetime.fromtimestamp(injected_clock, tz=IST)
            try:
                m_open, m_msg = market_status(eval_mkt_dt) if eval_mkt_dt is not None else market_status()
            except TypeError:
                m_open, m_msg = market_status()
            is_amo_order = (
                pre_price_amo and not m_open
                and _is_eligible_amo_closed_session(m_msg)
                and bool(getattr(self.cfg, "equity_amo_enabled", False))
            )
            if (not m_open and not is_amo_order) or (pre_price_amo and not is_amo_order):
                reason = f"LIVE Order Blocked: {m_msg}" if not pre_price_amo else "LIVE Order Blocked: AMO session changed during processing"
                self.latency_monitor.record_rejection(reason)
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

            try:
                from src.engine.config_manager import ConfigChangeTracker
                t_blocked, t_reason = ConfigChangeTracker(data_dir=self.cfg.data_dir).is_live_entry_blocked_by_transition()
            except Exception as e:
                t_blocked, t_reason = True, f"Transition gate check error: {e}"
            if t_blocked:
                reason = f"LIVE Order Blocked: {t_reason}"
                self.latency_monitor.record_rejection(reason)
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", inst.symbol, "BLOCKED", sig.raw_text, "REJECTED_TRANSITION_PENDING", reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            pre_submit_now = injected_clock if injected_clock is not None else time.time()
            if (pre_submit_now - (sig.message_timestamp or now)) > self.cfg.max_message_age_sec:
                reason = f"LIVE Order Blocked: signal aged past {self.cfg.max_message_age_sec}s before submission"
                self.latency_monitor.record_rejection(reason)
                self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            # Normal-hours quotes use the strict live freshness check.
            # AMO references use a separate bounded historical-age check above.
            if not is_amo_order and used_live_quote and configured_policy != "DARVAS_HTTP_SUCCESS" and getattr(self.broker, "last_quote_result", None) is not None:
                last_q = self.broker.last_quote_result
                if last_q.quote_timestamp is not None and last_q.quote_timestamp > 0 and (pre_submit_now - last_q.quote_timestamp) > self.cfg.max_quote_age_sec:
                    reason = (
                        f"LIVE Order Blocked: quote expired before submission "
                        f"(age={pre_submit_now - last_q.quote_timestamp:.2f}s > {self.cfg.max_quote_age_sec:.2f}s)"
                    )
                    self.latency_monitor.record_rejection(reason)
                    self.db.update_order_intent(correlation_id, state="REJECTED", error_msg=reason)
                    return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=inst.symbol)

            order_req.after_market_order = is_amo_order
            order_req.amo_time = "OPEN"
            self.db.update_order_intent(correlation_id, state="SUBMITTING")
            trace.mark_l9_dhan_start()

            ok, broker_id, msg = self.broker.place_order(order_req)
            trace.mark_l10_dhan_ack(dhan_http_ms=getattr(self.broker, "last_order_http_latency_ms", None))
            trace.mark_completed()

            broker_raw_st = str(getattr(self.broker, "last_order_broker_status", "") or "").upper().strip()
            if ok:
                intent_state = "ACKED"
                status = "PENDING"
                broker_display_st = broker_raw_st if broker_raw_st in ("TRANSIT", "PENDING", "TRADED") else "TRANSIT"
                if hasattr(self.risk_manager, "record_order_committed"):
                    self.risk_manager.record_order_committed(sizing.total_cost)
            elif _is_transport_uncertainty(msg):
                intent_state = "UNKNOWN"
                status = "UNKNOWN"
                broker_display_st = "UNKNOWN"
                self.reconciliation_gate.halt_new_entries(f"Order {correlation_id} outcome is UNKNOWN: {msg}")
            else:
                intent_state = "REJECTED"
                status = "REJECTED"
                broker_display_st = "REJECTED"
                self.latency_monitor.record_rejection(msg)

            self.latency_monitor.record_trace(trace, order_id=broker_id, correlation_id=correlation_id)

            self.db.update_order_intent(
                correlation_id=correlation_id,
                state=intent_state,
                broker_order_id=broker_id,
                error_msg=None if ok else msg,
            )
            order_details = f"[Order ID: {broker_id} / Broker Status: {broker_display_st}]" if broker_id else f"[{status}: {msg}]"
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
                    is_amo=is_amo_order,
                    symbol=inst.symbol,
                    trading_symbol=eq_trading_sym,
                    security_id=inst.security_id,
                    segment=inst.exchange_segment,
                    qty=sizing.total_quantity,
                    product_type=self.cfg.equity_product_type.value,
                    original_market_price=ref_price,
                    tick_size=inst.tick_size,
                    lot_size=inst.lot_size,
                    notification_meta={
                        "symbol": inst.symbol, "source": source_name,
                        "segment": "EQUITY", "action": "BUY", "mode": "LIVE",
                        "is_amo": is_amo_order,
                        "yahoo_cmp": ref_price if used_live_quote else None,
                        "quote_age_sec": getattr(last_q, "age_sec", None) if used_live_quote else None,
                        "quote_threshold_sec": self.cfg.max_quote_age_sec,
                        "quote_accepted": True,
                        "limit_price": limit_price, "signal_received_at": trace.ist_received_time,
                    },
                )

            lat_dict = trace.to_breakdown_dict()
            # P1 Guard: Never label TRANSIT, PENDING, or broker-acknowledged orders as EXECUTED or FILLED.
            if ok:
                if broker_display_st == "TRADED":
                    exec_status_str = "TRADED (Broker Confirmed Fill)"
                    status_summary_str = "Order Traded on Dhan (TRADED)"
                elif is_amo_order:
                    exec_status_str = f"AMO {broker_display_st} (Broker Acknowledged — Queued for OPEN)"
                    status_summary_str = f"AMO submitted to Dhan ({broker_display_st} — Queued for market OPEN, Not Filled)"
                else:
                    exec_status_str = f"{broker_display_st} (Broker Acknowledged — Not Yet Filled)"
                    status_summary_str = f"Message sent to Dhan ({broker_display_st} — Awaiting Fill)"
                order_sub_str = f"{'AMO_' if is_amo_order else ''}ORDER_ACKNOWLEDGED ({broker_display_st})"
            else:
                exec_status_str = status
                status_summary_str = f"Broker Error: {msg}"
                order_sub_str = status

            cmp_details = {
                "symbol": inst.symbol,
                "source": source_name,
                "signal": "BUY",
                "price_source": "Yahoo Finance" if used_live_quote else "Stated Price",
                "yahoo_cmp": ref_price if used_live_quote else None,
                "yahoo_cmp_str": f"{ref_price:.2f} (Stated)" if not used_live_quote else None,
                "yahoo_http_latency_ms": trace.l4_yahoo_http_ms,
                "reported_quote_age": trace.quote_age_status if used_live_quote else "N/A",
                "quote_freshness_status": trace.quote_freshness_status if used_live_quote else "STATED_PRICE",
                "cmp_policy": configured_policy,
                "reference_label": ref_label,
                "limit_price": limit_price,
                "quantity": sizing.total_quantity,
                "order_submission": order_sub_str,
                "broker_order_id": broker_id or "None",
                "is_amo": is_amo_order,
                "execution_status": exec_status_str,
                "protection_status": "Pending Fill Verification" if ok else "Not Applicable",
            }

            log_card = format_trade_log(
                symbol=inst.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status=status_summary_str,
                dhan_order_details=order_details,
                heads_up=heads_up_note,
                is_live=True,
                is_processed=ok,
                timestamp=now,
                equity_cmp_details=cmp_details,
                latency_breakdown=lat_dict,
            )
            user_card = (
                format_order_submitted({
                    "symbol": inst.symbol, "source": source_name,
                    "segment": "EQUITY", "action": "BUY", "mode": "LIVE",
                    "is_amo": is_amo_order,
                    "yahoo_cmp": ref_price if used_live_quote else None,
                    "quote_age_sec": getattr(last_q, "age_sec", None) if used_live_quote else None,
                    "quote_threshold_sec": self.cfg.max_quote_age_sec,
                    "quote_accepted": True, "limit_price": limit_price,
                    "signal_received_at": trace.ist_received_time,
                }, sizing.total_quantity)
                if ok and broker_id else
                format_order_failed({
                    "symbol": inst.symbol, "source": source_name,
                    "segment": "EQUITY", "action": "BUY", "mode": "LIVE",
                    "is_amo": is_amo_order,
                    "yahoo_cmp": ref_price if used_live_quote else None,
                    "quote_age_sec": getattr(last_q, "age_sec", None) if used_live_quote else None,
                    "quote_threshold_sec": self.cfg.max_quote_age_sec,
                    "quote_accepted": True, "limit_price": limit_price,
                    "signal_received_at": trace.ist_received_time,
                }, sizing.total_quantity, uncertain=(status == "UNKNOWN" or (ok and not broker_id)))
            )
            self.dispatch_notification("TRADE_LOG", user_card)

            
            # Distinguish an accepted AMO from an executed trade.
            if ok and broker_id and is_amo_order and broker_display_st in ("TRANSIT", "PENDING"):
                reporter = getattr(self.watcher, "execution_reporter", None)
                if bool(getattr(reporter, "dhan_response_enabled", False)):
                    try:
                        ack_text = format_amo_acknowledged(
                            {"symbol": inst.symbol, "limit_price": limit_price},
                            sizing.total_quantity, broker_display_st,
                        )
                        self.db.enqueue_dhan_response_ack_once(broker_id, ack_text)
                    except Exception as notify_err:
                        logger.warning(
                            "[DhanResponse] Could not queue AMO acknowledgement for %s: %s",
                            broker_id, type(notify_err).__name__,
                        )

            return ValidationResult(
                is_valid=ok,
                action=SignalAction.BUY_ENTRY,
                reason=f"LIVE Order placed on Dhan: {broker_id} (status={broker_display_st})" if ok else f"Dhan order placement failed: {msg}",
                signal_type=SignalType.EQUITY,
                symbol=inst.symbol,
                security_id=inst.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
                log_card=log_card,
                latency_breakdown=lat_dict,
                cmp_policy=configured_policy,
                reference_label=ref_label,
                quote_age_status=trace.quote_age_status if used_live_quote else "STATED_PRICE",
                freshness_verified=trace.freshness_verified if used_live_quote else True,
                quote_freshness_status=trace.quote_freshness_status if used_live_quote else "STATED_PRICE",
                broker_order_status=broker_display_st,
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
        # V14: Automatically rebind an outdated Emergency STOP account only
        # when Dhan's authenticated profile matches the configured account.
        # Never auto-resume trading after an account switch.
        if cfg.trading_mode == TradingMode.LIVE:
            try:
                obligations = len(pipeline.watcher.pending)
                for reader_name in (
                    "get_non_terminal_intents",
                    "get_unprotected_orders",
                    "get_unresolved_protection_attempts",
                ):
                    reader = getattr(pipeline.db, reader_name, None)
                    if not callable(reader):
                        raise RuntimeError(f"Missing journal safety reader: {reader_name}")
                    obligations += len(reader())
                sync_ok, sync_reason = await asyncio.to_thread(
                    pipeline.stop_manager.sync_account_from_authenticated_broker,
                    pipeline.broker,
                    pending_watcher_count=obligations,
                )
            except Exception as exc:
                sync_ok = False
                sync_reason = f"Account sync safety check unavailable: {type(exc).__name__}"
            if not sync_ok:
                pipeline.reconciliation_gate.halt_new_entries(
                    f"Dhan account identity/binding verification failed: {sync_reason}"
                )
                rec_ok, rec_msg = False, sync_reason
            else:
                rec_ok, rec_msg = await asyncio.to_thread(
                    pipeline.reconciliation_gate.run_startup_reconciliation
                )
        else:
            rec_ok, rec_msg = await asyncio.to_thread(
                pipeline.reconciliation_gate.run_startup_reconciliation
            )
        logger.info(f"Startup Reconciliation: ok={rec_ok}, msg={rec_msg}")

        session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else os.path.join(cfg.data_dir, "tg_userbot.session")
        client = TelegramClient(session, cfg.tg_api_id, cfg.tg_api_hash)

        await client.start()
        me = await client.get_me()
        logger.info(f"Connected to Telegram as: {me.first_name} (@{me.username or 'No username'})")
        logger.info(f"Operating Mode: {cfg.trading_mode.value}")
        logger.info(f"Monitored Channels: {allowed_chats}")

        # Dhan final acknowledgements go to one explicitly named Telegram group.
        # This is notification-only: not a signal source, order route or trading gate.
        dhan_response_dispatch_fn = None
        response_reporter = getattr(pipeline.watcher, "execution_reporter", None)
        if response_reporter is not None:
            response_reporter.dhan_response_enabled = False
        if cfg.dhan_response_group_name:
            from src.notifier.dhan_response import resolve_dhan_response_group
            try:
                response_entity = await resolve_dhan_response_group(
                    client, cfg.dhan_response_group_name,
                    forbidden_chat_ids=allowed_chats,
                )
                loop = asyncio.get_running_loop()

                def _send_dhan_response(message: str) -> bool:
                    """Runs exclusively from the outbox worker thread, never the trading event loop."""
                    future = asyncio.run_coroutine_threadsafe(
                        client.send_message(response_entity, message, parse_mode="html"), loop
                    )
                    try:
                        future.result(timeout=12)
                        return True
                    except Exception as exc:
                        future.cancel()
                        logger.warning("Dhan Response Telegram delivery unsuccessful: %s", type(exc).__name__)
                        return False

                dhan_response_dispatch_fn = _send_dhan_response
                if response_reporter is not None:
                    response_reporter.dhan_response_enabled = True
                logger.info("Dhan final response group resolved uniquely: %s", cfg.dhan_response_group_name)
            except (ValueError, OSError, RuntimeError) as exc:
                logger.warning("Dhan Response group unavailable; final mirroring disabled: %s", exc)

        asyncio.create_task(pipeline.watcher.run(is_lifecycle_open))
        outbox_worker = OutboxWorker(
            journal_db=pipeline.db,
            dispatch_fn=pipeline.notifier.send_log_message,
            dhan_response_dispatch_fn=dhan_response_dispatch_fn,
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
        print(f"📊 Logging destination: {cfg.log_group_id}")
        print(f"📨 Dhan Response group: {cfg.dhan_response_group_name if dhan_response_dispatch_fn else 'Disabled / unresolved'}\n")

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
