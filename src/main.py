"""
Main Application Pipeline Orchestrator.
Processes raw Telegram messages through deterministic validation, deduplication,
sizing, risk limits, and broker execution (strictly disabled in TEST mode).
Integrates visual Telegram Log Group card formatting.
"""

import logging
import os
import time
import uuid
from typing import Optional, Tuple
from src.config import AppConfig
from src.models import (
    TradingMode, ProductType, OrderType, TransactionType,
    SignalAction, SignalType, OrderRequest, ValidationResult
)
from src.parsers.equity_parser import parse_equity_regex, filter_equity_message
from src.parsers.fno_parser import parse_fno_regex
from src.parsers.duplicate_filter import DuplicateFilter
from src.instruments.resolver import InstrumentResolver
from src.engine.sizer import (
    size_equity, size_option, calculate_equity_limit_price,
    calculate_option_limit_price, calculate_protection_levels
)
from src.engine.risk_manager import RiskManager
from src.journal.db import JournalDB
from src.broker.dhan_client import DhanClient
from src.notifier.formatter import format_trade_log, format_exit_log
from src.notifier.telegram_notifier import TelegramNotifier

logger = logging.getLogger("trading_bot")


class TradingBotPipeline:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.db = JournalDB(config.journal_db_path)
        self.dedup = DuplicateFilter(window_sec=config.dedup_window_sec)
        self.resolver = InstrumentResolver()
        scrip_cache = os.path.join(config.data_dir, "scrip_master.csv")
        self.resolver.load_dhan_master(scrip_cache)
        self.risk_manager = RiskManager(config)
        self.broker = DhanClient(config)
        self.notifier = TelegramNotifier(config)

    def process_raw_message(
        self,
        text: str,
        channel_id: int = 0,
        message_id: int = 0,
        msg_timestamp: float = 0.0,
        current_time: Optional[float] = None,
        source_name: str = "Test group",
    ) -> ValidationResult:
        """
        End-to-end evaluation of an incoming Telegram message.
        Dispatches visual formatted logs to Telegram Log Group.
        """
        now = current_time if current_time is not None else time.time()
        msg_ts = msg_timestamp or now
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)

        # 1. Age check
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
            self.notifier.send_log_message(log_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason)

        # 2. Check if Stock Option signal first
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
            self.notifier.send_log_message(exit_card)
            return ValidationResult(is_valid=False, action=SignalAction.EXIT_SIGNAL, reason=fno_reason, signal_type=SignalType.OPTION)

        if fno_action == SignalAction.BLOCKED:
            self.db.record_signal(channel_id, message_id, "OPTION", "", "BLOCKED", text, "REJECTED", fno_reason)
            rej_card = format_trade_log(
                symbol="OPTION",
                source_name=source_name,
                original_message=text,
                segment="FNO",
                status=f"Rejected: {fno_reason}",
                dhan_order_details="None (Blocked)",
                heads_up=fno_reason,
                is_live=is_live,
                is_processed=False,
                timestamp=now,
            )
            self.notifier.send_log_message(rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=fno_reason, signal_type=SignalType.OPTION)

        if fno_sig is not None:
            return self._handle_fno_signal(fno_sig, now, source_name)

        # 3. Else evaluate as Cash Equity signal
        eq_sig, eq_reason = parse_equity_regex(
            text, channel_id=channel_id, message_id=message_id,
            msg_timestamp=msg_ts, max_age_sec=self.cfg.max_message_age_sec,
            current_time=now,
        )

        if eq_sig is not None:
            return self._handle_equity_signal(eq_sig, now, source_name)

        # Unrecognized / casual chatter
        self.db.record_signal(channel_id, message_id, "NONE", "", "IGNORED", text, "FILTERED", eq_reason)
        return ValidationResult(is_valid=False, action=SignalAction.IGNORED, reason=eq_reason)

    def _handle_fno_signal(self, sig, now: float, source_name: str) -> ValidationResult:
        """Processes a validated stock option signal with exact visual log output."""
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)

        if not self.cfg.fno_active:
            reason = "Stock Options processing is currently INACTIVE in configuration"
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "DUPLICATE", dup_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=dup_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        contract, res_reason = self.resolver.resolve_option(sig.symbol, sig.strike, sig.option_type, sig.expiry_month)
        heads_up_note = None
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
            self.notifier.send_log_message(rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=res_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        limit_price = calculate_option_limit_price(sig.entry_max, contract.tick_size)

        sizing = size_option(
            sizing_mode=self.cfg.fno_sizing_mode,
            capital_per_trade=self.cfg.fno_capital_per_trade,
            fixed_lots=self.cfg.fno_fixed_lots,
            max_lots=self.cfg.fno_max_lots,
            lot_size=contract.lot_size,
            limit_price=limit_price,
        )
        if not sizing.is_valid:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        _, avail_funds, _ = self.broker.get_fund_limits()
        risk_passed, risk_reason = self.risk_manager.validate_order_risk(sizing.total_cost, avail_funds)
        if not risk_passed:
            self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", risk_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=risk_reason, signal_type=SignalType.OPTION, symbol=sig.symbol)

        sig_id = self.db.record_signal(sig.channel_id, sig.message_id, "OPTION", sig.symbol, "BUY_ENTRY", sig.raw_text, "ACCEPTED", "Passed all validations")
        self.db.record_validation(sig_id, sig.symbol, limit_price, sizing.units, sizing.total_quantity, sizing.total_cost, True, "Approved")

        correlation_id = f"OPT-{sig.symbol}-{int(now)}"
        order_req = OrderRequest(
            correlation_id=correlation_id,
            symbol=contract.trading_symbol or f"{sig.symbol}-{sig.strike:g}{sig.option_type}",
            security_id=contract.security_id,
            exchange_segment=contract.exchange_segment,
            transaction_type=TransactionType.BUY,
            product_type=ProductType.MARGIN,
            order_type=OrderType.LIMIT,
            price=limit_price,
            quantity=sizing.total_quantity,
            trading_mode=self.cfg.trading_mode,
        )

        if self.cfg.trading_mode == TradingMode.TEST:
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
            
            log_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="TEST Mode: Validated & Sized (No broker order sent)",
                dhan_order_details=f"[TEST Mode / Qty: {sizing.total_quantity} ({sizing.units} lots) / Limit: {limit_price:.2f}]",
                heads_up=heads_up_note or "TEST Mode active - zero broker orders placed",
                is_live=False,
                is_processed=True,
                timestamp=now,
            )
            self.notifier.send_log_message(log_card)

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
            )
        else:
            ok, broker_id, msg = self.broker.place_order(order_req)
            status = "PENDING" if ok else "REJECTED"
            order_details = f"[Order ID: {broker_id} / Status: {status}]" if broker_id else f"[Rejected: {msg}]"
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

            log_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="FNO",
                status="Message sent to Dhan" if ok else f"Order Failed: {msg}",
                dhan_order_details=order_details,
                heads_up=heads_up_note,
                is_live=True,
                is_processed=ok,
                timestamp=now,
            )
            self.notifier.send_log_message(log_card)

            return ValidationResult(
                is_valid=ok,
                action=SignalAction.BUY_ENTRY,
                reason=msg,
                signal_type=SignalType.OPTION,
                symbol=sig.symbol,
                security_id=contract.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
            )

    def _handle_equity_signal(self, sig, now: float, source_name: str) -> ValidationResult:
        """Processes a validated cash equity signal with exact visual log output."""
        is_live = (self.cfg.trading_mode == TradingMode.LIVE)

        if not self.cfg.equity_active:
            reason = "Equity Cash processing is currently INACTIVE in configuration"
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED", reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        is_dup, dup_reason = self.dedup.is_duplicate(sig.symbol, sig.channel_id, sig.message_id, event_time=now)
        if is_dup:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "DUPLICATE", dup_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=dup_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        inst, res_reason = self.resolver.resolve_equity(sig.symbol)
        heads_up_note = None
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
            self.notifier.send_log_message(rej_card)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=res_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        ref_price = sig.stated_price
        if ref_price <= 0:
            ok, ltp, _ = self.broker.get_ltp(inst.exchange_segment, inst.security_id)
            ref_price = ltp if ok and ltp > 0 else 1000.0

        limit_price = calculate_equity_limit_price(ref_price, self.cfg.equity_price_markup_pct, inst.tick_size)

        sizing = size_equity(
            sizing_mode=self.cfg.equity_sizing_mode,
            budget_inr=self.cfg.equity_budget_inr,
            fixed_qty=self.cfg.equity_fixed_qty,
            limit_price=limit_price,
        )
        if not sizing.is_valid:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_SIZE", sizing.error_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=sizing.error_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        _, avail_funds, _ = self.broker.get_fund_limits()
        risk_passed, risk_reason = self.risk_manager.validate_order_risk(sizing.total_cost, avail_funds)
        if not risk_passed:
            self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BLOCKED", sig.raw_text, "REJECTED_RISK", risk_reason)
            return ValidationResult(is_valid=False, action=SignalAction.BLOCKED, reason=risk_reason, signal_type=SignalType.EQUITY, symbol=sig.symbol)

        sig_id = self.db.record_signal(sig.channel_id, sig.message_id, "EQUITY", sig.symbol, "BUY_ENTRY", sig.raw_text, "ACCEPTED", "Passed all validations")
        self.db.record_validation(sig_id, sig.symbol, limit_price, sizing.units, sizing.total_quantity, sizing.total_cost, True, "Approved")

        correlation_id = f"EQ-{sig.symbol}-{int(now)}"
        order_req = OrderRequest(
            correlation_id=correlation_id,
            symbol=f"{sig.symbol}-EQ",
            security_id=inst.security_id,
            exchange_segment=inst.exchange_segment,
            transaction_type=TransactionType.BUY,
            product_type=self.cfg.equity_product_type,
            order_type=OrderType.LIMIT,
            price=limit_price,
            quantity=sizing.total_quantity,
            trading_mode=self.cfg.trading_mode,
        )

        if self.cfg.trading_mode == TradingMode.TEST:
            self.db.record_order(
                correlation_id=correlation_id,
                trading_mode="TEST",
                product_type=self.cfg.equity_product_type.value,
                exchange_segment=inst.exchange_segment,
                symbol=sig.symbol,
                security_id=inst.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status="TEST_VALIDATED",
            )
            
            log_card = format_trade_log(
                symbol=sig.symbol,
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
            self.notifier.send_log_message(log_card)

            return ValidationResult(
                is_valid=True,
                action=SignalAction.BUY_ENTRY,
                reason="TEST Mode: Signal validated, share count sized, limit price computed. Zero live orders placed.",
                signal_type=SignalType.EQUITY,
                symbol=sig.symbol,
                security_id=inst.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
            )
        else:
            ok, broker_id, msg = self.broker.place_order(order_req)
            status = "PENDING" if ok else "REJECTED"
            order_details = f"[Order ID: {broker_id} / Status: {status}]" if broker_id else f"[Rejected: {msg}]"
            self.db.record_order(
                correlation_id=correlation_id,
                broker_order_id=broker_id,
                trading_mode="LIVE",
                product_type=self.cfg.equity_product_type.value,
                exchange_segment=inst.exchange_segment,
                symbol=sig.symbol,
                security_id=inst.security_id,
                order_type=OrderType.LIMIT.value,
                price=limit_price,
                quantity=sizing.total_quantity,
                status=status,
            )

            log_card = format_trade_log(
                symbol=sig.symbol,
                source_name=source_name,
                original_message=sig.raw_text,
                segment="EQUITY",
                status="Message sent to Dhan" if ok else f"Order Failed: {msg}",
                dhan_order_details=order_details,
                heads_up=heads_up_note,
                is_live=True,
                is_processed=ok,
                timestamp=now,
            )
            self.notifier.send_log_message(log_card)

            return ValidationResult(
                is_valid=ok,
                action=SignalAction.BUY_ENTRY,
                reason=msg,
                signal_type=SignalType.EQUITY,
                symbol=sig.symbol,
                security_id=inst.security_id,
                limit_price=limit_price,
                quantity_or_lots=sizing.units,
                calculated_exposure=sizing.total_cost,
            )


async def start_telegram_listener(pipeline: TradingBotPipeline):
    cfg = pipeline.cfg
    logger.info("Initializing Telegram Client Listener...")

    if not cfg.tg_api_id or not cfg.tg_api_hash:
        logger.error("TG_API_ID or TG_API_HASH not configured in .env!")
        return

    session_file = os.path.join(cfg.data_dir, "tg_userbot.session")
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession

    session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else session_file
    client = TelegramClient(session, cfg.tg_api_id, cfg.tg_api_hash)

    await client.start()
    me = await client.get_me()
    logger.info(f"Connected to Telegram as: {me.first_name} (@{me.username or 'No username'})")
    logger.info(f"Operating Mode: {cfg.trading_mode.value}")
    logger.info(f"Monitored Channels: {cfg.monitored_channel_ids}")

    @client.on(events.NewMessage(chats=cfg.monitored_channel_ids or None))
    async def on_new_message(event):
        if getattr(event.message, 'edit_date', None):
            return

        text = event.message.message or ''
        if not text.strip():
            return

        chat = await event.get_chat()
        source_name = getattr(chat, 'title', str(event.chat_id))
        msg_ts = event.message.date.timestamp() if hasattr(event.message, 'date') and event.message.date else time.time()

        logger.info(f"Incoming message from '{source_name}' ({event.chat_id}): {text[:60]}...")
        result = pipeline.process_raw_message(
            text=text,
            channel_id=event.chat_id,
            message_id=event.id,
            msg_timestamp=msg_ts,
            source_name=source_name,
        )
        logger.info(f"Evaluation outcome: is_valid={result.is_valid}, action={result.action.value}, reason={result.reason}")

    print(f"\n🚀 Bot is actively listening for trade signals in {cfg.trading_mode.value} mode...")
    print(f"📡 Monitored channels: {cfg.monitored_channel_ids}")
    print(f"📊 Logging destination: {cfg.log_group_id}\n")

    await client.run_until_disconnected()


def run():
    import sys
    import asyncio
    cfg = AppConfig.from_env()

    if len(sys.argv) > 1 and sys.argv[1] in ('--login', 'login'):
        from src.tg_login import interactive_login
        asyncio.run(interactive_login(cfg))
        return

    pipeline = TradingBotPipeline(cfg)
    asyncio.run(start_telegram_listener(pipeline))


if __name__ == '__main__':
    run()
