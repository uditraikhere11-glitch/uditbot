"""
Standalone Production Runner for Audit bot.
Implements STEP 05 (strict source allowlist), STEP 15 (startup reconciliation),
STEP 16 (durable asynchronous outbox worker without synchronous handler blocking),
STEP 23 (singleton process lease lock), and V13 Audit Hardening:
- V12-01 Fix: Does not duplicate scrip master load already performed by `TradingBotPipeline(cfg)`.
- V12-06 Fix: Passes `is_lifecycle_open` to `pipeline.watcher.run()` so fill/protection monitoring
  continues past the 15:25 fresh-entry cutoff.
- V12-14 Fix: Offloads synchronous `process_raw_message` and `dispatch_notification` calls via `asyncio.to_thread`.
- V12-18 Fix: Configures structured `INFO` logging and falls back to the authenticated Telethon client
  for log-group delivery if `TG_BOT_TOKEN` is not configured.
"""

import logging
import os
import sys
import time
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# 1. Directly parse .env into os.environ
env_paths = [".env", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")]
for p in env_paths:
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if not (v.startswith('"') and v.endswith('"')) and not (v.startswith("'") and v.endswith("'")):
                            v = v.split("#")[0].strip()
                        v = v.strip("'\"")
                        if k:
                            os.environ[k] = v
            break
        except Exception:
            pass

from src.config import AppConfig

# P0 Guard: If invoked with a CLI command (e.g. daily_health_check), route to src.cli without starting listener
if len(sys.argv) > 1 and sys.argv[1] in (
    "daily_health_check",
    "daily-health-check",
    "health-check",
    "status",
    "dashboard",
    "check-dhan",
    "login",
    "refresh-token",
):
    from src.cli import main as _cli_main
    _cli_main()
    sys.exit(0)

from src.main import TradingBotPipeline
from src.engine.market import is_lifecycle_open
from src.engine.lock import SingletonListenerLock
from src.engine.telegram_event_metadata import source_name_for_event
from src.engine.deterministic_preparse import prepare_deterministic_buy
from src.engine.ordered_ingress import OrderedSignalDispatcher
from src.notifier.outbox_worker import OutboxWorker
from src.notifier.dhan_response import configure_dhan_response_group, send_telethon_notification
from src.engine.config_manager import compute_config_version
from src.notifier.formatter import format_trade_log

cfg = AppConfig.from_env()

tg_api_id = cfg.tg_api_id or int(os.environ.get("TG_API_ID", "0") or os.environ.get("API_ID", "0") or "0")
tg_api_hash = cfg.tg_api_hash or os.environ.get("TG_API_HASH", "") or os.environ.get("API_HASH", "")

if not tg_api_id or not tg_api_hash:
    print(f"Error: TG_API_ID ({tg_api_id}) or TG_API_HASH is missing.")
    print("Please check your .env file with: grep API .env")
    sys.exit(1)

os.makedirs(cfg.data_dir, exist_ok=True)
session_file = os.path.join(cfg.data_dir, "tg_userbot.session")

# STEP 23: Acquire exclusive singleton lease to prevent duplicate runners
lock = SingletonListenerLock(os.path.join(cfg.data_dir, "listener.lock"))
acquired, lock_msg = lock.acquire()
if not acquired:
    print(f"\n❌ STARTUP ABORTED: {lock_msg}")
    print("Another instance of Audit bot is currently active on this machine.")
    print("To avoid duplicate order executions, only ONE listener instance is permitted.")
    sys.exit(1)

print("=" * 60)
print(f"🚀 STARTING AUDIT BOT IN {cfg.trading_mode.value} MODE")
print(f"📡 Monitored channels: {cfg.monitored_channel_ids}")
print(f"📊 Logging destination: {cfg.log_group_id}")
print(f"🔒 Session file: {session_file}")
print("=" * 60)

# V12-01: TradingBotPipeline.__init__ loads scrip_master.csv once into pipeline.resolver;
# only reload if resolver is still empty.
pipeline = TradingBotPipeline(cfg, service_process=True)
scrip_file = os.path.join(cfg.data_dir, "scrip_master.csv")
if not pipeline.resolver._equities and not pipeline.resolver._options_list:
    pipeline.resolver.load_dhan_master(scrip_file)
print("=" * 60 + "\n")

session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else session_file


async def main():
    outbox_worker = None
    ordered_dispatcher = None
    ingress_audit = None
    client = None
    try:
        # Ingress crash audit is opt-in AND requires the one-worker FIFO.
        # No automatic replay after a crashed order submission is allowed.
        if ("os" in globals()
                and os.environ.get("AUDITBOT_DURABLE_INGRESS_AUDIT", "0") == "1"):
            if os.environ.get("AUDITBOT_ORDERED_INGRESS", "0") != "1":
                raise RuntimeError("Durable ingress audit requires ordered ingress")
            from src.engine.durable_ingress_audit import DurableIngressAudit
            account_id = str(getattr(pipeline.db, "account_id", "") or "")
            if not account_id or account_id != str(getattr(cfg, "dhan_client_id", "") or ""):
                raise RuntimeError("Durable ingress account identity not verified")
            ingress_audit = DurableIngressAudit(
                os.path.join(cfg.data_dir, "signals.ingress-audit.sqlite3"),
                account_id=account_id,
            )
            existing = ingress_audit.summary()
            if existing["requires_manual_reconciliation"]:
                pipeline.reconciliation_gate.halt_new_entries(
                    "INGRESS_AUDIT_UNRETURNED_SIGNAL_REQUIRES_MANUAL_BROKER_REVIEW"
                )
                raise RuntimeError(
                    "Unreturned inbound signals exist: broker and journal review required; "
                    "automatic replay prohibited"
                )
        # Construct Telethon inside asyncio.run's *running* event loop.
        # Python 3.14 no longer implicitly creates a loop for get_event_loop().
        client = TelegramClient(session, tg_api_id, tg_api_hash)
        allowed_chats = list(cfg.monitored_channel_ids)
        if cfg.trading_mode.value != 'LIVE' and cfg.test_group_id and cfg.test_group_id not in allowed_chats:
            allowed_chats.append(cfg.test_group_id)

        # STEP 05: Reject empty source allowlist before connecting
        if not allowed_chats:
            print("❌ STARTUP ABORTED: MONITORED_CHANNEL_IDS is empty. Refusing to listen to all chats.")
            sys.exit(1)

        await client.start()
        me = await client.get_me()
        print(f"✅ Connected to Telegram as: {me.first_name} (@{me.username or 'No username'})")

        # STEP 15: Run startup reconciliation gate before accepting entries
        rec_ok, rec_msg = await asyncio.to_thread(pipeline.reconciliation_gate.run_startup_reconciliation)
        if rec_ok:
            print(f"✅ Startup Reconciliation: {rec_msg}")
        else:
            print(f"⚠️ Startup Reconciliation: {rec_msg}")

        # Configure the actual production listener, not src.main's alternate listener.
        # Resolve a joined group before enabling mirrored broker notifications.
        loop = asyncio.get_running_loop()
        dhan_response_dispatch_fn = await configure_dhan_response_group(
            client=client,
            group_name=cfg.dhan_response_group_name,
            signal_source_ids=allowed_chats,
            reporter=getattr(pipeline.watcher, "execution_reporter", None),
            loop=loop,
            data_dir=getattr(cfg, "data_dir", "data"),
            config_version_hash=compute_config_version(cfg) if hasattr(cfg, "data_dir") else "",
        )

        # STEP 14 & V12-06: Start persistent Protection Watcher background task using is_lifecycle_open
        asyncio.create_task(pipeline.watcher.run(is_lifecycle_open))
        print("🛡️ Persistent Protection Watcher activated (monitoring resting fills & OCO/Super legs through market close)")

        def _dispatch_notification_sync(formatted_text: str) -> bool:
            if pipeline.notifier.enabled:
                return pipeline.notifier.send_log_message(formatted_text, outbox=True)
            if cfg.log_group_id:
                return send_telethon_notification(client, cfg.log_group_id, formatted_text, loop, timeout=8.0)
            return False

        # Broker-native F&O AMO submits immediately while market is closed.
        # Legacy locally queued signals are never replayed into a new order route.

        # STEP 16: Start durable Outbox Worker background task
        outbox_worker = OutboxWorker(
            journal_db=pipeline.db,
            dispatch_fn=_dispatch_notification_sync,
            dhan_response_dispatch_fn=dhan_response_dispatch_fn,
        )
        asyncio.create_task(outbox_worker.run())
        print("📬 Durable Outbox Worker activated (non-blocking notification pipeline)")

        # Enqueue startup readiness card via durable outbox (H14: separate listener, entry gate, reconciliation & protection)
        if cfg.log_group_id:
            from src.cli import format_startup_online_card
            pending_cnt = len(pipeline.db.get_non_terminal_intents()) if hasattr(pipeline.db, "get_non_terminal_intents") else 0
            unprot_cnt = len(pipeline.db.get_unprotected_orders()) if hasattr(pipeline.db, "get_unprotected_orders") else 0
            startup_card = format_startup_online_card(
                cfg,
                allowed_chats_count=len(allowed_chats),
                reconciliation_ok=rec_ok,
                reconciliation_msg=rec_msg,
                unresolved_intents_count=pending_cnt,
                unprotected_positions_count=unprot_cnt,
                is_stopped=pipeline.stop_manager.is_stopped(),
            )
            await asyncio.to_thread(pipeline.dispatch_notification, "SYSTEM_STARTUP", startup_card)

        # OFF by default. A bounded single worker preserves UditBot's sole
        # serialized order authority and prevents unbounded executor queuing.
        # Compatibility with isolated runner tests that compile main() with
        # deliberately restricted globals and no os module. In that context
        # the optional feature remains OFF, as it does by default in service.
        if "os" in globals() and os.environ.get("AUDITBOT_ORDERED_INGRESS", "0") == "1":
            pending_bound = int(os.environ.get(
                "AUDITBOT_ORDERED_INGRESS_MAX_PENDING", "64"
            ))
            on_returned = None
            if ingress_audit is not None:
                def on_returned(kw):
                    try:
                        ingress_audit.mark_pipeline_returned(
                            channel_id=kw["channel_id"],
                            message_id=kw["message_id"],
                            text=kw["text"],
                            message_timestamp=kw["msg_timestamp"],
                        )
                    except Exception:
                        pipeline.reconciliation_gate.halt_new_entries(
                            "INGRESS_AUDIT_PIPELINE_RETURN_PERSISTENCE_UNVERIFIED"
                        )
                        raise
            ordered_dispatcher = OrderedSignalDispatcher(
                pipeline.process_raw_message, max_pending=pending_bound,
                on_pipeline_returned=on_returned,
            )
            await ordered_dispatcher.start()
            print("📥 Ordered ingress enabled (single broker-execution authority)")
        print(f"\n⚡ Listening for live signals on {len(allowed_chats)} authorized channels...\n")

        @client.on(events.NewMessage(chats=allowed_chats))
        async def handler(event):
            callback_monotonic_ns = time.perf_counter_ns()
            received_at = time.time()  # event callback arrival, before chat lookup / to_thread
            if event.chat_id not in allowed_chats:
                return
            if getattr(event.message, "edit_date", None):
                return
            text = event.message.message or ""
            if not text.strip():
                return
            # Capture durable ingress identity BEFORE the first await in this
            # callback. The record contains a SHA-256 digest only, not raw
            # signal text. A duplicate or edited identity must NEVER resubmit.
            msg_ts = event.message.date.timestamp() if hasattr(event.message, "date") and event.message.date else time.time()
            if ingress_audit is not None:
                try:
                    admission = ingress_audit.admit(
                        channel_id=event.chat_id, message_id=event.id,
                        text=text, message_timestamp=msg_ts,
                    )
                except Exception:
                    pipeline.reconciliation_gate.halt_new_entries(
                        "INGRESS_AUDIT_ADMISSION_UNVERIFIED"
                    )
                    raise RuntimeError(
                        "Inbound signal audit failed; live entry gate halted"
                    ) from None
                if admission != "ADMITTED":
                    logging.getLogger("trading_bot").warning(
                        "Duplicate inbound signal withheld; manual review if unresolved"
                    )
                    return
            # Use Telethon's already-cached chat whenever present; preserve
            # the original async lookup path when it is not yet cached.
            source_name = await source_name_for_event(event)

            print(f"📩 [{source_name}] New Signal: {text[:80]}")
            # Default OFF. Only deterministic REGEX BUY signals are prepared
            # concurrently. No AI, DB, broker, STOP, risk or journal side
            # effects occur outside the original serialized trading pipeline.
            preparse = None
            # Independent experiments: disable speculative preparse when
            # ordered ingress is enabled, to avoid completion-order reordering.
            if (ordered_dispatcher is None and
                    os.environ.get("AUDITBOT_DETERMINISTIC_PREPARSE", "0") == "1"):
                try:
                    preparse = await asyncio.to_thread(
                        prepare_deterministic_buy,
                        text=text,
                        channel_id=event.chat_id,
                        message_id=event.id,
                        msg_timestamp=msg_ts,
                        max_age_sec=cfg.max_message_age_sec,
                    )
                except Exception:
                    # Optional prework failure must never drop a signal.
                    logging.getLogger("trading_bot").warning(
                        "Optional deterministic preparse failed; using canonical parser",
                        exc_info=False,
                    )
            # Both paths preserve canonical claim/risk/broker serialization.
            # A bounded FIFO worker is optional and never silently drops.
            execution_kwargs = dict(
                text=text,
                channel_id=event.chat_id,
                message_id=event.id,
                msg_timestamp=msg_ts,
                source_name=source_name,
                signal_received_at=received_at,
                callback_monotonic_ns=callback_monotonic_ns,
                preparsed_brain=preparse,
            )
            if ordered_dispatcher is not None:
                result = await ordered_dispatcher.submit(**execution_kwargs)
            else:
                result = await asyncio.to_thread(
                    pipeline.process_raw_message, **execution_kwargs
                )
            print(f"🤖 Outcome: valid={result.is_valid} | action={result.action.value} | reason={result.reason}")

            if cfg.log_group_id and not getattr(result, "log_card", None):
                seg = result.signal_type.value if result.signal_type else "NONE"
                sym = result.symbol or "NONE"
                unit_label = "lots" if seg == "OPTION" else "shares"
                dhan_details = (
                    f"[TEST Mode / Qty: {result.quantity_or_lots} {unit_label} / Limit: {result.limit_price:.2f}]"
                    if result.is_valid else "None (Filtered/Blocked)"
                )
                heads_up_note = None if result.is_valid else result.reason
                card = format_trade_log(
                    symbol=sym,
                    source_name=source_name,
                    original_message=text,
                    segment=seg,
                    status=result.reason,
                    dhan_order_details=dhan_details,
                    heads_up=heads_up_note,
                    is_live=(cfg.trading_mode.value == "LIVE"),
                    is_processed=result.is_valid,
                    timestamp=msg_ts,
                )
                await asyncio.to_thread(pipeline.dispatch_notification, "TRADE_LOG", card)

        @client.on(events.MessageEdited(chats=allowed_chats))
        async def edit_handler(event):
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
            # An edited source signal must never remain staged for next open.
            try:
                await asyncio.to_thread(
                    pipeline._get_fno_amo_queue().cancel_queued,
                    f"{event.chat_id}:{event.id}",
                )
            except Exception:
                logging.getLogger("run").exception(
                    "F&O AMO edit cancellation failed closed; operator review required"
                )

        await client.run_until_disconnected()
    finally:
        try:
            if ordered_dispatcher is not None:
                await ordered_dispatcher.close()
                logging.getLogger("trading_bot").info(
                    "ORDERED_INGRESS_SUMMARY %s", ordered_dispatcher.metrics()
                )
        finally:
            try:
                if outbox_worker:
                    outbox_worker.stop()
            finally:
                try:
                    if client is not None:
                        await client.disconnect()
                finally:
                    try:
                        if ingress_audit is not None:
                            ingress_audit.close()
                    finally:
                        lock.release()


if __name__ == "__main__":
    asyncio.run(main())
