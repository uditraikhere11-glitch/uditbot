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
from src.main import TradingBotPipeline
from src.engine.market import is_lifecycle_open
from src.engine.lock import SingletonListenerLock
from src.notifier.outbox_worker import OutboxWorker
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
pipeline = TradingBotPipeline(cfg)
scrip_file = os.path.join(cfg.data_dir, "scrip_master.csv")
if not pipeline.resolver._equities and not pipeline.resolver._options_list:
    pipeline.resolver.load_dhan_master(scrip_file)
print("=" * 60 + "\n")

session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else session_file
client = TelegramClient(session, tg_api_id, tg_api_hash)


async def main():
    outbox_worker = None
    try:
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

        # STEP 14 & V12-06: Start persistent Protection Watcher background task using is_lifecycle_open
        asyncio.create_task(pipeline.watcher.run(is_lifecycle_open))
        print("🛡️ Persistent Protection Watcher activated (monitoring resting fills & OCO/Super legs through market close)")

        loop = asyncio.get_running_loop()

        def _dispatch_notification_sync(formatted_text: str) -> bool:
            if pipeline.notifier.enabled:
                return pipeline.notifier.send_log_message(formatted_text)
            if cfg.log_group_id:
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        client.send_message(cfg.log_group_id, formatted_text, parse_mode="html"),
                        loop,
                    )
                    fut.result(timeout=8.0)
                    return True
                except Exception as e:
                    logging.getLogger("run").error(f"Telethon log_group fallback delivery failed: {e}")
                    return False
            return False

        # STEP 16: Start durable Outbox Worker background task
        outbox_worker = OutboxWorker(
            journal_db=pipeline.db,
            dispatch_fn=_dispatch_notification_sync,
        )
        asyncio.create_task(outbox_worker.run())
        print("📬 Durable Outbox Worker activated (non-blocking notification pipeline)")

        # Enqueue startup readiness card via durable outbox
        if cfg.log_group_id:
            channel_note = " (monitored channels only)" if cfg.trading_mode.value == "LIVE" else " (including test channel)"
            startup_card = (
                f"🟢 <b>{getattr(cfg, 'instance_name', 'Audit bot')} Online</b>\n"
                f"Mode: <code>{cfg.trading_mode.value}</code>\n"
                f"Monitoring: {len(allowed_chats)} channels{channel_note}\n"
                f"Reconciliation: {'Healthy' if rec_ok else 'Blocked/Warning'}\n"
                f"Protection Watcher: Active\n"
                f"Status: Actively listening for trade signals"
            )
            await asyncio.to_thread(pipeline.dispatch_notification, "SYSTEM_STARTUP", startup_card)

        print(f"\n⚡ Listening for live signals on {len(allowed_chats)} authorized channels...\n")

        @client.on(events.NewMessage(chats=allowed_chats))
        async def handler(event):
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

            print(f"📩 [{source_name}] New Signal: {text[:80]}")
            # V12-14: Offload synchronous pipeline execution to worker thread
            result = await asyncio.to_thread(
                pipeline.process_raw_message,
                text=text,
                channel_id=event.chat_id,
                message_id=event.id,
                msg_timestamp=msg_ts,
                source_name=source_name,
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

        await client.run_until_disconnected()
    finally:
        if outbox_worker:
            outbox_worker.stop()
        lock.release()


if __name__ == "__main__":
    asyncio.run(main())
