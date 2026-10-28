"""
Standalone Production Runner for UditBot / Audit bot.
Directly loads .env, connects authenticated Telethon session,
downloads/loads Dhan scrip master, runs background persistent protection watcher,
listens for live signals, evaluates rules, and dispatches visual formatted cards
directly to Telegram Log Group via Telethon client.
"""

import os
import sys
import time
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# 1. Directly parse .env into os.environ
env_paths = [".env", os.path.join(os.path.dirname(__file__), ".env")]
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
from src.engine.market import is_market_open
from src.notifier.formatter import format_trade_log, format_exit_log

cfg = AppConfig.from_env()

tg_api_id = cfg.tg_api_id or int(os.environ.get("TG_API_ID", "0") or os.environ.get("API_ID", "0") or "0")
tg_api_hash = cfg.tg_api_hash or os.environ.get("TG_API_HASH", "") or os.environ.get("API_HASH", "")

if not tg_api_id or not tg_api_hash:
    print(f"Error: TG_API_ID ({tg_api_id}) or TG_API_HASH is missing.")
    print("Please check your .env file with: grep API .env")
    sys.exit(1)

os.makedirs(cfg.data_dir, exist_ok=True)
session_file = os.path.join(cfg.data_dir, "tg_userbot.session")

print("=" * 60)
print(f"🚀 STARTING AUDIT BOT IN {cfg.trading_mode.value} MODE")
print(f"📡 Monitored channels: {cfg.monitored_channel_ids}")
print(f"📊 Logging destination: {cfg.log_group_id}")
print(f"🔒 Session file: {session_file}")
print("=" * 60)

pipeline = TradingBotPipeline(cfg)

# Download / Load Dhan Scrip Master Catalog
scrip_file = os.path.join(cfg.data_dir, "scrip_master.csv")
pipeline.resolver.load_dhan_master(scrip_file)
print("=" * 60 + "\n")

session = StringSession(cfg.tg_session_string) if cfg.tg_session_string else session_file
client = TelegramClient(session, tg_api_id, tg_api_hash)


async def main():
    await client.start()
    me = await client.get_me()
    print(f"✅ Connected to Telegram as: {me.first_name} (@{me.username or 'No username'})")

    # Start persistent Protection Watcher background task
    asyncio.create_task(pipeline.watcher.run(is_market_open))
    print("🛡️ Persistent Protection Watcher activated (monitoring resting fills & OCO legs)")

    # Verify Log Group connectivity on startup
    if cfg.log_group_id:
        try:
            startup_card = (
                f"🟢 <b>{getattr(cfg, 'instance_name', 'Audit bot')} Online</b>\n"
                f"Mode: <code>{cfg.trading_mode.value}</code>\n"
                f"Monitoring: {len(cfg.monitored_channel_ids)} channels\n"
                f"Protection Watcher: Active\n"
                f"Status: Actively listening for trade signals"
            )
            await client.send_message(cfg.log_group_id, startup_card, parse_mode="html")
            print(f"✅ Log Group Verified: Sent online confirmation to {cfg.log_group_id}")
        except Exception as e:
            print(f"⚠️ Log Group Warning: Could not send test message to {cfg.log_group_id}: {e}")
            print(f"   (Please verify that your Telegram account @{me.username} is a member/admin in group {cfg.log_group_id})")

    print(f"\n⚡ Listening for live signals on {len(cfg.monitored_channel_ids)} channels...\n")

    @client.on(events.NewMessage(chats=cfg.monitored_channel_ids or None))
    async def handler(event):
        if getattr(event.message, "edit_date", None):
            return
        text = event.message.message or ""
        if not text.strip():
            return
        chat = await event.get_chat()
        source_name = getattr(chat, "title", str(event.chat_id))
        msg_ts = event.message.date.timestamp() if hasattr(event.message, "date") and event.message.date else time.time()

        print(f"📩 [{source_name}] New Signal: {text[:80]}")
        result = pipeline.process_raw_message(
            text=text,
            channel_id=event.chat_id,
            message_id=event.id,
            msg_timestamp=msg_ts,
            source_name=source_name,
        )
        print(f"🤖 Outcome: valid={result.is_valid} | action={result.action.value} | reason={result.reason}")

        # Send visual card directly to Log Group for all evaluated messages
        if cfg.log_group_id:
            card = getattr(result, "log_card", None)
            if not card:
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

            try:
                await client.send_message(cfg.log_group_id, card)
                print(f"📨 Dispatched visual card to Telegram Log Group ({cfg.log_group_id})\n")
            except Exception as e:
                print(f"⚠️ Failed to send log card to {cfg.log_group_id}: {e}\n")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
