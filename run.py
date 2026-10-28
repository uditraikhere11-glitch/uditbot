"""
Standalone Production Runner for UditBot.
Directly loads .env, connects authenticated Telethon session,
downloads/loads Dhan scrip master, listens for live signals, evaluates rules,
and dispatches visual formatted cards directly to Telegram Log Group via Telethon client.
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
print(f"🚀 STARTING UDITBOT IN {cfg.trading_mode.value} MODE")
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

    # Verify Log Group connectivity on startup
    if cfg.log_group_id:
        try:
            startup_card = (
                f"🟢 <b>{getattr(cfg, 'instance_name', 'UditBot')} Online</b>\n"
                f"Mode: <code>{cfg.trading_mode.value}</code>\n"
                f"Monitoring: {len(cfg.monitored_channel_ids)} channels\n"
                f"Status: Actively listening for trade signals"
            )
            await client.send_message(cfg.log_group_id, startup_card, parse_mode="html")
            print(f"✅ Log Group Verified: Sent online confirmation to {cfg.log_group_id}")
        except Exception as e:
            print(f"⚠️ Log Group Warning: Could not send message to {cfg.log_group_id}: {e}")
            print(f"   (Please verify that your Telegram account @{me.username} is an admin or has permission to post in that group)")

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

        # Dispatch visual card directly to Log Group using active Telegram client
        card = getattr(result, "log_card", None)
        if cfg.log_group_id and card:
            try:
                await client.send_message(cfg.log_group_id, card)
                print(f"📨 Dispatched visual card to Telegram Log Group ({cfg.log_group_id})\n")
            except Exception as e:
                print(f"⚠️ Failed to send log card to {cfg.log_group_id}: {e}\n")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())