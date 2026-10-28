"""
Interactive Telegram Authentication and Session Setup.
Logs into Telegram account using Phone Number + SMS/Telegram OTP,
and saves the session file safely into data/tg_userbot.session (chmod 0600).
"""

import os
import sys
import asyncio
from telethon import TelegramClient
from src.config import AppConfig, load_env_file


async def interactive_login(cfg: AppConfig):
    print("=" * 60)
    print("       TELEGRAM ACCOUNT AUTHENTICATION SETUP            ")
    print("=" * 60)

    if not cfg.tg_api_id or not cfg.tg_api_hash:
        print("Error: TG_API_ID or TG_API_HASH not found in .env.")
        print("Please check your .env file or run 'python3 -m src.setup_wizard'.")
        return

    os.makedirs(cfg.data_dir, exist_ok=True)
    session_file = os.path.join(cfg.data_dir, "tg_userbot.session")

    print(f"[*] API ID: {cfg.tg_api_id}")
    print(f"[*] Target Session File: {session_file}")
    print("[*] Starting interactive Telegram authentication...")
    print("    (You will be prompted for your phone number and OTP code)\n")

    client = TelegramClient(session_file, cfg.tg_api_id, cfg.tg_api_hash)
    await client.start()

    me = await client.get_me()
    print("\n" + "=" * 60)
    print(f"✅ Logged in successfully as: {me.first_name} (@{me.username or 'No username'})")
    print(f"🔒 Session file saved to: {session_file}")

    try:
        os.chmod(session_file, 0o600)
        print("🔒 Session permissions set to: chmod 0600")
    except Exception:
        pass

    print("=" * 60 + "\n")
    await client.disconnect()


def main():
    load_env_file(".env")
    cfg = AppConfig.from_env()
    asyncio.run(interactive_login(cfg))


if __name__ == "__main__":
    main()
