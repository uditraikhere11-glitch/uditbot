"""
Interactive Telegram Authentication and Session Setup + Portable StringSession Exporter.
Logs into Telegram account using Phone Number + SMS/Telegram OTP,
saves the session file safely into data/tg_userbot.session (chmod 0600),
and supports exporting an existing SQLite session file to TG_SESSION_STRING in .env
(`python3 -m src.tg_login --export-string`) using pure Python standard library (or Telethon)
so VM rebuilds never require OTP re-registration unless the session is revoked in Telegram.
"""

import asyncio
import base64
import ipaddress
import os
import sqlite3
import struct
import sys
from src.config import AppConfig, load_env_file, resolve_env_path
from src.engine.config_manager import write_env_atomic


def _encode_sqlite_session_to_string(session_file: str) -> str:
    """
    Reads `(dc_id, server_address, port, auth_key)` from a Telethon SQLite `.session` file
    and encodes it into Telethon's canonical `1...` StringSession format using only the
    Python standard library.
    """
    conn = sqlite3.connect(f"file:{session_file}?mode=ro", uri=True, timeout=5.0)
    try:
        row = conn.execute(
            "SELECT dc_id, server_address, port, auth_key FROM sessions LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise ValueError("No session record found in SQLite session file")
    dc_id, server_address, port, auth_key = row
    if not auth_key or len(bytes(auth_key)) != 256:
        raise ValueError("Session file does not contain a valid 256-byte auth_key")
    ip_packed = ipaddress.ip_address(str(server_address)).packed
    fmt = ">B4sH256s" if len(ip_packed) == 4 else ">B16sH256s"
    packed = struct.pack(fmt, int(dc_id), ip_packed, int(port), bytes(auth_key))
    return "1" + base64.urlsafe_b64encode(packed).decode("ascii")


def export_existing_session_to_env(cfg: AppConfig, env_path: str = ".env") -> bool:
    """
    Converts an existing data/tg_userbot.session SQLite file into a Telethon StringSession
    and persists TG_SESSION_STRING into .env (chmod 0600) offline without contacting Telegram
    or printing the secret string to stdout.
    """
    session_file = os.path.join(cfg.data_dir, "tg_userbot.session")
    if not os.path.isfile(session_file):
        print(f"Error: Session file not found at {session_file}")
        return False

    try:
        exported_str = _encode_sqlite_session_to_string(session_file)
    except Exception as e:
        print(f"Error reading SQLite session: {e}")
        return False

    resolved_env = resolve_env_path(env_path)
    existing_lines = []
    if os.path.isfile(resolved_env):
        with open(resolved_env, "r", encoding="utf-8") as f:
            existing_lines = f.read().splitlines()

    updated = False
    new_lines = []
    for line in existing_lines:
        stripped = line.strip()
        if stripped.startswith("TG_SESSION_STRING="):
            new_lines.append(f'TG_SESSION_STRING="{exported_str}"')
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f'TG_SESSION_STRING="{exported_str}"')

    write_env_atomic(resolved_env, "\n".join(new_lines) + "\n")
    print(f"✅ Portable TG_SESSION_STRING exported from {session_file} and saved to {resolved_env} (chmod 0600).")
    return True


async def interactive_login(cfg: AppConfig):
    from telethon import TelegramClient

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
    if "--export-string" in sys.argv or "--export-session" in sys.argv:
        ok = export_existing_session_to_env(cfg)
        sys.exit(0 if ok else 1)
    asyncio.run(interactive_login(cfg))


if __name__ == "__main__":
    main()
