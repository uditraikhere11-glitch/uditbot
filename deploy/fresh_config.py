"""Private, interactive first-run credential configuration for a NEW AuditBot VM.

Creates only a fresh .env and optional ~/.dhan_token. Never overwrites existing
credentials; refuses nonempty trading runtime; never generates broker orders.
"""
from __future__ import annotations
import argparse
import getpass
import os
from pathlib import Path
import re
import sys


class SetupError(Exception):
    pass


def ask(label: str, *, secret: bool = False, optional: bool = False) -> str:
    fn = getpass.getpass if secret else input
    while True:
        value = fn(f"{label}{' (optional)' if optional else ''}: ").strip()
        if value or optional:
            return value
        print("Required; please enter a value.")


def safe_value(value: str) -> str:
    if any(c in value for c in ('\n', '\r', '\x00', '#', '"', "'", '\\')):
        raise SetupError("Unsupported character in configuration field")
    return value


def channel_ids(value: str, *, required: bool = False) -> str:
    parts = [p.strip() for p in value.split(',') if p.strip()]
    if required and not parts:
        raise SetupError("At least one authorized Telegram source is required")
    if not all(re.fullmatch(r'-?[0-9]{1,20}', p) and int(p) != 0 for p in parts):
        raise SetupError("Telegram source IDs must be nonzero signed integers")
    if len(set(parts)) != len(parts):
        raise SetupError("Duplicate source ID")
    return ','.join(parts)


def optional_chat_id(value: str) -> str:
    if not value:
        return '0'
    answer = channel_ids(value)
    if ',' in answer:
        raise SetupError("This field needs exactly one group ID")
    return answer


def numeric_id(value: str, name: str, low: int, high: int) -> str:
    if not re.fullmatch(rf'[0-9]{{{low},{high}}}', value) or int(value) <= 0:
        raise SetupError(f"{name} must be {low}-{high} digits and nonzero")
    return value


def create_config(root: Path, home: Path) -> None:
    root, home = root.resolve(), home.resolve()
    env = root / '.env'
    token_path = home / '.dhan_token'
    if env.exists() or env.is_symlink():
        raise SetupError("Refusing to overwrite existing .env")
    if token_path.exists() or token_path.is_symlink():
        raise SetupError("Refusing to overwrite existing Dhan token")
    data = root / 'data'
    if data.exists():
        if not data.is_dir() or any(p.name != 'holidays.json' for p in data.iterdir()):
            raise SetupError("Existing runtime data detected; only a NEW VM is supported")

    print("\nNEW AUDITBOT — ZERO HISTORY — TEST MODE")
    print("Secrets stay on this VM; nothing is uploaded to GitHub.")
    print("Do not use the same Dhan account on two trading VMs without reconciliation.\n")
    operator = ask("Operator name")
    instance = ask("Bot instance name")
    for name in (operator, instance):
        if not re.fullmatch(r'[A-Za-z0-9 _-]{1,48}', name):
            raise SetupError("Names may contain letters, numbers, spaces, _ or -")
    tg_api_id = numeric_id(ask("Telegram API ID"), "Telegram API ID", 1, 15)
    tg_api_hash = ask("Telegram API hash", secret=True)
    if not re.fullmatch(r'[A-Fa-f0-9]{32}', tg_api_hash):
        raise SetupError("Telegram API hash must be 32 hexadecimal characters")
    monitored = channel_ids(ask("Authorized MONITORED channel IDs (comma-separated)"), required=True)
    log_group = optional_chat_id(ask("Telegram LOG group ID", optional=True))
    test_group = optional_chat_id(ask("Telegram TEST group ID", optional=True))
    phone = ask("Telegram phone (+countrycode and number)", optional=True)
    if phone and not re.fullmatch(r'\+[1-9][0-9]{7,14}', phone):
        raise SetupError("Phone must be in +countrycode format")
    session = ask("Own portable Telegram StringSession, or blank for later OTP login", secret=True, optional=True)
    if session and (not session.startswith('1') or len(session) < 100 or not re.fullmatch(r'[A-Za-z0-9_=-]+', session)):
        raise SetupError("Invalid portable Telegram session string")
    bot_token = ask("Telegram Bot API token for notifications", secret=True, optional=True)
    if bot_token and not re.fullmatch(r'[0-9]+:[A-Za-z0-9_-]{20,}', bot_token):
        raise SetupError("Malformed Telegram bot token")

    dhan_id = numeric_id(ask("Dhan client ID"), "Dhan client ID", 8, 15)
    pin = ask("Dhan six-digit PIN", secret=True, optional=True)
    totp = ask("Dhan TOTP secret", secret=True, optional=True)
    token = ask("Current Dhan access token, or blank if using PIN + TOTP", secret=True, optional=True)
    if bool(pin) != bool(totp):
        raise SetupError("Dhan PIN and TOTP must be provided together")
    if pin and not re.fullmatch(r'[0-9]{6}', pin):
        raise SetupError("Dhan PIN must be six digits")
    if totp and not re.fullmatch(r'[A-Za-z2-7=]{16,128}', totp):
        raise SetupError("Dhan TOTP secret has invalid format")
    if not token and not (pin and totp):
        raise SetupError("Provide a token or PIN and TOTP")

    config = {
        'OPERATOR_NAME': operator, 'BOT_INSTANCE_NAME': instance,
        'TRADING_MODE': 'TEST', 'EQUITY_ACTIVE': 'true', 'FNO_ACTIVE': 'true',
        'EQUITY_AMO_ENABLED': 'false', 'FNO_AMO_ENABLED': 'false',
        'DHAN_CLIENT_ID': dhan_id, 'DHAN_PIN': pin, 'DHAN_TOTP_SECRET': totp,
        'DHAN_TOKEN_FILE': str(token_path),
        'TG_API_ID': tg_api_id, 'TG_API_HASH': tg_api_hash, 'TG_PHONE': phone,
        'TG_SESSION_STRING': session, 'TG_BOT_TOKEN': bot_token,
        'MONITORED_CHANNEL_IDS': monitored, 'LOG_GROUP_ID': log_group,
        'TEST_GROUP_ID': test_group, 'IGNORE_TELEGRAM_BACKLOG_ON_RESTART': 'true',
        'EQUITY_SIZING_MODE': 'BUDGET', 'EQUITY_BUDGET_INR': '500',
        'FNO_SIZING_MODE': 'BUDGET', 'FNO_CAPITAL_PER_TRADE': '20000',
        'FNO_FIXED_LOTS': '1', 'FNO_MAX_LOTS': '1',
        'EQUITY_SL_PCT': '3.0', 'EQUITY_TARGET_PCT': '3.0',
        'FNO_SL_PCT': '3.0', 'FNO_TARGET_PCT': '3.0',
        'MAX_ORDER_AMOUNT_INR': '50000', 'MIN_TRADE_FUNDS_INR': '10000',
        'EQUITY_CMP_EXECUTION_POLICY': 'STRICT_TIMESTAMP',
        'MAX_MESSAGE_AGE_SEC': '60',
    }
    content = '# NEW private AuditBot instance — zero inherited data\n' + ''.join(
        f'{k}={safe_value(v)}\n' for k, v in config.items()
    )
    os.umask(0o077)
    created_env = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(env, flags, 0o600)
        created_env = True
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        os.chmod(env, 0o600)
        if token:
            fd = os.open(token_path, flags, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(safe_value(token) + '\n')
            os.chmod(token_path, 0o600)
    except Exception:
        if created_env:
            env.unlink(missing_ok=True)
        raise
    print("\nPrivate TEST-only .env created; no old journal or Telegram session imported.")
    print("No real broker order or Dhan token refresh was performed.")
    if not token:
        print("If the Dhan account is not already active elsewhere, run ./bot login after installation")
    if not session:
        print("Telegram OTP still required: venv/bin/python -m src.tg_login")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--home', type=Path, default=Path.home())
    args = parser.parse_args()
    if not sys.stdin.isatty():
        print("ERROR: interactive terminal required; never pipe credentials", file=sys.stderr)
        return 2
    try:
        create_config(args.repo, args.home)
    except (SetupError, OSError, EOFError, KeyboardInterrupt) as exc:
        print(f"ERROR: first-run setup failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
