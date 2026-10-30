"""
System Configuration & Environment Management.
Reads from environment variables and protected .env files with automatic file parsing.
Robust against formatting artifacts (braces, quotes, multi-value inputs).
Implements V13 Audit Hardening:
- Resolves application root (`APP_ROOT`) so CLI/systemd invocations from outside the repo find `.env` and `data/` (`V12-22`).
- Supports `TG_BOT_TOKEN` (`V12-18`), `MAX_DRAWDOWN_PCT` (`V12-23`), and finite/range validation on all numeric settings.
"""

import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from src.models import TradingMode, ProductType

APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolve_env_path(path: str = ".env") -> str:
    """Resolves .env path from explicit path, current working directory, or APP_ROOT."""
    if path and os.path.exists(path):
        return path
    if path == ".env":
        app_env = os.path.join(APP_ROOT, ".env")
        if os.path.exists(app_env):
            return app_env
    return path


def parse_env_dict(path: str = ".env") -> Dict[str, str]:
    """Reads key=value pairs from a .env file without mutating os.environ."""
    resolved = resolve_env_path(path)
    out: Dict[str, str] = {}
    if not resolved or not os.path.exists(resolved):
        return out
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not (v.startswith('"') and v.endswith('"')) and not (v.startswith("'") and v.endswith("'")):
                    v = v.split("#")[0].strip()
                v = v.strip("'\"")
                if k:
                    out[k] = v
    except Exception:
        pass
    return out


def load_env_file(path: str = ".env") -> None:
    """Parses .env file directly into os.environ with zero external dependencies."""
    for k, v in parse_env_dict(path).items():
        os.environ[k] = v


def _bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _clean_int(val: Any, default: int = 0) -> int:
    """Safely parses integers from strings, stripping brackets, braces, and trailing items."""
    if val is None or val == "":
        return default
    cleaned = str(val).strip().strip("{}[]()\"' ")
    if "," in cleaned:
        cleaned = cleaned.split(",")[0].strip().strip("{}[]()\"' ")
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return default


def _clean_float(val: Any, default: float = 0.0) -> float:
    """Safely parses finite floats from strings."""
    if val is None or val == "" or str(val).startswith("extra="):
        return default
    cleaned = str(val).strip().strip("{}[]()\"' ")
    try:
        res = float(cleaned)
        if not math.isfinite(res):
            raise ValueError(f"Non-finite float value: {val}")
        return res
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid numeric configuration value '{val}': {e}") from e


def _int_list(val: Any) -> List[int]:
    """Safely parses a comma-separated list of integers, stripping brackets and braces."""
    if not val:
        return []
    cleaned = str(val).strip().strip("{}[]()\"' ")
    res = []
    for part in cleaned.split(","):
        part = part.strip().strip("{}[]()\"' ")
        if part:
            try:
                res.append(int(part))
            except ValueError:
                pass
    return res


def _default_data_dir() -> str:
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, ".env")) or os.path.exists(os.path.join(cwd, "src")):
        return os.path.join(cwd, "data")
    if os.path.exists(os.path.join(APP_ROOT, ".env")):
        return os.path.join(APP_ROOT, "data")
    return os.path.join(cwd, "data")


@dataclass
class AppConfig:
    # Operating Mode: Master switch (default TEST)
    trading_mode: TradingMode = TradingMode.TEST
    equity_active: bool = True
    fno_active: bool = True
    test_mode_paper_funds_inr: float = 100000.0
    instance_name: str = "Audit bot"
    operator_name: str = "Udit"

    # Telegram Ingestion & Bot Notification
    tg_api_id: int = 0
    tg_api_hash: str = ""
    tg_phone: str = ""
    tg_session_string: str = ""
    tg_bot_token: str = ""
    monitored_channel_ids: List[int] = field(default_factory=list)
    test_group_id: int = 0
    log_group_id: int = 0
    authorized_operator_id: int = 0
    ignore_backlog_on_restart: bool = True

    # Dhan Credentials
    dhan_client_id: str = ""
    dhan_pin: str = ""
    dhan_totp_secret: str = ""
    dhan_access_token: str = ""
    dhan_token_file: str = os.path.expanduser("~/.dhan_token")

    # Equity Sizing & Order Setup
    equity_product_type: ProductType = ProductType.CNC
    equity_sizing_mode: str = "BUDGET"
    equity_budget_inr: float = 25000.0
    equity_fixed_qty: int = 10
    equity_price_markup_pct: float = 0.5

    # Stock Options Sizing
    fno_sizing_mode: str = "BUDGET"
    fno_capital_per_trade: float = 20000.0
    fno_fixed_lots: int = 1
    fno_max_lots: int = 3
    allow_next_month_expiry_fallback: bool = True

    # Protection (Post-Fill)
    sl_pct: float = 3.0
    target_pct: float = 3.0
    sl_limit_buffer_pct: float = 1.0

    # Risk Controls
    max_order_amount_inr: float = 50000.0
    min_trade_funds_inr: float = 10000.0
    max_daily_loss_inr: float = 15000.0
    max_drawdown_pct: float = 5.0
    max_total_exposure_inr: float = 150000.0
    max_open_positions: int = 5
    max_daily_trades: int = 10
    insufficient_funds_policy: str = "HARD_REJECT"
    mode_switch_position_policy: str = "REMAIN_UNTOUCHED"

    # Timing & Lifetimes
    max_message_age_sec: float = 60.0
    dedup_window_sec: float = 20.0
    order_timeout_sec: float = 60.0
    max_quote_age_sec: float = 2.0
    partial_fill_retry_timeout_sec: float = 2.0
    scrip_master_max_age_sec: float = 86400.0

    # Storage Paths
    data_dir: str = field(default_factory=_default_data_dir)
    journal_db_path: str = ""

    def __post_init__(self):
        if not self.journal_db_path:
            self.journal_db_path = os.path.join(self.data_dir, "trade_journal.db")
        for attr in (
            "equity_budget_inr",
            "equity_price_markup_pct",
            "fno_capital_per_trade",
            "sl_pct",
            "target_pct",
            "sl_limit_buffer_pct",
            "max_order_amount_inr",
            "min_trade_funds_inr",
            "max_daily_loss_inr",
            "max_drawdown_pct",
            "max_total_exposure_inr",
            "max_message_age_sec",
            "dedup_window_sec",
            "order_timeout_sec",
            "max_quote_age_sec",
            "partial_fill_retry_timeout_sec",
            "test_mode_paper_funds_inr",
        ):
            val = getattr(self, attr)
            if not isinstance(val, (int, float)) or isinstance(val, bool) or not math.isfinite(float(val)):
                raise ValueError(f"Configuration field '{attr}' must be a finite number, got {val!r}")
            if float(val) < 0.0:
                raise ValueError(f"Configuration field '{attr}' cannot be negative, got {val!r}")

    @classmethod
    def from_env(cls, env: Optional[dict] = None, env_file: str = ".env") -> "AppConfig":
        if env is None:
            load_env_file(env_file)
            env = os.environ
        elif env_file and os.path.exists(env_file):
            load_env_file(env_file)

        mode_str = str(env.get("TRADING_MODE", "TEST")).upper().strip()
        trading_mode = TradingMode.LIVE if mode_str == "LIVE" else TradingMode.TEST

        equity_prod_str = str(env.get("EQUITY_PRODUCT_TYPE", "CNC")).upper().strip()
        equity_prod = ProductType.MTF if equity_prod_str == "MTF" else ProductType.CNC

        default_dd = _default_data_dir()
        data_dir = str(env.get("DATA_DIR") or default_dd).strip()
        journal_db_path = str(env.get("JOURNAL_DB_PATH") or os.path.join(data_dir, "trade_journal.db")).strip()

        cfg = cls(
            trading_mode=trading_mode,
            equity_active=_bool(env.get("EQUITY_ACTIVE", "true"), default=True),
            fno_active=_bool(env.get("FNO_ACTIVE", "true"), default=True),
            test_mode_paper_funds_inr=_clean_float(env.get("TEST_MODE_PAPER_FUNDS_INR", "100000.0"), 100000.0),
            instance_name=str(env.get("BOT_INSTANCE_NAME", "Audit bot")).strip(),
            operator_name=str(env.get("OPERATOR_NAME", "Udit")).strip(),

            tg_api_id=_clean_int(env.get("TG_API_ID", "0")),
            tg_api_hash=str(env.get("TG_API_HASH", "")).strip(),
            tg_phone=str(env.get("TG_PHONE", "")).strip(),
            tg_session_string=str(env.get("TG_SESSION_STRING", "")).strip(),
            tg_bot_token=str(env.get("TG_BOT_TOKEN") or env.get("TELEGRAM_BOT_TOKEN") or "").strip(),
            monitored_channel_ids=_int_list(env.get("MONITORED_CHANNEL_IDS", "")),
            test_group_id=_clean_int(env.get("TEST_GROUP_ID", "0")),
            log_group_id=_clean_int(env.get("LOG_GROUP_ID", "0")),
            authorized_operator_id=_clean_int(env.get("AUTHORIZED_OPERATOR_TG_ID", "0")),
            ignore_backlog_on_restart=_bool(env.get("IGNORE_TELEGRAM_BACKLOG_ON_RESTART", "true"), default=True),

            dhan_client_id=str(env.get("DHAN_CLIENT_ID", "")).strip(),
            dhan_pin=str(env.get("DHAN_PIN", "")).strip(),
            dhan_totp_secret=str(env.get("DHAN_TOTP_SECRET", "")).strip(),
            dhan_access_token=str(env.get("DHAN_ACCESS_TOKEN", "")).strip(),
            dhan_token_file=str(env.get("DHAN_TOKEN_FILE", os.path.expanduser("~/.dhan_token"))).strip(),

            equity_product_type=equity_prod,
            equity_sizing_mode=str(env.get("EQUITY_SIZING_MODE", "BUDGET")).upper().strip(),
            equity_budget_inr=_clean_float(env.get("EQUITY_BUDGET_INR", "25000.0"), 25000.0),
            equity_fixed_qty=_clean_int(env.get("EQUITY_FIXED_QTY", "10"), 10),
            equity_price_markup_pct=_clean_float(env.get("EQUITY_PRICE_MARKUP_PCT", "0.5"), 0.5),

            fno_sizing_mode=str(env.get("FNO_SIZING_MODE", "BUDGET")).upper().strip(),
            fno_capital_per_trade=_clean_float(env.get("FNO_CAPITAL_PER_TRADE", "20000.0"), 20000.0),
            fno_fixed_lots=_clean_int(env.get("FNO_FIXED_LOTS", "1"), 1),
            fno_max_lots=_clean_int(env.get("FNO_MAX_LOTS", "3"), 3),
            allow_next_month_expiry_fallback=_bool(env.get("ALLOW_NEXT_MONTH_EXPIRY_FALLBACK", "true"), default=True),

            sl_pct=_clean_float(env.get("SL_PCT", "3.0"), 3.0),
            target_pct=_clean_float(env.get("TARGET_PCT", "3.0"), 3.0),
            sl_limit_buffer_pct=_clean_float(env.get("SL_LIMIT_BUFFER_PCT", "1.0"), 1.0),

            max_order_amount_inr=_clean_float(env.get("MAX_ORDER_AMOUNT_INR", "50000.0"), 50000.0),
            min_trade_funds_inr=_clean_float(env.get("MIN_TRADE_FUNDS_INR", "10000.0"), 10000.0),
            max_daily_loss_inr=_clean_float(env.get("MAX_DAILY_LOSS_INR", "15000.0"), 15000.0),
            max_drawdown_pct=_clean_float(env.get("MAX_DRAWDOWN_PCT", "5.0"), 5.0),
            max_total_exposure_inr=_clean_float(env.get("MAX_TOTAL_EXPOSURE_INR", "150000.0"), 150000.0),
            max_open_positions=_clean_int(env.get("MAX_OPEN_POSITIONS", "5"), 5),
            max_daily_trades=_clean_int(env.get("MAX_DAILY_TRADES", "10"), 10),
            insufficient_funds_policy=str(env.get("INSUFFICIENT_FUNDS_POLICY", "HARD_REJECT")).upper().strip(),
            mode_switch_position_policy=str(env.get("MODE_SWITCH_POSITION_POLICY", "REMAIN_UNTOUCHED")).upper().strip(),

            max_message_age_sec=_clean_float(env.get("MAX_MESSAGE_AGE_SEC", "60.0"), 60.0),
            dedup_window_sec=_clean_float(env.get("DEDUP_WINDOW_SEC", "20.0"), 20.0),
            order_timeout_sec=_clean_float(env.get("ORDER_TIMEOUT_SEC", "60.0"), 60.0),
            max_quote_age_sec=_clean_float(env.get("MAX_QUOTE_AGE_SEC", "2.0"), 2.0),
            partial_fill_retry_timeout_sec=_clean_float(env.get("PARTIAL_FILL_RETRY_TIMEOUT_SEC", "2.0"), 2.0),
            scrip_master_max_age_sec=_clean_float(env.get("SCRIP_MASTER_MAX_AGE_SEC", "86400.0"), 86400.0),

            data_dir=data_dir,
            journal_db_path=journal_db_path,
        )
        return cfg
