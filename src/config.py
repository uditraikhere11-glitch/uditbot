"""
System Configuration & Environment Management.
Reads from environment variables and protected .env files.
"""

import os
from dataclasses import dataclass, field
from typing import List
from src.models import TradingMode, ProductType


def _bool(val: str, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _int_list(val: str) -> List[int]:
    if not val:
        return []
    res = []
    for part in val.split(","):
        part = part.strip()
        if part:
            try:
                res.append(int(part))
            except ValueError:
                pass
    return res


@dataclass
class AppConfig:
    # Operating Mode: Master switch (default TEST)
    trading_mode: TradingMode = TradingMode.TEST
    equity_active: bool = True
    fno_active: bool = True

    # Telegram Ingestion
    tg_api_id: int = 0
    tg_api_hash: str = ""
    tg_phone: str = ""
    tg_session_string: str = ""
    monitored_channel_ids: List[int] = field(default_factory=list)
    test_group_id: int = 0
    log_group_id: int = 0
    authorized_operator_id: int = 0
    ignore_backlog_on_restart: bool = True  # Confirmed by Udit

    # Dhan Credentials
    dhan_client_id: str = ""
    dhan_pin: str = ""
    dhan_totp_secret: str = ""
    dhan_access_token: str = ""
    dhan_token_file: str = os.path.expanduser("~/.dhan_token")

    # Equity Sizing & Order Setup
    equity_product_type: ProductType = ProductType.CNC
    equity_sizing_mode: str = "BUDGET"  # BUDGET | FIXED_QTY
    equity_budget_inr: float = 25000.0
    equity_fixed_qty: int = 10
    equity_price_markup_pct: float = 0.5  # Entered via setup defaults

    # Stock Options Sizing
    fno_sizing_mode: str = "BUDGET"     # BUDGET | FIXED_LOTS
    fno_capital_per_trade: float = 20000.0
    fno_fixed_lots: int = 1
    fno_max_lots: int = 3               # 0 = no cap

    # Protection (Post-Fill)
    sl_pct: float = 3.0
    target_pct: float = 3.0
    sl_limit_buffer_pct: float = 1.0

    # Risk Controls
    max_order_amount_inr: float = 50000.0
    min_trade_funds_inr: float = 10000.0
    max_daily_loss_inr: float = 15000.0
    max_total_exposure_inr: float = 150000.0
    max_open_positions: int = 5
    max_daily_trades: int = 10
    insufficient_funds_policy: str = "HARD_REJECT"  # Confirmed by Udit: Hard reject
    mode_switch_position_policy: str = "REMAIN_UNTOUCHED"  # Confirmed by Udit: Untouched under Forever OCO

    # Timing & Lifetimes (Confirmed by Udit)
    max_message_age_sec: float = 60.0
    dedup_window_sec: float = 20.0
    order_timeout_sec: float = 60.0
    max_quote_age_sec: float = 2.0               # Confirmed: 2 seconds
    partial_fill_retry_timeout_sec: float = 2.0   # Confirmed: 2 seconds

    # Storage Paths
    data_dir: str = os.path.join(os.getcwd(), "data")
    journal_db_path: str = os.path.join(os.getcwd(), "data", "trade_journal.db")

    @classmethod
    def from_env(cls, env: dict = None) -> "AppConfig":
        if env is None:
            env = os.environ

        mode_str = env.get("TRADING_MODE", "TEST").upper().strip()
        trading_mode = TradingMode.LIVE if mode_str == "LIVE" else TradingMode.TEST

        equity_prod_str = env.get("EQUITY_PRODUCT_TYPE", "CNC").upper().strip()
        equity_prod = ProductType.MTF if equity_prod_str == "MTF" else ProductType.CNC

        cfg = cls(
            trading_mode=trading_mode,
            equity_active=_bool(env.get("EQUITY_ACTIVE", "true"), default=True),
            fno_active=_bool(env.get("FNO_ACTIVE", "true"), default=True),

            tg_api_id=int(env.get("TG_API_ID", "0") or "0"),
            tg_api_hash=env.get("TG_API_HASH", "").strip(),
            tg_phone=env.get("TG_PHONE", "").strip(),
            tg_session_string=env.get("TG_SESSION_STRING", "").strip(),
            monitored_channel_ids=_int_list(env.get("MONITORED_CHANNEL_IDS", "")),
            test_group_id=int(env.get("TEST_GROUP_ID", "0") or "0"),
            log_group_id=int(env.get("LOG_GROUP_ID", "0") or "0"),
            authorized_operator_id=int(env.get("AUTHORIZED_OPERATOR_TG_ID", "0") or "0"),
            ignore_backlog_on_restart=_bool(env.get("IGNORE_TELEGRAM_BACKLOG_ON_RESTART", "true"), default=True),

            dhan_client_id=env.get("DHAN_CLIENT_ID", "").strip(),
            dhan_pin=env.get("DHAN_PIN", "").strip(),
            dhan_totp_secret=env.get("DHAN_TOTP_SECRET", "").strip(),
            dhan_access_token=env.get("DHAN_ACCESS_TOKEN", "").strip(),
            dhan_token_file=env.get("DHAN_TOKEN_FILE", os.path.expanduser("~/.dhan_token")),

            equity_product_type=equity_prod,
            equity_sizing_mode=env.get("EQUITY_SIZING_MODE", "BUDGET").upper().strip(),
            equity_budget_inr=float(env.get("EQUITY_BUDGET_INR", "25000.0")),
            equity_fixed_qty=int(env.get("EQUITY_FIXED_QTY", "10")),
            equity_price_markup_pct=float(env.get("EQUITY_PRICE_MARKUP_PCT", "0.5")),

            fno_sizing_mode=env.get("FNO_SIZING_MODE", "BUDGET").upper().strip(),
            fno_capital_per_trade=float(env.get("FNO_CAPITAL_PER_TRADE", "20000.0")),
            fno_fixed_lots=int(env.get("FNO_FIXED_LOTS", "1")),
            fno_max_lots=int(env.get("FNO_MAX_LOTS", "3")),

            sl_pct=float(env.get("SL_PCT", "3.0")),
            target_pct=float(env.get("TARGET_PCT", "3.0")),
            sl_limit_buffer_pct=float(env.get("SL_LIMIT_BUFFER_PCT", "1.0")),

            max_order_amount_inr=float(env.get("MAX_ORDER_AMOUNT_INR", "50000.0")),
            min_trade_funds_inr=float(env.get("MIN_TRADE_FUNDS_INR", "10000.0")),
            max_daily_loss_inr=float(env.get("MAX_DAILY_LOSS_INR", "15000.0")),
            max_total_exposure_inr=float(env.get("MAX_TOTAL_EXPOSURE_INR", "150000.0")),
            max_open_positions=int(env.get("MAX_OPEN_POSITIONS", "5")),
            max_daily_trades=int(env.get("MAX_DAILY_TRADES", "10")),
            insufficient_funds_policy=env.get("INSUFFICIENT_FUNDS_POLICY", "HARD_REJECT").upper().strip(),
            mode_switch_position_policy=env.get("MODE_SWITCH_POSITION_POLICY", "REMAIN_UNTOUCHED").upper().strip(),

            max_message_age_sec=float(env.get("MAX_MESSAGE_AGE_SEC", "60.0")),
            dedup_window_sec=float(env.get("DEDUP_WINDOW_SEC", "20.0")),
            order_timeout_sec=float(env.get("ORDER_TIMEOUT_SEC", "60.0")),
            max_quote_age_sec=float(env.get("MAX_QUOTE_AGE_SEC", "2.0")),
            partial_fill_retry_timeout_sec=float(env.get("PARTIAL_FILL_RETRY_TIMEOUT_SEC", "2.0")),

            data_dir=env.get("DATA_DIR", os.path.join(os.getcwd(), "data")),
            journal_db_path=env.get("JOURNAL_DB_PATH", os.path.join(os.getcwd(), "data", "trade_journal.db")),
        )
        return cfg
