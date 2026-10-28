"""
Domain models for the single-account Telegram-to-Dhan trading system.
Compatible with Python 3.11/3.12+ and Pydantic v1 / v2.
"""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class TradingMode(str, Enum):
    TEST = "TEST"
    LIVE = "LIVE"


class ProductType(str, Enum):
    CNC = "CNC"          # Cash & Carry (Equity Delivery)
    MTF = "MTF"          # Margin Trading Facility (Equity Leveraged Delivery)
    MARGIN = "MARGIN"    # F&O Normal Margin
    INTRADAY = "INTRADAY"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class SignalAction(str, Enum):
    BUY_ENTRY = "BUY_ENTRY"
    EXIT_SIGNAL = "EXIT_SIGNAL"
    BLOCKED = "BLOCKED"
    IGNORED = "IGNORED"


class SignalType(str, Enum):
    EQUITY = "EQUITY"
    OPTION = "OPTION"


class EquitySignal(BaseModel):
    symbol: str
    action: SignalAction = SignalAction.BUY_ENTRY
    stated_price: float = 0.0  # 0.0 means CMP (no price stated)
    order_type: OrderType = OrderType.LIMIT
    raw_text: str = ""
    channel_id: int = 0
    message_id: int = 0
    message_timestamp: float = 0.0  # epoch seconds


class FnOSignal(BaseModel):
    symbol: str
    strike: float
    option_type: str  # CE | PE
    expiry_month: Optional[str] = None  # JAN..DEC
    expiry_day: Optional[int] = None
    entry_min: float
    entry_max: float
    stop_loss: Optional[float] = None
    targets: List[float] = Field(default_factory=list)
    action: SignalAction = SignalAction.BUY_ENTRY
    caution: bool = False
    raw_text: str = ""
    channel_id: int = 0
    message_id: int = 0
    message_timestamp: float = 0.0  # epoch seconds


class ValidationResult(BaseModel):
    is_valid: bool
    action: SignalAction
    reason: str
    signal_type: Optional[SignalType] = None
    symbol: Optional[str] = None
    security_id: Optional[str] = None
    limit_price: Optional[float] = None
    quantity_or_lots: Optional[int] = None
    calculated_exposure: Optional[float] = None


class SizingResult(BaseModel):
    is_valid: bool
    units: int  # Shares for Equity, Lots for F&O
    total_quantity: int  # units * lot_size
    price_per_unit: float
    total_cost: float
    error_reason: Optional[str] = None


class OrderRequest(BaseModel):
    correlation_id: str
    symbol: str
    security_id: str
    exchange_segment: str  # NSE_EQ, BSE_EQ, NSE_FNO, BSE_FNO
    transaction_type: TransactionType = TransactionType.BUY
    product_type: ProductType
    order_type: OrderType = OrderType.LIMIT
    price: float
    quantity: int
    trigger_price: float = 0.0
    trading_mode: TradingMode = TradingMode.TEST


class ProtectionLevels(BaseModel):
    executed_price: float
    sl_trigger: float
    sl_limit: float
    target_price: float
    quantity: int
