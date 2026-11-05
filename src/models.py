"""
Domain models for the single-account Telegram-to-Dhan trading system.
Compatible with Python 3.11/3.12+ and Pydantic v1 / v2.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
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


class ProtectionRoute(str, Enum):
    SUPER_ORDER = "SUPER_ORDER"
    FOREVER_OCO = "FOREVER_OCO"
    UNKNOWN = "UNKNOWN"


class PositionLifecycleState(str, Enum):
    ENTRY = "ENTRY"
    POSITION_OPEN = "POSITION_OPEN"
    PROTECTED = "PROTECTED"
    EXIT_TRIGGERED = "EXIT_TRIGGERED"
    EXIT_PART_FILLED = "EXIT_PART_FILLED"
    EXIT_FILLED = "EXIT_FILLED"
    POSITION_CLOSED = "POSITION_CLOSED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    PERSISTENCE_UNCERTAIN = "PERSISTENCE_UNCERTAIN"
    UNPROTECTED = "UNPROTECTED"
    UNKNOWN = "UNKNOWN"


class NotificationLifecycleState(str, Enum):
    SIGNAL_ACCEPTED = "SIGNAL_ACCEPTED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_ACK_PENDING = "ORDER_ACK_PENDING"
    ENTRY_PARTIAL_FILLED = "ENTRY_PARTIAL_FILLED"
    ENTRY_FILLED_VERIFIED = "ENTRY_FILLED_VERIFIED"
    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTION_VERIFIED_ACTIVE = "PROTECTION_VERIFIED_ACTIVE"
    PROTECTION_UNVERIFIED = "PROTECTION_UNVERIFIED"
    PROTECTION_REJECTED = "PROTECTION_REJECTED"
    TARGET_TRIGGERED = "TARGET_TRIGGERED"
    SL_TRIGGERED = "SL_TRIGGERED"
    EXIT_PARTIAL_FILLED = "EXIT_PARTIAL_FILLED"
    EXIT_FILLED = "EXIT_FILLED"
    POSITION_CLOSED = "POSITION_CLOSED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    ORDER_REJECTED = "ORDER_REJECTED"
    UNKNOWN = "UNKNOWN"


# P0-01: Explicit Dhan v2 product/segment/protection-route capability matrix.
# Dhan Super Order API (/v2/super/orders) supports CNC, INTRADAY, MARGIN, MTF across NSE_EQ, BSE_EQ, NSE_FNO, BSE_FNO.
# Dhan Forever Order API (/v2/forever/orders) supports CNC and MTF (note: CNC SELL OCO on existing holdings requires DDPI/POA or T+1 settlement).
PROTECTION_CAPABILITY_MATRIX: Dict[Tuple[str, str], Dict[str, Any]] = {
    ("NSE_EQ", "CNC"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value, ProtectionRoute.FOREVER_OCO.value),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": True,
    },
    ("BSE_EQ", "CNC"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value, ProtectionRoute.FOREVER_OCO.value),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": True,
    },
    ("NSE_EQ", "MTF"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value, ProtectionRoute.FOREVER_OCO.value),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("BSE_EQ", "MTF"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value, ProtectionRoute.FOREVER_OCO.value),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("NSE_EQ", "INTRADAY"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("BSE_EQ", "INTRADAY"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("NSE_FNO", "MARGIN"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("BSE_FNO", "MARGIN"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("NSE_FNO", "INTRADAY"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
    ("BSE_FNO", "INTRADAY"): {
        "supported_routes": (ProtectionRoute.SUPER_ORDER.value,),
        "default_new_entry_route": ProtectionRoute.SUPER_ORDER.value,
        "same_day_atomic_route": ProtectionRoute.SUPER_ORDER.value,
        "forever_oco_requires_settled_holdings_or_ddpi": False,
    },
}


def resolve_protection_route_capability(
    exchange_segment: str,
    product_type: str,
    preferred_route: Optional[str] = None,
) -> Tuple[bool, str, str]:
    """
    P0-01 & P0-02: Validates `(exchange_segment, product_type, preferred_route)` against
    `PROTECTION_CAPABILITY_MATRIX`.
    Returns `(is_supported, resolved_route, detail_message)`.
    Never infers protection route from `product_type == "MARGIN"` alone.
    """
    seg = str(exchange_segment or "").strip().upper()
    prod = str(product_type or "").strip().upper()
    cap = PROTECTION_CAPABILITY_MATRIX.get((seg, prod))
    if not cap:
        return (
            False,
            ProtectionRoute.UNKNOWN.value,
            f"Unsupported segment/product combination ({seg}/{prod}) in protection capability matrix",
        )
    supported = cap["supported_routes"]
    if preferred_route is not None and str(preferred_route).strip() != "":
        req_route = str(preferred_route).strip().upper()
        if req_route not in supported:
            return (
                False,
                ProtectionRoute.UNKNOWN.value,
                f"Protection route '{req_route}' is not supported for {seg}/{prod} (supported: {', '.join(supported)})",
            )
        return True, req_route, f"Verified route {req_route} for {seg}/{prod}"
    default_route = str(cap["default_new_entry_route"])
    return True, default_route, f"Default route {default_route} for {seg}/{prod}"


def infer_record_protection_route(rec: Dict[str, Any]) -> str:
    """
    P0-02: Resolves the authoritative `protection_route` for a watcher or journal record.
    - Uses explicit `protection_route` (`SUPER_ORDER` or `FOREVER_OCO`) when present.
    - Returns `UNKNOWN` if explicitly marked `UNKNOWN` or if ambiguous.
    - Safely migrates legacy records when unambiguous (`is_super_order=True` -> `SUPER_ORDER`;
      `is_super_order=False` with `product_type in ('CNC', 'MTF')` -> `FOREVER_OCO`).
    """
    if not isinstance(rec, dict):
        return ProtectionRoute.UNKNOWN.value
    explicit = str(rec.get("protection_route") or "").strip().upper()
    if explicit in (ProtectionRoute.SUPER_ORDER.value, ProtectionRoute.FOREVER_OCO.value):
        return explicit
    if explicit == ProtectionRoute.UNKNOWN.value:
        return ProtectionRoute.UNKNOWN.value

    raw_super = rec.get("is_super_order")
    prod = str(rec.get("product_type") or "").strip().upper()
    seg = str(rec.get("segment") or rec.get("exchange_segment") or "").strip().upper()

    if raw_super is True or (isinstance(raw_super, int) and not isinstance(raw_super, bool) and raw_super == 1):
        if (seg, prod) in PROTECTION_CAPABILITY_MATRIX and ProtectionRoute.SUPER_ORDER.value in PROTECTION_CAPABILITY_MATRIX[(seg, prod)]["supported_routes"]:
            return ProtectionRoute.SUPER_ORDER.value
        if not seg and prod in ("CNC", "MTF", "MARGIN", "INTRADAY"):
            return ProtectionRoute.SUPER_ORDER.value
        return ProtectionRoute.UNKNOWN.value

    if raw_super is False or (isinstance(raw_super, int) and not isinstance(raw_super, bool) and raw_super == 0):
        if prod in ("CNC", "MTF"):
            return ProtectionRoute.FOREVER_OCO.value
        return ProtectionRoute.UNKNOWN.value

    # Legacy record where is_super_order was omitted (None)
    pending_oco = rec.get("pending_oco")
    if isinstance(pending_oco, dict) and str(pending_oco.get("route") or "").strip().upper() in (
        ProtectionRoute.SUPER_ORDER.value,
        ProtectionRoute.FOREVER_OCO.value,
    ):
        return str(pending_oco["route"]).strip().upper()
    if prod in ("CNC", "MTF") and bool(
        rec.get("forever_order_id")
        or rec.get("oco_order_id")
        or rec.get("confirmed_protection_ids")
    ):
        return ProtectionRoute.FOREVER_OCO.value

    return ProtectionRoute.UNKNOWN.value


class ProtectionVerificationStatus(str, Enum):
    VERIFIED_ACTIVE = "VERIFIED_ACTIVE"
    UNVERIFIED = "UNVERIFIED"
    TERMINAL_OR_INVALID = "TERMINAL_OR_INVALID"


class ProtectionVerificationResult(BaseModel):
    """
    Single authoritative verification outcome for Forever OCO and Super Order protection.
    Distinguishes verified active resting coverage, unverified/unavailable evidence, and
    terminal/invalid/mismatched records.
    """
    status: ProtectionVerificationStatus
    is_verified: bool
    is_terminal_or_invalid: bool = False
    protected_quantity: int = 0
    group_id: str = ""
    parent_order_id: str = ""
    security_id: str = ""
    exchange_segment: str = ""
    product_type: str = ""
    account_id: str = ""
    sl_price: float = 0.0
    target_price: float = 0.0
    reason: str = ""


class BrokerReadStatus(str, Enum):
    OK_EMPTY = "OK_EMPTY"
    OK_RECORDS = "OK_RECORDS"
    FAILED = "FAILED"
    MALFORMED = "MALFORMED"
    UNAVAILABLE = "UNAVAILABLE"


class BrokerReadList(list):
    """
    List subclass returned when a broker collection endpoint succeeds with a valid list of dict records.
    Preserves standard list semantics for callers while exposing structured `.ok`, `.status`, `.records`,
    and `.error` attributes to distinguish `OK_EMPTY` from `OK_RECORDS`.
    """
    def __init__(
        self,
        iterable: Optional[List[Dict[str, Any]]] = None,
        status: Optional[str] = None,
        status_code: int = 200,
    ):
        items = list(iterable or [])
        super().__init__(items)
        self.ok: bool = True
        self.status: str = status or (
            BrokerReadStatus.OK_RECORDS.value if len(items) > 0 else BrokerReadStatus.OK_EMPTY.value
        )
        self.records: List[Dict[str, Any]] = items
        self.error: str = ""
        self.status_code: int = status_code

    @property
    def is_empty(self) -> bool:
        return len(self) == 0


class BrokerReadFailure:
    """
    Non-list sentinel returned when a broker collection endpoint fails (HTTP error),
    is unavailable (network/timeout exception), or returns a malformed payload (non-list JSON
    or list containing non-dict items).
    - Never compares equal to `[]` (`res == []` is False).
    - `isinstance(res, list)` is False.
    - `bool(res)` is False so legacy `(res or [])` expressions evaluate safely when appropriate,
      while strict callers (`StartupReconciliationGate`, `ProtectionWatcher`, `RiskManager`) inspect `res.ok` / `isinstance(res, list)`.
    """
    def __init__(self, status: str, error: str, status_code: int = 0):
        self.ok: bool = False
        self.status: str = status
        self.records: Optional[List[Dict[str, Any]]] = None
        self.error: str = str(error or "Broker read failed")
        self.status_code: int = int(status_code or 0)

    @property
    def is_empty(self) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BrokerReadFailure):
            return (
                self.status == other.status
                and self.error == other.error
                and self.status_code == other.status_code
            )
        return False

    def __repr__(self) -> str:
        return f"BrokerReadFailure(status={self.status!r}, status_code={self.status_code}, error={self.error!r})"


class QuoteResult(BaseModel):
    """
    Structured market quote result with strict source timestamp, venue, and delay metadata.
    - Never fabricates quote_timestamp from the local clock when Yahoo omits regularMarketTime.
    - Never fabricates missing exchangeName (`venue`) or `currency` metadata; preserves `"UNKNOWN"` honestly.
    - Distinguishes accepted pricing (is_valid=True under DARVAS_HTTP_SUCCESS) from verified
      quote freshness (freshness_verified=True / quote_freshness_status).
    """
    is_valid: bool
    price: float = 0.0
    source: str = "YAHOO_FINANCE"
    ticker: str = ""
    venue: str = "UNKNOWN"
    security_id: str = ""
    currency: str = "UNKNOWN"
    market_state: str = "UNKNOWN"
    quote_timestamp: Optional[float] = None
    age_sec: Optional[float] = None
    is_delayed: bool = False
    delay_seconds: Optional[float] = None
    delay_metadata_present: bool = False
    http_latency_ms: float = 0.0
    parse_latency_ms: float = 0.0
    auth_latency_ms: float = 0.0
    total_cmp_latency_ms: float = 0.0
    quote_age_status: str = "QUOTE_AGE_UNKNOWN"
    freshness_verified: bool = False
    quote_freshness_status: str = "UNVERIFIED"
    cmp_policy: str = "STRICT_TIMESTAMP"
    reference_label: str = "LIVE_REFERENCE"
    failure_reason: str = ""
    licensing_note: str = (
        "Yahoo Finance published market-data terms state that its information is not intended "
        "for trading; this tunnel is not an exchange-grade live execution feed."
    )


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
    hold_hint: Optional[str] = None
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
    log_card: Optional[str] = None
    latency_breakdown: Optional[Dict[str, Any]] = None
    cmp_policy: Optional[str] = None
    reference_label: Optional[str] = None
    quote_age_status: Optional[str] = None
    freshness_verified: Optional[bool] = None
    quote_freshness_status: Optional[str] = None
    broker_order_status: Optional[str] = None
    protection_route: Optional[str] = None


class SizingResult(BaseModel):
    is_valid: bool
    units: int  # Shares for Equity, Lots for F&O
    total_quantity: int  # units * lot_size
    price_per_unit: float
    total_cost: float
    error_reason: Optional[str] = None
    tranches: List[Tuple[int, float]] = Field(default_factory=list)  # [(lots, target_price), ...]


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
    after_market_order: bool = False
    amo_time: str = "OPEN"


class ProtectionLevels(BaseModel):
    executed_price: float
    sl_trigger: float
    sl_limit: float
    target_price: float
    quantity: int
    target_trigger: Optional[float] = None
