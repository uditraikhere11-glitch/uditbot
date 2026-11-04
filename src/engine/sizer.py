"""
Position Sizing, Tranche Splitting, and Tick-Aligned Price Calculation Engine.
Strictly enforces whole-unit constraints, price ceilings, and multi-target lot distribution.
"""

import math
from typing import List, Optional, Tuple
from src.models import SizingResult, ProtectionLevels


def round_to_tick(price: float, tick_size: float = 0.05, mode: str = "nearest") -> float:
    """
    Rounds price to the exchange tick size.
    Modes:
      - 'down': math.floor (used for SL and option price ceiling)
      - 'up': math.ceil (used for Target)
      - 'nearest': round to nearest tick (used for Equity CMP markup)
    """
    if tick_size <= 0:
        return round(price, 2)

    steps = price / tick_size
    if mode == "down":
        n = math.floor(steps + 1e-9)
    elif mode == "up":
        n = math.ceil(steps - 1e-9)
    else:
        n = round(steps)

    return round(n * tick_size, 4)


def split_tranches(lots: int, targets: List[float]) -> List[Tuple[int, float]]:
    """
    Splits lots across targets as [(lots, target_price), ...].

    Uses as many targets as there are lots (earliest targets first).
    Any remainder lots go to the last (furthest) target so runners ride the bigger move.
    Examples:
      - 3 lots, [500.0, 1000.0] -> [(1, 500.0), (2, 1000.0)]
      - 1 lot,  [500.0, 1000.0] -> [(1, 500.0)]
      - 4 lots, [10.0, 20.0, 30.0] -> [(1, 10.0), (1, 20.0), (2, 30.0)]
      - 2 lots, [50.0] -> [(2, 50.0)]
      - 0 lots or empty targets -> []
    """
    if lots <= 0 or not targets:
        return []
    sorted_targets = sorted(targets)
    n = min(len(sorted_targets), lots)
    base, rem = divmod(lots, n)
    tranches = [(base, t) for t in sorted_targets[:n]]
    tranches[-1] = (base + rem, tranches[-1][1])
    return tranches


def calculate_equity_limit_price(cmp: float, markup_pct: float, tick_size: float = 0.05) -> float:
    """
    Calculates equity limit price = CMP * (1 + markup_pct / 100),
    rounded to the nearest valid exchange tick.
    """
    raw_price = cmp * (1.0 + markup_pct / 100.0)
    return round_to_tick(raw_price, tick_size, mode="nearest")


def calculate_option_limit_price(entry_max: float, tick_size: float = 0.05) -> float:
    """
    Calculates stock option BUY LIMIT upper entry ceiling = entry_max * 1.04,
    rounded DOWN to the contract tick size so it never exceeds the calculated ceiling.
    """
    raw_ceiling = entry_max * 1.04
    return round_to_tick(raw_ceiling, tick_size, mode="down")


def calculate_protection_levels(
    executed_price: float,
    quantity: int,
    sl_pct: float = 3.0,
    target_pct: float = 3.0,
    sl_limit_buffer_pct: float = 1.0,
    tick_size: float = 0.05,
) -> ProtectionLevels:
    """
    Calculates broker-side Forever OCO levels strictly from the executed traded price.
    SL is floored below executed price; Target is ceiled above executed price.
    """
    sl_trigger = round_to_tick(executed_price * (1.0 - sl_pct / 100.0), tick_size, mode="down")
    # Ensure SL trigger is at least 1 tick below executed price
    if sl_trigger >= executed_price:
        sl_trigger = round_to_tick(executed_price - tick_size, tick_size, mode="down")

    target = round_to_tick(executed_price * (1.0 + target_pct / 100.0), tick_size, mode="up")
    # Ensure Target is at least 1 tick above executed price
    if target <= executed_price:
        target = round_to_tick(executed_price + tick_size, tick_size, mode="up")

    sl_limit = round_to_tick(sl_trigger * (1.0 - sl_limit_buffer_pct / 100.0), tick_size, mode="down")

    return ProtectionLevels(
        executed_price=executed_price,
        sl_trigger=max(sl_trigger, tick_size),
        sl_limit=max(sl_limit, tick_size),
        target_price=target,
        quantity=quantity,
        target_trigger=target,
    )


def size_equity(
    sizing_mode: str,
    budget_inr: float,
    fixed_qty: int,
    limit_price: float,
) -> SizingResult:
    """
    Calculates equity share quantity.
    Rejects if affordable quantity < 1 share.
    """
    if limit_price <= 0:
        return SizingResult(
            is_valid=False, units=0, total_quantity=0, price_per_unit=0, total_cost=0,
            error_reason="Invalid limit price <= 0"
        )

    if sizing_mode.upper() == "FIXED_QTY":
        qty = fixed_qty
    else:
        # BUDGET mode
        if budget_inr <= 0:
            return SizingResult(
                is_valid=False, units=0, total_quantity=0, price_per_unit=limit_price, total_cost=0,
                error_reason="Equity budget is <= 0"
            )
        qty = math.floor(budget_inr / limit_price)

    if qty < 1:
        return SizingResult(
            is_valid=False, units=0, total_quantity=0, price_per_unit=limit_price, total_cost=0,
            error_reason=f"Insufficient budget ({budget_inr:.2f} INR) to purchase 1 share at {limit_price:.2f}"
        )

    total_cost = round(qty * limit_price, 2)
    return SizingResult(
        is_valid=True,
        units=qty,
        total_quantity=qty,
        price_per_unit=limit_price,
        total_cost=total_cost,
    )


def size_option(
    sizing_mode: str,
    capital_per_trade: float,
    fixed_lots: int,
    max_lots: int,
    lot_size: int,
    limit_price: float,
    targets: Optional[List[float]] = None,
) -> SizingResult:
    """
    Calculates stock option lot sizing and splits tranches across multiple targets.
    Rejects if affordable lots < 1. Enforces max_lots cap.
    """
    if lot_size <= 0:
        return SizingResult(
            is_valid=False, units=0, total_quantity=0, price_per_unit=limit_price, total_cost=0,
            error_reason=f"Invalid lot size: {lot_size}"
        )

    if limit_price <= 0:
        return SizingResult(
            is_valid=False, units=0, total_quantity=0, price_per_unit=0, total_cost=0,
            error_reason="Invalid limit price <= 0"
        )

    cost_per_lot = lot_size * limit_price

    if sizing_mode.upper() == "FIXED_LOTS":
        lots = fixed_lots
    else:
        # BUDGET mode
        if capital_per_trade <= 0:
            return SizingResult(
                is_valid=False, units=0, total_quantity=0, price_per_unit=limit_price, total_cost=0,
                error_reason="Option capital per trade is <= 0"
            )
        lots = math.floor(capital_per_trade / cost_per_lot)

    if max_lots > 0:
        lots = min(lots, max_lots)

    if lots < 1:
        return SizingResult(
            is_valid=False, units=0, total_quantity=0, price_per_unit=limit_price, total_cost=0,
            error_reason=(
                f"Insufficient capital ({capital_per_trade:.2f} INR) for 1 lot "
                f"(cost={cost_per_lot:.2f} INR)"
            )
        )

    total_qty = lots * lot_size
    total_cost = round(total_qty * limit_price, 2)
    tranches = split_tranches(lots, targets or [])

    return SizingResult(
        is_valid=True,
        units=lots,
        total_quantity=total_qty,
        price_per_unit=limit_price,
        total_cost=total_cost,
        tranches=tranches,
    )
