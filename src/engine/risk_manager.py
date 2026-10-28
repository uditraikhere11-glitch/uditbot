"""
Risk Management and Emergency Circuit Breaker Engine.
Evaluates account health, trade limits, and provides instant halt controls.
"""

from typing import Tuple
from src.config import AppConfig


class RiskManager:
    """
    Validates orders against risk thresholds before broker submission.
    """

    def __init__(self, config: AppConfig):
        self.cfg = config
        self._emergency_stop: bool = False
        self._daily_trades_count: int = 0
        self._daily_realized_pnl: float = 0.0
        self._daily_unrealized_pnl: float = 0.0
        self._current_exposure: float = 0.0
        self._open_positions_count: int = 0

    @property
    def is_emergency_stopped(self) -> bool:
        return self._emergency_stop

    def trigger_emergency_stop(self, reason: str = "Operator Triggered") -> str:
        """Instantly blocks all new trade entries."""
        self._emergency_stop = True
        return f"EMERGENCY STOP ACTIVATED: {reason}"

    def resume_trading(self) -> str:
        """Resumes normal operations."""
        self._emergency_stop = False
        return "Trading operations resumed."

    def update_account_state(
        self,
        current_exposure: float,
        open_positions: int,
        daily_trades: int,
        daily_pnl: float,
    ):
        """Updates internal risk tracking from broker state or journal."""
        self._current_exposure = current_exposure
        self._open_positions_count = open_positions
        self._daily_trades_count = daily_trades
        self._daily_realized_pnl = daily_pnl

    def validate_order_risk(
        self,
        order_cost_inr: float,
        available_funds_inr: float,
    ) -> Tuple[bool, str]:
        """
        Runs comprehensive pre-order risk validation.
        Returns: (is_passed, rejection_reason)
        """
        # 1. Emergency stop check
        if self._emergency_stop:
            return False, "Order rejected: EMERGENCY STOP is active"

        # 2. Minimum funds check
        if available_funds_inr < self.cfg.min_trade_funds_inr:
            return False, (
                f"Available funds ({available_funds_inr:.2f} INR) below "
                f"minimum required threshold ({self.cfg.min_trade_funds_inr:.2f} INR)"
            )

        # 3. Sufficient funds for this specific order
        if available_funds_inr < order_cost_inr:
            return False, (
                f"Insufficient funds ({available_funds_inr:.2f} INR) for order "
                f"cost ({order_cost_inr:.2f} INR)"
            )

        # 4. Max single order value limit
        if order_cost_inr > self.cfg.max_order_amount_inr:
            return False, (
                f"Order value ({order_cost_inr:.2f} INR) exceeds "
                f"MAX_ORDER_AMOUNT_INR limit ({self.cfg.max_order_amount_inr:.2f} INR)"
            )

        # 5. Max daily trades cap
        if (self._daily_trades_count + 1) > self.cfg.max_daily_trades:
            return False, (
                f"Daily trade limit reached ({self._daily_trades_count} >= {self.cfg.max_daily_trades})"
            )

        # 6. Max open positions cap
        if (self._open_positions_count + 1) > self.cfg.max_open_positions:
            return False, (
                f"Max open positions reached ({self._open_positions_count} >= {self.cfg.max_open_positions})"
            )

        # 7. Total portfolio exposure cap
        new_total_exposure = self._current_exposure + order_cost_inr
        if new_total_exposure > self.cfg.max_total_exposure_inr:
            return False, (
                f"New exposure ({new_total_exposure:.2f} INR) exceeds "
                f"MAX_TOTAL_EXPOSURE_INR ({self.cfg.max_total_exposure_inr:.2f} INR)"
            )

        # 8. Daily loss circuit breaker
        if self._daily_realized_pnl < 0 and abs(self._daily_realized_pnl) >= self.cfg.max_daily_loss_inr:
            return False, (
                f"Daily loss limit breached: {self._daily_realized_pnl:.2f} INR "
                f"(limit: -{self.cfg.max_daily_loss_inr:.2f} INR)"
            )

        return True, "Passed risk validation"
