"""
Risk Management and Emergency Circuit Breaker Engine.
Evaluates account health, trade limits, and provides instant halt controls.
Integrates with durable EmergencyStopManager to ensure STOP state persists across restarts.
Implements V12-08 & V12-23 fixes:
- Synchronizes live portfolio state (`daily_trades`, `open_positions`, `current_exposure`, `daily_pnl`)
  from `JournalDB` and broker positions before validating new entries (`sync_from_journal_and_broker`).
- Updates in-memory counters immediately when an order is committed (`record_order_committed`).
- Enforces `max_drawdown_pct` alongside `max_daily_loss_inr`.
"""

import datetime
import os
import zoneinfo
from typing import Any, Optional, Tuple
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager

IST = zoneinfo.ZoneInfo("Asia/Kolkata")


class RiskManager:
    """
    Validates orders against risk thresholds before broker submission.
    Durable emergency stop state is managed via EmergencyStopManager.
    """

    def __init__(self, config: AppConfig, stop_manager: Optional[EmergencyStopManager] = None):
        self.cfg = config
        self.stop_manager = stop_manager or EmergencyStopManager(
            state_file=os.path.join(config.data_dir, "emergency_stop.json")
        )
        self._daily_trades_count: int = 0
        self._daily_realized_pnl: float = 0.0
        self._daily_unrealized_pnl: float = 0.0
        self._current_exposure: float = 0.0
        self._open_positions_count: int = 0
        self._last_day_str: str = ""

    @property
    def is_emergency_stopped(self) -> bool:
        """Checks if emergency stop is currently active or corrupt (fails closed)."""
        return self.stop_manager.is_stopped()

    def trigger_emergency_stop(self, reason: str = "Operator Triggered") -> str:
        """Instantly blocks all new trade entries durably on disk."""
        operator = getattr(self.cfg, "operator_name", "Operator")
        account_id = getattr(self.cfg, "dhan_client_id", "")
        self.stop_manager.trigger_stop(operator=operator, reason=reason, account_id=account_id)
        return f"EMERGENCY STOP ACTIVATED: {reason}"

    def resume_trading(self) -> str:
        """Explicitly resumes normal operations on disk."""
        operator = getattr(self.cfg, "operator_name", "Operator")
        self.stop_manager.resume(operator=operator, reason="Manual operator resume")
        return "Trading operations resumed."

    def update_account_state(
        self,
        current_exposure: float,
        open_positions: int,
        daily_trades: int,
        daily_pnl: float,
    ):
        """Updates internal risk tracking from broker state or journal."""
        self._current_exposure = float(current_exposure)
        self._open_positions_count = int(open_positions)
        self._daily_trades_count = int(daily_trades)
        self._daily_realized_pnl = float(daily_pnl)

    def sync_from_journal_and_broker(
        self,
        journal_db: Optional[Any] = None,
        broker: Optional[Any] = None,
        watcher: Optional[Any] = None,
    ) -> None:
        """
        V12-08: Synchronizes portfolio risk counters from durable SQLite journal, active watcher orders,
        and broker positions (when available), resetting daily counters across IST day boundaries.
        """
        now_ist = datetime.datetime.now(IST)
        day_str = now_ist.strftime("%Y-%m-%d")
        if self._last_day_str and self._last_day_str != day_str:
            self._daily_trades_count = 0
            self._daily_realized_pnl = 0.0
        self._last_day_str = day_str

        start_of_day = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        if journal_db and hasattr(journal_db, "get_live_portfolio_risk_state"):
            try:
                st = journal_db.get_live_portfolio_risk_state(start_of_day=start_of_day)
                self._daily_trades_count = max(self._daily_trades_count, int(st.get("daily_trades", 0)))
                self._open_positions_count = max(self._open_positions_count, int(st.get("open_positions", 0)))
                self._current_exposure = max(self._current_exposure, float(st.get("current_exposure", 0.0)))
            except Exception:
                pass

        if watcher and hasattr(watcher, "pending"):
            try:
                active_secs = {str(r.get("security_id")) for r in watcher.pending.values() if r.get("security_id")}
                if active_secs:
                    self._open_positions_count = max(self._open_positions_count, len(active_secs))
            except Exception:
                pass

        if broker and hasattr(broker, "get_positions"):
            try:
                positions = broker.get_positions()
                if isinstance(positions, list) and positions:
                    open_pos = 0
                    pos_exposure = 0.0
                    realized_pnl = 0.0
                    for p in positions:
                        if not isinstance(p, dict):
                            continue
                        net_qty = int(p.get("netQty") or 0)
                        if net_qty != 0:
                            open_pos += 1
                            avg_p = float(p.get("costPrice") or p.get("buyAvg") or 0.0)
                            pos_exposure += abs(net_qty) * avg_p
                        realized_pnl += float(p.get("realizedProfit") or 0.0)
                    self._open_positions_count = max(self._open_positions_count, open_pos)
                    self._current_exposure = max(self._current_exposure, pos_exposure)
                    if realized_pnl != 0.0:
                        self._daily_realized_pnl = realized_pnl
            except Exception:
                pass

    def record_order_committed(self, order_cost_inr: float, is_new_position: bool = True) -> None:
        """V12-08: Reserves exposure, open position count, and daily trade count upon order commitment."""
        self._daily_trades_count += 1
        if is_new_position:
            self._open_positions_count += 1
        self._current_exposure += max(0.0, float(order_cost_inr))

    def validate_order_risk(
        self,
        order_cost_inr: float,
        available_funds_inr: float,
    ) -> Tuple[bool, str]:
        """
        Runs comprehensive pre-order risk validation.
        Returns: (is_passed, rejection_reason)
        """
        # 1. Durable emergency stop check
        if self.is_emergency_stopped:
            st = self.stop_manager.get_state()
            return False, f"Order rejected: EMERGENCY STOP is active ({st.get('stopped_by')}: {st.get('reason')})"

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

        # 8. Daily loss circuit breaker (INR & Drawdown %)
        if self._daily_realized_pnl < 0 and abs(self._daily_realized_pnl) >= self.cfg.max_daily_loss_inr:
            return False, (
                f"Daily loss limit breached: {self._daily_realized_pnl:.2f} INR "
                f"(limit: -{self.cfg.max_daily_loss_inr:.2f} INR)"
            )

        drawdown_pct = float(getattr(self.cfg, "max_drawdown_pct", 0.0) or 0.0)
        if drawdown_pct > 0.0 and self._daily_realized_pnl < 0 and available_funds_inr > 0:
            starting_equity = available_funds_inr + abs(self._daily_realized_pnl)
            actual_dd_pct = (abs(self._daily_realized_pnl) / starting_equity) * 100.0
            if actual_dd_pct >= drawdown_pct:
                return False, (
                    f"Max daily drawdown breached: {actual_dd_pct:.2f}% >= {drawdown_pct:.2f}% "
                    f"(realized loss: {self._daily_realized_pnl:.2f} INR)"
                )

        return True, "Passed risk validation"
