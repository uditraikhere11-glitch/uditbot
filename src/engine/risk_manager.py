"""
Risk Management and Emergency Circuit Breaker Engine.
Evaluates account health, trade limits, and provides instant halt controls.
Integrates with durable EmergencyStopManager to ensure STOP state persists across restarts.
Implements V12-08, V12-23 & V14 market-safety fixes:
- Synchronizes live portfolio state (`daily_trades`, `open_positions`, `current_exposure`, `daily_pnl`)
  from `JournalDB` and broker positions before validating new entries (`sync_from_journal_and_broker`).
- Initializes `_broker_positions_verified = False` in LIVE mode and requires a current successful
  broker-position synchronization before approving any LIVE order or retry (`validate_order_risk`).
- Fails closed in LIVE mode when broker positions cannot be retrieved or verified (preventing
  approval of new LIVE orders using potentially incomplete exposure information).
- Updates in-memory counters immediately when an order is committed (`record_order_committed`).
- Enforces `max_drawdown_pct` alongside `max_daily_loss_inr`.
"""

import datetime
import math
import os
import time
import zoneinfo
from typing import Any, Optional, Tuple
from src.config import AppConfig
from src.engine.emergency_stop import EmergencyStopManager
from src.models import BrokerReadFailure, TradingMode

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
        is_live_init = self._is_live_mode()
        self._broker_positions_verified: bool = not is_live_init
        self._broker_positions_error: str = (
            "Broker positions have not been synchronized or verified for LIVE mode"
            if is_live_init
            else ""
        )
        self._last_positions_sync_ts: float = 0.0

    def _is_live_mode(self) -> bool:
        return (
            getattr(self.cfg, "trading_mode", None) == TradingMode.LIVE
            or str(getattr(getattr(self.cfg, "trading_mode", ""), "value", getattr(self.cfg, "trading_mode", ""))).upper() == "LIVE"
        )

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
        if not self._is_live_mode():
            self._broker_positions_verified = True
            self._broker_positions_error = ""

    def sync_from_journal_and_broker(
        self,
        journal_db: Optional[Any] = None,
        broker: Optional[Any] = None,
        watcher: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """
        V12-08 & V14: Synchronizes portfolio risk counters from durable SQLite journal, active watcher orders,
        and broker positions, resetting daily counters across IST day boundaries.
        Fails closed (`_broker_positions_verified = False`) when LIVE broker positions cannot be
        retrieved or verified, or when open positions lack valid price/exposure data.
        """
        now_ist = datetime.datetime.now(IST)
        day_str = now_ist.strftime("%Y-%m-%d")
        if self._last_day_str and self._last_day_str != day_str:
            self._daily_trades_count = 0
            self._daily_realized_pnl = 0.0
        self._last_day_str = day_str

        is_live = self._is_live_mode()

        start_of_day = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        if journal_db and hasattr(journal_db, "get_live_portfolio_risk_state"):
            try:
                st = journal_db.get_live_portfolio_risk_state(start_of_day=start_of_day)
                if not isinstance(st, dict):
                    raise ValueError(f"Expected dict from get_live_portfolio_risk_state, got {type(st).__name__}")
                self._daily_trades_count = max(self._daily_trades_count, int(st.get("daily_trades", 0)))
                self._open_positions_count = max(self._open_positions_count, int(st.get("open_positions", 0)))
                self._current_exposure = max(self._current_exposure, float(st.get("current_exposure", 0.0)))
            except Exception as e:
                if is_live:
                    self._broker_positions_verified = False
                    self._last_positions_sync_ts = 0.0
                    self._broker_positions_error = f"Journal risk state could not be verified: {e}"
                    return False, self._broker_positions_error

        if watcher and hasattr(watcher, "pending"):
            try:
                active_secs = {str(r.get("security_id")) for r in watcher.pending.values() if isinstance(r, dict) and r.get("security_id")}
                if active_secs:
                    self._open_positions_count = max(self._open_positions_count, len(active_secs))
            except Exception:
                pass

        if broker is not None or is_live:
            if broker is None or not hasattr(broker, "get_positions"):
                if is_live:
                    self._broker_positions_verified = False
                    self._last_positions_sync_ts = 0.0
                    self._broker_positions_error = (
                        "Broker positions could not be retrieved or verified: broker client unavailable in LIVE mode"
                    )
                    return False, self._broker_positions_error
            else:
                try:
                    positions = broker.get_positions()
                except Exception as e:
                    self._broker_positions_verified = False
                    self._last_positions_sync_ts = 0.0
                    self._broker_positions_error = f"Broker positions retrieval failed: {e}"
                    return False, self._broker_positions_error

                if (
                    isinstance(positions, BrokerReadFailure)
                    or getattr(positions, "ok", True) is False
                    or not isinstance(positions, list)
                ):
                    err_detail = getattr(positions, "error", None) or f"invalid positions payload ({type(positions).__name__})"
                    self._broker_positions_verified = False
                    self._last_positions_sync_ts = 0.0
                    self._broker_positions_error = f"Broker positions could not be retrieved or verified: {err_detail}"
                    return False, self._broker_positions_error

                open_pos = 0
                pos_exposure = 0.0
                realized_pnl = 0.0
                for idx, p in enumerate(positions):
                    if not isinstance(p, dict):
                        self._broker_positions_verified = False
                        self._last_positions_sync_ts = 0.0
                        self._broker_positions_error = (
                            f"Broker positions could not be verified: record at index {idx} is not a dict"
                        )
                        return False, self._broker_positions_error

                    raw_qty = p.get("netQty")
                    if raw_qty is None or isinstance(raw_qty, bool):
                        self._broker_positions_verified = False
                        self._last_positions_sync_ts = 0.0
                        self._broker_positions_error = (
                            f"Broker positions could not be verified: missing netQty in position record {p.get('securityId', idx)}"
                        )
                        return False, self._broker_positions_error
                    try:
                        qty_f = float(raw_qty)
                        if not math.isfinite(qty_f) or int(qty_f) != qty_f:
                            raise ValueError("non-integer netQty")
                        net_qty = int(qty_f)
                    except (ValueError, TypeError):
                        self._broker_positions_verified = False
                        self._last_positions_sync_ts = 0.0
                        self._broker_positions_error = (
                            f"Broker positions could not be verified: invalid netQty={raw_qty!r}"
                        )
                        return False, self._broker_positions_error

                    if net_qty != 0:
                        open_pos += 1
                        avg_p: Optional[float] = None
                        for price_key in ("costPrice", "buyAvg", "sellAvg"):
                            cand = p.get(price_key)
                            if cand is None:
                                continue
                            if isinstance(cand, bool):
                                avg_p = None
                                break
                            try:
                                cand_f = float(cand)
                            except (ValueError, TypeError):
                                avg_p = None
                                break
                            if math.isfinite(cand_f) and cand_f > 0.0:
                                avg_p = cand_f
                                break

                        if avg_p is None:
                            self._broker_positions_verified = False
                            self._last_positions_sync_ts = 0.0
                            self._broker_positions_error = (
                                f"Broker positions could not be verified: open position (netQty={net_qty}) "
                                f"has missing or invalid costPrice/buyAvg (incomplete exposure information)"
                            )
                            return False, self._broker_positions_error

                        pos_exposure += abs(net_qty) * avg_p

                    raw_pnl = p.get("realizedProfit", 0.0)
                    if raw_pnl is not None:
                        if isinstance(raw_pnl, bool):
                            self._broker_positions_verified = False
                            self._last_positions_sync_ts = 0.0
                            self._broker_positions_error = "Broker positions could not be verified: invalid realizedProfit"
                            return False, self._broker_positions_error
                        try:
                            pnl_f = float(raw_pnl)
                            if not math.isfinite(pnl_f):
                                raise ValueError("non-finite realizedProfit")
                            realized_pnl += pnl_f
                        except (ValueError, TypeError):
                            self._broker_positions_verified = False
                            self._last_positions_sync_ts = 0.0
                            self._broker_positions_error = f"Broker positions could not be verified: invalid realizedProfit={raw_pnl!r}"
                            return False, self._broker_positions_error

                self._broker_positions_verified = True
                self._broker_positions_error = ""
                self._last_positions_sync_ts = time.monotonic()
                self._open_positions_count = max(self._open_positions_count, open_pos)
                self._current_exposure = max(self._current_exposure, pos_exposure)
                if realized_pnl != 0.0:
                    self._daily_realized_pnl = realized_pnl
        else:
            self._broker_positions_verified = True
            self._broker_positions_error = ""

        return True, "OK"

    def record_order_committed(self, order_cost_inr: float, is_new_position: bool = True) -> None:
        """
        V12-08 & V14: Reserves exposure, open position count, and daily trade count upon order commitment.
        In LIVE mode, invalidates the cached position verification so any subsequent LIVE order or retry
        requires a fresh broker-position synchronization.
        """
        self._daily_trades_count += 1
        if is_new_position:
            self._open_positions_count += 1
        self._current_exposure += max(0.0, float(order_cost_inr))
        if self._is_live_mode():
            self._broker_positions_verified = False
            self._last_positions_sync_ts = 0.0
            self._broker_positions_error = (
                "Broker positions must be re-synchronized after LIVE order commitment before approving another LIVE order"
            )

    def validate_order_risk(
        self,
        order_cost_inr: float,
        available_funds_inr: float,
        broker: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """
        Runs comprehensive pre-order risk validation.
        Requires a current successful broker-position synchronization in LIVE mode.
        Returns: (is_passed, rejection_reason)
        """
        if broker is not None:
            self.sync_from_journal_and_broker(broker=broker)

        # 1. Durable emergency stop check
        if self.is_emergency_stopped:
            st = self.stop_manager.get_state()
            return False, f"Order rejected: EMERGENCY STOP is active ({st.get('stopped_by')}: {st.get('reason')})"

        # 2. Broker position verification gate (fail-closed in LIVE mode if unverified or never synchronized)
        is_live = self._is_live_mode()
        if not self._broker_positions_verified or (is_live and self._last_positions_sync_ts <= 0.0):
            return False, (
                f"Order rejected: {self._broker_positions_error or 'Broker positions could not be retrieved or verified for LIVE mode (incomplete exposure information)'}"
            )

        if (
            isinstance(order_cost_inr, bool)
            or not isinstance(order_cost_inr, (int, float))
            or not math.isfinite(float(order_cost_inr))
            or float(order_cost_inr) <= 0.0
        ):
            return False, f"Order rejected: invalid order cost ({order_cost_inr!r})"

        if (
            isinstance(available_funds_inr, bool)
            or not isinstance(available_funds_inr, (int, float))
            or not math.isfinite(float(available_funds_inr))
        ):
            return False, f"Order rejected: invalid available funds ({available_funds_inr!r})"

        # 3. Minimum funds check
        if available_funds_inr < self.cfg.min_trade_funds_inr:
            return False, (
                f"Available funds ({available_funds_inr:.2f} INR) below "
                f"minimum required threshold ({self.cfg.min_trade_funds_inr:.2f} INR)"
            )

        # 4. Sufficient funds for this specific order
        if available_funds_inr < order_cost_inr:
            return False, (
                f"Insufficient funds ({available_funds_inr:.2f} INR) for order "
                f"cost ({order_cost_inr:.2f} INR)"
            )

        # 5. Max single order value limit
        if order_cost_inr > self.cfg.max_order_amount_inr:
            return False, (
                f"Order value ({order_cost_inr:.2f} INR) exceeds "
                f"MAX_ORDER_AMOUNT_INR limit ({self.cfg.max_order_amount_inr:.2f} INR)"
            )

        # 6. Max daily trades cap
        if (self._daily_trades_count + 1) > self.cfg.max_daily_trades:
            return False, (
                f"Daily trade limit reached ({self._daily_trades_count} >= {self.cfg.max_daily_trades})"
            )

        # 7. Max open positions cap
        if (self._open_positions_count + 1) > self.cfg.max_open_positions:
            return False, (
                f"Max open positions reached ({self._open_positions_count} >= {self.cfg.max_open_positions})"
            )

        # 8. Total portfolio exposure cap
        new_total_exposure = self._current_exposure + order_cost_inr
        if new_total_exposure > self.cfg.max_total_exposure_inr:
            return False, (
                f"New exposure ({new_total_exposure:.2f} INR) exceeds "
                f"MAX_TOTAL_EXPOSURE_INR ({self.cfg.max_total_exposure_inr:.2f} INR)"
            )

        # 9. Daily loss circuit breaker (INR & Drawdown %)
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
