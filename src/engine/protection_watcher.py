"""
Persistent Partial-Fill Protection Watcher.
Tracks resting limit buy orders across restarts and polls DhanHQ for execution fills.
Places broker-side Forever OCO / Super Order protection (SL + Target) chunk-by-chunk
as partial fills occur, based strictly on the official executed price from the exchange.
"""

import json
import logging
import math
import os
import threading
import time
from typing import Any, Callable, Dict, Optional
from src.engine.sizer import round_to_tick, calculate_protection_levels
from src.models import ProtectionLevels

logger = logging.getLogger(__name__)

DEFAULT_PENDING_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "pending_protection.json",
)

TERMINAL_STATUSES = frozenset({"TRADED", "CLOSED", "CANCELLED", "REJECTED", "EXPIRED"})
TICK = 0.05
RETRY_AFTER_SEC = 60


class ProtectionWatcher:
    """
    Watches pending resting buy orders and places Forever OCO protection legs
    only AFTER the buy fills, matching the real executed trade price.
    """

    def __init__(
        self,
        broker: Any,
        sl_pct: float = 3.0,
        target_pct: float = 3.0,
        sl_limit_buffer_pct: float = 1.0,
        path: Optional[str] = None,
        dry_run: bool = False,
        clock: Callable[[], float] = time.time,
    ):
        self.broker = broker
        self.sl_pct = sl_pct
        self.target_pct = target_pct
        self.sl_limit_buffer_pct = sl_limit_buffer_pct
        self.path = path or os.environ.get("PENDING_PROTECTION_FILE") or DEFAULT_PENDING_FILE
        self.dry_run = dry_run
        self.clock = clock
        self._lock = threading.Lock()
        self.pending: Dict[str, dict] = self._load()

        if self.pending:
            logger.info(f"[Protection] Resumed {len(self.pending)} resting order(s) awaiting fill: {list(self.pending.keys())}")

    def _load(self) -> Dict[str, dict]:
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"[Protection] Could not read pending protection file {self.path}: {e}")
            return {}

    def _save(self) -> None:
        with self._lock:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
                tmp_path = f"{self.path}.tmp"
                snapshot = {k: dict(v) for k, v in self.pending.items()}
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, indent=2)
                os.replace(tmp_path, self.path)
            except Exception as e:
                logger.error(f"[Protection] Failed saving pending protection file: {e}")

    def track(
        self,
        order_id: str,
        symbol: str,
        security_id: str,
        segment: str,
        qty: int,
        product_type: str = "CNC",
        dry_price: Optional[float] = None,
    ) -> str:
        """
        Registers an active buy order to be protected upon execution.
        """
        oid = order_id
        if self.dry_run or not oid:
            oid = f"DRY-{int(self.clock() * 1000)}-{symbol}"

        record = {
            "order_id": oid,
            "symbol": symbol,
            "security_id": str(security_id),
            "segment": segment,
            "product_type": product_type,
            "qty": int(qty),
            "protected_qty": 0,
            "protected_value": 0.0,
            "placed_at": self.clock(),
            "dry_price": dry_price,
            "retry_at": 0.0,
        }

        with self._lock:
            self.pending[oid] = record
        self._save()

        logger.info(
            f"[Protection] Tracking {symbol} order {oid} ({qty} units). "
            f"SL/Target will be attached once filled at -{self.sl_pct:g}% / +{self.target_pct:g}%."
        )
        return oid

    def _get_order_status(self, rec: dict) -> Optional[dict]:
        oid = rec["order_id"]
        if oid.startswith("DRY-"):
            if rec.get("dry_price") is None:
                return {"orderStatus": "PENDING", "filledQty": 0}
            return {
                "orderStatus": "TRADED",
                "filledQty": rec["qty"],
                "averageTradedPrice": rec["dry_price"],
            }

        # Query live broker
        if hasattr(self.broker, "order_status"):
            return self.broker.order_status(oid)
        return None

    def check_once(self) -> bool:
        """
        Evaluates one pass over all tracked orders.
        Returns True if any orders were updated or resolved.
        """
        changed = False
        current_time = self.clock()

        for order_id, rec in list(self.pending.items()):
            if rec.get("retry_at", 0) > current_time:
                continue

            try:
                st = self._get_order_status(rec)
            except Exception as e:
                logger.warning(f"[Protection] Status poll failed for {rec['symbol']} {order_id}: {e}")
                continue

            if not st:
                continue

            status = str(st.get("orderStatus", "")).upper()
            filled = int(st.get("filledQty") or 0)
            new_qty = filled - rec["protected_qty"]

            # If new shares executed, place protection
            if new_qty > 0:
                changed |= self._protect(rec, st, filled, new_qty)

            # Check if order has reached terminal status
            if status in TERMINAL_STATUSES and rec["protected_qty"] >= filled:
                if filled == 0:
                    logger.info(f"[Protection] {rec['symbol']} order {order_id} ended ({status}) with 0 fills; dropped.")
                else:
                    logger.info(f"[Protection] {rec['symbol']} order {order_id} completed ({status}); {rec['protected_qty']} units protected.")

                with self._lock:
                    self.pending.pop(order_id, None)
                changed = True

        if changed:
            self._save()
        return changed

    def _protect(self, rec: dict, st: dict, filled: int, new_qty: int) -> bool:
        # Determine average execution price
        avg_all = float(st.get("averageTradedPrice") or 0.0)
        if not avg_all and not rec["order_id"].startswith("DRY-"):
            if hasattr(self.broker, "trades_avg_price"):
                avg_all = self.broker.trades_avg_price(rec["order_id"]) or 0.0

        if not avg_all:
            logger.warning(
                f"[Protection] {rec['symbol']} executed {filled} units, but Dhan hasn't reported "
                f"average execution price yet. Retrying in 5s."
            )
            rec["retry_at"] = self.clock() + 5.0
            return True

        # Compute price of the newly filled tranche (handles partial fills cleanly)
        executed_chunk = round((avg_all * filled - rec["protected_value"]) / new_qty, 4)
        levels = calculate_protection_levels(
            executed_price=executed_chunk,
            quantity=new_qty,
            sl_pct=self.sl_pct,
            target_pct=self.target_pct,
            sl_limit_buffer_pct=self.sl_limit_buffer_pct,
            tick_size=TICK,
        )

        # Place broker order
        if rec["order_id"].startswith("DRY-"):
            logger.info(
                f"[DRY RUN] Protected {new_qty}x {rec['symbol']} @ {executed_chunk}: "
                f"Target {levels.target_price} (+{self.target_pct:g}%), "
                f"SL Trigger {levels.sl_trigger} (-{self.sl_pct:g}%, Limit {levels.sl_limit})"
            )
            result = {"order_id": "DRY-OCO-SUCCESS"}
        else:
            result = None
            if hasattr(self.broker, "place_forever_oco"):
                # Call broker Forever OCO
                corr_id = f"OCO-{rec['order_id'][-8:]}-{int(self.clock())}"
                success, oco_id, msg = self.broker.place_forever_oco(
                    correlation_id=corr_id,
                    security_id=rec["security_id"],
                    exchange_segment=rec["segment"],
                    product_type=rec.get("product_type", "CNC"),
                    levels=levels,
                )
                if success:
                    result = {"order_id": oco_id}
                else:
                    logger.error(f"[Protection] Broker Forever OCO rejected for {rec['symbol']}: {msg}")

        if not result or not result.get("order_id"):
            logger.error(
                f"[Protection] ⚠️ {rec['symbol']}: {new_qty} units filled @ {executed_chunk} remain UNPROTECTED. "
                f"Retrying in {RETRY_AFTER_SEC}s."
            )
            rec["retry_at"] = self.clock() + RETRY_AFTER_SEC
            return True

        rec["protected_qty"] += new_qty
        rec["protected_value"] += executed_chunk * new_qty
        rec["retry_at"] = 0.0

        logger.info(
            f"[Protection] ✅ {rec['symbol']}: {new_qty} units filled @ {executed_chunk} → "
            f"SL {levels.sl_trigger} / Target {levels.target_price} placed at Dhan (Order ID: {result['order_id']})."
        )
        return True

    async def run(self, is_market_open_fn: Callable[[], bool]):
        """
        Polls tracked orders periodically in an async loop:
        Checks every 3s during active market hours, and every 60s when market is closed.
        """
        import asyncio

        while True:
            if self.pending:
                try:
                    await asyncio.to_thread(self.check_once)
                except Exception as e:
                    logger.exception(f"[Protection] Watcher pass exception: {e}")

            sleep_sec = 3 if is_market_open_fn() else 60
            await asyncio.sleep(sleep_sec)
