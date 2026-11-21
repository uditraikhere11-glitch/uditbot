"""Non-authoritative, in-memory execution event projector for offline replay.

This is NOT a broker adapter, journal, risk gate, or order authorizer. It has
no I/O and cannot place, modify, cancel, or authorize an order. Differences
between WebSocket, REST and trade-book evidence must be investigated by the
existing reconciliation gate, never settled in favor of this projection.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple


class EvidenceConflict(ValueError):
    """Order events cannot be safely combined; broker reconciliation required."""


_TERMINAL = {"CANCELLED", "REJECTED", "EXPIRED", "FILLED", "TRADED"}
_STATUSES = {"PENDING", "OPEN", "PART_TRADED", "TRADED", "FILLED",
             "CANCELLED", "REJECTED", "EXPIRED"}
_KINDS = {"ACK", "STATUS", "TRADE", "UNKNOWN"}


@dataclass(frozen=True)
class BrokerEvent:
    event_id: str
    account_id: str
    broker_order_id: str
    security_id: str
    exchange_segment: str
    product_type: str
    kind: str
    status: Optional[str] = None
    cumulative_filled_qty: Optional[int] = None
    exchange_trade_id: Optional[str] = None
    traded_qty: Optional[int] = None


@dataclass
class OrderProjection:
    account_id: str
    broker_order_id: str
    security_id: str
    exchange_segment: str
    product_type: str
    requested_qty: int
    status: str = "UNKNOWN"
    last_broker_status: str = ""
    reconciliation_required: bool = False
    uncertainty_observed: bool = False
    cumulative_filled_qty: int = 0
    _events: Dict[str, BrokerEvent] = field(default_factory=dict, repr=False)
    _trades: Dict[str, int] = field(default_factory=dict, repr=False)
    _terminal_observed: bool = False
    _status_filled_qty: int = 0
    _previous_statuses: Set[str] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        if type(self.requested_qty) is not int or self.requested_qty <= 0:
            raise ValueError("requested_qty must be a positive integer")
        for v in (self.account_id, self.broker_order_id, self.security_id,
                  self.exchange_segment, self.product_type):
            if not isinstance(v, str) or not v.strip():
                raise ValueError("all account/order/instrument identities are required")

    @property
    def _identity(self) -> Tuple[str, str, str, str, str]:
        return (self.account_id, self.broker_order_id, self.security_id,
                self.exchange_segment, self.product_type)

    @property
    def unique_traded_qty(self) -> int:
        return sum(self._trades.values())

    @property
    def unfilled_qty(self) -> int:
        """Remaining order quantity. Never interpret as account exposure."""
        return self.requested_qty - self.cumulative_filled_qty

    def apply(self, event: BrokerEvent) -> str:
        """Return APPLIED or DUPLICATE; conflicts mark reconciliation_required."""
        if not isinstance(event, BrokerEvent):
            raise TypeError("BrokerEvent required")
        identity=(event.account_id,event.broker_order_id,event.security_id,
                  event.exchange_segment,event.product_type)
        if identity != self._identity:
            self.reconciliation_required = True
            raise EvidenceConflict("account, order or instrument identity mismatch")
        if not event.event_id or event.kind not in _KINDS:
            self.reconciliation_required = True
            raise EvidenceConflict("invalid event identity or kind")
        previous = self._events.get(event.event_id)
        if previous is not None:
            if previous == event:
                return "DUPLICATE"
            self.reconciliation_required = True
            raise EvidenceConflict("duplicate event ID with contradictory payload")
        if event.cumulative_filled_qty is not None:
            qty = event.cumulative_filled_qty
            if type(qty) is not int or not 0 <= qty <= self.requested_qty:
                self.reconciliation_required = True
                raise EvidenceConflict("invalid cumulative filled quantity")
        if event.kind == "TRADE":
            if (not event.exchange_trade_id or type(event.traded_qty) is not int
                    or event.traded_qty <= 0):
                self.reconciliation_required = True
                raise EvidenceConflict("TRADE requires exchange trade ID and positive quantity")
            old_qty = self._trades.get(event.exchange_trade_id)
            if old_qty is not None and old_qty != event.traded_qty:
                self.reconciliation_required = True
                raise EvidenceConflict("same exchange trade ID with conflicting quantity")
            if old_qty is None and self.unique_traded_qty + event.traded_qty > self.requested_qty:
                self.reconciliation_required = True
                raise EvidenceConflict("sum of unique trades exceeds order quantity")
        elif event.exchange_trade_id is not None or event.traded_qty is not None:
            self.reconciliation_required = True
            raise EvidenceConflict("non-TRADE event carries execution fields")

        status=(event.status or "").strip().upper()
        if status and status not in _STATUSES:
            self.reconciliation_required = True
            raise EvidenceConflict("unknown broker status")
        if event.kind in ("ACK","STATUS") and not status:
            self.reconciliation_required = True
            raise EvidenceConflict("ACK/STATUS must carry broker status")
        if event.kind in ("TRADE","UNKNOWN") and status:
            self.reconciliation_required = True
            raise EvidenceConflict("TRADE/UNKNOWN may not assert broker status")
        if event.kind == "UNKNOWN":
            self.uncertainty_observed = True
            self.reconciliation_required = True
        if status:
            if status in ("TRADED","FILLED") and (event.cumulative_filled_qty is not None
                    and event.cumulative_filled_qty != self.requested_qty):
                self.reconciliation_required = True
            if self._terminal_observed and status in ("OPEN","PENDING","PART_TRADED"):
                self.reconciliation_required = True
            if (self.last_broker_status in _TERMINAL and status in _TERMINAL
                    and status != self.last_broker_status):
                self.reconciliation_required = True
            if status in _TERMINAL:
                self._terminal_observed = True
            if not self._terminal_observed or status in _TERMINAL:
                self.last_broker_status = status
            self._previous_statuses.add(status)
        if event.kind == "TRADE":
            if (event.exchange_trade_id not in self._trades
                    and self._terminal_observed
                    and self.last_broker_status in ("CANCELLED","REJECTED","EXPIRED")):
                self.reconciliation_required = True
            self._trades.setdefault(event.exchange_trade_id,event.traded_qty)
        if event.cumulative_filled_qty is not None:
            if event.cumulative_filled_qty < self._status_filled_qty:
                self.reconciliation_required = True
            self._status_filled_qty=max(self._status_filled_qty,event.cumulative_filled_qty)
        self.cumulative_filled_qty=max(self.unique_traded_qty,self._status_filled_qty)
        if self.last_broker_status in ("CANCELLED","REJECTED","EXPIRED"):
            self.status="PARTIAL_TERMINAL" if self.cumulative_filled_qty else self.last_broker_status
        elif self.cumulative_filled_qty == self.requested_qty:
            self.status="FILLED_UNVERIFIED_POSITION"
        elif self.cumulative_filled_qty:
            self.status="PARTIAL_FILL"
        elif self.last_broker_status:
            self.status=self.last_broker_status
        self._events[event.event_id]=event
        return "APPLIED"
