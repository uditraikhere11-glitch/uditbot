"""Read-only, bounded Dhan order-update observer for independent shadow testing.

This module is intentionally NOT imported by the production run.py, DhanClient,
reconciliation gate, ProtectionWatcher, or order placer. It NEVER writes to
UditBot's trading journal, sends REST calls, places orders, cancels orders,
or declares fills. An explicitly supplied separate .shadow.sqlite3 evidence
ledger may persist *only* observations and is not a trading-state store.
It accepts only broker updates that match one unique durable journal identity.

A new connection starts DEGRADED because a WebSocket does not replay every
missed order update. No stream observation may clear a reconciliation gate.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
from typing import Any, Dict, Optional, Tuple

from src.engine.dhan_order_update_shadow import (
    DhanUpdateRejected, normalize_dhan_order_update,
)
from src.engine.shadow_execution_events import (
    EvidenceConflict, OrderProjection,
)


@dataclass(frozen=True)
class OwnedOrder:
    account_id: str
    order_id: str
    security_id: str
    exchange_segment: str
    product_type: str
    requested_qty: int


class JournalOwnership:
    """Non-mutating Dhan order identity lookup, using parameterized SQL."""

    def __init__(self, journal_path: str):
        path = Path(journal_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError("existing journal is required")
        self.uri = path.as_uri() + "?mode=ro"

    def find(self, order_id: str) -> Optional[OwnedOrder]:
        conn = sqlite3.connect(self.uri, uri=True, timeout=3.0)
        try:
            conn.execute("PRAGMA query_only=ON")
            claims = []
            for sql in (
                "SELECT account_id, broker_order_id, security_id, "
                "exchange_segment, product_type, quantity "
                "FROM order_intents WHERE broker_order_id=?",
                "SELECT account_id, broker_order_id, security_id, "
                "exchange_segment, product_type, quantity "
                "FROM protection_attempts WHERE broker_order_id=?",
            ):
                claims.extend(conn.execute(sql, (order_id,)).fetchall())
            if not claims:
                return None
            if len(claims) != 1:
                raise ValueError("ambiguous duplicate broker-order ownership claims")
            a, oid, sec, seg, prod, qty = claims[0]
            if any(not isinstance(v, str) or not v.strip()
                   for v in (a, oid, sec, seg, prod)):
                raise ValueError("incomplete journal order identity")
            if type(qty) is not int or qty <= 0:
                raise ValueError("invalid journal requested quantity")
            return OwnedOrder(a, oid, sec, seg.upper(), prod.upper(), qty)
        finally:
            conn.close()


class ShadowOrderObserver:
    """Event-only ledger. All unknowns increase caution, never order permissions."""

    def __init__(self, account_id: str, ownership: Any, *,
                 max_orders: int = 512, max_events_per_order: int = 128,
                 shadow_ledger: Any = None):
        if not account_id or not isinstance(account_id, str):
            raise ValueError("account_id required")
        if type(max_orders) is not int or not 1 <= max_orders <= 50000:
            raise ValueError("invalid max_orders")
        if type(max_events_per_order) is not int or not 1 <= max_events_per_order <= 10000:
            raise ValueError("invalid max_events_per_order")
        self.account_id = account_id
        self.ownership = ownership
        self.shadow_ledger = shadow_ledger
        if shadow_ledger is not None and shadow_ledger.account_id != account_id:
            raise ValueError("shadow ledger/account identity mismatch")
        self.max_orders = max_orders
        self.max_events_per_order = max_events_per_order
        self.projections: Dict[str, OrderProjection] = OrderedDict()
        self.received = 0
        self.ignored_unowned = 0
        self.duplicate_events = 0
        self.rejected_evidence = 0
        self.stream_gaps = 0
        self.needs_authoritative_rest_sync = True
        self.reasons: list[str] = ["NEW_STREAM_WITHOUT_AUTHORITATIVE_SNAPSHOT"]

    def _degrade(self, reason: str) -> None:
        self.needs_authoritative_rest_sync = True
        self.rejected_evidence += 1
        if reason not in self.reasons:
            self.reasons.append(reason[:96])
            if len(self.reasons) > 32:
                self.reasons.pop(0)

    def connection_lost(self, reason: str = "DISCONNECT") -> None:
        self.stream_gaps += 1
        self._degrade("STREAM_GAP_" + str(reason)[:60])

    def observe(self, raw: Any) -> str:
        self.received += 1
        if not isinstance(raw, dict):
            self._degrade("MALFORMED_STREAM_MESSAGE")
            return "REJECTED"
        if raw.get("Type") != "order_alert":
            return "NON_ORDER_MESSAGE"
        try:
            event = normalize_dhan_order_update(raw, expected_account_id=self.account_id)
        except (DhanUpdateRejected, ValueError, TypeError):
            self._degrade("INVALID_ORDER_ALERT")
            return "REJECTED"
        try:
            owned = self.ownership.find(event.broker_order_id)
        except (sqlite3.Error, OSError, ValueError, TypeError):
            self._degrade("OWNERSHIP_LOOKUP_UNAVAILABLE")
            return "REJECTED"
        if owned is None:
            # Dhan streams account-wide; manual/unrelated orders are not ours.
            self.ignored_unowned += 1
            return "UNOWNED_IGNORED"
        if (owned.account_id, owned.order_id, owned.security_id,
            owned.exchange_segment, owned.product_type) != (
                event.account_id, event.broker_order_id, event.security_id,
                event.exchange_segment, event.product_type):
            self._degrade("OWNED_ORDER_IDENTITY_CONFLICT")
            return "REJECTED"
        if event.cumulative_filled_qty is None or event.cumulative_filled_qty > owned.requested_qty:
            self._degrade("OWNED_ORDER_QUANTITY_CONFLICT")
            return "REJECTED"
        projection = self.projections.get(event.broker_order_id)
        if projection is None:
            if len(self.projections) >= self.max_orders:
                self._degrade("SHADOW_CAPACITY_EXHAUSTED")
                return "REJECTED"
            try:
                if self.shadow_ledger is None:
                    projection = OrderProjection(
                        owned.account_id, owned.order_id, owned.security_id,
                        owned.exchange_segment, owned.product_type, owned.requested_qty,
                    )
                else:
                    projection, _ = self.shadow_ledger.replay_order(
                        broker_order_id=owned.order_id,
                        security_id=owned.security_id,
                        exchange_segment=owned.exchange_segment,
                        product_type=owned.product_type,
                        requested_qty=owned.requested_qty,
                        max_events=self.max_events_per_order,
                    )
            except (EvidenceConflict, ValueError, sqlite3.Error):
                self._degrade("SHADOW_REPLAY_UNVERIFIED")
                return "REJECTED"
            self.projections[event.broker_order_id] = projection
        if len(projection._events) >= self.max_events_per_order and event.event_id not in projection._events:
            self._degrade("SHADOW_EVENT_CAPACITY_EXHAUSTED")
            return "REJECTED"
        if self.shadow_ledger is not None:
            try:
                self.shadow_ledger.append(event)
            except (ValueError, sqlite3.Error, OSError):
                self._degrade("SHADOW_PERSISTENCE_UNVERIFIED")
                return "REJECTED"
        try:
            result = projection.apply(event)
        except (EvidenceConflict, ValueError, TypeError):
            self._degrade("PROJECTOR_EVIDENCE_CONFLICT")
            return "REJECTED"
        if projection.reconciliation_required:
            self._degrade("PROJECTOR_RECONCILIATION_REQUIRED")
        if result == "DUPLICATE":
            self.duplicate_events += 1
        return "OWNED_" + result

    def summary(self) -> dict:
        return {
            "kind": "uditbot-dhan-order-shadow-v1",
            "received": self.received,
            "owned_observed_orders": len(self.projections),
            "ignored_unowned": self.ignored_unowned,
            "duplicate_events": self.duplicate_events,
            "rejected_evidence": self.rejected_evidence,
            "stream_gaps": self.stream_gaps,
            "needs_authoritative_rest_sync": self.needs_authoritative_rest_sync,
            "reason_codes": list(self.reasons),
            "may_authorize_orders": False,
            "may_modify_reconciliation_gate": False,
            "may_assert_verified_fills": False,
        }


class DhanShadowWebSocket:
    """Single, explicitly invoked observational socket session, no retries.

    An operator-managed service can provide its own supervised reconnect
    policy later. Each disconnect is reported as a gap before reconnect.
    """

    ENDPOINT = "wss://api-order-update.dhan.co"

    def __init__(self, observer: ShadowOrderObserver):
        self.observer = observer

    async def run_once(self, *, access_token: str, network_opt_in: bool = False,
                       max_messages: int = 100000) -> dict:
        if network_opt_in is not True:
            raise PermissionError("read-only network operation requires explicit opt-in")
        if not isinstance(access_token, str) or not access_token:
            raise ValueError("a current Dhan access token is required")
        if type(max_messages) is not int or not 1 <= max_messages <= 1000000:
            raise ValueError("invalid max_messages")
        # Import inside the explicit opt-in boundary. Never use broker mutation APIs.
        import websockets
        auth = {
            "LoginReq": {
                "MsgCode": 42,
                "ClientId": self.observer.account_id,
                "Token": access_token,
            },
            "UserType": "SELF",
        }
        try:
            async with websockets.connect(
                self.ENDPOINT, open_timeout=15, close_timeout=5,
                max_size=1_000_000, ping_interval=20, ping_timeout=20,
            ) as ws:
                await ws.send(json.dumps(auth, separators=(",", ":")))
                for _ in range(max_messages):
                    message = await ws.recv()
                    try:
                        raw = json.loads(message)
                    except (ValueError, TypeError):
                        self.observer._degrade("STREAM_JSON_INVALID")
                        continue
                    self.observer.observe(raw)
        except Exception as exc:
            # Only the exception TYPE is retained; never leak token / payload.
            self.observer.connection_lost(type(exc).__name__)
        else:
            # A deliberately bounded session is still a gap for a subsequent
            # session, unless REST reconciliation independently proves state.
            self.observer.connection_lost("SESSION_END")
        return self.observer.summary()
