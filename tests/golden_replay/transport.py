"""Offline HTTP transport simulation and safety sandbox for Phase 3 Golden Replay.

Replaces only the external HTTP transport boundary on real DhanClient (`client.session`),
recording full request/response telemetry and blocking any unconfigured external host
or unexpected subprocess/network access.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import hashlib
import json
import threading
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from requests import Response

from .clocks import DeterministicClock


class OfflineBoundaryViolation(RuntimeError):
    """Raised whenever code attempts unexpected external network or service access."""


@dataclass(frozen=True)
class RecordedHTTPExchange:
    request_id: int
    method: str
    url: str
    endpoint: str
    params: dict[str, Any]
    http_status: int | None
    broker_error_code: str | None
    retry_after: str | None
    request_started_utc_ns: int
    received_utc_ns: int
    request_started_mono_ns: int
    received_mono_ns: int
    exchange_ist_date: str
    token_epoch: str
    transport_sent: bool
    payload_sha256: str | None
    completeness: str
    caller: str = "DhanClient"


@dataclass
class ScriptedResponse:
    status_code: int = 200
    payload: Any = None
    raw_body: bytes | None = None
    headers: dict[str, str] = field(default_factory=dict)
    latency_seconds: float = 0.005
    raise_exc: BaseException | None = None
    broker_error_code: str | None = None
    completeness: str = "COMPLETE"


class SimulatedHTTPTransport:
    """Drop-in requests.Session replacement for real DhanClient in offline replays."""

    ALLOWED_HOST = "api.dhan.co"

    def __init__(
        self,
        clock: DeterministicClock,
        *,
        account_id: str,
        expected_token: str | None = None,
        token_epoch_label: str = "epoch-1",
    ) -> None:
        self.clock = clock
        self.account_id = str(account_id)
        self.expected_token = expected_token
        self.token_epoch_label = token_epoch_label
        self.headers: dict[str, str] = {}
        self._lock = threading.RLock()
        self._static_routes: dict[tuple[str, str], ScriptedResponse | Callable[..., ScriptedResponse]] = {}
        self._queued_routes: dict[tuple[str, str], deque[ScriptedResponse | Callable[..., ScriptedResponse]]] = {}
        self._history: list[RecordedHTTPExchange] = []
        self._seq = 0
        self._closed = False
        self.mutation_sends: list[RecordedHTTPExchange] = []

    def set_token_expectation(self, token: str | None, *, epoch_label: str | None = None) -> None:
        with self._lock:
            self.expected_token = token
            if epoch_label is not None:
                self.token_epoch_label = epoch_label

    def register(
        self,
        endpoint: str,
        response: ScriptedResponse | Callable[..., ScriptedResponse],
        *,
        method: str = "GET",
    ) -> None:
        with self._lock:
            self._static_routes[(method.upper(), endpoint)] = response

    def enqueue(
        self,
        endpoint: str,
        *responses: ScriptedResponse | Callable[..., ScriptedResponse],
        method: str = "GET",
    ) -> None:
        with self._lock:
            q = self._queued_routes.setdefault((method.upper(), endpoint), deque())
            q.extend(responses)

    def clear_routes(self) -> None:
        with self._lock:
            self._static_routes.clear()
            self._queued_routes.clear()

    def history(self) -> list[RecordedHTTPExchange]:
        with self._lock:
            return list(self._history)

    def count_sends(self, *, endpoint: str | None = None, method: str | None = None) -> int:
        with self._lock:
            return sum(
                1
                for item in self._history
                if item.transport_sent
                and (endpoint is None or item.endpoint == endpoint)
                and (method is None or item.method == method.upper())
            )

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        method_u = method.upper()
        parsed = urlparse(url)
        if parsed.hostname != self.ALLOWED_HOST:
            raise OfflineBoundaryViolation(
                f"Blocked unexpected external HTTP {method_u} to disallowed host: {url}"
            )
        endpoint = parsed.path
        params = dict(kwargs.get("params") or {})
        req_headers = dict(self.headers)
        req_headers.update(kwargs.get("headers") or {})

        with self._lock:
            self._seq += 1
            req_id = self._seq
            start_u = self.clock.time_ns()
            start_m = self.clock.monotonic_ns()

            q = self._queued_routes.get((method_u, endpoint))
            if not q and (method_u, endpoint) not in self._static_routes:
                raise OfflineBoundaryViolation(
                    f"Unscripted offline HTTP {method_u} {endpoint} (params={params})"
                )
            supplied_token = req_headers.get("access-token")
            if self.expected_token is not None and supplied_token != self.expected_token:
                scripted = ScriptedResponse(
                    status_code=401,
                    payload={
                        "errorType": "Invalid_Authentication",
                        "errorCode": "DH-901",
                        "errorMessage": "Invalid or expired synthetic token",
                    },
                    broker_error_code="DH-901",
                    completeness="AUTH_FAILURE",
                )
            else:
                entry = q.popleft() if q else self._static_routes[(method_u, endpoint)]
                scripted = entry(method_u, endpoint, params, req_headers) if callable(entry) else entry

            if scripted.latency_seconds > 0:
                self.clock.advance(scripted.latency_seconds)

            recv_u = self.clock.time_ns()
            recv_m = self.clock.monotonic_ns()

            if scripted.raise_exc is not None:
                rec = RecordedHTTPExchange(
                    request_id=req_id,
                    method=method_u,
                    url=url,
                    endpoint=endpoint,
                    params=params,
                    http_status=None,
                    broker_error_code=scripted.broker_error_code or "TRANSPORT_ERROR",
                    retry_after=None,
                    request_started_utc_ns=start_u,
                    received_utc_ns=recv_u,
                    request_started_mono_ns=start_m,
                    received_mono_ns=recv_m,
                    exchange_ist_date=self.clock.exchange_date().isoformat(),
                    token_epoch=self.token_epoch_label,
                    transport_sent=True,
                    payload_sha256=None,
                    completeness="TRANSPORT_EXCEPTION",
                )
                self._history.append(rec)
                if method_u in ("POST", "PUT", "DELETE"):
                    self.mutation_sends.append(rec)
                raise scripted.raise_exc

            body = (
                scripted.raw_body
                if scripted.raw_body is not None
                else json.dumps(scripted.payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
            )
            digest = hashlib.sha256(body).hexdigest()
            resp = Response()
            resp.status_code = int(scripted.status_code)
            resp._content = body
            resp.url = url
            resp.headers.update({"Content-Type": "application/json"})
            resp.headers.update(scripted.headers)

            err_code = scripted.broker_error_code
            if err_code is None and isinstance(scripted.payload, dict):
                err_code = scripted.payload.get("errorCode")

            rec = RecordedHTTPExchange(
                request_id=req_id,
                method=method_u,
                url=url,
                endpoint=endpoint,
                params=params,
                http_status=resp.status_code,
                broker_error_code=str(err_code) if err_code is not None else None,
                retry_after=resp.headers.get("Retry-After"),
                request_started_utc_ns=start_u,
                received_utc_ns=recv_u,
                request_started_mono_ns=start_m,
                received_mono_ns=recv_m,
                exchange_ist_date=self.clock.exchange_date().isoformat(),
                token_epoch=self.token_epoch_label,
                transport_sent=True,
                payload_sha256=digest,
                completeness=scripted.completeness,
            )
            self._history.append(rec)
            if method_u in ("POST", "PUT", "DELETE"):
                self.mutation_sends.append(rec)
            return resp

    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Response:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Response:
        return self.request("DELETE", url, **kwargs)

    def close(self) -> None:
        self._closed = True
