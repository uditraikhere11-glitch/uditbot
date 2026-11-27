"""Immutable broker evidence records for the locked unified-ledger contract.

This module is deliberately pure. It grants no order, entry or shutdown authority.
The acquisition boundary must supply validated endpoint evidence and a pinned
Generation; consumers must independently recheck generation at authorization.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from types import MappingProxyType

NS = 1_000_000_000
LIVE_BOOKS = frozenset({"holdings", "positions", "trade_book", "order_book",
                        "forever_orders", "super_orders"})
OPTIONAL_BOOKS = frozenset({"funds", "edis"})
PROFILE_BOOK = "profile"
BOOK_NAMES = LIVE_BOOKS | OPTIONAL_BOOKS | {PROFILE_BOOK}


class BookStatus(str, Enum):
    OK_EMPTY = "OK_EMPTY"
    OK_RECORDS = "OK_RECORDS"
    LOCAL_PACING = "LOCAL_PACING"
    COOLDOWN = "COOLDOWN"
    HTTP_FAILURE = "HTTP_FAILURE"
    AUTH_FAILURE = "AUTH_FAILURE"
    MALFORMED = "MALFORMED"
    INCOMPLETE = "INCOMPLETE"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_REQUESTED = "NOT_REQUESTED"


class SessionPhase(str, Enum):
    CURRENT_SIGNED = "CURRENT_SIGNED"
    PRIOR_SIGNED = "PRIOR_SIGNED"
    SETTLED = "SETTLED"
    TRANSITION_UNVERIFIED = "TRANSITION_UNVERIFIED"


class Purpose(str, Enum):
    ENTRY = "ENTRY"
    WATCHER = "WATCHER"
    STARTUP = "STARTUP"
    SHUTDOWN = "SHUTDOWN"
    UI = "UI"
    HEALTH = "HEALTH"
    CLEANUP = "CLEANUP"
    RISK = "RISK"


def _text(value):
    if type(value) is not str or not value or value.strip() != value:
        raise ValueError("Nonempty exact string required")
    return value


def _uint(value):
    if type(value) is not int or value < 0:
        raise ValueError("Nonnegative integer required")
    return value


def _boolean(value):
    if type(value) is not bool:
        raise ValueError("Boolean required")
    return value


def _digest(value):
    if type(value) is not str or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError("Invalid SHA256")
    return value


def account_key(account_id: str) -> str:
    return hashlib.sha256(_text(account_id).encode('utf-8')).hexdigest()[:24]


# Secrets must never cross the evidence persistence boundary. Reject rather
# than silently editing a response and presenting the edited data as original.
_SECRET_KEYS = frozenset({"accesstoken", "token", "refreshtoken", "authorization", "pin", "totp", "totpsecret", "password", "apikey", "apisecret"})


def freeze_json(value):
    if value is None or type(value) in (str, int, bool):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, Mapping):
        copied = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("Non-string JSON key")
            normalized = ''.join(ch for ch in key.lower() if ch.isalnum())
            if normalized in _SECRET_KEYS:
                raise ValueError("Secret-bearing evidence rejected")
            copied[key] = freeze_json(item)
        return MappingProxyType(copied)
    if type(value) in (list, tuple):
        return tuple(freeze_json(item) for item in value)
    raise ValueError("Non-JSON or non-finite evidence")


def thaw_json(value):
    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if type(value) is tuple:
        return [thaw_json(item) for item in value]
    return value


def canonical_json(value) -> str:
    return json.dumps(thaw_json(value), sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def payload_digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode('utf-8')).hexdigest()


@dataclass(frozen=True, slots=True)
class ContractKey:
    account_id: str
    security_id: str
    exchange_segment: str
    product_type: str

    def __post_init__(self):
        for name in ('account_id', 'security_id', 'exchange_segment', 'product_type'):
            _text(getattr(self, name))


@dataclass(frozen=True, slots=True)
class Generation:
    account_key: str
    boot_id: str
    token_epoch: int
    mutation_epoch: int
    owner_epoch: int
    journal_epoch: int
    session_epoch: int

    def __post_init__(self):
        if (type(self.account_key) is not str or len(self.account_key) != 24
                or any(c not in '0123456789abcdef' for c in self.account_key)):
            raise ValueError('Invalid account key')
        _text(self.boot_id)
        for name in ('token_epoch', 'mutation_epoch', 'owner_epoch', 'journal_epoch', 'session_epoch'):
            _uint(getattr(self, name))


@dataclass(frozen=True, slots=True)
class SessionContext:
    phase: SessionPhase
    trading_session_date: date | None
    observed_utc_ns: int
    calendar_sha256: str | None
    calendar_verified: bool

    def __post_init__(self):
        object.__setattr__(self, 'phase', SessionPhase(self.phase))
        _uint(self.observed_utc_ns)
        _boolean(self.calendar_verified)
        if self.trading_session_date is not None and type(self.trading_session_date) is not date:
            raise ValueError('Exact session date required')
        if self.calendar_sha256 is not None:
            _digest(self.calendar_sha256)
        if self.calendar_verified and (self.trading_session_date is None or self.calendar_sha256 is None):
            raise ValueError('Verified calendar identity missing')


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    snapshot_id: str
    book_key: str
    row_index: int | None
    payload_sha256: str
    journal_ref: str | None

    def __post_init__(self):
        _text(self.snapshot_id)
        _text(self.book_key)
        _digest(self.payload_sha256)
        if self.row_index is not None:
            _uint(self.row_index)
        if self.journal_ref is not None:
            _text(self.journal_ref)


@dataclass(frozen=True, slots=True)
class BookEvidence:
    book_key: str
    endpoint: str
    request_params: Mapping
    status: BookStatus
    http_status: int | None
    broker_error_code: str | None
    retry_after_utc_ns: int | None
    transport_sent: bool
    request_started_utc_ns: int
    received_utc_ns: int | None
    request_started_mono_ns: int
    received_mono_ns: int | None
    account_id: str
    token_epoch: int
    payload: object | None
    payload_sha256: str | None
    complete: bool

    def __post_init__(self):
        for name in ('book_key', 'endpoint', 'account_id'):
            _text(getattr(self, name))
        if ('?' in self.endpoint or '#' in self.endpoint or not self.endpoint.startswith('/v2/')):
            raise ValueError('Only exact Dhan route path allowed in evidence')
        if not isinstance(self.request_params, Mapping):
            raise ValueError('Request parameters must be a mapping')
        params = freeze_json(self.request_params)
        if any(type(v) not in (str, int) for v in params.values()):
            raise ValueError('Invalid request parameter type')
        object.__setattr__(self, 'request_params', params)
        object.__setattr__(self, 'status', BookStatus(self.status))
        for name in ('request_started_utc_ns', 'request_started_mono_ns', 'token_epoch'):
            _uint(getattr(self, name))
        for name in ('received_utc_ns', 'received_mono_ns', 'retry_after_utc_ns', 'http_status'):
            value = getattr(self, name)
            if value is not None:
                _uint(value)
        _boolean(self.transport_sent)
        _boolean(self.complete)
        if self.broker_error_code is not None:
            _text(self.broker_error_code)
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError('Invalid HTTP status')
        if (self.received_utc_ns is None) != (self.received_mono_ns is None):
            raise ValueError('Incomplete receipt clocks')
        if self.received_mono_ns is not None and (self.received_mono_ns < self.request_started_mono_ns
                or self.received_utc_ns < self.request_started_utc_ns):
            raise ValueError('Regressed receipt clock')
        if self.status in (BookStatus.OK_EMPTY, BookStatus.OK_RECORDS):
            if (not self.complete or not self.transport_sent or self.http_status != 200
                    or self.received_mono_ns is None or self.broker_error_code is not None
                    or self.payload is None):
                raise ValueError('Successful evidence must be complete actual validated HTTP 200')
            frozen = freeze_json(self.payload)
            if self.status is BookStatus.OK_EMPTY:
                if type(frozen) is not tuple or frozen:
                    raise ValueError('OK_EMPTY requires a validated empty collection')
            elif isinstance(frozen, Mapping):
                if not frozen:
                    raise ValueError('Empty object is not a verified collection')
            elif type(frozen) is not tuple or not frozen or any(not isinstance(row, Mapping) for row in frozen):
                raise ValueError('Malformed records payload')
            if self.book_key in LIVE_BOOKS and type(frozen) is not tuple:
                raise ValueError('Collection must be a tuple of records')
            rows = (frozen,) if isinstance(frozen, Mapping) else frozen
            for row in rows:
                if 'dhanClientId' in row and row['dhanClientId'] != self.account_id:
                    raise ValueError('Mismatched broker account')
            if self.book_key == PROFILE_BOOK and (not isinstance(frozen, Mapping)
                    or frozen.get('dhanClientId') != self.account_id):
                raise ValueError('Profile account identity unverified')
            if self.payload_sha256 != payload_digest(frozen):
                raise ValueError('Payload digest mismatch')
            object.__setattr__(self, 'payload', frozen)
        elif self.complete or self.payload is not None or self.payload_sha256 is not None:
            raise ValueError('Failed or missing read cannot carry success payload')
        if self.status in (BookStatus.LOCAL_PACING, BookStatus.NOT_REQUESTED) and self.transport_sent:
            raise ValueError('Unsent evidence cannot claim transport')


# BrokerResult is the public result alias; the locked design calls it BookEvidence.
BrokerResult = BookEvidence


@dataclass(frozen=True, slots=True)
class HistoryCoverage:
    from_date: date
    to_date: date
    page_keys: tuple[str, ...]
    terminal_empty_page_verified: bool
    complete: bool
    received_mono_ns: int
    archive_only: bool

    def __post_init__(self):
        if type(self.from_date) is not date or type(self.to_date) is not date or self.from_date > self.to_date:
            raise ValueError('Invalid history date range')
        keys = tuple(self.page_keys)
        for key in keys:
            _text(key)
        if len(set(keys)) != len(keys):
            raise ValueError('Duplicate history page key')
        object.__setattr__(self, 'page_keys', keys)
        for name in ('terminal_empty_page_verified', 'complete', 'archive_only'):
            _boolean(getattr(self, name))
        _uint(self.received_mono_ns)
        if self.complete and (not keys or not self.terminal_empty_page_verified):
            raise ValueError('Complete history requires terminal evidence')


@dataclass(frozen=True, slots=True)
class EvidenceRequest:
    account_id: str
    purpose: Purpose
    required_books: frozenset[str]
    history_ranges: tuple[tuple[date, date], ...] = ()
    order_status_ids: tuple[str, ...] = ()
    trade_order_ids: tuple[str, ...] = ()
    min_capture_mono_ns: int = 0
    independent_after_snapshot_id: str | None = None
    deadline_mono_ns: int = 0
    priority: str = 'normal'
    # Additive, optional selector: the existing eDIS endpoint is ISIN-scoped.
    edis_isin: str | None = None

    def __post_init__(self):
        _text(self.account_id)
        object.__setattr__(self, 'purpose', Purpose(self.purpose))
        books = frozenset(self.required_books)
        if not books or not books <= BOOK_NAMES:
            raise ValueError('Unknown or empty required books')
        object.__setattr__(self, 'required_books', books)
        if self.edis_isin is not None and (
                'edis' not in books or type(self.edis_isin) is not str
                or re.fullmatch(r'[A-Z0-9]{12}', self.edis_isin) is None):
            raise ValueError('Exact requested eDIS ISIN required')
        ranges = tuple(tuple(pair) for pair in self.history_ranges)
        for pair in ranges:
            if len(pair) != 2 or any(type(d) is not date for d in pair) or pair[0] > pair[1]:
                raise ValueError('Invalid requested history range')
        ordered = sorted(ranges)
        if any(b[0] <= a[1] for a, b in zip(ordered, ordered[1:])):
            raise ValueError('Overlapping history requests')
        object.__setattr__(self, 'history_ranges', ranges)
        for name in ('order_status_ids', 'trade_order_ids'):
            ids = tuple(getattr(self, name))
            for oid in ids:
                _text(oid)
            if len(set(ids)) != len(ids):
                raise ValueError('Duplicate order request')
            object.__setattr__(self, name, ids)
        for name in ('min_capture_mono_ns', 'deadline_mono_ns'):
            _uint(getattr(self, name))
        if self.independent_after_snapshot_id is not None:
            _text(self.independent_after_snapshot_id)
        if self.priority not in {'routine', 'low', 'normal', 'identity', 'urgent'}:
            raise ValueError('Invalid priority')


@dataclass(frozen=True, slots=True)
class ReadDeferredResult:
    # A corrupt/unavailable generation store cannot supply even an epoch.
    # Explicit None is failure-only, never a synthetic all-zero generation.
    generation: Generation | None
    status: BookStatus
    retry_at_mono_ns: int | None
    reason_code: str
    transport_sent: bool

    def __post_init__(self):
        object.__setattr__(self, 'status', BookStatus(self.status))
        if self.status in (BookStatus.OK_EMPTY, BookStatus.OK_RECORDS):
            raise ValueError('Deferred result is never success')
        if self.retry_at_mono_ns is not None:
            _uint(self.retry_at_mono_ns)
        _text(self.reason_code)
        _boolean(self.transport_sent)


@dataclass(frozen=True, slots=True)
class BrokerSnapshot:
    snapshot_id: str
    generation: Generation
    account_id: str
    profile_account_id: str | None
    profile_verified: bool
    session: SessionContext
    capture_started_utc_ns: int
    capture_finished_utc_ns: int
    capture_started_mono_ns: int
    capture_finished_mono_ns: int
    holdings: BookEvidence
    positions: BookEvidence
    trade_book: BookEvidence
    order_book: BookEvidence
    forever_orders: BookEvidence
    super_orders: BookEvidence
    historical_trades: tuple[BookEvidence, ...]
    order_status: Mapping
    trades_for_order: Mapping
    profile: BookEvidence
    funds: BookEvidence | None
    edis: BookEvidence | None
    history_coverage: tuple[HistoryCoverage, ...]
    complete: bool
    invalid_reasons: tuple[str, ...]
    producer_pid: int
    producer_starttime: int
    journal_digest: str | None
    # Non-secret supplemental expiry metadata, derived from the authenticated
    # profile and pinned token. Never extends the existing 30-second profile TTL.
    profile_valid_until_mono_ns: int | None = None
    schema_version: int = field(default=1, init=False)

    def __post_init__(self):
        _text(self.snapshot_id)
        if account_key(self.account_id) != self.generation.account_key:
            raise ValueError('Snapshot account mismatch')
        _boolean(self.profile_verified)
        _boolean(self.complete)
        for name in ('capture_started_utc_ns', 'capture_finished_utc_ns', 'capture_started_mono_ns',
                     'capture_finished_mono_ns', 'producer_pid', 'producer_starttime'):
            _uint(getattr(self, name))
        if (self.capture_finished_utc_ns < self.capture_started_utc_ns
                or self.capture_finished_mono_ns < self.capture_started_mono_ns):
            raise ValueError('Capture clock regression')
        if not self.producer_pid or not self.producer_starttime:
            raise ValueError('Missing producer identity')
        if self.journal_digest is not None:
            _digest(self.journal_digest)
        if self.profile_valid_until_mono_ns is not None:
            _uint(self.profile_valid_until_mono_ns)
        object.__setattr__(self, 'historical_trades', tuple(sorted(self.historical_trades, key=lambda b: (
            str(b.request_params.get('from_date', '')), str(b.request_params.get('to_date', '')),
            int(b.request_params.get('page', 0)), b.book_key))))
        object.__setattr__(self, 'history_coverage', tuple(sorted(self.history_coverage,
            key=lambda c: (c.from_date, c.to_date))))
        for name in ('order_status', 'trades_for_order'):
            mapping = dict(getattr(self, name))
            for key, value in mapping.items():
                _text(key)
                if not isinstance(value, BookEvidence) or value.request_params.get('order_id') != key:
                    raise ValueError('Order-scoped evidence identity mismatch')
            object.__setattr__(self, name, MappingProxyType(mapping))
        reasons = tuple(self.invalid_reasons)
        for reason in reasons:
            _text(reason)
        object.__setattr__(self, 'invalid_reasons', reasons)
        for name in LIVE_BOOKS | {PROFILE_BOOK}:
            if not isinstance(getattr(self, name), BookEvidence) or getattr(self, name).book_key != name:
                raise ValueError('Book identity mismatch')
        for name in OPTIONAL_BOOKS:
            if getattr(self, name) is not None and getattr(self, name).book_key != name:
                raise ValueError('Optional book identity mismatch')
        books = self.all_books()
        if len({b.book_key for b in books}) != len(books):
            raise ValueError('Duplicate snapshot book identity')
        for book in books:
            if not isinstance(book, BookEvidence) or book.account_id != self.account_id or book.token_epoch != self.generation.token_epoch:
                raise ValueError('Book generation or account mismatch')
            if (not self.capture_started_mono_ns <= book.request_started_mono_ns <= self.capture_finished_mono_ns
                    or not self.capture_started_utc_ns <= book.request_started_utc_ns <= self.capture_finished_utc_ns
                    or (book.received_mono_ns is not None and book.received_mono_ns > self.capture_finished_mono_ns)
                    or (book.received_utc_ns is not None and book.received_utc_ns > self.capture_finished_utc_ns)):
                raise ValueError('Receipt outside capture')
        if self.profile_verified and (self.profile_account_id != self.account_id or not self.profile.complete
                or not isinstance(self.profile.payload, Mapping)
                or self.profile.payload.get('dhanClientId') != self.account_id):
            raise ValueError('Profile verification unsupported')
        if self.complete and (reasons or not self.profile_verified or not self.session.calendar_verified
                or self.session.phase is SessionPhase.TRANSITION_UNVERIFIED):
            raise ValueError('Incomplete identity or session cannot publish complete snapshot')

    def all_books(self):
        return (tuple(getattr(self, k) for k in sorted(LIVE_BOOKS | {PROFILE_BOOK}))
                + tuple(getattr(self, k) for k in sorted(OPTIONAL_BOOKS) if getattr(self, k) is not None)
                + self.historical_trades + tuple(self.order_status.values()) + tuple(self.trades_for_order.values()))

    def validation_reasons(self, request: EvidenceRequest, generation: Generation, now_mono_ns: int,
                           *, independent_after_mono_ns: int | None = None) -> tuple[str, ...]:
        """Return all blocking evidence reasons; no authority is granted by a cache hit."""
        _uint(now_mono_ns)
        reasons = list(self.invalid_reasons)
        if request.purpose is Purpose.UI:
            reasons.append('DISPLAY_ONLY_REQUEST')
        if request.account_id != self.account_id or generation != self.generation:
            reasons.append('GENERATION_MISMATCH')
        if not self.complete or not self.profile_verified:
            reasons.append('SNAPSHOT_INCOMPLETE')
        if (self.profile_valid_until_mono_ns is None
                or now_mono_ns >= self.profile_valid_until_mono_ns):
            reasons.append('PROFILE_EXPIRY_UNVERIFIED_OR_EXPIRED')
        if not self.session.calendar_verified or self.session.phase is SessionPhase.TRANSITION_UNVERIFIED:
            reasons.append('SESSION_UNVERIFIED')
        if request.deadline_mono_ns and now_mono_ns > request.deadline_mono_ns:
            reasons.append('REQUEST_DEADLINE_EXPIRED')
        if self.capture_finished_mono_ns > now_mono_ns or self.capture_started_mono_ns < request.min_capture_mono_ns:
            reasons.append('CAPTURE_OUTSIDE_FENCE')
        if request.independent_after_snapshot_id is not None:
            if (self.snapshot_id == request.independent_after_snapshot_id or independent_after_mono_ns is None
                    or self.capture_started_mono_ns <= independent_after_mono_ns):
                reasons.append('INDEPENDENT_CAPTURE_REQUIRED')
        required = set(request.required_books) | {PROFILE_BOOK}
        live = []
        for key in sorted(required):
            book = getattr(self, key, None)
            if book is None or not book.complete:
                reasons.append('BOOK_UNAVAILABLE:' + key)
            elif key == 'edis' and (request.edis_isin is None
                    or dict(book.request_params) != {'isin': request.edis_isin}
                    or book.endpoint != '/v2/edis/inquire/' + request.edis_isin
                    or not isinstance(book.payload, Mapping)
                    or book.payload.get('isin') != request.edis_isin
                    or book.payload.get('clientId') != self.account_id
                    or book.payload.get('status') != 'SUCCESS'
                    or type(book.payload.get('totalQty')) not in (str, int)
                    or re.fullmatch(r'[0-9]+', str(book.payload['totalQty'])) is None):
                reasons.append('EDIS_IDENTITY_UNVERIFIED')
            elif key == PROFILE_BOOK:
                if not 0 <= now_mono_ns - book.received_mono_ns <= 30 * NS:
                    reasons.append('PROFILE_STALE')
            else:
                live.append(book)
        for ids, mapping, label in ((request.order_status_ids, self.order_status, 'ORDER_STATUS'),
                                    (request.trade_order_ids, self.trades_for_order, 'ORDER_TRADES')):
            for oid in ids:
                book = mapping.get(oid)
                if book is None or not book.complete:
                    reasons.append(label + '_UNAVAILABLE:' + oid)
                else:
                    live.append(book)
        history = {b.book_key: b for b in self.historical_trades}
        execution_ids = set()
        for lo, hi in request.history_ranges:
            matches = [c for c in self.history_coverage if (c.from_date, c.to_date) == (lo, hi)]
            if len(matches) != 1 or not matches[0].complete:
                reasons.append('HISTORY_INCOMPLETE')
                continue
            coverage = matches[0]
            pages = [history.get(key) for key in coverage.page_keys]
            if (not pages or any(p is None or not p.complete for p in pages)
                    or pages[-1].status is not BookStatus.OK_EMPTY
                    or any(p.status is not BookStatus.OK_RECORDS for p in pages[:-1])
                    or any(p.book_key != f'history:{lo}:{hi}:{i}'
                           or p.endpoint != f'/v2/trades/{lo}/{hi}/{i}'
                           or dict(p.request_params) != {'from_date': str(lo), 'to_date': str(hi), 'page': i}
                           for i, p in enumerate(pages))
                    or coverage.received_mono_ns != max(p.received_mono_ns for p in pages)):
                reasons.append('HISTORY_INCOMPLETE')
                continue
            for page in pages[:-1]:
                for row in page.payload:
                    eid = row.get('exchangeTradeId')
                    # Blank-ID rollups identify a broker order/date/side, never
                    # a fabricated execution. Exact/rollup overlap is reviewed
                    # at the ledger allocation boundary in Phase 2C.
                    key = ('EID', eid) if isinstance(eid, str) and eid.strip() else (
                        'ROLLUP', row.get('orderId'), row.get('exchangeTime'), row.get('transactionType'))
                    if key in execution_ids:
                        reasons.append('HISTORY_DUPLICATE_EXECUTION')
                    execution_ids.add(key)
            if coverage.archive_only:
                if (self.session.trading_session_date is None or hi >= self.session.trading_session_date
                        or any(not 0 <= now_mono_ns - p.received_mono_ns <= 300 * NS for p in pages)):
                    reasons.append('ARCHIVE_STALE_OR_UNSETTLED')
            else:
                live.extend(pages)
        if live:
            oldest = min(b.received_mono_ns for b in live)
            newest = max(b.received_mono_ns for b in live)
            earliest = min(b.request_started_mono_ns for b in live)
            if not 0 <= now_mono_ns - oldest <= 10 * NS:
                reasons.append('LIVE_BOOK_STALE')
            if newest - oldest > 5 * NS or newest - earliest > 10 * NS:
                reasons.append('LIVE_BOOK_SKEW')
            if earliest < request.min_capture_mono_ns:
                reasons.append('BOOK_PRECEDES_FENCE')
        return tuple(dict.fromkeys(reasons))
