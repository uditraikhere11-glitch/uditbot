"""Read-only, pinned-token Dhan evidence collector for the snapshot coordinator.

No consumer migration is performed here. A verified exchange session supplier is
required; the source never derives authorization from the host date alone.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

from src.broker.broker_snapshot import (
    LIVE_BOOKS, NS, BookEvidence, BookStatus, BrokerSnapshot, HistoryCoverage,
    SessionPhase, payload_digest,
)
from src.broker.dhan_token import is_token_error
from src.broker.read_governor import RECONCILIATION_PACING
from src.broker.snapshot_coordinator import (
    CrossProcessReadCoordinator, EnvironmentObservation, EvidenceUnavailable,
)

ROUTES = {'profile': '/v2/profile', 'holdings': '/v2/holdings',
          'positions': '/v2/positions', 'trade_book': '/v2/trades',
          'order_book': '/v2/orders', 'forever_orders': '/v2/forever/orders',
          'super_orders': '/v2/super/orders', 'funds': '/v2/fundlimit'}
JOURNAL_TABLES = ('order_intents', 'retry_reservations', 'orders', 'fills', 'exit_allocations',
                  'positions_lifecycle', 'protection_orders', 'protection_attempts',
                  'watcher_orders', 'manual_risk_acknowledgements',
                  'broker_attested_historical_fills', 'system_events', 'outbox')
OWNER_FILES = ('service_control_state.json', 'mode_transition_gate.json',
               'emergency_stop.json', 'config_restart_v2/apply_barrier.json')


def _file(path, *, optional=False, token=False):
    """Return metadata and bytes from one file identity, never follow a symlink."""
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise EvidenceUnavailable('SYMLINKED_SOURCE')
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if optional:
            return None, None
        raise
    try:
        s = os.fstat(fd)
        if (not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid()
                or s.st_mode & (0o077 if token else 0o022)):
            raise EvidenceUnavailable('INSECURE_SOURCE')
        with os.fdopen(os.dup(fd), 'rb') as stream:
            raw = stream.read(8 * 1024 * 1024 + 1)
        end = path.stat(follow_symlinks=False)
        signature = lambda x: (x.st_dev, x.st_ino, x.st_mtime_ns, x.st_size)
        if signature(s) != signature(end) or len(raw) != s.st_size:
            raise EvidenceUnavailable('SOURCE_CHANGED_DURING_READ')
        return signature(s), raw
    finally:
        os.close(fd)


def _signature(root, names):
    observations = []
    for name in names:
        ident, raw = _file(root / name, optional=True)
        if raw is not None:
            # An unreadable/corrupt control file is not an empty control state.
            if not isinstance(json.loads(raw), dict):
                raise EvidenceUnavailable('CONTROL_STATE_MALFORMED')
        observations.append((name, ident, hashlib.sha256(raw).hexdigest() if raw is not None else None))
    return payload_digest(observations)


def _journal_digest(path):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise EvidenceUnavailable('SYMLINKED_JOURNAL')
    # Existing DB only: never initialize schema, create files or alter journal mode.
    if not path.is_file():
        raise EvidenceUnavailable('JOURNAL_UNAVAILABLE')
    digest = hashlib.sha256()
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=.15)) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        for table in JOURNAL_TABLES:
            cursor = conn.execute('SELECT * FROM ' + table + ' ORDER BY rowid')
            digest.update(json.dumps([table, [d[0] for d in cursor.description]]).encode())
            for row in cursor:
                digest.update(json.dumps(row, ensure_ascii=False, allow_nan=False,
                                         separators=(',', ':')).encode())
                digest.update(b'\n')
    return digest.hexdigest()


def _quantity(value, *, signed=False, positive=False):
    return (type(value) is int or (type(value) is str and re.fullmatch(r'-?\d+', value) is not None)) and (
        int(value) > 0 if positive else signed or int(value) >= 0)


def _identity(value):
    return type(value) is str and bool(value) and value == value.strip() and value.upper() not in {'0', 'NONE', 'NULL'}


def _price(value):
    if type(value) not in (str, int, float):
        return False
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (ValueError, OverflowError):
        return False


def valid_collection(key, rows, account, *, historical_validator=None, order_id=None):
    if type(rows) is not list:
        return False
    for row in rows:
        if type(row) is not dict or not row or ('dhanClientId' in row and row['dhanClientId'] != account):
            return False
        if not _identity(row.get('securityId')):
            return False
        if key == 'holdings':
            if not all(_quantity(row[k]) for k in ('dpQty', 'totalQty', 't1Qty', 'availableQty', 'collateralQty', 'mtfQty') if k in row):
                return False
            if 'dpQty' not in row and 'totalQty' not in row:
                return False
            continue
        if not all(_identity(row.get(k)) for k in ('exchangeSegment', 'productType')):
            return False
        if key == 'positions':
            if not _quantity(row.get('netQty'), signed=True):
                return False
            continue
        if not _identity(row.get('orderId')) or (order_id is not None and row['orderId'] != order_id):
            return False
        if row.get('transactionType') not in ('BUY', 'SELL'):
            return False
        if key in ('trade_book', 'trades_for_order', 'history'):
            if not _quantity(row.get('tradedQuantity'), positive=True) or not _price(row.get('tradedPrice')):
                return False
            if key == 'history':
                if historical_validator is None or not historical_validator(row, expected_account_id=account,
                        expected_transaction_type=None, allow_missing_exchange_trade_id=True)[0]:
                    return False
            else:
                eid = row.get('exchangeTradeId')
                if not _identity(eid) or any(m in eid for m in ('-TRD-', '-FILL-', '-RECON-')):
                    return False
        elif not _identity(row.get('orderStatus')):
            return False
    return True


class DhanSnapshotSource:
    def __init__(self, client, session_supplier, *, utc=time.time_ns, mono=time.monotonic_ns):
        self.client = client
        self.account = client.cfg.dhan_client_id
        if type(self.account) is not str or not self.account or self.account != self.account.strip():
            raise EvidenceUnavailable('ACCOUNT_ID_UNVERIFIED')
        self.root = Path(os.path.abspath(client.cfg.data_dir))
        self.journal = Path(os.path.abspath(client.cfg.journal_db_path))
        self.token_path = Path(os.path.abspath(os.path.expanduser(client.cfg.dhan_token_file)))
        self.session_supplier = session_supplier
        self.utc, self.mono = utc, mono

    def observe(self):
        ident, _ = _file(self.token_path, token=True)
        owner_signature = _signature(self.root, OWNER_FILES)
        mutation_id, raw = _file(self.root / 'active_broker_mutations.json', optional=True)
        mutation_signature = payload_digest((mutation_id, hashlib.sha256(raw).hexdigest() if raw is not None else None))
        mutation_clear = False
        if raw is not None:
            ledger = json.loads(raw)
            if (not isinstance(ledger, dict) or ledger.get('schema_version') != 2
                    or ledger.get('account_id') != self.account or ledger.get('handoff_verified') is not True
                    or type(ledger.get('active')) is not list or type(ledger.get('unresolved')) is not list):
                raise EvidenceUnavailable('MUTATION_LEDGER_UNVERIFIED')
            # Unresolved mutations are durable reconciliation obligations, not
            # a reason to erase them or prohibit gathering fresh evidence.
            mutation_clear = not ledger['active']
        return EnvironmentObservation(ident, owner_signature, mutation_signature,
            _journal_digest(self.journal), self.session_supplier(), mutation_clear=mutation_clear)

    def coordinator(self):
        governor = self.client._get_active_read_governor()
        if governor is None or governor.account_id != self.account:
            raise EvidenceUnavailable('GOVERNOR_UNAVAILABLE')
        return CrossProcessReadCoordinator(self.root, self.account, observe=self.observe,
            collect=self.collect, clock_utc_ns=self.utc, clock_mono_ns=self.mono,
            on_coherent_snapshot=governor.record_coherent_snapshot)

    def collect(self, request, generation, sid, obs):
        ident, raw = _file(self.token_path, token=True)
        if ident != obs.token_identity:
            raise EvidenceUnavailable('TOKEN_CHANGED')
        token = raw.decode('utf-8').strip()
        if not token:
            raise EvidenceUnavailable('TOKEN_MISSING')
        headers = {'access-token': token, 'client-id': self.account,
                   'Content-Type': 'application/json', 'Accept': 'application/json'}
        start_u, start_m = self.utc(), self.mono()
        profile_expiry_mono = None
        def read(key, endpoint, params=None, *, requested=True, kind=None, profile_ok=True):
            nonlocal profile_expiry_mono
            u, m = self.utc(), self.mono()
            received_u = received_m = None
            params = params or {}
            def result(status, *, http=None, error=None, retry=None, sent=False, received=False, payload=None):
                complete = status in (BookStatus.OK_EMPTY, BookStatus.OK_RECORDS)
                return BookEvidence(key, endpoint, params, status, http, error, retry, sent, u,
                    received_u if received else None, m, received_m if received else None,
                    self.account, generation.token_epoch, payload,
                    payload_digest(payload) if complete else None, complete)
            if not requested:
                return result(BookStatus.NOT_REQUESTED)
            if not profile_ok or m >= request.deadline_mono_ns:
                return result(BookStatus.UNAVAILABLE)
            if _file(self.token_path, token=True)[0] != ident:
                raise EvidenceUnavailable('TOKEN_CHANGED')
            try:
                # The shared governor permits one short pre-transport admission retry.
                # Never start a new snapshot wave or retry a true broker cooldown.
                pacing = RECONCILIATION_PACING.set(request.deadline_mono_ns-m > 2*NS)
                try:
                    response = self.client._governed_get('https://api.dhan.co' + endpoint,
                        headers=dict(headers), timeout=min(5, (request.deadline_mono_ns-m)/NS),
                        priority='identity' if key == 'profile' else request.priority)
                finally:
                    RECONCILIATION_PACING.reset(pacing)
            except (requests.RequestException, OSError):
                return result(BookStatus.UNAVAILABLE, sent=True)
            # Timestamp transport receipt before payload parsing/validation.
            received_u, received_m = self.utc(), self.mono()
            code = response.status_code
            if response.headers.get('X-AuditBot-Local-Deferral') == '1':
                # Local pseudo-response has no broker HTTP status.
                try:
                    seconds = float(response.headers['Retry-After'])
                    retry = self.utc() + int(seconds * NS) if 0 <= seconds < 864000 else None
                except (KeyError, ValueError, OverflowError):
                    retry = None
                return result(BookStatus.COOLDOWN if code == 429 and not getattr(response, "_uditbot_deferral_short_window", False) else BookStatus.LOCAL_PACING, retry=retry)
            try:
                data = response.json()
            except (ValueError, TypeError):
                data = None
            raw_error = data.get('errorCode') if isinstance(data, dict) else None
            err = str(raw_error) if type(raw_error) in (str, int) and re.fullmatch(r'[A-Za-z0-9-]{1,32}', str(raw_error)) else None
            http = getattr(response, '_uditbot_original_status', code)
            if code == 429:
                try:
                    seconds = float(response.headers['Retry-After'])
                    retry = self.utc() + int(seconds*NS) if 0 <= seconds < 864000 else None
                except (KeyError, ValueError, OverflowError):
                    retry = None
                return result(BookStatus.COOLDOWN, http=http, error=err, retry=retry, sent=True, received=True)
            if is_token_error(code, response.text):
                return result(BookStatus.AUTH_FAILURE, http=http, error=err, sent=True, received=True)
            if code != 200 or err not in (None, '0'):
                return result(BookStatus.HTTP_FAILURE, http=http, error=err, sent=True, received=True)
            if key == 'profile':
                if type(data) is not dict or data.get('dhanClientId') != self.account:
                    return result(BookStatus.AUTH_FAILURE, http=http, sent=True, received=True)
                exp, _ = self.client.extract_token_expiration(token, data)
                claims = self.client._decode_jwt_claims(token)
                claim_account = claims.get('dhanClientId') or claims.get('clientId') or claims.get('client_id')
                good = (type(exp) in (int, float) and math.isfinite(exp)
                        and exp > self.utc()/NS and (not claim_account or claim_account == self.account))
                if good:
                    profile_expiry_mono = received_m + max(0, int(exp*NS)-received_u)
            elif key == 'edis':
                good = (type(data) is dict and data.get('isin') == params.get('isin')
                        and data.get('clientId') == self.account and data.get('status') == 'SUCCESS'
                        and type(data.get('totalQty')) in (str, int)
                        and re.fullmatch(r'[0-9]+', str(data['totalQty'])) is not None)
            elif key == 'funds':
                good = (type(data) is dict and type(data.get('availabelBalance')) in (int,float,str)
                        and (data.get('dhanClientId', self.account) == self.account))
                if good:
                    try: good = math.isfinite(float(data['availabelBalance']))
                    except (ValueError, OverflowError): good = False
            else:
                if kind == 'history' and type(data) is dict:
                    data = data.get('data')
                rows = [data] if kind == 'order_status' and type(data) is dict else data
                good = valid_collection(kind or key, rows, self.account,
                    historical_validator=self.client.validate_historical_trade_record,
                    order_id=params.get('order_id'))
                if kind == 'order_status' and (type(data) is not dict or not data):
                    good = False
            if not good:
                return result(BookStatus.MALFORMED, http=http, sent=True, received=True)
            try:
                evidence = result(BookStatus.OK_EMPTY if data == [] else BookStatus.OK_RECORDS,
                    http=http, sent=True, received=True, payload=data)
            except ValueError:
                return result(BookStatus.MALFORMED, http=http, sent=True, received=True)
            self.client._record_validated_get(response)
            return evidence

        profile = read('profile', ROUTES['profile'])
        histories, coverages = [], []
        if profile.complete:
            for lo, hi in request.history_ranges:
                keys, identities = [], set()
                terminal = False
                # Bound collection; missing terminal page means incomplete.
                for page in range(100):
                    key = f'history:{lo}:{hi}:{page}'
                    book = read(key, f'/v2/trades/{lo}/{hi}/{page}',
                                {'from_date': str(lo), 'to_date': str(hi), 'page': page}, kind='history')
                    histories.append(book); keys.append(key)
                    if not book.complete:
                        break
                    if book.status is BookStatus.OK_EMPTY:
                        terminal = True; break
                    duplicate = False
                    for row in book.payload:
                        eid = row.get('exchangeTradeId')
                        marker = ('EID', str(eid)) if _identity(eid) else ('ROLLUP', str(row.get('exchangeOrderId')), str(row.get('exchangeTime')))
                        if marker in identities:
                            duplicate = True; break
                        identities.add(marker)
                        try:
                            exchange = datetime.fromisoformat(str(row['exchangeTime']).replace('Z', '+00:00'))
                            zone = ZoneInfo('Asia/Kolkata')
                            day = exchange.astimezone(zone).date() if exchange.tzinfo else exchange.date()
                            if not lo <= day <= hi:
                                duplicate = True; break
                        except (KeyError, ValueError, TypeError):
                            duplicate = True; break
                    if duplicate:
                        break
                coverages.append(HistoryCoverage(lo, hi, tuple(keys), terminal, terminal,
                    max((b.received_mono_ns or 0 for b in histories if b.book_key in keys), default=0),
                    bool(obs.session.trading_session_date and hi < obs.session.trading_session_date)))
        books = {key: read(key, ROUTES[key], requested=key in request.required_books,
                           profile_ok=profile.complete) for key in sorted(LIVE_BOOKS)}
        statuses = {oid: read('order_status:'+oid, '/v2/orders/'+quote(oid, safe=''),
                    {'order_id':oid}, kind='order_status', profile_ok=profile.complete) for oid in request.order_status_ids}
        trades = {oid: read('trades_for_order:'+oid, '/v2/trades/'+quote(oid, safe=''),
                    {'order_id':oid}, kind='trades_for_order', profile_ok=profile.complete) for oid in request.trade_order_ids}
        funds = read('funds', ROUTES['funds'], profile_ok=profile.complete) if 'funds' in request.required_books else None
        # eDIS is supplementary ownership evidence, never execution evidence.
        # No exact explicit ISIN means no network request and no complete result.
        edis = (read('edis', '/v2/edis/inquire/' + request.edis_isin,
                     {'isin': request.edis_isin}, profile_ok=profile.complete)
                if 'edis' in request.required_books and request.edis_isin is not None else None)
        reasons = []
        if not profile.complete: reasons.append('PROFILE_UNVERIFIED')
        if any(not b.complete for key,b in books.items() if key in request.required_books): reasons.append('REQUIRED_BOOK_UNAVAILABLE')
        if len(coverages) != len(request.history_ranges) or any(not c.complete for c in coverages): reasons.append('HISTORY_INCOMPLETE')
        if any(not b.complete for b in (*statuses.values(), *trades.values())): reasons.append('ORDER_SCOPED_EVIDENCE_UNAVAILABLE')
        if funds is not None and not funds.complete: reasons.append('FUNDS_UNAVAILABLE')
        if 'edis' in request.required_books:
            if edis is None: reasons.append('EDIS_ISIN_REQUIRED')
            elif not edis.complete: reasons.append('EDIS_UNAVAILABLE')
        if obs.session.phase is SessionPhase.TRANSITION_UNVERIFIED: reasons.append('SESSION_UNVERIFIED')
        if not obs.mutation_clear: reasons.append('MUTATION_IN_FLIGHT_OR_UNVERIFIED')
        if _file(self.token_path, token=True)[0] != ident: raise EvidenceUnavailable('TOKEN_CHANGED')
        proc = Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()
        return BrokerSnapshot(sid, generation, self.account, self.account if profile.complete else None,
            profile.complete, obs.session, start_u, self.utc(), start_m, self.mono(), **books,
            historical_trades=tuple(histories), order_status=statuses, trades_for_order=trades,
            profile=profile, funds=funds, edis=edis, history_coverage=tuple(coverages),
            complete=not reasons, invalid_reasons=tuple(reasons), producer_pid=os.getpid(),
            producer_starttime=int(proc[19]), journal_digest=obs.journal_digest,
            profile_valid_until_mono_ns=profile_expiry_mono)
