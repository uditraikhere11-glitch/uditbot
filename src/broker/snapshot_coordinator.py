"""Account-scoped SQLite snapshot store and single-flight acquisition boundary.

The environment and collector are injected: no authorization or journal writes
occur here. A caller must supply the production environment observer and an
endpoint-validating collector before enabling runtime consumers. This boundary
never obtains an operation or broker mutation lock while collecting.
"""
from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import stat
import time
from collections.abc import Callable
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from src.broker.broker_snapshot import (
    NS, LIVE_BOOKS, OPTIONAL_BOOKS, BookEvidence, BookStatus, BrokerSnapshot,
    EvidenceRequest, Generation, HistoryCoverage, Purpose, ReadDeferredResult,
    SessionContext, account_key, canonical_json,
)


class EvidenceUnavailable(RuntimeError):
    """Snapshot store, generation source or acquisition cannot be trusted."""


@dataclass(frozen=True, slots=True)
class EnvironmentObservation:
    """Only nonsecret generation observations; collector pins its own token.

    The digest of relevant journal rows is a read-only, consistent view, not
    merely a file mtime. Owner/mutation signatures must include durable file
    identity so an atomic replacement back to the same value is observable.
    """
    token_identity: tuple[int, int, int, int]
    owner_signature: str
    mutation_signature: str
    journal_digest: str
    session: SessionContext
    mutation_clear: bool = False

    def __post_init__(self):
        ident=tuple(self.token_identity)
        if len(ident)!=4 or any(type(x) is not int or x<0 for x in ident) or not ident[3]:
            raise EvidenceUnavailable('TOKEN_IDENTITY_UNAVAILABLE')
        object.__setattr__(self,'token_identity',ident)
        for name in ('owner_signature','mutation_signature','journal_digest'):
            v=getattr(self,name)
            if type(v) is not str or len(v)!=64 or any(c not in '0123456789abcdef' for c in v):
                raise EvidenceUnavailable('GENERATION_SOURCE_UNAVAILABLE')
        if type(self.mutation_clear) is not bool:
            raise EvidenceUnavailable('MUTATION_OBSERVATION_MALFORMED')
        if not self.session.calendar_verified:
            raise EvidenceUnavailable('SESSION_UNVERIFIED')


_SCHEMA = '''
CREATE TABLE IF NOT EXISTS account_state (
 account_key TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1), boot_id TEXT NOT NULL,
 token_epoch INTEGER NOT NULL CHECK(token_epoch>=0), token_dev INTEGER, token_inode INTEGER, token_mtime_ns INTEGER, token_size INTEGER,
 mutation_epoch INTEGER NOT NULL CHECK(mutation_epoch>=0), owner_epoch INTEGER NOT NULL CHECK(owner_epoch>=0),
 journal_epoch INTEGER NOT NULL CHECK(journal_epoch>=0), session_epoch INTEGER NOT NULL CHECK(session_epoch>=0),
 next_snapshot_seq INTEGER NOT NULL CHECK(next_snapshot_seq>=1), invalidation_reason TEXT NOT NULL, updated_utc_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots (
 snapshot_id TEXT PRIMARY KEY, account_key TEXT NOT NULL REFERENCES account_state(account_key), boot_id TEXT NOT NULL,
 token_epoch INTEGER NOT NULL, mutation_epoch INTEGER NOT NULL, owner_epoch INTEGER NOT NULL, journal_epoch INTEGER NOT NULL, session_epoch INTEGER NOT NULL,
 purpose TEXT NOT NULL, required_books_json TEXT NOT NULL,
 capture_started_utc_ns INTEGER NOT NULL, capture_finished_utc_ns INTEGER NOT NULL,
 capture_started_mono_ns INTEGER NOT NULL, capture_finished_mono_ns INTEGER NOT NULL,
 session_date TEXT, session_phase TEXT NOT NULL, calendar_sha256 TEXT, calendar_verified INTEGER NOT NULL CHECK(calendar_verified IN (0,1)),
 profile_account_id TEXT, profile_verified INTEGER NOT NULL CHECK(profile_verified IN (0,1)),
 state TEXT NOT NULL CHECK(state IN ('COMPLETE','INCOMPLETE','INVALID')), invalid_reasons_json TEXT NOT NULL, expires_mono_ns INTEGER NOT NULL,
 producer_pid INTEGER NOT NULL, producer_starttime INTEGER NOT NULL, journal_digest TEXT);
CREATE TABLE IF NOT EXISTS snapshot_books (
 snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE, book_key TEXT NOT NULL, endpoint TEXT NOT NULL,
 request_params_json TEXT NOT NULL, status TEXT NOT NULL, http_status INTEGER, broker_error_code TEXT, retry_after_utc_ns INTEGER,
 transport_sent INTEGER NOT NULL CHECK(transport_sent IN (0,1)), request_started_utc_ns INTEGER NOT NULL, received_utc_ns INTEGER,
 request_started_mono_ns INTEGER NOT NULL, received_mono_ns INTEGER, account_id TEXT NOT NULL, token_epoch INTEGER NOT NULL,
 payload_json TEXT, payload_sha256 TEXT, complete INTEGER NOT NULL CHECK(complete IN (0,1)), PRIMARY KEY(snapshot_id,book_key));
CREATE TABLE IF NOT EXISTS history_coverage (
 snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE, from_date TEXT NOT NULL, to_date TEXT NOT NULL,
 page_keys_json TEXT NOT NULL, terminal_empty_page_verified INTEGER NOT NULL CHECK(terminal_empty_page_verified IN (0,1)),
 complete INTEGER NOT NULL CHECK(complete IN (0,1)), received_mono_ns INTEGER NOT NULL,
 archive_only INTEGER NOT NULL CHECK(archive_only IN (0,1)), PRIMARY KEY(snapshot_id,from_date,to_date));
-- Supplemental sidecar metadata for legacy writer observation and exact
-- SessionContext reconstruction; never part of the existing trade journal.
CREATE TABLE IF NOT EXISTS source_observations (
 account_key TEXT PRIMARY KEY REFERENCES account_state(account_key), owner_signature TEXT NOT NULL,
 mutation_signature TEXT NOT NULL, journal_digest TEXT NOT NULL, session_signature TEXT NOT NULL,
 last_utc_ns INTEGER NOT NULL, last_mono_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot_session_observed (
 snapshot_id TEXT PRIMARY KEY REFERENCES snapshots(snapshot_id) ON DELETE CASCADE, observed_utc_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot_profile_expiry (
 snapshot_id TEXT PRIMARY KEY REFERENCES snapshots(snapshot_id) ON DELETE CASCADE, valid_until_mono_ns INTEGER);
CREATE INDEX IF NOT EXISTS snapshots_recent ON snapshots(account_key,capture_finished_utc_ns DESC);
'''
_TABLES={'account_state','snapshots','snapshot_books','history_coverage','source_observations','snapshot_session_observed','snapshot_profile_expiry'}
_GENERATION_FIELDS=('boot_id','token_epoch','mutation_epoch','owner_epoch','journal_epoch','session_epoch')
_EPOCH_PREFIX={'TOKEN':'token_epoch','MUTATION':'mutation_epoch','OWNER':'owner_epoch',
               'JOURNAL':'journal_epoch','SESSION':'session_epoch'}


def _private(path, *, directory=False):
    s=path.lstat()
    expected=0o700 if directory else 0o600
    if ((not stat.S_ISDIR(s.st_mode) if directory else not stat.S_ISREG(s.st_mode))
            or s.st_uid!=os.geteuid() or stat.S_IMODE(s.st_mode)!=expected):
        raise EvidenceUnavailable('INSECURE_SNAPSHOT_PATH')


def _private_dir(path):
    try: path.mkdir(mode=0o700)
    except FileExistsError: pass
    _private(path,directory=True)


@contextmanager
def _private_lock(path):
    fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        _private(path)
        s=os.fstat(fd);p=path.lstat()
        if (s.st_dev,s.st_ino)!=(p.st_dev,p.st_ino):
            raise EvidenceUnavailable('LOCK_IDENTITY_CHANGED')
        yield fd
    finally:
        os.close(fd)  # flock released on every path including process death


def _connect_existing(directory):
    _private(directory,directory=True)
    path=directory/'snapshots.sqlite3'
    _private(path)
    fault=directory/'authority_fault'
    if fault.exists() or fault.is_symlink():
        raise EvidenceUnavailable('EVIDENCE_INVALIDATION_FAULT')
    for suffix in ('-wal','-shm','-journal'):
        p=Path(str(path)+suffix)
        if p.exists() or p.is_symlink(): _private(p)
    # mode=rw refuses accidental creation if a store is removed between
    # verification and open. No journal path is used here.
    conn=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=.15)
    try:
        conn.row_factory=sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        return conn
    except BaseException:
        conn.close();raise


def _mark_authority_fault(directory):
    # Presence alone is failure evidence, including a crash before fsync. A
    # durable invalidation fault is never automatically cleared by a new poll.
    _private(directory,directory=True)
    path=directory/'authority_fault'
    try:
        fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    except FileExistsError:
        return
    try:
        os.write(fd,b'EVIDENCE_INVALIDATION_FAILED\n')
        os.fsync(fd)
    finally:
        os.close(fd)
    df=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        os.fsync(df)
    finally:
        os.close(df)


def _invalidate_initialized_directory(directory, key, reason_code):
    epoch=_EPOCH_PREFIX.get(reason_code.split('_',1)[0])
    if epoch is None:
        raise ValueError('Unclassified invalidation')
    path=directory/'snapshots.sqlite3'
    if not path.exists() and not path.is_symlink():
        # No authority has been published in an uninitialized store.
        return False
    try:
        with closing(_connect_existing(directory)) as conn,conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute('SELECT account_key FROM account_state WHERE account_key=?',(key,)).fetchone()
            if row is None:
                if conn.execute('SELECT 1 FROM snapshots LIMIT 1').fetchone() is not None:
                    raise EvidenceUnavailable('GENERATION_STATE_LOST')
                return False
            conn.execute('UPDATE account_state SET '+epoch+'='+epoch+'+1,invalidation_reason=?,updated_utc_ns=? WHERE account_key=?',
                         (reason_code,time.time_ns(),key))
            conn.execute("UPDATE snapshots SET state='INVALID' WHERE account_key=? AND state='COMPLETE'",(key,))
        return True
    except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError):
        # No automatic reset. Source signatures still independently observe
        # token/file/journal changes if the metadata transaction fails.
        _mark_authority_fault(directory)
        raise


def invalidate_existing(data_dir, account_id, reason_code):
    """Advance an initialized account sidecar without acquiring collect.lock."""
    if type(account_id) is not str or not account_id or account_id!=account_id.strip():
        return False
    key=account_key(account_id)
    directory=Path(os.path.abspath(data_dir))/'broker_evidence'/key
    if any(p.is_symlink() for p in (directory,*directory.parents)):
        raise EvidenceUnavailable('SYMLINKED_DATA_PATH')
    return _invalidate_initialized_directory(directory,key,reason_code)


def invalidate_owner_scope(data_dir, reason_code):
    """Owner controls apply to the whole service data directory, not a symbol."""
    root=Path(os.path.abspath(data_dir))/'broker_evidence'
    if not root.exists() and not root.is_symlink(): return False
    if any(p.is_symlink() for p in (root,*root.parents)):
        raise EvidenceUnavailable('SYMLINKED_DATA_PATH')
    _private(root,directory=True)
    changed=False
    failed=False
    for directory in root.iterdir():
        key=directory.name
        if len(key)!=24 or any(c not in '0123456789abcdef' for c in key):
            continue
        try:
            changed=_invalidate_initialized_directory(directory,key,reason_code) or changed
        except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError):
            failed=True
    if failed: raise EvidenceUnavailable('OWNER_INVALIDATION_FAILED')
    return changed


class CrossProcessReadCoordinator:
    def __init__(self, data_dir, account_id, *, observe: Callable[[],EnvironmentObservation],
                 collect: Callable, clock_utc_ns=time.time_ns, clock_mono_ns=time.monotonic_ns,
                 boot_id=None, on_coherent_snapshot=None):
        self.account_id=account_id
        self.account_key=account_key(account_id)
        self._observe=observe
        self._collect=collect
        self._utc=clock_utc_ns
        self._mono=clock_mono_ns
        self.boot_id=boot_id or Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if not self.boot_id:
            raise EvidenceUnavailable('BOOT_ID_UNAVAILABLE')
        # Refuse symlinks rather than resolving a redirected sidecar path.
        root=Path(os.path.abspath(data_dir))
        for component in (root,*root.parents):
            if component.is_symlink():
                raise EvidenceUnavailable('SYMLINKED_DATA_PATH')
        root.mkdir(parents=True,exist_ok=True,mode=0o700)
        si=root.lstat()
        if not stat.S_ISDIR(si.st_mode) or si.st_uid!=os.geteuid() or si.st_mode&0o022:
            raise EvidenceUnavailable('INSECURE_DATA_DIRECTORY')
        base=root/'broker_evidence';_private_dir(base)
        self.directory=base/self.account_key;_private_dir(self.directory)
        self.path=self.directory/'snapshots.sqlite3'
        self.lock_path=self.directory/'collect.lock'
        self._on_snapshot=on_coherent_snapshot
        with _private_lock(self.directory/'schema.lock') as fd:
            fcntl.flock(fd,fcntl.LOCK_EX)
            try:
                new=not self.path.exists()
                if new:
                    out=os.open(self.path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
                    os.close(out)
                with closing(self._connect()) as conn:
                    existing={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    if not new and not _TABLES<=existing:
                        raise EvidenceUnavailable('SNAPSHOT_SCHEMA_UNTRUSTED')
                    if new:
                        conn.executescript(_SCHEMA)
                    if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                        raise EvidenceUnavailable('SNAPSHOT_DB_CORRUPT')
            finally:
                fcntl.flock(fd,fcntl.LOCK_UN)

    def _connect(self):
        return _connect_existing(self.directory)

    def _generation(self,row):
        return Generation(self.account_key,*(row[k] for k in _GENERATION_FIELDS))

    def _read_state(self,conn):
        return conn.execute('SELECT * FROM account_state WHERE account_key=?',(self.account_key,)).fetchone()

    def observe_generation(self):
        """Refresh nonsecret source generations; no network or journal write.

        Source observations are sampled outside the short sidecar transaction.
        The final acquisition compares generations again; mutation/token event
        hooks and final owner/journal fencing must also be connected in runtime.
        """
        observation_started=self._mono()
        obs=self._observe()
        if not isinstance(obs,EnvironmentObservation):
            raise EvidenceUnavailable('GENERATION_OBSERVATION_MISSING')
        token=obs.token_identity
        session_sig=canonical_json((obs.session.phase.value,
                    obs.session.trading_session_date.isoformat() if obs.session.trading_session_date else None,
                    obs.session.calendar_sha256,obs.session.calendar_verified))
        with closing(self._connect()) as conn,conn:
            conn.execute('BEGIN IMMEDIATE')
            # Sample transaction clocks after obtaining the serialized writer
            # lock; pre-lock sampling can look like a clock regression when
            # concurrent writers commit in the opposite order.
            utc,mono=self._utc(),self._mono()
            row=self._read_state(conn)
            old=conn.execute('SELECT * FROM source_observations WHERE account_key=?',(self.account_key,)).fetchone()
            # An observation which began before another process published a
            # newer one cannot overwrite it. Defer and recollect instead.
            if (old is not None and row is not None and row['boot_id']==self.boot_id
                    and observation_started < old['last_mono_ns'] <= mono):
                # Concurrent identical observations may coalesce. A different
                # older observation must not advance or roll back source state.
                if (tuple(row[k] for k in ('token_dev','token_inode','token_mtime_ns','token_size'))!=token
                        or (old['owner_signature'],old['mutation_signature'],old['journal_digest'],old['session_signature'])
                           != (obs.owner_signature,obs.mutation_signature,obs.journal_digest,session_sig)):
                    raise EvidenceUnavailable('OBSERVATION_SUPERSEDED')
            if row is None:
                if old is not None:
                    raise EvidenceUnavailable('GENERATION_STATE_LOST')
                conn.execute('INSERT INTO account_state VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                             (self.account_key,1,self.boot_id,1,*token,1,1,1,1,1,'INITIAL_OBSERVATION',utc))
            else:
                if old is None:
                    raise EvidenceUnavailable('SOURCE_OBSERVATION_LOST')
                changes=[]
                if row['boot_id']!=self.boot_id:
                    changes.extend(_EPOCH_PREFIX.values())
                elif (mono<old['last_mono_ns'] or utc<old['last_utc_ns'] or
                      abs((utc-old['last_utc_ns'])-(mono-old['last_mono_ns']))>5*NS):
                    changes.extend(_EPOCH_PREFIX.values())
                if tuple(row[k] for k in ('token_dev','token_inode','token_mtime_ns','token_size'))!=token:
                    changes.append('token_epoch')
                for field,epoch,value in (('owner_signature','owner_epoch',obs.owner_signature),
                        ('mutation_signature','mutation_epoch',obs.mutation_signature),
                        ('journal_digest','journal_epoch',obs.journal_digest),
                        ('session_signature','session_epoch',session_sig)):
                    if old[field]!=value: changes.append(epoch)
                changes=sorted(set(changes))
                if changes:
                    sql=', '.join(k+'='+k+'+1' for k in changes)
                    conn.execute('UPDATE account_state SET '+sql+',boot_id=?,token_dev=?,token_inode=?,token_mtime_ns=?,token_size=?,invalidation_reason=?,updated_utc_ns=? WHERE account_key=?',
                                 (self.boot_id,*token,'SOURCE_CHANGED:'+','.join(changes),utc,self.account_key))
                    conn.execute("UPDATE snapshots SET state='INVALID' WHERE account_key=? AND state='COMPLETE'",(self.account_key,))
            conn.execute('INSERT OR REPLACE INTO source_observations VALUES (?,?,?,?,?,?,?)',
                         (self.account_key,obs.owner_signature,obs.mutation_signature,obs.journal_digest,session_sig,utc,mono))
            return self._generation(self._read_state(conn)),obs

    def invalidate(self,account_key,reason_code,event_ref):
        """Advance a single epoch without acquiring collect.lock or journal locks."""
        if account_key!=self.account_key or type(event_ref) is not str or not event_ref:
            raise EvidenceUnavailable('INVALID_INVALIDATION_IDENTITY')
        prefix=reason_code.split('_',1)[0]
        epoch=_EPOCH_PREFIX.get(prefix)
        if epoch is None:
            raise ValueError('Unclassified invalidation')
        # Do not call environment observers under an operation/mutation fence.
        with closing(self._connect()) as conn,conn:
            conn.execute('BEGIN IMMEDIATE')
            row=self._read_state(conn)
            if row is None:
                raise EvidenceUnavailable('GENERATION_NOT_INITIALIZED')
            conn.execute('UPDATE account_state SET '+epoch+'='+epoch+'+1,invalidation_reason=?,updated_utc_ns=? WHERE account_key=?',
                         (prefix+'_EVENT',self._utc(),self.account_key))
            conn.execute("UPDATE snapshots SET state='INVALID' WHERE account_key=? AND state='COMPLETE'",(self.account_key,))
            return self._generation(self._read_state(conn))

    def _reserve(self,generation):
        with closing(self._connect()) as conn,conn:
            conn.execute('BEGIN IMMEDIATE')
            row=self._read_state(conn)
            if row is None or self._generation(row)!=generation:
                raise EvidenceUnavailable('GENERATION_CHANGED')
            seq=row['next_snapshot_seq']
            conn.execute('UPDATE account_state SET next_snapshot_seq=next_snapshot_seq+1 WHERE account_key=?',(self.account_key,))
            return self.account_key+':'+str(seq)

    def _decode(self,conn,row):
        def boolean(v):
            if type(v) is not int or v not in (0,1): raise EvidenceUnavailable('INVALID_STORED_BOOLEAN')
            return bool(v)
        sid=row['snapshot_id'];books={}
        for b in conn.execute('SELECT * FROM snapshot_books WHERE snapshot_id=?',(sid,)):
            book=BookEvidence(b['book_key'],b['endpoint'],json.loads(b['request_params_json']),b['status'],b['http_status'],
                b['broker_error_code'],b['retry_after_utc_ns'],boolean(b['transport_sent']),b['request_started_utc_ns'],b['received_utc_ns'],
                b['request_started_mono_ns'],b['received_mono_ns'],b['account_id'],b['token_epoch'],
                json.loads(b['payload_json']) if b['payload_json'] is not None else None,b['payload_sha256'],boolean(b['complete']))
            books[book.book_key]=book
        histories=tuple(b for k,b in books.items() if k.startswith('history:'))
        statuses={b.request_params['order_id']:b for k,b in books.items() if k.startswith('order_status:')}
        trades={b.request_params['order_id']:b for k,b in books.items() if k.startswith('trades_for_order:')}
        coverage=tuple(HistoryCoverage(date.fromisoformat(r['from_date']),date.fromisoformat(r['to_date']),tuple(json.loads(r['page_keys_json'])),
                    boolean(r['terminal_empty_page_verified']),boolean(r['complete']),r['received_mono_ns'],boolean(r['archive_only']))
                    for r in conn.execute('SELECT * FROM history_coverage WHERE snapshot_id=?',(sid,)))
        observed=conn.execute('SELECT observed_utc_ns FROM snapshot_session_observed WHERE snapshot_id=?',(sid,)).fetchone()
        if observed is None: raise EvidenceUnavailable('SESSION_OBSERVATION_MISSING')
        expiry=conn.execute('SELECT valid_until_mono_ns FROM snapshot_profile_expiry WHERE snapshot_id=?',(sid,)).fetchone()
        if expiry is None: raise EvidenceUnavailable('PROFILE_EXPIRY_MISSING')
        session=SessionContext(row['session_phase'],date.fromisoformat(row['session_date']) if row['session_date'] else None,
                              observed[0],row['calendar_sha256'],boolean(row['calendar_verified']))
        return BrokerSnapshot(snapshot_id=sid,generation=self._generation(row),account_id=self.account_id,
                   profile_account_id=row['profile_account_id'],profile_verified=boolean(row['profile_verified']),session=session,
                   capture_started_utc_ns=row['capture_started_utc_ns'],capture_finished_utc_ns=row['capture_finished_utc_ns'],
                   capture_started_mono_ns=row['capture_started_mono_ns'],capture_finished_mono_ns=row['capture_finished_mono_ns'],
                   **{k:books[k] for k in LIVE_BOOKS|{'profile'}},historical_trades=histories,
                   order_status=statuses,trades_for_order=trades,funds=books.get('funds'),edis=books.get('edis'),history_coverage=coverage,
                   complete=row['state']=='COMPLETE',invalid_reasons=tuple(json.loads(row['invalid_reasons_json'])),
                   producer_pid=row['producer_pid'],producer_starttime=row['producer_starttime'],journal_digest=row['journal_digest'],
                   profile_valid_until_mono_ns=expiry[0])

    def _prior_end(self,conn,req,generation):
        if not req.independent_after_snapshot_id: return None
        row=conn.execute('SELECT account_key,boot_id,capture_finished_mono_ns FROM snapshots WHERE snapshot_id=?',
                         (req.independent_after_snapshot_id,)).fetchone()
        if row is None or row['account_key']!=self.account_key or row['boot_id']!=generation.boot_id:
            raise EvidenceUnavailable('INDEPENDENT_PREDECESSOR_UNAVAILABLE')
        return row['capture_finished_mono_ns']+(2*NS-1 if req.purpose is Purpose.CLEANUP else 0)

    def _cached(self,req,generation,obs):
        with closing(self._connect()) as conn:
            # One read transaction for metadata, books and coverage, including
            # if another process evicts old diagnostic rows.
            conn.execute('BEGIN')
            prior=self._prior_end(conn,req,generation)
            if req.purpose is Purpose.SHUTDOWN and not req.min_capture_mono_ns:
                raise EvidenceUnavailable('POST_INTENT_FENCE_REQUIRED')
            if not obs.mutation_clear:
                return None,prior
            rows=conn.execute("SELECT * FROM snapshots WHERE account_key=? AND state='COMPLETE' ORDER BY capture_finished_mono_ns DESC LIMIT 128",(self.account_key,)).fetchall()
            for row in rows:
                if row['purpose']=='UI' or self._generation(row)!=generation or row['journal_digest']!=obs.journal_digest:
                    continue
                if row['expires_mono_ns']<self._mono(): continue
                s=self._decode(conn,row)
                if not s.validation_reasons(req,generation,self._mono(),independent_after_mono_ns=prior):
                    return s,prior
            return None,prior

    def _publish(self,s,req,generation,obs):
        if s.generation!=generation or s.journal_digest!=obs.journal_digest:
            raise EvidenceUnavailable('CAPTURE_GENERATION_CHANGED')
        sess=s.session;sid=s.snapshot_id
        # Required authorization freshness is checked again in the transaction.
        with closing(self._connect()) as conn,conn:
            conn.execute('BEGIN IMMEDIATE')
            row=self._read_state(conn)
            if row is None or self._generation(row)!=generation:
                raise EvidenceUnavailable('GENERATION_CHANGED_BEFORE_PUBLICATION')
            prior=self._prior_end(conn,req,generation)
            reasons=s.validation_reasons(req,generation,self._mono(),independent_after_mono_ns=prior)
            if not obs.mutation_clear:
                reasons+=('MUTATION_IN_FLIGHT_OR_UNVERIFIED',)
            if reasons:
                s=replace(s,complete=False,invalid_reasons=tuple(dict.fromkeys(s.invalid_reasons+reasons)))
            # A newer failed required read must not leave older authority usable.
            if not s.complete:
                conn.execute("UPDATE snapshots SET state='INVALID' WHERE account_key=? AND state='COMPLETE'",
                             (self.account_key,))
            conn.execute('INSERT INTO snapshots VALUES ('+','.join('?' for _ in range(26))+')',
                (sid,self.account_key,*[getattr(generation,k) for k in _GENERATION_FIELDS],req.purpose.value,
                 canonical_json(sorted(req.required_books)),s.capture_started_utc_ns,s.capture_finished_utc_ns,
                 s.capture_started_mono_ns,s.capture_finished_mono_ns,sess.trading_session_date.isoformat() if sess.trading_session_date else None,
                 sess.phase.value,sess.calendar_sha256,int(sess.calendar_verified),s.profile_account_id,int(s.profile_verified),
                 'COMPLETE' if s.complete else 'INCOMPLETE',canonical_json(s.invalid_reasons),s.capture_finished_mono_ns+10*NS,
                 s.producer_pid,s.producer_starttime,s.journal_digest))
            conn.execute('INSERT INTO snapshot_session_observed VALUES (?,?)',(sid,sess.observed_utc_ns))
            conn.execute('INSERT INTO snapshot_profile_expiry VALUES (?,?)',(sid,s.profile_valid_until_mono_ns))
            for b in s.all_books():
                conn.execute('INSERT INTO snapshot_books VALUES ('+','.join('?' for _ in range(18))+')',
                    (sid,b.book_key,b.endpoint,canonical_json(b.request_params),b.status.value,b.http_status,b.broker_error_code,b.retry_after_utc_ns,
                     int(b.transport_sent),b.request_started_utc_ns,b.received_utc_ns,b.request_started_mono_ns,b.received_mono_ns,b.account_id,b.token_epoch,
                     canonical_json(b.payload) if b.payload is not None else None,b.payload_sha256,int(b.complete)))
            for h in s.history_coverage:
                conn.execute('INSERT INTO history_coverage VALUES (?,?,?,?,?,?,?,?)',
                    (sid,h.from_date.isoformat(),h.to_date.isoformat(),canonical_json(h.page_keys),int(h.terminal_empty_page_verified),
                     int(h.complete),h.received_mono_ns,int(h.archive_only)))
            # This is exclusively disposable broker-read evidence. Durable journal
            # rows and governor admissions are not in this database.
            conn.execute('DELETE FROM snapshots WHERE account_key=? AND capture_finished_utc_ns<?',(self.account_key,self._utc()-86400*NS))
            conn.execute('DELETE FROM snapshots WHERE account_key=? AND snapshot_id NOT IN (SELECT snapshot_id FROM snapshots WHERE account_key=? ORDER BY capture_finished_utc_ns DESC,rowid DESC LIMIT 128)',(self.account_key,self.account_key))
        return s

    def acquire(self,request):
        """One bounded collection flight, with compatible cache reuse.

        No network is performed while a SQLite transaction is held. Unknown
        storage or environment generations are typed deferrals, never empties.
        """
        if not isinstance(request,EvidenceRequest) or request.account_id!=self.account_id:
            raise ValueError('Snapshot request account mismatch')
        generation=None;sent=False
        try:
            generation,obs=self.observe_generation()
            if request.purpose is Purpose.UI:
                return self.peek_display(self.account_key) or ReadDeferredResult(generation,BookStatus.UNAVAILABLE,None,'DISPLAY_UNAVAILABLE',False)
            if request.deadline_mono_ns<=self._mono():
                return ReadDeferredResult(generation,BookStatus.UNAVAILABLE,None,'ACQUISITION_DEADLINE',False)
            with _private_lock(self.lock_path) as fd:
                while True:
                    try:
                        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if self._mono()>=request.deadline_mono_ns:
                            return ReadDeferredResult(generation,BookStatus.LOCAL_PACING,None,'SINGLE_FLIGHT_DEADLINE',False)
                        time.sleep(.01)
                generation,obs=self.observe_generation()
                cached,prior=self._cached(request,generation,obs)
                if cached is not None:
                    latest,final=self.observe_generation()
                    if (latest==generation and final.journal_digest==obs.journal_digest
                            and final.mutation_clear
                            and not cached.validation_reasons(request,generation,self._mono(),independent_after_mono_ns=prior)):
                        return cached
                    raise EvidenceUnavailable('GENERATION_CHANGED_BEFORE_REUSE')
                if prior is not None and self._mono()<=prior:
                    return ReadDeferredResult(generation,BookStatus.LOCAL_PACING,prior+1,'INDEPENDENT_CAPTURE_TOO_EARLY',False)
                if self._mono()<request.min_capture_mono_ns or self._mono()>=request.deadline_mono_ns:
                    return ReadDeferredResult(generation,BookStatus.LOCAL_PACING,None,'ACQUISITION_OUTSIDE_FENCE',False)
                sid=self._reserve(generation)
                # Once entered, a transport may have been sent, even on error.
                sent=True
                s=self._collect(request,generation,sid,obs)
                if isinstance(s,ReadDeferredResult):
                    return s
                if not isinstance(s,BrokerSnapshot) or s.snapshot_id!=sid:
                    raise EvidenceUnavailable('INVALID_COLLECTOR_RESULT')
                # A rejection or true broker cooldown invalidates every cached
                # complete view. Incomplete diagnostics must not leave an older
                # view eligible after a known auth or availability failure.
                if any(b.status is BookStatus.AUTH_FAILURE for b in s.all_books()):
                    self.invalidate(self.account_key,'TOKEN_AUTH_FAILURE',sid)
                    return replace(s,complete=False,invalid_reasons=tuple(dict.fromkeys(
                        s.invalid_reasons+('TOKEN_AUTH_FAILURE',))))
                if any(b.status is BookStatus.COOLDOWN and b.transport_sent for b in s.all_books()):
                    with closing(self._connect()) as conn,conn:
                        conn.execute('BEGIN IMMEDIATE')
                        conn.execute("UPDATE snapshots SET state='INVALID' WHERE account_key=? AND state='COMPLETE'",(self.account_key,))
                final,finalobs=self.observe_generation()
                if final!=generation or finalobs.journal_digest!=obs.journal_digest:
                    raise EvidenceUnavailable('GENERATION_CHANGED_DURING_CAPTURE')
                published=self._publish(s,request,generation,finalobs)
                latest,last_obs=self.observe_generation()
                if latest!=generation or last_obs.journal_digest!=obs.journal_digest:
                    raise EvidenceUnavailable('GENERATION_CHANGED_AFTER_PUBLICATION')
                if published.complete and (not last_obs.mutation_clear or published.validation_reasons(
                        request,generation,self._mono(),independent_after_mono_ns=prior)):
                    raise EvidenceUnavailable('EVIDENCE_EXPIRED_AFTER_PUBLICATION')
                if self._on_snapshot is not None and published.complete:
                    # Callback must independently validate full coherent live
                    # snapshot and only count a new publication, never cache hits.
                    self._on_snapshot(published,generation)
                    if self.revalidate(published, request):
                        raise EvidenceUnavailable('EVIDENCE_CHANGED_AFTER_RECOVERY_CALLBACK')
                return published
        except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError,KeyError,TypeError) as exc:
            # Do not expose source payloads, credentials or local paths in reasons.
            return ReadDeferredResult(generation,BookStatus.UNAVAILABLE,None,
                                      'EVIDENCE_UNAVAILABLE:'+type(exc).__name__,sent)

    def revalidate(self, snapshot, request):
        """Network-free proof check for the future consumer authorization fence.

        This does not authorize a mutation or systemctl. Callers must hold the
        existing operation/mutation fence and check policy/readback separately.
        Never takes collect.lock; a failed check must block the caller.
        """
        if (not isinstance(snapshot, BrokerSnapshot) or not isinstance(request, EvidenceRequest)
                or snapshot.account_id != self.account_id or request.account_id != self.account_id):
            return ('ACCOUNT_OR_SNAPSHOT_MISMATCH',)
        try:
            generation, obs = self.observe_generation()
            if not obs.mutation_clear or snapshot.journal_digest != obs.journal_digest:
                return ('MUTATION_OR_JOURNAL_FENCE_FAILED',)
            with closing(self._connect()) as conn:
                conn.execute('BEGIN')
                state = self._read_state(conn)
                if state is None or self._generation(state) != generation:
                    return ('GENERATION_CHANGED',)
                row = conn.execute("SELECT * FROM snapshots WHERE snapshot_id=? AND account_key=? AND state='COMPLETE'",
                                   (snapshot.snapshot_id, self.account_key)).fetchone()
                if row is None or self._decode(conn, row) != snapshot:
                    return ('PUBLISHED_EVIDENCE_MISMATCH',)
                if request.purpose is Purpose.SHUTDOWN and not request.min_capture_mono_ns:
                    return ('POST_INTENT_FENCE_REQUIRED',)
                prior = self._prior_end(conn, request, generation)
                reasons = snapshot.validation_reasons(request, generation, self._mono(),
                                                       independent_after_mono_ns=prior)
            if reasons:
                return reasons
            latest, final = self.observe_generation()
            if latest != generation or final.journal_digest != obs.journal_digest or not final.mutation_clear:
                return ('GENERATION_CHANGED_AT_FENCE',)
            return snapshot.validation_reasons(request, latest, self._mono(), independent_after_mono_ns=prior)
        except (EvidenceUnavailable, OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            return ('AUTHORITY_REVALIDATION_UNAVAILABLE',)

    def peek_display(self,account_key):
        if account_key!=self.account_key: return None
        try:
            generation,obs=self.observe_generation()
            with closing(self._connect()) as conn:
                conn.execute('BEGIN')
                rows=conn.execute("SELECT * FROM snapshots WHERE account_key=? AND state='COMPLETE' ORDER BY capture_finished_mono_ns DESC LIMIT 128",(account_key,)).fetchall()
                for row in rows:
                    if self._generation(row)!=generation or row['journal_digest']!=obs.journal_digest: continue
                    if not 0<=self._mono()-row['capture_finished_mono_ns']<=15*NS: continue
                    s=self._decode(conn,row)
                    now=self._mono()
                    if s.profile_valid_until_mono_ns is None or now>=s.profile_valid_until_mono_ns:
                        continue
                    live_receipts=[b.received_mono_ns for b in s.all_books()
                                   if b.book_key in LIVE_BOOKS and b.complete]
                    if not live_receipts or not 0<=now-min(live_receipts)<=15*NS:
                        continue
                    # Mark display-only intrinsically; this object can never
                    # be accidentally used as a complete authorization snapshot.
                    return replace(s,complete=False,invalid_reasons=('DISPLAY_ONLY',))
        except (EvidenceUnavailable,OSError,sqlite3.Error,ValueError,KeyError,TypeError):
            pass
        return None
