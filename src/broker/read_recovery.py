"""Account/category sustained broker-throttle recovery, inside governor transactions.

The legacy category cooldown row is mirrored atomically. A successful transport
is counted only once, against a specific admitted GET receipt. A failed probe
cannot open the half-open gate. No broker or journal I/O occurs here.
"""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass

NS = 1_000_000_000
QUIET_NS = 600 * NS
PROBE_LIMIT_NS = 60 * NS


@dataclass(frozen=True)
class RecoveryAdmission:
    receipt_id: str
    category: str


class RecoveryDeferred(RuntimeError):
    def __init__(self, seconds, reason, *, short_window=False):
        self.seconds = seconds
        self.reason = reason
        self.short_window = short_window
        super().__init__(reason)


_COLUMNS = (
    'category', 'state', 'strikes', 'last_actual_throttle_utc_ns', 'cooldown_until_utc_ns',
    'quiet_since_utc_ns', 'successful_get_count', 'coherent_snapshot_count', 'last_decay_utc_ns',
    'probe_lease_id', 'probe_lease_deadline_utc_ns', 'boot_id', 'cooldown_until_mono_ns',
)


def initialize(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS category_recovery (
        category TEXT PRIMARY KEY, state TEXT NOT NULL CHECK(state IN ('NORMAL','COOLDOWN','HALF_OPEN','RECOVERY')),
        strikes INTEGER NOT NULL CHECK(strikes BETWEEN 0 AND 7), last_actual_throttle_utc_ns INTEGER,
        cooldown_until_utc_ns INTEGER, quiet_since_utc_ns INTEGER,
        successful_get_count INTEGER NOT NULL CHECK(successful_get_count >= 0),
        coherent_snapshot_count INTEGER NOT NULL CHECK(coherent_snapshot_count >= 0), last_decay_utc_ns INTEGER,
        probe_lease_id TEXT, probe_lease_deadline_utc_ns INTEGER, boot_id TEXT NOT NULL,
        cooldown_until_mono_ns INTEGER)''')
    # Supplement the locked state record with clock and idempotency evidence;
    # these tables are governor metadata, NOT changes to the trade journal.
    conn.execute('''CREATE TABLE IF NOT EXISTS recovery_clock (
        category TEXT PRIMARY KEY, last_utc_ns INTEGER NOT NULL, last_mono_ns INTEGER NOT NULL,
        quiet_since_mono_ns INTEGER NOT NULL, probe_deadline_mono_ns INTEGER,
        last_admission_mono_ns INTEGER)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS recovery_get_receipts (
        receipt_id TEXT PRIMARY KEY, category TEXT NOT NULL, throttle_ns INTEGER NOT NULL,
        admitted_mono_ns INTEGER NOT NULL, deadline_mono_ns INTEGER NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS recovery_snapshot_receipts (
        category TEXT NOT NULL, snapshot_id TEXT NOT NULL, seen_utc_ns INTEGER NOT NULL,
        PRIMARY KEY(category, snapshot_id))''')


def _save(conn, state, clock):
    conn.execute('INSERT OR REPLACE INTO category_recovery VALUES ('+','.join('?' for _ in _COLUMNS)+')',
                 tuple(state[k] for k in _COLUMNS))
    conn.execute('INSERT OR REPLACE INTO recovery_clock VALUES (?,?,?,?,?,?)',
                 (state['category'], clock['last_utc_ns'], clock['last_mono_ns'], clock['quiet_since_mono_ns'],
                  clock['probe_deadline_mono_ns'], clock['last_admission_mono_ns']))
    if state['strikes'] or state['cooldown_until_utc_ns'] is not None:
        conn.execute('INSERT OR REPLACE INTO category_cooldown VALUES (?,?,?)',
                     (state['category'], (state['cooldown_until_utc_ns'] or 0)/NS, state['strikes']))


def _state(conn, category, utc, mono, boot):
    row = conn.execute('SELECT '+','.join(_COLUMNS)+' FROM category_recovery WHERE category=?',(category,)).fetchone()
    legacy = conn.execute('SELECT until,strikes FROM category_cooldown WHERE category=?',(category,)).fetchone()
    if row is None:
        until, strikes = (int((legacy[0] or 0)*NS), int(legacy[1] or 0)) if legacy else (0,0)
        if not 0 <= strikes <= 7:
            raise sqlite3.DatabaseError('Invalid persisted throttle strikes')
        state = dict(zip(_COLUMNS, (category, 'COOLDOWN' if strikes or until > utc else 'NORMAL',
                    strikes, utc if strikes else None, until or None, utc if strikes else None,
                    0,0,None,None,None,boot, mono+max(0,until-utc) if until else None)))
        clock = dict(last_utc_ns=utc,last_mono_ns=mono,quiet_since_mono_ns=mono,
                     probe_deadline_mono_ns=None,last_admission_mono_ns=None)
        return state,clock
    state = dict(zip(_COLUMNS,row))
    cr = conn.execute('SELECT last_utc_ns,last_mono_ns,quiet_since_mono_ns,probe_deadline_mono_ns,last_admission_mono_ns FROM recovery_clock WHERE category=?',(category,)).fetchone()
    if cr is None or not legacy and state['strikes']:
        raise sqlite3.DatabaseError('Recovery metadata missing')
    if legacy and (legacy[1] != state['strikes'] or abs((legacy[0] or 0)*NS-(state['cooldown_until_utc_ns'] or 0)) > 4096):
        raise sqlite3.DatabaseError('Conflicting recovery and cooldown evidence')
    clock = dict(zip(('last_utc_ns','last_mono_ns','quiet_since_mono_ns','probe_deadline_mono_ns','last_admission_mono_ns'),cr))
    # A new boot or wall/monotonic divergence invalidates quiet-period proof.
    # Keep the greater known remaining cooldown, and restart a conservative
    # strike-sized window if the old boot's remaining duration is uncertain.
    boot_changed = state['boot_id'] != boot
    elapsed_wall = utc-clock['last_utc_ns']
    elapsed_mono = mono-clock['last_mono_ns']
    clock_anomaly = elapsed_wall < 0 or elapsed_mono < 0 or abs(elapsed_wall-elapsed_mono) > 5*NS
    if boot_changed or clock_anomaly:
        if state['strikes']:
            wall_remaining = max(0,(state['cooldown_until_utc_ns'] or 0)-utc)
            mono_remaining = max(0,(state['cooldown_until_mono_ns'] or 0)-mono) if not boot_changed else 0
            remaining=max(wall_remaining,mono_remaining,60*2**(state['strikes']-1)*NS)
            state.update(state='COOLDOWN', cooldown_until_utc_ns=utc+remaining,
                         cooldown_until_mono_ns=mono+remaining, quiet_since_utc_ns=utc,
                         successful_get_count=0,coherent_snapshot_count=0,
                         probe_lease_id=None,probe_lease_deadline_utc_ns=None,boot_id=boot)
            conn.execute('DELETE FROM recovery_get_receipts WHERE category=?',(category,))
            clock.update(quiet_since_mono_ns=mono,probe_deadline_mono_ns=None,last_admission_mono_ns=None)
        else:
            state['boot_id']=boot
    clock.update(last_utc_ns=utc,last_mono_ns=mono)
    return state,clock


def throttle(conn, category, delay, utc, mono, boot):
    state,clock=_state(conn,category,utc,mono,boot)
    active = max(0,(state['cooldown_until_utc_ns'] or 0)-utc,
                 (state['cooldown_until_mono_ns'] or 0)-mono) if state['state']=='COOLDOWN' else 0
    strikes = state['strikes'] if active and state['strikes'] else min(state['strikes']+1,7)
    remaining=max(active, int((delay or 0)*NS), 0 if active else 60*2**(strikes-1)*NS)
    state.update(state='COOLDOWN',strikes=strikes,last_actual_throttle_utc_ns=utc,
                 cooldown_until_utc_ns=utc+remaining,cooldown_until_mono_ns=mono+remaining,
                 quiet_since_utc_ns=utc,successful_get_count=0,coherent_snapshot_count=0,
                 probe_lease_id=None,probe_lease_deadline_utc_ns=None)
    clock.update(quiet_since_mono_ns=mono,probe_deadline_mono_ns=None,last_admission_mono_ns=None)
    conn.execute('DELETE FROM recovery_get_receipts WHERE category=?',(category,))
    _save(conn,state,clock)
    return remaining/NS


def check_admission(conn,category,utc,mono,boot):
    state,clock=_state(conn,category,utc,mono,boot)
    remaining=max(0,(state['cooldown_until_utc_ns'] or 0)-utc,(state['cooldown_until_mono_ns'] or 0)-mono)
    if state['state']=='COOLDOWN' and remaining>0:
        # Persist boot/clock recovery even when a caller is deferred. Caller
        # commits this metadata-only decision, without a network admission.
        _save(conn,state,clock)
        raise RecoveryDeferred(remaining/NS,'broker sustained cooldown')
    if category!='NON_TRADING' or not state['strikes']:
        return state,clock
    if state['state']=='HALF_OPEN':
        wait=max(0,(clock['probe_deadline_mono_ns'] or 0)-mono,
                 (state['probe_lease_deadline_utc_ns'] or 0)-utc)
        if wait:
            _save(conn,state,clock)
            raise RecoveryDeferred(wait/NS,'single recovery probe in flight')
    if state['state']=='RECOVERY':
        last=clock['last_admission_mono_ns']
        if last is not None and mono-last<NS//2:
            _save(conn,state,clock)
            raise RecoveryDeferred((NS//2-(mono-last))/NS,'recovery 2 GET/s pacing',short_window=True)
    return state,clock


def admit(conn, category, state, clock, utc, mono, timeout):
    if category!='NON_TRADING' or not state['strikes']:
        # Other categories retain their original budgets and cooldown. Do not
        # erase sustained strikes on an unrelated single success.
        if state['strikes'] and state['state']=='COOLDOWN':
            state['state']='RECOVERY'
        _save(conn,state,clock)
        return None
    receipt=uuid.uuid4().hex
    deadline=mono+min(PROBE_LIMIT_NS,max(NS,int(timeout*NS)))
    if state['state'] in ('COOLDOWN','HALF_OPEN'):
        state.update(state='HALF_OPEN',probe_lease_id=receipt,
                     probe_lease_deadline_utc_ns=utc+deadline-mono)
        clock['probe_deadline_mono_ns']=deadline
    clock['last_admission_mono_ns']=mono
    conn.execute('DELETE FROM recovery_get_receipts WHERE deadline_mono_ns<?',(mono,))
    conn.execute('INSERT INTO recovery_get_receipts VALUES (?,?,?,?,?)',
                 (receipt,category,state['last_actual_throttle_utc_ns'] or utc,mono,deadline))
    _save(conn,state,clock)
    return RecoveryAdmission(receipt,category)


def _maybe_decay(conn,state,clock,utc,mono):
    if (state['state']=='RECOVERY' and state['strikes'] and
            utc-(state['quiet_since_utc_ns'] or utc)>=QUIET_NS and
            mono-clock['quiet_since_mono_ns']>=QUIET_NS and
            state['successful_get_count']>=10 and state['coherent_snapshot_count']>=2):
        state['strikes']-=1
        state.update(state='RECOVERY' if state['strikes'] else 'NORMAL',
                     last_decay_utc_ns=utc,quiet_since_utc_ns=utc,successful_get_count=0,coherent_snapshot_count=0)
        clock['quiet_since_mono_ns']=mono
        # Outstanding pre-decay receipts cannot count twice in the next epoch.
        conn.execute('DELETE FROM recovery_get_receipts WHERE category=?',(state['category'],))


def success(conn,category,receipt,utc,mono,boot):
    if category!='NON_TRADING' or not isinstance(receipt,RecoveryAdmission) or receipt.category!=category:
        return False
    state,clock=_state(conn,category,utc,mono,boot)
    row=conn.execute('SELECT throttle_ns,deadline_mono_ns FROM recovery_get_receipts WHERE receipt_id=? AND category=?',
                     (receipt.receipt_id,category)).fetchone()
    if not row or row[0]!=(state['last_actual_throttle_utc_ns'] or utc) or row[1]<mono:
        _save(conn,state,clock)
        return False
    conn.execute('DELETE FROM recovery_get_receipts WHERE receipt_id=?',(receipt.receipt_id,))
    if state['state']=='HALF_OPEN' and state['probe_lease_id']==receipt.receipt_id:
        state.update(state='RECOVERY',probe_lease_id=None,probe_lease_deadline_utc_ns=None)
        clock['probe_deadline_mono_ns']=None
    elif state['state']!='RECOVERY':
        _save(conn,state,clock)
        return False
    state['successful_get_count']+=1
    _maybe_decay(conn,state,clock,utc,mono)
    _save(conn,state,clock)
    return True


def snapshot_success(conn,snapshot_id,utc,mono,boot,capture_utc,capture_mono):
    category='NON_TRADING'
    state,clock=_state(conn,category,utc,mono,boot)
    if (state['state']!='RECOVERY' or not state['strikes'] or
            capture_utc < (state['quiet_since_utc_ns'] or utc) or
            capture_mono < clock['quiet_since_mono_ns']):
        return False
    # A snapshot ID may only contribute once, including across strike decay.
    inserted=conn.execute('INSERT OR IGNORE INTO recovery_snapshot_receipts VALUES (?,?,?)',
                          (category,snapshot_id,utc)).rowcount
    if not inserted:
        return False
    state['coherent_snapshot_count']+=1
    _maybe_decay(conn,state,clock,utc,mono)
    _save(conn,state,clock)
    return True
