"""Pure append-only durable issue evidence (no writer and no authorization).

Use the journal's existing protected OWNER_LOT_* system-event namespace. The
Phase 2D orchestration boundary must persist one proposed event under the
existing final generation/journal fence, then read it back. Observing a fresh
book or dropping a legacy flag never removes a previously opened occurrence.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
import json
import math

from src.broker.broker_snapshot import ContractKey, EvidenceRef, Generation, account_key, freeze_json, payload_digest, thaw_json
from src.engine.unified_exit_reconciliation import _exchange_time, _integer
from src.engine.signed_delivery_live_evidence import _decimal_price
from decimal import Decimal

OPENED = 'OWNER_LOT_LEDGER_ISSUE_OPENED'
RESOLVED = 'OWNER_LOT_LEDGER_ISSUE_RESOLVED'
EVENT_TYPES = frozenset({OPENED, RESOLVED})
# Only an exact parent ledger readback can resolve these categories. Other
# durable risks stay open until their dedicated operation proof is available.
_LEDGER_RESOLVABLE = frozenset('LEGACY_UNCLASSIFIED_RISK:'+field for field in (
    'owner_readback_failed','persistence_uncertain','last_persistence_error','reconciliation_required',
    'external_unlinked_sell_observed','manual_exit_detected','manual_exit_evidence_reason',
    'unverified_trade_detected','unverified_trade_reason','entry_fill_evidence_required',
    'broker_exit_already_executed','uncommitted_broker_sell_trades'))
_OBSERVED_EXECUTION = frozenset('LEGACY_UNCLASSIFIED_RISK:'+field for field in (
    'uncommitted_broker_sell_trades','broker_exit_already_executed'))



def issue_identity(contract, reason, scope, subject):
    return payload_digest([contract.account_id,contract.security_id,contract.exchange_segment,
                           contract.product_type,reason,scope,subject])


def _text(value):
    if type(value) is not str or not value or value != value.strip():
        raise ValueError('Invalid durable issue text')
    return value


def _uint(value):
    if type(value) is not int or value < 0:
        raise ValueError('Invalid durable issue timestamp/ID')
    return value


def _digest(value):
    if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('Invalid durable issue digest')
    return value


def _refs(values):
    if not isinstance(values,(tuple,list)) or not values:
        raise ValueError('No linked proof evidence')
    refs=tuple(EvidenceRef(**dict(v)) for v in values)
    if len(refs)!=len(set(refs)):
        raise ValueError('Duplicate durable issue proof')
    return refs


def ref_payload(ref):
    return dict(snapshot_id=ref.snapshot_id,book_key=ref.book_key,row_index=ref.row_index,
                payload_sha256=ref.payload_sha256,journal_ref=ref.journal_ref)


@dataclass(frozen=True,slots=True)
class DurableOccurrence:
    event_id: int
    issue_id: str
    contract: ContractKey
    reason_code: str
    classification: str
    scope: str
    subject_id: str
    source: str
    first_seen_utc_ns: int
    opened_snapshot_id: str
    evidence_refs: tuple[EvidenceRef,...]
    opening_sha256: str
    observed: Mapping | None


@dataclass(frozen=True,slots=True)
class DurableIssueState:
    unresolved: tuple[DurableOccurrence,...]
    malformed: bool


def _contract(raw):
    if not isinstance(raw,Mapping) or set(raw)!={'account_id','security_id','exchange_segment','product_type'}:
        raise ValueError('Invalid durable issue contract')
    return ContractKey(**dict(raw))


def _contract_payload(c):
    return dict(account_id=c.account_id,security_id=c.security_id,exchange_segment=c.exchange_segment,product_type=c.product_type)


def opening_payload(issue, contract, *, snapshot_id, journal_digest, journal):
    """Create one explicit append proposal, never mutate the journal."""
    if issue.classification not in ('DURABLE_UNRESOLVED','CONTRADICTION'):
        raise ValueError('Transient observations must not be persisted as durable risks')
    if issue.issue_id!=issue_identity(contract,issue.reason_code,issue.scope,issue.subject_id):
        raise ValueError('Issue identity mismatch')
    refs=tuple(issue.evidence_refs)
    if not refs:
        # The immutable journal digest is a real readback reference, not a
        # fabricated broker row. It preserves the evidence basis of the issue.
        refs=(EvidenceRef(snapshot_id,'journal:view',None,_digest(journal_digest),'journal:view'),)
    observed=None
    if issue.reason_code.startswith('LEGACY_UNCLASSIFIED_RISK:') and issue.scope=='PARENT':
        rows=[r for r in journal.watcher_orders if r.get('order_id')==issue.subject_id]
        if len(rows)==1:
            record=json.loads(rows[0]['record_json']);field=issue.reason_code.split(':',1)[1]
            if field in record:
                observed={'field':field,'value':record[field], 'record_sha256':payload_digest(record),
                          'staged_sells':record.get('uncommitted_broker_sell_trades',[])}
    return freeze_json(dict(version=1,issue_id=issue.issue_id,contract=_contract_payload(contract),
        reason_code=issue.reason_code,classification=issue.classification,scope=issue.scope,subject_id=issue.subject_id,
        source=issue.source,first_seen_utc_ns=issue.first_seen_utc_ns,snapshot_id=snapshot_id,
        journal_digest=_digest(journal_digest),evidence_refs=[ref_payload(r) for r in refs],observed=observed))


def read_durable_issues(journal):
    """Replay immutable issue events; malformed evidence cannot clear an issue.

    A resolution is a previously committed explicit proof-linked decision, not
    a new fresh-verification result. The caller must still independently
    evaluate all current evidence/flags. A repeated opening creates a new
    occurrence after resolution; it cannot reuse an old resolution event.
    """
    active={}; malformed=False; previous_id=-1
    for row in journal.relevant_system_events:
        typ=row.get('event_type')
        if typ not in EVENT_TYPES: continue
        try:
            eid=_uint(row.get('id'))
            if eid<=previous_id: raise ValueError('Issue event ordering invalid')
            previous_id=eid
            raw=json.loads(row['payload'])
            if not isinstance(raw,dict) or type(raw.get('version')) is not int or raw['version']!=1:
                raise ValueError('Unknown issue event version')
            contract=_contract(raw['contract']); ident=_digest(raw['issue_id'])
            reason=_text(raw['reason_code']); scope=_text(raw['scope']); subject=_text(raw['subject_id'])
            if (contract.account_id!=journal.account_id or scope not in ('ACCOUNT','CONTRACT','PARENT','ORDER','EXECUTION')
                    or ident!=issue_identity(contract,reason,scope,subject) or row.get('description')!=ident):
                raise ValueError('Issue event identity mismatch')
            snapshot=_text(raw['snapshot_id']); journal_digest=_digest(raw['journal_digest']); refs=_refs(raw['evidence_refs'])
            if typ==OPENED:
                classification=raw['classification']; source=_text(raw['source']);first_seen=_uint(raw['first_seen_utc_ns'])
                if classification not in ('DURABLE_UNRESOLVED','CONTRADICTION') or ident in active:
                    raise ValueError('Duplicate or non-durable issue opening')
                observed=raw.get('observed')
                if observed is not None:
                    if (not isinstance(observed,dict) or set(observed)!={'field','value','record_sha256','staged_sells'}
                            or reason!='LEGACY_UNCLASSIFIED_RISK:'+_text(observed['field'])):
                        raise ValueError('Legacy risk provenance mismatch')
                    _digest(observed['record_sha256']); observed=freeze_json(observed)
                active[ident]=DurableOccurrence(eid,ident,contract,reason,classification,scope,subject,source,
                    first_seen,snapshot,refs,payload_digest(raw),observed)
            else:
                target=active.get(ident)
                if (target is None or _uint(raw['opening_event_id'])!=target.event_id
                        or _digest(raw['opening_sha256'])!=target.opening_sha256
                        or snapshot==target.opened_snapshot_id or _uint(raw['resolved_utc_ns'])<target.first_seen_utc_ns
                        or raw.get('proof_kind')!='EXACT_PARENT_LEDGER_READBACK'
                        or target.reason_code not in _LEDGER_RESOLVABLE or target.scope!='PARENT' or not any(r.journal_ref for r in refs)
                        or not any(r.book_key not in ('journal:view','journal:watcher_orders') for r in refs)
                        or any(r.snapshot_id!=snapshot for r in refs)):
                    raise ValueError('Issue resolution is not linked to its occurrence and fresh proof')
                _digest(raw['parent_readback_sha256'])
                _verify_execution_refs(journal,target,refs)
                if target.reason_code in _OBSERVED_EXECUTION:
                    _verify_observed_sells(journal,target,refs)
                # The resolution event must be appended by the fenced writer;
                # malformed/replayed/cross-parent evidence never clears it.
                del active[ident]
        except (ValueError,TypeError,KeyError,AttributeError,ArithmeticError):
            malformed=True
    return DurableIssueState(tuple(active.values()),malformed)


def parent_readback_digest(journal, parent):
    """Bind exact lifecycle, durable executions/allocations and watcher row."""
    records={}
    for name,key in (('positions_lifecycle','parent_order_id'),('order_intents','broker_order_id'),
                     ('fills','parent_order_id'),('exit_allocations','parent_order_id'),
                     ('protection_orders','parent_order_id'),('manual_risk_acknowledgements','parent_order_id'),
                     ('watcher_orders','order_id')):
        records[name]=[thaw_json(r) for r in getattr(journal,name) if r.get(key)==parent]
    if len(records['positions_lifecycle'])!=1 or len(records['order_intents'])!=1:
        raise ValueError('Exact parent readback unavailable')
    return payload_digest(records)


def _parent_fills(journal, occurrence):
    c=occurrence.contract; p=occurrence.subject_id
    keys={(a.get('sell_order_id'),a.get('exchange_trade_id')) for a in journal.exit_allocations if a.get('parent_order_id')==p}
    return {str(f['id']):f for f in journal.fills if f.get('is_verified_trade')==1
            and (f.get('security_id'),f.get('exchange_segment'),f.get('product_type'))==(c.security_id,c.exchange_segment,c.product_type)
            and (f.get('parent_order_id')==p or (f.get('broker_order_id'),f.get('exchange_trade_id')) in keys)}


def _verify_execution_refs(journal, occurrence, refs):
    fills=_parent_fills(journal,occurrence)
    referenced={r.journal_ref.split(':',1)[1] for r in refs if r.journal_ref and r.journal_ref.startswith('fills:')}
    if not referenced or not referenced<=set(fills): raise ValueError('Resolution references a missing or other-parent execution')
    for fid in referenced:
        f=fills[fid]; eid=f.get('exchange_trade_id')
        if eid is None:
            if f.get('transaction_type')!='BUY' or f.get('evidence_source')!='BROKER_HISTORICAL_NO_EXECUTION_ID':
                raise ValueError('Unknown execution identity in resolution')
        elif type(eid) is not str or not eid or eid.upper() in ('0','NONE','NULL') or any(m in eid.upper() for m in ('-FILL-','-TRD-','-RECON-')):
            raise ValueError('Synthetic execution in resolution')
    return [fills[fid] for fid in referenced]


def _verify_observed_sells(journal, occurrence, refs):
    """An observed execution must remain exact and parent-bound after readback."""
    rows=occurrence.observed.get('staged_sells') if occurrence.observed else None
    if not isinstance(rows,tuple) or not rows: raise ValueError('Specific observed SELL provenance missing')
    fills=_verify_execution_refs(journal,occurrence,refs); c=occurrence.contract; seen=set()
    for raw in rows:
        if not isinstance(raw,Mapping): raise ValueError('Malformed observed SELL')
        oid=_text(raw.get('orderId')); eid=_text(raw.get('exchangeTradeId'))
        if ((oid,eid) in seen or raw.get('dhanClientId')!=c.account_id or raw.get('transactionType')!='SELL'
                or (raw.get('securityId'),raw.get('exchangeSegment'),raw.get('productType'))!=(c.security_id,c.exchange_segment,c.product_type)):
            raise ValueError('Observed SELL identity mismatch')
        seen.add((oid,eid)); matches=[f for f in fills if f.get('broker_order_id')==oid and f.get('exchange_trade_id')==eid and f.get('transaction_type')=='SELL']
        if (len(matches)!=1 or matches[0].get('traded_quantity')!=_integer(raw.get('tradedQuantity'),positive=True)
                or abs(_decimal_price(matches[0].get('traded_price'))-_decimal_price(raw.get('tradedPrice')))>=Decimal('0.000001')
                or abs(matches[0]['traded_at']-_exchange_time(raw.get('exchangeTime')))>=1):
            raise ValueError('Observed SELL execution not durably reconciled')


def resolution_payload(occurrence, verdict, journal, *, resolved_utc_ns):
    """Propose a scoped resolution only from independently verified evidence.

    The supplied verdict must be the private *current-evidence-only* evaluation
    produced by the reconciler's resolution planner (never a permission to act).
    Mutation/cancellation/retry/orphan categories require their own exact
    terminal operation proofs in the effectful Phase 2D boundary; this helper
    cannot resolve them by a generic healthy ledger observation.
    """
    if occurrence.contract!=verdict.contract or occurrence.scope!='PARENT':
        raise ValueError('Resolution scope mismatch')
    if occurrence.reason_code not in _LEDGER_RESOLVABLE:
        raise ValueError('Issue needs operation-specific terminal proof')
    if (not journal.read_complete or verdict.journal_digest!=journal.digest_sha256 or not verdict.persistence_verified
            or not verdict.identity_verified or not verdict.snapshot_complete or not verdict.fresh or not verdict.coherent
            or not verdict.history_complete or not verdict.ledger_verified or verdict.issues or verdict.unresolved_mutation_ids
            or verdict.unattributed_execution_refs or verdict.orphan_leg_ids or verdict.snapshot_id==occurrence.opened_snapshot_id):
        raise ValueError('Fresh complete ledger proof unavailable')
    parents=[p for p in verdict.parents if p.parent_order_id==occurrence.subject_id]
    if len(parents)!=1: raise ValueError('Exact parent proof missing')
    parent=parents[0]
    if (parent.unresolved_risk_ids or parent.open_qty is None or parent.verified_buy_qty is None or parent.verified_sell_qty is None
            or parent.verified_protected_qty is None or parent.manual_ack_qty is None
            or parent.open_qty!=parent.verified_buy_qty-parent.verified_sell_qty
            or (parent.open_qty==0 and not parent.closure_verified)
            or (parent.open_qty>0 and parent.verified_protected_qty+parent.manual_ack_qty!=parent.open_qty)):
        raise ValueError('Parent not independently resolved')
    refs=tuple(dict.fromkeys(parent.evidence_refs))
    if not refs or not any(r.journal_ref for r in refs) or any(r.snapshot_id!=verdict.snapshot_id for r in refs):
        raise ValueError('Parent execution proof references unavailable')
    _verify_execution_refs(journal,occurrence,refs)
    if occurrence.reason_code in _OBSERVED_EXECUTION:
        _verify_observed_sells(journal,occurrence,refs)
    if _uint(resolved_utc_ns)<occurrence.first_seen_utc_ns: raise ValueError('Resolution predates issue')
    return freeze_json(dict(version=1,issue_id=occurrence.issue_id,contract=_contract_payload(occurrence.contract),
        reason_code=occurrence.reason_code,scope=occurrence.scope,subject_id=occurrence.subject_id,
        opening_event_id=occurrence.event_id,opening_sha256=occurrence.opening_sha256,
        snapshot_id=verdict.snapshot_id,journal_digest=journal.digest_sha256,resolved_utc_ns=resolved_utc_ns,
        proof_kind='EXACT_PARENT_LEDGER_READBACK',parent_readback_sha256=parent_readback_digest(journal,occurrence.subject_id),
        evidence_refs=[ref_payload(r) for r in refs]))
