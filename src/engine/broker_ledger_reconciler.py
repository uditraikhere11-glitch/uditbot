"""Pure, shadow-mode broker ledger verification.

Never performs transport, SQL, writes, notification or service operations.
No caller-provided exposure/coverage quantities are accepted as evidence. The
verdict is conditional on the exact supplied snapshot and immutable journal.
Phase 2D must wire the authorization fence; a verdict is not permission to act.
"""
from __future__ import annotations
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
from zoneinfo import ZoneInfo

from src.broker.broker_snapshot import (
    NS, LIVE_BOOKS, BrokerSnapshot, ContractKey, EvidenceRef, EvidenceRequest,
    Generation, Purpose, SessionContext, SessionPhase, account_key, payload_digest,
    thaw_json, freeze_json,
)
from src.broker.dhan_client import verify_forever_oco_group, verify_super_order_record
from src.engine.holding_quantity import observed_open_quantity, aggregate_holdings
from src.engine.journal_view import JournalView
from src.engine.signed_delivery_live_evidence import (
    ClosedHistoricalBuyMatcher, closed_order_rollups, _decimal_price,
    closed_triggered_binding, verify_closed_triggered_group, _holdings_fingerprint, HistoricalSellMatch,
)
from src.engine.signed_cnc_delivery import validate_signed_position_components
from src.engine.unified_exit_reconciliation import _exchange_time, _integer, validate_forever_oco_child_orders
from src.engine.watcher_risk_classification import legacy_risks
from src.engine.durable_ledger_issues import (read_durable_issues, issue_identity, opening_payload, resolution_payload,
                                             OPENED, RESOLVED)

IST = ZoneInfo('Asia/Kolkata')
_FINAL_ORDER = frozenset({'TRADED','FILLED','COMPLETE','CLOSED','CANCELLED','CANCELED','REJECTED','EXPIRED'})
_DEAD = frozenset({'CANCELLED','CANCELED','REJECTED','EXPIRED'})
_SAFE_ATTEMPTS = frozenset({'CONFIRMED','REJECTED','CANCELLED','CANCELED','EXPIRED','FAILED','POSITION_CLOSED'})


class VerdictStatus(str, Enum):
    VERIFIED = 'VERIFIED'
    TRANSIENT_UNAVAILABLE = 'TRANSIENT_UNAVAILABLE'
    DURABLE_UNRESOLVED = 'DURABLE_UNRESOLVED'
    CONTRADICTORY = 'CONTRADICTORY'


@dataclass(frozen=True, slots=True)
class PolicyContext:
    mode: str
    purpose: Purpose
    owner_desired_state: str
    owner_observed_state: str
    deferred_stop_pending: bool
    owner_intent_epoch: int
    entry_fence_active: bool
    emergency_stop_active: bool
    transition_active: bool
    mutation_fence_verified: bool
    active_mutation_ids: tuple[str, ...]
    unknown_mutation_ids: tuple[str, ...]
    service_identity_verified: bool
    watcher_liveness_verified: bool
    risk_funds_mode_checks_passed: bool
    journal_readback_verified: bool
    # Operation-specific minimum capture fence. Required for shutdown.
    min_capture_mono_ns: int = 0
    # An independently acquired holdings/profile snapshot, not caller-supplied
    # ownership or quantity. Optional because explicit account-labelled rows
    # do not require the legacy independent holdings attestation.
    holdings_confirmation: BrokerSnapshot | None = None

    def __post_init__(self):
        object.__setattr__(self, 'purpose', Purpose(self.purpose))
        if self.holdings_confirmation is not None and not isinstance(self.holdings_confirmation, BrokerSnapshot):
            raise ValueError("Invalid holdings confirmation")
        if self.mode not in ('LIVE','TEST') or self.owner_desired_state not in ('RUNNING','STOPPED') or self.owner_observed_state not in ('RUNNING','STOPPED','UNKNOWN'):
            raise ValueError('Unknown policy identity')
        for name in ('deferred_stop_pending','entry_fence_active','emergency_stop_active','transition_active',
                     'mutation_fence_verified','service_identity_verified','watcher_liveness_verified',
                     'risk_funds_mode_checks_passed','journal_readback_verified'):
            if type(getattr(self,name)) is not bool:
                raise ValueError('Unknown policy fact: '+name)
        for name in ('owner_intent_epoch','min_capture_mono_ns'):
            if type(getattr(self,name)) is not int or getattr(self,name) < 0:
                raise ValueError('Unknown policy fence')
        for name in ('active_mutation_ids','unknown_mutation_ids'):
            v=tuple(getattr(self,name))
            if any(type(x) is not str or not x for x in v):
                raise ValueError('Unknown mutation identity')
            object.__setattr__(self,name,v)


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    issue_id: str
    reason_code: str
    classification: str
    scope: str
    subject_id: str
    source: str
    first_seen_utc_ns: int
    evidence_refs: tuple[EvidenceRef, ...]
    resolution_requirements: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ParentVerdict:
    parent_order_id: str
    lifecycle_state: str
    verified_buy_qty: int | None
    verified_sell_qty: int | None
    open_qty: int | None
    verified_protected_qty: int | None
    manual_ack_qty: int | None
    unprotected_qty: int | None
    closure_verified: bool
    protection_route: str
    verified_child_order_ids: tuple[str, ...]
    child_residual_qty: int | None
    manual_ack_verified: bool
    unresolved_risk_ids: tuple[str, ...]
    evidence_refs: tuple[EvidenceRef, ...]


@dataclass(frozen=True, slots=True)
class ActionProposal:
    action_id: str
    kind: str
    parent_order_id: str | None
    precondition_generation: Generation
    precondition_journal_digest: str
    evidence_refs: tuple[EvidenceRef, ...]
    payload: Mapping

    def __post_init__(self):
        if self.kind not in {'COMMIT_VERIFIED_EXECUTION', 'COMMIT_ALLOCATION', 'UPDATE_LIFECYCLE',
                             'VERIFY_PROTECTION', 'NOTIFY', 'RECHECK'}:
            raise ValueError('Unknown action kind')
        object.__setattr__(self, 'evidence_refs', tuple(self.evidence_refs))
        object.__setattr__(self, 'payload', freeze_json(self.payload))


@dataclass(frozen=True, slots=True)
class HistoricalMatch:
    """Corroboration reference, never a new fill or allocation."""
    summary_ref: EvidenceRef
    contract: ContractKey
    broker_order_id: str
    side: str
    trade_date: date
    matched_execution_refs: tuple[EvidenceRef, ...]
    matched_exchange_trade_ids: tuple[str, ...]
    matched_parent_ids: tuple[str, ...]
    total_qty: int
    weighted_price: Decimal
    corroboration_only: bool = True


@dataclass(frozen=True, slots=True)
class AccountVerdict:
    snapshot_id: str
    generation: Generation
    journal_digest: str | None
    contracts: tuple[ReconciliationVerdict, ...]
    reason_codes: tuple[str, ...]
    entry_eligible: bool
    shutdown_eligible: bool


@dataclass(frozen=True, slots=True)
class ReconciliationVerdict:
    verdict_id: str
    snapshot_id: str
    generation: Generation
    contract: ContractKey
    journal_digest: str | None
    evaluated_mono_ns: int
    valid_until_mono_ns: int
    session: SessionContext
    status: VerdictStatus
    identity_verified: bool
    snapshot_complete: bool
    fresh: bool
    coherent: bool
    history_complete: bool
    ledger_verified: bool
    persistence_verified: bool
    broker_open_qty: int | None
    ledger_open_qty: int | None
    verified_protected_qty: int | None
    manual_ack_qty: int | None
    unprotected_qty: int | None
    parents: tuple[ParentVerdict, ...]
    issues: tuple[ReconciliationIssue, ...]
    unresolved_mutation_ids: tuple[str, ...]
    unattributed_execution_refs: tuple[EvidenceRef, ...]
    orphan_leg_ids: tuple[str, ...]
    evidence_refs: tuple[EvidenceRef, ...]
    entry_eligible: bool
    shutdown_eligible: bool
    watcher_actions: tuple[ActionProposal, ...] = ()
    additional_evidence: tuple[EvidenceRequest, ...] = ()
    historical_matches: tuple[HistoricalMatch, ...] = ()
    schema_version: int = 1

    @property
    def account_verified(self): return self.identity_verified
    @property
    def open_quantity_verified(self):
        return self.ledger_verified and self.broker_open_qty is not None and self.ledger_open_qty is not None and self.broker_open_qty == self.ledger_open_qty
    @property
    def unresolved_mutations(self): return self.unresolved_mutation_ids
    @property
    def unknown_executions(self): return self.unattributed_execution_refs
    @property
    def unprotected_quantity(self): return self.unprotected_qty
    @property
    def acknowledged_manual_quantity(self): return self.manual_ack_qty
    @property
    def closed_parent_quantity(self): return sum(p.verified_sell_qty or 0 for p in self.parents if p.closure_verified)
    @property
    def reason_codes(self): return tuple(dict.fromkeys(i.reason_code for i in self.issues))


def _key(row, *, broker=False):
    names = ('securityId','exchangeSegment','productType') if broker else ('security_id','exchange_segment','product_type')
    return tuple(row.get(n) for n in names)


def _id(value):
    return type(value) is str and bool(value) and value.strip() == value and value.upper() not in {'0','NONE','NULL'}


def _eid(value):
    return _id(value) and not any(s in value.upper() for s in ('-FILL-','-TRD-','-RECON-'))


def _uint(value):
    if type(value) is not int or value < 0:
        raise ValueError('Invalid durable quantity')
    return value


def _date(ts):
    if type(ts) not in (int,float) or not math.isfinite(ts) or ts <= 0:
        raise ValueError('Invalid durable chronology')
    return datetime.fromtimestamp(ts,IST).date()


def _ref(snapshot, book, index=None, journal_ref=None):
    return EvidenceRef(snapshot.snapshot_id, book.book_key, index, book.payload_sha256, journal_ref)


def _attestation(row, intent, journal, contract):
    """Legacy null-ID BUY exception: never convert into an EID."""
    if row.get('exchange_trade_id') is not None or row.get('evidence_source') != 'BROKER_HISTORICAL_NO_EXECUTION_ID' or row.get('transaction_type') != 'BUY':
        return None
    matches=[a for a in journal.historical_fill_attestations if a.get('broker_order_id')==row['broker_order_id']]
    if len(matches)!=1: return None
    att=matches[0]; raw=att.get('evidence_json'); value=json.loads(raw)
    digest=hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if (digest != att.get('evidence_fingerprint') or att.get('account_id')!=contract.account_id
            or att.get('correlation_id')!=intent.get('correlation_id') or not _id(att.get('exchange_order_id'))
            or value.get('exchangeOrderId')!=att.get('exchange_order_id') or value.get('dhanClientId')!=contract.account_id
            or _key(value,broker=True)!=_key(row) or value.get('orderId')!=row.get('broker_order_id')
            or value.get('transactionType')!='BUY' or value.get('exchangeTradeId') is not None
            or value.get('tradedQuantity')!=row.get('traded_quantity')
            or abs(_decimal_price(value.get('tradedPrice'))-_decimal_price(row.get('traded_price')))>=Decimal('0.000001')
            or _date(_exchange_time(value.get('exchangeTime')))!=_date(row['traded_at'])):
        return None
    return value


def _holdings_confirmed(snapshot, policy, now_mono_ns):
    """Exact ownership evidence from distinct, pinned acquisition IDs.

    Do not inject account IDs into broker rows or allow price-only changes to
    invalidate the ownership fingerprint. Both captures must still be fresh.
    """
    rows = thaw_json(snapshot.holdings.payload)
    if not all(isinstance(r, dict) for r in rows):
        return False
    if any('dhanClientId' in r and r['dhanClientId'] != snapshot.account_id for r in rows):
        return False
    if all('dhanClientId' in r for r in rows):
        return True
    proof = policy.holdings_confirmation
    if (proof is None or proof.snapshot_id == snapshot.snapshot_id
            or proof.account_id != snapshot.account_id or proof.generation != snapshot.generation
            or (proof.session.phase, proof.session.trading_session_date, proof.session.calendar_sha256) !=
               (snapshot.session.phase, snapshot.session.trading_session_date, snapshot.session.calendar_sha256)
            or proof.journal_digest != snapshot.journal_digest
            or proof.capture_started_mono_ns <= snapshot.capture_finished_mono_ns):
        return False
    try:
        request = EvidenceRequest(snapshot.account_id, policy.purpose, frozenset({'holdings'}),
                                  min_capture_mono_ns=policy.min_capture_mono_ns,
                                  independent_after_snapshot_id=snapshot.snapshot_id)
        if proof.validation_reasons(request, snapshot.generation, now_mono_ns,
                                    independent_after_mono_ns=snapshot.capture_finished_mono_ns):
            return False
        if not proof.profile_verified or proof.profile_account_id != snapshot.account_id:
            return False
        return _holdings_fingerprint(rows, snapshot.account_id) == _holdings_fingerprint(
            thaw_json(proof.holdings.payload), snapshot.account_id)
    except (ValueError, TypeError, AttributeError, KeyError):
        return False


class BrokerLedgerReconciler:
    """Derive a conservative per-contract verdict from immutable evidence.

    This class has no injected broker or database and no persistent state. A
    complex case that is not proven by the supplied evidence is blocked.
    """
    def reconcile_account(self, snapshot: BrokerSnapshot, journal: JournalView,
                          policy: PolicyContext, now_mono_ns: int) -> AccountVerdict:
        """AND every relevant contract; unknown identity cannot be skipped.

        Includes all non-TEST journal orders, live books, order-scoped evidence,
        archive pages and watcher projections. Unknown holdings ownership or
        ambiguous cross-product ownership blocks rather than guessing a contract.
        """
        keys = set()
        reasons = []
        account = snapshot.account_id

        def collect(row, *, broker=False):
            if not isinstance(row, Mapping):
                reasons.append('MALFORMED_ACCOUNT_ROW')
                return
            k = _key(row, broker=broker)
            if not all(_id(v) for v in k):
                reasons.append('ACCOUNT_CONTRACT_UNVERIFIED')
                return
            acct = row.get('dhanClientId' if broker else 'account_id', account)
            if acct != account:
                reasons.append('ACCOUNT_IDENTITY_CONFLICT')
            keys.add(k)

        for records in (journal.order_intents, journal.positions_lifecycle,
                        journal.fills, journal.protection_attempts):
            for row in records:
                collect(row)
        for row in journal.orders:
            if row.get('trading_mode') != 'TEST':
                collect(row)
        for row in journal.watcher_orders:
            try:
                rec = json.loads(row['record_json'])
                collect(dict(security_id=rec.get('security_id'), exchange_segment=rec.get('segment'),
                             product_type=rec.get('product_type')))
            except (ValueError, TypeError, KeyError, AttributeError):
                reasons.append('WATCHER_RECORD_MALFORMED')
        for name in sorted(LIVE_BOOKS - {'holdings'}):
            book = getattr(snapshot, name)
            if book.complete:
                for row in book.payload:
                    collect(row, broker=True)
        for book in (*snapshot.historical_trades, *snapshot.trades_for_order.values()):
            if book.complete:
                for row in book.payload:
                    collect(row, broker=True)
        for book in snapshot.order_status.values():
            if book.complete:
                collect(book.payload, broker=True)
        if snapshot.holdings.complete:
            try:
                holdings = aggregate_holdings(thaw_json(snapshot.holdings.payload), account_id=account)
                for security, qty in holdings.items():
                    if qty and len([k for k in keys if k[0] == security and k[2] in {'CNC','MTF'}]) != 1:
                        reasons.append('HOLDING_CONTRACT_OR_OWNERSHIP_UNVERIFIED')
            except (ValueError, TypeError, KeyError):
                reasons.append('HOLDING_EVIDENCE_UNVERIFIED')
        risk_state=read_durable_issues(journal)
        if risk_state.malformed: reasons.append("DURABLE_ISSUE_EVENT_MALFORMED")
        for occurrence in risk_state.unresolved:
            k=occurrence.contract
            keys.add((k.security_id,k.exchange_segment,k.product_type))
        known_parents = {r.get('parent_order_id') for r in journal.positions_lifecycle}
        for rows in (journal.protection_orders, journal.exit_allocations,
                     journal.manual_risk_acknowledgements, journal.retry_reservations):
            for row in rows:
                if row.get('parent_order_id') not in known_parents:
                    reasons.append('DURABLE_PARENT_BINDING_UNVERIFIED')
        try:
            request=EvidenceRequest(account,policy.purpose,LIVE_BOOKS,
                history_ranges=tuple((h.from_date,h.to_date) for h in snapshot.history_coverage),
                order_status_ids=tuple(snapshot.order_status),trade_order_ids=tuple(snapshot.trades_for_order),
                min_capture_mono_ns=policy.min_capture_mono_ns)
            reasons.extend(snapshot.validation_reasons(request,snapshot.generation,now_mono_ns))
        except (ValueError, TypeError, KeyError, AttributeError):
            reasons.append('MALFORMED_EVIDENCE_PLAN')
        if (journal.account_id!=account or not journal.read_complete or journal.digest_sha256!=snapshot.journal_digest
                or journal.journal_epoch!=snapshot.generation.journal_epoch or not policy.journal_readback_verified):
            reasons.append('JOURNAL_READBACK_UNVERIFIED')
        if (policy.active_mutation_ids or policy.unknown_mutation_ids or not policy.mutation_fence_verified
                or policy.owner_intent_epoch!=snapshot.generation.owner_epoch
                or not policy.service_identity_verified or not policy.watcher_liveness_verified):
            reasons.append('ACCOUNT_AUTHORITY_FENCE_UNVERIFIED')
        # Relevant durable system events must remain visible on empty ledgers.
        for row in journal.relevant_system_events:
            if row.get('event_type') in ('OWNER_LOT_EXECUTION_CONFLICT','OWNER_LOT_DEMAT_REVIEW'):
                reasons.append(row['event_type'])
        results = tuple(self.reconcile_security(snapshot,journal,ContractKey(account,*k),policy,now_mono_ns)
                        for k in sorted(keys))
        for result in results:
            reasons.extend(result.reason_codes)
        eligible = not reasons
        entry = (eligible and all(r.entry_eligible for r in results)
                 and policy.purpose in (Purpose.ENTRY,Purpose.STARTUP,Purpose.WATCHER,Purpose.RISK)
                 and policy.mode=='LIVE' and policy.owner_desired_state==policy.owner_observed_state=='RUNNING'
                 and not (policy.deferred_stop_pending or policy.entry_fence_active or policy.emergency_stop_active or policy.transition_active)
                 and policy.risk_funds_mode_checks_passed)
        shutdown = (eligible and all(r.shutdown_eligible for r in results) and policy.purpose is Purpose.SHUTDOWN
                    and policy.owner_desired_state=='STOPPED' and policy.entry_fence_active
                    and policy.min_capture_mono_ns>0 and snapshot.capture_started_mono_ns>=policy.min_capture_mono_ns)
        return AccountVerdict(snapshot.snapshot_id,snapshot.generation,journal.digest_sha256,results,
                              tuple(dict.fromkeys(reasons)),entry,shutdown)

    def reconcile_security(self, snapshot: BrokerSnapshot, journal: JournalView,
                           contract: ContractKey, policy: PolicyContext,
                           now_mono_ns: int) -> ReconciliationVerdict:
        return self._evaluate(snapshot,journal,contract,policy,now_mono_ns,include_persisted=True)

    def _evaluate(self, snapshot, journal, contract, policy, now_mono_ns, *, include_persisted):
        key=(contract.security_id,contract.exchange_segment,contract.product_type)
        issues=[]; unknown=[]; orphans=set(); parents=[]; extras=[]; refs=[]; history_matches=[]
        def issue(code, classification='DURABLE_UNRESOLVED', subject='', scope='CONTRACT', source='ledger', evidence=(), first_seen=None):
            subject=subject or '/'.join(key)
            ident=issue_identity(contract,code,scope,subject)
            at=snapshot.capture_finished_utc_ns if first_seen is None else first_seen
            for i,old in enumerate(issues):
                if old.issue_id==ident:
                    issues[i]=replace(old,first_seen_utc_ns=min(old.first_seen_utc_ns,at),
                                      evidence_refs=tuple(dict.fromkeys(old.evidence_refs+tuple(evidence))))
                    return
            issues.append(ReconciliationIssue(ident,code,classification,scope,subject,source,
                                              at,tuple(evidence),('EXACT_EVIDENCE_AND_DURABLE_READBACK',)))
        mutations=tuple(sorted(set(policy.active_mutation_ids+policy.unknown_mutation_ids)))
        identity=(snapshot.account_id==journal.account_id==contract.account_id and snapshot.profile_verified
                  and snapshot.profile_account_id==contract.account_id and snapshot.generation.account_key==account_key(contract.account_id))
        persisted=(journal.read_complete and journal.digest_sha256==snapshot.journal_digest
                   and journal.journal_epoch==snapshot.generation.journal_epoch and policy.journal_readback_verified)
        if not identity: issue('ACCOUNT_IDENTITY_UNVERIFIED','TRANSIENT',scope='ACCOUNT')
        if not persisted: issue('JOURNAL_READBACK_UNVERIFIED','DURABLE_UNRESOLVED',scope='ACCOUNT')
        if mutations or not policy.mutation_fence_verified: issue('MUTATION_UNRESOLVED',scope='ACCOUNT')
        if policy.owner_intent_epoch!=snapshot.generation.owner_epoch: issue('OWNER_GENERATION_MISMATCH','TRANSIENT',scope='ACCOUNT')
        try:
            request=EvidenceRequest(contract.account_id,policy.purpose,LIVE_BOOKS,
                                history_ranges=tuple((h.from_date,h.to_date) for h in snapshot.history_coverage),
                                order_status_ids=tuple(snapshot.order_status),trade_order_ids=tuple(snapshot.trades_for_order),
                                min_capture_mono_ns=policy.min_capture_mono_ns)
            problems=snapshot.validation_reasons(request,snapshot.generation,now_mono_ns)
        except (ValueError, TypeError, KeyError, AttributeError):
            problems=('MALFORMED_EVIDENCE_PLAN',)
        declared_pages=[k for c in snapshot.history_coverage for k in c.page_keys]
        if (len(declared_pages)!=len(set(declared_pages))
                or set(declared_pages)!={b.book_key for b in snapshot.historical_trades}
                or any(not c.terminal_empty_page_verified for c in snapshot.history_coverage)):
            problems=tuple(problems)+('ARCHIVE_PAGE_INVENTORY_UNVERIFIED',)
        for reason in problems: issue(reason,'TRANSIENT',source='snapshot')
        complete=snapshot.complete and all(getattr(snapshot,n).complete for n in LIVE_BOOKS|{'profile'})
        fresh=complete and not problems
        coherent=complete and not problems
        safe_input=identity and persisted and complete and fresh and coherent and not problems
        valid_until=now_mono_ns
        if fresh and coherent and not problems:
            valid_until=min([snapshot.profile.received_mono_ns+30*NS,snapshot.profile_valid_until_mono_ns]
                +[getattr(snapshot,n).received_mono_ns+10*NS for n in LIVE_BOOKS]
                +[b.received_mono_ns+10*NS for b in (*snapshot.order_status.values(),*snapshot.trades_for_order.values())]
                +[b.received_mono_ns+(300 if any(c.archive_only and b.book_key in c.page_keys for c in snapshot.history_coverage) else 10)*NS for b in snapshot.historical_trades])
        if policy.holdings_confirmation is not None and policy.holdings_confirmation.profile_valid_until_mono_ns is not None:
            valid_until=min(valid_until,policy.holdings_confirmation.profile_valid_until_mono_ns)
        broker_qty=ledger_qty=protected_qty=manual_qty=unprotected_qty=None
        history_complete=False; ledger_verified=False
        if safe_input:
            try:
                broker_qty,ledger_qty,protected_qty,manual_qty,unprotected_qty,history_complete,ledger_verified=self._reconcile(
                    snapshot,journal,contract,policy,issue,issues,parents,unknown,orphans,refs,extras,history_matches,now_mono_ns)
            except (ValueError,TypeError,KeyError,AttributeError,OverflowError,OSError,ArithmeticError) as exc:
                issue('MALFORMED_OR_CONTRADICTORY_LEDGER','CONTRADICTION',source=type(exc).__name__)
        # Preserve durable flags even when broker evidence is transiently missing.
        # Durable attempt/reservation evidence cannot be hidden by a transient
        # broker outage. No lifecycle mutation is performed by this verifier.
        for attempt in journal.protection_attempts:
            if _key(attempt)==key and (attempt.get('account_id')!=contract.account_id or attempt.get('state') not in _SAFE_ATTEMPTS):
                issue('PROTECTION_ATTEMPT_UNRESOLVED',subject=attempt.get('attempt_id') or 'unknown',scope='ORDER')
        known_parent_ids={r.get('parent_order_id') for r in journal.positions_lifecycle if _key(r)==key}
        for reservation in journal.retry_reservations:
            if reservation.get('parent_order_id') in known_parent_ids or reservation.get('root_order_id') in known_parent_ids:
                issue('RETRY_RESERVATION_UNRESOLVED',subject=reservation.get('parent_order_id'),scope='PARENT')
        for event in journal.relevant_system_events:
            if (event.get('event_type') in ('OWNER_LOT_EXECUTION_CONFLICT','OWNER_LOT_DEMAT_REVIEW')
                    and event.get('description') in {contract.account_id+'/'+str(p) for p in known_parent_ids}):
                issue(event['event_type'],subject=event['description'],scope='PARENT',source='journal_event')
        for row in journal.watcher_orders:
            try:
                record=json.loads(row['record_json']); oid=row['order_id']
                matching=(_key({'security_id':record.get('security_id'),'exchange_segment':record.get('segment'),'product_type':record.get('product_type')})==key)
                if matching:
                    for risk in legacy_risks(record):
                        observed=row.get('updated_at')
                        first=(int(observed*NS) if type(observed) in (int,float) and math.isfinite(observed) and 0<observed<=snapshot.capture_finished_utc_ns/NS else None)
                        evidence=() if not journal.digest_sha256 else (EvidenceRef(snapshot.snapshot_id,'journal:watcher_orders',None,journal.digest_sha256,'watcher_orders:'+str(oid)),)
                        issue(risk,subject=oid,scope='PARENT',source='watcher',evidence=evidence,first_seen=first)
                elif not all((record.get('security_id'),record.get('segment'),record.get('product_type'))):
                    issue('WATCHER_CONTRACT_UNVERIFIED',subject=oid,scope='ACCOUNT',source='watcher')
            except (ValueError,KeyError,TypeError,AttributeError): issue('WATCHER_RECORD_MALFORMED',scope='ACCOUNT')
        risk_state=read_durable_issues(journal)
        if risk_state.malformed: issue('DURABLE_ISSUE_EVENT_MALFORMED',scope='ACCOUNT',source='journal_event')
        if include_persisted:
            for occurrence in risk_state.unresolved:
                if occurrence.contract==contract:
                    issue(occurrence.reason_code,occurrence.classification,occurrence.subject_id,occurrence.scope,
                          occurrence.source,occurrence.evidence_refs,occurrence.first_seen_utc_ns)
        # A durable parent issue recorded after quantity/protection evaluation
        # must be present in the per-parent result, not only the contract list.
        parents=[replace(p,unresolved_risk_ids=tuple(dict.fromkeys(p.unresolved_risk_ids+
                 tuple(i.issue_id for i in issues if i.scope=='PARENT' and i.subject_id==p.parent_order_id and i.classification!='TRANSIENT')))) for p in parents]
        # Quantity arithmetic alone does not verify a ledger whose required
        # archive or execution evidence is missing. Preserve separate quantity
        # estimates for diagnosis, but do not label the ledger verified.
        ledger_verified=ledger_verified and history_complete and not unknown and not issues
        if any(i.classification=='CONTRADICTION' for i in issues): status=VerdictStatus.CONTRADICTORY
        elif any(i.classification=='DURABLE_UNRESOLVED' for i in issues): status=VerdictStatus.DURABLE_UNRESOLVED
        elif issues: status=VerdictStatus.TRANSIENT_UNAVAILABLE
        else: status=VerdictStatus.VERIFIED
        verified=(status is VerdictStatus.VERIFIED and safe_input and history_complete and ledger_verified
                  and broker_qty is not None and broker_qty==ledger_qty and unprotected_qty is not None
                  and unprotected_qty==manual_qty
                  and not mutations and not unknown and not orphans and policy.mutation_fence_verified
                  and policy.service_identity_verified and policy.watcher_liveness_verified)
        entry=(include_persisted and verified and policy.purpose in (Purpose.ENTRY,Purpose.STARTUP,Purpose.WATCHER,Purpose.RISK)
               and policy.mode=='LIVE' and policy.owner_desired_state==policy.owner_observed_state=='RUNNING'
               and not policy.deferred_stop_pending and not policy.entry_fence_active and not policy.emergency_stop_active
               and not policy.transition_active and policy.risk_funds_mode_checks_passed)
        shutdown=(include_persisted and verified and policy.purpose is Purpose.SHUTDOWN and policy.owner_desired_state=='STOPPED'
                  and policy.entry_fence_active and policy.min_capture_mono_ns>0
                  and snapshot.capture_started_mono_ns>=policy.min_capture_mono_ns)
        actions=()
        if journal.read_complete and journal.digest_sha256 and extras:
            action_id=payload_digest([snapshot.snapshot_id,journal.digest_sha256,'RECHECK',*[i.issue_id for i in issues]])
            actions=(ActionProposal(action_id,'RECHECK',None,snapshot.generation,journal.digest_sha256,tuple(refs),
                      {'reason_codes':list(dict.fromkeys(i.reason_code for i in issues))}),)
        vid=payload_digest([snapshot.snapshot_id,journal.digest_sha256,contract.account_id,*key,now_mono_ns,
                            policy.purpose.value,[i.issue_id for i in issues],entry,shutdown])
        return ReconciliationVerdict(vid,snapshot.snapshot_id,snapshot.generation,contract,journal.digest_sha256,
             now_mono_ns,valid_until,snapshot.session,status,identity,complete,fresh,coherent,history_complete,
             ledger_verified,persisted,broker_qty,ledger_qty,protected_qty,manual_qty,unprotected_qty,
             tuple(parents),tuple(issues),mutations,tuple(unknown),tuple(sorted(orphans)),tuple(refs),entry,shutdown,
             watcher_actions=actions,additional_evidence=tuple(extras),historical_matches=tuple(history_matches))

    def plan_durable_issue_openings(self, snapshot, journal, contract, policy, now_mono_ns):
        """Return explicit immutable event proposals; no SQL or authorization.

        Persistence of these events must occur under the final generation and
        journal fence in the Phase 2D orchestrator. Do not bulk reset flags.
        """
        state=read_durable_issues(journal)
        if state.malformed or not journal.read_complete or not journal.digest_sha256:
            raise ValueError('Durable issue journal unavailable')
        verdict=self.reconcile_security(snapshot,journal,contract,policy,now_mono_ns)
        if not verdict.persistence_verified:
            raise ValueError('Pinned journal unavailable')
        active={o.issue_id for o in state.unresolved}
        return tuple((OPENED,issue.issue_id,opening_payload(issue,contract,snapshot_id=snapshot.snapshot_id,
                      journal_digest=journal.digest_sha256,journal=journal)) for issue in verdict.issues
                     if issue.classification in ('DURABLE_UNRESOLVED','CONTRADICTION') and issue.issue_id not in active)

    def plan_durable_issue_resolution(self, snapshot, journal, contract, policy, now_mono_ns, issue_id):
        """Return one proof-bound append proposal, never entry/stop authority.

        A missing or already resolved occurrence, ambiguous legacy cause or
        mutation-specific risk cannot be cleared by this generic planner.
        """
        state=read_durable_issues(journal)
        targets=[o for o in state.unresolved if o.issue_id==issue_id and o.contract==contract]
        if state.malformed or len(targets)!=1: raise ValueError('Exact unresolved occurrence unavailable')
        evidence=self._evaluate(snapshot,journal,contract,policy,now_mono_ns,include_persisted=False)
        assert not evidence.entry_eligible and not evidence.shutdown_eligible
        payload=resolution_payload(targets[0],evidence,journal,resolved_utc_ns=snapshot.capture_finished_utc_ns)
        return RESOLVED,issue_id,payload

    def _reconcile(self,s,j,c,policy,issue,issues,parents,unknown,orphans,refs,extras,history_matches,now_mono_ns):
        key=(c.security_id,c.exchange_segment,c.product_type); acct=c.account_id
        session=s.session.trading_session_date
        observed_wall=s.capture_finished_utc_ns/NS
        today=_date(observed_wall)
        if session is None: raise ValueError('Missing session date')
        if (s.session.observed_utc_ns > s.capture_finished_utc_ns
                or (s.session.phase in (SessionPhase.CURRENT_SIGNED, SessionPhase.SETTLED) and session != today)
                or (s.session.phase is SessionPhase.PRIOR_SIGNED and
                    (session >= today or datetime.fromtimestamp(observed_wall, IST).hour * 60 +
                     datetime.fromtimestamp(observed_wall, IST).minute >= 510))):
            issue('BROKER_SESSION_CHRONOLOGY_UNVERIFIED','TRANSIENT')
        holding_rows=[thaw_json(r) for r in s.holdings.payload if isinstance(r,Mapping) and r.get('securityId')==c.security_id]
        expected_isin=(holding_rows[0].get('isin') if len(holding_rows)==1 else None)
        def rows(book):
            return [(i,thaw_json(r)) for i,r in enumerate(book.payload)]
        def related(book):
            result=[]
            for i,r in rows(book):
                if not _id(r.get('securityId')) or not all(_id(x) for x in _key(r,broker=True)):
                    issue('BROKER_CONTRACT_UNVERIFIED','CONTRADICTION',scope='ACCOUNT',evidence=(_ref(s,book,i),))
                    continue
                if _key(r,broker=True)==key:
                    if r.get('dhanClientId',acct)!=acct: issue('BROKER_ACCOUNT_CONTRADICTION','CONTRADICTION',scope='ACCOUNT')
                    if expected_isin and r.get('isin') and r['isin']!=expected_isin:
                        issue('BROKER_ISIN_CONTRADICTION','CONTRADICTION',evidence=(_ref(s,book,i),))
                    result.append((i,r))
            return result
        lifecycle={}; intents={}; fills={}; allocated=defaultdict(list); by_parent=defaultdict(list)
        for r in j.positions_lifecycle:
            if _key(r)==key:
                oid=r.get('parent_order_id')
                if not _id(oid) or oid in lifecycle or r.get('account_id')!=acct:
                    issue('LIFECYCLE_IDENTITY_CONFLICT','CONTRADICTION',scope='ACCOUNT'); continue
                lifecycle[oid]=r
            elif not all(_id(x) for x in _key(r)):
                issue('LIFECYCLE_CONTRACT_UNVERIFIED',scope='ACCOUNT')
        for r in j.order_intents:
            if _key(r)!=key: continue
            oid=r.get('broker_order_id'); state=r.get('state'); cid=r.get('correlation_id')
            if r.get('account_id')!=acct: issue('INTENT_ACCOUNT_CONFLICT','CONTRADICTION',scope='ACCOUNT'); continue
            if state not in _FINAL_ORDER: issue('NONTERMINAL_INTENT',subject=cid or 'unknown',scope='ORDER')
            if not _id(oid):
                if state not in _DEAD: issue('INTENT_ORDER_ID_UNVERIFIED',subject=cid or 'unknown',scope='ORDER')
                continue
            if oid in intents: issue('DUPLICATE_PARENT_INTENT','CONTRADICTION',subject=oid,scope='PARENT')
            intents[oid]=r
        # An entry projection must not disappear simply because its lifecycle
        # record is missing. Bind orders through the original durable intent.
        intent_by_correlation = {r.get('correlation_id'): r for r in j.order_intents}
        for r in j.orders:
            if r.get('trading_mode') != 'LIVE':
                if r.get('trading_mode') != 'TEST':
                    issue('JOURNAL_ORDER_MODE_UNVERIFIED',scope='ACCOUNT')
                continue
            if _key(r)!=key:
                if not all(_id(x) for x in _key(r)):
                    issue('JOURNAL_ORDER_CONTRACT_UNVERIFIED',scope='ACCOUNT')
                continue
            oid=r.get('broker_order_id'); cid=r.get('correlation_id'); intent=intent_by_correlation.get(cid)
            filled=_uint(r.get('filled_quantity')); requested=_uint(r.get('quantity'))
            if (not intent or intent.get('account_id')!=acct or _key(intent)!=key
                    or intent.get('broker_order_id')!=oid or filled>requested or not requested):
                issue('JOURNAL_ORDER_IDENTITY_CONFLICT','CONTRADICTION',subject=str(cid),scope='ORDER')
            if r.get('status') not in _FINAL_ORDER:
                issue('JOURNAL_ORDER_NONTERMINAL',subject=str(cid),scope='ORDER')
            if (filled or r.get('status') not in _DEAD) and oid not in lifecycle:
                issue('JOURNAL_ORDER_WITHOUT_LIFECYCLE',subject=str(cid),scope='ORDER')
        for oid,intent in intents.items():
            if intent.get('state') not in _DEAD and oid not in lifecycle:
                issue('INTENT_WITHOUT_LIFECYCLE',subject=oid,scope='ORDER')
        for r in j.retry_reservations:
            if r.get('parent_order_id') in lifecycle or r.get('root_order_id') in lifecycle: issue('RETRY_RESERVATION_UNRESOLVED',subject=r.get('parent_order_id'),scope='PARENT')
        for r in j.protection_attempts:
            if _key(r)==key and (r.get('account_id')!=acct or r.get('state') not in _SAFE_ATTEMPTS):
                issue('PROTECTION_ATTEMPT_UNRESOLVED',subject=r.get('attempt_id') or 'unknown',scope='ORDER')
        for r in j.relevant_system_events:
            if r.get('event_type') in ('OWNER_LOT_EXECUTION_CONFLICT','OWNER_LOT_DEMAT_REVIEW') and r.get('description') in {acct+'/'+p for p in lifecycle}:
                issue(r['event_type'],subject=r['description'],scope='PARENT',source='journal_event')
        for f in j.fills:
            if _key(f)!=key: continue
            oid=f.get('broker_order_id'); eid=f.get('exchange_trade_id'); side=f.get('transaction_type'); p=f.get('parent_order_id')
            if f.get('is_verified_trade')!=1:
                issue('UNVERIFIED_JOURNAL_FILL',subject=oid or 'unknown',scope='EXECUTION');continue
            if not _id(oid) or side not in ('BUY','SELL') or not _uint(f.get('traded_quantity')) or _date(f['traded_at'])>today or f['traded_at']>observed_wall:
                raise ValueError('Invalid durable fill')
            _decimal_price(f.get('traded_price'))
            if eid is None and side=='BUY' and oid in intents and _attestation(f,intents[oid],j,c):
                pass
            elif not _eid(eid): issue('SYNTHETIC_OR_MISSING_VERIFIED_EID','CONTRADICTION',subject=oid,scope='EXECUTION');continue
            fid=(oid,eid)
            if fid in fills or (eid is not None and any(k[1]==eid for k in fills)):
                issue('DUPLICATE_DURABLE_EXECUTION','CONTRADICTION',subject=oid,scope='EXECUTION');continue
            fills[fid]=f
            if side=='BUY':
                if p!=oid or p not in lifecycle or oid not in intents: issue('BUY_PARENT_UNVERIFIED',subject=oid,scope='PARENT')
                else: by_parent[p].append((fid,f['traded_quantity']))
        for a in j.exit_allocations:
            fid=(a.get('sell_order_id'),a.get('exchange_trade_id')); p=a.get('parent_order_id')
            if p not in lifecycle and fid not in fills: continue
            f=fills.get(fid)
            if not f or f.get('transaction_type')!='SELL' or p not in lifecycle or not _uint(a.get('allocated_qty')) or abs(_decimal_price(a['traded_price'])-_decimal_price(f['traded_price']))>=Decimal('0.000001') or a['traded_at']!=f['traded_at']:
                issue('EXIT_ALLOCATION_CONFLICT','CONTRADICTION',subject=str(p),scope='PARENT');continue
            if any(q==p for q,_ in allocated[fid]): issue('DUPLICATE_EXIT_ALLOCATION','CONTRADICTION',subject=str(p),scope='PARENT');continue
            allocated[fid].append((p,a['allocated_qty']))
        for fid,f in fills.items():
            if f['transaction_type']!='SELL': continue
            shares=allocated[fid]; p=f.get('parent_order_id')
            if not shares and p in lifecycle: shares=[(p,f['traded_quantity'])]
            if not shares or sum(q for _,q in shares)!=f['traded_quantity'] or (p and p not in {a for a,_ in shares}):
                issue('SELL_ALLOCATION_UNVERIFIED',subject=fid[0],scope='EXECUTION');continue
            for owner,qty in shares: by_parent[owner].append((fid,qty))
        # Require exact prior-day coverage from original intent through the latest
        # settled date. Current executions must appear in the complete live book.
        prior_dates=[_date(r['created_at']) for r in intents.values() if r.get('state') not in _DEAD and r.get('created_at')]
        oldest=min(prior_dates,default=session)
        end=session-timedelta(days=1)
        missing=[]; d=oldest
        while d<=end:
            if not any(h.complete and h.from_date<=d<=h.to_date for h in s.history_coverage): missing.append(d)
            if len(missing)>3660: raise ValueError('Historical range unbounded')
            d+=timedelta(days=1)
        history_complete=not missing
        if missing:
            issue('REQUIRED_ARCHIVE_COVERAGE_MISSING','TRANSIENT',source='history')
            # Request missing contiguous windows, maximum 31 days inclusive.
            lo=hi=missing[0]; ranges=[]
            for d in missing[1:]:
                if d!=hi+timedelta(days=1) or (d-lo).days>=30: ranges.append((lo,hi));lo=d
                hi=d
            ranges.append((lo,hi))
            extras.append(EvidenceRequest(acct,policy.purpose,LIVE_BOOKS,history_ranges=tuple(ranges)))
        # Every current and archive execution must corroborate a durable fill.
        # Historical aggregates may match only fully closed parents, with an
        # independently observed exact exchange-order binding.
        closed_fills={fid:(f['transaction_type'],f['traded_quantity'],f['traded_price'],f['traded_at'])
                      for fid,f in fills.items() if fid[1] is not None and
                      all(lifecycle[p]['position_state']=='POSITION_CLOSED' for p,items in by_parent.items() if any(k==fid for k,q in items))
                      and any(any(k==fid for k,q in items) for items in by_parent.values())}
        matcher=ClosedHistoricalBuyMatcher(closed_fills,closed_order_rollups(closed_fills),account=acct,contract=key,today=session,now=observed_wall)
        seen_live=set();seen_archive=set(); day_buy=day_sell=0; day_by_parent=defaultdict(int)
        execution_refs=defaultdict(list); missing_status_ids=set()
        current=related(s.trade_book)
        # ISIN is supplementary evidence, never a substitute for exact contract
        # identity. Reject conflicting reported identities across both books.
        isin_rows=[r for _,r in current]
        isin_rows.extend(r for b in s.historical_trades for _,r in related(b))
        isin_rows.extend(r for r in thaw_json(s.holdings.payload) if r.get('securityId')==c.security_id)
        isins={r['isin'].strip().upper() for r in isin_rows if isinstance(r.get('isin'),str) and r['isin'].strip()}
        if len(isins)>1 or any(r.get('isin') is not None and type(r['isin']) is not str for r in isin_rows):
            issue('EXECUTION_ISIN_CONFLICT','CONTRADICTION')
        def match_exact(r,f):
            return (r.get('dhanClientId')==acct and r.get('transactionType')==f.get('transaction_type')
                    and _integer(r.get('tradedQuantity'),positive=True)==f.get('traded_quantity')
                    and abs(_decimal_price(r.get('tradedPrice'))-_decimal_price(f.get('traded_price')))<Decimal('0.000001')
                    and _date(_exchange_time(r.get('exchangeTime')))==_date(f['traded_at'])
                    and _exchange_time(r.get('exchangeTime'))<=observed_wall)
        for i,r in current:
            fid=(r.get('orderId'),r.get('exchangeTradeId')); f=fills.get(fid); ref=_ref(s,s.trade_book,i)
            if not f or not _eid(fid[1]) or fid in seen_live or not match_exact(r,f) or _date(f['traded_at'])!=session:
                unknown.append(ref);issue('UNATTRIBUTED_CURRENT_EXECUTION','DURABLE_UNRESOLVED',subject=str(fid[0]),scope='EXECUTION',evidence=(ref,));continue
            refs.append(ref);seen_live.add(fid);execution_refs[fid].append(_ref(s,s.trade_book,i,journal_ref='fills:'+str(f['id'])))
            if f['transaction_type']=='BUY':day_buy+=f['traded_quantity'];day_by_parent[fid[0]]+=f['traded_quantity']
            else:day_sell+=f['traded_quantity']
        for b in s.historical_trades:
            coverage=[h for h in s.history_coverage if b.book_key in h.page_keys and h.complete]
            if len(coverage)!=1:
                issue('ARCHIVE_PAGE_COVERAGE_UNVERIFIED','TRANSIENT',source='history')
                history_complete=False
                continue
            for i,r in related(b):
                ref=_ref(s,b,i); fid=(r.get('orderId'),r.get('exchangeTradeId')); f=fills.get(fid)
                matched=None
                try:
                    trade_day=_date(_exchange_time(r['exchangeTime']))
                    if not coverage[0].from_date<=trade_day<=coverage[0].to_date or trade_day>=session:
                        raise ValueError('Archive execution outside settled coverage')
                    eid=r.get('exchangeTradeId'); blank=eid is None or type(eid) in (int,str) and str(eid).upper() in ('','0','NONE','NULL')
                    if not blank and f and match_exact(r,f): matched={fid}
                    elif blank and (fid[0],None) in fills:
                        f=fills[(fid[0],None)]; att=_attestation(f,intents[fid[0]],j,c)
                        if att and match_exact(r,f) and all(r.get(k)==att[k] for k in ('exchangeOrderId','exchangeTime','isin')):
                            matched={(fid[0],None)}
                    elif blank and fid[0] in {k[0] for k in closed_fills}:
                        matched=matcher.match(r)
                        if matched:
                            # An exact order status book proves archive exchange order identity; never fabricate it.
                            st=s.order_status.get(fid[0]); payload=thaw_json(st.payload) if st and st.complete else None
                            if st is None or not st.complete: missing_status_ids.add(fid[0])
                            if (not isinstance(payload,dict) or payload.get('orderId')!=fid[0]
                                    or payload.get('exchangeOrderId')!=r.get('exchangeOrderId') or payload.get('dhanClientId')!=acct
                                    or _key(payload,broker=True)!=key or payload.get('transactionType')!=r.get('transactionType')
                                    or payload.get('orderStatus')!='TRADED' or _integer(payload.get('remainingQuantity'))!=0
                                    or _integer(payload.get('filledQty'))!=sum(fills[k]['traded_quantity'] for k in matched)
                                    or _integer(payload.get('quantity'))!=_integer(payload.get('filledQty'))):
                                matched=None
                    if matched and (matched & seen_archive or any(_date(fills[k]['traded_at'])!=_date(_exchange_time(r['exchangeTime'])) for k in matched)):
                        matched=None
                except (ValueError,TypeError,KeyError,ArithmeticError): matched=None
                if matched is None:
                    unknown.append(ref);issue('ARCHIVE_EXECUTION_UNATTRIBUTED_OR_OVERLAP',subject=str(fid[0]),scope='EXECUTION',evidence=(ref,));continue
                seen_archive.update(matched);refs.append(ref)
                for k in matched:
                    execution_refs[k].append(_ref(s,b,i,journal_ref='fills:'+str(fills[k]['id'])))
                if blank and all(k[1] is not None for k in matched):
                    keys=tuple(sorted(matched)); total=sum(fills[k]['traded_quantity'] for k in keys)
                    weighted=sum(_decimal_price(fills[k]['traded_price'])*fills[k]['traded_quantity'] for k in keys)/total
                    history_matches.append(HistoricalMatch(ref,c,fid[0],r['transactionType'],trade_day,
                        tuple(_ref(s,b,i,journal_ref='fills:'+str(fills[k]['id'])) for k in keys),
                        tuple(k[1] for k in keys),
                        tuple(sorted(p for p,items in by_parent.items() if any(k in matched for k,q in items))),
                        total,weighted))
        # The optional order-scoped executions are an independent corroboration
        # read, not another allocation source. Reject any unexplained row.
        for oid,b in s.trades_for_order.items():
            for i,r in related(b):
                fid=(r.get('orderId'),r.get('exchangeTradeId')); f=fills.get(fid); ref=_ref(s,b,i)
                if fid[0]!=oid or not f or not _eid(fid[1]) or not match_exact(r,f):
                    unknown.append(ref);issue('ORDER_SCOPED_EXECUTION_UNVERIFIED',subject=oid,scope='EXECUTION',evidence=(ref,))
        for fid,f in fills.items():
            day=_date(f['traded_at'])
            if day<session and fid not in seen_archive:
                history_complete=False;issue('DURABLE_EXECUTION_ARCHIVE_MISSING','TRANSIENT',subject=fid[0],scope='EXECUTION')
            if day==session and fid not in seen_live: issue('CURRENT_EXECUTION_MISSING',subject=fid[0],scope='EXECUTION')
            if day>session: issue('EXECUTION_AFTER_BROKER_SESSION','CONTRADICTION',subject=fid[0],scope='EXECUTION')
        # Validate journal parents and derive quantities from durable executions.
        totals={}; seen_groups={}; relevant_forever=related(s.forever_orders); relevant_super=related(s.super_orders)
        forever=defaultdict(list); super_rows=defaultdict(list)
        for i,r in relevant_forever: forever[r.get('orderId')].append(r)
        for i,r in relevant_super: super_rows[r.get('orderId')].append(r)
        order_rows={}; duplicate_orders=set()
        for i,r in related(s.order_book):
            oid=r.get('orderId')
            if oid in order_rows: duplicate_orders.add(oid)
            order_rows[oid]=r
            if r.get('orderStatus') not in _FINAL_ORDER: issue('NONTERMINAL_BROKER_ORDER',subject=str(oid),scope='ORDER')
        for oid in duplicate_orders: issue('DUPLICATE_BROKER_ORDER','CONTRADICTION',subject=str(oid),scope='ORDER')
        for oid,b in s.order_status.items():
            r=thaw_json(b.payload)
            if isinstance(r,dict) and _key(r,broker=True)==key:
                if oid in order_rows and order_rows[oid]!=r: issue('CONFLICTING_ORDER_STATUS','CONTRADICTION',subject=oid,scope='ORDER')
                order_rows.setdefault(oid,r)
        for oid,r in order_rows.items():
            if r.get('orderStatus') not in _DEAD and oid not in intents and not any(fid[0]==oid for fid in fills): issue('UNKNOWN_BROKER_ORDER',subject=str(oid),scope='ORDER')
        for p,life in sorted(lifecycle.items()):
            parent_refs=[ref for fid,q in by_parent[p] for ref in execution_refs[fid]]; start=len(issues); buy=sum(q for fid,q in by_parent[p] if fills[fid]['transaction_type']=='BUY');sell=sum(q for fid,q in by_parent[p] if fills[fid]['transaction_type']=='SELL')
            open_qty=buy-sell; route=life.get('protection_route');state=life.get('position_state'); verified_prot=manual=0;child_ids=[]; closure=False
            if p not in intents or sell>buy or buy!=_uint(life['buy_filled_qty']) or sell!=_uint(life['sell_filled_qty']) or open_qty!=_uint(life['confirmed_open_qty']) or buy>_uint(life['entry_qty']):
                issue('PARENT_LEDGER_QUANTITY_CONFLICT','CONTRADICTION',subject=p,scope='PARENT')
            if life.get('manual_exit_detected') or life.get('orphaned_legs_detected') or state in ('RECONCILIATION_REQUIRED','PERSISTENCE_UNCERTAIN','MANUAL_INTERVENTION_REQUIRED','UNKNOWN'):
                issue('LIFECYCLE_RISK_UNRESOLVED',subject=p,scope='PARENT')
            binds=[r for r in j.protection_orders if r.get('parent_order_id')==p]
            for binding in binds:
                gid=binding.get('forever_order_id')
                if not _id(gid) or gid in seen_groups:
                    issue('PROTECTION_GROUP_REUSED_OR_UNKNOWN','CONTRADICTION',subject=p,scope='PARENT');continue
                seen_groups[gid]=p
                if open_qty<=0: continue
                rs=forever.get(gid,[])
                if route=='FOREVER_OCO' and rs:
                    if any(r.get('parentOrderId') not in (None,'',p) for r in rs):
                        issue('PROTECTION_PARENT_MISMATCH','CONTRADICTION',subject=p,scope='PARENT');continue
                    proof=verify_forever_oco_group(rs,gid,expected_security_id=c.security_id,expected_account_id=acct,
                                                   expected_segment=c.exchange_segment,expected_product_type=c.product_type,
                                                   expected_qty=_uint(binding['quantity']),expected_sl_trigger=binding['sl_trigger'],expected_target_price=binding['target_price'])
                    if proof.is_verified: verified_prot+=proof.protected_quantity;child_ids.append(gid)
                    else: issue('PROTECTION_UNVERIFIED',subject=p,scope='PARENT')
                elif route=='SUPER_ORDER' and len(super_rows.get(gid,[]))==1 and gid==p:
                    proof=verify_super_order_record(super_rows[gid][0],gid,expected_security_id=c.security_id,
                          expected_account_id=acct,expected_segment=c.exchange_segment,expected_product_type=c.product_type,
                          expected_qty=_uint(binding['quantity']),expected_sl_price=binding['sl_trigger'],expected_target_price=binding['target_price'])
                    if proof.is_verified: verified_prot+=proof.protected_quantity;child_ids.append(gid)
                    else: issue('SUPER_PROTECTION_UNVERIFIED',subject=p,scope='PARENT')
                else: issue('PROTECTION_BOOK_BINDING_MISSING',subject=p,scope='PARENT')
            if open_qty>0 and state=='UNPROTECTED_MANUAL_RISK':
                acks=[a for a in j.manual_risk_acknowledgements if a.get('parent_order_id')==p]
                if (len(acks)==1 and p in intents and acks[0].get('correlation_id')==intents[p].get('correlation_id')
                        and acks[0].get('account_id')==acct and acks[0].get('security_id')==c.security_id
                        and acks[0].get('quantity')==open_qty and acks[0].get('status')=='UNPROTECTED_MANUAL_RISK'
                        and acks[0].get('acknowledged_by') and acks[0].get('reason') and not binds and not sell and history_complete
                        and c.product_type=='CNC' and _id(expected_isin) and _date(intents[p]['created_at'])<session): manual=open_qty
                else: issue('MANUAL_ACK_UNVERIFIED',subject=p,scope='PARENT')
            if verified_prot+manual>open_qty: issue('OVERPROTECTION_OR_DOUBLE_CREDIT','CONTRADICTION',subject=p,scope='PARENT')
            if open_qty==0 and buy>0:
                if route=='SUPER_ORDER' and not binds: issue('CLOSED_SUPER_BINDING_REQUIRED',subject=p,scope='PARENT')
                # Require terminal exact exit order and zero residual. Triggered
                # OCO closure is separately verified against its actual children.
                sold_by_oid=defaultdict(int)
                for fid,q in by_parent[p]:
                    if fills[fid]['transaction_type']=='SELL': sold_by_oid[fid[0]]+=q
                for child,qty in sold_by_oid.items():
                    r=order_rows.get(child)
                    if not r: missing_status_ids.add(child)
                    if not r or r.get('dhanClientId')!=acct or r.get('transactionType')!='SELL' or r.get('orderStatus')!='TRADED' or _integer(r.get('filledQty'))!=sum(f['traded_quantity'] for fid,f in fills.items() if fid[0]==child and f['transaction_type']=='SELL') or _integer(r.get('quantity'))!=_integer(r.get('filledQty')) or _integer(r.get('remainingQuantity'))!=0:
                        issue('CLOSED_CHILD_STATUS_UNVERIFIED',subject=child,scope='ORDER')
                    else: child_ids.append(child)
                for b in binds:
                    gid=b.get('forever_order_id'); group_rows=forever.get(gid,[])
                    active=[r for r in group_rows if r.get('orderStatus') not in _DEAD]
                    if route=='FOREVER_OCO' and (life.get('triggered_leg') in ('TARGET_LEG','STOP_LOSS_LEG') or active):
                        try:
                            records=[json.loads(w['record_json']) for w in j.watcher_orders if w.get('order_id')==p]
                            binding=closed_triggered_binding(records[0],thaw_json(life),[thaw_json(x) for x in binds],dict(sold_by_oid)) if len(records)==1 else None
                            if not binding: raise ValueError('Missing durable closed parent/group/leg binding')
                            # Protective children cannot be shared across parents.
                            if any(sum(q for fid,q in by_parent[other] if fid[0] in sold_by_oid and fills[fid]['transaction_type']=='SELL')
                                   for other in lifecycle if other!=p):
                                raise ValueError('Protective child belongs to another parent')
                            closed={'trades':{fid:(f['transaction_type'],f['traded_quantity'],f['traded_price'],f['traded_at']) for fid,f in fills.items() if fid[0] in sold_by_oid},
                                    'triggered_groups':{gid:binding}}
                            if active:
                                historical=[]
                                for match in history_matches:
                                    if match.side=='SELL' and match.broker_order_id in sold_by_oid:
                                        book=next(bk for bk in s.historical_trades if bk.book_key==match.summary_ref.book_key)
                                        raw=book.payload[match.summary_ref.row_index]
                                        historical.append(HistoricalSellMatch(match.broker_order_id,raw['exchangeOrderId'],match.trade_date,
                                            frozenset((match.broker_order_id,eid) for eid in match.matched_exchange_trade_ids),True))
                                # Exact prior-day executions are corroboration too, never fake live rows.
                                for fid in seen_archive:
                                    if fid[0] in sold_by_oid and fid[1] is not None and not any(fid in h.execution_keys for h in historical):
                                        historical.append(HistoricalSellMatch(fid[0], '', _date(fills[fid]['traded_at']),frozenset({fid}),False))
                                verify_closed_triggered_group(group=gid,closed=closed,forever=group_rows,orders=list(order_rows.values()),
                                    day_trades=[r for _,r in current],account=acct,security=c.security_id,segment=c.exchange_segment,
                                    product=c.product_type,historical_sell_matches=historical)
                            else:
                                # A retired group still needs the exact terminal children.
                                confirmed=validate_forever_oco_child_orders(forever_order_id=gid,expected_leg_qty=sell,key=key,account=acct,
                                    orders=list(order_rows.values()),sell_fill_qty_by_oid=dict(sold_by_oid))
                                if confirmed!=set(sold_by_oid): raise ValueError('Retired child set mismatch')
                        except (ValueError,TypeError,KeyError,IndexError,StopIteration):
                            issue('CLOSED_TRIGGERED_CHILD_UNVERIFIED',subject=p,scope='PARENT')
                    elif route=='SUPER_ORDER':
                        try:
                            candidates=super_rows.get(gid,[])
                            if gid!=p or len(candidates)!=1: raise ValueError('Missing closed Super Order parent')
                            candidate=candidates[0]
                            if candidate.get('orderStatus') not in ('CLOSED','TRADED','FILLED','COMPLETE'):
                                raise ValueError('Super parent not terminal')
                            legs=candidate.get('legDetails')
                            if not isinstance(legs,list): raise ValueError('Missing Super Order legs')
                            protective=[l for l in legs if isinstance(l,dict) and l.get('legName') in ('TARGET_LEG','STOP_LOSS_LEG')]
                            if len(protective)!=2 or {l['legName'] for l in protective}!={'TARGET_LEG','STOP_LOSS_LEG'}:
                                raise ValueError('Ambiguous Super Order protective legs')
                            executed=[]
                            for leg in protective:
                                oid=leg.get('orderId'); status=leg.get('orderStatus')
                                if not _id(oid) or leg.get('transactionType')!='SELL' or _integer(leg.get('remainingQuantity'))!=0:
                                    raise ValueError('Super Order leg identity/residual unverified')
                                if status=='TRADED':
                                    if leg.get('legName')!=life.get('triggered_leg') or oid not in sold_by_oid or sold_by_oid[oid]!=sell:
                                        raise ValueError('Super Order execution belongs to another leg/parent')
                                    if _integer(leg.get('quantity'))!=sell: raise ValueError('Super Order leg quantity conflict')
                                    executed.append(oid)
                                elif status not in _DEAD or oid in sold_by_oid:
                                    raise ValueError('Super Order leg remains unresolved')
                            if len({l['orderId'] for l in protective})!=2 or len(executed)>1:
                                raise ValueError('Duplicate or competing Super Order exits')
                            if life.get('triggered_leg') in ('TARGET_LEG','STOP_LOSS_LEG') and set(executed)!=set(sold_by_oid):
                                raise ValueError('Super Order triggered leg not verified')
                            if any(sum(q for fid,q in by_parent[other] if fid[0] in executed and fills[fid]['transaction_type']=='SELL')
                                   for other in lifecycle if other!=p): raise ValueError('Super Order exit shared across parents')
                        except (ValueError,TypeError,KeyError):
                            issue('CLOSED_SUPER_LEG_UNVERIFIED',subject=p,scope='PARENT')
                    elif active:
                        issue('CLOSED_PARENT_ACTIVE_LEG',subject=p,scope='PARENT')
                closure=state=='POSITION_CLOSED' and sell==buy and len(issues)==start and history_complete
                if not closure: issue('CLOSURE_UNVERIFIED',subject=p,scope='PARENT')
            # Owner acceptance is an authorization exception, not protection.
            deficit=max(0,open_qty-verified_prot)
            if deficit!=manual: issue('UNPROTECTED_QUANTITY',subject=p,scope='PARENT')
            totals[p]=(open_qty,verified_prot,manual,deficit)
            parents.append(ParentVerdict(p,state,buy,sell,open_qty,verified_prot,manual,deficit,closure,route,
                      tuple(sorted(set(child_ids))),0 if closure else None,manual>0,tuple(i.issue_id for i in issues[start:]),tuple(parent_refs)))
        for gid,rs in list(forever.items())+list(super_rows.items()):
            if gid not in seen_groups and any(r.get('orderStatus') not in _DEAD for r in rs):
                orphans.add(str(gid));issue('ORPHAN_PROTECTION_GROUP',subject=str(gid),scope='ORDER')
        if missing_status_ids:
            extras.append(EvidenceRequest(acct,policy.purpose,LIVE_BOOKS,
                                          order_status_ids=tuple(sorted(missing_status_ids))))
        # Holdings without a journal parent may belong to a manual portfolio;
        # never count them as bot-owned. Nonzero broker positions cannot be ignored.
        pos=[r for _,r in related(s.positions)];hold=thaw_json(s.holdings.payload)
        broker_qty,quantity_reason=observed_open_quantity(pos,hold,security_id=c.security_id,
                                         segment=c.exchange_segment,product=c.product_type,account_id=acct)
        ledger=sum(v[0] for v in totals.values())
        if c.product_type in ('CNC','MTF') and any(r.get('securityId')==c.security_id and r.get('dhanClientId')!=acct for r in hold):
            if not _holdings_confirmed(s,policy,now_mono_ns):
                issue('HOLDINGS_ACCOUNT_BINDING_REQUIRED','TRANSIENT')
                broker_qty=None
            else:
                refs.extend((_ref(s,s.holdings), _ref(policy.holdings_confirmation,policy.holdings_confirmation.holdings)))
        if c.product_type=='CNC' and quantity_reason=='VERIFIED_POSITIONS' and broker_qty:
            if (s.session.phase not in (SessionPhase.CURRENT_SIGNED,SessionPhase.PRIOR_SIGNED)
                    or len(pos)!=1 or any(v[0]>0 and _date(intents[p]['created_at'])!=session for p,v in totals.items())):
                issue('CNC_SIGNED_SESSION_UNVERIFIED','TRANSIENT')
            else:
                validate_signed_position_components(pos[0],broker_qty,day_buy,day_sell,require_day_totals=True)
                if broker_qty!=day_buy-day_sell:
                    issue('CNC_CURRENT_EXECUTION_QUANTITY_MISMATCH','CONTRADICTION')
        if (c.product_type=='CNC' and not any(i.reason_code=='HOLDINGS_ACCOUNT_BINDING_REQUIRED' for i in issues)
                and quantity_reason in ('NEGATIVE_CNC_POSITION_REVIEW','CNC_POSITIONS_HOLDINGS_CONTRADICT')):
            if len(pos)==1 and s.session.phase in (SessionPhase.CURRENT_SIGNED,SessionPhase.PRIOR_SIGNED):
                net=_integer_signed(pos[0].get('netQty'))
                validate_signed_position_components(pos[0],net,day_buy,day_sell,require_day_totals=True)
                settled=aggregate_holdings(hold,account_id=acct).get(c.security_id)
                prior=sum(v[0] for p,v in totals.items() if _date(intents[p]['created_at'])<session)
                current_open=sum(v[0] for p,v in totals.items() if _date(intents[p]['created_at'])==session)
                verified_closed_exits=all(p.closure_verified for p in parents if p.verified_sell_qty)
                if settled is not None and net==day_buy-day_sell and (net<0 and verified_closed_exits and settled+net==ledger or net>=0 and settled==prior and net==current_open):
                    broker_qty=ledger
        if c.product_type=='CNC' and (day_buy or day_sell):
            if s.session.phase not in (SessionPhase.CURRENT_SIGNED,SessionPhase.PRIOR_SIGNED) or len(pos)!=1:
                issue('CNC_SIGNED_DAY_ACTIVITY_UNVERIFIED','TRANSIENT')
            else:
                signed_net=_integer_signed(pos[0].get('netQty'))
                validate_signed_position_components(pos[0],signed_net,day_buy,day_sell,require_day_totals=True)
                if signed_net!=day_buy-day_sell:
                    issue('CNC_CURRENT_EXECUTION_QUANTITY_MISMATCH','CONTRADICTION')
        # Both books independently empty for this contract can prove zero only
        # when no durable open quantity or unresolved execution is present.
        if (broker_qty is None and quantity_reason=='SETTLED_HOLDING_NOT_REPORTED'
                and not pos and ledger==0 and all(not r.get('securityId')==c.security_id for r in hold)):
            broker_qty=0
        if broker_qty is None: issue('BROKER_QUANTITY_UNVERIFIED','TRANSIENT',source=quantity_reason)
        elif broker_qty!=ledger: issue('BROKER_LEDGER_QUANTITY_MISMATCH','CONTRADICTION')
        if not lifecycle and broker_qty==0 and fills: issue('FILLS_WITHOUT_LEDGER','CONTRADICTION')
        return broker_qty,ledger,sum(v[1] for v in totals.values()),sum(v[2] for v in totals.values()),sum(v[3] for v in totals.values()),history_complete,not unknown and all(v[0]>=0 for v in totals.values()) and broker_qty==ledger


def _integer_signed(value):
    if type(value) is int: return value
    if type(value) is str and value.lstrip('-').isdigit(): return int(value)
    raise ValueError('Invalid signed position quantity')
