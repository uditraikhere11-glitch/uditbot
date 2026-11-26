"""Approved Phase 1 classification; no reset or write operations.

Classification is descriptive, not a license to clear an issue. Legacy mixed
flags are conservatively imported for explicit, parent-bound reconciliation.
"""
from collections.abc import Mapping
from types import MappingProxyType

TRANSIENT_FIELDS = frozenset(('unreadable_broker_response', 'broker_quantity_unverified', 'broker_quantity_evidence', 'owner_evidence_error', 'manual_risk_ack_unverified', 'owner_history_day', 'reconciliation_required', '_schedule_ack_verified', '_is_terminal_col', 'broker_observed_exit_filled_qty', 'broker_observed_exit_vwap', 'oco_rejection', 'oco_retry_delay', 'oco_alert_after', 'retry_at', 'retry_quote_timestamp_type', 'retry_source_quote_timestamp', 'retry_local_retrieval_timestamp', 'orphaned_leg_cancel_details.*.last_broker_status', 'orphaned_leg_cancel_details.*.last_readback_at'))
DURABLE_RISK_FIELDS = frozenset(('external_unlinked_sell_observed', 'unverified_trade_detected', 'unverified_trade_reason', 'manual_exit_detected', 'manual_exit_evidence_reason', 'manual_exit_evidence_verified', 'owner_readback_failed', 'persistence_uncertain', 'last_persistence_error', 'uncommitted_broker_sell_trades', 'broker_exit_already_executed', 'entry_fill_evidence_required', 'pending_oco', 'protection_claim_uncertain', 'cancel_uncertain', 'cancel_requested', 'retry_reserved', 'retry_eligible', 'cancel_exhausted', 'cancel_error', 'retry_error', 'ambiguous_super_closed', 'exit_evidence_status', 'orphaned_legs_detected', 'orphaned_legs_manual_intervention_required', 'orphaned_leg_cancel_states', 'orphaned_leg_cancel_details', 'reanchor_status', 'target_leg_mod_state', 'sl_leg_mod_state', 'protection_status', 'pending_oco.state', 'pending_oco.target_leg_state', 'pending_oco.sl_leg_state', 'orphaned_leg_cancel_details.*.state', 'orphaned_leg_cancel_details.*.last_error', 'orphaned_leg_cancel_details.*.escalated'))
BUSINESS_FACT_FIELDS = frozenset(('state', 'position_state', 'is_terminal', 'terminal_confirmed', 'verified_entry_terminal_status', 'is_super_order', 'is_amo', 'entry_only_amo', 'protection_route', 'initial_protection_mode', 'entry_timeout_elapsed', 'timed_out_cancel', 'cancel_origin', 'cancel_reason', 'cancel_requested_at', 'cancel_acknowledged', 'cancel_attempts', 'max_cancel_attempts', 'next_cancel_retry_at', 'retry_count', 'max_retries', 'auto_retry_attempted', 'auto_retry_result', 'retry_final_status', 'retry_order_id', 'retry_correlation_id', 'retry_executed_at', 'amo_activated_at', 'manual_risk_acknowledged', 'manual_risk_acknowledged_by', 'manual_risk_reason', 'orphaned_legs_cancelled', 'confirmed_protection_ids', 'verified_leg_order_ids', 'verified_leg_map', 'triggered_leg', 'sl_order_id', 'tp_order_id', 'exit_order_id', 'forever_order_id', 'oco_order_id', 'broker_flat_verified_at', 'broker_flat_proof', 'position_closed_notified', 'notification_protection_pending', 'amo_manual_risk_alerted', 'cancel_exhausted_alerted', 'orphaned_legs_escalation_alerted', 'orphaned_legs_alerted', 'persistence_failure_alerted', 'orphaned_leg_cancel_details.*.escalated_at', 'orphaned_leg_cancel_details.*.attempt_count', 'orphaned_leg_cancel_details.*.max_attempts', 'orphaned_leg_cancel_details.*.first_requested_at', 'orphaned_leg_cancel_details.*.last_requested_at', 'orphaned_leg_cancel_details.*.next_retry_at', 'orphaned_leg_cancel_details.*.deadline_at'))

CLASSIFICATION = MappingProxyType({**dict.fromkeys(TRANSIENT_FIELDS, 'TRANSIENT'),
    **dict.fromkeys(DURABLE_RISK_FIELDS, 'DURABLE_UNRESOLVED'),
    **dict.fromkeys(BUSINESS_FACT_FIELDS, 'BUSINESS_FACT')})

# Status and provenance fields cannot be dismissed merely because their value
# is nonempty. This narrow allowlist describes resolved legacy projections;
# all other nonempty/unknown values require explicit proof.
_RESOLVED_STATUS = frozenset({'VERIFIED', 'CONFIRMED', 'PROTECTED', 'POSITION_CLOSED',
                              'POSITION_CLOSED_LOCAL', 'VERIFIED_PROTECTED',
                              'NOT_REQUIRED', 'NOT_APPLICABLE', 'NONE'})
_STATUS_FIELDS = frozenset({'exit_evidence_status', 'reanchor_status',
                            'target_leg_mod_state', 'sl_leg_mod_state', 'protection_status'})


def legacy_risks(record: Mapping) -> tuple[str, ...]:
    """Conservatively report durable risk without mutating the supplied record.

    Nested attempt/cancel objects remain whole durable obligations. Unknown
    legacy fields are preserved by the journal view, never deleted or used as
    authority. A positive legacy reconciliation flag lacks cause attribution.
    """
    if not isinstance(record, Mapping):
        return ('LEGACY_UNCLASSIFIED_RISK:record',)
    risks = []
    for field in sorted(DURABLE_RISK_FIELDS):
        if '.' in field or field not in record:
            continue
        value = record[field]
        if value is False or (field == 'pending_oco' and value is None) or (type(value) is str and value == ''):
            continue
        if isinstance(value, (tuple, list, Mapping)) and not value:
            continue
        if field in _STATUS_FIELDS and type(value) is str and value in _RESOLVED_STATUS:
            continue
        risks.append('LEGACY_UNCLASSIFIED_RISK:' + field)
    if record.get('reconciliation_required') is not False and 'reconciliation_required' in record:
        risks.append('LEGACY_UNCLASSIFIED_RISK:reconciliation_required')
    return tuple(risks)
