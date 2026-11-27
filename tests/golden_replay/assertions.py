"""Mandatory step-by-step checkpoint, prohibited-effect, and executable expectation assertions (Sections 18 & 31)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.broker.broker_snapshot import BrokerSnapshot
from src.engine.broker_ledger_reconciler import ReconciliationVerdict, VerdictStatus


@dataclass(frozen=True)
class CheckpointAssertion:
    name: str
    passed: bool
    detail: str


def assert_verdict_checkpoint(
    verdict: ReconciliationVerdict,
    *,
    expected_status: VerdictStatus | None = None,
    expected_entry_eligible: bool | None = None,
    expected_shutdown_eligible: bool | None = None,
    expected_broker_open_qty: int | None = None,
    expected_ledger_open_qty: int | None = None,
    expected_protected_qty: int | None = None,
    expected_manual_ack_qty: int | None = None,
    expected_unprotected_qty: int | None = None,
    required_reason_codes: tuple[str, ...] = (),
    forbidden_reason_codes: tuple[str, ...] = (),
) -> list[CheckpointAssertion]:
    checks: list[CheckpointAssertion] = []

    def _rec(name: str, cond: bool, detail: str) -> None:
        checks.append(CheckpointAssertion(name=name, passed=bool(cond), detail=detail))
        if not cond:
            raise AssertionError(f"Checkpoint {name} failed: {detail}")

    if expected_status is not None:
        _rec("status", verdict.status == expected_status, f"got={verdict.status} expected={expected_status} reasons={verdict.reason_codes}")
    if expected_entry_eligible is not None:
        _rec("entry_eligible", verdict.entry_eligible is expected_entry_eligible, f"got={verdict.entry_eligible} expected={expected_entry_eligible} reasons={verdict.reason_codes}")
    if expected_shutdown_eligible is not None:
        _rec("shutdown_eligible", verdict.shutdown_eligible is expected_shutdown_eligible, f"got={verdict.shutdown_eligible} expected={expected_shutdown_eligible} reasons={verdict.reason_codes}")
    if expected_broker_open_qty is not None:
        _rec("broker_open_qty", verdict.broker_open_qty == expected_broker_open_qty, f"got={verdict.broker_open_qty} expected={expected_broker_open_qty}")
    if expected_ledger_open_qty is not None:
        _rec("ledger_open_qty", verdict.ledger_open_qty == expected_ledger_open_qty, f"got={verdict.ledger_open_qty} expected={expected_ledger_open_qty}")
    if expected_protected_qty is not None:
        _rec("verified_protected_qty", verdict.verified_protected_qty == expected_protected_qty, f"got={verdict.verified_protected_qty} expected={expected_protected_qty}")
    if expected_manual_ack_qty is not None:
        _rec("manual_ack_qty", verdict.manual_ack_qty == expected_manual_ack_qty, f"got={verdict.manual_ack_qty} expected={expected_manual_ack_qty}")
    if expected_unprotected_qty is not None:
        _rec("unprotected_qty", verdict.unprotected_qty == expected_unprotected_qty, f"got={verdict.unprotected_qty} expected={expected_unprotected_qty}")

    codes = set(verdict.reason_codes)
    for rc in required_reason_codes:
        _rec(f"reason_present:{rc}", any(rc in c for c in codes), f"missing {rc} in {sorted(codes)}")
    for rc in forbidden_reason_codes:
        _rec(f"reason_absent:{rc}", all(rc not in c for c in codes), f"forbidden {rc} present in {sorted(codes)}")
    return checks


def evaluate_expectations(
    expected: Mapping[str, Any],
    observed: Mapping[str, Any],
    *,
    scenario_id: str,
) -> list[CheckpointAssertion]:
    """Executable fixture expectation evaluator (Section 18).

    Fails explicitly if any required key in `expected_results.json` is omitted from
    `observed` or disagrees with the observed runtime value.
    """
    if not expected:
        raise AssertionError(f"{scenario_id}: expected_results.json must not be empty")
    checks: list[CheckpointAssertion] = []
    missing_keys = sorted(set(expected.keys()) - set(observed.keys()))
    if missing_keys:
        raise AssertionError(
            f"{scenario_id}: unevaluated fixture expectations (missing from observed): {missing_keys}"
        )
    for key in sorted(expected.keys()):
        exp_val = expected[key]
        obs_val = observed[key]
        passed = obs_val == exp_val
        checks.append(
            CheckpointAssertion(
                name=f"{scenario_id}.expect.{key}",
                passed=passed,
                detail=f"expected={exp_val!r} observed={obs_val!r}",
            )
        )
        if not passed:
            raise AssertionError(
                f"{scenario_id}: fixture expectation '{key}' failed: expected={exp_val!r}, observed={obs_val!r}"
            )
    return checks


def assert_no_prohibited_effects(
    *,
    journal_diff: Mapping[str, Any],
    unexpected_mutation_sends: int = 0,
    unexpected_service_actions: int = 0,
    allow_journal_changes: bool = False,
) -> None:
    if not allow_journal_changes and journal_diff:
        raise AssertionError(f"Prohibited durable journal writes occurred: {journal_diff}")
    if unexpected_mutation_sends > 0:
        raise AssertionError(f"Prohibited broker mutation HTTP sends occurred: count={unexpected_mutation_sends}")
    if unexpected_service_actions > 0:
        raise AssertionError(f"Prohibited simulated systemctl actions occurred: count={unexpected_service_actions}")


def assert_snapshot_freshness_bounds(
    snapshot: BrokerSnapshot,
    *,
    max_span_ns: int = 10_000_000_000,
    max_skew_ns: int = 5_000_000_000,
) -> None:
    span = snapshot.capture_finished_mono_ns - snapshot.capture_started_mono_ns
    if span > max_span_ns:
        raise AssertionError(f"Snapshot acquisition span {span}ns exceeded ceiling {max_span_ns}ns")
    live_times = [
        b.received_mono_ns
        for b in (
            snapshot.holdings,
            snapshot.positions,
            snapshot.trade_book,
            snapshot.order_book,
            snapshot.forever_orders,
            snapshot.super_orders,
        )
        if b.received_mono_ns is not None
    ]
    if len(live_times) >= 2 and (max(live_times) - min(live_times)) > max_skew_ns:
        raise AssertionError(
            f"Live book receipt skew {max(live_times) - min(live_times)}ns exceeded ceiling {max_skew_ns}ns"
        )
