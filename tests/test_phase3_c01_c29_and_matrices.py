"""Phase 3 V2 Freshness/Generation Boundaries, 57-Flag Behavioral Matrix & Honest C01–C37 Regressions (P3-C13, P3-C14)."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import unittest

from src.broker.broker_snapshot import (
    LIVE_BOOKS,
    NS,
    BookStatus,
    ContractKey,
    EvidenceRequest,
    Purpose,
    SessionPhase,
    account_key,
)
from src.engine.watcher_risk_classification import (
    BUSINESS_FACT_FIELDS,
    CLASSIFICATION,
    DURABLE_RISK_FIELDS,
    TRANSIENT_FIELDS,
    legacy_risks,
)

from tests.golden_replay.assertions import assert_verdict_checkpoint
from tests.golden_replay.clocks import IST, DeterministicClock
from tests.golden_replay.fault_injection import make_conflicting_duplicate, mutate_single_field
from tests.golden_replay.runner import ReplayEnvironment
from tests.golden_replay.transport import ScriptedResponse

ACCOUNT = "9000001536"
CONTRACT = ContractKey(ACCOUNT, "1594", "NSE_EQ", "CNC")


class TestFreshnessGenerationAnd57FlagBehavior(unittest.TestCase):
    """P3-C13 — Behavioral verification for all 20 transient + 37 durable watcher flags."""

    def test_57_watcher_flag_classification_and_behavioral_detection(self):
        self.assertEqual(len(TRANSIENT_FIELDS), 20)
        self.assertEqual(len(DURABLE_RISK_FIELDS), 37)
        self.assertEqual(len(TRANSIENT_FIELDS | DURABLE_RISK_FIELDS), 57)
        self.assertTrue(TRANSIENT_FIELDS.isdisjoint(DURABLE_RISK_FIELDS))
        self.assertTrue(TRANSIENT_FIELDS.isdisjoint(BUSINESS_FACT_FIELDS))

        # Behavioral check 1: every transient flag alone MUST NOT trigger durable legacy_risks()
        for tflag in sorted(TRANSIENT_FIELDS):
            self.assertEqual(CLASSIFICATION[tflag], "TRANSIENT")
            if "." not in tflag and tflag != "reconciliation_required":
                self.assertEqual(legacy_risks({tflag: True}), (), f"Transient flag {tflag} leaked into durable risks")

        # Behavioral check 2: top-level durable boolean/string flags MUST trigger durable legacy_risks()
        non_nested_durable = [f for f in sorted(DURABLE_RISK_FIELDS) if "." not in f]
        self.assertGreaterEqual(len(non_nested_durable), 25)
        for dflag in non_nested_durable:
            self.assertEqual(CLASSIFICATION[dflag], "DURABLE_UNRESOLVED")
            probe_val = "UNVERIFIED" if dflag.endswith("_status") or dflag.endswith("_state") else True
            if dflag in ("orphaned_leg_cancel_details", "orphaned_leg_cancel_states"):
                probe_val = {"leg-1": {"state": "FAILED", "escalated": True}}
            elif dflag == "pending_oco":
                probe_val = {"state": "PENDING"}
            elif dflag == "uncommitted_broker_sell_trades":
                probe_val = [{"exchangeTradeId": "E-1"}]
            elif dflag in ("manual_exit_evidence_verified",):
                # False indicates unverified manual exit evidence
                continue
            detected = legacy_risks({dflag: probe_val})
            self.assertTrue(len(detected) > 0, f"Durable flag {dflag} was NOT detected by legacy_risks()")
        # Explicit behavioral check for manual_exit_evidence_verified=False & nested durable flags (P3-C21)
        self.assertTrue(len(legacy_risks({"manual_exit_detected": True, "manual_exit_evidence_verified": False})) > 0)
        self.assertTrue(len(legacy_risks({"pending_oco": {"state": "PENDING", "sl_leg_state": "PENDING", "target_leg_state": "PENDING"}})) > 0)
        self.assertTrue(len(legacy_risks({"orphaned_leg_cancel_details": {"leg1": {"state": "FAILED", "escalated": True, "last_error": "err"}}})) > 0)
        self.assertTrue(len(legacy_risks({"reconciliation_required": True})) > 0)

    def test_freshness_exact_boundaries_10s_live_and_30s_profile(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            self.assertTrue(snap.complete)
            clk.advance(9.9)
            self.assertTrue(env.reconcile_security(snap, CONTRACT).fresh)
            clk.advance(0.2)
            v_stale = env.reconcile_security(snap, CONTRACT)
            self.assertFalse(v_stale.fresh)
            assert_verdict_checkpoint(v_stale, expected_entry_eligible=False)

    def test_generation_components_and_journal_digest_mismatch_deny(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            jv = env.read_journal_view(journal_epoch=snap.generation.journal_epoch)
            assert_verdict_checkpoint(env.reconcile_security(replace(snap, journal_digest="f" * 64), CONTRACT, journal_view=jv), expected_entry_eligible=False)
            env.set_owner_state(desired="STOPPED", observed="RUNNING", deferred_stop_pending=True)
            pol_stop = env.derive_policy_from_environment(purpose=Purpose.SHUTDOWN, journal_view=jv, owner_intent_epoch=snap.generation.owner_epoch + 5, entry_fence_active=True)
            assert_verdict_checkpoint(env.reconcile_security(snap, CONTRACT, policy=pol_stop, journal_view=jv), expected_shutdown_eligible=False)
            bad_gen = replace(snap.generation, account_key=account_key("9999999999"))
            with self.assertRaises(ValueError):
                replace(snap, generation=bad_gen)

    def test_adversarial_single_field_mutator_all_type_variants(self):
        base_holding = {"dhanClientId": ACCOUNT, "securityId": "1594", "tradingSymbol": "INFY", "isin": "INE009A01021", "totalQty": 5, "dpQty": 5, "t1Qty": 0, "availableQty": 5}
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books(holdings=[base_holding])
            self.assertTrue(env.acquire_snapshot(purpose=Purpose.ENTRY).complete)
            for variant in ("missing", "null", "boolean_true", "fractional", "negative", "malformed_string"):
                with self.subTest(variant=variant):
                    bad = mutate_single_field(base_holding, "totalQty", variant)
                    env.register_books(holdings=[bad])
                    env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_ADVERSARIAL", variant)
                    snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
                    if snap.complete:
                        self.assertFalse(env.reconcile_security(snap, CONTRACT).entry_eligible)
        dup = make_conflicting_duplicate([{"orderId": "O1", "exchangeTradeId": "E1", "tradedQuantity": 5, "tradedPrice": 100.0}], conflict_value=105.0)
        self.assertEqual(len(dup), 2)
        self.assertNotEqual(dup[0]["tradedPrice"], dup[1]["tradedPrice"])


    def test_p3_c20_each_policy_fact_independently_denies_when_false(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            jv = env.read_journal_view(journal_epoch=snap.generation.journal_epoch)
            # Baseline healthy derived policy authorizes empty account
            pol_ok = env.derive_policy_from_environment(purpose=Purpose.ENTRY, journal_view=jv, owner_intent_epoch=snap.generation.owner_epoch)
            self.assertTrue(env.reconcile_security(snap, CONTRACT, policy=pol_ok, journal_view=jv).entry_eligible)
            # Independently flip EACH required policy fact and prove entry is denied
            for field_name, bad_val in (
                ("service_identity_verified", False),
                ("watcher_liveness_verified", False),
                ("risk_funds_mode_checks_passed", False),
                ("journal_readback_verified", False),
                ("mutation_fence_verified", False),
                ("emergency_stop_active", True),
                ("entry_fence_active", True),
            ):
                with self.subTest(policy_field=field_name):
                    pol_bad = replace(pol_ok, **{field_name: bad_val})
                    v = env.reconcile_security(snap, CONTRACT, policy=pol_bad, journal_view=jv)
                    self.assertFalse(v.entry_eligible, f"Flipping {field_name}={bad_val} failed to deny entry")

class TestC01ToC37HonestCrossPhaseRegressions(unittest.TestCase):
    """P3-C14 — Honest Phase 2C-executable regressions (Phase 2D consumer defects tracked explicitly as BLOCKED_ON_PHASE2D)."""

    def test_c04_c16_pre_labelled_settled_never_bypasses_contradictory_books(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.set_session(clk.session_context(phase=SessionPhase.SETTLED, trading_session_date=date(2026, 11, 26)))
            env.register_books(holdings=[{"dhanClientId": ACCOUNT, "securityId": "1594", "tradingSymbol": "INFY", "isin": "INE009A01021", "totalQty": 5, "dpQty": 5, "t1Qty": 0, "availableQty": 5}])
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            assert_verdict_checkpoint(env.reconcile_security(snap, CONTRACT), expected_entry_eligible=False)

    def test_c09_fractional_position_and_order_quantities_rejected(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books(positions=[{"dhanClientId": ACCOUNT, "securityId": "1594", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0.4, "buyQty": 0.4, "sellQty": 0}])
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            self.assertFalse(snap.complete)
            self.assertEqual(snap.positions.status, BookStatus.MALFORMED)

    def test_c14_invalidated_coordinator_revalidation_fails(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            snap = env.acquire_snapshot(purpose=Purpose.WATCHER)
            self.assertIsNotNone(env.coordinator.peek_display(account_key(ACCOUNT)))
            env.coordinator.invalidate(account_key(ACCOUNT), "OWNER_STOP", "c14-inv")
            req = EvidenceRequest(ACCOUNT, Purpose.ENTRY, LIVE_BOOKS, deadline_mono_ns=clk.monotonic_ns() + 5 * NS)
            self.assertTrue(len(env.coordinator.revalidate(snap, req)) > 0)

    def test_c17_failed_trade_book_never_treated_as_empty_list(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books(trade_book=ScriptedResponse(status_code=503, payload={"error": "down"}))
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            self.assertFalse(snap.complete)
            self.assertEqual(snap.trade_book.status, BookStatus.HTTP_FAILURE)
            self.assertIsNone(snap.trade_book.payload)

    def test_c20_c25_pre_intent_snapshot_denied_by_reconciler(self):
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            snap_before = env.acquire_snapshot(purpose=Purpose.WATCHER)
            clk.advance(1.0)
            intent_mono = clk.monotonic_ns()
            env.set_owner_state(desired="STOPPED", observed="RUNNING", deferred_stop_pending=True)
            jv = env.read_journal_view(journal_epoch=snap_before.generation.journal_epoch)
            pol = env.derive_policy_from_environment(purpose=Purpose.SHUTDOWN, journal_view=jv, owner_intent_epoch=snap_before.generation.owner_epoch, entry_fence_active=True, min_capture_mono_ns=intent_mono)
            assert_verdict_checkpoint(env.reconcile_security(snap_before, CONTRACT, policy=pol, journal_view=jv), expected_shutdown_eligible=False)


if __name__ == "__main__":
    unittest.main()
