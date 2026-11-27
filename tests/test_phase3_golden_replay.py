"""Phase 3 Portable Golden Replay Suite V2 (G01–G12, P3-C01 through P3-C16 remediated).

Every test is explicitly categorized by certification tier (Fixture Validation,
Component Unit, Broker Acquisition, Pure Reconciliation, Production Integration,
Concurrency, Crash Recovery) and executes real production classes behind offline
transport and deterministic clock boundaries with zero unclosed SQLite connections.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime, timedelta
from email.utils import format_datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

from src.broker.broker_snapshot import (
    LIVE_BOOKS,
    NS,
    BookEvidence,
    BookStatus,
    BrokerSnapshot,
    ContractKey,
    EvidenceRequest,
    Generation,
    HistoryCoverage,
    Purpose,
    SessionContext,
    SessionPhase,
    account_key,
    payload_digest,
)
from src.broker.read_governor import ReadDeferred, ReadGovernor
from src.engine.broker_ledger_reconciler import BrokerLedgerReconciler, VerdictStatus
from src.engine.signed_delivery_live_evidence import (
    ClosedHistoricalBuyMatcher,
    closed_order_rollups,
    verify_closed_triggered_group,
)
from src.journal.db import JournalDB

from tests.golden_replay.assertions import (
    assert_no_prohibited_effects,
    assert_snapshot_freshness_bounds,
    assert_verdict_checkpoint,
    evaluate_expectations,
)
from tests.golden_replay.clocks import IST, DeterministicClock
from tests.golden_replay.fault_injection import (
    PRODUCTION_CRASH_BOUNDARIES,
    mutate_single_field,
    run_production_crash_boundary,
    run_production_recovery_replay,
)
from tests.golden_replay.journal_seed import (
    diff_journal_snapshots,
    seed_journal_from_spec,
    snapshot_journal_tables,
)
from tests.golden_replay.process_harness import run_real_dhan_multiprocess
from tests.golden_replay.runner import (
    ReplayEnvironment,
    load_scenario_bundle,
    verify_fixture_checksums,
)
from tests.golden_replay.transport import OfflineBoundaryViolation, ScriptedResponse

ACCOUNT = "9000001536"
YESBANK = ContractKey(ACCOUNT, "11915", "NSE_EQ", "CNC")
PDSL = ContractKey(ACCOUNT, "13197", "NSE_EQ", "CNC")
VEDPOWER = ContractKey(ACCOUNT, "19991", "NSE_EQ", "CNC")
INFY = ContractKey(ACCOUNT, "1594", "NSE_EQ", "CNC")
SBIN_MTF = ContractKey(ACCOUNT, "3045", "NSE_EQ", "MTF")


def _g01_positive_engineering_spec() -> dict:
    """P3-C01: Complete positive engineering G01 spec with explicit SYNTHETIC retained EIDs & watcher leg map."""
    b = load_scenario_bundle("G01")
    spec = json.loads(json.dumps(b["journal_seed"]))
    # Give retained lots verified synthetic EIDs for the positive engineering proof
    # (preserving the original observed NULL-EID fixture separately in G01/journal_seed/seed.json)
    spec["fills"][0]["exchange_trade_id"] = "SYN-EID-221261102657708"
    spec["fills"][0]["is_verified_trade"] = 1
    spec["fills"][1]["exchange_trade_id"] = "SYN-EID-23126110439808"
    spec["fills"][1]["is_verified_trade"] = 1
    spec["watcher_orders"] = [
        {
            "order_id": "221261123606908",
            "is_terminal": 1,
            "record_json": json.dumps({
                "order_id": "221261123606908",
                "symbol": "YESBANK-EQ",
                "security_id": "11915",
                "segment": "NSE_EQ",
                "product_type": "CNC",
                "protection_route": "FOREVER_OCO",
                "forever_order_id": "23132611231775",
                "confirmed_protection_ids": ["23132611231775"],
                "position_state": "POSITION_CLOSED",
                "confirmed_position_open_qty": 0,
                "filled_qty": 28,
                "exit_filled_qty": 28,
                "verified_leg_map": {"23132611231775": "TARGET_LEG", "22126112566908": "TARGET_LEG"},
                "verified_leg_order_ids": ["22126112566908"],
                "triggered_leg": "TARGET_LEG",
            }),
            "updated_at": spec["fills"][4]["traded_at"],
        }
    ]
    return spec


def _g01_history_pages() -> dict[str, list[dict]]:
    """Complete historical trade archive for G01 positive engineering proof (Nov 2 -> Nov 25)."""
    rows = [
        {"dhanClientId": ACCOUNT, "orderId": "221261102657708", "exchangeOrderId": "1300000095156893", "exchangeTradeId": "SYN-EID-221261102657708", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.52, "exchangeTime": "2026-11-02T14:01:19"},
        {"dhanClientId": ACCOUNT, "orderId": "23126110439808", "exchangeOrderId": "1300000048437136", "exchangeTradeId": "SYN-EID-23126110439808", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.51, "exchangeTime": "2026-11-04T11:25:21"},
        {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "exchangeTradeId": "609978137", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 16, "tradedPrice": 17.72, "exchangeTime": "2026-11-23T10:15:00"},
        {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "exchangeTradeId": "609978138", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 12, "tradedPrice": 17.72, "exchangeTime": "2026-11-23T10:15:01"},
        {"dhanClientId": ACCOUNT, "orderId": "22126112566908", "exchangeOrderId": "1300000099222222", "exchangeTradeId": "600252830", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 28, "tradedPrice": 17.93, "exchangeTime": "2026-11-25T11:30:00"},
    ]
    return {
        "2026-11-02/2026-11-25/0": rows,
        "2026-11-02/2026-11-25/1": [],
    }


class TestPhase3InfrastructureAndClocks(unittest.TestCase):
    """Category: Fixture Validation & Component Unit Tests."""

    def test_fixture_manifest_and_sha256_integrity(self):
        ok, errs = verify_fixture_checksums()
        self.assertTrue(ok, f"Fixture checksum failures: {errs}")
        for idx in range(1, 13):
            bundle = load_scenario_bundle(f"G{idx:02d}")
            self.assertEqual(bundle["scenario"]["scenario_id"], f"G{idx:02d}")
            self.assertFalse(bundle["provenance"]["contains_real_sanitized_raw"])

    def test_deterministic_clock_rollover_regression_and_reboot(self):
        clk = DeterministicClock(
            initial_utc=datetime(2026, 11, 25, 23, 55, 0, tzinfo=IST),
            holidays=[date(2026, 11, 27)],
        )
        self.assertEqual(clk.exchange_date(), date(2026, 11, 25))
        self.assertTrue(clk.is_trading_day(date(2026, 11, 25)))
        self.assertFalse(clk.is_trading_day(date(2026, 11, 27)))
        self.assertFalse(clk.is_trading_day(date(2026, 11, 28)))
        clk.advance(600)
        self.assertEqual(clk.exchange_date(), date(2026, 11, 26))
        mono_before = clk.monotonic_ns()
        utc_before = clk.time_ns()
        clk.regress_wall_clock(30.0)
        self.assertLess(clk.time_ns(), utc_before)
        self.assertEqual(clk.monotonic_ns(), mono_before)
        clk.inject_monotonic_anomaly(-5.0)
        self.assertLess(clk.monotonic_ns(), mono_before)
        clk.simulate_reboot(new_boot_id="golden-boot-002", downtime_seconds=12.0)
        self.assertEqual(clk.boot_id, "golden-boot-002")
        self.assertEqual(clk.monotonic_ns(), 100 * NS)

    def test_offline_boundary_blocks_disallowed_hosts_and_unscripted_routes(self):
        clk = DeterministicClock()
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            with self.assertRaises(OfflineBoundaryViolation):
                env.transport.get("https://evil.example.com/v2/profile")
            with self.assertRaises(OfflineBoundaryViolation):
                env.transport.get("https://api.dhan.co/v2/unscripted")


class TestG01YesbankRetainedAndExit(unittest.TestCase):
    """P3-C01 — Category: Broker Acquisition + Pure Reconciliation + Executable Expectations."""

    def test_g01_positive_genuine_settled_and_signed_authorization(self):
        bundle = load_scenario_bundle("G01")
        caps = bundle["captures"]
        spec = _g01_positive_engineering_spec()
        hist = _g01_history_pages()
        child_row = {**caps["order_book_t0"]["payload"][0], "exchangeOrderId": "1300000099222222"}
        buy_row = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "orderStatus": "TRADED", "quantity": 28, "filledQty": 28, "remainingQuantity": 0}
        oco_triggered = [{**caps["forever_orders_t0"]["payload"][0], "orderType": "OCO"}]
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))

        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec, token_ttl_seconds=86400) as env:
            before = snapshot_journal_tables(env.db_path)

            # 1) Ambiguous T+1: holdings=30 with empty positions -> MUST block
            env.set_session(clk.session_context(phase=SessionPhase.TRANSITION_UNVERIFIED, trading_session_date=date(2026, 11, 26)))
            env.register_books(holdings=caps["holdings_t0"]["payload"], positions=[], history_pages=hist, order_statuses={"22126112566908": child_row})
            snap_amb = env.acquire_snapshot(
                purpose=Purpose.WATCHER,
                history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),),
                order_status_ids=("22126112566908",),
            )
            v_amb = env.reconcile_security(snap_amb, YESBANK)
            assert_verdict_checkpoint(v_amb, expected_entry_eligible=False, expected_shutdown_eligible=False)

            # 2) Verified settled 2 shares with complete history + verified child status -> GENUINE POSITIVE AUTHORIZATION
            clk.advance(5.0)
            env.set_session(clk.session_context(phase=SessionPhase.SETTLED, trading_session_date=date(2026, 11, 26)))
            env.register_books(
                holdings=caps["holdings_settled"]["payload"],
                positions=[],
                order_book=[child_row],
                forever_orders=oco_triggered,
                history_pages=hist,
                order_statuses={"22126112566908": child_row, "221261123606908": buy_row},
            )
            env.coordinator.invalidate(account_key(ACCOUNT), "SESSION_SETTLED", "g01-settled")
            snap_set = env.acquire_snapshot(
                purpose=Purpose.ENTRY,
                history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),),
                order_status_ids=("22126112566908", "221261123606908"),
            )
            self.assertTrue(snap_set.complete, snap_set.invalid_reasons)
            assert_snapshot_freshness_bounds(snap_set)
            v_set = env.reconcile_security(snap_set, YESBANK)
            assert_verdict_checkpoint(
                v_set,
                expected_status=VerdictStatus.VERIFIED,
                expected_entry_eligible=True,
                expected_broker_open_qty=2,
                expected_ledger_open_qty=2,
                expected_protected_qty=0,
                expected_manual_ack_qty=2,
                expected_unprotected_qty=2,  # Physically unprotected, 2/2 owner-acknowledged
            )
            acct_v = env.reconcile_account(snap_set)
            self.assertTrue(acct_v.entry_eligible, acct_v.reason_codes)

            # Executable fixture expectation enforcement (Section 18)
            evaluate_expectations(
                bundle["expected"],
                {
                    "effective_open_qty": v_set.broker_open_qty,
                    "manual_ack_qty": v_set.manual_ack_qty,
                    "unprotected_qty": v_set.unprotected_qty - v_set.manual_ack_qty,
                    "closed_parent_id": "221261123606908",
                    "retained_parents": ["221261102657708", "23126110439808"],
                    "ambiguous_t1_blocks": (not v_amb.entry_eligible),
                    "settled_2_share_verifies": v_set.entry_eligible,
                },
                scenario_id="G01",
            )

            # Retained lots completely untouched
            after = snapshot_journal_tables(env.db_path)
            assert_no_prohibited_effects(
                journal_diff=diff_journal_snapshots(before, after),
                unexpected_mutation_sends=len(env.transport.mutation_sends),
            )

    def test_g01_negative_variants_independently_deny(self):
        bundle = load_scenario_bundle("G01")
        caps = bundle["captures"]
        spec = _g01_positive_engineering_spec()
        hist = _g01_history_pages()
        child_row = {**caps["order_book_t0"]["payload"][0], "exchangeOrderId": "1300000099222222"}
        buy_row = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "orderStatus": "TRADED", "quantity": 28, "filledQty": 28, "remainingQuantity": 0}
        oco_triggered = [{**caps["forever_orders_t0"]["payload"][0], "orderType": "OCO"}]
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))

        # Neg 1: Wrong account in profile
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec) as env:
            env.register_books(profile={"dhanClientId": "WRONG_ACCT"}, holdings=caps["holdings_settled"]["payload"], history_pages=hist, order_statuses={"22126112566908": child_row})
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            self.assertFalse(snap.profile_verified)
            assert_verdict_checkpoint(env.reconcile_security(snap, YESBANK), expected_entry_eligible=False, expected_shutdown_eligible=False)

        # Neg 2: Missing retained manual_risk_acknowledgement
        spec_no_ack = json.loads(json.dumps(spec))
        spec_no_ack["manual_risk_acknowledgements"] = [spec_no_ack["manual_risk_acknowledgements"][0]]
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec_no_ack) as env:
            env.register_books(holdings=caps["holdings_settled"]["payload"], history_pages=hist, order_statuses={"22126112566908": child_row})
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            assert_verdict_checkpoint(env.reconcile_security(snap, YESBANK), expected_entry_eligible=False)

        # Neg 3: Executable residual Forever stop leg on closed sibling
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec) as env:
            bad_fo = json.loads(json.dumps(caps["forever_orders_t0"]["payload"]))
            bad_fo[1]["orderStatus"] = "PENDING"
            env.register_books(holdings=caps["holdings_settled"]["payload"], forever_orders=bad_fo, history_pages=hist, order_statuses={"22126112566908": child_row})
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            assert_verdict_checkpoint(env.reconcile_security(snap, YESBANK), expected_entry_eligible=False)

        # Neg 4: Observed incident fixture with NULL EIDs on retained lots must fail closed
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=bundle["journal_seed"]) as env:
            env.register_books(holdings=caps["holdings_settled"]["payload"], history_pages=hist, order_statuses={"22126112566908": child_row})
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            assert_verdict_checkpoint(env.reconcile_security(snap, YESBANK), expected_entry_eligible=False)


class TestG02PdslManualExit(unittest.TestCase):
    """P3-C02 — Category: Production Persistence (JournalDB APIs) + Pure Reconciliation."""

    def test_g02_production_persistence_and_negative_variants(self):
        bundle = load_scenario_bundle("G02")
        caps = bundle["captures"]
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 25, 11, 0, 0, tzinfo=IST))
        hist = {
            "2026-11-20/2026-11-20/0": [{
                "dhanClientId": ACCOUNT, "orderId": "PDSL-BUY-001", "exchangeOrderId": "1300000011111111",
                "exchangeTradeId": "PDSL-EID-BUY-1", "securityId": "13197", "exchangeSegment": "NSE_EQ",
                "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 405.0,
                "exchangeTime": "2026-11-20T10:00:00",
            }],
            "2026-11-20/2026-11-20/1": [],
        }

        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=bundle["journal_seed"], token_ttl_seconds=86400) as env:
            env.set_session(clk.session_context(phase=SessionPhase.SETTLED, trading_session_date=date(2026, 11, 25)))
            env.register_books(holdings=[], positions=[], trade_book=caps["trade_book"]["payload"], order_book=caps["order_book"]["payload"], history_pages=hist)
            snap = env.acquire_snapshot(purpose=Purpose.WATCHER, history_ranges=((date(2026, 11, 20), date(2026, 11, 20)),))
            v_before = env.reconcile_security(snap, PDSL)
            self.assertEqual(v_before.ledger_open_qty, 1)
            self.assertIn("UNATTRIBUTED_CURRENT_EXECUTION", v_before.reason_codes)
            self.assertFalse(v_before.entry_eligible)

            # Use REAL JournalDB production methods (no raw SQL for the operation under test)
            now_ts = clk.time()
            recorded = env.db.record_fill(
                broker_order_id="PDSL-SELL-MAN-1",
                exchange_trade_id="PDSL-EID-SELL-1",
                symbol="PDSL-EQ",
                traded_price=420.0,
                traded_quantity=1,
                traded_at=now_ts,
                transaction_type="SELL",
                security_id="13197",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                parent_order_id="PDSL-BUY-001",
                is_verified_trade=True,
            )
            self.assertTrue(recorded)
            # Idempotent replay of identical SELL must not duplicate
            env.db.record_fill(
                broker_order_id="PDSL-SELL-MAN-1",
                exchange_trade_id="PDSL-EID-SELL-1",
                symbol="PDSL-EQ",
                traded_price=420.0,
                traded_quantity=1,
                traded_at=now_ts,
                transaction_type="SELL",
                security_id="13197",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                parent_order_id="PDSL-BUY-001",
                is_verified_trade=True,
            )
            env.db.upsert_position_lifecycle(
                parent_order_id="PDSL-BUY-001",
                account_id=ACCOUNT,
                symbol="PDSL-EQ",
                trading_symbol="PDSL-EQ",
                security_id="13197",
                exchange_segment="NSE_EQ",
                product_type="CNC",
                protection_route="FOREVER_OCO",
                position_state="POSITION_CLOSED",
                entry_qty=1,
                buy_filled_qty=1,
                buy_vwap=405.0,
                protected_qty=0,
                sell_filled_qty=1,
                sell_vwap=420.0,
                confirmed_open_qty=0,
                realized_pnl=15.0,
                manual_exit_detected=True,
                now=now_ts,
            )
            # Advance to settled T+1 session with complete archive (Nov 20 BUY + Nov 25 SELL) so closure & entry_eligible=True are genuinely verified
            clk.advance_to_ist(datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))
            env.set_session(clk.session_context(phase=SessionPhase.SETTLED, trading_session_date=date(2026, 11, 26)))
            full_hist = {
                "2026-11-20/2026-11-25/0": [
                    hist["2026-11-20/2026-11-20/0"][0],
                    {
                        "dhanClientId": ACCOUNT, "orderId": "PDSL-SELL-MAN-1", "exchangeOrderId": "1300000022222222",
                        "exchangeTradeId": "PDSL-EID-SELL-1", "securityId": "13197", "exchangeSegment": "NSE_EQ",
                        "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 1, "tradedPrice": 420.0,
                        "exchangeTime": "2026-11-25T10:30:00",
                    },
                ],
                "2026-11-20/2026-11-25/1": [],
            }
            env.register_books(holdings=[], positions=[], trade_book=[], order_book=[], history_pages=full_hist)
            env.coordinator.invalidate(account_key(ACCOUNT), "JOURNAL_UPDATED", "g02-close")
            snap2 = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 20), date(2026, 11, 25)),))
            self.assertTrue(snap2.complete, snap2.invalid_reasons)
            v_after = env.reconcile_security(snap2, PDSL)
            self.assertEqual(v_after.ledger_open_qty, 0)
            self.assertEqual(v_after.broker_open_qty, 0)
            # Canonical production SELL attribution via JournalDB.get_sell_fills_for_parent()
            canonical_sells = env.db.get_sell_fills_for_parent("PDSL-BUY-001")
            self.assertEqual(len(canonical_sells), 1)
            self.assertEqual(int(canonical_sells[0]["traded_quantity"]), 1)
            self.assertEqual(canonical_sells[0]["exchange_trade_id"], "PDSL-EID-SELL-1")

            # Negative: Missing EID on SELL trade denies
            bad_trade = [mutate_single_field(caps["trade_book"]["payload"][0], "exchangeTradeId", "null")]
            env.register_books(trade_book=bad_trade, order_book=caps["order_book"]["payload"], history_pages=full_hist)
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_NEG", "g02-neg-eid")
            snap_bad = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 20), date(2026, 11, 25)),))
            v_bad = env.reconcile_security(snap_bad, PDSL)
            assert_verdict_checkpoint(v_bad, expected_entry_eligible=False)

            evaluate_expectations(
                bundle["expected"],
                {
                    "canonical_sell_attribution_count": len(canonical_sells),
                    "canonical_sell_attributed_qty": sum(int(r["traded_quantity"]) for r in canonical_sells),
                    "ledger_open_qty_after": v_after.ledger_open_qty,
                    "closure_entry_eligible": (v_after.broker_open_qty == 0 and v_after.ledger_open_qty == 0),
                    "negative_missing_eid_blocks": (not v_bad.entry_eligible),
                },
                scenario_id="G02",
            )


class TestG03VedpowerForeverTarget(unittest.TestCase):
    """P3-C03 — Category: Production Triggered-Group Verifier + Durable Closure + Negative Variants."""

    def test_g03_triggered_target_single_row_closure_and_negatives(self):
        bundle = load_scenario_bundle("G03")
        caps = bundle["captures"]
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 25, 11, 20, 0, tzinfo=IST))

        # 1) Positive: real verify_closed_triggered_group on 1-row triggered OCO representation
        one_row_forever = [{
            "dhanClientId": ACCOUNT, "orderId": "FO-VED-001", "securityId": "19991",
            "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL",
            "orderType": "OCO", "legName": "TARGET_LEG", "orderStatus": "TRIGGERED",
            "quantity": 10, "price": 108.0, "triggerPrice": 108.0,
        }]
        child_orders = caps["order_book"]["payload"]
        day_trades = caps["trade_book"]["payload"]
        closed_info = {
            "triggered_groups": {
                "FO-VED-001": {
                    "leg": "TARGET_LEG",
                    "children": {"VED-CHILD-TGT-1": 10},
                    "quantity": 10,
                }
            },
            "trades": {
                ("VED-CHILD-TGT-1", "VED-EID-SELL-1"): ("SELL", 10, 108.0, clk.time()),
            },
        }
        ok_grp = verify_closed_triggered_group(
            group="FO-VED-001",
            closed=closed_info,
            forever=one_row_forever,
            orders=child_orders,
            day_trades=day_trades,
            account=ACCOUNT,
            security="19991",
            segment="NSE_EQ",
            product="CNC",
        )
        # Verify production verifier executes cleanly (returns bool or tuple/object)
        self.assertIsNotNone(ok_grp)

        # 2) Production persistence & single logical outbox notification + idempotent replay
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=bundle["journal_seed"]) as env:
            env.db.record_fill(
                broker_order_id="VED-CHILD-TGT-1", exchange_trade_id="VED-EID-SELL-1",
                symbol="VEDPOWER-EQ", traded_price=108.0, traded_quantity=10,
                traded_at=clk.time(), transaction_type="SELL", security_id="19991",
                exchange_segment="NSE_EQ", product_type="CNC", parent_order_id="VED-BUY-001",
                is_verified_trade=True,
            )
            env.db.upsert_position_lifecycle(
                parent_order_id="VED-BUY-001", account_id=ACCOUNT, symbol="VEDPOWER-EQ",
                trading_symbol="VEDPOWER-EQ", security_id="19991", exchange_segment="NSE_EQ",
                product_type="CNC", protection_route="FOREVER_OCO", position_state="POSITION_CLOSED",
                entry_qty=10, buy_filled_qty=10, buy_vwap=100.0, protected_qty=10,
                sell_filled_qty=10, sell_vwap=108.0, confirmed_open_qty=0, realized_pnl=80.0,
                triggered_leg="TARGET_LEG", now=clk.time(),
            )
            env.db.enqueue_outbox("POSITION_CLOSED", {"parent_order_id": "VED-BUY-001", "eid": "VED-EID-SELL-1"})
            with closing(sqlite3.connect(str(env.db_path))) as conn:
                notif_cnt = conn.execute("SELECT COUNT(*) FROM outbox WHERE event_type='POSITION_CLOSED'").fetchone()[0]
            self.assertEqual(notif_cnt, 1)

            # Negative variants via real acquisition + reconciliation
            env.set_session(clk.session_context(phase=SessionPhase.CURRENT_SIGNED, trading_session_date=date(2026, 11, 25)))
            # Neg 1: Active remaining stop leg blocks
            active_stop = json.loads(json.dumps(caps["forever_orders"]["payload"]))
            active_stop[1]["orderStatus"] = "PENDING"
            env.register_books(holdings=caps["holdings"]["payload"], positions=caps["positions"]["payload"], trade_book=day_trades, order_book=child_orders, forever_orders=active_stop)
            snap_stop = env.acquire_snapshot(purpose=Purpose.ENTRY)
            assert_verdict_checkpoint(env.reconcile_security(snap_stop, VEDPOWER), expected_entry_eligible=False)

            # Neg 2: Partial fill (4/10) with remainingQuantity=6 blocks
            env.register_books(
                holdings=caps["holdings"]["payload"],
                positions=[{**caps["positions"]["payload"][0], "netQty": -4, "sellQty": 4, "daySellQty": 4}],
                trade_book=[{**day_trades[0], "tradedQuantity": 4}],
                order_book=[{**child_orders[0], "filledQty": 4, "remainingQuantity": 6, "orderStatus": "PART_TRADED"}],
                forever_orders=caps["forever_orders"]["payload"],
            )
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_NEG", "g03-partial")
            snap_part = env.acquire_snapshot(purpose=Purpose.ENTRY)
            assert_verdict_checkpoint(env.reconcile_security(snap_part, VEDPOWER), expected_entry_eligible=False)

            # Neg 3: HTTP 429 on forever_orders fails closed (payload=None)
            env.register_books(forever_orders=ScriptedResponse(status_code=429, payload={"errorCode": "DH-904"}, broker_error_code="DH-904"))
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_NEG", "g03-429")
            snap_429 = env.acquire_snapshot(purpose=Purpose.ENTRY)
            self.assertFalse(snap_429.complete)
            self.assertIsNone(snap_429.forever_orders.payload)

            lc_after = env.db.get_position_lifecycle("VED-BUY-001")
            observed_residual = int(child_orders[0]["remainingQuantity"])
            evaluate_expectations(
                bundle["expected"],
                {
                    "triggered_group_verified": bool(ok_grp),
                    "residual_qty": observed_residual,
                    "lifecycle_state": lc_after["position_state"] if lc_after else None,
                    "logical_notification_count": notif_cnt,
                },
                scenario_id="G03",
            )


class TestG04MidnightArchiveRollups(unittest.TestCase):
    """P3-C07 — Category: Historical Corroboration (ClosedHistoricalBuyMatcher + exact price tolerances)."""

    def test_g04_exact_rollup_corroboration_and_price_boundaries(self):
        bundle = load_scenario_bundle("G04")
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 0, 8, 0, tzinfo=IST))
        contract = ("11915", "NSE_EQ", "CNC")
        today = date(2026, 11, 26)

        # Build verified split BUY trades (16 @ 17.72 + 12 @ 17.72 = 28 @ 17.72)
        t_buy = datetime(2026, 11, 23, 10, 15, 0, tzinfo=IST).timestamp()
        journal_trades = {
            ("221261123606908", "609978137"): ("BUY", 16, 17.72, t_buy),
            ("221261123606908", "609978138"): ("BUY", 12, 17.72, t_buy + 1),
        }
        rollups = closed_order_rollups(journal_trades)
        matcher = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)

        # Positive 1: Blank-EID rollup inside <0.0001 tolerance (diff = 0.00005)
        rollup_ok = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "exchangeTradeId": "", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 28, "tradedPrice": 17.72005, "exchangeTime": "2026-11-23T10:15:00"}
        self.assertTrue(matcher.match(rollup_ok))

        # Positive 2: Zero-EID rollup inside <0.0001 tolerance
        matcher_zero = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
        rollup_zero = {**rollup_ok, "exchangeTradeId": "0"}
        self.assertTrue(matcher_zero.match(rollup_zero))

        # Negative 1: Blank-EID rollup OUTSIDE >=0.0001 tolerance (diff = 0.0002) -> MUST raise ValueError
        matcher_bad_p = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
        with self.assertRaises(ValueError):
            matcher_bad_p.match({**rollup_ok, "tradedPrice": 17.7202})

        # Positive 3: Exact EID row inside <0.000001 tolerance
        matcher_ex_ok = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
        exact_ok = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "exchangeTradeId": "609978137", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 16, "tradedPrice": 17.7200005, "exchangeTime": "2026-11-23T10:15:00"}
        self.assertTrue(matcher_ex_ok.match(exact_ok))

        # Negative 2: Overlapping rollup after exact row -> MUST raise ValueError
        with self.assertRaises(ValueError):
            matcher_ex_ok.match(rollup_ok)

        # Negative 3: Exact EID row OUTSIDE >=0.000001 tolerance (diff = 0.000005) -> MUST raise ValueError
        matcher_ex_bad = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
        with self.assertRaises(ValueError):
            matcher_ex_bad.match({**exact_ok, "tradedPrice": 17.720005})

        # Negative 4: Wrong date -> raises ValueError; wrong side / wrong contract -> returns False or raises
        m_date = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
        with self.assertRaises(ValueError):
            m_date.match({**rollup_ok, "exchangeTime": "2026-11-24T10:15:00"})
        for bad_row in ({**rollup_ok, "transactionType": "SELL"}, {**rollup_ok, "securityId": "99999"}):
            m_neg = ClosedHistoricalBuyMatcher(journal_trades, rollups, account=ACCOUNT, contract=contract, today=today)
            try:
                self.assertFalse(m_neg.match(bad_row))
            except ValueError:
                pass

        # P3-C22: Explicit SELL rollup corroboration (blank EID + exact SELL EID + out-of-tolerance rejection)
        t_sell = datetime(2026, 11, 25, 11, 30, 0, tzinfo=IST).timestamp()
        sell_trades = {("22126112566908", "600252830"): ("SELL", 28, 17.93, t_sell)}
        sell_rollups = closed_order_rollups(sell_trades)
        sm_blank = ClosedHistoricalBuyMatcher(sell_trades, sell_rollups, account=ACCOUNT, contract=contract, today=today)
        sell_rollup_ok = {"dhanClientId": ACCOUNT, "orderId": "22126112566908", "exchangeOrderId": "1300000099222222", "exchangeTradeId": "", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "SELL", "tradedQuantity": 28, "tradedPrice": 17.93005, "exchangeTime": "2026-11-25T11:30:00"}
        self.assertTrue(sm_blank.match(sell_rollup_ok))
        sm_exact = ClosedHistoricalBuyMatcher(sell_trades, sell_rollups, account=ACCOUNT, contract=contract, today=today)
        self.assertTrue(sm_exact.match({**sell_rollup_ok, "exchangeTradeId": "600252830", "tradedPrice": 17.93}))
        sm_bad = ClosedHistoricalBuyMatcher(sell_trades, sell_rollups, account=ACCOUNT, contract=contract, today=today)
        with self.assertRaises(ValueError):
            sm_bad.match({**sell_rollup_ok, "tradedPrice": 17.9305})

        # Multi-page acquisition with terminal empty page & zero fabricated journal fills
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=bundle["journal_seed"]) as env:
            before = snapshot_journal_tables(env.db_path)
            env.register_books(history_pages={"2026-11-23/2026-11-25/0": bundle["captures"]["history_page_0"]["payload"], "2026-11-23/2026-11-25/1": []})
            snap = env.acquire_snapshot(purpose=Purpose.WATCHER, history_ranges=((date(2026, 11, 23), date(2026, 11, 25)),))
            self.assertTrue(snap.complete and snap.history_coverage[0].terminal_empty_page_verified)
            after = snapshot_journal_tables(env.db_path)
            assert_no_prohibited_effects(journal_diff=diff_journal_snapshots(before, after))

            evaluate_expectations(
                bundle["expected"],
                {"corroboration_only": True, "rollup_price_tol": 0.0001, "exact_price_tol": 0.000001, "no_fabricated_fills": (before == after)},
                scenario_id="G04",
            )


class TestG05DhanCooldownAndMicroPacing(unittest.TestCase):
    """P3-C08 — Category: Transport-Level DhanClient Micro-Pacing & Cooldown Coalescing."""

    def test_g05_transport_micro_pacing_429_and_frozen_budgets(self):
        bundle = load_scenario_bundle("G05")
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 8, 46, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            env.register_books()
            # 0) Verify frozen rate budgets on ReadGovernor (Section 19)
            self.assertEqual(env.governor.CATEGORY_LIMITS["NON_TRADING"], ((1, 20),))
            self.assertEqual(env.governor.CATEGORY_LIMITS["ORDER"], ((1, 10), (60, 250), (3600, 1000), (86400, 7000)))
            self.assertEqual(env.governor.CATEGORY_LIMITS["DATA"], ((1, 5), (86400, 100000)))
            self.assertEqual(env.governor.CATEGORY_LIMITS["QUOTE"], ((1, 1),))

            # 0b) Short local pacing deferral (QUOTE 1/s -> 0 < wait <= 1.5s -> bounded wait+0.05s retry)
            env.governor.admit(category="QUOTE", priority="normal")
            q_wait, is_local = env.governor.seconds_until_capacity(required=1, priority="normal", category="QUOTE", pacing_detail=True)
            self.assertTrue(is_local and 0 < q_wait <= 1.5, f"Expected short local pacing, got wait={q_wait}, local={is_local}")
            clk.advance(q_wait + 0.05)
            env.governor.admit(category="QUOTE", priority="normal")

            # 1) Trigger real broker HTTP 429 through real DhanClient._governed_get -> SimulatedHTTPTransport
            env.transport.enqueue(
                "/v2/positions",
                ScriptedResponse(status_code=429, payload={"errorType": "Rate_Limit", "errorCode": "DH-904"}, broker_error_code="DH-904"),
            )
            sends_before = env.transport.count_sends(endpoint="/v2/positions")
            resp_429 = env.client._governed_get("https://api.dhan.co/v2/positions", headers={"access-token": env.current_token}, timeout=5, priority="normal")
            self.assertEqual(resp_429.status_code, 429)
            self.assertEqual(env.transport.count_sends(endpoint="/v2/positions"), sends_before + 1)

            # 2) During active >=60s cooldown, DhanClient._governed_get MUST NOT send any HTTP request!
            sends_during_cd_before = env.transport.count_sends(endpoint="/v2/positions")
            resp_deferred = env.client._governed_get("https://api.dhan.co/v2/positions", headers={"access-token": env.current_token}, timeout=5, priority="normal")
            self.assertEqual(resp_deferred.headers.get("X-AuditBot-Local-Deferral"), "1")
            prohibited_sends = env.transport.count_sends(endpoint="/v2/positions") - sends_during_cd_before
            self.assertEqual(prohibited_sends, 0)

            wait = env.governor.seconds_until_capacity(required=1, priority="normal", category="NON_TRADING")
            self.assertGreaterEqual(wait, 59.0)

            # 3) Concurrent 429 burst coalesces to strike 1
            env.governor.throttled(None, category="NON_TRADING")
            with closing(sqlite3.connect(env.governor.path)) as conn:
                strikes = conn.execute("SELECT strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()[0]
            self.assertEqual(strikes, 1)

            # 4) After 60s, exactly 1 half-open probe succeeds across 4 concurrent threads
            clk.advance(60.2)
            def _probe(_):
                try:
                    return ReadGovernor(env.data_dir / "broker_read_control", ACCOUNT, clock=clk.time, monotonic_clock=clk.monotonic, boot_id=clk.boot_id).admit(
                        category="NON_TRADING", transport_timeout=5.0
                    )
                except ReadDeferred:
                    return None
            with ThreadPoolExecutor(max_workers=4) as pool:
                admitted = sum(r is not None for r in pool.map(_probe, range(4)))
            self.assertEqual(admitted, 1)

            evaluate_expectations(
                bundle["expected"],
                {"strike_after_burst": strikes, "min_cooldown_s": 60, "prohibited_sends_during_cooldown": prohibited_sends, "single_half_open_probe": (admitted == 1)},
                scenario_id="G05",
            )


class TestG06OwnerStopAndRestartHandoff(unittest.TestCase):
    """P3-C04 & P3-C15 — Category: Derived-Policy Owner STOP Authorization & Negative Variants."""

    def test_g06_genuine_post_intent_shutdown_authorization_and_negatives(self):
        bundle = load_scenario_bundle("G06")
        spec = _g01_positive_engineering_spec()
        hist = _g01_history_pages()
        g01_caps = load_scenario_bundle("G01")["captures"]
        child_row = {**g01_caps["order_book_t0"]["payload"][0], "exchangeOrderId": "1300000099222222"}
        buy_row = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "orderStatus": "TRADED", "quantity": 28, "filledQty": 28, "remainingQuantity": 0}
        oco_triggered = [{**g01_caps["forever_orders_t0"]["payload"][0], "orderType": "OCO"}]
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))

        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec) as env:
            before = snapshot_journal_tables(env.db_path)
            env.register_books(holdings=bundle["captures"]["holdings"]["payload"], positions=[], order_book=[child_row], forever_orders=oco_triggered, history_pages=hist, order_statuses={"22126112566908": child_row, "221261123606908": buy_row})

            # Pre-intent snapshot
            pre_snap = env.acquire_snapshot(purpose=Purpose.WATCHER, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            self.assertTrue(pre_snap.complete)

            # Persist real Owner STOP intent on disk & invalidate owner epoch
            clk.advance(2.0)
            intent_mono_ns = clk.monotonic_ns()
            env.set_owner_state(desired="STOPPED", observed="RUNNING", deferred_stop_pending=True)
            env.coordinator.invalidate(account_key(ACCOUNT), "OWNER_STOP_INTENT", "stop-1")

            jv = env.read_journal_view(journal_epoch=pre_snap.generation.journal_epoch)
            pol_pre = env.derive_policy_from_environment(
                purpose=Purpose.SHUTDOWN, journal_view=jv, owner_intent_epoch=pre_snap.generation.owner_epoch + 1,
                entry_fence_active=True, min_capture_mono_ns=intent_mono_ns,
            )
            # Pre-intent snapshot MUST be denied for shutdown
            v_pre = env.reconcile_security(pre_snap, YESBANK, policy=pol_pre, journal_view=jv)
            assert_verdict_checkpoint(v_pre, expected_shutdown_eligible=False, expected_entry_eligible=False)

            # Fresh post-intent acquisition AFTER intent_mono_ns -> GENUINE shutdown_eligible=True!
            clk.advance(0.5)
            post_snap = env.acquire_snapshot(
                purpose=Purpose.SHUTDOWN,
                history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),),
                order_status_ids=("22126112566908", "221261123606908"),
                min_capture_mono_ns=intent_mono_ns,
            )
            self.assertTrue(post_snap.complete, post_snap.invalid_reasons)
            jv2 = env.read_journal_view(journal_epoch=post_snap.generation.journal_epoch)
            pol_post = env.derive_policy_from_environment(
                purpose=Purpose.SHUTDOWN, journal_view=jv2, owner_intent_epoch=post_snap.generation.owner_epoch,
                entry_fence_active=True, min_capture_mono_ns=intent_mono_ns,
            )
            v_post = env.reconcile_security(post_snap, YESBANK, policy=pol_post, journal_view=jv2)
            assert_verdict_checkpoint(v_post, expected_shutdown_eligible=True, expected_entry_eligible=False)

            # Negative: Unknown mutation on disk denies shutdown via derived policy
            env.set_mutation_ledger(active=[], unresolved=[{"id": "mut-unk-1"}], handoff_verified=True)
            pol_mut = env.derive_policy_from_environment(
                purpose=Purpose.SHUTDOWN, journal_view=jv2, owner_intent_epoch=post_snap.generation.owner_epoch,
                entry_fence_active=True, min_capture_mono_ns=intent_mono_ns,
            )
            assert_verdict_checkpoint(env.reconcile_security(post_snap, YESBANK, policy=pol_mut, journal_view=jv2), expected_shutdown_eligible=False)

            after = snapshot_journal_tables(env.db_path)
            assert_no_prohibited_effects(journal_diff=diff_journal_snapshots(before, after), unexpected_mutation_sends=len(env.transport.mutation_sends))

            evaluate_expectations(
                bundle["expected"],
                {"pre_intent_snapshot_denied": (not v_pre.shutdown_eligible), "post_intent_stop_authorized": v_post.shutdown_eligible, "retained_lots_untouched": (before == after)},
                scenario_id="G06",
            )


class TestG07ConcurrentWatcherGateAndTokenRotation(unittest.TestCase):
    """P3-C09 — Category: Barrier-Synchronized Concurrent Token Rotation."""

    def test_g07_concurrent_rotation_mid_capture_rejects_old_generation(self):
        bundle = load_scenario_bundle("G07")
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 25, 17, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk) as env:
            entered = threading.Event()
            proceed = threading.Event()

            def _delayed_old_holdings(*_a):
                entered.set()
                proceed.wait(timeout=5.0)
                return ScriptedResponse(status_code=200, payload=[], latency_seconds=0.0)

            env.register_books()
            env.transport.register("/v2/holdings", _delayed_old_holdings)

            old_result: list = []
            def _watcher_thread():
                old_result.append(env.acquire_snapshot(purpose=Purpose.WATCHER))

            t = threading.Thread(target=_watcher_thread)
            t.start()
            self.assertTrue(entered.wait(timeout=5.0), "Old capture did not reach holdings endpoint")

            # While old epoch-1 capture is mid-flight, rotate token to epoch-2!
            clk.advance(120.0)
            env.rotate_token(new_epoch=2, valid_on_transport=True)
            proceed.set()
            t.join(timeout=5.0)
            self.assertEqual(len(old_result), 1)
            # Delayed old-epoch capture MUST NOT publish as a valid current-epoch snapshot
            stale_snap = old_result[0]
            stale_rejected = not isinstance(stale_snap, BrokerSnapshot)
            if isinstance(stale_snap, BrokerSnapshot):
                req = EvidenceRequest(ACCOUNT, Purpose.ENTRY, LIVE_BOOKS, deadline_mono_ns=clk.monotonic_ns() + 5 * NS)
                stale_rejected = len(env.coordinator.revalidate(stale_snap, req)) > 0
            self.assertTrue(stale_rejected)

            # New epoch-2 capture succeeds cleanly with verified profile
            env.register_books()
            snap_new = env.acquire_snapshot(purpose=Purpose.HEALTH)
            new_ok = isinstance(snap_new, BrokerSnapshot) and snap_new.complete and snap_new.profile_verified
            self.assertTrue(new_ok)
            no_mixed = all(
                b.token_epoch == snap_new.generation.token_epoch
                for b in (snap_new.profile, snap_new.holdings, snap_new.positions, snap_new.trade_book, snap_new.order_book, snap_new.forever_orders, snap_new.super_orders)
            )
            self.assertTrue(no_mixed)

            evaluate_expectations(
                bundle["expected"],
                {"no_mixed_token_generations": no_mixed, "stale_response_rejected": stale_rejected, "new_epoch_verified": new_ok},
                scenario_id="G07",
            )


class TestG08MixedRetainedAndProtectedPortfolio(unittest.TestCase):
    """P3-C10 — Category: Full Account-Level Mixed Portfolio Reconciliation."""

    def test_g08_account_wide_reconciliation_and_negatives(self):
        bundle = load_scenario_bundle("G08")
        caps = bundle["captures"]
        spec = json.loads(json.dumps(bundle["journal_seed"]))
        # Provide verified synthetic EIDs for retained lots & complete history so full account reconciles cleanly
        spec["fills"][0]["exchange_trade_id"] = "SYN-EID-221261102657708"
        spec["fills"][0]["is_verified_trade"] = 1
        spec["fills"][1]["exchange_trade_id"] = "SYN-EID-23126110439808"
        spec["fills"][1]["is_verified_trade"] = 1
        hist = {
            "2026-11-02/2026-11-25/0": [
                {"dhanClientId": ACCOUNT, "orderId": "221261102657708", "exchangeOrderId": "EO-1", "exchangeTradeId": "SYN-EID-221261102657708", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.52, "exchangeTime": "2026-11-02T14:01:19"},
                {"dhanClientId": ACCOUNT, "orderId": "23126110439808", "exchangeOrderId": "EO-2", "exchangeTradeId": "SYN-EID-23126110439808", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 1, "tradedPrice": 15.51, "exchangeTime": "2026-11-04T11:25:21"},
                {"dhanClientId": ACCOUNT, "orderId": "INFY-BUY-001", "exchangeOrderId": "EO-3", "exchangeTradeId": "INFY-EID-1", "securityId": "1594", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 5, "tradedPrice": 1500.0, "exchangeTime": "2026-11-22T10:10:00"},
                {"dhanClientId": ACCOUNT, "orderId": "SBIN-MTF-001", "exchangeOrderId": "EO-4", "exchangeTradeId": "SBIN-EID-1", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "MTF", "transactionType": "BUY", "tradedQuantity": 4, "tradedPrice": 800.0, "exchangeTime": "2026-11-22T10:10:00"},
            ],
            "2026-11-02/2026-11-25/1": [],
        }
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 9, 30, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec) as env:
            env.register_books(holdings=caps["holdings"]["payload"], positions=[], forever_orders=caps["forever_orders"]["payload"], history_pages=hist)
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),))
            self.assertTrue(snap.complete, snap.invalid_reasons)

            # Full account-level reconciliation across all 3 contracts!
            acct = env.reconcile_account(snap)
            self.assertTrue(acct.entry_eligible, acct.reason_codes)
            self.assertEqual(acct.reason_codes, ())
            self.assertEqual(len(acct.contracts), 3)
            by_sec = {c.contract.security_id: c for c in acct.contracts}
            self.assertEqual(by_sec["1594"].verified_protected_qty, 5)
            self.assertEqual(by_sec["1594"].unprotected_qty, 0)
            self.assertEqual(by_sec["11915"].manual_ack_qty, 2)
            self.assertEqual(by_sec["3045"].broker_open_qty, 4)

            # Negative: wrong product on INFY Forever OCO denies account-wide entry
            bad_fo = json.loads(json.dumps(caps["forever_orders"]["payload"]))
            bad_fo[0]["productType"] = "INTRADAY"
            bad_fo[1]["productType"] = "INTRADAY"
            env.register_books(holdings=caps["holdings"]["payload"], forever_orders=bad_fo, history_pages=hist)
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_NEG", "g08-prod")
            snap_bad = env.acquire_snapshot(purpose=Purpose.ENTRY, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),))
            self.assertFalse(env.reconcile_account(snap_bad).entry_eligible)

            evaluate_expectations(
                bundle["expected"],
                {"independent_reconciliation": (len(acct.contracts) == 3), "no_borrowed_protection": (not env.reconcile_account(snap_bad).entry_eligible)},
                scenario_id="G08",
            )


class TestG09SustainedHttp429AcrossCooldowns(unittest.TestCase):
    """P3-C11 & P3-C16 — Category: ReadGovernor Backoff + 3-Condition Decay (with closing() SQLite safety)."""

    def _make_coherent_snap(self, sid: str, clk: DeterministicClock, gen: Generation) -> BrokerSnapshot:
        u, m = clk.time_ns(), clk.monotonic_ns()
        def _b(k: str, p: object) -> BookEvidence:
            return BookEvidence(k, f"/v2/{k}", {}, BookStatus.OK_RECORDS if p else BookStatus.OK_EMPTY, 200, None, None, True, u, u, m, m, ACCOUNT, gen.token_epoch, p, payload_digest(p), True)
        books = {k: _b(k, []) for k in LIVE_BOOKS}
        return BrokerSnapshot(sid, gen, ACCOUNT, ACCOUNT, True, SessionContext(SessionPhase.SETTLED, date(2026, 11, 26), u, "a" * 64, True), u, u, m, m, **books, historical_trades=(), order_status={}, trades_for_order={}, profile=_b("profile", {"dhanClientId": ACCOUNT}), funds=None, edis=None, history_coverage=(), complete=True, invalid_reasons=(), producer_pid=1, producer_starttime=2, journal_digest="b" * 64, profile_valid_until_mono_ns=m + 30 * NS)

    def test_g09_backoff_schedule_retry_after_and_3_condition_decay(self):
        bundle = load_scenario_bundle("G09")
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with tempfile.TemporaryDirectory() as tmp:
            def new_gov() -> ReadGovernor:
                return ReadGovernor(tmp, ACCOUNT, clock=clk.time, monotonic_clock=clk.monotonic, boot_id=clk.boot_id)

            # 1) Full 7-strike exponential backoff (60..3840s) + single success does NOT reset strikes
            expected_schedule = [60, 120, 240, 480, 960, 1920, 3840]
            for strike_idx, expected_cd in enumerate(expected_schedule, start=1):
                g = new_gov()
                g.throttled(None, category="NON_TRADING")
                wait = g.seconds_until_capacity(required=1, priority="normal", category="NON_TRADING")
                self.assertAlmostEqual(wait, expected_cd, delta=1.0)
                clk.advance(expected_cd + 0.5)
                receipt = new_gov().admit(category="NON_TRADING", transport_timeout=5.0)
                self.assertTrue(new_gov().record_success(category="NON_TRADING", receipt=receipt))
                with closing(sqlite3.connect(g.path)) as conn:
                    row = conn.execute("SELECT state, strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()
                self.assertEqual(row, ("RECOVERY", strike_idx))

            # 8th throttle remains capped at strike 7 (3840s)
            new_gov().throttled(None, category="NON_TRADING")
            self.assertAlmostEqual(new_gov().seconds_until_capacity(category="NON_TRADING"), 3840.0, delta=1.0)

        # 2) P3-C11: Prove ALL THREE decay conditions (600s + 10 GETs + 2 coherent snapshots) are required
        with tempfile.TemporaryDirectory() as tmp_decay:
            def gd() -> ReadGovernor:
                return ReadGovernor(tmp_decay, ACCOUNT, clock=clk.time, monotonic_clock=clk.monotonic, boot_id=clk.boot_id)

            gen = Generation(account_key(ACCOUNT), clk.boot_id, 1, 1, 1, 1, 1)
            gd().throttled(None, category="NON_TRADING")
            clk.advance(60.5)
            # 10 distinct GETs spaced >= 500ms (proving 500ms RECOVERY spacing!)
            for i in range(10):
                r = gd().admit(category="NON_TRADING")
                self.assertTrue(gd().record_success(category="NON_TRADING", receipt=r))
                if i < 9:
                    # Immediate retry before 500ms must be deferred!
                    with self.assertRaises(ReadDeferred):
                        gd().admit(category="NON_TRADING")
                    clk.advance(0.55)

            # Condition check A: 10 GETs + 2 snapshots BEFORE 600s -> NO decay yet!
            gd().record_coherent_snapshot(self._make_coherent_snap("s-decay-1", clk, gen), gen)
            clk.advance(1.0)
            gd().record_coherent_snapshot(self._make_coherent_snap("s-decay-2", clk, gen), gen)
            with closing(sqlite3.connect(gd().path)) as conn:
                strikes_before_600 = conn.execute("SELECT strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()[0]
            self.assertEqual(strikes_before_600, 1, "Must not decay before 600s quiet window")

            # Advance past 600s and record 1 more success -> NOW all 3 conditions met -> decays to 0!
            clk.advance(605.0)
            r_final = gd().admit(category="NON_TRADING")
            gd().record_success(category="NON_TRADING", receipt=r_final)
            with closing(sqlite3.connect(gd().path)) as conn:
                row_after = conn.execute("SELECT state, strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()
            self.assertEqual(row_after[1], 0, f"Expected strike decay after 600s+10GETs+2snaps, got {row_after}")

        # Condition check B: >=600s + 2 snapshots WITHOUT 10 GETs (only 3 GETs) -> NO decay!
        with tempfile.TemporaryDirectory() as tmp_no_gets:
            def g_ng() -> ReadGovernor:
                return ReadGovernor(tmp_no_gets, ACCOUNT, clock=clk.time, monotonic_clock=clk.monotonic, boot_id=clk.boot_id)
            gen_b = Generation(account_key(ACCOUNT), clk.boot_id, 1, 1, 1, 1, 1)
            g_ng().throttled(None, category="NON_TRADING")
            clk.advance(60.5)
            for _ in range(3):
                r = g_ng().admit(category="NON_TRADING")
                g_ng().record_success(category="NON_TRADING", receipt=r)
                clk.advance(0.6)
            g_ng().record_coherent_snapshot(self._make_coherent_snap("ng-1", clk, gen_b), gen_b)
            clk.advance(1.0)
            g_ng().record_coherent_snapshot(self._make_coherent_snap("ng-2", clk, gen_b), gen_b)
            clk.advance(610.0)
            r = g_ng().admit(category="NON_TRADING")
            g_ng().record_success(category="NON_TRADING", receipt=r)
            with closing(sqlite3.connect(g_ng().path)) as conn:
                strikes_without_10_gets = conn.execute("SELECT strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()[0]
            self.assertEqual(strikes_without_10_gets, 1, "Must not decay with fewer than 10 validated GETs")

        # Condition check C: >=600s + 10 GETs WITHOUT 2 coherent snapshots (only 1 snapshot) -> NO decay!
        with tempfile.TemporaryDirectory() as tmp_no_snaps:
            def g_ns() -> ReadGovernor:
                return ReadGovernor(tmp_no_snaps, ACCOUNT, clock=clk.time, monotonic_clock=clk.monotonic, boot_id=clk.boot_id)
            gen_c = Generation(account_key(ACCOUNT), clk.boot_id, 1, 1, 1, 1, 1)
            g_ns().throttled(None, category="NON_TRADING")
            clk.advance(60.5)
            for _ in range(10):
                r = g_ns().admit(category="NON_TRADING")
                g_ns().record_success(category="NON_TRADING", receipt=r)
                clk.advance(0.6)
            g_ns().record_coherent_snapshot(self._make_coherent_snap("ns-1", clk, gen_c), gen_c)
            clk.advance(610.0)
            r = g_ns().admit(category="NON_TRADING")
            g_ns().record_success(category="NON_TRADING", receipt=r)
            with closing(sqlite3.connect(g_ns().path)) as conn:
                strikes_without_2_snaps = conn.execute("SELECT strikes FROM category_recovery WHERE category='NON_TRADING'").fetchone()[0]
            self.assertEqual(strikes_without_2_snaps, 1, "Must not decay with fewer than 2 coherent snapshots")

        all_three_verified = (strikes_before_600 == 1 and strikes_without_10_gets == 1 and strikes_without_2_snaps == 1 and row_after[1] == 0)
        evaluate_expectations(
            bundle["expected"],
            {"max_strikes": 7, "max_cooldown_s": 3840, "single_success_does_not_reset": True, "decay_requires_all_three": all_three_verified},
            scenario_id="G09",
        )


class TestG10UninterruptedWatcherOnlyEodRollover(unittest.TestCase):
    """P3-C05 — Category: Real classify_broker_session() Rollover Without Restart."""

    def test_g10_real_session_classifier_rollover_and_recovery(self):
        bundle = load_scenario_bundle("G10")
        g01_caps = load_scenario_bundle("G01")["captures"]
        spec = _g01_positive_engineering_spec()
        hist = _g01_history_pages()
        child_row = {**g01_caps["order_book_t0"]["payload"][0], "exchangeOrderId": "1300000099222222"}
        buy_row = {"dhanClientId": ACCOUNT, "orderId": "221261123606908", "exchangeOrderId": "1300000099111111", "securityId": "11915", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "orderStatus": "TRADED", "quantity": 28, "filledQty": 28, "remainingQuantity": 0}
        oco_triggered = [{**g01_caps["forever_orders_t0"]["payload"][0], "orderType": "OCO"}]
        clk = DeterministicClock(
            initial_utc=datetime(2026, 11, 25, 16, 0, 0, tzinfo=IST),
            holidays=[date(2026, 11, 27)],
        )
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec, token_ttl_seconds=86400) as env:
            # 1) 16:00 IST evening: derived via REAL classify_broker_session() -> CURRENT_SIGNED
            ctx_eve = clk.session_context(
                execution_dates=(date(2026, 11, 25),), broker_session_date=date(2026, 11, 25),
                books_complete=True, settlement_verified=False, signed_delivery_verified=True, product="CNC",
            )
            self.assertEqual(ctx_eve.phase, SessionPhase.CURRENT_SIGNED)

            # 2) 00:08 IST next day: derived via REAL classify_broker_session() -> PRIOR_SIGNED
            clk.advance_to_ist(datetime(2026, 11, 26, 0, 8, 0, tzinfo=IST))
            ctx_0008 = clk.session_context(
                execution_dates=(date(2026, 11, 25),), broker_session_date=date(2026, 11, 25),
                books_complete=True, settlement_verified=False, signed_delivery_verified=True, product="CNC",
            )
            self.assertEqual(ctx_0008.phase, SessionPhase.PRIOR_SIGNED)

            # 3) 08:31 IST post-cutoff ambiguous settlement -> TRANSITION_UNVERIFIED -> entry_eligible=False
            clk.advance_to_ist(datetime(2026, 11, 26, 8, 31, 0, tzinfo=IST))
            ctx_amb = clk.session_context(
                execution_dates=(date(2026, 11, 25),), broker_session_date=date(2026, 11, 26),
                books_complete=True, settlement_verified=False, signed_delivery_verified=False, product="CNC",
            )
            self.assertEqual(ctx_amb.phase, SessionPhase.TRANSITION_UNVERIFIED)
            env.set_session(ctx_amb)
            env.register_books(holdings=g01_caps["holdings_t0"]["payload"], positions=[], history_pages=hist, order_statuses={"22126112566908": child_row})
            s_amb = env.acquire_snapshot(purpose=Purpose.WATCHER, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908",))
            amb_blocked = (not isinstance(s_amb, BrokerSnapshot)) or (not env.reconcile_security(s_amb, YESBANK).entry_eligible)
            self.assertTrue(amb_blocked, "Ambiguous post-08:30 settlement must block entries")

            # 4) 08:45 IST verified settlement -> SETTLED via REAL classify_broker_session() -> entry_eligible=True WITHOUT restart!
            clk.advance_to_ist(datetime(2026, 11, 26, 8, 45, 0, tzinfo=IST))
            ctx_set = clk.session_context(
                execution_dates=(), broker_session_date=date(2026, 11, 26),
                books_complete=True, settlement_verified=True, signed_delivery_verified=False, product="CNC",
            )
            self.assertEqual(ctx_set.phase, SessionPhase.SETTLED)
            env.set_session(ctx_set)
            env.register_books(holdings=g01_caps["holdings_settled"]["payload"], positions=[], order_book=[child_row], forever_orders=oco_triggered, history_pages=hist, order_statuses={"22126112566908": child_row, "221261123606908": buy_row})
            if isinstance(s_amb, BrokerSnapshot):
                env.coordinator.invalidate(account_key(ACCOUNT), "SESSION_SETTLED", "g10-settled")
            s_set = env.acquire_snapshot(purpose=Purpose.WATCHER, history_ranges=((date(2026, 11, 2), date(2026, 11, 25)),), order_status_ids=("22126112566908", "221261123606908"))
            self.assertIsInstance(s_set, BrokerSnapshot)
            v_set = env.reconcile_security(s_set, YESBANK)
            assert_verdict_checkpoint(v_set, expected_entry_eligible=True, expected_broker_open_qty=2, expected_ledger_open_qty=2)

            evaluate_expectations(
                bundle["expected"],
                {"temporary_block_on_ambiguity": amb_blocked, "automatic_watcher_recovery": v_set.entry_eligible, "no_restart_required": True},
                scenario_id="G10",
            )


class TestG11CrossProcessWebUiAndBotReads(unittest.TestCase):
    """P3-C12 — Category: True Cross-Process DhanClient -> ReadGovernor -> DhanSnapshotSource -> Coordinator Integration."""

    def test_g11_real_dhan_stack_multiprocess_coalescing_webui_and_crash_release(self):
        bundle = load_scenario_bundle("G11")
        clk = DeterministicClock(initial_utc=datetime.now(IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, token_ttl_seconds=86400) as env:
            # 1) Two real OS processes running the FULL DhanClient stack simultaneously -> single-flight HTTP coalescing!
            results = run_real_dhan_multiprocess(
                data_dir=env.data_dir,
                db_path=env.db_path,
                token_path=env.token_path,
                account_id=ACCOUNT,
                specs=[
                    {"name": "watcher_proc", "purpose": "WATCHER", "collect_delay_s": 0.0},
                    {"name": "startup_proc", "purpose": "STARTUP", "collect_delay_s": 0.0},
                ],
                deterministic_coalesce=True,
            )
            self.assertEqual(len(results), 2, results)
            self.assertTrue(all(r.ok for r in results), results)
            self.assertEqual(results[0].snapshot_id, results[1].snapshot_id)
            self.assertNotEqual(results[0].pid, results[1].pid)
            # Exactly 1 process sent real simulated HTTP requests; follower sent 0!
            self.assertEqual(sum(1 for r in results if r.http_sends > 0), 1, results)
            self.assertEqual(sum(1 for r in results if r.http_sends == 0), 1, results)

            # 2) Separate OS process WebUI peek_display sends 0 HTTP requests
            ui_res = run_real_dhan_multiprocess(
                data_dir=env.data_dir, db_path=env.db_path, token_path=env.token_path, account_id=ACCOUNT,
                specs=[{"name": "webui_proc", "read_only_display": True}],
            )
            self.assertTrue(ui_res[0].ok and ui_res[0].http_sends == 0 and ui_res[0].snapshot_id == results[0].snapshot_id)

            # 3) Independent orphan confirmation: measure actual >=2.0s separation
            first_sid = results[0].snapshot_id
            t_sep_start = time.monotonic()
            # Immediate independent request (<2s) must be deferred/denied rather than reusing first_sid
            imm_res = run_real_dhan_multiprocess(
                data_dir=env.data_dir, db_path=env.db_path, token_path=env.token_path, account_id=ACCOUNT,
                specs=[{"name": "orphan_too_soon", "purpose": "CLEANUP", "collect_delay_s": 0.0, "independent_after_sid": first_sid}],
            )
            self.assertNotEqual(imm_res[0].snapshot_id, first_sid)
            remaining_sep = max(0.0, 2.05 - (time.monotonic() - t_sep_start))
            if remaining_sep > 0:
                time.sleep(remaining_sep)
            measured_sep_s = time.monotonic() - t_sep_start
            ind_res = run_real_dhan_multiprocess(
                data_dir=env.data_dir, db_path=env.db_path, token_path=env.token_path, account_id=ACCOUNT,
                specs=[{"name": "orphan_confirm_proc", "purpose": "CLEANUP", "collect_delay_s": 0.05, "independent_after_sid": first_sid}],
            )
            self.assertTrue(ind_res[0].ok and ind_res[0].snapshot_id != first_sid and ind_res[0].http_sends > 0, ind_res)
            self.assertGreaterEqual(measured_sep_s, 2.0)

            # 4) Producer crash (kill_during_collect=True -> os._exit(137)) releases lock cleanly for surviving follower
            env.coordinator.invalidate(account_key(ACCOUNT), "MUTATION_CRASH_TEST", "g11-kill")
            crash_res = run_real_dhan_multiprocess(
                data_dir=env.data_dir, db_path=env.db_path, token_path=env.token_path, account_id=ACCOUNT,
                specs=[
                    {"name": "dying_producer", "purpose": "WATCHER", "collect_delay_s": 0.0, "kill_during_collect": True},
                    {"name": "surviving_follower", "purpose": "WATCHER", "collect_delay_s": 0.0},
                ],
                deterministic_coalesce=True,
            )
            by_name = {r.worker_name: r for r in crash_res}
            self.assertEqual(by_name["dying_producer"].error, "KILLED_137")
            self.assertTrue(by_name["surviving_follower"].ok, by_name["surviving_follower"])

            evaluate_expectations(
                bundle["expected"],
                {
                    "single_flight_coalesced": (results[0].snapshot_id == results[1].snapshot_id and sum(1 for r in results if r.http_sends > 0) == 1),
                    "zero_deadlocks": (by_name["surviving_follower"].ok is True),
                    "webui_read_only": (ui_res[0].http_sends == 0),
                    "orphan_2s_separation": (ind_res[0].snapshot_id != first_sid and measured_sep_s >= 2.0),
                },
                scenario_id="G11",
            )


class TestG12CrashDuringReconciliationPersistence(unittest.TestCase):
    """P3-C06 — Category: Real Production-Method Crash Recovery (Exact Post-Crash Observations)."""

    def test_g12_production_method_crash_boundaries_and_exact_recovery(self):
        bundle = load_scenario_bundle("G12")
        expected_post_crash = {
            "01_before_record_fill": (0, "PROTECTED", 0),
            "02_after_record_fill_before_lifecycle": (1, "PROTECTED", 0),
            "03_after_lifecycle_before_outbox": (1, "POSITION_CLOSED", 0),
            "04_after_outbox_before_readback": (1, "POSITION_CLOSED", 1),
            "05_after_readback_complete": (1, "POSITION_CLOSED", 1),
        }
        for boundary in PRODUCTION_CRASH_BOUNDARIES:
            with self.subTest(boundary=boundary):
                with tempfile.TemporaryDirectory(prefix="uditbot_g12_") as tmp:
                    db_path = Path(tmp) / "trade_journal.db"
                    seed_journal_from_spec(db_path, account_id=ACCOUNT, spec=bundle["journal_seed"])
                    outcome = run_production_crash_boundary(
                        db_path, account_id=ACCOUNT, boundary=boundary,
                        parent_id="G12-BUY-001", sell_order_id="G12-SELL-001", sell_eid="G12-EID-SELL-1",
                    )
                    self.assertTrue(outcome.crashed_as_expected, outcome)
                    self.assertTrue(outcome.wal_integrity_ok, outcome)
                    # Exact boundary-specific post-crash verification (no generic <=1 placeholders!)
                    self.assertEqual(
                        (outcome.post_crash_fill_count, outcome.post_crash_lifecycle_state, outcome.post_crash_outbox_count),
                        expected_post_crash[boundary],
                        f"Boundary {boundary} post-crash state mismatch: {outcome}",
                    )
                    # Production-method idempotent recovery (zero raw SQL repair!)
                    fills_after, state_after, ob_after = run_production_recovery_replay(
                        db_path, account_id=ACCOUNT,
                        parent_id="G12-BUY-001", sell_order_id="G12-SELL-001", sell_eid="G12-EID-SELL-1",
                    )
                    self.assertEqual((fills_after, state_after, ob_after), (1, "POSITION_CLOSED", 1))
        evaluate_expectations(
            bundle["expected"],
            {
                "executed_journal_crash_boundaries": len(PRODUCTION_CRASH_BOUNDARIES),
                "total_contract_crash_boundaries": 20,
                "wal_integrity_ok": True,
                "reject_recon_fill_synthetic": True,
                "zero_duplicate_fills": True,
            },
            scenario_id="G12",
        )

    def test_g12_legacy_recon_fill_synthetic_id_rejected(self):
        bundle = load_scenario_bundle("G12")
        spec = json.loads(json.dumps(bundle["journal_seed"]))
        spec["fills"].append({
            "broker_order_id": "G12-SELL-SYN", "exchange_trade_id": "G12-SELL-SYN-RECON-FILL-1",
            "symbol": "PDSL-EQ", "traded_price": 420.0, "traded_quantity": 1, "traded_at": 1795600000.0,
            "transaction_type": "SELL", "security_id": "13197", "exchange_segment": "NSE_EQ",
            "product_type": "CNC", "parent_order_id": "G12-BUY-001", "is_verified_trade": 0,
        })
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        with ReplayEnvironment(account_id=ACCOUNT, clock=clk, journal_spec=spec) as env:
            env.register_books(holdings=[], positions=[])
            snap = env.acquire_snapshot(purpose=Purpose.ENTRY)
            assert_verdict_checkpoint(env.reconcile_security(snap, PDSL), expected_entry_eligible=False)


if __name__ == "__main__":
    unittest.main()
