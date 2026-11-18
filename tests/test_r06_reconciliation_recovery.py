"""R06 offline YESBANK/manual-risk and broker-governor regression acceptance.

No HTTP, Telegram or real Dhan order calls. All broker books are synthetic.
"""
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

from src.engine.owner_lot_evidence import GATE_BOOKS, prepare_owner_lots
from src.engine.holding_quantity import (
    QuantityUnverified, aggregate_holdings, observed_open_quantity,
)
from src.engine.watch_poll_policy import (
    watcher_cadence_seconds, manual_risk_only,
    supervision_preflight_priority,
)
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.reconciliation import StartupReconciliationGate
from src.models import BrokerReadFailure, BrokerReadStatus
from src.broker.read_governor import ReadGovernor, ReadDeferred


def record(order_id):
    return {
        "order_id": order_id, "correlation_id": "EQ-YESBANK-" + order_id,
        "symbol": "YESBANK", "security_id": "11915",
        "segment": "NSE_EQ", "product_type": "CNC",
        "protection_route": "FOREVER_OCO", "state": "TRADED",
        "qty": 1, "filled_qty": 1, "exit_filled_qty": 0,
        "confirmed_position_open_qty": 1,
        "position_state": "UNPROTECTED_MANUAL_RISK",
        "manual_risk_acknowledged": True, "manual_exit_detected": False,
        "unreadable_broker_response": False,
        "reconciliation_required": False,
    }


class YesbankTwoParentRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.broker = NS(
            cfg=NS(dhan_client_id="SYNTHETIC"),
            get_order_book=Mock(return_value=[]), get_authenticated_profile_id=Mock(return_value="SYNTHETIC"),
            get_super_orders=Mock(return_value=[]),
            get_forever_orders=Mock(return_value=[]),
            get_trade_book=Mock(return_value=[]),
            get_trades_for_order=Mock(return_value=[]),
            get_positions=Mock(return_value=[]),
            get_holdings=Mock(return_value=[{"securityId": "11915", "totalQty": 2}]),
            reconciliation_gate=NS(halt_new_entries=Mock()),
        )
        self.db = NS(is_manual_risk_acknowledged=Mock(return_value=True),
                     record_system_event=Mock(), upsert_position_lifecycle=Mock())
        self.w = ProtectionWatcher(
            broker=self.broker, journal_db=self.db,
            path=str(self.root / "watcher.json"),
        )
        self.a = record("910000000000001")
        self.b = record("910000000000002")
        self.w.terminal_orders = {
            self.a["order_id"]: self.a,
            self.b["order_id"]: self.b,
        }

        self.db.get_open_positions_lifecycle = lambda: [
            dict(parent_order_id=r["order_id"], product_type="CNC", security_id="11915",
                 exchange_segment="NSE_EQ", confirmed_open_qty=1) for r in (self.a, self.b)]
        self.db.owner_lot_basis = lambda a, s, g, p, oid: (time.time() - 86400, [
            dict(exchange_trade_id=oid, traded_quantity=1, traded_price=15.)])
        self.db.owner_lot_review_reason = Mock(return_value=None)
        self.broker.get_edis_owned_quantity = Mock(return_value=None)
        self.history = [dict(dhanClientId="SYNTHETIC", securityId="11915", isin="TEST-ISIN",
                         exchangeSegment="NSE_EQ", productType="CNC", orderId=r["order_id"],
                         exchangeTradeId=r["order_id"], transactionType="BUY", tradedQuantity=1,
                         tradedPrice=15.) for r in (self.a, self.b)]
        self.broker.get_historical_trades = Mock(return_value=self.history)

    def full_pass(self):
        batch = {"verified": {}, "errors": {}, "share": False}
        token = GATE_BOOKS.set(batch)
        try:
            prepare_owner_lots(self.broker, self.db, batch, self.broker.get_positions(),
                               self.broker.get_trade_book(), time.time())
            self.w.supervise_open_positions()
        finally:
            GATE_BOOKS.reset(token)

    def unreadable(self, status="UNAVAILABLE"):
        self.broker.get_holdings.return_value = BrokerReadFailure(
            status=status, error="synthetic no broker read", status_code=429,
        )
        self.broker.get_positions.return_value = BrokerReadFailure(
            status=status, error="synthetic no broker read", status_code=429,
        )

    def healthy(self, qty=2, field="totalQty"):
        self.broker.get_positions.return_value = []
        self.broker.get_holdings.return_value = [
            {"securityId": "11915", "isin": "TEST-ISIN", field: qty}
        ]

    def test_acknowledged_stale_manual_risk_keeps_budget_but_not_health(self):
        for item in (self.a, self.b):
            item.update(
                position_state="RECONCILIATION_REQUIRED",
                reconciliation_required=True,
                manual_risk_acknowledged=True,
                unreadable_broker_response=True,
                manual_exit_detected=True,
            )
        pending, open_records, _signature = self.w._poll_snapshot()
        self.assertFalse(manual_risk_only(pending, open_records))
        self.assertEqual("routine", supervision_preflight_priority(pending, open_records))
        self.assertEqual(120.0, watcher_cadence_seconds(
            pending=pending, open_records=open_records, now=time.time(),
            market_open=True,
        ))
        self.assertTrue(all(r["reconciliation_required"] for r in (self.a, self.b)))
        self.assertFalse(self.broker.reconciliation_gate.halt_new_entries.called)

    def test_live_exit_leg_still_forces_urgent_broker_supervision(self):
        self.a.update(
            position_state="RECONCILIATION_REQUIRED",
            reconciliation_required=True,
            manual_risk_acknowledged=True,
            unreadable_broker_response=True,
            sl_order_id="LIVE-STOP-LEG",
        )
        self.b.update(
            position_state="RECONCILIATION_REQUIRED",
            reconciliation_required=True,
            manual_risk_acknowledged=True,
            unreadable_broker_response=True,
        )
        pending, open_records, _ = self.w._poll_snapshot()
        self.assertEqual("normal", supervision_preflight_priority(pending, open_records))
        self.assertEqual(10.0, watcher_cadence_seconds(
            pending=pending, open_records=open_records, now=time.time(),
            market_open=True,
        ))

    def test_read_denial_cannot_invent_manual_exit_or_sell(self):
        self.unreadable()
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertFalse(item.get("manual_exit_detected"))
            self.assertTrue(item["reconciliation_required"])
            self.assertEqual(0, item["exit_filled_qty"])
        self.broker.reconciliation_gate.halt_new_entries.assert_called()

    def test_live_two_share_recovery_after_denial_is_idempotent(self):
        self.unreadable()
        self.full_pass()
        self.healthy()
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("UNPROTECTED_MANUAL_RISK", item["position_state"])
            self.assertFalse(item["reconciliation_required"])
            self.assertFalse(item["manual_exit_detected"])
            self.assertFalse(item["unreadable_broker_response"])
            self.assertEqual(1, item["confirmed_position_open_qty"])
            self.assertEqual(0, item["protected_qty"] if "protected_qty" in item else 0)
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("UNPROTECTED_MANUAL_RISK", item["position_state"])
            self.assertEqual(0, item["exit_filled_qty"])

    def test_legacy_yesbank_stale_flags_need_full_evidence(self):
        for item in (self.a, self.b):
            item.update(position_state="RECONCILIATION_REQUIRED",
                        reconciliation_required=True, manual_exit_detected=True,
                        unreadable_broker_response=True)
        self.healthy()
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("UNPROTECTED_MANUAL_RISK", item["position_state"])
            self.assertFalse(item["manual_exit_detected"])

    def test_manual_sell_same_security_prevents_stale_flag_clear(self):
        # Even if an unrelated BUY restores aggregate DP quantity to two,
        # an observed unlinked manual SELL must not clear a historical warning.
        for item in (self.a, self.b):
            item.update(
                position_state="RECONCILIATION_REQUIRED",
                reconciliation_required=True, manual_exit_detected=True,
                unreadable_broker_response=True,
            )
        self.healthy()
        self.broker.get_trade_book.return_value = [{
            "transactionType": "SELL",
            "securityId": "11915", "exchangeSegment": "NSE_EQ",
            "productType": "CNC",
            "orderId": "OWNER-MANUAL-EXIT-42", "tradedQuantity": 1,
            "tradedPrice": 19.0, "exchangeTradeId": "OWNER-TRD-42",
        }]
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertTrue(item["manual_exit_detected"])
            self.assertTrue(item.get("external_unlinked_sell_observed"))

    def test_failed_parent_specific_trade_read_prevents_auto_recovery(self):
        for item in (self.a, self.b):
            item.update(
                position_state="RECONCILIATION_REQUIRED",
                reconciliation_required=True, manual_exit_detected=True,
                unreadable_broker_response=True,
            )
        self.healthy()
        self.broker.get_trades_for_order = Mock(return_value=BrokerReadFailure(
            status=BrokerReadStatus.UNAVAILABLE.value,
            error="synthetic per-order Dhan GET denied",
            status_code=429,
        ))
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertTrue(item["reconciliation_required"])

    def test_extra_manual_shares_cannot_conceal_unlinked_sale(self):
        # Two bot BUY parents total two shares. A separate manual BUY can
        # restore aggregate shares after an unlinked SELL. Quantity greater
        # than the bot total is NOT sufficient proof for legacy recovery.
        for item in (self.a, self.b):
            item.update(
                position_state="RECONCILIATION_REQUIRED",
                reconciliation_required=True,
                manual_exit_detected=True,
                unreadable_broker_response=True,
            )
        self.healthy(qty=3)
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertTrue(item["reconciliation_required"])
            self.assertTrue(item["manual_exit_detected"])
        # The record was already blocked before this pass, so another gate
        # halt event is not required. Persisted unresolved state is decisive.

    def test_intraday_buy_cannot_replace_settled_evidence_for_old_yesbank(self):
        for item in (self.a, self.b):
            item.update(
                position_state="RECONCILIATION_REQUIRED",
                reconciliation_required=True,
                manual_exit_detected=True,
                unreadable_broker_response=True,
            )
        self.broker.get_holdings.return_value = []
        self.broker.get_positions.return_value = [{
            "securityId": "11915", "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 2,
            "dhanClientId": "SYNTHETIC",
        }]
        self.full_pass()
        for item in (self.a, self.b):
            self.assertTrue(item["reconciliation_required"])
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertTrue(item["manual_exit_detected"])

    def test_legacy_flags_never_clear_without_verified_ack(self):
        for item in (self.a, self.b):
            item.update(position_state="RECONCILIATION_REQUIRED",
                        reconciliation_required=True, manual_exit_detected=True,
                        unreadable_broker_response=True)
        self.db.is_manual_risk_acknowledged.return_value = False
        self.healthy()
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertTrue(item["manual_exit_detected"])

    def test_readable_holdings_without_durable_owner_ack_stays_blocked(self):
        self.db.is_manual_risk_acknowledged.return_value = False
        self.healthy()
        self.full_pass()
        self.assertTrue(all(r["reconciliation_required"] for r in (self.a, self.b)))
        self.assertTrue(all(r["position_state"] == "RECONCILIATION_REQUIRED" for r in (self.a, self.b)))
        self.assertFalse(any(r.get("manual_exit_detected") for r in (self.a, self.b)))

    def test_uncorroborated_shortfall_never_makes_fake_sell(self):
        self.healthy(1)
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("RECONCILIATION_REQUIRED", item["position_state"])
            self.assertFalse(item.get("manual_exit_detected"))
            self.assertFalse(item.get("manual_exit_evidence_verified"))
            self.assertEqual(0, item.get("exit_filled_qty"))
        self.healthy(2)
        self.full_pass()
        for item in (self.a, self.b):
            self.assertEqual("UNPROTECTED_MANUAL_RISK", item["position_state"])

    def test_zero_intraday_row_does_not_override_two_settled_shares(self):
        self.broker.get_positions.return_value = [{
            "securityId": "11915", "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 0
        }]
        self.healthy()
        self.broker.get_positions.return_value = [{
            "securityId": "11915", "exchangeSegment": "NSE_EQ",
            "productType": "CNC", "netQty": 0
        }]
        self.full_pass()
        self.assertFalse(any(x.get("manual_exit_detected") for x in (self.a, self.b)))

    def test_dp_quantity_fallback(self):
        self.healthy(2, "dpQty")
        self.full_pass()
        self.assertFalse(any(x.get("manual_exit_detected") for x in (self.a, self.b)))

    def test_account_mismatch_stays_blocked(self):
        self.broker.get_holdings.return_value = [{
            "securityId": "11915", "totalQty": 2, "dhanClientId": "OTHER"
        }]
        self.full_pass()
        self.assertTrue(any(x.get("reconciliation_required") for x in (self.a, self.b)))
        self.assertFalse(any(x.get("manual_exit_detected") for x in (self.a, self.b)))

    def test_recovery_readback_requires_sqlite_files_and_lifecycle_to_agree(self):
        from src.journal.db import JournalDB
        db = JournalDB(str(self.root / "proof.sqlite"), account_id="SYNTHETIC")
        self.w.journal_db = db
        self.a["confirmed_position_open_qty"] = 1
        self.assertTrue(self.w._sync_position_lifecycle(self.a))
        self.assertTrue(self.w._save())
        ids = [self.a["order_id"]]
        self.assertTrue(self.w._verify_recovery_saved(ids))
        self.w._write_files_strict()
        db.save_watcher_state({}, {})
        self.assertFalse(self.w._verify_recovery_saved(ids))
        self.assertTrue(self.w._save())
        Path(self.w.path + ".terminal.json").write_text("{}")
        self.assertFalse(self.w._verify_recovery_saved(ids))

    def test_persistence_failure_never_reports_recovery(self):
        self.unreadable()
        self.full_pass()
        self.db.upsert_position_lifecycle.side_effect = OSError("synthetic SQLite write error")
        self.healthy()
        self.full_pass()
        self.assertTrue(all(x["reconciliation_required"] for x in (self.a, self.b)))
        self.assertTrue(all(x["owner_readback_failed"] for x in (self.a, self.b)))
        self.db.upsert_position_lifecycle.side_effect = None
        self.full_pass()
        self.assertTrue(all(x["position_state"] == "UNPROTECTED_MANUAL_RISK" for x in (self.a, self.b)))


class HoldingsQuantityContract(unittest.TestCase):
    def test_current_day_cnc_buy_before_dematerialized_holding(self):
        # Dhan /positions includes current-day CNC BUY; /holdings lists stock
        # bought in previous sessions. This evidence is valid for today's
        # positive position, not for legacy settled-position recovery.
        qty, provenance = observed_open_quantity(
            [{"securityId": "11915", "exchangeSegment": "NSE_EQ",
              "productType": "CNC", "netQty": 1, "dhanClientId": "TEST"}],
            [],
            security_id="11915", segment="NSE_EQ", product="CNC",
            account_id="TEST",
        )
        self.assertEqual((1, "VERIFIED_POSITIONS"), (qty, provenance))

    def test_current_day_and_settled_quantities_require_separate_proof(self):
        qty, provenance = observed_open_quantity(
            [{"securityId": "11915", "exchangeSegment": "NSE_EQ",
              "productType": "CNC", "netQty": 3, "dhanClientId": "TEST"}],
            [{"securityId": "11915", "totalQty": 2}],
            security_id="11915", segment="NSE_EQ", product="CNC",
            account_id="TEST",
        )
        self.assertIsNone(qty)
        self.assertEqual("CNC_POSITIONS_HOLDINGS_CONTRADICT", provenance)

    def test_zero_positions_with_dp_shares(self):
        result, source = observed_open_quantity(
            [{"securityId": "11915", "exchangeSegment": "NSE_EQ",
              "productType": "CNC", "netQty": 0}],
            [{"securityId": "11915", "dpQty": 2}],
            security_id="11915", segment="NSE_EQ", product="CNC", account_id="TEST",
        )
        self.assertEqual((2, "VERIFIED_SETTLED_HOLDINGS"), (result, source))

    def test_missing_holdings_is_unknown_not_zero(self):
        actual, _ = observed_open_quantity(
            [], None, security_id="11915", segment="NSE_EQ", product="CNC", account_id="TEST"
        )
        self.assertIsNone(actual)

    def test_conflicting_quantity_and_negative_net_stay_unknown(self):
        for positions, holdings in (
            ([], [{"securityId": "11915", "dpQty": 2, "totalQty": 0}]),
            ([{"securityId": "11915", "netQty": -1}],
             [{"securityId": "11915", "totalQty": 2}]),
        ):
            with self.subTest(positions=positions):
                qty, _ = observed_open_quantity(
                    positions, holdings, security_id="11915", segment="NSE_EQ",
                    product="CNC", account_id="TEST",
                )
                self.assertIsNone(qty)

    def test_malformed_quantity_never_parses_as_zero(self):
        for qty in (True, -1, "3.14", None):
            with self.subTest(qty=qty):
                with self.assertRaises(QuantityUnverified):
                    aggregate_holdings([{"securityId": "11915", "totalQty": qty}])


class ReadBudgetAndAfterHours(unittest.TestCase):
    def test_stale_reads_slow_after_hours_but_uncertain_mutations_remain_urgent(self):
        for flags in (
            {"reconciliation_required": True},
            {"unreadable_broker_response": True},
            {"cancel_uncertain": True},
            {"pending_oco": {"state": "UNKNOWN"}},
        ):
            with self.subTest(flags=flags):
                expected = 10.0 if ("cancel_uncertain" in flags or "pending_oco" in flags) else 300.0
                self.assertEqual(expected, watcher_cadence_seconds(
                    pending={"O": {"order_id": "O", "created_at": 1, **flags}},
                    open_records=[], now=100000, market_open=False,
                ))

    def test_new_amo_exact_45s_burst(self):
        rec = {"order_id": "AMO1", "entry_only_amo": True, "created_at": 1000}
        self.assertEqual(2.0, watcher_cadence_seconds(
            pending={"AMO1": rec}, open_records=[], now=1044, market_open=False))
        self.assertEqual(300.0, watcher_cadence_seconds(
            pending={"AMO1": rec}, open_records=[], now=1045, market_open=False))

    def test_market_hours_urgent_remains_fast(self):
        hold = record("H1")
        hold["reconciliation_required"] = True
        self.assertEqual(10.0, watcher_cadence_seconds(
            pending={}, open_records=[hold], now=100000, market_open=True))

    def test_normal_preflight_uses_95_percent_transport_ceiling(self):
        with tempfile.TemporaryDirectory() as root:
            gov = ReadGovernor(root, "TEST", clock=lambda: 100000.0)
            gov.LIMITS = ((86400, 100),)
            # 94 admissions leave insufficient capacity for an 8-GET pass.
            for _ in range(94):
                gov.admit(priority="normal")
            self.assertGreater(gov.seconds_until_capacity(required=8, priority="normal"), 0)
            with self.assertRaises(ReadDeferred):
                self._fill_to_limit(gov)

    @staticmethod
    def _fill_to_limit(gov):
        gov.admit(priority="normal")
        gov.admit(priority="normal")


class StartupCandidateCoverage(unittest.TestCase):
    def test_valid_owner_ack_does_not_bypass_unresolved_lifecycle(self):
        # Reproduces two acknowledged YESBANK BUY parents: even if holdings
        # sum is 2 and owner ACKs exist, no gate success while a lifecycle
        # still requires reconciliation. No Dhan HTTP traffic in this test.
        rows = []
        for oid in ("910000000000001", "910000000000002"):
            rows.append({
                "parent_order_id": oid, "symbol": "YESBANK",
                "product_type": "CNC", "security_id": "11915",
                "exchange_segment": "NSE_EQ",
                "confirmed_open_qty": 1, "entry_qty": 1,
                "buy_filled_qty": 1, "position_state": "RECONCILIATION_REQUIRED",
                "manual_exit_detected": True,
            })
        broker = NS(
            cfg=NS(dhan_client_id="TEST"),
            get_holdings=Mock(return_value=[{"securityId": "11915", "dpQty": 2}]),
            get_forever_orders=Mock(return_value=[]),
        )
        db = NS(
            get_open_positions_lifecycle=lambda: rows,
            is_manual_risk_acknowledged=lambda *args, **kw: True,
        )
        gate = StartupReconciliationGate(
            broker=broker, journal_db=db, watcher=None,
            stop_manager=NS(is_stopped=lambda: False),
        )
        ok, reason = gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        self.assertIn("unresolved", reason.lower())
        broker.get_holdings.assert_not_called()
        self.assertEqual(2, len(rows))
        # A later VERIFIED lifecycle recovery may be considered by startup,
        # but only after the watcher has durably changed both records.
        for row in rows:
            row["position_state"] = "UNPROTECTED_MANUAL_RISK"
            row["manual_exit_detected"] = False
        ok, reason = gate._verify_bot_owned_holdings_protected([])
        self.assertTrue(ok, reason)
        self.assertTrue(broker.get_holdings.called)

    def test_protected_day_position_only_explains_its_own_distinct_fills(self):
        contract = dict(securityId="11915", exchangeSegment="NSE_EQ", productType="CNC")
        row = dict(parent_order_id="A", security_id="11915", exchange_segment="NSE_EQ",
                   product_type="CNC", confirmed_open_qty=2, position_state="PROTECTED")
        broker = NS(cfg=NS(dhan_client_id="TEST"), get_holdings=Mock(return_value=[]))
        gate = StartupReconciliationGate(broker=broker, journal_db=NS(get_open_positions_lifecycle=lambda: [row]),
                                         watcher=None, stop_manager=NS(is_stopped=lambda: False))
        fill = dict(contract, orderId="A", transactionType="BUY", exchangeTradeId="10", tradedQuantity=1)
        positions = [dict(contract, netQty=2)]
        for fills in ([fill, fill], [dict(fill, orderId="B", tradedQuantity=2)]):
            self.assertFalse(gate._verify_bot_owned_holdings_protected(positions, fills)[0])
        self.assertTrue(gate._verify_bot_owned_holdings_protected(positions, [fill, dict(fill, exchangeTradeId="11")])[0])
        row["manual_exit_detected"] = True
        self.assertFalse(gate._verify_bot_owned_holdings_protected(positions, [dict(fill, tradedQuantity=2)])[0])

    def test_ambiguous_holdings_do_not_establish_ownership(self):
        self.assertIsNone(observed_open_quantity([{"securityId": "1", "netQty": 2}],
            [{"securityId": "1", "totalQty": 2}], security_id="1", segment="NSE_EQ", product="CNC", account_id="")[0])
        for rows in ([{"securityId": "1", "availableQty": 0}],
                     [{"securityId": "1", "dpQty": 1}] * 2):
            with self.assertRaises(QuantityUnverified):
                aggregate_holdings(rows)

    def test_stale_reconciliation_state_is_not_skipped(self):
        row = {
            "parent_order_id": "910000000000001",
            "symbol": "YESBANK", "product_type": "CNC",
            "security_id": "11915", "exchange_segment": "NSE_EQ",
            "confirmed_open_qty": 1, "entry_qty": 1, "buy_filled_qty": 1,
            "position_state": "RECONCILIATION_REQUIRED",
        }
        broker = NS(cfg=NS(dhan_client_id="TEST"),
                    get_holdings=Mock(return_value=[{"securityId": "11915", "totalQty": 1}]),
                    get_forever_orders=Mock(return_value=[]))
        db = NS(get_open_positions_lifecycle=lambda: [row],
                is_manual_risk_acknowledged=lambda *args, **kw: False)
        gate = StartupReconciliationGate(
            broker=broker, journal_db=db, watcher=None,
            stop_manager=NS(is_stopped=lambda: False),
        )
        ok, _ = gate._verify_bot_owned_holdings_protected([])
        self.assertFalse(ok)
        # The pending lifecycle itself is authoritative negative evidence.
        # Reject immediately; do NOT burn another Dhan GET or let a valid
        # old owner ACK override the unresolved position state.
        broker.get_holdings.assert_not_called()


if __name__ == "__main__":
    unittest.main()
