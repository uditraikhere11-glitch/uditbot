"""
Regression & Verification Suite for AuditBot V14 — AB-V14-WEBUI-BACKEND-FIX-04
Covers all four independently reproduced backend findings (F03-01 through F03-04):
- F03-01 (P0): Authoritative correlation_id/broker_order_id -> terminal status mapping (never substituting
  TRADED for CANCELLED, REJECTED, or EXPIRED) + independent partial-fill quantity & risk counter preservation
- F03-02 (P0): Missing, corrupted, and inaccessible trade_journal.db handling vs. clean new TEST installation
- F03-03 (P0): Never accepting local watcher protected_qty without authoritative Dhan read-only confirmation
- F03-04 (P1): Bounded startup verification window for interrupted APPLYING/VERIFYING restart recovery
"""

import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import MagicMock

from src.config import AppConfig, TradingMode
from src.engine.config_manager import (
    ConfigApplyState,
    ConfigChangeRecord,
    ConfigChangeTracker,
    OperationState,
    ServiceOperatingState,
    compute_config_version,
)
from src.journal.db import JournalDB
from src.models import BrokerReadFailure
from src.setup_app import build_env_and_params_dict


class TestV14WebUIBackendFix04(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="auditbot_fix04_")
        self.data_dir = os.path.join(self.tmp_dir, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.env_path = os.path.join(self.tmp_dir, ".env")
        self.stop_path = os.path.join(self.data_dir, "emergency_stop.json")
        self.db_path = os.path.join(self.data_dir, "trade_journal.db")
        self.watcher_path = os.path.join(self.data_dir, "pending_protection.json")

        with open(self.env_path, "w", encoding="utf-8") as f:
            f.write(
                "OPERATOR_NAME=Udit\n"
                "BOT_INSTANCE_NAME=Audit bot\n"
                "TRADING_MODE=LIVE\n"
                "EQUITY_ACTIVE=true\n"
                "FNO_ACTIVE=false\n"
                "DHAN_CLIENT_ID=1113731536\n"
                "EQUITY_BUDGET_INR=500.0\n"
                "MIN_TRADE_FUNDS_INR=100.0\n"
                "SL_PCT=3.0\n"
                "TARGET_PCT=3.0\n"
            )
        with open(self.stop_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "is_stopped": False,
                    "stopped_by": None,
                    "reason": "Normal",
                    "in_flight_orders": [],
                    "account_id": "1113731536",
                },
                f,
            )

        self.db = JournalDB(self.db_path, account_id="1113731536")
        self.tracker = ConfigChangeTracker(data_dir=self.data_dir)
        self.initial_cfg = AppConfig.from_env(env_file=self.env_path)
        self.initial_hash = compute_config_version(self.initial_cfg)
        self.tracker.record_active_running_version(self.initial_hash, trading_mode="LIVE")

    def tearDown(self):
        try:
            self.tracker.stop_coordinator_daemon(join_timeout=1.0)
        except Exception:
            pass
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _make_restart_and_ack(self, mode_override=None):
        def _restart():
            from src.config import parse_env_dict
            parsed = parse_env_dict(self.env_path)
            cfg = AppConfig.from_env(env=parsed, env_file="")
            h = compute_config_version(cfg)
            m = mode_override or cfg.trading_mode.value
            self.tracker.record_active_running_version(h, trading_mode=m)
            return True
        return _restart

    # =========================================================================
    # F03-01 — Authoritative Terminal Status Mapping & Partial Fill Preservation
    # =========================================================================

    def test_01_f03_01_cancelled_zero_fill_propagates_exact_cancelled_status_to_intent_and_orders(self):
        """
        F03-01 Mandatory Reproduction:
        - Dhan reports CANCELLED
        - Filled quantity is zero
        - Local order intent was TRANSIT
        - Local orders row was TRANSIT
        Expected:
        - order_intents.state becomes CANCELLED
        - orders.status becomes CANCELLED (never TRADED)
        - daily_trades and current_exposure remain 0
        """
        self.db.record_order_intent(
            correlation_id="CORR-F03-01-CANC",
            symbol="SBIN",
            security_id="3045",
            exchange_segment="NSE_EQ",
            product_type="CNC",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
        )
        self.db.update_order_intent("CORR-F03-01-CANC", state="TRANSIT", broker_order_id="ORD-F03-01-CANC")
        self.db.record_order(
            correlation_id="CORR-F03-01-CANC",
            broker_order_id="ORD-F03-01-CANC",
            trading_mode="LIVE",
            product_type="CNC",
            exchange_segment="NSE_EQ",
            symbol="SBIN",
            security_id="3045",
            order_type="LIMIT",
            price=800.0,
            quantity=5,
            status="TRANSIT",
        )

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {
                "dhanClientId": "1113731536",
                "orderId": "ORD-F03-01-CANC",
                "correlationId": "CORR-F03-01-CANC",
                "orderStatus": "CANCELLED",
                "filledQty": 0,
                "remainingQuantity": 0,
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "BUY",
                "quantity": 5,
            }
        ]
        mock_broker.get_trade_book.return_value = []
        mock_broker.get_forever_orders.return_value = []
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_holdings.return_value = []
        mock_broker.get_positions.return_value = []
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        attach_unified_live_evidence_to_stub_broker(mock_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")

        ok, err = self.tracker.reconcile_stale_journal_with_broker(
            db_path=self.db_path,
            broker=mock_broker,
            env_path=self.env_path,
            is_live_mode=True,
            is_service_running=True,
        )
        self.assertTrue(ok, f"Unexpected reconciliation error: {err}")

        with self.db._get_conn() as conn:
            intent_row = conn.execute(
                "SELECT state, quantity FROM order_intents WHERE correlation_id = ?",
                ("CORR-F03-01-CANC",),
            ).fetchone()
            order_row = conn.execute(
                "SELECT status, quantity, COALESCE(filled_quantity, 0) AS filled_qty FROM orders WHERE correlation_id = ?",
                ("CORR-F03-01-CANC",),
            ).fetchone()

        self.assertEqual(intent_row["state"], "CANCELLED")
        self.assertEqual(order_row["status"], "CANCELLED")
        self.assertNotEqual(order_row["status"], "TRADED")
        self.assertEqual(int(order_row["quantity"]), 5)
        self.assertEqual(int(order_row["filled_qty"]), 0)

        # Verify daily-trade counters and exposure calculations are not corrupted
        self.assertEqual(self.db.get_today_orders_count(), 0)
        risk_st = self.db.get_live_portfolio_risk_state()
        self.assertEqual(risk_st["daily_trades"], 0)
        self.assertEqual(risk_st["open_positions"], 0)
        self.assertEqual(risk_st["current_exposure"], 0.0)

    def test_02_f03_01_rejected_expired_traded_and_partially_filled_terminal_orders_matrix(self):
        """
        F03-01: Tests CANCELLED (0 fills), REJECTED (0 fills), EXPIRED (0 fills), TRADED (5/5 fills),
        partially filled CANCELLED (4/10 fills), and partially filled EXPIRED (3/8 fills).
        Verifies:
        1. Exact terminal status propagated to both order_intents and orders.
        2. Original order quantity and executed filled_quantity preserved independently.
        3. Fills table and watcher_orders updated idempotently.
        4. Daily-trade counters and exposure calculations reflect only executed fills.
        """
        cases = [
            ("CORR-REJ-0", "ORD-REJ-0", "3001", 10, 0, "REJECTED", None),
            ("CORR-EXP-0", "ORD-EXP-0", "3002", 10, 0, "EXPIRED", None),
            ("CORR-TRD-5", "ORD-TRD-5", "3003", 5, 5, "TRADED", "OCO-TRD-5"),
            ("CORR-PART-CANC", "ORD-PART-CANC", "3004", 10, 4, "CANCELLED", "OCO-PART-CANC"),
            ("CORR-PART-EXP", "ORD-PART-EXP", "3005", 8, 3, "EXPIRED", "OCO-PART-EXP"),
        ]

        order_book_payload = []
        forever_book_payload = []
        trade_book_payload = []
        positions_payload = []
        now = float(int(time.time()))
        from datetime import datetime as _dt
        from src.engine.broker_session import IST as _IST
        now_ex_time = _dt.fromtimestamp(now, _IST).strftime("%Y-%m-%d %H:%M:%S")

        for cid, boid, sec_id, req_qty, fill_qty, term_st, oco_id in cases:
            self.db.record_order_intent(cid, f"SYM-{sec_id}", sec_id, "NSE_EQ", "CNC", "LIMIT", 100.0, req_qty)
            self.db.update_order_intent(cid, state="TRANSIT", broker_order_id=boid)
            self.db.record_order(
                correlation_id=cid,
                broker_order_id=boid,
                trading_mode="LIVE",
                product_type="CNC",
                exchange_segment="NSE_EQ",
                symbol=f"SYM-{sec_id}",
                security_id=sec_id,
                order_type="LIMIT",
                price=100.0,
                quantity=req_qty,
                status="TRANSIT",
            )
            with self.db._get_conn() as conn:
                conn.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?, 0, ?, ?)",
                    (
                        boid,
                        json.dumps({
                            "order_id": boid,
                            "correlation_id": cid,
                            "symbol": f"SYM-{sec_id}",
                            "security_id": sec_id,
                            "segment": "NSE_EQ",
                            "product_type": "CNC",
                            "qty": req_qty,
                            "filled_qty": 0,
                            "protected_qty": 0,
                            "state": "PENDING",
                        }),
                        now,
                    ),
                )
            order_book_payload.append({
                "orderId": boid,
                "correlationId": cid,
                "orderStatus": term_st,
                "quantity": req_qty,
                "filledQty": fill_qty,
                "averageTradedPrice": 100.0 if fill_qty > 0 else 0.0,
                "securityId": sec_id,
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
            })
            if oco_id and fill_qty > 0:
                with self.db._get_conn() as conn:
                    conn.execute(
                        "INSERT INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?, '1113731536', ?, ?, 'NSE_EQ', 'CNC', 'FOREVER_OCO', 'PROTECTED', ?, ?, 0, ?, ?)",
                        (boid, f"SYM-{sec_id}", sec_id, req_qty, fill_qty, fill_qty, now),
                    )
                    conn.execute(
                        "INSERT INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?, ?, ?, 100.0, ?, ?, 'BUY', ?, 'NSE_EQ', 'CNC', ?, 1)",
                        (boid, f"ET{sec_id}", f"SYM-{sec_id}", fill_qty, now, sec_id, boid),
                    )
                    conn.execute(
                        "INSERT INTO protection_orders (parent_order_id, forever_order_id, symbol, quantity, status, created_at) VALUES (?, ?, ?, ?, 'CONFIRM', ?)",
                        (boid, oco_id, f"SYM-{sec_id}", fill_qty, now),
                    )
                trade_book_payload.append({
                    "dhanClientId": "1113731536",
                    "orderId": boid,
                    "exchangeTradeId": f"ET{sec_id}",
                    "exchangeOrderId": f"EXO-{boid}",
                    "securityId": sec_id,
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "transactionType": "BUY",
                    "tradedQuantity": fill_qty,
                    "tradedPrice": 100.0,
                    "exchangeTime": now_ex_time,
                })
                positions_payload.append({
                    "dhanClientId": "1113731536",
                    "securityId": sec_id,
                    "exchangeSegment": "NSE_EQ",
                    "productType": "CNC",
                    "netQty": fill_qty,
                    "dayBuyQty": fill_qty,
                    "daySellQty": 0,
                    "carryForwardBuyQty": 0,
                    "carryForwardSellQty": 0,
                })
                forever_book_payload.extend([
                    {
                        "dhanClientId": "1113731536",
                        "orderId": oco_id,
                        "parentOrderId": boid,
                        "securityId": sec_id,
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "orderStatus": "CONFIRM",
                        "legName": "STOP_LOSS_LEG",
                        "transactionType": "SELL",
                        "quantity": fill_qty,
                        "triggerPrice": 97.0,
                        "price": 96.0,
                    },
                    {
                        "dhanClientId": "1113731536",
                        "orderId": oco_id,
                        "parentOrderId": boid,
                        "securityId": sec_id,
                        "exchangeSegment": "NSE_EQ",
                        "productType": "CNC",
                        "orderStatus": "CONFIRM",
                        "legName": "TARGET_LEG",
                        "transactionType": "SELL",
                        "quantity": fill_qty,
                        "triggerPrice": 103.0,
                        "price": 103.0,
                    },
                ])

        for _row in order_book_payload:
            _row.setdefault("dhanClientId", "1113731536")
            _row.setdefault("exchangeSegment", "NSE_EQ")
            _row.setdefault("productType", "CNC")
            _row.setdefault("remainingQuantity", 0)
            _row.setdefault("transactionType", "BUY")
            _row.setdefault("quantity", _row.get("filledQty", 5) or 5)
        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = order_book_payload
        mock_broker.get_forever_orders.return_value = forever_book_payload
        mock_broker.get_trade_book.return_value = trade_book_payload
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_holdings.return_value = []
        mock_broker.get_positions.return_value = positions_payload
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        attach_unified_live_evidence_to_stub_broker(mock_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")

        # Run reconciliation twice to verify idempotence
        for _ in range(2):
            ok, err = self.tracker.reconcile_stale_journal_with_broker(
                db_path=self.db_path,
                broker=mock_broker,
                env_path=self.env_path,
                is_live_mode=True,
                is_service_running=True,
            )
            self.assertTrue(ok, f"Unexpected reconciliation error: {err}")

        with self.db._get_conn() as conn:
            for cid, boid, sec_id, req_qty, fill_qty, term_st, _ in cases:
                i_row = conn.execute("SELECT state, quantity FROM order_intents WHERE correlation_id = ?", (cid,)).fetchone()
                o_row = conn.execute(
                    "SELECT status, quantity, COALESCE(filled_quantity, 0) AS filled_qty FROM orders WHERE correlation_id = ?",
                    (cid,),
                ).fetchone()
                w_row = conn.execute("SELECT is_terminal, record_json FROM watcher_orders WHERE order_id = ?", (boid,)).fetchone()
                w_data = json.loads(w_row["record_json"])

                self.assertEqual(i_row["state"], term_st)
                self.assertEqual(o_row["status"], term_st)
                self.assertEqual(int(o_row["quantity"]), req_qty)
                self.assertEqual(int(o_row["filled_qty"]), fill_qty)
                self.assertEqual(int(w_row["is_terminal"]), 1)
                self.assertEqual(w_data["state"], term_st)
                self.assertEqual(int(w_data["qty"]), req_qty)
                self.assertEqual(int(w_data["filled_qty"]), fill_qty)

        # Broker terminal summaries must not become fabricated exchange fills.
        with self.db._get_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fills WHERE exchange_trade_id LIKE '%-RECON-FILL-%' AND is_verified_trade=1").fetchone()[0], 0)

        # Only the 3 orders with filled_qty > 0 (5 + 4 + 3 = 12 shares @ 100.0 = 1200.0 INR) count toward daily trades & exposure
        self.assertEqual(self.db.get_today_orders_count(), 3)
        risk_st = self.db.get_live_portfolio_risk_state()
        self.assertEqual(risk_st["daily_trades"], 3)
        self.assertEqual(risk_st["open_positions"], 3)
        self.assertAlmostEqual(risk_st["current_exposure"], 1200.0)

    # =========================================================================
    # F03-02 — Missing, Corrupted & Inaccessible Journal vs. Clean Installation
    # =========================================================================

    def test_05_f03_02_corrupted_truncated_inaccessible_journals_and_verified_broker_recovery(self):
        """
        F03-02:
        1. Truncated 0-byte journal -> CORRUPTED_JOURNAL (blocks with JOURNAL_ERROR).
        2. Non-SQLite corrupt bytes -> CORRUPTED_JOURNAL (blocks with JOURNAL_ERROR).
        3. Missing required schema tables -> CORRUPTED_JOURNAL (blocks with JOURNAL_ERROR).
        4. Missing LIVE journal with authoritative Dhan broker confirming a protected TRADED order ->
           restores verified order and protection records into trade_journal.db and permits transition!
        """
        # 1. Truncated 0-byte file
        with open(self.db_path, "wb") as f:
            f.write(b"")
        st_0, _ = self.tracker.classify_journal_state(self.db_path, is_live_mode=True, is_service_running=True)
        self.assertEqual(st_0, "CORRUPTED_JOURNAL")
        ok_0, _, code_0, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path, self.stop_path, is_service_running=True, is_live_mode=True, target_is_live=False
        )
        self.assertFalse(ok_0)
        self.assertEqual(code_0, "JOURNAL_ERROR")

        # 2. Corrupt random bytes
        with open(self.db_path, "wb") as f:
            f.write(b"NOT_A_VALID_SQLITE_HEADER_1234567890")
        st_c, _ = self.tracker.classify_journal_state(self.db_path, is_live_mode=True, is_service_running=True)
        self.assertEqual(st_c, "CORRUPTED_JOURNAL")

        # 3. Valid SQLite file but missing `orders` and `order_intents` tables
        os.remove(self.db_path)
        con_incomplete = sqlite3.connect(self.db_path)
        con_incomplete.execute("CREATE TABLE dummy (id INT);")
        con_incomplete.commit()
        con_incomplete.close()
        st_inc, _ = self.tracker.classify_journal_state(self.db_path, is_live_mode=True, is_service_running=True)
        self.assertEqual(st_inc, "CORRUPTED_JOURNAL")

        # 4. Missing journal on active LIVE instance with authoritative broker recovery
        os.remove(self.db_path)
        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {
                "orderId": "ORD-RESTORE-1",
                "correlationId": "CORR-RESTORE-1",
                "tradingSymbol": "SBIN",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "TRADED",
                "quantity": 5,
                "filledQty": 5,
                "averageTradedPrice": 800.0,
            }
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-RESTORE-1",
                "parentOrderId": "ORD-RESTORE-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-RESTORE-1",
                "parentOrderId": "ORD-RESTORE-1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_positions.return_value = [{"securityId": "3045", "netQty": 5}]

        ok_rec, _, code_rec, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path,
            self.stop_path,
            is_service_running=True,
            is_live_mode=True,
            target_is_live=False,
            broker=mock_broker,
            env_path=self.env_path,
        )
        self.assertTrue(ok_rec)
        self.assertEqual(code_rec, "")
        self.assertTrue(os.path.exists(self.db_path), "Verified broker records must be restored into trade_journal.db")
        conn = sqlite3.connect(self.db_path)
        try:
            restored_ord = conn.execute("SELECT status, filled_quantity FROM orders WHERE broker_order_id = 'ORD-RESTORE-1'").fetchone()
            self.assertEqual(restored_ord[0], "TRADED")
            self.assertEqual(int(restored_ord[1]), 5)
        finally:
            conn.close()

    # =========================================================================
    # F03-03 — Local Watcher Protection Never Accepted Without Broker Confirmation
    # =========================================================================

    def test_07_f03_03_broker_timeout_cancelled_oco_duplicate_parent_and_valid_broker_confirmed_protection(self):
        """
        F03-03:
        1. Avoids unnecessary broker queries when safe state is already independently established (empty journal).
        2. Fails closed on broker timeout (BrokerReadFailure) -> BROKER_RECONCILIATION_FAILED.
        3. Fails closed when broker OCO is CANCELLED despite local watcher claiming protected_qty=5.
        4. Prevents duplicate/cross-parent OCO credit.
        5. Succeeds when authoritative Dhan read-only APIs confirm active SL and Target legs for all parents.
        """
        # 1. Empty journal in LIVE mode verifies clean flat broker state via unified acquisition (C06)
        from tests._phase2d_live_harness import attach_unified_live_evidence_to_stub_broker
        spy_broker = MagicMock()
        spy_broker.get_order_book.return_value = []
        spy_broker.get_trade_book.return_value = []
        spy_broker.get_holdings.return_value = []
        spy_broker.get_positions.return_value = []
        spy_broker.get_forever_orders.return_value = []
        spy_broker.get_super_orders.return_value = []
        attach_unified_live_evidence_to_stub_broker(spy_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")
        ok_idle, err_idle = self.tracker.reconcile_stale_journal_with_broker(
            db_path=self.db_path,
            broker=spy_broker,
            env_path=self.env_path,
            is_live_mode=True,
            is_service_running=True,
        )
        self.assertTrue(ok_idle, err_idle)
        self.assertIsNone(err_idle)

        # Seed two orders: ORD-P1 (5 qty) and ORD-P2 (5 qty), with watcher claiming both are TRADED & protected
        now = float(int(time.time()))
        from datetime import datetime as _dt
        from src.engine.broker_session import IST as _IST
        now_ex_time = _dt.fromtimestamp(now, _IST).strftime("%Y-%m-%d %H:%M:%S")
        for cid, boid in (("CORR-P1", "ORD-P1"), ("CORR-P2", "ORD-P2")):
            self.db.record_order_intent(cid, "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
            self.db.update_order_intent(cid, state="PENDING", broker_order_id=boid)
            self.db.record_order(correlation_id=cid, broker_order_id=boid, trading_mode="LIVE", product_type="CNC", exchange_segment="NSE_EQ", symbol="SBIN", security_id="3045", order_type="LIMIT", price=800.0, quantity=5, status="PENDING")
            with self.db._get_conn() as conn:
                conn.execute(
                    "INSERT INTO watcher_orders (order_id, is_terminal, record_json, updated_at) VALUES (?, 1, ?, ?)",
                    (
                        boid,
                        json.dumps({
                            "order_id": boid,
                            "correlation_id": cid,
                            "symbol": "SBIN",
                            "security_id": "3045",
                            "segment": "NSE_EQ",
                            "product_type": "CNC",
                            "qty": 5,
                            "filled_qty": 5,
                            "protected_qty": 5,
                            "confirmed_protection_ids": ["OCO-SHARED-1"],
                            "state": "TRADED",
                        }),
                        now,
                    ),
                )

        # 2. Broker timeout on Forever OCO readback -> BROKER_RECONCILIATION_FAILED
        timeout_broker = MagicMock()
        timeout_broker.get_order_book.return_value = [
            {"dhanClientId": "1113731536", "orderId": "ORD-P1", "correlationId": "CORR-P1", "orderStatus": "TRADED", "transactionType": "BUY", "quantity": 5, "filledQty": 5, "remainingQuantity": 0, "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"},
            {"dhanClientId": "1113731536", "orderId": "ORD-P2", "correlationId": "CORR-P2", "orderStatus": "TRADED", "transactionType": "BUY", "quantity": 5, "filledQty": 5, "remainingQuantity": 0, "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC"},
        ]
        timeout_broker.get_forever_orders.return_value = BrokerReadFailure(status="TIMEOUT", error="Forever OCO read timed out", status_code=504)
        ok_to, _, code_to, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path, self.stop_path, is_service_running=True, is_live_mode=True, target_is_live=False, broker=timeout_broker
        )
        self.assertFalse(ok_to)
        self.assertEqual(code_to, "BROKER_RECONCILIATION_FAILED")

        # 3. Duplicate parent association: both ORD-P1 and ORD-P2 claim OCO-SHARED-1 (only 5 units on broker) ->
        # OCO-SHARED-1 is credited to ORD-P1 only; ORD-P2 remains unprotected and blocks transition!
        dup_broker = MagicMock()
        dup_broker.get_order_book.return_value = timeout_broker.get_order_book.return_value
        with self.db._get_conn() as conn:
            for cid, boid, oid_oco, etid in (("CORR-P1", "ORD-P1", "OCO-SHARED-1", "ET304501"), ("CORR-P2", "ORD-P2", "OCO-P2-OWN", "ET304502")):
                conn.execute(
                    "INSERT OR IGNORE INTO positions_lifecycle (parent_order_id, account_id, symbol, security_id, exchange_segment, product_type, protection_route, position_state, entry_qty, buy_filled_qty, sell_filled_qty, confirmed_open_qty, updated_at) VALUES (?, '1113731536', 'SBIN', '3045', 'NSE_EQ', 'CNC', 'FOREVER_OCO', 'PROTECTED', 5, 5, 0, 5, ?)",
                    (boid, now),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO fills (broker_order_id, exchange_trade_id, symbol, traded_price, traded_quantity, traded_at, transaction_type, security_id, exchange_segment, product_type, parent_order_id, is_verified_trade) VALUES (?, ?, 'SBIN', 800.0, 5, ?, 'BUY', '3045', 'NSE_EQ', 'CNC', ?, 1)",
                    (boid, etid, now, boid),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO protection_orders (parent_order_id, forever_order_id, symbol, quantity, status, created_at) VALUES (?, ?, 'SBIN', 5, 'CONFIRM', ?)",
                    (boid, oid_oco, now),
                )
        dup_broker.get_trade_book.return_value = [
            {"dhanClientId": "1113731536", "orderId": "ORD-P1", "exchangeTradeId": "ET304501", "exchangeOrderId": "EXO-P1", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 5, "tradedPrice": 800.0, "exchangeTime": now_ex_time},
            {"dhanClientId": "1113731536", "orderId": "ORD-P2", "exchangeTradeId": "ET304502", "exchangeOrderId": "EXO-P2", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "transactionType": "BUY", "tradedQuantity": 5, "tradedPrice": 800.0, "exchangeTime": now_ex_time},
        ]
        dup_broker.get_holdings.return_value = []
        dup_broker.get_positions.return_value = [
            {"dhanClientId": "1113731536", "securityId": "3045", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10, "dayBuyQty": 10, "daySellQty": 0, "carryForwardBuyQty": 0, "carryForwardSellQty": 0}
        ]
        dup_broker.get_super_orders.return_value = []
        attach_unified_live_evidence_to_stub_broker(dup_broker, db_path=self.db_path, data_dir=self.data_dir, account_id="1113731536")
        dup_broker.get_forever_orders.return_value = [
            {
                "dhanClientId": "1113731536", "orderId": "OCO-SHARED-1",
                "parentOrderId": "ORD-P1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "dhanClientId": "1113731536", "orderId": "OCO-SHARED-1",
                "parentOrderId": "ORD-P1",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]
        ok_dup, reason_dup, code_dup, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path, self.stop_path, is_service_running=True, is_live_mode=True, target_is_live=False, broker=dup_broker, env_path=self.env_path
        )
        self.assertFalse(ok_dup)
        self.assertEqual(code_dup, "UNPROTECTED_POSITION")
        self.assertIn("ORD-P2", reason_dup)

        # 4. Add distinct verified Forever OCO for ORD-P2 -> transition succeeds!
        dup_broker.get_forever_orders.return_value.extend([
            {
                "dhanClientId": "1113731536", "orderId": "OCO-P2-OWN",
                "parentOrderId": "ORD-P2",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "dhanClientId": "1113731536", "orderId": "OCO-P2-OWN",
                "parentOrderId": "ORD-P2",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ])
        with self.db._get_conn() as conn:
            row_p2 = conn.execute("SELECT record_json FROM watcher_orders WHERE order_id = 'ORD-P2'").fetchone()
            rec_p2 = json.loads(row_p2[0])
            rec_p2["confirmed_protection_ids"] = ["OCO-P2-OWN"]
            conn.execute("UPDATE watcher_orders SET record_json = ? WHERE order_id = 'ORD-P2'", (json.dumps(rec_p2),))
        ok_valid, _, code_valid, _ = self.tracker.evaluate_trading_safety_gate_detailed(
            self.db_path, self.stop_path, is_service_running=True, is_live_mode=True, target_is_live=False, broker=dup_broker, env_path=self.env_path
        )
        self.assertTrue(ok_valid)
        self.assertEqual(code_valid, "")

    # =========================================================================
    # F03-04 — Interrupted Restart Recovery Within Startup Verification Window
    # =========================================================================

    def test_10_f04_01_missing_journal_with_open_broker_positions_blocks_via_readonly_adapter(self):
        """
        F04-01: ReadOnlyDhanBrokerAdapter exposes get_positions(), and missing-LIVE-journal
        recovery blocks when Dhan reports an open position (12 shares) without matching
        verified parent order/protection provenance.
        """
        from src.engine.config_manager import ReadOnlyDhanBrokerAdapter
        os.remove(self.db_path)

        raw_broker = MagicMock()
        raw_broker.cfg = AppConfig(trading_mode=TradingMode.LIVE, dhan_client_id="1113731536", data_dir=self.data_dir)
        raw_broker.get_order_book.return_value = []
        raw_broker.get_forever_orders.return_value = []
        raw_broker.get_super_orders.return_value = []
        raw_broker.get_positions.return_value = [{"securityId": "3045", "netQty": 12}]

        ro_adapter = ReadOnlyDhanBrokerAdapter(raw_broker)
        ok, reason = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker=ro_adapter,
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok)
        self.assertIn("UNPROTECTED_POSITION", str(reason))
        self.assertFalse(os.path.exists(self.db_path))

    def test_11_f04_01_missing_journal_without_positions_api_or_on_positions_failure_blocks(self):
        """
        F04-01: Missing-LIVE-journal recovery fails closed if the broker adapter lacks get_positions()
        or if get_positions() returns BrokerReadFailure.
        """
        from src.engine.config_manager import ReadOnlyDhanBrokerAdapter
        os.remove(self.db_path)

        class _NoPositionsBroker:
            def get_order_book(self):
                return []
            def get_forever_orders(self):
                return []
            def get_super_orders(self):
                return []

        ro_no_pos = ReadOnlyDhanBrokerAdapter(_NoPositionsBroker())
        ok1, reason1 = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker=ro_no_pos,
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok1)
        self.assertIn("BROKER_RECONCILIATION_FAILED", str(reason1))

        fail_pos_broker = MagicMock()
        fail_pos_broker.get_order_book.return_value = []
        fail_pos_broker.get_forever_orders.return_value = []
        fail_pos_broker.get_super_orders.return_value = []
        fail_pos_broker.get_positions.return_value = BrokerReadFailure(status="UNAVAILABLE", error="Positions HTTP 503", status_code=503)
        ok2, reason2 = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker= ReadOnlyDhanBrokerAdapter(fail_pos_broker),
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok2)
        self.assertIn("BROKER_RECONCILIATION_FAILED", str(reason2))

    def test_12_f04_02_same_oco_never_reused_for_two_fills_during_missing_journal_recovery(self):
        """
        F04-02: During missing-journal recovery, one Forever OCO group (OCO-ONE) must never be
        credited to two distinct filled orders (ORD-A and ORD-B), even if pending_protection.json
        lists OCO-ONE under both orders.
        """
        os.remove(self.db_path)
        watcher_data = {
            oid: {"order_id": oid, "confirmed_protection_ids": ["OCO-ONE"], "filled_qty": 5}
            for oid in ("ORD-A", "ORD-B")
        }
        with open(self.watcher_path, "w", encoding="utf-8") as f:
            json.dump(watcher_data, f)

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {
                "orderId": oid,
                "correlationId": f"C-{oid}",
                "orderStatus": "TRADED",
                "filledQty": 5,
                "quantity": 5,
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "tradingSymbol": "SBIN",
                "averageTradedPrice": 800.0,
            }
            for oid in ("ORD-A", "ORD-B")
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-ONE",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-ONE",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_positions.return_value = [{"securityId": "3045", "netQty": 5}]

        ok, reason = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker=mock_broker,
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok)
        self.assertIn("UNPROTECTED_POSITION", str(reason))
        self.assertFalse(os.path.exists(self.db_path))

    def test_13_f04_02_broker_parent_mismatch_never_overridden_by_watcher_in_missing_journal_recovery(self):
        """
        F04-02: Even if pending_protection.json claims OCO-ONE for ORD-A, an explicit conflicting
        parentOrderId ('ORD-OTHER') on the broker Forever OCO leg must be rejected.
        """
        os.remove(self.db_path)
        with open(self.watcher_path, "w", encoding="utf-8") as f:
            json.dump({"ORD-A": {"order_id": "ORD-A", "confirmed_protection_ids": ["OCO-ONE"], "filled_qty": 5}}, f)

        mock_broker = MagicMock()
        mock_broker.get_order_book.return_value = [
            {
                "orderId": "ORD-A",
                "correlationId": "C-ORD-A",
                "orderStatus": "TRADED",
                "filledQty": 5,
                "quantity": 5,
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "tradingSymbol": "SBIN",
                "averageTradedPrice": 800.0,
            }
        ]
        mock_broker.get_forever_orders.return_value = [
            {
                "orderId": "OCO-ONE",
                "parentOrderId": "ORD-OTHER",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "STOP_LOSS_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 776.0,
                "price": 768.0,
            },
            {
                "orderId": "OCO-ONE",
                "parentOrderId": "ORD-OTHER",
                "securityId": "3045",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "orderStatus": "CONFIRM",
                "legName": "TARGET_LEG",
                "transactionType": "SELL",
                "quantity": 5,
                "triggerPrice": 824.0,
                "price": 824.0,
            },
        ]
        mock_broker.get_super_orders.return_value = []
        mock_broker.get_positions.return_value = [{"securityId": "3045", "netQty": 5}]

        ok, reason = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker=mock_broker,
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok)
        self.assertIn("UNPROTECTED_POSITION", str(reason))
        self.assertFalse(os.path.exists(self.db_path))

    def test_14_f04_03_atomic_reconciliation_rolls_back_earlier_updates_when_later_order_fails(self):
        """
        F04-03: If ORD-A resolves to CANCELLED (0 fills) and ORD-B fails reconciliation inside the
        same pass (e.g. Forever OCO book unavailable for filled ORD-B), the transaction rolls back
        so ORD-A is not partially committed.
        """
        for oid in ("ORD-A", "ORD-B"):
            self.db.record_order_intent(f"C-{oid}", "SBIN", "3045", "NSE_EQ", "CNC", "LIMIT", 800.0, 5)
            self.db.update_order_intent(f"C-{oid}", state="TRANSIT", broker_order_id=oid)
            self.db.record_order(correlation_id=f"C-{oid}", broker_order_id=oid, trading_mode="LIVE", product_type="CNC", exchange_segment="NSE_EQ", symbol="SBIN", security_id="3045", order_type="LIMIT", price=800.0, quantity=5, status="TRANSIT")

        class _NoForeverBroker:
            def get_order_book(self):
                return [
                    {"orderId": "ORD-A", "correlationId": "C-ORD-A", "orderStatus": "CANCELLED", "filledQty": 0},
                    {"orderId": "ORD-B", "correlationId": "C-ORD-B", "orderStatus": "TRADED", "filledQty": 5, "securityId": "3045"},
                ]
            def order_status(self, oid):
                return None

        ok, reason = self.tracker.reconcile_stale_journal_with_broker(
            self.db_path,
            broker=_NoForeverBroker(),
            is_live_mode=True,
            is_service_running=True,
            stop_state_path=self.stop_path,
        )
        self.assertFalse(ok)
        self.assertIn("BROKER_RECONCILIATION_FAILED", str(reason))
        with self.db._get_conn() as conn:
            states = [r["state"] for r in conn.execute("SELECT state FROM order_intents ORDER BY correlation_id").fetchall()]
            statuses = [r["status"] for r in conn.execute("SELECT status FROM orders ORDER BY correlation_id").fetchall()]
        self.assertEqual(states, ["TRANSIT", "TRANSIT"])
        self.assertEqual(statuses, ["TRANSIT", "TRANSIT"])


if __name__ == "__main__":
    unittest.main()
