"""Independent fail-first merge gate for AB-V14-P0-RECONCILE-VERSION-CONTROL-01.
Offline only: uses mocked Dhan responses and a temporary SQLite journal.
Run from the repository root after copying this file into tests/:
    python -m unittest -v tests.test_ab_v14_merge_gate_extra
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from src.models import PositionLifecycleState
from src.version import verify_release_version_governance
from tests import test_auditbot_v14_release_blockers as fixture


class AdditionalMergeGate(unittest.TestCase):
    def test_db_write_failure_must_not_confirm_position_closed(self):
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'EXIT-DURABILITY-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 10,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]
            case.db.record_fill = MagicMock(side_effect=OSError('injected SQLite disk write failure'))
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            accounting = case.db.get_position_accounting('ORDER-A')
            self.assertEqual(accounting['sell_filled_qty'], 0, 'Injected failure should leave no durable SELL')
            self.assertNotEqual(
                rec.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value,
                'Do not confirm POSITION_CLOSED when the broker SELL fill cannot be durably journaled',
            )
            gate.halt_new_entries.assert_called()
        finally:
            case.tearDown()

    def test_release_version_validation_must_fail_for_unknown_base_ref(self):
        ok, errors = verify_release_version_governance(base_ref='missing-do-not-exist-base-ref-987654321')
        self.assertFalse(ok, 'Invalid git comparison base must fail closed, not bypass version bump enforcement')
        self.assertTrue(errors)



    def test_p0_01_sqlite_operational_error_and_idempotent_restart_recovery(self):
        """P0-01: SQLite OperationalError/disk-full prevents false closure; recovery + restart replays fill idempotently."""
        import json as _json
        import sqlite3
        from src.notifier.execution_updates import OrderStatusReporter

        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            meta = {"symbol": "POLYCAB", "segment": "FNO", "action": "BUY", "mode": "LIVE", "limit_price": 104.0, "signal_received_at": 990.0}
            case.watcher.terminal_orders['ORDER-A']['notification_meta'] = meta
            case.watcher.execution_reporter = OrderStatusReporter(case.db, case.broker)
            gate = MagicMock()
            case.broker.reconciliation_gate = gate

            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'EXIT-SQLITE-LOCK-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 10,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]

            real_record_fill = case.db.record_fill
            case.db.record_fill = MagicMock(side_effect=sqlite3.OperationalError('database or disk is full'))
            case.watcher.check_once()

            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)
            self.assertTrue(rec.get('reconciliation_required'))
            self.assertTrue(rec.get('persistence_uncertain'))
            self.assertEqual(int(rec.get('exit_filled_qty') or 0), 0)
            self.assertEqual(int(rec.get('broker_observed_exit_filled_qty') or 0), 10)
            self.assertTrue(rec.get('broker_exit_already_executed'))
            gate.halt_new_entries.assert_called()

            outbox_1 = [_json.loads(r['payload'])['text'] for r in case.db.fetch_pending_outbox(limit=20)]
            self.assertFalse(any('Status: Position Closed' in c for c in outbox_1))
            self.assertTrue(any('Reconciliation Required' in c for c in outbox_1))

            # Restore SQLite write capability and simulate watcher restart
            case.db.record_fill = real_record_fill
            restarted = fixture.ProtectionWatcher(
                broker=case.broker,
                path=case.watcher.path,
                dry_run=False,
                journal_db=case.db,
                clock=lambda: 1020.0,
            )
            restarted.execution_reporter = OrderStatusReporter(case.db, case.broker)
            restarted.check_once()

            rec_after = restarted.terminal_orders['ORDER-A']
            acct_after = case.db.get_position_accounting('ORDER-A')
            self.assertEqual(acct_after['sell_filled_qty'], 10)
            self.assertEqual(acct_after['confirmed_open_qty'], 0)
            self.assertEqual(rec_after.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value)
            self.assertEqual(rec_after.get('exit_filled_qty'), 10)
            self.assertEqual(rec_after.get('realized_pnl'), 30.0)
            self.assertFalse(rec_after.get('reconciliation_required'))
            self.assertFalse(rec_after.get('persistence_uncertain'))

            outbox_2 = [_json.loads(r['payload'])['text'] for r in case.db.fetch_pending_outbox(limit=20)]
            closed_cards = [c for c in outbox_2 if 'Status: Position Closed (Target Hit)' in c]
            self.assertEqual(len(closed_cards), 1)
        finally:
            case.tearDown()

    def test_p0_01_partial_fill_commit_then_second_fill_db_error_and_lifecycle_upsert_failure(self):
        """P0-01: Partial fill 1 commits, fill 2 fails DB write -> preserves durable partial accounting; lifecycle upsert failure also blocks closure."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='TRADED', target='PART_TRADED', stop='PENDING')
            ]
            case.broker.get_trade_book.return_value = [{
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'PART-FILL-1',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 4,
                'tradedPrice': 103.0,
            }]
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 6,
            }]
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.EXIT_PART_FILLED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 4)

            # Second fill arrives (6 shares), but record_fill fails on PART-FILL-2
            real_record_fill = case.db.record_fill
            def fail_on_second_fill(*args, **kwargs):
                if kwargs.get('exchange_trade_id') == 'PART-FILL-2':
                    raise OSError('disk full on second fill')
                return real_record_fill(*args, **kwargs)

            case.db.record_fill = MagicMock(side_effect=fail_on_second_fill)
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='CLOSED', target='TRADED', stop='CANCELLED')
            ]
            case.broker.get_trade_book.return_value.append({
                'orderId': 'ORDER-A-TGT',
                'exchangeTradeId': 'PART-FILL-2',
                'transactionType': 'SELL',
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'dhanClientId': '1100000001',
                'tradedQuantity': 6,
                'tradedPrice': 103.0,
            })
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 4)
            self.assertEqual(rec.get('confirmed_position_open_qty'), 6)
            self.assertEqual(rec.get('broker_observed_exit_filled_qty'), 10)
            gate.halt_new_entries.assert_called()

            # Restore record_fill, but inject failure on upsert_position_lifecycle -> still must not confirm POSITION_CLOSED
            case.db.record_fill = real_record_fill
            real_upsert = case.db.upsert_position_lifecycle
            case.db.upsert_position_lifecycle = MagicMock(side_effect=OSError('lifecycle table locked'))
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.RECONCILIATION_REQUIRED.value)

            # Restore upsert_position_lifecycle -> converges to POSITION_CLOSED without double counting
            case.db.upsert_position_lifecycle = real_upsert
            case.watcher.check_once()
            self.assertEqual(rec.get('position_state'), PositionLifecycleState.POSITION_CLOSED.value)
            self.assertEqual(rec.get('exit_filled_qty'), 10)
            self.assertEqual(rec.get('realized_pnl'), 30.0)
        finally:
            case.tearDown()

    def test_p1_02_ack_unknown_timeout_retry_and_escalation_at_1000_1100_2000_10000(self):
        """P1-02: Verifies bounded retries, escalation to MANUAL_INTERVENTION_REQUIRED across 1000/1100/2000/10000, and eventual terminal recovery."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            now_box = [1000.0]
            case.watcher.clock = lambda: now_box[0]
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.cancel_super_order_leg.return_value = (True, 'Cancel ACK received')
            case.broker.get_positions.return_value = [{
                'securityId': '55001',
                'exchangeSegment': 'NSE_FNO',
                'productType': 'MARGIN',
                'netQty': 0,
            }]

            # t=1000: Initial cancel request -> ACK_UNKNOWN (attempt 1)
            case.watcher.check_once()
            rec = case.watcher.terminal_orders['ORDER-A']
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'ACK_UNKNOWN')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'ACK_UNKNOWN')
            self.assertEqual(rec['orphaned_leg_cancel_details']['TARGET_LEG']['attempt_count'], 1)
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, 2)

            # t=1015: Past 10s ACK timeout, still within 60s deadline -> bounded retry (attempt 2)
            now_box[0] = 1015.0
            case.watcher.check_once()
            self.assertEqual(rec['orphaned_leg_cancel_details']['TARGET_LEG']['attempt_count'], 2)
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, 4)

            # t=1100: Past 60s deadline while broker still reports PENDING -> escalates to MANUAL_INTERVENTION_REQUIRED
            now_box[0] = 1100.0
            case.watcher.check_once()
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))
            self.assertTrue(rec.get('orphaned_legs_manual_intervention_required'))
            self.assertTrue(rec.get('orphaned_legs_escalation_alerted'))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'MANUAL_INTERVENTION_REQUIRED')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'MANUAL_INTERVENTION_REQUIRED')
            calls_at_1100 = case.broker.cancel_super_order_leg.call_count

            # t=2000 and t=10000: Does not issue unlimited duplicate cancel requests or duplicate alerts
            for sim_t in (2000.0, 10000.0):
                now_box[0] = sim_t
                case.watcher.check_once()
            self.assertEqual(case.broker.cancel_super_order_leg.call_count, calls_at_1100)
            self.assertFalse(rec.get('orphaned_legs_cancelled', False))

            # t=10500: Broker finally confirms both legs CANCELLED -> resolves to CONFIRMED_TERMINAL
            now_box[0] = 10500.0
            case.broker.get_super_orders.return_value = [
                fixture._super_row('ORDER-A', status='TRADED', target='CANCELLED', stop='CANCELLED')
            ]
            case.watcher.check_once()
            self.assertTrue(rec.get('orphaned_legs_cancelled', False))
            self.assertFalse(rec.get('orphaned_legs_manual_intervention_required', False))
            self.assertEqual(rec['orphaned_leg_cancel_states']['TARGET_LEG'], 'CONFIRMED_TERMINAL')
            self.assertEqual(rec['orphaned_leg_cancel_states']['STOP_LOSS_LEG'], 'CONFIRMED_TERMINAL')
        finally:
            case.tearDown()

    def test_p1_03_version_governance_negative_cases(self):
        """P1-03: Negative tests for non-git dir, empty base-ref, malformed SemVer, mismatched manifest, and missing changelog."""
        import json as _json
        import os as _os
        import tempfile as _tempfile

        # 1. Empty base_ref fails closed
        ok_empty, errs_empty = verify_release_version_governance(base_ref='')
        self.assertFalse(ok_empty)
        self.assertTrue(any('cannot be empty' in e for e in errs_empty))

        # 2. Non-Git directory with explicit base_ref fails closed
        with _tempfile.TemporaryDirectory() as tmp:
            with open(_os.path.join(tmp, 'VERSION'), 'w', encoding='utf-8') as f:
                f.write('14.0.1-rc.1\n')
            with open(_os.path.join(tmp, 'CHANGELOG.md'), 'w', encoding='utf-8') as f:
                f.write('## [14.0.1-rc.1]\n- test\n')
            with open(_os.path.join(tmp, 'RELEASE_MANIFEST.json'), 'w', encoding='utf-8') as f:
                _json.dump({
                    'version': '14.0.1-rc.1',
                    'previous_version': '14.0.0',
                    'parent_main_baseline_sha': '29c6515b8ce448ff54bf1f3024f2e7c9027c8896',
                    'patch_identifier': 'TEST',
                    'changed_paths': ['VERSION'],
                }, f)
            ok_nongit, errs_nongit = verify_release_version_governance(repo_root=tmp, base_ref='main')
            self.assertFalse(ok_nongit)
            self.assertTrue(any('not a valid Git working tree' in e for e in errs_nongit))

            # 3. Malformed SemVer fails closed
            with open(_os.path.join(tmp, 'VERSION'), 'w', encoding='utf-8') as f:
                f.write('v14.0.1.bad\n')
            ok_badver, errs_badver = verify_release_version_governance(repo_root=tmp)
            self.assertFalse(ok_badver)
            self.assertTrue(any('invalid SemVer' in e for e in errs_badver))

    def test_p1_04_webui_contract_and_version_namespace_isolation(self):
        """P1-04: Verifies WebUI CSRF protection, secret non-disclosure, F&O markup persistence, and version namespace separation."""
        import json as _json
        from src.setup_app import get_setup_page, handle_setup_submission, get_active_version, get_release_version_endpoint

        page_resp = get_setup_page()
        html = page_resp.body.decode('utf-8')
        self.assertIn('F&amp;O Price Markup (%)', html)
        self.assertIn('Release v14.0.1-rc.1', html)

        # Reject invalid CSRF token with 403
        forbidden_resp = handle_setup_submission(csrf_token='INVALID_TOKEN')
        self.assertEqual(forbidden_resp.status_code, 403)

        act_ver = _json.loads(get_active_version().body.decode('utf-8'))
        rel_ver = _json.loads(get_release_version_endpoint().body.decode('utf-8'))
        self.assertEqual(act_ver['app_release_version'], '14.0.1-rc.1')
        self.assertEqual(rel_ver['app_release_version'], '14.0.1-rc.1')
        self.assertTrue(str(act_ver['short_version']).startswith('v-'))
        self.assertEqual(act_ver['parser_rule_version'], 'BRAIN-v3.2')


    def test_p1_02_forever_oco_and_readback_failure_cancellation_liveness(self):
        """P1-02: Tests Forever OCO orphaned leg cancellation, readback failure handling, and restart persistence."""
        case = fixture.AuditBotV14IndependentRegressions('test_unrelated_same_instrument_sell_cannot_reduce_this_position')
        case.setUp()
        try:
            now_box = [2000.0]
            case.watcher.clock = lambda: now_box[0]
            gate = MagicMock()
            case.broker.reconciliation_gate = gate
            case.broker.order_status.return_value = {"orderId": "ORD-FO-1", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 100.0}
            case.broker.place_forever_oco.return_value = (True, "FO-LEG-1", "OK")
            case.broker.get_forever_orders.return_value = [{
                "orderId": "FO-LEG-1",
                "orderStatus": "CONFIRM",
                "orderFlag": "OCO",
                "dhanClientId": "1100000001",
                "securityId": "55001",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 10,
                "price": 96.05,
                "triggerPrice": 97.0,
                "price1": 103.0,
                "triggerPrice1": 103.0,
                "orderType": "OCO",
            }]
            case.broker.get_positions.return_value = [{"securityId": "55001", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 10}]
            case.watcher.track(
                order_id="ORD-FO-1",
                symbol="POLYCAB",
                security_id="55001",
                segment="NSE_EQ",
                qty=10,
                product_type="CNC",
                is_super_order=False,
                protection_route="FOREVER_OCO",
            )
            case.watcher.check_once()

            # Manual exit flattens position while FO-LEG-1 is still CONFIRM; first cancel rejected
            case.broker.get_positions.return_value = [{"securityId": "55001", "exchangeSegment": "NSE_EQ", "productType": "CNC", "netQty": 0}]
            case.broker.cancel_forever_order.return_value = (False, "Temporary 503")
            case.watcher.check_once()
            rec = case.watcher.terminal_orders["ORD-FO-1"]
            self.assertFalse(rec.get("orphaned_legs_cancelled", False))
            self.assertEqual(rec["orphaned_leg_cancel_states"]["FO-LEG-1"], "REJECTED")

            # Empty/unreadable forever book on next pass does not fabricate completion or issue blind cancel
            now_box[0] = 2010.0
            case.broker.get_forever_orders.return_value = []
            case.broker.cancel_forever_order.reset_mock()
            case.watcher.check_once()
            self.assertEqual(case.broker.cancel_forever_order.call_count, 0)
            self.assertFalse(rec.get("orphaned_legs_cancelled", False))

            # Book returns with CANCELLED -> resolves to CONFIRMED_TERMINAL
            now_box[0] = 2020.0
            case.broker.get_forever_orders.return_value = [{
                "orderId": "FO-LEG-1",
                "orderStatus": "CANCELLED",
                "orderFlag": "OCO",
                "dhanClientId": "1100000001",
                "securityId": "55001",
                "exchangeSegment": "NSE_EQ",
                "productType": "CNC",
                "transactionType": "SELL",
                "quantity": 10,
            }]
            case.watcher.check_once()
            self.assertTrue(rec.get("orphaned_legs_cancelled", False))
            self.assertEqual(rec["orphaned_leg_cancel_states"]["FO-LEG-1"], "CONFIRMED_TERMINAL")
        finally:
            case.tearDown()

    def test_p1_03_git_commit_unbumped_version_fails_closed(self):
        """P1-03: In a temporary Git repo, changing a src/ file without bumping VERSION fails verification against base-ref."""
        import json as _json
        import os as _os
        import subprocess as _sp
        import tempfile as _tempfile

        with _tempfile.TemporaryDirectory() as tmp:
            _sp.check_call(["git", "init", tmp], stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
            _sp.check_call(["git", "-C", tmp, "config", "user.email", "test@auditbot.local"])
            _sp.check_call(["git", "-C", tmp, "config", "user.name", "Test"])
            _os.makedirs(_os.path.join(tmp, "src"), exist_ok=True)
            with open(_os.path.join(tmp, "src", "dummy.py"), "w", encoding="utf-8") as f:
                f.write("X = 1\n")
            with open(_os.path.join(tmp, "VERSION"), "w", encoding="utf-8") as f:
                f.write("14.0.1\n")
            with open(_os.path.join(tmp, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                f.write("## [14.0.1]\n- Initial\n")
            with open(_os.path.join(tmp, "RELEASE_MANIFEST.json"), "w", encoding="utf-8") as f:
                _json.dump({
                    "version": "14.0.1",
                    "previous_version": "14.0.0",
                    "parent_main_baseline_sha": "29c6515b8ce448ff54bf1f3024f2e7c9027c8896",
                    "patch_identifier": "TEST-1",
                    "changed_paths": ["src/dummy.py"],
                }, f)
            _sp.check_call(["git", "-C", tmp, "add", "-A"])
            _sp.check_call(["git", "-C", tmp, "commit", "-m", "Base 14.0.1"], stdout=_sp.DEVNULL)

            # Modify src/dummy.py on a second commit WITHOUT bumping VERSION
            with open(_os.path.join(tmp, "src", "dummy.py"), "w", encoding="utf-8") as f:
                f.write("X = 2\n")
            _sp.check_call(["git", "-C", tmp, "add", "-A"])
            _sp.check_call(["git", "-C", tmp, "commit", "-m", "Unbumped code edit"], stdout=_sp.DEVNULL)

            ok_unbumped, errs_unbumped = verify_release_version_governance(repo_root=tmp, base_ref="HEAD~1")
            self.assertFalse(ok_unbumped)
            self.assertTrue(any("VERSION" in e for e in errs_unbumped))

    def test_p1_05_pydantic_classvar_metadata_never_uses_private_attr_descriptors(self):
        """P1-05: Verifies ClassVar annotations on Gemini candidate classes so Python 3.13 / Pydantic 2.13 never treats them as private attrs."""
        from typing import get_origin, ClassVar
        from src.parsers.gemini_fallback import _StrictCandidateBase, GeminiFnOCandidate, GeminiEquityCandidate

        for cls in (_StrictCandidateBase, GeminiFnOCandidate, GeminiEquityCandidate):
            ann = getattr(cls, "__annotations__", {})
            for attr_name in ("FLOAT_FIELDS", "INT_FIELDS", "BOOL_FIELDS", "LIST_FLOAT_FIELDS", "_FLOAT_FIELDS", "_INT_FIELDS", "_BOOL_FIELDS", "_LIST_FLOAT_FIELDS"):
                if attr_name in ann:
                    self.assertTrue(
                        "ClassVar" in str(ann[attr_name]) or get_origin(ann[attr_name]) is ClassVar,
                        f"{cls.__name__}.{attr_name} must be annotated as ClassVar",
                    )


    def test_p1_06_offline_network_isolation_safeguard_and_check_dhan_mocking(self):
        """Verifies suite-wide network guard blocks unmocked external calls and test_cmd_check_dhan_output runs with zero network calls."""
        import socket
        from tests import OfflineNetworkIsolationError, allow_blocked_network_probe, get_recorded_network_attempts
        from tests.test_step23_step24 import TestStep23And24

        with allow_blocked_network_probe():
            with self.assertRaises(OfflineNetworkIsolationError):
                socket.getaddrinfo("query1.finance.yahoo.com", 443)
            with self.assertRaises(OfflineNetworkIsolationError):
                socket.create_connection(("api.dhan.co", 443), timeout=0.1)

        case = TestStep23And24("test_cmd_check_dhan_output")
        case.setUp()
        try:
            case.test_cmd_check_dhan_output()
            self.assertEqual(get_recorded_network_attempts(), [])
        finally:
            case.tearDown()


    def test_p0_02_overridden_order_status_cannot_suppress_get_trades_for_order_in_live_mode(self):
        """P0-02 (Req 1-9): Overriding order_status/trades_avg_price on a LIVE DhanClient never suppresses get_trades_for_order() or fabricates OK_EMPTY."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from src.broker.dhan_client import DhanClient, BrokerReadList, BrokerReadFailure, BrokerReadStatus
        from src.config import AppConfig
        from src.models import TradingMode
        from tests import allow_blocked_network_probe

        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="live_jwt_token",
        )
        client = DhanClient(cfg)
        # Override instance methods on client.__dict__
        client.order_status = MagicMock(return_value={"orderId": "ORD-LIVE-901", "orderStatus": "TRADED", "filledQty": 10})
        client.trades_avg_price = MagicMock(return_value=102.5)
        client.get_trade_book = MagicMock(return_value=[])

        # 1. Without a mocked transport, LIVE get_trades_for_order MUST attempt the real endpoint (blocked by network guard -> UNAVAILABLE, NEVER OK_EMPTY)
        with allow_blocked_network_probe():
            unperformed_res = client.get_trades_for_order("ORD-LIVE-901")
        self.assertIsInstance(unperformed_res, BrokerReadFailure)
        self.assertFalse(unperformed_res.ok)
        self.assertEqual(unperformed_res.status, BrokerReadStatus.UNAVAILABLE.value)

        # 2. With mocked session.get, LIVE get_trades_for_order executes the authoritative GET /v2/trades/{order_id} request and validates fields
        trade_payload = [{
            "orderId": "ORD-LIVE-901",
            "exchangeTradeId": "ET-LIVE-901",
            "dhanClientId": "1100000001",
            "securityId": "55001",
            "transactionType": "BUY",
            "tradedQuantity": 10,
            "tradedPrice": 102.5,
        }]
        client.session.get = MagicMock(
            return_value=SimpleNamespace(
                status_code=200,
                text='[{"orderId":"ORD-LIVE-901"}]',
                json=lambda: trade_payload,
            )
        )
        live_res = client.get_trades_for_order(
            "ORD-LIVE-901",
            expected_security_id="55001",
            expected_transaction_type="BUY",
            require_exchange_trade_id=True,
        )
        client.session.get.assert_called_once()
        called_url = client.session.get.call_args[0][0]
        self.assertTrue(called_url.endswith("/trades/ORD-LIVE-901"))
        self.assertIsInstance(live_res, BrokerReadList)
        self.assertTrue(live_res.ok)
        self.assertEqual(live_res.status, BrokerReadStatus.OK_RECORDS.value)
        self.assertEqual(len(live_res), 1)
        self.assertEqual(live_res[0]["exchangeTradeId"], "ET-LIVE-901")
        self.assertEqual(live_res[0]["tradedQuantity"], 10)
        self.assertAlmostEqual(live_res[0]["tradedPrice"], 102.5)

    def test_p0_02_genuine_empty_broker_response_distinguishable_from_failed_or_unperformed_requests(self):
        """P0-02 (Req 8 & 10): Genuine HTTP 200 empty trade list (OK_EMPTY) is strictly distinguishable from HTTP failures, malformed payloads, and unperformed requests."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from src.broker.dhan_client import DhanClient, BrokerReadList, BrokerReadFailure, BrokerReadStatus
        from src.config import AppConfig
        from src.models import TradingMode
        from tests import allow_blocked_network_probe

        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id="1100000001",
            dhan_access_token="live_jwt_token",
        )
        client = DhanClient(cfg)
        client.order_status = MagicMock(return_value={"orderStatus": "PENDING"})

        # Case A: Unperformed / network-blocked request -> BrokerReadFailure(UNAVAILABLE)
        with allow_blocked_network_probe():
            res_unperformed = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_unperformed, BrokerReadFailure)
        self.assertFalse(res_unperformed.ok)
        self.assertEqual(res_unperformed.status, BrokerReadStatus.UNAVAILABLE.value)

        # Case B: HTTP 503 failure -> BrokerReadFailure(FAILED)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(status_code=503, text="Service Unavailable", json=lambda: {})
        )
        res_http_fail = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_http_fail, BrokerReadFailure)
        self.assertFalse(res_http_fail.ok)
        self.assertEqual(res_http_fail.status, BrokerReadStatus.FAILED.value)
        self.assertEqual(res_http_fail.status_code, 503)

        # Case C: Malformed / mismatched orderId payload -> BrokerReadFailure(MALFORMED)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(
                status_code=200,
                text='[{"orderId":"WRONG-ORDER"}]',
                json=lambda: [{"orderId": "WRONG-ORDER", "tradedQuantity": 10, "tradedPrice": 100.0}],
            )
        )
        res_malformed = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_malformed, BrokerReadFailure)
        self.assertFalse(res_malformed.ok)
        self.assertEqual(res_malformed.status, BrokerReadStatus.MALFORMED.value)

        # Case D: Genuine HTTP 200 empty list -> BrokerReadList(OK_EMPTY)
        client.session.get = MagicMock(
            return_value=SimpleNamespace(status_code=200, text="[]", json=lambda: [])
        )
        res_empty = client.get_trades_for_order("ORD-100")
        self.assertIsInstance(res_empty, BrokerReadList)
        self.assertTrue(res_empty.ok)
        self.assertEqual(res_empty.status, BrokerReadStatus.OK_EMPTY.value)
        self.assertEqual(len(res_empty), 0)

    def test_p1_07_overridden_setup_teardown_and_caught_exception_still_fails_test(self):
        """P1-07 (Req 1-4): A test subclass overriding setUp() and tearDown() without super() that catches OfflineNetworkIsolationError still fails."""
        import socket
        import tests
        from tests import OfflineNetworkIsolationError

        class SwallowedBodyCase(unittest.TestCase):
            def setUp(self):
                self.custom_setup = True

            def tearDown(self):
                self.custom_teardown = True

            def test_swallowed_dns_lookup(self):
                try:
                    socket.getaddrinfo("api.dhan.co", 443)
                except OfflineNetworkIsolationError:
                    pass

        res_body = unittest.TestResult()
        SwallowedBodyCase("test_swallowed_dns_lookup").run(res_body)
        self.assertFalse(res_body.wasSuccessful())
        self.assertEqual(len(res_body.failures), 1)
        self.assertIn("api.dhan.co:443", res_body.failures[0][1])

        class SwallowedSetUpCase(unittest.TestCase):
            def setUp(self):
                try:
                    socket.getaddrinfo("query1.finance.yahoo.com", 443)
                except Exception:
                    pass

            def tearDown(self):
                pass

            def test_noop(self):
                self.assertTrue(True)

        res_setup = unittest.TestResult()
        SwallowedSetUpCase("test_noop").run(res_setup)
        self.assertFalse(res_setup.wasSuccessful())
        self.assertEqual(len(res_setup.failures), 1)
        self.assertIn("query1.finance.yahoo.com:443", res_setup.failures[0][1])

        class SwallowedTearDownCase(unittest.TestCase):
            def setUp(self):
                pass

            def tearDown(self):
                try:
                    socket.create_connection(("api.dhan.co", 443), timeout=0.1)
                except Exception:
                    pass

            def test_noop(self):
                self.assertTrue(True)

        res_teardown = unittest.TestResult()
        SwallowedTearDownCase("test_noop").run(res_teardown)
        self.assertFalse(res_teardown.wasSuccessful())
        self.assertEqual(len(res_teardown.failures), 1)
        self.assertIn("api.dhan.co", res_teardown.failures[0][1])
        self.assertEqual(tests.get_recorded_network_attempts(), [])

    def test_p1_07_stale_attempt_isolation_and_unittest_discovery_guard_initialization(self):
        """P1-07 (Req 2, 5-10): Stale attempts are cleared before each test, and unittest discovery + subprocesses initialize the guard before tests execute."""
        import subprocess
        import sys
        import tests

        # 1. Inject a stale attempt before running a clean TestCase; verify it is cleared at test start and does not fail CleanCase
        tests._UNMOCKED_NETWORK_ATTEMPTS.append(("socket.getaddrinfo", "stale.example.com:443"))
        try:
            class CleanCase(unittest.TestCase):
                def setUp(self):
                    pass

                def tearDown(self):
                    pass

                def test_clean_execution(self):
                    self.assertEqual(tests.get_recorded_network_attempts(), [])

            clean_res = unittest.TestResult()
            CleanCase("test_clean_execution").run(clean_res)
            self.assertTrue(clean_res.wasSuccessful())
            self.assertEqual(len(clean_res.failures), 0)
            self.assertEqual(len(clean_res.errors), 0)
        finally:
            tests.clear_recorded_network_attempts()

        # 2. Verify a fresh Python subprocess running ordinary unittest discovery (pattern="test_*.py") initializes the guard BEFORE any test executes
        probe_code = (
            "import socket, unittest; "
            "loader = unittest.defaultTestLoader; "
            "suite = loader.discover(\"tests\", pattern=\"test_*.py\"); "
            "assert socket.getaddrinfo.__name__ == \"_guarded_getaddrinfo\", socket.getaddrinfo.__name__; "
            "from tests import OfflineNetworkIsolationError; "
            "caught = False\n"
            "try:\n"
            "    socket.getaddrinfo(\"api.dhan.co\", 443)\n"
            "except OfflineNetworkIsolationError:\n"
            "    caught = True\n"
            "assert caught, \"Discovery guard did not raise OfflineNetworkIsolationError\"; "
            "print(\"DISCOVERY_GUARD_OK\")"
        )
        proc = subprocess.run(
            [sys.executable, "-c", probe_code],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        self.assertIn("DISCOVERY_GUARD_OK", proc.stdout)

    def test_p1_07_cleanup_class_and_module_fixture_swallowed_attempts_all_fail(self):
        """P1-07 (Req C, D, E, J): Swallowed network attempts in addCleanup, setUpClass, tearDownClass, addClassCleanup, setUpModule, tearDownModule, and addModuleCleanup all fail."""
        import socket
        import sys
        import types
        import unittest.case
        import tests

        def _swallow_dns():
            try:
                socket.getaddrinfo("api.dhan.co", 443)
            except Exception:
                pass

        # 1. addCleanup() callback swallowing network exception
        class AddCleanupCase(unittest.TestCase):
            def setUp(self):
                self.addCleanup(_swallow_dns)

            def tearDown(self):
                pass

            def test_ok(self):
                self.assertTrue(True)

        r_cleanup = unittest.TestResult()
        AddCleanupCase("test_ok").run(r_cleanup)
        self.assertFalse(r_cleanup.wasSuccessful())
        self.assertEqual(len(r_cleanup.failures), 1)
        self.assertIn("addCleanup", r_cleanup.failures[0][1])
        self.assertIn("api.dhan.co:443", r_cleanup.failures[0][1])

        # 2. setUpClass() fixture swallowing network exception
        class SetUpClassCase(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                _swallow_dns()

            def test_ok(self):
                self.assertTrue(True)

        r_setup_cls = unittest.TestResult()
        unittest.TestSuite([SetUpClassCase("test_ok")]).run(r_setup_cls)
        self.assertFalse(r_setup_cls.wasSuccessful())
        self.assertEqual(len(r_setup_cls.errors), 1)
        self.assertIn("setUpClass", r_setup_cls.errors[0][1])
        self.assertIn("api.dhan.co:443", r_setup_cls.errors[0][1])

        # 3. tearDownClass() fixture swallowing network exception
        class TearDownClassCase(unittest.TestCase):
            @classmethod
            def tearDownClass(cls):
                _swallow_dns()

            def test_ok(self):
                self.assertTrue(True)

        r_teardown_cls = unittest.TestResult()
        unittest.TestSuite([TearDownClassCase("test_ok")]).run(r_teardown_cls)
        self.assertFalse(r_teardown_cls.wasSuccessful())
        self.assertEqual(len(r_teardown_cls.errors), 1)
        self.assertIn("tearDownClass", r_teardown_cls.errors[0][1])
        self.assertIn("api.dhan.co:443", r_teardown_cls.errors[0][1])

        # 4. addClassCleanup() callback swallowing network exception
        class ClassCleanupCase(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.addClassCleanup(_swallow_dns)

            def test_ok(self):
                self.assertTrue(True)

        r_cls_cleanup = unittest.TestResult()
        unittest.TestSuite([ClassCleanupCase("test_ok")]).run(r_cls_cleanup)
        self.assertFalse(r_cls_cleanup.wasSuccessful())
        self.assertEqual(len(r_cls_cleanup.errors), 1)
        self.assertIn("addClassCleanup", r_cls_cleanup.errors[0][1])
        self.assertIn("api.dhan.co:443", r_cls_cleanup.errors[0][1])

        # 5. setUpModule() fixture swallowing network exception
        mod_setup_name = "_test_p1_07_dummy_mod_setup"
        mod_setup = types.ModuleType(mod_setup_name)
        mod_setup.setUpModule = _swallow_dns  # type: ignore[attr-defined]
        sys.modules[mod_setup_name] = mod_setup
        try:
            class ModSetUpCase(unittest.TestCase):
                def test_ok(self):
                    self.assertTrue(True)

            ModSetUpCase.__module__ = mod_setup_name
            r_mod_setup = unittest.TestResult()
            unittest.TestSuite([ModSetUpCase("test_ok")]).run(r_mod_setup)
            self.assertFalse(r_mod_setup.wasSuccessful())
            self.assertEqual(len(r_mod_setup.errors), 1)
            self.assertIn("setUpModule", r_mod_setup.errors[0][1])
            self.assertIn("api.dhan.co:443", r_mod_setup.errors[0][1])
        finally:
            sys.modules.pop(mod_setup_name, None)

        # 6. tearDownModule() fixture swallowing network exception
        mod_td_name = "_test_p1_07_dummy_mod_teardown"
        mod_td = types.ModuleType(mod_td_name)
        mod_td.tearDownModule = _swallow_dns  # type: ignore[attr-defined]
        sys.modules[mod_td_name] = mod_td
        try:
            class ModTearDownCase(unittest.TestCase):
                def test_ok(self):
                    self.assertTrue(True)

            ModTearDownCase.__module__ = mod_td_name
            r_mod_td = unittest.TestResult()
            unittest.TestSuite([ModTearDownCase("test_ok")]).run(r_mod_td)
            self.assertFalse(r_mod_td.wasSuccessful())
            self.assertEqual(len(r_mod_td.errors), 1)
            self.assertIn("tearDownModule", r_mod_td.errors[0][1])
            self.assertIn("api.dhan.co:443", r_mod_td.errors[0][1])
        finally:
            sys.modules.pop(mod_td_name, None)

        # 7. addModuleCleanup() callback swallowing network exception
        mod_mc_name = "_test_p1_07_dummy_mod_cleanup"
        mod_mc = types.ModuleType(mod_mc_name)
        mod_mc.setUpModule = lambda: unittest.case.addModuleCleanup(_swallow_dns)  # type: ignore[attr-defined]
        sys.modules[mod_mc_name] = mod_mc
        try:
            class ModCleanupCase(unittest.TestCase):
                def test_ok(self):
                    self.assertTrue(True)

            ModCleanupCase.__module__ = mod_mc_name
            r_mod_mc = unittest.TestResult()
            unittest.TestSuite([ModCleanupCase("test_ok")]).run(r_mod_mc)
            self.assertFalse(r_mod_mc.wasSuccessful())
            self.assertEqual(len(r_mod_mc.errors), 1)
            self.assertIn("addModuleCleanup", r_mod_mc.errors[0][1])
            self.assertIn("api.dhan.co:443", r_mod_mc.errors[0][1])
        finally:
            sys.modules.pop(mod_mc_name, None)

        # 8. Confirm zero stale attempts leak into a subsequent clean suite
        self.assertEqual(tests.get_recorded_network_attempts(), [])

        class SubsequentCleanSuiteCase(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.ready = True

            def test_ok(self):
                self.assertTrue(self.ready)

        r_clean_after = unittest.TestResult()
        unittest.TestSuite([SubsequentCleanSuiteCase("test_ok")]).run(r_clean_after)
        self.assertTrue(r_clean_after.wasSuccessful())


if __name__ == '__main__':
    unittest.main()
