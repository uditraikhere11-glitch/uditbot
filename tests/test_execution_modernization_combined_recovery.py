"""Lost ACK and notification isolation on the actual entry points, offline."""
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.main import TradingBotPipeline
from src.models import ProductType, TradingMode
from tests.test_execution_modernization_mutation_fence import ACCOUNT, client, order, stop, remote429


class CombinedRecovery(unittest.TestCase):
    def test_delivery_transport_never_runs_on_pipeline_thread(self):
        for mode in (TradingMode.TEST, TradingMode.LIVE):
            for state in ("ok", "unavailable", "enqueue_failed"):
                with self.subTest(mode=mode, state=state):
                    pipeline = object.__new__(TradingBotPipeline)
                    pipeline.cfg = SimpleNamespace(trading_mode=mode)
                    pipeline.notifier = Mock(enabled=True)
                    pipeline.db = None if state == "unavailable" else Mock()
                    if state == "enqueue_failed":
                        pipeline.db.enqueue_outbox.side_effect = OSError("fixture")
                    self.assertEqual(pipeline.dispatch_notification("PROTECTION", "fixture"),
                                     state == "ok")
                    pipeline.notifier.send_log_message.assert_not_called()

    def test_504_and_interrupt_remain_fenced_after_restart(self):
        for super_order in (False, True):
            for failure in ("504", "interrupt"):
                with self.subTest(super_order=super_order, failure=failure), tempfile.TemporaryDirectory() as root:
                    path = Path(root) / "stop.json"
                    manager = stop(path)
                    manager.provision_initial_clean_state(operator="offline", account_id=ACCOUNT)
                    first = client(manager)
                    req = order(ProductType.MARGIN, "NSE_FNO") if super_order else order()

                    def submit(broker):
                        if super_order:
                            return broker.place_super_order(req, stop_loss_price=95, target_price=105)
                        return broker.place_order(req)

                    with patch("src.engine.config_manager.ConfigChangeTracker") as tracker:
                        tracker.return_value.is_live_entry_blocked_by_transition.return_value = False, "OK"
                        if failure == "504":
                            response = remote429()
                            response.status_code = 504
                            first._broker_mutating_http.return_value = response
                            first._reconcile_uncertain_submission = Mock(return_value=(False, None, "UNKNOWN"))
                            ok, _, reason = submit(first)
                            self.assertFalse(ok)
                            self.assertIn("UNKNOWN", reason)
                        else:
                            first._broker_mutating_http.side_effect = KeyboardInterrupt
                            with self.assertRaises(KeyboardInterrupt):
                                submit(first)
                        first._broker_mutating_http.assert_called_once()
                        restarted = client(stop(path))
                        self.assertIn(req.correlation_id, restarted.stop_manager.get_in_flight())
                        self.assertFalse(restarted.stop_manager.is_stopped())
                        self.assertFalse(submit(restarted)[0])
                        restarted._broker_mutating_http.assert_not_called()
