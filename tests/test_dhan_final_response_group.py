"""Offline tests for Dhan final-response destination, WebUI save and durable routing."""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker
from src.journal.db import JournalDB
from src.notifier.dhan_response import normalize_group_name, resolve_dhan_response_group
from src.notifier.execution_updates import OrderStatusReporter
from src.notifier.outbox_worker import OutboxWorker
from src.setup_app import build_env_and_params_dict, HTML_TEMPLATE


class TestResponseGroupConfiguration(unittest.TestCase):
    def test_env_name_and_webui_form_roundtrip(self):
        cfg = AppConfig.from_env(
            env={"DHAN_RESPONSE_GROUP_NAME": "Dhan Response"},
            env_file="",
        )
        self.assertEqual(cfg.dhan_response_group_name, "Dhan Response")
        with tempfile.TemporaryDirectory() as tmp:
            env_text, params = build_env_and_params_dict(
                dhan_response_group_name="Dhan Response",
                env_path=os.path.join(tmp, "nonexistent.env"),
            )
            self.assertIn("DHAN_RESPONSE_GROUP_NAME=Dhan Response", env_text)
            self.assertEqual(params["DHAN_RESPONSE_GROUP_NAME"], "Dhan Response")
            tracker = ConfigChangeTracker(data_dir=tmp)
            self.assertTrue(tracker.validate_inputs(params)[0])

    def test_webui_has_simple_group_name_field(self):
        self.assertIn('name="dhan_response_group_name"', HTML_TEMPLATE)
        self.assertIn('<!--DHAN_RESPONSE_GROUP_NAME-->', HTML_TEMPLATE)

    def test_reject_env_injection_and_oversize(self):
        for name in ("A\nTRADING_MODE=LIVE", "A=B", "A#B", "x" * 101):
            with self.subTest(name=name[:12]):
                with self.assertRaises(ValueError):
                    normalize_group_name(name)
                with tempfile.TemporaryDirectory() as tmp:
                    with self.assertRaises(ValueError):
                        build_env_and_params_dict(
                            dhan_response_group_name=name,
                            env_path=os.path.join(tmp, "no.env"),
                        )

    def test_numeric_group_id_survives_env_and_webui(self):
        cfg = AppConfig.from_env(
            env={"DHAN_RESPONSE_GROUP_NAME": "-5254821642"}, env_file=""
        )
        self.assertEqual(cfg.dhan_response_group_name, "-5254821642")
        with tempfile.TemporaryDirectory() as tmp:
            env_text, params = build_env_and_params_dict(
                dhan_response_group_name="-5254821642",
                env_path=os.path.join(tmp, "nonexistent.env"),
            )
            self.assertIn("DHAN_RESPONSE_GROUP_NAME=-5254821642", env_text)
            self.assertEqual(params["DHAN_RESPONSE_GROUP_NAME"], "-5254821642")

    def test_empty_name_disables_feature_without_changing_log_group(self):
        cfg = AppConfig.from_env(env={"LOG_GROUP_ID": "-1001234567890"}, env_file="")
        self.assertFalse(cfg.dhan_response_group_name)
        self.assertEqual(cfg.log_group_id, -1001234567890)


class _FakeTelegram:
    def __init__(self, dialogs):
        self.dialogs = dialogs

    async def iter_dialogs(self):
        for dialog in self.dialogs:
            yield dialog


def _dialog(name, chat_id, is_group=True):
    return SimpleNamespace(
        name=name, id=chat_id, is_group=is_group,
        entity=SimpleNamespace(id=chat_id),
    )


class TestExactGroupLookup(unittest.IsolatedAsyncioTestCase):
    async def test_exact_group_name_resolves_to_entity(self):
        group = await resolve_dhan_response_group(
            _FakeTelegram([
                _dialog("Dhan Response Extra", -1),
                _dialog("Dhan Response", -2),
                _dialog("Dhan Response", -3, is_group=False),
            ]),
            "dhan response",
        )
        self.assertEqual(group.id, -2)

    async def test_duplicate_group_titles_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "found 2"):
            await resolve_dhan_response_group(
                _FakeTelegram([_dialog("Dhan Response", -2), _dialog("Dhan Response", -4)]),
                "Dhan Response",
            )

    async def test_unjoined_group_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            await resolve_dhan_response_group(_FakeTelegram([]), "Dhan Response")

    async def test_signed_group_id_resolves_exact_peer(self):
        group = await resolve_dhan_response_group(
            _FakeTelegram([
                _dialog("Other group", -1001111111111),
                _dialog("Dhan Response", -5254821642),
                _dialog("-5254821642", -1002222222222),
            ]),
            "-5254821642",
        )
        self.assertEqual(group.id, -5254821642)

    async def test_unjoined_numeric_group_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            await resolve_dhan_response_group(
                _FakeTelegram([_dialog("Other group", -123456789)]),
                "-5254821642",
            )

    async def test_numeric_id_does_not_match_non_group_dialog(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            await resolve_dhan_response_group(
                _FakeTelegram([_dialog("User", -5254821642, is_group=False)]),
                "-5254821642",
            )

    async def test_numeric_id_cannot_be_monitored_source(self):
        with self.assertRaisesRegex(ValueError, "monitored trade-signal source"):
            await resolve_dhan_response_group(
                _FakeTelegram([_dialog("Dhan Response", -5254821642)]),
                "-5254821642",
                forbidden_chat_ids=[-5254821642],
            )

    async def test_signal_source_cannot_be_response_destination(self):
        with self.assertRaisesRegex(ValueError, "monitored trade-signal source"):
            await resolve_dhan_response_group(
                _FakeTelegram([_dialog("Dhan Response", -2)]),
                "Dhan Response",
                forbidden_chat_ids=[-2],
            )


class TestDurableFinalResponse(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = JournalDB(os.path.join(tmp.name, "journal.db"))
        self.sent_log = []
        self.sent_response = []

    def test_final_mirror_is_atomic_and_deduplicated(self):
        self.assertTrue(self.db.enqueue_order_notification_once(
            "OID-1", "TRADED", "<b>Executed</b>", mirror_final=True,
        ))
        self.assertFalse(self.db.enqueue_order_notification_once(
            "OID-1", "TRADED", "<b>Executed</b>", mirror_final=True,
        ))
        pending = self.db.fetch_pending_outbox()
        self.assertEqual(len(pending), 2)
        self.assertEqual([x["event_type"] for x in pending],
                         ["ORDER_STATUS_UPDATE", "DHAN_FINAL_RESPONSE"])
        body = json.loads(pending[1]["payload"])["text"]
        self.assertIn("Final Dhan Response\n\n", body.replace("</b>", ""))
        self.assertNotIn("Order ID", body)

    def test_final_outbox_event_goes_only_to_response_destination(self):
        self.db.enqueue_order_notification_once(
            "OID-2", "TRADED", "Filled", mirror_final=True,
        )
        worker = OutboxWorker(
            self.db, dispatch_fn=lambda txt: self.sent_log.append(txt) or True,
            dhan_response_dispatch_fn=lambda txt: self.sent_response.append(txt) or True,
        )
        self.assertEqual(worker.process_pending_events(), 2)
        self.assertEqual(len(self.sent_log), 1)
        self.assertEqual(len(self.sent_response), 1)
        self.assertIn("Final Dhan Response", self.sent_response[0])
        self.assertFalse(self.db.fetch_pending_outbox())

    def test_no_response_dispatch_function_does_not_mark_delivered(self):
        self.db.enqueue_order_notification_once("OID-3", "TRADED", "Filled", mirror_final=True)
        worker = OutboxWorker(self.db, dispatch_fn=lambda txt: True)
        self.assertEqual(worker.process_pending_events(), 1)
        with self.db._get_conn() as conn:
            rec = conn.execute(
                "SELECT status,retry_count FROM outbox WHERE event_type='DHAN_FINAL_RESPONSE'"
            ).fetchone()
        self.assertEqual(rec["status"], "PENDING")
        self.assertEqual(rec["retry_count"], 1)

    def test_reporter_mirrors_only_verified_terminal_broker_status(self):
        journal = MagicMock()
        journal.enqueue_order_notification_once.return_value = True
        reporter = OrderStatusReporter(journal, MagicMock(), dhan_response_enabled=True)
        rec = {
            "order_id": "OID-4", "qty": 1, "protected_qty": 1,
            "pending_oco": False,
            "notification_meta": {
                "mode": "LIVE", "symbol": "IDEA", "segment": "EQUITY",
                "source": "Approved", "action": "BUY", "limit_price": 10.0,
            },
        }
        self.assertTrue(reporter.process(rec, {
            "orderStatus": "PENDING", "filledQty": 0,
        }, observed_at=100.0))
        journal.enqueue_order_notification_once.assert_not_called()
        self.assertTrue(reporter.process(rec, {
            "orderStatus": "TRADED", "filledQty": 1,
            "averageTradedPrice": 10.0,
        }, observed_at=101.0))
        args, kwargs = journal.enqueue_order_notification_once.call_args
        self.assertEqual(args[1], "TRADED")
        self.assertTrue(kwargs["mirror_final"])

    def test_cancellation_mirrored_without_falsely_claiming_fill(self):
        journal = MagicMock()
        reporter = OrderStatusReporter(journal, MagicMock(), dhan_response_enabled=True)
        rec = {
            "order_id": "OID-5", "qty": 5,
            "notification_meta": {
                "mode": "LIVE", "symbol": "IDEA", "segment": "EQUITY",
                "action": "BUY", "limit_price": 10.0,
            },
        }
        self.assertTrue(reporter.process(rec, {
            "orderStatus": "CANCELLED", "filledQty": 0,
        }, observed_at=101.0))
        args, kwargs = journal.enqueue_order_notification_once.call_args
        self.assertTrue(args[1].startswith("FINAL_CANCELLED"))
        self.assertTrue(kwargs["mirror_final"])
        self.assertIn("Cancelled", args[2])

    def test_legacy_logging_unchanged_when_group_is_unconfigured(self):
        journal = MagicMock()
        reporter = OrderStatusReporter(journal, MagicMock())
        rec = {
            "order_id": "OID-6", "qty": 1,
            "notification_meta": {
                "mode": "LIVE", "symbol": "IDEA", "limit_price": 10.0,
            },
        }
        reporter.process(rec, {"orderStatus": "REJECTED", "filledQty": 0}, 101.0)
        args, kwargs = journal.enqueue_order_notification_once.call_args
        self.assertEqual(kwargs, {})
        self.assertIn("REJECTED", args[1])


if __name__ == "__main__":
    unittest.main()
