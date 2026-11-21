"""Integration of optional durable ingress audit with FIFO executor callbacks.

This test simulates Telegram records, execution completion, callback failures,
and a process restart. It has no broker calls and never auto-replays.
"""
import asyncio
import tempfile
import unittest
from pathlib import Path

from src.engine.durable_ingress_audit import DurableIngressAudit
from src.engine.ordered_ingress import OrderedSignalDispatcher


def entry(number):
    return dict(
        text=f"Buy #SBIN @ {800+number}",
        channel_id=-100123456,
        message_id=number,
        msg_timestamp=1800000000.0+number,
    )


def prepare(ledger, kwargs):
    return ledger.admit(
        channel_id=kwargs["channel_id"],message_id=kwargs["message_id"],
        text=kwargs["text"],message_timestamp=kwargs["msg_timestamp"],
    )


def returned(ledger, kwargs):
    return ledger.mark_pipeline_returned(
        channel_id=kwargs["channel_id"],message_id=kwargs["message_id"],
        text=kwargs["text"],message_timestamp=kwargs["msg_timestamp"],
    )


class TestDurableAuditedDispatch(unittest.IsolatedAsyncioTestCase):
    async def test_successful_processing_closes_durable_inbound_claim(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"signals.ingress-audit.sqlite3"
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                processed=[]
                def process(**kw):
                    processed.append(kw["message_id"])
                    return "LOCAL_PIPELINE_RETURNED"
                dispatcher=OrderedSignalDispatcher(
                    process,max_pending=2,
                    on_pipeline_returned=lambda kw: returned(ledger,kw),
                )
                await dispatcher.start()
                tasks=[]
                for number in range(1,16):
                    kwargs=entry(number)
                    self.assertEqual(prepare(ledger,kwargs),"ADMITTED")
                    tasks.append(asyncio.create_task(dispatcher.submit(**kwargs)))
                    await asyncio.sleep(0)
                outputs=await asyncio.gather(*tasks)
                await dispatcher.close()
                self.assertEqual(outputs,["LOCAL_PIPELINE_RETURNED"]*15)
                self.assertEqual(processed,list(range(1,16)))
                self.assertEqual(ledger.summary()["unreturned_signals"],0)
                self.assertEqual(ledger.summary()["pipeline_returned_signals"],15)
            # Restart cannot replay an already completed identical source ID.
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                self.assertEqual(prepare(ledger,entry(1)),"DUPLICATE_RETURNED")

    async def test_crash_after_possible_broker_submission_retains_unknown(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"signals.ingress-audit.sqlite3"
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                kwargs=entry(2)
                self.assertEqual(prepare(ledger,kwargs),"ADMITTED")
                def process(**kw):
                    raise ConnectionError("mock lost broker ACK")
                dispatcher=OrderedSignalDispatcher(
                    process,on_pipeline_returned=lambda kw: returned(ledger,kw),
                )
                await dispatcher.start()
                with self.assertRaises(RuntimeError):
                    await dispatcher.submit(**kwargs)
                await dispatcher.close()
                self.assertEqual(ledger.summary()["unreturned_signals"],1)
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                self.assertTrue(ledger.summary()["requires_manual_reconciliation"])
                self.assertFalse(ledger.summary()["automatic_replay_permitted"])
                self.assertEqual(prepare(ledger,entry(2)),"DUPLICATE_PENDING")

    async def test_callback_cancellation_does_not_strand_completed_ingress(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"signals.ingress-audit.sqlite3"
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                kwargs=entry(3)
                self.assertEqual(prepare(ledger,kwargs),"ADMITTED")
                import time
                def process(**kw):
                    time.sleep(0.02)
                    return True
                dispatcher=OrderedSignalDispatcher(
                    process,on_pipeline_returned=lambda kw:returned(ledger,kw),
                )
                await dispatcher.start()
                task=asyncio.create_task(dispatcher.submit(**kwargs))
                await asyncio.sleep(0.001)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await dispatcher.close()
                self.assertEqual(ledger.summary()["pipeline_returned_signals"],1)
                self.assertEqual(ledger.summary()["unreturned_signals"],0)

    async def test_audit_complete_write_failure_returns_error_no_retry(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"signals.ingress-audit.sqlite3"
            with DurableIngressAudit(str(path),account_id="SYNTHETIC") as ledger:
                kwargs=entry(4)
                prepare(ledger,kwargs)
                def fail_on_returned(kw):
                    raise OSError("simulated audit disk unavailable")
                dispatcher=OrderedSignalDispatcher(
                    lambda **kw: True,
                    on_pipeline_returned=fail_on_returned,
                )
                await dispatcher.start()
                with self.assertRaises(RuntimeError):
                    await dispatcher.submit(**kwargs)
                await dispatcher.close()
                self.assertEqual(ledger.summary()["unreturned_signals"],1)
                self.assertEqual(dispatcher.metrics()["processing_failures"],1)
