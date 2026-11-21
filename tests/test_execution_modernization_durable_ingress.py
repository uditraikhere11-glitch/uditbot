"""Durable ingress audit: simulated crash, duplicate replay, edits and isolation.

No live orders or automatic replay. A retained RECEIVED record is a manual
reconciliation requirement, NOT permission to re-send to Dhan.
"""
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest

from src.engine.durable_ingress_audit import (
    DurableIngressAudit, IngressAuditConflict,
)


ARGS=dict(
    channel_id=-1001234, message_id=58,
    text="Buy #SBIN @805", message_timestamp=1800000001.0,
)


class TestDurableIngressAudit(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/"signals.ingress-audit.sqlite3"

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_admission_is_durable_before_work(self):
        with DurableIngressAudit(str(self.path), account_id="SYNTHETIC") as d:
            self.assertEqual(d.admit(**ARGS),"ADMITTED")
            self.assertEqual(d.summary()["unreturned_signals"],1)
            self.assertFalse(d.summary()["automatic_replay_permitted"])
            self.assertFalse(d.summary()["broker_fill_or_protection_certified"])
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode),0o600)
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            self.assertEqual(d.admit(**ARGS),"DUPLICATE_PENDING")
            self.assertTrue(d.summary()["requires_manual_reconciliation"])

    def test_mark_return_only_once_and_reopen(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            d.admit(**ARGS)
            self.assertEqual(d.mark_pipeline_returned(**ARGS),"RETURNED")
            self.assertEqual(d.mark_pipeline_returned(**ARGS),"ALREADY_RETURNED")
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            self.assertEqual(d.admit(**ARGS),"DUPLICATE_RETURNED")
            summary=d.summary()
            self.assertEqual(summary["unreturned_signals"],0)
            self.assertEqual(summary["pipeline_returned_signals"],1)

    def test_edit_or_changed_timestamp_cannot_reuse_original_identity(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            d.admit(**ARGS)
            with self.assertRaises(IngressAuditConflict):
                d.admit(**{**ARGS,"text":"Buy #SBIN @806"})
            with self.assertRaises(IngressAuditConflict):
                d.mark_pipeline_returned(
                    **{**ARGS,"message_timestamp":1800000002.0}
                )
            self.assertEqual(d.summary()["unreturned_signals"],1)

    def test_simulated_crash_during_sqlite_commit_rolls_back(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            def crash():
                raise OSError("synthetic process death before COMMIT")
            with self.assertRaises(OSError):
                d.admit(**ARGS,before_commit=crash)
            self.assertEqual(d.summary()["unreturned_signals"],0)
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            self.assertEqual(d.admit(**ARGS),"ADMITTED")

    def test_process_crash_after_admission_never_autoreplays_signal(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            d.admit(**ARGS)
            # The process can die after a broker POST but before returning.
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            self.assertTrue(d.summary()["requires_manual_reconciliation"])
            self.assertFalse(d.summary()["automatic_replay_permitted"])
            self.assertEqual(d.admit(**ARGS),"DUPLICATE_PENDING")

    def test_missing_admission_cannot_be_claimed_as_processed(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            with self.assertRaises(IngressAuditConflict):
                d.mark_pipeline_returned(**ARGS)

    def test_wrong_account_never_inherits_old_signal_ledger(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            d.admit(**ARGS)
        with self.assertRaises(IngressAuditConflict):
            DurableIngressAudit(str(self.path),account_id="OTHER")

    def test_existing_truncated_ledger_cannot_reset_record(self):
        self.path.touch()
        os.chmod(self.path,0o600)  # exercise truncation, not file permissions
        with self.assertRaises(IngressAuditConflict):
            DurableIngressAudit(str(self.path),account_id="SYNTHETIC")

    def test_symlink_and_weak_permissions_rejected(self):
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC"):
            pass
        os.chmod(self.path,0o644)
        with self.assertRaises(PermissionError):
            DurableIngressAudit(str(self.path),account_id="SYNTHETIC")
        os.chmod(self.path,0o600)
        alias=Path(self.tmp.name)/"alias.ingress-audit.sqlite3"
        alias.symlink_to(self.path)
        with self.assertRaises(ValueError):
            DurableIngressAudit(str(alias),account_id="SYNTHETIC")

    def test_invalid_telegram_source_and_message_id_rejected(self):
        bad=[
            {"channel_id":0},
            {"channel_id":True},
            {"message_id":0},
            {"message_id":True},
            {"text":""},
            {"message_timestamp":float("nan")},
            {"message_timestamp":False},
        ]
        with DurableIngressAudit(str(self.path),account_id="SYNTHETIC") as d:
            for overrides in bad:
                with self.subTest(overrides=overrides),self.assertRaises(ValueError):
                    d.admit(**{**ARGS,**overrides})
            self.assertEqual(d.summary()["unreturned_signals"],0)
