"""Isolated broker handoff tests. Never issue a real Dhan request."""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from src.engine.broker_handoff import (
    BrokerHandoffBlocked, BrokerMutationNotSubmitted,
    acquire_apply_barrier, broker_mutation,
    guarded_request, register_producer, release_apply_barrier, _paths,
)
from src.engine.tracker_v2_safety import proc_starttime


class BrokerHandoffTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name
        self.acc = "DEMO-ACCOUNT"
        register_producer(self.dir, self.acc)

    def ledger(self):
        return json.loads(_paths(self.dir)[1].read_text())

    def test_known_http_200_removes_active(self):
        class Response:
            status_code = 200
        self.assertIsInstance(guarded_request(self.dir, self.acc, "POST", Response), Response)
        self.assertEqual([], self.ledger()["active"])
        self.assertEqual([], self.ledger()["unresolved"])

    def test_broker_500_must_persist_uncertainty(self):
        class Response:
            status_code = 500
        guarded_request(self.dir, self.acc, "DELETE", Response)
        self.assertEqual(1, len(self.ledger()["unresolved"]))
        self.assertFalse(acquire_apply_barrier(
            self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())
        ))

    def test_unresolved_mutation_still_allows_independent_protection(self):
        class Response:
            status_code = 500
        guarded_request(self.dir, self.acc, "POST", Response)
        class ProtectedResponse:
            status_code = 202
        protected = guarded_request(self.dir, self.acc, "DELETE", ProtectedResponse)
        self.assertEqual(202, protected.status_code)
        self.assertEqual(1, len(self.ledger()["unresolved"]))
        self.assertFalse(acquire_apply_barrier(
            self.dir, self.acc, "RESTART", os.getpid(), proc_starttime(os.getpid())
        ))

    def test_same_process_re_register_never_clears_uncertainty(self):
        class Response:
            status_code = 500
        guarded_request(self.dir, self.acc, "POST", Response)
        prior = self.ledger()["unresolved"]
        register_producer(self.dir, self.acc)
        self.assertEqual(prior, self.ledger()["unresolved"])

    def test_network_timeout_preserves_unknown(self):
        def timeout():
            raise TimeoutError("simulated")
        with self.assertRaises(TimeoutError):
            guarded_request(self.dir, self.acc, "PUT", timeout)
        self.assertEqual(1, len(self.ledger()["unresolved"]))

    def test_existing_inflight_prevents_barrier(self):
        with broker_mutation(self.dir, self.acc, "POST") as finish:
            self.assertFalse(acquire_apply_barrier(
                self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())
            ))
            finish(True)
        self.assertTrue(acquire_apply_barrier(
            self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())
        ))

    def test_barrier_first_blocks_mutation(self):
        self.assertTrue(acquire_apply_barrier(
            self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())
        ))
        with self.assertRaises(BrokerHandoffBlocked):
            guarded_request(self.dir, self.acc, "POST", lambda: self.fail("HTTP should not run"))
        self.assertFalse(release_apply_barrier(self.dir, self.acc, "wrong", os.getpid(), proc_starttime(os.getpid())))
        self.assertTrue(release_apply_barrier(self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())))

    def test_fenced_request_is_not_submitted(self):
        self.assertTrue(acquire_apply_barrier(
            self.dir, self.acc, "CHG", os.getpid(), proc_starttime(os.getpid())
        ))
        with self.assertRaises(BrokerMutationNotSubmitted):
            guarded_request(self.dir, self.acc, "POST", lambda: self.fail("HTTP sent after barrier"))

    def test_post_http_ledger_failure_is_not_misreported_unsent(self):
        from unittest.mock import patch
        import src.engine.broker_handoff as mod
        class Response:
            status_code = 200
        orig = mod._write
        writes = {"count": 0, "sent": 0}
        def broken_write(path, data):
            writes["count"] += 1
            if writes["count"] == 2:
                raise BrokerHandoffBlocked("simulated fsync failure after HTTP send")
            return orig(path, data)
        def send():
            writes["sent"] += 1
            return Response()
        with patch.object(mod, "_write", side_effect=broken_write):
            with self.assertRaises(BrokerHandoffBlocked) as ctx:
                guarded_request(self.dir, self.acc, "POST", send)
        self.assertNotIsInstance(ctx.exception, BrokerMutationNotSubmitted)
        self.assertEqual(writes["sent"], 1)
        self.assertEqual(len(self.ledger()["active"]), 1)

    def test_wrong_account_and_bad_ledger_block(self):
        with self.assertRaises(BrokerHandoffBlocked):
            guarded_request(self.dir, "OTHER", "POST", lambda: self.fail("HTTP"))
        _paths(self.dir)[1].write_text("{broken")
        with self.assertRaises(BrokerHandoffBlocked):
            guarded_request(self.dir, self.acc, "POST", lambda: self.fail("HTTP"))

    def test_pre_handoff_missing_record_blocks(self):
        _paths(self.dir)[1].unlink()
        with self.assertRaises(BrokerHandoffBlocked):
            guarded_request(self.dir, self.acc, "POST", lambda: self.fail("HTTP"))

    def test_mutation_then_restart_race_uses_shared_lock(self):
        # The parent enters mutation, then the second worker is forbidden to
        # acquire the restart barrier while that mutation remains on the ledger.
        script = (
            "from src.engine.broker_handoff import acquire_apply_barrier\n"
            "import sys\n"
            "try:\n"
            " print(acquire_apply_barrier(sys.argv[1],sys.argv[2],'R',int(sys.argv[3]),int(sys.argv[4])))\n"
            "except Exception as e: print(type(e).__name__)\n"
        )
        with broker_mutation(self.dir, self.acc, "POST") as finish:
            run = subprocess.run(
                [sys.executable, "-c", script, self.dir, self.acc,
                 str(os.getpid()), str(proc_starttime(os.getpid()))],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8,
            )
            self.assertEqual(0, run.returncode, run.stderr)
            self.assertEqual("False", run.stdout.strip())
            finish(True)

    def test_interlock_works_across_thread_race(self):
        blocked = []
        with broker_mutation(self.dir, self.acc, "POST") as finish:
            t = threading.Thread(target=lambda: blocked.append(acquire_apply_barrier(
                self.dir, self.acc, "B", os.getpid(), proc_starttime(os.getpid())
            )))
            t.start()
            t.join(timeout=3)
            self.assertFalse(t.is_alive())
            self.assertEqual([False], blocked)
            finish(True)

    def test_symlinked_ledger_fails_closed(self):
        _, ledger, _ = _paths(self.dir)
        ledger.unlink()
        real = Path(self.dir) / "elsewhere"
        real.write_text("{}")
        ledger.symlink_to(real)
        with self.assertRaises(BrokerHandoffBlocked):
            register_producer(self.dir, self.acc)


if __name__ == "__main__":
    unittest.main()
