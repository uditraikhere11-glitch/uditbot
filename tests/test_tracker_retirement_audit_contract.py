"""Offline tests for read-only retirement structural acceptance guard."""
from __future__ import annotations
import importlib.util
from pathlib import Path
import tempfile
import unittest

_SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "audit_tracker_retirement.py"
_spec = importlib.util.spec_from_file_location("audit_tracker_retirement", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(audit)


class StructuralAuditTests(unittest.TestCase):
    def test_direct_broker_session_mutation_is_rejected(self):
        src = "def cleanup(broker):\n    return broker.session.delete('some-url')\n"
        self.assertTrue(audit._direct_http_calls(src))

    def test_broker_guarded_mutation_is_not_direct_session_bypass(self):
        src = "def cleanup(broker):\n    return broker._broker_mutating_http('DELETE', 'url')\n"
        self.assertEqual(audit._direct_http_calls(src), [])

    def test_existing_writer_symbols_are_detected(self):
        self.assertIn("submit_webui_patch", audit._functions("def submit_webui_patch(): pass"))

    def test_clean_fixture_passes_release_gate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for relative in audit.ESSENTIAL_SAFETY:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("def broker_mutation(): pass\ndef register_producer(): pass\ndef apply_settings(): pass\n")
            orphan = root / "src/engine/orphan_cleanup.py"
            orphan.write_text("def cancel(broker): return broker._broker_mutating_http('DELETE', 'url')\n")
            findings = audit.inspect(root)
            self.assertTrue(findings)
            self.assertEqual([f for f in findings if not f["pass"]], [])

    def test_legacy_writer_and_direct_delete_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for relative in audit.ESSENTIAL_SAFETY:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("def broker_mutation(): pass\ndef register_producer(): pass\ndef apply_settings(): pass\n")
            writer = root / "src/engine/webui_update.py"
            writer.write_text("def submit_webui_patch(): pass\n")
            (root / "src/engine/orphan_cleanup.py").write_text(
                "def x(broker): return broker.session.delete('endpoint')\n")
            failures = {f["code"] for f in audit.inspect(root) if not f["pass"]}
            self.assertIn("RETIRE-WRITER:src/engine/webui_update.py", failures)
            self.assertIn("FENCE-ORPHAN", failures)


if __name__ == "__main__":
    unittest.main()