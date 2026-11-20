#!/usr/bin/env python3
"""Read-only release guard for retiring UditBot's legacy/V2 settings writers.

No imports from the trading bot, broker, runtime service or credentials.
It is a structural guard, not a replacement for behavior/integration testing.
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any


# These are deliberately configuration WRITER APIs, not owner STOP or safety readers.
OBSOLETE_WRITERS = {
    "src/engine/webui_update.py": ("submit_webui_patch",),
    "src/engine/tracker_v2_integration.py": ("submit_v2_webui_patch",),
    "src/engine/config_manager.py": (
        "try_auto_resume_deferred_update",
        "enqueue_durable_configuration_update",
        "execute_configuration_update",
    ),
}
OBSOLETE_INSTALLERS = (
    "deploy/tracker_v2_activation.sh",
    "deploy/tracker_v2_preflight.py",
    "deploy/tracker_v2_production_evidence.sh",
)
ESSENTIAL_SAFETY = (
    "src/engine/broker_handoff.py",
    "src/engine/simple_save_apply.py",
    "src/engine/emergency_stop.py",
    "src/engine/reconciliation.py",
)


def _functions(src: str) -> set[str]:
    tree = ast.parse(src)
    return {
        node.name for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _direct_http_calls(src: str) -> list[str]:
    tree = ast.parse(src)
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if not isinstance(target, ast.Attribute) or target.attr.lower() not in {
            "post", "put", "patch", "delete", "request",
        }:
            continue
        receiver = target.value
        # Detect `broker.session.delete(...)` and any object `.session.delete(...)`.
        if isinstance(receiver, ast.Attribute) and receiver.attr == "session":
            found.append(f"line {node.lineno}: session.{target.attr} bypass")
    return found


def inspect(root: Path) -> list[dict[str, Any]]:
    root = root.resolve()
    findings: list[dict[str, Any]] = []

    def record(code: str, passed: bool, detail: str) -> None:
        findings.append({"code": code, "pass": bool(passed), "detail": detail})

    for path in ESSENTIAL_SAFETY:
        target = root / path
        record("PRESERVE-SAFETY:" + path, target.is_file() and not target.is_symlink(),
               "Required standalone safety or working Save & Apply source must remain")
    for path in OBSOLETE_INSTALLERS:
        record("RETIRE-INSTALLER:" + path, not (root / path).exists(),
               "Retired V2 installer/activation utility must not ship in release")

    for path, names in OBSOLETE_WRITERS.items():
        target = root / path
        if target.is_symlink():
            record("RETIRE-WRITER:" + path, False, "Symlinked writer module is unsafe")
            continue
        if not target.exists():
            record("RETIRE-WRITER:" + path, True, "Old writer module absent")
            continue
        try:
            present = _functions(target.read_text(encoding="utf-8")) & set(names)
        except (UnicodeError, SyntaxError, OSError) as exc:
            record("RETIRE-WRITER:" + path, False, "Cannot audit source: " + type(exc).__name__)
            continue
        record("RETIRE-WRITER:" + path, not present,
               "Obsolete callable settings writers remain: " + ", ".join(sorted(present))
               if present else "No old settings writers found")

    orphan = root / "src/engine/orphan_cleanup.py"
    if orphan.is_symlink() or not orphan.is_file():
        record("FENCE-ORPHAN", False, "Cannot inspect orphan-cleanup source")
    else:
        try:
            found = _direct_http_calls(orphan.read_text(encoding="utf-8"))
            record("FENCE-ORPHAN", not found,
                   "; ".join(found) if found else "No direct requests.session mutation found")
        except (OSError, UnicodeError, SyntaxError) as exc:
            record("FENCE-ORPHAN", False, "Cannot audit source: " + type(exc).__name__)

    # Direct Save & Apply and broker mutation fencing cannot disappear even if
    # the legacy/V2 settings writer modules are deleted.
    for path, func, code in (
        ("src/engine/simple_save_apply.py", "apply_settings", "KEEP-SAVE-APPLY"),
        ("src/engine/broker_handoff.py", "broker_mutation", "KEEP-BROKER-FENCE"),
        ("src/engine/broker_handoff.py", "register_producer", "KEEP-BROKER-PRODUCER"),
    ):
        target = root / path
        try:
            passed = target.is_file() and not target.is_symlink() and func in _functions(target.read_text(encoding="utf8"))
        except (OSError, UnicodeError, SyntaxError):
            passed = False
        record(code, passed, f"Expected preserved function: {path}::{func}")

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--mode", choices=("baseline", "release"), default="baseline")
    args = parser.parse_args()
    findings = inspect(args.root)
    failures = sum(not finding["pass"] for finding in findings)
    print(json.dumps({"mode": args.mode, "failures": failures, "checks": findings}, indent=2))
    if args.mode == "release" and failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())