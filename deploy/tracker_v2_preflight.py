#!/usr/bin/env python3
"""Read-only preflight for the opt-in UditBot tracker V2 activation.

This module never imports DhanClient, runs a broker request, edits owner intent,
reads secret contents into output, or creates a systemd transaction. The CLI
exits 2 on any unverified safety precondition.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil
import stat
import tempfile
import subprocess
import sys
import tarfile
from dataclasses import dataclass, field
from typing import Callable, Optional

UNITS = ("trading_bot.service", "auditbot-web.service")
FLAG = "UDITBOT_TRACKER_V2_ENABLE"


@dataclass
class Report:
    stage: str
    findings: list[tuple[str, str]] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)

    def add(self, name: str, value: object) -> None:
        self.findings.append((name, str(value)))

    def block(self, code: str) -> None:
        if code not in self.blockers:
            self.blockers.append(code)

    def render(self) -> None:
        print(f"===== UDITBOT V2 {self.stage.upper()} READ-ONLY PREFLIGHT =====")
        for k, v in self.findings:
            print(f"{k}: {v}")
        for b in self.blockers:
            print(f"BLOCK: {b}")
        print("PREFLIGHT: " + ("BLOCKED" if self.blockers else "LOCAL_GATES_PASSED"))
        print("No Dhan requests, file changes, or service restarts made.")
        print("Local gates do NOT establish Dhan availability or broker position safety.")


def run(argv: list[str], timeout: float = 8.0) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError("Command unavailable: " + argv[0])
    return result.stdout.strip()


def unit_info(name: str, command: Callable[[list[str]], str] = run) -> dict:
    def prop(key: str) -> str:
        return command(["systemctl", "show", name, "-p", key, "--value", "--no-pager"])
    try:
        pid_s = prop("MainPID")
        return {
            "state": prop("ActiveState"),
            "pid": int(pid_s) if pid_s.isdecimal() else -1,
            "user": prop("User"),
            "result": prop("Result"),
            "envfiles": prop("EnvironmentFiles"),
            "environment": prop("Environment"),
        }
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        return {"state": "UNKNOWN", "pid": -1, "user": "", "result": "UNKNOWN", "envfiles": "", "environment": ""}


def process_flag(pid: int, proc_root: Path = Path("/proc")) -> str:
    """Check the executable environment without exposing any other variables.

    A capability-protected systemd process may have an unreadable /proc/PID
    environ even when both units have the same User. Retry only that read via
    a fixed root-owned cat executable; never run checkout Python as root.
    A failed sudo read remains UNREADABLE and cannot authorize activation.
    """
    if pid < 1:
        return "NOT_RUNNING"
    target = proc_root / str(pid) / "environ"
    try:
        env = target.read_bytes()
    except PermissionError:
        if proc_root != Path("/proc"):
            return "UNREADABLE"
        try:
            result = subprocess.run(
                ["sudo", "-n", "/usr/bin/cat", str(target)],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                timeout=4, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return "UNREADABLE"
        if result.returncode != 0:
            return "UNREADABLE"
        env = result.stdout
    except OSError:
        return "UNREADABLE"
    flags = [e.partition(b"=")[2] for e in env.split(b"\0")
             if e.startswith((FLAG + "=").encode())]
    return "ON" if flags == [b"1"] else "OFF" if not flags or flags == [b"0"] else "AMBIGUOUS"


def _secure_regular_file(path: Path, *, mode_mask: int = 0o077) -> bool:
    try:
        s = path.lstat()
        return stat.S_ISREG(s.st_mode) and (s.st_mode & mode_mask) == 0
    except OSError:
        return False


def inspect_backup(path: Optional[Path], repo: Path, env: Path, journal: Path) -> str:
    """Validate an actual restorable runtime snapshot, not only tar filenames.

    Uses a private temporary SQLite copy for read-only quick_check and compares
    essential table row counts against the stopped local journal. Neither the
    running journal nor the archive is opened for writing, and no private
    contents are displayed. The temporary validation copy is always removed.
    """
    if path is None:
        return "NO_BACKUP_SUPPLIED"
    if not path.is_absolute():
        return "BACKUP_PATH_NOT_ABSOLUTE"
    try:
        path = path.absolute()
        if path.is_relative_to(repo.resolve()):
            return "BACKUP_INSIDE_GIT"
        if not _secure_regular_file(path):
            return "BACKUP_MISSING_OR_NOT_PRIVATE_0600"
        if path.stat().st_uid != os.getuid():
            return "BACKUP_NOT_OWNED_BY_OPERATOR"
        parent = path.parent.stat()
        if parent.st_mode & 0o077:
            return "BACKUP_DIRECTORY_NOT_PRIVATE_0700"
        if path.stat().st_size <= 0:
            return "BACKUP_EMPTY"
        if path.stat().st_mtime < max(env.stat().st_mtime, journal.stat().st_mtime):
            return "BACKUP_OLDER_THAN_RUNTIME"
        with tarfile.open(path, mode="r:gz") as tar:
            members = tar.getmembers()
            names = [m.name for m in members]
            # Conflicting archive members must not be interpreted differently
            # by a future restore tool.
            if len(names) != len(set(names)):
                return "BACKUP_DUPLICATE_MEMBERS"
            found = {m.name: m for m in members}
            for name in ("runtime/.env", "runtime/data/trade_journal.db"):
                entry = found.get(name)
                if entry is None or not entry.isfile() or entry.size <= 0:
                    return "BACKUP_MISSING_REQUIRED_RUNTIME"
            env_member = found["runtime/.env"]
            if env_member.size > 2 * 1024 * 1024:
                return "BACKUP_ENV_UNEXPECTED_SIZE"
            env_stream = tar.extractfile(env_member)
            if env_stream is None or env_stream.read() != env.read_bytes():
                return "BACKUP_ENV_DOES_NOT_MATCH_CURRENT"
            db_member = found["runtime/data/trade_journal.db"]
            with tempfile.TemporaryDirectory(prefix=".v2-backup-check-", dir=path.parent) as tmp:
                snapshot = Path(tmp) / "trade_journal.db"
                stream = tar.extractfile(db_member)
                if stream is None:
                    return "BACKUP_SQLITE_UNREADABLE"
                with snapshot.open("wb") as out:
                    shutil.copyfileobj(stream, out, length=1024 * 1024)
                snapshot.chmod(0o600)
                if snapshot.stat().st_size != db_member.size:
                    return "BACKUP_SQLITE_TRUNCATED"
                def evidence(db_path: Path, *, archived: bool) -> dict[str, int]:
                    # The archived SQLite backup is standalone/immutable;
                    # the stopped local database may still have a WAL whose
                    # committed rows must be included in the comparison.
                    opts = "?mode=ro&immutable=1" if archived else "?mode=ro"
                    with sqlite3.connect(db_path.as_uri() + opts, uri=True, timeout=5.0) as db:
                        check = db.execute("PRAGMA quick_check").fetchone()
                        if check is None or check[0] != "ok":
                            raise sqlite3.DatabaseError("SQLite quick_check failed")
                        tables = {row[0] for row in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )}
                        required = {"order_intents", "orders"}
                        if not required.issubset(tables):
                            raise sqlite3.DatabaseError("Trade tables missing")
                        return {name: db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                                for name in sorted(required)}
                try:
                    archived_counts = evidence(snapshot, archived=True)
                    current_counts = evidence(journal, archived=False)
                except sqlite3.DatabaseError:
                    return "BACKUP_SQLITE_INVALID_OR_INCOMPLETE"
                if archived_counts != current_counts:
                    return "BACKUP_JOURNAL_ROW_COUNTS_DIFFER"
        return "SQLITE_VALIDATED_CURRENT_LOCAL_SNAPSHOT"
    except (OSError, tarfile.TarError, EOFError, ValueError, sqlite3.Error):
        return "BACKUP_INVALID_OR_UNREADABLE"


def _private_json(path: Path, *, missing_ok: bool = False) -> Optional[dict]:
    from src.engine.tracker_v2_safety import read_private_json
    return read_private_json(path, missing_ok=missing_ok)


def _check_git(repo: Path, expected_head: str, report: Report) -> None:
    try:
        actual = run(["git", "-C", str(repo), "rev-parse", "HEAD"])
        status = run(["git", "-C", str(repo), "status", "--porcelain"])
        report.add("Git revision", actual[:12])
        if actual != expected_head or len(expected_head) != 40:
            report.block("GIT_REVISION_NOT_EXACTLY_APPROVED")
        if status:
            report.block("GIT_WORKTREE_NOT_CLEAN")
    except (OSError, RuntimeError, subprocess.SubprocessError):
        report.block("GIT_STATE_UNAVAILABLE")


def _check_common(root: Path, expected_head: str, report: Report):
    _check_git(root, expected_head, report)
    env = root / ".env"
    if not _secure_regular_file(env):
        report.block("ENV_NOT_PRIVATE_REGULAR_0600")
    if not (root / "src/engine/tracker_v2_integration.py").is_file():
        report.block("V2_INTEGRATION_SOURCE_MISSING")
    try:
        from src.config import AppConfig
        cfg = AppConfig.from_env()
        if Path(cfg.data_dir).expanduser().is_symlink():
            report.block("DATA_DIRECTORY_SYMLINK")
        data = Path(cfg.data_dir).expanduser().resolve()
        journal = Path(cfg.journal_db_path).expanduser().resolve()
        report.add("Trading mode", cfg.trading_mode.value)
        # Do not print data paths, accounts, tokens, credentials, hashes.
        if data.is_symlink() or (data.stat().st_mode & 0o077):
            report.block("DATA_DIRECTORY_NOT_PRIVATE_0700")
        if not _secure_regular_file(journal):
            report.block("TRADE_JOURNAL_MISSING_OR_UNSAFE")
        else:
            uri = journal.as_uri() + "?mode=ro"
            with sqlite3.connect(uri, uri=True, timeout=5.0) as db:
                qc = db.execute("PRAGMA quick_check").fetchone()
                if qc is None or qc[0] != "ok":
                    report.block("TRADE_JOURNAL_QUICK_CHECK_FAILED")
        raw = env.read_text(encoding="utf-8")
        if any(line.split("=", 1)[0].strip() == FLAG for line in raw.splitlines() if "=" in line and not line.lstrip().startswith("#")):
            report.block("ENV_FILE_HAS_COMPETING_V2_FLAG")
    except Exception as exc:
        report.add("Local config inspection", type(exc).__name__)
        report.block("LOCAL_CONFIGURATION_OR_JOURNAL_UNREADABLE")
        return None, None, None
    return cfg, data, journal


def _check_stopped(root: Path, data: Path, backup: Optional[Path], journal: Path, report: Report,
                   units: dict, *, managed_stage: bool = False):
    trade, web = units[UNITS[0]], units[UNITS[1]]
    for name, details in units.items():
        explicit_flag = (FLAG + '=') in details.get('environment', '')
        feature_file = '/etc/uditbot-tracker-v2/activation.env'
        has_feature_file = feature_file in details.get('envfiles', '')
        if managed_stage:
            # The staged validation runs AFTER installing our two unit flags.
            # It must require those flags, rather than reject its own work.
            if explicit_flag or not has_feature_file:
                report.block("MANAGED_V2_UNIT_CONFIGURATION_UNVERIFIED_" + name)
        elif explicit_flag or '/etc/uditbot-tracker-v2/' in details.get('envfiles', ''):
            report.block("PREEXISTING_SYSTEMD_V2_FLAG_REQUIRES_REVIEW")
    if not (trade["state"] == "inactive" and trade["pid"] == 0):
        report.block("TRADING_SERVICE_NOT_POSITIVELY_STOPPED")
    if not (web["state"] == "active" and web["pid"] > 0):
        report.block("WEBUI_SERVICE_NOT_ACTIVE")
    import pwd
    username = pwd.getpwuid(os.getuid()).pw_name
    if trade["user"] != username or web["user"] != username:
        report.block("SERVICE_ACCOUNT_NOT_OPERATOR")
    report.add("Backup", inspect_backup(backup, root, root / ".env", journal))
    if report.findings[-1][1] != "SQLITE_VALIDATED_CURRENT_LOCAL_SNAPSHOT":
        report.block("BACKUP_NOT_VERIFIED")
    try:
        control = _private_json(data / "service_control_state.json")
        if not isinstance(control, dict) or control.get("desired_service_state") != "STOPPED" or control.get("observed_state") != "STOPPED_BY_OWNER" or control.get("deferred_stop_pending") is not False:
            report.block("OWNER_STOP_NOT_POSITIVELY_VERIFIED")
        elif control.get("last_action") not in ("OWNER_STOP", "WEBUI_STOP", "CLI_STOP"):
            report.block("OWNER_STOP_ORIGIN_UNVERIFIED")
        from src.engine.tracker_v2_safety import (
            preview_legacy_tracker_migration, inspect_transaction_recovery, inspect_apply_barrier,
        )
        legacy = preview_legacy_tracker_migration(data, allow_owner_stopped=True)
        report.add("Legacy safety", legacy.code)
        if not legacy.allowed:
            report.block("LEGACY_SAFETY_" + legacy.code)
        tx = data / "config_restart_v2" / "current_transaction.json"
        if tx.exists() or tx.is_symlink():
            report.block("PREEXISTING_V2_TRANSACTION_REQUIRES_REVIEW")
        barrier = data / "config_restart_v2" / "apply_barrier.json"
        b = inspect_apply_barrier(barrier)
        report.add("V2 barrier", b.code)
        if b.code != "NO_BARRIER":
            report.block("PREEXISTING_V2_BARRIER_REQUIRES_REVIEW")
        mutation_path = data / "active_broker_mutations.json"
        mutation = _private_json(mutation_path, missing_ok=True)
        if mutation is not None or mutation_path.is_symlink():
            # Do not silently adopt old producer evidence while V2 is off.
            # Preserve it for manual review even if arrays look empty.
            report.block("PREEXISTING_BROKER_LEDGER_REQUIRES_REVIEW")
        stop = _private_json(data / "emergency_stop.json", missing_ok=True)
        if not isinstance(stop, dict) or stop.get("is_stopped") is not False or stop.get("in_flight_orders"):
            report.block("EMERGENCY_STOP_OR_IN_FLIGHT_UNVERIFIED")
    except (OSError, ValueError, TypeError):
        report.block("SAFETY_EVIDENCE_UNREADABLE")


def _check_running(root: Path, cfg, data: Path, report: Report, units: dict,
                   proc_root: Path = Path("/proc")):
    trade, web = units[UNITS[0]], units[UNITS[1]]
    for unit in UNITS:
        status = units[unit]
        flag = process_flag(status["pid"], proc_root=proc_root)
        report.add(unit + " V2 process flag", flag)
        if status["state"] != "active" or status["pid"] < 1 or flag != "ON":
            report.block(unit + "_NOT_RUNNING_WITH_V2")
    try:
        from src.engine.config_manager import compute_config_version
        from src.engine.tracker_credentials import fingerprint
        from src.engine.tracker_v2_safety import (
            proc_starttime, preview_legacy_tracker_migration, inspect_transaction_recovery,
            inspect_apply_barrier,
        )
        ack = _private_json(data / "active_config_version.json")
        pid = trade["pid"]
        if not isinstance(ack, dict) or ack.get("pid") != pid or ack.get("process_starttime") != proc_starttime(pid) or ack.get("version_hash") != compute_config_version(cfg) or ack.get("trading_mode") != cfg.trading_mode.value:
            report.block("RUNTIME_ACK_PID_HASH_OR_MODE_MISMATCH")
        key = data / ".tracker_credential_hmac_key"
        # fingerprint() creates a key if absent; never call it when absent.
        if not _secure_regular_file(key) or key.stat().st_size != 32:
            report.block("HMAC_KEY_NOT_ALREADY_INITIALIZED")
        elif not isinstance(ack, dict) or ack.get("credential_fingerprint") != fingerprint(cfg, str(data)):
            report.block("RUNTIME_CREDENTIAL_FINGERPRINT_MISMATCH")
        led = _private_json(data / "active_broker_mutations.json")
        if not isinstance(led, dict) or led.get("schema_version") != 2 or led.get("handoff_verified") is not True or led.get("account_id") != cfg.dhan_client_id or led.get("producer_pid") != pid or led.get("producer_starttime") != proc_starttime(pid) or led.get("active") != [] or led.get("unresolved") != []:
            report.block("BROKER_HANDOFF_OR_MUTATION_STATE_UNVERIFIED")
        c = _private_json(data / "service_control_state.json")
        if not isinstance(c, dict) or c.get("desired_service_state") != "RUNNING" or c.get("observed_state") != "RUNNING" or c.get("deferred_stop_pending") is not False:
            report.block("OWNER_INTENT_RUNTIME_MISMATCH")
        legacy = preview_legacy_tracker_migration(data)
        report.add("Legacy safety", legacy.code)
        if not legacy.allowed:
            report.block("LEGACY_SAFETY_" + legacy.code)
        tx = inspect_transaction_recovery(data, root / ".env")
        report.add("V2 transaction", tx.code)
        if not tx.allowed:
            report.block("V2_TRANSACTION_" + tx.code)
        barrier = inspect_apply_barrier(data / "config_restart_v2" / "apply_barrier.json")
        report.add("Apply barrier", barrier.code)
        if not barrier.allowed:
            report.block("V2_BARRIER_" + barrier.code)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        report.add("Runtime evidence inspection", type(exc).__name__)
        report.block("RUNNING_V2_EVIDENCE_UNREADABLE")


def inspect(stage: str, root: Path, expected_head: str, backup: Optional[Path] = None,
            command: Callable[[list[str]], str] = run) -> Report:
    report = Report(stage)
    root = root.resolve()
    old_cwd = Path.cwd()
    try:
        os.chdir(root)
        cfg, data, journal = _check_common(root, expected_head, report)
        if data is None:
            return report
        units = {name: unit_info(name, command=command) for name in UNITS}
        for name, item in units.items():
            report.add(name + " systemd", item["state"])
            if item["state"] == "UNKNOWN":
                report.block("SYSTEMD_STATUS_UNAVAILABLE")
        if stage in ("stopped", "staged"):
            _check_stopped(root, data, backup, journal, report, units,
                           managed_stage=stage == "staged")
        elif stage == "running":
            _check_running(root, cfg, data, report, units)
        else:
            report.block("INVALID_STAGE")
        return report
    finally:
        os.chdir(old_cwd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("stopped", "staged", "running"), required=True)
    parser.add_argument("--expected-head", required=True, help="exact approved full 40-character Git commit SHA")
    parser.add_argument("--backup", type=Path, help="fresh private unencrypted runtime archive")
    args = parser.parse_args()
    try:
        report = inspect(args.stage, Path.cwd(), args.expected_head, args.backup)
        report.render()
        return 2 if report.blockers else 0
    except Exception as exc:
        print("BLOCK: PREFLIGHT_EXCEPTION_" + type(exc).__name__)
        return 2


if __name__ == "__main__":
    sys.exit(main())
