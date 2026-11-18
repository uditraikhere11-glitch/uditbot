"""Offline unit tests for the CLI preflight's filesystem and systemd parsing.

These do not run the actual broker, WebUI, or systemctl; never touch a LIVE VM.
"""
from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deploy.tracker_v2_preflight import (
    inspect_backup, process_flag, Report, unit_info, _secure_regular_file,
)


class PreflightPureTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.p = Path(tmp.name)
        self.repo = self.p/'repo'
        self.repo.mkdir(mode=0o700)
        self.backupdir = self.p/'backups'
        self.backupdir.mkdir(mode=0o700)
        self.env = self.repo/'.env'
        self.env.write_text('TRADING_MODE=TEST\n')
        self.env.chmod(0o600)
        self.journal = self.repo/'journal.db'
        with sqlite3.connect(self.journal) as db:
            db.execute('CREATE TABLE order_intents (id INTEGER PRIMARY KEY)')
            db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
            db.execute('INSERT INTO order_intents VALUES (1)')
            db.execute('INSERT INTO orders VALUES (1)')
        self.journal.chmod(0o600)

    def archive(self, filename='latest.tar.gz', entries=None, chmod=0o600):
        path = self.backupdir/filename
        if entries is None:
            entries = {'runtime/.env': self.env.read_bytes(),
                       'runtime/data/trade_journal.db': self.journal.read_bytes()}
        with tarfile.open(path, 'w:gz') as tar:
            for name, content in entries.items():
                info = tarfile.TarInfo(name)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
        path.chmod(chmod)
        return path

    def test_private_recent_backup_accepted_structurally(self):
        b = self.archive()
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'SQLITE_VALIDATED_CURRENT_LOCAL_SNAPSHOT')

    def test_corrupt_sqlite_is_rejected(self):
        b = self.archive(entries={
            'runtime/.env': self.env.read_bytes(),
            'runtime/data/trade_journal.db': b'fake sqlite bytes',
        })
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_SQLITE_INVALID_OR_INCOMPLETE')

    def test_stale_configuration_in_backup_is_rejected(self):
        b = self.archive(entries={
            'runtime/.env': b'TRADING_MODE=LIVE\n',
            'runtime/data/trade_journal.db': self.journal.read_bytes(),
        })
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_ENV_DOES_NOT_MATCH_CURRENT')

    def test_stopped_journal_wal_frames_are_not_ignored(self):
        # A stopped process can leave committed -wal frames. The local state
        # must read those frames; immutable=1 would falsely compare old pages.
        b = self.archive()
        conn = sqlite3.connect(self.journal)
        try:
            self.assertEqual(conn.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            conn.execute('INSERT INTO orders VALUES (3)')
            conn.commit()
            os.utime(b, (self.journal.stat().st_mtime + 50,
                         self.journal.stat().st_mtime + 50))
            self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                             'BACKUP_JOURNAL_ROW_COUNTS_DIFFER')
        finally:
            conn.close()

    def test_wrong_journal_row_counts_are_rejected(self):
        with sqlite3.connect(self.journal) as db:
            db.execute('INSERT INTO orders VALUES (2)')
        other = self.repo/'previous.db'
        with sqlite3.connect(other) as db:
            db.execute('CREATE TABLE order_intents (id INTEGER PRIMARY KEY)')
            db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
            db.execute('INSERT INTO order_intents VALUES (1)')
            db.execute('INSERT INTO orders VALUES (1)')
        b = self.archive(entries={
            'runtime/.env': self.env.read_bytes(),
            'runtime/data/trade_journal.db': other.read_bytes(),
        })
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_JOURNAL_ROW_COUNTS_DIFFER')

    def test_duplicate_archive_paths_are_rejected(self):
        b = self.backupdir/'dup.tar.gz'
        with tarfile.open(b, 'w:gz') as t:
            for name, payload in [('runtime/.env', self.env.read_bytes()),
                                  ('runtime/.env', self.env.read_bytes()),
                                  ('runtime/data/trade_journal.db', self.journal.read_bytes())]:
                m = tarfile.TarInfo(name)
                m.size = len(payload)
                t.addfile(m, io.BytesIO(payload))
        b.chmod(0o600)
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_DUPLICATE_MEMBERS')

    def test_backup_missing_env_rejected(self):
        b = self.archive(entries={'runtime/data/trade_journal.db': b'journal'})
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_MISSING_REQUIRED_RUNTIME')

    def test_backup_missing_journal_rejected(self):
        b = self.archive(entries={'runtime/.env': b'config'})
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_MISSING_REQUIRED_RUNTIME')

    def test_archive_older_than_runtime_rejected(self):
        b = self.archive()
        os.utime(self.journal, (b.stat().st_mtime+30, b.stat().st_mtime+30))
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_OLDER_THAN_RUNTIME')

    def test_world_readable_archive_rejected(self):
        b = self.archive(chmod=0o644)
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_MISSING_OR_NOT_PRIVATE_0600')

    def test_backup_in_repo_rejected(self):
        b = self.archive()
        inside = self.repo/'copy.tar.gz'
        inside.write_bytes(b.read_bytes())
        inside.chmod(0o600)
        self.assertEqual(inspect_backup(inside, self.repo, self.env, self.journal),
                         'BACKUP_INSIDE_GIT')

    def test_backup_dir_broad_mode_rejected(self):
        b = self.archive()
        self.backupdir.chmod(0o755)
        self.assertEqual(inspect_backup(b, self.repo, self.env, self.journal),
                         'BACKUP_DIRECTORY_NOT_PRIVATE_0700')

    def test_backup_symlink_refused(self):
        b = self.archive()
        link = self.backupdir/'latest-link.tar.gz'
        link.symlink_to(b)
        self.assertNotEqual(inspect_backup(link, self.repo, self.env, self.journal),
                            'SQLITE_VALIDATED_CURRENT_LOCAL_SNAPSHOT')

    def test_no_backup_does_not_pass(self):
        self.assertEqual(inspect_backup(None, self.repo, self.env, self.journal),
                         'NO_BACKUP_SUPPLIED')

    def test_process_flag_on_and_no_secret_display(self):
        proc = self.p/'proc'/'1234'
        proc.mkdir(parents=True)
        (proc/'environ').write_bytes(
            b'REAL_CREDENTIAL=never-print\x00UDITBOT_TRACKER_V2_ENABLE=1\x00')
        self.assertEqual(process_flag(1234, self.p/'proc'), 'ON')
        self.assertEqual(process_flag(9999, self.p/'proc'), 'UNREADABLE')

    def test_process_flag_duplicate_ambiguous(self):
        proc = self.p/'proc'/'1234'
        proc.mkdir(parents=True)
        (proc/'environ').write_bytes(
            b'UDITBOT_TRACKER_V2_ENABLE=1\x00UDITBOT_TRACKER_V2_ENABLE=0\x00')
        self.assertEqual(process_flag(1234, self.p/'proc'), 'AMBIGUOUS')

    def test_process_flag_off(self):
        proc = self.p/'proc'/'1234'
        proc.mkdir(parents=True)
        (proc/'environ').write_bytes(b'FOO=bar\x00')
        self.assertEqual(process_flag(1234, self.p/'proc'), 'OFF')

    def test_capability_protected_real_proc_uses_privileged_cat_without_root_python(self):
        payload = b"SENSITIVE_VALUE=must-not-print\0UDITBOT_TRACKER_V2_ENABLE=1\0"
        proc = subprocess.CompletedProcess(["sudo"], 0, stdout=payload, stderr=b"")
        with patch("pathlib.Path.read_bytes", side_effect=PermissionError("protected")), \
             patch("deploy.tracker_v2_preflight.subprocess.run", return_value=proc) as called:
            self.assertEqual(process_flag(1234), "ON")
        self.assertEqual(called.call_args.args[0],
                         ["sudo", "-n", "/usr/bin/cat", "/proc/1234/environ"])
        self.assertEqual(called.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertEqual(called.call_args.kwargs["timeout"], 4)

    def test_privileged_proc_read_failure_stays_unreadable(self):
        for outcome in [
            subprocess.CompletedProcess(["sudo"], 1, stdout=b"", stderr=b""),
            subprocess.CompletedProcess(["sudo"], 1,
                                        stdout=b"UDITBOT_TRACKER_V2_ENABLE=1\0", stderr=b""),
        ]:
            with self.subTest(outcome=outcome.returncode):
                with patch("pathlib.Path.read_bytes", side_effect=PermissionError()), \
                     patch("deploy.tracker_v2_preflight.subprocess.run", return_value=outcome):
                    self.assertEqual(process_flag(4321), "UNREADABLE")

    def test_privileged_proc_duplicate_flags_stay_ambiguous(self):
        proc = subprocess.CompletedProcess(
            ["sudo"], 0,
            stdout=b"UDITBOT_TRACKER_V2_ENABLE=1\0UDITBOT_TRACKER_V2_ENABLE=0\0",
            stderr=b"",
        )
        with patch("pathlib.Path.read_bytes", side_effect=PermissionError()), \
             patch("deploy.tracker_v2_preflight.subprocess.run", return_value=proc):
            self.assertEqual(process_flag(4321), "AMBIGUOUS")

    def test_non_proc_fixture_does_not_request_privilege(self):
        with patch("pathlib.Path.read_bytes", side_effect=PermissionError()), \
             patch("deploy.tracker_v2_preflight.subprocess.run") as called:
            self.assertEqual(process_flag(4321, self.p / "proc"), "UNREADABLE")
            called.assert_not_called()

    def test_empty_report_must_not_imply_broker_pass(self):
        r = Report('stopped')
        r.block('OWNER_STOP_NOT_POSITIVELY_VERIFIED')
        self.assertEqual(r.blockers, ['OWNER_STOP_NOT_POSITIVELY_VERIFIED'])

    def test_systemd_parse_success(self):
        p = {'MainPID': '42', 'ActiveState': 'active', 'User': 'operator',
             'Result': 'success', 'EnvironmentFiles': '/etc/foo (ignore_errors=no)',
             'Environment': 'PYTHONUNBUFFERED=1'}
        def fake(args):
            return p[args[4]]
        i = unit_info('trading_bot.service', command=fake)
        self.assertEqual(i['pid'], 42)
        self.assertEqual(i['state'], 'active')

    def test_systemd_parse_unavailable_fails_closed(self):
        i = unit_info('trading_bot.service', command=lambda _: (_ for _ in ()).throw(RuntimeError()))
        self.assertEqual(i['state'], 'UNKNOWN')

    def test_secure_file_rejects_symlink(self):
        link = self.repo/'symlink'
        link.symlink_to(self.env)
        self.assertFalse(_secure_regular_file(link))

if __name__ == '__main__':
    unittest.main()
