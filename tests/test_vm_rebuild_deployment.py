"""CI-safe static and isolated behavioral regression tests for declarative fresh VM provisioning.
Covers ChatGPT baseline checks plus CR-01, H-02, H-03, H-04, and H-05 remediations.
No sudo/systemd/network writes; no real credentials, orders, or live tests.
"""
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class VMRebuildDeploymentTests(unittest.TestCase):
    @staticmethod
    def _bind_sys_executable_path(tmp_path: Path, env_vars: dict) -> None:
        pybin = tmp_path / "_pybin"
        pybin.mkdir(parents=True, exist_ok=True)
        py3 = pybin / "python3"
        if not py3.exists():
            import shlex
            py3.write_text(
                '#!/bin/sh\nexec ' + shlex.quote(sys.executable)
                + ' "$@"\n'
            )
            py3.chmod(0o755)
        env_vars["PATH"] = f"{pybin}:{Path(sys.executable).resolve().parent}:{env_vars.get('PATH', '')}"

    @classmethod
    def setUpClass(cls):
        cls.install = (ROOT / 'deploy/install_vm_services.sh').read_text()
        cls.setup = (ROOT / 'deploy/setup_vm.sh').read_text()
        cls.backup = (ROOT / 'deploy/backup_runtime.sh').read_text()
        cls.restore = (ROOT / 'deploy/restore_runtime.sh').read_text()
        cls.audit = (ROOT / 'deploy/audit_vm_state.sh').read_text()
        cls.bot_cli = (ROOT / 'bot').read_text()
        cls.refresh_svc = (ROOT / 'deploy/dhan-token-refresh.service').read_text()
        cls.refresh_tmr = (ROOT / 'deploy/dhan-token-refresh.timer').read_text()
        cls.cfg_mgr = (ROOT / 'src/engine/config_manager.py').read_text()

    def test_shell_syntax(self):
        for rel in (
            'deploy/setup_vm.sh',
            'deploy/install_vm_services.sh',
            'deploy/audit_vm_state.sh',
            'deploy/backup_runtime.sh',
            'deploy/restore_runtime.sh',
            'bot',
        ):
            with self.subTest(script=rel):
                self.assertEqual(
                    subprocess.run(['bash', '-n', str(ROOT / rel)], capture_output=True).returncode,
                    0,
                )

    def test_setup_invokes_installer_after_wizard(self):
        self.assertIn('deploy/install_vm_services.sh" --install', self.setup)
        self.assertGreater(
            self.setup.index('install_vm_services.sh'),
            self.setup.index('src.setup_wizard'),
        )

    def test_setup_restore_mode_runs_before_service_start_and_skips_wizard(self):
        """Verifies --restore mode runs restore_runtime.sh before install_vm_services.sh starts WebUI."""
        self.assertIn('--restore', self.setup)
        self.assertIn('deploy/restore_runtime.sh" "$RESTORE_ARCHIVE"', self.setup)
        self.assertGreater(
            self.setup.index('install_vm_services.sh'),
            self.setup.index('restore_runtime.sh'),
        )

    def test_H05_setup_runs_non_mutating_checks_by_default_and_gates_full_suite(self):
        """H-05: Verifies setup_vm.sh runs non-mutating py_compile + test_vm_rebuild_deployment by default and gates full discover behind RUN_FULL_TESTS."""
        self.assertIn('RUN_FULL_TESTS="${AUDITBOT_RUN_FULL_TESTS:-0}"', self.setup)
        self.assertIn('python3 -B -m py_compile', self.setup)
        self.assertIn('PYTHONPYCACHEPREFIX=', self.setup)
        self.assertIn('if [[ "$RUN_FULL_TESTS" == "1" ]]; then', self.setup)

    def test_sudo_privilege_not_broad(self):
        self.assertIn('NOPASSWD: /usr/bin/systemctl restart trading_bot.service', self.install)
        self.assertNotIn('NOPASSWD: /bin/systemctl, /usr/bin/systemctl', self.install)
        self.assertIn('/usr/sbin/visudo -cf', self.install)

    def test_port80_capability_fix(self):
        self.assertIn('AmbientCapabilities=CAP_NET_BIND_SERVICE', self.install)
        self.assertIn('NoNewPrivileges=false', self.install)
        self.assertNotIn('\nCapabilityBoundingSet=', self.install)
        self.assertIn('--host 127.0.0.1 --port 80', self.install)
        self.assertNotIn('--host 0.0.0.0', self.install)

    def test_preserves_existing_units(self):
        self.assertIn('WEB_EXISTED=1', self.install)
        self.assertIn('"PRESERVE $name"', self.install)
        self.assertIn('if [[ "$WEB_EXISTED" == 0 ]]', self.install)
        self.assertIn('if [[ "$TIMER_EXISTED" == 0 ]]', self.install)

    def test_does_not_start_trading_service(self):
        self.assertNotIn('systemctl start trading_bot.service', self.install)
        self.assertNotIn(
            'systemctl restart trading_bot.service',
            self.install.split('cat >"$TMP/auditbot-web-restart"')[0],
        )
        self.assertIn('AUDITBOT_ENABLE_TRADING_BOOT', self.install)

    def test_dynamic_user_and_repo_path(self):
        self.assertIn('BOT_USER="$(id -un)"', self.install)
        self.assertIn('REPO_DIR="$(cd', self.install)
        self.assertNotIn('/home/udit_raikhere11/', self.install)

    def test_static_deploy_unit_templates_have_no_hardcoded_user_or_catchup_conflict(self):
        """Ensures legacy deploy/*.service and deploy/*.timer templates align with install_vm_services.sh."""
        self.assertIn('install_vm_services.sh --install', self.refresh_svc)
        self.assertIn('UMask=0077', self.refresh_svc)
        self.assertIn('Persistent=false', self.refresh_tmr)
        self.assertNotIn('Persistent=true', self.refresh_tmr)

    def test_weekday_refresh_no_unplanned_catchup(self):
        self.assertIn('Mon..Fri *-*-* 08:00:00 Asia/Kolkata', self.install)
        self.assertIn('AUDITBOT_TOKEN_CATCHUP:-0', self.install)
        self.assertIn('bot daily_health_check', self.install)

    def test_backup_is_encrypted_and_sqlite_consistent(self):
        self.assertIn('gpg', self.backup)
        self.assertIn('--symmetric --cipher-algo AES256', self.backup)
        self.assertIn('db.backup(backup)', self.backup)
        self.assertIn('"$REPO_DIR/.env"', self.backup)
        self.assertIn("cfg.dhan_token_file", self.backup)
        self.assertIn("'scrip_master.csv'", self.backup)
        self.assertIn("'listener.lock'", self.backup)
        self.assertIn("'tg_userbot.session'", self.backup)

    def test_restore_guards(self):
        self.assertIn('TRADING_MODE=TEST', self.restore)
        self.assertIn('systemctl is-active --quiet', self.restore)
        self.assertIn('Existing journal detected: refuse overwrite', self.restore)
        self.assertIn('m.issym()', self.restore)
        self.assertNotIn('systemctl start', self.restore)
        self.assertIn("_normalize_target_path", self.restore)
        self.assertIn("cleanup_and_rollback", self.restore)

    def test_inventory_does_not_read_secrets_and_includes_H02_drift_checks(self):
        self.assertIn('stat -c', self.audit)
        self.assertNotIn('cat "$REPO_DIR/.env"', self.audit)
        self.assertNotIn('cat "$HOME/.dhan_token"', self.audit)
        self.assertIn('DRIFT_WARN: Legacy /etc/sudoers.d/auditbot-web exists', self.audit)
        self.assertIn('DRIFT_WARN: Public 0.0.0.0:80', self.audit)

    def test_config_manager_uses_noninteractive_sudo_and_preserves_stopped_service(self):
        """Verifies Save/Apply uses sudo -n /usr/bin/systemctl restart trading_bot.service and preserves STOPPED state."""
        self.assertIn('"/usr/bin/systemctl"', self.cfg_mgr)
        self.assertIn('"-n"', self.cfg_mgr)
        from src.engine.config_manager import ConfigApplyState, execute_configuration_update
        with tempfile.TemporaryDirectory() as tmpdir:
            env_p = os.path.join(tmpdir, ".env")
            data_d = os.path.join(tmpdir, "data")
            os.makedirs(data_d, exist_ok=True)
            with open(env_p, "w") as f:
                f.write("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=25000\n")
            restart_calls = []
            rec = execute_configuration_update(
                form_params={"TRADING_MODE": "TEST", "EQUITY_BUDGET_INR": 30000.0},
                env_content="TRADING_MODE=TEST\nEQUITY_BUDGET_INR=30000.0\n",
                data_dir=data_d,
                env_path=env_p,
                check_service_fn=lambda: False,
                systemctl_restart_fn=lambda: (restart_calls.append(1) or True),
            )
            self.assertEqual(rec.state, ConfigApplyState.SAVED_FOR_NEXT_START)
            self.assertEqual(len(restart_calls), 0, "Stopped service must NEVER be restarted on Save/Apply")

    def test_CR01_failed_or_incomplete_restore_leaves_zero_partial_files_and_allows_retry(self):
        """
        CR-01 Release-Blocker Regression Test:
        1. Encrypted archive with `runtime/.env` but NO `runtime/data` directory fails cleanly and leaves NO `.env` behind.
        2. Encrypted archive with corrupt `runtime/data/trade_journal.db` fails `PRAGMA quick_check` in staging and leaves NO `.env` behind.
        3. Simulated mid-copy failure after writing `.env` rolls back `.env` via `cleanup_and_rollback` trap.
        4. Subsequent retry with a valid archive on the exact same directory succeeds cleanly without manual cleanup!
        """
        if shutil.which("gpg") is None:
            self.skipTest("gpg binary not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            new_repo = tmp_path / "new_vm_repo"
            new_home = tmp_path / "new_vm_home"
            (new_repo / "deploy").mkdir(parents=True)
            (new_repo / "data").mkdir(parents=True)
            new_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/restore_runtime.sh", new_repo / "deploy/restore_runtime.sh")
            (new_repo / "data" / "holidays.json").write_text('{"holidays": []}')

            pass_file = tmp_path / "passphrase.txt"
            pass_file.write_text("cr01-test-passphrase-2026\n")
            os.chmod(pass_file, 0o600)

            env_vars = os.environ.copy()
            env_vars["HOME"] = str(new_home)
            env_vars["AUDITBOT_RESTORE_HOME"] = str(new_home)
            env_vars["AUDITBOT_BACKUP_PASSPHRASE_FILE"] = str(pass_file)
            self._bind_sys_executable_path(tmp_path, env_vars)
            env_vars["AUDITBOT_TEST_ISOLATION"] = "1"
            env_vars["AUDITBOT_SKIP_SYSTEMCTL_CHECK"] = "1"

            def _make_gpg_archive(stage_root: Path, out_gpg: Path):
                tar_bytes_proc = subprocess.run(
                    ["tar", "-czf", "-", "-C", str(stage_root), "runtime"],
                    capture_output=True,
                    check=True,
                )
                subprocess.run(
                    [
                        "gpg", "--batch", "--yes", "--pinentry-mode", "loopback",
                        "--passphrase-file", str(pass_file),
                        "--symmetric", "--cipher-algo", "AES256", "-o", str(out_gpg),
                    ],
                    input=tar_bytes_proc.stdout,
                    check=True,
                )

            # Case 1: Archive containing runtime/.env but NO runtime/data directory (exact reviewer reproduction!)
            stage1 = tmp_path / "stage_no_data"
            (stage1 / "runtime").mkdir(parents=True)
            (stage1 / "runtime" / ".env").write_text("TRADING_MODE=TEST\n")
            bad_gpg_1 = tmp_path / "bad_no_data.tar.gz.gpg"
            _make_gpg_archive(stage1, bad_gpg_1)

            r1 = subprocess.run(
                ["bash", str(new_repo / "deploy/restore_runtime.sh"), str(bad_gpg_1)],
                cwd=str(new_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(r1.returncode, 0)
            self.assertFalse((new_repo / ".env").exists(), "CR-01: Failed restore must NEVER leave partial .env on disk!")

            # Case 2: Archive containing runtime/.env and runtime/data/trade_journal.db with corrupt SQLite bytes
            stage2 = tmp_path / "stage_corrupt_db"
            (stage2 / "runtime" / "data").mkdir(parents=True)
            (stage2 / "runtime" / ".env").write_text("TRADING_MODE=TEST\n")
            (stage2 / "runtime" / "data" / "trade_journal.db").write_bytes(b"SQLite format 3\x00CORRUPT_BYTES_HERE")
            bad_gpg_2 = tmp_path / "bad_corrupt_db.tar.gz.gpg"
            _make_gpg_archive(stage2, bad_gpg_2)

            r2 = subprocess.run(
                ["bash", str(new_repo / "deploy/restore_runtime.sh"), str(bad_gpg_2)],
                cwd=str(new_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(r2.returncode, 0)
            self.assertFalse((new_repo / ".env").exists(), "CR-01: Corrupt DB in archive must not leave .env on disk!")

            # Case 3: Valid archive, but simulated mid-copy failure after writing .env -> rollback trap cleans up .env
            stage3 = tmp_path / "stage_valid"
            (stage3 / "runtime" / "data").mkdir(parents=True)
            (stage3 / "runtime" / "home").mkdir(parents=True)
            # Also include a custom path whose parent directory EXISTS on this machine to test H-03 normalization!
            existing_other_dir = tmp_path / "existing_old_custom_dir"
            existing_other_dir.mkdir(parents=True)
            (stage3 / "runtime" / ".env").write_text(
                f"TRADING_MODE=LIVE\n"
                f"DATA_DIR={existing_other_dir / 'data'}\n"
                f"JOURNAL_DB_PATH={existing_other_dir / 'data' / 'trade_journal.db'}\n"
                f"DHAN_TOKEN_FILE={existing_other_dir / '.dhan_token'}\n"
            )
            (stage3 / "runtime" / "home" / ".dhan_token").write_text("SYNTHETIC_TOKEN\n")
            good_db = stage3 / "runtime" / "data" / "trade_journal.db"
            con = sqlite3.connect(str(good_db))
            con.execute("CREATE TABLE t (id INT);")
            con.execute("INSERT INTO t VALUES (42);")
            con.commit()
            con.close()
            good_gpg = tmp_path / "good.tar.gz.gpg"
            _make_gpg_archive(stage3, good_gpg)

            env_sim_fail = env_vars.copy()
            env_sim_fail["AUDITBOT_SIMULATE_MID_RESTORE_FAILURE"] = "1"
            r3 = subprocess.run(
                ["bash", str(new_repo / "deploy/restore_runtime.sh"), str(good_gpg)],
                cwd=str(new_repo),
                env=env_sim_fail,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(r3.returncode, 0)
            self.assertFalse((new_repo / ".env").exists(), "CR-01: Rollback trap must remove .env on mid-restore failure!")

            # Case 4: Now retry with the valid archive on the exact same directory -> MUST SUCCEED!
            r4 = subprocess.run(
                ["bash", str(new_repo / "deploy/restore_runtime.sh"), str(good_gpg)],
                cwd=str(new_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(r4.returncode, 0, f"Retry after failed restore must succeed: {r4.stderr}")
            restored_env = (new_repo / ".env").read_text()
            self.assertIn("TRADING_MODE=TEST", restored_env)
            # H-03 Verification: Even though existing_other_dir existed on disk, paths were normalized to new_repo and new_home!
            self.assertIn(f"DATA_DIR={new_repo.resolve() / 'data'}", restored_env)
            self.assertIn(f"JOURNAL_DB_PATH={new_repo.resolve() / 'data' / 'trade_journal.db'}", restored_env)
            self.assertIn(f"DHAN_TOKEN_FILE={new_home.resolve() / '.dhan_token'}", restored_env)

    def test_end_to_end_encrypted_backup_and_restore_roundtrip(self):
        """Executes backup_runtime.sh and restore_runtime.sh in an isolated sandbox using GPG loopback passphrase."""
        if shutil.which("gpg") is None:
            self.skipTest("gpg binary not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            fake_repo = tmp_path / "repo"
            fake_home = tmp_path / "home"
            (fake_repo / "deploy").mkdir(parents=True)
            (fake_repo / "data").mkdir(parents=True)
            fake_home.mkdir(parents=True)

            shutil.copy2(ROOT / "deploy/backup_runtime.sh", fake_repo / "deploy/backup_runtime.sh")
            shutil.copy2(ROOT / "deploy/restore_runtime.sh", fake_repo / "deploy/restore_runtime.sh")
            os.symlink(ROOT / "src", fake_repo / "src")

            token_file = fake_home / ".dhan_token"
            token_file.write_text("SYNTHETIC_TEST_TOKEN_DO_NOT_USE\n")
            os.chmod(token_file, 0o600)

            env_file = fake_repo / ".env"
            env_file.write_text(
                f"TRADING_MODE=LIVE\n"
                f"DATA_DIR={fake_repo / 'data'}\n"
                f"JOURNAL_DB_PATH={fake_repo / 'data' / 'trade_journal.db'}\n"
                f"DHAN_TOKEN_FILE={token_file}\n"
                f"TG_SESSION_STRING=SYNTHETIC_SESSION_STR\n"
            )
            os.chmod(env_file, 0o600)

            db_file = fake_repo / "data" / "trade_journal.db"
            conn = sqlite3.connect(str(db_file))
            conn.execute("CREATE TABLE test_marker (id INTEGER PRIMARY KEY, note TEXT);")
            conn.execute("INSERT INTO test_marker (note) VALUES ('survived_backup');")
            conn.commit()
            conn.close()

            # Create a real SQLite tg_userbot.session to verify H-04 SQLite online backup + quick_check on session DB
            sess_db = sqlite3.connect(str(fake_repo / "data" / "tg_userbot.session"))
            sess_db.execute("CREATE TABLE sessions (dc_id INT, server_address TEXT, port INT, auth_key BLOB);")
            sess_db.execute("INSERT INTO sessions VALUES (2, '149.154.167.50', 443, ?);", (os.urandom(256),))
            sess_db.commit()
            sess_db.close()

            (fake_repo / "data" / "emergency_stop.json").write_text('{"is_stopped": false}')
            (fake_repo / "data" / "holidays.json").write_text('{"holidays": []}')
            (fake_repo / "data" / "scrip_master.csv").write_text("DUMMY_50MB_CSV")
            (fake_repo / "data" / "listener.lock").write_text("99999")

            pass_file = tmp_path / "passphrase.txt"
            pass_file.write_text("test-only-synthetic-passphrase-2026\n")
            os.chmod(pass_file, 0o600)

            archive_out = tmp_path / "outside" / "recovery.tar.gz.gpg"
            env_vars = os.environ.copy()
            env_vars["HOME"] = str(fake_home)
            env_vars["AUDITBOT_BACKUP_PASSPHRASE_FILE"] = str(pass_file)
            self._bind_sys_executable_path(tmp_path, env_vars)
            env_vars["AUDITBOT_TEST_ISOLATION"] = "1"
            env_vars["AUDITBOT_SKIP_SYSTEMCTL_CHECK"] = "1"

            b_res = subprocess.run(
                ["bash", str(fake_repo / "deploy/backup_runtime.sh"), str(archive_out)],
                cwd=str(fake_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(b_res.returncode, 0, f"Backup failed: {b_res.stderr}")
            self.assertTrue(archive_out.is_file())
            self.assertEqual(stat.S_IMODE(archive_out.stat().st_mode), 0o600)

            new_repo = tmp_path / "new_vm_repo"
            new_home = tmp_path / "new_vm_home"
            (new_repo / "deploy").mkdir(parents=True)
            (new_repo / "data").mkdir(parents=True)
            new_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/restore_runtime.sh", new_repo / "deploy/restore_runtime.sh")
            (new_repo / "data" / "holidays.json").write_text('{"holidays": []}')

            shutil.rmtree(fake_repo)
            shutil.rmtree(fake_home)

            env_vars["HOME"] = str(new_home)
            env_vars["AUDITBOT_RESTORE_HOME"] = str(new_home)
            r_res = subprocess.run(
                ["bash", str(new_repo / "deploy/restore_runtime.sh"), str(archive_out)],
                cwd=str(new_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(r_res.returncode, 0, f"Restore failed: {r_res.stderr}")

            restored_env = (new_repo / ".env").read_text()
            self.assertIn("TRADING_MODE=TEST", restored_env)
            self.assertNotIn("TRADING_MODE=LIVE", restored_env)
            self.assertIn("TG_SESSION_STRING=SYNTHETIC_SESSION_STR", restored_env)
            self.assertEqual(stat.S_IMODE((new_repo / ".env").stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((new_home / ".dhan_token").stat().st_mode), 0o600)
            self.assertFalse((new_repo / "data" / "scrip_master.csv").exists())
            self.assertFalse((new_repo / "data" / "listener.lock").exists())
            self.assertTrue((new_repo / "data" / "tg_userbot.session").is_file())

            r_conn = sqlite3.connect(str(new_repo / "data" / "trade_journal.db"))
            qc = r_conn.execute("PRAGMA quick_check;").fetchone()[0]
            row = r_conn.execute("SELECT note FROM test_marker;").fetchone()[0]
            r_conn.close()
            self.assertEqual(qc, "ok")
            self.assertEqual(row, "survived_backup")

    def test_tg_session_string_export_without_otp_reregistration(self):
        """Verifies export_existing_session_to_env exports SQLite session into TG_SESSION_STRING in .env (0600)."""
        from src.config import AppConfig
        from src.tg_login import export_existing_session_to_env

        with tempfile.TemporaryDirectory() as tmpdir:
            data_d = os.path.join(tmpdir, "data")
            os.makedirs(data_d, exist_ok=True)
            sess_path = os.path.join(data_d, "tg_userbot.session")
            conn = sqlite3.connect(sess_path)
            conn.execute(
                "CREATE TABLE sessions (dc_id INTEGER PRIMARY KEY, server_address TEXT, port INTEGER, auth_key BLOB, takeout_id INTEGER);"
            )
            conn.execute(
                "INSERT INTO sessions VALUES (2, '149.154.167.50', 443, ?, NULL);",
                (os.urandom(256),),
            )
            conn.commit()
            conn.close()

            env_p = os.path.join(tmpdir, ".env")
            with open(env_p, "w") as f:
                f.write("TRADING_MODE=TEST\n")

            cfg = AppConfig(data_dir=data_d)
            ok = export_existing_session_to_env(cfg, env_path=env_p)
            self.assertTrue(ok)
            env_text = Path(env_p).read_text()
            self.assertIn('TG_SESSION_STRING="1', env_text)
            self.assertEqual(stat.S_IMODE(os.stat(env_p).st_mode), 0o600)

    def test_webui_save_apply_reaches_applied_verified_and_rolls_back_on_restart_failure(self):
        # Verifies Save/Apply reaches APPLIED_VERIFIED when active service acknowledges hash, and rolls back .env on failure.
        from src.engine.config_manager import (
            ConfigApplyState,
            ConfigChangeTracker,
            compute_config_version,
            execute_configuration_update,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            env_p = os.path.join(tmpdir, ".env")
            data_d = os.path.join(tmpdir, "data")
            os.makedirs(data_d, exist_ok=True)
            orig_env = "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=25000.0\n"
            with open(env_p, "w") as f:
                f.write(orig_env)

            new_params = {"TRADING_MODE": "TEST", "EQUITY_BUDGET_INR": 35000.0}
            new_env = "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=35000.0\n"
            tracker = ConfigChangeTracker(data_dir=data_d)

            def _sim_restart_ok():
                tracker.record_active_running_version(compute_config_version(new_params), trading_mode="TEST")
                return True

            rec_ok = execute_configuration_update(
                form_params=new_params,
                env_content=new_env,
                data_dir=data_d,
                env_path=env_p,
                check_service_fn=lambda: True,
                systemctl_restart_fn=_sim_restart_ok,
                verify_delay_sec=0.05,
            )
            self.assertEqual(rec_ok.state, ConfigApplyState.APPLIED_VERIFIED)
            self.assertFalse(rec_ok.rolled_back)

            fail_params = {"TRADING_MODE": "TEST", "EQUITY_BUDGET_INR": 45000.0}
            fail_env = "TRADING_MODE=TEST\nEQUITY_BUDGET_INR=45000.0\n"
            rec_fail = execute_configuration_update(
                form_params=fail_params,
                env_content=fail_env,
                data_dir=data_d,
                env_path=env_p,
                check_service_fn=lambda: True,
                systemctl_restart_fn=lambda: False,
                verify_delay_sec=0.05,
            )
            self.assertEqual(rec_fail.state, ConfigApplyState.APPLY_FAILED)
            self.assertTrue(rec_fail.rolled_back)
            self.assertIn("35000.0", Path(env_p).read_text())

    def test_R01_missing_or_empty_journal_rejected_in_backup_and_restore_with_retry_and_exception_flag(self):
        # R-01 Regression Test:
        # 1. backup_runtime.sh fails early when trade_journal.db is missing or has no tables (unless --allow-missing-journal).
        # 2. restore_runtime.sh rejects an archive with empty runtime/data/ (no trade_journal.db) or schema-less trade_journal.db,
        #    leaves zero partial .env files on disk, allows retry with a valid archive, and supports --allow-missing-journal.
        if shutil.which("gpg") is None:
            self.skipTest("gpg binary not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pass_file = tmp_path / "passphrase.txt"
            pass_file.write_text("r01-test-passphrase-2026\n")
            os.chmod(pass_file, 0o600)

            # Part A: backup_runtime.sh refuses when trade_journal.db is missing
            b_repo = tmp_path / "b_repo"
            b_home = tmp_path / "b_home"
            (b_repo / "deploy").mkdir(parents=True)
            (b_repo / "data").mkdir(parents=True)
            b_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/backup_runtime.sh", b_repo / "deploy/backup_runtime.sh")
            os.symlink(ROOT / "src", b_repo / "src")
            (b_repo / ".env").write_text("TRADING_MODE=TEST\n")

            env_vars = os.environ.copy()
            env_vars["HOME"] = str(b_home)
            env_vars["AUDITBOT_BACKUP_PASSPHRASE_FILE"] = str(pass_file)
            self._bind_sys_executable_path(tmp_path, env_vars)
            env_vars["AUDITBOT_TEST_ISOLATION"] = "1"
            env_vars["AUDITBOT_SKIP_SYSTEMCTL_CHECK"] = "1"

            out_missing_gpg = tmp_path / "out_missing.tar.gz.gpg"
            b_fail = subprocess.run(
                ["bash", str(b_repo / "deploy/backup_runtime.sh"), str(out_missing_gpg)],
                cwd=str(b_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(b_fail.returncode, 0, "R-01: Backup must fail when trade_journal.db is missing!")
            self.assertIn("R-01", b_fail.stderr)
            self.assertFalse(out_missing_gpg.exists())

            # With explicit --allow-missing-journal flag, backup succeeds for an uninitialized setup
            b_allow = subprocess.run(
                ["bash", str(b_repo / "deploy/backup_runtime.sh"), "--allow-missing-journal", str(out_missing_gpg)],
                cwd=str(b_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(b_allow.returncode, 0, f"Backup with --allow-missing-journal failed: {b_allow.stderr}")
            self.assertTrue(out_missing_gpg.is_file())

            # Part B: restore_runtime.sh rejects out_missing_gpg by default (missing runtime/data/trade_journal.db)
            r_repo = tmp_path / "r_repo"
            r_home = tmp_path / "r_home"
            (r_repo / "deploy").mkdir(parents=True)
            (r_repo / "data").mkdir(parents=True)
            r_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/restore_runtime.sh", r_repo / "deploy/restore_runtime.sh")
            env_vars["HOME"] = str(r_home)
            env_vars["AUDITBOT_RESTORE_HOME"] = str(r_home)

            r_missing = subprocess.run(
                ["bash", str(r_repo / "deploy/restore_runtime.sh"), str(out_missing_gpg)],
                cwd=str(r_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(r_missing.returncode, 0, "R-01: Restore must reject archive missing trade_journal.db!")
            self.assertIn("R-01", r_missing.stderr)
            self.assertFalse((r_repo / ".env").exists(), "R-01: Failed restore must leave zero .env on disk!")

            # Part C: Schema-less SQLite DB (0 tables) is also rejected by default
            empty_db = b_repo / "data" / "trade_journal.db"
            con = sqlite3.connect(str(empty_db))
            con.execute("PRAGMA user_version = 1;")
            con.commit()
            con.close()
            out_schemaless_gpg = tmp_path / "out_schemaless.tar.gz.gpg"
            b_schemaless = subprocess.run(
                ["bash", str(b_repo / "deploy/backup_runtime.sh"), str(out_schemaless_gpg)],
                cwd=str(b_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(b_schemaless.returncode, 0, "R-01: Backup must reject schema-less trade_journal.db!")

            # Part D: Retry with --allow-missing-journal on r_repo succeeds cleanly
            r_allow = subprocess.run(
                ["bash", str(r_repo / "deploy/restore_runtime.sh"), "--allow-missing-journal", str(out_missing_gpg)],
                cwd=str(r_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(r_allow.returncode, 0, f"Restore with --allow-missing-journal failed: {r_allow.stderr}")
            self.assertTrue((r_repo / ".env").is_file())

    def test_R02_hidden_runtime_dotfiles_preserved_and_forbidden_lockfiles_excluded(self):
        # R-02 Regression Test:
        # Verifies that a hidden runtime file (`data/.hidden_recovery_state`) survives backup -> restore
        # with 0600 permissions, while forbidden lock/temp files (`data/.stale.lock`, `data/listener.lock`) are excluded.
        if shutil.which("gpg") is None:
            self.skipTest("gpg binary not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pass_file = tmp_path / "passphrase.txt"
            pass_file.write_text("r02-test-passphrase-2026\n")
            os.chmod(pass_file, 0o600)

            src_repo = tmp_path / "src_repo"
            src_home = tmp_path / "src_home"
            (src_repo / "deploy").mkdir(parents=True)
            (src_repo / "data").mkdir(parents=True)
            src_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/backup_runtime.sh", src_repo / "deploy/backup_runtime.sh")
            os.symlink(ROOT / "src", src_repo / "src")
            (src_repo / ".env").write_text("TRADING_MODE=TEST\n")

            db_p = src_repo / "data" / "trade_journal.db"
            con = sqlite3.connect(str(db_p))
            con.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, symbol TEXT);")
            con.execute("INSERT INTO signals (symbol) VALUES ('SBIN');")
            con.commit()
            con.close()

            # Create allowed hidden dotfile and forbidden hidden lock/tmp files
            (src_repo / "data" / ".hidden_recovery_state").write_text('{"seq": 108}')
            (src_repo / "data" / ".stale.lock").write_text("1234")
            (src_repo / "data" / ".temp.tmp").write_text("temp")

            env_vars = os.environ.copy()
            env_vars["HOME"] = str(src_home)
            env_vars["AUDITBOT_BACKUP_PASSPHRASE_FILE"] = str(pass_file)
            self._bind_sys_executable_path(tmp_path, env_vars)
            env_vars["AUDITBOT_TEST_ISOLATION"] = "1"
            env_vars["AUDITBOT_SKIP_SYSTEMCTL_CHECK"] = "1"

            gpg_out = tmp_path / "r02_bundle.tar.gz.gpg"
            b_res = subprocess.run(
                ["bash", str(src_repo / "deploy/backup_runtime.sh"), str(gpg_out)],
                cwd=str(src_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(b_res.returncode, 0, f"Backup failed: {b_res.stderr}")

            dst_repo = tmp_path / "dst_repo"
            dst_home = tmp_path / "dst_home"
            (dst_repo / "deploy").mkdir(parents=True)
            (dst_repo / "data").mkdir(parents=True)
            dst_home.mkdir(parents=True)
            shutil.copy2(ROOT / "deploy/restore_runtime.sh", dst_repo / "deploy/restore_runtime.sh")
            env_vars["HOME"] = str(dst_home)
            env_vars["AUDITBOT_RESTORE_HOME"] = str(dst_home)

            r_res = subprocess.run(
                ["bash", str(dst_repo / "deploy/restore_runtime.sh"), str(gpg_out)],
                cwd=str(dst_repo),
                env=env_vars,
                capture_output=True,
                text=True,
            )
            self.assertEqual(r_res.returncode, 0, f"Restore failed: {r_res.stderr}")
            hidden_restored = dst_repo / "data" / ".hidden_recovery_state"
            self.assertTrue(hidden_restored.is_file(), "R-02: Hidden dotfile .hidden_recovery_state must be restored!")
            self.assertEqual(hidden_restored.read_text(), '{"seq": 108}')
            self.assertEqual(stat.S_IMODE(hidden_restored.stat().st_mode), 0o600)
            self.assertFalse((dst_repo / "data" / ".stale.lock").exists())
            self.assertFalse((dst_repo / "data" / ".temp.tmp").exists())

    def test_bot_cli_exposes_audit_backup_restore_and_export_session(self):
        for cmd_token in ("audit|audit-vm)", "backup|backup-runtime)", "restore|restore-runtime)", "export-session)"):
            self.assertIn(cmd_token, self.bot_cli)


if __name__ == '__main__':
    unittest.main()
