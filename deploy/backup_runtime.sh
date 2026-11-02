#!/usr/bin/env bash
# Encrypted recovery bundle of mutable local state; copy file OFF the VM.
# Usage: bash deploy/backup_runtime.sh [--allow-live-backup] /path/outside/repo/auditbot-20261031.tar.gz.gpg
set -euo pipefail
umask 077
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
[[ "$(id -u)" != 0 ]] || { echo 'Run as AuditBot owner, not root' >&2; exit 2; }
ALLOW_LIVE="${AUDITBOT_ALLOW_LIVE_BACKUP:-0}"
ALLOW_MISSING_JOURNAL="${AUDITBOT_ALLOW_MISSING_JOURNAL:-0}"
while [[ $# -gt 1 ]]; do
    case "$1" in
        --allow-live-backup)
            ALLOW_LIVE=1
            shift
            ;;
        --allow-missing-journal)
            ALLOW_MISSING_JOURNAL=1
            shift
            ;;
        *)
            echo 'Usage: bash deploy/backup_runtime.sh [--allow-live-backup] [--allow-missing-journal] /path/to/backup.tar.gz.gpg' >&2
            exit 2
            ;;
    esac
done
[[ $# == 1 ]] || { echo 'Usage: bash deploy/backup_runtime.sh [--allow-live-backup] [--allow-missing-journal] /path/to/backup.tar.gz.gpg' >&2; exit 2; }
OUT="$(realpath -m -- "$1")"
[[ "$OUT" != "$REPO_DIR"/* ]] || { echo 'Backup must be OUTSIDE Git checkout' >&2; exit 2; }
[[ ! -e "$OUT" ]] || { echo 'Backup already exists; refusing overwrite' >&2; exit 2; }
command -v gpg >/dev/null || { echo 'Install gpg before backing up' >&2; exit 2; }
command -v tar >/dev/null || exit 2
# H-04: Quiet-window check so live listener does not mutate session/JSON state mid-backup unless explicitly authorized
if [[ "${AUDITBOT_TEST_ISOLATION:-0}" != "1" || "${AUDITBOT_SKIP_SYSTEMCTL_CHECK:-0}" != "1" ]]; then
    if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet trading_bot.service; then
        if [[ "$ALLOW_LIVE" != "1" ]]; then
            echo 'REFUSED (H-04): trading_bot.service is active. Stop trading_bot.service first for a coordinated snapshot, or pass --allow-live-backup.' >&2
            exit 2
        fi
        echo 'WARNING (H-04): Running live backup while trading_bot.service is active; a stopped bot is preferred for final recovery points.' >&2
    fi
fi
cd "$REPO_DIR"
[[ -f "$REPO_DIR/.env" ]] || { echo '.env missing' >&2; exit 2; }
[[ -d "$REPO_DIR/data" ]] || { echo 'data directory missing' >&2; exit 2; }
PYTHON_BIN="$REPO_DIR/venv/bin/python3"
[[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="python3"
mkdir -p "$(dirname "$OUT")"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
mkdir -p "$TMP/runtime/data" "$TMP/runtime/home"
cp -p "$REPO_DIR/.env" "$TMP/runtime/.env"
# Query configured locations from existing config, but don't print secrets.
ALLOW_MISSING_JOURNAL="$ALLOW_MISSING_JOURNAL" REPO_DIR="$REPO_DIR" STAGING="$TMP/runtime" "$PYTHON_BIN" - <<'PY'
import os, shutil, sqlite3
from pathlib import Path
from src.config import AppConfig
root = Path(os.environ['REPO_DIR'])
stage = Path(os.environ['STAGING'])
allow_missing_journal = (os.environ.get('ALLOW_MISSING_JOURNAL', '0') == '1')
os.chdir(root)
cfg = AppConfig.from_env()
data = Path(cfg.data_dir).expanduser().resolve()
if not data.is_dir():
    raise SystemExit('Configured DATA_DIR unavailable')
EXCLUDED_RUNTIME_FILES = {
    'trade_journal.db',
    'trade_journal.db-wal',
    'trade_journal.db-shm',
    'tg_userbot.session',
    'tg_userbot.session-journal',
    'tg_userbot.session-wal',
    'tg_userbot.session-shm',
    'holidays.json',
    'scrip_master.csv',
    'listener.lock',
    'emergency_stop.json.lock',
}
EXCLUDED_SUFFIXES = ('.lock', '.tmp', '.swp', '-wal', '-shm', '-journal')
for item in data.iterdir():
    if item.is_symlink():
        continue
    if (
        item.is_file()
        and item.name not in EXCLUDED_RUNTIME_FILES
        and not item.name.endswith(EXCLUDED_SUFFIXES)
    ):
        shutil.copy2(item, stage/'data'/item.name)

def _sqlite_consistent_backup(src_path: Path, dst_path: Path, require_valid_schema: bool = False) -> None:
    if not src_path.is_file():
        if require_valid_schema:
            raise SystemExit(
                f'R-01: Required trading journal missing at {src_path} (pass --allow-missing-journal only for uninitialized setups)'
            )
        return
    head = src_path.read_bytes()[:16]
    if not head.startswith(b'SQLite format 3'):
        if require_valid_schema:
            raise SystemExit(f'R-01: Trading journal at {src_path} is not a valid SQLite database')
        shutil.copy2(src_path, dst_path)
        return
    db = sqlite3.connect(f'file:{src_path}?mode=ro', uri=True, timeout=10)
    try:
        qc = db.execute('PRAGMA quick_check;').fetchone()
        if not qc or qc[0] != 'ok':
            raise SystemExit(f'R-01: SQLite quick_check failed on {src_path.name}: {qc}')
        if require_valid_schema:
            tbl_count = db.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            ).fetchone()[0]
            if not tbl_count or int(tbl_count) < 1:
                raise SystemExit(f'R-01: Trading journal {src_path.name} contains no application tables')
        backup = sqlite3.connect(str(dst_path))
        try:
            with backup:
                db.backup(backup)
        finally:
            backup.close()
    finally:
        db.close()

# R-01 & H-04: Validate and snapshot trade_journal.db (mandatory unless --allow-missing-journal) and tg_userbot.session
source = Path(cfg.journal_db_path).expanduser().resolve()
_sqlite_consistent_backup(source, stage / 'data' / 'trade_journal.db', require_valid_schema=(not allow_missing_journal))
session_src = data / 'tg_userbot.session'
_sqlite_consistent_backup(session_src, stage / 'data' / 'tg_userbot.session', require_valid_schema=False)

token_path = Path(cfg.dhan_token_file).expanduser()
if token_path.is_file():
    shutil.copy2(token_path, stage/'home'/'.dhan_token')
else:
    print('WARNING: Authoritative Dhan token file missing; backup incomplete')
PY
# All sensitive backup content is encrypted; no plaintext archive is emitted.
# The temporary plaintext staging directory is mode 0700 and removed on exit.
GPG_EXTRA_ARGS=()
if [[ -n "${AUDITBOT_BACKUP_PASSPHRASE_FILE:-}" && -f "${AUDITBOT_BACKUP_PASSPHRASE_FILE}" ]]; then
    GPG_EXTRA_ARGS=(--batch --yes --pinentry-mode loopback --passphrase-file "$AUDITBOT_BACKUP_PASSPHRASE_FILE")
fi
if ! tar -czf - -C "$TMP" runtime | gpg "${GPG_EXTRA_ARGS[@]}" --symmetric --cipher-algo AES256 -o "$OUT"; then
    rm -f -- "$OUT"
    echo 'Backup failed' >&2
    exit 1
fi
chmod 0600 "$OUT"
echo "Encrypted backup created: $OUT"
echo 'CRITICAL: Copy the encrypted file OFF this VM and securely store its passphrase.'
echo 'This is not a synchronized snapshot of non-SQLite runtime state during active trading.'
