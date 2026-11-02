#!/usr/bin/env bash
# Restore an encrypted runtime bundle after provisioning a fresh VM.
# REFUSES to run while the trading service or WebUI is active. Never starts either.
# CR-01 & H-03 Hardened: Validates full archive schema, runtime/data directory, and SQLite
# PRAGMA quick_check in staging BEFORE touching any live file, normalizes all custom paths
# coherently to the actual restore destinations, and rolls back any partial writes on failure.
set -euo pipefail
umask 077
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
[[ "$(id -u)" != 0 ]] || { echo 'Run as bot owner, not root' >&2; exit 2; }
ALLOW_MISSING_JOURNAL="${AUDITBOT_ALLOW_MISSING_JOURNAL:-0}"
if [[ "${1:-}" == "--allow-missing-journal" ]]; then
    ALLOW_MISSING_JOURNAL=1
    shift
fi
[[ $# == 1 && -f "$1" ]] || { echo 'Usage: bash deploy/restore_runtime.sh [--allow-missing-journal] /path/backup.tar.gz.gpg' >&2; exit 2; }
if [[ "${AUDITBOT_TEST_ISOLATION:-0}" != "1" || "${AUDITBOT_SKIP_SYSTEMCTL_CHECK:-0}" != "1" ]]; then
    if command -v systemctl >/dev/null 2>&1; then
        for svc in trading_bot.service auditbot-web.service; do
            if systemctl is-active --quiet "$svc"; then
                echo "REFUSED: $svc is running. Stop deliberately before restoring." >&2
                exit 2
            fi
        done
    fi
fi
PYTHON_BIN="$REPO_DIR/venv/bin/python3"
[[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="python3"
TARGET_HOME="${AUDITBOT_RESTORE_HOME:-$HOME}"
[[ -d "$REPO_DIR/data" ]] || { echo 'Missing data directory' >&2; exit 2; }
[[ ! -e "$REPO_DIR/data/trade_journal.db" ]] || { echo 'Existing journal detected: refuse overwrite' >&2; exit 2; }
[[ ! -e "$REPO_DIR/.env" ]] || { echo 'Existing .env detected: refuse overwrite' >&2; exit 2; }
[[ ! -e "$TARGET_HOME/.dhan_token" ]] || { echo 'Existing token detected: refuse overwrite' >&2; exit 2; }
if find "$REPO_DIR/data" -mindepth 1 -maxdepth 1 ! -name 'holidays.json' -print -quit | grep -q .; then
    echo 'Existing runtime data detected: refuse overwrite' >&2
    exit 2
fi

TMP="$(mktemp -d)"
COMMITTED=0
CREATED_LIVE_FILES=()
cleanup_and_rollback() {
    local rc=$?
    if [[ "$COMMITTED" != "1" && "${#CREATED_LIVE_FILES[@]}" -gt 0 ]]; then
        for f in "${CREATED_LIVE_FILES[@]}"; do
            rm -f -- "$f" 2>/dev/null || true
        done
        echo "RESTORE ROLLED BACK: cleaned up partial files so restore can be safely retried." >&2
    fi
    rm -rf -- "$TMP" 2>/dev/null || true
    exit "$rc"
}
trap cleanup_and_rollback EXIT

# 1. Decrypt and validate archive structure & SQLite integrity ENTIRELY inside $TMP before touching any live path (CR-01)
GPG_EXTRA_ARGS=()
if [[ -n "${AUDITBOT_BACKUP_PASSPHRASE_FILE:-}" && -f "${AUDITBOT_BACKUP_PASSPHRASE_FILE}" ]]; then
    GPG_EXTRA_ARGS=(--batch --yes --pinentry-mode loopback --passphrase-file "$AUDITBOT_BACKUP_PASSPHRASE_FILE")
fi
gpg "${GPG_EXTRA_ARGS[@]}" --decrypt --output "$TMP/bundle.tar.gz" "$1"
ALLOW_MISSING_JOURNAL="$ALLOW_MISSING_JOURNAL" "$PYTHON_BIN" - "$TMP/bundle.tar.gz" "$TMP" "$REPO_DIR" "$TARGET_HOME" <<'PY'
import json, os, pathlib, re, shutil, sqlite3, sys, tarfile

archive, dst, repo_dir_str, target_home_str = sys.argv[1:]
repo_dir = pathlib.Path(repo_dir_str).resolve()
target_home = pathlib.Path(target_home_str).resolve()
allow_missing_journal = (os.environ.get('ALLOW_MISSING_JOURNAL', '0') == '1')
FORBIDDEN_NAMES = {'holidays.json', 'scrip_master.csv', 'listener.lock', 'emergency_stop.json.lock'}
FORBIDDEN_SUFFIXES = ('.lock', '.tmp', '.swp', '-wal', '-shm', '-journal')

with tarfile.open(archive, 'r:gz') as tf:
    members = tf.getmembers()
    if not members:
        raise SystemExit('Empty backup archive')
    for m in members:
        path = pathlib.PurePosixPath(m.name)
        if (
            m.issym()
            or m.islnk()
            or path.is_absolute()
            or '..' in path.parts
            or not (path.parts and path.parts[0] == 'runtime')
            or path.name in FORBIDDEN_NAMES
            or path.name.endswith(FORBIDDEN_SUFFIXES)
        ):
            raise SystemExit(f'Unsafe backup entry: {m.name}')
        if not (m.isfile() or m.isdir()):
            raise SystemExit('Unexpected archive entry type')
        if len(path.parts) > 3:
            raise SystemExit(f'Unsupported nested archive path: {m.name}')
        if len(path.parts) >= 2 and path.parts[1] not in ('.env', 'data', 'home'):
            raise SystemExit(f'Unexpected top-level runtime member: {m.name}')
    base = pathlib.Path(dst).resolve()
    for m in members:
        target = base.joinpath(*pathlib.PurePosixPath(m.name).parts)
        if m.isdir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            with tf.extractfile(m) as reader, target.open('xb') as writer:
                shutil.copyfileobj(reader, writer)

runtime_root = pathlib.Path(dst).resolve() / 'runtime'
env_path = runtime_root / '.env'
data_dir = runtime_root / 'data'
if not env_path.is_file():
    raise SystemExit('Backup missing required runtime/.env')
if not data_dir.is_dir():
    raise SystemExit('Backup missing required runtime/data directory (CR-01 validation failed)')

# R-01: Require runtime/data/trade_journal.db with valid SQLite header, integrity_check, and user schema
# unless --allow-missing-journal (AUDITBOT_ALLOW_MISSING_JOURNAL=1) is explicitly passed.
journal_candidate = data_dir / 'trade_journal.db'
if not allow_missing_journal:
    if not journal_candidate.is_file():
        raise SystemExit(
            'R-01: Backup missing required runtime/data/trade_journal.db (pass --allow-missing-journal only for uninitialized setups)'
        )
    j_head = journal_candidate.read_bytes()[:16]
    if not j_head.startswith(b'SQLite format 3'):
        raise SystemExit('R-01: runtime/data/trade_journal.db is not a valid SQLite database file')

# Validate SQLite integrity_check/quick_check on any SQLite files (trade_journal.db, tg_userbot.session) in staging before live install
for db_candidate in data_dir.iterdir():
    if not db_candidate.is_file():
        raise SystemExit(f'Non-regular file inside runtime/data: {db_candidate.name}')
    raw_head = db_candidate.read_bytes()[:16]
    if db_candidate.name.endswith('.db') or raw_head.startswith(b'SQLite format 3'):
        try:
            con = sqlite3.connect(f'file:{db_candidate}?mode=ro', uri=True, timeout=5)
            ic = con.execute('PRAGMA integrity_check;').fetchone()
            qc = con.execute('PRAGMA quick_check;').fetchone()
            tbl_cnt = con.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            ).fetchone()[0]
            con.close()
            if not ic or ic[0] != 'ok' or not qc or qc[0] != 'ok':
                raise SystemExit(f'SQLite integrity/quick_check failed for {db_candidate.name}: {ic} / {qc}')
            if db_candidate.name == 'trade_journal.db' and not allow_missing_journal and int(tbl_cnt or 0) < 1:
                raise SystemExit('R-01: runtime/data/trade_journal.db contains no application tables')
        except SystemExit:
            raise
        except Exception as exc:
            raise SystemExit(f'Corrupt SQLite file in backup ({db_candidate.name}): {exc}')
    elif db_candidate.name.endswith('.json'):
        try:
            json.loads(db_candidate.read_text(encoding='utf-8'))
        except Exception as exc:
            raise SystemExit(f'Corrupt JSON file in backup ({db_candidate.name}): {exc}')

# H-03 & TEST-mode enforcement: Force TRADING_MODE=TEST and coherently normalize
# DATA_DIR, JOURNAL_DB_PATH, and DHAN_TOKEN_FILE to the actual target restore locations.
text = env_path.read_text(encoding='utf-8')
text, n = re.subn(r'(?m)^\s*TRADING_MODE\s*=.*$', 'TRADING_MODE=TEST', text)
if n == 0:
    text += '\nTRADING_MODE=TEST\n'

def _normalize_target_path(env_text: str, key: str, actual_dest: str) -> str:
    pat = re.compile(rf'(?m)^(\s*{key}\s*=\s*)(.+?)\s*$')
    return pat.sub(lambda m: f"{m.group(1)}{actual_dest}", env_text)

text = _normalize_target_path(text, 'DATA_DIR', str(repo_dir / 'data'))
text = _normalize_target_path(text, 'JOURNAL_DB_PATH', str(repo_dir / 'data' / 'trade_journal.db'))
text = _normalize_target_path(text, 'DHAN_TOKEN_FILE', str(target_home / '.dhan_token'))
env_path.write_text(text, encoding='utf-8')
PY

# 2. All staging validations passed. Install files with rollback tracking (CR-01)
if [[ "${AUDITBOT_SIMULATE_MID_RESTORE_FAILURE:-0}" == "1" ]]; then
    cp -- "$TMP/runtime/.env" "$REPO_DIR/.env"
    CREATED_LIVE_FILES+=("$REPO_DIR/.env")
    echo "Simulated mid-restore copy failure after writing .env" >&2
    exit 1
fi

# R-02: Use null-delimited find loop so hidden runtime dotfiles (e.g. .hidden_recovery_state) are preserved
while IFS= read -r -d '' item; do
    base_name="$(basename "$item")"
    CREATED_LIVE_FILES+=("$REPO_DIR/data/$base_name")
    cp -p -- "$item" "$REPO_DIR/data/$base_name"
    chmod 0600 "$REPO_DIR/data/$base_name"
done < <(find "$TMP/runtime/data" -mindepth 1 -maxdepth 1 -type f -print0)
chmod 0700 "$REPO_DIR/data"

if [[ -f "$TMP/runtime/home/.dhan_token" ]]; then
    CREATED_LIVE_FILES+=("$TARGET_HOME/.dhan_token")
    cp -- "$TMP/runtime/home/.dhan_token" "$TARGET_HOME/.dhan_token"
    chmod 0600 "$TARGET_HOME/.dhan_token"
fi

CREATED_LIVE_FILES+=("$REPO_DIR/.env")
cp -- "$TMP/runtime/.env" "$REPO_DIR/.env"
chmod 0600 "$REPO_DIR/.env"

COMMITTED=1
printf 'Restore complete; TRADING_MODE forced to TEST; no service started.\n'
printf 'Check custom DATA_DIR/JOURNAL_DB_PATH/DHAN_TOKEN_FILE absolute paths before any startup.\n'
printf 'Verify STOP state, claims, broker positions and protection before considering LIVE.\n'
