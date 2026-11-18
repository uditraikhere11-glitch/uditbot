(
set -Eeuo pipefail
umask 077
cd "$HOME/uditbot"
SHA=5ab19ef81873561fb49792b28b7e318fa74ae822
[[ "$(hostname -s)" == uditbot ]] || { echo 'ABORT: wrong VM'; exit 1; }
[[ "$(git rev-parse HEAD)" == "$SHA" ]] || { echo 'ABORT: unexpected Git commit'; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo 'ABORT: Git checkout has local changes'; exit 1; }
[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == inactive && \
   "$(systemctl show trading_bot.service -p MainPID --value)" == 0 ]] || {
  echo 'ABORT: trading bot is not confirmed stopped'; exit 1;
}

OUT="$HOME/uditbot-backups/stopped-pr93-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -m 700 -p "$OUT"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
mkdir -p "$TMP/runtime/data" "$TMP/runtime/secrets"

PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - "$TMP/runtime" <<'PY'
import os, shutil, sqlite3, sys
from pathlib import Path
from src.config import AppConfig

root = Path.cwd().resolve()
stage = Path(sys.argv[1]).resolve()
cfg = AppConfig.from_env()
data = Path(cfg.data_dir).expanduser().resolve()
journal = Path(cfg.journal_db_path).expanduser().resolve()
assert data.is_dir() and data != Path('/') and not data.is_symlink(), 'Unsafe data folder'
assert journal.is_file(), 'Missing trade journal'
assert (root / '.env').is_file(), 'Missing configuration'
shutil.copy2(root / '.env', stage / '.env')
(stage / '.env').chmod(0o600)

def capture(src, dst):
    if src.is_symlink():
        raise RuntimeError('Symlink in runtime; aborting: ' + str(src))
    dst.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with src.open('rb') as fh:
        is_db = fh.read(16) == b'SQLite format 3\0'
    if is_db:
        with sqlite3.connect(src.resolve().as_uri() + '?mode=ro', uri=True, timeout=10) as db:
            with sqlite3.connect(dst, timeout=10) as saved:
                db.backup(saved)
                assert saved.execute('PRAGMA quick_check').fetchone()[0] == 'ok', str(src)
    else:
        if src.name.endswith(('.db', '.sqlite', '.sqlite3', '.session')):
            raise RuntimeError('Invalid SQLite header: ' + str(src))
        shutil.copy2(src, dst)
    dst.chmod(0o600)

count = 0
for src in sorted(data.rglob('*')):
    if src.is_symlink():
        raise RuntimeError('Symlink in data directory: ' + str(src))
    dst = stage / 'data' / src.relative_to(data)
    if src.is_dir():
        dst.mkdir(parents=True, exist_ok=True, mode=0o700)
    elif src.is_file():
        if src.name.endswith(('-wal', '-shm', '-journal', '.lock', '.tmp', '.swp')):
            continue
        capture(src, dst)
        count += 1
    else:
        raise RuntimeError('Unsupported runtime entry: ' + str(src))

# Preflight expects this canonical archived journal path.
capture(journal, stage / 'data' / 'trade_journal.db')
token = Path(cfg.dhan_token_file).expanduser().resolve()
assert token.is_file(), 'Missing configured Dhan token'
capture(token, stage / 'secrets' / 'dhan_token')
for src in root.glob('*.session'):
    if src.is_file():
        capture(src, stage / 'secrets' / src.name)
print('Stopped-state runtime files captured:', count)
print('Nested governor and V2 records included; SQLite snapshots validated')
PY

[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == inactive && \
   "$(systemctl show trading_bot.service -p MainPID --value)" == 0 ]] || {
  echo 'ABORT: trading service state changed during backup'; exit 1;
}
ARCHIVE="$OUT/runtime-stopped.tar.gz"
tar -C "$TMP" -czf "$ARCHIVE.partial" runtime
mv -- "$ARCHIVE.partial" "$ARCHIVE"
chmod 600 "$ARCHIVE"
tar -tzf "$ARCHIVE" >/dev/null
(cd "$OUT" && sha256sum runtime-stopped.tar.gz > SHA256SUMS && sha256sum -c SHA256SUMS)
echo "Stopped backup: $ARCHIVE"
echo '===== V2 READ-ONLY PREFLIGHT ====='
bash deploy/tracker_v2_activation.sh --preflight "$SHA" "$ARCHIVE"
echo 'PREFLIGHT PASSED. Bot remains stopped; no activation or broker request performed.'
)
