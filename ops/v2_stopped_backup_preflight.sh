(
set -Eeuo pipefail
umask 077
cd "$HOME/uditbot"
SHA="736d34e3346ff4104c8045834f3637219c6639b8"
[[ "$(hostname -s)" == uditbot && "$(git rev-parse HEAD)" == "$SHA" && -z "$(git status --porcelain)" ]] || { echo 'BLOCKED: VM/source changed'; exit 2; }
[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == inactive && "$(systemctl show trading_bot.service -p MainPID --value)" == 0 ]] || { echo 'BLOCKED: Trading is not safely stopped'; exit 2; }
[[ "$(systemctl show auditbot-web.service -p ActiveState --value)" == active ]] || { echo 'BLOCKED: WebUI not active'; exit 2; }
OUT="$HOME/uditbot-backups/v2-stopped-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -m 0700 -- "$OUT"
STAGE="$(mktemp -d -p "$OUT" .stage.XXXXXXXX)"
trap 'rc=$?; rm -rf -- "$STAGE"; if (( rc != 0 )); then rm -f -- "$OUT/runtime-stopped.tar.gz.part"; fi' EXIT

PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - "$STAGE" "$PWD" <<'PY'
import shutil, sqlite3, sys
from contextlib import closing
from pathlib import Path
from src.config import AppConfig
stage = Path(sys.argv[1]) / 'runtime'
root = Path(sys.argv[2]).resolve()
cfg = AppConfig.from_env()
data = Path(cfg.data_dir).expanduser().resolve()
journal = Path(cfg.journal_db_path).expanduser().resolve()
if data != root / 'data' or journal != data / 'trade_journal.db' or not journal.is_file():
    raise SystemExit('BLOCKED: unexpected journal/data paths')
if not (root / '.env').is_file() or (root / '.env').is_symlink():
    raise SystemExit('BLOCKED: invalid .env')
stage.mkdir(parents=True)
shutil.copy2(root / '.env', stage / '.env')
count = databases = 0
def copy(source, dest):
    global count, databases
    if source.is_symlink() or not source.is_file():
        raise RuntimeError('Unsafe runtime item')
    dest.parent.mkdir(parents=True, exist_ok=True)
    with source.open('rb') as fd:
        header = fd.read(16)
    if header == b'SQLite format 3\x00':
        with closing(sqlite3.connect(source.as_uri()+'?mode=ro', uri=True, timeout=10)) as read:
            with closing(sqlite3.connect(dest, timeout=10)) as write:
                read.backup(write)
                if write.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise RuntimeError('SQLite backup integrity check failed')
        databases += 1
    elif source.suffix in ('.db', '.sqlite', '.sqlite3', '.session'):
        raise RuntimeError('Invalid SQLite runtime item')
    else:
        shutil.copy2(source, dest)
    dest.chmod(0o600)
    count += 1
for src in sorted(data.rglob('*')):
    if src.is_symlink():
        raise RuntimeError('Symlink in runtime data directory')
    if src.is_dir():
        continue
    if src.name.endswith(('-wal', '-shm', '-journal', '.lock', '.tmp', '.swp')):
        continue
    copy(src, stage / 'data' / src.relative_to(data))
if not (stage / 'data' / 'trade_journal.db').is_file():
    raise RuntimeError('Journal backup is missing')
token = Path(cfg.dhan_token_file).expanduser().resolve()
if not token.is_file():
    raise RuntimeError('Configured token file missing; cannot make complete backup')
copy(token, stage / 'secrets' / 'dhan_token')
for src in root.glob('*.session'):
    copy(src, stage / 'secrets' / src.name)
print(f'Consistent stopped-state backup: {count} files, {databases} SQLite snapshots')
PY

ARCHIVE="$OUT/runtime-stopped.tar.gz"
tar -czf "$ARCHIVE.part" -C "$STAGE" runtime
tar -tzf "$ARCHIVE.part" >/dev/null
mv -- "$ARCHIVE.part" "$ARCHIVE"
chmod 0600 "$ARCHIVE"
(cd "$OUT" && sha256sum runtime-stopped.tar.gz > SHA256SUMS && sha256sum -c SHA256SUMS)
[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == inactive && "$(systemctl show trading_bot.service -p MainPID --value)" == 0 ]] || { echo 'BLOCKED: trading service changed during backup'; exit 2; }

echo '===== V2 STOPPED PRE-FLIGHT ====='
bash deploy/tracker_v2_activation.sh --preflight "$SHA" "$ARCHIVE"
echo "BACKUP: $ARCHIVE"
echo 'Trading remained STOPPED. No LIVE or V2 activation performed.'
)
