(
set -Eeuo pipefail
umask 077

REPO="$HOME/uditbot"
TARGET="5ab19ef81873561fb49792b28b7e318fa74ae822"
[[ "$(hostname -s)" == "uditbot" ]] || { echo 'ABORT: wrong VM'; exit 1; }
[[ "$(id -u)" -ne 0 ]] || { echo 'ABORT: run as bot owner'; exit 1; }
cd "$REPO"
[[ "$(git branch --show-current)" == "main" ]] || { echo 'ABORT: not on main'; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo 'ABORT: working tree has local changes; no overwrite'; exit 1; }
[[ -x ./venv/bin/python3 && -r .env ]] || { echo 'ABORT: .env or venv missing'; exit 1; }
[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == "active" ]] || { echo 'ABORT: unexpected bot state; inspect before any action'; exit 1; }
command -v tar >/dev/null && command -v git >/dev/null || { echo 'ABORT: tar/git missing'; exit 1; }

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$HOME/uditbot-backups/pre-pr93-$STAMP"
[[ ! -e "$OUT" ]] || { echo 'ABORT: backup destination already exists'; exit 1; }
mkdir -m 0700 -p "$OUT"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
mkdir -p "$TMP/runtime/data" "$TMP/runtime/secrets" "$TMP/runtime/services"

printf '%s\n' '===== STEP 1: LIVE-SAFE ONLINE BACKUP (NO SERVICE CHANGES) ====='
PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - "$REPO" "$TMP/runtime" <<'PY'
from pathlib import Path
import os, shutil, sqlite3, sys
from src.config import AppConfig
root, stage = (Path(p).resolve() for p in sys.argv[1:])
os.chdir(root)
cfg = AppConfig.from_env()
data = Path(cfg.data_dir).expanduser().resolve()
assert data.is_dir() and data != Path('/') and not data.is_symlink(), 'Unsafe DATA_DIR'
(stage / 'data').mkdir(parents=True, exist_ok=True)
shutil.copy2(root / '.env', stage / '.env')

def snapshot(src, dst):
    if src.is_symlink():
        raise RuntimeError('Refusing symlink in protected runtime: ' + str(src))
    dst.parent.mkdir(parents=True, exist_ok=True)
    header = src.open('rb').read(16)
    if header == b'SQLite format 3\x00':
        with sqlite3.connect(src.resolve().as_uri() + '?mode=ro', uri=True, timeout=10) as read:
            with sqlite3.connect(dst, timeout=10) as write:
                read.backup(write)
                check = write.execute('PRAGMA quick_check').fetchone()
                if not check or check[0] != 'ok':
                    raise RuntimeError('SQLite backup check failed: ' + str(src))
    else:
        if src.suffix in ('.db', '.sqlite', '.sqlite3', '.session'):
            raise RuntimeError('Expected SQLite file has invalid header: ' + str(src))
        shutil.copy2(src, dst)
    os.chmod(dst, 0o600)

count = 0
for source in sorted(data.rglob('*')):
    if source.is_symlink():
        raise RuntimeError('Symlink inside runtime DATA_DIR: ' + str(source))
    relative = source.relative_to(data)
    if source.is_dir():
        (stage / 'data' / relative).mkdir(mode=0o700, parents=True, exist_ok=True)
        continue
    if not source.is_file():
        raise RuntimeError('Unsupported runtime item: ' + str(source))
    if source.name.endswith(('-wal', '-shm', '-journal', '.lock', '.tmp', '.swp')):
        continue
    snapshot(source, stage / 'data' / relative)
    count += 1

journal = Path(cfg.journal_db_path).expanduser().resolve()
if not journal.is_file():
    raise RuntimeError('Required trade journal missing')
if journal.is_relative_to(data):
    assert (stage / 'data' / journal.relative_to(data)).is_file(), 'Journal backup missing'
else:
    snapshot(journal, stage / 'secrets' / 'external_trade_journal.db')

token = Path(cfg.dhan_token_file).expanduser().resolve()
if token.is_file():
    snapshot(token, stage / 'secrets' / 'dhan_token')
else:
    raise RuntimeError('Configured Dhan token path missing; backup incomplete')

for session in root.glob('*.session'):
    if session.is_file():
        snapshot(session, stage / 'secrets' / session.name)
print('Runtime files backed up:', count)
print('Governor and V2 nested state:', 'included recursively if present')
print('SQLite files:', 'consistent copies validated individually')
PY
systemctl cat trading_bot.service > "$TMP/runtime/services/trading_bot.service.txt"
systemctl cat auditbot-web.service > "$TMP/runtime/services/auditbot-web.service.txt"
git rev-parse HEAD > "$TMP/runtime/source-head.txt"
git status --porcelain > "$TMP/runtime/source-status.txt"
git bundle create "$OUT/source-before-update.bundle" --all

# Password-free archive as explicitly requested. The content includes .env,
# account tokens and session files: keep the directory private (0700),
# archive owner-only (0600), and do not send it over chat or public links.
ARCHIVE_PART="$OUT/runtime-online.tar.gz.incomplete"
ARCHIVE="$OUT/runtime-online.tar.gz"
tar -czf "$ARCHIVE_PART" -C "$TMP" runtime
tar -tzf "$ARCHIVE_PART" >/dev/null
[[ -s "$ARCHIVE_PART" && -s "$OUT/source-before-update.bundle" ]] || { echo 'ABORT: backup incomplete'; exit 1; }
mv -- "$ARCHIVE_PART" "$ARCHIVE"
chmod 0600 "$ARCHIVE" "$OUT/source-before-update.bundle"
git bundle verify "$OUT/source-before-update.bundle" >/dev/null
sha256sum "$ARCHIVE" "$OUT/source-before-update.bundle" > "$OUT/SHA256SUMS"
chmod 0600 "$OUT/SHA256SUMS"
(cd "$OUT" && sha256sum -c SHA256SUMS)

printf '%s\n' '===== STEP 2: PINNED GITHUB RELEASE STAGING ====='
git fetch --no-tags origin main
[[ "$(git rev-parse refs/remotes/origin/main)" == "$TARGET" ]] || { echo 'ABORT: origin/main has a different commit; backup preserved'; exit 1; }
git merge-base --is-ancestor HEAD "$TARGET" || { echo 'ABORT: checkout cannot fast-forward; backup preserved'; exit 1; }
RELEASE="$HOME/uditbot-releases/pr93-$STAMP"
mkdir -p "$HOME/uditbot-releases"
[[ ! -e "$RELEASE" ]] || { echo 'ABORT: stage destination occupied'; exit 1; }
git worktree add --detach "$RELEASE" "$TARGET"
[[ "$(git -C "$RELEASE" rev-parse HEAD)" == "$TARGET" ]] || { echo 'ABORT: release verification failed'; exit 1; }

printf '%s\n' '===== RESULT ====='
printf 'Private backup (NO PASSWORD): %s\n' "$ARCHIVE"
printf 'Source bundle: %s\n' "$OUT/source-before-update.bundle"
printf 'Checksums: %s\n' "$OUT/SHA256SUMS"
printf 'New release staged: %s\n' "$RELEASE"
printf 'Release commit: %s\n' "$(git -C "$RELEASE" rev-parse --short HEAD)"
printf 'Active checkout unchanged: %s\n' "$(git -C "$REPO" rev-parse --short HEAD)"
printf 'Trading service: %s\n' "$(systemctl is-active trading_bot.service)"
printf 'WebUI service: %s\n' "$(systemctl is-active auditbot-web.service)"
echo 'IMPORTANT: Backup contains PLAIN-TEXT SECRETS. Copy to a private location OFF this VM, never to Telegram/GitHub/chat.'
echo 'This is a live, non-atomic snapshot. No services were stopped/restarted; V2 remains inactive.'
)
