#!/usr/bin/env bash
# UDITBOT release PR92+PR93: guarded one-session VM backup and V2 activation.
# Exact GitHub main commit pinned; never stop/restart trading_bot.service.
# Keeps LIVE broker safety, owner intent, Dhan governor and protection logs intact.
set -Eeuo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1

BASE_COMMIT='11d2c4c3c48264a03e498761b1e5158c8eb6706a'
INTERMEDIATE_COMMIT='8b18b0fec10e74ad065ef4d5a656c965af23a228'
RELEASE_COMMIT='5ab19ef81873561fb49792b28b7e318fa74ae822'
ACK='I_HAVE_OFF_VM_BACKUP_AND_AUTHORIZE_STOPPED_V2'

abort() { printf '\nBLOCKED: %s\n' "$*" >&2; exit 2; }
[[ $(hostname -s) == uditbot ]] || abort 'Wrong machine: expected uditbot.'
[[ $(id -u) != 0 ]] || abort 'Run as the bot service owner, not root.'
cd "$HOME/uditbot" || abort 'No ~/uditbot checkout.'
[[ -x ./venv/bin/python3 && -f .env ]] || abort 'Missing configured UditBot runtime.'
[[ $(git branch --show-current) == main ]] || abort 'Not on main.'
CURRENT_HEAD=$(git rev-parse HEAD)
[[ "$CURRENT_HEAD" == "$BASE_COMMIT" || "$CURRENT_HEAD" == "$INTERMEDIATE_COMMIT" ]] || abort "Unexpected installed HEAD ($CURRENT_HEAD); no changes made."
[[ -z $(git status --porcelain) ]] || abort 'Checkout has uncommitted/untracked files; no changes made.'
[[ $(systemctl is-active auditbot-web.service) == active ]] || abort 'Control Center must be active.'

printf '\n===== PIN REMOTE SOURCE — NO CHECKOUT MODIFICATIONS =====\n'
git fetch --no-tags origin main
[[ $(git rev-parse origin/main) == "$RELEASE_COMMIT" ]] || abort 'Remote main changed; no deployment performed.'
git merge-base --is-ancestor "$CURRENT_HEAD" origin/main || abort 'Not a fast-forward release.'

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="$HOME/uditbot-backups/pr93-$STAMP-$$"
mkdir -m 0700 -p "$BACKUP_DIR"
chmod 0700 "$BACKUP_DIR"
export BACKUP_DIR

backup_runtime() {
  local name="$1"
  export SNAPSHOT_NAME="$name"
  printf '\n===== CREATE PRIVATE %s BACKUP =====\n' "$name"
./venv/bin/python3 - <<'PY'
import hashlib, os, shutil, sqlite3, subprocess, tarfile, tempfile
from pathlib import Path
from urllib.parse import quote
from src.config import AppConfig

base = Path(os.environ["BACKUP_DIR"]).resolve()
root = Path.cwd().resolve()
cfg = AppConfig.from_env()
data_raw = Path(cfg.data_dir).expanduser()
if data_raw.is_symlink(): raise SystemExit("BLOCKED: symlinked DATA_DIR")
data = data_raw.resolve()
source_journal_raw = Path(cfg.journal_db_path).expanduser()
if source_journal_raw.is_symlink(): raise SystemExit("BLOCKED: symlinked journal")
source_journal = source_journal_raw.resolve()
source_token_raw = Path(cfg.dhan_token_file).expanduser()
if source_token_raw.is_symlink(): raise SystemExit("BLOCKED: symlinked token")
source_token = source_token_raw.resolve()
env = root / ".env"
if not data.is_dir() or not source_journal.is_file() or not env.is_file():
    raise SystemExit("BLOCKED: Required runtime path missing")
if not source_token.is_file():
    raise SystemExit("BLOCKED: Dhan token file missing, cannot make complete runtime backup")
if data.is_symlink() or source_journal.is_symlink() or env.is_symlink() or source_token.is_symlink():
    raise SystemExit("BLOCKED: Symlink in required runtime backup source")
for path in (base, data):
    if path.stat().st_mode & 0o077:
        raise SystemExit("BLOCKED: Unsafe data/backup directory permissions")

# SQLite online-backup API is used even while stopped, so committed WAL pages
# are included and no raw SQLite/WAL pair is ever mistaken for a snapshot.
def snapshot_sqlite(src: Path, dest: Path, journal=False):
    uri = 'file:' + quote(str(src), safe='/') + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=10.0) as db:
        if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
            raise ValueError('SQLite integrity failed')
        if journal:
            names = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'orders', 'order_intents'} <= names:
                raise ValueError('Required journal tables missing')
        with sqlite3.connect(dest) as target:
            db.backup(target)
    dest.chmod(0o600)

archive = base / os.environ['SNAPSHOT_NAME']
if archive.exists():
    raise SystemExit('BLOCKED: Backup already exists')
with tempfile.TemporaryDirectory(prefix='.stage-', dir=str(base)) as tmp:
    stage = Path(tmp) / 'runtime'
    (stage / 'data').mkdir(mode=0o700, parents=True)
    (stage / 'home').mkdir(mode=0o700)
    (stage / 'systemd').mkdir(mode=0o700)
    shutil.copy2(env, stage / '.env')
    shutil.copy2(source_token, stage / 'home' / '.dhan_token')
    # Preserve portable userbot sessions stored outside DATA_DIR, if any.
    for session in sorted(Path.home().glob('*.session')):
        if session.is_symlink() or not session.is_file():
            raise SystemExit('BLOCKED: Unexpected home Telegram session file type')
        shutil.copy2(session, stage / 'home' / session.name)
    for session in sorted(Path.home().glob('*.session-journal')):
        if session.is_symlink() or not session.is_file():
            raise SystemExit('BLOCKED: Unexpected home Telegram session journal file type')
        shutil.copy2(session, stage / 'home' / session.name)

    for item in data.rglob('*'):
        if item.is_symlink():
            raise SystemExit('BLOCKED: Symlinked runtime data item detected')
        relative = item.relative_to(data)
        dest = stage / 'data' / relative
        if item.is_dir():
            dest.mkdir(mode=0o700, parents=True, exist_ok=True)
            continue
        if not item.is_file():
            raise SystemExit('BLOCKED: Unexpected special runtime file')
        if item.name.endswith(('-wal', '-shm', '-journal')):
            continue
        dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if item.resolve() == source_journal:
            continue
        with item.open('rb') as item_fd:
            is_sqlite = item_fd.read(16) == b'SQLite format 3\x00'
        if is_sqlite:
            snapshot_sqlite(item, dest)
        else:
            shutil.copy2(item, dest)
    # The production preflight requires this EXACT archive member path.
    snapshot_sqlite(source_journal, stage / 'data' / 'trade_journal.db', journal=True)
    if os.environ['SNAPSHOT_NAME'] == 'runtime-stopped.tar.gz':
        # Recheck Owner STOP directly before the final authoritative snapshot.
        active = subprocess.run(['systemctl','show','trading_bot.service','-p','ActiveState','--value'],capture_output=True,text=True,check=True,timeout=10).stdout.strip()
        if active != 'inactive':
            raise SystemExit('BLOCKED: Trading service reactivated during stopped-state backup')
    for unit in ('trading_bot.service', 'auditbot-web.service'):
        output = subprocess.run(['systemctl', 'cat', unit], text=True,
                                capture_output=True, check=True, timeout=10)
        (stage / 'systemd' / (unit + '.txt')).write_text(output.stdout)
    (stage / 'source-commit.txt').write_text(subprocess.run(
        ['git', 'rev-parse', 'HEAD'], capture_output=True,
        text=True, check=True, timeout=10).stdout)
    temp_archive = base / '.runtime-building.tar.gz'
    try:
        with tarfile.open(temp_archive, 'w:gz') as tf:
            tf.add(stage, arcname='runtime', recursive=True)
        temp_archive.chmod(0o600)
        os.replace(temp_archive, archive)
    finally:
        temp_archive.unlink(missing_ok=True)
print('BACKUP VERIFIED WRITTEN:', archive)
digest = hashlib.sha256()
with archive.open('rb') as stream:
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
print('SHA256:', digest.hexdigest())
with tarfile.open(archive, 'r:gz') as verify:
    if not {'runtime/.env', 'runtime/data/trade_journal.db', 'runtime/source-commit.txt'} <= set(verify.getnames()):
        raise SystemExit('BLOCKED: Backup archive manifest missing required members')
    if verify.getmembers() == []:
        raise SystemExit('BLOCKED: Empty backup archive')
    print('ARCHIVE MANIFEST VERIFIED:', len(verify.getmembers()), 'members')
print('WARNING: backup includes tokens and credentials; do not share it in chat or GitHub.')
PY

}

# First protection: create an ONLINE emergency snapshot while the old LIVE
# process and reconciliation can continue uninterrupted. SQLite files use
# consistent online backups; mutable JSONs are diagnostic until stopped.
backup_runtime 'runtime-online-snapshot.tar.gz'
CODE_ARCHIVE="$BACKUP_DIR/source-before-pr93.tar.gz"
git archive --format=tar HEAD | gzip -9 > "$CODE_ARCHIVE"
chmod 0600 "$CODE_ARCHIVE"
sha256sum "$CODE_ARCHIVE" > "$BACKUP_DIR/source-before-pr93.sha256"
chmod 0600 "$BACKUP_DIR/source-before-pr93.sha256"

printf '\n===== OWNER-AUTHORIZED SAFE STOP CHECKPOINT =====\n'
printf '%s\n' \
  'The ONLINE backup is already saved. The LIVE service has not been changed.' \
  'Click STOP BOT once in the authenticated Control Center.' \
  'It must confirm a verified broker-safe STOP, NOT DEFERRED STOP.' \
  'Never use systemctl stop/kill, delete gate files, or reset the governor to proceed.'
read -r -p 'After the WebUI confirms STOPPED BY OWNER, press Enter (Ctrl+C aborts, backups remain): ' _
[[ $(systemctl show trading_bot.service -p ActiveState --value) == inactive ]] || abort "Trading service is not safely stopped; online backup saved at $BACKUP_DIR. No code changes."
[[ $(systemctl show trading_bot.service -p MainPID --value) == 0 ]] || abort 'Trading service PID remains nonzero; no code changes.'
./venv/bin/python3 - <<'PY'
from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker
cfg = AppConfig.from_env()
t = ConfigChangeTracker(cfg.data_dir)
s = t.control_service(action='status', env_path='.env')
c = t.get_service_control_record()
g = t.inspect_transition_gate_status(read_only=True)
if (s.get('service_state') != 'STOPPED_BY_OWNER'
        or c.get('desired_service_state') != 'STOPPED'
        or c.get('observed_state') != 'STOPPED_BY_OWNER'
        or c.get('last_action') not in ('OWNER_STOP','WEBUI_STOP','CLI_STOP')
        or c.get('deferred_stop_pending') is not False
        or g.get('active') is not False
        or g.get('status') != 'VALID_INACTIVE'):
    print('Owner control:', {k:c.get(k) for k in ('desired_service_state','last_action','observed_state','deferred_stop_pending')})
    print('Transition gate:', {k:g.get(k) for k in ('status','active')})
    raise SystemExit('BLOCKED: Broker-safe Owner STOP not positively verified; no update.')
print('VERIFIED: Broker-safe Owner STOP, inactive transition gate.')
PY

# Authoritative backup after service termination. This is the file accepted
# by V2 activation preflight, NOT the earlier online diagnostic snapshot.
backup_runtime 'runtime-stopped.tar.gz'
BACKUP_ARCHIVE="$BACKUP_DIR/runtime-stopped.tar.gz"
sha256sum "$BACKUP_ARCHIVE" "$CODE_ARCHIVE" > "$BACKUP_DIR/SHA256SUMS.txt"
chmod 0600 "$BACKUP_DIR/SHA256SUMS.txt"
(cd "$BACKUP_DIR" && sha256sum -c SHA256SUMS.txt)
printf '\n===== BACKUP COPIES REQUIRED BEFORE MERGE =====\n'
printf 'BACKUP FOLDER: %s\n' "$BACKUP_DIR"
printf '%s\n' \
  'Copy runtime-stopped.tar.gz and source-before-pr93.tar.gz off the VM to private storage.' \
  'Their SHA256 values are in SHA256SUMS.txt.' \
  'These archives contain UNENCRYPTED ACCESS TOKENS AND SESSIONS: never post publicly or in chat.'
read -r -p "Only after verifying off-VM copies, type $ACK : " confirmation
[[ "$confirmation" == "$ACK" ]] || abort 'Off-VM backup confirmation missing. No source update.'

printf '\n===== FINAL STOP AND VERSION RECHECK =====\n'
[[ $(systemctl show trading_bot.service -p ActiveState --value) == inactive ]] || abort 'Trading service reactivated. No update.'
[[ $(systemctl show trading_bot.service -p MainPID --value) == 0 ]] || abort 'Trading PID reappeared.'
[[ $(git rev-parse HEAD) == "$CURRENT_HEAD" ]] || abort 'Local checkout moved while backing up.'
[[ $(git rev-parse origin/main) == "$RELEASE_COMMIT" ]] || abort 'Remote update ref changed.'
[[ -z $(git status --porcelain) ]] || abort 'Working tree changed. No update.'

printf '\n===== FAST-FORWARD FROM PR90 TO MERGED PR92+PR93 =====\n'
git merge --ff-only origin/main
[[ $(git rev-parse HEAD) == "$RELEASE_COMMIT" ]] || abort 'Unexpected installed revision.'

printf '\n===== ISOLATED OFFLINE GOVERNOR + V2 REGRESSION TESTS =====\n'
AUDITBOT_TEST_ISOLATION=1 ./venv/bin/python3 -m unittest -q \
    tests.test_dhan_category_governor \
    tests.test_dhan_category_transport \
    tests.test_tracker_v2_activation_preflight \
    tests.test_tracker_v2_activation_script \
    tests.test_tracker_v2_dual_service \
    tests.test_tracker_v2_health \
    tests.test_tracker_v2_source_replacements \
    tests.test_tracker_v2_review \
    tests.test_tracker_v2_stress_matrix \
    tests.test_tracker_v2_authenticated_acceptance
[[ -z $(git status --porcelain) ]] || abort 'Unexpected modified source after tests.'
[[ $(systemctl show trading_bot.service -p ActiveState --value) == inactive ]] || abort 'Trading service reactivated during tests.'

printf '\n===== STAGE V2 IN WEBUI ONLY (NO LIVE TRADING RESTART) =====\n'
bash deploy/tracker_v2_activation.sh --preflight "$RELEASE_COMMIT" "$BACKUP_ARCHIVE"
bash deploy/tracker_v2_activation.sh --apply-stopped "$RELEASE_COMMIT" "$BACKUP_ARCHIVE" "$ACK"

printf '\n===== FINAL DEPLOYMENT AUDIT =====\n'
[[ $(git rev-parse HEAD) == "$RELEASE_COMMIT" ]] || abort 'Release hash mismatch.'
bash deploy/tracker_v2_activation.sh --inspect
systemctl show trading_bot.service -p ActiveState -p MainPID --no-pager
systemctl show auditbot-web.service -p ActiveState -p MainPID --no-pager
printf '\nRELEASE STAGED: %s\n' "$RELEASE_COMMIT"
printf 'BACKUPS: %s\n' "$BACKUP_DIR"
printf '%s\n' 'TRADING BOT WAS NOT AUTOMATICALLY RESTARTED.' \
  'Before any LIVE Start: verify Dhan credentials, broker order book, holdings, positions, OCO protections, account and reconciliation.' \
  'After owner-approved LIVE Start, verify via:' \
  "  cd ~/uditbot && bash deploy/tracker_v2_activation.sh --verify-running $RELEASE_COMMIT"
