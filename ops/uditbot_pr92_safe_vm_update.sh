#!/usr/bin/env bash
# UDITBOT PR92: one terminal session for broker-safe owner STOP -> private backup
# -> exact GitHub update -> offline tests -> stopped-only V2 WebUI staging.
# NEVER forces LIVE service stop/start. NEVER resets a Dhan GET governor.
set -Eeuo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1

BASE_COMMIT='11d2c4c3c48264a03e498761b1e5158c8eb6706a'
RELEASE_COMMIT='8b18b0fec10e74ad065ef4d5a656c965af23a228'
ACK='I_HAVE_OFF_VM_BACKUP_AND_AUTHORIZE_STOPPED_V2'

abort() { printf '\nBLOCKED: %s\n' "$*" >&2; exit 2; }
[[ $(hostname -s) == uditbot ]] || abort 'Wrong machine: expected uditbot.'
[[ $(id -u) != 0 ]] || abort 'Run as the bot owner, not root.'
cd "$HOME/uditbot" || abort 'No ~/uditbot checkout.'
[[ -x ./venv/bin/python3 && -f .env ]] || abort 'Missing configured UditBot runtime.'
[[ $(git branch --show-current) == main ]] || abort 'Not on main.'
[[ $(git rev-parse HEAD) == "$BASE_COMMIT" ]] || abort 'VM revision changed; review latest before running.'
[[ -z $(git status --porcelain) ]] || abort 'Checkout has uncommitted/untracked files.'
[[ $(systemctl is-active auditbot-web.service) == active ]] || abort 'Control Center not active.'
printf '\n===== FETCH AND PIN REVIEWED MERGE =====\n'
git fetch --no-tags origin main
[[ $(git rev-parse origin/main) == "$RELEASE_COMMIT" ]] || abort 'GitHub main no longer matches approved PR92 merge. No update performed.'

printf '\n===== OWNER-APPROVED BROKER-SAFE STOP =====\n'
printf '%s\n' 'In the Control Center, click STOP BOT.' \
  'It must confirm a broker-safe STOP, not a deferred STOP.' \
  'If the Dhan read budget is exhausted, STOP may be deferred; DO NOT force systemctl stop.'
read -r -p 'After confirming STOP in the WebUI, press Enter (or Ctrl+C to abort): ' _
[[ $(systemctl show trading_bot.service -p ActiveState --value) == inactive ]] || abort 'Trading service is NOT positively inactive; no upgrade.'
[[ $(systemctl show trading_bot.service -p MainPID --value) == 0 ]] || abort 'Trading service PID is not zero.'
./venv/bin/python3 - <<'PY'
from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker
cfg = AppConfig.from_env()
t = ConfigChangeTracker(cfg.data_dir)
s = t.control_service(action="status", env_path=".env")
c = t.get_service_control_record()
if (s.get("service_state") != "STOPPED_BY_OWNER"
        or c.get("desired_service_state") != "STOPPED"
        or c.get("observed_state") != "STOPPED_BY_OWNER"
        or c.get("last_action") not in ("OWNER_STOP", "WEBUI_STOP", "CLI_STOP")
        or c.get("deferred_stop_pending") is not False):
    raise SystemExit("BLOCKED: Owner STOP not positively verified. No upgrade.")
print("VERIFIED: Authorized broker-safe STOP and no deferred stop.")
PY

printf '\n===== FULL PRIVATE BACKUP, BEFORE GIT UPDATE =====\n'
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="$HOME/uditbot-backups/v2-pr92-$STAMP"
mkdir -m 0700 -p "$BACKUP_DIR"
chmod 0700 "$BACKUP_DIR"
export BACKUP_DIR
./venv/bin/python3 - <<'PY'
import hashlib, os, shutil, sqlite3, subprocess, tarfile, tempfile
from pathlib import Path
from urllib.parse import quote
from src.config import AppConfig

base = Path(os.environ["BACKUP_DIR"]).resolve()
root = Path.cwd().resolve()
cfg = AppConfig.from_env()
data = Path(cfg.data_dir).expanduser().resolve()
source_journal = Path(cfg.journal_db_path).expanduser().resolve()
source_token = Path(cfg.dhan_token_file).expanduser().resolve()
env = root / ".env"
if not data.is_dir() or not source_journal.is_file() or not env.is_file():
    raise SystemExit("BLOCKED: Required runtime path missing")
if not source_token.is_file():
    raise SystemExit("BLOCKED: Token file missing, cannot make complete runtime backup")
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

archive = base / 'runtime-stopped.tar.gz'
if archive.exists():
    raise SystemExit('BLOCKED: Backup already exists')
with tempfile.TemporaryDirectory(prefix='.stage-', dir=str(base)) as tmp:
    stage = Path(tmp) / 'runtime'
    (stage / 'data').mkdir(mode=0o700, parents=True)
    (stage / 'home').mkdir(mode=0o700)
    (stage / 'systemd').mkdir(mode=0o700)
    shutil.copy2(env, stage / '.env')
    shutil.copy2(source_token, stage / 'home' / '.dhan_token')

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
print('WARNING: backup includes tokens and credentials; do not share it in chat or GitHub.')
PY
CODE_ARCHIVE="$BACKUP_DIR/source-before-pr92.tar.gz"
git archive --format=tar HEAD | gzip -9 > "$CODE_ARCHIVE"
chmod 0600 "$CODE_ARCHIVE"
BACKUP_ARCHIVE="$BACKUP_DIR/runtime-stopped.tar.gz"
printf '\nBackups are PRIVATE on the VM:\n%s\n%s\n' "$BACKUP_ARCHIVE" "$CODE_ARCHIVE"
printf '%s\n' 'REQUIRED: Copy BOTH archives OFF this VM to private secure storage.' \
  'Do not post the plaintext backup to Telegram, a public drive or this chat.'
read -r -p "After independently transferring both files off-VM, type EXACTLY $ACK : " confirmation
[[ "$confirmation" == "$ACK" ]] || abort 'Off-VM backup not confirmed; trading remains stopped, no git update.'

printf '\n===== FAST-FORWARD ONLY TO REVIEWED PR92 MERGE =====\n'
[[ $(systemctl show trading_bot.service -p ActiveState --value) == inactive ]] || abort 'Trading service became active.'
[[ $(git rev-parse origin/main) == "$RELEASE_COMMIT" ]] || abort 'Remote revision changed.'
git merge --ff-only origin/main
[[ $(git rev-parse HEAD) == "$RELEASE_COMMIT" ]] || abort 'Unexpected merge result.'
[[ -z $(git status --porcelain) ]] || abort 'Checkout not clean after merge.'

printf '\n===== ISOLATED VM TESTS (NO BROKER REQUESTS) =====\n'
AUDITBOT_TEST_ISOLATION=1 ./venv/bin/python3 -m unittest -q \
  tests.test_tracker_v2_activation_preflight \
  tests.test_tracker_v2_activation_script \
  tests.test_tracker_v2_dual_service \
  tests.test_tracker_v2_health \
  tests.test_tracker_v2_source_replacements \
  tests.test_tracker_v2_review \
  tests.test_tracker_v2_stress_matrix \
  tests.test_tracker_v2_authenticated_acceptance
[[ -z $(git status --porcelain) ]] || abort 'Tests changed tracked/untracked source; review required.'

printf '\n===== V2 STOPPED PREFLIGHT AND ACTIVATION =====\n'
bash deploy/tracker_v2_activation.sh --preflight "$RELEASE_COMMIT" "$BACKUP_ARCHIVE"
bash deploy/tracker_v2_activation.sh --apply-stopped \
  "$RELEASE_COMMIT" "$BACKUP_ARCHIVE" "$ACK"

printf '\n===== CURRENT DEPLOYMENT SUMMARY =====\n'
bash deploy/tracker_v2_activation.sh --inspect
systemctl show trading_bot.service -p ActiveState -p MainPID --no-pager
systemctl show auditbot-web.service -p ActiveState -p MainPID --no-pager
printf '\nV2 is STAGED in the WebUI, not yet authorized to start LIVE trading.\n'
printf '%s\n' \
  'NEXT: Independently confirm Dhan auth/account, broker orders, holdings/positions, OCO protections, and GET budget.' \
  'Only after that, START via authenticated Control Center with LIVE confirmation.' \
  "Then verify: cd ~/uditbot && bash deploy/tracker_v2_activation.sh --verify-running $RELEASE_COMMIT" \
  "Private backup folder: $BACKUP_DIR"
