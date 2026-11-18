#!/usr/bin/env bash
# PR93: controlled owner-stop retry with the staged category-aware governor.
# Fail closed if broker obligations cannot be verified. No automatic LIVE start.
set -Eeuo pipefail
umask 077

ROOT="$HOME/uditbot"
RELEASE="$HOME/uditbot-releases/pr93-20261118T100717Z"
BACKUP="$HOME/uditbot-backups/pre-pr93-20261118T100717Z"
OLD='11d2c4c3c48264a03e498761b1e5158c8eb6706a'
TARGET='5ab19ef81873561fb49792b28b7e318fa74ae822'

abort(){ printf 'BLOCKED: %s\n' "$*" >&2; exit 3; }
[[ "$(hostname -s)" == 'uditbot' ]] || abort 'Wrong VM'
[[ "$(id -u)" != 0 ]] || abort 'Run as UditBot owner, not root'
[[ ! -L "$ROOT" && ! -L "$RELEASE" && ! -L "$BACKUP" ]] || abort 'Unexpected symlink in deployment paths'
[[ -x "$ROOT/venv/bin/python3" && -f "$ROOT/.env" ]] || abort 'Runtime configuration or Python missing'
[[ -d "$RELEASE" && -d "$BACKUP" ]] || abort 'Verified release or backup missing'
cd "$ROOT"
[[ "$(git branch --show-current)" == 'main' ]] || abort 'Active checkout is not main'
[[ "$(git rev-parse HEAD)" == "$OLD" ]] || abort 'Active checkout changed: review before deploying'
[[ "$(git -C "$RELEASE" rev-parse HEAD)" == "$TARGET" ]] || abort 'Staged release does not match approved PR93'
[[ -z "$(git status --porcelain)" ]] || abort 'Working tree is not clean'
[[ -s "$BACKUP/runtime-online.tar.gz" && -s "$BACKUP/source-before-update.bundle" ]] || abort 'Backup missing'
(cd "$BACKUP" && sha256sum -c SHA256SUMS >/dev/null) || abort 'Backup checksum mismatch'
[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == active ]] || abort 'Trading service state changed: inspect manually'

echo '===== OWNER-APPROVED SAFE STOP WITH STAGED PR93 GOVERNOR ====='
ACTIVE_ROOT="$ROOT" STAGED_RELEASE="$RELEASE" PYTHONDONTWRITEBYTECODE=1 "$ROOT/venv/bin/python3" - <<'PY'
import os, sys
from pathlib import Path

root = Path(os.environ['ACTIVE_ROOT']).resolve()
stage = Path(os.environ['STAGED_RELEASE']).resolve()
sys.path.insert(0, str(stage))
os.chdir(root)

from src.config import AppConfig, parse_env_dict
from src.models import TradingMode
from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor
from src.engine.config_manager import ConfigChangeTracker, ReadOnlyDhanBrokerAdapter

if ReadGovernor.CATEGORY_LIMITS.get('NON_TRADING') != ((1, 20),):
    raise SystemExit('BLOCKED: staged PR93 category policy not present')

cfg = AppConfig.from_env(env=parse_env_dict(str(root / '.env')), env_file='')
data = Path(cfg.data_dir).expanduser().resolve()
journal = Path(cfg.journal_db_path).expanduser().resolve()
if data != (root / 'data').resolve() or journal != (data / 'trade_journal.db').resolve():
    raise SystemExit('BLOCKED: unexpected runtime paths; no broker action')
if not journal.is_file() or not cfg.dhan_client_id or cfg.trading_mode != TradingMode.LIVE:
    raise SystemExit('BLOCKED: LIVE account or trading journal cannot be verified')

tracker = ConfigChangeTracker(data_dir=str(data))
owner = tracker.get_service_control_record()
gate = tracker.inspect_transition_gate_status(read_only=True)
status = tracker.control_service(action='status', env_path=str(root / '.env'))
if not (owner.get('desired_service_state') == 'STOPPED'
        and owner.get('deferred_stop_pending') is True
        and gate.get('active') is True
        and gate.get('ok') is True
        and gate.get('change_id') == 'OWNER-STOP-PENDING'
        and status.get('service_state') == 'RUNNING'
        and status.get('runtime_verified') is True):
    raise SystemExit('BLOCKED: owner intent, gate, or running state changed')
if os.path.exists(tracker.pending_op_file) or os.path.exists(tracker.deferred_env_file):
    raise SystemExit('BLOCKED: overlapping configuration transaction')

cfg.data_dir = str(data)
client = DhanClient(cfg, auto_generate_token=False)
broker = ReadOnlyDhanBrokerAdapter(client)
result = tracker.control_service(
    action='stop',
    reason='Owner-requested deferred STOP, retry with verified PR93 non-trading GET policy',
    env_path=str(root / '.env'),
    stop_state_path=str(data / 'emergency_stop.json'),
    db_path=str(journal),
    broker=broker,
)
for field in ('ok', 'deferred', 'blocked_reason', 'blocking_order_count',
              'stop_verified', 'service_state', 'message'):
    print(f'{field}: {result.get(field)}')
if not (result.get('ok') is True and result.get('stop_verified') is True):
    raise SystemExit('BLOCKED: broker-safe STOP has not completed. No Git deployment.')
PY

[[ "$(systemctl show trading_bot.service -p ActiveState --value)" == inactive ]] || abort 'STOP not observed inactive'
[[ "$(systemctl show trading_bot.service -p MainPID --value)" == 0 ]] || abort 'STOP not observed with MainPID=0'

echo '===== PINNED PR93 GIT FAST-FORWARD (SERVICE STOPPED) ====='
git merge-base --is-ancestor HEAD "$TARGET" || abort 'PR93 is not a descendant of old active code'
git merge --ff-only --no-edit "$TARGET" || abort 'Git fast-forward unsuccessful; trading bot remains stopped'
[[ "$(git rev-parse HEAD)" == "$TARGET" ]] || abort 'Post-merge Git HEAD mismatch'

echo '===== POST-SWITCH SERVICE STATUS ====='
if sudo -n systemctl restart auditbot-web.service; then
  echo 'WebUI restarted on PR93 source'
else
  echo 'WARNING: WebUI restart not permitted; trading bot remains stopped and source is PR93'
fi
printf 'Git commit: %s\n' "$(git rev-parse --short HEAD)"
printf 'Trading: %s\n' "$(systemctl show trading_bot.service -p ActiveState --value)"
printf 'WebUI: %s\n' "$(systemctl show auditbot-web.service -p ActiveState --value)"
echo 'NO automatic LIVE start. Broker protection and account state must be independently checked before starting.'
