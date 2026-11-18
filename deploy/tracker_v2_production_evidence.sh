#!/usr/bin/env bash
# Read-only UditBot V2 production evidence; no secrets, broker requests or mutations.
set -euo pipefail
umask 077
[[ "$(hostname -s)" == "uditbot" ]] || { echo 'BLOCK: wrong host'; exit 2; }
cd "$HOME/uditbot"
echo '===== SOURCE ====='
git log -1 --format='%H %s'
[[ -z "$(git status --porcelain)" ]] && echo 'WORKTREE: CLEAN' || echo 'WORKTREE: CHANGED (review required)'
echo '===== SYSTEMD ====='
for unit in trading_bot.service auditbot-web.service; do
  printf '%s state=' "$unit"
  systemctl show "$unit" -p ActiveState --value --no-pager
  printf '%s PID=' "$unit"
  systemctl show "$unit" -p MainPID --value --no-pager
 done
echo '===== READ-ONLY SAFETY EVIDENCE ====='
PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - <<'PY'
import json
import os
import subprocess
from pathlib import Path
from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker, compute_config_version
from src.engine.tracker_v2_safety import (
    preview_legacy_tracker_migration, inspect_transaction_recovery,
    inspect_apply_barrier, evaluate_active_broker_operation_gate,
)

def props(unit):
    p = subprocess.run(['systemctl','show',unit,'-p','MainPID','--value','--no-pager'],
                       capture_output=True,text=True,timeout=5,check=True)
    return int(p.stdout.strip())

def flag(pid):
    if pid <= 0: return 'NOT_RUNNING'
    try:
        pairs = Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        flags = [v.partition(b'=')[2] for v in pairs if v.startswith(b'UDITBOT_TRACKER_V2_ENABLE=')]
        return 'ON' if flags == [b'1'] else 'OFF_OR_UNSET' if not flags or flags == [b'0'] else 'AMBIGUOUS'
    except OSError:
        return 'UNREADABLE'

cfg = AppConfig.from_env()
t = ConfigChangeTracker(cfg.data_dir)
d = Path(cfg.data_dir).expanduser().resolve()
for unit in ('trading_bot.service','auditbot-web.service'):
    try:
        p=props(unit)
        print(unit, 'V2:',flag(p))
    except (OSError,ValueError,subprocess.SubprocessError):
        print(unit,'V2: UNKNOWN')
print('Mode:', cfg.trading_mode.value)
try:
    check, _, reason = t.is_active_version_confirmed(
        expected_hashes={compute_config_version(cfg)},
        expected_mode=cfg.trading_mode.value,
        require_systemd_active=True)
    print('Runtime version:', 'VERIFIED' if check else 'NOT_VERIFIED',
          'reason:', reason[:140] if not check else 'Verified')
except Exception as exc:
    print('Runtime version: ERROR', type(exc).__name__)
record=t.get_service_control_record()
for name in ('desired_service_state','observed_state','deferred_stop_pending','last_action'):
    print('Owner', name, ':', record.get(name,'MISSING'))
for name, callback in (
    ('Legacy migration',lambda: preview_legacy_tracker_migration(d)),
    ('V2 transaction',lambda: inspect_transaction_recovery(d,Path('.env').resolve())),
    ('V2 barrier',lambda: inspect_apply_barrier(d/'config_restart_v2/apply_barrier.json')),
    ('Broker handoff',lambda: evaluate_active_broker_operation_gate(d)),
):
    try:
        result=callback()
        print(name, ':', result.code, 'allowed=',result.allowed)
    except Exception as exc:
        print(name, ': ERROR',type(exc).__name__)
print('No Dhan or Telegram API calls made.')
PY
echo '===== COMPLETED; NO CHANGES ====='
