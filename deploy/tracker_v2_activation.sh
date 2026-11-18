#!/usr/bin/env bash
# UDITBOT V2 activation: no LIVE restart/stop/start and no brokerage operations.
# Run from the reviewed GitHub checkout as the bot service owner, not root.
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
TRADING='trading_bot.service'
WEB='auditbot-web.service'
MARKER='# managed-by: uditbot-tracker-v2-activation-v1'
FLAG='UDITBOT_TRACKER_V2_ENABLE=1'
DROP_NAME='zzzzzz-uditbot-tracker-v2-activation.conf'
FEATURE_DIR='/etc/uditbot-tracker-v2'
FEATURE_FILE="$FEATURE_DIR/activation.env"
ACK_APPLY='I_HAVE_OFF_VM_BACKUP_AND_AUTHORIZE_STOPPED_V2'
ACK_ROLLBACK='I_AUTHORIZE_STOPPED_V2_ROLLBACK'
BACKUP_ROOT="$HOME/uditbot-backups"

say() { printf '%s\n' "$*"; }
block() { say "BLOCK: $*" >&2; exit 2; }

[[ $(id -u) != 0 ]] || block 'Run as the UditBot service owner, not root.'
[[ $(hostname -s) == uditbot ]] || block 'Wrong host. Production activation is only for uditbot.'
[[ -x ./venv/bin/python3 ]] || block 'Missing expected virtual environment.'

mode="${1:---plan}"
expected_head="${2:-}"
backup="${3:-}"
confirmation="${4:-}"

verify_git() {
  [[ "$expected_head" =~ ^[0-9a-f]{40}$ ]] || block 'An exact approved 40-character commit is required.'
  [[ $(git rev-parse HEAD) == "$expected_head" ]] || block 'Current Git HEAD does not match approved commit.'
  [[ -z $(git status --porcelain) ]] || block 'Git checkout is dirty. Use a reviewed clean release checkout.'
}

preflight() {
  local stage="$1"
  if [[ "$stage" == stopped || "$stage" == staged ]]; then
    [[ -n "$backup" && "$backup" == /* ]] || block 'Supply an absolute fresh runtime backup path.'
    PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 -m deploy.tracker_v2_preflight \
      --stage "$stage" --expected-head "$expected_head" --backup "$backup"
  else
    PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 -m deploy.tracker_v2_preflight \
      --stage running --expected-head "$expected_head"
  fi
}

stopped_now() {
  [[ $(systemctl show "$TRADING" -p ActiveState --value) == inactive ]] || return 1
  [[ $(systemctl show "$TRADING" -p MainPID --value) == 0 ]] || return 1
}

managed_file() {
  local path="$1"
  sudo test -f "$path" && sudo grep -Fxq "$MARKER" "$path"
}

no_existing_managed_files() {
  local unit
  if sudo test -L "$FEATURE_DIR"; then
    block 'V2 feature directory is symlinked. Refusing root install.'
  fi
  for unit in "$TRADING" "$WEB"; do
    if sudo test -L "/etc/systemd/system/$unit.d"; then
      block "Systemd unit drop-in directory is symlinked: $unit"
    fi
    local path="/etc/systemd/system/$unit.d/$DROP_NAME"
    if sudo test -e "$path" || sudo test -L "$path"; then
      block "Existing V2 drop-in for $unit needs explicit audit. No overwrite."
    fi
  done
  if sudo test -e "$FEATURE_FILE" || sudo test -L "$FEATURE_FILE"; then
    block 'Existing V2 activation flag file needs explicit audit. No overwrite.'
  fi
}

verify_unit_config() {
  local unit
  for unit in "$TRADING" "$WEB"; do
    if sudo test -L "/etc/systemd/system/$unit.d"; then
      block "Systemd unit drop-in directory is symlinked: $unit"
    fi
    local path="/etc/systemd/system/$unit.d/$DROP_NAME"
    managed_file "$path" || block "Managed drop-in missing/changed: $unit"
    # Check systemd parsed the exact EnvironmentFile reference. Runtime
    # process flags are checked separately, after service starts.
    local values
    values=$(systemctl show "$unit" -p EnvironmentFiles --value)
    [[ "$values" == *"$FEATURE_FILE"* ]] || block "Effective EnvironmentFiles omits V2 flag for $unit"
  done
  managed_file "$FEATURE_FILE" || block 'Flag file missing/changed'
  sudo grep -Fxq "$FLAG" "$FEATURE_FILE" || block 'Flag file value differs'
}

lock_owner() {
  # Use the same cross-process gate as the existing WebUI service coordinator.
  # The data directory is already required private by preflight.
  local data_dir lock
  data_dir=$(PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - <<'PYCODE'
from pathlib import Path
from src.config import AppConfig
print(Path(AppConfig.from_env().data_dir).expanduser().resolve())
PYCODE
)
  lock="$data_dir/config_operation.lock"
  [[ -d "$data_dir" && ! -L "$data_dir" ]] || block 'Unsafe configured data directory.'
  [[ ! -L "$lock" ]] || block 'Unsafe config operation lock symlink.'
  if [[ -e "$lock" ]]; then
    [[ -f "$lock" && $(stat -c %a "$lock") == 600 && $(stat -c %u "$lock") == "$(id -u)" ]] ||
      block 'Existing configuration lock must be private and operator-owned.'
  fi
  exec 9>>"$lock"
  flock -n 9 || block 'Another configuration/service operation is in progress.'
}

# This checks only the authoritative V2 records; it never clears them. On a
# newly activated WebUI, a failed startup may coexist with a saved operation.
# In that situation auto rollback would re-enable the legacy worker: refuse.
v2_recovery_clear() {
  PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - <<'PY'
from pathlib import Path
from src.config import AppConfig
from src.engine.tracker_v2_safety import read_private_json
p = Path(AppConfig.from_env().data_dir).expanduser().resolve()/'config_restart_v2'
for name in ('current_transaction.json', 'apply_barrier.json'):
    target = p/name
    if target.exists() or target.is_symlink():
        raise SystemExit(2)
ledger = p.parent/'active_broker_mutations.json'
# Once a V2 trading producer has run, even an empty ledger is recovery
# evidence. An automatic downgrade to legacy may bypass the V2 fence.
if ledger.exists() or ledger.is_symlink():
    raise SystemExit(2)
PY
}

remove_ours() {
  local unit path
  for unit in "$TRADING" "$WEB"; do
    path="/etc/systemd/system/$unit.d/$DROP_NAME"
    if sudo test -e "$path" || sudo test -L "$path"; then
      managed_file "$path" || return 1
      sudo rm -- "$path" || return 1
    fi
  done
  if sudo test -e "$FEATURE_FILE" || sudo test -L "$FEATURE_FILE"; then
    managed_file "$FEATURE_FILE" || return 1
    sudo rm -- "$FEATURE_FILE" || return 1
  fi
  sudo systemctl daemon-reload
}

case "$mode" in
  --plan)
    say 'V2 release flow: owner-approved STOP (broker safe) -> fresh private backup -> staged dual-service activation -> WebUI restart -> owner-approved START -> V2 running verification.'
    say 'No command here stops, starts or restarts trading_bot.service.'
    say 'Run --preflight SHA /absolute/backup.tar.gz first.'
    ;;
  --preflight)
    verify_git
    preflight stopped
    ;;
  --inspect)
    say '===== V2 UNIT INSPECTION (NO CHANGES) ====='
    for unit in "$TRADING" "$WEB"; do
      say "$unit: $(systemctl show "$unit" -p ActiveState --value)"
      values=$(systemctl show "$unit" -p EnvironmentFiles --value)
      [[ "$values" == *"$FEATURE_FILE"* ]] && say 'V2 unit configuration: PRESENT' || say 'V2 unit configuration: ABSENT'
    done
    ;;
  --apply-stopped)
    [[ "$confirmation" == "$ACK_APPLY" ]] || block "Explicit approval phrase required: $ACK_APPLY"
    verify_git
    preflight stopped
    stopped_now || block 'Trading process became active. No activation during LIVE operation.'
    sudo -v
    no_existing_managed_files
    lock_owner
    preflight stopped
    # Capture current effective service definitions, PRIVATE, before touching
    # units. No credentials are printed to the terminal.
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    save_dir="$BACKUP_ROOT/v2-activation-$stamp"
    mkdir -m 0700 -p "$save_dir"
    systemctl cat "$TRADING" > "$save_dir/trading.before.txt"
    systemctl cat "$WEB" > "$save_dir/web.before.txt"
    chmod 0600 "$save_dir"/*
    staged=0
    completed=0
    cleanup_on_exit() {
      local rc=$?
      trap - EXIT
      [[ -z "${tmp:-}" ]] || rm -rf -- "$tmp"
      if [[ "$staged" == 1 && "$completed" != 1 ]]; then
        if stopped_now && v2_recovery_clear; then
          if remove_ours; then
            sudo systemctl restart "$WEB" || true
            say 'Failed activation: managed flags removed; trading unit remains STOPPED.' >&2
          else
            say 'ACTION REQUIRED: managed flags could not be removed safely.' >&2
          fi
        else
          say 'ACTION REQUIRED: recovery/STOP state unknown. Flags PRESERVED; do not re-enable legacy.' >&2
        fi
      fi
      exit "$rc"
    }
    trap cleanup_on_exit EXIT
    tmp=$(mktemp -d)
    printf '%s\n%s\n' "$MARKER" "$FLAG" > "$tmp/activation.env"
    printf '%s\n[Service]\nEnvironmentFile=%s\n' "$MARKER" "$FEATURE_FILE" > "$tmp/v2.conf"
    sudo install -d -o root -g root -m 0755 "$FEATURE_DIR"
    sudo install -o root -g root -m 0644 "$tmp/activation.env" "$FEATURE_FILE"
    staged=1
    for unit in "$TRADING" "$WEB"; do
      sudo install -d -o root -g root -m 0755 "/etc/systemd/system/$unit.d"
      sudo install -o root -g root -m 0644 "$tmp/v2.conf" "/etc/systemd/system/$unit.d/$DROP_NAME"
    done
    sudo systemctl daemon-reload
    stopped_now || block 'Trading bot was started by another actor; preserve/abort.'
    verify_unit_config
    preflight staged
    sudo systemctl restart "$WEB"
    [[ $(systemctl show "$WEB" -p ActiveState --value) == active ]] || block 'WebUI did not return active'
    stopped_now || block 'Trading bot became active during activation'
    # Check the new WebUI *process* reads the feature flag; a unit drop-in
    # alone is insufficient proof if other overrides take precedence.
    PYTHONDONTWRITEBYTECODE=1 ./venv/bin/python3 - <<'PY'
from deploy.tracker_v2_preflight import process_flag, run
pid = int(run(['systemctl','show','auditbot-web.service','-p','MainPID','--value']))
flag = process_flag(pid)  # unprivileged Python; sudo -n cat only if /proc denied
if flag != 'ON':
    raise SystemExit(f'WebUI V2 process flag not verified ({flag})')
PY
    # Confirm no login bypass on the public surface. Do not send credentials.
    status=$(curl --noproxy '*' -sS --connect-timeout 2 --max-time 6 -o /dev/null -w '%{http_code}' http://127.0.0.1/)
    [[ "$status" == 303 ]] || block "WebUI login protection smoke-check unexpected ($status)"
    completed=1
    say '===== STAGED V2 ACTIVATION VERIFIED ====='
    say 'WebUI V2 flag ON; trading process is intentionally STOPPED.'
    say 'No bot start, trading restart, broker GET or broker mutation performed.'
    say 'Use owner-approved Control Center START only after independent broker/account/position verification.'
    say "Private service backup: $save_dir"
    ;;
  --verify-running)
    verify_git
    preflight running
    ;;
  --rollback-stopped)
    [[ "$backup" == "$ACK_ROLLBACK" ]] || block "Explicit approval phrase required: $ACK_ROLLBACK"
    verify_git
    sudo -v
    lock_owner
    stopped_now || block 'LIVE trading process must be stopped before rollback.'
    v2_recovery_clear || block 'V2 transaction, barrier or broker mutations: rollback prohibited.'
    verify_unit_config
    remove_ours || block 'Managed unit files changed; refusing unsafe partial rollback.'
    stopped_now || block 'Trading process started during rollback.'
    sudo systemctl restart "$WEB"
    [[ $(systemctl show "$WEB" -p ActiveState --value) == active ]] || block 'WebUI rollback restart not verified.'
    say 'ROLLBACK: V2 service flags removed, WebUI restarted; trading service still STOPPED.'
    say 'Owner STOP, journal, keys and broker safety evidence untouched.'
    ;;
  *)
    say 'Usage:' >&2
    say '  bash deploy/tracker_v2_activation.sh --plan' >&2
    say '  bash deploy/tracker_v2_activation.sh --preflight <exact-git-sha> /absolute/runtime.tar.gz' >&2
    say "  bash deploy/tracker_v2_activation.sh --apply-stopped <exact-git-sha> /absolute/runtime.tar.gz $ACK_APPLY" >&2
    say '  bash deploy/tracker_v2_activation.sh --verify-running <exact-git-sha>' >&2
    say "  bash deploy/tracker_v2_activation.sh --rollback-stopped <exact-git-sha> $ACK_ROLLBACK" >&2
    exit 2
    ;;
esac
