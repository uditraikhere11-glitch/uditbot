#!/usr/bin/env bash
# AmanBot opt-in dual 08:00 weekday + 17:00 daily Dhan health report timer.
# --plan is read-only. No trading bot or WebUI restart. No broker calls.
set -Eeuo pipefail
MODE=--plan
if [[ $# -gt 0 ]]; then MODE="$1"; fi
[[ "$MODE" == --plan || "$MODE" == --apply || "$MODE" == --rollback ]] || { echo 'Usage: script --plan|--apply|--rollback' >&2; exit 2; }
[[ "$(id -u)" != 0 ]] || { echo "Run as bot owner, not root" >&2; exit 2; }

ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
OWNER="$(id -un)"
TIMER=/etc/systemd/system/dhan-token-refresh.timer
REFRESH=/etc/systemd/system/dhan-token-refresh.service
AM_SERVICE=/etc/systemd/system/auditbot-morning-health.service
AM_TIMER=/etc/systemd/system/auditbot-morning-health.timer
MARK='# amanbot_dual_health_schedule managed'
BACKUP="$ROOT/data/systemd_backups/dhan-token-refresh.timer.bak.latest"
die() { echo "AMANBOT PARITY ERROR: $*" >&2; exit 1; }

[[ -x "$ROOT/bot" ]] || die 'Bot CLI is not executable'
[[ -f "$ROOT/.env" ]] || die 'AmanBot local .env is absent'

if [[ "$MODE" == --plan ]]; then
  echo "===== AMANBOT DUAL TIMER PLAN (READ ONLY) ====="
  echo "Owner: $OWNER; checkout: $ROOT"
  git -C "$ROOT" log -1 --format='%h %s' || true
  systemctl cat dhan-token-refresh.timer --no-pager 2>/dev/null | grep -E '^OnCalendar=|^Persistent=|^RandomizedDelaySec=' || true
  systemctl cat dhan-token-refresh.service --no-pager 2>/dev/null | grep -E '^User=|^WorkingDirectory=|^ExecStart=' || true
  echo 'Proposed 08:00 Mon-Fri separate report + existing Udit-compatible 17:00 daily refresh'
  echo 'No .env, trading service, web service, broker account or Telegram group changes'
  exit 0
fi

if [[ "$MODE" == --rollback ]]; then
  [[ -f "$AM_TIMER" && -f "$AM_SERVICE" && -f "$BACKUP" ]] || die 'Managed files/backup missing: no action'
  grep -Fq "$MARK" "$AM_TIMER" && grep -Fq "$MARK" "$AM_SERVICE" || die 'Unrecognized units; refuse deletion'
  bash "$ROOT/deploy/install_vm_services.sh" --rollback-timer
  sudo systemctl disable --now auditbot-morning-health.timer
  sudo rm -- "$AM_TIMER" "$AM_SERVICE"
  sudo systemctl daemon-reload
  echo 'ROLLBACK COMPLETE: previous timer restored; morning helper removed'
  exit 0
fi

[[ -f "$TIMER" && -f "$REFRESH" ]] || die 'Existing Dhan timer or service missing'
[[ ! -e "$AM_SERVICE" && ! -e "$AM_TIMER" ]] || die 'Morning unit already exists; do not overwrite'
grep -Fq 'def _is_morning_health_slot()' "$ROOT/src/cli.py" || die 'Slot-aware CLI code not installed'
[[ -z "$(git -C "$ROOT" status --porcelain)" ]] || die 'Uncommitted Git changes; refuse apply'
CAL="$(systemctl cat dhan-token-refresh.timer --no-pager 2>/dev/null | grep -E '^OnCalendar=' | tail -1 || true)"
[[ "$CAL" == *'Mon..Fri '* && "$CAL" == *'08:00:00'* ]] || die "Expected 08:00 weekday timer, found: $CAL"
SVC="$(systemctl cat dhan-token-refresh.service --no-pager 2>/dev/null)"
grep -Fq "WorkingDirectory=$ROOT" <<< "$SVC" || die 'Refresh service points to a different checkout'
grep -Eq '^ExecStart=.*(bot daily_health_check|src.cli daily_health_check)' <<< "$SVC" || die 'Service does not run daily_health_check'
grep -Eq "^User=$OWNER$" <<< "$SVC" || die 'Refresh service owner mismatch'

# Never print secrets or Dhan account values.
python3 - "$ROOT/.env" <<'PY'
import sys
from pathlib import Path
vals={}
for line in Path(sys.argv[1]).read_text().splitlines():
    if line.lstrip().startswith("#") or "=" not in line:
        continue
    k,v=line.split("=",1)
    vals[k.strip()]=v.strip().strip("\"'")
if not vals.get("LOG_GROUP_ID"):
    raise SystemExit("LOG_GROUP_ID missing in AmanBot .env; no changes made")
print("AmanBot Telegram log destination: CONFIGURED (value hidden)")
PY

TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
cat > "$TMP/am.service" <<EOF
$MARK
[Unit]
Description=AmanBot weekday 08:00 Dhan health report
Wants=network-online.target time-sync.target
After=network-online.target time-sync.target
[Service]
Type=oneshot
User=$OWNER
Group=$OWNER
WorkingDirectory=$ROOT
EnvironmentFile=$ROOT/.env
Environment="AUDITBOT_HEALTH_SLOT=AM_0800"
ExecStart=$ROOT/bot daily_health_check
UMask=0077
StandardOutput=journal
StandardError=journal
EOF
cat > "$TMP/am.timer" <<EOF
$MARK
[Unit]
Description=AmanBot weekday 08:00 Dhan health report
[Timer]
OnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata
Persistent=false
RandomizedDelaySec=15
Unit=auditbot-morning-health.service
[Install]
WantedBy=timers.target
EOF

sudo install -o root -g root -m 0644 "$TMP/am.service" "$AM_SERVICE"
sudo install -o root -g root -m 0644 "$TMP/am.timer" "$AM_TIMER"
sudo systemctl daemon-reload
# The existing upstream utility takes an automatic backup and rolls back on failure.
if ! bash "$ROOT/deploy/install_vm_services.sh" --upgrade-timer; then
  sudo systemctl disable --now auditbot-morning-health.timer 2>/dev/null || true
  sudo rm -f -- "$AM_TIMER" "$AM_SERVICE"
  sudo systemctl daemon-reload
  die '17:00 upgrade failed; morning helper removed'
fi
if ! sudo systemctl enable --now auditbot-morning-health.timer; then
  bash "$ROOT/deploy/install_vm_services.sh" --rollback-timer || true
  sudo systemctl disable --now auditbot-morning-health.timer 2>/dev/null || true
  sudo rm -f -- "$AM_TIMER" "$AM_SERVICE"
  sudo systemctl daemon-reload
  die 'Morning timer enable failed; original timer rollback attempted'
fi
echo '===== ACTIVE SCHEDULES ====='
systemctl list-timers --all dhan-token-refresh.timer auditbot-morning-health.timer --no-pager || true
echo 'Configured: 08:00 Mon-Fri + 17:00 daily; no Dhan/Telegram calls and no trading/WebUI restarts performed'
echo 'Rollback: bash deploy/amanbot_dual_health_schedule.sh --rollback'
