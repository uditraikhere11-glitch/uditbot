#!/usr/bin/env bash
# Read-only inventory & existing-VM security drift detector (H-02).
# No .env, token, sudoers contents, or Telegram session printed.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$REPO_DIR"
echo '===== AuditBot VM rebuild drift report (read-only) ====='
echo "Owner: $(id -un); Checkout: $REPO_DIR"
printf 'Git commit: '; git rev-parse --short HEAD 2>/dev/null || true
printf 'Git clean: '; [[ -z "$(git status --porcelain 2>/dev/null)" ]] && echo YES || echo NO
echo '===== systemd units / overrides ====='
for svc in trading_bot.service auditbot-web.service dhan-token-refresh.service dhan-token-refresh.timer; do
  echo "--- $svc"
  systemctl show "$svc" -p LoadState -p ActiveState -p UnitFileState \
    -p Restart -p MainPID -p User -p WorkingDirectory -p FragmentPath \
    -p DropInPaths -p AmbientCapabilities -p CapabilityBoundingSet \
    -p NoNewPrivileges -p NextElapseUSecRealtime --no-pager 2>/dev/null || true
done
echo '===== restricted paths (metadata only) ====='
for file in "$REPO_DIR/.env" "$HOME/.dhan_token" "$REPO_DIR/data/trade_journal.db" \
  "$REPO_DIR/data/tg_userbot.session" "$REPO_DIR/data/emergency_stop.json" \
  "$REPO_DIR/data/pending_protection.json"; do
  if [[ -f "$file" ]]; then stat -c '%a %U:%G %n' "$file"; else echo "ABSENT $file"; fi
done
echo '===== installed systemd/security override inventory (paths only) ====='
for d in /etc/systemd/system/auditbot-web.service.d \
         /etc/systemd/system/trading_bot.service.d \
         /etc/systemd/system/dhan-token-refresh.timer.d \
         /etc/systemd/system/dhan-token-refresh.service.d; do
    if [[ -d "$d" ]]; then find "$d" -maxdepth 1 -type f -printf '%p\n' 2>/dev/null || true; fi
done
for f in /etc/sudoers.d/auditbot-web /etc/sudoers.d/auditbot-web-restart; do
    if [[ -e "$f" ]]; then echo "REVIEW SUDOERS POLICY: $f"; fi
done
echo '===== WebUI sockets (no HTTP request) ====='
ss -ltn 2>/dev/null | grep -E '(^State|:80[[:space:]]|:8080[[:space:]])' || true
echo '===== timer schedule ====='
systemctl list-timers dhan-token-refresh.timer --no-pager 2>/dev/null || true
echo '===== existing-VM security drift checklist (H-02 read-only) ====='
if [[ -e /etc/sudoers.d/auditbot-web ]]; then
    echo 'DRIFT_WARN: Legacy /etc/sudoers.d/auditbot-web exists; manual admin review/removal required (install_vm_services.sh preserves existing files).'
else
    echo 'DRIFT_OK: Legacy /etc/sudoers.d/auditbot-web absent.'
fi
if ss -ltn 2>/dev/null | grep -E '0\.0\.0\.0:80[[:space:]]|\*:80[[:space:]]|:::80[[:space:]]' >/dev/null; then
    echo 'DRIFT_WARN: Public 0.0.0.0:80 / :::80 listener detected; manual remediation to 127.0.0.1:80 required on existing VM.'
else
    echo 'DRIFT_OK: No public 0.0.0.0:80 listener detected.'
fi
echo 'NOTE: Effective local overrides/permissions require separate administrator review.'
echo 'NOTE: Git cannot preserve .env, access tokens, Telegram sessions, or SQLite journal.'
