#!/usr/bin/env bash
# AuditBot V14: fresh-VM, idempotent service provisioning. Never run as root.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
BOT_USER="$(id -un)"
MODE="${1:---plan}"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ "$MODE" == "--plan" || "$MODE" == "--install" ]] || fail "Use --plan (read-only) or --install."
[[ "$(id -u)" != 0 ]] || fail "Run as the non-root bot owner (not sudo)."
[[ "$BOT_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] || fail "Invalid Unix username."
[[ "$REPO_DIR" =~ ^/[a-zA-Z0-9_./-]+$ ]] || fail "Unsupported checkout path characters."
[[ -x "$REPO_DIR/venv/bin/python3" ]] || fail "Python venv missing."
[[ -f "$REPO_DIR/src/setup_app.py" ]] || fail "Not an AuditBot source checkout."
[[ -f "$REPO_DIR/.env" ]] || fail "No .env: configure TEST mode first."
[[ -d "$REPO_DIR/data" ]] || fail "Data directory missing."

TRADING=/etc/systemd/system/trading_bot.service
WEB=/etc/systemd/system/auditbot-web.service
TIMER=/etc/systemd/system/dhan-token-refresh.timer
REFRESH=/etc/systemd/system/dhan-token-refresh.service
SUDOERS=/etc/sudoers.d/auditbot-web-restart
WEB_DROPIN=/etc/systemd/system/auditbot-web.service.d
TIMER_DROPIN=/etc/systemd/system/dhan-token-refresh.timer.d
CATCHUP="${AUDITBOT_TOKEN_CATCHUP:-0}"
BOOT="${AUDITBOT_ENABLE_TRADING_BOOT:-0}"
[[ "$CATCHUP" == 0 || "$CATCHUP" == 1 ]] || fail "AUDITBOT_TOKEN_CATCHUP must be 0 or 1"
[[ "$BOOT" == 0 || "$BOOT" == 1 ]] || fail "AUDITBOT_ENABLE_TRADING_BOOT must be 0 or 1"
if [[ "$MODE" == "--plan" ]]; then
    printf 'AUDITBOT INSTALL PLAN (no changes)\nUser: %s\nCheckout: %s\n' "$BOT_USER" "$REPO_DIR"
    for unit in "$TRADING" "$WEB" "$TIMER" "$REFRESH"; do
        if [[ -e "$unit" ]]; then printf 'PRESERVE: %s\n' "$unit"; else printf 'CREATE: %s\n' "$unit"; fi
    done
    if [[ -e "$SUDOERS" ]]; then printf 'PRESERVE: %s\n' "$SUDOERS"; else printf 'CREATE IF NEW WEB: %s\n' "$SUDOERS"; fi
    printf 'Token timer catch-up (new install only): %s\n' "$CATCHUP"
    printf 'Trading service enabled on boot (opt-in): %s\n' "$BOOT"
    printf 'No service is started, stopped or restarted by --plan.\n'
    exit 0
fi
command -v sudo >/dev/null || fail "sudo unavailable."
[[ -x /usr/bin/systemctl ]] || fail "/usr/bin/systemctl required on Debian VM."
[[ -x /usr/sbin/visudo ]] || fail "/usr/sbin/visudo required; install sudo."
# Existing units and drop-ins are NEVER rewritten, reloaded, enabled, started,
# or stopped by the installer. Only missing units on a fresh VM are provisioned.
WEB_EXISTED=0; TIMER_EXISTED=0; TRADING_EXISTED=0
[[ -e "$WEB" ]] && WEB_EXISTED=1
[[ -e "$TIMER" ]] && TIMER_EXISTED=1
[[ -e "$TRADING" ]] && TRADING_EXISTED=1
if [[ ! -e "$WEB" && -d "$WEB_DROPIN" ]] || [[ ! -e "$WEB" && -e "$SUDOERS" ]] || [[ ! -e "$WEB" && -e /etc/sudoers.d/auditbot-web ]]; then
    fail "WebUI orphaned drop-in/sudoers detected; inspect manually before installation."
fi
if [[ ! -e "$TIMER" && -d "$TIMER_DROPIN" ]]; then
    fail "Timer orphaned drop-in detected; inspect manually before installation."
fi
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
umask 077

cat >"$TMP/trading_bot.service" <<EOF
[Unit]
Description=AuditBot Telegram-to-Dhan Trading Service
Wants=time-sync.target
After=network-online.target time-sync.target

[Service]
Type=simple
User=$BOT_USER
Group=$BOT_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$REPO_DIR/venv/bin/python3 run.py
Restart=on-failure
RestartSec=10
UMask=0077
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
cat >"$TMP/auditbot-web.service" <<EOF
[Unit]
Description=AuditBot WebUI (localhost-only; setup dashboard)
After=network.target

[Service]
Type=simple
User=$BOT_USER
Group=$BOT_USER
WorkingDirectory=$REPO_DIR
Environment=PYTHONUNBUFFERED=1
ExecStart=$REPO_DIR/venv/bin/python3 -m src.setup_app --host 127.0.0.1 --port 80
Restart=on-failure
RestartSec=5
UMask=0077
AmbientCapabilities=CAP_NET_BIND_SERVICE
NoNewPrivileges=false
# Do not add CapabilityBoundingSet=CAP_NET_BIND_SERVICE: blocks sudo capabilities.

[Install]
WantedBy=multi-user.target
EOF
cat >"$TMP/dhan-token-refresh.service" <<EOF
[Unit]
Description=AuditBot daily Dhan TOTP refresh and health probe
Wants=network-online.target time-sync.target
After=network-online.target time-sync.target

[Service]
Type=oneshot
User=$BOT_USER
Group=$BOT_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$REPO_DIR/bot daily_health_check
UMask=0077
StandardOutput=journal
StandardError=journal
EOF
cat >"$TMP/dhan-token-refresh.timer" <<EOF
[Unit]
Description=Daily Dhan token refresh at 08:00 IST Mon-Fri

[Timer]
OnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata
Persistent=$([[ "$CATCHUP" == 1 ]] && echo true || echo false)
RandomizedDelaySec=15
Unit=dhan-token-refresh.service

[Install]
WantedBy=timers.target
EOF
cat >"$TMP/auditbot-web-restart" <<EOF
$BOT_USER ALL=(root) NOPASSWD: /usr/bin/systemctl restart trading_bot.service
EOF
sudo /usr/sbin/visudo -cf "$TMP/auditbot-web-restart" >/dev/null || fail "Sudoers syntax invalid."
# Existing sudoers policies elsewhere must be independently audited.
for name in trading_bot.service auditbot-web.service dhan-token-refresh.service dhan-token-refresh.timer; do
    [[ -e "/etc/systemd/system/$name" ]] && { echo "PRESERVE $name"; continue; }
    sudo install -o root -g root -m 0644 "$TMP/$name" "/etc/systemd/system/$name"
    echo "INSTALLED $name"
done
# Do not install or replace sudoers for an existing WebUI automatically.
if [[ "$WEB_EXISTED" == 0 ]]; then
    sudo install -o root -g root -m 0440 "$TMP/auditbot-web-restart" "$SUDOERS"
    echo "INSTALLED exact-command WebUI sudoers"
fi
# Enforce private runtime permissions without reading or printing any secret.
chmod 0700 "$REPO_DIR/data"
[[ ! -d "$REPO_DIR/logs" ]] || chmod 0700 "$REPO_DIR/logs"
chmod 0600 "$REPO_DIR/.env"
if [[ -f "${HOME}/.dhan_token" ]]; then chmod 0600 "${HOME}/.dhan_token"; fi
find "$REPO_DIR/data" -maxdepth 1 -type f \
    \( -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' -o -name '*.session' \
       -o -name '*.session-journal' -o -name 'emergency_stop.json' -o -name 'pending_protection.json' \) \
    -exec chmod 0600 {} +
sudo systemctl daemon-reload
if [[ "$WEB_EXISTED" == 0 ]]; then
    sudo systemctl enable auditbot-web.service
    sudo systemctl start auditbot-web.service
fi
if [[ "$TIMER_EXISTED" == 0 ]]; then
    sudo systemctl enable --now dhan-token-refresh.timer
fi
if [[ "$TRADING_EXISTED" == 0 && "$BOOT" == 1 ]]; then
    # Explicit opt-in enables NEXT boot; never starts the trading service now.
    sudo systemctl enable trading_bot.service
fi
printf '\nProvisioning finished. Existing service units were not overwritten.\n'
printf 'Trading bot not started or restarted by this installer.\n'
printf 'Fresh WebUI defaults to http://127.0.0.1:80 (SSH tunnel required).\n'
