#!/usr/bin/env bash
# AuditBot V14: fresh-VM, idempotent service provisioning + opt-in backup-first timer upgrade. Never run as root.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
BOT_USER="$(id -un)"
MODE="${1:---plan}"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ "$MODE" == "--plan" || "$MODE" == "--install" || "$MODE" == "--plan-timer-upgrade" || "$MODE" == "--upgrade-timer" || "$MODE" == "--rollback-timer" ]] || fail "Use --plan (read-only), --install, --plan-timer-upgrade, --upgrade-timer, or --rollback-timer."
[[ "$(id -u)" != 0 ]] || fail "Run as the non-root bot owner (not sudo)."
[[ "$BOT_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] || fail "Invalid Unix username."
[[ "$REPO_DIR" =~ ^/[a-zA-Z0-9_./-]+$ ]] || fail "Unsupported checkout path characters."
[[ -x "$REPO_DIR/venv/bin/python3" || ( "${AUDITBOT_TEST_ISOLATION:-0}" == "1" && -f "$REPO_DIR/venv/bin/python3" ) ]] || fail "Python venv missing."
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
if [[ "${AUDITBOT_TEST_ISOLATION:-0}" == "1" && -n "${AUDITBOT_SYSTEMD_DIR:-}" ]]; then
    TRADING="${AUDITBOT_SYSTEMD_DIR}/trading_bot.service"
    WEB="${AUDITBOT_SYSTEMD_DIR}/auditbot-web.service"
    TIMER="${AUDITBOT_SYSTEMD_DIR}/dhan-token-refresh.timer"
    REFRESH="${AUDITBOT_SYSTEMD_DIR}/dhan-token-refresh.service"
    WEB_DROPIN="${AUDITBOT_SYSTEMD_DIR}/auditbot-web.service.d"
    TIMER_DROPIN="${AUDITBOT_SYSTEMD_DIR}/dhan-token-refresh.timer.d"
fi
SYSTEMCTL_BIN="${AUDITBOT_SYSTEMCTL_BIN:-/usr/bin/systemctl}"
VISUDO_BIN="${AUDITBOT_VISUDO_BIN:-/usr/sbin/visudo}"
BACKUP_DIR="$REPO_DIR/data/systemd_backups"

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
    printf 'Token timer schedule: *-*-* 17:00:00 Asia/Kolkata (all calendar days)\n'
    printf 'Token timer catch-up (new install only): %s\n' "$CATCHUP"
    printf 'Trading service enabled on boot (opt-in): %s\n' "$BOOT"
    printf 'No service is started, stopped or restarted by --plan.\n'
    printf 'To preview or apply a timer-only upgrade on an existing VM, use --plan-timer-upgrade or --upgrade-timer.\n'
    exit 0
fi

if [[ "$MODE" == "--plan-timer-upgrade" ]]; then
    printf 'AUDITBOT TIMER-ONLY UPGRADE PREVIEW (no changes)\nUser: %s\nCheckout: %s\n' "$BOT_USER" "$REPO_DIR"
    printf 'Target timer unit: %s\n' "$TIMER"
    if [[ -f "$TIMER" ]]; then
        printf 'Current OnCalendar in %s:\n' "$TIMER"
        grep -E '^(Description|OnCalendar|Persistent|RandomizedDelaySec|Unit)=' "$TIMER" || true
    else
        printf 'Current timer status: NOT INSTALLED (will be created)\n'
    fi
    printf '\nProposed 17:00 IST timer configuration:\n'
    printf '  Description=Daily Dhan token refresh at 17:00 IST (all calendar days)\n'
    printf '  OnCalendar=*-*-* 17:00:00 Asia/Kolkata\n'
    printf '  Persistent=%s\n' "$([[ "$CATCHUP" == 1 ]] && echo true || echo false)"
    printf '  RandomizedDelaySec=15\n'
    printf '  Unit=dhan-token-refresh.service\n'
    if command -v systemd-analyze >/dev/null 2>&1; then
        printf '\nCalendar schedule verification (systemd-analyze calendar):\n'
        systemd-analyze calendar "*-*-* 17:00:00 Asia/Kolkata" 2>/dev/null || true
    fi
    printf '\nPreserved untouched: %s, %s, %s, %s, .env, ~/.dhan_token, data/*\n' "$TRADING" "$WEB" "$REFRESH" "$SUDOERS"
    printf 'Backup destination before replace: %s/dhan-token-refresh.timer.bak.<UTC_TS>\n' "$BACKUP_DIR"
    printf 'Rollback command: bash deploy/install_vm_services.sh --rollback-timer\n'
    exit 0
fi

command -v sudo >/dev/null || fail "sudo unavailable."
[[ -x "$SYSTEMCTL_BIN" || ( "${AUDITBOT_TEST_ISOLATION:-0}" == "1" && -f "$SYSTEMCTL_BIN" ) ]] || fail "/usr/bin/systemctl required on Debian VM."

if [[ "$MODE" == "--rollback-timer" ]]; then
    ROLLBACK_SRC="${2:-$BACKUP_DIR/dhan-token-refresh.timer.bak.latest}"
    [[ -f "$ROLLBACK_SRC" ]] || fail "Backup timer file not found: $ROLLBACK_SRC"
    printf 'Rolling back %s from backup %s...\n' "$TIMER" "$ROLLBACK_SRC"
    sudo install -o root -g root -m 0644 "$ROLLBACK_SRC" "$TIMER"
    sudo "$SYSTEMCTL_BIN" daemon-reload
    sudo "$SYSTEMCTL_BIN" restart dhan-token-refresh.timer
    printf 'Rollback complete. Active timer state:\n'
    "$SYSTEMCTL_BIN" list-timers dhan-token-refresh.timer --no-pager 2>/dev/null || true
    printf 'Trading and WebUI services were not touched.\n'
    exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
umask 077

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
Description=Daily Dhan token refresh at 17:00 IST (all calendar days)

[Timer]
OnCalendar=*-*-* 17:00:00 Asia/Kolkata
Persistent=$([[ "$CATCHUP" == 1 ]] && echo true || echo false)
RandomizedDelaySec=15
Unit=dhan-token-refresh.service

[Install]
WantedBy=timers.target
EOF

if [[ "$MODE" == "--upgrade-timer" ]]; then
    BACKUP_FILE=""
    rollback_timer_on_failure() {
        local reason="$1"
        printf 'ERROR during --upgrade-timer: %s\n' "$reason" >&2
        if [[ -n "$BACKUP_FILE" && -f "$BACKUP_FILE" ]]; then
            printf 'Executing automatic rollback of %s from %s...\n' "$TIMER" "$BACKUP_FILE" >&2
            sudo install -o root -g root -m 0644 "$BACKUP_FILE" "$TIMER" || true
            sudo "$SYSTEMCTL_BIN" daemon-reload || true
            sudo "$SYSTEMCTL_BIN" restart dhan-token-refresh.timer || true
        fi
        exit 1
    }

    printf '=== AUDITBOT OPT-IN TIMER-ONLY UPGRADE (17:00 IST DAILY) ===\n'
    printf '[1/5] Before-upgrade timer state:\n'
    "$SYSTEMCTL_BIN" list-timers dhan-token-refresh.timer --no-pager 2>/dev/null || true
    if [[ -f "$TIMER" ]]; then
        grep -E '^(Description|OnCalendar|Persistent|RandomizedDelaySec|Unit)=' "$TIMER" || true
    fi
    mkdir -p "$BACKUP_DIR"
    chmod 0700 "$BACKUP_DIR"
    TS_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
    if [[ -f "$TIMER" ]]; then
        BACKUP_FILE="$BACKUP_DIR/dhan-token-refresh.timer.bak.$TS_UTC"
        cp -p "$TIMER" "$BACKUP_FILE"
        chmod 0600 "$BACKUP_FILE"
        cp -p "$TIMER" "$BACKUP_DIR/dhan-token-refresh.timer.bak.latest"
        chmod 0600 "$BACKUP_DIR/dhan-token-refresh.timer.bak.latest"
        printf '[2/5] Backed up existing timer to: %s\n' "$BACKUP_FILE"
    else
        printf '[2/5] No pre-existing %s found; proceeding with clean timer install.\n' "$TIMER"
    fi
    if [[ -d "$TIMER_DROPIN" ]]; then
        for dropin_conf in "$TIMER_DROPIN"/*.conf; do
            [[ -e "$dropin_conf" ]] || continue
            if grep -Eq '^[[:space:]]*OnCalendar=' "$dropin_conf"; then
                if ! grep -Eq '^[[:space:]]*OnCalendar=\*-\*-\* 17:00:00 Asia/Kolkata[[:space:]]*$' "$dropin_conf"; then
                    rollback_timer_on_failure "Conflicting OnCalendar drop-in override detected in $dropin_conf"
                fi
            fi
        done
    fi
    if [[ ! -e "$REFRESH" ]]; then
        sudo install -o root -g root -m 0644 "$TMP/dhan-token-refresh.service" "$REFRESH" || rollback_timer_on_failure "Failed to install $REFRESH"
        printf '      Installed missing %s\n' "$REFRESH"
    else
        printf '      Preserved existing %s\n' "$REFRESH"
    fi
    printf '[3/5] Installing 17:00 IST timer unit to %s...\n' "$TIMER"
    sudo install -o root -g root -m 0644 "$TMP/dhan-token-refresh.timer" "$TIMER" || rollback_timer_on_failure "Failed to install $TIMER"
    printf '[4/5] Reloading systemd daemon and restarting dhan-token-refresh.timer ONLY...\n'
    sudo "$SYSTEMCTL_BIN" daemon-reload || rollback_timer_on_failure "systemctl daemon-reload failed"
    sudo "$SYSTEMCTL_BIN" enable --now dhan-token-refresh.timer || rollback_timer_on_failure "systemctl enable --now dhan-token-refresh.timer failed"
    sudo "$SYSTEMCTL_BIN" restart dhan-token-refresh.timer || rollback_timer_on_failure "systemctl restart dhan-token-refresh.timer failed"
    printf '[5/5] After-upgrade calendar and effective timer verification:\n'
    if ! grep -Fq "OnCalendar=*-*-* 17:00:00 Asia/Kolkata" "$TIMER"; then
        rollback_timer_on_failure "Installed timer file $TIMER failed schedule verification"
    fi
    EFFECTIVE_CAT="$("$SYSTEMCTL_BIN" cat dhan-token-refresh.timer 2>/dev/null || true)"
    if [[ -n "$EFFECTIVE_CAT" ]]; then
        LAST_ONCAL="$(printf '%s\n' "$EFFECTIVE_CAT" | grep -E '^[[:space:]]*OnCalendar=' | tail -n 1 || true)"
        if [[ -n "$LAST_ONCAL" && "$LAST_ONCAL" != *"*-*-* 17:00:00 Asia/Kolkata"* ]]; then
            rollback_timer_on_failure "Effective systemd unit (systemctl cat) overrides OnCalendar away from 17:00:00 Asia/Kolkata"
        fi
    fi
    EFFECTIVE_SHOW="$("$SYSTEMCTL_BIN" show -p TimersCalendar dhan-token-refresh.timer 2>/dev/null || true)"
    if [[ -n "$EFFECTIVE_SHOW" ]]; then
        if [[ "$EFFECTIVE_SHOW" != *"17:00:00"* ]]; then
            rollback_timer_on_failure "Effective TimersCalendar (systemctl show) does not match 17:00:00 Asia/Kolkata: $EFFECTIVE_SHOW"
        fi
    fi
    if command -v systemd-analyze >/dev/null 2>&1; then
        systemd-analyze calendar "*-*-* 17:00:00 Asia/Kolkata" 2>/dev/null || true
    fi
    "$SYSTEMCTL_BIN" list-timers dhan-token-refresh.timer --no-pager 2>/dev/null || true
    printf '\nSUCCESS: dhan-token-refresh.timer upgraded to 17:00 IST every calendar day.\n'
    printf 'Preserved untouched: trading_bot.service, auditbot-web.service, .env, ~/.dhan_token, data/*\n'
    printf 'To rollback if needed: bash deploy/install_vm_services.sh --rollback-timer\n'
    exit 0
fi

[[ -x "$VISUDO_BIN" ]] || fail "/usr/sbin/visudo required; install sudo."
# Existing units and drop-ins are NEVER rewritten, reloaded, enabled, started,
# or stopped by the default --install mode. Only missing units on a fresh VM are provisioned.
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
# Fresh WebUI provisioning is explicit: never boot an unconfigured login screen.
if [[ "$WEB_EXISTED" == 0 && ! -f "$REPO_DIR/data/webui_password.json" ]]; then
    fail "Provision one-password WebUI first: $REPO_DIR/venv/bin/python3 -m src.webui_password provision"
fi
cat >"$TMP/auditbot-web.service" <<EOF
[Unit]
Description=UDITBOT Control Center (localhost-only; password protected)
After=network.target

[Service]
Type=simple
User=$BOT_USER
Group=$BOT_USER
WorkingDirectory=$REPO_DIR
Environment=PYTHONUNBUFFERED=1
ExecStart=$REPO_DIR/venv/bin/python3 -m src.control_center_entry --host 127.0.0.1 --port 80
Restart=on-failure
RestartSec=5
UMask=0077
AmbientCapabilities=CAP_NET_BIND_SERVICE
NoNewPrivileges=false
# Do not add CapabilityBoundingSet=CAP_NET_BIND_SERVICE: blocks sudo capabilities.

[Install]
WantedBy=multi-user.target
EOF
cat >"$TMP/auditbot-web-restart" <<EOF
$BOT_USER ALL=(root) NOPASSWD: /usr/bin/systemctl restart trading_bot.service
$BOT_USER ALL=(root) NOPASSWD: /usr/bin/systemctl --no-block start trading_bot.service
$BOT_USER ALL=(root) NOPASSWD: /usr/bin/systemctl stop trading_bot.service
EOF
sudo /usr/sbin/visudo -cf "$TMP/auditbot-web-restart" >/dev/null || fail "Sudoers syntax invalid."
# Existing sudoers policies elsewhere must be independently audited.
for name in trading_bot.service auditbot-web.service dhan-token-refresh.service dhan-token-refresh.timer; do
    [[ -e "/etc/systemd/system/$name" ]] && { echo "PRESERVE $name"; continue; }
    sudo install -o root -g root -m 0644 "$TMP/$name" "/etc/systemd/system/$name"
    echo "INSTALLED $name"
done
# Do not install or replace sudoers for an existing WebUI automatically unless AUDITBOT_UPGRADE_SUDOERS=1 is explicitly passed.
if [[ "$WEB_EXISTED" == 0 ]]; then
    sudo install -o root -g root -m 0440 "$TMP/auditbot-web-restart" "$SUDOERS"
    echo "INSTALLED exact-command WebUI sudoers"
elif [[ "${AUDITBOT_UPGRADE_SUDOERS:-0}" == "1" ]]; then
    sudo install -o root -g root -m 0440 "$TMP/auditbot-web-restart" "$SUDOERS"
    echo "UPGRADED exact-command WebUI sudoers"
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
sudo "$SYSTEMCTL_BIN" daemon-reload
if [[ "$WEB_EXISTED" == 0 ]]; then
    sudo "$SYSTEMCTL_BIN" enable auditbot-web.service
    sudo "$SYSTEMCTL_BIN" start auditbot-web.service
fi
if [[ "$TIMER_EXISTED" == 0 ]]; then
    sudo "$SYSTEMCTL_BIN" enable --now dhan-token-refresh.timer
fi
if [[ "$TRADING_EXISTED" == 0 && "$BOOT" == 1 ]]; then
    # Explicit opt-in enables NEXT boot; never starts the trading service now.
    sudo "$SYSTEMCTL_BIN" enable trading_bot.service
fi
printf '\nProvisioning finished. Existing service units were not overwritten.\n'
printf 'Trading bot not started or restarted by this installer.\n'
printf 'Fresh WebUI Control Center defaults to http://127.0.0.1:80 (private SSH tunnel or local TLS proxy required).\n'
