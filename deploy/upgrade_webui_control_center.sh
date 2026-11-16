#!/usr/bin/env bash
# Explicit existing-VM WebUI migration; never modifies or starts trading_bot.service.
# Source of truth: GitHub. Default is private loopback, public HTTP is acknowledged opt-in.
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
MODE="${1:---plan}"
shift "$(( $# > 0 ? 1 : 0 ))"
HOST="127.0.0.1"
PUBLIC=0
ACK=0
ROLLBACK_DIR=""

fail() { printf 'STOP: %s\n' "$*" >&2; exit 1; }
usage() {
    printf 'Usage: bash deploy/upgrade_webui_control_center.sh --plan [--public-http --accept-plaintext-risk]\n'
    printf '       bash deploy/upgrade_webui_control_center.sh --apply [--public-http --accept-plaintext-risk]\n'
    printf '       bash deploy/upgrade_webui_control_center.sh --rollback BACKUP_DIRECTORY\n'
}
case "$MODE" in --plan|--apply|--rollback) ;; *) usage; exit 2 ;; esac
if [[ "$MODE" == --rollback ]]; then
    [[ $# == 1 ]] || { usage; exit 2; }
    ROLLBACK_DIR="$1"
else
    while (( $# )); do
        case "$1" in
            --public-http) PUBLIC=1 ;;
            --accept-plaintext-risk) ACK=1 ;;
            *) usage; exit 2 ;;
        esac
        shift
    done
    if [[ "$PUBLIC" == 1 && "$ACK" != 1 ]] || [[ "$PUBLIC" == 0 && "$ACK" == 1 ]]; then
        fail 'Public HTTP requires BOTH --public-http and --accept-plaintext-risk.'
    fi
    [[ "$PUBLIC" != 1 ]] || HOST="0.0.0.0"
fi

[[ "$(id -u)" != 0 ]] || fail 'Run as the non-root VM operator (sudo is invoked only for systemd changes).'
[[ "$ROOT" =~ ^/[a-zA-Z0-9_./-]+$ ]] || fail 'Unsafe repository directory characters.'
[[ -x "$ROOT/venv/bin/python3" ]] || fail 'Python virtualenv missing.'
[[ -f "$ROOT/src/control_center_entry.py" && -f "$ROOT/src/webui_password.py" ]] || fail 'Authenticated Control Center source missing.'
[[ -f "$ROOT/.env" && -d "$ROOT/data" ]] || fail 'Expected existing .env and data directory.'

UNIT=/etc/systemd/system/auditbot-web.service
DROPINS=/etc/systemd/system/auditbot-web.service.d
OVERRIDE="$DROPINS/99-github-authenticated-control.conf"
SYSTEMCTL=/usr/bin/systemctl
SUDO=sudo
CURL=curl
BACKUP_ROOT="$HOME/uditbot-backups/webui-service-migrations"

# Test-only substitutes NEVER apply to production. All unit paths must be isolated.
if [[ "${AUDITBOT_TEST_ISOLATION:-}" == 1 ]]; then
    [[ -n "${AUDITBOT_SYSTEMD_DIR:-}" && -d "${AUDITBOT_SYSTEMD_DIR:-}" ]] || fail 'Isolated systemd dir required for test mode.'
    UNIT="$AUDITBOT_SYSTEMD_DIR/auditbot-web.service"
    DROPINS="$AUDITBOT_SYSTEMD_DIR/auditbot-web.service.d"
    OVERRIDE="$DROPINS/99-github-authenticated-control.conf"
    SYSTEMCTL="${AUDITBOT_SYSTEMCTL_BIN:?Isolated systemctl required}"
    SUDO="${AUDITBOT_SUDO_BIN:?Isolated sudo required}"
    CURL="${AUDITBOT_CURL_BIN:?Isolated curl required}"
    BACKUP_ROOT="$ROOT/test-backups"
fi

[[ -f "$UNIT" && ! -L "$UNIT" ]] || fail "Expected existing system service not found: $UNIT"
[[ ! -L "$DROPINS" && ! -L "$OVERRIDE" ]] || fail 'Refusing symlinked systemd override path.'

if [[ "$MODE" == --plan ]]; then
    printf 'GitHub-managed WebUI upgrade plan (READ-ONLY)\n'
    printf 'Repository: %s\nTarget: %s\nCurrent service: %s\n' "$ROOT" "$HOST:80" "$UNIT"
    printf 'Current state: %s\n' "$("$SYSTEMCTL" is-active auditbot-web.service 2>/dev/null || true)"
    printf 'Existing authenticated override: %s\n' "$(test -e "$OVERRIDE" && echo PRESENT || echo absent)"
    printf 'Password verifier: '
    if "$ROOT/venv/bin/python3" -c 'from src.webui_password import password_configured; raise SystemExit(0 if password_configured() else 1)' >/dev/null 2>&1; then
        echo configured
    else
        echo missing
    fi
    if [[ "$PUBLIC" == 1 ]]; then
        printf 'WARNING: Public plaintext HTTP exposes the WebUI password and session cookie to interception.\n'
    else
        printf 'Private localhost only (SSH tunnel or encrypted reverse proxy required).\n'
    fi
    printf 'On apply: backup service and drop-ins, replace ExecStart via one managed override, restart WEBUI only if already active, verify / /setup /login, stop WebUI if verification fails.\n'
    printf 'Trading bot, token timer, .env, SQLite and runtime data remain untouched.\n'
    exit 0
fi

command -v "$SUDO" >/dev/null || fail 'sudo unavailable.'
[[ -x "$SYSTEMCTL" ]] || fail 'systemctl unavailable.'

if [[ "$MODE" == --rollback ]]; then
    [[ -d "$ROLLBACK_DIR" && ! -L "$ROLLBACK_DIR" && -f "$ROLLBACK_DIR/UDITBOT_WEBUI_BACKUP_V1" ]] || fail 'Invalid migration backup directory.'
    [[ -e "$OVERRIDE" ]] || fail 'Managed override absent: refusing to touch other service files.'
    grep -Fq '# UDITBOT_GITHUB_WEBUI_MIGRATION_V1' "$OVERRIDE" || fail 'Managed override marker missing.'
    "$SUDO" "$SYSTEMCTL" stop auditbot-web.service || fail 'Could not stop WebUI for rollback.'
    "$SUDO" rm -- "$OVERRIDE"
    "$SUDO" "$SYSTEMCTL" daemon-reload
    echo 'Rollback removed GitHub-managed override; WebUI intentionally STOPPED.'
    echo 'Legacy service can be unauthenticated; it was NOT restarted.'
    echo "Original service configuration was preserved in: $ROLLBACK_DIR"
    exit 0
fi

[[ ! -e "$OVERRIDE" ]] || fail 'Managed override already exists. Inspect instead of overwriting.'
"$ROOT/venv/bin/python3" -c 'from src.webui_password import password_configured; raise SystemExit(0 if password_configured() else 1)' \
    || fail 'Provision a private password first: ./venv/bin/python3 -m src.webui_password provision'
[[ -d "$ROOT/data" && ! -L "$ROOT/data" ]] || fail 'Auth data directory must be real, not a symlink.'
[[ $(stat -c %a "$ROOT/data") == 700 ]] || fail 'data directory must have 0700 permissions before password-based WebUI starts.'

STATE="$("$SYSTEMCTL" is-active auditbot-web.service 2>/dev/null || true)"
[[ "$STATE" == active || "$STATE" == inactive || "$STATE" == failed ]] || fail "Uncertain WebUI service state: $STATE"

mkdir -p "$BACKUP_ROOT"
chmod 0700 "$BACKUP_ROOT"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="$BACKUP_ROOT/$STAMP-$$"
mkdir -m 0700 "$BACKUP"
cp -p "$UNIT" "$BACKUP/base.service"
if [[ -d "$DROPINS" ]]; then
    cp -a "$DROPINS" "$BACKUP/original-dropins"
fi
"$SYSTEMCTL" cat auditbot-web.service > "$BACKUP/effective-unit.before.txt"
printf 'UDITBOT_WEBUI_BACKUP_V1\n' > "$BACKUP/UDITBOT_WEBUI_BACKUP_V1"
printf 'original_service_state=%s\nchosen_host=%s\n' "$STATE" "$HOST" > "$BACKUP/manifest.txt"

TMP="$(mktemp)"
STARTED=0
COMPLETE=0
cleanup() {
    rc=$?
    rm -f -- "$TMP"
    if [[ "$STARTED" == 1 && "$COMPLETE" == 0 ]]; then
        echo 'Verification failed; stopping WebUI and removing managed override (fail closed).' >&2
        "$SUDO" "$SYSTEMCTL" stop auditbot-web.service >/dev/null 2>&1 || true
        if [[ -f "$OVERRIDE" ]] && grep -Fq '# UDITBOT_GITHUB_WEBUI_MIGRATION_V1' "$OVERRIDE"; then
            "$SUDO" rm -f -- "$OVERRIDE" || true
        fi
        "$SUDO" "$SYSTEMCTL" daemon-reload >/dev/null 2>&1 || true
        echo "Previous systemd configuration remains intact; legacy WebUI NOT restarted. Backup: $BACKUP" >&2
    fi
    return "$rc"
}
trap cleanup EXIT

{
    printf '# UDITBOT_GITHUB_WEBUI_MIGRATION_V1\n'
    printf '[Service]\n'
    printf 'WorkingDirectory=%s\n' "$ROOT"
    printf 'Environment=PYTHONUNBUFFERED=1\n'
    if [[ "$PUBLIC" == 1 ]]; then
        printf 'Environment=UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK\n'
    else
        printf 'Environment=UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=\n'
    fi
    printf 'ExecStart=\n'
    printf 'ExecStart=%s/venv/bin/python3 -m src.control_center_entry --host %s --port 80\n' "$ROOT" "$HOST"
} > "$TMP"

STARTED=1
"$SUDO" "$SYSTEMCTL" stop auditbot-web.service
"$SUDO" mkdir -p -- "$DROPINS"
"$SUDO" install -m 0644 "$TMP" "$OVERRIDE"
"$SUDO" "$SYSTEMCTL" daemon-reload

EFFECTIVE="$("$SYSTEMCTL" show auditbot-web.service -p ExecStart --value)"
[[ "$EFFECTIVE" == *"-m src.control_center_entry --host $HOST --port 80"* && "$EFFECTIVE" != *"-m src.setup_app"* ]] \
    || fail 'Effective systemd ExecStart does NOT match authenticated Control Center.'

if [[ "$STATE" == active ]]; then
    "$SUDO" "$SYSTEMCTL" restart auditbot-web.service
    [[ "$("$SYSTEMCTL" is-active auditbot-web.service)" == active ]] || fail 'WebUI did not become active.'
    check_http() {
        local url="$1" expected="$2" got
        got="$("$CURL" --noproxy '*' --max-time 10 -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1${url}")" || return 1
        [[ "$got" == "$expected" ]] || { echo "Unexpected HTTP status for ${url}: $got (expected $expected)" >&2; return 1; }
    }
    check_http / 303 || fail 'Unauthenticated root did not redirect to login.'
    check_http /setup 303 || fail 'Unauthenticated Setup did not redirect to login.'
    check_http /login 200 || fail 'Login page is unavailable.'
    printf 'Authenticated WebUI verified (root 303, setup 303, login 200).\n'
else
    printf 'WebUI was %s before migration and remains STOPPED; no automatic start.\n' "$STATE"
fi
COMPLETE=1
printf 'SUCCESS: Authenticated WebUI service configuration installed.\nBackup: %s\n' "$BACKUP"
printf 'UNCHANGED: trading_bot.service, token timer, .env, journal, Telegram session, emergency STOP, order state.\n'
if [[ "$PUBLIC" == 1 ]]; then
    printf 'WARNING: Public HTTP is unencrypted. Migrate to HTTPS/private access.\n'
fi
