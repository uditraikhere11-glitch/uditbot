#!/usr/bin/env bash
# PR #78 — explicitly approved WebUI-only migration for an existing UDITBOT VM.
# NEVER restarts trading_bot.service or modifies trading data/journals.
# The direct public HTTP option is unencrypted: passwords and sessions can be intercepted.
set -Eeuo pipefail
umask 077
export LC_ALL=C

readonly SERVICE='auditbot-web.service'
readonly SYSTEMCTL='/usr/bin/systemctl'
readonly UNIT_DIR='/etc/systemd/system/auditbot-web.service.d'
readonly OVERRIDE="$UNIT_DIR/zzzz-uditbot-control-center-pr78.conf"
readonly MARKER='# managed-by: uditbot-pr78-webui-only-migration'
readonly ACK='I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK'
readonly BACKUP_ROOT='/var/backups/uditbot-webui-migration'
readonly LOCK_PATH='/run/lock/uditbot-webui-migration.lock'

say() { printf '[UDITBOT WEBUI] %s\n' "$*"; }
fail() { printf '[UDITBOT WEBUI] ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'USAGE'
Usage (after independent PR approval and installation on this VM):
  sudo bash deploy/migrate_existing_webui_to_control_center.sh --check-public-http
  sudo bash deploy/migrate_existing_webui_to_control_center.sh --apply-public-http
  sudo bash deploy/migrate_existing_webui_to_control_center.sh --rollback

Before --apply-public-http, run as the normal VM user:
  cd ~/uditbot && venv/bin/python3 -m src.webui_password provision

WARNING: --apply-public-http exposes the login password and session cookie to
network interception on direct HTTP. Use a private encrypted tunnel or HTTPS
front end for confidential access. Changes ONLY auditbot-web.service.
USAGE
}

[[ "$#" == 1 ]] || { usage; exit 2; }
case "$1" in
  --check-public-http|--apply-public-http|--rollback) MODE="$1" ;;
  --help|-h) usage; exit 0 ;;
  *) usage; exit 2 ;;
esac
[[ "$EUID" == 0 ]] || fail 'Use sudo for systemd WebUI diagnostics and changes.'
[[ -x "$SYSTEMCTL" ]] || fail 'systemctl not found on this VM.'
command -v flock >/dev/null || fail 'flock required.'
exec 9>"$LOCK_PATH"
flock -n 9 || fail 'Another WebUI migration is in progress.'

# User-specific WebUI service path is discovered rather than hard-coded.
resolve_app() {
  local path user
  path="$($SYSTEMCTL show -P WorkingDirectory "$SERVICE")"
  user="$($SYSTEMCTL show -P User "$SERVICE")"
  [[ -n "$path" && "$path" != '/' && -d "$path" ]] || fail 'Cannot verify WebUI WorkingDirectory.'
  [[ "$path" != *[[:space:]]* ]] || fail 'WebUI WorkingDirectory contains whitespace; audit manually.'
  [[ -n "$user" && "$user" != root ]] || fail 'WebUI service must have an explicit non-root User.'
  REPO="$(realpath -e -- "$path")"
  WEBUSER="$user"
  PY="$REPO/venv/bin/python3"
  [[ -x "$PY" && -f "$REPO/src/control_center_entry.py" && -f "$REPO/src/webui_password.py" ]] ||
    fail 'New Control Center source and Python venv must be installed first.'
  [[ -f "$REPO/src/setup_app.py" ]] || fail 'Original Setup source must remain present.'
  # Do not rely on whatever happens to be in a different VM's checkout.
  [[ "$(stat -c %U -- "$REPO")" == "$WEBUSER" ]] || fail 'Checkout owner differs from WebUI service User.'
  # Do not use a root-owned password verifier. The service user must read it.
  ( cd "$REPO" && /usr/sbin/runuser -u "$WEBUSER" -- "$PY" -c \
    'from src.webui_password import password_configured; raise SystemExit(0 if password_configured() else 3)' ) ||
    fail "Password not provisioned/readable for $WEBUSER. Run as that user: cd $REPO && venv/bin/python3 -m src.webui_password provision"
}

# A restore simply removes our additional drop-in. Previous base unit and
# all pre-existing overrides are left untouched, so no blind sed replacement.
rollback_our_override() {
  [[ -f "$OVERRIDE" ]] || fail 'No PR78 migration override is installed.'
  grep -Fxq -- "$MARKER" "$OVERRIDE" || fail 'Override exists but is not owned by this migration. Manual audit required.'
  rm -- "$OVERRIDE"
  "$SYSTEMCTL" daemon-reload
  "$SYSTEMCTL" restart "$SERVICE"
  "$SYSTEMCTL" is-active --quiet "$SERVICE" || fail 'Previous WebUI did not restart; inspect its journal.'
  say 'Prior WebUI service command restored; only auditbot-web.service restarted.'
}

if [[ "$MODE" == --rollback ]]; then
  rollback_our_override
  exit 0
fi

# Prevent a cutover when the WebUI was intentionally stopped.
"$SYSTEMCTL" is-active --quiet "$SERVICE" || fail 'WebUI service must already be running.'
[[ ! -e "$OVERRIDE" && ! -L "$OVERRIDE" ]] || fail 'Migration override already exists; use --rollback or audit manually.'
resolve_app

# Both commands must refer to the same service user and checkout.
PREVIOUS_START="$($SYSTEMCTL show -P ExecStart "$SERVICE")"
[[ "$PREVIOUS_START" == *"src.setup_app"* ]] ||
  fail 'Current WebUI does not start src.setup_app. Inspect systemctl cat manually first.'

say "WebUI service user: $WEBUSER"
say "Checkout: $REPO"
say 'Current WebUI starts legacy src.setup_app; trading service will not be modified.'
say 'Requested WebUI entrypoint: src.control_center_entry --host 0.0.0.0 --port 80'
say 'PUBLIC HTTP WARNING: password and session token are not encrypted.'
if [[ "$MODE" == --check-public-http ]]; then
  say 'PRE-FLIGHT PASS: no files changed or services restarted.'
  exit 0
fi

# An explicit second acknowledgement is required for actual direct-HTTP cutover.
[[ "${UDITBOT_CONFIRM_PUBLIC_HTTP_RISK:-}" == "$ACK" ]] ||
  fail "For --apply-public-http, export UDITBOT_CONFIRM_PUBLIC_HTTP_RISK=$ACK in this command's sudo environment."

mkdir -p -m 0700 -- "$BACKUP_ROOT"
BACKUP="$BACKUP_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -m 0700 -- "$BACKUP"
"$SYSTEMCTL" cat "$SERVICE" > "$BACKUP/effective-unit-before.txt"
"$SYSTEMCTL" show "$SERVICE" -p ExecStart -p Environment -p User -p WorkingDirectory -p DropInPaths > "$BACKUP/effective-settings-before.txt"
[[ ! -f /etc/systemd/system/auditbot-web.service ]] ||
  cp -p -- /etc/systemd/system/auditbot-web.service "$BACKUP/base-unit-before.service"
[[ ! -d "$UNIT_DIR" ]] || cp -a -- "$UNIT_DIR" "$BACKUP/pre-existing-dropins"
printf '%s\n' "$PREVIOUS_START" > "$BACKUP/previous-execstart.txt"
say "Restricted root-owned backup: $BACKUP"

mkdir -p -m 0755 -- "$UNIT_DIR"
TMP="$(mktemp "$UNIT_DIR/.pr78-migration.XXXXXXXX")"
trap 'rm -f -- "${TMP:-}"' EXIT
cat > "$TMP" <<EOF
$MARKER
# Removing only this drop-in restores the previous WebUI command.
# PUBLIC HTTP DOES NOT ENCRYPT PASSWORDS OR SESSION COOKIES.
[Service]
Environment=UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=$ACK
ExecStart=
ExecStart=$PY -m src.control_center_entry --host 0.0.0.0 --port 80
EOF
chmod 0644 "$TMP"
mv -n -- "$TMP" "$OVERRIDE"
TMP=''

# An error after adding this file restores the old WebUI automatically.
CUTOVER_STARTED=1
COMPLETED=0
on_exit() {
  local rc="$?"
  trap - EXIT
  if [[ "$CUTOVER_STARTED" == 1 && "$COMPLETED" != 1 ]]; then
    say 'Cutover did not verify; automatically restoring prior WebUI.' >&2
    if [[ -f "$OVERRIDE" ]] && grep -Fxq -- "$MARKER" "$OVERRIDE"; then
      rm -f -- "$OVERRIDE"
      "$SYSTEMCTL" daemon-reload || true
      "$SYSTEMCTL" restart "$SERVICE" || true
    fi
  fi
  exit "$rc"
}
trap on_exit EXIT

"$SYSTEMCTL" daemon-reload
NOW_START="$($SYSTEMCTL show -P ExecStart "$SERVICE")"
[[ "$NOW_START" == *"$PY -m src.control_center_entry --host 0.0.0.0 --port 80"* ]] ||
  fail 'Another systemd override has priority; refusing to restart.'
NOW_ENV="$($SYSTEMCTL show -P Environment "$SERVICE")"
[[ "$NOW_ENV" == *"UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=$ACK"* ]] ||
  fail 'Public HTTP acknowledgement is not in the WebUI service environment.'

"$SYSTEMCTL" restart "$SERVICE"
"$SYSTEMCTL" is-active --quiet "$SERVICE" || fail 'New WebUI service is not active.'

# Curl is local and read-only; it NEVER posts a password or invokes a trade action.
GET_LOGIN="$(curl -fsS --max-time 8 http://127.0.0.1:80/login)" || fail 'GET /login failed.'
[[ "$GET_LOGIN" == *'UDITBOT'* && "$GET_LOGIN" == *'Public HTTP does not encrypt'* ]] ||
  fail 'New password-only login page or public HTTP warning was not served.'
for route in / /setup /control/status; do
  headers="$(curl -sS --max-time 8 -D - -o /dev/null "http://127.0.0.1:80$route")" || fail "GET $route failed."
  [[ "$headers" == *' 303 '* && "${headers,,}" == *'location: /login'* ]] ||
    fail "GET $route was not protected by redirect to /login."
done

COMPLETED=1
say 'SUCCESS: New Control Center serves /login; Control Center, Setup and API require password.'
say 'Only auditbot-web.service was restarted. Existing direct HTTP URL remains on port 80.'
say 'To revoke immediately: sudo bash deploy/migrate_existing_webui_to_control_center.sh --rollback'
