#!/usr/bin/env bash
# SAFE ONE-TIME INSTALLER FOR A NEW Debian/Ubuntu VM ONLY.
# Never runs or authorizes LIVE trading. Does not import existing data.
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
fail() { printf '[FRESH INSTALL REFUSED] %s\n' "$*" >&2; return 1; }

if [[ "$#" == 1 && "$1" == '--preflight' ]]; then
    bash -n "$ROOT/deploy/fresh_install.sh"
    echo 'Fresh installer syntax OK; no state changed.'
    exit 0
fi
[[ $# -eq 0 ]] || { echo 'Usage: bash deploy/fresh_install.sh [--preflight]' >&2; exit 2; }
[[ "$(id -u)" != 0 ]] || { fail 'Run as a normal sudo-enabled user, not root.'; exit 2; }
[[ "$ROOT" == "$HOME/uditbot" ]] || { fail 'Clone into a fresh ~/uditbot directory.'; exit 2; }
[[ -r /etc/os-release ]] || { fail 'OS release information missing'; exit 2; }
# shellcheck disable=SC1091
source /etc/os-release
[[ "$ID" == debian || "$ID" == ubuntu ]] || { fail 'Debian/Ubuntu only.'; exit 2; }
command -v systemctl >/dev/null || { fail 'systemd is required'; exit 2; }
for unit in trading_bot.service auditbot-web.service dhan-token-refresh.timer; do
    if [[ -e "/etc/systemd/system/$unit" ]] || systemctl cat "$unit" >/dev/null 2>&1; then
        fail "$unit already exists. This script must NEVER run on a working bot VM."
        exit 2
    fi
done
[[ ! -e "$ROOT/.env" && ! -L "$ROOT/.env" ]] || { fail 'Private .env already exists'; exit 2; }
[[ ! -e "$HOME/.dhan_token" && ! -L "$HOME/.dhan_token" ]] || { fail 'Dhan token already exists'; exit 2; }
if [[ -d "$ROOT/data" ]]; then
    for entry in "$ROOT/data"/* "$ROOT/data"/.[!.]* "$ROOT/data"/..?*; do
        [[ ! -e "$entry" && ! -L "$entry" ]] && continue
        [[ "$entry" == "$ROOT/data/holidays.json" ]] || {
            fail 'Existing data in runtime directory'; exit 2;
        }
    done
fi
command -v sudo >/dev/null || { fail 'sudo required'; exit 2; }
command -v git >/dev/null || { fail 'git required'; exit 2; }
[[ "$(git -C "$ROOT" branch --show-current)" == 'release/auditbot-zero-data-installer-v1' ]] || {
    fail 'Unexpected source branch; use the published fresh-install branch'; exit 2;
}
[[ -z "$(git -C "$ROOT" status --porcelain)" ]] || {
    fail 'Fresh checkout is modified; stop and inspect'; exit 2;
}

printf '\nAUDITBOT — NEW BOT FROM ZERO DATA\n'
printf 'ONLY NEW VMs; no LIVE execution or historical credentials.\n'
printf '1/6 — Installing operating-system dependencies...\n'
sudo -v
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    git python3 python3-venv python3-pip sudo ca-certificates curl gpg

printf '2/6 — Installing Python virtual environment...\n'
python3 -m venv "$ROOT/venv"
"$ROOT/venv/bin/python" -m pip install -r "$ROOT/requirements.txt"

printf '3/6 — Preparing private empty runtime directories...\n'
mkdir -p "$ROOT/data" "$ROOT/logs"
chmod 700 "$ROOT/data" "$ROOT/logs"

printf '4/6 — Configuring new credentials (TEST mode only)...\n'
"$ROOT/venv/bin/python" "$ROOT/deploy/fresh_config.py" --repo "$ROOT" --home "$HOME"

printf '5/6 — Installing bot services without starting trading...\n'
AUDITBOT_SKIP_TOKEN_TIMER_START=1 bash "$ROOT/deploy/install_vm_services.sh" --install

# Optional OCO cleanup can cancel broker instructions.
# Stage the units, but NEVER start or enable the timer here.
if [[ -d "$ROOT/deploy/user-services" ]]; then
    mkdir -p "$HOME/.config/systemd/user"
    for unit in uditbot-orphan-oco-cleanup.service uditbot-orphan-oco-cleanup.timer; do
        src="$ROOT/deploy/user-services/$unit"
        dst="$HOME/.config/systemd/user/$unit"
        if [[ -f "$src" && ! -e "$dst" ]]; then
            install -m 600 "$src" "$dst"
            echo "Staged DISABLED optional service: $unit"
        fi
    done
    systemctl --user daemon-reload 2>/dev/null || true
fi

printf '6/6 — Running safe installation checks...\n'
"$ROOT/venv/bin/python" -m py_compile \
    "$ROOT/run.py" "$ROOT/src/setup_app.py" "$ROOT/src/config.py"
cd "$ROOT"
AUDITBOT_TEST_ISOLATION=1 "$ROOT/venv/bin/python" -m unittest -q tests.test_vm_rebuild_deployment tests.test_fresh_install
[[ "$(grep '^TRADING_MODE=' "$ROOT/.env")" == 'TRADING_MODE=TEST' ]] || {
    fail 'TRADING_MODE must remain TEST'; exit 3;
}
if systemctl is-active --quiet trading_bot.service; then
    fail 'Trading unexpectedly active; stopping newly installed trading service.'
    sudo systemctl stop trading_bot.service || true
    exit 3
fi
sleep 1
if ! systemctl is-active --quiet auditbot-web.service; then
    fail 'WebUI did not start. Inspect: journalctl -u auditbot-web.service -n 50'
    exit 3
fi

printf '\n========================================================\n'
printf 'AUDITBOT SOFTWARE INSTALLED: NEW, PRIVATE, TEST MODE\n'
printf 'Trading service: NOT STARTED; LIVE orders not enabled.\n'
printf 'WebUI: 127.0.0.1:80 via SSH tunnel (localhost only).\n'
printf 'History: No imported orders, journal or Telegram session.\n'
printf 'Optional OCO cleanup timer: STAGED BUT DISABLED.\n'
printf '========================================================\n'
printf 'Next: cd ~/uditbot && venv/bin/python -m src.tg_login\n'
printf '      (Skip only if you provided your own valid StringSession.)\n'
printf 'If needed, deliberately create Dhan token: ./bot login\n'
printf '      (Only after checking another VM is not using the account.)\n'
printf 'Check: ./bot status && ./bot check-dhan\n'
printf 'Secure WebUI tunnel: ssh -L 8080:127.0.0.1:80 USER@NEW_VM_IP\n'
printf 'Then browse: http://127.0.0.1:8080/setup\n'
printf 'After account verification, start TEST only: ./bot start\n'
printf 'LIVE mode requires a separate explicit operator approval.\n'
