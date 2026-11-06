# AuditBot — brand-new bot, zero existing data (install release v1)

This is a **separate install branch**, derived from the integrated, tested GitHub main through PR #70.
It is NOT a clone of Aman/Udit credentials, journal, orders, Telegram sessions, or VM services.
It retains the working Equity/F&O AMO, BRAIN, WebUI, Dhan protections, notification and risk code.

**One copy/paste command starts installation; a literal 60-second completion is not guaranteed.**
OS package download, Python dependencies, Telegram OTP and broker identity checks take time.
Do not compromise correctness or skip verification to meet a time target.

## New Debian 12/13 or Ubuntu VM only

Use a new VM and a normal Linux user with sudo access. It must have no AuditBot systemd
services, no existing ~/uditbot checkout, no .env, and no ~/.dhan_token.
Never run this installer on a LIVE AmanBot/UditBot VM.

```bash
sudo apt-get update && sudo apt-get install -y git sudo && \
git clone --branch release/auditbot-zero-data-installer-v1 --single-branch \
  https://github.com/uditraikhere11-glitch/uditbot.git "$HOME/uditbot" && \
bash "$HOME/uditbot/deploy/fresh_install.sh"
```

The installer refuses existing production services or runtime data. It:
1. Installs Python, venv and requirements in ~/uditbot/venv.
2. Creates empty, private data/log directories.
3. Interactively prompts for **new** Telegram API ID/hash, explicitly authorized channel IDs,
   Dhan client ID and a token or PIN+TOTP. Optionally provide your own portable StringSession,
   Telegram Bot API token, Telegram log/test groups and phone.
4. Saves .env and token privately (0600), never commits them, and never copies historical data.
5. Installs systemd trading/WebUI/daily token-refresh services using the existing service installer.
   **Trading stays stopped and in TEST mode**. Equity and F&O AMO start disabled.
   The token-refresh timer is installed but intentionally **disabled** until the new account is verified,
   preventing a surprise Dhan TOTP login that could invalidate credentials used by another bot.
   After verifying it is a dedicated account, opt in with `sudo systemctl enable --now dhan-token-refresh.timer`.
6. Stages the independent orphan Forever OCO cleanup timer **disabled**; it must be
   enabled only after reviewing bot-owned orders, holdings and read-only preview results.
7. Runs Python syntax and VM installation regression tests.

## Finishing credentials and Telegram login

The installer captures credentials locally. Telegram generally also needs one-time
phone OTP or a valid user-owned StringSession:

```bash
cd ~/uditbot
venv/bin/python -m src.tg_login
```

If you supplied PIN+TOTP instead of an access token, issue a token only after
confirming the same Dhan account is not currently trading on another VM.
Refreshing a Dhan token may affect other sessions:

```bash
cd ~/uditbot
./bot login
```

Check account binding, channel allowlist, budgets, positions, open orders, AMO and
protection policies in TEST mode before enabling LIVE trading:

```bash
cd ~/uditbot
./bot status
./bot check-dhan
systemctl is-active auditbot-web.service
systemctl is-active trading_bot.service # should be inactive until you start it
```

## Secure WebUI

The installer configures localhost-only WebUI on port **80**, not public HTTP.
On your own laptop, open a tunnel to the new VM:

```bash
ssh -L 8080:127.0.0.1:80 NEW_USER@NEW_VM_IP
```

Then browse **http://127.0.0.1:8080/setup**.

If on Google Cloud and using IAP, use `gcloud compute ssh ... --tunnel-through-iap -- -N -L 8080:127.0.0.1:80` instead.
Do not expose credential-entry WebUI directly over public HTTP without authentication/TLS.

Only after TEST acceptance, manually start the service with `./bot start`.
Switching LIVE is a separate explicitly approved decision; there is no default LIVE entry.
For reboot persistence, explicitly enable after testing with `sudo systemctl enable trading_bot.service`.

## What is and is not replicated

**Included:** source code, parser/AMO/protection logic, setup dashboard, tested deployment
scripts, service definitions, optional OCO timer sources, Python requirements and tests.

**Not copied:** any .env, Dhan/Telegram credential, Telegram session or OTP, order journal,
transaction history, account-specific emergency STOP state, private logs or service
customizations from a prior VM. GCP firewall, IAM, static IP and SSH user configuration are
outside Git and must be configured on the new VM.

The included data/holidays.json is an exchange holiday reference calendar, **not a trading journal**.
Source-level CI and installer contract tests do not prove a live broker integration works.
Perform a real new-VM smoke test before describing it as 100% production verified.

## Security and backups

New credentials are typed into the new VM, never into GitHub. The .env and token are
file-mode 0600 and Git-ignored. Use encrypted off-VM recovery snapshots separately.
Publishing a clean branch does not erase historical leaks from other branches:
if a real credential was ever committed anywhere, rotate it; removing a log file
from this branch alone does not revoke a stolen token.

## Read-only self-check

```bash
bash ~/uditbot/deploy/fresh_install.sh --preflight
```
