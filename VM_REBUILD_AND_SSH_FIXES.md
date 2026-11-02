# AuditBot V14 — Complete VM rebuild & SSH-fix persistence patch

Baseline: GitHub `uditraikhere11-glitch/uditbot` `main` at `50e83d1009a3e27bf543c01b16c4f2c74b906745` (31 Oct 2026). Source-only change; no trading logic, mode, WebUI layout, database schema, or broker order logic changes.

## Why this patch exists

Manual SSH commands that edit `/etc/systemd/system`, `/etc/sudoers.d`, or runtime secrets **do not survive deleting/recreating the VM**. GitHub source files do survive, but `.env`, `~/.dhan_token`, Telegram sessions, SQLite journal, and STOP/watchers are intentionally not in Git.

**Two separate recovery layers:**
1. **Reproducible software/config:** commit this patch to GitHub; run `bash deploy/setup_vm.sh` on a clean Debian VM.
2. **Mutable private state:** make regular encrypted `deploy/backup_runtime.sh` archives, and independently copy each encrypted archive OFF the VM before loss. GitHub does not replace this backup.

A source patch cannot recover files that were destroyed before being backed up. GCP VM metadata, disks, service accounts, firewall rules, IP addresses, IAM and SSH keys are also outside GitHub; preserve separately as GCP configuration/infrastructure-as-code or documented settings.

## SSH fix inventory — what was verified vs not verified

| SSH work / behaviour | Evidence available | What this patch does |
|---|---|---|
| WebUI `CapabilityBoundingSet=CAP_NET_BIND_SERVICE` blocked sudo restart | Owner reported fix applied and Save/Apply recovered; original problem and drop-in filename recorded, but **effective current VM unit not fetched** | Fresh WebUI unit keeps `AmbientCapabilities=CAP_NET_BIND_SERVICE`, omits restrictive bounding set, and makes `NoNewPrivileges=false` for current sudo-based restart path |
| Port 80 + non-root WebUI service | HTTP 200 on `127.0.0.1:80`; earlier externally bound `0.0.0.0:80` | Fresh install binds `127.0.0.1:80` (SSH tunnel / authenticated TLS reverse proxy for remote access). Does not overwrite existing unit |
| WebUI config apply, recheck running/stopped, rollback, no token-to-.env | Existing GitHub `src/engine/config_manager.py`, `src/setup_app.py` implements these behaviours | Leaves working application unchanged; only enables exact service restart permission on a fresh VM |
| Broad passwordless `/bin/systemctl, /usr/bin/systemctl` | Previously suggested/applied according to user logs; **actual active sudoers not independently checked** | Never installs broad rule. Grants only `/usr/bin/systemctl restart trading_bot.service` to new-VM bot owner; existing broad rules require manual audit/removal |
| Trading bot & WebUI services running (0 restarts) | Batch 05-C observed both active; trading bot disabled at boot, web enabled | Fresh installer creates both with process-failure restart; WebUI enabled/started. Bot **not started or boot-enabled by default** to respect intentional stops. Opt into next-boot enable using `AUDITBOT_ENABLE_TRADING_BOOT=1` |
| Token-refresh service/timer | 05-B verified installed, active, enabled; next Mon 2 Nov 2026 08:00:06 IST, zero recorded successful refreshes | Fresh install creates username/path-correct timer and oneshot service using `./bot daily_health_check` at Mon–Fri 08:00 IST, with no automatic missed-run catch-up by default. Override opt-in `AUDITBOT_TOKEN_CATCHUP=1` |
| `10-no-catchup.conf` timer drop-in | `systemctl cat` failed permission denied; separate unprivileged existence check said absent; **contents/effective policy not verified** | Fresh default `Persistent=false`, intended to avoid immediate forced TOTP login on a rebuilt VM. Existing timer/drop-ins never overwritten. Record any different local policy before rebuild |
| Shared Dhan token `~/.dhan_token`, `0600` | Batch 05 verified token present, private and authenticated by GET `/fundlimit` HTTP 200 | Installed service runs as same owner, keeps file private. Actual token content must come from encrypted backup/new TOTP login, NOT GitHub |
| Dhan TOTP credentials / master IDs / market toggles | 05-B verified configured and tests passed; values confidential | `.env` encrypted backup and optional wizard restore; never commit secrets |
| `data/trade_journal.db`, WAL/outbox/claims/protection and listener state | 05-A passed SQLite quick_check, 7 processed notifications; repo `.gitignore` already excludes mutable data | SQLite online backup via Python backup API; encrypted archive includes data runtime files (except generated master/holiday file); restore never starts services and forces TEST |
| Telegram session / listener singleton | Prior SSH hardening `tg_userbot.session 0600`; active listener observed | Encrypted backup copies runtime session; source `run.py` retains existing singleton lock. No new listener created by patch |
| Git tracked runtime secrets | `.gitignore` already excludes `.env`, DB, sessions; history not necessarily clean | No changes to ignore rules; `audit_vm_state.sh` reports Git cleanliness but cannot inspect historical leaked secrets |
| Startup broker reconciliation and safety checks | Batch 05 gate-open service log event found; no persisted reconciliation event; app code exists | No behavioural changes; manual read-only post-rebuild validation required |
| GCP firewall, VM static IP, port exposure | Port 80 bound on all interfaces previously; GCP firewall not inspected | Fresh WebUI localhost only; external GCP firewall/IAM remain separate rebuild tasks |

## Install on a NEW VM (not on your active, working VM)

1. Create non-root service account and install OS prerequisites (Python venv/pip, sudo, systemd, GPG for backups, Git, system time sync). **Usernames/paths are discovered dynamically**.
2. Clone the GitHub repo **after this PR is merged** and run `bash deploy/setup_vm.sh` as that Unix owner. This configures `.env` using the existing wizard and installs only missing units.
3. Newly provisioned WebUI: `127.0.0.1:80`, `AmbientCapabilities=CAP_NET_BIND_SERVICE`, no restrictive bounding set, no privileged remote exposure. To use SSH tunnel: `ssh -L 8080:127.0.0.1:80 USER@VM` then browse `http://127.0.0.1:8080`.
4. Trading service is **not started** and is **not enabled for boot** unless explicitly requested. To opt into *future reboot activation only*, use `AUDITBOT_ENABLE_TRADING_BOOT=1 bash deploy/install_vm_services.sh --install` **before first install** (does not start bot now). On an already installed trading service, decide deliberately with `sudo systemctl enable trading_bot.service`.
5. `dhan-token-refresh.timer` is enabled; the first success requires observation at the next scheduled 08:00 IST Mon–Fri run. It performs real TOTP authentication and can update the token, so do **not** call it manually for a read-only test.
6. If you want systemd missed-trigger catch-up after VM downtime, set `AUDITBOT_TOKEN_CATCHUP=1` during first provisioning. Default 0 deliberately prevents an unplanned token refresh just after reinstall. Configuring an existing unit is not automatic.
7. The WebUI lacks verified login enforcement in current V14 source; keep it on localhost, ideally behind authenticated TLS before any remote access. Do not toggle LIVE to test the VM.

### On an existing running VM

Run **read-only** `bash deploy/install_vm_services.sh --plan` and `bash deploy/audit_vm_state.sh`; preserve output outside the VM. **Do not rerun the full setup script against production without a backup**, because package installation/tests and service installation are for fresh provisioning.

Existing `/etc/systemd/system/*.service`, timer files, drop-ins and sudoers are preserved by this installer. That means any existing unrestricted sudoers or old bounding override are *not silently fixed*: audit those separately under administrator control before replacing them. New unit configuration is never forced over your working service.

## Encrypted runtime backups — essential to survive VM deletion

Run on the existing VM from repository root:

```bash
bash deploy/backup_runtime.sh "$HOME/auditbot-recovery-$(date +%Y%m%d).tar.gz.gpg"
```

- Requires `gpg` and an interactively entered strong passphrase. The plaintext temporary staging directory is mode 0700, deleted on normal exit (deletion is not secure disk erasure).
- Includes `.env`, all direct regular files from configured `DATA_DIR` other than regenerable `holidays.json`, and a consistent live SQLite backup using Python's backup API instead of copying `.db-wal`/`.db-shm` live. Also copies configured Dhan token into the archived `runtime/home/.dhan_token`.
- Does **not** guarantee transactional consistency between independent JSON/session files and the SQLite snapshot while the listener is processing messages. A stopped bot or GCP disk snapshot is preferable for a final recovery point.
- **Immediately download/copy the `.gpg` file to storage OUTSIDE that VM.** Store the passphrase separately. A local file on a deleted disk cannot be recovered.
- Do not upload decrypted backups or secrets to GitHub. GitHub will contain only source/configuration scripts.
- Consider GCP snapshots/backups and separately preserve GCP firewall/VPC/IAM/static-IP configuration.

## Restore on a clean replacement VM

1. Clone patched repo and provision Python dependencies. `setup_vm.sh` may have created a provisional `.env` and started the localhost WebUI. **Deliberately stop WebUI and trading service**, verify inactive, and move any wizard-generated `.env`, token, or `data` runtime contents to a private quarantine path first; do not overwrite them in place.
2. Copy the encrypted `.gpg` file from OFF-VM storage to the new VM.
3. Run `bash deploy/restore_runtime.sh /path/to/recovery.tar.gz.gpg` as the non-root owner. It **refuses** if WebUI/trading service is running or if existing `.env`, token or non-holiday runtime files could be overwritten.
4. Restore verifies archive entry paths/types, restores private permissions, forces `TRADING_MODE=TEST`, and **does not start any service**. Check custom absolute `DATA_DIR`, `JOURNAL_DB_PATH` and `DHAN_TOKEN_FILE` paths from the old VM, and adjust to the new VM deliberately before startup.
5. Only after validating database `PRAGMA quick_check`, Telegram credentials/session, stop state, funds auth, order intents, protection watcher and broker account should an operator start the bot in TEST. Restore must never be treated as LIVE approval.

## Validation commands (read-only)

```bash
bash -n deploy/*.sh
python3 -m unittest tests.test_vm_rebuild_deployment -v
bash deploy/install_vm_services.sh --plan
bash deploy/audit_vm_state.sh
systemctl show auditbot-web.service -p ActiveState -p AmbientCapabilities -p CapabilityBoundingSet -p NoNewPrivileges -p DropInPaths
systemctl show trading_bot.service -p UnitFileState -p Restart -p ActiveState
systemctl list-timers dhan-token-refresh.timer --no-pager
```

For actual new-VM acceptance, separately prove: localhost-only bind, WebUI Save/Apply of a harmless parameter **while TEST**, original stopped-state preservation, timer's first successful TOTP run, no secrets logged, reconciliation fail-closed with unresolved state, and no LIVE broker orders. Static CI tests alone are not deployment proof.

## Known issues deliberately out of scope

1. WebUI full authentication, reverse-proxy TLS and GCP firewall review (network binding is now secure by default, but app login still needs a separate patch).
2. Yahoo `DARVAS_HTTP_SUCCESS` allowing quotes >2 seconds for pricing; cannot approve LIVE until corrected.
3. BRAIN F&O strike/expiry/parser issues awaiting separately reviewed BRAIN upgrade.
4. TEST-mode synthetic `TRADED` journal status despite zero fills.
5. First successful daily token refresh is unobserved as of 31 October 2026.

**Do not merge source patch as a substitute for encrypted, off-VM runtime backups.**
