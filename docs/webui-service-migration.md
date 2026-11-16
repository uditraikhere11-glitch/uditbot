# GitHub-managed WebUI service migration (existing VMs)

## Why this exists

The existing deployment installer provisions the authenticated Control Center
for **new** VMs. It deliberately preserves an already installed
`auditbot-web.service` and its systemd drop-ins. Consequently, a production
VM may still run `python -m src.setup_app --host 0.0.0.0 --port 80`,
which bypasses the outer login middleware even after the Git checkout is updated.
An unauthenticated HTTP `200` from `/` is not a successful protection check.

This GitHub-tracked migration closes that **existing-VM service drift** without
changing any trading logic.

## Prerequisites

- Prefer a flat/after-market deployment window and verify live Dhan orders and
  protections separately before *any* trading-service operations.
- Confirm GitHub and VM source are synced and backups are retained.
- Run as the non-root VM operator with the Python virtualenv.
- Provision your password locally if missing:
  `./venv/bin/python3 -m src.webui_password provision`.
- Ensure `data` is private (0700), and the password verifier is private (0600).
  Never commit passwords, .env, SQLite journals or sessions to GitHub.
- The currently installed systemd unit must run as the same **non-root operator**
  who provisioned the password; the migration checks `systemctl show -p User`.
  If the service is configured as root or another user, correct its ownership
  and credentials in an independent, reviewed change.
- Before restarting a WebUI that has an outstanding deferred Owner Stop, review
  `data/service_control_state.json` and any pending configuration state.
  The migration does not cancel, modify or supersede those obligations.

## Safe default (recommended)

~~~bash
cd ~/uditbot
bash deploy/upgrade_webui_control_center.sh --plan
bash deploy/upgrade_webui_control_center.sh --apply
~~~

The default binds the password-protected `src.control_center_entry` to
**127.0.0.1:80 only**. Use an SSH tunnel or encrypted reverse proxy.

## Explicit compatibility mode (public port 80)

For the owner-directed choice to preserve an existing public HTTP link:

~~~bash
cd ~/uditbot
bash deploy/upgrade_webui_control_center.sh --plan --public-http --accept-plaintext-risk
bash deploy/upgrade_webui_control_center.sh --apply --public-http --accept-plaintext-risk
~~~

**Important:** HTTP does not encrypt passwords or browser login cookies, even
when a password form exists. Network intermediaries could intercept credentials.
Private localhost or HTTPS is preferable. Public HTTP requires both explicit
flags and is never the default.

## Migration guarantees

The migration backs up the existing service unit and all drop-ins outside the
repository at `~/uditbot-backups/webui-service-migrations/`. It preserves
the old unit and existing `10-original-access.conf`, then adds a higher-priority,
GitHub-managed `99-github-authenticated-control.conf` drop-in.

It checks the effective systemd ExecStart and service identity, and requires
these results from unauthenticated loopback requests before reporting success:

- `GET /`: HTTP 303 redirect to password login
- `GET /setup`: HTTP 303 redirect to password login
- `GET /login`: HTTP 200 (password form itself)

Only `auditbot-web.service` is directly restarted by this migration, and
only if it was already active. Existing trading and token timer unit files,
emergency STOP, .env, SQLite journal and broker order records are not edited.
A stopped WebUI stays stopped. **The WebUI itself runs an existing background
coordinator on startup**. If a deferred Owner Stop or transition already
exists, this coordinator may resume the previously requested service-control
operation, including read-only Dhan safety checks. Check the existing control
state and broker obligations before restarting the WebUI; the migration warns
when it detects a pending Owner Stop.

On verification failure, the migration removes its managed drop-in and leaves
the WebUI **stopped**. It never silently brings the old unauthenticated public
Setup application back online. Running `--apply` again with the same security
mode verifies the existing deployment without restarting services; a different
mode requires deliberate rollback and review.

## Rollback

The successful deployment prints a private backup directory. If required:

~~~bash
bash deploy/upgrade_webui_control_center.sh --rollback /path/from/printed/backup
~~~

Rollback stops WebUI, removes only the marked managed drop-in, and reloads
systemd. The legacy WebUI is deliberately **not** restarted because it may be
unauthenticated. All original unit files remain available in the backup.

## Moving to a new VM

For a fresh installation, the existing `deploy/install_vm_services.sh --install`
already provisions the password-protected Control Center on localhost. A new
VM needs its own local password provisioning; authentication secrets belong
on the VM, never in the Git repository. Use this migration only when an older
WebUI systemd unit already exists.

## Offline regression

~~~bash
AUDITBOT_TEST_ISOLATION=1 python -m unittest -v tests.test_webui_service_migration
~~~

The tests mock sudo, systemd, HTTP, and filesystem service paths, exercise
idempotent repeat runs and mismatched service identities, and make no Dhan,
Telegram, or production VM calls.
