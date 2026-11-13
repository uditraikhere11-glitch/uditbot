# UDITBOT Control Center — one-password WebUI (draft deployment)

Source baseline: `6ac2d99a3746a7cc5ae833fed839c95659539d37` (2026-11-13). Revalidate HEAD before applying.

## What changes

- The Control Center is the main WebUI (`/`); existing Setup stays at `/setup`.
- One password-only login (`/login`) protects all Control Center and mounted Setup endpoints, including control APIs and service actions. Logout is a POST form and sessions expire after 8 hours or a WebUI process restart.
- The password verifier uses PBKDF2-HMAC-SHA256 with a private per-user random salt and 600,000 iterations; the plaintext password is never saved.
- Sessions stay in memory (one Uvicorn worker), with unpredictable, `HttpOnly`, `SameSite=Strict` cookies. HTTPS browser cookies use `Secure`.
- New installations use `src.control_center_entry` on loopback only. Existing installed service units are **not overwritten** by the installer.
- No trading service, Dhan order, journal, budget, parser, or AMO execution logic is changed.

## Ingress and explicitly insecure public HTTP opt-in

**Public HTTP does not encrypt passwords or session cookies.** A person who can intercept the network connection can take over the administrative WebUI even after a strong password is configured. The recommended default is loopback or encrypted private ingress. The owner has explicitly requested an alternative direct public-IP HTTP mode; it is available only after an operator sets a long, exact acknowledgement string on the WebUI process. It is not a secure mode and must never be presented as such. `X-Forwarded-*`, fabricated IAP headers and the Host header do not bypass the default private access checks.

The recommended access choices:

1. Existing HTTP port 80, **through SSH local forwarding or a verified encrypted private VPN/tunnel**, with app bound to `127.0.0.1`. For an iPhone, arrange an SSH-forwarding client, or use a separately verified private TLS ingress.
2. Keep a bookmarked public-facing URL with **HTTPS** using a managed TLS front door / reverse proxy that connects locally to the app. Proxy access and TLS setup are a deployment prerequisite, not supplied by this code.

The application itself can keep serving localhost HTTP in either case. This PR does not provision a hostname, cloud IAP, firewall, or certificate.

### Operator-requested direct public-IP HTTP (insecure, explicit opt-in)

Only after independent PR approval, on the WebUI process **alone**, set:

```ini
[Service]
Environment=UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP=I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK
ExecStart=
ExecStart=/home/<actual-VM-username>/uditbot/venv/bin/python3 -m src.control_center_entry --host 0.0.0.0 --port 80
```

This is an example systemd drop-in, **not** a command to paste unchanged. Replace the exact Python path after verifying it on that VM, back up all existing systemd WebUI unit/drop-ins, and do not overwrite unrelated overrides. This setting must be present in the **WebUI process environment**, not just in `.env`. The default installer deliberately remains loopback-bound, so enabling public HTTP always requires an explicit operator action. Do not set this flag on the trading service.

After the WebUI-only service restart, the existing `http://<VM-public-IP>/login` URL can serve the one-password form on port 80. The form visibly warns that the entered password and session are unencrypted. The setting disables the browser's `Secure` cookie attribute only for explicit HTTP mode (necessary for login to work over public HTTP). `HttpOnly`, `SameSite=Strict`, the password verifier, login throttling, and the single shared authorization boundary remain active, but **none prevents network interception or session theft**. Disable by removing the environment override and returning to a loopback or encrypted entrypoint.

Never restart `trading_bot.service` to change WebUI access. Do not interpret this opt-in as a production security endorsement.

## Local provisioning (no trade service impact)

On a checked-out staging instance, as the service user:

```bash
cd ~/uditbot
source venv/bin/activate
python -m src.webui_password provision
```

The interactive command prompts twice. Keep `data/` private (0700). Ensure `data/webui_password.json` is mode 0600. Each VM must provision its own password; never copy private auth files across accounts. On rotation, active sessions become invalid.

## Isolated staging test (no production port or services)

```bash
cd ~/uditbot
source venv/bin/activate
python -m src.control_center_entry --host 127.0.0.1 --port 18080
```

From an SSH tunnel or on the VM, visit `http://127.0.0.1:18080/login` and confirm `/`, `/setup`, login/logout, and read-only health. Do **not** invoke Start, Stop, Restart or AMO toggles while staging against a LIVE account.

## Existing VM: reversible WebUI-only startup migration (after independent approval)

The fresh-install template alone **does not** switch an existing VM. On UditBot,
the old WebUI was started by an existing `auditbot-web.service.d/override.conf`,
with `src.setup_app --host 0.0.0.0 --port 80`. This PR now supplies
`deploy/migrate_existing_webui_to_control_center.sh`, which adds a separate
later-ordered systemd drop-in and verifies the resulting effective ExecStart.

**Do not execute this before the PR has been independently audited and installed
on the intended VM.** Do not copy source between UditBot and AmanBot blindly.

The operator-requested direct public HTTP mode is deliberately insecure:
interception of the password or session permits takeover of the WebUI.
For an encrypted alternative, use the loopback/tunnel configuration described above.

First, from the **actual WebUI service user's** shell on the **intended VM**:

```bash
cd ~/uditbot
venv/bin/python3 -m src.webui_password provision
sudo bash deploy/migrate_existing_webui_to_control_center.sh --check-public-http
```

The provision command prompts privately for a strong password (entered twice).
It saves a salted, private verifier, not a plaintext password. If provisioning or
preflight fails, **stop** and inspect the error; do not bypass permissions.
The read-only preflight discovers the live WebUI WorkingDirectory, service User,
effective existing ExecStart and password readiness. It does not alter systemd
or restart services.

After the **separate approval to activate** the unencrypted public-HTTP mode:

```bash
cd ~/uditbot
sudo env UDITBOT_CONFIRM_PUBLIC_HTTP_RISK=I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK \
  bash deploy/migrate_existing_webui_to_control_center.sh --apply-public-http
```

This backs up the existing unit/drop-ins to a root-private timestamped directory,
writes **only** the migration-owned drop-in, performs `systemctl daemon-reload`,
validates `ExecStart` and the WebUI-only HTTP environment opt-in, restarts
**only** `auditbot-web.service`, and locally checks that `/login` serves the
password form and unauthenticated `/`, `/setup` and `/control/status`
redirect to `/login`. If activation fails, the migration attempts to restore
the old WebUI automatically. Do not claim remote access is secure: port 80 is
plaintext HTTP.

Expected URL after successful cutover:
`http://<existing-VM-IP>/login`. One login opens Control Center `/` and
the original Setup at `/setup`. The migration does not verify authenticated
Setup writes or execute trading controls; those remain separate review gates.

**Immediate rollback to previous WebUI startup** (does not change the password
verifier and does not restart trading):

```bash
cd ~/uditbot
sudo bash deploy/migrate_existing_webui_to_control_center.sh --rollback
```

Rollback removes the uniquely marked PR78 drop-in, reloads systemd, and restarts
only `auditbot-web.service`; all pre-existing base units and override files
remain intact. Review `systemctl cat auditbot-web.service` and the WebUI journal
if another override or an unrelated service failure prevents either startup.

No automatic VM deployment, trading service restart, order journal edit,
`.env` replacement, broker call or token modification is performed by this PR.

## Known limitation

The one-password access layer is scoped to the WebUI. The existing Setup/Control Center business logic, including native F&O entry-only AMO limitations and backend-level safety gates, is unchanged. Production approval still requires independent review of these existing controls and network ingress; this PR alone is not a license to enable unprotected F&O trading.
