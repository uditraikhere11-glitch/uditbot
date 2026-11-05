# UDITBOT Control Center — isolated, opt-in deployment

Base branch: `60331f98c6c01a3bc35d7307f5e28a9e149d887d`

## Architecture

- The original `src/setup_app.py`, `src/setup_wizard.py` and `tests/test_setup.py` remain **byte-for-byte identical** to GitHub main.
- The new `src/control_center_entry.py` is a separate ASGI entrypoint; when selected it serves Control Center at `/` and wraps the unchanged Setup GET renderer at `/setup`. All other Setup POST and status endpoints are delegated to the original FastAPI app.
- The existing persistent control coordinator startup handler is explicitly forwarded by the parent app. No additional background worker or broker worker is started.
- The facade uses mandatory HTTP Basic operator authentication for *all* endpoints, including mounted legacy service-control routes.
- The expected SHA-256 of a strong operator password must be supplied via `UDITBOT_WEBUI_PASSWORD_SHA256` in service environment or in existing private `.env`. Without a valid 64-char lowercase/uppercase hex digest, the new facade returns HTTP 503; it does not fall back to unauthenticated mode.
- The existing `src.setup_app` entrypoint remains available unchanged and the running unit is not switched by this PR. **Do not activate the new entrypoint until HTTPS or a secure SSH/IAP tunnel is established, a strong password is provisioned, and staged tests have passed**. HTTP Basic credentials must not be sent over publicly exposed unencrypted HTTP.
- The proposed Equity AMO switch uses the existing config coordinator with pending transition/stop safety checks. F&O AMO remains disabled; its broker API semantics and protection must be validated separately.

## Secure staged activation (manual, after merge approval)

1. Prepare a new 20+-character randomly generated operator password **offline**, not in repository or chat. Store only its SHA-256 hex digest as `UDITBOT_WEBUI_PASSWORD_SHA256` in the private 0600 `.env` or systemd environment. Use the `uditbot` Basic Auth username on login.
2. Preserve an image/snapshot of the working VM; record systemd unit ExecStart, active MainPID, active configuration version and pending order/protection state without displaying credentials.
3. Verify the WebUI is behind TLS or a trusted SSH/IAP tunnel to 127.0.0.1. Do not activate the password-protected facade on public HTTP.
4. On staging only, run `python -m src.control_center_entry --host 127.0.0.1 --port 8080`. Test login, `/setup` read, existing POST `/setup`, CSRF rejection, start/stop in TEST mode only, deferred stop and a mocked Equity AMO update. Never make live Dhan order calls during UI acceptance testing.
5. After staged approval, deliberately change the systemd WebUI service ExecStart from `python -m src.setup_app ...` to `python -m src.control_center_entry ...` preserving host, port, user, working directory, and all safety permissions. Restart **only the WebUI service**, not the trading bot; verify the new endpoint before marking success.
6. Rollback: return systemd WebUI ExecStart to `python -m src.setup_app` and restart only WebUI. The original Setup code remains intact; the trading bot was never changed by the feature itself.

## Open gates before merge or production activation

- Full CI must pass on GitHub PR after all changes; mock-only 39 local tests are not enough.
- Confirm the source/integration `/setup/service-control` delegation works in TestClient against real full source (including startup coordinator) and staged systemd.
- Simulate concurrent Setup Save and Equity AMO updates; reject/safely serialize overlaps, prove no superseded request is silently lost.
- Confirm authentication works through actual chosen TLS/reverse proxy and cannot be bypassed via legacy endpoints or alternate listener. Strong TLS/ingress authentication is a deployment requirement.
- F&O AMO must remain disabled until broker-protective route support is independently verified.