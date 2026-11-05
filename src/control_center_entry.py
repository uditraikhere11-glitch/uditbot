"""Opt-in, authenticated Control Center facade around unchanged AuditBot Setup.

The legacy src.setup_app module and all existing trading/business logic remain
byte-for-byte unchanged. Deployment must explicitly switch the WebUI process to
``python -m src.control_center_entry`` after independent approval.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse

from src.config import parse_env_dict
from src.control_center_backend import register_control_center_routes
from src.control_center_ui import render_control_center
from src.setup_app import (
    SETUP_CSRF_TOKEN,
    _on_webui_startup,
    app as legacy_setup_app,
    get_setup_page,
)

app = FastAPI(title="UDITBOT — Control Center")
register_control_center_routes(app, SETUP_CSRF_TOKEN)


def _password_hash() -> str:
    """Require explicit operator provisioning; no insecure hard-coded defaults."""
    target = str(os.environ.get("UDITBOT_WEBUI_PASSWORD_SHA256") or "").strip().lower()
    if not target:
        try:
            target = str(parse_env_dict(".env").get("UDITBOT_WEBUI_PASSWORD_SHA256") or "").strip().lower()
        except (OSError, ValueError):
            return ""
    return target if len(target) == 64 and all(ch in "0123456789abcdef" for ch in target) else ""


def _valid_basic_auth(header: str, expected_digest: str) -> bool:
    if not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
        username, password = decoded.split(":", 1)
    except (binascii.Error, UnicodeError, ValueError):
        return False
    supplied = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return hmac.compare_digest(username, "uditbot") and hmac.compare_digest(supplied, expected_digest)


@app.middleware("http")
async def require_operator_login(request: Request, call_next):
    """Gate *all* routes, including mounted legacy POST /setup/service-control."""
    # Refuse direct non-TLS remote use. HTTPS reverse proxies and loopback
    # SSH/IAP tunnels remain supported; never offer a Basic challenge to a
    # remote plaintext HTTP client.
    client_host = request.client.host if request.client else ""
    if request.url.scheme != "https" and client_host not in ("127.0.0.1", "::1", "testclient"):
        return PlainTextResponse("Secure HTTPS or a loopback tunnel is required", status_code=426,
                                 headers={"Cache-Control": "no-store"})
    expected = _password_hash()
    if not expected:
        return PlainTextResponse(
            "UDITBOT Control Center is inactive: operator authentication has not been configured.",
            status_code=503, headers={"Cache-Control": "no-store"},
        )
    if not _valid_basic_auth(request.headers.get("authorization", ""), expected):
        return PlainTextResponse(
            "Operator authentication required", status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="UDITBOT Control Center"',
                     "Cache-Control": "no-store"},
        )
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.on_event("startup")
def start_legacy_coordinator_once() -> None:
    """Mounted sub-app lifespan is not invoked automatically by Starlette."""
    _on_webui_startup()


@app.get("/", response_class=HTMLResponse)
def get_control_center_page():
    return HTMLResponse(content=render_control_center(SETUP_CSRF_TOKEN))


@app.get("/setup", response_class=HTMLResponse)
def get_existing_setup_page():
    """Render the original Setup unchanged; add only a navigation link to response."""
    original = get_setup_page()
    content = original.body.decode("utf-8")
    anchor = '<body>\n  <div class="container">\n'
    back = ('<body>\n  <div class="container">\n'
            '    <a href="/" style="display:inline-block;margin-bottom:16px;'
            'color:#93c5fd;text-decoration:none;font-weight:600">'
            '\u2190 Back to Control Center</a>\n')
    if content.count(anchor) != 1:
        return PlainTextResponse("Legacy Setup HTML contract changed; refusing unsafe replacement", status_code=503)
    return HTMLResponse(content=content.replace(anchor, back, 1), status_code=original.status_code)


# Keep legacy /setup POST, /setup/submit-async, /setup/service-control,
# /setup/change-status, etc. delegated byte-for-byte, no reimplementation.
app.mount("/", legacy_setup_app, name="existing_setup")


if __name__ == "__main__":
    import argparse
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=80)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)