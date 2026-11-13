"""Password-only Control Center with optional explicitly insecure public HTTP.

The original Setup application and trading service are not rewritten. Public
plaintext HTTP is opt-in, exposes passwords/sessions, and is NOT secure access.
"""
from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from src.control_center_backend import register_control_center_routes
from src.control_center_ui import render_control_center
from src.setup_app import (
    SETUP_CSRF_TOKEN,
    _on_webui_startup,
    app as legacy_setup_app,
    get_setup_page,
)
from src import webui_password as auth

app = FastAPI(title="UDITBOT — Control Center")
register_control_center_routes(app, SETUP_CSRF_TOKEN)


@app.middleware("http")
async def require_operator_login(request: Request, call_next):
    """Single outer gate for the Control Center and ALL mounted Setup routes."""
    if not auth.permitted_transport(request):
        return PlainTextResponse(
            "Private access required (public HTTP is disabled unless explicitly opted in)", status_code=403,
            headers={"Cache-Control": "no-store"},
        )
    if not auth.password_configured():
        return PlainTextResponse(
            "Control Center password not provisioned. Run: python -m src.webui_password provision",
            status_code=503, headers={"Cache-Control": "no-store"},
        )
    if request.url.path not in ("/login", "/logout") and not auth.valid_session(
        request.cookies.get(auth.cookie_name(), "")
    ):
        return RedirectResponse("/login", status_code=303) if request.method in ("GET", "HEAD") else PlainTextResponse(
            "Authentication required", status_code=401,
        )
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


_LOGIN_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>UDITBOT — Unlock</title><style>
:root{font-family:system-ui,-apple-system,sans-serif;color-scheme:dark}
body{background:#0b0f19;color:#f1f5f9;min-height:100svh;display:grid;place-items:center;margin:0;padding:18px}
main{width:min(100%,390px);background:#111726;border:1px solid #26334d;border-radius:20px;padding:30px}
h1{text-align:center;margin:0 0 10px}p{text-align:center;color:#94a3b8}
label{display:block;margin:22px 0 8px}input,button{font:inherit;width:100%;min-height:50px;box-sizing:border-box;border-radius:10px}
input{background:#0b0f19;color:#fff;border:1px solid #64748b;padding:12px}
button{background:#047857;color:#fff;border:0;margin-top:18px;font-weight:700;cursor:pointer}
.notice{color:#fda4af;min-height:1.3em;font-size:14px}</style></head><body>
<main><h1>🔒 UDITBOT</h1><p>One password for Control Center and Setup</p>
<p class="notice">__MESSAGE__</p><p class="notice">__RISK_WARNING__</p><form method="post" action="/login">
<label for="password">WebUI password</label><input id="password" type="password" name="password" autocomplete="current-password" required autofocus>
<button type="submit">Unlock Control Center</button></form></main></body></html>"""


def _login_html(message: str = "") -> str:
    # Only caller-owned constant strings are accepted; never reflect user input.
    warning = (
        "WARNING: Public HTTP does not encrypt your password or login session. "
        "Network intermediaries may intercept either."
        if auth.public_http_explicitly_enabled() else ""
    )
    return _LOGIN_PAGE.replace("__MESSAGE__", message).replace("__RISK_WARNING__", warning)


@app.get("/login", response_class=HTMLResponse)
def login_page():
    return HTMLResponse(_login_html())


@app.post("/login")
async def login_submit(request: Request):
    ip = request.client.host if request.client else "unknown"
    if auth.blocked(ip):
        return HTMLResponse(_login_html("Too many attempts. Try later."), status_code=429)
    if "application/x-www-form-urlencoded" not in request.headers.get("content-type", ""):
        return PlainTextResponse("Form encoded password required", status_code=415)
    if len(await request.body()) > 2048:
        return PlainTextResponse("Form too large", status_code=413)
    form = await request.form()
    password = form.get("password")
    if not isinstance(password, str) or not auth.authenticate(password, ip):
        return HTMLResponse(_login_html("Invalid password"), status_code=401)
    token = auth.start_session()
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(auth.cookie_name(), token, max_age=8*3600, httponly=True,
                        secure=auth.cookie_secure(request), samesite="strict", path="/")
    return response


@app.post("/logout")
async def logout(request: Request):
    form = await request.form()
    if form.get("csrf_token") != SETUP_CSRF_TOKEN:
        return PlainTextResponse("Invalid CSRF token", status_code=403)
    auth.end_session(request.cookies.get(auth.cookie_name(), ""))
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(auth.cookie_name(), path="/")
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
    if args.host not in ("127.0.0.1", "::1", "localhost"):
        if args.host != "0.0.0.0" or not auth.public_http_explicitly_enabled():
            parser.error(
                "Public binding requires UDITBOT_WEBUI_ALLOW_INSECURE_PUBLIC_HTTP="
                "I_ACCEPT_PLAINTEXT_PASSWORD_AND_SESSION_RISK"
            )
        print("WARNING: PUBLIC HTTP MODE ENABLED: PASSWORDS AND SESSION COOKIES ARE UNENCRYPTED", flush=True)
    uvicorn.run(app, host=args.host, port=args.port, proxy_headers=False)