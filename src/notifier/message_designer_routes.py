"""Read/write-only presentation settings routes for the existing Setup WebUI.

No trading services or Dhan APIs are touched. Saving is atomic and does not
restart the trading process. Uses the Setup app's existing CSRF secret.
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.config import AppConfig
from src.notifier import message_designer as designer
from src.notifier.message_designer_ui import render_page


def register_routes(app: FastAPI, csrf_token: str) -> None:
    def _data_dir() -> str:
        return AppConfig.from_env().data_dir

    @app.get("/setup/telegram-designer", response_class=HTMLResponse)
    def telegram_designer_page():
        return HTMLResponse(render_page(csrf_token), headers={"Cache-Control": "no-store"})

    @app.get("/setup/telegram-designer/config")
    def telegram_designer_config():
        return JSONResponse({
            "settings": designer.load(_data_dir()),
            "defaults": designer.defaults(),
            "events": designer.EVENTS,
            "groups": designer.EVENT_GROUPS,
            "fields": designer.FIELDS,
            "emojis": designer.EMOJIS,
            "samples": designer.SAMPLE_VALUES,
        }, headers={"Cache-Control": "no-store"})

    @app.post("/setup/telegram-designer/preview")
    async def telegram_designer_preview(request: Request):
        if request.headers.get("X-CSRF-Token", "") != csrf_token:
            return JSONResponse({"error": "Missing or invalid CSRF token"}, status_code=403)
        if "application/json" not in request.headers.get("content-type", "").lower():
            return JSONResponse({"error": "JSON required"}, status_code=415)
        body = await request.body()
        if len(body) > 16000:
            return JSONResponse({"error": "Preview payload too large"}, status_code=413)
        try:
            import html as _html
            import json
            import re as _re
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("Invalid preview payload")
            event = str(payload.get("event") or "").strip()
            tpl = payload.get("template")
            rendered_html = designer.render_event_preview(event, tpl)
            plain = _html.unescape(_re.sub(r"</?b>", "", rendered_html))
        except (ValueError, TypeError) as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        return JSONResponse(
            {"event": event, "enabled": bool((tpl or {}).get("enabled")), "html": rendered_html, "plain": plain},
            headers={"Cache-Control": "no-store"},
        )

    @app.post("/setup/telegram-designer/save")
    async def telegram_designer_save(request: Request):
        if request.headers.get("X-CSRF-Token", "") != csrf_token:
            return JSONResponse({"error": "Missing or invalid CSRF token"}, status_code=403)
        if "application/json" not in request.headers.get("content-type", "").lower():
            return JSONResponse({"error": "JSON required"}, status_code=415)
        if int(request.headers.get("content-length", "0") or "0") > 32000:
            return JSONResponse({"error": "Settings too large"}, status_code=413)
        body = await request.body()
        if len(body) > 32000:
            return JSONResponse({"error": "Settings too large"}, status_code=413)
        try:
            import json
            payload = json.loads(body)
            settings = designer.save(payload, _data_dir())
        except (ValueError, TypeError) as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        except OSError:
            return JSONResponse({"error": "Could not persist notification settings"}, status_code=500)
        return JSONResponse({"saved": True, "settings": settings},
                            headers={"Cache-Control": "no-store"})
