from __future__ import annotations

import asyncio
from html import escape
from pathlib import Path
import secrets
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

from .service import MeshcoreRuntimeService


def create_app(service: MeshcoreRuntimeService, log_file_path: Path) -> FastAPI:
    app = FastAPI(title="meshcore-bot", version="0.1.0")
    sessions: dict[str, str] = {}

    def is_authenticated(request: Request) -> bool:
        token = request.cookies.get("meshcore_admin_session")
        return bool(token and token in sessions)

    async def parse_form(request: Request) -> dict[str, str]:
        body = (await request.body()).decode("utf-8", errors="replace")
        parsed = parse_qs(body, keep_blank_values=True)
        return {key: values[-1] if values else "" for key, values in parsed.items()}

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({
            "status": "ok",
            "service": service.config.service.name,
            "started_at": service.started_at.isoformat(),
            "database": service.database.snapshot_overview(),
        })

    @app.get("/api/state")
    async def api_state() -> JSONResponse:
        return JSONResponse(service.snapshot())

    @app.get("/")
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/admin", status_code=303)

    @app.get("/admin", response_class=HTMLResponse)
    async def admin(request: Request) -> str:
        if not is_authenticated(request):
            return _render_login()
        snapshot = service.snapshot()
        bot = snapshot["bot"]
        endpoints = snapshot["endpoints"]
        messages = snapshot["messages"][:20]
        packets = snapshot["packets"][:20]
        return f"""
<!doctype html>
<html><head><meta charset=\"utf-8\"><title>MeshCore Admin</title>
<style>
body {{ font-family: ui-sans-serif, system-ui, sans-serif; background:#f4f1ea; color:#1d2a2f; margin:0; }}
.wrap {{ max-width:1100px; margin:0 auto; padding:24px; }}
.top {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:18px; }}
.card {{ background:#fffdf8; border:1px solid #d9d1c3; border-radius:14px; padding:16px; margin-bottom:16px; box-shadow:0 8px 24px rgba(0,0,0,0.05); }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th, td {{ border-bottom:1px solid #ece6da; padding:8px; text-align:left; vertical-align:top; }}
.mono {{ font-family: ui-monospace, monospace; font-size:13px; }}
a {{ color:#005f73; text-decoration:none; }}
button {{ border:0; background:#8d5524; color:white; padding:10px 14px; border-radius:10px; cursor:pointer; }}
</style></head><body>
<div class=\"wrap\">
  <div class=\"top\"><div><h1>MeshCore Admin</h1><p>Runtime over RS232Bridge TCP with live endpoint state and stored packets.</p></div>
  <form method=\"post\" action=\"/admin/logout\"><button type=\"submit\">Log Out</button></form></div>
  <div class=\"card\"><h2>Bot</h2><div><strong>Name:</strong> {escape(str(bot.get('name') or ''))}</div><div><strong>Reply prefix:</strong> <span class=\"mono\">{escape(str(bot.get('reply_prefix') or ''))}</span></div><div><strong>Command prefix:</strong> <span class=\"mono\">{escape(str(bot.get('command_prefix') or ''))}</span></div><div><strong>Logs:</strong> <a href=\"/logs\">open live logs</a></div></div>
  <div class=\"card\"><h2>Endpoints</h2><table><thead><tr><th>Name</th><th>Raw</th><th>Connected</th><th>Last seen</th><th>Last error</th></tr></thead><tbody>
  {''.join(f"<tr><td>{escape(name)}</td><td class='mono'>{escape(str(item.get('raw_host') or ''))}:{escape(str(item.get('raw_port') or ''))}</td><td>{'yes' if item.get('connected') else 'no'}</td><td class='mono'>{escape(str(item.get('last_seen_at') or ''))}</td><td>{escape(str(item.get('last_error') or ''))}</td></tr>" for name, item in endpoints.items())}
  </tbody></table></div>
  <div class=\"card\"><h2>Recent Messages</h2><table><thead><tr><th>When</th><th>Endpoint</th><th>Channel</th><th>Sender</th><th>Content</th></tr></thead><tbody>
  {''.join(f"<tr><td class='mono'>{escape(str(item.get('received_at') or ''))}</td><td>{escape(str(item.get('endpoint_name') or ''))}</td><td>{escape(str(item.get('channel_name') or ''))}</td><td>{escape(str(item.get('sender') or ''))}</td><td>{escape(str(item.get('content') or ''))}</td></tr>" for item in messages)}
  </tbody></table></div>
  <div class=\"card\"><h2>Recent Packets</h2><table><thead><tr><th>When</th><th>Endpoint</th><th>Type</th><th>Route</th><th>Payload</th></tr></thead><tbody>
  {''.join(f"<tr><td class='mono'>{escape(str(item.get('observed_at') or ''))}</td><td>{escape(str(item.get('endpoint_name') or ''))}</td><td>{escape(str(item.get('packet_type') or ''))}</td><td>{escape(str(item.get('route_name') or ''))}</td><td class='mono'>{escape(str(item.get('payload_hex') or ''))[:80]}</td></tr>" for item in packets)}
  </tbody></table></div>
</div></body></html>
"""

    @app.post("/admin/login")
    async def admin_login(request: Request) -> RedirectResponse:
        form = await parse_form(request)
        password = form.get("password", "")
        if not service.database.verify_admin_password("admin", password):
            return HTMLResponse(_render_login(error="Invalid admin password."), status_code=401)
        token = secrets.token_urlsafe(24)
        sessions[token] = "admin"
        service.database.touch_admin_login("admin")
        service.database.record_admin_audit(
            actor_username="admin",
            action="login",
            target_type="admin",
            remote_addr=request.client.host if request.client else None,
        )
        response = RedirectResponse(url="/admin", status_code=303)
        response.set_cookie("meshcore_admin_session", token, httponly=True, samesite="lax")
        return response

    @app.post("/admin/logout")
    async def admin_logout(request: Request) -> RedirectResponse:
        token = request.cookies.get("meshcore_admin_session")
        if token:
            sessions.pop(token, None)
        response = RedirectResponse(url="/admin", status_code=303)
        response.delete_cookie("meshcore_admin_session")
        return response

    @app.get("/logs", response_class=HTMLResponse)
    async def logs_page(request: Request) -> HTMLResponse:
        if not is_authenticated(request):
            return RedirectResponse(url="/admin", status_code=303)
        return HTMLResponse(
            """
<!doctype html>
<html><head><meta charset=\"utf-8\"><title>System Logs</title>
<style>body{margin:0;background:#111;color:#d7e3e7;font-family:ui-monospace,monospace}header{padding:14px 18px;background:#1f2a30;position:sticky;top:0}pre{margin:0;padding:18px;white-space:pre-wrap;word-break:break-word}</style>
</head><body><header><a href=\"/admin\" style=\"color:#9ad1d4;text-decoration:none\">back</a> <strong style=\"margin-left:12px\">Live Logs</strong></header><pre id=\"log\"></pre>
<script>
const log = document.getElementById('log');
const source = new EventSource('/logs/stream');
source.onmessage = (event) => { log.textContent += event.data + "\\n"; window.scrollTo(0, document.body.scrollHeight); };
</script></body></html>
            """
        )

    @app.get("/logs/stream")
    async def logs_stream(request: Request) -> StreamingResponse:
        if not is_authenticated(request):
            return StreamingResponse(_single_event("unauthorized"), status_code=401, media_type="text/event-stream")
        return StreamingResponse(_tail_log_stream(log_file_path), media_type="text/event-stream")

    return app


def _render_login(error: str | None = None) -> str:
    error_html = f"<p style='color:#9f2a2a'>{escape(error)}</p>" if error else ""
    return f"""
<!doctype html>
<html><head><meta charset=\"utf-8\"><title>MeshCore Admin Login</title>
<style>body{{font-family:ui-sans-serif,system-ui,sans-serif;background:#f4f1ea;color:#1d2a2f}}.box{{max-width:420px;margin:80px auto;background:#fffdf8;border:1px solid #d9d1c3;border-radius:14px;padding:24px}}input{{width:100%;padding:12px;border:1px solid #c6bcad;border-radius:10px;margin:8px 0 14px}}button{{width:100%;padding:12px;border:0;border-radius:10px;background:#8d5524;color:#fff}}</style>
</head><body><div class=\"box\"><h1>MeshCore Admin</h1><p>Default password is changeme.</p>{error_html}<form method=\"post\" action=\"/admin/login\"><input type=\"password\" name=\"password\" placeholder=\"Admin password\"><button type=\"submit\">Log In</button></form></div></body></html>
"""


async def _tail_log_stream(log_file_path: Path):
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    log_file_path.touch(exist_ok=True)
    with log_file_path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()[-200:]
        for line in lines:
            yield f"data: {line.rstrip()}\n\n"
        while True:
            line = handle.readline()
            if line:
                yield f"data: {line.rstrip()}\n\n"
            else:
                await asyncio.sleep(1)


async def _single_event(message: str):
    yield f"data: {message}\n\n"