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
        async def root() -> HTMLResponse:
                return HTMLResponse(_render_public_map_page())

        @app.get("/admin", response_class=HTMLResponse)
        async def admin(request: Request):
                if not is_authenticated(request):
                        return HTMLResponse(_render_login())
                snapshot = service.snapshot()
                bot = snapshot["bot"]
                endpoints = snapshot["endpoints"]
                messages = snapshot["messages"][:20]
                packets = snapshot["packets"][:20]
                return HTMLResponse(
                        f"""
<!doctype html>
<html><head><meta charset="utf-8"><title>MeshCore Admin</title>
<style>
body {{ font-family: ui-sans-serif, system-ui, sans-serif; background:#f4f1ea; color:#1d2a2f; margin:0; }}
.wrap {{ max-width:1100px; margin:0 auto; padding:24px; }}
.top {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:18px; gap:16px; }}
.card {{ background:#fffdf8; border:1px solid #d9d1c3; border-radius:14px; padding:16px; margin-bottom:16px; box-shadow:0 8px 24px rgba(0,0,0,0.05); }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th, td {{ border-bottom:1px solid #ece6da; padding:8px; text-align:left; vertical-align:top; }}
.mono {{ font-family: ui-monospace, monospace; font-size:13px; }}
a {{ color:#005f73; text-decoration:none; }}
button {{ border:0; background:#8d5524; color:white; padding:10px 14px; border-radius:10px; cursor:pointer; }}
</style></head><body>
<div class="wrap">
    <div class="top"><div><h1>MeshCore Admin</h1><p>Runtime over RS232Bridge TCP with live endpoint state and stored packets.</p></div>
    <form method="post" action="/admin/logout"><button type="submit">Log Out</button></form></div>
    <div class="card"><h2>Bot</h2><div><strong>Name:</strong> {escape(str(bot.get('name') or ''))}</div><div><strong>Reply prefix:</strong> <span class="mono">{escape(str(bot.get('reply_prefix') or ''))}</span></div><div><strong>Command prefix:</strong> <span class="mono">{escape(str(bot.get('command_prefix') or ''))}</span></div><div><strong>Public map:</strong> <a href="/">open runtime map</a></div><div><strong>Logs:</strong> <a href="/admin/logs">open live logs</a></div></div>
    <div class="card"><h2>Endpoints</h2><table><thead><tr><th>Name</th><th>Raw</th><th>Connected</th><th>Last seen</th><th>Last error</th></tr></thead><tbody>
    {''.join(f"<tr><td>{escape(name)}</td><td class='mono'>{escape(str(item.get('raw_host') or ''))}:{escape(str(item.get('raw_port') or ''))}</td><td>{'yes' if item.get('connected') else 'no'}</td><td class='mono'>{escape(str(item.get('last_seen_at') or ''))}</td><td>{escape(str(item.get('last_error') or ''))}</td></tr>" for name, item in endpoints.items())}
    </tbody></table></div>
    <div class="card"><h2>Recent Messages</h2><table><thead><tr><th>When</th><th>Endpoint</th><th>Channel</th><th>Sender</th><th>Content</th></tr></thead><tbody>
    {''.join(f"<tr><td class='mono'>{escape(str(item.get('received_at') or ''))}</td><td>{escape(str(item.get('endpoint_name') or ''))}</td><td>{escape(str(item.get('channel_name') or ''))}</td><td>{escape(str(item.get('sender') or ''))}</td><td>{escape(str(item.get('content') or ''))}</td></tr>" for item in messages)}
    </tbody></table></div>
    <div class="card"><h2>Recent Packets</h2><table><thead><tr><th>When</th><th>Endpoint</th><th>Type</th><th>Route</th><th>Payload</th></tr></thead><tbody>
    {''.join(f"<tr><td class='mono'>{escape(str(item.get('observed_at') or ''))}</td><td>{escape(str(item.get('endpoint_name') or ''))}</td><td>{escape(str(item.get('packet_type') or ''))}</td><td>{escape(str(item.get('route_name') or ''))}</td><td class='mono'>{escape(str(item.get('payload_hex') or ''))[:80]}</td></tr>" for item in packets)}
    </tbody></table></div>
</div></body></html>
"""
                )

        @app.post("/admin/login")
        async def admin_login(request: Request):
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

        @app.get("/logs")
        async def logs_redirect() -> RedirectResponse:
                return RedirectResponse(url="/admin/logs", status_code=303)

        @app.get("/admin/logs", response_class=HTMLResponse)
        async def logs_page(request: Request):
                if not is_authenticated(request):
                        return RedirectResponse(url="/admin", status_code=303)
                return HTMLResponse(
                        """
<!doctype html>
<html><head><meta charset="utf-8"><title>System Logs</title>
<style>body{margin:0;background:#111;color:#d7e3e7;font-family:ui-monospace,monospace}header{padding:14px 18px;background:#1f2a30;position:sticky;top:0}pre{margin:0;padding:18px;white-space:pre-wrap;word-break:break-word}</style>
</head><body><header><a href="/admin" style="color:#9ad1d4;text-decoration:none">back</a> <strong style="margin-left:12px">Live Logs</strong></header><pre id="log"></pre>
<script>
const log = document.getElementById('log');
const source = new EventSource('/admin/logs/stream');
source.onmessage = (event) => { log.textContent += event.data + "\\n"; window.scrollTo(0, document.body.scrollHeight); };
</script></body></html>
                        """
                )

        @app.get("/admin/logs/stream")
        async def logs_stream(request: Request) -> StreamingResponse:
                if not is_authenticated(request):
                        return StreamingResponse(_single_event("unauthorized"), status_code=401, media_type="text/event-stream")
                return StreamingResponse(_tail_log_stream(log_file_path), media_type="text/event-stream")

        return app


def _render_login(error: str | None = None) -> str:
        error_html = f"<p style='color:#9f2a2a'>{escape(error)}</p>" if error else ""
        return f"""
<!doctype html>
<html><head><meta charset="utf-8"><title>MeshCore Admin Login</title>
<style>body{{font-family:ui-sans-serif,system-ui,sans-serif;background:#f4f1ea;color:#1d2a2f}}.box{{max-width:420px;margin:80px auto;background:#fffdf8;border:1px solid #d9d1c3;border-radius:14px;padding:24px}}input{{width:100%;padding:12px;border:1px solid #c6bcad;border-radius:10px;margin:8px 0 14px}}button{{width:100%;padding:12px;border:0;border-radius:10px;background:#8d5524;color:#fff}}</style>
</head><body><div class="box"><h1>MeshCore Admin</h1><p>Default password is changeme.</p>{error_html}<form method="post" action="/admin/login"><input type="password" name="password" placeholder="Admin password"><button type="submit">Log In</button></form></div></body></html>
"""


def _render_public_map_page() -> str:
        return """
<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>MeshCore Runtime Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
<style>
:root{--sand:#efe2cc;--ink:#182126;--panel:#fffaf1;--line:#d6c6ab;--accent:#0b6e4f;--accent2:#b5651d;}
body{margin:0;font-family:Georgia,"Iowan Old Style",serif;background:linear-gradient(180deg,#f5ecd9,#e8dcc3);color:var(--ink)}
.layout{display:grid;grid-template-columns:minmax(300px,420px) 1fr;min-height:100vh}
.sidebar{padding:24px 20px 18px;background:rgba(255,250,241,.88);backdrop-filter:blur(8px);border-right:1px solid var(--line);overflow:auto}
.hero h1{margin:0;font-size:2.1rem;line-height:1}.hero p{margin:.65rem 0 0;color:#4e5b61}
.topline{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:18px}.topline a{color:var(--accent);text-decoration:none;font-weight:700}
.summary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:18px 0}.card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:14px 16px;box-shadow:0 12px 30px rgba(67,49,23,.08)}
.metric{font-size:1.5rem;font-weight:700}.label{font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;color:#6c7b80}
.node-list{display:grid;gap:10px;margin-top:16px}.node{display:block;width:100%;padding:12px 14px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.6);cursor:pointer;text-align:left}.node strong{display:block}.node small{color:#617076}
#map{min-height:100vh}.empty{padding:12px 14px;border:1px dashed var(--line);border-radius:14px;background:rgba(255,255,255,.45);color:#5d6a70}
@media (max-width: 900px){.layout{grid-template-columns:1fr;grid-template-rows:auto minmax(360px,55vh)}.sidebar{border-right:0;border-bottom:1px solid var(--line)}#map{min-height:55vh}}
</style></head><body>
<div class="layout">
    <aside class="sidebar">
        <div class="topline"><div class="hero"><h1>MeshCore</h1><p>Publiczny widok sieci i aktywności bota.</p></div><a href="/admin">Admin</a></div>
        <section class="summary" id="summary"></section>
        <section>
            <h2>Węzły</h2>
            <div id="nodes" class="node-list"></div>
        </section>
    </aside>
    <main id="map"></main>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const map = L.map('map', { zoomControl: true });
const markers = [];
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '&copy; OpenStreetMap' }).addTo(map);
map.setView([53.43, 14.55], 10);

function fmtDate(value) {
    if (!value) return 'brak';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function renderSummary(data) {
    const summary = document.getElementById('summary');
    const cards = [
        ['Węzły', String((data.nodes || []).length)],
        ['Wiadomości', String((data.messages || []).length)],
        ['Pakiety', String(data.diagnostics?.total_packets_seen || 0)],
        ['Endpointy', String(Object.keys(data.endpoints || {}).length)],
    ];
    summary.innerHTML = cards.map(([label, value]) => `<div class="card"><div class="label">${label}</div><div class="metric">${value}</div></div>`).join('');
}

function renderNodes(data) {
    const nodesWrap = document.getElementById('nodes');
    const nodes = data.nodes || [];
    if (!nodes.length) {
        nodesWrap.innerHTML = '<div class="empty">Brak węzłów w bazie. Gdy runtime zacznie je utrwalać, pojawią się tutaj automatycznie.</div>';
        return;
    }
    nodesWrap.innerHTML = nodes.map((node, index) => `
        <button class="node" type="button" data-index="${index}">
            <strong>${node.name || node.hash_prefix_hex || node.identity_hex.slice(0, 8)}</strong>
            <small>${node.role || 'unknown'} · ostatnio: ${fmtDate(node.last_seen_at)}</small>
        </button>
    `).join('');
    for (const button of nodesWrap.querySelectorAll('[data-index]')) {
        button.addEventListener('click', () => {
            const node = nodes[Number(button.dataset.index)];
            if (typeof node.latitude === 'number' && typeof node.longitude === 'number') {
                map.flyTo([node.latitude, node.longitude], 12, { duration: 0.8 });
            }
        });
    }
}

function renderMap(data) {
    while (markers.length) {
        map.removeLayer(markers.pop());
    }
    const bounds = [];
    for (const endpoint of Object.values(data.endpoints || {})) {
        if (typeof endpoint.latitude === 'number' && typeof endpoint.longitude === 'number') {
            const marker = L.circleMarker([endpoint.latitude, endpoint.longitude], { radius: 9, color: '#b5651d', weight: 2, fillColor: '#f4a261', fillOpacity: 0.85 })
                .addTo(map)
                .bindPopup(`<strong>${endpoint.name}</strong><br>${endpoint.raw_host}:${endpoint.raw_port}`);
            markers.push(marker);
            bounds.push([endpoint.latitude, endpoint.longitude]);
        }
    }
    for (const node of data.nodes || []) {
        if (typeof node.latitude === 'number' && typeof node.longitude === 'number') {
            const marker = L.circleMarker([node.latitude, node.longitude], { radius: 7, color: '#0b6e4f', weight: 2, fillColor: '#2a9d8f', fillOpacity: 0.9 })
                .addTo(map)
                .bindPopup(`<strong>${node.name || node.hash_prefix_hex}</strong><br>${node.role || 'unknown'}<br>Ostatnio: ${fmtDate(node.last_seen_at)}`);
            markers.push(marker);
            bounds.push([node.latitude, node.longitude]);
        }
    }
    if (bounds.length) {
        map.fitBounds(bounds, { padding: [24, 24] });
    }
}

async function loadState() {
    const response = await fetch('/api/state', { cache: 'no-store' });
    const data = await response.json();
    renderSummary(data);
    renderNodes(data);
    renderMap(data);
}

loadState();
setInterval(loadState, 15000);
</script></body></html>
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