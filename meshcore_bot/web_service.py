from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from .database import BotDatabase


def create_app(database: BotDatabase) -> FastAPI:
    app = FastAPI(title="meshcore-bot", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "database": database.snapshot_overview()})

    @app.get("/api/state")
    async def api_state() -> JSONResponse:
        return JSONResponse(
            {
                "overview": database.snapshot_overview(),
                "repeaters": database.list_repeaters(),
                "probe_jobs": database.list_probe_jobs(limit=100),
            }
        )

    @app.get("/", response_class=HTMLResponse)
    async def root() -> HTMLResponse:
        repeaters = database.list_repeaters()
        jobs = database.list_probe_jobs(limit=50)
        return HTMLResponse(
            """
<!doctype html>
<html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>meshcore-bot</title>
<style>
body{font-family:ui-sans-serif,system-ui,sans-serif;background:#f6f1e8;color:#1f2a30;margin:0;padding:24px}
.wrap{max-width:1200px;margin:0 auto}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.card{background:white;border:1px solid #dccfb9;border-radius:16px;padding:16px;box-shadow:0 10px 24px rgba(0,0,0,.05)}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{padding:8px;border-bottom:1px solid #eee;text-align:left;vertical-align:top}
.mono{font-family:ui-monospace,monospace;font-size:12px}
@media (max-width:900px){.grid{grid-template-columns:1fr}}
</style></head><body><div class=\"wrap\"><h1>meshcore-bot</h1><div class=\"grid\"><section class=\"card\"><h2>Repeaters</h2>"""
            + _render_repeaters(repeaters)
            + """</section><section class=\"card\"><h2>Probe Jobs</h2>"""
            + _render_jobs(jobs)
            + """</section></div></div></body></html>"""
        )

    return app


def _render_repeaters(repeaters: list[dict[str, Any]]) -> str:
    if not repeaters:
        return "<p>No repeaters recorded yet.</p>"
    rows = "".join(
        f"<tr><td class='mono'>{item['pubkey_hex'][:16]}</td><td>{item.get('last_name_from_advert') or ''}</td><td>{item.get('last_probe_status') or ''}</td><td>{item.get('last_seen_at') or ''}</td></tr>"
        for item in repeaters
    )
    return f"<table><thead><tr><th>Pubkey</th><th>Name</th><th>Probe</th><th>Last Seen</th></tr></thead><tbody>{rows}</tbody></table>"


def _render_jobs(jobs: list[dict[str, Any]]) -> str:
    if not jobs:
        return "<p>No probe jobs yet.</p>"
    rows = "".join(
        f"<tr><td>{item['id']}</td><td>{item['status']}</td><td>{item.get('endpoint_name') or ''}</td><td class='mono'>{str(item.get('pubkey_hex') or '')[:16]}</td><td>{item.get('scheduled_at') or ''}</td></tr>"
        for item in jobs
    )
    return f"<table><thead><tr><th>ID</th><th>Status</th><th>Endpoint</th><th>Repeater</th><th>Scheduled</th></tr></thead><tbody>{rows}</tbody></table>"
