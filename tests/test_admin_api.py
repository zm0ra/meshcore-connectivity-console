"""HTTP-level tests for the admin API."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from meshcore_bot.config import load_config
from meshcore_bot.web_service import (
    ADMIN_LOGIN_RATE_LIMIT_MAX_ATTEMPTS,
    _ADMIN_LOGIN_ATTEMPTS,
    create_app,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


class _DummyDatabase:
    def snapshot_overview(self) -> dict[str, object]:
        return {"repeater_count": 0, "raw_packet_count": 0}

    def list_repeaters_for_web(self) -> list[dict[str, object]]:
        return []

    def list_probe_jobs(self, limit: int = 100) -> list[dict[str, object]]:
        return []

    def latest_repeater_neighbor_links(self, limit_repeaters: int = 128) -> list[dict[str, object]]:
        return []

    def repeater_route_hints(self, limit_repeaters: int = 128) -> dict[str, dict[str, object]]:
        return {}

    def repeater_historical_neighbor_links(self, limit_repeaters: int = 128) -> list[dict[str, object]]:
        return []

    def repeater_full_state(self, *, repeater_id: int) -> dict[str, object] | None:
        return {"id": repeater_id}

    def repeater_signal_history(self, *, repeater_id: int, limit_samples: int = 128) -> list[dict[str, object]]:
        return []

    def list_recent_raw_packets(self, limit: int = 100) -> list[dict[str, object]]:
        return []

    def admin_dashboard_summary(self) -> dict[str, object]:
        return {"nodes_with_data": 0, "active_jobs": 0, "recent_failures": 0}

    def list_recent_failed_probe_jobs(self, limit: int = 20) -> list[dict[str, object]]:
        return []

    # Tolerant fallback for any other method names accessed during payload build.
    def __getattr__(self, name: str) -> Any:
        def _missing(*_args: object, **_kwargs: object) -> list[Any]:
            return []

        return _missing


@pytest.fixture()
def admin_config_path(tmp_path: Path) -> Path:
    # Start from the example config, then override admin credentials & db location.
    example = (REPO_ROOT / "config" / "config.example.toml").read_text(encoding="utf-8")
    db_file = tmp_path / "test.db"
    cfg = example.replace(
        'database_path = "./data/meshcore-bot.db"',
        f'database_path = "{db_file}"',
    ).replace(
        'admin_password = ""',
        'admin_password = "secret123"',
        1,
    )
    target = tmp_path / "config.toml"
    target.write_text(cfg, encoding="utf-8")
    return target


@pytest.fixture()
def app(admin_config_path: Path):
    cfg = load_config(admin_config_path)
    application = create_app(_DummyDatabase(), cfg, config_path=admin_config_path)
    _ADMIN_LOGIN_ATTEMPTS.clear()
    return application


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


def test_admin_dashboard_requires_auth(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/admin/dashboard")
            assert response.status_code == 401

    _run(_do())


def test_admin_session_unauthenticated_returns_false(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/admin/session")
            assert response.status_code == 200
            assert response.json() == {"authenticated": False}

    _run(_do())


def test_admin_login_then_dashboard_succeeds(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            login = await client.post(
                "/api/admin/login",
                json={"username": "admin", "password": "secret123"},
            )
            assert login.status_code == 200
            payload = login.json()
            assert payload["status"] == "ok"
            assert payload["session"]["authenticated"] is True

            dashboard = await client.get("/api/admin/dashboard")
            assert dashboard.status_code == 200
            body = dashboard.json()
            assert "summary" in body
            assert "endpoints" in body

    _run(_do())


def test_admin_login_wrong_password_returns_401(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/api/admin/login",
                json={"username": "admin", "password": "WRONG"},
            )
            assert response.status_code == 401

    _run(_do())


def test_admin_login_rate_limit_after_repeated_failures(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for _ in range(ADMIN_LOGIN_RATE_LIMIT_MAX_ATTEMPTS):
                bad = await client.post(
                    "/api/admin/login",
                    json={"username": "admin", "password": "x"},
                )
                assert bad.status_code == 401
            blocked = await client.post(
                "/api/admin/login",
                json={"username": "admin", "password": "x"},
            )
            assert blocked.status_code == 429
            assert "Retry-After" in blocked.headers

    _run(_do())


def test_admin_logout_clears_cookie(app) -> None:
    async def _do() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            login = await client.post(
                "/api/admin/login",
                json={"username": "admin", "password": "secret123"},
            )
            assert login.status_code == 200

            session_before = await client.get("/api/admin/session")
            assert session_before.json().get("authenticated") is True

            logout = await client.post("/api/admin/logout")
            assert logout.status_code == 200

            session_after = await client.get("/api/admin/session")
            assert session_after.json() == {"authenticated": False}

    _run(_do())
