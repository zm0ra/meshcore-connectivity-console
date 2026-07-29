from meshcore_bot.web_service import (
    MANAGEMENT_CACHE_ACTIVE_TTL_SECS,
    MANAGEMENT_CACHE_IDLE_TTL_SECS,
    STATE_CACHE_ACTIVE_TTL_SECS,
    STATE_CACHE_IDLE_TTL_SECS,
    _build_management_payload,
    create_app,
    _etag_matches,
    _management_cache_ttl_secs,
    _probe_jobs_have_active_entries,
    _state_cache_ttl_secs,
)


class _DummyDatabase:
    def snapshot_overview(self) -> dict[str, object]:
        return {}

    def list_repeaters_for_web(self) -> list[dict[str, object]]:
        return []

    def list_probe_jobs(self, limit: int = 100) -> list[dict[str, object]]:
        return []

    def latest_repeater_neighbor_links(self, limit_repeaters: int = 128) -> list[dict[str, object]]:
        return [{"source_identity_hex": "a", "target_identity_hex": "b"}]

    def repeater_route_hints(self, limit_repeaters: int = 128) -> dict[str, dict[str, object]]:
        return {"a": {"latest_saved_path": None}}

    def repeater_historical_neighbor_links(self, limit_repeaters: int = 128) -> list[dict[str, object]]:
        return [{"source_identity_hex": "a", "target_identity_hex": "c"}]

    def repeater_full_state(self, *, repeater_id: int) -> dict[str, object] | None:
        return {"id": repeater_id}

    def repeater_signal_history(self, *, repeater_id: int, limit_samples: int = 128) -> list[dict[str, object]]:
        return []


class _DummyProbe:
    advert_probe_min_interval_secs = 60
    advert_reprobe_failure_cooldown_secs = 60
    request_timeout_secs = 30


class _DummyConfig:
    endpoints = []
    probe = _DummyProbe()


def test_state_cache_uses_idle_ttl_without_active_jobs() -> None:
    payload = {
        "probe_jobs": [
            {"status": "success"},
            {"status": "failed"},
        ]
    }

    assert _state_cache_ttl_secs(payload) == STATE_CACHE_IDLE_TTL_SECS


def test_state_cache_uses_active_ttl_with_pending_jobs() -> None:
    payload = {
        "probe_jobs": [
            {"status": "pending"},
        ]
    }

    assert _state_cache_ttl_secs(payload) == STATE_CACHE_ACTIVE_TTL_SECS


def test_probe_jobs_have_active_entries_detects_running_job() -> None:
    probe_jobs = [
        {"status": "success"},
        {"status": "running"},
    ]

    assert _probe_jobs_have_active_entries(probe_jobs) is True


def test_management_cache_uses_idle_ttl_without_active_jobs() -> None:
    payload = {
        "has_active_probe_jobs": False,
    }

    assert _management_cache_ttl_secs(payload) == MANAGEMENT_CACHE_IDLE_TTL_SECS


def test_management_cache_uses_active_ttl_with_active_jobs() -> None:
    payload = {
        "has_active_probe_jobs": True,
    }

    assert _management_cache_ttl_secs(payload) == MANAGEMENT_CACHE_ACTIVE_TTL_SECS


def test_build_management_payload_excludes_signal_history() -> None:
    payload = _build_management_payload(_DummyDatabase())

    assert "signal_history" not in payload
    assert payload["map_links"]
    assert payload["route_hints"]


def test_build_management_payload_loads_historical_links_only_on_demand() -> None:
    assert "historical_links" not in _build_management_payload(_DummyDatabase())

    payload = _build_management_payload(_DummyDatabase(), include_historical=True)
    assert payload["historical_links"]


def test_create_app_registers_signal_history_route() -> None:
    app = create_app(_DummyDatabase(), _DummyConfig())
    paths = {route.path for route in app.routes}

    assert "/api/management" in paths
    assert "/api/repeaters/{repeater_id}/signal-history" in paths


def test_etag_matches_strong_and_weak_headers() -> None:
    etag = '"abc123"'

    assert _etag_matches(etag, etag)
    assert _etag_matches(f'W/{etag}', etag)
    assert _etag_matches(f'"other", W/{etag}', etag)
    assert not _etag_matches('"other"', etag)