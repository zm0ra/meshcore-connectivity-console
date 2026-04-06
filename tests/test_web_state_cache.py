from meshcore_bot.web_service import (
    STATE_CACHE_ACTIVE_TTL_SECS,
    STATE_CACHE_IDLE_TTL_SECS,
    _etag_matches,
    _state_cache_ttl_secs,
)


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


def test_etag_matches_strong_and_weak_headers() -> None:
    etag = '"abc123"'

    assert _etag_matches(etag, etag)
    assert _etag_matches(f'W/{etag}', etag)
    assert _etag_matches(f'"other", W/{etag}', etag)
    assert not _etag_matches('"other"', etag)