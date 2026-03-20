from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import struct
import time
from datetime import UTC, datetime
from dataclasses import asdict
from typing import Any, Callable, cast

from .config import AppConfig, EndpointConfig
from .database import BotDatabase
from .endpoint_console import parse_console_neighbors_reply, parse_console_text_reply, run_console_command
from .identity import LocalIdentity
from .mesh_builders import build_login_packet, build_request_packet, next_request_tag, parse_encrypted_datagram, parse_path_response
from .mesh_builders import build_advert_packet
from .mesh_packets import AdvertType, PayloadType, RouteType, describe_packet_summary
from .repeater_protocol import (
    REQ_TYPE_GET_TELEMETRY_DATA,
    REQ_TYPE_GET_NEIGHBOURS,
    REQ_TYPE_GET_OWNER_INFO,
    REQ_TYPE_GET_STATUS,
    RESP_SERVER_LOGIN_OK,
    build_path_discovery_request,
    parse_login_response,
    parse_neighbours_response,
    parse_owner_info_response,
    parse_status_response,
)
from .tcp_client import MeshcoreTCPClient, ReceivedPacket
from .transport import PacketTransportClient


class ProbeTimeoutError(TimeoutError):
    pass


def select_login_candidates(
    *,
    config,
    remote_pubkey: bytes,
    repeater_name: str | None,
    preferred_login: tuple[str, str] | None = None,
    forced_login: tuple[str, str] | None = None,
    allow_default_guest_fallback: bool = True,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    pubkey_hex = remote_pubkey.hex().upper()
    normalized_name = (repeater_name or "").strip().upper()

    if forced_login is not None:
        return [forced_login]

    if preferred_login is not None:
        candidates.append(preferred_login)

    if config.admin_password and (
        any(pubkey_hex.startswith(prefix) for prefix in config.admin_password_pubkey_prefixes)
        or any(normalized_name.startswith(prefix.upper()) for prefix in config.admin_password_name_prefixes)
    ):
        candidates.append(("admin", config.admin_password))

    if config.guest_password and (
        any(pubkey_hex.startswith(prefix) for prefix in config.guest_password_pubkey_prefixes)
        or any(normalized_name.startswith(prefix.upper()) for prefix in config.guest_password_name_prefixes)
    ):
        candidates.append(("guest", config.guest_password))

    if allow_default_guest_fallback and (config.default_guest_password == "" or config.default_guest_password or not candidates):
        candidates.append(("guest", config.default_guest_password))

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def is_recent_observation(observed_at: str | None, max_age_secs: float, *, now: datetime | None = None) -> bool:
    if not observed_at:
        return False
    if now is None:
        now = datetime.now(tz=UTC)
    try:
        observed = datetime.fromisoformat(observed_at)
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    age_secs = (now - observed).total_seconds()
    return 0 <= age_secs <= max_age_secs


def is_within_hour_window(*, hour: int, start_hour: int, end_hour: int) -> bool:
    normalized_hour = hour % 24
    normalized_start = start_hour % 24
    normalized_end = end_hour % 24
    if normalized_start == normalized_end:
        return True
    if normalized_start < normalized_end:
        return normalized_start <= normalized_hour < normalized_end
    return normalized_hour >= normalized_start or normalized_hour < normalized_end


def select_login_route_attempts(*, known_paths: list[tuple[int, bytes]], local_zero_hop_visible: bool) -> list[tuple[int, bytes]]:
    attempts: list[tuple[int, bytes]] = []
    seen: set[tuple[int, bytes]] = set()
    for candidate in known_paths:
        if candidate[0] <= 0 or candidate[1] == b"":
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        attempts.append(candidate)
    if attempts or local_zero_hop_visible:
        attempts.append((0, b""))
        return attempts
    return []


class LocalConsoleEndpointResolver:
    def __init__(self, config: AppConfig, *, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(f"{config.service.name}.probe")
        self._endpoint_map = {endpoint.name: endpoint for endpoint in config.endpoints if endpoint.enabled}
        self._endpoint_local_node_name_cache: dict[str, str | None] = {}
        self._endpoint_local_node_name_checked: set[str] = set()
        for endpoint in self._endpoint_map.values():
            if endpoint.local_node_name:
                self._endpoint_local_node_name_cache[endpoint.name] = endpoint.local_node_name.strip()
                self._endpoint_local_node_name_checked.add(endpoint.name)

    def remember_endpoint_node_name(self, endpoint_name: str, node_name: str | None) -> None:
        normalized = (node_name or "").strip() or None
        self._endpoint_local_node_name_checked.add(endpoint_name)
        self._endpoint_local_node_name_cache[endpoint_name] = normalized

    async def resolve_endpoint(self, repeater_name: str | None) -> EndpointConfig | None:
        normalized = (repeater_name or "").strip().upper()
        if not normalized:
            return None
        for endpoint in self._endpoint_map.values():
            endpoint_node_name = await self.resolve_endpoint_local_node_name(endpoint)
            if endpoint_node_name and endpoint_node_name.strip().upper() == normalized:
                return endpoint
        return None

    async def resolve_endpoint_local_node_name(self, endpoint: EndpointConfig) -> str | None:
        target = endpoint.console_probe_target()
        if target is None:
            return None
        if endpoint.name in self._endpoint_local_node_name_checked:
            return self._endpoint_local_node_name_cache.get(endpoint.name)
        try:
            reply = await run_console_command(
                target[0],
                target[1],
                "get name",
                timeout=max(2.0, self.config.gateway.console_probe_timeout_secs),
            )
        except Exception as exc:
            self.logger.debug("console get name failed endpoint=%s error=%s", endpoint.name, exc)
            return None
        node_name = parse_console_text_reply(reply) or None
        if node_name:
            self.remember_endpoint_node_name(endpoint.name, node_name)
        return node_name


class GuestProbeWorker:
    SCHEDULED_REPROBE_SCAN_INTERVAL_SECS = 300.0
    NIGHT_FAILED_RETRY_REASON = "night failed advert retry"
    LEARNED_LOGIN_STABLE_SUCCESS_COUNT = 3
    ENDPOINT_FALLBACK_REASON = "endpoint fallback verification"
    LOCAL_CONSOLE_REDIRECT_REASON = "endpoint local console redirect"
    CONSOLE_TEXT_COMMAND_RETRY_ATTEMPTS = 3
    CONSOLE_NEIGHBORS_RETRY_ATTEMPTS = 20

    def __init__(
        self,
        config: AppConfig,
        database: BotDatabase,
        *,
        transport_factory: Callable[[EndpointConfig], PacketTransportClient] | None = None,
        progress_callback: Callable[[str, dict[str, object]], None] | None = None,
    ) -> None:
        self.config = config
        self.database = database
        probe_key_path = config.probe.key_file_path or config.identity.key_file_path
        self.identity = LocalIdentity.load_or_create(probe_key_path)
        self.logger = logging.getLogger(f"{config.service.name}.probe")
        self._stop_event = asyncio.Event()
        self._endpoint_map = {endpoint.name: endpoint for endpoint in config.endpoints if endpoint.enabled}
        self._local_hash = self.identity.public_hash(1)
        self._transport_factory = transport_factory or self._build_direct_transport
        self._next_scheduled_reprobe_scan_monotonic = 0.0
        self._progress_callback = progress_callback
        self._local_console_resolver = LocalConsoleEndpointResolver(config, logger=self.logger)

    def _progress(self, event: str, **payload: object) -> None:
        if self._progress_callback is None:
            return
        self._progress_callback(event, payload)

    async def run(self) -> None:
        self.database.initialize()
        recovered = self.database.recover_interrupted_probe_work()
        self._next_scheduled_reprobe_scan_monotonic = time.monotonic() + self.SCHEDULED_REPROBE_SCAN_INTERVAL_SECS
        if recovered["jobs_interrupted"] or recovered["runs_interrupted"]:
            self.logger.warning(
                "marked interrupted probe work jobs=%s runs=%s",
                recovered["jobs_interrupted"],
                recovered["runs_interrupted"],
            )
        while not self._stop_event.is_set():
            self._schedule_stale_reprobes_if_due()
            job = self.database.claim_probe_job()
            if job is None:
                await asyncio.sleep(self.config.probe.poll_interval_secs)
                continue
            await self._run_job(job)

    async def stop(self) -> None:
        self._stop_event.set()

    def _schedule_stale_reprobes_if_due(self) -> None:
        now_utc = datetime.now(tz=UTC)
        interval_secs = self.config.probe.scheduled_reprobe_interval_secs
        now_monotonic = time.monotonic()
        if now_monotonic < self._next_scheduled_reprobe_scan_monotonic:
            return
        self._next_scheduled_reprobe_scan_monotonic = now_monotonic + self.SCHEDULED_REPROBE_SCAN_INTERVAL_SECS
        endpoint_names = sorted(self._endpoint_map)
        if not endpoint_names:
            return
        if interval_secs > 0:
            enqueued = self.database.schedule_stale_repeater_probe_jobs(
                endpoint_names=endpoint_names,
                stale_after_secs=interval_secs,
                seen_within_secs=max(interval_secs * 3, interval_secs),
                reason="scheduled stale refresh",
                success_cooldown_secs=interval_secs,
                failure_cooldown_secs=max(interval_secs / 2, self.config.probe.advert_reprobe_failure_cooldown_secs),
                now=now_utc,
                max_recent_jobs=self.config.probe.automatic_probe_max_per_day,
            )
            if enqueued:
                self.logger.info("scheduled stale reprobe jobs=%s stale_after_secs=%s", enqueued, interval_secs)

        night_interval_secs = self.config.probe.night_failed_retry_interval_secs
        if night_interval_secs <= 0:
            return
        if not is_within_hour_window(
            hour=now_utc.astimezone().hour,
            start_hour=self.config.probe.night_failed_retry_start_hour,
            end_hour=self.config.probe.night_failed_retry_end_hour,
        ):
            return
        night_seen_within_secs = max(
            night_interval_secs * 2,
            self.config.probe.advert_reprobe_failure_cooldown_secs,
            1800.0,
        )
        enqueued_failed = self.database.schedule_recent_failed_repeater_probe_jobs(
            endpoint_names=endpoint_names,
            seen_within_secs=night_seen_within_secs,
            reason=self.NIGHT_FAILED_RETRY_REASON,
            success_cooldown_secs=night_interval_secs,
            failure_cooldown_secs=night_interval_secs,
            now=now_utc,
            max_recent_jobs=self.config.probe.automatic_probe_max_per_day,
        )
        if enqueued_failed:
            self.logger.info(
                "scheduled night failed retries jobs=%s window=%02d-%02d interval_secs=%s",
                enqueued_failed,
                self.config.probe.night_failed_retry_start_hour,
                self.config.probe.night_failed_retry_end_hour,
                night_interval_secs,
            )

    async def _run_job(self, job: dict[str, object]) -> None:
        job_id = int(cast(int, job["id"]))
        endpoint_name = str(cast(str, job["endpoint_name"]))
        job_reason = str(cast(str, job["reason"]))
        endpoint = self._endpoint_map.get(endpoint_name)
        if endpoint is None:
            self.database.finish_probe_job(job_id, status="failed", last_error=f"unknown endpoint {endpoint_name}")
            return

        repeater_id = int(cast(int, job["repeater_id"]))
        remote_pubkey = bytes(cast(bytes, job["pubkey"]))
        repeater_name = cast(str | None, job.get("last_name_from_advert"))
        local_console_endpoint = await self.resolve_local_console_endpoint(repeater_name)
        if local_console_endpoint is not None and local_console_endpoint.name != endpoint.name:
            self.database.enqueue_probe_job(
                repeater_id=repeater_id,
                endpoint_name=local_console_endpoint.name,
                reason=self.LOCAL_CONSOLE_REDIRECT_REASON,
                success_cooldown_secs=0.0,
                failure_cooldown_secs=0.0,
            )
            self.database.finish_probe_job(job_id, status="completed")
            return

        probe_run_id = self.database.create_probe_run(repeater_id=repeater_id, endpoint_name=endpoint.name)
        used_direct_console = False

        try:
            if local_console_endpoint is not None and local_console_endpoint.name == endpoint.name:
                used_direct_console = True
                await self.probe_repeater_via_console(
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    endpoint=endpoint,
                    repeater_name=repeater_name,
                )
            else:
                await self.probe_repeater_as_guest(
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    endpoint=endpoint,
                    remote_pubkey=remote_pubkey,
                    repeater_name=repeater_name,
                )
        except Exception as exc:
            self.logger.warning("probe job %s failed: %s", job_id, exc)
            self.database.complete_probe_run(
                probe_run_id,
                repeater_id=repeater_id,
                result="failed",
                guest_login_ok=False,
                guest_permissions=None,
                firmware_capability_level=None,
                login_server_time=None,
                error_message=str(exc),
            )
            self.database.finish_probe_job(job_id, status="failed", last_error=str(exc))
            if not used_direct_console:
                self._enqueue_endpoint_fallback_jobs(
                    repeater_id=repeater_id,
                    failed_endpoint_name=endpoint.name,
                    trigger_reason=job_reason,
                )
            return

        self.database.set_repeater_preferred_endpoint(repeater_id=repeater_id, endpoint_name=endpoint.name)
        self.database.finish_probe_job(job_id, status="completed")

    async def resolve_local_console_endpoint(self, repeater_name: str | None) -> EndpointConfig | None:
        return await self._local_console_resolver.resolve_endpoint(repeater_name)

    async def _run_console_text_command(
        self,
        *,
        endpoint_name: str,
        target: tuple[str, int],
        command: str,
        timeout: float,
        retries: int | None = None,
    ) -> str:
        max_attempts = retries or self.CONSOLE_TEXT_COMMAND_RETRY_ATTEMPTS
        last_value = ""
        for attempt in range(1, max_attempts + 1):
            reply = await run_console_command(target[0], target[1], command, timeout=timeout)
            parsed = parse_console_text_reply(reply)
            if parsed or reply.strip() == "-none-":
                return parsed
            last_value = parsed
            if attempt < max_attempts:
                self.logger.debug(
                    "console text command returned empty response endpoint=%s command=%s attempt=%s/%s",
                    endpoint_name,
                    command,
                    attempt,
                    max_attempts,
                )
                await asyncio.sleep(0.25 * attempt)
        return last_value

    async def _run_console_neighbors_command(
        self,
        *,
        endpoint_name: str,
        target: tuple[str, int],
        timeout: float,
        retries: int | None = None,
    ) -> list[dict[str, object]]:
        max_attempts = retries or self.CONSOLE_NEIGHBORS_RETRY_ATTEMPTS
        last_neighbours: list[dict[str, object]] = []
        for attempt in range(1, max_attempts + 1):
            reply = await run_console_command(target[0], target[1], "neighbors", timeout=timeout)
            neighbours = parse_console_neighbors_reply(reply)
            if neighbours or reply.strip() == "-none-":
                return neighbours
            last_neighbours = neighbours
            if attempt < max_attempts:
                self.logger.debug(
                    "console neighbors command returned empty response endpoint=%s attempt=%s/%s",
                    endpoint_name,
                    attempt,
                    max_attempts,
                )
                await asyncio.sleep(0.1)
        raise RuntimeError(f"console neighbors command returned empty response on endpoint {endpoint_name}")

    async def probe_repeater_via_console(
        self,
        *,
        probe_run_id: int,
        repeater_id: int,
        endpoint: EndpointConfig,
        repeater_name: str | None,
    ) -> None:
        target = endpoint.console_probe_target()
        if target is None:
            raise RuntimeError(f"endpoint {endpoint.name} has no console port configured")
        command_timeout = max(2.0, self.config.probe.request_timeout_secs, self.config.gateway.console_probe_timeout_secs)

        self._progress("owner_requested", endpoint_name=endpoint.name)
        node_name = (
            await self._run_console_text_command(
                endpoint_name=endpoint.name,
                target=target,
                command="get name",
                timeout=command_timeout,
            )
        ) or endpoint.local_node_name or repeater_name
        firmware_version: str | None = None
        owner_info: str | None = None
        with contextlib.suppress(Exception):
            firmware_version = (
                await self._run_console_text_command(
                    endpoint_name=endpoint.name,
                    target=target,
                    command="ver",
                    timeout=command_timeout,
                )
            ) or None
        with contextlib.suppress(Exception):
            owner_reply = await self._run_console_text_command(
                endpoint_name=endpoint.name,
                target=target,
                command="get owner.info",
                timeout=command_timeout,
            )
            owner_info = owner_reply.replace("|", "\n") if owner_reply else None
        if node_name:
            self._local_console_resolver.remember_endpoint_node_name(endpoint.name, node_name)
            self.database.update_repeater_metadata(repeater_id=repeater_id, name=node_name)
        self.database.save_owner_snapshot(
            probe_run_id=probe_run_id,
            firmware_version=firmware_version,
            node_name=node_name,
            owner_info=owner_info,
        )
        self._progress("owner_received", endpoint_name=endpoint.name)

        self._progress("neighbours_started", endpoint_name=endpoint.name)
        neighbours = await self._run_console_neighbors_command(
            endpoint_name=endpoint.name,
            target=target,
            timeout=command_timeout,
        )
        if neighbours:
            self.database.save_neighbour_snapshot_page(
                probe_run_id=probe_run_id,
                page_offset=0,
                total_neighbours_count=len(neighbours),
                results_count=len(neighbours),
                entries=[
                    {
                        "neighbour_pubkey_prefix_hex": str(entry["neighbor_hash_prefix"]),
                        "heard_seconds_ago": int(entry["last_heard_seconds"]),
                        "snr": float(entry["snr"]),
                    }
                    for entry in neighbours
                ],
            )
        self._progress(
            "neighbours_page_saved",
            endpoint_name=endpoint.name,
            page_offset=0,
            results_count=len(neighbours),
            total_neighbours_count=len(neighbours),
        )
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=3,
            firmware_capability_level=None,
            login_server_time=None,
            error_message=None,
        )
        self.database.set_repeater_preferred_endpoint(repeater_id=repeater_id, endpoint_name=endpoint.name)
        self._progress("probe_completed", endpoint_name=endpoint.name, result="success")

    def _enqueue_endpoint_fallback_jobs(self, *, repeater_id: int, failed_endpoint_name: str, trigger_reason: str) -> None:
        if trigger_reason == self.ENDPOINT_FALLBACK_REASON:
            return
        fallback_endpoints = [name for name in sorted(self._endpoint_map) if name != failed_endpoint_name]
        if not fallback_endpoints:
            return
        cooldown_secs = max(self.config.probe.advert_reprobe_failure_cooldown_secs, self.config.probe.request_timeout_secs)
        for endpoint_name in fallback_endpoints:
            self.database.enqueue_probe_job(
                repeater_id=repeater_id,
                endpoint_name=endpoint_name,
                reason=self.ENDPOINT_FALLBACK_REASON,
                success_cooldown_secs=cooldown_secs,
                failure_cooldown_secs=cooldown_secs,
            )

    async def probe_repeater_as_guest(
        self,
        *,
        probe_run_id: int,
        repeater_id: int,
        endpoint: EndpointConfig,
        remote_pubkey: bytes,
        repeater_name: str | None,
        forced_login: tuple[str, str] | None = None,
        allow_default_guest_fallback: bool = True,
        force_path_discovery: bool = False,
    ) -> None:
        shared_secret = self.identity.calc_shared_secret(remote_pubkey)
        client = self._transport_factory(endpoint)
        learned_path_len = 0
        learned_path_bytes = b""
        guest_permissions: int | None = None
        firmware_capability_level: int | None = None
        login_server_time: int | None = None
        successful_login: tuple[str, str] | None = None
        preferred_login = self.database.preferred_repeater_login(repeater_id=repeater_id)
        preferred_login_candidate = None
        if preferred_login is not None:
            preferred_login_candidate = (
                str(preferred_login["learned_login_role"]),
                str(preferred_login["learned_login_password"]),
            )
        login_candidates = select_login_candidates(
            config=self.config.probe,
            remote_pubkey=remote_pubkey,
            repeater_name=repeater_name,
            preferred_login=preferred_login_candidate,
            forced_login=forced_login,
            allow_default_guest_fallback=allow_default_guest_fallback,
        )
        latest_zero_hop_advert = self.database.latest_repeater_zero_hop_advert(
            repeater_id=repeater_id,
            endpoint_name=endpoint.name,
        )
        local_zero_hop_visible = self._is_local_zero_hop_visible(latest_zero_hop_advert)
        known_direct_paths: list[tuple[int, bytes]] = []
        if force_path_discovery:
            local_zero_hop_visible = False
            self._progress("path_discovery_forced", endpoint_name=endpoint.name)
        else:
            latest_path = self.database.latest_repeater_path(repeater_id=repeater_id)
            if latest_path is not None and not self._is_usable_stored_path(latest_path):
                latest_path = None
            if latest_path is not None:
                learned_path_len = int(cast(int, latest_path.get("out_path_len", latest_path.get("path_len"))))
                learned_path_bytes = bytes.fromhex(str(cast(str, latest_path.get("out_path_hex", latest_path.get("path_hex")))))
                known_direct_paths.append((learned_path_len, learned_path_bytes))
            endpoint_advert_paths = self.database.recent_repeater_advert_paths(
                repeater_id=repeater_id,
                endpoint_name=endpoint.name,
            )
            for advert_path in endpoint_advert_paths:
                if not self._is_usable_stored_path(advert_path):
                    continue
                path_len = int(cast(int, advert_path.get("path_len", advert_path.get("out_path_len"))))
                path_bytes = bytes.fromhex(str(cast(str, advert_path.get("path_hex", advert_path.get("out_path_hex")))))
                candidate = (path_len, path_bytes)
                if candidate not in known_direct_paths:
                    known_direct_paths.append(candidate)
            if not endpoint_advert_paths:
                for advert_path in self.database.recent_repeater_advert_paths(repeater_id=repeater_id):
                    if not self._is_usable_stored_path(advert_path):
                        continue
                    path_len = int(cast(int, advert_path.get("path_len", advert_path.get("out_path_len"))))
                    path_bytes = bytes.fromhex(str(cast(str, advert_path.get("path_hex", advert_path.get("out_path_hex")))))
                    candidate = (path_len, path_bytes)
                    if candidate not in known_direct_paths:
                        known_direct_paths.append(candidate)
            if known_direct_paths:
                learned_path_len, learned_path_bytes = known_direct_paths[0]

        await client.connect()
        try:
            if self.config.probe.pre_login_advert_name:
                advert_packet = build_advert_packet(
                    identity=self.identity,
                    name=self.config.probe.pre_login_advert_name,
                    advert_type=int(AdvertType.CHAT),
                )
                advert_frame_hex = await client.send_packet(advert_packet.packet)
                self.database.insert_raw_packet(
                    probe_run_id=probe_run_id,
                    endpoint_name=endpoint.name,
                    observed_at=datetime.now(tz=UTC).isoformat(),
                    direction="tx",
                    transport="rs232bridge",
                    rs232_frame_hex=advert_frame_hex,
                    mesh_packet_hex=advert_packet.packet.hex().upper(),
                    payload_type=int(advert_packet.summary.payload_type),
                    route_type=int(advert_packet.summary.route_type),
                    remote_pubkey_hex=remote_pubkey.hex().upper(),
                    notes=f"pre-login advert name={self.config.probe.pre_login_advert_name}",
                )
                self.logger.info(
                    "decoded tx frame endpoint=%s repeater=%s name=%s %s notes=%s frame=%s packet=%s",
                    endpoint.name,
                    remote_pubkey.hex().upper()[:12],
                    (repeater_name or "").strip() or "-",
                    describe_packet_summary(advert_packet.summary),
                    f"pre-login advert name={self.config.probe.pre_login_advert_name}",
                    advert_frame_hex,
                    advert_packet.packet.hex().upper(),
                )
                if self.config.probe.pre_login_advert_delay_secs > 0:
                    await asyncio.sleep(self.config.probe.pre_login_advert_delay_secs)

            login_payload = b""
            login_error: Exception | None = None
            route_attempts = select_login_route_attempts(
                known_paths=known_direct_paths,
                local_zero_hop_visible=local_zero_hop_visible,
            )
            if not route_attempts:
                self._progress("path_discovery_started", endpoint_name=endpoint.name)
                learned_path_len, learned_path_bytes = await self._discover_repeater_path(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                )
                known_direct_paths = [(learned_path_len, learned_path_bytes), *known_direct_paths]
                route_attempts = select_login_route_attempts(
                    known_paths=known_direct_paths,
                    local_zero_hop_visible=local_zero_hop_visible,
                )
            if not route_attempts:
                raise RuntimeError(f"path discovery produced no usable route on endpoint {endpoint.name}")
            for login_role, login_password in login_candidates:
                for route_path_len, route_path_bytes in route_attempts:
                    if login_password == "":
                        password_label = "empty"
                    elif forced_login is not None and (login_role, login_password) == forced_login:
                        password_label = "provided"
                    elif preferred_login_candidate is not None and (login_role, login_password) == preferred_login_candidate:
                        password_label = "learned"
                    else:
                        password_label = "configured"
                    route_label = "direct" if route_path_len else "flood"
                    self._progress(
                        "login_attempt_started",
                        endpoint_name=endpoint.name,
                        login_role=login_role,
                        route=route_label,
                        password_label=password_label,
                        path_len=route_path_len,
                    )
                    login_packet = build_login_packet(
                        identity=self.identity,
                        remote_public_key=remote_pubkey,
                        guest_password=login_password,
                        encoded_path_len=route_path_len,
                        path_bytes=route_path_bytes,
                    )
                    frame_hex = await client.send_packet(login_packet.packet)
                    self.database.insert_raw_packet(
                        probe_run_id=probe_run_id,
                        endpoint_name=endpoint.name,
                        observed_at=datetime.now(tz=UTC).isoformat(),
                        direction="tx",
                        transport="rs232bridge",
                        rs232_frame_hex=frame_hex,
                        mesh_packet_hex=login_packet.packet.hex().upper(),
                        payload_type=int(login_packet.summary.payload_type),
                        route_type=int(login_packet.summary.route_type),
                        remote_pubkey_hex=remote_pubkey.hex().upper(),
                        notes=f"{login_role} login route={route_label} password={password_label}",
                    )
                    self.logger.info(
                        "decoded tx frame endpoint=%s repeater=%s name=%s role=%s route=%s password=%s %s notes=%s frame=%s packet=%s",
                        endpoint.name,
                        remote_pubkey.hex().upper()[:12],
                        (repeater_name or "").strip() or "-",
                        login_role,
                        route_label,
                        password_label,
                        describe_packet_summary(login_packet.summary),
                        f"{login_role} login route={route_label} password={password_label}",
                        frame_hex,
                        login_packet.packet.hex().upper(),
                    )

                    try:
                        login_payload, login_path_len, login_path_bytes = await self._await_login_response(
                            client=client,
                            endpoint_name=endpoint.name,
                            probe_run_id=probe_run_id,
                            remote_pubkey=remote_pubkey,
                            shared_secret=shared_secret,
                        )
                        if login_path_len:
                            learned_path_len = login_path_len
                            learned_path_bytes = login_path_bytes
                        elif not learned_path_len and route_path_len:
                            learned_path_len = route_path_len
                            learned_path_bytes = route_path_bytes
                        successful_login = (login_role, login_password)
                        break
                    except ProbeTimeoutError as exc:
                        login_error = exc
                        self._progress(
                            "login_attempt_failed",
                            endpoint_name=endpoint.name,
                            login_role=login_role,
                            route=route_label,
                            error=str(exc),
                        )
                        self.logger.warning(
                            "login attempt failed endpoint=%s repeater=%s role=%s route=%s error=%s",
                            endpoint.name,
                            remote_pubkey.hex().upper()[:12],
                            login_role,
                            route_label,
                            exc,
                        )
                        continue
                else:
                    continue
                break
            else:
                if self.database.reset_repeater_login_if_stable(
                    repeater_id=repeater_id,
                    min_success_count=self.LEARNED_LOGIN_STABLE_SUCCESS_COUNT,
                ):
                    self.logger.warning(
                        "reset learned login after stable login failures endpoint=%s repeater=%s",
                        endpoint.name,
                        remote_pubkey.hex().upper()[:12],
                    )
                assert login_error is not None
                raise login_error

            login = parse_login_response(login_payload)
            if login.response_code != RESP_SERVER_LOGIN_OK:
                raise RuntimeError(f"guest login rejected with code {login.response_code}")
            guest_permissions = login.permissions
            firmware_capability_level = login.firmware_capability_level
            login_server_time = login.server_time
            self._progress(
                "login_succeeded",
                endpoint_name=endpoint.name,
                login_role=successful_login[0],
                guest_permissions=guest_permissions,
                firmware_capability_level=firmware_capability_level,
            )
            assert successful_login is not None
            self.database.remember_repeater_login(
                repeater_id=repeater_id,
                login_role=successful_login[0],
                login_password=successful_login[1],
            )
            if learned_path_len:
                self.database.save_repeater_path(
                    repeater_id=repeater_id,
                    encoded_path_len=learned_path_len,
                    path_hex=learned_path_bytes.hex().upper(),
                    source="login_response_path",
                )
            learned_path_len, learned_path_bytes = await self._settle_post_login_frames(
                client=client,
                endpoint_name=endpoint.name,
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                remote_pubkey=remote_pubkey,
                shared_secret=shared_secret,
                current_path_len=learned_path_len,
                current_path_bytes=learned_path_bytes,
            )

            neighbour_pages_saved = 0
            offset = 0
            self._progress("neighbours_started", endpoint_name=endpoint.name)
            while True:
                neighbours_tag = next_request_tag()
                neighbours_plaintext = (
                    struct.pack("<I", neighbours_tag)
                    + bytes([
                        REQ_TYPE_GET_NEIGHBOURS,
                        0,
                        self.config.probe.neighbours_page_size,
                    ])
                    + struct.pack("<H", offset)
                    + bytes([
                        0,
                        self.config.probe.neighbours_prefix_len,
                    ])
                )
                neighbours_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=neighbours_plaintext,
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                neighbours_payload, learned_path_len, learned_path_bytes = await self._send_with_tagged_response_retries(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    packet=neighbours_request,
                    expected_tag=neighbours_tag,
                    notes=f"get_neighbours offset={offset}",
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                    max_attempts=3,
                )
                neighbours = parse_neighbours_response(
                    neighbours_payload,
                    pubkey_prefix_len=self.config.probe.neighbours_prefix_len,
                )
                self.database.save_neighbour_snapshot_page(
                    probe_run_id=probe_run_id,
                    page_offset=offset,
                    total_neighbours_count=neighbours.neighbours_count,
                    results_count=neighbours.results_count,
                    entries=[
                        {
                            "neighbour_pubkey_prefix_hex": entry.pubkey_prefix_hex,
                            "heard_seconds_ago": entry.heard_seconds_ago,
                            "snr": entry.snr,
                        }
                        for entry in neighbours.entries
                    ],
                )
                self._progress(
                    "neighbours_page_saved",
                    endpoint_name=endpoint.name,
                    page_offset=offset,
                    results_count=neighbours.results_count,
                    total_neighbours_count=neighbours.neighbours_count,
                )
                neighbour_pages_saved += 1
                offset += neighbours.results_count
                if neighbours.results_count == 0 or offset >= neighbours.neighbours_count:
                    break

            if neighbour_pages_saved == 0:
                raise RuntimeError("neighbours polling returned no pages")

            try:
                self._progress("status_requested", endpoint_name=endpoint.name)
                status_tag = next_request_tag()
                status_plaintext = struct.pack("<IB4s4s", status_tag, REQ_TYPE_GET_STATUS, b"\x00\x00\x00\x00", os.urandom(4))
                status_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=status_plaintext,
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                status_payload, learned_path_len, learned_path_bytes = await self._send_with_tagged_response_retries(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    packet=status_request,
                    expected_tag=status_tag,
                    notes="get_status",
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                    max_attempts=2,
                )
                status = parse_status_response(status_payload)
                self.database.save_status_snapshot(probe_run_id=probe_run_id, status=asdict(status))
                self._progress("status_received", endpoint_name=endpoint.name)
            except Exception as exc:
                self._progress("status_failed", endpoint_name=endpoint.name, error=str(exc))
                self.logger.warning("optional status polling failed for repeater %s: %s", remote_pubkey.hex().upper()[:12], exc)

            try:
                self._progress("owner_requested", endpoint_name=endpoint.name)
                owner_tag = next_request_tag()
                owner_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=struct.pack("<I", owner_tag) + bytes([REQ_TYPE_GET_OWNER_INFO]),
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                owner_payload, learned_path_len, learned_path_bytes = await self._send_with_tagged_response_retries(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    packet=owner_request,
                    expected_tag=owner_tag,
                    notes="get_owner_info",
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                    max_attempts=2,
                )
                owner = parse_owner_info_response(owner_payload)
                self.database.save_owner_snapshot(
                    probe_run_id=probe_run_id,
                    firmware_version=owner.firmware_version,
                    node_name=owner.node_name,
                    owner_info=owner.owner_info,
                )
                self._progress("owner_received", endpoint_name=endpoint.name)
            except Exception as exc:
                self._progress("owner_failed", endpoint_name=endpoint.name, error=str(exc))
                self.logger.warning("optional owner polling failed for repeater %s: %s", remote_pubkey.hex().upper()[:12], exc)

            self.database.complete_probe_run(
                probe_run_id,
                repeater_id=repeater_id,
                result="success",
                guest_login_ok=True,
                guest_permissions=guest_permissions,
                firmware_capability_level=firmware_capability_level,
                login_server_time=login_server_time,
                error_message=None,
            )
            self.database.set_repeater_preferred_endpoint(repeater_id=repeater_id, endpoint_name=endpoint.name)
            self._progress("probe_completed", endpoint_name=endpoint.name, result="success")
        finally:
            await client.close()

    async def _send_and_record(self, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, client: PacketTransportClient, packet, request_tag: int, notes: str) -> None:
        frame_hex = await client.send_packet(packet.packet)
        self.database.insert_raw_packet(
            probe_run_id=probe_run_id,
            endpoint_name=endpoint_name,
            observed_at=datetime.now(tz=UTC).isoformat(),
            direction="tx",
            transport="rs232bridge",
            rs232_frame_hex=frame_hex,
            mesh_packet_hex=packet.packet.hex().upper(),
            payload_type=int(packet.summary.payload_type),
            route_type=int(packet.summary.route_type),
            remote_pubkey_hex=remote_pubkey.hex().upper(),
            request_tag=request_tag,
            notes=notes,
        )
        self.logger.info(
            "decoded tx frame endpoint=%s repeater=%s %s tag=%s notes=%s frame=%s packet=%s",
            endpoint_name,
            remote_pubkey.hex().upper()[:12],
            describe_packet_summary(packet.summary),
            request_tag,
            notes,
            frame_hex,
            packet.packet.hex().upper(),
        )

    async def _send_with_tagged_response_retries(
        self,
        *,
        client: PacketTransportClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        packet,
        expected_tag: int,
        notes: str,
        current_path_len: int,
        current_path_bytes: bytes,
        max_attempts: int,
    ) -> tuple[bytes, int, bytes]:
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            attempt_notes = notes if attempt == 1 else f"{notes} retry={attempt}"
            await self._send_and_record(endpoint_name, probe_run_id, remote_pubkey, client, packet, expected_tag, attempt_notes)
            try:
                return await self._await_tagged_response(
                    client=client,
                    endpoint_name=endpoint_name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    expected_tag=expected_tag,
                    current_path_len=current_path_len,
                    current_path_bytes=current_path_bytes,
                )
            except ProbeTimeoutError as exc:
                last_error = exc
                latest_path = self.database.latest_repeater_path(repeater_id=repeater_id)
                if latest_path is not None and self._is_usable_stored_path(latest_path):
                    current_path_len = int(cast(int, latest_path.get("out_path_len", latest_path.get("path_len"))))
                    current_path_bytes = bytes.fromhex(
                        str(cast(str, latest_path.get("out_path_hex", latest_path.get("path_hex"))))
                    )
                if attempt == max_attempts:
                    break
                self.logger.warning(
                    "retrying tagged request endpoint=%s repeater=%s tag=%s attempt=%s/%s reason=%s",
                    endpoint_name,
                    remote_pubkey.hex().upper()[:12],
                    expected_tag,
                    attempt + 1,
                    max_attempts,
                    exc,
                )
        assert last_error is not None
        raise last_error

    async def _await_login_response(self, *, client: PacketTransportClient, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, shared_secret: bytes) -> tuple[bytes, int, bytes]:
        deadline = asyncio.get_running_loop().time() + self.config.probe.request_timeout_secs
        remote_hash = remote_pubkey[:1]
        last_observation = "none"
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise ProbeTimeoutError(f"timeout waiting for login response; last_observation={last_observation}")
            try:
                received = await client.receive_packet(timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise ProbeTimeoutError(f"timeout waiting for login response; last_observation={last_observation}") from exc
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.ANON_REQ and len(summary.payload) >= 33:
                sender_public_key = summary.payload[1:33]
                if sender_public_key == self.identity.public_key:
                    last_observation = "echoed-own-anon-req"
                    self.logger.info("ignored echoed own login anon request")
                    continue
            if summary.payload_type is PayloadType.PATH:
                try:
                    path_response = parse_path_response(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"path-decrypt-failed:{exc}"
                    self.logger.info("ignored login candidate path frame reason=%s", exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=path_response.source_hash,
                    destination_hash=path_response.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-path"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored login PATH from foreign hashes src=%s dst=%s",
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = (
                    "path"
                    f":extra_type={path_response.extra_type}"
                    f":path_len={path_response.encoded_path_len & 0x3F}"
                    f":src={path_response.source_hash.hex().upper()}"
                    f":dst={path_response.destination_hash.hex().upper()}"
                )
                if path_response.extra_type == int(PayloadType.RESPONSE):
                    self.logger.info(
                        "accepted login response via PATH src=%s dst=%s path_len=%s",
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                        path_response.encoded_path_len & 0x3F,
                    )
                    return path_response.extra_payload, path_response.encoded_path_len, path_response.path_bytes
                self.logger.info(
                    "ignored login PATH frame extra_type=%s src=%s dst=%s",
                    path_response.extra_type,
                    path_response.source_hash.hex().upper(),
                    path_response.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is PayloadType.RESPONSE:
                try:
                    decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                    parse_login_response(decrypted.plaintext)
                except Exception as exc:
                    last_observation = f"response-decrypt-failed:{exc}"
                    self.logger.info("ignored login candidate response reason=%s", exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=decrypted.source_hash,
                    destination_hash=decrypted.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-response"
                        f":src={decrypted.source_hash.hex().upper()}"
                        f":dst={decrypted.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored login RESPONSE from foreign hashes src=%s dst=%s",
                        decrypted.source_hash.hex().upper(),
                        decrypted.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = (
                    "response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "accepted login response via RESPONSE src=%s dst=%s",
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                return decrypted.plaintext, 0, b""
            last_observation = f"ignored-payload-type:{summary.payload_type.name}"
            self.logger.info("ignored login frame payload_type=%s", summary.payload_type.name)

    async def _settle_post_login_frames(
        self,
        *,
        client: PacketTransportClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        current_path_len: int,
        current_path_bytes: bytes,
    ) -> tuple[int, bytes]:
        deadline = asyncio.get_running_loop().time() + 1.5
        learned_path_len = current_path_len
        learned_path_bytes = current_path_bytes
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return learned_path_len, learned_path_bytes
            try:
                received = await client.receive_packet(timeout=min(remaining, 0.25))
            except asyncio.TimeoutError:
                continue
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.ANON_REQ and len(summary.payload) >= 33:
                sender_public_key = summary.payload[1:33]
                if sender_public_key == self.identity.public_key:
                    continue
            if summary.payload_type is not PayloadType.PATH:
                continue
            try:
                path_response = parse_path_response(summary, shared_secret=shared_secret)
            except Exception:
                continue
            if path_response.extra_type != int(PayloadType.RESPONSE):
                continue
            try:
                parse_login_response(path_response.extra_payload)
            except Exception:
                continue
            if path_response.encoded_path_len:
                learned_path_len = path_response.encoded_path_len
                learned_path_bytes = path_response.path_bytes
                self.database.save_repeater_path(
                    repeater_id=repeater_id,
                    encoded_path_len=learned_path_len,
                    path_hex=learned_path_bytes.hex().upper(),
                    source="login_response_path",
                )

    async def _discover_repeater_path(
        self,
        *,
        client: PacketTransportClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
    ) -> tuple[int, bytes]:
        discovery_tag = next_request_tag()
        discovery_plaintext = build_path_discovery_request(discovery_tag, random_bytes=os.urandom(4))
        discovery_request = build_request_packet(
            identity=self.identity,
            remote_public_key=remote_pubkey,
            plaintext=discovery_plaintext,
            encoded_path_len=0,
            path_bytes=b"",
        )
        await self._send_and_record(
            endpoint_name,
            probe_run_id,
            remote_pubkey,
            client,
            discovery_request,
            discovery_tag,
            f"path_discovery req_type={REQ_TYPE_GET_TELEMETRY_DATA}",
        )
        return await self._await_path_discovery_response(
            client=client,
            endpoint_name=endpoint_name,
            probe_run_id=probe_run_id,
            repeater_id=repeater_id,
            remote_pubkey=remote_pubkey,
            shared_secret=shared_secret,
            expected_tag=discovery_tag,
        )

    async def _await_path_discovery_response(
        self,
        *,
        client: PacketTransportClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        expected_tag: int,
    ) -> tuple[int, bytes]:
        deadline = asyncio.get_running_loop().time() + self.config.probe.request_timeout_secs
        remote_hash = remote_pubkey[:1]
        last_observation = "none"
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise ProbeTimeoutError(
                    f"timeout waiting for discovery response tag={expected_tag}; last_observation={last_observation}"
                )
            try:
                received = await client.receive_packet(timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise ProbeTimeoutError(
                    f"timeout waiting for discovery response tag={expected_tag}; last_observation={last_observation}"
                ) from exc
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.PATH:
                try:
                    path_response = parse_path_response(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"path-decrypt-failed:{exc}"
                    self.logger.info("ignored discovery PATH frame tag=%s reason=%s", expected_tag, exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=path_response.source_hash,
                    destination_hash=path_response.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-path"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored discovery PATH from foreign hashes tag=%s src=%s dst=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if path_response.extra_type != int(PayloadType.RESPONSE):
                    last_observation = f"path-extra-type={path_response.extra_type}"
                    self.logger.info(
                        "ignored discovery PATH frame tag=%s extra_type=%s",
                        expected_tag,
                        path_response.extra_type,
                    )
                    continue
                actual_tag = struct.unpack_from("<I", path_response.extra_payload, 0)[0] if len(path_response.extra_payload) >= 4 else None
                if actual_tag != expected_tag:
                    last_observation = f"path-tag-mismatch={actual_tag}"
                    self.logger.info(
                        "ignored discovery PATH response expected_tag=%s actual_tag=%s",
                        expected_tag,
                        actual_tag,
                    )
                    continue
                if not path_response.encoded_path_len:
                    last_observation = "path-missing-route"
                    self.logger.info("ignored discovery PATH response tag=%s because it carried no route", expected_tag)
                    continue
                learned_path_len, learned_path_bytes = self._save_repeater_path_update(
                    repeater_id=repeater_id,
                    encoded_path_len=path_response.encoded_path_len,
                    path_bytes=path_response.path_bytes,
                    source="path_discovery",
                )
                self.logger.info(
                    "accepted discovery PATH response tag=%s path_len=%s",
                    expected_tag,
                    path_response.encoded_path_len & 0x3F,
                )
                return learned_path_len, learned_path_bytes
            if summary.payload_type is PayloadType.RESPONSE:
                try:
                    decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"response-decrypt-failed:{exc}"
                    self.logger.info("ignored discovery RESPONSE tag=%s reason=%s", expected_tag, exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=decrypted.source_hash,
                    destination_hash=decrypted.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = "foreign-response"
                    continue
                actual_tag = struct.unpack_from("<I", decrypted.plaintext, 0)[0] if len(decrypted.plaintext) >= 4 else None
                last_observation = f"response-without-path={actual_tag}"
                self.logger.info(
                    "ignored discovery RESPONSE without path expected_tag=%s actual_tag=%s",
                    expected_tag,
                    actual_tag,
                )
                continue
            if summary.payload_type is PayloadType.REQ:
                try:
                    decrypted_req = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"req-decrypt-failed:{exc}"
                    continue
                request_tag = struct.unpack_from("<I", decrypted_req.plaintext, 0)[0] if len(decrypted_req.plaintext) >= 4 else None
                if decrypted_req.source_hash == self._local_hash and decrypted_req.destination_hash == remote_hash:
                    last_observation = f"echoed-own-req:{request_tag}"
                    continue
                last_observation = f"unexpected-req:{request_tag}"
                continue
            last_observation = f"ignored-payload-type:{summary.payload_type.name}"

    async def _await_tagged_response(
        self,
        *,
        client: PacketTransportClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        expected_tag: int,
        current_path_len: int,
        current_path_bytes: bytes,
    ) -> tuple[bytes, int, bytes]:
        deadline = asyncio.get_running_loop().time() + self.config.probe.request_timeout_secs
        remote_hash = remote_pubkey[:1]
        last_observation = "none"
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise ProbeTimeoutError(
                    f"timeout waiting for tagged response tag={expected_tag}; last_observation={last_observation}"
                )
            try:
                received = await client.receive_packet(timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise ProbeTimeoutError(
                    f"timeout waiting for tagged response tag={expected_tag}; last_observation={last_observation}"
                ) from exc
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.PATH:
                try:
                    path_response = parse_path_response(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"path-decrypt-failed:{exc}"
                    self.logger.info("ignored tagged PATH frame tag=%s reason=%s", expected_tag, exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=path_response.source_hash,
                    destination_hash=path_response.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-path"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored tagged PATH from foreign hashes tag=%s src=%s dst=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if path_response.encoded_path_len:
                    current_path_len, current_path_bytes = self._save_repeater_path_update(
                        repeater_id=repeater_id,
                        encoded_path_len=path_response.encoded_path_len,
                        path_bytes=path_response.path_bytes,
                        source="path_update",
                    )
                if path_response.extra_type != int(PayloadType.RESPONSE):
                    last_observation = (
                        f"path-extra-type={path_response.extra_type}"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored tagged PATH frame tag=%s extra_type=%s src=%s dst=%s",
                        expected_tag,
                        path_response.extra_type,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if self._is_login_response_payload(path_response.extra_payload):
                    last_observation = (
                        "late-login-response"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored late login PATH while waiting for tag=%s src=%s dst=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if len(path_response.extra_payload) >= 4 and struct.unpack_from("<I", path_response.extra_payload, 0)[0] == expected_tag:
                    current_path_len, current_path_bytes = self._save_repeater_path_update(
                        repeater_id=repeater_id,
                        encoded_path_len=path_response.encoded_path_len,
                        path_bytes=path_response.path_bytes,
                        source="response_path",
                    )
                    self.logger.info(
                        "accepted tagged PATH response tag=%s src=%s dst=%s path_len=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                        path_response.encoded_path_len & 0x3F,
                    )
                    return path_response.extra_payload, current_path_len, current_path_bytes
                path_tag = struct.unpack_from("<I", path_response.extra_payload, 0)[0] if len(path_response.extra_payload) >= 4 else None
                last_observation = (
                    f"path-tag-mismatch={path_tag}"
                    f":src={path_response.source_hash.hex().upper()}"
                    f":dst={path_response.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored tagged PATH response expected_tag=%s actual_tag=%s src=%s dst=%s",
                    expected_tag,
                    path_tag,
                    path_response.source_hash.hex().upper(),
                    path_response.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is PayloadType.REQ:
                try:
                    decrypted_req = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"req-decrypt-failed:{exc}"
                    self.logger.info("ignored req frame while waiting for tag=%s reason=%s", expected_tag, exc)
                    continue
                request_tag = struct.unpack_from("<I", decrypted_req.plaintext, 0)[0] if len(decrypted_req.plaintext) >= 4 else None
                if decrypted_req.source_hash == self._local_hash and decrypted_req.destination_hash == remote_hash:
                    last_observation = f"echoed-own-req:{request_tag}"
                    self.logger.info(
                        "ignored echoed own request expected_tag=%s echoed_tag=%s src=%s dst=%s",
                        expected_tag,
                        request_tag,
                        decrypted_req.source_hash.hex().upper(),
                        decrypted_req.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = f"unexpected-req:{request_tag}"
                self.logger.info(
                    "ignored foreign req while waiting expected_tag=%s actual_tag=%s src=%s dst=%s",
                    expected_tag,
                    request_tag,
                    decrypted_req.source_hash.hex().upper(),
                    decrypted_req.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is not PayloadType.RESPONSE:
                last_observation = f"ignored-payload-type:{summary.payload_type.name}"
                self.logger.info(
                    "ignored frame while waiting for tag=%s payload_type=%s",
                    expected_tag,
                    summary.payload_type.name,
                )
                continue
            try:
                decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
            except Exception as exc:
                last_observation = f"response-decrypt-failed:{exc}"
                self.logger.info("ignored response frame tag=%s reason=%s", expected_tag, exc)
                continue
            if not self._is_remote_to_local_datagram(
                source_hash=decrypted.source_hash,
                destination_hash=decrypted.destination_hash,
                remote_hash=remote_hash,
            ):
                last_observation = (
                    "foreign-response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored RESPONSE from foreign hashes expected_tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                continue
            if self._is_login_response_payload(decrypted.plaintext):
                last_observation = (
                    "late-login-response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored late login RESPONSE while waiting for tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                continue
            if len(decrypted.plaintext) >= 4 and struct.unpack_from("<I", decrypted.plaintext, 0)[0] == expected_tag:
                self.logger.info(
                    "accepted RESPONSE tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                return decrypted.plaintext, current_path_len, current_path_bytes
            actual_tag = struct.unpack_from("<I", decrypted.plaintext, 0)[0] if len(decrypted.plaintext) >= 4 else None
            last_observation = (
                f"response-tag-mismatch={actual_tag}"
                f":src={decrypted.source_hash.hex().upper()}"
                f":dst={decrypted.destination_hash.hex().upper()}"
            )
            self.logger.info(
                "ignored RESPONSE expected_tag=%s actual_tag=%s src=%s dst=%s",
                expected_tag,
                actual_tag,
                decrypted.source_hash.hex().upper(),
                decrypted.destination_hash.hex().upper(),
            )

    def _is_remote_to_local_datagram(self, *, source_hash: bytes, destination_hash: bytes, remote_hash: bytes) -> bool:
        return source_hash == remote_hash and destination_hash == self._local_hash

    def _is_usable_stored_path(self, path_row: dict[str, object]) -> bool:
        path_len = int(cast(int | str, path_row.get("out_path_len", path_row.get("path_len", 0))) or 0)
        path_hex = str(cast(str | None, path_row.get("out_path_hex", path_row.get("path_hex", ""))) or "").strip()
        return path_len > 0 and path_hex != ""

    def _is_fresh_observation(self, row: dict[str, object]) -> bool:
        observed_at = str(cast(str | None, row.get("observed_at")) or "")
        return is_recent_observation(observed_at, self.config.probe.route_freshness_secs)

    def _is_local_zero_hop_visible(self, advert_row: dict[str, object] | None) -> bool:
        if advert_row is None:
            return False
        if not self._is_fresh_observation(advert_row):
            return False
        path_len = int(cast(int | str, advert_row.get("path_len", 0)) or 0)
        path_hex = str(cast(str | None, advert_row.get("path_hex", "")) or "").strip()
        return path_len == 0 and path_hex == ""

    def _is_login_response_payload(self, payload: bytes) -> bool:
        if len(payload) not in {12, 13}:
            return False
        try:
            login = parse_login_response(payload)
        except Exception:
            return False
        return login.response_code == RESP_SERVER_LOGIN_OK

    def _save_repeater_path_update(
        self,
        *,
        repeater_id: int,
        encoded_path_len: int,
        path_bytes: bytes,
        source: str,
    ) -> tuple[int, bytes]:
        if not encoded_path_len:
            return 0, b""
        self.database.save_repeater_path(
            repeater_id=repeater_id,
            encoded_path_len=encoded_path_len,
            path_hex=path_bytes.hex().upper(),
            source=source,
        )
        return encoded_path_len, path_bytes

    def _record_rx(self, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, received: ReceivedPacket) -> None:
        self.database.insert_raw_packet(
            probe_run_id=probe_run_id,
            endpoint_name=endpoint_name,
            observed_at=received.observed_at,
            direction="rx",
            transport="rs232bridge",
            rs232_frame_hex=received.frame_hex,
            mesh_packet_hex=received.packet_hex,
            payload_type=int(received.summary.payload_type),
            route_type=int(received.summary.route_type),
            remote_pubkey_hex=remote_pubkey.hex().upper(),
        )
        self.logger.info(
            "decoded rx frame endpoint=%s repeater=%s %s frame=%s packet=%s",
            endpoint_name,
            remote_pubkey.hex().upper()[:12],
            describe_packet_summary(received.summary),
            received.frame_hex,
            received.packet_hex,
        )

    def _build_direct_transport(self, endpoint: EndpointConfig) -> PacketTransportClient:
        return MeshcoreTCPClient(endpoint.raw_host, endpoint.raw_port)

