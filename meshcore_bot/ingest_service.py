from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Callable

from .config import AppConfig, EndpointConfig
from .database import BotDatabase
from .mesh_packets import AdvertType, describe_packet_summary, parse_advert
from .tcp_client import MeshcoreTCPClient, ReceivedPacket
from .transport import PacketTransportClient
from .probe_service import LocalConsoleEndpointResolver, is_recent_observation


@dataclass(slots=True)
class IngestStats:
    packets_seen: int = 0
    adverts_seen: int = 0
    repeater_adverts_seen: int = 0
    jobs_enqueued: int = 0
    advert_jobs_skipped_stable: int = 0
    advert_jobs_skipped_recent_path_change: int = 0
    advert_jobs_deferred: int = 0


class AdvertIngestService:
    RECEIVE_IDLE_TIMEOUT_SECS = 60.0

    def __init__(
        self,
        config: AppConfig,
        database: BotDatabase,
        *,
        transport_factory: Callable[[EndpointConfig], PacketTransportClient] | None = None,
    ) -> None:
        self.config = config
        self.database = database
        self.logger = logging.getLogger(f"{config.service.name}.ingest")
        self.stats = IngestStats()
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._transport_factory = transport_factory or self._build_direct_transport
        self._next_advert_probe_slot_at: dict[str, datetime] = {}
        self._local_console_resolver = LocalConsoleEndpointResolver(config, logger=self.logger)

    async def run(self) -> None:
        self.database.initialize()
        enabled_endpoints = [endpoint for endpoint in self.config.endpoints if endpoint.enabled]
        self._tasks = [
            asyncio.create_task(self._run_endpoint(endpoint), name=f"ingest:{endpoint.name}")
            for endpoint in enabled_endpoints
        ]
        if not self._tasks:
            self.logger.warning("no enabled endpoints configured for ingest")
            await self._stop_event.wait()
            return
        await asyncio.gather(*self._tasks)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run_endpoint(self, endpoint: EndpointConfig) -> None:
        while not self._stop_event.is_set():
            client = self._transport_factory(endpoint)
            try:
                await client.connect()
                self.logger.info("ingest connected to %s (%s:%s)", endpoint.name, endpoint.raw_host, endpoint.raw_port)
                while not self._stop_event.is_set():
                    try:
                        packet = await client.receive_packet(timeout=self.RECEIVE_IDLE_TIMEOUT_SECS)
                    except asyncio.TimeoutError:
                        continue
                    await self._handle_packet(endpoint, packet)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("ingest endpoint %s failed: %s", endpoint.name, exc)
                await asyncio.sleep(3.0)
            finally:
                await client.close()

    def _build_direct_transport(self, endpoint: EndpointConfig) -> PacketTransportClient:
        return MeshcoreTCPClient(endpoint.raw_host, endpoint.raw_port)

    async def _handle_packet(self, endpoint: EndpointConfig, packet: ReceivedPacket) -> None:
        self.stats.packets_seen += 1
        summary = packet.summary
        self.logger.debug(
            "[RX] endpoint=%s %s frame=%s packet=%s",
            endpoint.name,
            describe_packet_summary(summary),
            packet.frame_hex,
            packet.packet_hex,
        )
        self.database.insert_raw_packet(
            endpoint_name=endpoint.name,
            observed_at=packet.observed_at,
            direction="rx",
            transport="rs232bridge",
            rs232_frame_hex=packet.frame_hex,
            mesh_packet_hex=packet.packet_hex,
            payload_type=int(summary.payload_type),
            route_type=int(summary.route_type),
        )

        if summary.payload_type.name != "ADVERT":
            return

        self.stats.adverts_seen += 1
        try:
            advert = parse_advert(summary)
        except Exception as exc:
            self.logger.debug("invalid advert on %s: %s", endpoint.name, exc)
            return

        if advert.advert_type is not AdvertType.REPEATER:
            return

        self.stats.repeater_adverts_seen += 1
        self.logger.info(
            "[ADVERT] endpoint=%s repeater=%s pubkey=%s path_len=%s path=%s",
            endpoint.name,
            (advert.name or advert.public_key.hex().upper()[:8]).strip(),
            advert.public_key.hex().upper()[:12],
            summary.path_len,
            summary.path_bytes.hex().upper() or "-",
        )
        repeater_id = self.database.upsert_repeater_from_advert(
            endpoint_name=endpoint.name,
            observed_at=packet.observed_at,
            public_key=advert.public_key,
            advert_name=advert.name,
            advert_lat=advert.latitude,
            advert_lon=advert.longitude,
            advert_timestamp_remote=advert.timestamp,
            path_len=summary.path_len,
            path_hex=summary.path_bytes.hex().upper(),
            raw_packet_hex=packet.packet_hex,
        )
        probe_endpoint = await self._local_console_resolver.resolve_endpoint(advert.name)
        target_endpoint_name = probe_endpoint.name if probe_endpoint is not None else endpoint.name
        job_id = self.database.enqueue_probe_job(
            repeater_id=repeater_id,
            endpoint_name=target_endpoint_name,
            reason="repeater advert observed",
            success_cooldown_secs=self.config.probe.advert_reprobe_success_cooldown_secs,
            failure_cooldown_secs=self.config.probe.advert_reprobe_failure_cooldown_secs,
            scheduled_at=self._planned_advert_probe_time(target_endpoint_name, packet.observed_at, repeater_id, summary.path_len, summary.path_bytes.hex().upper()),
            max_recent_jobs=self.config.probe.automatic_probe_max_per_day,
        ) if self._should_enqueue_advert_probe(
            repeater_id=repeater_id,
            endpoint_name=target_endpoint_name,
            observed_at=packet.observed_at,
            current_path_len=summary.path_len,
            current_path_hex=summary.path_bytes.hex().upper(),
        ) else None
        if job_id is not None:
            self.stats.jobs_enqueued += 1
            self.logger.info(
                "[PROBE-QUEUE] job=%s repeater=%s via=%s",
                job_id,
                advert.public_key.hex().upper()[:12],
                target_endpoint_name,
            )

    def _should_enqueue_advert_probe(
        self,
        *,
        repeater_id: int,
        endpoint_name: str,
        observed_at: str,
        current_path_len: int,
        current_path_hex: str,
    ) -> bool:
        state = self.database.repeater_probe_state(repeater_id=repeater_id)
        if state is None:
            return True
        last_probe_at = str(state.get("last_probe_at") or "") or None
        last_probe_status = str(state.get("last_probe_status") or "") or None
        if last_probe_at is None:
            return True
        if last_probe_status in {"failed", "interrupted", "running"}:
            return True

        current_path = self._normalized_path(current_path_len, current_path_hex)
        latest_path_row = self.database.latest_repeater_path(repeater_id=repeater_id)
        latest_probe_path = self._normalized_path_from_row(latest_path_row, len_key="out_path_len", hex_key="out_path_hex")
        if latest_probe_path is not None and current_path != latest_probe_path:
            if is_recent_observation(last_probe_at, self.config.probe.advert_path_change_cooldown_secs):
                self.stats.advert_jobs_skipped_recent_path_change += 1
                return False
            return True

        recent_adverts = self.database.recent_repeater_adverts(repeater_id=repeater_id, endpoint_name=endpoint_name, limit=2)
        if len(recent_adverts) >= 2:
            previous_path = self._normalized_path_from_row(recent_adverts[1], len_key="path_len", hex_key="path_hex")
            if previous_path is not None and current_path != previous_path:
                if is_recent_observation(last_probe_at, self.config.probe.advert_path_change_cooldown_secs):
                    self.stats.advert_jobs_skipped_recent_path_change += 1
                    return False
                return True

        self.stats.advert_jobs_skipped_stable += 1
        return False

    def _planned_advert_probe_time(
        self,
        endpoint_name: str,
        observed_at: str,
        repeater_id: int,
        current_path_len: int,
        current_path_hex: str,
    ) -> str:
        observed = self._parse_iso_timestamp(observed_at)
        min_interval = self.config.probe.advert_probe_min_interval_secs
        if min_interval <= 0:
            return observed.isoformat()
        next_slot = self._next_advert_probe_slot_at.get(endpoint_name)
        scheduled = observed if next_slot is None or observed >= next_slot else next_slot
        if scheduled > observed:
            self.stats.advert_jobs_deferred += 1
            self.logger.info(
                "[PROBE-DEFER] endpoint=%s repeater_id=%s delay=%.1fs path=%s/%s",
                endpoint_name,
                repeater_id,
                (scheduled - observed).total_seconds(),
                current_path_len,
                current_path_hex or "-",
            )
        self._next_advert_probe_slot_at[endpoint_name] = scheduled + timedelta(seconds=min_interval)
        return scheduled.isoformat()

    def _normalized_path_from_row(self, row: dict[str, object] | None, *, len_key: str, hex_key: str) -> tuple[int, str] | None:
        if row is None:
            return None
        return self._normalized_path(int(row.get(len_key) or 0), str(row.get(hex_key) or ""))

    def _normalized_path(self, path_len: int, path_hex: str) -> tuple[int, str]:
        return max(0, int(path_len)), str(path_hex or "").strip().upper()

    def _parse_iso_timestamp(self, value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
