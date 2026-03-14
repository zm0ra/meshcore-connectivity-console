from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Callable

from .config import AppConfig, EndpointConfig
from .database import BotDatabase
from .mesh_packets import AdvertType, describe_packet_summary, parse_advert
from .tcp_client import MeshcoreTCPClient, ReceivedPacket
from .transport import PacketTransportClient


@dataclass(slots=True)
class IngestStats:
    packets_seen: int = 0
    adverts_seen: int = 0
    repeater_adverts_seen: int = 0
    jobs_enqueued: int = 0


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
        self.logger.info(
            "decoded rx frame endpoint=%s %s frame=%s packet=%s",
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
        job_id = self.database.enqueue_probe_job(
            repeater_id=repeater_id,
            endpoint_name=endpoint.name,
            reason="repeater advert observed",
            cooldown_secs=self.config.probe.advert_reprobe_cooldown_secs,
        )
        if job_id is not None:
            self.stats.jobs_enqueued += 1
            self.logger.info(
                "queued probe job %s for repeater %s via %s",
                job_id,
                advert.public_key.hex().upper()[:12],
                endpoint.name,
            )
