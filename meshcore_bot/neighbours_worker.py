from __future__ import annotations

import asyncio

from .config import AppConfig
from .database import BotDatabase
from .gateway_client import GatewayTransportClient
from .ingest_service import AdvertIngestService
from .probe_service import GuestProbeWorker


class NeighboursWorkerApp:
    def __init__(self, config: AppConfig, database: BotDatabase) -> None:
        self.config = config
        self.database = database

    async def run(self) -> None:
        transport_factory = self._build_gateway_transport
        ingest = AdvertIngestService(self.config, self.database, transport_factory=transport_factory)
        probe = GuestProbeWorker(self.config, self.database, transport_factory=transport_factory)
        await asyncio.gather(ingest.run(), probe.run())

    def _build_gateway_transport(self, endpoint) -> GatewayTransportClient:
        return GatewayTransportClient(
            endpoint_name=endpoint.name,
            control_socket_path=self.config.gateway.control_socket_path,
            event_socket_path=self.config.gateway.event_socket_path,
            traffic_class="probe",
        )