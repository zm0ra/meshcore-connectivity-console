from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import uvicorn

from .config import AppConfig
from .database import BotDatabase
from .service import MeshcoreRuntimeService
from .web import create_app


class BotApplication:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(config.service.name)
        self._shutdown_event = asyncio.Event()
        self.database = BotDatabase(config.storage.database_path)
        self.service: MeshcoreRuntimeService | None = None
        self._web_server: uvicorn.Server | None = None
        self._web_task: asyncio.Task[Any] | None = None
        self.log_file_path = self.config.storage.logs_dir / f"{self.config.service.name}.log"

    async def run(self) -> None:
        self._prepare_directories()
        self.database.bootstrap_from_config(self.config)
        self.logger.info("starting %s", self.config.service.name)

        self.service = MeshcoreRuntimeService(self.config, self.database, self.log_file_path)
        await self.service.start()

        app = create_app(self.service, self.log_file_path)
        self._web_server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=self.config.web.host,
                port=self.config.web.port,
                log_level=self.config.service.log_level.lower(),
            )
        )
        self._web_task = asyncio.create_task(self._web_server.serve(), name="uvicorn")

        try:
            await self._shutdown_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if self._web_server is not None:
            self._web_server.should_exit = True
        if self._web_task is not None:
            await self._web_task
            self._web_task = None
        if self.service is not None:
            await self.service.stop()
            self.service = None
        self._shutdown_event.set()
        self.logger.info("shutdown complete")

    def _prepare_directories(self) -> None:
        self.config.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.storage.logs_dir.mkdir(parents=True, exist_ok=True)