"""Application bootstrap."""

from __future__ import annotations

import asyncio
import logging

import uvicorn

from .config import AppConfig
from .service import MeshcoreTCPBotService
from .web import create_app


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def run(config: AppConfig) -> None:
    configure_logging(config.logging.level)
    service = MeshcoreTCPBotService(config)
    await service.start()

    web_server: uvicorn.Server | None = None
    web_task: asyncio.Task | None = None
    if config.web.enabled:
        app = create_app(service)
        uvicorn_config = uvicorn.Config(app, host=config.web.host, port=config.web.port, log_level=config.logging.level.lower())
        web_server = uvicorn.Server(uvicorn_config)
        web_task = asyncio.create_task(web_server.serve(), name="uvicorn")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        raise
    finally:
        if web_server is not None:
            web_server.should_exit = True
        if web_task is not None:
            await web_task
        await service.stop()