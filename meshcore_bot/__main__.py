from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from .app import BotApplication
from .config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="meshcore-bot runtime")
    parser.add_argument(
        "--config",
        default="config/config.toml",
        help="Path to TOML configuration file",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, config.service.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = BotApplication(config)
    asyncio.run(_run(app))


async def _run(app: BotApplication) -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop() -> None:
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_stop)

    app_task = asyncio.create_task(app.run())
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        {app_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

    if stop_task in done:
        await app.shutdown()

    await app_task


if __name__ == "__main__":
    main()