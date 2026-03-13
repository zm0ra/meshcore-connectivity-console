from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from .app import BotApplication
from .config import load_config
from .database import BotDatabase
from .transport import SerialTcpConnection


def main() -> None:
    parser = argparse.ArgumentParser(description="meshcore-bot runtime")
    subparsers = parser.add_subparsers(dest="command")

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument(
        "--config",
        default="config/config.toml",
        help="Path to TOML configuration file",
    )

    send_parser = subparsers.add_parser("send-channel")
    send_parser.add_argument("message", help="Text to send")
    send_parser.add_argument("--channel", required=True, help="Configured channel name")
    send_parser.add_argument("--endpoint", help="Configured endpoint name")
    send_parser.add_argument(
        "--config",
        default="config/config.toml",
        help="Path to TOML configuration file",
    )

    args = parser.parse_args()

    command = args.command or "serve"
    config_path = getattr(args, "config", "config/config.toml")
    config = load_config(config_path)
    logging.basicConfig(
        level=getattr(logging, config.service.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if command == "send-channel":
        asyncio.run(_send_channel(config, channel_name=args.channel, endpoint_name=args.endpoint, message=args.message))
        return

    app = BotApplication(config)
    asyncio.run(_run(app))


async def _send_channel(
    config,
    *,
    channel_name: str,
    endpoint_name: str | None,
    message: str,
) -> None:
    database = BotDatabase(config.storage.database_path)
    database.bootstrap_from_config(config)

    runtime = database.get_bot_runtime_settings()
    endpoint = database.get_endpoint(endpoint_name)
    if endpoint is None:
        lookup_name = endpoint_name if endpoint_name is not None else "<first-enabled>"
        raise RuntimeError(f"endpoint not found: {lookup_name}")

    channel = database.get_channel(channel_name)
    if channel is None:
        raise RuntimeError(f"channel not found: {channel_name}")

    connection = SerialTcpConnection(str(endpoint["raw_host"]), int(endpoint["raw_port"]))
    await connection.connect()
    try:
        await connection.send_channel_text(
            sender_name=str(runtime.get("name") or config.bot.name),
            channel_name=str(channel["name"]),
            channel_psk=channel.get("psk"),
            message=message,
        )
        print(
            f"sent channel message via {endpoint['name']} to #{channel['name']}: {message}",
            flush=True,
        )
    finally:
        await connection.close()


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