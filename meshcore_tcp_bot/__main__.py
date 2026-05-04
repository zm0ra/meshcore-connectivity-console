"""Command line entry point."""

from __future__ import annotations

import argparse
import asyncio

from .app import run
from .config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MeshCore TCP bot for XIAO WiFi repeaters")
    parser.add_argument("--config", default="config/config.toml", help="Path to TOML configuration file")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    asyncio.run(run(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())