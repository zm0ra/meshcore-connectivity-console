from __future__ import annotations

import asyncio
import contextlib


def normalize_console_reply(transcript: str, command: str) -> str:
    lines: list[str] = []
    for raw_line in transcript.replace("\r", "").split("\n"):
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("MeshCore repeater console"):
            continue
        if line == command:
            continue
        if line == ">":
            continue
        while True:
            if line.startswith("> "):
                line = line[2:]
                continue
            if line.startswith("-> "):
                line = line[3:]
                continue
            if line.startswith("  -> "):
                line = line[5:]
                continue
            break
        line = line.strip()
        if line == command:
            continue
        if not line or line == ">" or line == "->":
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def parse_console_neighbors_reply(reply: str) -> list[dict[str, object]]:
    text = reply.strip()
    if not text or text == "-none-":
        return []

    neighbors: list[dict[str, object]] = []
    for line in text.splitlines():
        parts = line.strip().split(":", 2)
        if len(parts) != 3:
            continue
        prefix_hex, heard_seconds_text, snr_text = parts
        try:
            heard_seconds = int(heard_seconds_text)
            snr_raw = int(snr_text)
        except ValueError:
            continue
        neighbors.append(
            {
                "neighbor_hash_prefix": prefix_hex.upper(),
                "last_heard_seconds": heard_seconds,
                "snr": snr_raw / 4.0,
            }
        )
    return neighbors


def parse_console_text_reply(reply: str) -> str:
    return reply.strip()


async def run_console_command(host: str, port: int, command: str, *, timeout: float) -> str:
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    try:
        await _drain_console_banner(reader, timeout=min(timeout, 0.5))
        writer.write((command + "\n").encode("utf-8"))
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        transcript = await _read_console_reply(reader, timeout=timeout)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
    return normalize_console_reply(transcript, command)


async def _drain_console_banner(reader: asyncio.StreamReader, *, timeout: float) -> None:
    if timeout <= 0:
        return
    with contextlib.suppress(Exception):
        await asyncio.wait_for(reader.read(4096), timeout=timeout)


async def _read_console_reply(reader: asyncio.StreamReader, *, timeout: float) -> str:
    deadline = asyncio.get_running_loop().time() + timeout
    chunks: list[bytes] = []
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=remaining)
        except asyncio.TimeoutError:
            break
        if not data:
            break
        chunks.append(data)
        joined = b"".join(chunks).replace(b"\r", b"")
        if b"\n>" in joined or joined.rstrip().endswith(b">"):
            break
    return b"".join(chunks).decode("utf-8", errors="replace")