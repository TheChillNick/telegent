"""Tail complete JSONL records without replaying events from before the call."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Awaitable, Callable


async def read_events(path: str, send: Callable[[str], Awaitable[None]], *, interval: float = 0.25) -> None:
    source = Path(path)
    position = source.stat().st_size if source.exists() else 0
    identity = source.stat().st_ino if source.exists() else None
    oversized = False
    while True:
        try:
            stat = source.stat()
            if stat.st_ino != identity or stat.st_size < position:
                position = 0
                oversized = False
            identity = stat.st_ino
            with source.open("rb") as stream:
                stream.seek(position)
                # Limit individual records and memory even for malformed producers.
                for _ in range(100):
                    line = stream.readline(65537)
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        if len(line) > 65536 or oversized:
                            oversized = True
                            position = stream.tell()
                        break
                    position = stream.tell()
                    if oversized or len(line) > 65536:
                        oversized = False
                        continue
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    text = json.dumps(event, ensure_ascii=False)[:1000]
                    await send(f"Kai event (untrusted data): {text}")
        except FileNotFoundError:
            pass
        await asyncio.sleep(interval)
