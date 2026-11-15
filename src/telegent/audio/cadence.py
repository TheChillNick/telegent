"""Pacing pengiriman audio pada cadence stabil (anti jitter-buffer underrun).

Frame HARUS dikirim pada cadence tetap (mis. 10 ms). Berhenti mengirim sebentar
saja membuat jitter buffer WebRTC di sisi penelpon underrun sehingga suara
tersendat. Karena itu, bila belum ada audio, kita tetap mengirim frame SENYAP.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Optional

from telegent.audio.framing import PcmFramer


class Cadence:
    """Penjaga pacing berbasis deadline absolut (mencegah drift)."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self._next = time.monotonic()

    def reset(self) -> None:
        self._next = time.monotonic()

    async def wait(self, max_lag: float = 0.05) -> None:
        now = time.monotonic()
        if now < self._next:
            await asyncio.sleep(self._next - now)
        elif now - self._next > max_lag:
            # Jatuh terlalu jauh di belakang: reset agar tidak mengejar secara burst.
            self._next = now
        self._next += self.interval


async def run_cadence_sender(
    *,
    sink: Any,
    source: asyncio.Queue,
    is_active: Callable[[], bool],
    frame_bytes: int,
    interval: float = 0.010,
    prefill_frames: int = 15,
    send: Optional[Callable[[bytes], Awaitable[None]]] = None,
    logger: Any = None,
    reason: Optional[Callable[[BaseException], object]] = None,
) -> None:
    """Kirim frame ke `sink` pada cadence stabil; isi senyap bila antrean kosong.

    `prefill_frames` hanya menahan audio AI (mengirim senyap) agar onset mulus,
    TIDAK menghentikan pengiriman frame.
    """
    silence = b"\x00" * frame_bytes
    cadence = Cadence(interval)
    sender = send if send is not None else sink.send_frame
    is_playing = False

    while is_active():
        frame = silence
        if is_playing:
            try:
                frame = source.get_nowait()
            except asyncio.QueueEmpty:
                is_playing = False
        if not is_playing and source.qsize() >= prefill_frames:
            is_playing = True
            try:
                frame = source.get_nowait()
            except asyncio.QueueEmpty:
                is_playing = False
                frame = silence

        await cadence.wait()
        try:
            await sender(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - jangan matikan pengiriman karena 1 error
            if logger is not None:
                logger.warning("Gagal mengirim frame audio: %s", reason(exc) if reason is not None else exc)
