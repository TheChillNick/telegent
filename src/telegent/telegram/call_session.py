"""State machine dan pipeline audio satu panggilan (satu instance per panggilan)."""

from __future__ import annotations

import asyncio
import time
from contextlib import AbstractAsyncContextManager
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from telegent.ai.event_reader import read_events
from telegent.ai.events import (
    AudioOut,
    Interrupted,
    ToolCallCancelled,
    ToolCallEvent,
    TranscriptIn,
    TranscriptOut,
    TurnComplete,
)
from telegent.ai.tools.registry import ToolRegistry
from telegent.ai.tools.spec import ToolContext, ToolRequest
from telegent.audio.cadence import Cadence, run_cadence_sender
from telegent.audio.framing import PcmFramer
from telegent.audio.resampler import pcm24k_to_pcm48k, pcm48k_to_pcm16k
from telegent.config import Settings
from telegent.protocols import AudioSink, LiveVoiceSession

# 20 ms @ 48 kHz mono s16le (audio masuk dari Telegram)
INBOUND_FRAME_BYTES = 1920
# 20 ms @ 16 kHz mono s16le (comfort silence ke Gemini)
GEMINI_SILENCE_16K = b"\x00" * 640

# Pengakhiran panggilan: tunggu audio jawaban benar-benar habis sebelum menutup.
HANGUP_QUIET_SEC = 0.5
HANGUP_MAX_WAIT = 15.0

# Anti-idle: konfirmasi bila penelpon diam, lalu akhiri bila tetap tidak ada respons.
IDLE_CHECK_INTERVAL = 0.5
IDLE_AI_QUIET_SEC = 1.5
IDLE_FAREWELL_MIN_WAIT = 6.0

LiveFactory = Callable[[], AbstractAsyncContextManager[LiveVoiceSession]]


class SessionState(Enum):
    IDLE = "IDLE"
    INCOMING_CALL = "INCOMING_CALL"
    CONNECTING = "CONNECTING"
    ACTIVE = "ACTIVE"
    ENDING = "ENDING"


class CallSession:
    """Mengelola lifecycle, media, dan sesi AI untuk satu panggilan.

    Semua state bersifat per-instance sehingga aman untuk banyak panggilan
    bersamaan.
    """

    def __init__(
        self,
        *,
        user_id: int,
        call_id: int,
        access_hash: int,
        sink: AudioSink,
        live_factory: LiveFactory,
        tools: ToolRegistry,
        settings: Settings,
        logger: Any,
        on_stop: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        self.user_id = user_id
        self.call_id = call_id
        self.access_hash = access_hash
        self._sink = sink
        self._live_factory = live_factory
        self._tools = tools
        self._settings = settings
        self._log = logger
        self._on_stop = on_stop

        self.state = SessionState.INCOMING_CALL
        self.gemini_in_queue: asyncio.Queue = asyncio.Queue(maxsize=150)
        self.tg_out_queue: asyncio.Queue = asyncio.Queue(maxsize=1500)

        self._inbound_framer = PcmFramer(INBOUND_FRAME_BYTES)
        self._outbound_framer = PcmFramer(settings.tg_frame_bytes)
        self._tasks: list[asyncio.Task] = []
        self._tool_tasks: dict[str, asyncio.Task] = {}
        self._live: Optional[LiveVoiceSession] = None
        self._live_task: Optional[asyncio.Task] = None
        self._ending = asyncio.Event()
        self._closing = False

        # Pengakhiran panggilan oleh AI (tool end_call) / watchdog anti-idle.
        self._hangup_task: Optional[asyncio.Task] = None
        self._hangup_scheduled = False
        self._hangup_started = 0.0
        self._hangup_min_wait = 0.0
        self._connected_at: Optional[float] = None
        self._last_output_activity = 0.0
        self.on_hangup: Optional[Callable[[], Awaitable[None]]] = None

        # Anti-idle: penelpon tidak menjawab.
        self._last_caller_activity = 0.0
        self._idle_prompted = False
        self._idle_deadline = 0.0

        self.is_greeting = True
        self.inbound_frame_count = 0

        # Signaling WebRTC hanya dikirim setelah call aktif di server Telegram.
        self.signaling_ready = False
        self.pending_signaling: list[bytes] = []

    # ------------------------------------------------------------------ util
    def set_state(self, new_state: SessionState) -> None:
        self._log.info("Transisi status: %s -> %s", self.state.value, new_state.value)
        self.state = new_state

    @staticmethod
    def _drain_queue(queue: asyncio.Queue) -> None:
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    def flush_outbound(self) -> None:
        """Kosongkan audio keluar saat barge-in."""
        dropped = 0
        self._outbound_framer.clear()
        while not self.tg_out_queue.empty():
            try:
                self.tg_out_queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break
        if dropped:
            self._log.info("Barge-in: memotong %d frame audio respons.", dropped)

    def finish_greeting(self) -> None:
        """Akhiri mode sapaan dan bersihkan buffer input yang menumpuk."""
        if not self.is_greeting:
            return
        self.is_greeting = False
        self._last_caller_activity = time.monotonic()
        self._drain_queue(self.gemini_in_queue)
        self._inbound_framer.clear()
        self._log.info("Sapaan selesai; AI siap mendengarkan.")

    # --------------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Mulai sesi AI (dipanggil setelah koneksi media P2P siap)."""
        if self._live_task is None:
            self._live_task = asyncio.create_task(self._run_live(), name=f"live-{self.user_id}")

    async def _run_live(self) -> None:
        try:
            async with self._live_factory() as live:
                self._live = live
                self._connected_at = time.monotonic()
                self._last_caller_activity = self._connected_at
                self.set_state(SessionState.ACTIVE)
                try:
                    await live.send_greeting(self._settings.greeting_prompt)
                except Exception as exc:  # noqa: BLE001
                    self._log.warning("Gagal mengirim sapaan awal: %s", exc)

                self._tasks.append(asyncio.create_task(self._gemini_sender_worker()))
                self._tasks.append(asyncio.create_task(self._gemini_receiver_worker()))
                self._tasks.append(asyncio.create_task(self._telegram_audio_sender_worker()))
                self._tasks.append(asyncio.create_task(self._auto_clear_greeting()))
                self._tasks.append(asyncio.create_task(self._idle_watchdog()))
                self._tasks.append(asyncio.create_task(self._enforce_max_duration()))

                if self._settings.kai_events_file:
                    self._tasks.append(asyncio.create_task(read_events(
                        self._settings.kai_events_file, self._send_prompt)))
                await self._ending.wait()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log.exception("Kesalahan sesi live: %s", exc)
        finally:
            await self.close()

    async def _auto_clear_greeting(self) -> None:
        await asyncio.sleep(4.0)
        self.finish_greeting()

    async def _enforce_max_duration(self) -> None:
        """Akhiri panggilan secara paksa setelah `max_call_duration` detik."""
        limit = self._settings.max_call_duration
        if limit <= 0:
            return
        try:
            await asyncio.sleep(limit)
        except asyncio.CancelledError:
            return
        if self.state == SessionState.ACTIVE and not self._closing:
            self._log.warning("Batas durasi panggilan (%.0f dtk) tercapai.", limit)
            self.schedule_hangup()

    async def close(self) -> None:
        """Tutup sesi (idempoten)."""
        if self._closing:
            return
        self._closing = True
        self.set_state(SessionState.ENDING)
        self._ending.set()

        current = asyncio.current_task()

        for task in list(self._tool_tasks.values()):
            if task is not current:
                task.cancel()
        self._tool_tasks.clear()

        cancellable = [task for task in self._tasks if task is not current]
        for task in cancellable:
            task.cancel()
        if cancellable:
            await asyncio.gather(*cancellable, return_exceptions=True)
        self._tasks.clear()

        if self._live_task is not None and self._live_task is not current:
            self._live_task.cancel()
        if self._hangup_task is not None and self._hangup_task is not current:
            self._hangup_task.cancel()

        self._inbound_framer.clear()
        self._outbound_framer.clear()
        self.pending_signaling.clear()
        self.flush_outbound()

        if self._on_stop is not None:
            try:
                await self._on_stop()
            except Exception as exc:  # noqa: BLE001
                self._log.warning("Gagal menghentikan media: %s", exc)

        self.set_state(SessionState.IDLE)

    # --------------------------------------------------------------- workers
    async def _gemini_sender_worker(self) -> None:
        """Kirim suara penelpon (dengan comfort silence) ke Gemini secara kontinu."""
        cadence = Cadence(0.020)
        sent = 0
        try:
            while self.state == SessionState.ACTIVE:
                if self.is_greeting:
                    self._drain_queue(self.gemini_in_queue)
                    cadence.reset()
                    await asyncio.sleep(0.02)
                    continue

                try:
                    chunk = self.gemini_in_queue.get_nowait()
                    sent += 1
                    if sent % 50 == 0:
                        self._log.debug("Mengirim %d chunk suara penelpon ke Gemini.", sent)
                except asyncio.QueueEmpty:
                    chunk = GEMINI_SILENCE_16K

                if self._live is not None:
                    try:
                        await self._live.send_audio(chunk)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        self._log.warning("Gagal mengirim audio ke Gemini: %s", exc)
                        break
                await cadence.wait(0.1)
        except asyncio.CancelledError:
            pass

    async def _gemini_receiver_worker(self) -> None:
        if self._live is None:
            return
        try:
            async for event in self._live.events():
                if self.state != SessionState.ACTIVE:
                    break
                if isinstance(event, AudioOut):
                    pcm48 = pcm24k_to_pcm48k(event.pcm24k)
                    for frame in self._outbound_framer.push(pcm48):
                        await self._enqueue_outbound(frame)
                elif isinstance(event, TranscriptIn):
                    self._log.info("[Penelpon] %s", event.text)
                    self._note_caller_activity()
                elif isinstance(event, TranscriptOut):
                    self._log.info("[AI] %s", event.text)
                elif isinstance(event, Interrupted):
                    self._note_caller_activity()
                    if not self.is_greeting:
                        self.flush_outbound()
                elif isinstance(event, ToolCallEvent):
                    self._spawn_tool(event)
                elif isinstance(event, ToolCallCancelled):
                    self._cancel_tool(event.id)
                elif isinstance(event, TurnComplete):
                    self._last_output_activity = time.monotonic()
                    if self.is_greeting:
                        self.finish_greeting()
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            self._log.exception("Kesalahan penerima Gemini: %s", exc)

    async def _enqueue_outbound(self, frame: bytes) -> None:
        self._last_output_activity = time.monotonic()
        queue = self.tg_out_queue
        if not queue.full():
            queue.put_nowait(frame)
            return
        try:
            await asyncio.wait_for(queue.put(frame), timeout=5.0)
        except asyncio.TimeoutError:
            # Konsumen macet (jarang): buang frame tertua sebagai jalan terakhir.
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                queue.put_nowait(frame)
            except asyncio.QueueFull:
                pass

    async def _telegram_audio_sender_worker(self) -> None:
        await run_cadence_sender(
            sink=self._sink,
            source=self.tg_out_queue,
            is_active=lambda: self.state == SessionState.ACTIVE,
            frame_bytes=self._settings.tg_frame_bytes,
            interval=0.010,
            prefill_frames=15,
            logger=self._log,
        )

    # --------------------------------------------------------------- input media
    def handle_incoming_telegram_pcm(self, pcm_bytes: bytes) -> None:
        """Dipanggil di thread asyncio utama saat paket PCM masuk dari penelpon."""
        if self.state != SessionState.ACTIVE:
            return
        self.inbound_frame_count += 1
        if self.inbound_frame_count == 1:
            self._log.info("Audio pertama dari penelpon diterima.")

        for chunk48 in self._inbound_framer.push(pcm_bytes):
            chunk16 = pcm48k_to_pcm16k(chunk48)
            if self.gemini_in_queue.full():
                try:
                    self.gemini_in_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                self.gemini_in_queue.put_nowait(chunk16)
            except asyncio.QueueFull:
                pass

    # --------------------------------------------------------------- tools
    def _spawn_tool(self, event: ToolCallEvent) -> None:
        request = ToolRequest(id=event.id, name=event.name, args=event.args)
        task = asyncio.create_task(self._run_tool(request), name=f"tool-{event.name}-{event.id}")
        self._tool_tasks[event.id] = task

    async def _run_tool(self, request: ToolRequest) -> None:
        self._log.info("Menjalankan tool '%s' args=%s", request.name, request.args)
        ctx = ToolContext(
            user_id=self.user_id,
            call_id=self.call_id,
            logger=self._log,
            hangup=self.schedule_hangup,
        )
        try:
            result = await self._tools.execute(ctx, request)
        finally:
            self._tool_tasks.pop(request.id, None)

        if self._live is not None:
            try:
                await self._live.send_tool_response([result])
                self._log.info("Tool '%s' selesai: %s", request.name, result.response)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._log.warning("Gagal mengirim hasil tool '%s': %s", request.name, exc)

    def _cancel_tool(self, tool_id: str) -> None:
        task = self._tool_tasks.pop(tool_id, None)
        if task is not None:
            task.cancel()

    # --------------------------------------------------------------- hangup
    def schedule_hangup(self, min_wait: float = 0.0) -> None:
        """Jadwalkan pengakhiran panggilan setelah audio jawaban selesai diputar.

        `min_wait` memberi tenggang minimal (detik) sebelum panggilan ditutup —
        dipakai saat kita minta AI mengucapkan pamitan lebih dulu. Sinkron: aman
        dipanggil dari handler tool (tidak memblokir penerimaan).
        """
        if self._hangup_scheduled or self._closing:
            return
        self._hangup_scheduled = True
        now = time.monotonic()
        self._hangup_started = now
        self._hangup_min_wait = min_wait
        self._last_output_activity = now
        self._log.info("Permintaan mengakhiri panggilan; menunggu audio habis diputar.")
        self._hangup_task = asyncio.create_task(
            self._hangup_when_drained(), name=f"hangup-{self.user_id}"
        )

    async def _hangup_when_drained(self) -> None:
        # Tunggu sampai tidak ada aktivitas audio/giliran selama HANGUP_QUIET_SEC
        # (agar kalimat pamitan & lanjutan model tidak terpotong), dan minimal
        # `min_wait` sejak penjadwalan.
        deadline = time.monotonic() + HANGUP_MAX_WAIT
        while time.monotonic() < deadline:
            now = time.monotonic()
            idle = now - self._last_output_activity
            waited = now - self._hangup_started
            if (
                self.tg_out_queue.empty()
                and idle >= HANGUP_QUIET_SEC
                and waited >= self._hangup_min_wait
            ):
                break
            await asyncio.sleep(0.05)

        self._log.info("Audio selesai; mengakhiri panggilan.")
        if self.on_hangup is not None:
            try:
                await self.on_hangup()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self._log.exception("Gagal mengakhiri panggilan.")

    # --------------------------------------------------------------- anti-idle
    def _note_caller_activity(self) -> None:
        """Catat bahwa penelpon bersuara (mereset timer diam & status konfirmasi)."""
        self._last_caller_activity = time.monotonic()
        self._idle_prompted = False

    async def _send_prompt(self, text: str) -> None:
        if self._live is None:
            return
        try:
            await self._live.send_prompt(text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log.warning("Gagal mengirim prompt: %s", exc)

    async def _idle_watchdog(self) -> None:
        """Konfirmasi bila penelpon diam; akhiri bila tetap tidak ada respons."""
        if not self._settings.idle_enabled:
            return
        try:
            while self.state == SessionState.ACTIVE:
                await asyncio.sleep(IDLE_CHECK_INTERVAL)
                if self.is_greeting:
                    continue

                now = time.monotonic()
                if not self._idle_prompted:
                    caller_idle = now - self._last_caller_activity
                    ai_idle = now - self._last_output_activity
                    if (
                        caller_idle >= self._settings.idle_prompt_seconds
                        and ai_idle >= IDLE_AI_QUIET_SEC
                        and self.tg_out_queue.empty()
                        and not self._tool_tasks
                    ):
                        self._idle_prompted = True
                        self._idle_deadline = now + self._settings.idle_hangup_seconds
                        self._log.info(
                            "Penelpon diam %.0f dtk; mengirim konfirmasi.", caller_idle
                        )
                        await self._send_prompt(self._settings.idle_prompt_instruction)
                elif now >= self._idle_deadline:
                    self._log.info("Penelpon tetap tidak menjawab; mengakhiri panggilan.")
                    self._idle_prompted = False
                    await self._send_prompt(self._settings.idle_farewell_instruction)
                    self.schedule_hangup(min_wait=IDLE_FAREWELL_MIN_WAIT)
                    break
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            self._log.exception("Idle watchdog error: %s", exc)

    @property
    def duration_seconds(self) -> int:
        if self._connected_at is None:
            return 0
        return int(max(0.0, time.monotonic() - self._connected_at))
