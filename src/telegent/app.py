"""Orkestrasi Telegent: klien Telegram, media ntgcalls, dan dispatch panggilan."""

from __future__ import annotations

import asyncio
import signal
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, AsyncIterator, Optional

import ntgcalls
from google import genai
from google.genai import types as genai_types
from pyrogram import Client
from pyrogram.handlers import RawUpdateHandler
from pyrogram.raw import functions, types

from telegent.ai.live_session import GeminiLiveSession
from telegent.ai.tools.builtin import default_tools
from telegent.ai.tools.kai import KaiTools
from telegent.ai.tools.registry import ToolRegistry
from telegent.config import Settings
from telegent.logging_setup import call_logger, get_logger
from telegent.rate_limit import CallRateLimiter
from telegent.telegram.call_session import CallSession, SessionState
from telegent.telegram.media import NtgAudioSink, external_audio_media
from telegent.telegram.signaling import SignalingService, to_ntg_rtc_server, to_tg_protocol


class TelegentApp:
    """Aplikasi utama: menerima panggilan Telegram dan menjalankan sesi AI per panggilan."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._log = get_logger("telegent.app")
        self.app: Optional[Client] = None
        self.ntg: Optional[ntgcalls.NTgCalls] = None
        self.gemini_client: Optional[genai.Client] = None
        self.signaling: Optional[SignalingService] = None
        self.tools = ToolRegistry()
        for spec in default_tools():
            self.tools.register(spec)
        self.kai_tools = KaiTools(settings.kai_api_base, settings.kai_api_token)
        if settings.kai_api_base:
            for spec in self.kai_tools.specs():
                self.tools.register(spec)
        self.sessions: dict[int, CallSession] = {}
        self.rate_limiter = CallRateLimiter(
            settings.max_calls_per_window, settings.call_window_seconds
        )
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = asyncio.Event()

    # ---------------------------------------------------------------- lifecycle
    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.app = Client(
            self.settings.telegram_session_name,
            api_id=self.settings.telegram_api_id,
            api_hash=self.settings.telegram_api_hash,
            workdir=str(self.settings.session_workdir),
        )
        self.ntg = ntgcalls.NTgCalls()
        self.gemini_client = genai.Client(api_key=self.settings.gemini_api_key)
        self.signaling = SignalingService(self.app, self.loop, self._log)

        self._setup_ntg_callbacks()
        self._setup_handlers()
        self._install_signal_handlers()

        await self.app.start()
        me = await self.app.get_me()
        self._log.info("Login sebagai: %s (@%s, id=%s)", me.first_name, me.username or "-", me.id)
        self._log.info(
            "Telegent aktif. Menunggu panggilan (maks %d bersamaan, model=%s).",
            self.settings.max_concurrent_calls,
            self.settings.gemini_model,
        )

        await self._stop.wait()
        await self._shutdown()

    async def _shutdown(self) -> None:
        self._log.info("Mematikan; menutup %d sesi aktif.", len(self.sessions))
        for session in list(self.sessions.values()):
            await session.close()
        self.sessions.clear()
        if self.app is not None:
            try:
                await self.app.stop()
            except Exception as exc:  # noqa: BLE001
                self._log.warning("Gagal menutup klien Telegram: %s", exc)

    def _install_signal_handlers(self) -> None:
        assert self.loop is not None
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self.loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError):
                pass

    # ---------------------------------------------------------------- callbacks
    def _setup_ntg_callbacks(self) -> None:
        ntg = self.ntg
        signaling = self.signaling
        loop = self.loop
        if ntg is None or signaling is None or loop is None:
            return

        def on_signaling_data(user_id: int, data: bytes) -> None:
            # Callback native (thread ntgcalls): hanya dispatch ke loop utama,
            # jangan sentuh state sesi dari thread ini.
            if not loop.is_closed():
                loop.call_soon_threadsafe(self._on_signaling_data, user_id, data)

        def on_connection_change(user_id: int, info: ntgcalls.ConnectionInfo) -> None:
            state_name = str(info.state).split(".")[-1]
            kind_name = str(info.kind).split(".")[-1]
            self._log.info("[Media] user=%s state=%s kind=%s", user_id, state_name, kind_name)

        def on_frames(user_id: int, mode, _device, frames) -> None:
            if mode != ntgcalls.StreamMode.PLAYBACK:
                return
            raw = bytearray()
            for frame in frames:
                if frame.data:
                    raw.extend(frame.data)
            if raw and not loop.is_closed():
                loop.call_soon_threadsafe(self._deliver_incoming_pcm, user_id, bytes(raw))

        ntg.on_signaling_data(on_signaling_data)
        ntg.on_connection_change(on_connection_change)
        ntg.on_frames(on_frames)

    # --- Handler yang berjalan di event loop utama (state sesi aman) ---
    def _on_signaling_data(self, user_id: int, data: bytes) -> None:
        session = self.sessions.get(user_id)
        if session is None or self.signaling is None:
            return
        if not session.signaling_ready:
            if len(session.pending_signaling) < 512:
                session.pending_signaling.append(data)
            return
        self.signaling.dispatch(session, data)

    def _deliver_incoming_pcm(self, user_id: int, raw: bytes) -> None:
        session = self.sessions.get(user_id)
        if session is not None:
            session.handle_incoming_telegram_pcm(raw)

    def _setup_handlers(self) -> None:
        assert self.app is not None

        async def handle_raw_update(_client, update, _users, _chats) -> None:
            if isinstance(update, types.UpdatePhoneCall):
                call = update.phone_call
                self._log.info("[PhoneCall] %s (id=%s)", type(call).__name__, getattr(call, "id", "?"))
                if isinstance(call, types.PhoneCallRequested):
                    await self._handle_call_requested(call)
                elif isinstance(call, types.PhoneCall):
                    await self._handle_call_established(call)
                elif isinstance(call, types.PhoneCallDiscarded):
                    await self._handle_call_discarded(call)
            elif isinstance(update, types.UpdatePhoneCallSignalingData):
                session = self._session_by_call_id(update.phone_call_id)
                if session is not None and self.ntg is not None:
                    try:
                        await self.ntg.send_signaling_data(session.user_id, update.data)
                    except Exception as exc:  # noqa: BLE001
                        self._log.warning("Gagal memproses signaling masuk: %s", exc)

        self.app.add_handler(RawUpdateHandler(handle_raw_update))

    def _session_by_call_id(self, call_id: int) -> Optional[CallSession]:
        for session in self.sessions.values():
            if session.call_id == call_id:
                return session
        return None

    # ---------------------------------------------------------------- gemini
    def _live_factory(self, user_id: int, log):
        settings = self.settings
        tools_decl = self.tools.declarations()

        @asynccontextmanager
        async def factory() -> AsyncIterator[GeminiLiveSession]:
            assert self.gemini_client is not None
            config = genai_types.LiveConnectConfig(
                session_resumption=genai_types.SessionResumptionConfig(),
                context_window_compression=genai_types.ContextWindowCompressionConfig(
                    sliding_window=genai_types.SlidingWindow()
                ),
                response_modalities=[genai_types.Modality.AUDIO],
                input_audio_transcription=genai_types.AudioTranscriptionConfig(),
                output_audio_transcription=genai_types.AudioTranscriptionConfig(),
                system_instruction=genai_types.Content(
                    parts=[genai_types.Part.from_text(text=settings.system_instruction)]
                ),
                realtime_input_config=genai_types.RealtimeInputConfig(
                    automatic_activity_detection=genai_types.AutomaticActivityDetection(
                        start_of_speech_sensitivity=genai_types.StartSensitivity.START_SENSITIVITY_HIGH,
                        end_of_speech_sensitivity=genai_types.EndSensitivity.END_SENSITIVITY_HIGH,
                        silence_duration_ms=800,
                    )
                ),
                tools=tools_decl,
            )
            async with AsyncExitStack() as stack:
                client = self.gemini_client
                connection: Any = None

                async def connect(handle):
                    nonlocal connection
                    if connection is not None:
                        await connection.__aexit__(None, None, None)
                    config.session_resumption = genai_types.SessionResumptionConfig(handle=handle)
                    connection = client.aio.live.connect(model=settings.gemini_model, config=config)
                    return await connection.__aenter__()

                async def disconnect():
                    if connection is not None:
                        await connection.__aexit__(None, None, None)

                stack.push_async_callback(disconnect)
                ws = await connect(None)
                yield GeminiLiveSession(ws, logger=log, reconnect=connect)

        return factory

    # ---------------------------------------------------------------- call flow
    async def _handle_call_requested(self, call: types.PhoneCallRequested) -> None:
        user_id = call.admin_id
        log = call_logger(user_id, call.id)
        log.info("Panggilan masuk dari user %s.", user_id)

        allowed = self.settings.allowed_user_id_set
        if allowed and user_id not in allowed:
            log.warning("Ditolak: user tidak ada di whitelist.")
            await self._discard_call(call, types.PhoneCallDiscardReasonBusy(), user_id)
            return

        if not self.rate_limiter.allow(user_id):
            log.warning(
                "Ditolak: rate-limit terlampaui (%d panggilan / %.0f dtk).",
                self.settings.max_calls_per_window,
                self.settings.call_window_seconds,
            )
            await self._discard_call(call, types.PhoneCallDiscardReasonBusy(), user_id)
            return

        if user_id in self.sessions:
            # Panggilan ganda dari user yang sama (mis. update terduplikasi):
            # tolak yang baru agar sesi lama tidak tertimpa/bocor.
            log.warning("Ditolak: sudah ada sesi aktif untuk user ini.")
            await self._discard_call(call, types.PhoneCallDiscardReasonBusy(), user_id)
            return

        if len(self.sessions) >= self.settings.max_concurrent_calls:
            log.warning("Ditolak: kapasitas penuh (%d sesi).", len(self.sessions))
            await self._discard_call(call, types.PhoneCallDiscardReasonBusy(), user_id)
            return

        assert self.ntg is not None
        session = CallSession(
            user_id=user_id,
            call_id=call.id,
            access_hash=call.access_hash,
            sink=NtgAudioSink(self.ntg, user_id),
            live_factory=self._live_factory(user_id, log),
            tools=self.tools,
            settings=self.settings,
            logger=log,
            on_stop=self._make_stop(user_id),
        )
        self.sessions[user_id] = session
        session.on_hangup = self._make_hangup(session)

        try:
            assert self.app is not None
            peer = types.InputPhoneCall(id=call.id, access_hash=call.access_hash)
            await self.app.invoke(functions.phone.ReceivedCall(peer=peer))  # pyright: ignore[reportArgumentType]

            dh = await self.app.invoke(functions.messages.GetDhConfig(version=0, random_length=256))
            dh_config = ntgcalls.DhConfig(g=dh.g, p=dh.p, random=dh.random)

            await self.ntg.create_p2p_call(user_id)
            g_b = await self.ntg.init_exchange(user_id, dh_config, call.g_a_hash)
            proto = to_tg_protocol(self.ntg.get_protocol(), getattr(call, "protocol", None))
            await self.app.invoke(functions.phone.AcceptCall(peer=peer, g_b=g_b, protocol=proto))  # pyright: ignore[reportArgumentType]
            log.info("AcceptCall terkirim; menunggu konfirmasi penelpon.")
        except Exception:  # noqa: BLE001
            log.exception("Gagal menerima panggilan.")
            self.sessions.pop(user_id, None)
            await session.close()

    async def _handle_call_established(self, call: types.PhoneCall) -> None:
        user_id = call.admin_id or call.participant_id
        session = self.sessions.get(user_id) or self._session_by_call_id(call.id)
        if session is None:
            self._log.warning("Sesi untuk call id %s tidak ditemukan.", call.id)
            return

        if session.state != SessionState.INCOMING_CALL:
            # Update PhoneCall terduplikasi/terlambat: jangan setup media dua kali.
            self._log.warning(
                "PhoneCall untuk user %s diabaikan (state=%s).",
                session.user_id,
                session.state.value,
            )
            return

        log = call_logger(session.user_id, session.call_id)
        session.set_state(SessionState.CONNECTING)

        # Call sudah aktif di Telegram -> signaling ICE aman dikirim.
        session.signaling_ready = True
        assert self.signaling is not None
        self.signaling.flush_pending(session)

        try:
            assert self.ntg is not None
            await self.ntg.exchange_keys(session.user_id, call.g_a_or_b, call.key_fingerprint)
            rtc_servers = [to_ntg_rtc_server(conn) for conn in call.connections]
            lib_versions = list(call.protocol.library_versions) if call.protocol else ["13.0.0"]  # pyright: ignore[reportAttributeAccessIssue]

            await self.ntg.connect_p2p(
                session.user_id,
                rtc_servers,
                lib_versions,
                p2p_allowed=True,
                custom_parameters=None,
            )

            media = external_audio_media(self.settings.tg_sample_rate, keep_open=True)
            await self.ntg.set_stream_sources(session.user_id, ntgcalls.StreamMode.CAPTURE, media)
            await self.ntg.set_stream_sources(session.user_id, ntgcalls.StreamMode.PLAYBACK, media)

            log.info("Media P2P terhubung; membuka sesi Gemini.")
            session.start()
        except Exception:  # noqa: BLE001
            log.exception("Gagal menyiapkan media/koneksi.")
            self.sessions.pop(session.user_id, None)
            await session.close()

    async def _handle_call_discarded(self, call: types.PhoneCallDiscarded) -> None:
        session = self._session_by_call_id(call.id)
        if session is None:
            return
        self.sessions.pop(session.user_id, None)
        call_logger(session.user_id, session.call_id).info("Panggilan berakhir.")
        await session.close()

    async def _discard_call(self, call, reason, user_id: int) -> None:
        if self.app is None:
            return
        peer = types.InputPhoneCall(id=call.id, access_hash=call.access_hash)
        try:
            await self.app.invoke(
                functions.phone.DiscardCall(
                    peer=peer, duration=0, reason=reason, connection_id=0  # pyright: ignore[reportArgumentType]
                )
            )
        except Exception:  # noqa: BLE001
            self._log.exception("Gagal menolak panggilan user=%s", user_id)

    def _make_stop(self, user_id: int):
        async def stop() -> None:
            if self.ntg is not None:
                try:
                    await self.ntg.stop(user_id)
                except Exception:  # noqa: BLE001
                    pass

        return stop

    def _make_hangup(self, session: "CallSession"):
        """Callback untuk mengakhiri panggilan dari sisi AI (tool end_call)."""

        async def hangup() -> None:
            log = call_logger(session.user_id, session.call_id)
            if self.app is not None:
                peer = types.InputPhoneCall(
                    id=session.call_id, access_hash=session.access_hash
                )
                try:
                    await self.app.invoke(
                        functions.phone.DiscardCall(
                            peer=peer,  # pyright: ignore[reportArgumentType]
                            duration=session.duration_seconds,
                            reason=types.PhoneCallDiscardReasonHangup(),  # pyright: ignore[reportArgumentType]
                            connection_id=0,
                        )
                    )
                    log.info("Panggilan diakhiri oleh AI (DiscardCall terkirim).")
                except Exception:  # noqa: BLE001
                    log.exception("Gagal mengirim DiscardCall.")
            self.sessions.pop(session.user_id, None)
            await session.close()

        return hangup
