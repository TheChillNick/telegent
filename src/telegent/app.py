"""Orkestrasi Telegent: klien Telegram, media ntgcalls, dan dispatch panggilan."""

from __future__ import annotations

import asyncio
import signal
import secrets
from pathlib import Path
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
from telegent.logging_setup import call_logger, error_label, get_logger
from telegent.rate_limit import CallRateLimiter
from telegent.outbox import Outbox
from telegent.telegram.call_session import CallSession, SessionState
from telegent.telegram.media import NtgAudioSink, external_audio_media
from telegent.telegram.signaling import SignalingService, to_ntg_rtc_server, to_tg_protocol


NTG_STOP_TIMEOUT = 10.0  # seconds; then the session is unregistered anyway


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
        self._outbox_task: asyncio.Task | None = None
        self._outgoing_accept: dict[int, asyncio.Future[Any]] = {}
        self._outgoing_ready = asyncio.Event()
        self._outgoing_ready.set()

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

        try:
            self._start_outbox_watcher()
            await self._stop.wait()
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        if self._outbox_task is not None:
            self._outbox_task.cancel()
            try:
                await self._outbox_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self._log.warning("Outgoing watcher failed (%s); continuing shutdown.", error_label(exc))
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
                elif isinstance(call, types.PhoneCallAccepted):
                    await self._handle_call_accepted(call)
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
                        self._log.warning("Gagal memproses signaling masuk: %s",
                                          error_label(exc) if session.outgoing else exc)

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

    def _start_outbox_watcher(self) -> None:
        if self.settings.kai_outbox_dir:
            self._outbox_task = asyncio.create_task(self._watch_outbox(), name="kai-outbox")

    async def _watch_outbox(self) -> None:
        cfg = self.settings
        box: Outbox | None = None
        while True:
            try:
                if box is None:
                    box = Outbox(Path(cfg.kai_outbox_dir), self._call_outgoing,
                                 allowed=cfg.kai_call_user_id > 0 and cfg.kai_call_user_id in cfg.allowed_user_id_set,
                                 max_per_hour=cfg.kai_call_max_per_hour, min_gap=cfg.kai_call_min_gap,
                                 stale_seconds=cfg.kai_call_stale_seconds, quiet_start=cfg.kai_call_quiet_start,
                                 quiet_end=cfg.kai_call_quiet_end, timezone_name=cfg.kai_call_timezone)
                await box.process(active=bool(self.sessions))
            except Exception as exc:
                self._log.warning("Outgoing spool unavailable; no call attempted/retried: %s", error_label(exc))
            await asyncio.sleep(1)

    @staticmethod
    def _outcome(session: CallSession, result: str) -> None:
        future = session.outgoing_result
        if future is not None and not future.done():
            future.set_result(result)

    async def _call_outgoing(self, text: str) -> str:
        """One caller-side attempt. The spool handles queueing and rate policy."""
        user_id = self.settings.kai_call_user_id
        if user_id <= 0 or user_id not in self.settings.allowed_user_id_set:
            return "not-whitelisted"
        if self.sessions:
            # Normally prevented by the spool. No await separates its activity
            # check and this reservation, so incoming calls cannot race it.
            return "busy"
        assert self.ntg is not None and self.app is not None
        log = call_logger(user_id, 0)
        session = CallSession(
            user_id=user_id, call_id=0, access_hash=0,
            sink=NtgAudioSink(self.ntg, user_id), live_factory=self._live_factory(user_id, log),
            tools=self.tools, settings=self.settings, logger=log,
            opening_text=text, on_stop=self._make_stop(user_id),
        )
        session.set_state(SessionState.OUTGOING_CALL)
        session.on_hangup = self._make_hangup(session)
        session.outgoing_result = asyncio.get_running_loop().create_future()
        self._outgoing_accept[user_id] = asyncio.get_running_loop().create_future()
        self.sessions[user_id] = session
        result = "failed"
        try:
            result = await self._dial_outgoing(session)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Class + RPC id only: never the message (may echo request data).
            self._log.warning("Outgoing setup failed: %s", error_label(exc))
        finally:
            outcome = session.outgoing_result
            if outcome is not None and outcome.done() and not outcome.cancelled():
                result = outcome.result()
            self._outgoing_ready.set()
            self._outgoing_accept.pop(user_id, None)
            if result != "answered":
                # Also cancel a remotely ringing call when shutdown interrupts it.
                if session.call_id and self.sessions.get(user_id) is session:
                    reason = (types.PhoneCallDiscardReasonMissed() if result == "no-answer"
                              else types.PhoneCallDiscardReasonDisconnect())
                    await self._discard_call(session, reason, user_id)
                await session.close()
        self._log.info("Outgoing call result=%s", result)
        return result

    async def _dial_outgoing(self, session: CallSession) -> str:
        """Ring under the ring deadline, then set up under the setup deadline."""
        try:
            accepted = await asyncio.wait_for(self._ring_outgoing(session), self.settings.kai_call_ring_timeout)
        except asyncio.TimeoutError:
            return "no-answer"
        if isinstance(accepted, str):
            return accepted
        try:
            return await asyncio.wait_for(self._setup_outgoing(session, accepted), self.settings.kai_call_setup_timeout)
        except asyncio.TimeoutError:
            self._log.warning("Outgoing setup timed out after answer (%.0f s).", self.settings.kai_call_setup_timeout)
            return "failed"

    async def _ring_outgoing(self, session: CallSession):
        assert self.app is not None and self.ntg is not None
        user_id = session.user_id
        peer = await self.app.resolve_peer(user_id)
        if not isinstance(peer, types.InputPeerUser) or peer.user_id != user_id:
            raise ValueError("callee is not the configured user")
        dh = await self.app.invoke(functions.messages.GetDhConfig(version=0, random_length=256))
        await self.ntg.create_p2p_call(user_id)
        # ntgcalls hashes g_a itself for the calling side (ga_hash=None).
        ga_hash = await self.ntg.init_exchange(user_id, ntgcalls.DhConfig(g=dh.g, p=dh.p, random=dh.random), None)
        self._outgoing_ready.clear()
        try:
            request = asyncio.create_task(self.app.invoke(functions.phone.RequestCall(
                user_id=types.InputUser(user_id=peer.user_id, access_hash=peer.access_hash),  # pyright: ignore[reportArgumentType]
                random_id=secrets.randbits(31), g_a_hash=ga_hash,
                protocol=to_tg_protocol(self.ntg.get_protocol()),  # pyright: ignore[reportArgumentType]
            ), retries=0, timeout=min(15, self.settings.kai_call_ring_timeout), sleep_threshold=0))
            try:
                response = await asyncio.shield(request)
            except asyncio.CancelledError:
                # Telegram may already be ringing. Keep its response so cleanup
                # can discard the remote call, including during shutdown. This
                # can extend local cleanup beyond the ring deadline by RPC latency.
                try:
                    response = await request
                    call = response.phone_call
                    session.call_id, session.access_hash = call.id, call.access_hash
                except Exception as exc:
                    self._log.warning("RequestCall response unavailable; remote state unknown: %s", error_label(exc))
                raise
            call = response.phone_call
            session.call_id, session.access_hash = call.id, call.access_hash
        finally:
            # Raw accepted/discarded updates can arrive before the RPC returns.
            self._outgoing_ready.set()
        accepted = self._outgoing_accept[user_id]
        outcome = session.outgoing_result
        assert outcome is not None
        await asyncio.wait((accepted, outcome), return_when=asyncio.FIRST_COMPLETED)
        if outcome.done():
            return outcome.result()
        return accepted.result()

    async def _setup_outgoing(self, session: CallSession, call) -> str:
        assert self.app is not None and self.ntg is not None
        user_id = session.user_id
        outcome = session.outgoing_result
        assert outcome is not None
        auth = await self.ntg.exchange_keys(user_id, call.g_b, 0)
        if outcome.done():
            return outcome.result()
        session.set_state(SessionState.CONFIRMING)
        confirmation = asyncio.create_task(self.app.invoke(functions.phone.ConfirmCall(
            peer=types.InputPhoneCall(id=session.call_id, access_hash=session.access_hash),  # pyright: ignore[reportArgumentType]
            g_a=auth.g_a_or_b, key_fingerprint=auth.key_fingerprint,
            protocol=to_tg_protocol(self.ntg.get_protocol(), call.protocol),  # pyright: ignore[reportArgumentType]
        )))
        try:
            await asyncio.wait((confirmation, outcome), return_when=asyncio.FIRST_COMPLETED)
            if outcome.done():
                return outcome.result()
            response = confirmation.result()
        finally:
            if not confirmation.done():
                confirmation.cancel()
            await asyncio.gather(confirmation, return_exceptions=True)
        await self._handle_call_established(response.phone_call)
        return await outcome

    async def _handle_call_accepted(self, call: types.PhoneCallAccepted) -> None:
        await self._outgoing_ready.wait()
        session = self._session_by_call_id(call.id)
        if session is None or session.state != SessionState.OUTGOING_CALL:
            return
        accepted = self._outgoing_accept.get(session.user_id)
        if accepted is not None and not accepted.done():
            accepted.set_result(call)

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
            await session.close()

    async def _handle_call_established(self, call: types.PhoneCall) -> None:
        await self._outgoing_ready.wait()
        session = self._session_by_call_id(call.id)
        if session is None:
            self._log.warning("Sesi untuk call id %s tidak ditemukan.", call.id)
            return

        if session.state not in (SessionState.INCOMING_CALL, SessionState.CONFIRMING):
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
            if not session.outgoing:
                await self.ntg.exchange_keys(session.user_id, call.g_a_or_b, call.key_fingerprint)
            rtc_servers = [to_ntg_rtc_server(conn) for conn in call.connections]
            lib_versions = list(call.protocol.library_versions) if call.protocol else ["13.0.0"]  # pyright: ignore[reportAttributeAccessIssue]

            await self.ntg.connect_p2p(
                session.user_id,
                rtc_servers,
                lib_versions,
                p2p_allowed=bool(getattr(call, "p2p_allowed", False)),
                custom_parameters=None,
            )

            media = external_audio_media(self.settings.tg_sample_rate, keep_open=True)
            await self.ntg.set_stream_sources(session.user_id, ntgcalls.StreamMode.CAPTURE, media)
            await self.ntg.set_stream_sources(session.user_id, ntgcalls.StreamMode.PLAYBACK, media)

            if session._closing:
                return
            log.info("Media P2P terhubung; membuka sesi Gemini.")
            session.start()
            self._outcome(session, "answered")
        except Exception as exc:  # noqa: BLE001
            log.warning("Media setup failed: %s", error_label(exc))
            self._outcome(session, "failed")
            await self._discard_call(session, types.PhoneCallDiscardReasonDisconnect(), session.user_id)
            await session.close()

    async def _handle_call_discarded(self, call: types.PhoneCallDiscarded) -> None:
        await self._outgoing_ready.wait()
        session = self._session_by_call_id(call.id)
        if session is None:
            return
        reason = getattr(call, "reason", None)
        result = ("busy" if isinstance(reason, types.PhoneCallDiscardReasonBusy) else
                  "no-answer" if isinstance(reason, types.PhoneCallDiscardReasonMissed) else
                  "declined" if isinstance(reason, types.PhoneCallDiscardReasonHangup) else "failed")
        self._outcome(session, result)
        call_logger(session.user_id, session.call_id).info("Panggilan berakhir.")
        await session.close()

    async def _discard_call(self, call, reason, user_id: int) -> None:
        if self.app is None:
            return
        peer = types.InputPhoneCall(id=getattr(call, "call_id", None) or call.id, access_hash=call.access_hash)
        try:
            await self.app.invoke(
                functions.phone.DiscardCall(
                    peer=peer, duration=0, reason=reason, connection_id=0  # pyright: ignore[reportArgumentType]
                )
            )
        except Exception as exc:  # noqa: BLE001
            self._log.warning("Discard failed for user=%s: %s", user_id, error_label(exc))

    def _make_stop(self, user_id: int):
        async def stop() -> None:
            session = self.sessions.get(user_id)
            try:
                if self.ntg is not None:
                    await asyncio.wait_for(self.ntg.stop(user_id), NTG_STOP_TIMEOUT)
            except Exception:  # noqa: BLE001  a hanging native stop must not keep the session registered
                pass
            finally:
                if self.sessions.get(user_id) is session:
                    self.sessions.pop(user_id, None)

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
                except Exception as exc:  # noqa: BLE001
                    if session.outgoing:
                        log.warning("Gagal mengirim DiscardCall: %s", error_label(exc))
                    else:
                        log.exception("Gagal mengirim DiscardCall.")
            await session.close()

        return hangup
