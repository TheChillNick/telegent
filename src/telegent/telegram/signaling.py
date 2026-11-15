"""Signaling panggilan MTProto (konversi protokol + pengiriman data signaling)."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import ntgcalls
from pyrogram.raw import functions, types

from telegent.logging_setup import error_label


def to_ntg_rtc_server(conn: Any) -> ntgcalls.RTCServer:
    """Konversi koneksi server Telegram ke objek RTCServer ntgcalls."""
    if isinstance(conn, types.PhoneConnectionWebrtc):
        return ntgcalls.RTCServer(
            id=conn.id,
            ipv4=conn.ip,
            ipv6=conn.ipv6,
            port=conn.port,
            username=conn.username,
            password=conn.password,
            turn=conn.turn,
            stun=conn.stun,
            tcp=False,
            peer_tag=None,
        )
    return ntgcalls.RTCServer(
        id=conn.id,
        ipv4=conn.ip,
        ipv6=conn.ipv6,
        port=conn.port,
        username=None,
        password=None,
        turn=False,
        stun=False,
        tcp=conn.tcp,
        peer_tag=conn.peer_tag,
    )


def to_tg_protocol(
    proto: ntgcalls.Protocol, caller_proto: Optional[types.PhoneCallProtocol] = None
) -> types.PhoneCallProtocol:
    """Konversi protokol ntgcalls ke tipe MTProto PhoneCallProtocol."""
    if caller_proto:
        return types.PhoneCallProtocol(
            min_layer=min(proto.min_layer, caller_proto.min_layer),
            max_layer=min(proto.max_layer, caller_proto.max_layer),
            udp_p2p=proto.udp_p2p and caller_proto.udp_p2p,
            udp_reflector=proto.udp_reflector or caller_proto.udp_reflector,
            library_versions=list(caller_proto.library_versions),
        )
    return types.PhoneCallProtocol(
        min_layer=proto.min_layer,
        max_layer=proto.max_layer,
        udp_p2p=proto.udp_p2p,
        udp_reflector=proto.udp_reflector,
        library_versions=list(proto.library_versions),
    )


class SignalingService:
    """Mengirim data signaling ICE ke Telegram dengan retry & penahanan.

    Telegram kadang belum menandai call aktif tepat setelah update `PhoneCall`,
    sehingga `phone.SendSignalingData` sesaat mengembalikan 400 CALL_NOT_ACTIVE.
    Data yang datang terlalu dini ditahan di `session.pending_signaling` lalu
    dikirim saat call terkonfirmasi (`flush_pending`).
    """

    def __init__(self, app: Any, loop: Optional[asyncio.AbstractEventLoop], logger: Any) -> None:
        self._app = app
        self._loop = loop
        self._log = logger

    async def _send(self, session: Any, data: bytes, attempts: int = 5) -> None:
        peer = types.InputPhoneCall(id=session.call_id, access_hash=session.access_hash)
        for attempt in range(attempts):
            try:
                await self._app.invoke(functions.phone.SendSignalingData(peer=peer, data=data))  # pyright: ignore[reportArgumentType]
                return
            except Exception as exc:  # noqa: BLE001
                if "CALL_NOT_ACTIVE" in str(exc) and attempt < attempts - 1:
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                reason = error_label(exc) if getattr(session, "outgoing", False) else exc
                self._log.warning("Gagal mengirim signaling (call=%s): %s", session.call_id, reason)
                return

    def dispatch(self, session: Any, data: bytes) -> None:
        """Jadwalkan pengiriman signaling.

        Aman dipanggil dari thread native ntgcalls maupun dari event loop utama.
        """
        if self._loop is None or self._loop.is_closed():
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            self._loop.create_task(self._send(session, data))
        else:
            asyncio.run_coroutine_threadsafe(self._send(session, data), self._loop)

    def flush_pending(self, session: Any) -> None:
        if not session.pending_signaling:
            return
        pending = session.pending_signaling
        session.pending_signaling = []
        self._log.info("Mengirim %d paket signaling yang tertahan.", len(pending))
        for data in pending:
            self.dispatch(session, data)
