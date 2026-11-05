"""Adapter sesi Gemini Live: menerjemahkan pesan SDK menjadi `LiveEvent` netral.

Catatan penting: `session.receive()` pada SDK google-genai hanya mengembalikan
SATU giliran (turn) lalu berhenti saat `turn_complete=True`. Karena itu
`events()` memanggil ulang `receive()` di dalam loop agar giliran berikutnya
(temasuk lanjutan setelah tool call) tetap diproses.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Sequence

from google.genai import types as genai_types

from telegent.ai.events import (
    AudioOut,
    Interrupted,
    LiveEvent,
    ToolCallCancelled,
    ToolCallEvent,
    TranscriptIn,
    TranscriptOut,
    TurnComplete,
)
from telegent.ai.tools.spec import ToolResponse


class GeminiLiveSession:
    """Membungkus `AsyncSession` milik google-genai menjadi antarmuka netral."""

    def __init__(self, ws: Any, *, logger: Any = None, reconnect: Any = None) -> None:
        self._reconnect = reconnect
        self._handle = None
        self._send_lock = asyncio.Lock()
        self._ws = ws
        self._log = logger
        self._closed = False

    async def send_audio(self, pcm16k: bytes) -> None:
        async with self._send_lock:
            await self._ws.send_realtime_input(
                audio=genai_types.Blob(data=pcm16k, mime_type="audio/pcm;rate=16000")
            )

    async def send_greeting(self, prompt: str) -> None:
        await self.send_prompt(prompt)

    async def send_prompt(self, text: str) -> None:
        """Kirim teks (mis. instruksi sistem) sebagai giliran pengguna agar model merespons."""
        async with self._send_lock:
            await self._ws.send_client_content(
                turns=[genai_types.Content(
                    role="user", parts=[genai_types.Part.from_text(text=text)]
                )],
                turn_complete=True,
            )

    async def send_tool_response(self, responses: Sequence[ToolResponse]) -> None:
        async with self._send_lock:
            await self._ws.send_tool_response(
                function_responses=[
                    genai_types.FunctionResponse(id=r.id, name=r.name, response=r.response)
                    for r in responses
                ]
            )

    async def events(self) -> AsyncIterator[LiveEvent]:
        while not self._closed:
            async for response in self._ws.receive():
                update = getattr(response, "session_resumption_update", None)
                if update is not None:
                    self._handle = update.new_handle if update.resumable else None
                if getattr(response, "go_away", None) is not None and self._reconnect:
                    async with self._send_lock:
                        self._ws = await self._reconnect(self._handle)
                    break
                server_content = response.server_content
                if server_content is not None:
                    if server_content.input_transcription and server_content.input_transcription.text:
                        yield TranscriptIn(server_content.input_transcription.text)
                    if server_content.output_transcription and server_content.output_transcription.text:
                        yield TranscriptOut(server_content.output_transcription.text)
                    if server_content.interrupted:
                        yield Interrupted()
                    if server_content.model_turn and server_content.model_turn.parts:
                        for part in server_content.model_turn.parts:
                            if part.inline_data and part.inline_data.data:
                                yield AudioOut(part.inline_data.data)
                    if server_content.turn_complete:
                        yield TurnComplete()

                tool_call = response.tool_call
                if tool_call and tool_call.function_calls:
                    for fc in tool_call.function_calls:
                        yield ToolCallEvent(id=fc.id or "", name=fc.name or "", args=dict(fc.args or {}))

                cancellation = response.tool_call_cancellation
                if cancellation and cancellation.ids:
                    for cancelled_id in cancellation.ids:
                        yield ToolCallCancelled(cancelled_id)

    async def close(self) -> None:
        self._closed = True
