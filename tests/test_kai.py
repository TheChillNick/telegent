import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace

import httpx
import pytest
from google.genai import types

from telegent.app import TelegentApp
from telegent.ai.event_reader import read_events
from telegent.ai.tools.kai import KaiTools
from telegent.ai.tools.spec import ToolContext
from telegent.config import Settings
from telegent.telegram.call_session import CallSession, SessionState
from helpers import FakeLiveSession, FakeSink, make_session


def settings(**kwargs):
    return Settings(telegram_api_id=1, telegram_api_hash="fake", gemini_api_key="fake",
                    _env_file=None, **kwargs)


def test_persona_and_open_line_defaults():
    assert settings().max_call_duration == 600
    assert settings(idle_enabled=False).max_call_duration == 0
    assert settings(idle_enabled=False, max_call_duration=42).max_call_duration == 42
    kai = settings(persona="kai")
    assert "2–3" in kai.system_instruction
    assert "anglicky" in kai.system_instruction
    assert settings(persona="kai", system_instruction="override").system_instruction == "override"
    assert settings().system_instruction != kai.system_instruction


async def test_idle_disabled_never_prompts_or_hangs_up():
    live = FakeLiveSession()
    session = make_session(settings=settings(idle_enabled=False, idle_prompt_seconds=0,
                                            idle_hangup_seconds=0), live=live)
    session.state = SessionState.ACTIVE
    session._live = live
    session.is_greeting = False
    session._idle_prompted = True
    await session._idle_watchdog()
    assert not live.prompts
    assert not session._hangup_scheduled


async def test_goaway_resumes_without_ending_telegram():
    configs = []
    resumed = asyncio.Event()
    hold = asyncio.Event()

    class WS:
        def __init__(self, first):
            self.first = first
        async def send_client_content(self, **kwargs):
            pass
        async def send_realtime_input(self, **kwargs):
            pass
        async def receive(self):
            if self.first:
                self.first = False
                yield types.LiveServerMessage(session_resumption_update=types.LiveServerSessionResumptionUpdate(
                    new_handle="resume-me", resumable=True))
                yield types.LiveServerMessage(go_away=types.LiveServerGoAway(time_left="1s"))
            else:
                resumed.set()
                await hold.wait()
                yield types.LiveServerMessage()

    @asynccontextmanager
    async def connect(*, model, config):
        configs.append(config.model_copy(deep=True))
        yield WS(len(configs) == 1)

    app = TelegentApp(settings(idle_enabled=False))
    app.gemini_client = SimpleNamespace(aio=SimpleNamespace(live=SimpleNamespace(connect=connect)))
    stopped = []
    async def stop():
        stopped.append(True)
    session = CallSession(user_id=1, call_id=1, access_hash=1, sink=FakeSink(),
                          live_factory=app._live_factory(1, logging.getLogger("test")),
                          tools=app.tools, settings=app.settings, logger=logging.getLogger("test"),
                          on_stop=stop)
    session.start()
    try:
        await asyncio.wait_for(resumed.wait(), 2)
        assert configs[0].session_resumption.handle is None
        assert configs[1].session_resumption.handle == "resume-me"
        assert configs[0].context_window_compression.sliding_window is not None
        assert session.state == SessionState.ACTIVE
        assert not stopped
    finally:
        await session.close()


@pytest.mark.parametrize("name,args,method", [
    ("get_status", {}, "GET"), ("create_task", {"title": "work"}, "POST"),
    ("capture_note", {"text": "note"}, "POST"),
    ("request_confirm", {"question": "Proceed?"}, "POST"),
])
async def test_kai_tools_fake_transport(name, args, method):
    def transport(request):
        assert request.method == method
        assert request.url.path == "/api/" + name
        assert request.headers["authorization"] == "Bearer fake"
        if method == "POST":
            assert json.loads(request.content) == {**args, "user_id": 7, "call_id": 8}
        else:
            assert request.url.params["user_id"] == "7"
        return httpx.Response(202 if name == "create_task" else 200, json={"id": "queued"})
    tools = KaiTools("https://kai.invalid/api", "fake", transport=httpx.MockTransport(transport))
    spec = next(s for s in tools.specs() if s.name == name)
    result = await spec.handler(ToolContext(7, 8), args)
    assert result == ({"message": "on it", "task": {"id": "queued"}} if name == "create_task" else {"id": "queued"})


async def test_jsonl_new_complete_lines_only(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"old":true}\n')
    received = []
    async def send(text):
        received.append(text)
    task = asyncio.create_task(read_events(str(path), send, interval=0.01))
    try:
        await asyncio.sleep(0.03)
        with path.open("a") as f:
            f.write('bad json\n{"message":"hotovo"}')
        await asyncio.sleep(0.03)
        assert not received
        with path.open("a") as f:
            f.write('\n')
        for _ in range(30):
            if received:
                break
            await asyncio.sleep(0.01)
        assert len(received) == 1
        assert "hotovo" in received[0]
        assert "old" not in received[0]
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def test_persona_and_idle_from_environment(monkeypatch):
    monkeypatch.setenv("PERSONA", "kai")
    monkeypatch.setenv("IDLE_ENABLED", "false")
    monkeypatch.delenv("MAX_CALL_DURATION", raising=False)
    config = settings()
    assert config.max_call_duration == 0
    assert "Jsi Kai" in config.system_instruction


async def test_event_text_uses_client_content():
    from telegent.ai.live_session import GeminiLiveSession
    sent = []
    class WS:
        async def send_client_content(self, **kwargs):
            sent.append(kwargs)
    await GeminiLiveSession(WS()).send_prompt('Kai event: task finished')
    assert sent[0]["turn_complete"] is True
    assert sent[0]["turns"][0].role == "user"
    assert sent[0]["turns"][0].parts[0].text == 'Kai event: task finished'
