"""MOU-408 probes: outgoing log policy (N1). Every planted string contains SECRET."""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from pyrogram.errors import RPCError
from pyrogram.raw import functions, types

from telegent.ai.events import ToolCallEvent, TranscriptIn, TranscriptOut, TurnComplete
from telegent.ai.tools.registry import ToolRegistry
from telegent.ai.tools.spec import ToolSpec
from telegent.telegram.call_session import CallSession, SessionState
from helpers import FakeLiveSession, FakeSink


# ---------------------------------------------------------------- error_label
@pytest.mark.parametrize('code,message,expected', [
    (400, 'CALL_PROTOCOL_LAYER_INVALID', 'BadRequest [400 CALL_PROTOCOL_LAYER_INVALID]'),
    (403, 'USER_PRIVACY_RESTRICTED', 'UserPrivacyRestricted USER_PRIVACY_RESTRICTED'),
    (420, 'FLOOD_WAIT_37', 'FloodWait FLOOD_WAIT_X'),
    (400, 'SECRET text with spaces', 'BadRequest'),
    (599, 'SOME_NEW_CODE', 'UnknownError [599 SOME_NEW_CODE]'),
    (599, 'lower case SECRET', 'UnknownError'),
    (-503, 'Timeout', 'Timeout Timeout'),
])
def test_r01_error_label_on_real_pyrogram_errors(code, message, expected):
    from telegent.app import error_label
    try:
        RPCError.raise_it(types.RpcError(error_code=code, error_message=message), functions.phone.ConfirmCall)
    except RPCError as exc:
        label = error_label(exc)
    assert label == expected
    assert 'SECRET' not in label


def test_error_label_rejects_trailing_newline():
    from pyrogram.errors import UnknownError
    from telegent.app import error_label
    label = error_label(UnknownError(value='[400 FOO]\n'))
    assert '\n' not in label, repr(label)


# ---------------------------------------------------------------- session scan
class LeakyLive(FakeLiveSession):
    def __init__(self, events):
        super().__init__(events)
        self.tool_sends = 0

    async def send_greeting(self, prompt):
        self.greetings.append(prompt)
        raise ValueError('SECRET-greet ' + prompt[-20:])

    async def send_audio(self, pcm16k):
        raise RuntimeError('SECRET-audio')

    async def send_prompt(self, text):
        raise RuntimeError('SECRET-prompt')

    async def send_tool_response(self, responses):
        self.tool_sends += 1
        self.tool_responses.extend(responses)
        if responses[0].name != 'probe_tool':
            raise RuntimeError('SECRET-toolresp')

    async def events(self):
        for event in self._events:
            yield event
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        raise RuntimeError('SECRET-recv')


def registry():
    reg = ToolRegistry()

    def probe_tool(ctx, args):
        ctx.logger.info('SECRET-toollog %s', args)
        ctx.logger.error('SECRET-toolerr')
        return {'r': 'SECRET-result', 'echo': args}

    def boom_tool(ctx, args):
        raise RuntimeError('SECRET-toolexc')

    reg.register(ToolSpec(name='probe_tool', description='p', handler=probe_tool))
    reg.register(ToolSpec(name='boom_tool', description='b', handler=boom_tool))
    return reg


async def scenario(settings, outgoing, sink=None):
    events = [TurnComplete(), TranscriptIn('SECRET-in'), TranscriptOut('SECRET-out'),
              ToolCallEvent(id='t1', name='probe_tool', args={'a': 'SECRET-arg'}),
              ToolCallEvent(id='t2', name='SECRET_TOOL', args={'a': 'SECRET-arg2'}),
              ToolCallEvent(id='t3', name='boom_tool', args={'a': 'SECRET-arg3'})]
    live = LeakyLive(events)

    @asynccontextmanager
    async def factory():
        yield live

    async def bad_stop():
        raise RuntimeError('SECRET-stop')

    async def bad_hangup():
        raise RuntimeError('SECRET-hangup')

    settings = settings.model_copy(update={'idle_enabled': False})
    session = CallSession(user_id=7, call_id=10, access_hash=1, sink=sink or FakeSink(), live_factory=factory,
                          tools=registry(), settings=settings, logger=logging.getLogger('telegent.call'),
                          on_stop=bad_stop, opening_text='SECRET-request' if outgoing else None)
    session.on_hangup = bad_hangup
    session.start()
    for _ in range(200):
        if live.tool_sends >= 3:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)  # receiver raises SECRET-recv
    await session._send_prompt('x')
    session.schedule_hangup()
    await asyncio.wait_for(session._hangup_task, 20)
    await session.close()
    assert session.state == SessionState.IDLE

    @asynccontextmanager
    async def broken():
        raise RuntimeError('SECRET-connect')
        yield  # pragma: no cover

    s2 = CallSession(user_id=7, call_id=11, access_hash=1, sink=FakeSink(), live_factory=broken,
                     tools=registry(), settings=settings, logger=logging.getLogger('telegent.call'),
                     opening_text='SECRET-request2' if outgoing else None)
    s2.start()
    await asyncio.wait_for(s2._live_task, 2)
    return live


async def test_r03_outgoing_session_never_logs_planted_text(settings, caplog):
    caplog.set_level(logging.DEBUG)
    live = await scenario(settings, outgoing=True)
    text = caplog.text
    for line in ('Gagal mengirim sapaan awal: ValueError', 'Gagal mengirim audio ke Gemini: RuntimeError',
                 'Kesalahan penerima Gemini: RuntimeError', "Menjalankan tool 'probe_tool'.",
                 "Menjalankan tool '<unknown>'.", "Tool 'probe_tool' selesai.",
                 "Gagal mengirim hasil tool '<unknown>': RuntimeError", 'Gagal mengirim prompt: RuntimeError',
                 'Gagal mengakhiri panggilan: RuntimeError', 'Gagal menghentikan media: RuntimeError',
                 'Kesalahan sesi live: RuntimeError'):
        assert line in text, line
    leaks = [r.getMessage() for r in caplog.records if 'SECRET' in r.getMessage() or (r.exc_text and 'SECRET' in r.exc_text)]
    assert not leaks, leaks
    assert 'SECRET' not in text
    assert not any(r.exc_info for r in caplog.records), 'no tracebacks for outgoing'
    assert 'SECRET-request' in live.greetings[0]  # the text did reach Gemini


async def test_outgoing_cadence_frame_error_logs_class_only(settings, caplog):
    """A failing audio frame send on an outgoing call logs the error class, never its message (MOU-408 F3)."""
    class BadSink(FakeSink):
        async def send_frame(self, frame):
            raise RuntimeError('SECRET-frame')

    caplog.set_level(logging.DEBUG)
    await scenario(settings, outgoing=True, sink=BadSink())
    hits = sorted({r.getMessage() for r in caplog.records if 'SECRET' in r.getMessage()})
    assert not hits, hits


async def test_app_level_outgoing_lines_log_labels_only(monkeypatch, caplog):
    """Signaling in/out and DiscardCall failures on an outgoing call log error labels only (MOU-407 F1, MOU-408 F3)."""
    from test_outgoing import setup_app, start_dial
    from telegent.telegram.signaling import SignalingService
    from telegent.logging_setup import call_logger
    app, live, invoked, established = setup_app(monkeypatch)
    caplog.set_level(logging.DEBUG)
    dial, session = await start_dial(app, 'SECRET-request')
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    assert await dial == 'answered'
    # 1. raw signaling update -> ntg.send_signaling_data fails
    app.app.add_handler = Mock()
    app._setup_handlers()
    handler = app.app.add_handler.call_args.args[0].callback

    async def bad_sig(*a):
        raise RuntimeError('SECRET-sig-in')

    app.ntg.send_signaling_data = bad_sig
    await handler(None, types.UpdatePhoneCallSignalingData(phone_call_id=10, data=b'x'), {}, {})
    # 2. SignalingService._send fails
    async def bad_invoke(*a, **k):
        raise RuntimeError('SECRET-sig-out')

    svc = SignalingService(NS(invoke=bad_invoke), asyncio.get_running_loop(), call_logger(7, 10))
    await svc._send(session, b'x', attempts=1)
    # 3. AI hang-up: DiscardCall fails
    original = app.app.invoke.side_effect

    async def side(query, **kw):
        if isinstance(query, functions.phone.DiscardCall):
            raise RuntimeError('SECRET-discard')
        return await original(query, **kw)

    app.app.invoke.side_effect = side
    await session.on_hangup()
    hits = []
    for r in caplog.records:
        blob = r.getMessage() + (r.exc_text or '')
        if 'SECRET' in blob:
            hits.append(f'{r.module}:{r.lineno} {r.getMessage()[:60]!r} traceback={bool(r.exc_info)}')
    assert not any('SECRET-request' in (r.getMessage() + (r.exc_text or '')) for r in caplog.records)
    assert not hits, hits


# ---------------------------------------------------------------- incoming differential
