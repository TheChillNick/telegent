"""MOU-407 D4: plant MRK7Q in every content source of an outgoing call; no captured log line may contain it."""
import asyncio, logging
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
from pyrogram.errors import UnknownError
from pyrogram.raw import functions
from telegent.ai.events import TranscriptIn, TranscriptOut, ToolCallEvent
from telegent.ai.tools.spec import ToolSpec
from telegent.logging_setup import call_logger
from telegent.telegram.signaling import SignalingService
from test_outgoing import setup_app, start_dial

M = 'MRK7Q'


def leaks(caplog):
    # caplog.text = formatted records incl. tracebacks; also check raw args/exc_info
    out = [l for l in caplog.text.splitlines() if M in l]
    for r in caplog.records:
        if r.exc_info and M in str(r.exc_info[1]) and not out:
            out.append(f'exc_info of {r.getMessage()!r}')
    return out


async def answer(app, established):
    dial, session = await start_dial(app, f'request {M} text')
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    return dial, session


async def settle(n=80):
    for _ in range(n):
        await asyncio.sleep(0)


async def test_answered_call_content(monkeypatch, caplog):
    """request text, transcripts in/out, registered + unregistered tool args/results, tool logger, greeting/receiver/tool-send errors."""
    caplog.set_level(logging.DEBUG)
    app, live, _, established = setup_app(monkeypatch)
    def handler(ctx, args):
        ctx.logger.warning('tool logs %s', args)
        return {'echo': M}
    app.tools.register(ToolSpec(name='probe_tool', description='d', handler=handler))
    async def gen():
        yield TranscriptIn(f'in {M}')
        yield TranscriptOut(f'out {M}')
        yield ToolCallEvent(id='t1', name='probe_tool', args={'a': M})
        yield ToolCallEvent(id='t2', name=f'bad{M}', args={'a': M})
        await asyncio.sleep(0.05)
        raise RuntimeError(f'receiver {M}')
    live.events = gen
    async def bad_greet(prompt): raise ValueError(f'greet {M}')
    async def bad_tool_resp(r): raise RuntimeError(f'toolresp {M}')
    async def bad_prompt(t): raise RuntimeError(f'prompt {M}')
    live.send_greeting, live.send_tool_response, live.send_prompt = bad_greet, bad_tool_resp, bad_prompt
    dial, session = await answer(app, established)
    assert await dial == 'answered'
    await asyncio.sleep(0.2)
    await session._send_prompt('x') if hasattr(session, '_send_prompt') else None
    await session.on_hangup()
    text = caplog.text
    assert 'Kesalahan penerima Gemini: RuntimeError' in text and 'Gagal mengirim sapaan awal: ValueError' in text
    assert not leaks(caplog), leaks(caplog)


@pytest.mark.parametrize('where', ['resolve_peer_rpc', 'resolve_peer_plain', 'connect_p2p', 'discard', 'live_enter'])
async def test_failure_paths(monkeypatch, caplog, where):
    caplog.set_level(logging.DEBUG)
    app, live, _, established = setup_app(monkeypatch)
    if where == 'resolve_peer_rpc':
        app.app.resolve_peer.side_effect = UnknownError(value=f'[400 private {M.lower()} {M}]')
        assert await app._call_outgoing(f'req {M}') == 'failed'
    elif where == 'resolve_peer_plain':
        app.app.resolve_peer.side_effect = RuntimeError(f'peer {M}')
        assert await app._call_outgoing(f'req {M}') == 'failed'
    elif where == 'connect_p2p':
        app.ntg.connect_p2p.side_effect = RuntimeError(f'native {M}')
        orig = app.app.invoke.side_effect
        async def inv(q, **kw):
            if isinstance(q, functions.phone.DiscardCall):
                raise RuntimeError(f'discard {M}')
            return await orig(q)
        app.app.invoke.side_effect = inv
        dial, _ = await answer(app, established)
        assert await dial == 'failed'
    elif where == 'discard':
        orig = app.app.invoke.side_effect
        async def inv(q, **kw):
            if isinstance(q, functions.phone.DiscardCall):
                raise RuntimeError(f'discard {M}')
            return await orig(q)
        app.app.invoke.side_effect = inv
        app.settings.kai_call_ring_timeout = 0.05
        assert await app._call_outgoing(f'req {M}') == 'no-answer'
    elif where == 'live_enter':
        @asynccontextmanager
        async def broken():
            raise RuntimeError(f'gemini connect {M}')
            yield
        monkeypatch.setattr(app, '_live_factory', lambda *a: broken)
        dial, session = await answer(app, established)
        await dial
        await settle()
        assert 'Kesalahan sesi live: RuntimeError' in caplog.text
    assert not leaks(caplog), leaks(caplog)


async def test_watcher_and_shutdown(monkeypatch, caplog, tmp_path):
    caplog.set_level(logging.DEBUG)
    app, *_ = setup_app(monkeypatch, kai_outbox_dir=str(tmp_path))
    import telegent.app as appmod
    def boom(*a, **k): raise ValueError(f'spool {M}')
    monkeypatch.setattr(appmod, 'Outbox', boom)
    app._start_outbox_watcher()
    await asyncio.sleep(0.05)
    assert 'Outgoing spool unavailable; no call attempted/retried: ValueError' in caplog.text
    app._outbox_task.cancel()
    async def failing(): raise RuntimeError(f'watcher {M}')
    app._outbox_task = asyncio.create_task(failing())
    await asyncio.sleep(0)
    app.app.stop = AsyncMock()
    await app._shutdown()
    assert 'Outgoing watcher failed (RuntimeError)' in caplog.text
    assert not leaks(caplog), leaks(caplog)


async def test_ai_hangup_discard_failure(monkeypatch, caplog):
    """_make_hangup (end_call by Kai) when DiscardCall fails on an outgoing call: app.py log.exception."""
    caplog.set_level(logging.DEBUG)
    app, live, _, established = setup_app(monkeypatch)
    dial, session = await answer(app, established)
    assert await dial == 'answered'
    orig = app.app.invoke.side_effect
    async def inv(q, **kw):
        if isinstance(q, functions.phone.DiscardCall):
            raise RuntimeError(f'hangup discard {M}')
        return await orig(q)
    app.app.invoke.side_effect = inv
    await session.on_hangup()
    assert not leaks(caplog), leaks(caplog)


async def test_signaling_send_failure(monkeypatch, caplog):
    """SignalingService._send on an outgoing call when SendSignalingData fails (signaling.py:86)."""
    caplog.set_level(logging.DEBUG)
    async def inv(q, **kw): raise RuntimeError(f'signaling {M}')
    svc = SignalingService(NS(invoke=inv), asyncio.get_running_loop(), logging.getLogger('telegent.app'))
    await svc._send(NS(call_id=10, access_hash=99, outgoing=True), b'x', attempts=1)
    assert not leaks(caplog), leaks(caplog)


async def test_incoming_signaling_failure_on_outgoing_call(monkeypatch, caplog):
    """app.py:206 handle_raw_update -> ntg.send_signaling_data fails while an outgoing call is up."""
    from pyrogram.raw import types
    caplog.set_level(logging.DEBUG)
    app, live, _, established = setup_app(monkeypatch)
    dial, session = await answer(app, established)
    assert await dial == 'answered'
    handlers = []
    app.app.add_handler = lambda h: handlers.append(h)
    app._setup_handlers()
    app.ntg.send_signaling_data = AsyncMock(side_effect=RuntimeError(f'native sig {M}'))
    await handlers[0].callback(None, types.UpdatePhoneCallSignalingData(phone_call_id=10, data=b'x'), {}, {})
    await session.on_hangup()
    assert not leaks(caplog), leaks(caplog)


def test_error_label_anchor():
    from telegent.app import error_label
    assert error_label(UnknownError(value='[400 CALL_X] private mrk7q')) == 'UnknownError'
