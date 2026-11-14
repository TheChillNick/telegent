import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
from pyrogram.raw import functions, types

from telegent.app import TelegentApp
from telegent.config import Settings
from telegent.telegram.call_session import SessionState
from helpers import FakeLiveSession


def setup_app(monkeypatch, **overrides):
    settings = Settings(telegram_api_id=1, telegram_api_hash='fake', gemini_api_key='fake',
                        kai_call_user_id=7, allowed_user_ids='7', idle_enabled=False,
                        _env_file=None, **overrides)
    app = TelegentApp(settings)
    protocol = NS(min_layer=92, max_layer=92, udp_p2p=True, udp_reflector=True,
                  library_versions=['13.0.0'])
    app.ntg = NS(create_p2p_call=AsyncMock(), init_exchange=AsyncMock(return_value=b'hash'),
                 exchange_keys=AsyncMock(return_value=NS(g_a_or_b=b'ga', key_fingerprint=123)),
                 get_protocol=Mock(return_value=protocol), connect_p2p=AsyncMock(),
                 set_stream_sources=AsyncMock(), stop=AsyncMock(), send_external_frame=AsyncMock())
    app.signaling = NS(flush_pending=Mock())
    live = FakeLiveSession()
    @asynccontextmanager
    async def live_factory():
        yield live
    monkeypatch.setattr(app, '_live_factory', lambda *args: live_factory)
    established = NS(id=10, access_hash=99, admin_id=42, participant_id=7,
                     g_a_or_b=b'ga', key_fingerprint=123, connections=[], protocol=protocol,
                     p2p_allowed=False)
    invoked = []
    async def invoke(query, **kwargs):
        invoked.append(query)
        if isinstance(query, functions.messages.GetDhConfig):
            return NS(g=3, p=b'p', random=b'r')
        if isinstance(query, functions.phone.RequestCall):
            return NS(phone_call=NS(id=10, access_hash=99))
        if isinstance(query, functions.phone.ConfirmCall):
            return NS(phone_call=established)
    app.app = NS(invoke=AsyncMock(side_effect=invoke),
                 resolve_peer=AsyncMock(return_value=types.InputPeerUser(user_id=7, access_hash=88)))
    return app, live, invoked, established


async def start_dial(app, text='Decision?'):
    task = asyncio.create_task(app._call_outgoing(text))
    for _ in range(100):
        session = app.sessions.get(7)
        if session and session.call_id == 10:
            return task, session
        if task.done():
            await task
            pytest.fail('dial ended before RequestCall')
        await asyncio.sleep(0)
    pytest.fail('dial never requested')


async def test_caller_handshake_reuses_live_and_end_call(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch)
    dial, session = await start_dial(app, 'x'*1200)
    assert not live.greetings  # never speak before answer
    try:
        await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
        assert await dial == 'answered'
        for _ in range(20):
            if live.greetings:
                break
            await asyncio.sleep(0)
        app.ntg.init_exchange.assert_awaited_once()
        assert app.ntg.init_exchange.await_args.args[2] is None  # caller side
        app.ntg.exchange_keys.assert_awaited_once_with(7, b'gb', 0)
        request = next(q for q in invoked if isinstance(q, functions.phone.RequestCall))
        assert request.user_id.user_id == 7
        assert request.g_a_hash == b'hash'
        rpc = next(c for c in app.app.invoke.call_args_list if isinstance(c.args[0], functions.phone.RequestCall))
        assert rpc.kwargs['retries'] == 0
        assert rpc.kwargs['sleep_threshold'] == 0
        assert rpc.kwargs['timeout'] <= app.settings.kai_call_ring_timeout
        confirm = next(q for q in invoked if isinstance(q, functions.phone.ConfirmCall))
        assert (confirm.g_a, confirm.key_fingerprint) == (b'ga', 123)
        assert live.greetings and 'untrusted data' in live.greetings[0]
        assert 'x'*1000 in live.greetings[0] and 'x'*1001 not in live.greetings[0]
        assert session.state == SessionState.ACTIVE
        assert app.ntg.connect_p2p.await_args.kwargs['p2p_allowed'] is False
        await app._handle_call_established(established)  # duplicate is harmless
        assert app.ntg.connect_p2p.await_count == 1
        await session.on_hangup()
        assert not app.sessions
        assert any(isinstance(q, functions.phone.DiscardCall) for q in invoked)
    finally:
        await session.close()


@pytest.mark.parametrize('reason,outcome', [(types.PhoneCallDiscardReasonBusy(), 'busy'),
    (types.PhoneCallDiscardReasonHangup(), 'declined'),
    (types.PhoneCallDiscardReasonMissed(), 'no-answer')])
async def test_remote_discard_no_redial(monkeypatch, reason, outcome):
    app, live, invoked, _ = setup_app(monkeypatch)
    dial, session = await start_dial(app)
    await app._handle_call_discarded(NS(id=10, reason=reason))
    assert await dial == outcome
    assert not app.sessions and not live.greetings
    assert sum(isinstance(q, functions.phone.RequestCall) for q in invoked) == 1
    app.ntg.stop.assert_awaited_once()


async def test_ring_timeout_discards_and_stops(monkeypatch):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.02)
    assert await app._call_outgoing('hello') == 'no-answer'
    assert not app.sessions and not live.greetings
    discard = next(q for q in invoked if isinstance(q, functions.phone.DiscardCall))
    assert isinstance(discard.reason, types.PhoneCallDiscardReasonMissed)
    app.ntg.stop.assert_awaited_once()


async def test_no_whitelist_means_no_outgoing_even_if_incoming_unrestricted(monkeypatch):
    app, _, _, _ = setup_app(monkeypatch)
    app.settings.allowed_user_ids = ''
    assert await app._call_outgoing('hello') == 'not-whitelisted'
    app.app.resolve_peer.assert_not_awaited()
    app.ntg.create_p2p_call.assert_not_awaited()


async def test_request_failure_is_terminal_and_private(monkeypatch, caplog):
    app, _, invoked, _ = setup_app(monkeypatch)
    app.app.resolve_peer.side_effect = RuntimeError('sensitive')
    assert await app._call_outgoing('hello') == 'failed'
    assert not app.sessions
    assert 'sensitive' not in caplog.text


async def test_feature_off_by_default():
    settings = Settings(telegram_api_id=1, telegram_api_hash='fake', gemini_api_key='fake', _env_file=None)
    assert settings.kai_outbox_dir == ''


async def test_accepted_update_before_request_response(monkeypatch):
    app, _, _, established = setup_app(monkeypatch)
    original = app.app.invoke.side_effect
    early = []
    async def invoke(query, **kwargs):
        if isinstance(query, functions.phone.RequestCall):
            early.append(asyncio.create_task(app._handle_call_accepted(
                NS(id=10, g_b=b'gb', protocol=established.protocol))))
            await asyncio.sleep(0)  # update arrives while call_id is still unknown
            assert not early[0].done()
        return await original(query)
    app.app.invoke.side_effect = invoke
    assert await app._call_outgoing('Decision?') == 'answered'
    await asyncio.gather(*early)
    await app.sessions[7].on_hangup()


async def test_cancel_ringing_discards_and_cleans_up(monkeypatch):
    app, _, invoked, _ = setup_app(monkeypatch)
    dial, _ = await start_dial(app)
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    assert not app.sessions
    assert any(isinstance(q, functions.phone.DiscardCall) for q in invoked)
    app.ntg.stop.assert_awaited_once()


async def test_incoming_call_during_outgoing_reservation_is_rejected(monkeypatch):
    app, _, invoked, _ = setup_app(monkeypatch, max_concurrent_calls=1)
    dial, _ = await start_dial(app)
    await app._handle_call_requested(NS(admin_id=7, id=11, access_hash=100))
    assert app.sessions[7].call_id == 10
    assert any(isinstance(q, functions.phone.DiscardCall) and q.peer.id == 11 for q in invoked)
    await app._handle_call_discarded(NS(id=10, reason=types.PhoneCallDiscardReasonBusy()))
    assert await dial == 'busy'


async def test_confirm_failure_discarded_without_speech(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch)
    original = app.app.invoke.side_effect
    async def invoke(query, **kwargs):
        if isinstance(query, functions.phone.ConfirmCall):
            raise RuntimeError('sensitive')
        return await original(query)
    app.app.invoke.side_effect = invoke
    dial, _ = await start_dial(app)
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    assert await dial == 'failed'
    assert not app.sessions and not live.greetings
    assert any(isinstance(q, functions.phone.DiscardCall) for q in invoked)


async def test_outbox_initialization_recovers_after_transient_failure(monkeypatch, tmp_path):
    import telegent.app as module
    app, _, _, _ = setup_app(monkeypatch, kai_outbox_dir=str(tmp_path))
    recovered = asyncio.Event()
    creations = []
    class Box:
        async def process(self, **kwargs):
            recovered.set()
    def factory(*args, **kwargs):
        creations.append(True)
        if len(creations) == 1:
            raise OSError('temporarily unavailable')
        return Box()
    monkeypatch.setattr(module, 'Outbox', factory)
    task = asyncio.create_task(app._watch_outbox())
    try:
        await asyncio.wait_for(recovered.wait(), 2.5)
        assert len(creations) == 2
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_failed_watcher_does_not_skip_shutdown_cleanup(monkeypatch):
    app, _, _, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    async def failed():
        raise OSError('unavailable')
    app._outbox_task = asyncio.create_task(failed())
    await asyncio.sleep(0)
    await app._shutdown()
    app.app.stop.assert_awaited_once()


async def test_timeout_during_request_keeps_response_to_discard_remote_call(monkeypatch):
    app, _, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.01)
    original = app.app.invoke.side_effect
    async def invoke(query, **kwargs):
        if isinstance(query, functions.phone.RequestCall):
            await asyncio.sleep(0.03)  # remote call already exists, RPC reply is delayed
        return await original(query)
    app.app.invoke.side_effect = invoke
    assert await app._call_outgoing('Decision?') == 'no-answer'
    assert any(isinstance(q, functions.phone.DiscardCall) and q.peer.id == 10 for q in invoked)
    assert not app.sessions


async def test_remote_discard_during_pending_confirmation_is_prompt_and_preserved(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=1)
    confirming = asyncio.Event()
    original = app.app.invoke.side_effect
    async def invoke(query, **kwargs):
        if isinstance(query, functions.phone.ConfirmCall):
            confirming.set()
            await asyncio.Event().wait()
        return await original(query)
    app.app.invoke.side_effect = invoke
    dial, _ = await start_dial(app)
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    await asyncio.wait_for(confirming.wait(), 0.5)
    await app._handle_call_discarded(NS(id=10, reason=types.PhoneCallDiscardReasonBusy()))
    assert await asyncio.wait_for(dial, 0.2) == 'busy'
    assert not app.sessions and not live.greetings



async def test_outgoing_transcripts_tools_and_errors_do_not_log_request_content(monkeypatch, caplog):
    import logging
    from telegent.ai.events import TranscriptIn, TranscriptOut, ToolCallEvent
    app, live, _, established = setup_app(monkeypatch)
    private = 'Private announcement content'
    live._events = [TranscriptIn(private), TranscriptOut(private),
                    ToolCallEvent(id='test', name=private, args={'text': private})]
    caplog.set_level(logging.DEBUG)
    dial, session = await start_dial(app, private)
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    assert await dial == 'answered'
    for _ in range(50):
        if live.tool_responses:
            break
        await asyncio.sleep(0)
    assert live.tool_responses  # tools still work and deliver their response to Gemini
    await session.on_hangup()
    assert private not in caplog.text
    assert 'result=answered' in caplog.text
