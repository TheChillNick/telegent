"""MOU-408 probes: every timeout/cancel path of _dial_outgoing -> _ring_outgoing -> _setup_outgoing."""
import asyncio
import logging
from types import SimpleNamespace as NS

import pytest
from pyrogram.errors import UnknownError
from pyrogram.raw import functions, types

from test_outgoing import setup_app, start_dial
from telegent.telegram.call_session import SessionState


def discards(invoked):
    return [q for q in invoked if isinstance(q, functions.phone.DiscardCall)]


def patch_invoke(app, rpc, behaviour):
    original = app.app.invoke.side_effect

    async def side(query, **kw):
        if isinstance(query, rpc):
            return await behaviour(query, original, **kw)
        return await original(query, **kw)

    app.app.invoke.side_effect = side


async def accept(app, established):
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))


async def hang(*_a, **_k):
    await asyncio.Event().wait()


# ---------------------------------------------------------------- ring phase
async def test_p01_resolve_peer_hang_is_no_answer_without_discard(monkeypatch):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    app.app.resolve_peer.side_effect = hang
    assert await asyncio.wait_for(app._call_outgoing('x'), 2) == 'no-answer'
    assert not discards(invoked) and not app.sessions and not live.greetings
    app.ntg.stop.assert_awaited_once()


async def test_p02_dh_config_error_is_failed(monkeypatch, caplog):
    app, _, invoked, _ = setup_app(monkeypatch)

    async def boom(query, original, **kw):
        raise RuntimeError('SECRET dh')

    patch_invoke(app, functions.messages.GetDhConfig, boom)
    caplog.set_level(logging.DEBUG)
    assert await app._call_outgoing('x') == 'failed'
    assert 'Outgoing setup failed: RuntimeError' in caplog.text and 'SECRET' not in caplog.text
    assert not discards(invoked) and not app.sessions


async def test_p03_requestcall_in_flight_at_ring_deadline_is_discarded_missed(monkeypatch):
    app, _, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)

    async def slow(query, original, **kw):
        await asyncio.sleep(0.2)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.RequestCall, slow)
    assert await asyncio.wait_for(app._call_outgoing('x'), 2) == 'no-answer'
    d = discards(invoked)
    assert len(d) == 1 and d[0].peer.id == 10
    assert isinstance(d[0].reason, types.PhoneCallDiscardReasonMissed)
    assert not app.sessions


async def test_p04_requestcall_in_flight_then_error_logs_label_only(monkeypatch, caplog):
    app, _, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)

    async def slow_fail(query, original, **kw):
        await asyncio.sleep(0.2)
        raise UnknownError(value='[400 SECRET text]')

    patch_invoke(app, functions.phone.RequestCall, slow_fail)
    caplog.set_level(logging.DEBUG)
    assert await asyncio.wait_for(app._call_outgoing('x'), 2) == 'no-answer'
    assert 'RequestCall response unavailable; remote state unknown: UnknownError' in caplog.text
    assert 'SECRET' not in caplog.text
    assert not discards(invoked) and not app.sessions


@pytest.mark.parametrize('timeout_error', [TimeoutError, asyncio.TimeoutError], ids=['builtin', 'asyncio'])
async def test_p05_requestcall_rpc_timeout_result(monkeypatch, caplog, timeout_error):
    """Pyrogram raises builtin TimeoutError('Request timed out') with retries=0.
    asyncio.TimeoutError is builtin TimeoutError only on 3.11+; on 3.10 both must give no-answer."""
    app, _, invoked, _ = setup_app(monkeypatch)

    async def rpc_timeout(query, original, **kw):
        raise timeout_error('Request timed out')

    patch_invoke(app, functions.phone.RequestCall, rpc_timeout)
    caplog.set_level(logging.DEBUG)
    result = await app._call_outgoing('x')
    assert result == 'no-answer', result


async def test_p06_ring_timeout_while_waiting_answer(monkeypatch):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05, kai_call_setup_timeout=5)
    assert await asyncio.wait_for(app._call_outgoing('x'), 2) == 'no-answer'
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonMissed)
    assert not app.sessions and not live.greetings
    assert not app._outgoing_accept and app._outgoing_ready.is_set()


# ---------------------------------------------------------------- setup phase
async def test_p07_exchange_keys_hang_is_failed_disconnect(monkeypatch, caplog):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=0.05)
    app.ntg.exchange_keys.side_effect = hang
    caplog.set_level(logging.DEBUG)
    dial, session = await start_dial(app)
    await accept(app, established)
    assert await asyncio.wait_for(dial, 2) == 'failed'
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonDisconnect)
    assert 'Outgoing setup timed out after answer' in caplog.text
    assert not app.sessions and not live.greetings and session.state == SessionState.IDLE


async def test_p08_confirmcall_rpc_timeout_logged_as_rpc_failure(monkeypatch, caplog):
    """ConfirmCall uses Pyrogram defaults; its own RPC timeout raises builtin TimeoutError."""
    app, _, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=15)

    async def rpc_timeout(query, original, **kw):
        raise TimeoutError('Request timed out')

    patch_invoke(app, functions.phone.ConfirmCall, rpc_timeout)
    caplog.set_level(logging.DEBUG)
    dial, _ = await start_dial(app)
    await accept(app, established)
    result = await asyncio.wait_for(dial, 2)
    lines = [r.getMessage() for r in caplog.records if 'Outgoing setup' in r.getMessage()]
    assert result == 'failed'
    assert not app.sessions
    # Honest diagnosis: the deadline did not expire (it was 15 s, the RPC failed at once).
    assert 'Outgoing setup failed: TimeoutError' in lines, lines
    assert not any('timed out after answer' in line for line in lines), lines


async def test_p08b_key_exchange_timeout_logged_as_setup_failure(monkeypatch, caplog):
    """A TimeoutError of the key exchange after the answer is `failed`, not the setup deadline."""
    app, _, _, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=15)
    app.ntg.exchange_keys.side_effect = TimeoutError('Request timed out')
    caplog.set_level(logging.DEBUG)
    dial, _ = await start_dial(app)
    await accept(app, established)
    result = await asyncio.wait_for(dial, 2)
    lines = [r.getMessage() for r in caplog.records if 'Outgoing setup' in r.getMessage()]
    assert result == 'failed'
    assert not app.sessions
    assert 'Outgoing setup failed: TimeoutError' in lines, lines
    assert not any('timed out after answer' in line for line in lines), lines


async def test_p09_confirmcall_hangs_but_update_path_connects(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=1)
    patch_invoke(app, functions.phone.ConfirmCall, hang)
    dial, session = await start_dial(app)
    await accept(app, established)
    for _ in range(100):
        if session.state == SessionState.CONFIRMING:
            break
        await asyncio.sleep(0)
    await app._handle_call_established(established)  # raw UpdatePhoneCall(PhoneCall)
    assert await asyncio.wait_for(dial, 2) == 'answered'
    assert not discards(invoked) and app.sessions.get(7) is session
    await session.on_hangup()
    assert not app.sessions


async def test_p10_setup_deadline_during_connect_p2p(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=0.05)
    app.ntg.connect_p2p.side_effect = hang
    dial, session = await start_dial(app)
    await accept(app, established)
    assert await asyncio.wait_for(dial, 2) == 'failed'
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonDisconnect)
    assert not app.sessions and not live.greetings and session.state == SessionState.IDLE
    app.ntg.stop.assert_awaited_once()


async def test_p11_callee_hangs_up_during_setup(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=1)
    patch_invoke(app, functions.phone.ConfirmCall, hang)
    dial, session = await start_dial(app)
    await accept(app, established)
    for _ in range(100):
        if session.state == SessionState.CONFIRMING:
            break
        await asyncio.sleep(0)
    await app._handle_call_discarded(NS(id=10, reason=types.PhoneCallDiscardReasonHangup()))
    assert await asyncio.wait_for(dial, 2) == 'declined'
    assert not discards(invoked) and not app.sessions and not live.greetings


async def test_p12_late_confirmcall_after_setup_deadline_does_not_set_up_media(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=0.05)

    async def slow(query, original, **kw):
        await asyncio.sleep(0.2)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.ConfirmCall, slow)
    dial, session = await start_dial(app)
    await accept(app, established)
    assert await asyncio.wait_for(dial, 2) == 'failed'
    await asyncio.sleep(0.3)
    app.ntg.connect_p2p.assert_not_awaited()
    assert not app.sessions and not live.greetings
    await app._handle_call_established(established)  # late raw update: session is gone
    app.ntg.connect_p2p.assert_not_awaited()


async def test_p13_media_failure_after_ring_deadline_is_failed(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=0.1, kai_call_setup_timeout=1)
    app.ntg.connect_p2p.side_effect = RuntimeError('SECRET native')

    async def slow(query, original, **kw):
        await asyncio.sleep(0.2)  # the answer lands, setup finishes after the ring deadline
        return await original(query, **kw)

    patch_invoke(app, functions.phone.ConfirmCall, slow)
    dial, session = await start_dial(app)
    await accept(app, established)
    assert await asyncio.wait_for(dial, 2) == 'failed'
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonDisconnect)
    assert not app.sessions


async def test_p14_update_path_media_hang_then_setup_deadline(monkeypatch, caplog):
    """Raw PhoneCall update wins the race and hangs in connect_p2p; ConfirmCall path is ignored."""
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=0.1)
    gate = asyncio.Event()

    async def slow_connect(*a, **k):
        await gate.wait()
        raise RuntimeError('native gone')

    app.ntg.connect_p2p.side_effect = slow_connect

    async def slow(query, original, **kw):
        await asyncio.sleep(0.02)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.ConfirmCall, slow)
    dial, session = await start_dial(app)
    await accept(app, established)
    for _ in range(100):
        if session.state == SessionState.CONFIRMING:
            break
        await asyncio.sleep(0)
    update = asyncio.create_task(app._handle_call_established(established))
    assert await asyncio.wait_for(dial, 2) == 'failed'
    assert not app.sessions
    gate.set()
    await asyncio.wait_for(update, 1)
    assert len(discards(invoked)) <= 2


# ---------------------------------------------------------------- outer cancellation (shutdown)
async def test_p15_shutdown_cancel_during_setup_discards_and_closes(monkeypatch):
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=5)
    patch_invoke(app, functions.phone.ConfirmCall, hang)
    dial, session = await start_dial(app)
    await accept(app, established)
    for _ in range(100):
        if session.state == SessionState.CONFIRMING:
            break
        await asyncio.sleep(0)
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonDisconnect)
    assert not app.sessions and session.state == SessionState.IDLE


async def test_p16_shutdown_cancel_with_requestcall_in_flight(monkeypatch):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=5)
    started = asyncio.Event()

    async def slow(query, original, **kw):
        started.set()
        await asyncio.sleep(0.1)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.RequestCall, slow)
    dial = asyncio.create_task(app._call_outgoing('x'))
    await started.wait()
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(dial, 2)
    d = discards(invoked)
    assert len(d) == 1 and d[0].peer.id == 10
    assert not app.sessions
