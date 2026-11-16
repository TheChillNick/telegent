"""MOU-574: the app stop stays inside Docker's 10 s grace.

Item 1 (MOU-559 F1 = MOU-560 R2): a stop during the RequestCall itself hands the id to the late discard.
Item 3 (MOU-559 F2): an id that came in time gets its own DiscardCall wait, and every DiscardCall during
the stop is one try of at most STOP_DISCARD_TIMEOUT.
Item 5 (MOU-560 R5): a failing client stop logs the class only.
"""
import asyncio
import logging
from unittest.mock import AsyncMock

from pyrogram.raw import functions, types

import telegent.app as app_module
from test_outgoing import setup_app
from test_outgoing_deadlines import discards, patch_invoke
from test_outgoing_gaps import slow_requestcall


def warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


async def cancel_leftovers(app):
    leftover = list(app._late_discards)
    for task in leftover:  # what asyncio.run does at the end of the stop
        task.cancel()
    await asyncio.gather(*leftover, return_exceptions=True)


async def test_stop_during_the_requestcall_discards_the_late_id_as_missed(monkeypatch, caplog):
    """Default ring deadline (40 s): the stop itself interrupts the RequestCall; the id comes at 0.3 s."""
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    slow_requestcall(app, seconds=0.3)
    app._outbox_task = asyncio.create_task(app._call_outgoing('SECRET text'))
    await asyncio.sleep(0.1)  # RequestCall in flight, ring deadline far away
    caplog.set_level(logging.INFO)
    await asyncio.wait_for(app._shutdown(), 2)
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonMissed)
    assert not app._late_discards and not app.sessions
    app.app.stop.assert_awaited_once()
    assert warnings(caplog) == [] and 'SECRET' not in caplog.text


async def test_stop_during_a_slow_requestcall_waits_only_the_late_discard_bound(monkeypatch, caplog):
    """MOU-560 P1 F-slow: the stop no longer waits for the RequestCall RPC timeout (was 11.91 s with id at 12 s)."""
    monkeypatch.setattr(app_module, 'LATE_DISCARD_TIMEOUT', 0.2)
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    slow_requestcall(app, seconds=1.0)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    caplog.set_level(logging.INFO)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(app._shutdown(), 2)
    assert loop.time() - started < 0.6  # the 0.2 s bound, not the 1 s RPC
    assert not discards(invoked)
    app.app.stop.assert_awaited_once()
    assert warnings(caplog) == [
        'Ring discard not sent for 1 call(s): no RequestCall id within 0 s of the stop; '
        'the phone may ring until Telegram ends the call.']
    await cancel_leftovers(app)


async def test_stop_before_the_requestcall_keeps_the_old_path(monkeypatch):
    """A stop while the phone already rings (id known) still sends one Disconnect from _call_outgoing."""
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    for _ in range(100):
        session = app.sessions.get(7)
        if session and session.call_id == 10:
            break
        await asyncio.sleep(0.01)
    await asyncio.wait_for(app._shutdown(), 2)
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonDisconnect)
    assert not app._late_discards


async def test_ring_deadline_cancel_without_stop_waits_for_the_id(monkeypatch):
    """The new early hand-off needs the stop: a ring-deadline cancel still waits for the id (MOU-429 R5 path)."""
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    slow_requestcall(app, seconds=0.2)
    result = await asyncio.wait_for(app._call_outgoing('x'), 2)
    assert result == 'no-answer'
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonMissed)
    assert not app._late_discards


async def test_discard_rpc_after_a_late_id_gets_its_own_wait(monkeypatch, caplog):
    """MOU-559 F2: id at 0.15 s of a 0.2 s id bound, DiscardCall takes 0.2 s: it is not cut at the id bound."""
    monkeypatch.setattr(app_module, 'LATE_DISCARD_TIMEOUT', 0.2)
    monkeypatch.setattr(app_module, 'STOP_DISCARD_TIMEOUT', 0.5)
    app, live, invoked, _ = setup_app(monkeypatch)
    order = []

    async def slow_discard(query, original, **kw):
        await asyncio.sleep(0.2)
        order.append('discard-done')
        return await original(query, **kw)

    async def client_stop():
        order.append('client-stop')

    app.app.stop = client_stop
    slow_requestcall(app, seconds=0.25)  # id 0.15 s after the stop below
    patch_invoke(app, functions.phone.DiscardCall, slow_discard)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    caplog.set_level(logging.INFO)
    await asyncio.wait_for(app._shutdown(), 2)
    assert order == ['discard-done', 'client-stop'], order
    assert warnings(caplog) == []
    assert not app._late_discards


async def test_hanging_discard_rpc_is_bounded_and_logged(monkeypatch, caplog):
    """MOU-559 F2: the DiscardCall wait is bounded too; the whole late path is at most id bound + RPC bound."""
    monkeypatch.setattr(app_module, 'LATE_DISCARD_TIMEOUT', 0.2)
    monkeypatch.setattr(app_module, 'STOP_DISCARD_TIMEOUT', 0.2)
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    never = asyncio.Event()

    async def hanging_discard(query, original, **kw):
        await never.wait()

    slow_requestcall(app, seconds=0.15)
    patch_invoke(app, functions.phone.DiscardCall, hanging_discard)
    app._outbox_task = asyncio.create_task(app._call_outgoing('SECRET text'))
    await asyncio.sleep(0.1)
    caplog.set_level(logging.INFO)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(app._shutdown(), 2)
    elapsed = loop.time() - started
    assert 0.15 < elapsed < 0.6, elapsed  # id at ~0.05 s, then the 0.2 s RPC bound; never the id bound + more
    assert warnings(caplog) in ([
        'Ring discard not confirmed for 1 call(s): DiscardCall still running 0 s after the id; '
        'the phone may ring until Telegram ends the call.'], ['Discard failed for user=7: TimeoutError'])
    assert 'SECRET' not in caplog.text
    app.app.stop.assert_awaited_once()
    await cancel_leftovers(app)


async def test_failing_client_stop_logs_the_class_only(monkeypatch, caplog):
    """MOU-560 R5: app.py client-stop line uses error_label, never the exception text."""
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock(side_effect=ConnectionError('SECRET peer 7 text'))
    caplog.set_level(logging.INFO)
    await app._shutdown()
    assert warnings(caplog) == ['Gagal menutup klien Telegram: ConnectionError']
    assert 'SECRET' not in caplog.text


async def test_discard_during_the_stop_is_one_bounded_try(monkeypatch):
    """Stop while the phone rings: the Disconnect DiscardCall uses retries=0, timeout=STOP_DISCARD_TIMEOUT."""
    app, live, invoked, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    kwargs = []

    async def record(query, original, **kw):
        kwargs.append(kw)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.DiscardCall, record)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    for _ in range(100):
        session = app.sessions.get(7)
        if session and session.call_id == 10:
            break
        await asyncio.sleep(0.01)
    await asyncio.wait_for(app._shutdown(), 2)
    assert kwargs == [{'retries': 0, 'timeout': app_module.STOP_DISCARD_TIMEOUT, 'sleep_threshold': 0}]
    assert app_module.STOP_DISCARD_TIMEOUT == 2.0


async def test_discard_outside_the_stop_keeps_pyrogram_defaults(monkeypatch):
    """An incoming reject (no stop) keeps Pyrogram's retries and timeout."""
    app, live, invoked, _ = setup_app(monkeypatch)
    kwargs = []

    async def record(query, original, **kw):
        kwargs.append(kw)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.DiscardCall, record)
    await app._discard_call(types.PhoneCallRequested(id=5, access_hash=6, date=0, admin_id=9, participant_id=7,
                                                     g_a_hash=b'', protocol=None), types.PhoneCallDiscardReasonBusy(), 9)
    assert kwargs == [{}]


async def test_stop_while_ringing_bounds_invoke_before_send(monkeypatch, caplog):
    monkeypatch.setattr(app_module, 'STOP_DISCARD_TIMEOUT', 0.2)
    app, _, _, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    async def hang(query, original, **kw):
        await asyncio.Event().wait()
    patch_invoke(app, functions.phone.DiscardCall, hang)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    while not (app.sessions.get(7) and app.sessions[7].call_id == 10):
        await asyncio.sleep(0.01)
    start = asyncio.get_running_loop().time()
    await asyncio.wait_for(app._shutdown(), 1)
    assert asyncio.get_running_loop().time() - start <= 0.3
    assert 'Discard failed for user=7: TimeoutError' in caplog.text


async def test_stop_floodwait_does_not_sleep_or_retry(monkeypatch, caplog):
    from pyrogram.session import Session
    from pyrogram.errors import FloodWait
    from types import SimpleNamespace
    monkeypatch.setattr(app_module, 'STOP_DISCARD_TIMEOUT', 0.2)
    app, _, _, _ = setup_app(monkeypatch)
    app.app.stop = AsyncMock()
    sends = []
    async def send(query, timeout):
        sends.append(query)
        raise FloodWait(value=1)
    stub = SimpleNamespace(is_started=asyncio.Event(), WAIT_TIMEOUT=0.01, send=send, client=SimpleNamespace(name='test'))
    async def invoke(query, original, **kw):
        return await Session.invoke(stub, query, **kw)
    patch_invoke(app, functions.phone.DiscardCall, invoke)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    while not (app.sessions.get(7) and app.sessions[7].call_id == 10):
        await asyncio.sleep(0.01)
    start = asyncio.get_running_loop().time()
    await asyncio.wait_for(app._shutdown(), 1)
    assert asyncio.get_running_loop().time() - start <= 0.3
    assert len(sends) == 1
    assert 'Discard failed for user=7: FloodWait' in caplog.text


async def test_discard_outside_stop_has_no_outer_deadline(monkeypatch):
    monkeypatch.setattr(app_module, 'STOP_DISCARD_TIMEOUT', 0.05)
    app, _, _, _ = setup_app(monkeypatch)
    completed = []
    async def slow(query, original, **kw):
        await asyncio.sleep(0.1)
        completed.append(True)
        return await original(query, **kw)
    patch_invoke(app, functions.phone.DiscardCall, slow)
    await app._discard_call(types.PhoneCallRequested(id=5, access_hash=6, date=0, admin_id=9, participant_id=7,
                                                     g_a_hash=b'', protocol=None), types.PhoneCallDiscardReasonBusy(), 9)
    assert completed == [True]
