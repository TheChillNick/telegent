"""MOU-408 probes: a cancelled close() always unregisters, reaches IDLE, runs each clean-up step once."""
import asyncio
import gc
import logging
from collections import Counter
from contextlib import asynccontextmanager

import pytest

from telegent.ai.tools.registry import ToolRegistry
from telegent.telegram.call_session import CallSession, SessionState
from helpers import FakeLiveSession, FakeSink
from test_outgoing import setup_app


def make(settings, *, outgoing=False, on_stop=None, logger=None):
    @asynccontextmanager
    async def factory():
        yield FakeLiveSession()

    return CallSession(user_id=7, call_id=10, access_hash=1, sink=FakeSink(), live_factory=factory,
                       tools=ToolRegistry(), settings=settings, logger=logger or logging.getLogger('probe'),
                       on_stop=on_stop, opening_text='SECRET request' if outgoing else None)


def instrument(session):
    calls = Counter()
    states = []
    orig_flush, orig_set = session.flush_outbound, session.set_state

    def flush():
        calls['flush'] += 1
        orig_flush()

    def set_state(s):
        states.append(s)
        orig_set(s)

    session.flush_outbound = flush
    session.set_state = set_state
    return calls, states


async def slow_to_die(delay=0.05):
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await asyncio.sleep(delay)
        raise


async def registered(monkeypatch, outgoing=True):
    """Session registered in a real TelegentApp with the real _make_stop."""
    app, *_ = setup_app(monkeypatch)
    stop_calls = []

    async def ntg_stop(uid):
        stop_calls.append(uid)
        await asyncio.sleep(0.05)

    app.ntg.stop = ntg_stop
    session = make(app.settings, outgoing=outgoing, on_stop=app._make_stop(7))
    app.sessions[7] = session
    return app, session, stop_calls


@pytest.mark.parametrize('outgoing', [True, False])
async def test_q01_cancel_during_task_gather_unregisters_once(monkeypatch, outgoing):
    app, session, stop_calls = await registered(monkeypatch, outgoing)
    calls, states = instrument(session)
    session._tasks.append(asyncio.create_task(slow_to_die()))
    session._live_task = asyncio.create_task(asyncio.Event().wait())
    session._hangup_task = asyncio.create_task(asyncio.Event().wait())
    await asyncio.sleep(0)
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.01)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert session.state == SessionState.IDLE
    assert not app.sessions and stop_calls == [7]
    assert calls['flush'] == 1
    assert states == [SessionState.ENDING, SessionState.IDLE]
    await session.close()  # second close: no step repeated
    assert calls['flush'] == 1 and stop_calls == [7] and states == [SessionState.ENDING, SessionState.IDLE]
    await asyncio.sleep(0)
    assert session._live_task.cancelled() and session._hangup_task.cancelled()


async def test_q02_cancel_twice_second_during_on_stop(monkeypatch):
    app, session, stop_calls = await registered(monkeypatch)
    calls, states = instrument(session)
    session._tasks.append(asyncio.create_task(slow_to_die(0.02)))
    await asyncio.sleep(0)
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.005)
    closing.cancel()  # 1st: while tasks die
    for _ in range(200):
        if stop_calls:
            break
        await asyncio.sleep(0.001)
    assert stop_calls, 'on_stop never started'
    closing.cancel()  # 2nd: while the shielded stop runs
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert session.state == SessionState.IDLE  # reached at once
    assert 7 in app.sessions  # stop still running in the background ...
    await asyncio.sleep(0.1)
    assert not app.sessions and stop_calls == [7]  # ... and finishes
    assert states == [SessionState.ENDING, SessionState.IDLE] and calls['flush'] == 1


async def test_q03_pending_cancel_with_no_tasks_still_unregisters(monkeypatch):
    app, session, stop_calls = await registered(monkeypatch)

    async def caller():
        asyncio.current_task().cancel()  # cancellation already pending when close() starts
        await session.close()

    task = asyncio.create_task(caller())
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.state == SessionState.IDLE
    await asyncio.sleep(0.1)
    assert not app.sessions and stop_calls == [7]


@pytest.mark.parametrize('outgoing,expect', [(True, 'Gagal menghentikan media: RuntimeError'),
                                             (False, 'Gagal menghentikan media: SECRET stop')])
async def test_q04_on_stop_error_is_logged_and_close_finishes(settings, caplog, outgoing, expect):
    async def bad_stop():
        raise RuntimeError('SECRET stop')

    session = make(settings, outgoing=outgoing, on_stop=bad_stop)
    caplog.set_level(logging.DEBUG)
    await session.close()
    assert session.state == SessionState.IDLE
    assert expect in caplog.text
    assert ('SECRET' in caplog.text) is (not outgoing)


async def test_q05_stop_error_after_cancel_is_retrieved(settings):
    """A shielded stop that fails after close() was cancelled must not log 'exception was never retrieved'."""
    seen = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _l, ctx: seen.append(ctx.get('message')))

    async def bad_stop():
        await asyncio.sleep(0.02)
        raise RuntimeError('SECRET late stop')

    session = make(settings, outgoing=True, on_stop=bad_stop)
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.005)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await asyncio.sleep(0.05)
    gc.collect()
    await asyncio.sleep(0)
    loop.set_exception_handler(None)
    assert not seen, seen


async def test_q06_close_from_own_live_task(settings):
    session = make(settings, outgoing=True)
    session.start()
    for _ in range(50):
        if session.state == SessionState.ACTIVE:
            break
        await asyncio.sleep(0.01)
    assert session.state == SessionState.ACTIVE
    session._ending.set()  # live task leaves the context and closes itself (current task)
    await asyncio.wait_for(session._live_task, 2)
    assert session.state == SessionState.IDLE and not session._tasks


async def test_q07_close_from_hangup_task_then_outer_close(monkeypatch):
    app, session, stop_calls = await registered(monkeypatch)
    session._tasks.append(asyncio.create_task(slow_to_die(0.02)))

    async def hangup():
        await session.close()

    session.on_hangup = hangup
    session.schedule_hangup()
    await asyncio.wait_for(session._hangup_task, 20)
    assert not app.sessions and stop_calls == [7] and session.state == SessionState.IDLE


async def test_q08_call_outgoing_finally_with_cancelled_close_unregisters(monkeypatch):
    """Shutdown cancels _call_outgoing twice: once in setup, once inside session.close()."""
    from types import SimpleNamespace as NS
    from pyrogram.raw import functions
    app, live, invoked, established = setup_app(monkeypatch, kai_call_ring_timeout=5, kai_call_setup_timeout=5)
    original = app.app.invoke.side_effect

    async def side(query, **kw):
        if isinstance(query, functions.phone.ConfirmCall):
            await asyncio.Event().wait()
        return await original(query, **kw)

    app.app.invoke.side_effect = side
    stop_started = asyncio.Event()

    async def ntg_stop(uid):
        stop_started.set()
        await asyncio.sleep(0.05)

    app.ntg.stop = ntg_stop
    dial = asyncio.create_task(app._call_outgoing('x'))
    for _ in range(200):
        s = app.sessions.get(7)
        if s and s.call_id == 10:
            break
        await asyncio.sleep(0)
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    for _ in range(200):
        if s.state.value == 'CONFIRMING':
            break
        await asyncio.sleep(0)
    dial.cancel()
    await stop_started.wait()
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    await asyncio.sleep(0.1)
    assert not app.sessions and s.state == SessionState.IDLE


async def test_hanging_ntg_stop_is_bounded_and_unregisters(monkeypatch):
    import telegent.app as app_module
    monkeypatch.setattr(app_module, "NTG_STOP_TIMEOUT", 0.05)
    """F2: a hanging ntg.stop is bounded; the session is unregistered, the user stays busy until
    the native stop ends (a new call would hit ntgcalls.ConnectionError), then the next call is not busy."""
    app, *_ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    release = asyncio.Event()
    app.ntg.stop = lambda uid: release.wait()
    session = make(app.settings, outgoing=True, on_stop=app._make_stop(7))
    app.sessions[7] = session
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.01)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await asyncio.sleep(0.1)
    assert 7 not in app.sessions
    assert 7 in app._native_stops
    assert await app._call_outgoing('x') == 'busy'
    release.set()
    for _ in range(5):
        await asyncio.sleep(0)
    assert not app._native_stops
    assert await app._call_outgoing('x') != 'busy'
