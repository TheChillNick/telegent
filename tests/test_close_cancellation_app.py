"""MOU-407 D3: close() cancelled during _close_tasks / during _stop_media leaves app.sessions empty, state IDLE."""
import asyncio
from types import SimpleNamespace as NS
import pytest
from telegent.telegram.call_session import SessionState
from test_outgoing import setup_app, start_dial


async def answered(monkeypatch, **kw):
    app, live, invoked, established = setup_app(monkeypatch, **kw)
    dial, session = await start_dial(app)
    await app._handle_call_accepted(NS(id=10, g_b=b'gb', protocol=established.protocol))
    assert await dial == 'answered'
    assert app.sessions.get(7) is session
    return app, session


async def wait_empty(app, limit=1.0):
    for _ in range(int(limit / 0.01)):
        if not app.sessions:
            return True
        await asyncio.sleep(0.01)
    return not app.sessions


async def test_cancel_during_close_tasks(monkeypatch):
    app, session = await answered(monkeypatch)
    dying = asyncio.Event()
    async def slow_to_die():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            dying.set()
            await asyncio.sleep(0.1)
            raise
    session._tasks.append(asyncio.create_task(slow_to_die()))
    await asyncio.sleep(0)
    closing = asyncio.create_task(session.close())
    await asyncio.wait_for(dying.wait(), 1)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert session.state == SessionState.IDLE
    assert await wait_empty(app), app.sessions
    app.ntg.stop.assert_awaited_with(7)


async def test_cancel_during_stop_media(monkeypatch):
    app, session = await answered(monkeypatch)
    in_stop = asyncio.Event()
    async def slow_stop(user_id):
        in_stop.set()
        await asyncio.sleep(0.1)
    app.ntg.stop.side_effect = slow_stop
    closing = asyncio.create_task(session.close())
    await asyncio.wait_for(in_stop.wait(), 1)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert session.state == SessionState.IDLE
    assert await wait_empty(app), app.sessions


async def test_new_call_not_busy_after_cancelled_close(monkeypatch):
    """Consequence check: after a cancelled close, the next outgoing call is not 'busy'."""
    app, session = await answered(monkeypatch)
    in_stop = asyncio.Event()
    async def slow_stop(user_id):
        in_stop.set()
        await asyncio.sleep(0.05)
    app.ntg.stop.side_effect = slow_stop
    closing = asyncio.create_task(session.close())
    await asyncio.wait_for(in_stop.wait(), 1)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert await wait_empty(app)
    app.ntg.stop.side_effect = None
    dial2, s2 = await start_dial(app)
    assert s2 is not session
    dial2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial2
