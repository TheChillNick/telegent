import asyncio
import logging

import pytest

from helpers import FakeLiveSession, make_session
from test_outgoing import setup_app


async def test_late_stop_error_after_cancel_is_logged_as_class_only(settings, caplog):
    seen = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _l, ctx: seen.append(ctx.get('message')))
    session = make_session(settings=settings, live=FakeLiveSession())
    session.outgoing = True  # outgoing log policy: class only

    async def bad_stop():
        await asyncio.sleep(0.02)
        raise RuntimeError('SECRET late stop')

    session._on_stop = bad_stop
    caplog.set_level(logging.INFO)
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.005)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await asyncio.sleep(0.05)
    loop.set_exception_handler(None)
    assert not seen, seen
    assert 'Gagal menghentikan media: RuntimeError' in caplog.text
    assert 'SECRET' not in caplog.text


async def test_cancelled_stop_still_unregisters(monkeypatch):
    app, *_ = setup_app(monkeypatch)
    app.ntg.stop = lambda uid: asyncio.Event().wait()
    app.sessions[7] = object()
    stop = asyncio.create_task(app._make_stop(7)())
    await asyncio.sleep(0.01)
    stop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop
    assert 7 not in app.sessions
