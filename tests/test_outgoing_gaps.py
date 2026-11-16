"""MOU-450: pin behaviour the MOU-428 mutants showed the suite does not check (F5, F6)."""
import asyncio
import gc
import logging

import pytest
from pyrogram.errors import UnknownError

import telegent.app as app_module
from helpers import FakeLiveSession, make_session


def capture_loop_errors():
    seen = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _l, ctx: seen.append(ctx.get('message')))
    return loop, seen


async def close_and_cancel(session):
    closing = asyncio.create_task(session.close())
    await asyncio.sleep(0.01)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing


# ---------------------------------------------------------------- F5 (c04, c06)
async def test_stop_task_cancelled_at_loop_end_logs_no_callback_error(settings):
    """c04: asyncio.run cancels a still-running shielded stop when the loop ends; the late-error
    callback must not call exception() on a cancelled task."""
    loop, seen = capture_loop_errors()
    session = make_session(settings=settings, live=FakeLiveSession())
    session.outgoing = True

    async def slow_stop():
        await asyncio.sleep(10)

    session._on_stop = slow_stop
    await close_and_cancel(session)
    stop = session._stop_task
    assert stop is not None and not stop.done()
    stop.cancel()  # what asyncio.run does to leftover tasks
    await asyncio.gather(stop, return_exceptions=True)
    await asyncio.sleep(0)
    loop.set_exception_handler(None)
    assert not seen, seen


async def test_unreferenced_stop_task_is_kept_alive_by_the_session(settings):
    """c06: the shielded stop awaits a future nobody else holds; without the session's strong
    reference the GC destroys the pending task ("Task was destroyed but it is pending!")."""
    loop, seen = capture_loop_errors()
    session = make_session(settings=settings, live=FakeLiveSession())
    session.outgoing = True

    async def orphan_stop():
        await asyncio.get_running_loop().create_future()

    session._on_stop = orphan_stop
    await close_and_cancel(session)
    for _ in range(3):
        gc.collect()
        await asyncio.sleep(0.01)
    stop = session._stop_task
    assert stop is not None and not stop.done()
    stop.cancel()
    await asyncio.gather(stop, return_exceptions=True)
    loop.set_exception_handler(None)
    assert not seen, seen


# ---------------------------------------------------------------- F6 (s08, l05)
def test_native_stop_timeout_is_ten_seconds():
    """s08: the other tests patch NTG_STOP_TIMEOUT to 0.05; docs/kai.md promises 10 s."""
    assert app_module.NTG_STOP_TIMEOUT == 10


@pytest.mark.parametrize('length,keeps_id', [(64, True), (65, False)])
def test_error_label_rpc_value_is_capped_at_64_chars(length, keeps_id):
    """l05: an upper-case RPC value longer than 64 chars is not a Telegram id: class only."""
    value = f'[400 {"A" * length}]'
    label = app_module.error_label(UnknownError(value=value))
    assert label == (f'UnknownError {value}' if keeps_id else 'UnknownError')


# ---------------------------------------------------------------- F8 (s03)
from pyrogram.raw import functions, types  # noqa: E402

from test_outgoing import setup_app  # noqa: E402
from test_outgoing_deadlines import discards, patch_invoke  # noqa: E402


@pytest.mark.parametrize('error', [ValueError('SECRET native'), OSError('SECRET native'), RuntimeError('SECRET native')],
                         ids=['ValueError', 'OSError', 'RuntimeError'])
async def test_failing_native_stop_logs_the_class_and_unregisters(monkeypatch, caplog, error):
    """s03: any Exception from ntg.stop is caught (session unregistered, no media-stop error from
    CallSession) and logged as class only at WARNING."""
    app, *_ = setup_app(monkeypatch)

    async def failing_stop(uid):
        raise error

    app.ntg.stop = failing_stop
    app.sessions[7] = object()
    caplog.set_level(logging.INFO)
    await app._make_stop(7)()
    assert 7 not in app.sessions
    lines = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert lines == [f'Native media stop failed: {type(error).__name__}'], lines
    assert 'SECRET' not in caplog.text


async def test_hanging_native_stop_logs_only_the_still_running_line(monkeypatch, caplog):
    """F8 must not add a second line when the stop only timed out (R1 already logs that)."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    release = asyncio.Event()
    app.ntg.stop = lambda uid: release.wait()
    app.sessions[7] = object()
    caplog.set_level(logging.INFO)
    await app._make_stop(7)()
    release.set()
    await asyncio.sleep(0)
    assert 'Native media stop failed' not in caplog.text
    assert 'still running after' in caplog.text


# ---------------------------------------------------------------- R5
def slow_requestcall(app, seconds=0.3):
    async def slow(query, original, **kw):
        await asyncio.sleep(seconds)
        return await original(query, **kw)

    patch_invoke(app, functions.phone.RequestCall, slow)


async def test_second_cancel_during_ring_cleanup_still_discards_the_call(monkeypatch):
    """R5 (MOU-429 P3c): ring deadline passes while RequestCall is in flight, then the worker is
    cancelled again (app stop). The id arrives later and the call is discarded once (Missed)."""
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    slow_requestcall(app)
    dial = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)  # deadline expired at 0.05; cleanup awaits the RequestCall response
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    await asyncio.sleep(0.4)  # the RequestCall finishes now
    d = discards(invoked)
    assert len(d) == 1 and isinstance(d[0].reason, types.PhoneCallDiscardReasonMissed)
    assert not app._late_discards and not app.sessions


async def test_app_stop_waits_for_the_late_discard(monkeypatch):
    """R5: the app stop waits (at most LATE_DISCARD_TIMEOUT) so the discard is sent before the
    Telegram client stops."""
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    slow_requestcall(app)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    await asyncio.wait_for(app._shutdown(), 2)
    assert len(discards(invoked)) == 1
    assert app_module.LATE_DISCARD_TIMEOUT == 5


async def test_late_requestcall_error_after_second_cancel_logs_class_only(monkeypatch, caplog):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)

    async def failing(query, original, **kw):
        await asyncio.sleep(0.3)
        raise RuntimeError('SECRET late')

    patch_invoke(app, functions.phone.RequestCall, failing)
    caplog.set_level(logging.INFO)
    dial = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    await asyncio.sleep(0.4)
    assert not discards(invoked) and not app._late_discards
    assert 'RequestCall response unavailable; remote state unknown: RuntimeError' in caplog.text
    assert 'SECRET' not in caplog.text
