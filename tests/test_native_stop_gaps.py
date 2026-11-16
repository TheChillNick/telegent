"""MOU-469 F5: pins for the PR #5 lines that the suite left open (mutants n05, n08, n13, n15)."""
import asyncio
import gc
import logging

import telegent.app as app_module
from test_native_stop_pending import native_stop, settle
from test_outgoing import setup_app


async def test_watcher_keeps_requests_waiting_while_only_a_native_stop_runs(monkeypatch, tmp_path):  # n08
    app, *_ = setup_app(monkeypatch, kai_outbox_dir=str(tmp_path))
    actives, seen = [], asyncio.Event()

    class Box:
        async def process(self, *, active):
            actives.append(active)
            seen.set()

    monkeypatch.setattr(app_module, 'Outbox', lambda *a, **k: Box())
    app._native_stops[7] = asyncio.get_running_loop().create_future()  # no session, native stop pending
    task = asyncio.create_task(app._watch_outbox())
    try:
        await asyncio.wait_for(seen.wait(), 2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not app.sessions and actives[0] is True


async def test_stop_cancelled_before_the_deadline_keeps_the_native_stop_tracked(monkeypatch, caplog):  # n13
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 5)
    app, *_ = setup_app(monkeypatch)
    finish = native_stop(app)
    loop = asyncio.get_running_loop()
    seen = []
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    caplog.set_level(logging.DEBUG)
    stop = asyncio.ensure_future(app._make_stop(7)())
    await settle()
    stop.cancel()
    await asyncio.gather(stop, return_exceptions=True)
    assert 7 in app._native_stops and await app._call_outgoing('Decision?') == 'busy'
    finish(RuntimeError('SECRET native'))
    await settle()
    gc.collect()
    await settle()
    loop.set_exception_handler(None)
    assert not app._native_stops and not seen and 'SECRET' not in caplog.text


async def test_native_stop_cancelled_by_the_library_clears_busy_quietly(monkeypatch):  # n05
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    loop = asyncio.get_running_loop()
    native = loop.create_future()
    app.ntg.stop = lambda uid: native
    seen = []
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    await app._make_stop(7)()
    assert 7 in app._native_stops
    native.cancel()
    await settle()
    loop.set_exception_handler(None)
    assert not app._native_stops and not seen


async def test_stop_that_ends_inside_the_deadline_is_not_tracked(monkeypatch, caplog):  # n15
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 5)
    app, *_ = setup_app(monkeypatch)
    loop = asyncio.get_running_loop()

    def stop(uid):
        future = loop.create_future()
        loop.call_later(0.01, future.set_result, None)
        return future

    app.ntg.stop = stop
    caplog.set_level(logging.DEBUG)
    await app._make_stop(7)()
    assert not app._native_stops and 'still running' not in caplog.text


def test_retry_filter_passes_other_records_unchanged():  # l05, l06
    from telegent.logging_setup import _RETRY_REASON_FILTER, _RETRY_WITH_REASON

    def record(msg, args):
        return logging.LogRecord('pyrogram.session.session', logging.WARNING, __file__, 1, msg, args, None)

    error = record(RuntimeError('x'), None)  # Pyrogram's own log.exception(e), session.py:178
    assert _RETRY_REASON_FILTER.filter(error) and error.msg.args == ('x',)
    mapping = record(_RETRY_WITH_REASON + '%(why)s', ({'why': 'x'},))  # a single mapping argument
    assert _RETRY_REASON_FILTER.filter(mapping) and mapping.args == {'why': 'x'}
