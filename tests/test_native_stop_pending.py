"""MOU-429 R1: a native stop that outlives NTG_STOP_TIMEOUT keeps the user busy until it ends."""
import asyncio
import gc
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace as NS

import ntgcalls

import telegent.app as app_module
from telegent.outbox import Outbox
from test_outgoing import setup_app

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)


def native_stop(app):
    """ntgcalls semantics: stop() returns a Future the caller cannot cancel natively; a second
    create_p2p_call for a chat id that still has a connection raises ntgcalls.ConnectionError."""
    loop = asyncio.get_running_loop()
    connections, pending = set(), []

    async def create(uid):
        if uid in connections:
            raise ntgcalls.ConnectionError('exists')
        connections.add(uid)

    def stop(uid):
        future = loop.create_future()
        pending.append((uid, future))
        return future

    def finish(error=None):
        uid, future = pending.pop(0)
        connections.discard(uid)
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    app.ntg.create_p2p_call.side_effect = create
    app.ntg.stop = stop
    return finish


def request(spool, name):
    (spool / f'{name}.json').write_text(json.dumps({'text': name, 'priority': 'urgent', 'created': NOW.timestamp()}))


async def settle():
    for _ in range(10):
        await asyncio.sleep(0)


async def test_queued_request_waits_for_a_slow_native_stop_then_rings(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch, kai_call_ring_timeout=0.05, kai_call_min_gap=0)
    finish = native_stop(app)
    box = Outbox(tmp_path, app._call_outgoing, allowed=True, min_gap=0, max_per_hour=99)
    request(tmp_path, '0-first')
    request(tmp_path, '1-second')
    caplog.set_level(logging.DEBUG)
    await box.process(active=app._spool_busy(), now=NOW)
    await asyncio.sleep(0.1)
    assert not app.sessions and 7 in app._native_stops
    assert 'still running after 0 s' in caplog.text
    await box.process(active=app._spool_busy(), now=NOW)
    assert (tmp_path / '1-second.json').exists()  # waits, not dialled into ConnectionError
    finish()
    await settle()
    assert not app._native_stops
    await box.process(active=app._spool_busy(), now=NOW)
    assert json.loads((tmp_path / 'failed/1-second.json').read_text())['result'] == 'no-answer'  # it rang
    assert 'ConnectionError' not in caplog.text


async def test_incoming_from_the_same_user_is_busy_while_native_stop_runs(monkeypatch):
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    finish = native_stop(app)
    await app._make_stop(7)()
    assert 7 in app._native_stops
    call = NS(id=55, access_hash=1, admin_id=7, g_a_hash=b'h', protocol=None)
    await app._handle_call_requested(call)
    assert 7 not in app.sessions and app.ntg.create_p2p_call.await_count == 0
    finish()
    await settle()
    assert not app._native_stops


async def test_late_native_stop_error_is_retrieved_without_text(monkeypatch, caplog):
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    finish = native_stop(app)
    loop = asyncio.get_running_loop()
    seen = []
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    caplog.set_level(logging.DEBUG)
    await app._make_stop(7)()
    finish(RuntimeError('SECRET native'))
    await settle()
    gc.collect()
    await settle()
    loop.set_exception_handler(None)
    assert not seen and 'SECRET' not in caplog.text and not app._native_stops
