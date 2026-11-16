"""MOU-559 (Verify of PR #6): pin PR #6 behaviour that Themis's own mutants showed is unchecked.

Every planted text holds SECRET; no log line may contain it.
"""
import asyncio
import logging
import os
import socket
import stat
from pathlib import Path

import pytest
from pyrogram.raw import functions, types

import telegent.app as app_module
from telegent.logging_setup import _TRANSPORT_TEXT_FILTER
from telegent.outbox import Outbox
from test_outbox import NOW, put
from test_outgoing import setup_app
from test_outgoing_deadlines import discards, patch_invoke


def skip_lines(caplog):
    return [r.getMessage() for r in caplog.records if 'non-regular entry' in r.getMessage()]


def make_box(tmp_path, calls):
    return Outbox(tmp_path, lambda text: calls.append(text), allowed=True, min_gap=0, max_per_hour=99)


# ---------------------------------------------------------------- Outbox._skip (o01 o02 o04-o07)
async def test_entry_vanishing_before_lstat_is_skipped_quietly(tmp_path, caplog, monkeypatch):
    """o01: an entry gone between glob() and lstat() is skipped: no failed/ file, no error, no line."""
    calls = []
    box = make_box(tmp_path, calls)
    put(tmp_path, '00-SECRET-gone')
    put(tmp_path, '10-ok')
    real_lstat = Path.lstat

    def vanishing(self, *args, **kwargs):
        if self.name == '00-SECRET-gone.json' and self.exists():
            self.unlink()  # really removed at the race point
        return real_lstat(self, *args, **kwargs)

    monkeypatch.setattr(Path, 'lstat', vanishing)
    caplog.set_level(logging.INFO)
    await box.process(active=False, now=NOW)
    assert calls == ['Decision?']
    assert os.listdir(tmp_path / 'failed') == []
    assert skip_lines(caplog) == [] and 'SECRET' not in caplog.text


async def test_other_lstat_error_still_ends_the_pass(tmp_path, monkeypatch):
    """o02: only FileNotFoundError means 'vanished'; another OSError ends the pass as before PR #6."""
    calls = []
    box = make_box(tmp_path, calls)
    put(tmp_path, '00-SECRET-denied')
    real_lstat = Path.lstat

    def denied(self, *args, **kwargs):
        if self.name == '00-SECRET-denied.json':
            raise PermissionError(13, 'Permission denied')
        return real_lstat(self, *args, **kwargs)

    monkeypatch.setattr(Path, 'lstat', denied)
    with pytest.raises(PermissionError):
        await box.process(active=False, now=NOW)
    assert calls == [] and os.listdir(tmp_path / 'failed') == []


async def test_socket_and_fifo_get_their_own_kind(tmp_path, caplog, monkeypatch):
    """o04/o05: a socket is logged as 'socket' (not merged into 'fifo'), left in place, never counted."""
    calls = []
    box = make_box(tmp_path, calls)
    sockets = []
    for where in (tmp_path, tmp_path / 'processing'):
        monkeypatch.chdir(where)  # a relative bind keeps the AF_UNIX path under 108 bytes
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind('00-SECRET-sock.json')
        sockets.append(s)
    monkeypatch.undo()
    os.mkfifo(tmp_path / '01-SECRET-fifo.json')
    put(tmp_path, '10-ok')
    caplog.set_level(logging.INFO)
    try:
        for _ in range(2):
            await box.process(active=False, now=NOW)
    finally:
        for s in sockets:
            s.close()
    assert calls == ['Decision?']
    assert stat.S_ISSOCK((tmp_path / '00-SECRET-sock.json').lstat().st_mode)
    assert stat.S_ISSOCK((tmp_path / 'processing' / '00-SECRET-sock.json').lstat().st_mode)
    assert os.listdir(tmp_path / 'failed') == []
    assert sorted(skip_lines(caplog)) == [
        'Outgoing spool: skipped a non-regular entry (fifo); left in place',
        'Outgoing spool: skipped a non-regular entry (socket); left in place']
    assert 'SECRET' not in caplog.text


@pytest.mark.parametrize('mode,kind', [(stat.S_IFCHR, 'device'), (stat.S_IFBLK, 'device'), (0, 'other')],
                         ids=['char', 'block', 'no-type-bits'])
def test_device_and_unknown_kinds(tmp_path, caplog, monkeypatch, mode, kind):
    """o06/o07: device nodes cannot be created without root, so the st_mode is faked."""
    box = make_box(tmp_path, [])
    path = tmp_path / '00-SECRET-dev.json'
    path.write_text('{}')
    real_lstat = Path.lstat

    class St:
        st_mode = mode | 0o600

    monkeypatch.setattr(Path, 'lstat', lambda self, *a, **k: St() if self.name == path.name else real_lstat(self, *a, **k))
    caplog.set_level(logging.INFO)
    assert box._skip(path) is True
    assert skip_lines(caplog) == [f'Outgoing spool: skipped a non-regular entry ({kind}); left in place']


# ---------------------------------------------------------------- late discard (d01 d02 w01-w03)
async def second_cancel_with_pending_requestcall(app, invoked, release):
    async def held(query, original, **kw):
        await release.wait()
        return await original(query, **kw)

    patch_invoke(app, functions.phone.RequestCall, held)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)  # ring deadline (0.05 s) passed; cleanup awaits the RequestCall id


async def test_late_discard_wait_is_bounded_when_the_id_never_comes(monkeypatch, caplog):
    """w01/w03: the app stop gives up after LATE_DISCARD_TIMEOUT, not after the RPC or the native-stop timeout."""
    monkeypatch.setattr(app_module, 'LATE_DISCARD_TIMEOUT', 0.2)
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    app.app.stop = lambda: asyncio.sleep(0)
    release = asyncio.Event()
    await second_cancel_with_pending_requestcall(app, invoked, release)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(app._shutdown(), 2)
    assert loop.time() - started < 1
    assert not discards(invoked)
    leftover = set(app._late_discards)
    assert len(leftover) == 1
    caplog.set_level(logging.INFO)
    for task in leftover:  # what asyncio.run does to leftover tasks
        task.cancel()
    await asyncio.gather(*leftover, return_exceptions=True)
    assert all(task.cancelled() for task in leftover)  # d01: a cancel is not reported as an RPC failure
    assert 'RequestCall response unavailable' not in caplog.text
    release.set()


async def test_late_discard_is_sent_before_the_client_stops(monkeypatch):
    """w02: the late-discard wait must come before app.stop(), else DiscardCall meets a stopped client."""
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    order = []
    original = app.app.invoke.side_effect

    async def side(query, **kw):
        if isinstance(query, functions.phone.DiscardCall):
            order.append('discard')
        return await original(query, **kw)

    app.app.invoke.side_effect = side

    async def client_stop():
        order.append('client-stop')

    app.app.stop = client_stop
    release = asyncio.Event()
    await second_cancel_with_pending_requestcall(app, invoked, release)
    asyncio.get_running_loop().call_later(0.1, release.set)
    await asyncio.wait_for(app._shutdown(), 2)
    assert order == ['discard', 'client-stop'], order
    assert isinstance(discards(invoked)[0].reason, types.PhoneCallDiscardReasonMissed)


async def test_late_requestcall_error_leaves_no_failed_task(monkeypatch, caplog):
    """d02: after the class-only line the discard task ends cleanly (no unretrieved exception)."""
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)

    async def failing(query, original, **kw):
        await asyncio.sleep(0.2)
        raise RuntimeError('SECRET late')

    patch_invoke(app, functions.phone.RequestCall, failing)
    dial = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    tasks = set()
    real = app._discard_when_known

    def spy(request, user_id):
        real(request, user_id)
        tasks.update(app._late_discards)

    app._discard_when_known = spy
    caplog.set_level(logging.INFO)
    dial.cancel()
    await asyncio.gather(dial, return_exceptions=True)
    await asyncio.sleep(0.3)
    assert len(tasks) == 1
    task = tasks.pop()
    assert task.done() and not task.cancelled() and task.exception() is None
    assert 'RequestCall response unavailable; remote state unknown: RuntimeError' in caplog.text
    assert 'SECRET' not in caplog.text


# ---------------------------------------------------------------- app stop INFO line (s11 s12)
async def test_app_stop_counts_every_pending_native_stop_at_info(monkeypatch, caplog):
    app, *_ = setup_app(monkeypatch)
    app.app.stop = lambda: asyncio.sleep(0)
    futures = {uid: asyncio.get_running_loop().create_future() for uid in (7, 8)}
    app._native_stops.update(futures)
    caplog.set_level(logging.INFO)
    await app._shutdown()
    lines = [(r.levelno, r.getMessage()) for r in caplog.records if 'Native media stop' in r.getMessage()]
    assert lines == [(logging.INFO, 'Native media stop still running for 2 user(s); not awaited.')]
    for future in futures.values():
        future.cancel()


# ---------------------------------------------------------------- _make_stop (f8f f8g)
async def test_native_stop_raising_synchronously_logs_the_class(monkeypatch, caplog):
    """f8f: ntg.stop() can raise before it returns a Future; `started` is still None then."""
    app, *_ = setup_app(monkeypatch)

    def raising(uid):
        raise RuntimeError('SECRET sync')

    app.ntg.stop = raising
    app.sessions[7] = object()
    caplog.set_level(logging.INFO)
    await app._make_stop(7)()
    assert 7 not in app.sessions
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        'Native media stop failed: RuntimeError']
    assert 'SECRET' not in caplog.text


async def test_native_stop_deadline_says_after_n_seconds(monkeypatch, caplog):
    """f8g: on 3.10 asyncio.TimeoutError is not the builtin TimeoutError; the deadline text must not
    turn into the 'wait was cancelled' text."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    release = asyncio.Event()
    app.ntg.stop = lambda uid: release.wait()
    app.sessions[7] = object()
    caplog.set_level(logging.INFO)
    await app._make_stop(7)()
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        'Native media stop for user=7 still running after 0 s; user stays busy.']
    release.set()
    await asyncio.sleep(0)


# ---------------------------------------------------------------- _TransportTextFilter (t05 t06)
@pytest.mark.parametrize('msg', ['Send exception: %s %s', 'Close exception: %s %s',
                                 'Unable to connect due to network issues: %s'])
@pytest.mark.parametrize('args', [(), {'k': 'v'}], ids=['empty', 'mapping'])
def test_transport_filter_never_raises_on_odd_records(msg, args):
    """t05/t06: the filter only rewrites tuple args; other records pass unchanged and never raise."""
    record = logging.LogRecord('pyrogram.connection.connection', logging.INFO, __file__, 1, msg, None, None)
    record.args = args
    assert _TRANSPORT_TEXT_FILTER.filter(record) is True
    assert record.msg == msg and record.args == args
