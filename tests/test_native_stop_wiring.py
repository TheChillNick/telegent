"""MOU-470 F5/F8: pin the production wiring of the native-stop wait (proposed test, not in PR #5)."""
import asyncio
from types import SimpleNamespace as NS

import telegent.app as app_module
from pyrogram.raw import functions, types
from test_outgoing import setup_app
from test_native_stop_pending import native_stop, settle


async def test_watch_outbox_passes_native_stop_as_active(monkeypatch, tmp_path):
    """The real watcher, not a hand-made flag: a pending native stop alone makes the spool wait."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch, kai_outbox_dir=str(tmp_path))
    finish = native_stop(app)
    await app._make_stop(7)()
    assert not app.sessions and 7 in app._native_stops
    actives, seen = [], asyncio.Event()

    class Box:
        async def process(self, *, active):
            actives.append(active)
            seen.set()
    monkeypatch.setattr(app_module, 'Outbox', lambda *a, **k: Box())
    task = asyncio.create_task(app._watch_outbox())
    try:
        await asyncio.wait_for(seen.wait(), 2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    finish()
    await settle()
    assert actives == [True]


async def test_incoming_during_native_stop_is_discarded_busy(monkeypatch):
    """The caller gets an explicit busy (DiscardCall Busy), not a call left ringing."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, _, invoked, _ = setup_app(monkeypatch)
    finish = native_stop(app)
    await app._make_stop(7)()
    await app._handle_call_requested(NS(id=55, access_hash=1, admin_id=7, g_a_hash=b'h', protocol=None))
    discards = [q for q in invoked if isinstance(q, functions.phone.DiscardCall)]
    assert len(discards) == 1 and isinstance(discards[0].reason, types.PhoneCallDiscardReasonBusy)
    assert discards[0].peer.id == 55
    assert not any(isinstance(q, functions.phone.ReceivedCall) for q in invoked)
    finish()
    await settle()


async def test_other_users_native_stop_holds_the_spool_but_not_their_calls(monkeypatch):
    """One NTgCalls instance serves every chat: a pending stop of user 8 keeps Kai's spool waiting,
    but an incoming call from user 7 (no pending stop) is still accepted."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    app.settings.allowed_user_ids = '7,8'
    finish = native_stop(app)
    await app._make_stop(8)()
    assert not app.sessions and 8 in app._native_stops
    assert app._spool_busy() is True
    await app._handle_call_requested(NS(id=56, access_hash=1, admin_id=7, g_a_hash=b'h', protocol=None))
    assert 7 in app.sessions
    app.ntg.create_p2p_call.assert_awaited_once_with(7)
    finish()
    await settle()


async def test_pyrogram_last_retry_at_warning_has_no_reason(monkeypatch, caplog):
    """MOU-470 F8: Pyrogram logs the last retry at WARNING (retries < 2); that branch must be clean too."""
    import logging
    from pyrogram import raw
    from pyrogram.session import Session
    from telegent import logging_setup

    pyro = logging.getLogger('pyrogram.session.session')
    monkeypatch.setattr(logging_setup, '_configured', False)
    monkeypatch.setattr(logging.getLogger('websockets'), 'level', logging.NOTSET)
    monkeypatch.setattr(pyro, 'filters', list(pyro.filters))
    logging_setup.configure_logging('INFO')
    caplog.set_level(logging.DEBUG)
    calls = []

    async def send(query, timeout=None):
        calls.append(query)
        if len(calls) == 1:
            raise OSError(104, 'Connection reset by peer SECRET')
        return 'ok'
    session = object.__new__(Session)
    session.is_started = asyncio.Event()
    session.is_started.set()
    session.client = NS(name='kai')
    session.send = send
    query = raw.functions.phone.DiscardCall(peer=raw.types.InputPhoneCall(id=1, access_hash=2), duration=0,
                                            reason=raw.types.PhoneCallDiscardReasonMissed(), connection_id=0)
    assert await Session.invoke(session, query, retries=1) == 'ok'
    records = [r for r in caplog.records if r.name == 'pyrogram.session.session']
    assert [(r.levelname, r.getMessage()) for r in records] == [('WARNING', '[10] Retrying "phone.DiscardCall"')]
    assert 'SECRET' not in caplog.text
