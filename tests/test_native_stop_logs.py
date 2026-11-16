"""MOU-450 round 2: log lines from MOU-469 F2-F4 / MOU-470 F1, F4, F6. Every planted text holds SECRET."""
import asyncio
import logging
from types import SimpleNamespace as NS

import telegent.app as app_module
from telegent import logging_setup
from test_native_stop_pending import native_stop, settle
from test_outgoing import setup_app

TRANSPORT = ('pyrogram.connection.transport.tcp.tcp', 'pyrogram.connection.connection')


async def test_pyrogram_transport_lines_keep_the_class_without_the_text(monkeypatch, caplog):
    """MOU-470 F1: drives the real Pyrogram 2.0.106 TCP.send, TCP.close and Connection.connect."""
    import pyrogram.connection.connection as connmod
    from pyrogram.connection import Connection
    from pyrogram.connection.transport.tcp import tcp as tcpmod

    for name in ('pyrogram.session.session', *TRANSPORT):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, 'filters', list(logger.filters))
    monkeypatch.setattr(logging_setup, '_configured', False)
    monkeypatch.setattr(logging.getLogger('websockets'), 'level', logging.NOTSET)
    logging_setup.configure_logging('INFO')
    caplog.set_level(logging.DEBUG)

    class Writer:
        def write(self, data):
            pass

        async def drain(self):
            raise ConnectionResetError(104, 'Connection reset by peer SECRET')

        def close(self):
            pass

        async def wait_closed(self):
            raise OSError('close SECRET')

    tcp = object.__new__(tcpmod.TCP)
    tcp.lock, tcp.writer = asyncio.Lock(), Writer()
    try:
        await tcp.send(b'x')
    except OSError:
        pass
    await tcp.close()

    class Proto:
        def __init__(self, *args):
            pass

        async def connect(self, address):
            raise OSError(101, 'Network is unreachable SECRET')

        async def close(self):
            pass

    connection = object.__new__(Connection)
    connection.ipv6, connection.proxy, connection.address = False, None, ('1.2.3.4', 443)
    connection.test_mode, connection.dc_id, connection.media = False, 4, False
    monkeypatch.setattr(connmod, 'TCPAbridged', Proto)
    monkeypatch.setattr(Connection, 'MAX_CONNECTION_ATTEMPTS', 1)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(connmod, 'asyncio', NS(sleep=lambda seconds: real_sleep(0)))
    try:
        await connection.connect()
    except ConnectionError:
        pass
    lines = [r.getMessage() for r in caplog.records if r.name in TRANSPORT and 'exception' in r.getMessage() or
             r.name in TRANSPORT and 'Unable to connect' in r.getMessage()]
    assert lines == ['Send exception: ConnectionResetError', 'Close exception: OSError',
                     'Unable to connect due to network issues: OSError'], lines
    assert 'SECRET' not in caplog.text


async def test_stop_cancelled_before_the_deadline_says_so(monkeypatch, caplog):
    """MOU-469 F2: a cancel at 0 s must not claim 'still running after 5 s'."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 5)
    app, *_ = setup_app(monkeypatch)
    finish = native_stop(app)
    caplog.set_level(logging.INFO)
    stop = asyncio.ensure_future(app._make_stop(7)())
    await settle()
    stop.cancel()
    await asyncio.gather(stop, return_exceptions=True)
    lines = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert lines == ['Native media stop for user=7 still running after its wait was cancelled; user stays busy.'], lines
    finish()
    await settle()
    assert not app._native_stops


async def test_incoming_during_native_stop_logs_its_own_reason(monkeypatch, caplog):
    """MOU-469 F3 / MOU-470 F4: no 'active session' claim when only a native stop runs."""
    monkeypatch.setattr(app_module, 'NTG_STOP_TIMEOUT', 0.05)
    app, *_ = setup_app(monkeypatch)
    finish = native_stop(app)
    await app._make_stop(7)()
    caplog.set_level(logging.INFO)
    await app._handle_call_requested(NS(id=55, access_hash=1, admin_id=7, g_a_hash=b'h', protocol=None))
    assert "Rejected: the previous call's native media stop is still running for this user." in caplog.text
    assert 'sudah ada sesi aktif' not in caplog.text
    finish()
    await settle()


async def test_app_stop_logs_pending_native_stops(monkeypatch, caplog):
    """MOU-469 F4 / MOU-470 F6: one INFO line with the count; nothing when none is pending."""
    app, *_ = setup_app(monkeypatch)
    caplog.set_level(logging.INFO)
    await app._shutdown()
    assert 'Native media stop still running' not in caplog.text
    app, *_ = setup_app(monkeypatch)
    app._native_stops[7] = asyncio.get_running_loop().create_future()
    caplog.clear()
    await app._shutdown()
    assert [r.getMessage() for r in caplog.records if 'Native media stop' in r.getMessage()] == [
        'Native media stop still running for 1 user(s); not awaited.']
    app._native_stops[7].cancel()
