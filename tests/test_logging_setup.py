import asyncio
import logging
from types import SimpleNamespace as NS

from telegent import logging_setup


def test_debug_level_never_enables_websockets_frames(monkeypatch):
    ws, root = logging.getLogger('websockets'), logging.getLogger()
    monkeypatch.setattr(logging_setup, '_configured', False)
    monkeypatch.setattr(ws, 'level', logging.NOTSET)
    monkeypatch.setattr(root, 'level', logging.DEBUG)  # what LOG_LEVEL=DEBUG gives in production
    logging_setup.configure_logging('DEBUG')
    assert not ws.isEnabledFor(logging.DEBUG)
    assert ws.isEnabledFor(logging.INFO)


async def test_pyrogram_retry_line_keeps_the_rpc_name_without_the_reason(monkeypatch, caplog):
    """MOU-429 R3: Pyrogram's own retry line on an outgoing-call RPC must not log str(exception)."""
    from pyrogram import raw
    from pyrogram.errors import RPCError
    from pyrogram.session import Session

    pyro = logging.getLogger('pyrogram.session.session')
    monkeypatch.setattr(logging_setup, '_configured', False)
    monkeypatch.setattr(logging.getLogger('websockets'), 'level', logging.NOTSET)
    monkeypatch.setattr(pyro, 'filters', list(pyro.filters))
    logging_setup.configure_logging('INFO')
    caplog.set_level(logging.DEBUG)

    attempts = []

    async def send(query, timeout=None):
        attempts.append(query)
        if len(attempts) == 1:
            RPCError.raise_it(raw.types.RpcError(error_code=500, error_message='CALL_OCCUPY_FAILED_SERVER_SIDE'),
                              type(query))
        if len(attempts) == 2:
            raise OSError(104, 'Connection reset by peer')
        return 'ok'

    session = object.__new__(Session)
    session.is_started = asyncio.Event()
    session.is_started.set()
    session.client = NS(name='kai')
    session.send = send
    query = raw.functions.phone.ConfirmCall(
        peer=raw.types.InputPhoneCall(id=1, access_hash=2), g_a=b'x', key_fingerprint=1,
        protocol=raw.types.PhoneCallProtocol(min_layer=92, max_layer=92, library_versions=['13.0.0'],
                                             udp_p2p=True, udp_reflector=True))
    assert await Session.invoke(session, query) == 'ok'
    lines = [r.getMessage() for r in caplog.records if r.name == 'pyrogram.session.session']
    assert lines == ['[1] Retrying "phone.ConfirmCall"', '[2] Retrying "phone.ConfirmCall"'], lines
    assert 'CALL_OCCUPY' not in caplog.text and 'Errno' not in caplog.text and 'reset' not in caplog.text


def test_retry_filter_leaves_other_pyrogram_lines_alone():
    record = logging.LogRecord('pyrogram.session.session', logging.WARNING, __file__, 1,
                               '[%s] Waiting for %s seconds before continuing (required by "%s")',
                               ('kai', 3, 'phone.ConfirmCall'), None)
    assert logging_setup._RETRY_REASON_FILTER.filter(record)
    assert record.getMessage() == '[kai] Waiting for 3 seconds before continuing (required by "phone.ConfirmCall")'
