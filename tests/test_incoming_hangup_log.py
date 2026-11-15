import logging

import telegent.telegram.call_session as cs
from helpers import FakeLiveSession, make_session


async def test_incoming_hangup_failure_keeps_the_old_line(settings, monkeypatch, caplog):
    monkeypatch.setattr(cs, 'HANGUP_MAX_WAIT', 0.0)
    session = make_session(settings=settings, live=FakeLiveSession())
    async def boom():
        raise RuntimeError('private detail')
    session.on_hangup = boom
    caplog.set_level(logging.INFO)
    await session._hangup_when_drained()
    record = next(r for r in caplog.records if r.getMessage().startswith('Gagal mengakhiri panggilan'))
    assert record.getMessage() == 'Gagal mengakhiri panggilan.' and record.exc_info is not None
