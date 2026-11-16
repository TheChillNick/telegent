"""MOU-560 R1: the app stop's wait for a late RequestCall id is bounded and says when it gives up."""
import asyncio
import logging
from unittest.mock import AsyncMock

import telegent.app as app_module
from test_outgoing import setup_app
from test_outgoing_deadlines import discards
from test_outgoing_gaps import slow_requestcall


async def test_app_stop_gives_up_on_a_late_id_after_the_bound_and_says_so(monkeypatch, caplog):
    monkeypatch.setattr(app_module, 'LATE_DISCARD_TIMEOUT', 0.2)
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    app.app.stop = AsyncMock()
    slow_requestcall(app, seconds=1.0)
    app._outbox_task = asyncio.create_task(app._call_outgoing('SECRET text'))
    await asyncio.sleep(0.1)  # ring deadline passed, cleanup waits for the RequestCall id
    caplog.set_level(logging.INFO)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(app._shutdown(), 2)
    assert loop.time() - started < 0.6  # the bound, not the 1 s RPC
    assert not discards(invoked)
    app.app.stop.assert_awaited_once()
    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING] == [
        'Ring discard not sent for 1 call(s): no RequestCall id within 0 s of the stop; '
        'the phone may ring until Telegram ends the call.']
    assert 'SECRET' not in caplog.text
    leftover = list(app._late_discards)
    for task in leftover:  # what asyncio.run does at the end of the stop
        task.cancel()
    await asyncio.gather(*leftover, return_exceptions=True)


async def test_app_stop_with_an_id_in_time_logs_no_give_up_line(monkeypatch, caplog):
    app, live, invoked, _ = setup_app(monkeypatch, kai_call_ring_timeout=0.05)
    app.app.stop = AsyncMock()
    slow_requestcall(app, seconds=0.3)
    app._outbox_task = asyncio.create_task(app._call_outgoing('x'))
    await asyncio.sleep(0.1)
    caplog.set_level(logging.INFO)
    await asyncio.wait_for(app._shutdown(), 2)
    assert len(discards(invoked)) == 1
    assert 'Ring discard not sent' not in caplog.text
