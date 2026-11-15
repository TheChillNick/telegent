import logging
from types import SimpleNamespace as NS

import pytest
from pyrogram.raw import types

from test_outgoing import setup_app, start_dial


@pytest.mark.parametrize('reason,outcome', [(types.PhoneCallDiscardReasonBusy(), 'busy'),
                                            (types.PhoneCallDiscardReasonHangup(), 'declined')])
async def test_remote_outcome_before_answer_logs_no_setup_failure(monkeypatch, caplog, reason, outcome):
    app, *_ = setup_app(monkeypatch)
    caplog.set_level(logging.INFO)
    dial, _ = await start_dial(app)
    await app._handle_call_discarded(NS(id=10, reason=reason))
    assert await dial == outcome
    assert 'Outgoing setup failed' not in caplog.text
