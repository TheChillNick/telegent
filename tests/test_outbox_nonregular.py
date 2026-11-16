"""MOU-450 F3 (MOU-428): a non-regular *.json never jams the spool and is never opened."""
import asyncio
import json
import logging
import os
import threading
import time

import pytest

from telegent.outbox import Outbox
from test_outbox import NOW, put


def warnings_about_skips(caplog):
    return [r.getMessage() for r in caplog.records if 'non-regular entry' in r.getMessage()]


@pytest.mark.parametrize('where', ['root', 'processing'])
async def test_directory_named_json_is_skipped_and_later_request_rings(tmp_path, caplog, where):
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True, min_gap=0, max_per_hour=99)
    jam = (tmp_path if where == 'root' else tmp_path / 'processing') / '00-SECRETNAME.json'
    jam.mkdir()
    put(tmp_path, '10-ok')
    caplog.set_level(logging.INFO)
    for _ in range(3):
        await box.process(active=False, now=NOW)
    assert calls == ['Decision?']
    assert jam.is_dir()  # left in place, never moved
    assert not (tmp_path / 'failed' / jam.name).exists()
    assert warnings_about_skips(caplog) == ['Outgoing spool: skipped a non-regular entry (directory); left in place']
    assert 'SECRETNAME' not in caplog.text


async def test_fifo_in_root_never_blocks_the_pass(tmp_path, caplog):
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True, min_gap=0, max_per_hour=99)
    fifo = tmp_path / '00-fifo.json'
    os.mkfifo(fifo)
    put(tmp_path, '10-ok')
    caplog.set_level(logging.INFO)
    await asyncio.wait_for(box.process(active=False, now=NOW), 2)
    await asyncio.wait_for(box.process(active=False, now=NOW), 2)
    assert calls == ['Decision?']
    assert fifo.exists()
    assert warnings_about_skips(caplog) == ['Outgoing spool: skipped a non-regular entry (fifo); left in place']


async def test_symlink_request_is_still_invalid(tmp_path):
    """Unchanged behaviour: _read refuses symlinks (O_NOFOLLOW); they are not skipped."""
    target = tmp_path.parent / f'{tmp_path.name}-target.json'
    target.write_text(json.dumps({'text': 'x', 'priority': 'normal', 'created': NOW.timestamp()}))
    (tmp_path / '00-link.json').symlink_to(target)
    box = Outbox(tmp_path, lambda text: pytest.fail('called'), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/00-link.json').read_text()) == {'result': 'invalid'}
    assert not (tmp_path / '00-link.json').exists()


def test_read_refuses_a_fifo_without_blocking(tmp_path):
    """A FIFO swapped in after the lstat check: _read must fail at once (O_NONBLOCK + fstat).
    The helper thread opens the write end after 1 s so an old blocking open still returns."""
    fifo = tmp_path / 'race.json'
    os.mkfifo(fifo)

    def late_writer():
        time.sleep(1)
        try:
            os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
        except OSError:
            pass  # no reader left: the fix already returned

    threading.Thread(target=late_writer, daemon=True).start()
    start = time.monotonic()
    with pytest.raises(ValueError):
        Outbox._read(fifo)
    assert time.monotonic() - start < 0.5
