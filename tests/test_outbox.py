import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from telegent.outbox import Outbox

NOW = datetime(2026, 11, 14, 12, tzinfo=ZoneInfo('Europe/Prague'))


def put(path, name='one', **overrides):
    data = {'text': 'Decision?', 'priority': 'normal', 'created': NOW.timestamp()}
    data.update(overrides)
    (path / (name + '.json')).write_text(json.dumps(data))


async def test_request_waits_during_active_call_and_then_calls(tmp_path):
    put(tmp_path)
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=True, now=NOW)
    assert (tmp_path / 'one.json').exists()
    await box.process(active=False, now=NOW)
    assert calls == ['Decision?']
    assert json.loads((tmp_path / 'done/one.json').read_text())['result'] == 'answered'


@pytest.mark.parametrize('outcome', ['declined', 'busy', 'no-answer', 'failed'])
async def test_failed_outcomes(tmp_path, outcome):
    put(tmp_path, priority='urgent')
    box = Outbox(tmp_path, lambda text: outcome, allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == outcome
    await box.process(active=False, now=NOW + timedelta(minutes=2))  # no redial


async def test_stale_and_not_whitelisted(tmp_path):
    put(tmp_path, created=NOW.timestamp()-901)
    box = Outbox(tmp_path, lambda text: pytest.fail('called'), allowed=False)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == 'stale'
    put(tmp_path)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == 'not-whitelisted'


async def test_quiet_hours_urgent_and_minimum_gap(tmp_path):
    night = NOW.replace(hour=23)
    put(tmp_path, created=night.timestamp())
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=night)
    assert not calls
    put(tmp_path, name='urgent', priority='urgent', created=night.timestamp())
    await box.process(active=False, now=night)
    assert calls == ['Decision?']
    put(tmp_path, name='urgent2', priority='urgent', created=night.timestamp())
    await box.process(active=False, now=night + timedelta(seconds=59))
    assert len(calls) == 1
    await box.process(active=False, now=night + timedelta(seconds=60))
    assert len(calls) == 2
    assert (tmp_path / 'one.json').exists()


async def test_hourly_limit_survives_restart(tmp_path):
    calls = []
    for minute in range(5):
        put(tmp_path, name=str(minute), created=NOW.timestamp()+minute*60)
        box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
        await box.process(active=False, now=NOW + timedelta(minutes=minute))
    assert len(calls) == 4
    assert (tmp_path / '4.json').exists()
    # Keep fifth request fresh for checking the rolling hour boundary.
    put(tmp_path, name='4', created=NOW.timestamp()+3600)
    await box.process(active=False, now=NOW + timedelta(hours=1))
    assert len(calls) == 5


async def test_callback_error_is_terminal_and_text_is_capped(tmp_path, caplog):
    put(tmp_path, text='x'*1200)
    async def fail(text):
        assert len(text) == 1000
        raise RuntimeError('sensitive request content')
    box = Outbox(tmp_path, fail, allowed=True)
    await box.process(active=False, now=NOW)
    data = json.loads((tmp_path / 'failed/one.json').read_text())
    assert data['result'] == 'failed'
    assert data['text'] == 'x'*1000
    assert 'sensitive request content' not in caplog.text


async def test_interrupted_request_is_not_redialed(tmp_path):
    running = tmp_path / 'processing'
    running.mkdir()
    put(running)
    box = Outbox(tmp_path, lambda text: pytest.fail('redial'), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == 'failed'


@pytest.mark.parametrize('overrides', [{'priority': 'invalid'}, {'created': 'oops'}, {'text': ''}, {'created': float('nan')}])
async def test_invalid_request_never_calls(tmp_path, overrides):
    put(tmp_path, **overrides)
    box = Outbox(tmp_path, lambda text: pytest.fail('called'), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == 'invalid'


async def test_overflowing_timestamp_does_not_block_later_requests(tmp_path):
    put(tmp_path, name='0-bad', created=10**400)
    put(tmp_path, name='1-good')
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/0-bad.json').read_text())['result'] == 'invalid'
    assert calls == ['Decision?']


async def test_journal_io_failure_recovers_claim_without_redial(tmp_path, monkeypatch):
    import telegent.outbox as module
    put(tmp_path)
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    original = module.write_private
    def fail_once(path, data):
        if path.name == '.attempts.json':
            monkeypatch.setattr(module, 'write_private', original)
            raise OSError('transient')
        original(path, data)
    monkeypatch.setattr(module, 'write_private', fail_once)
    with pytest.raises(OSError):
        await box.process(active=False, now=NOW)
    await box.process(active=False, now=NOW)
    assert not calls
    assert not list((tmp_path / 'processing').iterdir())
    assert json.loads((tmp_path / 'failed/one.json').read_text())['result'] == 'failed'


async def test_result_io_failure_recovers_without_another_attempt(tmp_path, monkeypatch):
    import telegent.outbox as module
    put(tmp_path)
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    original = module.write_private
    def fail_once(path, data):
        if path.parent.name == 'done':
            monkeypatch.setattr(module, 'write_private', original)
            raise OSError('transient')
        original(path, data)
    monkeypatch.setattr(module, 'write_private', fail_once)
    with pytest.raises(OSError):
        await box.process(active=False, now=NOW)
    await box.process(active=False, now=NOW)
    assert calls == ['Decision?']
    assert not list((tmp_path / 'processing').iterdir())
    assert json.loads((tmp_path / 'done/one.json').read_text())['result'] == 'answered'


async def test_minimum_gap_longer_than_hour_survives_restart(tmp_path):
    calls = []
    put(tmp_path)
    await Outbox(tmp_path, lambda text: calls.append(text), allowed=True, min_gap=7200).process(active=False, now=NOW)
    put(tmp_path, name='two', created=NOW.timestamp()+3600)
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True, min_gap=7200)
    await box.process(active=False, now=NOW+timedelta(hours=1))
    assert len(calls) == 1
    put(tmp_path, name='two', created=NOW.timestamp()+7200)
    await box.process(active=False, now=NOW+timedelta(hours=2))
    assert len(calls) == 2


# --- MOU-414: poison requests must never stop the spool -----------------------------------------
SURROGATE = b'Z\xe1loha'.decode('utf-8', 'surrogateescape')


def put_raw(path, name, raw):
    (path / (name + '.json')).write_text(raw)


@pytest.mark.parametrize('allowed,age', [(True, 0), (False, 0), (True, 3600)], ids=['call', 'not-whitelisted', 'stale'])
async def test_surrogate_request_is_invalid_and_later_request_rings(tmp_path, allowed, age):
    put(tmp_path, name='0-bad', text=SURROGATE, created=NOW.timestamp() - age)  # json.dumps escapes it as \udce1
    put(tmp_path, name='1-good')
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=allowed)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/0-bad.json').read_text()) == {'result': 'invalid'}
    assert calls == (['Decision?'] if allowed else [])


async def test_deeply_nested_request_is_invalid_and_later_request_rings(tmp_path):
    put_raw(tmp_path, '0-bad', '[' * 60000)
    put(tmp_path, name='1-good')
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/0-bad.json').read_text()) == {'result': 'invalid'}
    assert calls == ['Decision?']


async def test_unwritable_claim_is_invalid_without_a_call_or_attempt(tmp_path, monkeypatch):
    import telegent.outbox as module
    put(tmp_path, name='0-bad', text='Bad')
    put(tmp_path, name='1-good')
    original = module.write_private
    def fail_claim(path, data):
        if path.parent.name == 'processing' and data.get('text') == 'Bad':
            raise UnicodeEncodeError('utf-8', 'x', 0, 1, 'surrogates not allowed')
        original(path, data)
    monkeypatch.setattr(module, 'write_private', fail_claim)
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/0-bad.json').read_text()) == {'result': 'invalid'}
    assert calls == ['Decision?']
    assert json.loads((tmp_path / '.attempts.json').read_text()) == [NOW.timestamp()]
    assert not list((tmp_path / 'processing').iterdir())


@pytest.mark.parametrize('raw', ['[' * 60000, '{"text": "Z\\udce1loha", "priority": "urgent", "created": 1, "result": "answered"}'], ids=['nested', 'surrogate'])
async def test_poison_claim_left_in_processing_is_reconciled(tmp_path, raw):
    (tmp_path / 'processing').mkdir()
    put_raw(tmp_path / 'processing', 'old', raw)
    put(tmp_path, name='new')
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/old.json').read_text())['result'] == 'failed'
    assert calls == ['Decision?']


async def test_reconcile_write_error_on_content_falls_back_to_empty_failed(tmp_path, monkeypatch):
    import telegent.outbox as module
    (tmp_path / 'processing').mkdir()
    put(tmp_path / 'processing', 'old')
    original = module.write_private
    def reject_text(path, data):
        if 'text' in data:
            raise UnicodeEncodeError('utf-8', 'x', 0, 1, 'surrogates not allowed')
        original(path, data)
    monkeypatch.setattr(module, 'write_private', reject_text)
    box = Outbox(tmp_path, lambda text: pytest.fail('redial'), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/old.json').read_text()) == {'result': 'failed'}
    assert not list((tmp_path / 'processing').iterdir())


# MOU-429 R2: a crash-left claim whose raw "result" is not a string must not stop the spool.
@pytest.mark.parametrize('result', [['x'], {'k': 1}])
async def test_claim_with_non_string_result_is_reconciled(tmp_path, result):
    (tmp_path / 'processing').mkdir()
    (tmp_path / 'processing' / 'old.json').write_text(json.dumps(
        {'text': 'Old', 'priority': 'urgent', 'created': NOW.timestamp(), 'result': result}))
    put(tmp_path, name='new')
    calls = []
    box = Outbox(tmp_path, lambda text: calls.append(text), allowed=True)
    await box.process(active=False, now=NOW)
    assert json.loads((tmp_path / 'failed/old.json').read_text())['result'] == 'failed'
    assert calls == ['Decision?']
