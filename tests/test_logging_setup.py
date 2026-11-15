import logging

from telegent import logging_setup


def test_debug_level_never_enables_websockets_frames(monkeypatch):
    ws, root = logging.getLogger('websockets'), logging.getLogger()
    monkeypatch.setattr(logging_setup, '_configured', False)
    monkeypatch.setattr(ws, 'level', logging.NOTSET)
    monkeypatch.setattr(root, 'level', logging.DEBUG)  # what LOG_LEVEL=DEBUG gives in production
    logging_setup.configure_logging('DEBUG')
    assert not ws.isEnabledFor(logging.DEBUG)
    assert ws.isEnabledFor(logging.INFO)
