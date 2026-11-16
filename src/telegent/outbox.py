"""Private disk spool for one-shot outgoing calls; no listening socket."""
from __future__ import annotations

import inspect
import json
import math
import os
import stat
import tempfile
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_KINDS = ((stat.S_ISDIR, 'directory'), (stat.S_ISFIFO, 'fifo'), (stat.S_ISSOCK, 'socket'),
          (stat.S_ISCHR, 'device'), (stat.S_ISBLK, 'device'))


def write_private(path: Path, data: Any) -> None:
    """Publish a complete 0600 JSON file in the destination directory."""
    fd, name = tempfile.mkstemp(prefix='.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(data, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class Outbox:
    def __init__(self, directory: Path, call: Callable[[str], str | None | Awaitable[str]], *,
                 allowed: bool, max_per_hour: int = 4, min_gap: float = 60,
                 stale_seconds: float = 900, quiet_start: int = 22, quiet_end: int = 7,
                 timezone_name: str = 'Europe/Prague') -> None:
        self.directory = Path(directory)
        self.call = call
        self.allowed = allowed
        self.max_per_hour = max_per_hour
        self.min_gap = min_gap
        self.stale_seconds = stale_seconds
        self.quiet_start, self.quiet_end = quiet_start, quiet_end
        self.timezone = ZoneInfo(timezone_name)
        for path in (self.directory, *(self.directory / x for x in ('processing', 'done', 'failed'))):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.journal = self.directory / '.attempts.json'
        self._skipped_kinds: set[str] = set()

    def _skip(self, path: Path) -> bool:
        """True for an entry that must never be opened (directory, FIFO, socket, device) or that vanished.

        Symlinks are not skipped: _read refuses them (O_NOFOLLOW) and they end as invalid/failed.
        """
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            return True  # vanished between glob and lstat
        if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
            return False
        kind = next((name for test, name in _KINDS if test(mode)), 'other')
        if kind not in self._skipped_kinds:
            self._skipped_kinds.add(kind)
            # The kind only: never the entry name (kai-call names can carry request data).
            log.warning('Outgoing spool: skipped a non-regular entry (%s); left in place', kind)
        return True

    @staticmethod
    def _read(path: Path) -> dict[str, Any]:
        # Bound memory and never follow a request symlink outside the spool.
        # O_NONBLOCK: a FIFO swapped in after _skip() must not block the event loop.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'r', encoding='utf-8') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError('oversized request')
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError('invalid request')
        return data

    @staticmethod
    def _request(data: dict[str, Any]) -> dict[str, Any]:
        text, created = data.get('text'), data.get('created')
        if (not isinstance(text, str) or not text.strip()
                or data.get('priority') not in ('normal', 'urgent')
                or not isinstance(created, (float, int)) or isinstance(created, bool)
                or not math.isfinite(created)):
            raise ValueError('invalid request')
        try:
            text.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('invalid request') from None  # e.g. a lone surrogate from a non-UTF-8 terminal
        return {'text': text[:1000], 'priority': data['priority'], 'created': created}

    def _finish(self, path: Path, data: dict[str, Any], result: str) -> None:
        data['result'] = result
        if path.parent == self.directory / 'processing':
            write_private(path, data)  # preserve the outcome if final publication fails
        target = self.directory / ('done' if result == 'answered' else 'failed') / path.name
        write_private(target, data)
        path.unlink()
        # Never log the request, model context or exception contents.
        log.info('Outgoing request result=%s', result)

    async def process(self, *, active: bool, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)
        stamp = now.timestamp()
        # Only one consumer owns the spool. Every pass reconciles claims left
        # behind by crashes or transient I/O errors, always without redialing.
        for path in sorted((self.directory / 'processing').glob('*.json')):
            if self._skip(path):
                continue
            try:
                original = self._read(path)
                data = self._request(original)
                result = original.get('result', 'failed')
                if not isinstance(result, str) or result not in {'answered', 'declined', 'busy', 'no-answer', 'failed', 'not-whitelisted'}:
                    result = 'failed'
            except (OSError, ValueError, UnicodeError, OverflowError, RecursionError):
                data, result = {}, 'failed'
            try:
                self._finish(path, data, result)
            except (ValueError, RecursionError):  # content that cannot be written; OSError still ends the pass
                self._finish(path, {}, 'failed')
        # Invalid/missing journal fails closed (worker logs and retries reading,
        # never resets quota after corruption). Future entries also consume quota.
        attempts = json.loads(self.journal.read_text()) if self.journal.exists() else []
        if not isinstance(attempts, list) or any(
            type(x) not in (float, int) or not math.isfinite(x) for x in attempts
        ):
            raise ValueError('invalid attempt journal')
        attempts = [x for x in attempts if x > stamp - max(3600, self.min_gap)]
        hour = now.astimezone(self.timezone).hour
        quiet = (self.quiet_start <= hour < self.quiet_end if self.quiet_start < self.quiet_end
                 else hour >= self.quiet_start or hour < self.quiet_end)
        if self.quiet_start == self.quiet_end:
            quiet = False
        for path in sorted(self.directory.glob('*.json')):
            if path.name.startswith('.') or self._skip(path):
                continue
            data: dict[str, Any] = {}
            try:
                data = self._read(path)
                data = self._request(data)
            except (OSError, ValueError, UnicodeError, OverflowError, RecursionError):
                self._finish(path, {}, 'invalid')
                continue
            if stamp - data['created'] > self.stale_seconds:
                self._finish(path, data, 'stale')
                continue
            if not self.allowed:
                self._finish(path, data, 'not-whitelisted')
                continue
            if active or (quiet and data['priority'] == 'normal'):
                continue
            if sum(x > stamp - 3600 for x in attempts) >= self.max_per_hour or (attempts and stamp - max(attempts) < self.min_gap):
                continue
            claimed = self.directory / 'processing' / path.name
            os.replace(path, claimed)
            try:
                write_private(claimed, data)
            except (ValueError, RecursionError):  # never dialled, journal untouched
                self._finish(claimed, {}, 'invalid')
                continue
            # Commit the attempt before invoking Telegram, including failed calls.
            attempts.append(stamp)
            write_private(self.journal, attempts)
            try:
                result = self.call(data['text'])
                if inspect.isawaitable(result):
                    result = await result
                result = result or 'answered'
                if result not in {'answered', 'declined', 'busy', 'no-answer', 'failed', 'not-whitelisted'}:
                    result = 'failed'
            except Exception:
                result = 'failed'
            self._finish(claimed, data, result)
            return  # Recheck app activity before taking another request.
