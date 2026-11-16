"""Konfigurasi logging terpusat dan logger berkonteks sesi."""

from __future__ import annotations

import logging
import re

from pyrogram.errors import RPCError

_configured = False

_RETRY_WITH_REASON = '[%s] Retrying "%s" due to: '


class _RetryReasonFilter(logging.Filter):
    """Pyrogram's retry line ends in str(exception); keep the line, drop the reason."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str) and record.msg.startswith(_RETRY_WITH_REASON) and isinstance(record.args, tuple):
            record.msg, record.args = '[%s] Retrying "%s"', record.args[:2]
        return True


_RETRY_REASON_FILTER = _RetryReasonFilter()


def configure_logging(level: str = "INFO") -> None:
    """Inisialisasi logging global (idempoten)."""
    global _configured
    if _configured:
        return
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # DEBUG in websockets would log Gemini frames, incl. transcript fragments.
    logging.getLogger("websockets").setLevel(max(logging.INFO, logging.getLogger().level))
    # Pyrogram logs the exception message when it retries an RPC (Telegram error text, errno).
    logging.getLogger("pyrogram.session.session").addFilter(_RETRY_REASON_FILTER)
    _configured = True


_RPC_VALUE = re.compile(r"\[\d{3} [A-Z0-9_]{1,64}\]")


def error_label(exc: BaseException) -> str:
    """Exception class plus Telegram RPC error id; never the exception message."""
    label = type(exc).__name__
    if isinstance(exc, RPCError):
        rpc_id = exc.ID
        if not rpc_id and isinstance(exc.value, str) and _RPC_VALUE.fullmatch(exc.value):
            rpc_id = exc.value
        if rpc_id:
            label = f"{label} {rpc_id}"
    return label


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class _ContextAdapter(logging.LoggerAdapter):
    """LoggerAdapter yang menyisipkan prefix konteks (user/call) ke tiap pesan."""

    def process(self, msg, kwargs):  # type: ignore[override]
        prefix = self.extra.get("prefix", "") if self.extra else ""
        return f"{prefix}{msg}", kwargs


def call_logger(user_id: int, call_id: int) -> logging.LoggerAdapter:
    return _ContextAdapter(
        logging.getLogger("telegent.call"),
        {"prefix": f"[user={user_id} call={call_id}] "},
    )
