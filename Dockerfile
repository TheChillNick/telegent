# Telegent - image runtime untuk AI voice agent panggilan Telegram (berbasis uv)

# ---- Builder: siapkan .venv lengkap (butuh compiler untuk tgcrypto) ----
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# tgcrypto adalah ekstensi C -> butuh compiler saat tidak ada wheel.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# ---- Runtime: hanya bawa .venv, tanpa toolchain ----
FROM python:3.13-slim AS runtime

# Quiet hours need the IANA database even on a minimal runtime image.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TELEGRAM_SESSION_DIR=/data

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Direktori sesi Telegram (di-mount sebagai volume).
RUN mkdir -p /data

ENTRYPOINT ["telegent"]
