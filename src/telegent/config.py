"""Konfigurasi aplikasi (terpusat, tervalidasi, dari environment / .env)."""

from __future__ import annotations

from pathlib import Path

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from telegent.ai.prompt import (
    DEFAULT_GREETING_PROMPT,
    DEFAULT_IDLE_FAREWELL,
    DEFAULT_IDLE_PROMPT,
    DEFAULT_SYSTEM_INSTRUCTION,
)

DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-live-preview"


class Settings(BaseSettings):
    """Semua konfigurasi runtime dari environment / .env.

    Umumnya nama env var = nama field (case-insensitive), kecuali yang diberi
    ``validation_alias`` eksplisit (mis. ``TELEGRAM_SESSION_DIR``).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # --- Kredensial (wajib) ---
    telegram_api_id: int
    telegram_api_hash: str
    gemini_api_key: str

    # --- Sesi Telegram ---
    telegram_session_name: str = "userbot_session"
    # Nama env resmi adalah TELEGRAM_SESSION_DIR (lihat .env.example & Dockerfile).
    # "SESSION_DIR"/"session_dir" dipertahankan agar tetap kompatibel.
    session_dir: str = Field(
        default=".",
        validation_alias=AliasChoices(
            "TELEGRAM_SESSION_DIR", "SESSION_DIR", "session_dir"
        ),
    )

    # --- Gemini Live ---
    gemini_model: str = DEFAULT_GEMINI_MODEL
    system_instruction: str = DEFAULT_SYSTEM_INSTRUCTION
    greeting_prompt: str = DEFAULT_GREETING_PROMPT

    persona: str = "default"
    kai_api_base: str = ""
    kai_api_token: str = ""
    kai_events_file: str = ""

    @model_validator(mode="after")
    def apply_persona(self) -> "Settings":
        if self.persona not in {"default", "kai"}:
            raise ValueError("PERSONA must be default or kai")
        if not self.idle_enabled and "max_call_duration" not in self.model_fields_set:
            self.max_call_duration = 0
        if self.persona == "kai":
            if "system_instruction" not in self.model_fields_set:
                self.system_instruction = Path(__file__).with_name("ai").joinpath("kai.txt").read_text(encoding="utf-8")
            if "greeting_prompt" not in self.model_fields_set:
                self.greeting_prompt = "Pozdrav krátce česky jako Kai a nabídni pomoc."
        return self

    # --- Kebijakan akses & beban ---
    allowed_user_ids: str = ""
    max_concurrent_calls: int = Field(default=4, ge=1)
    # Durasi maksimum satu panggilan (detik). 0 = tanpa batas.
    max_call_duration: float = Field(default=600.0, ge=0)
    # Rate-limit panggilan masuk per pengguna: maksimum `max_calls_per_window`
    # panggilan dalam `call_window_seconds` detik. 0 = tanpa batas.
    max_calls_per_window: int = Field(default=5, ge=0)
    call_window_seconds: float = Field(default=60.0, gt=0)

    # --- Logging ---
    log_level: str = "INFO"

    # --- Anti-idle (penelpon tidak menjawab) ---
    idle_enabled: bool = True
    idle_prompt_seconds: float = 15.0
    idle_hangup_seconds: float = 15.0
    idle_prompt_instruction: str = DEFAULT_IDLE_PROMPT
    idle_farewell_instruction: str = DEFAULT_IDLE_FAREWELL

    # --- Audio (parameter Telegram VoIP & Gemini) ---
    tg_sample_rate: int = 48000
    tg_frame_ms: int = 10
    gemini_input_rate: int = 16000
    gemini_output_rate: int = 24000

    @field_validator("allowed_user_ids")
    @classmethod
    def _validate_allowed_user_ids(cls, value: str) -> str:
        """Pastikan setiap ID valid saat load (fail-fast, bukan crash saat panggilan)."""
        for part in value.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                int(part)
            except ValueError:
                raise ValueError(
                    f"ID Telegram tidak valid: {part!r} "
                    "(harus bilangan bulat, dipisah koma/titik-koma)"
                ) from None
        return value

    @property
    def allowed_user_id_set(self) -> set[int]:
        """Parse daftar ID (dipisah koma/titik-koma) dari env ALLOWED_USER_IDS."""
        ids: set[int] = set()
        for part in self.allowed_user_ids.replace(";", ",").split(","):
            part = part.strip()
            if part:
                ids.add(int(part))
        return ids

    @property
    def session_workdir(self) -> Path:
        """Direktori tempat file *.session Pyrogram disimpan (mis. /data di Docker)."""
        path = Path(self.session_dir).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def tg_frame_bytes(self) -> int:
        """Ukuran satu frame audio Telegram (10 ms) dalam byte: 48000 * 0.01 * 2 = 960."""
        return int(self.tg_sample_rate * (self.tg_frame_ms / 1000.0) * 2)
