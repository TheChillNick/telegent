# Kai voice preparation

Set `PERSONA=kai` for the packaged Czech Kai prompt (English when the caller
speaks English; answers in 2–3 sentences). The default Indonesian persona is
unchanged. `SYSTEM_INSTRUCTION` and `GREETING_PROMPT` override either preset.
For an open line, set `IDLE_ENABLED=false`: idle prompts and idle hang-up are
disabled, and `MAX_CALL_DURATION` defaults to 0 (unlimited). An explicit positive
`MAX_CALL_DURATION` still applies.

Gemini Live enables session resumption and sliding-window context compression.
On GoAway, the adapter reconnects using the latest resumable handle while keeping
Telegram media and call timers active. Without a resumable handle it opens a
fresh Gemini session. A failed reconnect is logged; this is not an automatic
retry policy for arbitrary network failures.

## Shared backend contract

Set `KAI_API_BASE` (for example `http://127.0.0.1:8080/api`) and `KAI_API_TOKEN`.
Tools are registered only when the base URL is configured. Requests use
`Authorization: Bearer <token>`, a four-second timeout, and no redirects.
Use HTTPS for remote backends. No backend is contacted during startup.

| Tool / relative endpoint | Method | Arguments | Response |
| --- | --- | --- | --- |
| `get_status` | GET | `user_id`, `call_id` query parameters | 200 JSON status |
| `create_task` | POST | `title`, `user_id`, `call_id` JSON | 202 JSON with queued task ID |
| `capture_note` | POST | `text`, `user_id`, `call_id` JSON | 200 JSON acknowledgement |
| `request_confirm` | POST | `question`, `user_id`, `call_id` JSON | 200 JSON pending confirmation ID |

The task endpoint must enqueue work and return immediately, never wait for task
execution. Kai returns `on it` on acceptance; completion arrives through events.
Confirmation requests record a pending question, never approval. The backend
must enforce authorization and approval before executing actions.

## Event file

Set `KAI_EVENTS_FILE=/var/lib/pc-events/events.jsonl`. Each active call tails new
complete JSON lines, skipping existing history at startup and invalid JSON.
Each record becomes a short user text turn through `send_client_content`, marked
as untrusted event data and capped at 1,000 characters. The producer should use
short records (at most 64 KiB), append a newline, and avoid secrets. File creation,
rotation and truncation are supported; partial final lines wait for a newline.
The reader is cancelled when the call closes. Events go to every active call,
so restrict `ALLOWED_USER_IDS` and concurrency appropriately.

No outgoing calls, Telegram login, live Gemini access, or real backend requests
are required for tests: `uv sync --frozen && uv run pytest -q` and
`uv run pyright src/telegent`.
