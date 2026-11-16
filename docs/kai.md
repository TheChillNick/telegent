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

## Outgoing calls (MOU-316; live validation pending)

`KAI_OUTBOX_DIR` enables a private spool, with no listening port. Unset/empty
means no watcher and no outgoing calls. Only `KAI_CALL_USER_ID` can be called;
it must be a positive ID explicitly present in `ALLOWED_USER_IDS` (an empty
incoming whitelist does **not** authorize outgoing calls).

Write one UTF-8 `*.json` per request, atomically by temporary file + rename:

```json
{"text":"The build failed. Should I revert it?","priority":"normal","created":1794654000}
```

Use a private 0700 directory and 0600 files. The host `kai-call` helper in
Hetzner-Server-Setup implements this contract. The worker polls once per second.
Files wait while any incoming/outgoing call is present, during quiet hours, or
when the rate budget is exhausted. Requests older than the stale limit become
`failed/` results even if they were waiting for another call. Urgent requests
bypass quiet hours only, never the whitelist, active-call check or rate limits.
When a call ends, telegent waits at most 10 s for ntgcalls to stop the media, then
unregisters the session. If the native stop is still running, it logs one WARNING
`Native media stop for user=<id> still running after 10 s; user stays busy.` (after a
cancelled wait: `... still running after its wait was cancelled; user stays busy.`).
Until the native stop ends, queued requests wait (none is dialled) and an incoming
call from that user is discarded as busy (`Rejected: the previous call's native media
stop is still running for this user.`). If it never ends, there is no further log line:
queued requests become `stale` (the host forwards each one), and telegent must be
restarted. A native stop that fails within the 10 s logs `Native media stop failed: <Class>`;
a failure after the 10 s only ends the wait (no line).
A directory, FIFO, socket or device named `*.json` in the spool or in `processing/` is
skipped and left in place, never opened; telegent logs `Outgoing spool: skipped a
non-regular entry (<kind>); left in place` once per kind (never the name). A symlink is
refused as `invalid` (in `processing/`: `failed`).

| Environment | Default | Meaning |
| --- | --- | --- |
| `KAI_OUTBOX_DIR` | empty | Opt-in spool directory |
| `KAI_CALL_USER_ID` | 0 | Single authorized callee (0 refuses) |
| `KAI_CALL_MAX_PER_HOUR` | 4 | Rolling-hour attempt budget, including failures |
| `KAI_CALL_MIN_GAP` | 60 | Seconds between attempt starts |
| `KAI_CALL_RING_TIMEOUT` | 40 | Seconds from the attempt until the callee answers; then discard (Missed) and result no-answer |
| `KAI_CALL_SETUP_TIMEOUT` | 15 | Seconds after the answer for key exchange, ConfirmCall and media; then discard and result failed |
| `KAI_CALL_STALE_SECONDS` | 900 | Maximum queued age in seconds |
| `KAI_CALL_QUIET_START` | 22 | Quiet-hours start, local hour inclusive |
| `KAI_CALL_QUIET_END` | 7 | Quiet-hours end, exclusive; equal hours disable quiet hours |
| `KAI_CALL_TIMEZONE` | Europe/Prague | IANA zone, including DST |

An invalid `KAI_CALL_TIMEZONE` (or an image without the tz database) stops startup of the whole container, incoming calls included.

A request is claimed into `processing/` before dialing. `.attempts.json` preserves
the rolling budget across restarts; an invalid journal prevents dialing until an
operator repairs it. Interrupted `processing/` requests become `failed`, with no
redial. Run one telegent process per spool. The runtime image explicitly installs IANA
`tzdata` for quiet hours; native installations also need a system timezone database. Do not delete the attempt journal to
work around limits. The queue is polled in filename order; a quiet normal request
does not prevent a later urgent request from being selected.

Caller flow uses `create_p2p_call`, `init_exchange(..., None)` (ntgcalls returns
the g_a hash), `phone.RequestCall`, `PhoneCallAccepted`, `exchange_keys(..., g_b, 0)`,
`phone.ConfirmCall`, and the shared P2P media/signaling and `CallSession` lifecycle.
Reference: [ntgcalls 3.0.2 caller example](https://github.com/pytgcalls/ntgcalls/blob/v3.0.2/examples/go/ubot/connect_call.go).
The answer starts the same Gemini Live connection, tools, events and `end_call`
handling as an incoming call. Its initial user turn marks the request as
**untrusted data**, caps it at 1,000 characters, and asks Kai to explain it in
2–3 short Czech sentences ending with the question/decision needed. No request
text is added to the system instruction or application log.

Outgoing calls log state changes, registered tool names and error classes with the Telegram RPC error id; never the request text, transcripts, tool arguments/results or exception messages. Pyrogram's retry line keeps the RPC name only, and its transport lines (`Send exception`, `Close exception`, `Unable to connect due to network issues`) keep the exception class only. Keep LOG_LEVEL=INFO: the websockets logger is capped at INFO, because at DEBUG it logs Gemini frames including transcript fragments.

Completed JSON retains `text`, `priority`, `created` and adds `result`:
`done/` for `answered`; `failed/` for `no-answer`, `declined`, `busy`, `failed`,
`stale`, `not-whitelisted`, or `invalid`. The ring deadline starts at the attempt and ends at the answer; the setup deadline starts at the answer; the RequestCall RPC disables Pyrogram retries/FloodWait sleeping and
uses a timeout no longer than 15 seconds or that deadline. A RequestCall RPC timeout counts as `no-answer`. After the answer, an RPC timeout of the key exchange or ConfirmCall is `failed` with `Outgoing setup failed: TimeoutError`; only the setup deadline logs `Outgoing setup timed out after answer`. CI runs Python 3.10, 3.12 and 3.13. If cancellation
occurs while that RPC is in flight, cleanup retains its response to discard a
remotely created call; waiting for that response can extend local cleanup beyond
the deadline. If telegent is stopped while the RequestCall is in flight (also while
cleanup after the ring deadline waits for it), the stop waits up to 5 s for the ID and
then discards the call (Missed), so the phone stops ringing; an ID that comes later is
lost and logged as `Ring discard not sent for 1 call(s) …` (the phone may ring until
Telegram ends the call). During the stop every DiscardCall is one try of at most 2 s, including reconnect waits and FloodWait handling;
a late discard still running after that logs `Ring discard not confirmed for 1 call(s) …`.
The stop so takes at most about 7 s plus the client stop, or 2 s plus the 10 s native
media stop when that hangs; the deploy compose sets `stop_grace_period: 25s`.
At stop it also logs how many native media stops are still running (not awaited).
If Telegram never returns an ID, remote state is unknown (logged)
and no redial is attempted. Invalid JSON produces a result without
request text. `answered` means P2P setup completed and Gemini startup was
scheduled, not proof that the entire announcement was heard. The host can forward
unsuccessful attempts through `hz-tg`; no call is automatically redialed. A request whose text is not valid UTF-8 (e.g. a lone surrogate from a non-UTF-8 terminal) or whose JSON nests too deeply becomes `invalid` without request text and never blocks later requests.

Offline tests cover caller handshake/duplicate updates, early acceptance,
remote busy/decline/missed calls, timeout, cancellation, setup failure,
whitelist refusal, active-call queueing, normal/urgent quiet hours, stale/invalid
requests, capped text, persistent rate limits, a slow native media stop (queued
request waits, incoming call busy, late error retrieved), non-regular spool entries,
a stop during ring cleanup, and Pyrogram retry/transport log lines without exception text. **No real Telegram/Gemini call
has been made for this change.** After review and a separately authorized deploy,
Claude/Atlas and Štěpán still need to verify ringing, speech, bidirectional audio,
tools/events, decline/timeout and host fallback on the real account. Automatic
alerts/briefings must remain disconnected until that live test passes.
