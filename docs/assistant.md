# Assistant (API 1.54.0)

SPARK uses a C++ OpenAI Responses adapter with `gpt-6-luna`, authenticated queries and durable results. No dashboard agent is needed. Each request supplies instructions and a strict response schema, sets `store:false`, and never sends the SPARK token to OpenAI.

The assistant supports caller-scoped conversation follow-ups, timer creation/listing, calendar occurrence queries and one-off event creation. Timer and event creations can be undone. Server-side permissions and validation control execution; model text alone never proves an action occurred. Calendar edits and removals require saved confirmation previews. Recurring-event creation, occurrence-level exceptions, file operations and assistant TCP updates remain future work.

## Configuration

Requires an authenticated instance by default. Omit `assistant` to disable. Private files
must be outside shared storage; never upload the API key through /files.

Run as the server OS user (pmc on the VPS):

```sh
install -d -m 700 ~/.config/spark ~/.local/share/spark-assistant
install -m 600 /dev/null ~/.config/spark/openai.json
nano ~/.config/spark/openai.json
```

Enter locally:

```json
{"api_key":"YOUR_OPENAI_API_KEY"}
```

Add this top-level object to config/config.json, keeping the existing sections:

```json
"assistant": {
  "credentials_file": "/home/pmc/.config/spark/openai.json",
  "state_directory": "/home/pmc/.local/share/spark-assistant",
  "model": "gpt-6-luna",
  "reasoning": "none"
}
```

The directory must exist with mode 700 and the credentials file with mode 600,
owned by the running user. Reasoning supports none, low, medium. Config validation
checks local settings without contacting OpenAI or printing the key. The existing
curl runtime dependency provides verified HTTPS. Rebuild, check config and restart.
Apply migrations through 026 using ./scripts/setup-db.sh before starting an assistant-enabled instance. Provision a device token with the
new `assistant.use` permission; existing tokens do not gain it automatically.

## Client protocol

In token mode all routes require authentication and assistant.use, even if
allow_open_access is set. Local open mode requires explicit opt-in as below. Browser sessions retain ordinary CSRF requirements.

- GET /assistant: availability, contract version and capability flags.
- POST /assistant/requests: JSON `{ "request_id":"phone-001", "text":"Hello" }`.
- GET /assistant/requests/phone-001: retrieve the caller's result.

POST is synchronous with up to two 45-second provider calls when resolving an event edit/removal; configure the phone timeout to at least 110 seconds. One request executes at a time per state directory; concurrent submits
receive 429 assistant_busy. Poll GET after a lost POST response, or repeat the exact
POST. IDs are scoped to identity kind/subject; other callers get 404. Reusing an ID
with different text returns 409. Failed requests stay failed: use a new ID to retry.
After a server interruption, repeat the exact POST to reconcile any committed action. An abandoned processing record without a committed action becomes failed; it is not automatically sent to OpenAI again.

Responses contain version, request_id, status, message, resources,
undo_available, error (null or a stable code), and created_at (Unix seconds).
Statuses are processing, completed, needs_clarification, needs_confirmation and failed.
For clarification, send a new request_id with the returned conversation_id and the answer. Omitting conversation_id starts fresh.
Maximum transcript size is 8192 UTF-8 bytes. Request IDs use 1–128 ASCII letters,
digits, underscores or hyphens. Provider refusal, truncation, invalid JSON and
transport errors return stable error codes without exposing provider bodies.

## Retention and deployment

Private records store the transcript, request fingerprint, generated reply and conversation metadata, never the API key. Relevant history is sent to the model provider. Transcripts and replies may contain sensitive information. Conversation expiry prevents reuse after 24 hours but does not erase records from disk. This
initial harness caps the directory at 10,000 JSON records and rejects new requests
with assistant_history_full when full (existing results remain readable). It
retains results until the operator removes records while the
server is stopped. Removing a record loses ordinary reply deduplication, but committed mutations remain deduplicated by SQL action history.
The directory is not included in normal shared-file backups. SQL action history is included in database backups; automatic retention remains deferred.

A live OpenAI call incurs API usage. Automated tests use an injected transport and
fake credentials; a live smoke test requires the operator's configured key.
Neither a dashboard setup nor an OpenAI-hosted agent object is required.

## Local open-mode testing

Add `"allow_open_access": true` inside `assistant` to test on an instance without
an `auth` section. It defaults to false; `--remote` rejects true even when auth is
configured. The OpenAI API key is still required and stays private.

Anyone able to reach the open instance can submit billable queries and read its
open-mode results. All open callers share one request-ID namespace; use random
UUIDs for request IDs. This option does not limit traffic to private IP addresses.
It never bypasses authentication on a token-enabled instance.

Rebuild, rerun --check-config (which reports assistant_allow_open_access), and
restart the server. Apply the action-history migration described below before enabling timer tools.

## Timer actions (API 1.53.0)

This milestone enables timer creation and active-timer listing. Daily-task and file tools, asynchronous execution and assistant
TCP updates are still pending. Standard timer TCP notifications work normally.

Before starting an assistant-enabled instance, apply migration
`sql/025_action_history.sql` using `./scripts/setup-db.sh` as the server OS user.
Existing non-assistant instances do not need this table to start. Rebuild and restart.
Tokens need assistant.use plus timers.write for creation/undo or timers.read for
listing. Open access continues to require explicit local opt-in.

Try “Set a ten-minute tea timer” and “What timers are running?”. Missing durations
should prompt clarification; send the answer with the returned conversation_id.
Only one mutation is supported per request. The model proposes a typed intent;
SPARK validates the parameters and caller permissions, then executes it. Current
permission/session state is checked again after the provider returns.

Creation replies include action_id, a timer resource reference and undo_available.
Listing replies contain a timers array and a server-generated count. Normal success
text is generated from committed data. Timer events are written transactionally.

- GET /assistant/actions/{id}: inspect the caller's action and snapshots.
- POST /assistant/actions/{id}/undo with an empty body or `{}`: stop its created timer.

Both routes require assistant.use and timers.write, plus action ownership. Undo is
available for 24 hours from database creation time. It stops running or naturally
expired timers, but cannot retract alarms already delivered. A later manual stop,
confirmation or conflicting edit returns 409 undo_conflict. Expired undo returns
409 undo_expired. Repeated successful undo returns the same recorded reversal.
No model call is required for undo.

Action history is stored in MariaDB with before/after snapshots, undo snapshots,
owner and request fingerprint. Timer creation, its event and history commit as one
transaction; reversal also commits atomically. Committed actions remain deduplicated
even if private response files are lost. Repeated POST recovers a lost receipt; GET and repeated POST reconcile existing receipts, including undone state. Following database_unavailable, retry the SAME request ID
before creating a new one because a commit may have succeeded. Ordinary replies
remain in private files; they have not yet migrated to SQL.

History remains retained to preserve deduplication; expiry disables undo, not the
history record. SQL backups include action_history. Do not prune those records
without a future explicit retention/deduplication policy.

Provider errors now distinguish provider_quota_exhausted, provider_rate_limited,
provider_authentication_failed and provider_unavailable when the upstream supplies
those codes. Provider response bodies and keys are never returned to clients.

Run scripts/test-assistant-actions.sh for isolated database tests (no live model
calls). It covers atomic rollback, retries, concurrent creation, ownership,
intervening changes, expiration, undo and receipt-loss recovery.

## Conversation follow-ups (API 1.53.0)

Start with `POST /assistant/requests`:

```json
{"request_id":"phone-turn-1","text":"Set a timer"}
```

The reply includes `conversation_id` (equal to the first request ID),
`conversation_expires_at` (Unix seconds), and normally `needs_clarification`.
Continue with a NEW request ID:

```json
{"request_id":"phone-turn-2","conversation_id":"phone-turn-1","text":"Ten minutes"}
```

The same request ID is only for transport retries with the identical packet.
Conversation state survives restart and is scoped to identity kind/subject.
Tokens representing the same subject share that scope; all open-mode clients
share one identity. Use unpredictable request IDs in open mode.

Conversations expire 24 hours after the first turn, have at most 32 turns, and
allow at most 64 KiB of prior context. Limits fail explicitly rather than silently
losing earlier details. Errors: 404 conversation_not_found, 410 conversation_expired,
409 conversation_full, 409 conversation_pending. Start fresh after expiry/full;
reconcile an interrupted earlier request before continuing a pending conversation.
Existing pre-conversation receipts remain retrievable but cannot start follow-ups.
If a receipt was deleted and recovered solely from action history, start a new
conversation because its original context is no longer available.

Permissions for prior tool results are checked again before disclosure to the
provider or retrieval; losing a permission can make a conversation unavailable.
Undo status is refreshed from action history before passing it as context. Dates
use the current server time and Australia/Perth (+08:00). Clarify ambiguous dates.
History is context, never authority to replay completed actions.

## Calendar tools

- `events.read`: query occurrences in an explicitly selected local/shared calendar.
  The result contains `occurrences`, `calendar`, `range` and `has_more`. Bounds are
  inclusive `from`, exclusive `to`, with explicit offsets normalized to UTC.
  Range limit: 366 days. Result limit: 100; narrow the range when has_more is true.
  Recurring series are expanded using the existing occurrence engine.
- `events.write`: create one non-recurring event. Supply name, date/time and an
  explicit local/shared calendar (which may be resolved from conversation).
  Description, place and reminder_minutes are optional. Missing calendar or
  ambiguous date/time prompts clarification. The existing reminder scheduler and
  calendar notification channel handle the resulting event normally.
- Undo: `POST /assistant/actions/{id}/undo`, body `{}`, requires events.write and
  ownership. Deletes only the event created by that action, within 24 hours, if
  its editable fields/calendar/update timestamp still match. A subsequent edit or
  deletion conflicts. Reminder delivery alone does not block undo; already sent
  reminders cannot be retracted. Shared-calendar undo writes a sync tombstone.

Mutation, action record, calendar sync state and notification event are committed
atomically. Duplicate requests recover the original event instead of inserting
another. No new permission names or migration beyond 025 are introduced.

## Assistant integration guide

`GET /assistant/setup` returns a machine-readable guide covering the caller's
permissions, authentication headers, examples for initial and follow-up requests,
response states, polling/retries, undo, calendar selection, limits and privacy.
It requires assistant.use in token mode, with the same explicit opt-in in open
mode. It contains no provider credentials or stored transcripts. The endpoint is
also documented in `/api` and `/api/docs`.

## Confirmed calendar edits and removals (API 1.54.0)

Apply `sql/026_assistant_confirmations.sql` using `./scripts/setup-db.sh` and rebuild.
This adds durable pending previews and decision receipts, plus undo metadata.
No new permission names: edits/removals and their confirmations require
`assistant.use`, `events.read` and `events.write` together.

Example conversation:

1. Send “Move my shared-calendar dentist appointment from Wednesday to Friday.”
2. SPARK resolves the existing event. Multiple matches return `needs_clarification`
   and `candidates`; select an event before a preview is offered.
3. SPARK returns `needs_confirmation`, `confirmation_id`, `confirmation_status`,
   `confirmation_expires_at`, a human-readable question and `preview` containing
   operation, calendar, scope, event ID, before and proposed after snapshots.
4. Show Confirm / Cancel buttons. POST an empty body or `{}` to one of:
   - `/assistant/confirmations/{id}/confirm`
   - `/assistant/confirmations/{id}/cancel`
5. Alternatively, send a NEW request_id in the SAME conversation with the `confirmation_id` the user heard/saw and “yes”,
   “yes please”, “confirm”, “confirm it”, “go ahead” or “do it”. Cancellation accepts
   “no”, “no thanks”, “cancel” or “cancel it”. Matching ignores punctuation/case.
   These decisions do not call the model or regenerate the proposed operation. A bare yes/no without confirmation_id never executes a proposal; the server returns the preview again. Attach confirmation_id only to an explicit confirm/cancel utterance, not a correction.

GET `/assistant/confirmations/{id}` retrieves current proposal/action status.
Confirm/cancel bodies cannot change any event fields. Five-minute expiry uses the
DB clock. The decision rechecks ownership, permissions and the locked event
snapshot. Changed or removed events produce a conflict without mutation; the
response includes current_event so the UI can explain the change and request a
fresh preview. Typed POST returns 410 on expiry or 409 for invalidated/conflicting
proposals; conversational replies keep HTTP 200 with a failed status/error.

Any other follow-up supersedes an outstanding proposal before model processing.
“Actually Saturday” therefore creates a new preview requiring fresh confirmation.
Old buttons cannot execute superseded proposals. An ambiguous response may lead
to another clarification; it never counts as approval. Confirmation retries are
idempotent, including concurrent requests. Spoken decisions are recorded in SQL
alongside the mutation, so a lost private receipt cannot approve a newer proposal.

Recurring edits/removals require explicit entire-series scope. Single-occurrence
exceptions are not implemented and are never silently converted to a whole-series
operation. Entire-series edits can modify the weekly recurrence rule; the normal
calendar validation applies. Cross-calendar moves remain unsupported here.

Confirmed edits and removals return action_id and undo_available. Undo restores
prior fields or the deleted event (including its original event ID). Shared event
restoration reuses the original sync identity. Subsequent edits, recreation, or a
changed shared tombstone block unsafe undo. Ordinary calendar notifications and
sync changes are written atomically with actions and confirmation decisions.
Already delivered reminders cannot be retracted. Explicit undo uses the existing
action endpoint and does not ask for an additional confirmation.

The normal `/events` APIs retain their existing contract; these previews govern
assistant-mediated edits/removals. Creation still executes directly with undo.
