# Assistant (API 1.63.0)

SPARK uses a C++ OpenAI Responses adapter with `gpt-6-luna`, authenticated queries and durable results. No dashboard agent is needed. Each request supplies instructions and a strict response schema, sets `store:false`, and never sends the SPARK token to OpenAI.

The assistant supports caller-scoped conversation follow-ups, timer creation/listing, calendar occurrence queries, one-off event creation, and read-only project names/descriptions, boards and tasks. Timer and event creations can be undone. Server-side permissions and validation control execution; model text alone never proves an action occurred. Calendar edits and removals require saved confirmation previews. Recurring-event creation, occurrence-level exceptions, file mutations and assistant TCP updates remain future work.

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

## Confirmed calendar edits and removals (API 1.56.0)

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

## Weekday dates and joint calendar reads

For one-off creation, a weekday without a date means the next named day strictly
**after today**, in Perth time. The server supplies the model with the upcoming
dates. The assistant states the inferred date and asks the user to confirm via
`needs_clarification`, collecting any missing time and destination calendar in
the same question. Reply with a new request ID and the same conversation ID;
no `confirmation_id` is needed for this date clarification. Explicit dates retain
the existing creation flow. This conversational check is model-guided; the saved,
server-enforced confirmation protocol continues to apply to edits/deletions.

Occurrence queries accept `local`, `shared`, or `both`. “Both” may be supplied as
a follow-up to the calendar selection question. The combined result is ordered
by occurrence time then event ID, with a global 100-entry limit and `has_more`.
Each entry retains `event.calendar`; separate events are not deduplicated by name.
Writes still require a single destination calendar.

## Read-only projects (API 1.56.0)

The assistant can list project names/descriptions and read a selected project's
boards, columns and tasks, including descriptions and severity. Examples: “List
my projects”, “Read the SPARK Mobile project”, and “What tasks are on that project?”
Subprojects are independently selectable, with parent_id included in results.
Ambiguous names return needs_clarification and a projects array of candidates;
answer with a new request ID and the same conversation ID. No project, board or
task mutations are exposed. File paths and file contents are not read.

Permissions: assistant.use plus projects.read for listing; selected project/board
details additionally require boards.read. These are existing permissions. Reads
and stored conversation context are permission-checked on every access.

Responses include projects or project, a readable message and has_more. Board
objects contain columns and tasks; tasks retain column_id and severity. Limits:
100 project matches, 20 boards, 100 columns and 200 tasks per board. Per-board
columns_has_more/tasks_has_more and overall has_more identify incomplete lists.
Results exceeding 48 KiB fail with project_result_too_large; use the project
explorer for larger projects. No new SQL migration is required.

## Project text-file search (API 1.56.0)

“Search the SPARK project files for notification” searches registered text-file
names and contents in that project's directory and descendants (including nested
subproject directories), on its assigned drive only. Project selection can come
from a previous turn. Ambiguous project names require clarification. High-level requests do not require search terms.
The tool requires assistant.use, projects.read and files.read; boards.read is not
required. These are existing permissions. Saved results and conversational context
recheck permissions. File contents are untrusted data, never instructions.

Matching is literal and ASCII case-insensitive, not semantic or regex-based.
Supported extensions: .txt, .json, .md, .markdown, .log, .csv, .tsv, .yaml, .yml,
.toml, .ini and .xml, case-insensitive. Files must contain valid UTF-8 text without
binary control bytes. Binary files, symlinks, nonregular files and oversized
contents are skipped. A filename match may still be reported with content_skipped.
An offline project drive fails with project_storage_unavailable; no fallback drive
is searched. Arbitrary host paths, URLs, archives and file mutations are unsupported.

The file_search response provides query, matches, files_scanned, bytes_read, skipped
counts and has_more. Matches include file_id, logical path, drive_id, download_url,
content_revision, name_match and up to three matching lines with line numbers and
snippets. more_matching_lines indicates additional matching lines. Downloads still
require the caller's normal authentication. No public lease is generated.

Limits per page: 200 registered candidates in the order documented below, 1 MiB per file,
8 MiB total bytes, 30 matching files, 320-byte UTF-8-safe snippets. has_more signals
candidate/result/budget truncation; skipped counts describe content not searched.
A zero-match result applies only to the scanned eligible text, not every project
file. The existing 48 KiB response cap also applies. No new migration is required.

## Browser query page

Open `/assistant/explorer` (also linked from the dashboard) to type queries and
follow-ups. It uses the current browser session, requires assistant.use, and
respects each tool's normal permissions. Local open-mode access follows the
existing explicit opt-in. The page never asks for or stores an OpenAI key.

Enter sends; Shift+Enter inserts a newline. Confirm/Cancel approve saved previews;
Undo reverses eligible actions. Response details expose structured results,
including file-search citations. After a connection failure, Recover last request
reuses the original request ID before allowing another query. New conversation
clears the browser view but does not delete server history. The page's state is
in memory only; reloading starts a fresh view.

## High-level project file exploration

Requests such as “Summarize the files in Demo” or “What does this project do?”
can use project-file overview mode without keywords. The planner selects the
existing file tool with a null/empty search query. SPARK reads up to 12 supported
text files, prioritizing README/overview documents among the first 200 candidates,
and supplies up to 1,600 UTF-8-safe bytes from each to a separate summary call.
That call has no action tools and can only return a completed text response.
Retrieved text is explicitly untrusted data. Existing project/file permissions,
drive boundaries, binary checks and byte budgets continue to apply.

file_search.mode is overview or search. Overview entries include excerpt,
excerpt_start_line and content_truncated. Summaries cite sources and include a
server-generated coverage list; has_more/skipped continue to indicate unexamined
files. This is bounded exploration, not a guarantee that all files were read.
summary_status reports completed, unavailable or no_readable_text. A failed
summary retains retrieved excerpts and reports summary_error. No migration or
new permission is needed. Excerpts are sent to the configured model provider to
produce the summary, and retained in the caller's private conversation history.

## Paginated project-file exploration (API 1.59.0)

After a file overview or literal search, say **next page**, **continue**, or
**more files** in the same conversation, or click **Explore next files** in the
browser page. Each request reads one more bounded page and summarizes its
excerpts. It does not launch unlimited automatic model calls.

`file_search` now includes `page`, `candidates_examined`, `next_cursor` (server
metadata), and `next_page_from`. To continue programmatically, send a new request:

```json
{"request_id":"page-2","text":"Next page","file_page_from":"previous-request-id"}
```

Use the returned next_page_from value. Do not send raw cursor fields, a new
project or query, or conversation_id with this packet. The server loads the
caller-owned saved result, rechecks permissions, and preserves its project and
search terms. The text does not change the operation. Each continuation starts
a fresh conversation ID to keep old excerpts from exhausting context limits;
clients should use the returned ID for subsequent follow-ups. The exploration
retains its original 24-hour expiry. Repeating an identical request ID returns
the same page. A new ID is required for another page.

Ordering is globally README first, overview.md next, then binary directory/name
and file ID. Keyset pagination continues beyond the original 200-file candidate
window, advances past unsupported/skipped files, and resumes at an unexamined
file when a byte budget ends. Each page retains the existing 12-file overview,
30-result search, 200-candidate and byte limits. `has_more: false` with a null
next_page_from marks the end. An all-skipped page may still have a next page.

This is a live view, not an immutable snapshot. Renames/moves or new files can
affect later pages; begin a new exploration to refresh coverage after changes.
Continuation errors include file_page_not_found, file_page_expired, and
no_more_files. No migration or new permission is required.

Pages created before pagination was deployed have no continuation cursor; start a fresh file overview/search to use this feature.

## Filename-only project listing (API 1.60.0)

Ask “List the filenames in Demo” or “What files are available in Demo?” to use
metadata-only listing. It includes all registered file types, recursively within
the selected project's directory and drive. No file bytes are opened or read,
and no content summarization call is made. Metadata remains available for an
offline drive. The filenames and sizes reflect registered database records.

Requires assistant.use, projects.read and files.read. file_search.mode is names;
matches contain file_id, name, path, size_bytes, extension, drive_id and download_url.
bytes_read and files_scanned are zero; entries have no excerpts or content matches.
Twenty files are returned per page. Use the existing next-page button, spoken
continuation or file_page_from API field; continuation preserves names mode and
requires no model call. Content search and overview remain separate choices.
No database migration or new permission is required.

## Directory-scoped project file queries (API 1.61.0)

Examples: “List all files in the docs directory of Demo”, “Include subdirectories”,
“Search Demo's docs/design folder for retries”, and “List only Demo's root files”.
Both metadata listing and content search/overview accept an exact project-relative
folder. A specifically named folder defaults to direct files only; explicitly
request inclusion of subdirectories for recursive results. Whole-project queries
retain their previous recursive default.

The model tool fields are project_directory (nullable string) and recursive
(nullable boolean). They are resolved from the query, not extra top-level REST
request fields. The response echoes file_search.directory (logical absolute path),
relative_directory and recursive. Saved continuation pages preserve all three
scope choices and the project/query; asking for a different scope starts a new
exploration. Paths remain case-sensitive and bound to the project's stored root
and assigned drive. Absolute paths, parent/dot segments, backslashes and control
characters are rejected. A trailing slash is normalized. Missing folders return
project_directory_not_found, distinct from existing folders with zero files, and
never fall back to the full project. No migration or new permission is required.

## Private/local assistant processing (API 1.62.0)

POST `/assistant/requests` accepts an optional boolean `secure`. Use `true` for
local inference; all planning, calendar proposal preparation and file summary
calls use the configured local backend, with no external fallback. Responses and
stored receipts include `secure` and `processing_backend` (`local` or `external`).

```json
{"request_id":"private-001","text":"Summarise the Demo project files","secure":true}
```

Follow-ups and file pagination inherit privacy from their originating conversation
or page. Omit `secure` on these requests. An explicit change in either direction
returns `409 conversation_privacy_conflict`; start a new conversation to change
backend. Legacy conversations without a privacy field are external. Retries must
resubmit the identical packet (including the flag) with the same request ID.

Add `local` inside the existing `assistant` configuration:

```json
"local": {
  "endpoint": "http://192.168.4.30:8080/v1/chat/completions",
  "protocol": "chat_completions",
  "model": "your-local-model"
}
```

Alternatively use `protocol: "responses"` and an endpoint ending in
`/v1/responses`. These are full URLs, not API base URLs. The Chat Completions
adapter sends system/user/assistant messages and strict `response_format` JSON
schema (not tool calls). The Responses adapter sends the existing structured
output schema without an OpenAI reasoning setting. The backend must support that
schema and return valid JSON; plain prose or tool-call output is rejected.
Maximum output is 2,048 tokens; each provider call has a 45-second timeout and a
1 MiB response limit. Model sizing and end-to-end latency need testing on your
actual hardware.

An optional `local.credentials_file` points to a private mode-0600 JSON file
containing `{"api_key":"your-local-server-token"}`. It is independent of the
external provider key; without it no Authorization header is sent locally.
Private files must remain outside shared storage. Paths resolve relative to the
config file. `--check-config` validates settings, not model reachability.

Only literal private IPv4 addresses (RFC1918, loopback or Tailscale/CGNAT
100.64.0.0/10), IPv6 ULA and IPv6 loopback are accepted. DNS names, public
addresses, URL credentials, query strings and redirects are prohibited. Local
calls bypass environment HTTP proxies. HTTPS certificate verification remains
enabled. HTTP is allowed for a trusted LAN or Tailscale path; the `secure` flag
is an inference routing policy, not a promise of HTTP transport encryption.

Set `assistant.require_secure: true` to default new conversations to local and
reject explicit external requests or external conversation continuations with
`403 secure_assistant_required`. External `assistant.credentials_file` may be
omitted when a local backend exists. Without `require_secure`, new unmarked
requests still select external inference and fail if no external key is configured.
A secure request without a configured local backend returns
`503 local_assistant_not_configured`. Runtime local failures produce failed
receipts (or an unavailable summary with locally retrieved excerpts), never an
external retry. `/assistant` and `/assistant/setup` advertise local availability;
`/assistant/explorer` offers a Secure? checkbox locked for a conversation.

Permissions, confirmations, undo and file-read bounds remain unchanged. SPARK
still stores transcripts and retrieved plaintext in its private state directory;
24-hour context expiry does not delete disk history. This feature does not decrypt
files or add per-project restrictions. Use `require_secure` on an instance that
must never send assistant data externally until per-project policies are added.
When away from home, connect the client to the home SPARK instance through
Tailscale. This feature adds no VPS relay or automatic instance switching.

## Preload the local model

Call `POST /assistant/prime` with an empty body or `{}` when opening the assistant
or beginning voice capture. Use the same SPARK authentication as assistant queries
(`assistant.use`; browser sessions require CSRF). Explicit local open access also
works. The endpoint always targets `assistant.local`, never the external provider.

```json
{"status":"ready","secure":true,"processing_backend":"local","model":"gpt-oss:20b","elapsed_ms":1234}
```

SPARK sends a fixed tiny prompt with a minimal output budget, with no user data,
conversation history, tools or stored request receipt. Token-limit completion is
accepted because warming the model does not require a useful answer. The call is
synchronous with a 180-second provider timeout; allow at least 190 seconds on the
client. Do not block microphone capture while waiting. A failed call never falls
back externally. Missing backend: 503 `local_assistant_not_configured`; active
assistant operation: 429 `assistant_busy`. Prime holds the same operation lock as
queries; wait for it to complete or retry after a busy response. Requests with
fields or query parameters are rejected. Retrying is safe and starts another
small inference call; no request ID is required.

Readiness is point-in-time: this does not pin the model in memory or override
Ollama's configured ten-minute inactivity unload. Prime again before use after
idle periods, rather than running a periodic keepalive that defeats unloading.
Ordinary queries retain their existing 45-second per-call timeout.

### Local model output contracts

Chat Completions requests explicitly set `tool_choice: "none"`, use temperature
zero to reduce output variation, and include the
JSON proposal schema in the system instructions as well as `response_format`.
SPARK executes validated JSON proposals itself; native model function calls are
not accepted. A backend returning such a call produces
`unexpected_provider_tool_call`. `incomplete_provider_response` remains the error
for truncated or otherwise unfinished output. Neither error executes the proposed
action or triggers an external fallback. Use a new request ID after correcting
the problem; retrying the identical failed request returns its stored receipt.
