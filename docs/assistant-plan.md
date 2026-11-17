# SPARK assistant implementation plan

Status: phase 1 query harness implemented using OpenAI Responses and GPT-6 Luna.
See [harness setup](assistant.md). Timer creation/listing and transactional action history/undo are implemented.
Caller-scoped 24-hour conversations, calendar occurrence reads, one-off event creation/undo and GET /assistant/setup are implemented.
Calendar edit/removal previews, five-minute confirmation, durable spoken decisions and edit/deletion undo are implemented (migration 026).
Remaining tools and asynchronous processing remain pending; this
first harness uses private request files, synchronous queries and polling.

## Goals and boundaries

Accept phone transcripts produced by Parakeet. SPARK owns authentication,
authorization, validation, execution, confirmations, durable history and undo.
A replaceable external model proposes structured tool calls, never executable code.
The phone receives a stable SPARK response contract rather than provider responses.

Initial tools cover timers, calendar reads/creation/modification, daily-task
reads/checking, and bounded text-file reads. Exclude file mutations, unrestricted
device commands, deletion tools, bulk mutations, autonomous execution and
cross-instance routing. Existing Google browser login is not phone API auth.

## Phase 1: Contract and provider adapter

- Version requests and responses and document them in /api and /api/docs.
- Accept a unique retry ID, transcript, optional conversation ID and timezone;
  derive identity from authentication.
- States: processing, needs_clarification, needs_confirmation,
  waiting_for_device, completed, failed.
- Add disabled-by-default assistant configuration, model selection, private API
  credentials and bounded latency, input size and tool-call budgets.
- Implement one hosted provider after provider selection; independently validate
  structured proposals and fail closed on malformed output or provider failure.
- Require assistant-use permission plus each underlying resource permission.
  The phase-1 local query harness also supports explicit allow_open_access for
  open-mode testing; --remote rejects this opt-in.

## Phase 2: Shared action history and undo

- Persist caller, request, operation, resources, before/after state, outcome and
  undo availability. Build this for reuse; enable it for assistant actions first.
- Atomically commit database mutations, history and notification events.
- Deduplicate requests and actions; reconcile interrupted work without repeating
  mutations blindly.
- Provide authorized inspection and undo. Recheck permissions and detect later
  edits before reversal. Undo produces ordinary notifications and sync changes.
- Default undo retention: configurable 24 hours. Already-delivered alarms cannot
  be retracted. File recovery snapshots are deferred.

## Phase 3: Initial tools

- Use existing timer, calendar and daily-task services without bypassing REST's
  authorization rules through direct store access.
- Resolve real resource IDs; clarify ambiguous targets and missing arguments.
- Interpret dates using request timezone or configured default; explicitly select
  local/shared calendar using request intent or configured default.
- Support one mutation per request initially. Generate routine outcome text from
  verified service results, with action IDs and undo information.

## Phase 4: Conversation and phone integration

- Bind conversations to authenticated callers; default retention to 24 hours.
- Store exact pending operations for confirmation, with expiry and revalidation.
- Return durable request IDs promptly. REST retrieves authoritative state; filtered
  TCP notifications announce updates, including reconnect catch-up.
- Document retries, polling fallback and structured clarification/confirmation
  replies. Never equate queued device work with completed execution.

## Phase 5: Read-only files

- Locate and read authorized text files with strict content bounds and format
  checks; return source references.
- File contents are untrusted data, never authority to run tools. Send only
  relevant content to the provider and document this disclosure.
- Reject offline, inaccessible, binary and oversized content clearly. No indirect
  file mutation through command tools.

## Phase 6: Verification and rollout

- Test schemas, permission denial, provider failures, malformed proposals,
  duplicate submissions, restart interruption and conflicting undo.
- Maintain transcript fixtures covering dates near Perth midnight, duplicate
  names, corrections, clarification and expired confirmation.
- Evaluate the selected live model before enabling mutations in production.
- Log IDs, timing and outcomes without secrets, transcripts or file contents.
- Deploy disabled first; test on the isolated instance before production.
- Document configuration, permissions, retention and recovery. Do not restart or
  push production instances without a deployment request.

## Deferred work

Device action descriptions/schemas/reversal contracts; private file recovery
snapshots and mutation tools; bulk and multi-step actions; inter-instance routing;
autonomy; local inference and optional Laya routing.

## Pending local model integration — 2026-11-16

The owner is setting up **Ollama** at API base URL
`http://192.168.4.60:11434/v1`. Setup is not complete; model name and further
configuration details will be provided later. Do not enable or assume readiness
of this backend yet.

The SPARK local adapter expects a full endpoint, so the intended Chat Completions
endpoint is `http://192.168.4.60:11434/v1/chat/completions` with
`protocol: "chat_completions"`. Once setup is complete, confirm the model name,
authentication requirements and structured JSON-schema compatibility, then test
the secure assistant flow. This note does not change runtime configuration.

### Connectivity confirmed — 2026-11-16

Owner supplied model `gpt-oss:20b`. A direct POST from this machine to
`http://192.168.4.60:11434/v1/chat/completions` returned HTTP 200 in about 0.56 seconds,
with assistant content `SPARK connection successful` and finish reason `stop`.
Basic reachability and generation are verified. Structured JSON-schema responses
and the complete SPARK secure assistant workflow still need verification; runtime
configuration has not been changed.

Ollama is configured by the owner to unload after ten minutes of inactivity.
`POST /assistant/prime` provides on-demand warming without overriding that policy.

Prime endpoint verified against Ollama on 2026-11-16 using an isolated HTTP
harness (no running instance changed): HTTP 200,
`{"elapsed_ms":5445,"model":"gpt-oss:20b","processing_backend":"local","secure":true,"status":"ready"}`.
This verifies on-demand warming; it does not establish that the model was fully
unloaded before the test or validate structured assistant planning.

### Ollama timer proposal compatibility — 2026-11-17

Reproduced the reported `incomplete_provider_response`: gpt-oss:20b returned
`finish_reason: tool_calls` and a native `timer.create_timer` call instead of the
required JSON proposal. The Chat Completions adapter now sends `tool_choice: none`,
explicit JSON-only instructions including the schema, and temperature zero.
Native calls are rejected as `unexpected_provider_tool_call`; truncation remains
`incomplete_provider_response`. No arbitrary tool-call translation or execution
was added. One exploratory run produced another invalid response and was safely
rejected; subsequent final-build tests twice created a 600-second timer and undid
it successfully in fresh disposable databases. Mock response/error tests passed.
The home instance was not restarted and its real timer data was not changed.
