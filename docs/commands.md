# Device commands (API 1.26.0)

SPARK stores work in MariaDB and uses TCP to wake eligible devices. It does not
execute shell commands or device-specific handlers on the server. Clients agree
on action names and input/result formats; there is no capability schema registry.
Action names are case sensitive. Use a new name (for example `myservice.status.v2`)
for an incompatible contract.

## Provisioning

Run `./scripts/setup-db.sh` as the server's OS user to apply
`sql/015_commands.sql` and grant permissions. Rebuild and relaunch SPARK.
The new tables are included in database/file backups. All timestamps use UTC;
MariaDB time is authoritative. The scheduler processes expiry every second and
queue requests also reconcile overdue work.

## Advertise actions

Register the device with the existing POST `/devices/{id}` first, then:

```http
PUT /devices/windows-pc/actions
Content-Type: application/json

{"actions":["myservice.start","myservice.stop","myservice.status"]}
```

GET the same URL to read `{device_id, actions}`. PUT replaces the entire list
atomically; an empty list withdraws everything. At most 128 unique names are
accepted, matching `[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}`. Device deletion removes its
advertisements but retains command history. Withdrawal does not interrupt work
already claimed. `device.actions_updated` is emitted only when the list changes.

After advertising, look for pending work immediately. Previously queued commands
need not generate another notification just because a new handler appeared.

## Submit and inspect

```http
POST /commands
Content-Type: application/json

{
  "source_device":"watch",
  "target_device":null,
  "action":"myservice.status",
  "input":{},
  "idempotency_key":"watch-request-1042",
  "expires_in_seconds":300
}
```

The requester must be registered. A non-null target must also be registered.
Omitting `target_device` is equivalent to null: first eligible claim wins. An
explicit target restricts execution to that device. **Both routes require the
executor to advertise the action.** Work stays queued when no eligible device
is online or advertising it. A target need not currently advertise the action
for the request to be accepted.

`input` is an object, default `{}`. The target validates its action-specific
contents. The expiry is 1..604800 seconds, default 300, and limits how long the
command may wait for a claim. Running work is governed by its lease instead.
JSON bodies are limited to 16 KiB, including results. Larger inputs/results can
reference files stored through the file API.

The required idempotency key matches `[A-Za-z0-9_.:-]{1,128}` and is unique per
requester. Retrying the same normalized packet returns the original command,
including its current status, without extending expiry or emitting duplicate
events. Reusing a key for different target/action/input/expiry returns 409
`idempotency_conflict`. Keys and history are retained indefinitely in this version.

HTTP 202 and a `Location` header mean accepted, not executed. The response contains
`id`, `source_device`, `target_device`, `executing_device`, `action`, `input`,
`idempotency_key`, `expires_in_seconds`, `status`, `result`, `error`, `created_at`,
`updated_at`, `expires_at`, and `lease_expires_at`. Executor, result, error and lease
are initially null. GET `/commands/{id}` reads the same structure.

GET `/commands` lists history newest first, filtering by `source_device`,
`target_device`, `executing_device` and/or `status`. Use `limit` (1..100, default
100) and `offset` (default 0). Filters combine with AND. Explicit target filtering
does not include untargeted work; executor filtering finds the actual winner.
Offset pagination can shift as commands arrive; deduplicate by ID when refreshing.

## Claim, run and report

On `command.available`, after reconnect/registration, and periodically as a
fallback, the device requests:

```http
POST /devices/windows-pc/commands/claim
Content-Type: application/json

{"lease_seconds":60}
```

Without `command_id`, the database atomically chooses one oldest eligible queued
command. To choose work in client priority order, supply its ID instead:

```json
{"command_id":"12345678-1234-1234-1234-123456789abc","lease_seconds":60}
```

This claims **only that command**, with the same action-advertisement and target
restrictions. Multiple devices competing for that ID still produce at most one
winner. HTTP 200 with `{ "command": null }` means the command is nonexistent,
ineligible, expired, already claimed, or otherwise no longer queued. There is no
fallback to another command. Invalid IDs (including null) return HTTP 400
`invalid_command_id`; omit the field to use queue selection. IDs must be lowercase
UUIDs. The winning response is `{ "command": {...} }`.
The command is now `running`, identifies `executing_device`, and includes a random
64-character hexadecimal `claim_token`. Only this response exposes the token;
GET endpoints and TCP events do not. Save the command ID/token durably before
executing. Do not claim more work than the device can execute within its leases.

An ambiguous/lost claim response is **not** reissued automatically: another claim
without an ID may obtain different work; retrying with the already claimed ID
returns `{ "command": null }`, not the token again. The first command will become
`outcome_unknown` if its lease is not renewed/completed. Devices should retain a local execution journal
keyed by command ID; a notification itself never authorizes execution.

Renew before the lease runs out:

```http
POST /commands/{id}/renew
Content-Type: application/json

{"device_id":"windows-pc","claim_token":"<token>","lease_seconds":60}
```

Lease lengths are 1..3600 seconds, default 60, measured from current server time.
The initial queue deadline does not cap renewals. Both the executor ID and token
must match. A renewal emits `command.updated` with no token.

Complete with success:

```http
POST /commands/{id}/complete
Content-Type: application/json

{
  "device_id":"windows-pc",
  "claim_token":"<token>",
  "status":"succeeded",
  "result":{"state":"running","observed_at":"2026-10-06T08:30:00Z"}
}
```

Or `status: "failed"` and `error: {"code":"unsupported_action","message":"..."}`.
A failure requires an error object with a code matching `[A-Za-z0-9_.:-]{1,100}`;
it may include message/details. Failed results must be null. On success, result
may be any JSON (default null), and error must be absent or null. Identical
completion retries succeed without another event. Different terminal results
return 409 `completion_conflict`.

For service control, document whether success means the start request was accepted
or the service was actually observed running. `myservice.status` uses this same
command/result mechanism; no cached reported-state API is included yet.

## States and interruption

- `queued` → `running` when claimed, `expired` when its queue deadline passes, or
  `cancelled` when its requester cancels it.
- `running` → `succeeded` or `failed` when completed with a valid live claim.
- `running` → `outcome_unknown` when its lease expires.

A requester cancels via POST `/commands/{id}/cancel` with
`{"source_device":"watch"}`. Cancellation only applies to queued work; repeating
an already successful cancellation is idempotent. Running work cannot be cancelled
in this version.

Expired execution leases **never** automatically requeue, retry or transfer work.
The device might have executed a side effect before disconnecting. Late renewals
and completions return 409, and the last known executor remains recorded. Inspect
its actual state before deliberately submitting a new command with a new key.
Exactly-once physical execution is not guaranteed. Server restart preserves active
leases; a device with its saved token may renew/complete while the lease remains
valid. Dashboard maintenance does not freeze lease or queue deadlines.
Restoring an older backup can restore commands that executed after that snapshot.
Reconcile queued work before reconnecting executors, and preserve device execution
journals across server restores to avoid repeating side effects.

## Notifications and reconnect

Command changes and their outbox events commit in the same transaction:

- `command.available`: sent only to devices currently advertising the action and
  matching an explicit target, if present. Contains action, resource, status and
  non-null routing IDs, alongside normal event ID/sequence/source fields.
- `command.updated`: sent to the requester, explicit target and actual executor.
  Read the command resource for full current state and result.

Command notifications contain no input, result, error or claim token. Normal TCP
protocol-2 replay/checkpoints apply, including cursor advancement over filtered
notifications. Availability routing uses advertisements at delivery/replay time.
A replayed availability event may be stale: use the claim endpoint, optionally
with the ID from its `/commands/{id}` resource, and execute only a returned claim. Competing eligible devices can be notified, but
only one receives a claim.

After reconnect, refresh paginated `/commands?source_device={id}` and
`/commands?executing_device={id}` for outstanding results/execution records, restore
saved local claims, and claim pending work up to local capacity. A new or previously
unsupported action can become claimable without replaying a historical event.

The existing no-auth model remains: IDs are self-reported, and anyone with REST
access can register/claim as a device or read command inputs/results. Claim tokens
prevent accidental stale completion; they are not a substitute for authentication.
A single queue lock serializes transitions for predictable PoC correctness.

## Validation

`./scripts/test-local-db.sh` runs the real command REST/TCP integration against a
private MariaDB, including competing claims, idempotent retries, notification
filtering/replay, rollback, lease loss, queue expiry and post-restart persistence.

## Command explorer (API 1.27.0)

Open `/commands/explorer` (also linked from `/`) to browse devices exposing actions.
**Run once** opens a dialog with an input JSON object (default `{}`) and queue expiry
(default 300 seconds). Confirmation submits one targeted command to that device.
The page polls for queued/running status, executor, result and error. It does not
claim or execute the command itself: the device must have a running command client.
Advertisements remain listed while a device is offline; queued work can expire.

The browser registers a requester ID `explorer-<random hex>` on its first run and
keeps it in local storage when available. Its placeholder `local_ip: "0.0.0.0"`
means the browser's local address is unknown. It advertises no actions and does
not appear in the explorer's device list. Run history displayed on the page lasts
until reload; command records remain in MariaDB and can be queried by source ID.
An unconfirmed submission offers **Retry submission** using the same immutable
packet and idempotency key. Selecting Run once again creates a distinct request.

GET `/commands/devices` provides the explorer's discovery data:

```json
{
  "devices":[{
    "id":"windows-pc",
    "local_ip":"192.168.4.10",
    "external_ip":null,
    "actions":["myservice.start","myservice.status","myservice.stop"]
  }],
  "limit":100,
  "offset":0
}
```

Only registered devices with nonempty action lists are returned, ordered by ID;
action names are sorted. `limit` accepts 1..100 (default 100); `offset` accepts
0..1000000000 (default 0). Unknown or duplicate query parameters return 400.
