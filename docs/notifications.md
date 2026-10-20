> For remote access, use [TCP over TLS and public endpoint discovery](remote-deployment.md). The native loopback/LAN listener described below remains plain TCP.

# TCP notifications: protocol version 2

SPARK listens on raw TCP port **47684** (REST: 47683). `--notification-port`
overrides the port; `--host` controls both listeners. `/api.notifications` describes
the protocol. Messages are UTF-8 JSON objects terminated by a newline, not HTTP
or WebSocket. Remote profiles wrap this protocol in TLS at the proxy; the backend
listener itself is plaintext. Authentication is optional by instance configuration. In token mode,
add `"token":"<device-token>"` to every hello, including reconnects. The token must
match `device_id` (or explicitly delegate), grant `notifications.read`, and grant
read access to each resource family delivered. Live and replayed events are filtered
identically; welcome resources are permission-filtered. Revoked/changed credentials
disconnect active sessions. See [authentication protocol](identity-permissions.md).
The examples below omit the token for open-mode instances.

Protocol 2 adds opt-in, durable reconnect catch-up. Existing two-field hello
messages still receive a protocol-1 welcome and use the REST resync flow.

## First connection or lost local state

Send within 10 seconds of connecting:

```json
{"type":"hello","device_id":"watch-1"}
```

The response includes a safe starting boundary:

```json
{"type":"welcome","protocol_version":1,"device_id":"watch-1","resync_required":true,"sequence":42,"resources":["/timers","/pulse","/events","/daily-tasks","/projects","/files","/files/directories","/devices/watch-1"],"heartbeat_interval_seconds":30,"idle_timeout_seconds":90}
```

Refresh the resources through REST, following pagination and fetching project
board lists and boards in use. Refresh file lists and directories too. The welcome
includes `/devices/{your_device_id}`; 404 is normal for an unregistered device.
There is no device-list endpoint: refresh other device IDs you already cache.
Keep reading TCP while refreshing. Queue incoming
hints and refresh again if an event arrives during an in-flight REST request.
Only after the full refresh succeeds may you durably save the welcome boundary;
then process queued notifications in order and advance the cursor as they succeed.
Do not let an older refresh overwrite newer local state.

The device ID accepts 1–64 ASCII letters, digits, underscores or hyphens. It is
case-sensitive, not an authentication credential, and registration is not required.
Multiple connections can share an ID and each has its own delivery cursor.

## Reconnect with catch-up

Persist the last **successfully processed sequence**, then send it on reconnect:

```json
{"type":"hello","device_id":"watch-1","after_sequence":42}
```

`after_sequence` must be an unsigned 64-bit JSON integer (0 through
18446744073709551615). Zero requests all retained history. Negative numbers,
floats, strings, booleans, null, overflow and unknown hello fields are rejected.
Use an integer-capable JSON implementation; do not round large cursors through
floating point. **Never use event_id as the cursor.**

With a valid cursor the server responds:

```json
{"type":"welcome","protocol_version":2,"device_id":"watch-1","resync_required":false,"sequence":55,"resources":["/timers","/pulse","/events","/daily-tasks","/projects","/files","/files/directories","/devices/watch-1"],"heartbeat_interval_seconds":30,"idle_timeout_seconds":90}
```

It sends eligible notifications strictly after the supplied cursor, in ascending
sequence order, through the welcome boundary. It then sends:

```json
{"type":"catchup_complete","sequence":55}
```

Live notifications follow on the same connection without a separate subscription
or a replay/live delivery gap. Heartbeats can appear throughout catch-up.
The boundary refers to the durable notification log at handshake processing time;
committed events not yet sequenced, including late commits, are delivered later.
If no events were missed, completion is immediate after welcome.

The server also sends progress markers:

```json
{"type":"checkpoint","sequence":60}
```

These allow progress past pulse notifications filtered out by subscription.
`checkpoint` and `catchup_complete` are safe cursor positions **only after all
preceding notifications have been processed successfully**. Welcome.sequence is
an advertised boundary, not permission to skip the replay. If processing uses
parallel REST requests, do not save a later cursor while earlier work is pending.
Failed REST requests must be retried before advancing past their notifications.

If the cursor is ahead of the database, welcome has `resync_required:true` and the
current sequence instead. Perform the full initial REST refresh and reset the
cursor to that boundary before applying subsequent progress. Also omit the cursor
when replacing/restoring a database or when client state has been lost; a sequence
alone cannot identify a different database history.

Delivery is at least once across reconnects: disconnecting before saving progress
can replay the same event. Use `event_id` for deduplication or make refresh handling
idempotent. Cursors are held by clients, not stored per device on the server. There
is no delivery acknowledgement or user-action acknowledgement in this protocol.
Notification history currently has no automatic expiry/pruning. Manual deletion
of history is unsupported; clients must fully resync if that history is removed.

## Notification contents and filtering

```json
{"sequence":43,"event_id":123,"type":"timer.created","resource":"/timers/12345678-1234-1234-1234-123456789abc","source_device":"watch-1"}
```

| Field | Meaning |
| --- | --- |
| `sequence` | Durable dispatch order and reconnect cursor; gaps are normal |
| `event_id` | Committed domain-event identity, not commit order |
| `type` | Event kind; ignore types not relevant to your client |
| `resource` | REST resource to refresh; deletion can mean it now returns 404 |
| `source_device` | Originating device, or null for unattributed/server actions |

Timer (`created`, `expired`, `confirmed`, `stopped`), calendar (`created`, `updated`,
`deleted`, `reminder`), daily-task (`updated`, `reset`) and board (`created`,
`updated`, `deleted`) notifications go to all devices, including the actor.
Prefixes are `timer.`, `calendar.`, `daily_tasks.` and `board.` respectively.

Pulse events (`pulse.updated`, `pulse.triggered`) go only to subscribed device IDs.
Use `POST /pulse/subscriptions/{device_id}` and DELETE on the same path. Membership
is checked when reading a delivery page; **current subscription applies to replay
as well as live delivery**. A currently unsubscribed client does not get historic
pulse events; a currently subscribed client can get pulses from before it subscribed.
Already queued frames may still arrive after unsubscribe. Changing subscription
should also refresh `/pulse` through REST.

These events are hints to fetch **current state**, not state snapshots. Historical
`pulse.triggered`, `timer.expired` or `calendar.reminder` events should not blindly
produce fresh alerts: use current REST state and the client's reminder policy.
Calendar reminders include `occurrence_at` (UTC), identifying the event instance;
see [events.md](events.md). Board resources identify the full project/board path;
see [boards.md](boards.md). File, directory, project and device changes are also covered, as listed below.

Only committed events enter `notification_log`. A transactional dispatcher lock
assigns sequence numbers safely even when a smaller event ID commits later. Both
history and live delivery read that log; nothing relies on an in-memory broadcast
queue surviving a restart. Multiple listeners can read the same log safely.

## Framing, heartbeats and limits

Accumulate socket bytes and extract complete newline-terminated JSON objects;
reads can contain partial or multiple frames. Input CRLF is also accepted. Do not
pretty-print JSON across lines. Maximum incoming frame size is 4096 bytes.

The server sends `{"type":"ping"}` every 30 seconds; reply with
`{"type":"pong"}` and a newline. Client ping receives pong. Ping/pong have no
other fields. A client silent for 90 seconds is disconnected even while receiving
history. Detect missing server traffic too. On EOF, errors or heartbeat failure,
reconnect with exponential backoff and jitter capped at 30 seconds, using your
last durably processed sequence.

There are at most 128 connections and a 256 KiB per-client output limit. Database
pages contain at most 256 events and delivery pages at most 64 KiB. Slow readers
pause page requests as their output fills; history is not loaded all at once.
Database polling runs separately from socket I/O, approximately every 250 ms.
A database failure disconnects sessions; clients retry from their saved cursors
when service recovers. REST may also return 503 during an outage.

Malformed input receives an error frame where possible, then disconnects:

```json
{"type":"error","error":"invalid_hello"}
```

Errors: `invalid_message`, `invalid_hello`, `unsupported_message`,
`frame_too_large`. Capacity, timeout and network failures can close silently.

## Activation and checks

Rebuild and restart SPARK. Catch-up uses the existing `notification_log` and
`events` tables; no new migration or grants are required. Legacy clients continue
to work unchanged; update clients to send `after_sequence` to enable catch-up.

For a manual check, connect with `nc 192.168.4.20 47684`, send hello and record a
processed notification's sequence. Disconnect, perform actions over REST, then
reconnect with that sequence. Reply to ping frames throughout.

`./scripts/test-local-db.sh` exercises the protocol with an isolated MariaDB
instance, including multi-page replay, replay/live handoff, subscription filtering,
invalid cursors, legacy clients, duplicate retries, outages and process restarts.

## Additional change coverage (API 1.23.0)

| Type | Resource | Client action |
| --- | --- | --- |
| `file.created` | `/files/{id}` | Fetch completed upload metadata; refresh file lists |
| `file.updated` | `/files/{id}` | Refresh metadata, content and cached directory lists; covers moves and text saves |
| `file.deleted` | `/files/{id}` | Remove cached file; REST now returns 404; refresh lists |
| `files.directories_updated` | `/files/directories` | Reload directory tree, including automatically created parents |
| `project.created` | `/projects/{id}` | Fetch project and refresh project lists |
| `project.updated` | `/projects/{id}` | Refresh project/list/tree; includes immediate-parent and owning-project changes |
| `project.deleted` | `/projects/{id}` | Remove project and invalidate cached deep trees |
| `device.created` | `/devices/{id}` | Fetch new registered device |
| `device.updated` | `/devices/{id}` | Refresh its IP details |
| `device.deleted` | `/devices/{id}` | Remove cached registration |
| `device.subscription_updated` | `/devices/{id}` | Refresh subscription state; packet also includes boolean `subscribed` |

These all broadcast to every TCP client and are replayable. `source_device` is null:
a changed device ID identifies the affected record, not an authenticated actor.
The subscription event deliberately uses the `device.` prefix, so unsubscribing
can be announced even to devices that no longer receive `pulse.*` notifications:

```json
{"sequence":61,"event_id":200,"type":"device.subscription_updated","resource":"/devices/watch-1","source_device":null,"subscribed":false}
```

Subscription identities need not be registered: `/devices/{id}` can return 404
while a subscription still exists. Device deletion does not remove the independent
pulse subscription. Subscription packets describe the state at that event;
process them in sequence order and do not mistake replayed state for a later
change. Repeated subscribe/unsubscribe requests emit no new event.

All notifications are written in the transaction that makes their state change
visible. Failures roll back both. File-created events wait for completed durable
uploads; deletes mean logical removal, not completion of physical cleanup.
Recursive project deletion includes descendant project, board and file events;
retained files get no deletion event. Recovery cleanup emits no duplicate events.
No events are retroactively generated for changes made before this upgrade.

A browser page cannot directly use this raw TCP channel. SPARK's HTML explorers
continue to use their existing refresh controls; native clients can subscribe to
these hints. No new migration is needed; rebuild and restart SPARK.

## Commands (API 1.25.0)

`command.available` is filtered to devices advertising the requested action and
matching its optional target. `command.updated` is delivered to source, explicit
target and executor. Routing fields/action/status may accompany the notification;
inputs, results and claim tokens are never sent. `device.actions_updated` announces
advertisement changes. See [commands](commands.md) for atomic claim, lease, result,
expiry and reconnect semantics. Refresh requester/executor history and check the
claim endpoint after reconnect, after registering handlers and periodically;
notification receipt alone never grants execution permission.

`drives.updated` (`resource: /files/drives`) announces committed drive registration,
path changes and explicit detachment. Fetch GET /files/drives for letters and current
availability. Unexpected unplugging is detected during storage access/status reads,
not by an OS hotplug monitor, so clients should refresh drive status after I/O errors.

`files.directories_updated` now includes top-level `drive_id`. Resolve its letter using
GET /files/drives and refresh `/files/directories?drive=<letter>`; directory trees
are independent per drive.
Older replayed directory events may omit `drive_id`; refresh every registered
drive tree in that case. Full reconnect resync should also enumerate all drives.

Calendar notifications (`calendar.created`, `calendar.updated`, `calendar.deleted`,
and `calendar.reminder`) include `calendar: "local" | "shared"`. Legacy replay
records without that field are treated as local. Calendar selection does not
change notification permissions or subscriptions.

An event move emits `calendar.updated` with `calendar` (destination) and
`previous_calendar` (source). Refresh both calendars; the event UUID remains
unchanged. A move to its current calendar emits no notification.

Shared-calendar sync imports use the same calendar.created/updated/deleted
notifications as ordinary changes. Imported data always has calendar=shared.
An unchanged or retried import emits no duplicate notification. Reminder
delivery markers remain local to each instance. See [calendar sync](calendar-sync.md).
