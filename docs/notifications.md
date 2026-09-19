# TCP notifications: protocol version 1

SPARK listens for raw TCP connections on port **47684** by default. REST remains on
47683. Override the TCP port with `--notification-port PORT`; `--host ADDRESS`
controls both listeners. `/api.notifications` advertises the configured port and
protocol. Connect to the server's address (for example `192.168.4.20`), not a device
address. This is raw TCP, not WebSocket, HTTP, or TLS. There is no authentication.

## Client startup

1. Open a persistent TCP connection to SPARK.
2. Within 10 seconds, send a hello followed by a newline:

   ```json
   {"type":"hello","device_id":"watch-1"}
   ```

3. Wait for welcome:

   ```json
   {"type":"welcome","protocol_version":1,"device_id":"watch-1","resync_required":true,"resources":["/timers","/pulse","/events","/daily-tasks"],"heartbeat_interval_seconds":30,"idle_timeout_seconds":90}
   ```

4. Fetch `GET /timers`, `GET /pulse`, all pages of `GET /events`, and `GET /daily-tasks` over REST. Keep reading the TCP stream during this fetch.
   If an event arrives while a refresh is in flight, schedule another refresh after
   it completes. This avoids overwriting newer state with an older HTTP response.
5. Process notifications and heartbeats until disconnected.

The device ID uses the REST registry's syntax: 1–64 ASCII letters, digits,
underscores or hyphens. It identifies the connection but is not an authentication
credential; device registration is not required. Multiple connections may use the
same ID, and all receive timer, calendar, and daily-task events, including events initiated by that device. Pulse
events require a persisted subscription for that ID.
Hello accepts exactly `type` and `device_id`. Manage pulse subscriptions through
REST: `POST /pulse/subscriptions/{device_id}` or
`DELETE /pulse/subscriptions/{device_id}`. There is no resume cursor, event
acknowledgement, or action command in the TCP protocol.

## Framing

Messages are UTF-8 JSON objects terminated by `\n`. CRLF is also accepted on input.
A socket read can contain part of a message, multiple messages, or both: accumulate
bytes, extract complete newline-terminated frames, and retain the unfinished tail.
Do not pretty-print JSON across lines. Client frames may contain at most 4096 bytes
before the newline. Ping/pong messages contain only their `type` field.

## Events

```json
{"sequence":42,"event_id":123,"type":"timer.created","resource":"/timers/12345678-1234-1234-1234-123456789abc","source_device":"watch-1"}
```

| Field | Meaning |
| --- | --- |
| `sequence` | Durable notification dispatch order, distinct from event ID |
| `event_id` | Identity of the committed domain event |
| `type` | `timer.created`, `timer.expired`, `timer.confirmed`, `timer.stopped`, `pulse.updated`, `pulse.triggered`, `calendar.created`, `calendar.updated`, `calendar.deleted`, `daily_tasks.updated`, or `daily_tasks.reset` |
| `resource` | REST path to refresh |
| `source_device` | Device that performed the action, or null for expiry/unattributed actions |

These messages are hints to fetch current state, not state snapshots. Fetch the
resource or refresh `/timers` for an active-timer UI. Pulse events use `/pulse`; only
`pulse.triggered` is a reminder (see [pulse.md](pulse.md)). Confirmed/stopped timers remain
queryable by ID but disappear from the active list. Future services can emit other
event types using the same durable `events` table; ignore unknown types/fields that
your client does not use. Device registry history is not currently broadcast.

Only committed events are eligible. SPARK polls for them about every 250 ms and
records each event in `notification_log` under a transactional dispatcher lock.
That produces a safe dispatch sequence even when a lower event ID commits after
a higher ID. Multiple SPARK processes can sequence events without duplicates, and
each listener delivers the resulting stream to its own clients. Pulse events are
filtered to the device IDs subscribed at dispatch time; timer, calendar, and daily-task events go to all
clients. Subscription changes affect existing connections, but already queued
frames may still arrive after unsubscribe. Sequence numbers
can have gaps; do not assume contiguous numbering or use `event_id` as a cursor.
Older unsequenced events may arrive as the dispatcher catches up; always fetch
current REST state rather than applying an event as a state mutation.

## Heartbeats and reconnects

SPARK sends `{"type":"ping"}` every 30 seconds. Reply with
`{"type":"pong"}` followed by a newline. A client can also send ping and receive
pong. A connection with no valid client message for 90 seconds is closed, even if
SPARK is sending events to it. Clients should also detect missing server traffic
and reconnect instead of waiting indefinitely on a broken connection.

On EOF, socket error, heartbeat failure, or uncertain stream state:

1. Reconnect using exponential backoff with jitter, capped at 30 seconds.
2. Send hello again and wait for welcome.
3. Refresh `/timers`, `/pulse`, all pages of `/events`, and `/daily-tasks`, then continue handling events.

Offline events are not replayed to a client and delivery is not acknowledged.
Refresh after **every** welcome, even when the same process/server is still running.
Clients should also refresh after resuming from sleep. Event IDs can be used for
local deduplication, but the reconnect contract is state resynchronization, not
exactly-once delivery. Failed REST refreshes need retries with backoff.

## Limits and errors

Up to 128 simultaneous connections are accepted. Excess connections are closed.
Each connection has a 256 KiB outgoing application buffer. Slow consumers are
closed when this limit is reached; they recover with the reconnect/resync flow.
The shared publisher queue is also bounded; overflow closes clients to force
resynchronization. Database polling runs separately from network I/O. A database
polling failure is logged, existing clients are disconnected to prompt a refresh,
and polling retries automatically. REST may report 503 during the same outage.

Malformed messages receive an error frame when possible, followed by disconnect:

```json
{"type":"error","error":"invalid_hello"}
```

Error codes: `invalid_message`, `invalid_hello`, `unsupported_message`, and
`frame_too_large`. Silent connections, heartbeat expiry, capacity limits, and
network failures can close without an error frame.

## Setup and manual check

Stop the old SPARK process, run `./scripts/setup-db.sh`, and start the new binary.
The setup adds `sql/003_notifications.sql` and runtime grants without deleting
existing devices, timers, or events. SPARK checks the schema before opening its
listeners and fails startup if either requested port cannot be bound.

For a manual check, run `nc 192.168.4.20 47684` (netcat), paste the hello line, then
create a timer over REST from another terminal. Reply to ping lines with pong.
The automated `./scripts/test-local-db.sh` suite uses a private MariaDB instance
and covers the TCP/REST integration without changing the live database.

Calendar reminders use `calendar.reminder` and include `occurrence_at` (UTC),
identifying the specific instance of a one-off or recurring event. They broadcast
to all connected devices; fetch the `/events/{id}` resource for its definition.
See [event recurrence and reminders](events.md) for scheduling and restart behaviour.
