# Shared pulse

The pulse is one recurring reminder shared by all SPARK devices. The core stores
and times the schedule, then sends `pulse.triggered` to subscribed device IDs through the existing TCP
notification channel on port 47684. Clients decide what to display, play, vibrate,
or require the user to do. SPARK does not prescribe a habit or require acknowledgement.

The initial interval is **3600 seconds**, and the pulse is **disabled** until enabled.

## REST

`GET /pulse` returns the shared state without triggering a reminder:

```json
{
  "enabled": false,
  "interval_seconds": 3600,
  "next_pulse_at": null,
  "last_pulse_at": null,
  "pulse_count": 0,
  "updated_at": "2026-09-18T00:00:00.000000Z"
}
```

Use `PATCH /pulse` with `Content-Type: application/json` to change settings:

```sh
# Enable a reminder every 30 minutes.
curl -X PATCH http://192.168.4.20:47683/pulse \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"interval_seconds":1800,"source_device":"phone-1"}'

# Disable it for all devices.
curl -X PATCH http://192.168.4.20:47683/pulse \
  -H 'Content-Type: application/json' -d '{"enabled":false}'
```

PATCH requires `enabled` (boolean) and/or `interval_seconds` (integer 1–604800).
The optional `source_device` uses the existing device-ID syntax and is attribution
metadata, not authentication. Unknown fields are rejected. Both endpoints return
200 with the current state. Validation errors return 400, incorrect content type
returns 415, and database failures return 503 using the usual JSON error packet.
HEAD and browser OPTIONS are supported. `/api` documents both endpoints.

Enabling schedules the first pulse one full interval later. A changed interval
restarts the countdown from that update; sending identical values preserves it.
Disabling clears `next_pulse_at` and preserves the most recent pulse and count.
Re-enabling starts a new full interval. There is no immediate manual pulse endpoint.

## Subscriptions

Devices are **unsubscribed by default**, including existing clients after this
upgrade. Subscribe using the exact case-sensitive device ID from TCP `hello`:

```sh
curl -X POST http://192.168.4.20:47683/pulse/subscriptions/watch-1
curl -X DELETE http://192.168.4.20:47683/pulse/subscriptions/watch-1
```

Neither endpoint takes a body or requires headers. Both return 200:
`{"device_id":"watch-1","subscribed":true}` for POST, or `subscribed:false` for
DELETE. Repeating either operation succeeds; unsubscribing an ID that was never
subscribed also succeeds. IDs must be 1–64 ASCII letters, digits, underscores or
hyphens. Invalid IDs and nonempty bodies return 400; database errors return 503.
OPTIONS advertises `POST, DELETE, OPTIONS`. Registration is not required, matching
the existing TCP identity rules. These endpoints remain unauthenticated.

`GET /devices/{id}` includes `subscribed: true` or `false` for registered devices.
A subscription alone does not create a registry record; missing devices still
return 404.

Subscriptions persist in MariaDB across socket reconnects and server/database
restarts. All connections using a subscribed ID receive its pulse events. The
recipient set is captured when events are dispatched: changes affect subsequent
dispatches without reconnecting, but already queued/in-flight frames cannot be
recalled. Both `pulse.triggered` and `pulse.updated` are restricted to subscribers.
Timer events remain available to every connected device.

Subscribing does not enable or reset the global schedule; enable it separately
with `PATCH /pulse`. Unsubscribing does not disable the schedule for other devices.
`GET /pulse` remains available regardless of subscription. Subscriptions use TCP
identity IDs and are independent of registry record deletion.

## TCP delivery

A scheduled pulse is sent to subscribed devices:

```json
{"sequence":42,"event_id":123,"type":"pulse.triggered","resource":"/pulse","source_device":null}
```

Fetch `GET /pulse` for the latest `pulse_count` and `last_pulse_at`. The count
increases once per generated pulse and never resets when disabling/re-enabling.
Clients can use it to avoid showing the same pulse more than once. Each device
controls its own presentation, dismissal, and stale-reminder policy. There is no
core acknowledgement endpoint for pulses, unlike timers.

`pulse.updated` indicates changed schedule settings. It is not a reminder and
should not cause an intrusive prompt. Both events are hints to refresh `/pulse`;
TCP event envelopes do not include the state snapshot.

TCP welcome now lists both `/timers` and `/pulse` for resynchronization. Fetch them
after reconnecting; do not treat a settings refresh itself as a new pulse. Offline
notifications are not replayed. Delayed/unsequenced events may arrive after a
recovery, so clients should check the latest timestamp/count before presenting a
reminder. See [notifications.md](notifications.md) for framing and heartbeats.

## Timing and reliability

The schedule uses MariaDB UTC deadlines, checked about once per second. The next
deadline and pulse event commit atomically; a failed event insert cannot consume a
pulse. Concurrent scheduler instances lock the shared row, preventing duplicate
pulses for the same deadline. Database failures are logged and retried.

Settings, deadlines, and pulse count persist across restarts. If the server was
away for several intervals, it generates **one** overdue pulse on recovery and
schedules the next one a full interval after that. It never emits one reminder for
every missed interval. This is a recurring delay from actual emission, not a
wall-clock calendar schedule; delays can shift the cadence.

Disabling prevents future generation but cannot recall a pulse already committed
or in flight. A client can check `enabled` before displaying delayed notifications.

## Upgrade

Stop SPARK, run `./scripts/setup-db.sh`, and restart `./build/spark_server`.
The additive `sql/004_pulse.sql` and `sql/005_pulse_subscriptions.sql` schemas initialize the shared settings only when
missing; re-running setup preserves existing pulse configuration and subscriptions. Server
version is 0.9.0 and API version is 1.8.0. HTTP/TCP ports are unchanged.
