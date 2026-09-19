# Dated events

User-facing events are stored in `calendar_events`, separately from SPARK's
internal `events` notification log. Each has a name, description, date/time, and
optional place. Events can be one-off or recur weekly, with optional TCP reminders
before each occurrence. Events do not have a duration.

## Endpoints

| Method/path | Behavior |
| --- | --- |
| `POST /events` | Create; 201 plus record and Location header |
| `PATCH /events/{id}` | Modify supplied fields; 200 with updated record |
| `DELETE /events/{id}` | Delete; 204 with no body |
| `GET /events/{id}` | Retrieve one record; 200 |
| `GET /events` | Filtered, paginated collection; 200 |

Missing UUIDs return `404 {"error":"event_not_found"}`. Malformed IDs/input/filter
values return 400, wrong JSON content types return 415, and database failures return
503. Read routes support HEAD; all routes support OPTIONS. No authentication is
required. `/api` provides descriptions, request/response packets, and field rules.

## Create or modify

```sh
curl -X POST http://192.168.4.20:47683/events \
  -H 'Content-Type: application/json' \
  -d '{"name":"Walk","description":"Meet by the gate","occurs_at":"2026-09-19T09:00:00+08:00","place":"Park","source_device":"phone-1"}'
```

Creation requires `name`, `description`, and `occurs_at`. `name` must be nonblank
and at most 200 UTF-8 bytes. `description` can be empty, up to 4000 UTF-8 bytes.
`place` is optional: null or a nonblank string up to 200 UTF-8 bytes. The existing
16 KiB limit applies to the whole encoded request body. Optional `source_device`
provides mutation attribution and uses the existing device-ID syntax.

`occurs_at` accepts `YYYY-MM-DDTHH:MM:SSZ` or an explicit offset such as
`2026-09-19T09:00:00+08:00`. Seconds are required; fractional seconds and timezone
names are not supported. Dates and offsets are validated (maximum offset ±14:00,
years 1000–9999 before and after conversion). Naive timestamps are rejected.
SPARK stores and returns UTC, so the example above returns `2026-09-19T01:00:00Z`.

The record contains `id` (server-generated lowercase UUID), `name`, `description`,
`occurs_at`, nullable `place`, and UTC `created_at`/`updated_at` timestamps.
Repeated POSTs create separate records. PATCH requires at least one editable field;
omitted values are preserved, and `place:null` clears the place. Unknown fields
are rejected. Identical PATCHes preserve the record and emit no extra event.

```sh
curl -X PATCH 'http://192.168.4.20:47683/events/<id>' \
  -H 'Content-Type: application/json' -d '{"place":"Library"}'
curl 'http://192.168.4.20:47683/events/<id>'
curl -X DELETE 'http://192.168.4.20:47683/events/<id>'
```

## Filters

All query parameters are optional; supplied filters combine with **AND**.

| Parameter | Meaning |
| --- | --- |
| `place` | Exact, case-sensitive place, including spaces |
| `name` | Case-sensitive literal substring in name; `%` and `_` are not wildcards |
| `day` | UTC calendar day, `YYYY-MM-DD` |
| `from` | Inclusive timestamp, with explicit zone |
| `to` | Exclusive timestamp, with explicit zone |
| `limit` | Page size, 1–500; default 100 |
| `offset` | Rows to skip, 0–1000000; default 0 |

A `day` filter covers midnight through 23:59:59 UTC. For a Perth calendar day,
use local-offset range boundaries instead. A range can be open-ended; when both
ends are given, `from` must be earlier than `to`. Unknown or repeated query
parameters are rejected. Encode plus signs in offsets as `%2B`, or use curl's
`--data-urlencode`:

```sh
# All events at Park on a UTC day:
curl --get http://192.168.4.20:47683/events \
  --data-urlencode 'place=Park' --data-urlencode 'day=2026-09-19'

# All events on September 19 in Perth (UTC+08:00):
curl --get http://192.168.4.20:47683/events \
  --data-urlencode 'from=2026-09-19T00:00:00+08:00' \
  --data-urlencode 'to=2026-09-20T00:00:00+08:00'
```

The response is `{"events":[...],"limit":100,"offset":0,"has_more":false}`.
Results are ordered by `occurs_at`, then ID. While `has_more` is true, increment
`offset` by `limit` and repeat with the same filters to retrieve all matches.
Pagination is not a frozen snapshot: concurrent modifications can move rows between
pages. Clients refreshing a collection after notifications should restart paging
if another relevant notification arrives during the refresh.

## Notifications and persistence

Creation, modification, and deletion emit `calendar.created`, `calendar.updated`,
and `calendar.deleted` through the existing TCP stream to all connected devices.
Their `resource` is `/events/{id}`. For a deletion, remove that item locally or
refresh the collection; a subsequent GET by ID returns 404. TCP welcome now lists
`/events` for reconnect resync; follow pagination to fetch the full collection.
Pulse subscription rules do not restrict calendar events.

The record change and domain event commit in one transaction. Database failures
roll back both. Notification history retains a snapshot after a record is deleted.

To upgrade: stop SPARK, run `./scripts/setup-db.sh`, and restart
`./build/spark_server`. The calendar schemas and grants preserve existing data.
Server version is 0.12.0; API version is 1.11.0.
Run `./scripts/test-local-db.sh` for the complete isolated integration suite.

## Recurrence and reminders (API 1.11.0)

Existing events remain one-off events. POST and PATCH accept `recurrence` and
`reminder_minutes`; GET and list responses include both. PATCH replaces the entire
recurrence object; omitted top-level fields are retained, and `null` clears either
option.

```json
{
  "name": "Exercise",
  "description": "Tuesday and Wednesday, every two weeks",
  "occurs_at": "2030-01-01T09:00:00+08:00",
  "recurrence": {
    "frequency": "weekly",
    "interval_weeks": 2,
    "weekdays": [2, 3],
    "start_date": "2030-01-01",
    "end_date": "2030-02-16",
    "timezone": "+08:00"
  },
  "reminder_minutes": 15
}
```

- Weekdays are ISO numbers: Monday=1 through Sunday=7. Duplicate days are invalid.
- `interval_weeks` is 1..52, default 1. Weeks begin Monday; the week containing
  `start_date` is the first active week. Every second Tuesday and Wednesday means
  both days in alternating weeks, not the second weekday of each month.
- `start_date` is required. `end_date` is inclusive or null (no end), default null.
  Dates require the year (`YYYY-MM-DD`) to avoid day/month ambiguity.
- `frequency` defaults to `weekly`; only weekly is supported. `timezone` defaults
  to `+08:00`; only fixed Perth/GMT+8 is supported. `occurs_at` supplies the local
  clock time and earliest possible occurrence, and need not itself be a selected
  weekday. A rule with no possible occurrence is rejected.
- Every Friday uses `[5]` and interval 1. A specific Friday uses `recurrence:null`.
  Every Monday from 2 January to 16 February uses `[1]`, interval 1, and those
  full dates in `start_date`/`end_date`.
- `GET /events` still returns series records, not expanded copies. A date filter
  matches a series if any occurrence falls within all the date bounds. Each series
  appears once, sorted by its stored `occurs_at` then ID. `day` remains a UTC day;
  use explicit-offset `from`/`to` to query a Perth day. Place/name filters and
  pagination continue to apply. GET by ID returns the complete series definition.

`reminder_minutes` is null (disabled) or an integer 0..10080; zero means at the
occurrence time. The server polls once per second and broadcasts to all connected
TCP clients:

```json
{"sequence":42,"event_id":123,"type":"calendar.reminder","resource":"/events/<id>","source_device":null,"occurrence_at":"2030-01-01T01:00:00Z"}
```

Fetch `resource` for the event details. `occurrence_at` identifies the specific
instance, even if several instances of the same series have reminders. Delivery
uses the existing TCP channel; it has no offline replay or client acknowledgment.

Reminder state and the notification outbox are committed together. Multiple
workers or restarts do not enqueue a reminder twice for the same occurrence.
`last_reminder_occurs_at` exposes the latest occurrence enqueued. Changing the
schedule (`occurs_at` or `recurrence`) resets that marker; changing descriptive
fields or reminder lead time does not resend an already reminded occurrence.
Disabling reminders or deleting the event prevents future scheduling, but cannot
recall notifications already committed or queued.

After downtime, overdue reminders are caught up while their occurrence is still
upcoming, with a 60-second grace after the occurrence to tolerate scheduler delays
and allow zero-minute reminders. Older occurrences are skipped. If multiple
reminders are already due, they are enqueued in occurrence order, one per series
per scheduler tick. The PoC scheduler scans reminder-enabled series each second;
large deployments will need indexed scheduling.

Upgrade: stop SPARK, run `./scripts/setup-db.sh` to apply the additive, repeatable
`008_calendar_recurrence.sql` migration, then restart `./build/spark_server`.
