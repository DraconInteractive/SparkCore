# Daily tasks

SPARK maintains one shared daily checklist. Task names and descriptions persist;
checked state resets at midnight in **fixed GMT+8 (Perth)**, equivalent to 16:00 UTC.
The database clock determines the day, not a client's clock or the host timezone.

## Endpoints

| Method/path | Purpose | Success |
| --- | --- | --- |
| `POST /daily-tasks` | Create an initially unchecked task | 201 and task; Location header |
| `GET /daily-tasks` | Fetch the complete shared list | 200, day/timezone/tasks |
| `GET /daily-tasks/{id}` | Fetch a task | 200 and task |
| `PATCH /daily-tasks/{id}` | Modify name and/or description | 200 and task |
| `PUT /daily-tasks/{id}/checked` | Set checked or unchecked for today | 200 and task |
| `DELETE /daily-tasks/{id}` | Permanently delete a task | 204, no body |

Write bodies require `Content-Type: application/json`. Creation requires a nonblank
`name` (up to 200 UTF-8 bytes); `description` is optional and defaults to empty
(up to 4000 UTF-8 bytes). The existing 16 KiB encoded-request limit still applies.
PATCH requires name and/or description; omitted values and checked state are
preserved. The checked endpoint requires a boolean `checked`. Unknown body fields
are rejected. POST, PATCH and PUT accept optional `source_device` attribution.
The existing device-ID syntax applies; registration and authentication are not
required. IDs for tasks are server-generated lowercase UUIDs.

```sh
curl -X POST http://192.168.4.20:47683/daily-tasks \
  -H 'Content-Type: application/json' \
  -d '{"name":"Drink water","description":"A daily habit"}'

curl http://192.168.4.20:47683/daily-tasks

curl -X PUT 'http://192.168.4.20:47683/daily-tasks/<id>/checked' \
  -H 'Content-Type: application/json' \
  -d '{"checked":true,"source_device":"watch-1"}'

# Uncheck by sending checked:false to the same endpoint.
curl -X PATCH 'http://192.168.4.20:47683/daily-tasks/<id>' \
  -H 'Content-Type: application/json' -d '{"name":"Drink a glass of water"}'
curl -X DELETE 'http://192.168.4.20:47683/daily-tasks/<id>'
```

A list response looks like:

```json
{
  "day": "2026-09-19",
  "timezone": "+08:00",
  "tasks": [{
    "id": "12345678-1234-1234-1234-123456789abc",
    "name": "Drink water",
    "description": "A daily habit",
    "checked": false,
    "checked_at": null,
    "day": "2026-09-19",
    "created_at": "2026-09-19T00:00:00.000000Z",
    "updated_at": "2026-09-19T00:00:00.000000Z"
  }]
}
```

Tasks sort by creation timestamp then ID. `day` refers to the GMT+8 checklist day;
all timestamps are UTC ISO 8601. Unchecking or resetting clears `checked_at`.
Sending an identical check state or identical edit is a no-op, preserving timestamps
and producing no extra mutation notification. Repeated POST creates distinct tasks.
A queued check received after midnight applies to the new current day; there is no
endpoint for modifying an earlier day's state.

Missing tasks return `404 {"error":"task_not_found"}`. Invalid UUIDs/packets/fields
return 400, wrong content type returns 415, and database failures return 503.
Read endpoints support HEAD, all paths support OPTIONS, and `/api` documents the
full input and output contracts.

## Reset and notifications

A background worker checks for day changes once per second. Task reads and writes
also settle any overdue day change before returning results. Therefore the first
request on a new day sees reset state even if the worker has not yet run. Requests
use the day sampled after acquiring the day lock; a request crossing midnight may
finish with its sampled day, which is included in its response.

Rollover clears checkmarks without deleting definitions. After downtime spanning
multiple days, SPARK performs one catch-up reset to the current day. Same-day
restarts preserve checks. The persisted day marker serializes all task changes and
resets, so concurrent workers cannot emit duplicate resets or clear a new-day check.
The clock uses the explicit GMT+8 offset without requiring database timezone tables.
If the system clock is corrected into another calendar day, that day transition
also resets the checklist.

TCP notifications go to **all connected devices**, independent of pulse subscriptions:

- `daily_tasks.updated`: creation, editing, checking/unchecking, or deletion.
- `daily_tasks.reset`: a daily rollover, including a catch-up reset.

Both use `resource: "/daily-tasks"`. Clients should fetch the list again. Welcome
includes `/daily-tasks` so clients resync after reconnecting. No acknowledgement is
required. Offline notification behavior follows the existing TCP protocol.

Task state changes and notification events commit atomically. If writing an event
fails, the task change/reset rolls back; the worker logs the failure and retries.
History in the internal event log retains mutation snapshots, while the REST API
shows only current task definitions and today's checkmarks.

## Upgrade

Stop SPARK, run `./scripts/setup-db.sh`, then restart `./build/spark_server`.
`sql/007_daily_tasks.sql` adds the task and day-marker tables. Re-running setup
preserves the checklist and day marker. Server version is 0.11.0; API version is
1.10.0. Run `./scripts/test-local-db.sh` for isolated integration tests.
