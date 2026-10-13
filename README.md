# SPARK

Second-stage proof of concept: a C++ intelligence hub for an ArkOS handheld,
Tizen watch, Android handheld, Windows PC, and Android phone.

## Build and run

Requires a C++17 compiler, CMake 3.16+, pkg-config, MariaDB Server, and the MariaDB
Connector/C development package. HTTP and JSON headers are vendored. Builds do not
need network access after system dependencies are installed.

On antiX/Debian, install and start MariaDB using its SysV service script:

```sh
sudo apt-get update
sudo apt-get install -y mariadb-server libmariadb-dev libssl-dev pkg-config
sudo service mariadb start
./scripts/setup-db.sh
```

Run setup as the same OS user that will run SPARK. It creates the `spark` database,
applies the numbered SQL schemas, and grants that user access through MariaDB's local
Unix socket authentication. No database password is needed for this setup; HTTP
is open unless `auth` is configured. Existing MariaDB accounts are not changed by setup.
If the OS username already has a password-based MariaDB account, configure SPARK
with that account's credentials instead. The app account receives CRUD on devices, SELECT/INSERT/UPDATE on timers, and
SELECT/INSERT on history/events, with no schema-administration privileges.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
./scripts/test-db.sh
./build/spark_server
```

Default: HTTP on `0.0.0.0:47683`, accessible to devices using this machine's LAN IP.
The default launch has no authentication or TLS. Configure token mode using the
[identity guide](docs/identity-permissions.md), or use the guarded
[remote deployment profile](docs/remote-deployment.md) for HTTPS and TCP TLS. Stop with Ctrl+C.
Override the listener with `--host 127.0.0.1 --port 47684`.

```sh
curl http://localhost:47683/health
curl http://localhost:47683/version
curl http://localhost:47683/api
```

## Device contract

The three GET endpoints take no required headers, query parameters, or request
body and return HTTP 200 with `Content-Type: application/json`. Unknown query
parameters are ignored. Responses use `Cache-Control: no-store`.

| Endpoint | Purpose | Response |
| --- | --- | --- |
| `/health` | HTTP service liveness | `{"status":"ok"}` |
| `/version` | Release and contract revision | `{"server_version":"0.11.0","api_version":"1.10.0"}` |
| `/api` | Full discovery document | Versions, authentication mode, polling guidance, errors, and endpoint input/output packets |

Fetch `/api` on startup. Poll `/health` and `/version` as needed (suggested default:
30 seconds); refetch discovery when `api_version` changes. Back off exponentially
with jitter on failures, up to 300 seconds. Ignore unknown response fields.
API versions use semantic versioning: breaking changes increment major, compatible
additions increment minor, and contract fixes increment patch. Server releases are
versioned independently in `CMakeLists.txt`.

Each discovery entry contains a method, path, description, input packet (headers,
query, body, raw HTTP example), and output packet (status, headers, field descriptions,
and JSON body example). The `/api` response example omits its innermost self-example
to keep the document finite.

HEAD is supported on the same paths. OPTIONS returns 204 and allowed methods.
Browser clients receive `Access-Control-Allow-Origin: *`; no credentials are needed.
Unknown paths return JSON 404; unsupported methods on known paths return JSON 405.
Health reports server liveness only, not device or downstream service health.

## Structure

- `src/main.cpp`: command-line options and HTTP listener.
- `src/api.cpp`: endpoint registry, discovery document, and HTTP behavior.
- `src/devices.cpp`: device HTTP handlers and validation.
- `src/database.cpp`: MariaDB prepared statements and transactional storage.
- `sql/001_devices.sql`: device records and change history.
- `src/timers.cpp`: timer HTTP handlers, discovery, and background expiry worker.
- `sql/002_timers.sql`: persistent deadlines and generic domain events.
- `tests/api_test.cpp`: integration checks using a real loopback HTTP listener.
- `third_party/`: pinned HTTP and JSON dependencies with licenses.

Core GET routes share a discovery registry; the device module provides its own
handlers and discovery entries. Add foundational services behind these handlers
as device capabilities are defined.

## Device registry

Records persist in MariaDB across server restarts. Devices should register on
first use and use PATCH for subsequent updates (POST returns 409 for existing IDs). IDs are case-sensitive, 1–64 ASCII
letters, digits, underscores, or hyphens. The URL supplies the ID; an optional JSON
`id` must match it. POST and PATCH require `Content-Type: application/json`.

| Endpoint | Input | Success |
| --- | --- | --- |
| `GET /devices/{id}` | No body | 200 with the device record; 404 if missing |
| `POST /devices/{id}` | Required `local_ip`; optional `external_ip` | 201 with the device record |
| `PATCH /devices/{id}` | One or both IP fields | 200 with the updated record |
| `DELETE /devices/{id}` | No body | 204 with no body |

Both addresses accept IPv4 or IPv6 literals. `local_ip` cannot be null.
On POST, omitted `external_ip` is inferred from a public connection source IP when
possible, otherwise stored as null. An explicit null leaves it unknown. On PATCH,
omitted fields are preserved and `external_ip: null` clears the address.
Forwarded headers are ignored. A LAN connection cannot reveal a device's public
NAT address; the device may supply an address it already knows. Supplied addresses
are recorded without reachability checks.

```sh
curl -X POST http://localhost:47683/devices/watch-1 \
  -H 'Content-Type: application/json' \
  -d '{"local_ip":"192.168.1.50"}'
curl -X PATCH http://localhost:47683/devices/watch-1 \
  -H 'Content-Type: application/json' \
  -d '{"local_ip":"192.168.1.51","external_ip":null}'
curl -X DELETE http://localhost:47683/devices/watch-1
```

A record looks like `{"id":"watch-1","local_ip":"192.168.1.50","external_ip":null}`.
Duplicate POST returns 409; GET/PATCH/DELETE for a missing ID return 404. Invalid JSON,
IDs, IPs, unknown fields, or empty updates return 400. Incorrect content types
return 415. Error responses contain `error`; invalid IP errors also name `field`.
Device paths support OPTIONS with GET/HEAD/POST/PATCH/DELETE and JSON CORS preflight.
Registry operations are open by default; token mode enforces device permissions and ownership.
`/api` describes all twenty-seven endpoints, including device and timer request/response packets.

## Database operations

`devices` stores the current record plus `created_at` and `updated_at` timestamps.
`device_events` stores an ordered history of creates, updates, and deletes with an
IP snapshot and `recorded_at`. Times are UTC. Deleting a device preserves its
history; re-registering the ID creates a new current record under the same history.
History is currently available through SQL, with no additional HTTP endpoints.
HTTP device response packets remain unchanged.

Prepared statements bind values, and InnoDB transactions commit the record and
history together. Row locks serialize concurrent PATCH/DELETE requests, while the
case-sensitive primary key enforces unique IDs. SPARK checks database connectivity
and schema at startup and exits with an error if unavailable. Database failures
during requests return `503 {"error":"database_unavailable"}`; `/health` remains a
liveness check. Each request opens a bounded-time connection, so later requests
can recover after MariaDB restarts. A connection lost during commit can leave the
client uncertain whether the change committed; retrying POST may return 409 and
retrying DELETE may return 404. Repeating PATCH is safe for current IP state but
adds a history event.

Configuration uses `SPARK_DB_HOST` (default `localhost`), `SPARK_DB_USER` (default
`USER`), `SPARK_DB_PASSWORD` (default empty), `SPARK_DB_NAME` (default `spark`),
`SPARK_DB_PORT` (default `3306`), and optional `SPARK_DB_SOCKET`. `localhost` uses
a Unix socket; TCP/password accounts must be provisioned separately. Set
`SPARK_DB_USER` explicitly when running SPARK under a supervisor. `.env.example`
lists overrides; `.env.local` is ignored by Git and is not loaded automatically:

```sh
set -a
. ./.env.local
set +a
./build/spark_server
```

The setup script is safe to repeat for this initial schema. Future schema changes
should be added as numbered SQL migrations. Back up the database and file bytes
together using `scripts/backup.sh`; a database-only dump cannot restore uploaded
files. See [external-drive backups and recovery](docs/backups.md).

`./scripts/test-db.sh` provisions and removes a uniquely named test database. It
requires sudo MariaDB access and runs the HTTP contract, concurrency, persistence,
and database-failure checks. Direct CTest execution skips database integration
unless `SPARK_DB_NAME` starts with `spark_test_`; never point tests at production.

Fetch a device from any client with no request body or required headers:

```sh
curl http://192.168.4.20:47683/devices/watch-1
```

GET returns `200` with `id`, `local_ip`, nullable `external_ip`, and a boolean
`subscribed` indicating this device ID’s current pulse subscription. POST/PATCH
retain their existing device packets. Missing devices return `404 {"error":"device_not_found"}`;
invalid IDs return `400 {"error":"invalid_id"}`. Database failures return
`503 {"error":"database_unavailable"}`. Reads do not change timestamps or history.

## Shared timers and domain events

For an existing installation, stop SPARK, run `./scripts/setup-db.sh` to add the
new tables and grants, then restart `./build/spark_server`. Existing device data
is preserved. Version 0.11.0 requires all numbered schemas at startup; the API contract version
is 1.10.0. HTTP remains on port 47683; authentication is enabled through instance configuration.

| Endpoint | Input | Result |
| --- | --- | --- |
| `POST /timers` | `duration_seconds`, optional `label` and `source_device` | 201, timer record and Location header |
| `GET /timers` | None | 200, `{"timers":[...]}` with running and expired-unconfirmed timers |
| `GET /timers/{id}` | None | 200, timer record in any state; 404 if missing |
| `POST /timers/{id}/confirm` | Optional `source_device` | 200, acknowledge an expired timer |
| `POST /timers/{id}/stop` | Optional `source_device` | 200, stop a running or expired timer |

`duration_seconds` is an integer from 1 to 604800 (seven days). `label` defaults
to empty and accepts up to 200 UTF-8 bytes. `source_device` is nullable attribution
metadata, with the same syntax as a device ID; it does not require registration.
The creator is retained in the timer record; confirm/stop record the acting device
in their respective events. Server-generated UUIDs are lowercase. Unknown body
fields are rejected. POST bodies use `Content-Type: application/json`; confirm
and stop also accept an empty body without that header. GET supports HEAD, and
all timer paths support OPTIONS for browser clients.

```sh
curl -X POST http://localhost:47683/timers \
  -H 'Content-Type: application/json' \
  -d '{"duration_seconds":180,"label":"Tea","source_device":"watch-1"}'
curl http://localhost:47683/timers
# Replace <id> with the ID returned when creating the timer:
curl 'http://localhost:47683/timers/<id>'
curl -X POST 'http://localhost:47683/timers/<id>/confirm' \
  -H 'Content-Type: application/json' -d '{"source_device":"phone-1"}'
curl -X POST 'http://localhost:47683/timers/<id>/stop'
```

Timer records contain `id`, `label`, `duration_seconds`, `status`, `source_device`,
`created_at`, `ends_at`, `updated_at`, and `remaining_seconds`. Timestamps are UTC
ISO 8601 strings. Remaining time is rounded up to seconds while running, and zero
in every other state. Stop retains the original deadline for history.

The lifecycle is `running → expired → confirmed`, or `running/expired → stopped`.
Confirming early returns 409. Confirming an already confirmed timer or stopping an
already stopped timer succeeds without another event. Confirming a stopped timer
or stopping a confirmed timer returns 409. Creation is not idempotent: repeated
POSTs create separate timers. Confirmed/stopped records remain queryable by ID.

The server uses MariaDB's UTC clock and persisted deadlines rather than decrementing
stored counters. A worker checks due timers once per second, processing up to 100
per pass; large backlogs take additional passes. Startup catches up overdue timers.
Queries also process an expiry batch, and confirm/stop settle their own timer's
deadline under a row lock. Clock corrections affect wall-clock deadlines. Worker
failures are logged and retried; HTTP database failures return JSON 503.

Every successful state change atomically appends one event in `events`:
`timer.created`, `timer.expired`, `timer.confirmed`, or `timer.stopped`. Events contain
`event_id`, `type`, `resource` (such as `/timers/<id>`), nullable `source_device`, a
JSON timer snapshot in `payload`, and UTC `recorded_at`. Expiry events have no source
device. Reads and idempotent retries do not generate action events; a read that
settles a deadline may generate its one expiry event. If event insertion fails,
the state change rolls back. Event IDs identify events but are not guaranteed to
represent commit order across concurrent transactions.

The structure separates HTTP validation, transactional state changes/event recording,
and background scheduling so future services can use the same pattern. Events are
broadcast as hints through the TCP notification listener; REST remains the source
of current state.

Timer errors use the existing JSON error packet: 400 for invalid inputs, 404
`timer_not_found`, 409 `invalid_timer_state`, 415 `unsupported_media_type`, and 503
`database_unavailable`. See `/api` for input/output examples and field descriptions.

Run the full integration suite without administrative database access:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
./scripts/test-local-db.sh
```

This creates a private MariaDB instance in a temporary directory, with only a Unix
socket and no TCP listener, and removes it afterward. It tests device contracts,
timer lifecycle, background expiry, concurrent confirmation, overdue recovery,
transaction rollback, TCP delivery/framing/reconnects, commit-order handling, and
persistence through a MariaDB restart. The existing
`test-db.sh` runner also supports both schemas but uses sudo on the system database.

## TCP notifications

SPARK now starts a TCP notification listener on **47684**, alongside REST on 47683.
Clients send a newline-terminated `{"type":"hello","device_id":"watch-1"}`,
wait for welcome, and refresh `/timers`. Committed timer events then tell every
connected client which REST resource changed. Reply to server ping messages with
pong, and refresh REST state when welcome has `resync_required:true`. This is an
raw TCP stream; token mode requires a token in hello and filters events by permissions. Clients can resume using `after_sequence` to replay
missed events.

The complete client contract, examples, framing rules, heartbeat intervals, and
limits are in [docs/notifications.md](docs/notifications.md) and `/api.notifications`.
Override the port with `--notification-port 47685`. Both listeners use `--host`.

For an existing installation: stop SPARK, run `./scripts/setup-db.sh` to apply the
new notification schema/grants, and restart `./build/spark_server`. This preserves
existing data. SQL tables `notification_log` and `notification_dispatch_lock`
provide dispatch ordering independent of transaction commit order. The listener
and dispatcher live in `src/notifications.cpp`; storage is in `src/database.cpp`.

## Pulse reminders

SPARK has one shared recurring pulse schedule, initially disabled with a 60-minute
interval. `GET /pulse` reads its state; `PATCH /pulse` changes `enabled` and/or
`interval_seconds`. Due pulses emit **`pulse.triggered`** to subscribed TCP device IDs.
Configuration changes emit `pulse.updated`, which is not a reminder. Clients own
presentation and dismissal. The core persists deadlines and coalesces missed
intervals into one reminder after downtime.

See [docs/pulse.md](docs/pulse.md) for request examples, scheduling rules, and client
handling. Apply the additive schema with `./scripts/setup-db.sh`, then restart SPARK.
The existing private-database test suite covers pulse timing, TCP delivery,
configuration, concurrent scheduling, rollback, and restart persistence.

Subscribe with `POST /pulse/subscriptions/{device_id}` and unsubscribe with
`DELETE /pulse/subscriptions/{device_id}`, using the exact ID from TCP hello.
Neither endpoint requires a request body; both return 200 and the resulting
`subscribed` boolean. Devices are unsubscribed by default. Both pulse reminder and
settings events are filtered; timer notifications remain shared with all devices.
Subscriptions persist across reconnects/restarts and do not alter the pulse schedule.
Apply `./scripts/setup-db.sh` and restart SPARK to load the subscription schema.

## Dated events

`POST /events` creates an event with `name`, `description`, `occurs_at`, and optional
`place`. Use `PATCH /events/{id}`, `DELETE /events/{id}`, and `GET /events/{id}` to
modify, delete, or retrieve one. `GET /events` supports combined `place`, `name`,
`day`, `from`, and `to` filters plus `limit`/`offset` pagination. Timestamps require
an explicit UTC offset or Z and are returned in UTC; `day` means a UTC calendar day.

Changes emit `calendar.created`, `calendar.updated`, and `calendar.deleted` over
TCP. Full examples and filter rules: [docs/events.md](docs/events.md). Apply
`./scripts/setup-db.sh` and restart SPARK for the new schema. Optional
`reminder_minutes` schedules `calendar.reminder` notifications; `recurrence` adds
weekly schedules and inclusive Perth date bounds.

## Daily tasks

The shared checklist lives at `/daily-tasks`. Create with POST, read the list or
`/{id}` with GET, edit with PATCH, and delete with DELETE. Set a task's state using
`PUT /daily-tasks/{id}/checked` with `{"checked":true}` or `{"checked":false}`.
Task definitions persist; checkmarks reset at midnight GMT+8 (Perth), with recovery
across downtime. All clients receive `daily_tasks.updated` and `daily_tasks.reset`
notifications pointing to `/daily-tasks`.

See [docs/daily-tasks.md](docs/daily-tasks.md) for examples and reset semantics.
Apply `./scripts/setup-db.sh` and restart SPARK for the new schema.

Events support weekly/fortnightly recurrence with Perth date bounds and optional
TCP reminders before each occurrence. `GET /events/occurrences?from=...&to=...`
expands schedules into dated instances, ordered by occurrence time, with optional
name/place filters and pagination. `/events` continues to return series.
See [the event API](docs/events.md).

## Browser API reference

Open `/api/docs` (for example, `http://192.168.4.20:47683/api/docs`) for a
human-readable reference. Select an endpoint in the scrollable left sidebar to
view its description, request fields, response examples and errors. Search by
method, path or description. Selections have shareable URL fragments; narrow
screens show the endpoint list above the details. The page fetches the current
`/api` document and needs JavaScript. No external assets or internet connection
are required. Restart the rebuilt server to enable this route; no database
migration is needed.

## Shared files

SPARK stores arbitrary files on disk and metadata in MariaDB, with streaming
uploads/downloads and no application-level file size cap. `POST /files?name=...`
accepts raw bytes with `Content-Type: application/octet-stream`; `GET /files` lists
them, `GET /files/{id}` returns metadata, `GET /files/{id}/content` downloads them,
and `DELETE /files/{id}` removes them. Downloads support HTTP byte ranges.

Set `SPARK_FILE_DIR` to choose the storage directory (default `storage/files`).
Stop SPARK, run `./scripts/setup-db.sh`, and restart the rebuilt server to activate.
See [the file API and recovery behaviour](docs/files.md), or browse `/api/docs`.

The explorer can view and edit UTF-8 text up to 2 MiB, with Save, Reload and
concurrent-edit protection. Larger logs offer first/last 64 KiB previews.
`PUT /files/{id}/content` saves text with the current content ETag in `If-Match`.
Apply `014_file_revisions.sql` through `./scripts/setup-db.sh` before restarting
the rebuilt server; existing uploads remain compatible.

Browse `/files/explorer` to upload, preview, download, move and delete files, and
create directories. Files now have a logical `directory` (default `/`); upload and
list requests accept it, and `PATCH /files/{id}` moves a file. Run
`./scripts/setup-db.sh` with SPARK stopped, then restart to apply directory support.

## Projects

`POST /projects` creates a named project reference and its `/projects/{name}`
logical file directory. Projects have descriptions and optional `github_url`
links. List with `GET /projects`, retrieve or edit with `GET/PATCH /projects/{id}`.
Renaming preserves the assigned directory. `DELETE /projects/{id}` requires
`{"destroy_directory":false}` to retain files, or `true` to remove the full tree.
See [the project API](docs/projects.md). Apply `./scripts/setup-db.sh` with SPARK
stopped, then restart the rebuilt server.

Open `/projects/explorer` for browser-based project management, with links to each
project’s files and repository and an explicit file-retention choice on deletion.

Projects support nested subprojects through `parent_id` on creation. Child file
directories sit beneath their parent's directory. `GET /projects/{id}?depth=shallow`
(default) returns child IDs; `?depth=deep` embeds the entire descendant tree.
The project explorer includes New subproject and parent/child navigation.
Apply `./scripts/setup-db.sh` with SPARK stopped for the hierarchy migration.

Each project can also have multiple named Kanban boards. Manage columns and tasks
in `/projects/explorer`, or use `/projects/{id}/boards` and the nested board API.
Tasks move between columns without changing identity; non-empty columns cannot be
deleted. Changes use TCP `board.created`, `board.updated` and `board.deleted`.
See [the board API](docs/boards.md). Stop SPARK and run `./scripts/setup-db.sh`
before restarting the rebuilt server for this migration.

### Notification reconnect catch-up (API 1.22.0)

TCP clients can send `{"type":"hello","device_id":"watch-1","after_sequence":42}`
to replay missed notifications from the durable log. Save the last successfully
processed **sequence**, not event ID. `catchup_complete` marks the transition to
live delivery; checkpoints also advance past subscription-filtered pulses.
Existing hello messages still request a full REST resync. See the
[notification protocol](docs/notifications.md) for cursor persistence and recovery.
No database migration is needed for this addition; rebuild and restart SPARK.

### Consistent change notifications (API 1.23.0)

Files, directories, projects, device registrations and pulse subscriptions now
emit durable TCP notifications alongside timers, calendars, daily tasks and boards.
Changes and events commit together, including recursive project deletions. They
also replay on reconnect. See [the notification coverage table](docs/notifications.md#additional-change-coverage-api-1230)
for event names and refresh behavior. Rebuild/restart; no new migration.

### External-drive backups

Stop SPARK (leave MariaDB running), mount an external drive, and run:

```sh
./scripts/backup.sh
```

Choose a previous successful location or enter the current mount path. For a
direct command, use `--mount "/path/to/mounted-drive"`. This creates a timestamped,
compressed backup of the SPARK database and file
storage, split into 1 GiB chunks for FAT32 support. It checks the storage lock,
refuses an unmounted destination or the source filesystem, and verifies checksums
before publishing a completed backup. Restart SPARK after it finishes. Backups
are retained until explicitly removed; no schedule is installed automatically.
See [backup verification and restore instructions](docs/backups.md).

### Dashboard

Open `/` for links to the projects explorer, files explorer and API reference.
The dashboard can restart SPARK's services and run a compressed external-drive
backup using a previous or new mount path. Backup temporarily takes REST and TCP
offline; the page reconnects and shows the result when services resume. See
[dashboard backup details](docs/backups.md#dashboard-backups). Restarting services
does not load a newly compiled binary; deploy those by relaunching the process.

### Device commands (API 1.25.0)

Devices advertise a small action-name list using `PUT /devices/{id}/actions`.
Requesters submit `POST /commands` with an optional target: without one, the first
eligible device to atomically claim the work executes it.
As of API 1.26.0, claim requests accept an optional `command_id` so clients can
choose their own execution priority. A specified ID never falls back to other work;
omitting it retains oldest-eligible selection. The TCP layer delivers
`command.available` and `command.updated` to the appropriate devices. Claims use
leases and tokens, and commands retain results and idempotency keys in MariaDB.
Expired execution leases become `outcome_unknown`; work is never automatically
reassigned. See [device command protocol](docs/commands.md) and `/api/docs` for
packets, endpoints and reconnect handling.

Apply the new schema/permissions using `./scripts/setup-db.sh`, then rebuild and
relaunch SPARK. Client devices must implement their local action handlers.

Open `/commands/explorer` to browse devices exposing actions and run a targeted
command once with optional JSON inputs. The page tracks execution results and is
linked from the dashboard. Device discovery is also available as paginated JSON
at `GET /commands/devices` (API 1.27.0).

### Planned configuration and authentication

The agreed [configuration and authentication plan](docs/authentication-plan.md)
tracks optional auth, shared REST/TCP identity and permissions, browser sessions,
and secure remote deployment. Steps 1–4 are implemented: see [identity and permissions](docs/identity-permissions.md) and [instance configuration](docs/configuration.md)
for `--config`, `--check-config`, precedence and the `storage.drives` array. Configuring `auth` now enforces bearer tokens on REST and a token in TCP hello.
[Browser login](docs/browser-login.md) is available with `auth.browser_origin`.
Apply migration 019 for administrator commands. [Encrypted remote deployment](docs/remote-deployment.md) now covers both HTTPS and TCP TLS. Multiple drives
are supported with persistent file locations, uploads directed to a chosen drive,
and backups covering every drive. Apply migration 018 using `./scripts/setup-db.sh`
before restarting the new binary. An open-mode example is provided in
[config/open.example.json](config/open.example.json).

Runtime storage management is available at `/files/drives` and in the Files explorer:
attach mounted directories, detach removable storage, and browse/upload by stable
drive letters. Offline drives retain their metadata and letters; server storage
remains persistent. See [configuration](docs/configuration.md).

Directories now belong to drives, including empty folders. Projects select a drive
at creation; children inherit it. Offline project metadata and boards stay usable,
while file operations require the selected drive. See [project storage](docs/projects.md).

Device tokens can be provisioned from **Device access** on the dashboard
(`/admin/credentials/explorer`). An administrator with `admin.credentials` can
select permissions, copy a newly issued token once, and revoke device credentials.
See [browser device provisioning](docs/identity-permissions.md#browser-device-provisioning-api-1340).

### Local backups and SPARK connections

Use `/backups/explorer` to create backups on persistent storage, copy/move them to
an attached drive, send them to another SPARK, or delete local copies. Register
remote credentials in `/connections/explorer` (`admin.connections` required).
See [backup and connection setup](docs/backups.md). Outbound HTTPS transfers require
`curl` and CA certificates. No database migration is required for this feature.

Backup creation now opens `/backups/new` for a size/free-space estimate and explicit
confirmation. `/backups/progress` remains available during the offline snapshot;
ordinary REST requests return 503 with Retry-After. Live logs and interrupted state
are persisted. Existing `admin.backup` credentials suffice; no migration is needed.
