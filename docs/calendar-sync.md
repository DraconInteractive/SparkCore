# Shared calendar synchronization

SPARK initiates synchronization through an existing saved HTTPS connection.
There are no hard-coded deployment roles or hostnames. The initiating instance
is the coordinator; the peer needs no reciprocal connection or inbound route to
the coordinator. Both servers implement the same protocol.

## Deployment and setup

Stop SPARK on each instance, rebuild, run `./scripts/setup-db.sh` against its
configured database, then restart. Migration 021 adds the persistent instance
identity, shared-event revisions, tombstones, receipts, checkpoints and conflict
history. Existing shared events are seeded without changing their local IDs or
reminder history. Local events are not published.

Provision a **device token on the peer** with `calendar.sync.read` and
`calendar.sync.write`. Existing tokens do not automatically gain new permissions.
Use that token and its device ID in the coordinator's existing
`/connections/explorer`. Backup permissions are unnecessary for calendar-only
connections. The drive/directory fields are backup defaults and do not select
a calendar.

Enable calendar sync on that connection, then click **Sync now**. The first run
merges both shared calendars. Subsequent runs exchange changes since the saved
checkpoints. Only one outgoing connection can have calendar sync enabled.
Enable it on the coordinator only, not on both ends of the same pairing.
Synchronization is manual in this release; enabling it does not start a schedule.

The explorer shows state, last success, changes remaining in the current scan,
errors and conflict history. New peer changes are discovered when a sync runs;
the pending count is not an always-live peer inventory. Transfers and backup
maintenance cannot run alongside a coordinator sync. Ordinary event editing and
reminders remain available.

## Identity, moves and reminders

Each database has a persistent instance UUID. A connection pins the peer UUID
and refuses a different server at the same URL. Restoring a database restores its
identity: do not run a cloned database as a separate peer without a deliberate
identity migration.

Shared entries have sync UUIDs separate from local REST event IDs. The same
shared entry may therefore have different `/events/{id}` URLs on each server.
No name/date heuristic merges independently created entries.

Moving local → shared publishes a new shared identity. Moving shared → local
leaves a deletion record for the old shared identity and detaches the local
record. A later move back to shared starts a new shared identity, so an old
deletion cannot delete the newly published entry. A conflicting peer edit can
create a shared copy if the coordinator wins; the detached local copy is retained.

Name, description, date/time, place, recurrence and reminder settings sync.
Creation/update timestamps and reminder delivery markers are instance-local.
Imports emit normal calendar notifications with `calendar: "shared"`.
Unchanged imports produce no notification. Each server schedules reminders for
its own devices. Changing an imported schedule resets its reminder marker in the
same way as a normal event PATCH; unrelated edits do not reset it.

## Conflict and failure behaviour

A three-way comparison uses the last synchronized data, current coordinator data
and current peer data. A change on only one side wins. If both changed differently,
the coordinator wins; both versions and their previous baseline are retained in
a conflict record. This also applies to edit-versus-deletion conflicts.

Writes use expected revisions and atomic database transactions. Concurrent edits
are never blindly overwritten; a revision race can finish the run with
`concurrent_changes_retry`. Click Sync now again to process the latest changes.

Before applying a change, the coordinator persists the exact pending requests.
Each server records a durable operation receipt alongside its mutation and
notification. Lost responses and restarts replay that receipt. Checkpoints advance
only after processing the scan. An interrupted run can be resumed with Sync now.
Disabling sync retains recovery state; deletion or repointing of a connection
with pending work is blocked.

Tombstones, receipts and conflicts are retained without automatic pruning.
They are included in database backups. Saved connection tokens remain excluded
from backups under the existing credential-storage policy.

## API

Coordinator management requires administrator identity and `admin.connections`:

- `GET /connections/{id}/calendar-sync`: status/checkpoints.
- `PUT /connections/{id}/calendar-sync`, `{"enabled":true}`: enable or disable.
- `POST /connections/{id}/calendar-sync/run`, `{}`: enqueue a run; returns 202
  with a job ID. Job progress is also available through existing backup job APIs.
- `GET /connections/{id}/calendar-sync/conflicts?after=0`: retained conflicts,
  50 per page; use the returned `next` cursor.

Peer protocol endpoints:

- `GET /calendar-sync`: protocol version 1, instance UUID and committed sequence.
- `GET /calendar-sync/changes?after=0&through=N`: up to 50 latest shared records
  or tombstones changed in the sequence interval. The cursor is exclusive;
  `through` pins the upper bound. An entry edited beyond that bound appears on
  the next scan. A tombstone has `data:null`.
- `GET /calendar-sync/items/{sync_id}`: latest portable record and revision.
  Unknown identities return null revision/data rather than a local event.
- `POST /calendar-sync/apply`: one atomic, idempotent application, bounded to
  64 KiB. Body fields are `instance_id` (target), `coordinator_id`,
  `operation_id`, `sync_id`, `expected_revision` and `data`.
  Data is null for deletion, otherwise the six portable fields listed above.
  HTTP 200 with `applied:false` indicates a revision conflict, not success.

Read endpoints require `calendar.sync.read`; apply requires
`calendar.sync.write`. These grants expose only the shared calendar. They do not
grant access to local calendar events through ordinary event endpoints.
