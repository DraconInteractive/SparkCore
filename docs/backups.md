# Backups and transfers

A recoverable SPARK backup needs both the MariaDB database and every storage drive.
File blobs alone do not retain names, logical directories, or project associations.
The backup command captures all SPARK database tables, including projects, boards,
devices, timers, events, tasks, subscriptions and notification history, alongside
file storage. It does not change the live data.

Command-line creation is **manual and offline**: stop every SPARK process using this
database before starting, but leave MariaDB running. The script holds the same
exclusive storage lock as SPARK for the entire backup. It refuses to run while
the server owns that lock, and the server cannot start against that storage until
the backup releases it. Do not use another application or SQL session to modify
the database or stored files while backing up. The database dump uses a single
InnoDB transaction; the storage lock keeps file revisions aligned with it.

## Create an external-drive backup from the command line

1. Mount the external drive using the OS. The scripts do not mount, format or erase
   drives. `lsblk -f` can help identify the current mount path.
2. Load the same `SPARK_DB_*` and `SPARK_FILE_DIR` environment as the server, and
   stop SPARK. The default file directory is this checkout's `storage/files`.
   Set `SPARK_FILE_DIR` explicitly if your server uses a different directory.
   For multiple drives, pass `--file-dir` once per configured root (the script does
   not read the server JSON configuration):

   ```sh
   ./scripts/backup.sh --mount /media/demo/backup-drive \
     --file-dir /srv/spark/files --file-dir /media/demo/data/spark-files
   ```

   Every registered drive must be included, even if empty. The script checks the
   database registry and locks all roots before dumping data; an omitted drive or
   missing `.spark-drive` marker fails the backup. Start the upgraded server once
   after migration 018 to register legacy storage before taking a new backup.
3. Run as the same OS user used to run SPARK:

   ```sh
   ./scripts/backup.sh --choose
   ```

   The menu offers up to 20 previously successful mount/destination pairs, marked
   mounted or not mounted, plus **Enter a new location**. Enter the mount path and
   optionally a destination subdirectory. A remembered path is checked again on
   every run; it is not assumed to still be mounted. If the mount path changes,
   choose a new location. Only successful backups are remembered.

   To open the selector even when environment defaults are set, use `--choose`.
   To bypass the menu, pass `--mount` explicitly. The destination defaults to
   `<mount>/spark-backups`. To choose another directory:

   ```sh
   ./scripts/backup.sh --mount "/media/demo/backup-drive" \
     --destination "/media/demo/backup-drive/SPARK backups"
   ```

   Environment alternatives are `SPARK_BACKUP_MOUNT` and `SPARK_BACKUP_DIR`.
   Location history is stored locally in `storage/backup-locations.tsv` (outside
   the file blobs); `SPARK_BACKUP_HISTORY` can override its path. History contains
   only paths, never credentials. You can remove that file to forget locations.
   Without an explicit mount or environment target, backups use persistent storage.
   Jobs do not wait for input unless `--choose` is explicitly requested.
   `.env.local` is not loaded automatically; use the same environment-loading
   method as the server. Paths containing spaces must be quoted.
4. Wait for **Backup complete**, restart SPARK, then safely unmount/eject the drive
   using the OS before disconnecting it.

Requires Bash, MariaDB client tools (`mariadb` and `mariadb-dump`), GNU tar/coreutils, gzip and
util-linux (`flock`, `mountpoint`). These tools are available on this antiX host.
The normal provisioned SPARK
account has sufficient read permissions. Credentials are placed in a temporary
mode-600 client option file, removed on normal exit/error, and are never written
into the backup or command-line arguments.

For explicit external targets, the destination must be under the specified mounted directory and on a different
filesystem from every source storage drive. An unplugged/unmounted drive is an error; the
script will not silently put the backup on the internal disk. Separate backup
runs are serialized. Keep the chosen drive mounted for the entire command.

## Format, verification and retention

A completed directory looks like:

```text
spark-20261006T040000Z-Ab12Cd34/
  manifest.txt
  SHA256SUMS
  database.sql.gz.part-000000
  files.tar.gz.part-000000
  files.tar.gz.part-000001
  ...
```

Each compressed stream is split into chunks of at most **1 GiB**. This supports
FAT32's individual-file size limit, including backups containing source files
larger than 4 GiB. It does not set a maximum total backup size; available drive
capacity remains the limit. Compression streams directly to the drive, without
staging a second full copy on the server. Already compressed media may shrink very
little. Every run is a full backup; no old backups are automatically deleted.

Single-drive backups retain `format=spark-backup-v1` and the layout above.
Multi-drive backups use `format=spark-backup-v2`: `drives.tsv` lists each persistent
drive ID and its original path, and each drive has its own
`drive-<id>.tar.gz.part-000000` stream instead of `files.tar.gz.part-*`. Both formats
include the `.spark-drive` marker inside each storage archive. The verifier checks
all expected drive archives and, for v2, their marker identities. Original paths
are informational; restore roots can be elsewhere.

Files are initially written into `.incomplete-spark-...`. After checksum and
archive validation, the filesystem is flushed and the directory is renamed to its
completed name. Errors return nonzero and leave an explicitly incomplete directory
for diagnosis; it is not a recovery point. Remove incomplete directories manually
when appropriate. Existing completed backups are never replaced by a new attempt.
The script deliberately performs no automatic service stop/restart or scheduling.

Verify a completed backup later, including after copying it to another drive:

```sh
./scripts/verify-backup.sh "/media/demo/backup-drive/spark-backups/spark-..."
```

Verification checks that every expected part is listed, verifies SHA-256 hashes,
and checks both compressed streams and the tar structure. Checksums detect damage,
not malicious replacement. Backups are **not encrypted**; protect the drive as you
would the server. FAT32 cannot enforce Unix file permissions. Store another copy
separately if protection from loss of the machine and attached drive is needed.

## Restore into a fresh database and storage directory

Restore only trusted backups that pass verification. Keep SPARK stopped until the
restored database and file directory are paired and runtime permissions are ready.
Use a **new database and empty directory**, preserving current data while checking
recovery. The SQL dump contains table definitions and data, including DROP TABLE
statements; never import it into a database whose existing contents you need.

Example recovery commands (substitute your backup path and new empty directory):

```sh
backup_path="/media/demo/backup-drive/spark-backups/spark-..."
./scripts/verify-backup.sh "$backup_path"

# CREATE DATABASE without IF NOT EXISTS refuses accidental reuse.
sudo mariadb -e 'CREATE DATABASE spark_restored;'
# Use pipefail so decompression/import failures are not hidden.
set -o pipefail
cat "$backup_path"/database.sql.gz.part-* | gzip -dc | sudo mariadb spark_restored

mkdir /home/demo/spark/storage/restored-files
cat "$backup_path"/files.tar.gz.part-* | gzip -dc | \
  tar --extract --sparse --file=- --directory=/home/demo/spark/storage/restored-files \
      --no-same-owner --no-same-permissions
chmod 700 /home/demo/spark/storage/restored-files
```

Grant the SPARK service account access to the restored database. For this host's
existing `demo` account (replace with your actual service account):

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON spark_restored.* TO 'demo'@'localhost';
```

Run that statement through `sudo mariadb`. The backup does not contain MariaDB
accounts/passwords/grants, server configuration, binaries, custom database
triggers, stored routines or scheduled SQL events. SPARK's shipped schema does
not use those custom database objects. On a replacement machine, install MariaDB
and the SPARK build dependencies and provision the local account first; the
existing `setup-db.sh` handles normal local-account provisioning.

Start the matching SPARK version with:

```sh
export SPARK_DB_NAME=spark_restored
export SPARK_FILE_DIR=/home/demo/spark/storage/restored-files
./build/spark_server
```

Its usual startup recovery handles any unfinished upload/deletion records captured
from a previous interruption. Check file names/directories and download sample
files (including edited text), and check project links/boards. Update service
configuration to those restored paths only after recovery is verified. Keep the
old data and backup until satisfied. Restoring a point-in-time database can move
notification history backwards; clients must discard their reconnect cursors and
perform a full REST resync. Older timer/reminder state may be present as of backup
time. For upgrades, apply the appropriate later migrations after restoration.

### Restoring multiple drives (v2)

Verify the backup and import `database.sql.gz.part-*` into a fresh database as above.
For each ID in `drives.tsv`, create an empty directory on the intended storage
filesystem and extract that drive's stream into it:

```sh
drive_id="REPLACE_WITH_ID_FROM_drives.tsv"
restore_root="/media/demo/data/restored-spark-files"
mkdir "$restore_root"
cat "$backup_path"/drive-"$drive_id".tar.gz.part-* | gzip -dc | \
  tar --extract --sparse --file=- --directory="$restore_root" \
      --no-same-owner --no-same-permissions
chmod 700 "$restore_root"
```

Repeat for every ID, retaining each extracted `.spark-drive` marker. Set the new
database name and every restored root in a JSON configuration's `storage.drives`
array, then launch with `--config`. Array order need not match the backup. Do not
merge drive archives into one root. For a complete restore, supply every archived drive, including ones that were
detached when the backup was made; reattach those through the drives API. Test downloads from every drive before switching production to the restore.

## Automated validation

`./scripts/test-local-db.sh` now also exercises backup creation and restore using
its private MariaDB instance and a temporary `/dev/shm` destination. It checks
mount/storage-lock rejection, new and remembered location selection, paths with
spaces, multi-chunk reassembly, corruption/missing chunks, failed-dump handling,
and restored metadata/downloads through the actual server API. It does not write
to the user's external drive or production database. Multi-drive tests also cover
legacy migration, placement, edits, project deletion across drives, missing/live
replacement roots, reordered roots, relocated restores and omitted backup drives.

## Dashboard backups

Open `/` and choose **Back up storage**. Select a previous successful location or
leave the mount blank for persistent storage, or enter an absolute mount path and optional destination directory. An external drive must
already be mounted and writable by the server's OS account.

SPARK returns HTTP 202, drains HTTP requests, stops its TCP notification service
and schedulers, releases the storage lock, runs `scripts/backup.sh`, and starts
its services again even if the backup fails. Ordinary REST returns 503 and TCP is paused during
this interval. The authenticated backup progress page remains available. Backup
status, live logs, completed archives and successful location history persist on disk. No other process should write to the database
or storage during backup. Configuration and database credentials come from the
running server environment. Every registered storage root (including runtime additions) is passed to the backup
script; dashboard backups include all drives automatically.

The dashboard also offers **Restart server**, which recreates the service objects
and connections within the running process. To deploy a newly built binary, stop
and launch the server as usual. Maintenance operations are serialized: a second
request receives HTTP 409 while one is queued. POST `/admin/restart` accepts `{}`;
POST `/admin/backup` accepts `{"mount":"/media/demo/drive","destination":"/media/demo/drive/spark-backups"}`
(the destination is optional). Both require `Content-Type: application/json` and
`X-SPARK-Admin: dashboard`. GET `/admin/status` exposes `state` (`idle`, `queued`,
`succeeded`, `failed`), `version`, and a result `message` when available;
GET `/admin/backup-locations` returns a `locations` array of mount/destination pairs.

These controls use the existing unauthenticated trust model: anyone able to reach
SPARK can invoke them. Cross-site browser mutation requests are rejected. Backup
scripts must remain at the source path used when building the server.

Runtime detachment does not remove a drive from full backups. Reconnect every
registered drive at its recorded path, or attach it at its new path, before backup.
An offline/missing drive fails the backup rather than silently creating a partial
recovery point. The database dump preserves drive letters and attachment state.

## Local backup catalog and inter-SPARK transfers (API 1.36.0)

The dashboard backup destination is optional. An empty request to `POST /admin/backup`
(with `Content-Type: application/json` and `X-SPARK-Admin: dashboard`) stores the
backup on the persistent drive in `.spark-backups`. An explicit `mount` and optional
`destination` preserve external-drive backups. The command-line equivalent is
`scripts/backup.sh --local`; `--choose` still selects an external location.
All registered source drives must be available for a complete backup.

Backup creation pauses ordinary REST and TCP to obtain the existing offline-consistent
snapshot. An authenticated maintenance page remains available; see visibility below. Transfers are separate
background jobs and leave ordinary services running. A local copy does not protect
against loss of the persistent drive; use Copy or Send for another physical copy.

`/backups/explorer` lists persistent-drive backups, incomplete attempts, sizes,
manifests and individual archive downloads. It offers deletion, verified copy/move
to another attached SPARK drive, and sending to a saved SPARK connection. Copy and
Send retain the source. Move verifies and flushes the destination before removing
the source. One transfer runs at a time; backup deletion and maintenance are blocked
until it finishes. Download all archive parts plus manifest and SHA256SUMS to restore.
The viewer does not inventory external copies or remotely received backups.

Both `.spark-backups` and `.spark-control` are excluded from every storage archive.
This prevents recursive backups and excludes outbound credentials and transfer
receipts. Existing backup formats and restore tools remain compatible.

### API

- `GET /backups`: persistent-drive catalog.
- `GET /backups/drives`: destinations available to backup administrators.
- `GET /backups/{id}/files/{name}`: stream an archive member.
- `DELETE /backups/{id}`: permanently delete a local complete or incomplete backup.
- `POST /backups/{id}/transfers`: `{ "action": "copy", "drive": "B" }`,
  `{ "action": "move", "drive": "B" }`, or
  `{ "action": "send", "connection_id": "..." }`; returns 202 with `job_id`.
- `GET /backups/jobs`: durable job state, stage, completed/total bytes and errors.
- `POST /backups/jobs/{id}/retry`: retry a failed or interrupted job.

Requires `admin.backup`; sending and retrying sends additionally require
`admin.connections`. A process restart marks unfinished jobs interrupted. A retry
rechecks the source and already-uploaded remote parts. It resumes between archive
parts (up to 1 GiB), not within an interrupted part. Integrity mismatches stop the
transfer instead of overwriting an unrelated remote file. Checksum verification is
repeated before transfer; the catalog's creation verification is not a continuous
integrity monitor. Creation failures retain an incomplete directory for inspection
or deletion.

### Saved connections

Visit `/connections/explorer` with `admin.connections`. Register a name, HTTPS
origin, remote device ID, provisioned token, remote drive letter, and base directory.
Saving or editing verifies the token before persisting it. Test refreshes its
permission snapshot. Deleting a connection removes the local secret but does not
revoke it on the remote instance. Secrets are held in a private owner-only file,
never returned through GET, and are not included in backups. Host administrators
and local processes running as the SPARK OS user can access them.

- `GET/POST /connections`: list/register.
- `PATCH/DELETE /connections/{id}`: edit/remove.
- `POST /connections/{id}/test`: refresh status and grants.
- `POST /auth/verify`: bearer token plus `{"device_id":"spark-a"}`. Returns only
  the token's own current permissions; no additional grant required. Wrong device
  returns 403; invalid/revoked token returns 401. Administrator tokens are rejected.
  Open mode explicitly returns `authenticated:false, auth_mode:open`.

Both instances need this release. Provision a dedicated device token on B with
`files.read` and `files.write`, then register it on A. Those grants remain global
file access on B; there is no directory-scoped authorization. Existing administrator
credentials need the new `admin.connections` grant (issue a replacement administrator
credential with the desired grants). Device credentials cannot receive admin grants.

Transfers require the `curl` executable and system CA certificates. Certificates
and hostnames are verified; HTTP, redirects and embedded URL credentials are not
accepted. Remote backups land in `{base}/{source-drive-id}/{backup-id}` on the
selected drive. A downloads each uploaded member as a stream and verifies SHA-256;
`SPARK-BACKUP-COMPLETE.json` is published only after all parts pass. Files are ordinary
shared files on B and can be downloaded through its file explorer. Connection
credentials, jobs, and source-instance identity remain separate from calendar sync
or other future connection uses.

## Estimates, confirmation and live visibility (API 1.37.0)

Both the dashboard and backup catalog now open `/backups/new`. Select a local or
external destination, choose **Estimate backup**, review the results, and select
**Confirm and start backup**. Changing the destination invalidates the estimate.
API clients may continue submitting directly to `POST /admin/backup`; preflight
space checks are repeated before the child backup process starts.

`POST /admin/backup-estimate` accepts the same optional `mount`/`destination` fields.
It returns per-drive logical and allocated data sizes, file counts, approximate
MariaDB table/index bytes, destination free space, a compressed-size range and a
conservative space reserve. Sparse holes are excluded from allocated data; existing
backups and private control data are excluded. Database size and compression are
estimates, not guarantees. No file contents are read for compression sampling.

The first duration estimate is `null` (displayed as **Not yet known**). Successful
backups record source/output sizes and elapsed time including verification; later
estimates provide a range based on the latest measurement. Different contents,
CPU load and destination hardware can change actual results. Transfer time is
separate. Historical stage durations are also retained for diagnosis.

During backup, SPARK releases its ordinary services and storage locks, then runs a
small HTTP maintenance listener at the same configured address and port. Existing
HTTPS proxies and browser sessions continue working. There can be a brief reconnect
at each listener handover. The maintenance listener serves `/backups/progress`,
`GET /admin/backup-status`, login/session routes, and legacy `/admin/status`.
Authorized ordinary REST requests receive `503 maintenance_in_progress` with
`Retry-After: 5`; TCP notifications remain paused. No domain handlers or schedulers
run during the offline snapshot. Normal operation returns after backup finishes.

The progress page shows stage (export, per-drive compression, checksums, archive
verification, finalization), elapsed time, heartbeat, process liveness, observed
activity, output size, checksummed-file count, and a bounded live log tail. Activity
is inferred from backup-process/descendant CPU counters and archive growth; liveness
or CPU activity alone does not prove useful progress. A quiet interval is reported
as **no activity observed**, not automatically treated as a failed backup.

All new pages and APIs require `admin.backup` in token mode. The maintenance listener
uses the same identity provider, browser sessions, CSRF rules and security headers.
Status/logs are not public. Ordinary endpoint permissions are still checked before
returning maintenance responses.

Progress is persisted atomically at least once per observation interval (about one
second) in the persistent drive's `.spark-control/backup-status.json`. Each backup
has an owner-only `backup-{id}.log`, and output is also streamed to the running
terminal/tmux session. The JSON includes `log_path` for manual inspection with
`tail -f`. A server restart marks an unfinished status **interrupted**. This does
not automatically remove or resume its incomplete archive. Timing history is in
`.spark-control/backup-history.json`; control files remain excluded from backups.
Logs are retained for diagnosis; there is no automatic log rotation in this version.
