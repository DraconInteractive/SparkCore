# Instance configuration

SPARK can load a JSON configuration file:

```sh
./build/spark_server --config /etc/spark/config.json
```

Use [the open-mode example](../config/open.example.json) as a starting point. It
binds to all interfaces for LAN use. The [token-mode example](../config/tokens.example.json)
binds to loopback and requires a provisioned credential file. Configure the
appropriate interface for your network. Use [the remote profile](remote-deployment.md) for encrypted remote deployment.

## Current scope

Steps 1–5 implement configuration, shared identity/permissions, and REST/TCP
enforcement. A configuration without `auth` starts in the existing open mode.
With `auth`, every REST endpoint requires a bearer token and TCP hello requires a
`token`. Invalid/unreadable authentication configuration fails startup; there is
no fallback to open mode. Browser sessions require `auth.browser_origin`;
[remote deployment](remote-deployment.md) protects both channels with TLS. See [identity and permissions](identity-permissions.md).

Storage supports up to 64 attached roots, plus a registry of previously used
removable drives. Apply migration 018 using `./scripts/setup-db.sh` before launching
the new binary. Keep the server's internal storage first in `storage.drives` on
first startup/upgrade: it becomes the persistent drive and cannot be detached.
Its persistent identity remains authoritative after configuration reordering.

Each storage root has a `.spark-drive` marker; MariaDB gives that identity a stable
letter (`A` through `Z`, then `AA`, `AB`, etc.). Letters are local to this SPARK
instance, never reused, and unrelated to Linux mount paths or Windows drive letters.
Preserve markers when moving or restoring storage. Existing files retain their
IDs and are assigned to their actual drive during migration.

Use `POST /files/drives` to attach an already-mounted directory at runtime, and
`DELETE /files/drives/{letter}` to detach it without deleting anything. These
operations are also available in `/files/explorer`. SPARK does not mount, format
or eject disks. Detach before using the OS's safe-eject operation. A different SSD
mounted at the same path gets a different identity and letter; reconnecting the
original restores its original letter.

Paths and explicit detach state are saved in MariaDB and survive process/dashboard
restarts. Configuration supplies initial roots; it does not override explicit
runtime detachment. Enabled registered roots are retried at startup. Missing
removable drives remain offline and do not prevent startup. The persistent server
drive must be available. For runtime reconnection after an unexpected disconnection,
use Attach again if the drive remains offline.

Metadata for offline files remains queryable, including via `GET /files?drive=B`.
Reads, edits and deletion requiring offline bytes return 503. Project deletion
with `destroy_directory=true` also fails before deleting project data if its files
are on an offline drive. Logical moves only change metadata; they do not transfer
files between physical drives. Each drive has its own folder tree, including empty folders.

Uploads may specify `drive=B`; otherwise SPARK selects a uniquely matching project
drive, or persistent storage. Ambiguous project paths require an explicit drive.
A selected drive that is offline or full never falls back to another drive. Files are stored whole, edits stay on the assigned drive,
and existing data is not automatically rebalanced or replicated. Capacity checks
are best effort: concurrent writes or unknown-length streams can still receive
507 when capacity runs out. Removing a drive does not free its letter or erase its
registry entry. Full backups still require every registered drive to be present;
see [backup and restore instructions](backups.md).

## Schema

All sections are optional; the configuration must be a JSON object. Unknown fields,
duplicate keys, wrong types, comments and trailing commas are rejected. Files must
be readable regular files, at most 1 MiB. Paths/ordinary strings must be nonempty
(except the database socket), at most 4096 bytes, without NUL, tabs or newlines.
No storage directories are created during parsing or `--check-config`.

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 47683,
    "notification_port": 47684
  },
  "database": {
    "host": "localhost",
    "port": 3306,
    "name": "spark",
    "user": "demo",
    "socket": "/run/mysqld/mysqld.sock"
  },
  "storage": {
    "drives": ["/srv/spark/files"]
  }
}
```

- `server`: optional `host`, `port`, `notification_port`, `tls_notification_port`. Ports are JSON integers
  1..65535, and the effective HTTP and notification ports must differ.
  `tls_notification_port` opts into the remote proxy profile: token auth, HTTPS
  `browser_origin`, host `127.0.0.1`, and four distinct public/backend ports are
  required. Keep `--remote` in remote launches to fail closed if this profile is
  accidentally removed. See [deployment](remote-deployment.md).
- `database`: optional `host`, `port`, `name`, `user`, `socket`. An empty socket
  leaves socket selection to the connector. The database password remains in
  `SPARK_DB_PASSWORD`; a `password` field is intentionally not accepted.
- `storage`: when present, requires a nonempty `drives` array of path strings.
  Each is a file-blob storage directory, not a logical `/files` directory. Existing
  entries must be directories; a missing directory is created during startup only
  for the first root of a new instance. Aliases, duplicate and nested roots are rejected,
  including aliases through existing symlinks. Validation does not mount drives,
  verify external mounts/capacity, or write data. Replace the earlier proposed
  `storage.directory` with `storage.drives`; the single-path key is rejected.
- `auth`: if present, must be an object with the required fields
  `provider: "tokens"` and `credentials_file: "path/to/credentials.json"`, plus
  optional `browser_origin: "https://spark.example.com"` for browser sessions.
  See [browser login](browser-login.md) for origin validation and proxy requirements.
  Null, empty, false, unsupported providers and unknown keys are errors.
  The referenced file must be a readable regular file containing a nonempty JSON
  object without duplicate keys, at most 1 MiB. Step 2 validates the complete
  [credential schema and private file permissions](identity-permissions.md). Passing these checks
  does not test a specific token, database connectivity or encrypted transport.

Do not put real tokens into example files or commit instance secrets. Validation
messages do not echo document contents, credential records or database passwords.

## Precedence and paths

Listener settings: explicit `--host`, `--port`, `--notification-port` options
 override file values, which override built-in defaults (`0.0.0.0`, 47683, 47684).
A malformed value in the file is still an error even if a CLI value would override
it. Duplicate CLI options, unknown flags and missing values are rejected.
There are no new listener environment variables.

Database settings: explicit file fields override corresponding `SPARK_DB_HOST`,
`SPARK_DB_PORT`, `SPARK_DB_NAME`, `SPARK_DB_USER`, `SPARK_DB_SOCKET` values. Defaults
remain localhost, 3306, spark, `$USER` (or spark), and the default socket. Passwords
come only from `SPARK_DB_PASSWORD` (default empty).

Storage: `storage.drives` overrides `SPARK_FILE_DIR`. Without either, the default
is `storage/files` in the working directory. Empty `SPARK_FILE_DIR` retains that
same default. Existing CLI-only and environment-based launches remain supported.
`.env.local` is not loaded automatically; source/export it as before.

Relative paths explicitly written in the configuration resolve beside the
specified configuration file. If the configuration is a symlink, use the directory
of the specified symlink path as the base. Relative environment paths resolve
against the process working directory. Paths are normalized, resolving existing
symlinks. No shell expansion (`~`, `$HOME`, etc.) occurs within JSON strings.

Authentication has one authority: the `auth` section in the explicitly loaded
configuration. There are no CLI or environment switches to override/disable it.
No file is implicitly discovered: omitting `--config` preserves the legacy open
launch. An explicitly requested missing/unreadable file is a fatal error.

The file is loaded once before database checks, storage creation, worker threads
or listeners. Effective database and storage settings are applied to the process
and inherited by dashboard backup subprocesses. Dashboard service restarts reuse
this snapshot. Editing the file requires a full process relaunch to take effect.
The standalone `scripts/backup.sh` does not parse instance JSON: supply matching
`SPARK_DB_*` settings and repeat `--file-dir` for every configured drive. Single-drive
launches can still use `SPARK_FILE_DIR`. `SPARK_FILE_DRIVES` is an internal runtime
export, not a configuration input; use JSON for initial roots and the drives API for runtime changes.

## Check configuration without starting services

```sh
./build/spark_server --config config/open.example.json --check-config
```

This returns a redacted JSON summary with `configuration_valid`,
`runtime_supported`, `startup_blockers`, resolved listener settings/storage paths,
`auth_mode` (`open` or `required`) and `remote`. Remote profiles also report
`browser_origin` and `public_notifications`. No database connection, directory creation
or network listener is attempted.

Exit code 0 means configuration and credential validation passed. Both open and
valid token configurations now report `runtime_supported: true` with no startup
blockers. This does not mean the service is safe for unencrypted public access.
Malformed/unreadable configurations also exit 2. `--help` must be used alone.

This is not a database-connectivity, storage-writeability or TLS validation test.
Startup still performs its normal database, storage and binding checks afterward.

## Verification

`ctest --test-dir build -R '^configuration$' --output-on-failure` tests defaults,
precedence, relative paths, duplicate/unknown keys, invalid auth, unreadable and
nonregular files, drive aliases/overlap, secret redaction and token-mode validation.
Python 3 is used only as a test harness; SPARK remains C++.

`./scripts/test-local-db.sh` additionally launches a real configured instance with
conflicting inherited settings and verifies database/storage selection, dashboard
restart snapshot semantics and backup configuration propagation, alongside all
existing command, notification and storage tests.
