# Runtime logging

SPARK writes newline-delimited JSON to stderr. Each record contains:

- `timestamp`: UTC ISO 8601 with milliseconds and an explicit `Z`.
- `level`: `info`, `warning`, or `error`.
- `component`: for example `calendar.scheduler`, `notification.poller`, or `http`.
- `pid`: process ID, useful for distinguishing restarts.
- `message`: fixed operational description.
- `details`: selected diagnostic fields, when applicable.

Writes are serialized across threads so separate components cannot interleave
their JSON records. Records are flushed immediately. UTC makes these timestamps
directly comparable with `journalctl --utc`; Perth time is UTC plus eight hours.

## Database interruptions

The first failed scheduler attempt emits a warning. Continued failures emit at
most one reminder per minute per worker. Recovery emits an informational record
with `failed_attempts` and `outage_ms`. The duration uses a monotonic clock and
measures time from the first observed failure until the first successful attempt;
it is not a precise measurement of the database's total downtime.

Database errors include `database_code`, `sqlstate`, `operation` and a safe
`database_reason` when available. Examples include:

- 2002: local socket connection failed.
- 2003: TCP connection failed.
- 1040: too many connections.
- 1045/1698: authentication denied.
- 1142/1143: permission denied.
- 1146: missing table.
- 1205/1213: lock timeout/deadlock.

Startup failures additionally identify the failing `check`, such as `calendar`.
Codes come directly from the MariaDB connector, including prepare/execute errors.
Native database error text, SQL, bound values, passwords, bearer tokens and HTTP
request bodies are not included. Arbitrary exception text is omitted because it
can contain user data; known configuration validation messages remain available.

## Other operational events

Startup logs identify authentication mode and listener ports. Managed listener
shutdowns, backup state/stage changes, and backup-transfer/calendar-sync job state
changes use the same format. This does not add HTTP access logging.

Detailed backup subprocess output remains in the existing private backup log and
the backup progress page. Runtime stderr records state/stage changes instead of
copying arbitrary child-process output into the log stream.

`--help`, `--check-config`, and the credential CLI retain their existing stdout
formats. In particular, `--check-config` still prints a standalone JSON document
and the credential helper still prints its token result.

## Capturing logs in tmux

Normal terminal launches display the records directly. To retain a copy:

```bash
./build/spark_server --config config/config.json --remote 2>&1 | tee -a ~/spark.log
```

SPARK does not manage or rotate that file. A future systemd deployment can capture
stderr directly into the journal. No database migration or new configuration
setting is required for logging; rebuild and restart SPARK.
