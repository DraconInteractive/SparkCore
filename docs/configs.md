# Device configs

Configs are database-backed JSON objects, independent of server startup configuration.
Use `/configs/explorer` to create, filter, edit, import, export and delete them.
The dashboard links to the explorer. `/api` and `/api/docs` describe the REST contract.

Each config has a stable UUID, name, description, tags, data, revision and UTC creation/update timestamps.
Names need not be unique. Tags are case-sensitive selection labels, not permissions.
Devices should discover a suitable config, retain its ID, and fetch it on subsequent starts.
Creating again creates a distinct config; declaring defaults must not overwrite human edits.
Multiple filter matches are returned explicitly rather than choosing a hidden winner.

| Endpoint | Purpose |
| --- | --- |
| `POST /configs` | Create with `name` and object `data`; optional `description`, `tags`, `source_device` |
| `GET /configs` | Metadata list; exact `name`, repeated `tag` (all must match), `limit` (1–100, default 50), `offset` |
| `GET /configs/{id}` | Full record including data and revision |
| `GET /configs/{id}/content` | Raw JSON object for download or device consumption |
| `PATCH /configs/{id}` | Change fields, supplying current `revision` in the JSON body |
| `DELETE /configs/{id}?revision=N` | Delete at the current revision |
| `GET /configs/explorer` | Human manager and JSON editor |

Example creation:

```json
{
  "name": "watch-display",
  "description": "Production watch settings",
  "tags": ["watch", "production"],
  "data": {"brightness": 70, "clock_format": "24h"}
}
```

`data` replaces the entire object when patched. A stale revision returns HTTP 409
with `error: revision_conflict` and `current_revision`. Reload and reconcile before
retrying. Identical edits do not increase the revision or emit a notification.
Individual record and raw-content reads supply an ETag; the exact value can be
sent as `If-None-Match` to get HTTP 304 when unchanged.

Data is limited to 64 KiB of compact UTF-8 JSON, requests to 128 KiB, and nesting
to 64 levels. Duplicate object keys are rejected. Names are nonblank, at most
200 bytes; descriptions at most 4,000 bytes. Up to 32 distinct tags are allowed,
each 1–64 ASCII characters (letters, digits, `_`, `.`, `:`, `-`, starting with a
letter or digit). Tags are stored sorted.

`configs.read` permits list/read/export and the explorer; `configs.write` permits
creation, changes and deletion. These permissions cover all configs on an instance,
not only configs tagged for a particular device. Do not put secrets in configs
accessible to devices that should not see them. Existing administrator tokens need
these new permissions before they can grant them through the credential UI.
The administrator token helper issues a new token with all current permissions.

Changes emit `config.created`, `config.updated`, or `config.deleted`, atomically
with the database change. Notification payloads contain only `config_id` and
`revision`; the resource points to `/configs/{id}`. Subscribers need both
`notifications.read` and `configs.read`. Devices fetch the current record through
REST. Deletion carries the previous revision plus one. Reconnect catchup uses the
existing notification log.

Configs are included in database backups. They are not writable files-area copies:
parallel editing through file endpoints would bypass validation, revision checks
and config notifications. The editor supports importing raw JSON into a draft and
exporting saved JSON instead. This feature does not add inter-instance config sync.

## Deployment

Rebuild and run `./scripts/setup-db.sh` before restarting. Migration
`022_configs.sql` creates the table; setup also grants application-user access.

## Future: optional schemas

A later addition may associate an optional JSON Schema with a config or config
family, validating field types, ranges and required properties and allowing a
form-based editor. Schema versioning, migration of existing data, and defaults
need an explicit design before introduction. Current configs accept arbitrary
JSON objects; devices are responsible for interpreting their contents.
