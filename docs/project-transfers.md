# Project ZIP export and import

The Projects explorer has **Export project ZIP** on the selected project and
**Import project ZIP** above the project list. Export includes the selected
project and all descendants, GitHub URLs, boards, columns, tasks (including
severity), empty directories, and every completed file below the project's
stored directory on its drive. Archives are compressed ZIPs with a versioned
`manifest.json` and `data/<file UUID>` entries verified by SHA-256.

Export runs in the background and publishes a normal downloadable file in
`/project-exports` on persistent storage. It can then be downloaded, leased,
transferred, or deleted through the existing files API. Polling shows bytes
processed. Project/file operations wait while a consistent export is made.

Import uploads the ZIP through the normal files framework to `/project-imports`
on the selected drive. It prepares a review without modifying project data.
The uploaded ZIP is retained even after cancellation or a completed import;
delete it through Files when no longer needed.

## Review and matching

Select a drive and optionally explicitly select the current project as the
target. Automatic matching prefers the root UUID on that drive, then a root
with the same case-sensitive name. Subprojects, boards, columns and tasks match
UUID first, then name within their parent. Ambiguous names are rejected instead
of guessing. Files match their relative directory and filename. Duplicate
filenames at the same path are rejected, since they cannot be safely matched.
Project directory names are preserved, including directories left unchanged by
project renames. A standalone export of a subproject becomes a root on import.

New items default to **Import**, existing items to **Keep local**. Every item
can be inspected and individually selected. **Replace with incoming** updates
an existing item. **Discard incoming branch** skips that item and its incoming
descendants. It never deletes existing target records or files. Local-only
items remain. Keeping a parent retains its metadata while permitting changes
to its children. To skip the whole branch use Discard.

**Merge text** is offered only for conflicting UTF-8 text files of at most
1 MiB on each side, without binary control bytes. Read-only local/incoming
panes and an editable final result support manual merging. There is no reliable
common ancestor in a portable archive, so no automatic three-way merge is
attempted. Binary files may be kept or replaced but cannot be merged.

Metadata and file publication are committed together with notifications in one
MariaDB transaction. File bytes are staged first. Changing target data after
review invalidates that review (`409 import_target_changed`); prepare a fresh
review to include those changes. The comparison conservatively covers the
whole target drive. New objects retain archive UUIDs; matching existing objects
retain target UUIDs. Creation/update timestamps reflect the import. UUID
collisions outside the target scope fail rather than overwrite unrelated data.

## API

- `POST /projects/{id}/export`, JSON `{}` → `202` transfer job.
- `POST /projects/imports`, JSON `{"file_id":"uploaded ZIP UUID","drive":"A"}`
  with optional `target_project_id` → `202` transfer job.
- `GET /projects/transfers/{id}` → state, processed bytes, result and review
  entries once ready. States: preparing, review, applying, complete, failed.
  Export completion supplies `result.file` (a standard Files record).
- `GET /projects/transfers/{id}?text={entry.key}` → local/incoming text for a
  mergeable file conflict.
- `POST /projects/transfers/{id}/apply`, JSON
  `{"resolutions":{"files:source-uuid":{"action":"merge","content":"final text"}}}`.
  Actions: keep, import, discard, merge. Omitted entries use their review defaults.
  Metadata does not support the merge action. Returns the completed job.
- `DELETE /projects/transfers/{id}` dismisses a finished/reviewed job and removes
  temporary staging, preserving original uploads and published exports.

Export/status require projects.read, boards.read, files.read, files.write.
Import/apply additionally require projects.write and boards.write. No new
permission or database migration is required. Every request uses normal token
or browser-session authentication and CSRF protection. Jobs are private to the
initiating credential (shared in open mode), expire after one hour, and are
cancelled by server restart. At most two preparations and sixteen retained jobs
are allowed; dismiss finished jobs to free a slot.

Only SPARK project-format ZIPs are supported. Links, special files, encrypted
entries, path traversal, duplicate ZIP entry names and unreferenced payloads
are rejected. Limits: 8 MiB manifest, 10,000 manifest
items, project depth up to 64, and 16 MiB apply request (including edited text). There is no fixed ZIP or payload-file size limit. Unpacking checks available
staging space and keeps a 64 MiB reserve. Choose a temporary filesystem with
enough room using the server process’s `TMPDIR` environment variable; imports
need space for expanded files as well as the eventual copy on the target drive. Interrupted processes may leave
private `spark-project-*` staging directories in the OS temporary directory;
normal shutdown and dismissal remove them. Superseded file revisions are removed after successful publication. Bytes from
uncertain database commits are retained until startup recovery can safely
reconcile them with MariaDB.

## Build and deployment

Install `libarchive-dev` alongside existing dependencies, then configure and
build normally:

```sh
sudo apt-get install libarchive-dev
cmake -S . -B build
cmake --build build -j2
```

Restart SPARK to enable API version 1.49.0. No database setup is needed for this
feature if migration 024 (task severity) is already applied.

`./scripts/test-local-db.sh` includes two-instance transfer tests, rollback and
malformed-archive checks. Set `SPARK_TEST_FIREFOX=1` for explorer coverage.
