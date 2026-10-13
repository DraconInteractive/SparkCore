# Shared file storage

SPARK accepts any file type and has no application-level file size limit. Transfers
stream between the network and disk; the server does not buffer the whole file in
RAM. Disk capacity, filesystem/platform limits, and any proxy in front of SPARK
remain practical limits. Metadata is stored in MariaDB; bytes are stored under
the directories in the configuration's `storage.drives` array. Without a storage
configuration, `SPARK_FILE_DIR` still defaults to `storage/files` relative to the
working directory. Use stable absolute directories when running as a service.
See [multi-drive setup and placement](configuration.md). Each drive has its own directory tree, including empty folders; each file's `drive_id` identifies its storage
root without exposing its physical path. Use `drive=B` on uploads or file listings to select a drive letter.

Files form one shared collection without device assignments. In token mode,
`files.read`/`files.write` protect metadata, directories and content, and
`admin.storage` protects drive attachment/detachment. See [permissions](identity-permissions.md). Filenames are labels, not paths. Repeated names create distinct files
with distinct UUIDs. UTF-8 text can be edited in place through the explorer or the conditional content
replacement endpoint. `file.created`, `file.updated` and `file.deleted` notify clients about committed
changes; clients fetch metadata and refresh their file lists.

## Endpoints

| Method and path | Result |
| --- | --- |
| `POST /files?name=example.bin` | Stream an upload; 201 metadata and `Location: /files/{id}` |
| `GET /files` | List completed uploads with pagination |
| `GET /files/{id}` | Metadata for one completed upload |
| `GET /files/{id}/content` | Download current bytes; supports byte ranges |
| `PUT /files/{id}/content` | Save UTF-8 text with `If-Match`; 200 metadata and new ETag |
| `DELETE /files/{id}` | Remove metadata and stored bytes; 204 with no body |

GET endpoints also support HEAD; all routes support OPTIONS. The contracts are
available through `/api` and `/api/docs`.

## Upload

```sh
curl -X POST 'http://192.168.4.20:47683/files?name=example.bin' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @./example.bin
```

The request body is the original file, **not JSON, base64, or multipart form data**.
Use `application/octet-stream` for every file type, including images, documents,
archives, audio and executables. Send `Content-Length` or use HTTP chunked transfer
encoding. Empty files are supported. Content encoding other than identity is
rejected to avoid transforming the original bytes.

`name` is required, URL encoded, and must contain 1..255 valid UTF-8 bytes. Slashes,
backslashes, control characters and the names `.` and `..` are rejected. No other
query parameters beyond `name`, optional `directory` and optional `drive`, or duplicate parameters,
are accepted. The filename is never used as a
filesystem path. Stored files use generated UUIDs and private filesystem permissions.

Example metadata response:

```json
{
  "id": "12345678-1234-1234-1234-123456789abc",
  "name": "example.bin",
  "drive_id": "0123456789abcdef0123456789abcdef",
  "drive_letter": "A",
  "size_bytes": 1024,
  "content_revision": "",
  "content_type": "application/octet-stream",
  "download_url": "/files/12345678-1234-1234-1234-123456789abc/content",
  "created_at": "2026-09-19T00:00:00.000000Z"
}
```

`size_bytes` is the byte count as an unsigned 64-bit JSON integer. Clients handling
values above JavaScript's exact integer range must use a suitable integer parser.
Timestamps use UTC. The download URL is relative to this server.

The original 16 KiB body limit still applies to non-file REST requests, including
chunked JSON bodies. File transfers retain the server's five-second socket
inactivity timeout; there is no total transfer-duration limit imposed by this feature.

## List and download

```sh
curl 'http://192.168.4.20:47683/files?limit=100&offset=0'
curl 'http://192.168.4.20:47683/files/<id>'
curl 'http://192.168.4.20:47683/files/<id>/content' --output example.bin
# Resume a partially downloaded local file:
curl -C - 'http://192.168.4.20:47683/files/<id>/content' --output example.bin
curl -X DELETE 'http://192.168.4.20:47683/files/<id>'
```

Lists return `{ "files": [...], "limit": 100, "offset": 0, "has_more": false }`.
`limit` is 1..500 (default 100), `offset` is 0..1000000 (default 0). Results are
ordered by creation time then ID. Follow pages while `has_more` is true. Concurrent
collection changes can shift pages; refresh when needed.

Downloads return `application/octet-stream`, `Content-Disposition: attachment`
with the UTF-8 filename, `X-Content-Type-Options: nosniff`, `Accept-Ranges: bytes`,
and an ETag identifying the file and its current content revision. A valid Range request returns 206;
an unsatisfiable range returns 416. HEAD returns download headers without bytes.
Files are delivered as attachments, not rendered as pages within SPARK's origin.

## Failure and recovery

- Malformed filenames, parameters or IDs return 400. Unknown IDs return 404.
- Uploads without Content-Length or chunked framing return 411; unsupported
  content type or encoding returns 415.
- MariaDB or filesystem failures return 503. Exhausted disk space or quota returns
  507 where reported by the operating system.
- A broken upload is not listed. Its partial data is removed immediately when
  possible; startup recovery cleans uploads left unfinished by a crash or failure.
- Completed bytes and their directory entry are synced before metadata is marked
  ready. If the commit result is uncertain, bytes are retained: restart recovery
  preserves a committed ready file or removes an unpublished one.
- Deletion first hides the record, then removes bytes and metadata. An interrupted
  deletion is finished at startup. A download that already opened the file can
  complete while it is being deleted.
- Retrying a POST always creates a new ID. If the client loses the success response,
  an upload may already exist; list the collection before retrying if duplication
  matters. Resumable uploads are not provided; resumable downloads are supported.

Only one running SPARK instance can own a storage directory. Keep the directory
paired with its database; do not point another database at the same directory or
change the directory while existing file metadata is in use. Back up/restore both
MariaDB and the file directory together, preferably with SPARK stopped. Do not
modify stored files directly. Files that are externally removed or have a different size produce 503 on
download. Same-size external modifications are not detected; checksums are not
stored in this implementation.

## Activation

Stop SPARK, then run:

```sh
./scripts/setup-db.sh
export SPARK_FILE_DIR=/home/demo/spark/storage/files  # optional; shown explicitly for services
./build/spark_server
```

Setup applies the additive `009_files.sql` migration and runtime permissions.
Startup checks the schema, creates the storage directory if necessary, obtains its
exclusive lock, and recovers unfinished operations. No external storage service is
required. The isolated integration suite is `./scripts/test-local-db.sh`.

## Explorer and directories (API 1.14.0)

Open `/files/explorer` in a browser to browse folders, upload files (including drag
and drop), view details, download, move or delete files. Uploads show progress and
can be cancelled. The explorer asks for confirmation before deletion. File names
open details with previews for common text and image formats. UTF-8 text up to 2 MiB can be viewed and edited. Larger text files have read-only
first/last 64 KiB previews; image previews are limited to 10 MiB. These preview limits do not
limit uploads or downloads. Other formats can be downloaded to open locally.
HTML and SVG are displayed as text, never executed as pages.

Directories are **logical paths**, stored in MariaDB, not physical paths exposed
from the server. Existing files are assigned to `/`. File metadata now includes
`directory`. Paths are absolute, case-sensitive UTF-8 strings of at most 1024 bytes,
with each component at most 255 bytes. Use `/` for root, or `/folder/subfolder`.
Empty components, trailing slashes, `.`/`..`, backslashes and control characters
are rejected. Duplicate filenames within the same directory remain allowed.

- `POST /files?name=notes.txt&directory=%2Fdocuments`: upload into `/documents`.
  Omitting `directory` keeps the existing root default. Missing parent folders
  are created automatically.
- `GET /files?directory=%2Fdocuments`: list files directly in `/documents`, with
  normal pagination. Without the filter, the endpoint still lists all files.
- `PATCH /files/{id}` with `{"directory":"/archive/2026"}`: move the file.
  Missing parents are created in the same transaction. File ID, download URL,
  bytes and creation timestamp remain unchanged. Repeating the same move is a no-op.
- `GET /files/directories`: returns `{"directories":["/","/documents",...]}`,
  sorted by path, including persistent empty directories.
- `POST /files/directories` with `{"directory":"/documents/manuals"}`: create
  that directory and its parents. Returns 200 and the directory; idempotent.

Directory creation and move requests require `Content-Type: application/json`.
Invalid paths return 400 `invalid_directory`; moving a missing or non-ready file
returns 404 `file_not_found`. The explorer loads files in pages of 100 and its
search filters loaded files. Directory paths can be bookmarked in the URL hash.
The sidebar has a **Show files** checkbox, remembered in this browser. When enabled,
it lists files under their directories; clicking a filename opens the viewer/editor.
It follows file-list pagination independently of the main table. Refresh and file
changes reload the sidebar.

Folders are retained when their last file is moved or deleted. Folder deletion
and folder renaming are not provided in this release.

Apply `010_file_directories.sql` using `./scripts/setup-db.sh` while SPARK is
stopped, then restart the rebuilt server. File bytes stay in their existing
storage location; no physical file migration is required.

## Text viewer/editor (API 1.21.0)

Click a filename in `/files/explorer`, then **Edit text**. **Save changes** updates
that shared file; its ID, name, directory, creation time and download URL stay
unchanged. **Reload** fetches the current contents, with confirmation before
unsaved edits are discarded. Closing the editor or leaving the page also protects
unsaved edits. Text is displayed safely, including HTML and SVG. Extensionless
UTF-8 text is supported; binary or invalid UTF-8 content remains download-only.

Editing is limited to complete UTF-8 files of at most **2 MiB**, including any BOM.
LF and CRLF line endings and UTF-8 BOMs are preserved. Mixed line endings and legacy
CR-only files are read-only to avoid silently converting their contents. Large
logs have **First 64 KiB**, **Last 64 KiB** and **Reload** controls. These are snapshots,
not live tailing, and partial previews cannot be saved. Uploads/downloads remain
uncapped. This browser accesses SPARK shared files, not arbitrary machine log paths.

`PUT /files/{id}/content` accepts raw text with `Content-Type: text/plain` or
`text/plain; charset=utf-8`. Send Content-Length or chunked framing and the exact
quoted **ETag** returned by `GET /files/{id}/content` in **If-Match**. It returns
200 metadata (including `content_revision`) and a new ETag. Metadata for old uploads
has an empty revision until first edited. Missing If-Match returns 428, an outdated
or wildcard tag returns 412 `file_changed`, invalid UTF-8 or control characters
other than tab/CR/LF return 400 `invalid_text`, and text exceeding 2 MiB returns 413.
Empty text is allowed. Missing files return 404. A conflicting editor retains its
unsaved text so the user can copy it before reloading and comparing.

Saves write and sync a new private blob before transactionally switching the
MariaDB content revision and size together. Existing downloads retain their old
open descriptor. Failed/uncertain commits retain versions until recovery; startup
removes versions not referenced by committed metadata. Deletion removes all
versions. A committed save emits `file.updated`. If a save loses its response, reload to determine
whether it committed before trying again.

Stop SPARK, apply `014_file_revisions.sql` with `./scripts/setup-db.sh`, and restart
the rebuilt server. The migration is additive and existing blobs stay compatible.

## Change notifications (API 1.23.0)

Completed uploads emit `file.created`; text saves and directory moves emit
`file.updated`; logical deletion emits `file.deleted`. Their `resource` is
`/files/{id}`. All have null `source_device` and reach all TCP clients, including
through reconnect catch-up. Contents never appear in notifications. Fetch metadata
and refresh affected lists; if you do not know a file's previous directory, refresh
all cached directory listings. Content changes require reloading an open preview;
do not overwrite unsaved editor text. Existing ETag conflict checks still apply.

`files.directories_updated` uses `/files/directories` and covers explicit folder
creation, automatically created parents, and recursive project-directory removal.
Repeated mkdir or unchanged moves produce no new event. A text save creates a new
revision and emits an update even if its bytes happen to match the previous version.

Events commit with metadata changes. Upload-start and partial/failed uploads do
not produce `file.created`. Folders created at upload start are independently
committed and can remain after an upload fails; their directory notification is
valid. Deletion is announced when the file becomes unavailable through REST;
physical cleanup may finish later or on restart, without another delete event.
Project deletion that retains files does not announce them as deleted.

The HTML explorer does not connect to raw TCP; its existing Refresh controls still
apply. Native TCP clients can react to these events immediately. No new database
migration is needed for notification coverage.

## Create an empty file

In `/files/explorer`, **Create File** sits beside **Upload files**. Its modal asks
for a filename and an extension (default `txt`) and previews the current folder's
destination path. Confirm to create an empty file, then click its name to edit it.
A blank extension creates an extensionless file. Existing same-name files remain
untouched. The list and sidebar refresh after creation. This uses the normal upload
API and emits `file.created`. Project directories work the same way: open them
through the project explorer's **Open files** link.

## Runtime drives

- `GET /files/drives` lists registered identities with `letter`, `path`, `persistent`,
  `attached`, `online` and `available_bytes` (null when unavailable).
- `POST /files/drives`, JSON `{"path":"/media/demo/SSD/spark-files"}`, attaches an
  existing mounted directory and returns its drive record (201). A new storage
  directory must be empty; known drives reconnect with their existing contents.
- `DELETE /files/drives/B` detaches B without deleting bytes or metadata (204).
  Detach waits for file transfers to complete; the permanent server drive returns
  409. Use the OS to safely eject the disk afterward.
- `GET /files?drive=B&directory=/documents` filters by drive and logical directory.
  Omitting `drive` lists across all drives, including offline metadata.
- `POST /files?name=notes.txt&drive=B` places a new file on B. Unknown letters return
  404; offline destinations return 503. File metadata includes `drive_letter`.

In the explorer, choose a drive in the left sidebar. Upload/Create File target the
selected drive. The explorer defaults to persistent storage. Files show their letters and
an offline label. Attach and detach controls are beside the selector. Refresh
updates connection state. Empty folders belong to their selected drive.
Registered drive letters remain reserved across SSD swaps and restarts. Preserve
`.spark-drive` markers; these identify storage roots, not hardware serial numbers.
See [configuration](configuration.md) for migration and persistent storage setup.

## Drive-owned directories (API 1.30.0)

`GET /files/directories?drive=B` lists B's folder tree, including empty folders.
Omitting the drive selects persistent storage. The response contains `drive_id`
and `directories`. Offline trees remain readable from MariaDB.

`POST /files/directories` accepts `{"directory":"/notes/empty","drive":"B"}`.
Folders with the same path on A and B are independent. Uploads create parent folders
only on the destination drive. PATCH `/files/{id}` moves the file within its current
drive; it does not change physical drives. Folder creation and file moves require
that drive online, otherwise 503.

Uploads without a drive now use a uniquely matching project drive, or persistent
storage for non-project paths. Ambiguous project paths on multiple drives return
409 `drive_required`. There is no capacity/offline fallback to another drive.
GET `/files` without a drive still lists all files for existing API clients.
Project links open `/files/explorer?drive=B#<encoded-directory>`.

Migration 018 assigns existing global folders and projects to persistent storage.
Already assigned file locations are preserved; the migration never moves or
relabels physical blobs on another drive. Backups include the new directory and
project ownership through their normal database dump.

## Rename and delete folders (API 1.35.0)

`PATCH /files/directories` accepts JSON such as
`{"drive":"A","directory":"/notes","destination":"/archive/notes"}`.
It moves the entire logical subtree on the same drive, creating missing destination
parents. File IDs, content and download URLs stay unchanged. The destination must
not exist or be inside the source. Paths are case-sensitive and limited to 1024
UTF-8 bytes, including each resulting descendant path.

`DELETE /files/directories` accepts
`{"drive":"A","directory":"/notes","recursive":false}` and returns 204.
Nonempty folders return 409 `directory_not_empty`; explicitly use `recursive:true`
to permanently delete their files and subfolders. The drive root is protected.
Both operations require `files.write` and an online drive; omitted `drive` selects
the persistent drive. Missing folders return 404 `directory_not_found`.

Folders that are project roots or contain project roots return 409
`directory_in_use`. Ordinary folders inside projects can be managed normally.
To delete a project and its directory, use the project deletion endpoint instead.

Metadata and notifications commit together. Clients receive
`files.directories_updated` with `drive_id`, plus `file.updated` or `file.deleted`
for affected completed files. Deletion byte cleanup resumes on restart if interrupted;
a 503 during cleanup can mean metadata deletion has already committed.
Already-open downloads may finish. Folder mutations wait for active file transfers.

In `/files/explorer`, each folder row offers **Rename / Move** and **Delete**.
Deletion defaults to empty folders; a checkbox explicitly enables deleting contents.
