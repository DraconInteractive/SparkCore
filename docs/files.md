# Shared file storage

SPARK accepts any file type and has no application-level file size limit. Transfers
stream between the network and disk; the server does not buffer the whole file in
RAM. Disk capacity, filesystem/platform limits, and any proxy in front of SPARK
remain practical limits. Metadata is stored in MariaDB; bytes are stored under
`SPARK_FILE_DIR`, default `storage/files` relative to the server's working directory.
Use a stable absolute directory when running SPARK as a service.

All devices share the same collection, with no authentication or device
assignments. Filenames are labels, not paths. Repeated names create distinct files
with distinct UUIDs. File contents are immutable; upload another file to replace content,
then delete the old ID when appropriate. No TCP notification is emitted for file
changes; clients list the collection to discover them.

## Endpoints

| Method and path | Result |
| --- | --- |
| `POST /files?name=example.bin` | Stream an upload; 201 metadata and `Location: /files/{id}` |
| `GET /files` | List completed uploads with pagination |
| `GET /files/{id}` | Metadata for one completed upload |
| `GET /files/{id}/content` | Download original bytes; supports byte ranges |
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
query parameters beyond `name` and optional `directory`, or duplicate parameters,
are accepted. The filename is never used as a
filesystem path. Stored files use generated UUIDs and private filesystem permissions.

Example metadata response:

```json
{
  "id": "12345678-1234-1234-1234-123456789abc",
  "name": "example.bin",
  "size_bytes": 1024,
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
and an ETag derived from the immutable file ID. A valid Range request returns 206;
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
open details with previews for common text and image formats. Text previews show
at most 64 KiB; image previews are limited to 10 MiB. These preview limits do not
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
Folders are retained when their last file is moved or deleted. Folder deletion
and folder renaming are not provided in this release.

Apply `010_file_directories.sql` using `./scripts/setup-db.sh` while SPARK is
stopped, then restart the rebuilt server. File bytes stay in their existing
storage location; no physical file migration is required.
