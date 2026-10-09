# Projects

Projects are persistent references to ongoing work. They contain a UUID `id`,
`name`, `description`, `directory`, nullable `github_url`, nullable `parent_id`,
`subprojects`, and UTC
`created_at`/`updated_at` timestamps. GitHub URLs are references only: SPARK does
not fetch or clone repositories.

| Endpoint | Behavior |
| --- | --- |
| `POST /projects` | Create project and directory; 201 with record and Location |
| `GET /projects` | Paginated list ordered by case-sensitive name then ID |
| `GET /projects/{id}` | Retrieve complete project record |
| `PATCH /projects/{id}` | Update supplied fields; 200 with updated record |
| `DELETE /projects/{id}` | Delete with explicit directory retention choice; 204 |

```sh
curl -X POST http://192.168.4.20:47683/projects \
  -H 'Content-Type: application/json' \
  -d '{"name":"SPARK","description":"Cross-device intelligence hub","github_url":null}'
```

Creation requires `name` and `description`. The name is nonblank, at most 200
UTF-8 bytes, and a single directory component: no slash, backslash, control
characters, `.` or `..`. Description may be empty, at most 4000 UTF-8 bytes.
`github_url` defaults to null or accepts an HTTPS `github.com/owner/repository`
URL (optional trailing slash), at most 2048 bytes. Other Git hosts, query strings,
and fragment links are not accepted. Set it to null to clear it.

The server assigns the logical file directory `/projects/{name}`. This is a path
in SPARK's file API, not a host filesystem path. Creation and directory metadata
commit together. A pre-existing directory is reused with all its contents intact.
Project names are unique among siblings on the same drive (including roots);
assigned directories are unique per drive and case-sensitive. Collisions
return `409 project_conflict`.

`directory` and `parent_id` are read-only after creation. PATCH can change name, description and GitHub URL.
Renaming keeps the existing directory so file references stay stable. Omitted
fields are preserved; identical updates preserve `updated_at`. Unknown fields
are rejected. JSON requests retain the 16 KiB body limit.

List responses use `{"projects":[...],"limit":100,"offset":0,"has_more":false}`.
`limit` is 1–500, default 100; `offset` is 0–1000000, default 0. Follow `has_more`
and increment offset by limit. Concurrent edits can change pages.

Use `GET /files?directory=...` to list a project's immediate files (URL-encode the
path), `GET /files/directories` to discover subdirectories, and existing file
upload/download/move endpoints to manage its contents. Folders appear in the file
explorer. Projects emit committed `project.created`, `project.updated` and `project.deleted`
notifications; TCP clients can refresh on these hints.

## Deletion

Deleting a project deletes its entire descendant project tree, regardless of the
file-retention choice. It does not promote children to roots. A JSON boolean is
mandatory; omission is an error:

```sh
# Delete project and descendant references, keeping directories and all files:
curl -X DELETE http://192.168.4.20:47683/projects/PROJECT_UUID \
  -H 'Content-Type: application/json' -d '{"destroy_directory":false}'

# Delete the project and its entire directory tree:
curl -X DELETE http://192.168.4.20:47683/projects/PROJECT_UUID \
  -H 'Content-Type: application/json' -d '{"destroy_directory":true}'
```

Destruction includes every nested file/folder, including files uploaded before
the project was created. Other directories, including names sharing only a text
prefix, are preserved. Already-open downloads may finish. A later upload or
project creation can recreate the deleted path.

Project deletion, directory removal, and marking files for deletion commit in one
MariaDB transaction. Physical bytes are removed after commit. If interrupted,
remaining files stay hidden and their bytes are removed on server restart using
the existing file recovery mechanism. A 503 during deletion can therefore mean
the project was already deleted; GET by ID establishes whether it still exists.
File requests share a storage gate; project mutations wait for in-progress file
requests and exclude new ones until finished. Long uploads can delay deletion.

Missing projects return `404 project_not_found`; malformed IDs, fields and
parameters return 400, incorrect content types return 415, and database/storage
failures return 503. Reads support HEAD; open mode also supports OPTIONS. Token mode enforces
project, board and file permissions, including cascade and shorthand operations;
see [permissions](identity-permissions.md).

## Upgrade

Stop SPARK, run `./scripts/setup-db.sh` to apply `011_projects.sql`, `012_project_hierarchy.sql`, and grants,
then start `./build/spark_server`. Build with `cmake -S . -B build` and
`cmake --build build -j2`. Existing file data is preserved. The new endpoints are
included in `/api` and `/api/docs` (API 1.18.0, server 0.16.0).

## Browser explorer

Open `/projects/explorer` to browse and filter loaded projects, create or edit
entries, open their file directories and GitHub repositories, or delete them.
Deletion defaults to retaining files; choose permanent directory deletion explicitly
in the dialog. Load more retrieves subsequent pages. Project selections have
shareable URL fragments. The page requires JavaScript and uses the REST API.
Use New subproject on a selected project to create a child. Parent and child links
let you navigate the hierarchy. The deletion dialog applies to all descendants.


## Subprojects and recursive reads

Existing projects remain top-level projects. Create a child by passing an existing
project UUID as `parent_id` on `POST /projects`; omit it or use JSON null for a root:

```json
{
  "name": "SPARK Mobile",
  "description": "Mobile client",
  "parent_id": "12345678-1234-1234-1234-123456789abc",
  "github_url": null
}
```

If the parent directory is `/projects/SPARK`, this creates
`/projects/SPARK/SPARK Mobile`. Children can have their own children. The full
logical path must fit the existing 1024 UTF-8 byte file-directory limit; creation
fails with `400 directory_too_long` if it cannot. Renaming any project leaves its
own and all descendant directories unchanged. New children use the parent's
stored directory, not its display name. Changing parents is not supported.

`GET /projects/{id}?depth=shallow` is the default and returns immediate child IDs:

```json
{
  "id": "12345678-1234-1234-1234-123456789abc",
  "name": "SPARK",
  "parent_id": null,
  "directory": "/projects/SPARK",
  "subprojects": ["22345678-1234-1234-1234-123456789abc"]
}
```

`GET /projects/{id}?depth=deep` returns complete child records recursively:

```json
{
  "id": "12345678-1234-1234-1234-123456789abc",
  "name": "SPARK",
  "parent_id": null,
  "directory": "/projects/SPARK",
  "subprojects": [
    {
      "id": "22345678-1234-1234-1234-123456789abc",
      "name": "SPARK Mobile",
      "parent_id": "12345678-1234-1234-1234-123456789abc",
      "directory": "/projects/SPARK/SPARK Mobile",
      "subprojects": []
    }
  ]
}
```

Both examples abbreviate the other fields: every actual record also includes
`description`, `github_url`, `created_at`, and `updated_at`. Leaves have an empty
`subprojects` array in both modes. Child arrays are sorted by name then ID. Deep
reads include every descendant in one response, with no pagination or silent
truncation. The current PoC reads all project rows in one consistent SELECT and
assembles the requested subtree; large installations may need a more selective
query. Files themselves are never embedded.

Creation, PATCH and collection responses use shallow records. GET collection
still lists all projects by default for compatibility. Use
`GET /projects?parent_id=null` for roots or `GET /projects?parent_id={uuid}` for
immediate children; usual pagination applies. A nonexistent parent filter returns
an empty collection. Duplicate or unsupported query parameters are rejected;
`depth` accepts only `shallow` or `deep` on detail reads. Invalid parent UUIDs
return `400 invalid_parent_id`, missing parents on creation return
`404 parent_not_found`, and invalid depth values return `400 invalid_depth`.

Creating or deleting a child updates the immediate parent's `updated_at`.
Descendant metadata deletion and file deletion markers share one transaction;
failure rolls the entire tree back. With `destroy_directory:false`, all directory
and file data remains accessible through the file API, but all descendant project
records are removed. Recreating a project in a retained folder does not recreate
its previous child project records automatically.

## Kanban boards

Projects and subprojects each own their own named boards, columns and tasks.
Use `/projects/{id}/boards` or the Boards section in `/projects/explorer`.
Project deletion removes all boards and tasks on the deleted project tree,
regardless of file retention. See [the board API](boards.md) for routes, task
moves, column deletion rules and TCP notifications.

## Project file shorthand

`GET /project/{id}/files` (singular `project`) lists files directly in the
project's stored directory, exactly like `GET /files?directory=...`. It accepts
`limit` (1–500, default 100) and `offset` (0–1000000, default 0) and returns the
standard `{files, limit, offset, has_more}` packet. Nested/subproject files are
excluded. Renames retain the same directory. Missing projects return
`404 project_not_found`; malformed IDs return `400 invalid_project_id`.
HEAD and OPTIONS are supported. Restart the rebuilt server to activate; no new
database migration is required.

## Project change notifications (API 1.23.0)

`project.created`, `project.updated` and `project.deleted` use `/projects/{id}`,
with null `source_device`. Metadata changes emit updates; identical PATCH requests
do not. Creating/deleting a subproject also updates its immediate parent. Board,
column and task mutations update their owning project as well as emitting the
existing board notification, matching the project's updated timestamp.

Clients should refresh project lists and invalidate cached deep project trees
that contain the affected project. A deletion includes `project.deleted` for
every descendant and `board.deleted` for each contained board. When
`destroy_directory:true`, each previously visible file gets `file.deleted` and
removed directories cause `files.directories_updated`. With false, files and
directories remain and get no removal notifications. All these events and the
subtree deletion commit or roll back together. Deferred byte cleanup emits no
extra deletion notifications. All events participate in reconnect catch-up.

No new schema migration is required; restart the rebuilt server. The browser
explorer continues to use its Refresh controls; raw TCP notifications are for
native clients and do not automatically update browser pages.

Use **Open files** to open the project's directory in `/files/explorer`. Its
**Create File** button (next to **Upload files**) creates an empty file in the
current directory, with a filename and extension defaulting to `txt`.

## Project storage drives

POST `/projects` accepts an optional `drive` letter (for example `"B"`). Root
projects default to the persistent server drive. Subprojects inherit the parent's
drive; explicitly selecting another returns 409 `parent_drive_mismatch`. Creation
requires an online drive. The drive is fixed at creation, like the physical
storage directory; PATCH edits project metadata and does not migrate files.

Every project record includes `drive_id`, `drive_letter` and live `drive_online`.
These also appear on nested deep-read records. Names and paths can be reused on
separate drives. GET `/project/{id}/files` filters by the project's drive and path,
returns its availability, and preserves offline file metadata for browsing.

While offline, project details, metadata edits and boards/tasks remain available.
Creating subprojects, uploading/editing/moving/deleting files, creating folders,
and destroying a project directory require its drive online (503 otherwise).
Deleting a project with `destroy_directory:false` remains possible offline and
retains the files/folders. Destructive deletion is scoped to the project's drive,
including child projects; matching paths on other drives are unaffected.

The Projects explorer offers a drive picker when creating root projects, shows
drive availability, and links to the correct drive in the Files explorer.
Migration 018 assigns existing projects and global directory records to persistent
storage. Existing file identities and physical locations remain unchanged.
