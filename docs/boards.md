# Project boards and tasks

Each project (including each subproject) can own multiple named Kanban boards.
Boards start empty. Add named columns and tasks with a name and description.
These tasks are independent of daily tasks: they never reset at midnight.

All IDs are lowercase UUIDs. The API consistently scopes resources to their
owning project and board; unrelated IDs return 404. Names are nonblank strings
of at most 200 UTF-8 bytes. Task descriptions are required at creation (empty is
allowed), at most 4000 UTF-8 bytes. Board names are unique within a project and
column names within a board, case-sensitive. Task names need not be unique.

## API

Use `/projects/{project_id}/boards` as the collection and
`/projects/{project_id}/boards/{board_id}` as the board path below.

| Method/path | Behavior |
| --- | --- |
| `GET /projects/{project_id}/boards` | All board summaries in `{"boards":[...]}` |
| `POST /projects/{project_id}/boards` | Create with `{"name":"Development"}`; 201 |
| `GET {board}` | Complete board, columns and tasks; 200 |
| `PATCH {board}` | Rename with `{"name":"Release"}`; 200 |
| `DELETE {board}` | Remove board and all columns/tasks; 204 |
| `POST {board}/columns` | Add with `{"name":"To do"}`; 201 |
| `PATCH {board}/columns/{column_id}` | Rename with `{"name":"In progress"}` |
| `DELETE {board}/columns/{column_id}` | Delete empty column; 204 |
| `POST {board}/columns/{column_id}/tasks` | Create with name and description; 201 |
| `GET {board}/tasks/{task_id}` | Retrieve task; 200 |
| `PATCH {board}/tasks/{task_id}` | Edit name/description or move via `column_id` |
| `DELETE {board}/tasks/{task_id}` | Remove task; 204 |

POST/PATCH require `Content-Type: application/json`. DELETE takes no body.
PATCH preserves omitted fields and requires at least one editable field. Unknown
fields and query parameters are rejected. Names can contain slashes since they
are display labels, not directories. All JSON bodies retain the 16 KiB limit.

Example move:

```sh
curl -X PATCH 'http://192.168.4.20:47683/projects/PROJECT_ID/boards/BOARD_ID/tasks/TASK_ID' \
  -H 'Content-Type: application/json' -d '{"column_id":"DESTINATION_COLUMN_ID"}'
```

The destination must belong to the same board. Moves retain the task ID, name,
description and creation time. Sending the same state again is a no-op and does
not change timestamps or emit another notification. Board, column and task
responses include IDs, ownership IDs, and UTC creation/update timestamps. A full
board response has this shape (abbreviated metadata):

```json
{
  "id": "board-uuid",
  "project_id": "project-uuid",
  "name": "Development",
  "columns": [
    {
      "id": "column-uuid",
      "board_id": "board-uuid",
      "name": "To do",
      "tasks": [
        {
          "id": "task-uuid",
          "board_id": "board-uuid",
          "column_id": "column-uuid",
          "name": "Implement mobile timer view",
          "description": "Display running timers"
        }
      ]
    }
  ]
}
```

Board lists, columns and tasks are ordered by creation time then ID. Moving a task
retains this ordering key. No custom reordering is implemented. Board lists and
full board reads are unpaginated and complete; project shallow/deep reads retain
their existing project hierarchy structure. Fetch boards through the routes above.

Deleting a non-empty column returns `409 column_not_empty`; first move or delete
its tasks. Deleting a board deletes all its columns and tasks. Deleting a project
or ancestor also deletes the boards of every deleted project, regardless of file
retention. Database foreign keys enforce ownership and cascading cleanup.

Missing IDs return 404 (`project_not_found`, `board_not_found`, `column_not_found`,
`task_not_found`), malformed input 400, duplicate board/column names
`409 name_conflict`, wrong content types 415, and database failures 503. Reads
support HEAD and all routes OPTIONS.

## Notifications and persistence

Committed changes emit `board.created`, `board.updated` or `board.deleted` through
the existing TCP channel, to all connected devices. Any column/task mutation emits
`board.updated`. `resource` is `/projects/{project_id}/boards/{board_id}`;
`source_device` is null. Clients fetch that path to refresh the whole board;
`board.deleted` means remove it locally. Project subtree deletion emits
`board.deleted` for each contained board. Writes and notifications commit in the
same transaction. Failed writes do not partially move tasks or delete boards.
Actual board mutations also update the owning project's `updated_at` and emit
`project.updated` for that project.

The notification welcome includes `/projects`. On reconnect, fetch relevant
projects, their board lists and active boards when `resync_required` is true.
Protocol-2 clients can instead resume notification delivery using `after_sequence`.
Boards, columns and tasks survive server and MariaDB restarts.

## Explorer and upgrade

Select a project in `/projects/explorer` to manage its boards. Create/select/rename
boards, add/rename columns, add/edit/delete tasks, and move tasks using the column
selector in Edit / move. Deleting a board requires confirmation. The page refreshes
after its own changes; use Refresh boards for changes from other clients.

Stop SPARK, run `./scripts/setup-db.sh` to apply `013_project_boards.sql` and grants,
then restart `./build/spark_server`. Existing projects are preserved and initially
have no boards. API version 1.19.0; server version 0.17.0. Run
`./scripts/test-local-db.sh` for integration and restart persistence checks.
