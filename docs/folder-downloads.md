# Resumable folder downloads

In `/files/explorer`, select a drive and directory and click **Download folder**.
Confirm recursive inclusion, wait for checksum preparation, then click **Download
ZIP**. All files in that directory and its subdirectories are included, regardless
of the explorer's loaded page or search filter. Empty directories are preserved.
The selected directory's contents are relative to the ZIP root.

SPARK generates an uncompressed ZIP64 stream, following the
[PKWARE ZIP format specification](https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT).
No complete archive or copy of the source data is stored. Preparation reads each
file once to compute CRC32. Streaming uses a fixed-size data buffer plus metadata
proportional to the number of entries. ZIP64 supports files/archives beyond 4 GiB
and more than 65,535 entries. Preparation rejects duplicate paths rather than
silently overwriting files on extraction. Up to 100,000 entries per session and
64 retained sessions are supported; there is no fixed file-size cap (the server's
64-bit addressable stream size applies). One preparation runs at a time.

## API

All routes require `files.read`. In token mode, sessions belong to the credential
that created them; the URL is not a bearer credential. Browser downloads use the
existing authenticated session cookie. Device clients use Authorization: Bearer.

- `POST /files/downloads` with `{"drive":"A","directory":"/backups"}` returns
  202 and a session ID, `state`, progress counters, `expires_at` (Unix seconds),
  and `download_url`.
- `GET /files/downloads/{id}` polls `preparing`, `ready`, `failed`, or
  `interrupted`. `prepared_bytes` / `total_bytes` describes the checksum scan.
  A ready session includes `archive_bytes`.
- `GET /files/downloads/{id}/content` streams the ZIP. HEAD returns its length.
  `Accept-Ranges: bytes`, a strong ETag, and HTTP byte ranges allow download tools
  to resume using the **same URL**. Send `If-Range` with the ETag; a mismatch returns
  the full representation. Unsatisfiable ranges return 416.
- `DELETE /files/downloads/{id}` discards a finished session only, never source
  files. Preparing sessions return 409.

Ready sessions last 24 hours from creation and survive full server restarts.
A restarted server marks unfinished preparations interrupted; create a new
session to retry. Expired manifests are cleaned on initialization and creation.
Metadata is private under persistent storage `.spark-control/downloads`, excluded
from backups along with other runtime control data. No database migration is needed.

The browser tab remembers its latest session ID so refreshing the explorer can
restore preparation status. Download/resume itself is managed by the browser or
client tool. **Dismiss** hides this panel without deleting the session, allowing
an existing browser download to resume. A browser login may expire sooner than
the download session; sign in again with the same credential before resuming.

A session represents the files selected during preparation. New files are not
added to an existing session. Before serving it, SPARK checks that selected file
IDs, locations, content revisions and filesystem metadata still match. Deleted,
moved, edited or unavailable source files reject the request. In-stream file
changes or read failures terminate the response; it cannot be safely completed
as a valid ZIP. Create a fresh session after changing the source. This is not a
filesystem snapshot, and external programs must not rewrite managed blob files.
Completed backups make good stable sources for resumable downloads.
