# Identity, permissions and transport enforcement (steps 2–4)

Implemented as a transport-independent C++ library in `src/auth.hpp`, with local
credential provisioning through `build/spark_credentials`. REST/TCP enforcement
is implemented: configuring `auth` now starts an authenticated instance.
Open instances retain existing behavior. Step 4 requires migration 019 for administrator command attribution.

## Identities and credentials

An identity contains credential ID, subject, kind (`device` or `administrator`),
and explicit permissions. A device subject is its registry ID; an administrator
subject is an operator name. Subjects use 1–128 ASCII letters, digits, `. _ - : @`.
Multiple credentials may represent the same subject for independent rotation.
No device record is created by issuing a credential.

Tokens are generated with OpenSSL's secure random generator: 32 random bytes,
hex-encoded after `spark1_`. Only a SHA-256 digest is stored; token verification
uses constant-time digest comparison. These are generated high-entropy secrets,
not passwords. There is no custom-token or password import option.

The versioned JSON credential store has exactly this structure:

```json
{"version":1,"credentials":[{
  "id":"32 lowercase hexadecimal characters",
  "kind":"device",
  "subject":"watch-1",
  "token_sha256":"64 lowercase hexadecimal characters",
  "permissions":["events.read","notifications.read"],
  "revoked":false
}]}
```

The example describes the format; use the tool to generate valid records. Unknown
fields/permissions, duplicate keys/IDs/hashes, wrong types and malformed records
are rejected. Empty stores and identities without permissions are valid and grant
nothing. Limits: 1 MiB, 1,024 records. The store must be a regular file owned by the
process user, without group/other permissions (normally `0600`). Keep its parent
directory private and outside shared SPARK file storage. Run the tool as the same
OS user as SPARK. There is deliberately no HTTP provisioning endpoint yet.

```sh
mkdir -m 700 ~/.config/spark
./build/spark_credentials init ~/.config/spark/credentials.json
./build/spark_credentials issue ~/.config/spark/credentials.json device watch-1 events.read notifications.read
./build/spark_credentials issue ~/.config/spark/credentials.json administrator owner discovery.read admin.restart admin.backup
./build/spark_credentials list ~/.config/spark/credentials.json
./build/spark_credentials revoke ~/.config/spark/credentials.json CREDENTIAL_ID
./build/spark_credentials permissions
```

`issue` prints the token exactly once: save it in the client's secret storage.
`list` excludes both tokens and hashes. Tokens are never supplied in CLI arguments.
Initialization refuses to overwrite an existing store; updates serialize using a
sibling lock file and atomically replace the store after syncing. Revocation is
idempotent for an existing ID. Rotate by issuing a new token, updating the client,
then revoking the old ID. Protect credentials and backups as sensitive data;
restoring an older credential file can restore credentials previously revoked.

`--check-config` now checks the full schema and file permissions. Valid credentials
now report `runtime_supported: true` and an empty `startup_blockers` array.
OpenSSL development headers (`libssl-dev`) are now required to build.

## Explicit permission matrix

Permissions apply to a resource family across this SPARK instance. There are no
implicit role grants, wildcards or per-project/drive ACLs in this version. An administrator
must receive each permission explicitly. Only administrators may hold `admin.*`.
Unknown permission names deny access, even through the open provider.

These rules are enforced by the shared route allowlist:

| Permission | Operations |
| --- | --- |
| `discovery.read` | Health/version/API discovery, documentation and dashboard entry |
| `devices.read` | Device details |
| `devices.write` | Register, modify or delete a device; additionally requires acting as that device |
| `devices.delegate` | Explicitly act as another device, including source attribution; does not grant an operation permission |
| `pulse.read`, `pulse.write` | Read pulse settings/state, modify settings or subscription; subscription changes additionally require device identity |
| `timers.read`, `timers.write` | Read timers, create/change/acknowledge/stop timers |
| `events.read`, `events.write` | Read calendar series/occurrences, create/change/delete series |
| `tasks.read`, `tasks.write` | Read daily tasks, create/change/delete/check tasks |
| `files.read`, `files.write` | Read metadata/directories/content, upload/create/edit/move/delete files or folders |
| `projects.read`, `projects.write` | Read projects/hierarchies, create/change/delete projects |
| `boards.read`, `boards.write` | Read boards/columns/tasks, create/change/move/delete them |
| `commands.read` | Inspect advertised commands and executions |
| `commands.invoke` | Submit an execution request |
| `commands.advertise` | Register/update/remove commands, additionally acting as their device |
| `commands.execute` | Claim/renew/complete execution, additionally acting as the eligible executing device |
| `notifications.read` | Connect and replay, additionally requiring the relevant resource read permission |
| `admin.restart`, `admin.backup` | Restart service, manage/run backups; `/admin/status` requires both because it exposes shared maintenance results |
| `admin.storage` | Attach/detach storage roots (listing requires `files.read`) |
| `admin.credentials` | Reserved for future remote credential administration; local tool is controlled by OS file permissions |

Indirect operations combine permissions: project file listings/download links need
`projects.read` and `files.read`; creating a project's folder needs `projects.write`
and `files.write`; deleting a project requires `projects.write` and `boards.write`
because boards/tasks cascade, and destroying its directory also needs `files.write`. Browser
pages do not exempt their underlying API/content requests from authorization.
All supplied `source_device` values must match the device identity or require
`devices.delegate`. Command lease/claim tokens remain additional proof, never a
replacement for authenticated identity. Unspecified routes deny by default; no
domain endpoints are public in token mode. Login and browser bootstrap routes are
explicit exceptions with their own session/origin policy.

Notifications require `notifications.read` and the domain's read permission, for
both live delivery and replay. Pulse subscription and command recipient/eligibility
filters also apply. Disclosure of command availability must be limited to eligible
executors with `commands.execute`; auth must not broaden broadcast recipients.

## Provider contract and browser decision

The open provider resolves an anonymous open identity and permits known operations,
preserving legacy behavior. The token provider returns no identity for invalid,
revoked, missing or unreadable credentials; it never falls back to open mode.
It rereads the complete store on each authentication and `current(identity)` call.
A permission change invalidates an existing identity snapshot; callers must resolve
again. This initially favors straightforward revocation over caching/performance.
Callers must use the provider's identity, never construct identities from request
JSON. `permitted` and `acts_as` are independent checks; an ownership match alone
never grants a resource operation.

REST authenticates each request, with an additional check after JSON body intake
and after streamed uploads before publishing. TCP rechecks identities before
sending live/replayed data. Revoked/changed TCP identities are disconnected.
In-flight HTTP operations and already-open downloads may finish; revocation is
not a transaction cancellation mechanism.
Step 4 implements administrator-token login with server-side sessions and CSRF
protection. See [browser login](browser-login.md) for HTTPS origin configuration,
expiry, cookies, revocation and explorer behavior.


## Client protocol (API 1.33.0)

Configure token mode explicitly:

```json
{"server":{"host":"127.0.0.1","port":47683,"notification_port":47684},
 "auth":{"provider":"tokens","credentials_file":"credentials.json"}}
```

REST clients send `Authorization: Bearer <token>` on every request. No bearer token is
accepted through query parameters or cookies. Browser sessions use their own
Secure HttpOnly cookie and require CSRF/Origin checks on mutations. Missing/invalid/revoked credentials
return `401 {"error":"unauthorized"}` with `WWW-Authenticate: Bearer`; insufficient
permissions return `403 {"error":"forbidden"}`. Impersonating another device returns
403 `device_identity_mismatch`. HEAD uses the GET policy. Unknown routes/methods,
including OPTIONS, deny by default in token mode. Duplicate Authorization headers
are rejected. `/api` reports `authentication: "bearer"` and required permissions
for each endpoint; conditional ownership and file-destruction checks also apply.

TCP hello adds the token to the existing packet:

```json
{"type":"hello","device_id":"watch-1","token":"<device-token>","after_sequence":42}
```

`after_sequence` remains optional. Authentication, `notifications.read`, and device
identity are required before welcome/replay. Failed authentication produces an
`unauthorized` error and closes the connection; missing permissions or wrong
device identity produces `forbidden`. No credential is echoed. Welcome resources
are filtered by read permissions. Existing subscription and command recipient
filters still apply, as do domain permissions, for live and replayed events.
`command.available` additionally requires `commands.execute` and `commands.read`.
Sequence checkpoints can advance past events hidden by permissions. When gaining
permissions, refresh the newly accessible REST resources or deliberately replay
from an earlier cursor; previously skipped events are not automatically replayed.

Browser login is now available when `auth.browser_origin` is configured. HTTPS
termination is supplied by the [remote deployment profile](remote-deployment.md); see also [browser setup](browser-login.md).

`./scripts/test-local-db.sh` runs open-mode regressions and the disposable
`test-authentication.py` integration suite, including every documented route,
HEAD aliases, device ownership, streams, commands, TCP replay/revocation, and
maintenance restart. No production data or credentials are used.
