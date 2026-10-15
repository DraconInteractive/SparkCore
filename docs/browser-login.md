# Browser login (step 4)

Token-mode SPARK now provides `/login`. Enter an **administrator** token to create
a browser session; device tokens continue to use REST bearer headers and TCP hello.
The files, projects/boards, commands, API reference and dashboard pages share the
session and include a Sign out button. Browser tokens are not saved in localStorage
or sessionStorage. Open-mode pages continue to work without signing in.

## Configure

Provision credentials using [the identity guide](identity-permissions.md), then add
an explicit browser origin to the instance's auth section:

```json
{
  "server": {"host": "127.0.0.1", "port": 47683, "notification_port": 47684},
  "auth": {
    "provider": "tokens",
    "credentials_file": "/home/demo/.config/spark/credentials.json",
    "browser_origin": "https://spark.example.com"
  }
}
```

`browser_origin` is optional: omitting it keeps token mode usable by native clients
but disables browser sessions. It must be a canonical HTTPS origin: lowercase host,
optional non-default port, no path/trailing slash, query, userinfo or fragment.
Use `https://localhost:8443` for a local HTTPS test proxy. Explicit `:443` and
zero-padded ports are rejected to avoid disagreement with browser Origin headers.

**HTTPS termination is required.** The [remote deployment guide](remote-deployment.md)
provides the proxy configuration; enable it on the intended host. The TLS proxy
must preserve the external Host header (including a non-default port), and the
backend listeners must be private/firewalled. SPARK compares Host and Origin with
configuration; it does not infer trust from Forwarded/X-Forwarded-* headers.
Origin checking is CSRF protection, not proof that a native client used TLS.
Use the [step 5 remote profile and proxy setup](remote-deployment.md) for HTTPS/TCP TLS. The login page refuses to submit
secrets from an HTTP page, and session cookies always carry Secure.

Before starting the new binary, stop the old server and run:

```sh
./scripts/setup-db.sh
./build/spark_server --config /path/to/instance.json --check-config
./build/spark_server --config /path/to/instance.json
```

Migration **019** adds administrator command requesters. Existing command history
and device idempotency keys are preserved. No live database is changed by building.

## Session contract

- `GET /login`: public sign-in page. Unauthenticated HTML navigation to an explorer,
  dashboard or API reference redirects here; API callers continue to receive 401.
- `POST /auth/login`: JSON `{"token":"<administrator-token>"}` with exact configured
  Origin/Host and `X-SPARK-Login: 1`. No CORS is enabled. Success sets
  `__Host-spark_session` with Path=/, Secure, HttpOnly, SameSite=Strict and a 12-hour
  Max-Age. A fresh random session and CSRF token are generated; the previous cookie's
  session is invalidated on successful re-login. Login does not grant permissions.
- `GET /auth/session`: returns mode, subject, explicit permissions and `csrf_token`
  for the current session. In open mode it returns `{"mode":"open"}`. Responses use
  Cache-Control: no-store. Merely reading this endpoint does not extend idle expiry.
- `POST /auth/logout`: requires the session, Origin and `X-SPARK-CSRF`; invalidates
  the session and expires the cookie. Logging out does not revoke the device/admin
  credential or sessions in other browsers.
- `/auth/client.js`: shared public browser helper. It adds CSRF headers to same-origin
  mutations and routes expired sessions to login; file XHR uploads use the same token.
  Downloads and previews receive cookies normally without putting secrets in URLs.

Sessions are held in process memory, limited to 256 active sessions, expire after
30 minutes without an authenticated API request unless “Remember this device” was
selected, and expire absolutely after 12
hours even if polling continues. A dashboard restart/backup preserves them; a full
process restart requires login again. Revocation or a permission/identity change
invalidates the session on its next request. In-flight HTTP operations may finish.

Cookie-authenticated POST/PUT/PATCH/DELETE requests require both the exact Origin
and a session-specific `X-SPARK-CSRF` header. Cross-site/same-site-other-origin
Fetch Metadata is rejected. This applies before body intake and again before JSON
mutations or upload publication. Duplicate session cookies/CSRF headers are rejected.
No cookie fallback occurs when an Authorization header is supplied: that request
must have a valid bearer token and follows native-client rules. Bearer credentials
are not accepted as browser session cookies.

Pages hide controls for missing permissions, while every server operation remains
authoritatively permission-checked. For full administration, issue the appropriate
permissions from `spark_credentials permissions`; there is no implicit administrator
superuser role. A typical read-only file user needs `discovery.read` and `files.read`.
Server controls use `admin.restart`/`admin.backup`, and their shared status requires
both permissions. Drive management requires `admin.storage`.

## Commands from the browser

A signed-in administrator submits commands with no `source_device`; the server sets
`source_admin` to the authenticated subject and stores `source_device: null`.
It does not register a synthetic device or require `devices.write`/`devices.delegate`
for command submission. `commands.invoke` is still required; `commands.read` enables
listing and result polling. Administrators cancel their own queued commands by
omitting `source_device` in the cancel packet too. A client-supplied `source_admin`
is rejected. Device requests retain their existing source-device checks and claims.

Idempotency keys are scoped to requester kind and subject, so an administrator and
a similarly named device cannot collide. Requester identity and results survive
restarts/backups. The command response now includes nullable `source_admin`.

## Verification

`browser_sessions` CTest covers cookie flags, origins, CSRF, logout, revocation and
idle/absolute expiry. `scripts/test-local-db.sh` includes a disposable database test
for session REST behavior, permission denial, administrator command attribution,
rotation and maintenance restart. Set `SPARK_TEST_FIREFOX=1` when running it to also
exercise the actual pages in an isolated Firefox profile through a temporary local
HTTPS proxy. This optional test requires Firefox and Python's `websockets` package;
the server and session implementation remain C++.

Security design references: [OWASP session management](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
and [OWASP CSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html).

The login checkbox “Remember this device” sends optional `remember_device: true`
to `POST /auth/login`. It disables the 30-minute idle timeout for that session,
while retaining the 12-hour absolute expiry. Omitted or false keeps the idle
timeout. Sessions remain in memory and do not survive a full server restart.
