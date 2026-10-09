# Encrypted remote deployment

Already using Caddy? Follow the [Ubuntu VPS coexistence guide](caddy-vps-deployment.md)
instead of installing the combined proxy on its ports.

Step 5 supplies a guarded SPARK configuration and a standalone nginx proxy for
HTTPS and TLS-encrypted notification sockets. The application remains C++.
Nothing is installed, exposed or changed on an existing instance automatically.
Use [remote.example.json](../config/remote.example.json) as the starting point.

The supported topology is one trusted machine:

```text
Devices / browsers -- HTTPS :48443 ------ nginx -- HTTP 127.0.0.1:47683 -- SPARK
Devices ----------- TCP over TLS :47685 - nginx -- TCP  127.0.0.1:47684 -- SPARK
```

Public ports are configurable. HTTPS may use 443 instead. Both TLS listeners
use the hostname in `auth.browser_origin` and the same certificate. This profile
requires browser login to be configured even for an initially device-only deployment.
It intentionally uses four distinct ports, avoiding wildcard/loopback bind conflicts.

## Configuration and startup guards

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 47683,
    "notification_port": 47684,
    "tls_notification_port": 47685
  },
  "storage": {"drives": ["/srv/spark/files"]},
  "auth": {
    "provider": "tokens",
    "credentials_file": "/etc/spark/credentials.json",
    "browser_origin": "https://spark.example.net:48443"
  }
}
```

Keep your actual database/socket/user and storage configuration. Provision device
and administrator credentials using [the credentials CLI](identity-permissions.md).
Use separate identities with only the required permissions. Apply existing migrations
through 019 before starting an upgraded instance; step 5 adds no database migration.

`server.tls_notification_port` activates the remote profile. It requires token
authentication, an explicit HTTPS browser origin and **exactly** `127.0.0.1` as the
backend host. Invalid configuration fails before starting any service. CLI listener
overrides are checked too. Always retain `--remote` in the production launch:
removing the remote field or auth section will then fail closed instead of silently
starting a LAN/open instance. Ordinary launches without this profile retain their
existing open/token behaviour.

```sh
./build/spark_server --config /etc/spark/config.json --remote --check-config
./build/spark_server --config /etc/spark/config.json --remote
```

`--check-config` validates the profile and reports `remote`, `browser_origin` and
`public_notifications`. It does not check certificates, start nginx, inspect firewall
rules or prove that an already-running process uses the file. Configuration changes
require a full SPARK process restart, not the dashboard's service restart.

## Proxy preparation

nginx needs both HTTP SSL and the stream SSL module. On Debian/antiX the packages
are `nginx` and `libnginx-mod-stream`. Package installation can start the distribution's
default nginx service: review that service and disable its default website/listeners
before publishing anything. The generated file is a **standalone configuration**;
do not include it inside an existing `http` block or a distro `sites-enabled` file.
If this host already uses nginx, have its operator integrate both top-level blocks
and audit every other listener/site instead of starting a competing service.

Supply a valid certificate **full chain** and private key for the configured hostname.
Use a public CA or a private CA installed on every client. Keep the key readable only
by the proxy account (or root master), never world-readable or in SPARK file storage.
Certificate acquisition/renewal belongs to your chosen CA tooling. DNS validation
can avoid opening port 80. The test suite uses an explicitly trusted temporary CA;
that is not a production certificate.

Run the following from the repository as the account that can validate SPARK's
private credentials. Use real absolute paths. The output path must not already
exist; render a new candidate when changing configuration.

```sh
python3 scripts/configure-remote.py \
  --config /etc/spark/config.json \
  --certificate /etc/spark/tls/fullchain.pem \
  --key /etc/spark/tls/privkey.pem \
  --runtime /var/lib/spark-proxy \
  --output /etc/spark/nginx.conf
```

The generator calls the C++ server's `--remote --check-config`, then renders the
validated origin and ports. It does not start services or copy credentials into
nginx. `--server` selects another built binary; `--stream-module` overrides the
Debian default `/usr/lib/nginx/modules/ngx_stream_module.so`. The runtime directory
must be writable by the proxy account. If rendering as the SPARK user, choose a
writable output/runtime location and have your service operator install/assign them.

Validate and launch nginx as its service account:

```sh
nginx -t -c /etc/spark/nginx.conf
nginx -c /etc/spark/nginx.conf
```

The generated config runs in the foreground, suitable for tmux, runit or another
supervisor; it makes no systemd assumption. High public ports allow a non-root
proxy account. Keep SPARK and nginx as separate supervised processes; retain
`--remote` for every SPARK relaunch. Do not give nginx access to credential files,
storage roots or database credentials. After certificate renewal, run `nginx -t`
and reload the existing proxy (`nginx -c /etc/spark/nginx.conf -s reload`). A dashboard
SPARK restart leaves nginx running and browser sessions intact; brief 502 responses
and TCP disconnect/reconnect during maintenance are expected.

## Network and proxy boundary

- Publish/forward only the chosen HTTPS and TCP TLS ports. Block backend ports,
  MariaDB and unrelated services at the host/provider firewall. Never forward or
  tunnel a public listener to 47683/47684 directly. Loopback is a local trust boundary;
  processes on the same host can reach the authenticated plaintext backends.
- nginx listens on IPv4 and IPv6. Audit both; do not publish an AAAA record until
  the IPv6 routing and firewall are ready. The profile assumes same-host proxying,
  not an unencrypted hop between containers or separate hosts.
- There is no HTTP port-80 redirect, alternate website, static-file alias, WebSocket
  listener or unauthenticated health bypass in the generated configuration.
- The exact external Host is preserved (including a nondefault port). Unexpected
  Hosts return 421. Forwarded and X-Forwarded-* identity/origin headers are stripped;
  SPARK never trusts them. Bearer headers, cookies, Origin and CSRF headers reach the
  shared application authorization layer. Device requests must send the configured Host.
- TLS 1.2/1.3 are enabled for **both** channels; clients must validate the certificate
  chain and hostname. Never retry plaintext or disable verification after TLS failure.
- Upload/download buffering is disabled and no upload-size ceiling is introduced.
  Timeouts apply to stalled transfers. Notification heartbeats keep the TLS connection
  active; continue the existing ping/pong and reconnect/backoff protocol.
- Access/request error logging is disabled in the generated proxy to avoid retaining
  credentials accidentally placed in URLs. Startup/config diagnostics still go to
  the launching process. Any future logging must exclude tokens, cookies and bodies.
  HTTPS sends HSTS for one year; use the final intended HTTPS hostname.

After starting, inspect `ss -ltnp`: SPARK's two listeners must be **127.0.0.1 only**;
nginx alone should own the public ports. Inspect `ss -lxnp`/the MariaDB configuration
as appropriate. From another machine, verify that backend ports are unreachable,
HTTPS `/health` without credentials returns 401, browser login works, and TCP TLS
rejects an unauthenticated hello. Test both IPv4 and IPv6 when available. These
operator checks are necessary because repository tests cannot audit your router,
provider rules, other proxies or unrelated services.

## Client discovery

API version **1.33.0** adds remote notification discovery:

```json
{
  "transport": "tcp+tls",
  "host": "spark.example.net",
  "port": 47685,
  "tls": {"required": true, "minimum_version": "1.2", "verify_certificate": true}
}
```

These fields appear in the existing `/api` `notifications` object alongside framing,
hello and replay details. The host is derived from `auth.browser_origin`; the port
is the public proxy port. Plain configurations continue to advertise `transport: tcp`
and their backend notification port. Update clients to understand `tcp+tls` before
moving them to this profile. Open a TLS socket with the advertised hostname for SNI
and certificate verification, then send the unchanged newline JSON hello containing
the device token. Browser sessions do not authenticate device TCP sockets.

## Verification

Build normally, install/provide nginx plus its stream module, then run:

```sh
SPARK_TEST_NGINX=/usr/sbin/nginx \
SPARK_TEST_NGINX_MODULE=/usr/lib/nginx/modules/ngx_stream_module.so \
./scripts/test-local-db.sh
```

The suite uses disposable MariaDB, storage, credentials, certificates and ports.
It runs the open-mode suite, direct token-mode tests, the **same authenticated
REST/TCP suite through real nginx TLS**, and browser-session tests. TLS checks cover
all documented protected routes, claims, streamed upload revocation, live/replayed
notification filtering, credential revocation, maintenance/reconnect, HTTPS browser
login/CSRF/logout, verified certificates, incorrect hostnames, plaintext rejection,
spoofed proxy headers and actual IPv4/IPv6 backend listener bindings. Without
`SPARK_TEST_NGINX`, the suite explicitly reports that TLS proxy tests were not selected.

The deployment uses nginx's documented [HTTP proxy](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)
and [stream TLS](https://nginx.org/en/docs/stream/ngx_stream_ssl_module.html) modules.
