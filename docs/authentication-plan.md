# Configuration and authentication implementation plan

Agreed: 2026-10-09. Status: step 1 implemented; authentication enforcement is not implemented yet.

See [instance configuration](configuration.md) for the implemented schema, precedence,
validation and temporary startup restrictions.

## Objective

Allow each SPARK instance to start from a configuration file, with optional,
modular authentication shared by REST and TCP. Preserve existing unauthenticated
behaviour for configurations without an `auth` section. Support secure remote
deployment without duplicating authentication logic in individual features.

## Agreed startup rules

- No `auth` section: open mode, preserving current behaviour.
- Valid `auth` section: authentication is required.
- Present but invalid authentication configuration, unreadable credentials, or a
  missing explicitly requested config file: refuse startup. Never fall back to
  open mode after an error.
- Keep secrets separate from ordinary configuration. Authentication settings
  have one authoritative source; define and document precedence for other
  configuration, environment variables and command-line arguments.

Configuration shape (storage updated to an array of drive locations):

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 47683,
    "notification_port": 47684
  },
  "storage": {
    "drives": ["/srv/spark/files"]
  },
  "auth": {
    "provider": "tokens",
    "credentials_file": "/etc/spark/credentials.json"
  }
}
```

Invocation:

```sh
./spark_server --config /etc/spark/config.json
```

## Identity and authorization architecture

Introduce shared identity resolution and permission checks with open-mode and
token-provider implementations. Domain features should receive a resolved
identity and use a common policy interface rather than parse credentials.

The first authenticated provider uses separate opaque tokens for devices and
separate administrator credentials. Tokens map to identities and permissions;
individual credentials must be revocable without rotating every client's token.
Authenticated mode denies operations without an explicit permission.

Identity checks must cover:

- Devices advertising actions, modifying their registration/subscriptions, and
  claiming work only as themselves unless explicitly authorized otherwise.
- Request `source_device` matching the authenticated requester unless a policy
  explicitly permits acting for another identity.
- Command completion and renewal requiring both authorized executor identity
  and the existing execution claim token. Authentication does not replace leases
  or claim tokens.
- Administrator-only restart, backup, device provisioning and credential
  management operations.
- Resource access across files, projects, boards, tasks, calendars, timers and
  other existing endpoints, including indirect access and downloads.
- Notification authorization during live delivery and historical replay, in
  addition to existing pulse subscription and command routing filters.

Exact roles, resource permissions and any public endpoint exceptions are to be
defined explicitly during implementation; no domain is implicitly public in
authenticated mode.

## REST, TCP and browser access

REST device clients use `Authorization: Bearer <device-token>`. TCP clients
authenticate in their initial handshake before receiving notifications or replay
data. Both transports resolve credentials through the same provider.

Human pages need a login flow with secure, HttpOnly session cookies and CSRF
protection. In authenticated mode the command explorer submits as the logged-in
identity, replacing its current unauthenticated browser-device registration flow.
Open mode retains existing behaviour.

Authentication must cover ordinary routes, streaming uploads/downloads, text
editing, maintenance controls and alternate methods/access paths. Define how
credential revocation affects existing TCP connections and browser sessions.

## Remote transport security

Use HTTPS for REST and TLS for TCP notifications, directly or through a correctly
configured proxy. Protecting only the HTTPS listener leaves the notification
listener exposed. Ensure proxy-facing plaintext listeners cannot be reached
directly by remote clients, and define trusted proxy handling explicitly.

Never send bearer credentials over plaintext remote connections or put them in
URLs. Credential and session values must not appear in application logs,
discovery responses or error messages.

## Implementation sequence and completion checks

1. **Configuration loading and strict startup validation — implemented.** Define the schema,
   defaults, precedence, path resolution and existing CLI/environment
   compatibility. Test absent auth, valid auth, malformed/null/empty auth,
   unreadable credentials and missing requested configuration. Errors must not
   start an open server.
2. **Shared identity and permissions.** Implement open and token providers,
   credential provisioning/revocation, and an explicit authorization matrix.
   Decide credential storage representation and browser administrator login
   details before implementation; the example above does not specify these.
3. **REST and TCP enforcement.** Cover every route, ownership check, command
   operation and notification/replay path. Verify missing/invalid credentials,
   impersonation attempts, insufficient permissions and revoked credentials.
4. **Browser login and explorer integration.** Protect sessions and state-changing
   requests; verify file viewing/downloading/editing, command submission and
   administrator controls still work for authorized users.
5. **Remote deployment and regression tests.** Provide encrypted deployment
   configuration for both channels. Verify there are no unprotected alternate
   routes/listeners and run the existing open-mode suite plus authenticated
   integration tests, including reconnect, replay and maintenance restart.

Step 1 decisions: JSON via explicit `--config`; CLI listener overrides take priority;
file database/storage values override existing environment settings. Relative file
paths resolve beside the specified config. Configuration is fixed for the process
lifetime and shared with dashboard restarts/backups. `--check-config` validates
without startup and reports runtime blockers. Unknown/duplicate fields fail.

Until later phases implement enforcement, even structurally valid `auth` causes
normal startup to refuse service; it never starts open. Credential document syntax
and readability are checked, but credential-record validation belongs to step 2.

Storage uses `storage.drives`, an array of paths. Multi-drive placement, persistent
drive identities, legacy file assignment and backup/restore are implemented ahead
of authentication step 2. Whole files are allocated to the selected project or storage drive; existing files stay on their assigned drive. Runtime attachment/detachment and stable drive letters are supported. Only the
persistent server root is required at startup; full backups require every root. See [configuration](configuration.md) and [backups](backups.md).

Keep this checklist current as phases are completed. Record concrete policy and
deployment decisions here so later phases use the same contract.

## Reference guidance

- [OWASP REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html)
- [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html)

