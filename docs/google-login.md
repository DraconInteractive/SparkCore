# Single-owner Google login

Google OpenID Connect is an optional **browser login method**, alongside SPARK's
administrator-token login. Only the Google subject enrolled by an existing
administrator can sign in. Google email addresses are display metadata, not the
access-control identity. This is not a multi-user or device-pairing system.

The implementation uses Google's authorization-code flow with PKCE S256, a
random state and nonce, a browser-binding cookie, Google RS256 signature/JWKS
validation, exact client audience/authorized-party and issuer checks, expiry,
fresh issued-at and verified email. Google signing keys are cached for five
minutes and refreshed when an unfamiliar key ID appears. Endpoints are pinned
to Google's documented HTTPS addresses. No client-supplied identity token is
accepted and no Google access/refresh token is saved or sent to the browser.

## VPS configuration

Keep the downloaded **Web application** OAuth credentials JSON outside the
repository, for example:

```
/home/pmc/.config/spark/google-client.json
```

The file must be owned by the OS user running SPARK, with mode 0600. The owner
binding directory must already exist, be owned by that user, and have mode 0700:

```sh
install -d -m 0700 ~/.config/spark
chmod 0600 ~/.config/spark/google-client.json
```

In Google Auth Platform, the client must have this exact authorized redirect URI:

```
https://spark.dracon.au/auth/google/callback
```

The downloaded JSON's `web.redirect_uris` array must also contain it. Download
an updated credentials file if the callback was added after the original download.
SPARK uses `web.client_id` and `web.client_secret`; it ignores endpoint URLs in
that file and uses pinned Google endpoints. Credentials are validated at startup
and by `--check-config`, without contacting Google or printing secrets.

Extend the existing `auth` block in `~/SparkCore/config/config.json`:

```json
{
  "provider": "tokens",
  "credentials_file": "/home/pmc/.config/spark/credentials.json",
  "browser_origin": "https://spark.dracon.au",
  "google": {
    "client_credentials_file": "/home/pmc/.config/spark/google-client.json",
    "owner_file": "/home/pmc/.config/spark/google-owner.json"
  }
}
```

Keep the existing server, database and storage sections. Relative paths resolve
against the config file directory. **Do not create an empty owner file**; SPARK
creates it atomically during enrolment. Preserve this file across deployments.
Removing the `google` block disables this method on restart. Instances without
it retain existing behaviour. No database migration or new permission is needed.
The runtime uses the existing `curl` executable and system CA trust store for
Google HTTPS requests. It does not put secrets in command arguments, shell
commands, persistent temporary request files or application logs.

After pulling and building:

```sh
./build/spark_server --config config/config.json --remote --check-config
```

Restart the VPS instance using its usual command. Existing Caddy routing to
SPARK also covers the callback; no additional port is needed. If proxy access
logging is enabled, avoid recording OAuth callback query strings, which contain
short-lived authorization codes. SPARK's own authentication logs omit them.

## Enrolment and daily use

1. Sign in through `/login` using a recovery administrator token.
2. Open **Google login** on the dashboard, or `/admin/google/explorer`.
3. Select **Enrol my Google account**, then choose your Google account.
4. Google returns to SPARK. The initiating admin session must still be valid.
5. SPARK binds the Google issuer and subject and opens an administrator session.
6. Test **Sign in with Google** from `/login` in a separate browser/private window.
   Verify that another Google account is refused.

Enrolment requires an administrator **browser session** with `admin.credentials`.
The owner receives exactly that administrator's explicit permissions, not an
implicit superuser role. Keep a full administrator token securely for recovery.
New permissions added in future releases are not automatically granted: unlink
and re-enrol using an appropriately provisioned administrator session if needed.

Login appears only after configuration and enrolment are complete. Normal login
never creates or switches the approved owner. Unlink before enrolling a different
account. The home instance needs no configuration changes.

## Sessions and recovery

Google login uses the existing Secure, HttpOnly, SameSite=Strict SPARK cookie and
CSRF protection. Sessions last at most 12 hours; default idle timeout is 30 minutes.
Remember device removes only the idle timeout. Process restart clears sessions.
The separate temporary Google flow cookie is SameSite=Lax to permit Google's
top-level callback, expires in ten minutes, and cannot authenticate API requests.

Logout revokes the current SPARK session. The Google management page can revoke
all Google sessions or unlink the owner. Both actions rotate persistent owner
state, invalidating existing Google sessions and pending flows. Recovery-token
credentials and their sessions remain valid. Google authenticates at login;
changing Google account settings does not instantly revoke an already-issued
SPARK session. Use SPARK's revocation control when that is required.

If Google is unavailable, administrator-token login remains available. A failed
flow must be restarted; callbacks cannot be replayed. Up to 128 pending flows
are retained for ten minutes. No permission is inferred from Google's test-user
list or from an email-address match.

## API

- `GET /auth/google/options`: public `{ "enabled": true|false }`.
- `POST /auth/google/start`: JSON `{ "remember_device": false }`, exact Origin
  and `X-SPARK-Login: 1`; returns `authorization_url` and flow cookie.
- `GET /auth/google/callback`: Google code/state response, bound to that browser.
- `GET /admin/google`: configuration/enrolled identity and permissions.
- `GET /admin/google/explorer`: management UI.
- `POST /admin/google/enroll`: browser-session and CSRF-protected enrolment start;
  same optional remember_device body and authorization_url response as login.
- `POST /admin/google/sessions/revoke`: JSON `{}`, revokes all Google sessions.
- `DELETE /admin/google/owner`: no body, unlinks the account and revokes sessions.

All `/admin/google` routes require an administrator with `admin.credentials`.
Management mutations use existing bearer or browser-session protections;
enrolment specifically requires a browser session. Browser cookies and Google
ID tokens are not supported in TCP hello packets. Native-client sessions and
phone-assisted pairing remain future work.

Protocol references: [Google OIDC](https://developers.google.com/identity/openid-connect/openid-connect)
and [Google discovery metadata](https://accounts.google.com/.well-known/openid-configuration).
