# SPARK alongside the existing relay on the Ubuntu VPS

Prepared for Ubuntu 24.04, the existing stock Caddy systemd service and
`spoken-command-relay`. The existing `relay.dracon.au` block and relay service
stay in place. This guide is preparation, not an automated VPS deployment.

Public endpoints:

- `https://relay.dracon.au` -> Caddy -> existing relay `127.0.0.1:8080`.
- `https://spark.dracon.au` -> Caddy -> SPARK `127.0.0.1:47683`.
- `spark.dracon.au:47685` TCP TLS -> dedicated nginx -> SPARK `127.0.0.1:47684`.

Caddy manages the HTTPS certificate. Certbot manages a separate certificate for
TCP, also valid for `spark.dracon.au`, under the explicit certificate name
`spark-tcp`. No Caddy private keys are copied or shared. The two certificates
renew independently. Only nginx's dedicated TCP service is reloaded by the
Certbot hook. Do not use Certbot standalone mode or replace the Caddy binary.

## Current VPS operation and deferred systemd migration

Decision recorded 2026-10-10: continue running the SPARK application in tmux
for now. HTTPS administrator login and an external TCP TLS handshake have been
verified. Caddy and the TCP proxy retain their existing systemd services.

Run SPARK as `pmc`, with working directory `/home/pmc/SparkCore`:

```sh
./build/spark_server --config /home/pmc/SparkCore/config/config.json --remote
```

When ready, migrate the SPARK application to a dedicated systemd unit with
`User=pmc`, the working directory and command above, startup after MariaDB,
automatic restart on failure, and enablement at boot. Preserve credential ownership
and `/home/pmc/spark-data/files`. Stop the tmux instance before starting the unit
to avoid duplicate processes and port conflicts. This migration is deferred until
the user is ready; tmux does not provide startup after a VPS reboot.

## Prepare while DNS propagates

Commands below run on the VPS, from a checkout of this SPARK repository unless
otherwise stated. Do not run them on the current antiX server. Build/install SPARK,
provision its MariaDB database and credentials separately following the existing
project setup and [identity guide](identity-permissions.md). Use a dedicated SPARK
OS account and absolute storage paths; do not reuse relay credentials or its data.

Before installing nginx, prevent its distribution default service from attempting
to bind Caddy's ports. This assumes nginx is not already used by another service;
the supplied listener output showed none.

```sh
sudo systemctl mask nginx.service
sudo apt update
sudo apt install nginx libnginx-mod-stream certbot
sudo install -d -m 0755 /var/lib/spark-acme/.well-known/acme-challenge
```

The masked `nginx.service` is unrelated to our new `spark-tcp-proxy.service`.
Do not unmask/start the default nginx service. Preparation does not require
stopping or restarting Caddy or the relay.

Create your SPARK config using the [remote example](../config/remote.example.json):
keep the three SPARK ports as shown, change `auth.browser_origin` to
`https://spark.dracon.au`, and set your actual database, private credentials and
storage paths. Run validation as the account that owns the credentials:

```sh
./build/spark_server --config /etc/spark/config.json --remote --check-config
```

Render nginx as that same account into a writable candidate location. It need not
have access to root's certificate files. `--defer-certificate-check` permits
preparation before issuance; nginx validates the real files before service startup.
The stream module must already exist. The output file must not already exist.

For this systemd-managed runtime directory, create it before rendering if it does
not exist (the generator otherwise tries to create it as the invoking user):
`sudo install -d -m 0700 /run/spark-tcp-proxy`. Do not change its ownership to the
SPARK user; nginx runs with a root master to read the protected certificate key.

```sh
python3 scripts/configure-remote.py --tcp-only --defer-certificate-check \
  --config /etc/spark/config.json \
  --certificate /etc/letsencrypt/live/spark-tcp/fullchain.pem \
  --key /etc/letsencrypt/live/spark-tcp/privkey.pem \
  --runtime /run/spark-tcp-proxy \
  --output /tmp/spark-nginx-tcp.conf
```

Install the reviewed candidate and service/hook:

```sh
sudo install -d -m 0755 /etc/spark
sudo install -m 0644 /tmp/spark-nginx-tcp.conf /etc/spark/nginx-tcp.conf
sudo install -m 0644 deploy/caddy/spark-tcp-proxy.service /etc/systemd/system/spark-tcp-proxy.service
sudo install -d -m 0755 /etc/letsencrypt/renewal-hooks/deploy
sudo install -m 0755 deploy/caddy/renew-spark-tcp /etc/letsencrypt/renewal-hooks/deploy/50-spark-tcp
sudo systemctl daemon-reload
```

Do not start the TCP proxy until its certificate exists. Keep the SPARK launch
command's `--remote` guard, whether running manually or through your own systemd
unit. The proxy service does not start or provision SPARK for you.

## After DNS is correct

Check that A and AAAA records both resolve to this VPS. Confirm both IPv4 and IPv6
allow inbound ports 80/443 through the OS and Hetzner firewalls. TCP 47685 will
also need opening for notifications; keep SPARK backend ports and MariaDB private.

Back up `/etc/caddy/Caddyfile`, then append the blocks in
[deploy/caddy/Caddyfile.spark](../deploy/caddy/Caddyfile.spark). Preserve the
existing relay block. The HTTP block serves only ACME challenge files and redirects
other requests; the HTTPS block proxies to authenticated SPARK. No SPARK routes
are exposed over plaintext HTTP.

```sh
sudo caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
sudo systemctl reload caddy
curl -fsS https://relay.dracon.au/health
```

Caddy will request the HTTPS certificate for the new hostname. If SPARK has not
started, its HTTPS site may return 502 until startup; the existing relay remains
independent. Inspect `sudo journalctl -u caddy -n 50 --no-pager` for issuance errors.

Before asking Certbot to issue the TCP certificate, create a harmless test file:

```sh
printf 'spark-acme-ready\n' | sudo tee /var/lib/spark-acme/.well-known/acme-challenge/spark-probe >/dev/null
curl -4fsS http://spark.dracon.au/.well-known/acme-challenge/spark-probe
curl -6fsS http://spark.dracon.au/.well-known/acme-challenge/spark-probe
```

Both should return `spark-acme-ready`. Also test from another machine with IPv6
connectivity; a local request cannot prove external firewall access. Fix incorrect
AAAA routing before issuance, or remove AAAA until IPv6 is ready.

Request the certificate without stopping Caddy:

```sh
sudo certbot certonly --webroot -w /var/lib/spark-acme \
  --cert-name spark-tcp -d spark.dracon.au
sudo nginx -t -c /etc/spark/nginx-tcp.conf
sudo systemctl enable --now spark-tcp-proxy.service
sudo systemctl enable --now certbot.timer
sudo certbot renew --cert-name spark-tcp --dry-run --run-deploy-hooks
```

Certbot asks for email/terms on initial setup. Keep both its timer and the webroot
route in place for renewal. The deploy hook validates nginx config through the
service's ExecReload before signalling a graceful reload. Failed validation leaves
the existing process running and must be investigated. Caddy renewal needs no
additional hook. Never make certificate private keys world-readable.

Finally, check `sudo ss -ltnp`: Caddy owns public 80/443; the dedicated nginx owns
47685 on IPv4/IPv6; SPARK owns only loopback 47683/47684. Unauthenticated HTTPS
`/health` should return 401, `/login` should work, and a TLS notification client
must verify `spark.dracon.au` before sending its authenticated hello. Check both
live notifications and reconnect/replay. No domain/token values should be supplied
in URLs. Confirm the relay's public health again after setup.

## Validation scope

The nginx generator supports both its original combined mode and `--tcp-only`.
The latter omits the HTTP block entirely; it cannot claim Caddy's 80/443 ports.
The supplied systemd unit and Caddy fragment must be validated on the target VPS;
local preparation does not verify DNS, public CA issuance or the remote firewall.

References: [Caddy handle routing](https://caddyserver.com/docs/caddyfile/directives/handle),
[Certbot webroot and renewals](https://eff-certbot.readthedocs.io/en/stable/using.html).
