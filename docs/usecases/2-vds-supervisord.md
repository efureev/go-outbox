# One VDS, supervisord

English | [Русский](2-vds-supervisord.ru.md)

Back to the [use case index](../UseCases.md).


One machine, the application and PostgreSQL already on it, the dispatcher as a
supervised process.

### Install

```bash
VERSION=1.0.0
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/outbox_${VERSION}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf "outbox_${VERSION}_linux_amd64.tar.gz"

# The binary is static: no runtime, no libc to match.
install -o root -g root -m 0755 "outbox_${VERSION}_linux_amd64/outbox" /usr/local/bin/outbox

useradd --system --no-create-home --shell /usr/sbin/nologin outbox
install -d -o root -g outbox -m 0750 /etc/outbox
```


`SHA256SUMS` tells you the download was not corrupted. To also establish that it
came from this repository's release workflow and not from somebody who reached
the release page:

```bash
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/SHA256SUMS.cosign.bundle"

cosign verify-blob SHA256SUMS \
  --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity-regexp 'https://github.com/efureev/go-outbox/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Signing is keyless: there is no public key to distribute, and no private key for
anyone to steal. What the two `--certificate-*` flags check is that the
signature was made by that workflow in that repository — so pin them, because
without them any Sigstore signature at all would satisfy the command.

The container image is signed by digest, for the same reason a tag is not worth
signing: a tag is a name that can be moved.

```bash
cosign verify ghcr.io/efureev/go-outbox:$VERSION \
  --certificate-identity-regexp 'https://github.com/efureev/go-outbox/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Each release also carries a CycloneDX SBOM per platform,
`outbox_${VERSION}_linux_amd64.cdx.json` and its siblings. They are generated
from the compiled binaries rather than from the source tree, so they list what
was actually linked in.

`/etc/outbox/outbox.env`, mode `0640`, owned `root:outbox` — it holds a
password:

```dotenv
OUTBOX_DB_DSN=postgres://outbox:secret@127.0.0.1:5432/app?sslmode=disable
OUTBOX_DB_SCHEMA=outbox
OUTBOX_LOG_FORMAT=json

OUTBOX_STREAMS=local
OUTBOX_STREAM_LOCAL_DRIVER=rmq
OUTBOX_DRIVER_RMQ_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_DSN=amqp://app:secret@127.0.0.1:5672/

OUTBOX_HTTP_PORT=8085
OUTBOX_METRICS_PORT=9100
OUTBOX_APP_SHUTDOWN_TIMEOUT=30s
```

Apply the schema once, as a deliberate step:

```bash
set -a; . /etc/outbox/outbox.env; set +a
outbox migrate status
outbox migrate up
```

### supervisord

`/etc/supervisor/conf.d/outbox.conf`:

```ini
[program:outbox]
; supervisord does not read env files, so a shell loads it — and exec replaces
; that shell, or the signal would stop /bin/sh and leave the dispatcher running.
command=/bin/sh -c 'set -a; . /etc/outbox/outbox.env; set +a; exec /usr/local/bin/outbox run'
user=outbox
directory=/var/lib/outbox

autostart=true
autorestart=true
startsecs=5

; SIGTERM starts the drain: stop claiming, finish the batch in flight, record
; its outcome, hand back what was never started.
stopsignal=TERM
; Must exceed OUTBOX_APP_SHUTDOWN_TIMEOUT. Set it lower and supervisord sends
; SIGKILL mid-drain, so whatever was claimed waits out its lease instead of
; being handed back.
stopwaitsecs=40
; Kill the process group, not just the shell above.
stopasgroup=true
killasgroup=true

stdout_logfile=/var/log/outbox/outbox.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=5
redirect_stderr=true
```

```bash
supervisorctl reread && supervisorctl update
supervisorctl status outbox
curl -s localhost:8085/api/v1/stats | jq .messages
```

### systemd instead

Same machine, no supervisord:

```ini
[Unit]
Description=Transactional Outbox dispatcher
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=exec
User=outbox
EnvironmentFile=/etc/outbox/outbox.env
ExecStart=/usr/local/bin/outbox run
Restart=always
RestartSec=5s
# Above OUTBOX_APP_SHUTDOWN_TIMEOUT, for the same reason as stopwaitsecs.
TimeoutStopSec=40s

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### What goes wrong here

- **`stopwaitsecs` below the shutdown timeout.** The most common one. Nothing is
  lost — the leases expire and another run picks the work up — but delivery
  stalls for `OUTBOX_DISPATCH_LEASE_TTL` on every restart.
- **`command=` without `exec`.** The signal reaches `/bin/sh`, which dies while
  the dispatcher keeps running, and supervisord loses track of it.
- **Migrating on start in production.** `OUTBOX_DB_AUTO_MIGRATE` makes a
  restart a schema change. Leave it off and run `outbox migrate up` on purpose.
- **A world-readable env file.** It contains the database password.

---
