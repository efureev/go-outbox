# Use cases

English | [Русский](UseCases.ru.md)

Four deployments, from a laptop to a cluster. Each is complete enough to copy,
and each ends with the mistakes that environment invites — which is the part
worth reading twice.

One rule runs through all of them: **the message is written in the same
transaction as the business change**. Everything else here is plumbing.

- [1. Laravel, PostgreSQL 18 and RabbitMQ under Docker Compose](#1-laravel-postgresql-18-and-rabbitmq-under-docker-compose)
- [2. One VDS, supervisord](#2-one-vds-supervisord)
- [3. Kubernetes, Kafka, autoscaled on backlog age](#3-kubernetes-kafka-autoscaled-on-backlog-age)
- [4. Bare metal, systemd, several instances on one host](#4-bare-metal-systemd-several-instances-on-one-host)
- [5. Four streams across three RabbitMQ instances](#5-four-streams-across-three-rabbitmq-instances)

---

## 1. Laravel, PostgreSQL 18 and RabbitMQ under Docker Compose

A local development setup: a Laravel application writes outbox rows, the
dispatcher runs beside it as a sidecar, and a consumer reads from RabbitMQ.

### Compose

```yaml
services:
  app:                                  # your Laravel container
    build: .
    environment:
      DB_CONNECTION: pgsql
      DB_HOST: postgres
      DB_PORT: 5432
      DB_DATABASE: app
      DB_USERNAME: app
      DB_PASSWORD: secret
    depends_on:
      postgres: {condition: service_healthy}

  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_DB: app
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
    # PostgreSQL 18 wants a single mount one level up from the data directory.
    volumes:
      - postgres-data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d app"]
      interval: 2s
      retries: 15

  rabbitmq:
    image: rabbitmq:4-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: app
      RABBITMQ_DEFAULT_PASS: secret
    ports: ["15672:15672"]              # management UI
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      retries: 15

  outbox:
    image: ghcr.io/efureev/go-outbox:1.0.0
    environment:
      OUTBOX_DB_DSN: postgres://app:secret@postgres:5432/app?sslmode=disable
      OUTBOX_DB_SCHEMA: outbox
      # Fine locally. In production apply migrations as a deliberate step.
      OUTBOX_DB_AUTO_MIGRATE: "true"
      OUTBOX_LOG_FORMAT: console
      OUTBOX_STREAMS: local
      OUTBOX_STREAM_LOCAL_DRIVER: rmq
      OUTBOX_DRIVER_RMQ_TYPE: rabbitmq
      OUTBOX_DRIVER_RMQ_DSN: amqp://app:secret@rabbitmq:5672/
      # The dispatcher declares queues so a fresh checkout has somewhere to
      # publish. Turn this off once the consumer owns the topology.
      OUTBOX_DRIVER_RMQ_DECLARE: "true"
    ports:
      - "8085:8085"                     # /api/v1/stats
      - "9100:9100"                     # /metrics
    depends_on:
      postgres: {condition: service_healthy}
      rabbitmq: {condition: service_healthy}

volumes:
  postgres-data: {}
```

### Writing a message from Laravel

`payload` is `BYTEA`, and PDO will not turn a PHP string into one on its own:
bind it through `convert_to(?, 'UTF8')`. The two JSON columns need an explicit
`::jsonb`. Everything else has a server default and belongs to the dispatcher.

```php
<?php

namespace App\Outbox;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class Outbox
{
    /**
     * Queue a message for delivery. Call it inside the transaction that carries
     * the business change — that is the whole point of the pattern.
     */
    public function publish(
        string $topic,
        array $payload,
        string $stream = 'local',
        array $target = [],
        array $headers = [],
    ): string {
        // A time-ordered id keeps the primary key index append-ordered, and it
        // is what a consumer deduplicates on. Str::uuid() is v4 — do not use it
        // here. Before Laravel 12: \Ramsey\Uuid\Uuid::uuid7()->toString().
        $id = (string) Str::uuid7();

        DB::insert(
            'INSERT INTO outbox.messages (id, stream, topic, payload, headers, target)
             VALUES (?, ?, ?, convert_to(?, \'UTF8\'), ?::jsonb, ?::jsonb)',
            [
                $id,
                $stream,
                $topic,
                json_encode($payload, JSON_THROW_ON_ERROR),
                json_encode($headers + ['content-type' => 'application/json'], JSON_THROW_ON_ERROR),
                json_encode((object) $target, JSON_THROW_ON_ERROR),
            ],
        );

        return $id;
    }
}
```

Used the way it is meant to be used:

```php
DB::transaction(function () use ($request, $outbox) {
    $order = Order::create($request->validated());

    $outbox->publish(
        topic: 'order.placed',
        payload: ['order_id' => $order->id, 'total' => (string) $order->total],
        target: ['key' => (string) $order->customer_id],   // Kafka partition key
    );
});
```

If the transaction rolls back, so does the message. That is the guarantee, and
it is the only thing a producer has to get right.

### Consuming

The dispatcher applies the driver's prefix and version suffix, so the consumer
subscribes to the **effective** name, not to what is in the `topic` column.
`GET http://localhost:8085/api/v1/stats` reports each driver's prefix and
separators, so the name can be derived without reading anyone's configuration.

With no prefix configured, `order.placed` is the queue name. Deduplicate on the
AMQP `message_id` property, which carries the outbox row id.

### What goes wrong here

- **Writing outside the transaction.** An Eloquent observer on `created` fires
  inside `Order::create`'s implicit transaction, but a queued job or an event
  listener dispatched with `afterCommit` does not. If the row is written after
  the commit, a crash in between loses the message — which is the failure the
  outbox exists to prevent.
- **Binding `payload` directly.** `DB::table('outbox.messages')->insert([...])`
  sends a text parameter for a `BYTEA` column and PostgreSQL refuses it. Use the
  raw statement above.
- **Hand-writing the schema as a Laravel migration.** It will drift from the one
  the dispatcher expects. Let `outbox migrate up` own the `outbox` schema, and
  keep Laravel's migrations to Laravel's tables.
- **Expecting a Laravel queue worker to consume these.** Laravel's queue driver
  expects its own serialized job envelope. This dispatcher publishes your bytes
  unchanged; either consume with `php-amqplib` directly, or make the payload a
  Laravel job envelope yourself.

---

## 2. One VDS, supervisord

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

## 3. Kubernetes, Kafka, autoscaled on backlog age

The dispatcher as its own Deployment beside the producing service, publishing to
Kafka, with the replica count following the backlog rather than CPU.

### Secret and Deployment

```yaml
apiVersion: v1
kind: Secret
metadata: {name: outbox}
stringData:
  dsn: postgres://outbox:secret@postgres:5432/app?sslmode=require
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox
  labels: {app: outbox}
spec:
  replicas: 2
  selector:
    matchLabels: {app: outbox}
  template:
    metadata:
      labels: {app: outbox}
    spec:
      # Above OUTBOX_APP_SHUTDOWN_TIMEOUT. Below it, the kubelet sends SIGKILL
      # mid-drain and the claimed batch waits out its lease.
      terminationGracePeriodSeconds: 45
      containers:
        - name: outbox
          image: ghcr.io/efureev/go-outbox:1.0.0
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
            # Recorded in the owner column of a claimed row, so an operator can
            # tell which pod holds a lease.
            - name: OUTBOX_APP_INSTANCE
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: OUTBOX_STREAMS
              value: events
            - name: OUTBOX_STREAM_EVENTS_DRIVER
              value: kafka
            - name: OUTBOX_DRIVER_KAFKA_TYPE
              value: kafka
            - name: OUTBOX_DRIVER_KAFKA_BROKERS
              value: my-cluster-kafka-bootstrap:9092
            - name: OUTBOX_DISPATCH_WORKERS
              value: "8"
          ports:
            - {name: http, containerPort: 8085}
            - {name: metrics, containerPort: 9100}
          livenessProbe:
            httpGet: {path: /health, port: http}
          readinessProbe:
            httpGet: {path: /ready, port: http}
          resources:
            requests: {cpu: 100m, memory: 64Mi}
            limits: {memory: 256Mi}
---
apiVersion: v1
kind: Service
metadata:
  name: outbox
  labels: {app: outbox}
spec:
  selector: {app: outbox}
  ports:
    - {name: http, port: 8085}
    - {name: metrics, port: 9100}
```

### Migrations as a Job, not an init container

An init container runs once per pod, so every replica and every restart repeats
it. A Job runs once per release. The advisory lock makes both safe; only one is
sensible.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: outbox-migrate
  annotations:                          # if you deploy with Helm
    "helm.sh/hook": pre-install,pre-upgrade
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: ghcr.io/efureev/go-outbox:1.0.0
          args: ["migrate", "up"]
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
```

### Scraping and scaling

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: {name: outbox}
spec:
  selector:
    matchLabels: {app: outbox}
  endpoints:
    - {port: metrics, interval: 30s}
```

Scale on how long the oldest message has waited. CPU says nothing useful here —
the dispatcher is idle whether the backlog is empty or hours deep, because what
it waits on is the broker.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: {name: outbox}
spec:
  scaleTargetRef: {name: outbox}
  minReplicaCount: 2
  maxReplicaCount: 10
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: max(outbox_oldest_pending_age_seconds)
        threshold: "30"                 # a replica per 30s of backlog age
```

Replicas divide the work safely: claims are taken with `FOR UPDATE SKIP LOCKED`
and carry a lease token every write-back must present, and the periodic
housekeeping takes an advisory lock so it stays on one replica per cycle.

### What goes wrong here

- **`terminationGracePeriodSeconds` below the shutdown timeout.** Every rollout
  then leaves a batch to expire instead of being handed back. Watch
  `outbox_messages_reclaimed_total` after a deploy.
- **Scaling on CPU.** It will never trigger. Scale on
  `outbox_oldest_pending_age_seconds`.
- **More workers than Kafka can absorb, or than the database pool allows.** Each
  replica also holds one pooled connection for the notification listener, so
  size `OUTBOX_DB_MAX_CONNS` with room to spare and remember the total across
  replicas.
- **Leaving `OUTBOX_DB_AUTO_MIGRATE` on.** Two replicas starting together are
  safe, but a schema change should be a release step, not a side effect of a
  pod restart.

---

## 4. Bare metal, systemd, several instances on one host

One machine with enough cores to be worth using, PostgreSQL and Kafka reachable
from it, and several dispatcher instances sharing the table. A systemd template
unit gives each its own identity.

`/etc/systemd/system/outbox@.service`:

```ini
[Unit]
Description=Transactional Outbox dispatcher (instance %i)
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=outbox
EnvironmentFile=/etc/outbox/outbox.env
# %i is the instance name, so a claimed row records which one holds its lease.
Environment=OUTBOX_APP_INSTANCE=%H-%i
# One port per instance, so each has its own /metrics and /ready.
Environment=OUTBOX_HTTP_PORT=80%i
Environment=OUTBOX_METRICS_PORT=91%i
ExecStart=/usr/local/bin/outbox run
Restart=always
RestartSec=5s
TimeoutStopSec=45s

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
# Install the binary as in the VDS recipe above.
outbox migrate up                       # once, before the instances start
systemctl enable --now outbox@85 outbox@86 outbox@87
systemctl status 'outbox@*'
```

That gives three instances on ports 8085/8086/8087 and 9185/9186/9187. They
share one table and divide the work between them; none can overwrite another's
result, because every write-back has to present the lease token its claim
stamped on the row.

Prometheus scrapes all three:

```yaml
scrape_configs:
  - job_name: outbox
    static_configs:
      - targets: ["10.0.0.10:9185", "10.0.0.10:9186", "10.0.0.10:9187"]
```

### Tuning on hardware you own

This is the case where the defaults are most likely to be leaving throughput on
the table, and the only way to know is to measure on the machine itself:

```bash
make up && make bench
```

The sweep answers the two questions that matter. Throughput is bounded by the
smaller of `OUTBOX_DISPATCH_WORKERS` and the driver's channel pool, so raising
one alone does nothing. And a larger `OUTBOX_DISPATCH_BATCH_SIZE` amortises the
claim over more messages, until a batch stops fitting inside the lease.

### What goes wrong here

- **Reaching for more instances first.** On one host, workers and batch size are
  cheaper than processes. Instances are for availability, or for when one
  process genuinely cannot keep up.
- **Forgetting that connections multiply.** Three instances at
  `OUTBOX_DB_MAX_CONNS=10` is thirty connections, plus one held by each
  notification listener. Check `max_connections`.
- **One env file, one port.** Without the per-instance `OUTBOX_HTTP_PORT` above,
  the second instance fails to bind and restarts forever.
- **Instances that all look alike in metrics.** Set `OUTBOX_APP_INSTANCE`, or
  the `owner` column cannot tell you which process is sitting on a stuck lease.

---

## 5. Four streams across three RabbitMQ instances

Not a deployment target but a routing arrangement: one dispatcher feeding several
brokers at once. A driver is one connection, and there may be as many as there are
brokers to reach — so separating environments, tenants or blast radius is a matter
of configuration.

Here `local` and `tetra` share an instance through drivers of their own, `test` and
`global` each get one to themselves.

```dotenv
OUTBOX_STREAMS=local,test,global,tetra

OUTBOX_STREAM_LOCAL_DRIVER=rmq_local
OUTBOX_STREAM_TEST_DRIVER=rmq_test
OUTBOX_STREAM_GLOBAL_DRIVER=rmq_global
OUTBOX_STREAM_TETRA_DRIVER=rmq_tetra

OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=loc
OUTBOX_DRIVER_RMQ_LOCAL_DECLARE=true

OUTBOX_DRIVER_RMQ_TEST_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TEST_DSN=amqp://user:pass@rabbit-b:5672/
OUTBOX_DRIVER_RMQ_TEST_PREFIX=tst
OUTBOX_DRIVER_RMQ_TEST_DECLARE=true

OUTBOX_DRIVER_RMQ_GLOBAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_GLOBAL_DSN=amqp://user:pass@rabbit-c:5672/
OUTBOX_DRIVER_RMQ_GLOBAL_PREFIX=glb
OUTBOX_DRIVER_RMQ_GLOBAL_DECLARE=true

# Back to the first instance, but a driver of its own: a separate connection, its
# own channel pool, its own naming.
OUTBOX_DRIVER_RMQ_TETRA_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TETRA_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_TETRA_PREFIX=ttr
OUTBOX_DRIVER_RMQ_TETRA_DECLARE=true
```

`DECLARE` is on here so a fresh set of brokers has somewhere to publish. Leave it
off once the consumers own the topology: with `MANDATORY` on by default, a queue
nobody declared makes the broker return the message and the dispatcher record a
permanent failure — visible, but not delivered.

A producer picks its destination with one column:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES (gen_random_uuid(), 'tetra', 'orders.placed', convert_to('{}', 'UTF8'), '{}');
```

which lands where the prefix says it will:

| Stream | Driver | Instance | Queue |
|---|---|---|---|
| `local` | `rmq_local` | rabbit-a | `loc_orders.placed` |
| `test` | `rmq_test` | rabbit-b | `tst_orders.placed` |
| `global` | `rmq_global` | rabbit-c | `glb_orders.placed` |
| `tetra` | `rmq_tetra` | rabbit-a | `ttr_orders.placed` |

`GET /api/v1/stats` reports the mapping as it was actually parsed, each driver with
the address it connects to and its credentials removed — which is the quickest way
to confirm a stream reaches the instance you meant, and not merely that it resolved
to a driver of the right name.

### Trying it locally

```yaml
# The credentials matter: the built-in guest account is refused from anything but
# loopback, and a connection through a published port is not loopback.
x-rabbit: &rabbit
  image: rabbitmq:4-alpine
  environment:
    RABBITMQ_DEFAULT_USER: outbox
    RABBITMQ_DEFAULT_PASS: outbox

services:
  rabbit-a: {<<: *rabbit, ports: ["55672:5672"]}
  rabbit-b: {<<: *rabbit, ports: ["55673:5672"]}
  rabbit-c: {<<: *rabbit, ports: ["55674:5672"]}
```

Point the three DSNs at `amqp://outbox:outbox@localhost:55672/` and the two ports
after it, insert one message per stream, and each instance ends up holding exactly
its own.

### What goes wrong here

- **Counting connections once instead of per driver.** `CHANNELS` and the publish
  concurrency belong to a driver, not to the process. Four drivers at the default
  four channels is sixteen AMQP channels, and `OUTBOX_DISPATCH_WORKERS` applies to
  every stream's pipeline separately — so this arrangement runs the default eight
  workers against four channels four times over. Half of each pipeline's workers
  wait for a channel; raise `CHANNELS` and `WORKERS` together or leave both.
- **A driver name that is a prefix of another.** `rmq` alongside `rmq_local` is
  handled — settings are matched by exact key, not by string prefix — but it reads
  badly. Give drivers names that do not nest.
- **Assuming one broker being down is contained.** Once running, it is: each stream
  has its own pipeline, so the others keep publishing. It is not contained at
  startup — every driver connects before the dispatcher starts, so one unreachable
  broker means none of the streams run. And it is never contained for the table,
  which keeps growing while that stream's messages retry. Watch
  `max(outbox_oldest_pending_age_seconds)` alongside
  `outbox_messages_retried_total{stream=…}`, which is the one that names the culprit.
- **Forgetting that a prefix is part of the consumer contract.** Change
  `OUTBOX_DRIVER_*_PREFIX` and every consumer of that stream is subscribed to a queue
  nobody publishes to any more.

---

## Where to go next

- [Public contract](PublicContract.md) — the columns a producer writes and what
  is explicitly not guaranteed.
- [Configuration](Config.md) — every environment variable.
- [Operations](Operations.md) — scaling, and what to do when something is wrong.
- [Metrics and alerts](MetricsAndAlerts.md).
