# go-outbox

[![Test](https://github.com/efureev/go-outbox/actions/workflows/test.yml/badge.svg)](https://github.com/efureev/go-outbox/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/efureev/go-outbox.svg)](https://pkg.go.dev/github.com/efureev/go-outbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/efureev/go-outbox)](https://goreportcard.com/report/github.com/efureev/go-outbox)
[![License](https://img.shields.io/github/license/efureev/go-outbox)](LICENSE)

English | [Русский](README.ru.md)

**Your application writes a message in the same transaction as the business change.
This delivers it — to RabbitMQ, to Kafka, to several of each — and does not lose it.**

A Transactional Outbox dispatcher: a sidecar that reads a database table and publishes
what it finds. No client library to embed, no framework to adopt, nothing in your
application beyond one `INSERT`.

## Why this one

- **Nothing is lost.** A row becomes `sent` only after the broker acknowledges it —
  a publisher confirmation on AMQP, `acks=all` on Kafka. Not when the bytes leave
  the process.

- **Scale sideways, safely.** Run as many replicas as you like against one table.
  Claims are taken with `FOR UPDATE SKIP LOCKED` and carry a lease token that every
  write-back must present, so a replica whose lease expired mid-flight cannot
  overwrite what another already delivered. The schema enforces it, not a convention.

- **Fast, and you can check.** ~7 300 msg/s through RabbitMQ with confirms on;
  ~12 000 with workers and the channel pool widened together; ~46 000 with the broker
  taken out of the picture — the dispatcher is never the bottleneck. `make bench`
  reproduces every figure on your own hardware.

- **Milliseconds, not poll intervals.** A trigger wakes the pipeline the moment a row
  is written — ~105 ms end to end on the shipped defaults, ~5 ms once the debounce
  window and the replica jitter are tuned away. Polling stays on as reconciliation, so
  a lost notification costs one interval and never a message.

- **Many brokers at once.** Four streams across three RabbitMQ instances and a Kafka
  cluster is a configuration, not a fork. A producer picks its destination with one
  column, and each stream gets its own pipeline, so a broker that goes down while
  running delays only the streams pointed at it. (Every broker must be reachable at
  startup: one that is not fails the boot rather than starting half a dispatcher.)

- **Recovers on its own.** A replica killed mid-batch leaves rows leased; the lease
  expires and another picks them up. A clean shutdown does not even wait for that —
  it hands unfinished claims straight back.

- **An outage costs you latency, not an evening.** The retry budget counts times a
  broker *refused* a message. A broker that cannot be reached never saw it, so it
  spends nothing: the message waits and goes out when the broker returns, however long
  that takes. No table full of `failed` rows to requeue by hand after a twenty-minute
  restart.

- **Tells you what happened.** 20 Prometheus metrics, including the one that actually
  matters: how long the oldest undelivered message has waited. Failed messages are
  listable and requeueable over HTTP, so nobody writes an `UPDATE` by hand.

- **Visible inside a trace.** Point it at an OpenTelemetry collector and each message
  gets an `outbox.publish` span between the producer's and the consumer's, so the wait
  between committing a row and handing it to a broker stops being a number nobody can
  explain. Off by default, and off costs nothing: no exporter, no span, no allocation.

- **Boring to operate.** A 29 MB `scratch` image with no shell and no package manager,
  or a static binary. Delivered rows are swept on a retention you set. Ten direct
  dependencies, no web framework, no DI container.

- **Fails at boot, not at 3am.** The whole configuration is read and validated before
  anything connects, and every problem is reported together — a bad threshold, a
  misspelled driver key, three drivers each missing a DSN — so a misconfigured
  deployment takes one restart to diagnose rather than one per mistake.

- **Proven, not asserted.** 178 tests, half of them against real PostgreSQL, RabbitMQ
  and Redpanda, because a set of concurrency rules expressed in SQL cannot be checked
  by a mock that matches query text.

## Install

### Container image

```bash
docker pull ghcr.io/efureev/go-outbox:1.0.0
```

Published on every tag for `linux/amd64` and `linux/arm64`. Tags are the exact
version, the minor line (`1.0`), and `latest`; a pre-release moves neither of
the last two. The image is `scratch` plus a CA bundle — no shell, no package
manager, nothing for anything that gets in to use.

### Binary

Prebuilt archives for Linux and macOS, amd64 and arm64, on
[the releases page](https://github.com/efureev/go-outbox/releases):

```bash
VERSION=1.0.0
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/outbox_${VERSION}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS

tar -xzf "outbox_${VERSION}_linux_amd64.tar.gz"
sudo install -m 0755 "outbox_${VERSION}_linux_amd64/outbox" /usr/local/bin/outbox
outbox version
```

The binary is static: no runtime to install, no libc to match.

### With Go

```bash
go install github.com/efureev/go-outbox/cmd/outbox@latest
```

Convenient, but it stamps no version — `outbox version` reports `dev`. Prefer a
release archive or the image where knowing what is deployed matters.

### From source

```bash
git clone https://github.com/efureev/go-outbox.git && cd go-outbox
make build              # ./bin/outbox
make image              # a local container image
make dist               # release archives for every platform
```

## Quick start

```bash
docker compose up -d                       # PostgreSQL, RabbitMQ, Redpanda
cp .env.example .env
go run ./cmd/outbox migrate up
go run ./cmd/outbox run
```

Write a message the way a producer would:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES (gen_random_uuid(), 'local', 'orders.placed',
        '{"order":"A-1"}'::text::bytea, '{"key":"customer-1"}'::jsonb);
```

or, from Go, in the transaction that carries the business change:

```go
client := outboxclient.Default()

tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, id); err != nil {
    return err
}

if err := client.Enqueue(ctx, tx, outboxclient.Message{
    Stream:  "local",
    Topic:   "account.debited",
    Payload: payload,
    Headers: map[string]string{"traceparent": traceparent},
}); err != nil {
    return err
}

return tx.Commit(ctx)
```

## How it works

```
producer transaction                 go-outbox                     broker
┌────────────────────┐        ┌────────────────────────┐        ┌─────────┐
│ UPDATE accounts …  │        │ claim (SKIP LOCKED,    │        │         │
│ INSERT outbox row  │───────▶│        lease token)    │───────▶│ RabbitMQ│
│ COMMIT             │        │ publish (confirmed)    │        │  Kafka  │
└────────────────────┘        │ write back (lease-     │        │         │
                              │        checked)        │        └─────────┘
                              └────────────────────────┘
```

The producer's message and its business change commit or roll back together. That
single property is what the pattern buys; everything else here exists to deliver on
it without losing, duplicating or stalling messages once the transaction has
committed.

Delivery is **at-least-once**. A replica can die between the broker accepting a
message and the database recording that, so a consumer must be idempotent — every
message carries its outbox id for that purpose (`MessageId` on AMQP, the
`message_id` header on Kafka).

Ordering is **not** guaranteed. Replicas publish concurrently, workers within a
replica publish concurrently, and a retried message lands after messages written
later. A consumer needing per-key order has to establish it itself.

The full contract, including what is deliberately not promised, is in
[docs/PublicContract.md](docs/PublicContract.md).

## Configuration

Every setting is an environment variable under `OUTBOX_`, read at startup and
validated before anything connects. A malformed value stops the process with
every problem listed at once, rather than producing a service that starts and is
permanently unready.

```
OUTBOX_DB_DSN=postgres://user:pass@db:5432/app?sslmode=require
OUTBOX_STREAMS=local
OUTBOX_STREAM_LOCAL_DRIVER=rmq
OUTBOX_DRIVER_RMQ_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_DSN=amqp://user:pass@rabbit:5672/
```

A **stream** is a logical destination a producer names in a row; a **driver** is
a connection to one broker. There may be as many drivers as there are brokers
to reach — several RabbitMQ instances, several Kafka clusters, or a mix of both
— and a producer picks between them with one column. Each stream gets its own
pipeline, so a broker that is down delays only the streams pointed at it.

Full reference: [docs/Config.md](docs/Config.md).

## Operating it

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness. Does not probe dependencies. |
| `GET /ready` | Readiness: every module reports healthy. |
| `GET /api/v1/stats` | Backlog counts, configured streams and drivers, settings. |
| `GET /api/v1/messages/failed` | Page through what stopped, and why. `?stream=` narrows it to one. |
| `POST /api/v1/messages/requeue` | Return failed messages to the queue. Requires `OUTBOX_HTTP_ADMIN_TOKEN`. |
| `GET /metrics` (port 9100) | Prometheus. |

The last three are also subcommands — `outbox stats`, `outbox failed`,
`outbox requeue` — reaching the database directly instead of the pod. Same store
calls, different authorisation: the endpoints take a token, the commands take the
database credentials.

Returning failed messages to the queue, from anywhere:

```bash
curl -X POST -H 'Authorization: Bearer $TOKEN' \
  localhost:8085/api/v1/messages/requeue \
  -d '{"failed_before":"2026-01-01T00:00:00Z"}'
```

```sql
SELECT outbox.requeue(ARRAY['…']::uuid[]);
```

Metrics, starting alert rules and a Grafana dashboard to import
([`dashboards/outbox.json`](dashboards/outbox.json)):
[docs/MetricsAndAlerts.md](docs/MetricsAndAlerts.md).

## Documentation

- [Use cases](docs/UseCases.md) — complete recipes: Laravel under Docker, a VDS
  with supervisord, Kubernetes with autoscaling, bare metal with systemd.
- [Public contract](docs/PublicContract.md) — the columns a producer writes, the
  routing rules, and what is explicitly not guaranteed.
- [Configuration](docs/Config.md) — every environment variable.
- [Metrics and alerts](docs/MetricsAndAlerts.md) — every metric, starting alert
  rules, and the dashboard.
- [Operations](docs/Operations.md) — deploying, scaling, and what to do when
  something is wrong.
- [Roadmap](docs/Roadmap.md) — what has shipped, what comes next in the order it
  would be built, and what is deliberately not planned.

## Development

```bash
make up                 # start PostgreSQL, RabbitMQ and Redpanda
make test               # unit tests
make test-integration   # integration tests against the real thing
make soak SOAK=1h       # the same failures, under load, for as long as you like
make bench              # the numbers quoted in the changelog
make lint               # golangci-lint, in a container pinned to the CI version
make fmt
```

`lint` and `fmt` run in a container rather than against whatever is on `PATH`,
because a linter that differs between a developer's machine and CI reports
different things — and the way that shows up is a green local run turning into a
red pipeline. `make lint-host` uses the binary on `PATH` when a faster loop
matters more than matching CI exactly.

The integration tests carry the weight of the suite on purpose: this is a set of
concurrency and ownership rules expressed in SQL, and those cannot be checked by
asserting that a query string has not changed.

## Requirements

- Go 1.26
- PostgreSQL 13 or newer
- RabbitMQ 3.8+ and/or Kafka 2.4+

## License

MIT.
