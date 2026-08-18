# go-outbox

English | [Русский](README.ru.md)

A Transactional Outbox dispatcher. A producer writes a message to a database
table inside the same transaction as the business change it describes; this
service reads those rows and publishes them to RabbitMQ or Kafka.

That single property — the message and the change commit or roll back together —
is what the pattern buys. Everything else here exists to deliver on it without
losing, duplicating or stalling messages once the transaction has committed.

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

## What it guarantees

- **At-least-once delivery.** A message that commits is published, eventually.
  A consumer must be idempotent; every message carries its outbox id
  (`MessageId` on AMQP, the `message_id` header on Kafka) to deduplicate on.
- **Confirmed publication.** A row is marked delivered only after the broker
  says it has it: a publisher confirmation on AMQP, `acks=all` on Kafka.
- **Safe horizontal scaling.** Any number of replicas may run against one table.
  Claims are taken with `FOR UPDATE SKIP LOCKED` and carry a lease token that
  every write-back must present, so a replica whose lease expired mid-flight
  cannot overwrite the outcome recorded by whoever reclaimed the row.
- **Recovery without intervention.** A replica that dies mid-batch leaves rows
  leased; the lease expires and another replica picks them up. A clean shutdown
  does not even wait for that — it hands unfinished claims back immediately.

What it does **not** guarantee is ordering. Several replicas publish
concurrently, and a retried message lands after messages written later. If a
consumer needs per-key ordering it has to establish it itself.

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
a connection to one broker. Several streams may share a driver, and one
deployment may publish to RabbitMQ and Kafka at once. Each stream gets its own
pipeline, so a broker that is down delays only its own messages.

Full reference: [docs/Config.md](docs/Config.md).

## Operating it

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness. Does not probe dependencies. |
| `GET /ready` | Readiness: every module reports healthy. |
| `GET /api/v1/stats` | Backlog counts, configured streams and drivers, settings. |
| `GET /api/v1/messages/failed` | Page through what stopped, and why. |
| `POST /api/v1/messages/requeue` | Return failed messages to the queue. Requires `OUTBOX_HTTP_ADMIN_TOKEN`. |
| `GET /metrics` (port 9100) | Prometheus. |

Returning failed messages to the queue, from anywhere:

```bash
curl -X POST -H 'Authorization: Bearer $TOKEN' \
  localhost:8085/api/v1/messages/requeue \
  -d '{"failed_before":"2026-01-01T00:00:00Z"}'
```

```sql
SELECT outbox.requeue(ARRAY['…']::uuid[]);
```

Metrics and starting alert rules: [docs/MetricsAndAlerts.md](docs/MetricsAndAlerts.md).

## Documentation

- [Public contract](docs/PublicContract.md) — the columns a producer writes, the
  routing rules, and what is explicitly not guaranteed.
- [Configuration](docs/Config.md) — every environment variable.
- [Metrics and alerts](docs/MetricsAndAlerts.md).
- [Operations](docs/Operations.md) — deploying, scaling, and what to do when
  something is wrong.

## Development

```bash
make up                 # start PostgreSQL, RabbitMQ and Redpanda
make test               # unit tests
make test-integration   # integration tests against the real thing
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
