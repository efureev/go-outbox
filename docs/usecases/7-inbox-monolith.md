# A modular monolith: an inbox instead of a broker

English | [Русский](7-inbox-monolith.ru.md)

Back to the [use case index](../UseCases.md).

The application is split into bounded contexts — orders, billing, notifications —
but there is one database. Orders publish an event, billing reads it. **There is
no broker at all.**

The dispatcher delivers into a table: `OUTBOX_DRIVER_*_TYPE=postgres`. The
producer does not know that. It writes a row to the outbox exactly as it would
for RabbitMQ.

### The inbox

The table is created by its owner — the context that will read from it. The full
example, with the reasoning, is in
[migrations/inbox/messages.sql](../../migrations/inbox/messages.sql).

```sql
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE billing.inbox
(
    -- Written by the dispatcher. INSERT only.
    id      UUID PRIMARY KEY,   -- the outbox row id; deduplication rests on it
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,     -- the effective name: prefix and version applied
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Your columns. The dispatcher does not know they exist.
    processed_at TIMESTAMPTZ
);

CREATE INDEX inbox_unprocessed_idx ON billing.inbox (received_at)
    WHERE processed_at IS NULL;
```

`PRIMARY KEY (id)` is not decoration. Delivery is at-least-once: the insert here
and marking the outbox row `sent` are two commits, and a replica can die between
them. The primary key makes the repeat harmless, because the driver inserts with
`ON CONFLICT (id) DO NOTHING` and reports a conflict as a delivery.

### Configuration

```dotenv
OUTBOX_DB_DSN=postgres://app:secret@localhost:5432/app?sslmode=disable
OUTBOX_DB_SCHEMA=outbox

OUTBOX_STREAMS=billing,notifications

OUTBOX_STREAM_BILLING_DRIVER=inb_billing
OUTBOX_DRIVER_INB_BILLING_TYPE=postgres
OUTBOX_DRIVER_INB_BILLING_DSN=              # empty: the dispatcher's own database
OUTBOX_DRIVER_INB_BILLING_SCHEMA=billing
OUTBOX_DRIVER_INB_BILLING_TABLE=inbox

OUTBOX_STREAM_NOTIFICATIONS_DRIVER=inb_notify
OUTBOX_DRIVER_INB_NOTIFY_TYPE=postgres
OUTBOX_DRIVER_INB_NOTIFY_SCHEMA=notifications
OUTBOX_DRIVER_INB_NOTIFY_TABLE=inbox
```

An empty `DSN` means the database the dispatcher already reads its outbox from.
Nothing is duplicated, and two connection strings cannot drift apart.

### The producer does not change

```go
// Orders. Nothing here knows about billing.inbox, and nothing should.
err := outbox.Enqueue(ctx, tx, outboxclient.Message{
    Stream:  "billing",
    Topic:   "order.paid",
    Payload: payload,
})
```

The `billing` stream goes to a table today, to RabbitMQ tomorrow and to Kafka the
day after. Switching is an environment variable on the dispatcher, not an edit to
the application.

### Reading the inbox

That is your work, not the dispatcher's. It inserts and stops; there is no
processing loop here, no lease, no consumer framework — and there will not be.

```sql
-- The simplest form, inside your transaction.
SELECT id, topic, payload, headers
  FROM billing.inbox
 WHERE processed_at IS NULL
 ORDER BY received_at
 LIMIT 100
   FOR UPDATE SKIP LOCKED;
```

Set `processed_at` in the same transaction once you are done. You need no
deduplication: `id` is unique, so a repeat never reaches you.

### The local setup

A side benefit that often turns out to be the main one: the whole application
comes up with one command.

```bash
docker compose up postgres
```

No RabbitMQ and no Redpanda in a developer's `docker-compose.yml`. The same holds
in CI: integration tests need no broker, because there is none.

### What goes wrong here

- **Forgetting to clean the inbox.** The janitor sweeps **its own** table, the
  outbox. It does not touch the inbox, which then grows quietly: no metric and no
  log will say so, because nothing on our side is looking at it. This is the one
  failure that surfaces six months later rather than immediately. Clean it
  yourself: `DELETE FROM billing.inbox WHERE processed_at < now() - interval '7
  days'`.
- **Expecting fan-out.** An inbox is point-to-point. For an event to reach both
  billing and notifications the producer writes **two rows**, one per stream, and
  a third consumer becomes an edit to the producer. If you need a fan-out, you
  need a broker.
- **An inbox without `PRIMARY KEY (id)`.** Then there is no deduplication, and a
  repeat after a replica dies becomes a second message. The driver will not
  notice: it inserts and is told it worked.
- **A schema or table that does not match the configuration.** The dispatcher
  refuses to start — the table must exist, and it does not create it. That is
  deliberate: a destination that is not there is a configuration mistake, and
  configuration mistakes belong at the boot.
- **Pointing a driver at the outbox table itself.** Refused at startup: that is
  delivery to itself, where every published message becomes a new message to
  publish.

---
