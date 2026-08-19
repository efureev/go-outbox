# Two services, two databases, no broker

English | [Русский](8-inbox-two-services.ru.md)

Back to the [use case index](../UseCases.md).

The orders service delivers events into the billing service's inbox. Each has its
own database and there is nothing between them — no RabbitMQ, no Kafka. The
classic outbox/inbox pair with no intermediary.

Before copying any of it: **read "What it costs" first**. This one has a price
that closes the question outright in a good many organisations, and it is better
learned here than at review.

### The inbox belongs to the consumer

Billing creates the table, in its own database, in its own migration. The full
example is in [migrations/inbox/messages.sql](../../migrations/inbox/messages.sql).

```sql
CREATE TABLE inbox.orders
(
    id      UUID PRIMARY KEY,
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);
```

The role the orders dispatcher uses needs to be able to do exactly one thing:

```sql
CREATE ROLE orders_dispatcher LOGIN PASSWORD '…';
GRANT USAGE          ON SCHEMA inbox TO orders_dispatcher;
GRANT INSERT, SELECT ON inbox.orders TO orders_dispatcher;
-- And nothing else: no UPDATE, no DELETE.
```

**`SELECT` is required here, and it is not a typo.** The driver only inserts, but
it does so with `ON CONFLICT (id) DO NOTHING`, and PostgreSQL requires read
access to the table when `ON CONFLICT` names a conflict target. Measured: with
`INSERT` alone a plain insert succeeds and the same insert with `ON CONFLICT
(id)` fails with `42501 permission denied`.

The target-less form — `ON CONFLICT DO NOTHING` — would need only `INSERT`, and
it is deliberately not used: it swallows **any** unique violation, not just a
repeat of the same `id`. A message with a new `id` but a taken business key would
then vanish silently and be reported as delivered. Measured by the same
experiment. The wider grant is the lesser evil: a lost message is worse than a
read privilege.

### The orders dispatcher's configuration

```dotenv
OUTBOX_DB_DSN=postgres://orders:secret@orders-db:5432/orders?sslmode=require
OUTBOX_STREAMS=billing

OUTBOX_STREAM_BILLING_DRIVER=inb
OUTBOX_DRIVER_INB_TYPE=postgres
OUTBOX_DRIVER_INB_DSN=postgres://orders_dispatcher:secret@billing-db:5432/billing?sslmode=require
OUTBOX_DRIVER_INB_SCHEMA=inbox
OUTBOX_DRIVER_INB_TABLE=orders
OUTBOX_DRIVER_INB_PREFIX=orders
```

`PREFIX` earns its place here: the `topic` column then holds `orders.order.paid`,
so an event can be filtered by its source without a column for it.

The credentials appear in `GET /api/v1/stats` **without the password** — the
endpoint strips it and names the destination table alongside:

```json
{ "name": "inb", "type": "postgres",
  "endpoint": "postgres://billing-db:5432/billing?sslmode=require#inbox.orders" }
```

### What the guarantees come to

Delivery is **at-least-once**, as with any broker: the insert into the inbox and
marking the outbox row `sent` are two commits in two different databases, and a
replica can die between them.

The primary key is what makes the difference. A repeat is absorbed by
`ON CONFLICT (id) DO NOTHING`, and the driver reports the conflict as a delivery —
the message is in the inbox either way. **Deduplication stops being an obligation
on billing's code and becomes a property of its schema.**

### What it costs

**An inversion of ownership.** The orders dispatcher gets write access to
billing's database. In many organisations that settles it, and arguing is
pointless — policy is policy.

If the conversation does continue, the mitigating facts are:

- The interface is narrow: one table, `INSERT` only, a fixed and documented
  shape. It is arguably narrower than a shared broker topic, where both sides
  agree on a message schema and neither can enforce it.
- The grant is checkable: `GRANT INSERT, SELECT` and nothing more — no `UPDATE`,
  no `DELETE`. The read is needed by `ON CONFLICT` itself, not by us: the
  dispatcher issues no `SELECT` against this table.
- But the credentials, the network path between databases and a migration
  coordinated between two teams do not go away.

**No fan-out.** An inbox is point-to-point. A third consumer means a third stream
and a third row from the producer. If you need a fan-out, you need a broker, and
this recipe is the wrong one.

### What goes wrong here

- **The network between the databases.** An unreachable inbox is `Unavailable`:
  messages are deferred **without spending an attempt**, the breaker stops
  claiming for that stream and `outbox_stream_paused` goes to one. Nothing is
  lost and nothing reaches `failed` — the same behaviour as a broker that went
  away.
- **A billing migration that dropped a column.** `42703 undefined_column` is
  `Permanent`: straight to `failed`, visible in `outbox failed`. That is right —
  retrying will not bring the column back. But it does mean inbox migrations have
  to be backwards compatible in exactly the way a broker message schema does.
- **A revoked grant.** `42501 insufficient_privilege`, also `Permanent`, for the
  same reason.
- **Forgetting to clean up.** Billing cleans the inbox. The orders dispatcher
  does not look at it and will not report that it is growing.
- **`sslmode=disable` between databases.** Within one host that is a choice;
  between hosts it is messages and credentials in the clear.

---
