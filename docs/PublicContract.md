# Public contract

English | [Русский](PublicContract.ru.md)

This document is the boundary between a producer and the dispatcher: what a
producer writes, what the dispatcher owns, how a message is routed, and what is
deliberately not promised.

Everything here is versioned. Anything not here — the internal columns, the
index set, the SQL, the metric names — may change without notice.

## 1. The table

**`<schema>.<table>`**, by default `outbox.messages`, in PostgreSQL.

### 1.1 What a producer writes

| Column | Type | Required | Meaning |
|---|---|---|---|
| `id` | `UUID` | yes | The message identity. Generate a UUIDv7: it sorts by creation time, so the primary key index stays append-ordered instead of scattering writes across the tree. This is also the identifier a consumer deduplicates on. |
| `stream` | `TEXT` | yes | The logical destination. Must name a configured stream. |
| `topic` | `TEXT` | yes | The logical topic or queue, **without** any prefix or version suffix. |
| `payload` | `BYTEA` | yes | Delivered to the broker byte for byte. The dispatcher never inspects it. |
| `headers` | `JSONB` | no | A flat string-to-string object, delivered as broker headers. Put `traceparent` here to let the consumer continue your trace. |
| `target` | `JSONB` | no | The routing envelope; see §2. |
| `available_at` | `TIMESTAMPTZ` | no | Delays the first attempt. Defaults to now. |

Every other column has a server default and belongs to the dispatcher. Naming
one in an INSERT is how a producer ends up writing a status or a lease it has no
business setting.

The write must happen **in the same transaction as the business change**. That is
the whole pattern; writing through a separate connection means the message can
be published for a change that was rolled back, or lost for one that was not.

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 42;
INSERT INTO outbox.messages (id, stream, topic, payload, headers, target)
VALUES (gen_random_uuid(), 'local', 'account.debited',
        convert_to('{"account":42,"amount":100}', 'UTF8'),
        '{"traceparent":"00-…-01"}'::jsonb,
        '{"key":"42"}'::jsonb);
COMMIT;
```

From Go, use [`pkg/outboxclient`](../pkg/outboxclient), which takes the
transaction as an argument so the transactional part is not something to
remember.

### 1.2 Returning a failed message to the queue

The only supported way, from any of three places:

```sql
SELECT outbox.requeue(ARRAY['…','…']::uuid[]);
SELECT outbox.requeue_failed_before(now() - interval '1 day', 1000);
```

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://outbox:8085/api/v1/messages/requeue \
  -d '{"ids":["…"]}'
```

Do not write your own UPDATE. Requeueing has to reset the attempt counter and
the availability time together with the status; a partial version of it produces
a row that is nominally pending and will never be selected again, which is a
much quieter failure than an error would have been.

### 1.3 Columns the dispatcher owns

Readable for observability, never writable.

| Column | Meaning |
|---|---|
| `status` | `0` pending, `1` processing, `2` sent, `3` failed. |
| `attempts` | Publish attempts made. |
| `lease_token`, `lease_until`, `owner` | The current claim: which replica holds the row and until when. |
| `last_error` | Why the last attempt failed. |
| `created_at` | Set by the database. |
| `dispatched_at` | When the broker accepted it. |

Two invariants are enforced by the schema rather than by convention: a row is
leased exactly while it is processing, and `attempts` is never negative.

## 2. Routing

### 2.1 `target`

| Key | Type | Meaning |
|---|---|---|
| `key` | string | Partition key. Kafka uses it; RabbitMQ ignores it. |
| `version` | integer | Appends a `vN` suffix to the topic name when above zero. |
| `exchange` | string | AMQP exchange. Empty means the default exchange. |
| `routing_key` | string | AMQP routing key. Empty means the topic name. |

Unknown keys are stored and ignored. That is the extension point.

### 2.2 The effective name

```
effective = [prefix + prefix_sep] + topic + [version_sep + "v" + version]
```

The prefix and separators come from the driver's configuration. **A consumer
subscribes to the effective name**, not to the `topic` column: with
`OUTBOX_DRIVER_RMQ_PREFIX=prod` and `topic = 'user.created'`, the queue is
`prod_user.created`.

`GET /api/v1/stats` reports each driver's prefix and separators, so the
effective name can be derived without reading anyone's configuration file.

## 3. Delivery semantics

| Status | Set by | Meaning |
|---|---|---|
| pending | producer, dispatcher | Ready once `available_at` has passed. |
| processing | dispatcher | Claimed, with a lease. |
| sent | dispatcher | The broker acknowledged it. Terminal. |
| failed | dispatcher | Attempts exhausted, or a permanent error. Terminal until requeued. |

**At-least-once.** A message that commits is published. It may be published more
than once: a replica can die between the broker accepting a message and the
database recording that. Consumers must be idempotent, and every message carries
its outbox id for that purpose — `MessageId` on AMQP, the `message_id` header on
Kafka.

**Confirmed publication.** A row becomes `sent` only after the broker says it has
the message: a publisher confirmation on AMQP, `acks=all` on Kafka by default.

**Permanent failures skip the retry budget.** An unroutable message, an unknown
stream, a payload above the broker's limit or a rejected credential fails at
once rather than spending five attempts and an hour of backoff rediscovering it.

## 4. What is not guaranteed

- **Ordering.** Replicas publish concurrently, workers within a replica publish
  concurrently, and a retried message lands after messages written later.
  Consumers needing per-key order must establish it themselves.
- **Latency.** With notifications enabled, delivery is typically within tens of
  milliseconds; with them disabled it is bounded by the poll interval. Neither
  is an SLA. `outbox_oldest_pending_age_seconds` is the metric to hold anyone to.
- **Retention.** Delivered rows are removed after `OUTBOX_JANITOR_RETENTION`. Do
  not use this table as an event log.
- **Uniqueness.** Two rows with the same payload are two messages. Deduplication,
  if wanted, is the producer's responsibility, keyed on `id`.

## 5. Versioning

The contract covers the producer columns (§1.1), requeueing (§1.2), the `target`
keys and naming rule (§2), the statuses and guarantees (§3), and the shape of
the documented HTTP responses.

A breaking change to any of them is a major version, called out in the release
notes.
