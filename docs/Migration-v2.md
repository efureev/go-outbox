# Migrating from v2

English | [Русский](Migration-v2.ru.md)

This is a rewrite, not an upgrade. The schema, the configuration and the module
path all change. What follows is what changed, why, and how to move a running
deployment across.

## Why not an in-place upgrade

Four defects in v2 could not be fixed without changing the table:

**Write-backs did not check lease ownership.** `UpdateSentMessages` and
`UpdateFailedMessages` updated rows by id alone. A replica whose processing
timeout had elapsed mid-flight would overwrite the status of a row another
replica had already reclaimed and delivered — resurrecting a delivered message
and republishing it, indefinitely. Fixing it requires a lease token, which is a
new column.

**The documented recovery procedure did not work.** Both `PublicContract.md` and
`Integration.md` instructed consumers to run:

```sql
UPDATE tech.outbox_messages SET status='pending', last_try_at=NULL
WHERE id = $1 AND status='failed';
```

The transition to `failed` set `next_retry_at` to `NULL`, and the claim query
required `next_retry_at IS NOT NULL`. A message rehabilitated by the documented
procedure was never selected again. `retry_count` was left at its maximum too,
so even with that corrected it would fail again on its first error.

**RabbitMQ confirmations could be crossed.** One `NotifyPublish` channel was
shared across every publish, and a single value was read after each. When a
publish timed out its confirmation stayed queued and the *next* message consumed
it — reported as delivered without the broker ever having confirmed it. Silent
message loss.

**The table grew without bound, and was counted on every poll.**
`CountByStatuses` ran `GROUP BY status` across the entire table every
`APP_POLLING_INTERVAL`, and nothing ever removed a delivered row.

Alongside those, the throughput ceiling was structural: one batch per poll
interval, published serially under a single channel mutex, with a
`QueueDeclare` round trip per message. A hundred messages every ten seconds,
whatever the hardware.

## What changed

| v2 | v3 | Why |
|---|---|---|
| `tech.outbox_messages` | `outbox.messages`, both configurable | The schema was hardcoded in the SQL. |
| `queue` | `topic` | It is a topic on Kafka; `queue` only ever fitted AMQP. |
| `message TEXT` | `payload BYTEA` | Binary payloads survive intact. |
| `status VARCHAR(255)` | `status SMALLINT` + CHECK | The set is closed; 255 bytes said otherwise. |
| `retry_count` | `attempts` | It counts attempts, including the first, which is not a retry. |
| `next_retry_at` | `available_at NOT NULL` | Never null, so a null can no longer make a row unreachable. |
| `processing_started_at` | `lease_token`, `lease_until`, `owner` | Ownership, not just a timestamp. |
| `version SMALLINT` column | `target.version` | It versioned the topic name, not the row. |
| — | `headers JSONB` | Trace context, content type, anything a consumer needs. |
| — | `last_error` | Why a message stopped, without reading logs. |
| `APP_*`, `BROKER_*`, `DRIVER_*` | all under `OUTBOX_` | One namespace. |
| durations as strings, parsed at use | `time.Duration`, parsed at load | A typo stops the process instead of producing a permanently unready service. |
| gin, uber/fx, six internal packages | net/http, appmod | Portability. |

Behaviour that stayed: the stream and driver model, the topic naming rule
(`prefix + sep + topic + sep + vN`) and its per-driver defaults, and
at-least-once delivery.

## Moving a deployment

The two versions can run side by side, because they read different tables. That
is the safe path.

**1. Create the new schema next to the old one.**

```bash
OUTBOX_DB_SCHEMA=outbox go run ./cmd/outbox migrate up
```

**2. Point producers at the new table.** Change the INSERT — or adopt
`pkg/outboxclient` — keeping the write inside the same business transaction:

```sql
-- v2
INSERT INTO tech.outbox_messages (id, status, queue, message, target, created_at)
VALUES ($1, 'pending', $2, $3, $4::jsonb, NOW());

-- v3: no status, no created_at, and the stream is its own column
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES ($1, $2, $3, $4, $5::jsonb);
```

The `stream` moves out of `target` into a column of its own; anything else in
`target` carries over unchanged.

**3. Run both dispatchers** until the v2 table stops receiving rows and its
backlog reaches zero:

```sql
SELECT status, count(*) FROM tech.outbox_messages
WHERE status IN ('pending', 'processing') GROUP BY status;
```

**4. Stop v2, and deal with what it left behind.** Anything stuck in `failed`
can be copied across rather than lost:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
SELECT id,
       lower(target ->> 'stream'),
       queue || CASE WHEN version > 0 THEN '_v' || version ELSE '' END,
       convert_to(message, 'UTF8'),
       target - 'stream'
FROM tech.outbox_messages
WHERE status = 'failed';
```

Note that the version suffix is folded into the topic here. Setting
`target.version` instead would be equivalent; folding it in avoids depending on
the new driver's separator matching the old one.

**5. Drop the old table** once nothing has referenced it for a retention period.

## If you cannot run both

A cutover is possible: stop producers, let v2 drain to zero, migrate the rows,
start v3, restart producers. It costs a window in which no business transaction
that writes an outbox message can commit, which is usually the more expensive
option.

Do **not** point v3 at the v2 table. The columns it needs are not there, and the
guarantees it makes depend on them.

## Configuration

| v2 | v3 |
|---|---|
| `APP_POLLING_INTERVAL` | `OUTBOX_DISPATCH_POLL_INTERVAL` |
| `APP_PROCESSING_TIMEOUT` | `OUTBOX_DISPATCH_LEASE_TTL` |
| `APP_LIMIT_PER_ITERATION` | `OUTBOX_DISPATCH_BATCH_SIZE` |
| `APP_BASE_DELAY` (seconds) | `OUTBOX_DISPATCH_BACKOFF_BASE` (a duration) |
| `APP_MAX_RETRY_COUNT` | `OUTBOX_DISPATCH_MAX_ATTEMPTS` |
| `BROKER_STREAMS` | `OUTBOX_STREAMS` |
| `STREAM_<S>_DRIVER` | `OUTBOX_STREAM_<S>_DRIVER` |
| `DRIVER_<D>_*` | `OUTBOX_DRIVER_<D>_*` |
| `DB_*` | `OUTBOX_DB_*` |
| `PROM_PORT`, `PROM_PATH` | `OUTBOX_METRICS_PORT`, `OUTBOX_METRICS_PATH` |
| `APP_AUTH_SECRET` | removed; see `OUTBOX_HTTP_ADMIN_TOKEN` |

New and worth setting deliberately: `OUTBOX_JANITOR_RETENTION` (v2 had none, and
the table grew forever) and `OUTBOX_DISPATCH_BACKOFF_MAX` (v2 doubled without a
ceiling).

## Metrics

Names changed enough that dashboards need editing. The mapping:

| v2 | v3 |
|---|---|
| `outbox_messages_claimed_total{source,…}` | `outbox_messages_claimed_total{…}` + `outbox_messages_reclaimed_total` |
| `outbox_messages_processed_total{status="sent"}` | `outbox_messages_dispatched_total` |
| `outbox_messages_processed_total{status="requeued"}` | `outbox_messages_retried_total` |
| `outbox_messages_failed_total` | same, now with `stream`, `driver` and `reason` |
| `outbox_processing_errors_total{stage}` | `outbox_db_errors_total{op}` |
| `outbox_broker_operation_errors_total` | `outbox_broker_errors_total`, now with `kind` |
| `outbox_pending_messages` | removed; it duplicated `outbox_messages_by_status` |
| — | `outbox_oldest_pending_age_seconds`, `outbox_lease_conflicts_total`, `outbox_batch_size` |
