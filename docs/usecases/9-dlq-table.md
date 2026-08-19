# Dead letters in a table, not a topic

English | [Русский](9-dlq-table.ru.md)

Back to the [use case index](../UseCases.md).

Messages that stopped being retried are collected in a table — read with eyes and
a `SELECT` rather than by a subscriber.

The motive is plain: a dead-letter queue in a broker is almost always a queue
nobody subscribes to. A person works through it, and a person would rather have
`WHERE` and `ORDER BY` than a consumer written for the sole purpose of looking.

It needs **no code change at all**: the dead-letter forwarder publishes to an
ordinary stream through the same router, so pointing the DLQ stream at a
`postgres` driver is the whole of it.

### The table

```sql
CREATE TABLE ops.dead_letter
(
    id      UUID PRIMARY KEY,
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The forwarder puts the circumstances in the headers; that is what you sort by.
CREATE INDEX dead_letter_origin_idx
    ON ops.dead_letter ((headers ->> 'x-outbox-original-stream'), received_at);
```

### Configuration

```dotenv
OUTBOX_DLQ_ENABLED=true
OUTBOX_DLQ_STREAM=dead_letter
OUTBOX_DLQ_TOPIC=outbox.dead-letter

OUTBOX_STREAMS=events,dead_letter

OUTBOX_STREAM_DEAD_LETTER_DRIVER=dlq_table
OUTBOX_DRIVER_DLQ_TABLE_TYPE=postgres
OUTBOX_DRIVER_DLQ_TABLE_DSN=            # empty: the dispatcher's own database
OUTBOX_DRIVER_DLQ_TABLE_SCHEMA=ops
OUTBOX_DRIVER_DLQ_TABLE_TABLE=dead_letter
```

### What working through it looks like

The forwarder attaches the circumstances of the death to the message:

```sql
SELECT headers ->> 'x-outbox-original-stream' AS stream,
       headers ->> 'x-outbox-original-topic'  AS topic,
       headers ->> 'x-outbox-attempts'        AS attempts,
       headers ->> 'x-outbox-permanent'       AS permanent,
       count(*)
  FROM ops.dead_letter
 WHERE received_at > now() - interval '1 day'
 GROUP BY 1, 2, 3, 4
 ORDER BY count(*) DESC;
```

One query answers the question that would have needed a consumer against a
broker: what stopped being sent in the last day, and why.

### It is a signal, not the record

The outbox row stays where it is with status `failed` — that is the source of
truth. The dead-letter table exists so that somebody **looks**; requeueing still
goes through `outbox requeue` or `POST /api/v1/messages/requeue`, and both work
on the outbox rather than on this table.

Which has a useful consequence: deleting a row from here is safe. Nothing is
lost.

### What goes wrong here

- **Trying to requeue from this table.** It will not work: requeue changes a
  status in the outbox. What is here is a copy to read.
- **An `OUTBOX_DLQ_STREAM` that is not in `OUTBOX_STREAMS`.** Refused while the
  configuration is being read, together with every other mistake at once.
- **Forgetting to clean up.** Like any inbox, this table is ours to write and
  yours to keep. Here it is easiest of all: rows older than a month can go
  without a second thought, because the source of truth is in the outbox.
- **Expecting everything that failed to appear here.** What appears is what
  **stopped being retried**. A message waiting for its next attempt, or deferred
  because its broker is unreachable, will not be here — and should not be.

---
