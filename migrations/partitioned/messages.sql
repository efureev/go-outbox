-- The outbox table, range-partitioned by day.
--
-- WHO THIS IS FOR
--
-- Deployments past roughly ten million rows a day. Below that the ordinary
-- table is simpler and behaves better, and adopting this buys nothing but
-- moving parts. At that volume the retention sweep stops keeping up: a chunked
-- DELETE creates dead tuples faster than autovacuum reclaims them, so the table
-- keeps growing while apparently being cleaned. Dropping a partition is a
-- catalogue change and an unlink, and costs the same whatever it held.
--
-- HOW TO USE IT
--
-- Apply this file first, with psql, against an empty schema. Then run
-- `outbox migrate up` as usual: 0001 creates the table only IF NOT EXISTS, so
-- it finds this one, and its indexes are created on the parent and propagated
-- to every partition. Nothing else in the migration set knows the difference.
--
--   psql "$DSN" -f migrations/partitioned/messages.sql
--   outbox migrate up
--
-- The dispatcher notices the shape of the table by itself and switches its
-- retention from deleting rows to dropping partitions. Every query it runs is
-- unchanged: partitioning is transparent to DML.
--
-- Substitute the identifiers below if OUTBOX_DB_SCHEMA or OUTBOX_DB_TABLE are
-- not the defaults.
--
-- WHAT IT COSTS
--
-- The primary key has to change. PostgreSQL requires a unique constraint on a
-- partitioned table to include the partition key, so `id` alone cannot be the
-- primary key and becomes `(id, created_at)`. The database therefore no longer
-- enforces that an id appears once across the whole table — only once per day.
-- Two rows with the same id and different created_at would be published twice
-- under one message id, which consumers already have to tolerate under
-- at-least-once delivery, but it is a guarantee given up rather than a detail.
--
-- Claiming stays fast. Measured on 405k rows across 31 daily partitions, the
-- claim query executes in about 0.25 ms against 0.18 ms unpartitioned: the
-- partial indexes on older partitions are empty, so the merge across them costs
-- almost nothing. Planning is what partitioning makes expensive — about 2 ms
-- against 0.4 ms — and the dispatcher pays it once because pgx prepares its
-- statements, after which it is 0.29 ms.

CREATE SCHEMA IF NOT EXISTS outbox;

CREATE TABLE IF NOT EXISTS outbox.messages
(
    -- Written by the producer, inside its own business transaction.
    id      UUID  NOT NULL,
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,

    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    target  JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Everything below is written only by the dispatcher.
    status        SMALLINT    NOT NULL DEFAULT 0,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    lease_token   UUID,
    lease_until   TIMESTAMPTZ,
    owner         TEXT,

    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at TIMESTAMPTZ,

    -- The partition key has to be part of every unique constraint, which is
    -- why this is not `id` alone. See WHAT IT COSTS above.
    PRIMARY KEY (id, created_at),

    CONSTRAINT messages_status_check CHECK (status BETWEEN 0 AND 3),
    CONSTRAINT messages_attempts_check CHECK (attempts >= 0),

    CONSTRAINT messages_lease_check CHECK (
        ((status = 1) = (lease_token IS NOT NULL))
        AND ((lease_token IS NULL) = (lease_until IS NULL))
    )
) PARTITION BY RANGE (created_at);

COMMENT ON TABLE outbox.messages IS
    'Transactional outbox, partitioned by day. Producers insert '
    '(id, stream, topic, payload, target[, headers]) inside their business transaction; every '
    'other column belongs to the dispatcher.';

-- The catch-all, and the reason a missing daily partition is an inconvenience
-- rather than an outage.
--
-- A row that fits no partition is a failed INSERT, and that INSERT is inside
-- the producer's business transaction: without this, a janitor that stopped
-- running would not delay messages, it would roll back whatever the application
-- was doing. Rows that land here are counted by outbox_default_partition_rows
-- and reported in the log, because until they are moved the proper partition
-- for their range cannot be created.
CREATE TABLE IF NOT EXISTS outbox.messages_default
    PARTITION OF outbox.messages DEFAULT;

-- Today, so the table is usable the moment this file finishes. The dispatcher's
-- janitor keeps OUTBOX_JANITOR_PARTITION_AHEAD days created in front of itself
-- from then on, and drops the ones retention has finished with.
DO $$
DECLARE
    day DATE := current_date;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS outbox.%I PARTITION OF outbox.messages '
        'FOR VALUES FROM (%L) TO (%L)',
        'messages_' || to_char(day, 'YYYYMMDD'), day, day + 1);
END $$;
