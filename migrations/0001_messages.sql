-- The outbox table.
--
-- @schema@ and @table@ are substituted by the migration runner from
-- OUTBOX_DB_SCHEMA and OUTBOX_DB_TABLE. Both are validated against a strict
-- identifier pattern before they reach here.

CREATE TABLE IF NOT EXISTS @schema@.@table@
(
    -- Written by the producer, inside its own business transaction.
    id      UUID PRIMARY KEY,
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,

    -- headers travel with the message to the broker. A traceparent placed here
    -- lets the consumer continue the producer's trace, which is not possible
    -- when the row carries nothing but a payload.
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- target routes the message: partition key, topic version, and the
    -- exchange/routing key for AMQP.
    target  JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Everything below is written only by the dispatcher.
    status        SMALLINT    NOT NULL DEFAULT 0,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- lease_token identifies one claim. A write-back must present it, so an
    -- instance whose lease expired mid-flight cannot overwrite the outcome
    -- recorded by whoever reclaimed the row.
    lease_token   UUID,
    lease_until   TIMESTAMPTZ,
    owner         TEXT,

    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at TIMESTAMPTZ,

    CONSTRAINT @table@_status_check CHECK (status BETWEEN 0 AND 3),
    CONSTRAINT @table@_attempts_check CHECK (attempts >= 0),

    -- A row is leased exactly while it is being processed. The constraint is
    -- what makes the ownership rule an invariant of the data rather than a
    -- convention every query has to remember.
    CONSTRAINT @table@_lease_check CHECK (
        ((status = 1) = (lease_token IS NOT NULL))
        AND ((lease_token IS NULL) = (lease_until IS NULL))
    )
);

COMMENT ON TABLE @schema@.@table@ IS
    'Transactional outbox. Producers insert (id, stream, topic, payload, target[, headers]) '
    'inside their business transaction; every other column belongs to the dispatcher.';

-- Claim path. The index carries exactly the predicate and the ordering the
-- claim query uses, and covers only pending rows.
--
-- The previous version selected pending and stale-processing rows in one
-- statement joined by OR, so the planner had to combine two partial indexes
-- and sort the result — the opposite of what the comment above that query
-- claimed. Reclaiming is a separate concern here, with its own index below.
CREATE INDEX IF NOT EXISTS @table@_claim_idx
    ON @schema@.@table@ (stream, available_at, id)
    WHERE status = 0;

-- Backlog age. A second partial index over pending rows is a deliberate cost:
-- it makes both "how many are waiting" and "how long has the oldest waited"
-- index-only, and the second is O(1) because created_at leads. That metric is
-- the clearest signal that delivery is falling behind, so it has to be cheap
-- enough to sample every half minute. The previous version answered the same
-- question with a GROUP BY over the entire table on every poll iteration.
CREATE INDEX IF NOT EXISTS @table@_pending_age_idx
    ON @schema@.@table@ (created_at)
    WHERE status = 0;

-- Reclaim path: expired leases.
CREATE INDEX IF NOT EXISTS @table@_lease_idx
    ON @schema@.@table@ (lease_until)
    WHERE status = 1;

-- Retention sweep.
CREATE INDEX IF NOT EXISTS @table@_dispatched_idx
    ON @schema@.@table@ (dispatched_at)
    WHERE status = 2;

-- Inspecting what failed.
CREATE INDEX IF NOT EXISTS @table@_failed_idx
    ON @schema@.@table@ (created_at, id)
    WHERE status = 3;
