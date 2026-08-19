-- Deferral: a message held back because its broker could not be reached.
--
-- Before this, every failure advanced the attempt counter, which conflated two
-- events that deserve opposite responses. A broker that looks at a message and
-- refuses it should exhaust a budget — retrying forever will not change its
-- mind. A broker that cannot be reached at all never saw the message, and
-- charging the message for that outage spends its budget on somebody else's
-- problem: at the default backoff the whole budget is gone in fifteen minutes,
-- so a twenty-minute restart used to leave a table full of failed rows that
-- only ever needed to wait.
--
-- deferred_since records when a message was first held back this way. It is the
-- start of the window OUTBOX_DISPATCH_MAX_DEFER bounds, and it is cleared the
-- moment the message goes through, is failed, or fails for a reason the broker
-- actually gave — so the window measures a continuous outage rather than the
-- lifetime of the row.

ALTER TABLE @schema@.@table@
    ADD COLUMN IF NOT EXISTS deferred_since TIMESTAMPTZ;

COMMENT ON COLUMN @schema@.@table@.deferred_since IS
    'When this message was first held back by an unreachable broker, cleared as soon as it is '
    'delivered, failed, or fails for a reason the broker gave. NULL means nothing is waiting on '
    'a broker to come back.';

-- A partial index over a column that is NULL for essentially every row costs
-- almost nothing to carry and makes the "how much is stuck behind a broker that
-- is down" gauge index-only, so it can be sampled alongside the other counts
-- rather than being the one that scans the table.
CREATE INDEX IF NOT EXISTS @table@_deferred_idx
    ON @schema@.@table@ (deferred_since)
    WHERE deferred_since IS NOT NULL;

-- The requeue functions have to clear the new column along with everything else
-- they reset. A requeued message that kept a deferred_since from an outage
-- weeks ago would be failed by the very next deferral, having waited none of
-- that window itself.
--
-- They are replaced here rather than edited in 0002: a released migration is
-- immutable, and the runner's checksums say so.

CREATE OR REPLACE FUNCTION @schema@.requeue(p_ids UUID[])
    RETURNS SETOF UUID
    LANGUAGE sql
AS $$
UPDATE @schema@.@table@
SET status         = 0,
    attempts       = 0,
    available_at   = now(),
    last_error     = NULL,
    lease_token    = NULL,
    lease_until    = NULL,
    owner          = NULL,
    dispatched_at  = NULL,
    deferred_since = NULL
WHERE id = ANY (p_ids)
  AND status = 3
RETURNING id;
$$;

CREATE OR REPLACE FUNCTION @schema@.requeue_failed_before(p_before TIMESTAMPTZ, p_limit INTEGER DEFAULT 1000)
    RETURNS SETOF UUID
    LANGUAGE sql
AS $$
UPDATE @schema@.@table@
SET status         = 0,
    attempts       = 0,
    available_at   = now(),
    last_error     = NULL,
    lease_token    = NULL,
    lease_until    = NULL,
    owner          = NULL,
    dispatched_at  = NULL,
    deferred_since = NULL
WHERE id IN (SELECT id
             FROM @schema@.@table@
             WHERE status = 3
               AND created_at < p_before
             ORDER BY created_at, id
             LIMIT p_limit)
RETURNING id;
$$;
