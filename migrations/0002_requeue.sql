-- Returning a failed message to the queue.
--
-- The previous version documented this as a raw UPDATE the consumer was to run
-- by hand:
--
--     UPDATE tech.outbox_messages
--     SET status = 'pending', last_try_at = NULL
--     WHERE id = $1 AND status = 'failed';
--
-- It did not work. Moving a message to failed also set next_retry_at to NULL,
-- and the claim query required next_retry_at IS NOT NULL, so a message
-- rehabilitated by the documented procedure was never selected again. The
-- retry counter was left at its maximum too, so even with that fixed the
-- message would fail again on its first error.
--
-- Shipping the operation as a function removes both traps: there is one
-- correct way to do it and it is not a snippet in a document.

CREATE OR REPLACE FUNCTION @schema@.requeue(p_ids UUID[])
    RETURNS SETOF UUID
    LANGUAGE sql
AS $$
UPDATE @schema@.@table@
SET status       = 0,
    attempts     = 0,
    available_at = now(),
    last_error   = NULL,
    lease_token  = NULL,
    lease_until  = NULL,
    owner        = NULL,
    dispatched_at = NULL
WHERE id = ANY (p_ids)
  AND status = 3
RETURNING id;
$$;

COMMENT ON FUNCTION @schema@.requeue(UUID[]) IS
    'Return failed messages to pending, resetting the attempt counter and clearing any lease.';

-- The bulk form, for reprocessing everything that failed before a point in
-- time without first collecting the identifiers.
CREATE OR REPLACE FUNCTION @schema@.requeue_failed_before(p_before TIMESTAMPTZ, p_limit INTEGER DEFAULT 1000)
    RETURNS SETOF UUID
    LANGUAGE sql
AS $$
UPDATE @schema@.@table@
SET status       = 0,
    attempts     = 0,
    available_at = now(),
    last_error   = NULL,
    lease_token  = NULL,
    lease_until  = NULL,
    owner        = NULL,
    dispatched_at = NULL
WHERE id IN (SELECT id
             FROM @schema@.@table@
             WHERE status = 3
               AND created_at < p_before
             ORDER BY created_at, id
             LIMIT p_limit)
RETURNING id;
$$;
