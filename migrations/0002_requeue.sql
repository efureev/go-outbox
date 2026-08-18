-- Returning a failed message to the queue.
--
-- The obvious hand-written version of this is wrong in two ways at once:
--
--     UPDATE outbox.messages
--     SET status = 0
--     WHERE id = $1 AND status = 3;
--
-- The row keeps an available_at from whenever it last failed and an attempts
-- counter already at the maximum, so it is either never claimed again or fails
-- for good on its first error. Both have to be reset along with the status.
--
-- Shipping the operation as a function removes both traps: there is one correct
-- way to do it, and it is not a snippet in a document for everyone to copy.

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
