package store

import "fmt"

// queries holds the SQL with the configured schema and table already
// substituted, rendered once at construction.
type queries struct {
	claim         string
	ack           string
	nack          string
	releaseLease  string
	reclaim       string
	requeue       string
	requeueBefore string
	stats         string
	purge         string
	listFailed    string
	fetchByIDs    string
}

func newQueries(schema, table string) queries {
	return queries{
		claim:        fmt.Sprintf(claimSQL, table),
		ack:          fmt.Sprintf(ackSQL, table),
		nack:         fmt.Sprintf(nackSQL, table),
		releaseLease: fmt.Sprintf(releaseLeaseSQL, table),
		reclaim:      fmt.Sprintf(reclaimSQL, table),
		stats:        fmt.Sprintf(statsSQL, table),
		purge:        fmt.Sprintf(purgeSQL, table),
		listFailed:   fmt.Sprintf(listFailedSQL, table),
		fetchByIDs:   fmt.Sprintf(fetchByIDsSQL, table),

		requeue:       fmt.Sprintf(requeueSQL, schema),
		requeueBefore: fmt.Sprintf(requeueBeforeSQL, schema),
	}
}

// claimSQL takes a batch of due messages for one stream and leases them.
//
// FOR UPDATE SKIP LOCKED is what makes several replicas safe: two instances
// polling at the same instant take disjoint batches instead of colliding.
//
// Only pending rows are considered. Selecting pending and stale-processing rows
// in one statement joined by OR forces the planner to combine two partial
// indexes and sort the result; reclaiming expired leases is a separate
// operation here, with its own index and its own metric.
const claimSQL = `
WITH due AS (
    SELECT id
      FROM %[1]s
     WHERE status = 0
       AND stream = $1
       AND available_at <= now()
     ORDER BY available_at, id
     LIMIT $2
       FOR UPDATE SKIP LOCKED
)
UPDATE %[1]s m
   SET status      = 1,
       lease_token = $3,
       lease_until = now() + make_interval(secs => $4),
       owner       = $5
  FROM due
 WHERE m.id = due.id
RETURNING m.id, m.stream, m.topic, m.payload, m.headers, m.target, m.attempts, m.created_at`

// ackSQL records a delivery.
//
// The lease_token predicate is what makes running several replicas safe:
// without it, an instance whose lease had expired mid-flight would overwrite the
// status of a row another replica had already reclaimed and delivered —
// resurrecting a delivered message, or marking one sent while someone else was
// still publishing it.
//
// The lag is computed from the database clock alone. Measuring it as
// time.Since(created_at) in the process would fold the clock difference between
// the application and the database into the metric.
const ackSQL = `
UPDATE %[1]s
   SET status        = 2,
       dispatched_at = now(),
       lease_token   = NULL,
       lease_until   = NULL,
       owner         = NULL,
       last_error    = NULL
 WHERE id = ANY($1)
   AND lease_token = $2
RETURNING id, stream, extract(epoch FROM (now() - created_at))`

// nackSQL records a failure for a batch, each message with its own error,
// its own permanent/retryable classification and its own delay.
//
// The delay arrives computed rather than derived in SQL: the backoff policy —
// exponential, capped, jittered — belongs somewhere it can be unit-tested, and
// a POWER() expression buried in an UPDATE is not that place.
//
// A permanent failure goes straight to failed without spending the remaining
// attempts on an error that retrying cannot fix.
const nackSQL = `
UPDATE %[1]s m
   SET attempts    = m.attempts + 1,
       last_error  = left(v.err, 2000),
       lease_token = NULL,
       lease_until = NULL,
       owner       = NULL,
       status      = CASE WHEN v.permanent OR m.attempts + 1 >= $2 THEN 3 ELSE 0 END,
       available_at = CASE
                          WHEN v.permanent OR m.attempts + 1 >= $2 THEN m.available_at
                          ELSE now() + make_interval(secs => v.delay)
                      END
  FROM (
        SELECT * FROM unnest($3::uuid[], $4::text[], $5::bool[], $6::float8[])
            AS t(id, err, permanent, delay)
       ) v
 WHERE m.id = v.id
   AND m.lease_token = $1
RETURNING m.id, m.stream, m.status, m.attempts`

// releaseLeaseSQL hands unfinished claims back on a clean shutdown, so another
// replica picks them up immediately instead of waiting out the lease.
const releaseLeaseSQL = `
UPDATE %[1]s
   SET status       = 0,
       available_at = now(),
       lease_token  = NULL,
       lease_until  = NULL,
       owner        = NULL
 WHERE id = ANY($1)
   AND lease_token = $2
   AND status = 1
RETURNING id`

// reclaimSQL returns expired leases to pending.
//
// It returns them to the queue rather than claiming them directly. Recovery
// then travels the ordinary claim path, which keeps the hot query simple and
// makes reclaims separately observable — a rising reclaim rate means workers
// are dying or the lease is too short, and that should not be buried inside the
// claim counter.
//
// The old owner and overdue interval come from the CTE, not from RETURNING:
// RETURNING sees the updated row, whose owner this statement has just cleared.
const reclaimSQL = `
WITH expired AS (
    SELECT id, stream, owner, lease_until
      FROM %[1]s
     WHERE status = 1
       AND lease_until <= now()
     ORDER BY lease_until
     LIMIT $1
       FOR UPDATE SKIP LOCKED
)
UPDATE %[1]s m
   SET status       = 0,
       available_at = now(),
       lease_token  = NULL,
       lease_until  = NULL,
       owner        = NULL
  FROM expired
 WHERE m.id = expired.id
RETURNING expired.id, expired.stream, coalesce(expired.owner, ''),
          extract(epoch FROM (now() - expired.lease_until))`

// statsSQL samples the gauges.
//
// Three scalar sub-queries, each answered by its own partial index, and a
// min() over the pending-age index that stops at the first entry. Nothing here
// touches a delivered row — those are counted by
// outbox_messages_dispatched_total. A GROUP BY status across the whole table
// would be a sequential scan over every delivered row ever written.
const statsSQL = `
SELECT
    (SELECT count(*) FROM %[1]s WHERE status = 0),
    (SELECT count(*) FROM %[1]s WHERE status = 1),
    (SELECT count(*) FROM %[1]s WHERE status = 3),
    (SELECT coalesce(extract(epoch FROM (now() - min(created_at))), 0)
       FROM %[1]s WHERE status = 0)`

// purgeSQL removes delivered rows past their retention, in bounded chunks so
// the transaction stays short. SKIP LOCKED keeps two replicas from waiting on
// each other should both reach the sweep.
const purgeSQL = `
WITH doomed AS (
    SELECT id
      FROM %[1]s
     WHERE status = 2
       AND dispatched_at < now() - make_interval(secs => $1)
     ORDER BY dispatched_at
     LIMIT $2
       FOR UPDATE SKIP LOCKED
)
DELETE FROM %[1]s m
 USING doomed
 WHERE m.id = doomed.id`

// fetchByIDsSQL reads whole messages back, for the dead-letter forwarder. It
// takes no lease: the rows it reads are terminal, so nobody else is going to
// change them.
const fetchByIDsSQL = `
SELECT id, stream, topic, payload, headers, target, attempts, created_at
  FROM %[1]s
 WHERE id = ANY($1)`

// listFailedSQL pages through failed messages, newest last, for the admin
// endpoint. Keyset pagination rather than OFFSET: the table is large and an
// offset scan degrades with the page number.
// requeueSQL and requeueBeforeSQL call the functions shipped with the
// migrations rather than repeating their bodies. The operation exists as a
// database function so that an operator working in psql, the admin endpoint and
// the CLI all take the same path: it has to reset the attempt counter and the
// availability time along with the status, and a hand-written UPDATE that
// changes only the status produces a row that is never selected again.
const requeueSQL = `SELECT * FROM %[1]s.requeue($1::uuid[])`

const requeueBeforeSQL = `SELECT * FROM %[1]s.requeue_failed_before($1, $2)`

const listFailedSQL = `
SELECT id, stream, topic, attempts, coalesce(last_error, ''), created_at
  FROM %[1]s
 WHERE status = 3
   AND (created_at, id) > ($1, $2)
 ORDER BY created_at, id
 LIMIT $3`
