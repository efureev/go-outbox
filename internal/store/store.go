// Package store is the database side of the dispatcher: claiming a batch,
// recording what happened to it, recovering leases their owner never released,
// and the housekeeping around all three.
//
// The rule that runs through every write is lease ownership. A claim stamps a
// row with a token; every statement that finalizes a row requires that token to
// match. It is what makes running several replicas safe — without it, claiming
// concurrently is safe but recording the outcome is not.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// Store runs the outbox queries against one table.
type Store struct {
	pool  *pgxpool.Pool
	q     queries
	table string
	// schema and tableName are kept apart from the qualified name because
	// partition maintenance builds child table names from them, and taking
	// them back apart from the quoted form would mean parsing what was just
	// assembled.
	schema    string
	tableName string
}

// New builds a Store for the configured schema and table. Both identifiers are
// validated by config.Validate before they reach here, which is what makes
// interpolating them into SQL safe — a placeholder cannot stand in for an
// identifier.
func New(pool *pgxpool.Pool, cfg config.DBConfig) *Store {
	table := fmt.Sprintf("%q.%q", cfg.Schema, cfg.Table)

	return &Store{
		pool:      pool,
		q:         newQueries(quoteIdent(cfg.Schema), table),
		table:     table,
		schema:    quoteIdent(cfg.Schema),
		tableName: cfg.Table,
	}
}

// Pool exposes the underlying pool for the listener, which needs a dedicated
// connection.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Claim takes up to limit due messages from one stream and leases them to the
// caller. The returned messages are the caller's until lease.Until passes.
func (s *Store) Claim(ctx context.Context, stream string, limit int, lease core.Lease) ([]core.Message, error) {
	ttl := time.Until(lease.Until).Seconds()
	if ttl <= 0 {
		return nil, fmt.Errorf("claim %s: lease already expired", stream)
	}

	rows, err := s.pool.Query(ctx, s.q.claim, stream, limit, lease.Token, ttl, lease.Owner)
	if err != nil {
		return nil, fmt.Errorf("claim %s: %w", stream, err)
	}
	defer rows.Close()

	messages := make([]core.Message, 0, limit)
	for rows.Next() {
		var (
			m       core.Message
			headers []byte
			target  []byte
		)

		if err := rows.Scan(&m.ID, &m.Stream, &m.Topic, &m.Payload, &headers, &target,
			&m.Attempts, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan claimed message: %w", err)
		}

		if err := decodeJSON(headers, &m.Headers); err != nil {
			return nil, fmt.Errorf("message %s: decode headers: %w", m.ID, err)
		}
		if err := decodeJSON(target, &m.Target); err != nil {
			return nil, fmt.Errorf("message %s: decode target: %w", m.ID, err)
		}

		messages = append(messages, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim %s: %w", stream, err)
	}

	return messages, nil
}

// Delivered is one row the broker accepted, with the lag measured entirely by
// the database clock.
type Delivered struct {
	ID     string
	Stream string
	Lag    time.Duration
}

// AckResult reports what an Ack actually changed.
type AckResult struct {
	Delivered []Delivered
	// Conflicts is how many of the given ids were not ours any more, because
	// the lease had been reclaimed while we were publishing. It is not an error
	// — the message belongs to whoever holds the lease now — but it means the
	// lease is shorter than the work, and it is exported as a metric for
	// exactly that reason.
	Conflicts int
}

// Ack marks messages as delivered, but only those still held by token.
func (s *Store) Ack(ctx context.Context, ids []string, token string) (AckResult, error) {
	var res AckResult
	if len(ids) == 0 {
		return res, nil
	}

	rows, err := s.pool.Query(ctx, s.q.ack, ids, token)
	if err != nil {
		return res, fmt.Errorf("ack: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			d       Delivered
			seconds float64
		)
		if err := rows.Scan(&d.ID, &d.Stream, &seconds); err != nil {
			return res, fmt.Errorf("scan ack result: %w", err)
		}
		d.Lag = time.Duration(seconds * float64(time.Second))
		res.Delivered = append(res.Delivered, d)
	}

	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("ack: %w", err)
	}

	res.Conflicts = len(ids) - len(res.Delivered)

	return res, nil
}

// NackOutcome is what became of one failed message.
type NackOutcome struct {
	ID       string
	Stream   string
	Status   core.Status
	Attempts int
	// Deferred reports that the row went back to pending still waiting on an
	// unreachable broker, rather than because the broker gave a reason. It is
	// read from the stored marker, so it cannot disagree with what the row says.
	Deferred bool
}

// NackResult reports the outcome of a batch of failures.
type NackResult struct {
	Outcomes  []NackOutcome
	Conflicts int
}

// Retried and Failed split the outcomes by where the message ended up.
func (r NackResult) Retried() []NackOutcome { return r.filter(core.StatusPending) }
func (r NackResult) Failed() []NackOutcome  { return r.filter(core.StatusFailed) }

// Deferred is the subset of Retried that is waiting on a broker rather than on
// a backoff it earned.
func (r NackResult) Deferred() []NackOutcome {
	var out []NackOutcome
	for _, o := range r.Outcomes {
		if o.Deferred {
			out = append(out, o)
		}
	}

	return out
}

func (r NackResult) filter(status core.Status) []NackOutcome {
	var out []NackOutcome
	for _, o := range r.Outcomes {
		if o.Status == status {
			out = append(out, o)
		}
	}

	return out
}

// Nack records a batch of failures, each with its own error text,
// classification and retry delay, against the lease that produced them.
func (s *Store) Nack(
	ctx context.Context,
	outcomes []core.Outcome,
	token string,
	limits core.RetryLimits,
) (NackResult, error) {
	var res NackResult
	if len(outcomes) == 0 {
		return res, nil
	}

	ids := make([]string, len(outcomes))
	errs := make([]string, len(outcomes))
	permanent := make([]bool, len(outcomes))
	deferred := make([]bool, len(outcomes))
	delays := make([]float64, len(outcomes))

	for i, o := range outcomes {
		ids[i] = o.ID
		errs[i] = errText(o.Err)
		permanent[i] = o.Permanent
		deferred[i] = o.Deferred && !o.Permanent
		delays[i] = o.Delay.Seconds()
	}

	rows, err := s.pool.Query(ctx, s.q.nack,
		token, limits.MaxAttempts, ids, errs, permanent, deferred, delays, limits.MaxDefer.Seconds())
	if err != nil {
		return res, fmt.Errorf("nack: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o NackOutcome
		if err := rows.Scan(&o.ID, &o.Stream, &o.Status, &o.Attempts, &o.Deferred); err != nil {
			return res, fmt.Errorf("scan nack result: %w", err)
		}
		res.Outcomes = append(res.Outcomes, o)
	}

	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("nack: %w", err)
	}

	res.Conflicts = len(outcomes) - len(res.Outcomes)

	return res, nil
}

// ReleaseLease hands unfinished claims back to the queue and reports how many
// it released. It is what a clean shutdown does with work it did not get to.
func (s *Store) ReleaseLease(ctx context.Context, ids []string, token string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	rows, err := s.pool.Query(ctx, s.q.releaseLease, ids, token)
	if err != nil {
		return 0, fmt.Errorf("release lease: %w", err)
	}
	defer rows.Close()

	released := 0
	for rows.Next() {
		released++
	}

	if err := rows.Err(); err != nil {
		return released, fmt.Errorf("release lease: %w", err)
	}

	return released, nil
}

// Reclaimed is one lease whose owner never released it.
type Reclaimed struct {
	ID      string
	Stream  string
	Owner   string
	Overdue time.Duration
}

// Reclaim returns expired leases to pending.
func (s *Store) Reclaim(ctx context.Context, limit int) ([]Reclaimed, error) {
	rows, err := s.pool.Query(ctx, s.q.reclaim, limit)
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	defer rows.Close()

	var out []Reclaimed
	for rows.Next() {
		var (
			r       Reclaimed
			seconds float64
		)
		if err := rows.Scan(&r.ID, &r.Stream, &r.Owner, &seconds); err != nil {
			return nil, fmt.Errorf("scan reclaimed message: %w", err)
		}
		r.Overdue = time.Duration(seconds * float64(time.Second))
		out = append(out, r)
	}

	return out, rows.Err()
}

// Stats is the gauge snapshot. Delivered rows are deliberately absent: counting
// them means scanning them.
type Stats struct {
	Pending       int64
	Processing    int64
	Failed        int64
	OldestPending time.Duration
	// Deferred counts rows waiting on a broker that could not be reached. It is
	// a subset of Pending and Processing rather than a status of its own, and it
	// is what separates a backlog that is merely growing from one that is stuck.
	Deferred int64
}

// Stats samples the current backlog.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var (
		st      Stats
		seconds float64
	)

	err := s.pool.QueryRow(ctx, s.q.stats).
		Scan(&st.Pending, &st.Processing, &st.Failed, &seconds, &st.Deferred)
	if err != nil {
		return st, fmt.Errorf("stats: %w", err)
	}
	st.OldestPending = time.Duration(seconds * float64(time.Second))

	return st, nil
}

// Purge deletes delivered rows older than the retention window, up to limit
// per call, and reports how many it removed.
func (s *Store) Purge(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, s.q.purge, retention.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge: %w", err)
	}

	return tag.RowsAffected(), nil
}

func decodeJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}

	return json.Unmarshal(raw, dst)
}

func errText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// Requeue returns the named failed messages to the queue and reports which
// ones it moved. Messages that were not failed are left alone and are simply
// absent from the result.
func (s *Store) Requeue(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	return s.collectIDs(ctx, "requeue", s.q.requeue, ids)
}

// RequeueFailedBefore returns up to limit messages that failed before the given
// time, for reprocessing a batch without first collecting its identifiers.
func (s *Store) RequeueFailedBefore(ctx context.Context, before time.Time, limit int) ([]string, error) {
	return s.collectIDs(ctx, "requeue failed", s.q.requeueBefore, before, limit)
}

func (s *Store) collectIDs(ctx context.Context, op, sql string, args ...any) ([]string, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan %s result: %w", op, err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// FailedMessage is one entry of the failed-message listing.
type FailedMessage struct {
	ID        string    `json:"id"`
	Stream    string    `json:"stream"`
	Topic     string    `json:"topic"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	CreatedAt time.Time `json:"created_at"`
}

// Cursor positions a page of failed messages. The zero value starts at the
// beginning.
type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// ListFailed pages through failed messages in creation order, optionally
// restricted to one stream. An empty stream means every stream.
//
// Keyset pagination rather than OFFSET: an offset scan re-reads every skipped
// row, so paging through a large failure backlog gets slower with every page.
func (s *Store) ListFailed(
	ctx context.Context, after Cursor, limit int, stream string,
) ([]FailedMessage, error) {
	id := after.ID
	if id == "" {
		id = "00000000-0000-0000-0000-000000000000"
	}

	rows, err := s.pool.Query(ctx, s.q.listFailed, after.CreatedAt, id, limit, stream)
	if err != nil {
		return nil, fmt.Errorf("list failed: %w", err)
	}
	defer rows.Close()

	out := make([]FailedMessage, 0, limit)
	for rows.Next() {
		var m FailedMessage
		if err := rows.Scan(&m.ID, &m.Stream, &m.Topic, &m.Attempts, &m.LastError, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan failed message: %w", err)
		}
		out = append(out, m)
	}

	return out, rows.Err()
}

// TryLock takes a session-scoped advisory lock and reports whether it got it.
//
// It is how the singleton work — reclaiming, sampling the gauges, sweeping
// delivered rows — stays on one replica per cycle. Every instance tries; the
// one that wins does the work and the rest move on without waiting, which is
// the right shape for a periodic task nobody is blocked on.
func (s *Store) TryLock(ctx context.Context, class, key int32) (release func(), ok bool, err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire connection for advisory lock: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, class, key).Scan(&acquired); err != nil {
		conn.Release()

		return nil, false, fmt.Errorf("try advisory lock: %w", err)
	}

	if !acquired {
		conn.Release()

		return nil, false, nil
	}

	// Idempotent: releasing early and then again through a defer is the
	// ordinary way to write this, and a release that panics the second time is
	// a trap rather than a contract.
	return idempotent(func() {
		// The caller's context is often already canceled by the time a periodic
		// task unwinds; an advisory lock left held would block every later cycle
		// until the connection is recycled.
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1, $2)`, class, key)
		conn.Release()
	}), true, nil
}

// idempotent wraps fn so that only the first call runs it.
func idempotent(fn func()) func() {
	var once sync.Once

	return func() { once.Do(fn) }
}

// quoteIdent wraps an identifier that config.Validate has already checked.
func quoteIdent(ident string) string { return `"` + ident + `"` }

// FetchByIDs reads whole messages back by identifier, in no particular order.
//
// The dead-letter forwarder uses it: an iteration event carries the ids of the
// messages that stopped being retried, not their contents, and keeping payloads
// off the event bus is what stops an observation channel from carrying the data
// itself.
func (s *Store) FetchByIDs(ctx context.Context, ids []string) ([]core.Message, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, s.q.fetchByIDs, ids)
	if err != nil {
		return nil, fmt.Errorf("fetch messages: %w", err)
	}
	defer rows.Close()

	out := make([]core.Message, 0, len(ids))
	for rows.Next() {
		var (
			m       core.Message
			headers []byte
			target  []byte
		)

		if err := rows.Scan(&m.ID, &m.Stream, &m.Topic, &m.Payload, &headers, &target,
			&m.Attempts, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		if err := decodeJSON(headers, &m.Headers); err != nil {
			return nil, fmt.Errorf("message %s: decode headers: %w", m.ID, err)
		}
		if err := decodeJSON(target, &m.Target); err != nil {
			return nil, fmt.Errorf("message %s: decode target: %w", m.ID, err)
		}

		out = append(out, m)
	}

	return out, rows.Err()
}
