// Package postgres delivers messages into a table instead of to a broker.
//
// The table is the consumer's inbox: the dispatcher inserts, and nothing else.
// It does not create the table, does not read it, does not update it and does
// not clean it up. That boundary is what keeps this a destination rather than
// the beginnings of a consumer framework, which is a different product.
//
// What it buys is a guarantee no broker offers. Delivery is still at-least-once
// — the insert and the write-back marking the row sent are two commits, and a
// replica can die between them — but the inbox's primary key makes the repeat
// harmless. Deduplication stops being an obligation on the consumer's code and
// becomes a property of its schema.
//
// See docs/InboxSpec.ru.md for the shape of the table and the flows.
package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// Publisher writes into one inbox table.
type Publisher struct {
	pool   *pgxpool.Pool
	cfg    *config.PostgresDriver
	insert string
	log    *slog.Logger
}

// New opens a pool and verifies the destination.
//
// The table is checked here rather than on the first message, for the same
// reason every broker must be reachable at startup: a destination that is not
// there is a configuration mistake, and configuration mistakes should fail the
// boot rather than surface as a permanent failure on a real message at three in
// the morning.
func New(ctx context.Context, cfg *config.PostgresDriver, log *slog.Logger) (*Publisher, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres driver %q: parse DSN: %w", cfg.Name(), err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = 0
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// Named after the driver, not the process: an idle connection in
	// pg_stat_activity should say which destination opened it.
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "go-outbox/" + cfg.Name()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres driver %q: open pool: %w", cfg.Name(), err)
	}

	p := &Publisher{
		pool: pool,
		cfg:  cfg,
		log:  log,
		// Only the columns the dispatcher owns. Everything the consumer adds —
		// processed_at, an attempt counter, whatever it needs — has a default
		// and is none of this driver's business.
		//
		// ON CONFLICT is the whole point: a repeat after a replica died between
		// the insert and the write-back is the expected case, not an error.
		//
		// The conflict target is named on purpose, and it costs a wider grant:
		// PostgreSQL requires SELECT on the table when ON CONFLICT has a target,
		// so the role needs INSERT and SELECT rather than INSERT alone. The
		// target-less form would need only INSERT and is wrong here — it
		// swallows any unique violation, so a message with a new id but a taken
		// business key would vanish and be reported as delivered.
		insert: fmt.Sprintf(`
			INSERT INTO %s (id, stream, topic, payload, headers)
			SELECT id, stream, topic, payload, headers::jsonb
			  FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bytea[], $5::text[])
			    AS t(id, stream, topic, payload, headers)
			ON CONFLICT (id) DO NOTHING`, cfg.Qualified()),
	}

	if err := p.verify(ctx); err != nil {
		pool.Close()

		return nil, err
	}

	log.Info("inbox ready",
		slog.String("table", cfg.Schema+"."+cfg.Table),
		slog.Bool("same_database", cfg.SameDatabase),
	)

	return p, nil
}

// verify checks that the destination exists and that this connection may write
// to it, without writing anything.
func (p *Publisher) verify(ctx context.Context) error {
	var exists bool

	err := p.pool.QueryRow(ctx,
		`SELECT to_regclass($1) IS NOT NULL`, p.cfg.Qualified()).Scan(&exists)
	if err != nil {
		return fmt.Errorf("postgres driver %q: reach %s: %w", p.cfg.Name(), p.cfg.Qualified(), err)
	}
	if !exists {
		return fmt.Errorf(
			"postgres driver %q: table %s does not exist; the inbox belongs to its consumer and is not created here",
			p.cfg.Name(), p.cfg.Qualified())
	}

	return nil
}

// Publish inserts the batch.
//
// One statement for the whole batch on the happy path, which is the only path
// that runs at volume. When the database answers with a refusal the batch is
// replayed one message at a time, because a positional error slice is the
// contract and a single malformed message must not condemn the rest to a
// pointless retry. That second pass is skipped when the database could not be
// reached at all: there is nothing to learn from asking it n more times.
func (p *Publisher) Publish(ctx context.Context, msgs []core.Message) []error {
	results := make([]error, len(msgs))
	if len(msgs) == 0 {
		return results
	}

	writeCtx, cancel := context.WithTimeout(ctx, p.cfg.WriteTimeout)
	defer cancel()

	if err := p.insertBatch(writeCtx, msgs); err == nil {
		return results
	} else if classified := classify(err, ctx.Err() == nil); core.IsUnavailable(classified) {
		for i := range results {
			results[i] = classified
		}

		return results
	}

	// The database refused something. Find out what.
	for i := range msgs {
		if err := p.insertBatch(writeCtx, msgs[i:i+1]); err != nil {
			results[i] = classify(err, ctx.Err() == nil)
		}
	}

	return results
}

func (p *Publisher) insertBatch(ctx context.Context, msgs []core.Message) error {
	ids := make([]string, len(msgs))
	streams := make([]string, len(msgs))
	topics := make([]string, len(msgs))
	payloads := make([][]byte, len(msgs))
	headers := make([]string, len(msgs))

	for i, msg := range msgs {
		dest := broker.Resolve(p.cfg.Naming(), msg)

		encoded, err := encodeHeaders(msg)
		if err != nil {
			return err
		}

		ids[i] = msg.ID
		streams[i] = msg.Stream
		// The effective name, prefix and version applied — the same string a
		// consumer would have subscribed to at a broker.
		topics[i] = dest.Topic
		payloads[i] = msg.Payload
		headers[i] = encoded
	}

	_, err := p.pool.Exec(ctx, p.insert, ids, streams, topics, payloads, headers)

	return err
}

// HealthCheck reports whether the destination is reachable.
func (p *Publisher) HealthCheck(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres driver %q: %w", p.cfg.Name(), err)
	}

	return nil
}

// Close releases the pool.
func (p *Publisher) Close(context.Context) error {
	p.pool.Close()

	return nil
}

var _ broker.Publisher = (*Publisher)(nil)
