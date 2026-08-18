// Package outboxclient writes outbox messages from a producer.
//
// It exists to make the transactional part hard to get wrong. The whole pattern
// rests on one property — the message is written in the same transaction as the
// business change it describes — and the way to lose that property is to hand
// people a column list and an INSERT to copy.
//
//	tx, err := pool.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback(ctx)
//
//	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, id); err != nil {
//	    return err
//	}
//
//	if err := client.Enqueue(ctx, tx, outboxclient.Message{
//	    Stream:  "local",
//	    Topic:   "account.debited",
//	    Payload: payload,
//	}); err != nil {
//	    return err
//	}
//
//	return tx.Commit(ctx)
//
// Writing through anything other than the transaction carrying the business
// change defeats the point: the message can then be published for a change that
// was rolled back, or lost for one that was not.
package outboxclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Message is what a producer writes.
type Message struct {
	// ID identifies the message. Left empty, a UUIDv7 is generated: it sorts by
	// creation time, so the primary key index stays append-ordered instead of
	// scattering writes across the whole tree the way a v4 does.
	ID string
	// Stream selects the broker. It must name a stream the dispatcher has
	// configured.
	Stream string
	// Topic is the logical name, without the driver's prefix or version suffix.
	// The dispatcher applies those; a consumer subscribes to the result.
	Topic string
	// Payload is delivered to the broker byte for byte.
	Payload []byte
	// Headers travel with the message. A traceparent placed here lets the
	// consumer continue this producer's trace.
	Headers map[string]string
	// Target routes the message: partition key, topic version, and the
	// exchange and routing key for AMQP.
	Target Target
	// AvailableAt delays the first delivery attempt. Zero means immediately.
	AvailableAt time.Time
}

// Target is the routing envelope.
type Target struct {
	Key        string `json:"key,omitempty"`
	Version    int    `json:"version,omitempty"`
	Exchange   string `json:"exchange,omitempty"`
	RoutingKey string `json:"routing_key,omitempty"`
}

// Execer is the subset of pgx a writer needs. Both *pgxpool.Pool and pgx.Tx
// satisfy it; passing a transaction is the entire point.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Client writes into one outbox table.
type Client struct {
	insert string
	table  string
}

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// New builds a client for the given schema and table, which must match the
// dispatcher's OUTBOX_DB_SCHEMA and OUTBOX_DB_TABLE.
func New(schema, table string) (*Client, error) {
	if !identifier.MatchString(schema) {
		return nil, fmt.Errorf("schema %q is not a valid lower-case unquoted identifier", schema)
	}
	if !identifier.MatchString(table) {
		return nil, fmt.Errorf("table %q is not a valid lower-case unquoted identifier", table)
	}

	qualified := fmt.Sprintf("%q.%q", schema, table)

	return &Client{
		table: qualified,
		// Only the producer's columns. Everything else has a server default,
		// and naming a dispatcher-owned column here is how a producer ends up
		// writing a lease or a status it has no business setting.
		insert: fmt.Sprintf(
			`INSERT INTO %s (id, stream, topic, payload, headers, target, available_at)
			 VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()))`, qualified),
	}, nil
}

// Default builds a client for the dispatcher's default table, outbox.messages.
func Default() *Client {
	c, err := New("outbox", "messages")
	if err != nil {
		panic(err) // The literals above are valid identifiers.
	}

	return c
}

// Enqueue writes one message. Pass the transaction carrying the business change.
func (c *Client) Enqueue(ctx context.Context, db Execer, msg Message) error {
	id, headers, target, err := c.prepare(msg)
	if err != nil {
		return err
	}

	var availableAt any
	if !msg.AvailableAt.IsZero() {
		availableAt = msg.AvailableAt
	}

	if _, err := db.Exec(ctx, c.insert,
		id, msg.Stream, msg.Topic, msg.Payload, headers, target, availableAt,
	); err != nil {
		return fmt.Errorf("enqueue outbox message: %w", err)
	}

	return nil
}

// EnqueueBatch writes several messages in one round trip, still inside the
// caller's transaction.
func (c *Client) EnqueueBatch(ctx context.Context, db pgx.Tx, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	for _, msg := range msgs {
		id, headers, target, err := c.prepare(msg)
		if err != nil {
			return err
		}

		var availableAt any
		if !msg.AvailableAt.IsZero() {
			availableAt = msg.AvailableAt
		}

		batch.Queue(c.insert, id, msg.Stream, msg.Topic, msg.Payload, headers, target, availableAt)
	}

	results := db.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()

	for i := range msgs {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("enqueue outbox message %d of %d: %w", i+1, len(msgs), err)
		}
	}

	return nil
}

func (c *Client) prepare(msg Message) (id string, headers, target []byte, err error) {
	if msg.Stream == "" {
		return "", nil, nil, errors.New("stream must not be empty")
	}
	if msg.Topic == "" {
		return "", nil, nil, errors.New("topic must not be empty")
	}

	id = msg.ID
	if id == "" {
		v7, err := uuid.NewV7()
		if err != nil {
			return "", nil, nil, fmt.Errorf("generate message id: %w", err)
		}
		id = v7.String()
	}

	if msg.Headers == nil {
		headers = []byte(`{}`)
	} else if headers, err = json.Marshal(msg.Headers); err != nil {
		return "", nil, nil, fmt.Errorf("encode headers: %w", err)
	}

	if target, err = json.Marshal(msg.Target); err != nil {
		return "", nil, nil, fmt.Errorf("encode target: %w", err)
	}

	return id, headers, target, nil
}
