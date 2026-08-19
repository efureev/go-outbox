// Package outboxsql writes outbox messages from a producer through
// database/sql.
//
// It is [outboxclient] for everybody who is not on pgx. The pattern rests on one
// property — the message is written in the same transaction as the business
// change it describes — and that property does not depend on which driver
// carries the statement.
//
//	tx, err := db.BeginTx(ctx, nil)
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback()
//
//	if _, err := tx.ExecContext(ctx,
//	    `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, id); err != nil {
//	    return err
//	}
//
//	if err := client.Enqueue(ctx, tx, outboxsql.Message{
//	    Stream:  "local",
//	    Topic:   "account.debited",
//	    Payload: payload,
//	}); err != nil {
//	    return err
//	}
//
//	return tx.Commit()
//
// Writing through anything other than the transaction carrying the business
// change defeats the point: the message can then be published for a change that
// was rolled back, or lost for one that was not.
//
// This package deliberately imports no driver and no pgx. It is the reason it
// exists as a package of its own rather than as another constructor in
// [outboxclient]: importing that one brings pgx with it, which is exactly what a
// team on sqlx, gorm or the standard library was trying to avoid.
//
// [outboxclient]: https://pkg.go.dev/github.com/efureev/go-outbox/pkg/outboxclient
package outboxsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Message is what a producer writes.
//
// It mirrors outboxclient.Message field for field, and a test holds the two to
// that. They are separate types rather than one shared one because sharing would
// put this package back in the same module graph as pgx, which is the whole
// thing it avoids.
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

// Execer is the subset of database/sql a writer needs. *sql.DB, *sql.Tx,
// *sqlx.DB and *sqlx.Tx all satisfy it; passing a transaction is the entire
// point.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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
		//
		// The placeholders are PostgreSQL's $1…$n rather than the ? that some
		// database/sql drivers use, because the target is PostgreSQL either
		// way: lib/pq and pgx/v5/stdlib both take them.
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
	args, err := c.prepare(msg)
	if err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, c.insert, args...); err != nil {
		return fmt.Errorf("enqueue outbox message: %w", err)
	}

	return nil
}

// EnqueueBatch writes several messages inside the caller's transaction.
//
// It is a statement per message rather than one round trip for all of them.
// pgx has a batch protocol and database/sql has none, so this is the cost of
// working through any driver: at a few messages per business transaction it does
// not matter, and a producer writing hundreds at a time is better served by
// outboxclient and pgx.
//
// Nothing is committed here. A failure part-way leaves the caller's transaction
// to roll back, which is the same guarantee as writing one message.
func (c *Client) EnqueueBatch(ctx context.Context, db Execer, msgs []Message) error {
	for i, msg := range msgs {
		if err := c.Enqueue(ctx, db, msg); err != nil {
			return fmt.Errorf("enqueue message %d of %d: %w", i+1, len(msgs), err)
		}
	}

	return nil
}

// prepare validates the message and renders the statement's arguments.
//
// The JSON columns are passed as strings rather than as byte slices. Both forms
// reach a jsonb column intact through lib/pq and pgx/v5/stdlib, but a []byte is
// the shape a driver is entitled to encode as bytea, and this package cannot
// know which driver it is talking to.
func (c *Client) prepare(msg Message) ([]any, error) {
	if msg.Stream == "" {
		return nil, errors.New("stream must not be empty")
	}
	if msg.Topic == "" {
		return nil, errors.New("topic must not be empty")
	}

	id := msg.ID
	if id == "" {
		v7, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("generate message id: %w", err)
		}
		id = v7.String()
	}

	headers := "{}"
	if msg.Headers != nil {
		raw, err := json.Marshal(msg.Headers)
		if err != nil {
			return nil, fmt.Errorf("encode headers: %w", err)
		}
		headers = string(raw)
	}

	target, err := json.Marshal(msg.Target)
	if err != nil {
		return nil, fmt.Errorf("encode target: %w", err)
	}

	// nil rather than a zero time, so the statement's coalesce falls through to
	// the database's now() instead of scheduling delivery for the year zero.
	var availableAt any
	if !msg.AvailableAt.IsZero() {
		availableAt = msg.AvailableAt
	}

	return []any{
		id, msg.Stream, msg.Topic, msg.Payload, headers, string(target), availableAt,
	}, nil
}
