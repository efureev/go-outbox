//go:build integration

package integration

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/efureev/go-outbox/pkg/outboxclient"
	"github.com/efureev/go-outbox/pkg/outboxsql"
)

// The producer's side of the cost, which is the side that sits inside a business
// transaction and is therefore the one a user feels.
//
// Use case 6 says EnqueueBatch on database/sql is a statement per message rather
// than one round trip, that at a handful per transaction this is not worth a
// thought, and that a producer writing hundreds at a time belongs on
// outboxclient and pgx. Those are three separate claims and none of them had a
// number behind it.
//
// Three arms, so the two effects can be told apart:
//
//	outboxclient/batch  pgx, one round trip for the whole batch
//	outboxclient/loop   pgx, one round trip per message
//	outboxsql/loop      database/sql, one round trip per message
//
// The gap between the two loops is the driver. The gap between batch and its own
// loop is the batch protocol. Comparing only the first and the last would credit
// the protocol with both.
//
//	go test -tags integration -run '^$' -bench BenchmarkEnqueue -benchtime 200x ./test/integration/
//
// One iteration is one business transaction: begin, enqueue, commit. Committing
// is part of the cost a producer pays and part of what amortises a large batch,
// so it is inside the timer.

var enqueueSizes = []int{1, 10, 100, 500}

func benchMessages(n int) []outboxclient.Message {
	msgs := make([]outboxclient.Message, n)
	for i := range msgs {
		msgs[i] = outboxclient.Message{
			Stream:  "local",
			Topic:   fmt.Sprintf("bench.%d", i),
			Payload: []byte(`{"benchmark":true,"n":` + itoa(i) + `}`),
		}
	}

	return msgs
}

func benchSQLMessages(n int) []outboxsql.Message {
	msgs := make([]outboxsql.Message, n)
	for i := range msgs {
		msgs[i] = outboxsql.Message{
			Stream:  "local",
			Topic:   fmt.Sprintf("bench.%d", i),
			Payload: []byte(`{"benchmark":true,"n":` + itoa(i) + `}`),
		}
	}

	return msgs
}

func BenchmarkEnqueue(b *testing.B) {
	for _, n := range enqueueSizes {
		b.Run(fmt.Sprintf("outboxclient/batch/messages=%d", n), func(b *testing.B) {
			benchmarkPgxEnqueue(b, n, true)
		})
		b.Run(fmt.Sprintf("outboxclient/loop/messages=%d", n), func(b *testing.B) {
			benchmarkPgxEnqueue(b, n, false)
		})
		b.Run(fmt.Sprintf("outboxsql/loop/messages=%d", n), func(b *testing.B) {
			benchmarkSQLEnqueue(b, n)
		})
	}
}

func benchmarkPgxEnqueue(b *testing.B, n int, useBatch bool) {
	b.Helper()

	f := newFixture(b)

	client, err := outboxclient.New(f.Schema, "messages")
	if err != nil {
		b.Fatalf("client: %v", err)
	}

	msgs := benchMessages(n)
	ctx := b.Context()

	b.ResetTimer()

	for range b.N {
		tx, err := f.Pool.Begin(ctx)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}

		if useBatch {
			err = client.EnqueueBatch(ctx, tx, msgs)
		} else {
			err = enqueueOneByOne(ctx, client, tx, msgs)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			b.Fatalf("enqueue: %v", err)
		}

		if err := tx.Commit(ctx); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}

	b.StopTimer()

	// Per message rather than per transaction, which is the figure the two arms
	// can be compared on across batch sizes.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/msg")
}

func enqueueOneByOne(
	ctx context.Context, c *outboxclient.Client, tx pgx.Tx, msgs []outboxclient.Message,
) error {
	for _, m := range msgs {
		if err := c.Enqueue(ctx, tx, m); err != nil {
			return err
		}
	}

	return nil
}

func benchmarkSQLEnqueue(b *testing.B, n int) {
	b.Helper()

	f := newFixture(b)

	// pgx through database/sql, so the wire protocol is the same as the arm
	// above and the only difference left is the batch protocol and the
	// database/sql layer itself.
	db, err := gosql.Open("pgx", dsn())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	client, err := outboxsql.New(f.Schema, "messages")
	if err != nil {
		b.Fatalf("client: %v", err)
	}

	msgs := benchSQLMessages(n)
	ctx := b.Context()

	b.ResetTimer()

	for range b.N {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}

		if err := client.EnqueueBatch(ctx, tx, msgs); err != nil {
			_ = tx.Rollback()
			b.Fatalf("enqueue: %v", err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}

	b.StopTimer()

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/msg")
}
